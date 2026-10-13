"""Generate synthetic e-commerce events and land them in the raw zone (local folder or S3).

About 3% of rows are deliberately broken so the validation step has something to catch.

Usage:
    python -m pipeline.generate_events --date 2026-10-06 --num-events 100000
"""

import argparse
import os
import shutil
import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline import config

NUM_USERS = 200_000
NUM_PRODUCTS = 5_000
CATEGORIES = np.array(
    ["electronics", "fashion", "home", "beauty", "sports", "books", "toys", "grocery"], dtype=object
)
DEVICES = np.array(["mobile", "desktop", "tablet"], dtype=object)
DEVICE_WEIGHTS = [0.6, 0.32, 0.08]
COUNTRIES = np.array(["US", "CA", "GB", "DE", "IN", "BR", "AU", "FR"], dtype=object)
COUNTRY_WEIGHTS = [0.45, 0.08, 0.1, 0.09, 0.12, 0.06, 0.05, 0.05]
EVENT_TYPES = np.array(config.ALLOWED_EVENT_TYPES, dtype=object)
EVENT_WEIGHTS = [0.62, 0.17, 0.04, 0.10, 0.07]

# Share of rows that get each kind of problem.
BAD_DATA_RATES = {
    "missing_user_id": 0.010,
    "negative_price": 0.005,
    "unknown_event_type": 0.005,
    "bad_timestamp": 0.003,
    "future_timestamp": 0.002,
    "zero_quantity": 0.002,
    "duplicate_event_id": 0.005,
}


def build_product_catalog(seed: int):
    rng = np.random.default_rng(seed)
    prices = np.round(rng.uniform(4.99, 499.99, NUM_PRODUCTS + 1), 2)
    categories = CATEGORIES[np.arange(NUM_PRODUCTS + 1) % len(CATEGORIES)]
    return prices, categories


def generate_chunk(run_date: str, start_index: int, size: int, rng, prices, categories) -> pd.DataFrame:
    day_start = pd.Timestamp(run_date)
    seconds = rng.integers(0, 86_400, size)
    timestamps = np.asarray(
        (day_start + pd.to_timedelta(seconds, unit="s")).strftime("%Y-%m-%d %H:%M:%S"), dtype=object
    )

    index_str = pd.Series(np.arange(start_index, start_index + size)).astype(str).str.zfill(10)
    event_ids = ("evt_" + run_date.replace("-", "") + "_" + index_str).to_numpy(dtype=object)

    users = rng.integers(1, NUM_USERS + 1, size)
    user_ids = ("u_" + pd.Series(users).astype(str)).to_numpy(dtype=object)
    session_no = rng.integers(1, 4, size)
    session_ids = (
        "s_" + pd.Series(users).astype(str) + "_" + run_date.replace("-", "") + "_"
        + pd.Series(session_no).astype(str)
    ).to_numpy(dtype=object)

    products = rng.integers(1, NUM_PRODUCTS + 1, size)
    product_ids = ("p_" + pd.Series(products).astype(str)).to_numpy(dtype=object)
    price = prices[products].astype(float)
    quantity = rng.integers(1, 4, size)
    event_type = rng.choice(EVENT_TYPES, size=size, p=EVENT_WEIGHTS)

    # Inject data quality problems.
    def pick(rate: float) -> np.ndarray:
        return rng.random(size) < rate

    user_ids[pick(BAD_DATA_RATES["missing_user_id"])] = None
    neg = pick(BAD_DATA_RATES["negative_price"])
    price[neg] = -price[neg]
    event_type[pick(BAD_DATA_RATES["unknown_event_type"])] = "unknown_event"
    timestamps[pick(BAD_DATA_RATES["bad_timestamp"])] = "not-a-date"
    timestamps[pick(BAD_DATA_RATES["future_timestamp"])] = "2099-01-01 00:00:00"
    quantity[pick(BAD_DATA_RATES["zero_quantity"])] = 0
    dup = pick(BAD_DATA_RATES["duplicate_event_id"])
    dup[0] = False  # row 0 has no previous row to copy from
    dup_idx = np.flatnonzero(dup)
    event_ids[dup_idx] = event_ids[dup_idx - 1]

    return pd.DataFrame(
        {
            "event_id": event_ids,
            "event_timestamp": timestamps,
            "user_id": user_ids,
            "session_id": session_ids,
            "event_type": event_type,
            "product_id": product_ids,
            "category": categories[products],
            "price": price,
            "quantity": quantity,
            "device": rng.choice(DEVICES, size=size, p=DEVICE_WEIGHTS),
            "country": rng.choice(COUNTRIES, size=size, p=COUNTRY_WEIGHTS),
        },
        columns=config.RAW_COLUMNS,
    )


def write_chunks(run_date: str, num_events: int, chunk_size: int, out_dir: Path, seed: int) -> list:
    out_dir.mkdir(parents=True, exist_ok=True)
    prices, categories = build_product_catalog(seed)
    rng = np.random.default_rng([seed, date.fromisoformat(run_date).toordinal()])
    files = []
    for part, start in enumerate(range(0, num_events, chunk_size)):
        size = min(chunk_size, num_events - start)
        df = generate_chunk(run_date, start, size, rng, prices, categories)
        path = out_dir / f"part-{part:05d}.csv"
        df.to_csv(path, index=False)
        files.append(path)
        print(f"  wrote {path.name}: {size:,} rows")
    return files


def upload_to_s3(files: list, run_date: str) -> str:
    import boto3  # imported here so local mode does not need boto3

    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    prefix = f"raw/events/ingest_date={run_date}/"

    # Delete old files for this date so re-runs are idempotent.
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=config.S3_BUCKET, Delete={"Objects": keys})

    for path in files:
        key = prefix + path.name
        s3.upload_file(str(path), config.S3_BUCKET, key)
        print(f"  uploaded s3://{config.S3_BUCKET}/{key}")
    return f"s3://{config.S3_BUCKET}/{prefix}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Event date, YYYY-MM-DD")
    parser.add_argument("--num-events", type=int, default=int(os.getenv("NUM_EVENTS", "100000")))
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-monitoring", action="store_true", help="Do not write to the monitoring DB")
    args = parser.parse_args()

    date.fromisoformat(args.date)  # fails fast on a bad date
    if args.num_events <= 0:
        raise ValueError("--num-events must be positive")
    config.validate_storage_settings()

    print(f"Generating {args.num_events:,} events for {args.date} (storage: {config.STORAGE_MODE})")
    if config.STORAGE_MODE == "s3":
        with tempfile.TemporaryDirectory() as tmp:
            files = write_chunks(args.date, args.num_events, args.chunk_size, Path(tmp), args.seed)
            location = upload_to_s3(files, args.date)
    else:
        out_dir = Path(config.LOCAL_DATA_ROOT) / "raw" / "events" / f"ingest_date={args.date}"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        write_chunks(args.date, args.num_events, args.chunk_size, out_dir, args.seed)
        location = str(out_dir)

    if not args.skip_monitoring:
        from pipeline.monitoring_db import update_freshness

        update_freshness("raw_events", location, args.num_events)
    print(f"Done. Raw data at {location}")


if __name__ == "__main__":
    main()
