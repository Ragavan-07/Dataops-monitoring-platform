"""Central configuration. Every setting can be overridden with an environment variable."""

import os
from dataclasses import dataclass
from pathlib import Path

# "local" writes to the ./data folder, "s3" writes to an Amazon S3 bucket.
STORAGE_MODE = os.getenv("STORAGE_MODE", "local").strip().lower()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1").strip()

# <repo>/data when run from the repo, /opt/airflow/data inside the Airflow containers.
_DEFAULT_LOCAL_ROOT = Path(__file__).resolve().parents[1] / "data"
LOCAL_DATA_ROOT = os.getenv("LOCAL_DATA_ROOT", str(_DEFAULT_LOCAL_ROOT))

MONITORING_DB_URL = os.getenv(
    "MONITORING_DB_URL", "postgresql://monitor:monitor@localhost:5433/monitoring"
)

# Validation fails the Airflow task if more than this share of rows is invalid.
MAX_INVALID_RATIO = float(os.getenv("MAX_INVALID_RATIO", "0.10"))

# A daily pipeline is "stale" after 26 hours without an update.
FRESHNESS_SLA_MINUTES = int(os.getenv("FRESHNESS_SLA_MINUTES", "1560"))

ALLOWED_EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "checkout", "purchase"]

RAW_COLUMNS = [
    "event_id",
    "event_timestamp",
    "user_id",
    "session_id",
    "event_type",
    "product_id",
    "category",
    "price",
    "quantity",
    "device",
    "country",
]


def validate_storage_settings() -> None:
    if STORAGE_MODE not in ("local", "s3"):
        raise ValueError(f"STORAGE_MODE must be 'local' or 's3', got '{STORAGE_MODE}'")
    if STORAGE_MODE == "s3" and not S3_BUCKET:
        raise ValueError("STORAGE_MODE is 's3' but S3_BUCKET is not set")


def data_root() -> str:
    """Root URI that Spark reads from and writes to."""
    validate_storage_settings()
    if STORAGE_MODE == "s3":
        return f"s3a://{S3_BUCKET}"
    return LOCAL_DATA_ROOT


@dataclass(frozen=True)
class Paths:
    raw: str
    validated: str
    quarantine: str
    daily_sales: str
    conversion_funnel: str
    top_products: str

    def raw_partition(self, run_date: str) -> str:
        return f"{self.raw}/ingest_date={run_date}"


def get_paths(prefix: str = "") -> Paths:
    """Dataset locations. `prefix` lets the benchmark write to a separate folder."""
    root = data_root()
    out = f"{root}/{prefix}".rstrip("/")
    return Paths(
        raw=f"{root}/raw/events",  # raw data is always shared
        validated=f"{out}/validated/events",
        quarantine=f"{out}/quarantine/events",
        daily_sales=f"{out}/curated/daily_sales",
        conversion_funnel=f"{out}/curated/conversion_funnel",
        top_products=f"{out}/curated/top_products",
    )
