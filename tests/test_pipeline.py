from pathlib import Path

import pytest

from pipeline.config import Paths
from pipeline.generate_events import write_chunks
from pipeline.transform_events import run_transform
from pipeline.validate_events import RAW_SCHEMA, apply_quality_rules, run_validation

RUN_DATE = "2026-10-06"
GOOD = ("e1", "2026-10-06 10:00:00", "u1", "s1", "purchase", "p1", "books", "10.50", "2", "mobile", "US")


def row(**changes):
    names = [f.name for f in RAW_SCHEMA.fields]
    values = dict(zip(names, GOOD))
    values.update(changes)
    return tuple(values[n] for n in names)


def failed_rules_by_id(spark, rows):
    df = spark.createDataFrame(rows, RAW_SCHEMA)
    return {r["event_id"]: sorted(r["failed_rules"]) for r in apply_quality_rules(df).collect()}


def test_quality_rules_flag_each_problem(spark):
    result = failed_rules_by_id(spark, [
        row(event_id="ok"),
        row(event_id="no_user", user_id=None),
        row(event_id="bad_type", event_type="teleport"),
        row(event_id="bad_ts", event_timestamp="not-a-date"),
        row(event_id="future", event_timestamp="2099-01-01 00:00:00"),
        row(event_id="neg_price", price="-5"),
        row(event_id="zero_qty", quantity="0"),
        row(event_id="two_problems", user_id="", price="abc"),
    ])
    assert result["ok"] == []
    assert result["no_user"] == ["missing_user_id"]
    assert result["bad_type"] == ["invalid_event_type"]
    assert result["bad_ts"] == ["invalid_timestamp"]
    assert result["future"] == ["future_timestamp"]
    assert result["neg_price"] == ["invalid_price"]
    assert result["zero_qty"] == ["invalid_quantity"]
    assert result["two_problems"] == ["invalid_price", "missing_user_id"]


def make_paths(root: Path) -> Paths:
    return Paths(
        raw=str(root / "raw" / "events"),
        validated=str(root / "validated" / "events"),
        quarantine=str(root / "quarantine" / "events"),
        daily_sales=str(root / "curated" / "daily_sales"),
        conversion_funnel=str(root / "curated" / "conversion_funnel"),
        top_products=str(root / "curated" / "top_products"),
    )


@pytest.mark.parametrize("optimized", [True, False])
def test_end_to_end(spark, tmp_path, optimized):
    paths = make_paths(tmp_path)
    write_chunks(RUN_DATE, 5000, 2000, Path(paths.raw_partition(RUN_DATE)), seed=7)

    result = run_validation(spark, RUN_DATE, paths, optimized=optimized, record_metrics=False)
    assert result["total_records"] == 5000
    assert 0 < result["invalid_records"] < 500  # roughly 3% bad rows are injected
    # valid_records excludes duplicate event_ids, so it can be smaller than total - invalid
    assert result["valid_records"] <= result["total_records"] - result["invalid_records"]

    validated = spark.read.parquet(paths.validated)
    assert validated.count() == result["valid_records"]
    assert validated.select("event_id").distinct().count() == result["valid_records"]

    counts = run_transform(spark, RUN_DATE, paths, optimized=optimized, record_metrics=False)
    assert counts["daily_sales"] > 0
    assert counts["conversion_funnel"] == 1
    assert counts["top_products"] == 10
