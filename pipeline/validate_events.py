"""Validate raw events with PySpark.

Valid rows    -> validated/events   (Parquet, partitioned by event_date)
Invalid rows  -> quarantine/events  (Parquet, with the list of rules each row broke)
Rule counts   -> monitoring DB (quality_checks table)

Usage:
    python -m pipeline.validate_events --run-date 2026-10-06
"""

import argparse
from typing import Dict

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config
from pipeline.spark_utils import get_spark

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in config.RAW_COLUMNS])
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


class DataQualityError(Exception):
    """Raised when too many rows fail validation."""


def read_raw(spark: SparkSession, path: str, optimized: bool) -> DataFrame:
    reader = spark.read.option("header", True)
    if optimized:
        # An explicit schema avoids an extra full pass over the files.
        df = reader.schema(RAW_SCHEMA).csv(path)
    else:
        # Baseline: let Spark scan the data to guess types, then normalise to strings.
        df = reader.option("inferSchema", True).csv(path)
        df = df.select([F.col(c).cast("string").alias(c) for c in config.RAW_COLUMNS])
    return df


def quality_rules() -> Dict[str, Column]:
    """Each rule is a condition that is TRUE when the row is bad."""
    return {
        "missing_event_id": F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""),
        "missing_user_id": F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""),
        "invalid_event_type": F.col("event_type").isNull()
        | ~F.col("event_type").isin(config.ALLOWED_EVENT_TYPES),
        "invalid_timestamp": F.col("event_ts").isNull(),
        "future_timestamp": F.col("event_ts") > F.current_timestamp(),
        "invalid_price": F.col("price_num").isNull() | (F.col("price_num") < 0),
        "invalid_quantity": F.col("quantity_num").isNull() | (F.col("quantity_num") < 1),
    }


def apply_quality_rules(raw: DataFrame) -> DataFrame:
    """Adds typed columns and a `failed_rules` array (empty array = valid row)."""
    df = (
        raw.withColumn("event_ts", F.to_timestamp(F.col("event_timestamp"), TIMESTAMP_FORMAT))
        .withColumn("price_num", F.col("price").cast("double"))
        .withColumn("quantity_num", F.col("quantity").cast("int"))
    )
    flags = [
        F.when(F.coalesce(condition, F.lit(False)), F.lit(name))
        for name, condition in quality_rules().items()
    ]
    return df.withColumn("failed_rules", F.filter(F.array(*flags), lambda x: x.isNotNull()))


def to_validated_schema(checked: DataFrame) -> DataFrame:
    return checked.filter(F.size("failed_rules") == 0).select(
        "event_id",
        F.col("event_ts").alias("event_timestamp"),
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "category",
        F.col("price_num").alias("price"),
        F.col("quantity_num").alias("quantity"),
        "device",
        "country",
        F.to_date("event_ts").alias("event_date"),
    )


def compute_metrics(checked: DataFrame) -> Dict[str, int]:
    """One aggregation pass that returns total rows, valid rows and per-rule violation counts."""
    is_valid = F.size("failed_rules") == 0
    aggs = [
        F.count(F.lit(1)).alias("total_records"),
        F.sum(F.when(is_valid, 1).otherwise(0)).alias("valid_records"),
        F.countDistinct(F.when(is_valid, F.col("event_id"))).alias("distinct_valid_ids"),
    ]
    aggs += [
        F.sum(F.when(F.array_contains("failed_rules", name), 1).otherwise(0)).alias(name)
        for name in quality_rules()
    ]
    row = checked.agg(*aggs).collect()[0].asDict()
    return {key: int(value or 0) for key, value in row.items()}


def run_validation(
    spark: SparkSession,
    run_date: str,
    paths: config.Paths,
    optimized: bool = True,
    record_metrics: bool = True,
) -> Dict[str, int]:
    raw = read_raw(spark, paths.raw_partition(run_date), optimized)
    checked = apply_quality_rules(raw).withColumn("ingest_date", F.lit(run_date).cast("date"))
    if optimized:
        # Reused by 3 actions (metrics, valid write, quarantine write): read the CSV only once.
        checked = checked.cache()

    try:
        metrics = compute_metrics(checked)
        total = metrics["total_records"]
        if total == 0:
            raise DataQualityError(f"No raw records found at {paths.raw_partition(run_date)}")

        duplicates = metrics["valid_records"] - metrics["distinct_valid_ids"]
        violations = {name: metrics[name] for name in quality_rules()}
        violations["duplicate_event_id"] = duplicates
        invalid_rows = total - metrics["valid_records"]
        invalid_ratio = invalid_rows / total

        print(f"Total rows: {total:,} | invalid: {invalid_rows:,} ({invalid_ratio:.2%}) "
              f"| duplicates removed: {duplicates:,}")
        for name, count in violations.items():
            print(f"  {name:<22} {count:,}")

        if record_metrics:
            from pipeline.monitoring_db import record_quality_checks

            record_quality_checks(run_date, "raw_events", total, violations)

        if invalid_ratio > config.MAX_INVALID_RATIO:
            raise DataQualityError(
                f"{invalid_ratio:.2%} of rows are invalid, above the "
                f"{config.MAX_INVALID_RATIO:.0%} limit. Data was not promoted."
            )

        valid = to_validated_schema(checked).dropDuplicates(["event_id"])
        quarantine = checked.filter(F.size("failed_rules") > 0).select(
            *config.RAW_COLUMNS,
            F.concat_ws(",", "failed_rules").alias("failed_rules"),
            "ingest_date",
        )

        if optimized:
            valid.write.mode("overwrite").partitionBy("event_date").parquet(paths.validated)
            quarantine.write.mode("overwrite").partitionBy("ingest_date").parquet(paths.quarantine)
        else:
            valid.write.mode("overwrite").parquet(paths.validated)
            quarantine.write.mode("overwrite").parquet(paths.quarantine)

        valid_count = metrics["distinct_valid_ids"]
        if record_metrics:
            from pipeline.monitoring_db import update_freshness

            update_freshness("validated_events", paths.validated, valid_count)
            update_freshness("quarantine_events", paths.quarantine, invalid_rows)

        return {"total_records": total, "valid_records": valid_count, "invalid_records": invalid_rows}
    finally:
        if optimized:
            checked.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--skip-monitoring", action="store_true")
    args = parser.parse_args()

    spark = get_spark("validate_events")
    try:
        result = run_validation(
            spark, args.run_date, config.get_paths(), record_metrics=not args.skip_monitoring
        )
        print(f"Validation finished: {result}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
