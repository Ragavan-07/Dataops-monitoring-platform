"""Measure pipeline runtime before and after optimisation, on the same raw data.

baseline : inferSchema, no caching, 200 shuffle partitions, AQE off, unpartitioned output
optimized: explicit schema, caching, tuned shuffle partitions, AQE on, date-partitioned output

Results are written to benchmark_runs in the monitoring DB and shown on the dashboard.
Generate the raw data first (e.g. 5,000,000 events), then:

    python -m pipeline.benchmark --run-date 2026-10-06
"""

import argparse
import time

from pipeline import config
from pipeline.spark_utils import configure_tuning, get_spark
from pipeline.transform_events import run_transform
from pipeline.validate_events import RAW_SCHEMA, run_validation


def timed_run(spark, run_date: str, optimized: bool) -> tuple:
    label = "optimized" if optimized else "baseline"
    paths = config.get_paths(prefix=f"benchmark/{label}")
    configure_tuning(spark, optimized)
    spark.catalog.clearCache()

    start = time.perf_counter()
    result = run_validation(spark, run_date, paths, optimized=optimized, record_metrics=False)
    run_transform(spark, run_date, paths, optimized=optimized, record_metrics=False)
    seconds = time.perf_counter() - start
    print(f"[{label}] {seconds:.1f} s")
    return seconds, result["total_records"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", required=True, help="Date whose raw data to use, YYYY-MM-DD")
    parser.add_argument("--repeats", type=int, default=1, help="Runs per mode; the average is reported")
    parser.add_argument("--skip-monitoring", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1:
        raise ValueError("--repeats must be at least 1")

    spark = get_spark("pipeline_benchmark")
    try:
        # Warm-up: start the JVM and pull the files into the OS cache so the
        # first measured mode is not penalised.
        warm = spark.read.option("header", True).schema(RAW_SCHEMA).csv(
            config.get_paths().raw_partition(args.run_date)
        ).count()
        print(f"Warm-up read: {warm:,} rows")

        baseline_times, optimized_times = [], []
        num_events = warm
        for _ in range(args.repeats):
            seconds, num_events = timed_run(spark, args.run_date, optimized=False)
            baseline_times.append(seconds)
            seconds, num_events = timed_run(spark, args.run_date, optimized=True)
            optimized_times.append(seconds)

        baseline = sum(baseline_times) / len(baseline_times)
        optimized = sum(optimized_times) / len(optimized_times)
        improvement = (baseline - optimized) / baseline * 100
        print("-" * 50)
        print(f"Events:     {num_events:,}")
        print(f"Baseline:   {baseline:.1f} s ({baseline / 60:.1f} min)")
        print(f"Optimized:  {optimized:.1f} s ({optimized / 60:.1f} min)")
        print(f"Improvement: {improvement:.1f}%")

        if not args.skip_monitoring:
            from pipeline.monitoring_db import record_benchmark

            record_benchmark(args.run_date, num_events, baseline, optimized)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
