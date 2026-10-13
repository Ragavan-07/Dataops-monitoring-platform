"""Small helper for writing monitoring data into Postgres."""

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import psycopg2

from pipeline.config import FRESHNESS_SLA_MINUTES, MONITORING_DB_URL


@contextmanager
def get_conn() -> Iterator["psycopg2.extensions.connection"]:
    conn = psycopg2.connect(MONITORING_DB_URL, connect_timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_task_status(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    status: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None,
) -> None:
    duration = None
    if start_time is not None and end_time is not None:
        duration = (end_time - start_time).total_seconds()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (dag_id, run_id, task_id, try_number, status,
                 start_time, end_time, duration_seconds, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_id, run_id, task_id, try_number) DO UPDATE SET
                status = EXCLUDED.status,
                start_time = COALESCE(pipeline_runs.start_time, EXCLUDED.start_time),
                end_time = EXCLUDED.end_time,
                duration_seconds = EXCLUDED.duration_seconds,
                error_message = EXCLUDED.error_message
            """,
            (dag_id, run_id, task_id, try_number, status,
             start_time, end_time, duration, error_message),
        )


def record_quality_checks(
    run_date: str, dataset: str, total_records: int, violations: Dict[str, int]
) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        for rule_name, count in violations.items():
            cur.execute(
                """
                INSERT INTO quality_checks
                    (run_date, dataset, rule_name, violation_count, total_records, checked_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (run_date, dataset, rule_name) DO UPDATE SET
                    violation_count = EXCLUDED.violation_count,
                    total_records = EXCLUDED.total_records,
                    checked_at = EXCLUDED.checked_at
                """,
                (run_date, dataset, rule_name, int(count), int(total_records)),
            )


def update_freshness(
    dataset: str, location: str, record_count: int, sla_minutes: int = FRESHNESS_SLA_MINUTES
) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dataset_freshness (dataset, location, last_updated, record_count, sla_minutes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dataset) DO UPDATE SET
                location = EXCLUDED.location,
                last_updated = EXCLUDED.last_updated,
                record_count = EXCLUDED.record_count,
                sla_minutes = EXCLUDED.sla_minutes
            """,
            (dataset, location, datetime.now(timezone.utc), int(record_count), sla_minutes),
        )


def record_benchmark(
    run_date: str, num_events: int, baseline_seconds: float, optimized_seconds: float
) -> float:
    improvement = (baseline_seconds - optimized_seconds) / baseline_seconds * 100
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO benchmark_runs
                (run_date, num_events, baseline_seconds, optimized_seconds, improvement_pct)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_date, int(num_events), baseline_seconds, optimized_seconds, improvement),
        )
    return improvement
