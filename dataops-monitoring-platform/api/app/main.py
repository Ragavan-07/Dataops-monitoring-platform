"""DataOps monitoring API: data freshness, job runs, quality violations, benchmarks."""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import psycopg2
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.db import fetch_all

app = FastAPI(title="DataOps Monitoring API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(psycopg2.Error)
async def database_error_handler(request: Request, exc: psycopg2.Error) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": "Monitoring database is unavailable. Is the monitoring-db container running?"},
    )


def freshness_status(
    last_updated: datetime, sla_minutes: int, now: Optional[datetime] = None
) -> Dict[str, Any]:
    """fresh: within 75% of the SLA, warning: within the SLA, stale: past the SLA."""
    now = now or datetime.now(timezone.utc)
    minutes = max((now - last_updated).total_seconds() / 60, 0.0)
    if minutes > sla_minutes:
        status = "stale"
    elif minutes > 0.75 * sla_minutes:
        status = "warning"
    else:
        status = "fresh"
    return {"minutes_since_update": round(minutes, 1), "status": status}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/freshness")
def get_freshness() -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT dataset, location, last_updated, record_count, sla_minutes
        FROM dataset_freshness
        ORDER BY dataset
        """
    )
    now = datetime.now(timezone.utc)
    return [{**row, **freshness_status(row["last_updated"], row["sla_minutes"], now)} for row in rows]


@app.get("/api/jobs")
def get_jobs(
    status: Optional[Literal["running", "success", "failed", "up_for_retry"]] = None,
    limit: int = Query(50, ge=1, le=500),
) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, dag_id, run_id, task_id, try_number, status,
               start_time, end_time, duration_seconds, error_message
        FROM pipeline_runs
    """
    params: List[Any] = []
    if status:
        sql += " WHERE status = %s"
        params.append(status)
    sql += " ORDER BY start_time DESC NULLS LAST, id DESC LIMIT %s"
    params.append(limit)
    return fetch_all(sql, params)


@app.get("/api/quality")
def get_latest_quality() -> List[Dict[str, Any]]:
    """Violations per rule for the most recent run date."""
    return fetch_all(
        """
        SELECT run_date, dataset, rule_name, violation_count, total_records,
               ROUND(100.0 * violation_count / NULLIF(total_records, 0), 3)::float AS violation_pct,
               checked_at
        FROM quality_checks
        WHERE run_date = (SELECT MAX(run_date) FROM quality_checks)
        ORDER BY violation_count DESC, rule_name
        """
    )


@app.get("/api/quality/history")
def get_quality_history(days: int = Query(14, ge=1, le=365)) -> List[Dict[str, Any]]:
    """Total rule violations per run date."""
    return fetch_all(
        """
        SELECT run_date,
               SUM(violation_count)::bigint AS violations,
               MAX(total_records)::bigint   AS total_records
        FROM quality_checks
        WHERE run_date >= CURRENT_DATE - %s
        GROUP BY run_date
        ORDER BY run_date
        """,
        [days],
    )


@app.get("/api/benchmarks")
def get_benchmarks(limit: int = Query(10, ge=1, le=100)) -> List[Dict[str, Any]]:
    return fetch_all(
        """
        SELECT id, run_date, num_events, baseline_seconds, optimized_seconds,
               improvement_pct, created_at
        FROM benchmark_runs
        ORDER BY created_at DESC
        LIMIT %s
        """,
        [limit],
    )


@app.get("/api/summary")
def get_summary() -> Dict[str, Any]:
    jobs = fetch_all(
        """
        SELECT COUNT(*)                                     AS total,
               COUNT(*) FILTER (WHERE status = 'success')   AS succeeded,
               COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
               COUNT(*) FILTER (WHERE status = 'running')   AS running
        FROM pipeline_runs
        WHERE start_time >= now() - INTERVAL '24 hours'
        """
    )[0]
    quality = fetch_all(
        """
        SELECT run_date,
               SUM(violation_count)::bigint AS violations,
               MAX(total_records)::bigint   AS total_records
        FROM quality_checks
        WHERE run_date = (SELECT MAX(run_date) FROM quality_checks)
        GROUP BY run_date
        """
    )
    freshness = get_freshness()
    benchmark = fetch_all(
        """
        SELECT baseline_seconds, optimized_seconds, improvement_pct, num_events
        FROM benchmark_runs ORDER BY created_at DESC LIMIT 1
        """
    )
    return {
        "jobs_last_24h": jobs,
        "latest_quality": quality[0] if quality else None,
        "datasets_total": len(freshness),
        "datasets_stale": sum(1 for d in freshness if d["status"] == "stale"),
        "latest_benchmark": benchmark[0] if benchmark else None,
    }
