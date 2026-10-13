"""Daily e-commerce events pipeline: ingest -> validate -> transform.

Every task attempt is written to the monitoring DB (pipeline_runs table)
through callbacks, which is what the dashboard's "Jobs" view reads.
"""

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline.monitoring_db import record_task_status

log = logging.getLogger(__name__)


def _record(context, status: str, finished: bool) -> None:
    """Callbacks must never crash the task, so monitoring errors are only logged."""
    try:
        ti = context["task_instance"]
        exception = context.get("exception")
        record_task_status(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            try_number=ti.try_number,
            status=status,
            start_time=ti.start_date or datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc) if finished else None,
            error_message=str(exception)[:2000] if exception else None,
        )
    except Exception:  # noqa: BLE001
        log.exception("Could not write task status to the monitoring DB")


def on_execute(context):
    _record(context, "running", finished=False)


def on_success(context):
    _record(context, "success", finished=True)


def on_retry(context):
    _record(context, "up_for_retry", finished=True)


def on_failure(context):
    _record(context, "failed", finished=True)


default_args = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_execute_callback": on_execute,
    "on_success_callback": on_success,
    "on_retry_callback": on_retry,
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="ecommerce_events_pipeline",
    description="Ingest, validate and transform synthetic e-commerce events",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dataops", "spark"],
) as dag:
    generate_events = BashOperator(
        task_id="generate_events",
        bash_command="python -m pipeline.generate_events --date {{ ds }} --num-events ${NUM_EVENTS:-100000}",
        execution_timeout=timedelta(minutes=30),
    )

    validate_events = BashOperator(
        task_id="validate_events",
        bash_command="python -m pipeline.validate_events --run-date {{ ds }}",
        execution_timeout=timedelta(minutes=60),
    )

    transform_events = BashOperator(
        task_id="transform_events",
        bash_command="python -m pipeline.transform_events --run-date {{ ds }}",
        execution_timeout=timedelta(minutes=60),
    )

    generate_events >> validate_events >> transform_events
