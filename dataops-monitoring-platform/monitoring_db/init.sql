-- Monitoring database schema.
-- Postgres runs this file automatically the FIRST time the monitoring-db
-- container starts (when its data volume is empty).

-- One row per Airflow task attempt.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    dag_id           TEXT        NOT NULL,
    run_id           TEXT        NOT NULL,
    task_id          TEXT        NOT NULL,
    try_number       INTEGER     NOT NULL DEFAULT 1,
    status           TEXT        NOT NULL,  -- running | success | failed | up_for_retry
    start_time       TIMESTAMPTZ,
    end_time         TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    error_message    TEXT,
    UNIQUE (dag_id, run_id, task_id, try_number)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON pipeline_runs (start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);

-- One row per (run date, dataset, quality rule). Re-runs overwrite the row.
CREATE TABLE IF NOT EXISTS quality_checks (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL,
    dataset         TEXT        NOT NULL,
    rule_name       TEXT        NOT NULL,
    violation_count BIGINT      NOT NULL,
    total_records   BIGINT      NOT NULL,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_date, dataset, rule_name)
);

-- Latest write of each dataset, used to measure data freshness.
CREATE TABLE IF NOT EXISTS dataset_freshness (
    dataset      TEXT PRIMARY KEY,
    location     TEXT        NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    record_count BIGINT      NOT NULL,
    sla_minutes  INTEGER     NOT NULL DEFAULT 1560  -- 26 hours for a daily pipeline
);

-- Results of pipeline.benchmark (baseline vs optimized runtime).
CREATE TABLE IF NOT EXISTS benchmark_runs (
    id                SERIAL PRIMARY KEY,
    run_date          DATE             NOT NULL,
    num_events        BIGINT           NOT NULL,
    baseline_seconds  DOUBLE PRECISION NOT NULL,
    optimized_seconds DOUBLE PRECISION NOT NULL,
    improvement_pct   DOUBLE PRECISION NOT NULL,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);
