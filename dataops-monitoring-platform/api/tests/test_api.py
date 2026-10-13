from datetime import datetime, timedelta, timezone

import psycopg2
from fastapi.testclient import TestClient

import app.main as main

client = TestClient(main.app)
NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_freshness_status_levels():
    assert main.freshness_status(NOW - timedelta(minutes=10), 100, NOW)["status"] == "fresh"
    assert main.freshness_status(NOW - timedelta(minutes=80), 100, NOW)["status"] == "warning"
    stale = main.freshness_status(NOW - timedelta(minutes=101), 100, NOW)
    assert stale["status"] == "stale"
    assert stale["minutes_since_update"] == 101.0


def test_freshness_endpoint(monkeypatch):
    rows = [{
        "dataset": "validated_events",
        "location": "/opt/airflow/data/validated/events",
        "last_updated": datetime.now(timezone.utc) - timedelta(minutes=5),
        "record_count": 1000,
        "sla_minutes": 1560,
    }]
    monkeypatch.setattr(main, "fetch_all", lambda sql, params=None: rows)
    response = client.get("/api/freshness")
    assert response.status_code == 200
    body = response.json()
    assert body[0]["dataset"] == "validated_events"
    assert body[0]["status"] == "fresh"


def test_jobs_status_filter_is_passed_to_sql(monkeypatch):
    captured = {}

    def fake_fetch_all(sql, params=None):
        captured["sql"], captured["params"] = sql, params
        return []

    monkeypatch.setattr(main, "fetch_all", fake_fetch_all)
    response = client.get("/api/jobs?status=failed&limit=5")
    assert response.status_code == 200
    assert "WHERE status = %s" in captured["sql"]
    assert captured["params"] == ["failed", 5]


def test_jobs_rejects_unknown_status():
    assert client.get("/api/jobs?status=exploded").status_code == 422


def test_database_down_returns_503(monkeypatch):
    def broken(sql, params=None):
        raise psycopg2.OperationalError("connection refused")

    monkeypatch.setattr(main, "fetch_all", broken)
    response = client.get("/api/freshness")
    assert response.status_code == 503
