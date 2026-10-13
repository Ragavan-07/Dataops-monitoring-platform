"""Read-only access to the monitoring database."""

import os
from typing import Any, Dict, List, Optional, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv(
    "MONITORING_DB_URL", "postgresql://monitor:monitor@localhost:5433/monitoring"
)


def fetch_all(sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
