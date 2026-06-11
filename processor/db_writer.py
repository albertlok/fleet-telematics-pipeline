"""
Bulk writer for the relational database (PostgreSQL).

THE PROBLEM: telemetry arrives at thousands of events per minute. If we
ran one INSERT statement per event, every insert would pay a full network
round-trip to the database, and the indexes on the production table would
be updated thousands of times per minute. The database would fall over
exactly when traffic peaks.

THE FIX — two classic techniques used together:

1. BATCHED INSERTS: psycopg2's execute_values() sends hundreds of rows
   in a single SQL statement, cutting network round-trips by ~95%.

2. STAGING TABLE PATTERN: we first insert into staging_telemetry_events,
   a table with NO indexes (so inserts are as cheap as possible), then
   call a stored procedure that merges those rows into the real,
   indexed telemetry_events table in one set-based operation. The
   expensive index maintenance happens once per batch instead of once
   per row, and readers of the production table are barely disturbed.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

# Column order here MUST match the tuple order built in _to_staging_row()
# below, and the column names must exist in staging_telemetry_events
# (see sql/001_schema.sql).
_STAGING_COLUMNS = [
    "ingestion_id",
    "ingested_at_ms",
    "event_id",
    "event_type",
    "event_time_ms",
    "org_id",
    "vehicle_id",
    "driver_id",
    "raw_payload",
]


class DBWriter:
    def __init__(self, dsn: str):
        # DSN = "Data Source Name", the connection string:
        # postgresql://user:password@host:5432/dbname
        self._dsn = dsn
        self._conn: psycopg2.extensions.connection | None = None

    def _get_conn(self) -> psycopg2.extensions.connection:
        """
        Return a live connection, reconnecting if the old one dropped.
        Database connections die all the time in practice (failovers,
        idle timeouts, network blips) — always be ready to reconnect.
        """
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            # autocommit=False means nothing is permanent until we call
            # conn.commit() — so a batch either fully lands or fully
            # rolls back. No half-written batches.
            self._conn.autocommit = False
        return self._conn

    def write_batch(self, records: list[dict[str, Any]]) -> int:
        """
        Write a batch of transformed events: staging insert + merge,
        committed as one transaction. Returns the number of rows sent.
        """
        if not records:
            return 0

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                rows = [_to_staging_row(r) for r in records]
                # execute_values expands the rows into one multi-row
                # INSERT ... VALUES (...), (...), ... statement.
                psycopg2.extras.execute_values(
                    cur,
                    f"""
                    INSERT INTO staging_telemetry_events
                        ({", ".join(_STAGING_COLUMNS)})
                    VALUES %s
                    """,
                    rows,
                    page_size=500,  # rows per statement; bigger batches are split
                )
                # Move everything from staging into the indexed master
                # table. The procedure also skips event_ids that already
                # exist there (a second safety net behind the Redis dedup).
                cur.execute("CALL merge_staging_to_master()")
            conn.commit()
            log.info("Wrote %d events to staging → master", len(rows))
            return len(rows)
        except Exception:
            # Roll back so the transaction doesn't stay open holding locks.
            # Re-raise so the caller's retry logic can decide what to do.
            conn.rollback()
            raise

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()


def _to_staging_row(record: dict) -> tuple:
    """
    Convert a transformed record dict into a tuple matching
    _STAGING_COLUMNS. Event-type-specific fields (latitude, duty_status,
    severity, ...) aren't separate columns — they travel inside
    raw_payload, a JSONB column that analysts can query with Postgres's
    JSON operators (e.g. raw_payload->>'latitude').
    """
    return (
        record.get("ingestion_id"),
        record.get("ingested_at_ms"),
        record.get("event_id"),
        record.get("event_type"),
        record.get("event_time_ms"),
        record.get("org_id"),
        record.get("vehicle_id"),
        record.get("driver_id"),
        json.dumps(record),
    )
