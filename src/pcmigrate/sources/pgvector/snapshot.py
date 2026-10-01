"""One REPEATABLE READ snapshot shared by every export worker (ADR-008).

The coordinator opens the transaction, exports the snapshot, and records the WAL position
and per-table ``updated_at`` watermarks at that instant. Workers attach with
``SET TRANSACTION SNAPSHOT``. The transaction must stay open until every worker has finished;
a killed coordinator loses the snapshot, so a resumed run takes a new one and records it too.
Change capture starts from the first recorded LSN, and later snapshots only make re-exported
partitions newer, which idempotent upserts absorb.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql
from pydantic import BaseModel, ConfigDict, Field


class SnapshotInfo(BaseModel):
    """What the coordinator recorded."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str
    wal_lsn: str
    exported_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    watermarks: dict[str, str] = Field(default_factory=dict)
    backend_pid: int | None = None


class SnapshotCoordinator:
    """Holds the exporting transaction open for the life of the export stage."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection[Any] | None = None
        self.info: SnapshotInfo | None = None

    def open(self, watermark_columns: dict[str, str] | None = None) -> SnapshotInfo:
        """Begin REPEATABLE READ, export the snapshot, and read the boundary values.

        *watermark_columns* maps ``schema.table`` to its ``updated_at`` column.
        """
        if self._conn is not None:
            raise RuntimeError("snapshot already open")
        conn = psycopg.connect(
            self._dsn,
            autocommit=False,
            application_name="pcmigrate-snapshot",
            options="-c default_transaction_read_only=on -c idle_in_transaction_session_timeout=0",
        )
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_export_snapshot(), pg_current_wal_lsn()::text, pg_backend_pid()")
            row = cur.fetchone()
            assert row is not None
            snapshot_id, lsn, pid = str(row[0]), str(row[1]), int(row[2])
            watermarks: dict[str, str] = {}
            for table, column in (watermark_columns or {}).items():
                schema, _, name = table.partition(".")
                cur.execute(
                    sql.SQL("SELECT max({})::text FROM {}").format(
                        sql.Identifier(column), sql.Identifier(schema, name)
                    )
                )
                value = cur.fetchone()
                if value and value[0] is not None:
                    watermarks[table] = str(value[0])
        self._conn = conn
        self.info = SnapshotInfo(
            snapshot_id=snapshot_id, wal_lsn=lsn, watermarks=watermarks, backend_pid=pid
        )
        return self.info

    @property
    def is_open(self) -> bool:
        """True while the transaction is held."""
        return self._conn is not None and not self._conn.closed

    def close(self) -> None:
        """End the transaction and release the snapshot."""
        if self._conn is not None:
            try:
                self._conn.rollback()
            finally:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> SnapshotCoordinator:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def attach_snapshot(conn: psycopg.Connection[Any], snapshot_id: str) -> None:
    """Make *conn* read the coordinator's snapshot. Must be the transaction's first statement."""
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    conn.read_only = True
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot_id)))
