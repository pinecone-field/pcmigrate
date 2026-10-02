"""pgvector change capture tiers: none, watermark, trigger outbox, logical replication (ADR-008).

Every tier yields ChangeEnvelopes for the buffer. CDC envelopes carry only the key: the
applier re-reads the current row before upserting, so a burst of updates costs one read.
The trigger outbox is ported from pinecone-field/pgvector-migration with the PK column made
configurable; the logical tier decodes pgoutput through the SQL polling functions so no
streaming replication client is needed.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from pcmigrate.config.mapping import Mapping, ObjectMapping
from pcmigrate.core.buffer import ChangeEnvelope
from pcmigrate.sources.pgvector.pgoutput import PgoutputDecoder
from pcmigrate.sources.pgvector.records import render_id

CHANGELOG_TABLE = "pinecone_changelog"
PUBLICATION = "pcmigrate_pub"


def split_qualified(name: str) -> tuple[str, str]:
    """``public.documents`` -> (public, documents)."""
    schema, _, table = name.partition(".")
    return (schema, table) if table else ("public", schema)


@dataclass(frozen=True, slots=True)
class TableKey:
    """How to turn a source row key into a record id."""

    obj: ObjectMapping
    primary_key: tuple[str, ...]

    def record_id(self, values: dict[str, Any]) -> str:
        """Render id_template from key values."""
        return render_id(self.obj.id_template, values)


class ChangeFeed(Protocol):
    """One tier's view of the source's changes since the snapshot boundary."""

    @property
    def tier(self) -> str:
        """Which tier this is."""
        ...

    def install(self) -> dict[str, Any]:
        """Create what the tier needs before export starts (triggers, slot). Idempotent."""
        ...

    def boundary(self, conn: psycopg.Connection[Any]) -> dict[str, Any]:
        """Read the position at the snapshot, inside the coordinator's transaction."""
        ...

    def start(self, boundary: dict[str, Any]) -> None:
        """Begin forwarding changes after *boundary*."""
        ...

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        """Changes since the last poll, oldest first."""
        ...

    def uninstall(self) -> None:
        """Remove triggers, slots, publications. Called at cutover."""
        ...


def _connect(dsn: str) -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(
        dsn, autocommit=True, row_factory=dict_row, application_name="pcmigrate-cdc"
    )


def primary_keys(dsn: str, mapping: Mapping) -> dict[str, TableKey]:
    """Primary key columns per mapped object."""
    out: dict[str, TableKey] = {}
    with _connect(dsn) as conn:
        for obj in mapping.objects:
            schema, table = split_qualified(obj.source)
            rows = conn.execute(
                """
                SELECT a.attname AS name
                FROM pg_index i
                JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE i.indrelid = %s::regclass AND i.indisprimary ORDER BY k.ord
                """,
                [f"{schema}.{table}"],
            ).fetchall()
            out[obj.source] = TableKey(obj, tuple(str(r["name"]) for r in rows))
    return out


@dataclass
class WatermarkFeed:
    """Tier 1: poll ``updated_at > watermark``. No DDL. Deletes come from reconcile."""

    dsn: str
    mapping: Mapping
    column: str = "updated_at"
    keys: dict[str, TableKey] = field(default_factory=dict)
    watermarks: dict[str, str] = field(default_factory=dict)

    @property
    def tier(self) -> str:
        """Watermark."""
        return "watermark"

    def install(self) -> dict[str, Any]:
        """Nothing to create."""
        self.keys = primary_keys(self.dsn, self.mapping)
        return {}

    def boundary(self, conn: psycopg.Connection[Any]) -> dict[str, Any]:
        """max(updated_at) per table inside the snapshot."""
        marks: dict[str, str] = {}
        with conn.cursor() as cur:
            for obj in self.mapping.objects:
                schema, table = split_qualified(obj.source)
                cur.execute(
                    sql.SQL("SELECT max({})::text AS m FROM {}").format(
                        sql.Identifier(self.column), sql.Identifier(schema, table)
                    )
                )
                row = cur.fetchone()
                value = row[0] if isinstance(row, tuple) else (row["m"] if row else None)
                marks[obj.source] = str(value) if value is not None else "epoch"
        return {"watermarks": marks}

    def start(self, boundary: dict[str, Any]) -> None:
        """Remember the boundary."""
        self.watermarks = dict(boundary.get("watermarks", {}))
        if not self.keys:
            self.keys = primary_keys(self.dsn, self.mapping)

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        """Rows changed since the watermark, advancing it."""
        out: list[ChangeEnvelope] = []
        with _connect(self.dsn) as conn:
            for obj in self.mapping.objects:
                key = self.keys[obj.source]
                schema, table = split_qualified(obj.source)
                mark = self.watermarks.get(obj.source, "epoch")
                cols = sql.SQL(", ").join(sql.Identifier(c) for c in key.primary_key)
                rows = conn.execute(
                    sql.SQL(
                        "SELECT {cols}, {wm}::text AS wm FROM {t} WHERE {wm} > %s::timestamptz "
                        "ORDER BY {wm} LIMIT {lim}"
                    ).format(
                        cols=cols,
                        wm=sql.Identifier(self.column),
                        t=sql.Identifier(schema, table),
                        lim=sql.Literal(limit),
                    ),
                    [mark],
                ).fetchall()
                for row in rows:
                    pk_values = {c: row[c] for c in key.primary_key}
                    out.append(
                        ChangeEnvelope(
                            op="upsert",
                            id=key.record_id(pk_values),
                            namespace=obj.namespace,
                            source_change_id=f"wm:{obj.source}:{row['wm']}:{key.record_id(pk_values)}",
                            source_pk=":".join(str(v) for v in pk_values.values()),
                        )
                    )
                    self.watermarks[obj.source] = str(row["wm"])
        return out

    def uninstall(self) -> None:
        """Nothing to remove."""


@dataclass
class TriggerOutboxFeed:
    """Tier 2: an outbox table plus AFTER triggers, ported from the field repo."""

    dsn: str
    mapping: Mapping
    keys: dict[str, TableKey] = field(default_factory=dict)
    last_seq: int = 0

    @property
    def tier(self) -> str:
        """trigger_outbox."""
        return "trigger_outbox"

    def install(self) -> dict[str, Any]:
        """Create the changelog table and one trigger per mapped table. Idempotent."""
        self.keys = primary_keys(self.dsn, self.mapping)
        with _connect(self.dsn) as conn:
            conn.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {} ("
                    "seq bigserial PRIMARY KEY, table_name text NOT NULL, row_id text NOT NULL, "
                    "op text NOT NULL CHECK (op IN ('upsert', 'delete')), "
                    "changed_at timestamptz NOT NULL DEFAULT now())"
                ).format(sql.Identifier(CHANGELOG_TABLE))
            )
            for obj in self.mapping.objects:
                key = self.keys[obj.source]
                schema, table = split_qualified(obj.source)
                fn = f"pcmigrate_log_{table}"
                row_id = sql.SQL(" || ':' || ").join(
                    sql.SQL("{}.{}::text").format(sql.SQL("{rec}"), sql.Identifier(c))
                    for c in key.primary_key
                )
                body = sql.SQL(
                    "CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger AS $fn$ BEGIN "
                    "IF (TG_OP = 'DELETE') THEN "
                    "INSERT INTO {log}(table_name, row_id, op) VALUES (TG_TABLE_NAME, {old_id}, 'delete'); "
                    "RETURN OLD; ELSE "
                    "INSERT INTO {log}(table_name, row_id, op) VALUES (TG_TABLE_NAME, {new_id}, 'upsert'); "
                    "RETURN NEW; END IF; END; $fn$ LANGUAGE plpgsql"
                ).format(
                    fn=sql.Identifier(schema, fn),
                    log=sql.Identifier(CHANGELOG_TABLE),
                    old_id=sql.SQL(row_id.as_string(conn).replace("{rec}", "OLD")),
                    new_id=sql.SQL(row_id.as_string(conn).replace("{rec}", "NEW")),
                )
                conn.execute(body)
                conn.execute(
                    sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                        sql.Identifier(f"trg_pcmigrate_{table}"), sql.Identifier(schema, table)
                    )
                )
                conn.execute(
                    sql.SQL(
                        "CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE ON {} "
                        "FOR EACH ROW EXECUTE FUNCTION {}()"
                    ).format(
                        sql.Identifier(f"trg_pcmigrate_{table}"),
                        sql.Identifier(schema, table),
                        sql.Identifier(schema, fn),
                    )
                )
        return {"changelog_table": CHANGELOG_TABLE}

    def boundary(self, conn: psycopg.Connection[Any]) -> dict[str, Any]:
        """max(seq) inside the snapshot: rows with a greater seq happened after it."""
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COALESCE(max(seq), 0) AS s FROM {}").format(
                    sql.Identifier(CHANGELOG_TABLE)
                )
            )
            row = cur.fetchone()
            value = row[0] if isinstance(row, tuple) else (row["s"] if row else 0)
        return {"outbox_seq": int(value)}

    def start(self, boundary: dict[str, Any]) -> None:
        """Forward rows after the boundary."""
        self.last_seq = int(boundary.get("outbox_seq", 0))
        if not self.keys:
            self.keys = primary_keys(self.dsn, self.mapping)

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        """Changelog rows after last_seq, oldest first."""
        by_table = {split_qualified(o.source)[1]: o for o in self.mapping.objects}
        out: list[ChangeEnvelope] = []
        with _connect(self.dsn) as conn:
            rows = conn.execute(
                sql.SQL(
                    "SELECT seq, table_name, row_id, op FROM {} WHERE seq > %s ORDER BY seq LIMIT {}"
                ).format(sql.Identifier(CHANGELOG_TABLE), sql.Literal(limit)),
                [self.last_seq],
            ).fetchall()
        for row in rows:
            obj = by_table.get(str(row["table_name"]))
            self.last_seq = int(row["seq"])
            if obj is None:
                continue
            key = self.keys[obj.source]
            parts = str(row["row_id"]).split(":")
            pk_values = dict(zip(key.primary_key, parts, strict=False))
            out.append(
                ChangeEnvelope(
                    op="upsert" if row["op"] == "upsert" else "delete",
                    id=key.record_id(pk_values),
                    namespace=obj.namespace,
                    source_change_id=f"ob:{row['seq']}",
                    source_pk=str(row["row_id"]),
                    sequence=int(row["seq"]),
                )
            )
        return out

    def uninstall(self) -> None:
        """Drop triggers and functions; keep the changelog table for audit."""
        with _connect(self.dsn) as conn:
            for obj in self.mapping.objects:
                schema, table = split_qualified(obj.source)
                conn.execute(
                    sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                        sql.Identifier(f"trg_pcmigrate_{table}"), sql.Identifier(schema, table)
                    )
                )
                conn.execute(
                    sql.SQL("DROP FUNCTION IF EXISTS {}()").format(
                        sql.Identifier(schema, f"pcmigrate_log_{table}")
                    )
                )


@dataclass
class LogicalFeed:
    """Tier 3: a pgoutput slot created at the snapshot, polled through SQL."""

    dsn: str
    mapping: Mapping
    slot: str = "pcmigrate_slot"
    publication: str = PUBLICATION
    keys: dict[str, TableKey] = field(default_factory=dict)
    decoder: PgoutputDecoder = field(default_factory=PgoutputDecoder)
    started: bool = False

    @property
    def tier(self) -> str:
        """Logical."""
        return "logical"

    def install(self) -> dict[str, Any]:
        """Create the publication for the mapped tables. The slot is created with the snapshot."""
        self.keys = primary_keys(self.dsn, self.mapping)
        tables = sql.SQL(", ").join(
            sql.Identifier(*split_qualified(o.source)) for o in self.mapping.objects
        )
        with _connect(self.dsn) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s", [self.publication]
            ).fetchone()
            if exists:
                conn.execute(
                    sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(
                        sql.Identifier(self.publication), tables
                    )
                )
            else:
                conn.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                        sql.Identifier(self.publication), tables
                    )
                )
        return {"publication": self.publication, "slot": self.slot}

    def create_slot_with_snapshot(self) -> tuple[psycopg.Connection[Any], str, str]:
        """CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT on a replication connection.

        Returns the connection (keep it open and idle while export runs), the exported
        snapshot name, and the consistent point LSN. Export and CDC then share one boundary.
        """
        conn = psycopg.connect(
            self.dsn,
            autocommit=True,
            cursor_factory=psycopg.ClientCursor,
            application_name="pcmigrate-slot",
            replication="database",
        )
        with conn.cursor() as cur:
            cur.execute(f'CREATE_REPLICATION_SLOT "{self.slot}" LOGICAL pgoutput EXPORT_SNAPSHOT')
            row = cur.fetchone()
        assert row is not None
        _, consistent_point, snapshot_name = str(row[0]), str(row[1]), str(row[2])
        return conn, snapshot_name, consistent_point

    def boundary(self, conn: psycopg.Connection[Any]) -> dict[str, Any]:
        """The slot's consistent point is the boundary; nothing to read here."""
        return {}

    def start(self, boundary: dict[str, Any]) -> None:
        """Nothing to do: the slot only holds changes after its consistent point."""
        if not self.keys:
            self.keys = primary_keys(self.dsn, self.mapping)
        self.started = True

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        """Consume and decode pgoutput messages from the slot."""
        out: list[ChangeEnvelope] = []
        by_table = {split_qualified(o.source): o for o in self.mapping.objects}
        with _connect(self.dsn) as conn:
            rows = conn.execute(
                "SELECT lsn::text AS lsn, xid, data FROM pg_logical_slot_get_binary_changes(%s, NULL, %s, "
                "'proto_version', '1', 'publication_names', %s)",
                [self.slot, limit * 4, self.publication],
            ).fetchall()
        for row in rows:
            change = self.decoder.decode(str(row["lsn"]), bytes(row["data"]))
            if change is None:
                continue
            obj = by_table.get((change.relation.namespace, change.relation.name))
            if obj is None:
                continue
            key = self.keys[obj.source]
            values = change.new if change.op != "delete" else change.key
            assert values is not None
            pk_values = {c: values.get(c) for c in key.primary_key}
            out.append(
                ChangeEnvelope(
                    op="delete" if change.op == "delete" else "upsert",
                    id=key.record_id(pk_values),
                    namespace=obj.namespace,
                    source_change_id=f"lsn:{change.lsn}:{row['xid']}:{key.record_id(pk_values)}",
                    source_pk=":".join(str(v) for v in pk_values.values()),
                )
            )
        return out

    def uninstall(self) -> None:
        """Drop the slot and the publication."""
        with _connect(self.dsn) as conn:
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                [self.slot],
            )
            conn.execute(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(self.publication))
            )


def make_feed(dsn: str, mapping: Mapping) -> ChangeFeed | None:
    """The feed for mapping.change_capture.tier, or None for tier none / dual_write only."""
    spec = mapping.change_capture
    if spec is None or spec.tier in ("none", "dual_write"):
        return None
    if spec.tier == "watermark":
        return WatermarkFeed(dsn, mapping, column=spec.watermark_column or "updated_at")
    if spec.tier == "trigger_outbox":
        return TriggerOutboxFeed(dsn, mapping)
    if spec.tier == "logical":
        return LogicalFeed(dsn, mapping, slot=f"pcm_{mapping.migration_id}".replace("-", "_")[:63])
    raise ValueError(f"unknown change capture tier {spec.tier!r}")


def iter_envelopes(
    feed: ChangeFeed, *, rounds: int, limit: int = 1000
) -> Iterator[Sequence[ChangeEnvelope]]:
    """Poll *rounds* times, yielding non-empty batches."""
    for _ in range(rounds):
        batch = feed.poll(limit)
        if batch:
            yield batch
