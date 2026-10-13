"""pgvector discovery: vector/halfvec/sparsevec/tsvector columns, PK, metric, RLS, workload.

Read-only by construction: the session sets default_transaction_read_only=on, and every
statement passes through an audit that rejects anything but SELECT, WITH, SET, and SHOW.
Discovery queries are ported from pinecone-field/pgvector-migration (README, Step 2) and
extended to every pgvector type, partitioned tables, pg_index primary keys, and opclasses.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from pcmigrate.config.mapping import Mapping
from pcmigrate.sources.base import Change, Partition, RecordSink, SourceSchema
from pcmigrate.sources.pgvector.models import (
    ColumnInfo,
    DuplicateEstimate,
    IndexInfo,
    MetricGuess,
    PgInventory,
    RlsPolicy,
    TableInfo,
    TextSearchField,
    ThroughputSample,
    VectorField,
    VectorKind,
    WorkloadSample,
)
from pcmigrate.sources.pgvector.rls import classify_qual

READ_ONLY_VERBS = ("SELECT", "WITH", "SET", "SHOW", "EXPLAIN")
VECTOR_TYPES = {
    "vector": VectorKind.VECTOR,
    "halfvec": VectorKind.HALFVEC,
    "sparsevec": VectorKind.SPARSEVEC,
}
OPCLASS_METRIC = {
    "cosine": MetricGuess.COSINE,
    "ip": MetricGuess.DOTPRODUCT,
    "l2": MetricGuess.EUCLIDEAN,
}
_OPCLASS = re.compile(r"^(vector|halfvec|sparsevec|bit)_(cosine|ip|l2|l1|hamming|jaccard)_ops$")
EXACT_COUNT_MAX_ROWS = 5_000_000
BOOKKEEPING_TABLES = frozenset({"pinecone_changelog", "pinecone_ledger", "pinecone_sync_state"})
EXACT_DUPLICATES_MAX_ROWS = 100_000
SAMPLE_VALUES = 3


class ReadOnlyViolationError(RuntimeError):
    """Introspection tried to run something other than a read."""


def metric_from_opclass(opclass: str) -> MetricGuess:
    """vector_cosine_ops -> cosine; halfvec_ip_ops -> dotproduct; *_l2_ops -> euclidean."""
    match = _OPCLASS.match(opclass)
    if not match:
        return MetricGuess.UNKNOWN
    return OPCLASS_METRIC.get(match.group(2), MetricGuess.UNKNOWN)


@dataclass
class StatementAudit:
    """Every statement the introspector ran. Tests assert all of them are reads."""

    statements: list[str] = field(default_factory=list)

    def check(self, statement: str) -> None:
        """Record *statement*, raising if its first word is not a read-only verb."""
        head = statement.lstrip().split(None, 1)[0].upper() if statement.strip() else ""
        if head not in READ_ONLY_VERBS:
            raise ReadOnlyViolationError(f"refusing non-read statement: {statement[:80]!r}")
        self.statements.append(statement)


@dataclass(frozen=True, slots=True)
class IntrospectOptions:
    """Knobs for one introspection run."""

    schemas: tuple[str, ...] = ("public",)
    tables: tuple[str, ...] | None = None
    sample_rows: int = 1000
    workload_window_s: float = 300.0
    full_scan: bool = False
    throughput_rows: int = 2000


class PgvectorIntrospector:
    """Read-only discovery against one PostgreSQL database.

    Also satisfies the SourceConnector protocol's introspect(); the other methods arrive with
    Phase 3 and raise until then.
    """

    def __init__(
        self,
        dsn: str,
        options: IntrospectOptions | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._dsn = dsn
        self._opts = options or IntrospectOptions()
        self._sleep = sleep
        self._clock = clock
        self.audit = StatementAudit()
        self._conn: psycopg.Connection[tuple[Any, ...]] | None = None

    @property
    def kind(self) -> str:
        """Always ``pgvector``."""
        return "pgvector"

    def connect(self) -> psycopg.Connection[tuple[Any, ...]]:
        """Open the read-only session."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                self._dsn,
                autocommit=True,
                application_name="pcmigrate-introspect",
                options="-c default_transaction_read_only=on",
                row_factory=tuple_row,
            )
            self._run("SET default_transaction_read_only = on")
            self._run("SET statement_timeout = '10min'")
        return self._conn

    def close(self) -> None:
        """Close the session."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def _run(
        self, statement: str | sql.Composed, params: Sequence[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        conn = self._conn if self._conn is not None else self.connect()
        text = statement if isinstance(statement, str) else statement.as_string(conn)
        self.audit.check(text)
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []

    def _one(
        self, statement: str | sql.Composed, params: Sequence[Any] | None = None
    ) -> tuple[Any, ...] | None:
        rows = self._run(statement, params)
        return rows[0] if rows else None

    def introspect(self) -> SourceSchema:
        """SourceConnector entry point: structural schema for plan.lock."""
        return self.inventory().to_source_schema()

    def plan_partitions(self, mapping: Mapping) -> list[Partition]:
        """Phase 3."""
        raise NotImplementedError("pgvector export arrives in Phase 3")

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """Phase 3."""
        raise NotImplementedError("pgvector export arrives in Phase 3")

    def change_feed(self) -> Iterator[Change]:
        """Phase 4."""
        raise NotImplementedError("pgvector change capture arrives in Phase 4")

    def inventory(self) -> PgInventory:
        """Run every discovery step and return the inventory."""
        self.connect()
        version = self._one("SHOW server_version")
        read_only = self._one("SHOW default_transaction_read_only")
        extensions = dict(
            self._run(
                "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'pg_stat_statements')"
            )
        )
        tables = [
            self._describe_table(oid, schema, name, relkind)
            for oid, schema, name, relkind in self._list_tables()
        ]
        before = self._workload_snapshot(tables, "pg_stat_statements" in extensions)
        if self._opts.workload_window_s > 0 and tables:
            self._sleep(self._opts.workload_window_s)
        after = self._workload_snapshot(tables, "pg_stat_statements" in extensions)
        for table in tables:
            table.workload = self._workload_delta(
                table, before, after, "pg_stat_statements" in extensions
            )
        default_config = self._one("SHOW default_text_search_config")
        return PgInventory(
            server_version=str(version[0]) if version else "",
            pgvector_version=extensions.get("vector"),
            pg_stat_statements="pg_stat_statements" in extensions,
            sampled_at=datetime.now(UTC),
            read_only_session=bool(read_only and str(read_only[0]) == "on"),
            tables=tables,
            cluster={
                "default_text_search_config": str(default_config[0]) if default_config else None
            },
        )

    def _list_tables(self) -> list[tuple[int, str, str, str]]:
        rows = self._run(
            """
            SELECT c.oid, n.nspname, c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname = ANY(%s)
              AND NOT EXISTS (
                SELECT 1 FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
                WHERE i.inhrelid = c.oid AND p.relkind = 'p')
            ORDER BY n.nspname, c.relname
            """,
            [list(self._opts.schemas)],
        )
        wanted = self._opts.tables
        return [
            (int(oid), str(schema), str(name), str(relkind))
            for oid, schema, name, relkind in rows
            if str(name) not in BOOKKEEPING_TABLES
            and (wanted is None or name in wanted or f"{schema}.{name}" in wanted)
        ]

    def _describe_table(self, oid: int, schema: str, name: str, relkind: str) -> TableInfo:
        table = TableInfo(schema_name=schema, name=name, relkind=relkind)
        ident = sql.Identifier(schema, name)
        if relkind == "p":
            table.partitions = [
                str(r[0])
                for r in self._run(
                    "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = %s ORDER BY 1",
                    [oid],
                )
            ]
        stats = self._one(
            """
            SELECT c.reltuples::bigint, pg_total_relation_size(c.oid), c.relrowsecurity, c.relforcerowsecurity
            FROM pg_class c WHERE c.oid = %s
            """,
            [oid],
        )
        if stats:
            table.row_estimate = max(0, int(stats[0]))
            table.total_bytes = int(stats[1])
            table.rls_enabled = bool(stats[2])
            table.rls_forced = bool(stats[3])
        if relkind == "p":
            size = self._one(
                "SELECT COALESCE(sum(pg_total_relation_size(inhrelid)), 0), COALESCE(sum(reltuples), 0)::bigint "
                "FROM pg_inherits JOIN pg_class ON pg_class.oid = inhrelid WHERE inhparent = %s",
                [oid],
            )
            if size:
                table.total_bytes += int(size[0])
                table.row_estimate = max(table.row_estimate, int(size[1]))
        if self._opts.full_scan or table.row_estimate <= EXACT_COUNT_MAX_ROWS:
            count = self._one(sql.SQL("SELECT count(*) FROM {}").format(ident))
            table.exact_rows = int(count[0]) if count else None
        if (
            table.rls_enabled
            and table.exact_rows is not None
            and table.exact_rows < table.row_estimate
        ):
            table.rls_hides_rows = True

        table.primary_key = self._primary_key(oid)
        table.indexes = self._indexes(oid)
        pg_stats = {
            str(r[0]): r
            for r in self._run(
                "SELECT attname, null_frac, avg_width, n_distinct FROM pg_stats WHERE schemaname = %s AND tablename = %s",
                [schema, name],
            )
        }
        columns = self._run(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod), t.typname, a.atttypmod,
                   a.attnotnull, a.attnum, a.attgenerated <> ''
            FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
            WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            [oid],
        )
        plain: list[ColumnInfo] = []
        for attname, type_text, typname, typmod, notnull, attnum, generated in columns:
            if typname in VECTOR_TYPES:
                table.vectors.append(
                    VectorField(
                        column=str(attname),
                        kind=VECTOR_TYPES[str(typname)],
                        dimension=int(typmod) if int(typmod) > 0 else None,
                        nullable=not notnull,
                    )
                )
                continue
            if typname == "tsvector":
                table.text_search.append(self._text_search_field(oid, str(attname), table.indexes))
                continue
            stat = pg_stats.get(str(attname))
            plain.append(
                ColumnInfo(
                    name=str(attname),
                    type=str(type_text),
                    base_type=str(typname),
                    nullable=not notnull,
                    position=int(attnum),
                    null_frac=float(stat[1]) if stat and stat[1] is not None else None,
                    avg_width=int(stat[2]) if stat and stat[2] is not None else None,
                    n_distinct=float(stat[3]) if stat and stat[3] is not None else None,
                    is_generated=bool(generated),
                )
            )
        table.columns = plain
        for text in table.text_search:
            self._parse_text_search(text, table)
        table.text_search.extend(self._expression_text_indexes(table))
        self._sample_values(ident, table)
        for vector in table.vectors:
            self._infer_metric(vector, table.indexes)
            self._sample_vector(ident, table, vector)
        table.policies = self._policies(schema, name, [c.name for c in table.columns])
        table.throughput = self._throughput(ident, table)
        return table

    def _primary_key(self, oid: int) -> list[str]:
        rows = self._run(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
            WHERE i.indrelid = %s AND i.indisprimary
            ORDER BY k.ord
            """,
            [oid],
        )
        return [str(r[0]) for r in rows]

    def _indexes(self, oid: int) -> list[IndexInfo]:
        rows = self._run(
            """
            SELECT ic.relname, am.amname, i.indisprimary, i.indisunique, pg_get_indexdef(i.indexrelid),
                   ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                         LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         ORDER BY k.ord),
                   ARRAY(SELECT o.opcname::text FROM unnest(i.indclass) WITH ORDINALITY AS u(opcoid, ord)
                         JOIN pg_opclass o ON o.oid = u.opcoid ORDER BY u.ord)
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_am am ON am.oid = ic.relam
            WHERE i.indrelid = %s
            ORDER BY ic.relname
            """,
            [oid],
        )
        return [
            IndexInfo(
                name=str(r[0]),
                method=str(r[1]),
                is_primary=bool(r[2]),
                is_unique=bool(r[3]),
                definition=str(r[4]),
                columns=[str(c) for c in r[5] if c is not None],
                opclasses=[str(o) for o in r[6]],
            )
            for r in rows
        ]

    @staticmethod
    def _infer_metric(vector: VectorField, indexes: list[IndexInfo]) -> None:
        for index in indexes:
            if index.method not in ("hnsw", "ivfflat") or vector.column not in index.columns:
                continue
            position = index.columns.index(vector.column)
            opclass = index.opclasses[position] if position < len(index.opclasses) else ""
            vector.metric = metric_from_opclass(opclass)
            vector.metric_index = index.name
            vector.index_method = index.method
            return

    def _text_search_field(
        self, oid: int, column: str, indexes: list[IndexInfo]
    ) -> TextSearchField:
        expr = self._one(
            """
            SELECT pg_get_expr(d.adbin, d.adrelid)
            FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = %s AND a.attname = %s
            """,
            [oid, column],
        )
        gin = next((i.name for i in indexes if i.method == "gin" and column in i.columns), None)
        return TextSearchField(
            column=column, expression=str(expr[0]) if expr and expr[0] else None, gin_index=gin
        )

    def _parse_text_search(self, text: TextSearchField, table: TableInfo) -> None:
        """Fill the regconfig, source columns, and dictionaries of a tsvector field."""
        from pcmigrate.sources.pgvector.fts_map import parse_expression

        if text.expression is None:
            return
        parsed = parse_expression(text.expression, [c.name for c in table.columns])
        text.config = parsed.config
        text.source_columns = [*parsed.columns, *parsed.json_paths]
        if text.config is not None:
            text.dictionaries = self._config_dictionaries(text.config)

    def _config_dictionaries(self, config: str) -> list[str]:
        """``name:template`` of every dictionary a text search configuration maps to."""
        rows = self._run(
            """
            SELECT DISTINCT d.dictname, t.tmplname
            FROM pg_ts_config c
            JOIN pg_ts_config_map m ON m.mapcfg = c.oid
            JOIN pg_ts_dict d ON d.oid = m.mapdict
            JOIN pg_ts_template t ON t.oid = d.dicttemplate
            WHERE c.cfgname = %s
            ORDER BY 1
            """,
            [config],
        )
        return [f"{r[0]}:{r[1]}" for r in rows]

    def _expression_text_indexes(self, table: TableInfo) -> list[TextSearchField]:
        """GIN indexes over ``to_tsvector(...)`` expressions, with no tsvector column behind them."""
        from pcmigrate.sources.pgvector.fts_map import parse_expression

        out: list[TextSearchField] = []
        covered = {t.gin_index for t in table.text_search if t.gin_index}
        for index in table.indexes:
            if index.method != "gin" or index.name in covered:
                continue
            definition = index.definition
            marker = "USING gin ("
            start = definition.find(marker)
            if start < 0 or "to_tsvector(" not in definition.lower():
                continue
            expression = definition[start + len(marker) :].rstrip()
            if expression.endswith(")"):
                expression = expression[:-1]
            if " WHERE " in expression:
                expression = expression.split(" WHERE ", 1)[0].rstrip().removesuffix(")")
            parsed = parse_expression(expression, [c.name for c in table.columns])
            field = TextSearchField(
                column=index.name,
                kind="tsvector_index",
                expression=expression,
                gin_index=index.name,
                config=parsed.config,
                source_columns=[*parsed.columns, *parsed.json_paths],
            )
            if field.config is not None:
                field.dictionaries = self._config_dictionaries(field.config)
            out.append(field)
        return out

    def _sample_values(self, ident: sql.Composable, table: TableInfo) -> None:
        if not table.columns:
            return
        cols = sql.SQL(", ").join(sql.Identifier(c.name) for c in table.columns)
        rows = self._run(
            sql.SQL("SELECT {} FROM {} LIMIT {}").format(
                cols, ident, sql.Literal(SAMPLE_VALUES * 4)
            )
        )
        for position, column in enumerate(table.columns):
            values = [r[position] for r in rows if r[position] is not None][:SAMPLE_VALUES]
            column.sample_values = [_render_sample(v) for v in values]
            if column.base_type in ("json", "jsonb"):
                depths = [_json_depth(v) for v in values]
                column.sample_json_depth = max(depths) if depths else None

    def _sample_vector(self, ident: sql.Composable, table: TableInfo, vector: VectorField) -> None:
        col = sql.Identifier(vector.column)
        if vector.kind is VectorKind.SPARSEVEC:
            dims = sql.SQL("NULL::int")
        else:
            dims = sql.SQL("vector_dims(v::vector)")
        row = self._one(
            sql.SQL(
                "WITH s AS (SELECT {col} AS v FROM {tbl} LIMIT {lim}) "
                "SELECT count(*), count(*) FILTER (WHERE v IS NULL), "
                "ARRAY(SELECT DISTINCT {dims} FROM s WHERE v IS NOT NULL) FROM s"
            ).format(col=col, tbl=ident, lim=sql.Literal(self._opts.sample_rows), dims=dims)
        )
        if row:
            vector.sampled_rows = int(row[0])
            vector.null_rows = int(row[1])
            vector.distinct_dimensions = sorted(int(d) for d in row[2] if d is not None)
        table.duplicates[vector.column] = self._duplicates(ident, table, vector)

    def _duplicates(
        self, ident: sql.Composable, table: TableInfo, vector: VectorField
    ) -> DuplicateEstimate:
        col = sql.Identifier(vector.column)
        if self._opts.full_scan or table.rows <= EXACT_DUPLICATES_MAX_ROWS:
            row = self._one(
                sql.SQL(
                    "SELECT count(*), COALESCE(sum(n) - count(*), 0) FROM "
                    "(SELECT md5({col}::text) AS h, count(*) AS n FROM {tbl} WHERE {col} IS NOT NULL "
                    "GROUP BY 1 HAVING count(*) > 1) g"
                ).format(col=col, tbl=ident)
            )
            groups = int(row[0]) if row else 0
            extra = int(row[1]) if row else 0
            return DuplicateEstimate(
                method="exact",
                sampled_rows=table.rows,
                duplicate_rows_in_sample=extra,
                duplicate_groups=groups,
                estimated_duplicate_rows=extra,
            )
        percent = min(100.0, max(0.01, 100.0 * self._opts.sample_rows * 2 / max(1, table.rows)))
        row = self._one(
            sql.SQL(
                "WITH s AS (SELECT {col} AS v FROM {tbl} TABLESAMPLE BERNOULLI ({pct}) "
                "WHERE {col} IS NOT NULL LIMIT {lim}) "
                "SELECT count(*), count(*) - count(DISTINCT md5(v::text)) FROM s"
            ).format(
                col=col,
                tbl=ident,
                pct=sql.Literal(percent),
                lim=sql.Literal(self._opts.sample_rows),
            )
        )
        sampled = int(row[0]) if row else 0
        extra = int(row[1]) if row else 0
        scale = (table.rows / sampled) if sampled else 0.0
        return DuplicateEstimate(
            method="sampled",
            sampled_rows=sampled,
            duplicate_rows_in_sample=extra,
            estimated_duplicate_rows=round(extra * scale),
        )

    def _policies(self, schema: str, name: str, columns: list[str]) -> list[RlsPolicy]:
        rows = self._run(
            "SELECT policyname, cmd, roles, qual, with_check FROM pg_policies WHERE schemaname = %s AND tablename = %s ORDER BY policyname",
            [schema, name],
        )
        policies: list[RlsPolicy] = []
        for policyname, cmd, roles, qual, with_check in rows:
            classification, column, note = classify_qual(qual, columns)
            policies.append(
                RlsPolicy(
                    name=str(policyname),
                    cmd=str(cmd),
                    roles=[str(r) for r in roles],
                    qual=qual,
                    with_check=with_check,
                    classification=classification,
                    column=column,
                    note=note,
                )
            )
        return policies

    def _throughput(self, ident: sql.Composable, table: TableInfo) -> ThroughputSample | None:
        dense = table.dense
        if dense is None or table.rows == 0:
            return None
        limit = min(self._opts.throughput_rows, table.rows)
        started = self._clock()
        row = self._one(
            sql.SQL(
                "SELECT count(*), COALESCE(sum(pg_column_size({col})), 0) FROM (SELECT {col} FROM {tbl} LIMIT {lim}) s"
            ).format(col=sql.Identifier(dense.column), tbl=ident, lim=sql.Literal(limit))
        )
        elapsed = self._clock() - started
        return ThroughputSample(
            rows=int(row[0]) if row else 0, seconds=elapsed, bytes=int(row[1]) if row else 0
        )

    def _workload_snapshot(
        self, tables: list[TableInfo], statements: bool
    ) -> dict[str, tuple[float, ...]]:
        snapshot: dict[str, tuple[float, ...]] = {}
        rows = self._run(
            "SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del, seq_scan, idx_scan FROM pg_stat_user_tables"
        )
        counters = {f"{r[0]}.{r[1]}": tuple(float(x or 0) for x in r[2:]) for r in rows}
        for table in tables:
            parts = [table.qualified_name, *table.partitions]
            total = [0.0] * 5
            for part in parts:
                key = part if "." in part else f"{table.schema_name}.{part}"
                for i, v in enumerate(counters.get(key, (0.0,) * 5)):
                    total[i] += v
            calls = 0.0
            if statements:
                row = self._one(
                    "SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements "
                    "WHERE query ILIKE %s AND query ILIKE 'SELECT%%' AND query NOT ILIKE '%%pg_stat%%'",
                    [f"%{table.name}%"],
                )
                calls = float(row[0]) if row else 0.0
            snapshot[table.qualified_name] = (*total, calls, self._clock())
        return snapshot

    def _workload_delta(
        self,
        table: TableInfo,
        before: dict[str, tuple[float, ...]],
        after: dict[str, tuple[float, ...]],
        statements: bool,
    ) -> WorkloadSample:
        b = before.get(table.qualified_name)
        a = after.get(table.qualified_name)
        if not b or not a or self._opts.workload_window_s <= 0:
            return WorkloadSample(window_s=0.0, measured=False, statements_available=statements)
        window = max(0.0, a[6] - b[6])
        if window <= 0:
            return WorkloadSample(window_s=0.0, measured=False, statements_available=statements)
        per_min = [(a[i] - b[i]) / window * 60.0 for i in range(5)]
        read_qps = (a[5] - b[5]) / window if statements else None
        return WorkloadSample(
            window_s=window,
            measured=True,
            inserts_per_min=per_min[0],
            updates_per_min=per_min[1],
            deletes_per_min=per_min[2],
            seq_scans_per_min=per_min[3],
            idx_scans_per_min=per_min[4],
            read_qps=read_qps,
            statements_available=statements,
        )


def _render_sample(value: Any) -> str:
    if isinstance(value, dict | list):
        text = json.dumps(value, default=str, ensure_ascii=False)
    else:
        text = str(value)
    return text if len(text) <= 80 else text[:77] + "..."


def _json_depth(value: Any) -> int:
    if isinstance(value, dict):
        return 1 + max((_json_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_json_depth(v) for v in value), default=0)
    return 0
