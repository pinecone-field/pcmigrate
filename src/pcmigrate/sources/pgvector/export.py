"""PgvectorSource: the SourceConnector for pgvector, plus ground truth for validation.

Export attaches to the coordinator's snapshot, streams one PK range through a named
server-side cursor, and writes canonical records as it goes. Nothing holds a partition in
memory. The source is a plain dataclass with no open connections, so a process pool can
pickle it and each worker opens its own session.
"""

from __future__ import annotations

import random
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from pcmigrate.config.mapping import Mapping as MappingDoc
from pcmigrate.config.mapping import Metric, ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord, SparseVector
from pcmigrate.core.sparse import format_sparsevec
from pcmigrate.sources.base import Change, Partition, RecordSink, SourceSchema
from pcmigrate.sources.pgvector.introspect import IntrospectOptions, PgvectorIntrospector
from pcmigrate.sources.pgvector.partition import (
    SAMPLE_ROWS_PER_BOUNDARY,
    KeyInfo,
    build_partitions,
    integer_bounds,
    partition_count,
    range_predicate,
    sampled_bounds,
)
from pcmigrate.sources.pgvector.records import RecordBuilder
from pcmigrate.sources.pgvector.snapshot import attach_snapshot

FETCH_ROWS = 2000
DISTANCE_OPERATOR = {
    Metric.COSINE: "<=>",
    Metric.EUCLIDEAN: "<->",
    Metric.DOTPRODUCT: "<#>",
}


def split_qualified(name: str) -> tuple[str, str]:
    """``public.documents`` -> (public, documents)."""
    schema, _, table = name.partition(".")
    return (schema, table) if table else ("public", schema)


@dataclass(frozen=True, slots=True)
class TableMeta:
    """Catalog facts export needs per object."""

    primary_key: tuple[str, ...]
    key: KeyInfo
    dense_kind: str
    rows: int


@dataclass
class PgvectorSource:
    """SourceConnector for PostgreSQL with pgvector."""

    dsn: str
    mapping: MappingDoc
    snapshot_id: str | None = None
    target_rows_per_partition: int = 2_000_000
    _meta: dict[str, TableMeta] = field(default_factory=dict, repr=False)

    @property
    def kind(self) -> str:
        """Always ``pgvector``."""
        return "pgvector"

    def connect(self) -> psycopg.Connection[dict[str, Any]]:
        """A read-only autocommit session."""
        return psycopg.connect(
            self.dsn,
            autocommit=True,
            application_name="pcmigrate-export",
            options="-c default_transaction_read_only=on",
            row_factory=dict_row,
        )

    def object_for(self, name: str) -> ObjectMapping:
        """The mapping entry for ``schema.table``."""
        for obj in self.mapping.objects:
            if obj.source == name:
                return obj
        raise KeyError(f"{name} is not in the mapping")

    def object_for_namespace(self, namespace: str) -> ObjectMapping:
        """The mapping entry whose namespace is *namespace* (or its namespace_by prefix)."""
        base = namespace.split(":", 1)[0]
        for obj in self.mapping.objects:
            if obj.namespace == base:
                return obj
        raise KeyError(f"no object targets namespace {namespace!r}")

    def table_meta(self, conn: psycopg.Connection[dict[str, Any]], name: str) -> TableMeta:
        """Primary key, key type, vector kind, and row estimate, cached per object."""
        if name in self._meta:
            return self._meta[name]
        schema, table = split_qualified(name)
        obj = self.object_for(name)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, t.typname AS base
                FROM pg_index i
                JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY k.ord
                """,
                [f"{schema}.{table}"],
            )
            pk_rows = cur.fetchall()
            if not pk_rows:
                raise RuntimeError(f"{name} has no primary key")
            cur.execute(
                """
                SELECT t.typname AS base, GREATEST(c.reltuples, 0)::bigint AS rows
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE a.attrelid = %s::regclass AND a.attname = %s
                """,
                [f"{schema}.{table}", obj.dense.column if obj.dense else ""],
            )
            dense_row = cur.fetchone()
            if dense_row is None:
                raise RuntimeError(f"{name} has no column {obj.dense.column if obj.dense else ''}")
        meta = TableMeta(
            primary_key=tuple(str(r["name"]) for r in pk_rows),
            key=KeyInfo(column=str(pk_rows[0]["name"]), type_name=str(pk_rows[0]["type"])),
            dense_kind=str(dense_row["base"]),
            rows=int(dense_row["rows"]),
        )
        self._meta[name] = meta
        return meta

    def introspect(self) -> SourceSchema:
        """Structural schema via the introspector, with no workload window."""
        introspector = PgvectorIntrospector(self.dsn, IntrospectOptions(workload_window_s=0))
        try:
            return introspector.introspect()
        finally:
            introspector.close()

    def plan_partitions(self, mapping: MappingDoc) -> list[Partition]:
        """PK ranges per object, about ``target_rows_per_partition`` rows each."""
        plan: list[Partition] = []
        with self.connect() as conn:
            for obj in mapping.objects:
                if obj.dense is None:
                    continue
                meta = self.table_meta(conn, obj.source)
                schema, table = split_qualified(obj.source)
                ident = sql.Identifier(schema, table)
                key = sql.Identifier(meta.key.column)
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(ident))
                    row = cur.fetchone()
                    rows = int(row["n"]) if row else meta.rows
                    count = partition_count(rows, self.target_rows_per_partition)
                    if count == 1:
                        bounds: list[tuple[Any, Any]] = [(None, None)]
                    elif meta.key.is_integer:
                        cur.execute(
                            sql.SQL("SELECT min({k}) AS lo, max({k}) AS hi FROM {t}").format(
                                k=key, t=ident
                            )
                        )
                        mm = cur.fetchone()
                        assert mm is not None
                        bounds = list(integer_bounds(int(mm["lo"]), int(mm["hi"]), count))
                    else:
                        sample = min(rows, count * SAMPLE_ROWS_PER_BOUNDARY)
                        percent = min(100.0, max(0.01, 100.0 * sample * 2 / max(1, rows)))
                        cur.execute(
                            sql.SQL(
                                "SELECT {k}::text AS v FROM {t} TABLESAMPLE BERNOULLI ({p}) "
                                "ORDER BY {k} LIMIT {n}"
                            ).format(k=key, t=ident, p=sql.Literal(percent), n=sql.Literal(sample))
                        )
                        values = [r["v"] for r in cur.fetchall()]
                        bounds = sampled_bounds(values, count)
                plan.extend(build_partitions(obj.source, obj.namespace, bounds, rows))
        return plan

    def _select(
        self, builder: RecordBuilder, meta: TableMeta, ident: sql.Composable
    ) -> sql.Composable:
        columns: list[sql.Composable] = []
        for name in builder.source_columns:
            if name == builder.dense_column:
                cast = "::vector::real[]" if meta.dense_kind == "halfvec" else "::real[]"
                columns.append(
                    sql.SQL("{}{} AS {}").format(
                        sql.Identifier(name), sql.SQL(cast), sql.Identifier(name)
                    )
                )
            elif name == builder.sparse_column:
                columns.append(
                    sql.SQL("{}::text AS {}").format(sql.Identifier(name), sql.Identifier(name))
                )
            else:
                columns.append(sql.Identifier(name))
        return sql.SQL("SELECT {} FROM {}").format(sql.SQL(", ").join(columns), ident)

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """Stream one partition under the shared snapshot."""
        obj = self.object_for(partition.object)
        schema, table = split_qualified(partition.object)
        ident = sql.Identifier(schema, table)
        count = 0
        with psycopg.connect(
            self.dsn,
            autocommit=False,
            application_name=f"pcmigrate-export-{partition.id}",
            options="-c default_transaction_read_only=on",
            row_factory=dict_row,
        ) as conn:
            meta = self.table_meta(conn, partition.object)
            if self.snapshot_id:
                attach_snapshot(conn, self.snapshot_id)
            builder = RecordBuilder(obj, meta.primary_key)
            predicate, params = range_predicate(meta.key, partition.lower, partition.upper)
            query = sql.SQL("{} WHERE {} ORDER BY {}").format(
                self._select(builder, meta, ident), predicate, sql.Identifier(meta.key.column)
            )
            with conn.cursor(name=f"pcm_{partition.id}", withhold=False) as cur:
                cur.itersize = FETCH_ROWS
                cur.execute(query, params)
                for row in cur:
                    writer.write(builder.build(row))
                    count += 1
            conn.rollback()
        return count

    def change_feed(self) -> Iterator[Change]:
        """Phase 4."""
        raise NotImplementedError("pgvector change capture arrives in Phase 4")

    def _builder(
        self, conn: psycopg.Connection[dict[str, Any]], namespace: str
    ) -> tuple[RecordBuilder, TableMeta, sql.Composable]:
        obj = self.object_for_namespace(namespace)
        meta = self.table_meta(conn, obj.source)
        schema, table = split_qualified(obj.source)
        return RecordBuilder(obj, meta.primary_key), meta, sql.Identifier(schema, table)

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """Random ids rendered through id_template."""
        with self.connect() as conn:
            builder, meta, ident = self._builder(conn, namespace)
            pk = sql.SQL(", ").join(sql.Identifier(c) for c in meta.primary_key)
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(ident))
                row = cur.fetchone()
                total = int(row["n"]) if row else 0
                percent = min(100.0, max(0.01, 100.0 * count * 3 / max(1, total)))
                cur.execute(
                    sql.SQL("SELECT {} FROM {} TABLESAMPLE BERNOULLI ({}) LIMIT {}").format(
                        pk, ident, sql.Literal(percent), sql.Literal(count * 3)
                    )
                )
                rows = cur.fetchall()
                if len(rows) < count:
                    cur.execute(
                        sql.SQL("SELECT {} FROM {} LIMIT {}").format(pk, ident, sql.Literal(count))
                    )
                    rows = cur.fetchall()
        random.Random(seed).shuffle(rows)
        return [builder.build_id(r) for r in rows[:count]]

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """Rebuild records for *ids* by matching the rendered id."""
        with self.connect() as conn:
            builder, meta, ident = self._builder(conn, namespace)
            id_expr = builder.id_sql(meta.primary_key)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("{} WHERE {} = ANY(%s)").format(
                        self._select(builder, meta, ident), id_expr
                    ),
                    [list(ids)],
                )
                return {rec.id: rec for rec in (builder.build(r) for r in cur.fetchall())}

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact nearest neighbours with index scans off, by the mapped metric."""
        with self.connect() as conn:
            builder, meta, ident = self._builder(conn, namespace)
            obj = builder.mapping
            assert obj.dense is not None
            operator = sql.SQL(DISTANCE_OPERATOR[obj.dense.metric])
            dense = sql.Identifier(obj.dense.column)
            literal = "[" + ",".join(f"{v:.9g}" for v in vector) + "]"
            clauses: list[sql.Composable] = []
            params: list[Any] = [literal]
            for key, cond in (filter or {}).items():
                column = self._source_column(obj, key)
                if isinstance(cond, dict) and "$eq" in cond:
                    clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                    params.append(cond["$eq"])
            where = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
            cast = sql.SQL("::halfvec") if meta.dense_kind == "halfvec" else sql.SQL("::vector")
            query = sql.SQL(
                "SELECT {id_expr} AS rid FROM {t} WHERE {where} ORDER BY {dense} {op} %s{cast} LIMIT {k}"
            ).format(
                id_expr=builder.id_sql(meta.primary_key),
                t=ident,
                where=where,
                dense=dense,
                op=operator,
                cast=cast,
                k=sql.Literal(k),
            )
            with conn.cursor() as cur:
                cur.execute("SET enable_indexscan = off")
                cur.execute("SET enable_bitmapscan = off")
                cur.execute("SET enable_indexonlyscan = off")
                cur.execute(query, [*params[1:], params[0]])
                return [str(r["rid"]) for r in cur.fetchall()]

    @staticmethod
    def _source_column(obj: ObjectMapping, target: str) -> str:
        for spec in obj.metadata:
            if spec.action == "map" and (spec.target or spec.source) == target:
                return spec.source.split(".", 1)[0]
        return target

    def knn_sparse(
        self,
        namespace: str,
        sparse: SparseVector,
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact sparse inner-product neighbours (``<#>`` on sparsevec), index scans off."""
        with self.connect() as conn:
            builder, meta, ident = self._builder(conn, namespace)
            obj = builder.mapping
            if obj.sparse is None or obj.sparse.column is None:
                raise RuntimeError(f"{obj.source} stores no sparse column to search")
            sparse_column = obj.sparse.column
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT atttypmod FROM pg_attribute WHERE attrelid = %s::regclass AND attname = %s"
                    ),
                    [
                        f"{split_qualified(obj.source)[0]}.{split_qualified(obj.source)[1]}",
                        sparse_column,
                    ],
                )
                row = cur.fetchone()
                dimension = (
                    int(row["atttypmod"])
                    if row and int(row["atttypmod"]) > 0
                    else (obj.sparse.dimension or 0)
                )
            literal = format_sparsevec(sparse, dimension)
            clauses: list[sql.Composable] = []
            params: list[Any] = []
            for key, cond in (filter or {}).items():
                column = self._source_column(obj, key)
                if isinstance(cond, dict) and "$eq" in cond:
                    clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                    params.append(cond["$eq"])
            where = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
            query = sql.SQL(
                "SELECT {id_expr} AS rid FROM {t} WHERE {where} ORDER BY {col} <#> %s::sparsevec LIMIT {k}"
            ).format(
                id_expr=builder.id_sql(meta.primary_key),
                t=ident,
                where=where,
                col=sql.Identifier(sparse_column),
                k=sql.Literal(k),
            )
            with conn.cursor() as cur:
                cur.execute("SET enable_indexscan = off")
                cur.execute("SET enable_bitmapscan = off")
                cur.execute("SET enable_indexonlyscan = off")
                cur.execute(query, [*params, literal])
                return [str(r["rid"]) for r in cur.fetchall()]

    def all_ids(self, namespace: str) -> Iterator[str]:
        """Every rendered id, streamed."""
        with self.connect() as conn:
            builder, meta, ident = self._builder(conn, namespace)
            with conn.transaction(), conn.cursor(name="pcm_ids") as cur:
                cur.itersize = 10_000
                cur.execute(
                    sql.SQL("SELECT {} AS rid FROM {} ORDER BY 1").format(
                        builder.id_sql(meta.primary_key), ident
                    )
                )
                for row in cur:
                    yield str(row["rid"])

    def common_filters(self, namespace: str, count: int) -> list[dict[str, Any]]:
        """Most frequent values of low-cardinality mapped string columns.

        Columns whose top value appears once (identifiers, titles) and values longer than
        200 characters (free text) are skipped, since nobody filters on them.
        """
        with self.connect() as conn:
            builder, _, ident = self._builder(conn, namespace)
            obj = builder.mapping
            candidates = [
                f
                for f in obj.metadata
                if f.action == "map"
                and f.type is not None
                and f.type.value == "string"
                and "." not in f.source
            ]
            ranked: list[tuple[int, str, list[Any]]] = []
            with conn.cursor() as cur:
                for spec in candidates:
                    cur.execute(
                        sql.SQL(
                            "SELECT {c} AS v, count(*) AS n FROM {t} WHERE {c} IS NOT NULL AND length({c}::text) <= 200 "
                            "GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
                        ).format(c=sql.Identifier(spec.source), t=ident)
                    )
                    rows = [r for r in cur.fetchall() if int(r["n"]) > 1]
                    if rows:
                        ranked.append(
                            (-int(rows[0]["n"]), spec.target or spec.source, [r["v"] for r in rows])
                        )
            ranked.sort(key=lambda item: (item[0], item[1]))
            filters: list[dict[str, Any]] = []
            for _, target, values in ranked:
                for value in values:
                    filters.append({target: {"$eq": value}})
                    if len(filters) >= count:
                        return filters
        return filters
