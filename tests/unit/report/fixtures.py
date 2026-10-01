"""Hand-built inventories for findings and mapping tests."""

from __future__ import annotations

from datetime import UTC, datetime

from pcmigrate.sources.pgvector.models import (
    ColumnInfo,
    DuplicateEstimate,
    IndexInfo,
    MetricGuess,
    PgInventory,
    RlsClass,
    RlsPolicy,
    TableInfo,
    TextSearchField,
    ThroughputSample,
    VectorField,
    VectorKind,
)


def column(name: str, base_type: str, *, type_text: str | None = None, **kw: object) -> ColumnInfo:
    nullable = bool(kw.pop("nullable", True))
    return ColumnInfo(
        name=name,
        type=type_text or base_type,
        base_type=base_type,
        nullable=nullable,
        position=1,
        **kw,  # type: ignore[arg-type]
    )


def documents_table(**overrides: object) -> TableInfo:
    table = TableInfo(
        schema_name="public",
        name="documents",
        relkind="r",
        row_estimate=1000,
        exact_rows=1005,
        total_bytes=8_000_000,
        primary_key=["id"],
        columns=[
            column("id", "int8", type_text="bigint", nullable=False),
            column("title", "text", avg_width=12),
            column("category", "text", avg_width=5, null_frac=0.14),
            column("published_at", "timestamptz", type_text="timestamp with time zone"),
            column("tags", "_text", type_text="text[]"),
            column(
                "attrs",
                "jsonb",
                sample_values=['{"views": 3, "nested": {"lang": "en", "score": 1}}'],
                sample_json_depth=2,
            ),
            column("price", "numeric", type_text="numeric(10,2)"),
            column("body", "text", avg_width=250),
            column("author_email", "text"),
        ],
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.VECTOR,
                dimension=768,
                nullable=False,
                metric=MetricGuess.COSINE,
                metric_index="documents_embedding_cosine_idx",
                index_method="hnsw",
                sampled_rows=1000,
                distinct_dimensions=[768],
            )
        ],
        indexes=[
            IndexInfo(
                name="documents_embedding_cosine_idx",
                method="hnsw",
                columns=["embedding"],
                opclasses=["vector_cosine_ops"],
                definition="CREATE INDEX ... USING hnsw (embedding vector_cosine_ops)",
            )
        ],
        duplicates={
            "embedding": DuplicateEstimate(
                method="exact",
                sampled_rows=1005,
                duplicate_rows_in_sample=4,
                duplicate_groups=1,
                estimated_duplicate_rows=4,
            )
        },
        throughput=ThroughputSample(rows=2000, seconds=0.5, bytes=6_000_000),
    )
    return table.model_copy(update=overrides)


def tenants_table() -> TableInfo:
    return TableInfo(
        schema_name="public",
        name="tenants",
        relkind="r",
        row_estimate=150,
        exact_rows=150,
        primary_key=["id"],
        columns=[
            column("id", "int8", nullable=False),
            column("tenant_id", "text"),
            column("label", "text"),
        ],
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.VECTOR,
                dimension=768,
                nullable=False,
                sampled_rows=150,
                distinct_dimensions=[768],
            )
        ],
        rls_enabled=True,
        rls_forced=True,
        policies=[
            RlsPolicy(
                name="tenants_isolation",
                cmd="ALL",
                roles=["public"],
                qual="(tenant_id = current_setting('app.tenant_id'::text, true))",
                with_check=None,
                classification=RlsClass.NAMESPACE,
                column="tenant_id",
                note="one namespace per tenant_id",
            )
        ],
    )


def inventory(*tables: TableInfo) -> PgInventory:
    return PgInventory(
        server_version="16.15",
        pgvector_version="0.8.7",
        pg_stat_statements=True,
        sampled_at=datetime(2026, 10, 1, tzinfo=UTC),
        read_only_session=True,
        tables=list(tables) or [documents_table(), tenants_table()],
    )


def text_search_documents() -> TableInfo:
    return documents_table(
        text_search=[TextSearchField(column="body_tsv", gin_index="documents_body_tsv_idx")]
    )
