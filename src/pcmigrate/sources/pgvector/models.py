"""Inventory models produced by pgvector introspection and consumed by the report."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.plan_lock import schema_fingerprint
from pcmigrate.sources.base import SourceSchema


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class VectorKind(StrEnum):
    """pgvector column types that carry vectors."""

    VECTOR = "vector"
    HALFVEC = "halfvec"
    SPARSEVEC = "sparsevec"


class MetricGuess(StrEnum):
    """Metric inferred from an index operator class, or unknown when no index exists."""

    COSINE = "cosine"
    DOTPRODUCT = "dotproduct"
    EUCLIDEAN = "euclidean"
    UNKNOWN = "unknown"


class RlsClass(StrEnum):
    """How a row-level security policy maps onto Pinecone (design, report section 4)."""

    NAMESPACE = "namespace"
    FILTER = "filter"
    DENORMALIZE = "denormalize"
    DOES_NOT_MAP = "does_not_map"


class ColumnInfo(_Model):
    """One non-vector column."""

    name: str
    type: str
    base_type: str
    nullable: bool
    position: int
    null_frac: float | None = None
    avg_width: int | None = None
    n_distinct: float | None = None
    is_generated: bool = False
    sample_values: list[str] = Field(default_factory=list)
    sample_json_depth: int | None = None


class VectorField(_Model):
    """A vector, halfvec, or sparsevec column and what its index says about the metric."""

    column: str
    kind: VectorKind
    dimension: int | None
    nullable: bool
    metric: MetricGuess = MetricGuess.UNKNOWN
    metric_index: str | None = None
    index_method: str | None = None
    sampled_rows: int = 0
    null_rows: int = 0
    distinct_dimensions: list[int] = Field(default_factory=list)


class TextSearchField(_Model):
    """A tsvector column, generated or stored, with its GIN index if any."""

    column: str
    expression: str | None = None
    gin_index: str | None = None


class IndexInfo(_Model):
    """One index on a table."""

    name: str
    method: str
    columns: list[str]
    opclasses: list[str]
    definition: str
    is_primary: bool = False
    is_unique: bool = False


class RlsPolicy(_Model):
    """One row-level security policy and its classification."""

    name: str
    cmd: str
    roles: list[str]
    qual: str | None
    with_check: str | None
    classification: RlsClass
    column: str | None = None
    note: str = ""


class WorkloadSample(_Model):
    """Deltas from pg_stat_user_tables and pg_stat_statements over the window."""

    window_s: float
    measured: bool
    inserts_per_min: float = 0.0
    updates_per_min: float = 0.0
    deletes_per_min: float = 0.0
    seq_scans_per_min: float = 0.0
    idx_scans_per_min: float = 0.0
    read_qps: float | None = None
    statements_available: bool = False


class DuplicateEstimate(_Model):
    """Rows whose vector equals another row's, beyond the first in each group."""

    method: str
    sampled_rows: int
    duplicate_rows_in_sample: int
    duplicate_groups: int | None = None
    estimated_duplicate_rows: int = 0


class ThroughputSample(_Model):
    """A timed read of the vector column, for export ETA."""

    rows: int
    seconds: float
    bytes: int

    @property
    def rows_per_s(self) -> float:
        """Rows per second, zero when nothing was timed."""
        return self.rows / self.seconds if self.seconds > 0 else 0.0

    @property
    def bytes_per_s(self) -> float:
        """Bytes per second, zero when nothing was timed."""
        return self.bytes / self.seconds if self.seconds > 0 else 0.0


class TableInfo(_Model):
    """Everything introspection learned about one table or partitioned table."""

    schema_name: str
    name: str
    relkind: str
    partitions: list[str] = Field(default_factory=list)
    row_estimate: int = 0
    exact_rows: int | None = None
    total_bytes: int = 0
    primary_key: list[str] = Field(default_factory=list)
    columns: list[ColumnInfo] = Field(default_factory=list)
    vectors: list[VectorField] = Field(default_factory=list)
    text_search: list[TextSearchField] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    rls_enabled: bool = False
    rls_forced: bool = False
    rls_hides_rows: bool = False
    policies: list[RlsPolicy] = Field(default_factory=list)
    workload: WorkloadSample | None = None
    duplicates: dict[str, DuplicateEstimate] = Field(default_factory=dict)
    throughput: ThroughputSample | None = None

    @property
    def qualified_name(self) -> str:
        """schema.table."""
        return f"{self.schema_name}.{self.name}"

    @property
    def rows(self) -> int:
        """Exact count when known and trustworthy, else the planner estimate."""
        if self.exact_rows is not None and not self.rls_hides_rows:
            return self.exact_rows
        return self.row_estimate

    @property
    def dense(self) -> VectorField | None:
        """The first vector or halfvec column, which the dense wave migrates."""
        for field in self.vectors:
            if field.kind in (VectorKind.VECTOR, VectorKind.HALFVEC):
                return field
        return None

    @property
    def sparse(self) -> list[VectorField]:
        """Sparsevec columns, for the hybrid wave."""
        return [f for f in self.vectors if f.kind is VectorKind.SPARSEVEC]

    def column(self, name: str) -> ColumnInfo | None:
        """Look up a column by name."""
        return next((c for c in self.columns if c.name == name), None)


class PgInventory(_Model):
    """The full introspection result."""

    server_version: str
    pgvector_version: str | None
    pg_stat_statements: bool
    sampled_at: datetime
    read_only_session: bool
    tables: list[TableInfo] = Field(default_factory=list)

    def table(self, qualified: str) -> TableInfo | None:
        """Look up a table by schema.name."""
        return next((t for t in self.tables if t.qualified_name == qualified), None)

    def structural_snapshot(self) -> dict[str, Any]:
        """The schema facts that invalidate an approval when they change. Counts excluded."""
        snapshot: dict[str, Any] = {}
        for table in self.tables:
            snapshot[table.qualified_name] = {
                "relkind": table.relkind,
                "partitions": sorted(table.partitions),
                "primary_key": table.primary_key,
                "columns": {c.name: c.type for c in table.columns},
                "vectors": {
                    v.column: {"kind": v.kind.value, "dimension": v.dimension, "metric": v.metric}
                    for v in table.vectors
                },
                "text_search": [t.column for t in table.text_search],
                "policies": [
                    {"name": p.name, "qual": p.qual, "classification": p.classification.value}
                    for p in table.policies
                ],
                "rls_enabled": table.rls_enabled,
            }
        return snapshot

    @property
    def fingerprint(self) -> str:
        """SHA-256 over the structural snapshot."""
        return schema_fingerprint(self.structural_snapshot())

    def to_source_schema(self) -> SourceSchema:
        """The connector-level schema used for plan.lock."""
        return SourceSchema(kind="pgvector", objects=self.structural_snapshot())
