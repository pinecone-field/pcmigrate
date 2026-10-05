"""Source-neutral inventory models shared by introspection, the findings engine, and the report.

pgvector tables and OpenSearch indices both become an ``ObjectInfo``: fields with types and
sampled statistics, vector fields with a dimension and an inferred metric, access policies,
workload deltas, duplicate estimates, and a throughput sample. Source-specific facts live in
optional fields so one findings engine and one report template serve every connector.
"""

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
    """Source column or field types that carry vectors."""

    VECTOR = "vector"
    HALFVEC = "halfvec"
    SPARSEVEC = "sparsevec"
    KNN_VECTOR = "knn_vector"
    RANK_FEATURES = "rank_features"


DENSE_KINDS = frozenset({VectorKind.VECTOR, VectorKind.HALFVEC, VectorKind.KNN_VECTOR})
SPARSE_KINDS = frozenset({VectorKind.SPARSEVEC, VectorKind.RANK_FEATURES})


class MetricGuess(StrEnum):
    """Metric inferred from an index operator class or space type, or unknown."""

    COSINE = "cosine"
    DOTPRODUCT = "dotproduct"
    EUCLIDEAN = "euclidean"
    UNKNOWN = "unknown"
    UNSUPPORTED = "unsupported"


class RlsClass(StrEnum):
    """How a row- or document-level access rule maps onto Pinecone (design, report section 4)."""

    NAMESPACE = "namespace"
    FILTER = "filter"
    DENORMALIZE = "denormalize"
    DOES_NOT_MAP = "does_not_map"


class ColumnInfo(_Model):
    """One non-vector column or field.

    ``base_type`` drives the mapping rules: PostgreSQL type names for pgvector, OpenSearch
    mapping types for OpenSearch, with ``<type>_array`` when samples show arrays and
    ``object`` / ``nested`` for structured fields.
    """

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
    analyzer: str | None = None


class VectorField(_Model):
    """A vector column or field and what its index or mapping says about the metric."""

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
    space_type: str | None = None
    engine: str | None = None
    data_type: str | None = None
    in_source: bool | None = None
    source_setting: str | None = None


class TextSearchField(_Model):
    """A full-text field: a tsvector column with its GIN index, or an analyzed text field."""

    column: str
    kind: str = "tsvector"
    expression: str | None = None
    gin_index: str | None = None
    analyzer: str | None = None


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
    """One access rule (RLS policy, or document-/field-level security) and its classification."""

    name: str
    cmd: str
    roles: list[str]
    qual: str | None
    with_check: str | None
    classification: RlsClass
    column: str | None = None
    note: str = ""


class WorkloadSample(_Model):
    """Write and read rate deltas over the measurement window."""

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
    """A timed read of the vector field, for export ETA."""

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


class ObjectInfo(_Model):
    """Everything introspection learned about one table, partitioned table, or index.

    ``relkind`` is ``r`` (table), ``p`` (partitioned table), or ``i`` (OpenSearch index).
    """

    schema_name: str
    name: str
    relkind: str
    partitions: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    shards: int | None = None
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
    settings: dict[str, Any] = Field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        """schema.table, or the bare index name when there is no schema."""
        return f"{self.schema_name}.{self.name}" if self.schema_name else self.name

    @property
    def kind_label(self) -> str:
        """How the report describes the object."""
        if self.relkind == "p":
            return f"partitioned ({len(self.partitions)})"
        if self.relkind == "i":
            shards = f" ({self.shards} shards)" if self.shards else ""
            return f"index{shards}"
        return "table"

    @property
    def rows(self) -> int:
        """Exact count when known and trustworthy, else the planner estimate."""
        if self.exact_rows is not None and not self.rls_hides_rows:
            return self.exact_rows
        return self.row_estimate

    @property
    def dense(self) -> VectorField | None:
        """The first dense vector field, which the dense wave migrates."""
        for field in self.vectors:
            if field.kind in DENSE_KINDS:
                return field
        return None

    @property
    def sparse(self) -> list[VectorField]:
        """Sparse vector fields, for the hybrid wave."""
        return [f for f in self.vectors if f.kind in SPARSE_KINDS]

    def column(self, name: str) -> ColumnInfo | None:
        """Look up a column by name."""
        return next((c for c in self.columns if c.name == name), None)


class Inventory(_Model):
    """The full introspection result for one source."""

    kind: str = "pgvector"
    server_version: str
    pgvector_version: str | None = None
    pg_stat_statements: bool = False
    sampled_at: datetime
    read_only_session: bool
    tables: list[ObjectInfo] = Field(default_factory=list)
    plugin_versions: dict[str, str] = Field(default_factory=dict)
    cluster: dict[str, Any] = Field(default_factory=dict)

    @property
    def source_label(self) -> str:
        """Engine and version line for the report header."""
        if self.kind == "opensearch":
            knn = self.plugin_versions.get("opensearch-knn")
            plugin = f" (k-NN plugin {knn})" if knn else " (k-NN plugin not installed)"
            return f"OpenSearch {self.server_version}{plugin}"
        extension = self.pgvector_version or "not installed"
        return f"PostgreSQL {self.server_version} (pgvector {extension})"

    @property
    def workload_note(self) -> str:
        """What the workload section could and could not measure."""
        if self.kind == "opensearch":
            return (
                "Rates from _stats and _nodes/stats deltas; index operations count creates "
                "and updates together."
            )
        if self.pg_stat_statements:
            return "pg_stat_statements: available."
        return "pg_stat_statements: not installed, read QPS unavailable."

    @property
    def object_noun(self) -> str:
        """``table`` or ``index``."""
        return "index" if self.kind == "opensearch" else "table"

    @property
    def object_plural(self) -> str:
        """``tables`` or ``indices``."""
        return "indices" if self.kind == "opensearch" else "tables"

    def table(self, qualified: str) -> ObjectInfo | None:
        """Look up an object by qualified name."""
        return next((t for t in self.tables if t.qualified_name == qualified), None)

    def structural_snapshot(self) -> dict[str, Any]:
        """The schema facts that invalidate an approval when they change. Counts excluded."""
        snapshot: dict[str, Any] = {}
        for table in self.tables:
            entry: dict[str, Any] = {
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
            if table.aliases:
                entry["aliases"] = sorted(table.aliases)
            if table.settings:
                entry["settings"] = table.settings
            snapshot[table.qualified_name] = entry
        return snapshot

    @property
    def fingerprint(self) -> str:
        """SHA-256 over the structural snapshot."""
        return schema_fingerprint(self.structural_snapshot())

    def to_source_schema(self) -> SourceSchema:
        """The connector-level schema used for plan.lock."""
        return SourceSchema(kind=self.kind, objects=self.structural_snapshot())
