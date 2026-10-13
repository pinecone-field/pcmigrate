"""Generate the proposed mapping.yaml from a pgvector inventory, in the documented shape."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pcmigrate.config.mapping import (
    DEFAULT_SPARSE_MODEL,
    ChangeCaptureSpec,
    DenseSpec,
    HybridCheck,
    Mapping,
    MetadataField,
    MetadataType,
    Metric,
    ObjectMapping,
    ReadNodesSpec,
    SourceKind,
    SourceSpec,
    SparseKind,
    SparseModelSpec,
    SparseSpec,
    TargetSpec,
    ValidationSpec,
)
from pcmigrate.report import limits
from pcmigrate.sources.inventory import (
    ColumnInfo,
    Inventory,
    MetricGuess,
    ObjectInfo,
    RlsClass,
    VectorField,
    VectorKind,
)

STRING_TYPES = {
    "text",
    "varchar",
    "bpchar",
    "char",
    "name",
    "uuid",
    "citext",
    "inet",
    "cidr",
    "macaddr",
    "xml",
}
INTEGER_TYPES = {"int2", "int4", "int8", "oid"}
FLOAT_TYPES = {"float4", "float8", "numeric", "money"}
BOOL_TYPES = {"bool"}
TIME_TYPES = {"timestamp", "timestamptz", "date"}
STRING_LIST_TYPES = {"_text", "_varchar", "_bpchar", "_uuid", "_citext"}
NUMERIC_ARRAY_TYPES = {"_int2", "_int4", "_int8", "_float4", "_float8", "_numeric"}
JSON_TYPES = {"json", "jsonb", "object"}
UPDATED_AT_NAMES = ("updated_at", "modified_at", "last_modified", "updatedat")
OS_STRING_TYPES = {
    "keyword",
    "wildcard",
    "constant_keyword",
    "ip",
    "version",
    "match_only_text",
    "search_as_you_type",
}
OS_INTEGER_TYPES = {"integer", "long", "short", "byte", "unsigned_long"}
OS_FLOAT_TYPES = {"float", "double", "half_float", "scaled_float"}
OS_BOOL_TYPES = {"boolean"}
OS_DATE_TYPES = {"date", "date_nanos"}
NESTED_TYPES = {"nested"}
ARRAY_SUFFIX = "_array"


@dataclass(frozen=True, slots=True)
class MappingOptions:
    """FDE-supplied values the inventory cannot know."""

    migration_id: str
    index: str
    region: str
    dsn_secret: str | None = None
    capacity: Literal["on_demand", "dedicated"] = "on_demand"
    roadmap: Literal["dense", "hybrid", "fts"] = "dense"
    mode: Literal["create", "adopt"] = "create"
    node_type: Literal["b1", "t1"] = "b1"
    shards: int = 1
    replicas: int = 1
    multi_namespace: bool = False
    sparse_from: tuple[str, ...] = ()
    sparse_model: str = DEFAULT_SPARSE_MODEL

    def read_nodes(self) -> ReadNodesSpec | None:
        """The dedicated-read-node spec when capacity is dedicated."""
        if self.capacity != "dedicated":
            return None
        return ReadNodesSpec(
            node_type=self.node_type,
            shards=self.shards,
            replicas=self.replicas,
            multi_namespace=self.multi_namespace,
        )


def scalar_type(column: ColumnInfo) -> MetadataType | None:
    """The metadata type a scalar column maps to, or None when it has no scalar form."""
    return scalar_type_of(column.base_type)


def scalar_type_of(base: str) -> MetadataType | None:
    """Type rules for PostgreSQL type names and OpenSearch mapping types."""
    if base in STRING_TYPES or base in OS_STRING_TYPES:
        return MetadataType.STRING
    if base in INTEGER_TYPES or base in OS_INTEGER_TYPES:
        return MetadataType.INTEGER
    if base in FLOAT_TYPES or base in OS_FLOAT_TYPES:
        return MetadataType.FLOAT
    if base in BOOL_TYPES or base in OS_BOOL_TYPES:
        return MetadataType.BOOLEAN
    if base in TIME_TYPES or base in OS_DATE_TYPES:
        return MetadataType.EPOCH_SECONDS
    if base in STRING_LIST_TYPES:
        return MetadataType.STRING_LIST
    if base.endswith(ARRAY_SUFFIX):
        stem = scalar_type_of(base[: -len(ARRAY_SUFFIX)])
        return MetadataType.STRING_LIST if stem is MetadataType.STRING else None
    return None


def is_numeric_array(base: str) -> bool:
    """A sampled array of numbers, booleans, or dates: stringified, not a string list."""
    if base in NUMERIC_ARRAY_TYPES:
        return True
    if not base.endswith(ARRAY_SUFFIX):
        return False
    stem = scalar_type_of(base[: -len(ARRAY_SUFFIX)])
    return stem in (
        MetadataType.INTEGER,
        MetadataType.FLOAT,
        MetadataType.BOOLEAN,
        MetadataType.EPOCH_SECONDS,
    )


def _json_type_for(value: object) -> MetadataType | None:
    if isinstance(value, bool):
        return MetadataType.BOOLEAN
    if isinstance(value, int):
        return MetadataType.INTEGER
    if isinstance(value, float):
        return MetadataType.FLOAT
    if isinstance(value, str):
        return MetadataType.STRING
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return MetadataType.STRING_LIST
    return None


def _flatten_json(
    prefix: str, value: object, depth: int, out: dict[str, MetadataType | None]
) -> None:
    if isinstance(value, dict) and depth < limits.JSON_FLATTEN_MAX_DEPTH:
        for key, inner in value.items():
            _flatten_json(f"{prefix}.{key}", inner, depth + 1, out)
        return
    out[prefix] = _json_type_for(value) if not isinstance(value, dict) else None


def json_fields(column: ColumnInfo) -> list[MetadataField]:
    """Flatten a JSON column's sampled keys to dotted targets, else stringify the whole value."""
    import json

    merged: dict[str, MetadataType | None] = {}
    for raw in column.sample_values:
        if raw.endswith("..."):
            continue
        try:
            value = json.loads(raw)
        except ValueError:
            continue
        if isinstance(value, dict):
            _flatten_json(column.name, value, 0, merged)
    if (column.sample_json_depth or 0) > limits.JSON_FLATTEN_MAX_DEPTH or not merged:
        return [
            MetadataField(
                source=column.name,
                target=column.name,
                type=MetadataType.STRING,
                transform="stringify",
            )
        ]
    fields: list[MetadataField] = []
    for dotted, kind in sorted(merged.items()):
        if kind is None:
            fields.append(
                MetadataField(
                    source=dotted, target=dotted, type=MetadataType.STRING, transform="stringify"
                )
            )
        else:
            fields.append(
                MetadataField(source=dotted, target=dotted, type=kind, transform="flatten")
            )
    return fields


def generated_sparse_spec(table: ObjectInfo, options: MappingOptions) -> SparseSpec | None:
    """A sparse vector encoded from ``--sparse-from`` fields the table has (ADR-017)."""
    if not options.sparse_from:
        return None
    present = [name for name in options.sparse_from if table.column(name) is not None]
    if not present:
        return None
    return SparseSpec(
        kind=SparseKind.GENERATED,
        text_fields=present,
        model=SparseModelSpec(name=options.sparse_model),
    )


def sparse_spec(
    table: ObjectInfo, roadmap: str, options: MappingOptions | None = None
) -> SparseSpec | None:
    """The hybrid wave sparse vector: generated from text when asked, else the first stored."""
    if options is not None and options.sparse_from:
        return generated_sparse_spec(table, options)
    if roadmap == "dense":
        return None
    for vector in table.sparse:
        if vector.kind is VectorKind.SPARSEVEC:
            return SparseSpec(
                column=vector.column, kind=SparseKind.SPARSEVEC, dimension=vector.dimension
            )
        if vector.kind is VectorKind.RANK_FEATURES:
            return SparseSpec(
                column=vector.column, kind=SparseKind.RANK_FEATURES, vocabulary="xxh32"
            )
    return None


def hybrid_readiness(table: ObjectInfo, obj: ObjectMapping | None = None) -> str:
    """``hybrid``, ``generated`` (sparse encoded from text), ``fts``, ``dense``, or ``none``."""
    if table.dense is None:
        return "none"
    if obj is not None and obj.generated_sparse is not None:
        return "generated"
    if table.sparse:
        return "hybrid"
    if table.text_search:
        return "fts"
    return "dense"


def text_width(table: ObjectInfo, fields: Sequence[str]) -> int:
    """Mean bytes per row of the concatenated text fields, from column statistics."""
    total = 0
    for name in fields:
        column = table.column(name)
        if column is not None and column.avg_width is not None:
            total += int(column.avg_width)
    return total


def metadata_fields(
    table: ObjectInfo, exclude: set[str], *, sparse: VectorField | None = None
) -> list[MetadataField]:
    """Apply the type rules from the design to every non-vector column."""
    fields: list[MetadataField] = []
    for column in table.columns:
        if column.name in exclude:
            continue
        if column.avg_width is not None and column.avg_width > limits.METADATA_BYTES_PER_RECORD:
            fields.append(
                MetadataField(
                    source=column.name,
                    action="drop",
                    reason=f"exceeds metadata size limit (avg {column.avg_width} bytes)",
                )
            )
            continue
        target = column.name.lstrip("".join(limits.RESERVED_FIELD_PREFIXES)) or column.name
        if target != column.name:
            target = f"f_{target}"
        if column.base_type in JSON_TYPES:
            fields.extend(json_fields(column))
            continue
        if is_numeric_array(column.base_type) or column.base_type in NESTED_TYPES:
            fields.append(
                MetadataField(
                    source=column.name,
                    target=target,
                    type=MetadataType.STRING,
                    transform="stringify",
                )
            )
            continue
        kind = scalar_type(column)
        if kind is None:
            fields.append(
                MetadataField(
                    source=column.name,
                    action="drop",
                    reason=f"type {column.type} has no scalar metadata form",
                )
            )
            continue
        fields.append(MetadataField(source=column.name, target=target, type=kind))
    for text in table.text_search:
        if text.kind != "tsvector":
            continue
        fields.append(
            MetadataField(
                source=text.column,
                action="drop",
                reason="tsvector is derived; the FTS wave carries the source text instead",
            )
        )
    for vector in table.vectors:
        if table.dense is not None and vector.column == table.dense.column:
            continue
        if sparse is not None and vector.column == sparse.column:
            continue
        reason = (
            f"additional {vector.kind.value} column; the dense wave carries one vector per record"
            if sparse is None
            else f"additional {vector.kind.value} column; one sparse vector per record"
        )
        fields.append(MetadataField(source=vector.column, action="drop", reason=reason))
    return fields


def id_template(table: ObjectInfo) -> str:
    """``table#{pk}``, joining composite keys with ``:``."""
    keys = table.primary_key or ["id"]
    return f"{table.name}#" + ":".join("{" + k + "}" for k in keys)


def dense_spec(table: ObjectInfo) -> DenseSpec | None:
    """The dense column with its inferred metric, defaulting to cosine when unknown."""
    dense = table.dense
    if dense is None or dense.dimension is None:
        return None
    metric = (
        Metric.COSINE
        if dense.metric in (MetricGuess.UNKNOWN, MetricGuess.UNSUPPORTED)
        else Metric(dense.metric.value)
    )
    return DenseSpec(column=dense.column, dimension=dense.dimension, metric=metric)


def namespace_for(table: ObjectInfo) -> tuple[str, str | None]:
    """Namespace name and, when a policy maps to a namespace, the column that selects it."""
    for policy in table.policies:
        if policy.classification is RlsClass.NAMESPACE and policy.column:
            return table.name, policy.column
    return table.name, None


def measured_writes(inventory: Inventory) -> float | None:
    """Writes per minute across mapped tables, or None when the window was not measured."""
    total = 0.0
    measured = False
    for table in inventory.tables:
        if table.dense is None or table.workload is None or not table.workload.measured:
            continue
        measured = True
        total += (
            table.workload.inserts_per_min
            + table.workload.updates_per_min
            + table.workload.deletes_per_min
        )
    return total if measured else None


def recommend_change_capture(inventory: Inventory) -> ChangeCaptureSpec:
    """``none`` when the window measured zero writes, else the source's live tier.

    pgvector proposes trigger_outbox, or watermark when every table carries an updated_at
    column (ADR-008). OpenSearch proposes dual_write (ADR-009), with an updated_at rescan
    when a date field by that name exists. The FDE can still pick another tier by hand.
    """
    writes = measured_writes(inventory)
    if writes is not None and writes == 0:
        return ChangeCaptureSpec(tier="none")
    if inventory.kind == "opensearch":
        column = next(
            (
                c.name
                for t in inventory.tables
                for c in t.columns
                if c.name in UPDATED_AT_NAMES and c.base_type in OS_DATE_TYPES
            ),
            None,
        )
        return ChangeCaptureSpec(
            tier="dual_write", watermark_column=column, rescan=column is not None
        )
    for table in inventory.tables:
        if table.dense is None:
            continue
        if not any(c.name in UPDATED_AT_NAMES for c in table.columns):
            return ChangeCaptureSpec(tier="trigger_outbox")
    column = next(
        (c.name for t in inventory.tables for c in t.columns if c.name in UPDATED_AT_NAMES),
        None,
    )
    return ChangeCaptureSpec(tier="watermark", watermark_column=column)


def generate_mapping(inventory: Inventory, options: MappingOptions) -> Mapping:
    """Build the proposed mapping for every table with a dense vector."""
    objects: list[ObjectMapping] = []
    for table in inventory.tables:
        dense = dense_spec(table)
        if dense is None:
            continue
        namespace, namespace_by = namespace_for(table)
        exclude = set(table.primary_key)
        sparse = sparse_spec(table, options.roadmap, options)
        sparse_field = next(
            (v for v in table.sparse if sparse and sparse.column and v.column == sparse.column),
            None,
        )
        objects.append(
            ObjectMapping(
                source=table.qualified_name,
                namespace=namespace,
                namespace_by=namespace_by,
                id_template=id_template(table),
                dense=dense,
                sparse=sparse,
                metadata=metadata_fields(table, exclude, sparse=sparse_field),
            )
        )
    if not objects:
        raise ValueError(f"no {inventory.object_noun} with a dense vector field was found")
    hybrid = any(o.sparse is not None for o in objects)
    target_type: Literal["vector", "document"] = (
        "document" if options.roadmap in ("hybrid", "fts") or hybrid else "vector"
    )
    validation = ValidationSpec(hybrid=HybridCheck()) if hybrid else ValidationSpec()
    return Mapping(
        migration_id=options.migration_id,
        source=SourceSpec(kind=SourceKind(inventory.kind), dsn_secret=options.dsn_secret),
        target=TargetSpec(
            index=options.index,
            mode=options.mode,
            type=target_type,
            region=options.region,
            capacity=options.capacity,
            read_nodes=options.read_nodes(),
        ),
        objects=objects,
        change_capture=recommend_change_capture(inventory),
        validation=validation,
    )
