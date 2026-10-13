"""Findings engine: one function per check, each returning a Finding with severity and fix."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict

from pcmigrate.config.mapping import Mapping
from pcmigrate.report import limits
from pcmigrate.report.sizing import SizingPlan
from pcmigrate.sources.inventory import (
    DENSE_KINDS,
    Inventory,
    MetricGuess,
    ObjectInfo,
    RlsClass,
    VectorKind,
)

Roadmap = Literal["dense", "hybrid", "fts"]


class Severity(StrEnum):
    """Blocker stops the run at the gate; warning needs acknowledgement; info is advisory."""

    BLOCKER = "blocker"
    WARNING = "warning"
    INFO = "info"


class Finding(BaseModel):
    """One risk, graded, with the fix."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    check: str
    severity: Severity
    object: str
    field: str | None
    message: str
    fix: str


@dataclass(frozen=True, slots=True)
class FindingsContext:
    """Inputs the checks read."""

    inventory: Inventory
    mapping: Mapping
    sizing: SizingPlan
    roadmap: Roadmap = "dense"


Check = Callable[[FindingsContext], list[Finding]]

NUMERIC_ARRAY_TYPES = {"_int2", "_int4", "_int8", "_float4", "_float8", "_numeric"}
NUMERIC_ARRAY_STEMS = {
    "integer",
    "long",
    "short",
    "byte",
    "unsigned_long",
    "float",
    "double",
    "half_float",
    "scaled_float",
    "boolean",
}
PII_PATTERNS = re.compile(
    r"(^|_)(email|e_mail|phone|mobile|ssn|social_security|passport|dob|birth|first_name|last_name|"
    r"full_name|surname|address|street|zip|postal|ip_addr|ip_address|credit_card|card_number|iban|salary)($|_)",
    re.IGNORECASE,
)


def check_vector_dimensions(ctx: FindingsContext) -> list[Finding]:
    """Blocker: nulls or mixed dimensions in a vector column abort import on first mismatch."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for vector in table.vectors:
            problems: list[str] = []
            if vector.kind is VectorKind.KNN_VECTOR:
                if len(vector.distinct_dimensions) > 1 or (
                    vector.dimension is not None
                    and any(d != vector.dimension for d in vector.distinct_dimensions)
                ):
                    problems.append(
                        f"dimensions {vector.distinct_dimensions} seen against mapping dimension {vector.dimension}"
                    )
                if problems:
                    out.append(
                        Finding(
                            check="vector_dimensions",
                            severity=Severity.BLOCKER,
                            object=table.qualified_name,
                            field=vector.column,
                            message="; ".join(problems),
                            fix="Re-index the offending documents with the mapped dimension.",
                        )
                    )
                continue
            if vector.null_rows:
                problems.append(
                    f"{vector.null_rows} of {vector.sampled_rows} sampled rows are NULL"
                )
            if len(vector.distinct_dimensions) > 1:
                problems.append(f"dimensions {vector.distinct_dimensions} in one column")
            if vector.dimension is None and vector.distinct_dimensions:
                problems.append("column declared without a dimension")
            if problems:
                out.append(
                    Finding(
                        check="vector_dimensions",
                        severity=Severity.BLOCKER,
                        object=table.qualified_name,
                        field=vector.column,
                        message="; ".join(problems),
                        fix="Filter or fix the offending rows before export, or declare the column with a fixed dimension and NOT NULL.",
                    )
                )
    return out


def check_target_cloud(ctx: FindingsContext) -> list[Finding]:
    """Blocker: S3 import only reaches AWS-hosted indexes."""
    target = ctx.mapping.target
    if target.cloud != "aws":
        return [
            Finding(
                check="target_cloud",
                severity=Severity.BLOCKER,
                object=target.index,
                field="target.cloud",
                message=f"target cloud is {target.cloud!r}; bulk import from S3 requires an AWS-hosted index",
                fix="Create the index in an AWS region.",
            )
        ]
    return []


def check_primary_key(ctx: FindingsContext) -> list[Finding]:
    """Blocker: without a primary key there is no stable record id."""
    return [
        Finding(
            check="primary_key",
            severity=Severity.BLOCKER,
            object=table.qualified_name,
            field=None,
            message="table has no primary key, so record ids cannot be derived",
            fix="Add a primary key or unique NOT NULL column and set id_template to use it.",
        )
        for table in ctx.inventory.tables
        if table.dense is not None and not table.primary_key
    ]


def check_duplicate_vectors(ctx: FindingsContext) -> list[Finding]:
    """Warning: bulk import drops records whose vector duplicates another's (ADR-011)."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for column, estimate in table.duplicates.items():
            if estimate.estimated_duplicate_rows <= 0:
                continue
            how = (
                "exact count"
                if estimate.method == "exact"
                else f"estimate from {estimate.sampled_rows} sampled rows"
            )
            out.append(
                Finding(
                    check="duplicate_vectors",
                    severity=Severity.WARNING,
                    object=table.qualified_name,
                    field=column,
                    message=f"about {estimate.estimated_duplicate_rows} rows share a vector with another row ({how})",
                    fix="Keep duplicates: upsert_after_import (default). Drop them: set duplicates: drop in mapping.yaml. Rerun with --full-scan for the exact count.",
                )
            )
    return out


def check_metadata_size(ctx: FindingsContext) -> list[Finding]:
    """Warning: a field whose average width exceeds the per-record metadata limit."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for column in table.columns:
            if column.avg_width is not None and column.avg_width > limits.METADATA_BYTES_PER_RECORD:
                out.append(
                    Finding(
                        check="metadata_size",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=column.name,
                        message=f"average width {column.avg_width} bytes exceeds the {limits.METADATA_BYTES_PER_RECORD}-byte metadata limit",
                        fix="The mapping drops this field. Truncate it, move it to a document store, or keep it out of metadata.",
                    )
                )
    return out


def check_unsupported_shapes(ctx: FindingsContext) -> list[Finding]:
    """Warning: nested objects, numeric arrays, and reserved field names need a transform."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for column in table.columns:
            reasons: list[str] = []
            if column.base_type in NUMERIC_ARRAY_TYPES or is_numeric_array(column.base_type):
                reasons.append("numeric arrays are not valid metadata")
            if column.base_type == "nested":
                reasons.append("nested arrays of objects become one JSON string")
            if (
                column.base_type in ("json", "jsonb")
                and (column.sample_json_depth or 0) > limits.JSON_FLATTEN_MAX_DEPTH
            ):
                reasons.append(f"JSON nested deeper than {limits.JSON_FLATTEN_MAX_DEPTH} levels")
            if column.name.startswith(limits.RESERVED_FIELD_PREFIXES):
                reasons.append("field names starting with _ or $ are reserved")
            if len(column.name.encode()) > limits.FIELD_NAME_MAX_BYTES:
                reasons.append(f"field name longer than {limits.FIELD_NAME_MAX_BYTES} bytes")
            if reasons:
                out.append(
                    Finding(
                        check="unsupported_shapes",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=column.name,
                        message="; ".join(reasons),
                        fix="The mapping flattens, stringifies, or renames the field; confirm the proposed target in mapping.yaml.",
                    )
                )
    return out


def check_roadmap(ctx: FindingsContext) -> list[Finding]:
    """Warning: a dense-only customer planning hybrid or FTS should start on a document index."""
    out: list[Finding] = []
    if ctx.roadmap in ("hybrid", "fts") and ctx.mapping.target.type == "vector":
        out.append(
            Finding(
                check="roadmap",
                severity=Severity.WARNING,
                object=ctx.mapping.target.index,
                field="target.type",
                message=f"roadmap includes {ctx.roadmap} but the target is a vector index",
                fix="Set target.type: document now; switching later means a re-import (ADR-004).",
            )
        )
    for table in ctx.inventory.tables:
        signals = [f.column for f in table.sparse] + [t.column for t in table.text_search]
        if signals and ctx.mapping.target.type == "vector":
            out.append(
                Finding(
                    check="roadmap",
                    severity=Severity.INFO,
                    object=table.qualified_name,
                    field=", ".join(signals),
                    message="sparse or full-text columns found; the dense wave does not migrate them",
                    fix="Plan hybrid (wave 2) or FTS (wave 3) on a document index if these are queried.",
                )
            )
    return out


def check_export_duration(ctx: FindingsContext) -> list[Finding]:
    """Warning: a long export holds one snapshot open and blocks vacuum (ADR-008)."""
    out: list[Finding] = []
    for item in ctx.sizing.tables:
        if item.export_eta_s is not None and item.export_eta_s > limits.EXPORT_WARN_SECONDS:
            out.append(
                Finding(
                    check="export_duration",
                    severity=Severity.WARNING,
                    object=item.object,
                    field=None,
                    message=f"estimated export of {item.export_eta_s / 3600:.1f} hours on one snapshot holds back vacuum",
                    fix="Increase workers, split the table across runs, or schedule the export off-peak.",
                )
            )
    return out


def check_import_size(ctx: FindingsContext) -> list[Finding]:
    """Warning: more than 1 TB of input for one on-demand import."""
    if ctx.mapping.target.capacity != "on_demand":
        return []
    if ctx.sizing.total_import_bytes <= limits.ON_DEMAND_IMPORT_MAX_BYTES:
        return []
    return [
        Finding(
            check="import_size",
            severity=Severity.WARNING,
            object=ctx.mapping.target.index,
            field=None,
            message=f"{ctx.sizing.total_import_bytes / 1024**4:.2f} TB of import input exceeds the 1 TB on-demand limit",
            fix=f"The plan splits into {len(ctx.sizing.batches)} imports over disjoint namespaces, or use dedicated read nodes.",
        )
    ]


def check_rls(ctx: FindingsContext) -> list[Finding]:
    """Warning when a policy does not map; info when it maps to a namespace or filter."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for policy in table.policies:
            if policy.classification in (RlsClass.DOES_NOT_MAP, RlsClass.DENORMALIZE):
                out.append(
                    Finding(
                        check="rls",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=policy.name,
                        message=f"policy {policy.name} ({policy.classification.value}): {policy.note}",
                        fix="The application must enforce this rule at query time; record the decision with the security owner.",
                    )
                )
            else:
                out.append(
                    Finding(
                        check="rls",
                        severity=Severity.INFO,
                        object=table.qualified_name,
                        field=policy.name,
                        message=f"policy {policy.name} maps to {policy.classification.value}: {policy.note}",
                        fix="Confirm the namespace or server-side filter with the security owner; a metadata filter is not a security boundary.",
                    )
                )
    return out


def check_change_capture(ctx: FindingsContext) -> list[Finding]:
    """Warning: no change capture while the workload window measured writes (ADR-007)."""
    from pcmigrate.report.mapping_gen import measured_writes

    if ctx.mapping.change_capture_enabled:
        return []
    writes = measured_writes(ctx.inventory)
    if writes is None:
        return [
            Finding(
                check="change_capture",
                severity=Severity.INFO,
                object=ctx.mapping.target.index,
                field="change_capture.tier",
                message="change capture is off and the workload window was not measured",
                fix="Rerun introspect with --window during representative traffic, or confirm a write freeze with the customer.",
            )
        ]
    if writes > 0:
        return [
            Finding(
                check="change_capture",
                severity=Severity.WARNING,
                object=ctx.mapping.target.index,
                field="change_capture.tier",
                message=f"change capture is none but the window measured {writes:.1f} writes/min; writes after the snapshot will be missed",
                fix="Agree a write freeze from snapshot to cutover, or set change_capture.tier to watermark, trigger_outbox, or logical.",
            )
        ]
    return []


def check_rls_visibility(ctx: FindingsContext) -> list[Finding]:
    """Blocker: the introspecting role sees fewer rows than exist, so export would too."""
    return [
        Finding(
            check="rls_visibility",
            severity=Severity.BLOCKER,
            object=table.qualified_name,
            field=None,
            message=(
                f"row-level security hides rows from this role: {table.exact_rows} visible of "
                f"about {table.row_estimate}; samples and counts above are incomplete"
            ),
            fix="Run introspection and export with a role that has BYPASSRLS, or as the table owner with FORCE ROW LEVEL SECURITY off.",
        )
        for table in ctx.inventory.tables
        if table.rls_hides_rows
    ]


def check_unknown_metric(ctx: FindingsContext) -> list[Finding]:
    """Warning: no vector index or space type, so the metric was guessed."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for vector in table.vectors:
            if vector.metric is not MetricGuess.UNKNOWN or vector.kind not in DENSE_KINDS:
                continue
            if vector.kind is VectorKind.KNN_VECTOR:
                message = "the mapping names a trained model and no space_type, so the distance metric is unknown"
                fix = "Read the model's space type (GET _plugins/_knn/models/<id>) and set dense.metric in mapping.yaml."
            else:
                message = (
                    "no hnsw or ivfflat index on this column, so the distance metric is unknown"
                )
                fix = "Set dense.metric in mapping.yaml to the operator the application queries with (<=> cosine, <#> dotproduct, <-> euclidean)."
            out.append(
                Finding(
                    check="unknown_metric",
                    severity=Severity.WARNING,
                    object=table.qualified_name,
                    field=vector.column,
                    message=message,
                    fix=fix,
                )
            )
    return out


def check_vector_source(ctx: FindingsContext) -> list[Finding]:
    """Blocker: a knn_vector field absent from _source cannot be exported (ADR-009)."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for vector in table.vectors:
            if vector.kind is not VectorKind.KNN_VECTOR:
                continue
            if vector.in_source is False:
                out.append(
                    Finding(
                        check="vector_source",
                        severity=Severity.BLOCKER,
                        object=table.qualified_name,
                        field=vector.column,
                        message=f"vector missing from _source in every one of {vector.sampled_rows} sampled documents; responsible setting: {vector.source_setting}",
                        fix="Re-index with the vector in _source (remove it from _source.excludes, enable _source, or turn off derived source); OSIS reads documents through _source.",
                    )
                )
            elif vector.source_setting and vector.source_setting.startswith(
                "index.knn.derived_source"
            ):
                out.append(
                    Finding(
                        check="vector_source",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=vector.column,
                        message=f"vectors are served from derived source ({vector.source_setting}); sampled documents did return them",
                        fix="Confirm the OSIS scan returns vectors on this OpenSearch version before approving; count parity and fidelity will catch a silent gap.",
                    )
                )
            elif vector.null_rows and vector.in_source:
                out.append(
                    Finding(
                        check="vector_source",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=vector.column,
                        message=f"{vector.null_rows} of {vector.sampled_rows} sampled documents have no vector; they will be rejected",
                        fix="Expect rejects/ to hold them, or filter the index before export.",
                    )
                )
    return out


def check_vector_space_type(ctx: FindingsContext) -> list[Finding]:
    """Blocker: a space type or data type with no Pinecone equivalent."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        for vector in table.vectors:
            if vector.kind is not VectorKind.KNN_VECTOR:
                continue
            if vector.metric is MetricGuess.UNSUPPORTED:
                out.append(
                    Finding(
                        check="vector_space_type",
                        severity=Severity.BLOCKER,
                        object=table.qualified_name,
                        field=vector.column,
                        message=f"space_type {vector.space_type!r} has no Pinecone metric (supported: l2, cosinesimil, innerproduct)",
                        fix="Pinecone indexes use cosine, euclidean, or dotproduct; choose the closest in dense.metric only if the application's ranking tolerates it.",
                    )
                )
            if vector.data_type not in (None, "float"):
                out.append(
                    Finding(
                        check="vector_data_type",
                        severity=Severity.BLOCKER,
                        object=table.qualified_name,
                        field=vector.column,
                        message=f"knn_vector data_type {vector.data_type!r} is not a float vector",
                        fix="The dense wave migrates float vectors only; byte and binary vectors need re-embedding or a different target.",
                    )
                )
    return out


DEDICATED_SHARD_BYTES = 250 * 1024**3


def check_dedicated_read_nodes(ctx: FindingsContext) -> list[Finding]:
    """Dedicated read nodes: namespace enablement, 250 GB per shard, node count and cost."""
    target = ctx.mapping.target
    if target.capacity != "dedicated" or target.read_nodes is None:
        return []
    out: list[Finding] = []
    namespaces = ctx.mapping.namespaces
    nodes = target.read_nodes
    multi = len(namespaces) > 1 or any(o.namespace_by for o in ctx.mapping.objects)
    if multi and not nodes.multi_namespace:
        out.append(
            Finding(
                check="dedicated_namespaces",
                severity=Severity.WARNING,
                object=target.index,
                field="target.read_nodes.multi_namespace",
                message=f"the plan targets {len(namespaces)} namespaces ({', '.join(namespaces)}); a dedicated-read-node index serves several namespaces only when Pinecone has enabled that for the project, otherwise the second namespace is rejected at import",
                fix="Confirm multi-namespace dedicated read nodes are enabled for the project and set target.read_nodes.multi_namespace: true, or migrate one object per dedicated index.",
            )
        )
    elif multi:
        out.append(
            Finding(
                check="dedicated_namespaces",
                severity=Severity.INFO,
                object=target.index,
                field="target.read_nodes.multi_namespace",
                message=f"{len(namespaces)} namespaces on one dedicated-read-node index; the FDE confirmed the project is enabled for multi-namespace dedicated capacity",
                fix="Nothing to do; import fails clearly if the enablement is missing.",
            )
        )
    needed = (
        -(-ctx.sizing.total_import_bytes // DEDICATED_SHARD_BYTES)
        if ctx.sizing.total_import_bytes
        else 1
    )
    if needed > nodes.shards:
        out.append(
            Finding(
                check="dedicated_shards",
                severity=Severity.WARNING,
                object=target.index,
                field="target.read_nodes.shards",
                message=f"about {ctx.sizing.total_import_bytes / 1024**3:.0f} GB of data needs at least {needed} shard(s) at 250 GB each; the plan has {nodes.shards}",
                fix=f"Set target.read_nodes.shards to {needed} or more.",
            )
        )
    out.append(
        Finding(
            check="dedicated_read_nodes",
            severity=Severity.INFO,
            object=target.index,
            field="target.read_nodes",
            message=f"index is created with dedicated read nodes: {nodes.node_type}, {nodes.shards} shard(s) x {nodes.replicas} replica(s) = {nodes.shards * nodes.replicas} node(s), billed hourly from creation",
            fix="Confirm node type, shards, and replicas with the customer; the project allows 20 nodes in total.",
        )
    )
    return out


def check_hybrid_readiness(ctx: FindingsContext) -> list[Finding]:
    """Per object: hybrid-ready, FTS wave (BM25 plus kNN without stored sparse), or dense only."""
    from pcmigrate.report.mapping_gen import hybrid_readiness

    out: list[Finding] = []
    mapped = {o.source: o for o in ctx.mapping.objects}
    for table in ctx.inventory.tables:
        obj = mapped.get(table.qualified_name)
        if obj is None:
            continue
        readiness = hybrid_readiness(table)
        if readiness == "fts" and ctx.roadmap == "hybrid":
            out.append(
                Finding(
                    check="hybrid_readiness",
                    severity=Severity.WARNING,
                    object=table.qualified_name,
                    field=", ".join(t.column for t in table.text_search),
                    message="hybrid here is BM25 plus kNN with no stored sparse vectors; that is the FTS wave (wave 3), not a hybrid migration",
                    fix="Migrate dense now and plan the text fields for the FTS wave, or store sparse vectors (sparsevec / rank_features) in the source first.",
                )
            )
        if obj.sparse is not None:
            out.append(
                Finding(
                    check="hybrid_readiness",
                    severity=Severity.INFO,
                    object=table.qualified_name,
                    field=obj.sparse.column,
                    message=f"hybrid-ready: dense {obj.dense.column if obj.dense else '?'} plus sparse {obj.sparse.column} ({obj.sparse.kind.value}) land in a document index; queries rank by one clause per request and fuse client-side (ADR-016)",
                    fix="Confirm the application can run dense and sparse searches separately and fuse them.",
                )
            )
            if obj.sparse.kind.value == "rank_features":
                out.append(
                    Finding(
                        check="sparse_vocabulary",
                        severity=Severity.WARNING,
                        object=table.qualified_name,
                        field=obj.sparse.column,
                        message="rank_features tokens become sparse indices through the xxh32 vocabulary recorded under manifest/vocabulary/; query-time sparse encoding must use the same token-to-index function",
                        fix="Ship the vocabulary file to the application, or encode query tokens with xxh32 as pcmigrate.core.sparse.token_index does.",
                    )
                )
    return out


def is_numeric_array(base_type: str) -> bool:
    """``long_array`` and friends, as OpenSearch sampling names them."""
    return base_type.endswith("_array") and base_type[: -len("_array")] in NUMERIC_ARRAY_STEMS


def check_pii(ctx: FindingsContext) -> list[Finding]:
    """Info: field names that look like personal data, for the FDE to confirm."""
    out: list[Finding] = []
    for table in ctx.inventory.tables:
        hits = [c.name for c in table.columns if PII_PATTERNS.search(c.name)]
        if hits:
            out.append(
                Finding(
                    check="pii",
                    severity=Severity.INFO,
                    object=table.qualified_name,
                    field=", ".join(hits),
                    message="field names suggest personal data",
                    fix="Confirm each field should migrate, or drop it in mapping.yaml.",
                )
            )
    return out


ALL_CHECKS: tuple[Check, ...] = (
    check_vector_dimensions,
    check_target_cloud,
    check_primary_key,
    check_duplicate_vectors,
    check_metadata_size,
    check_unsupported_shapes,
    check_roadmap,
    check_export_duration,
    check_import_size,
    check_rls,
    check_rls_visibility,
    check_change_capture,
    check_unknown_metric,
    check_vector_source,
    check_vector_space_type,
    check_dedicated_read_nodes,
    check_hybrid_readiness,
    check_pii,
)


def run_checks(ctx: FindingsContext, checks: Sequence[Check] = ALL_CHECKS) -> list[Finding]:
    """Every finding, blockers first."""
    order = {Severity.BLOCKER: 0, Severity.WARNING: 1, Severity.INFO: 2}
    findings = [f for check in checks for f in check(ctx)]
    return sorted(findings, key=lambda f: (order[f.severity], f.object, f.field or "", f.check))


def blockers(findings: Sequence[Finding]) -> list[Finding]:
    """Just the blockers."""
    return [f for f in findings if f.severity is Severity.BLOCKER]


def tables_with_dense(inventory: Inventory) -> list[ObjectInfo]:
    """Tables the dense wave can migrate."""
    return [t for t in inventory.tables if t.dense is not None]
