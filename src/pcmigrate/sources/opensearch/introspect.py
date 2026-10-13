"""OpenSearch discovery: indices, mappings, knn_vector fields, _source vector presence, workload.

Read-only by construction: every request goes through the audited client, which refuses
anything but reads. Discovery reads ``/``, ``_cat/plugins``, ``_cat/indices``, ``_alias``,
``_mapping``, ``_settings``, ``_count``, sampled ``_search`` hits, ``_stats`` and
``_nodes/stats`` deltas over the workload window, and the security plugin's roles when it
answers. The result is the source-neutral ``Inventory`` the findings engine and report use.
"""

from __future__ import annotations

import fnmatch
import json
import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pcmigrate.config.mapping import Mapping
from pcmigrate.core.canonical import vector_hash
from pcmigrate.report import limits
from pcmigrate.sources.base import Change, Partition, RecordSink, SourceSchema
from pcmigrate.sources.inventory import (
    ColumnInfo,
    DuplicateEstimate,
    Inventory,
    MetricGuess,
    ObjectInfo,
    RlsClass,
    RlsPolicy,
    TextSearchField,
    ThroughputSample,
    VectorField,
    VectorKind,
    WorkloadSample,
)
from pcmigrate.sources.opensearch.client import (
    OpenSearchClient,
    is_backpressure,
    vector_page_size,
)

SPACE_TYPE_METRIC: dict[str, MetricGuess] = {
    "l2": MetricGuess.EUCLIDEAN,
    "cosinesimil": MetricGuess.COSINE,
    "innerproduct": MetricGuess.DOTPRODUCT,
    "l1": MetricGuess.UNSUPPORTED,
    "linf": MetricGuess.UNSUPPORTED,
    "hamming": MetricGuess.UNSUPPORTED,
    "hammingbit": MetricGuess.UNSUPPORTED,
}
METRIC_SPACE_TYPE = {
    "euclidean": "l2",
    "cosine": "cosinesimil",
    "dotproduct": "innerproduct",
}
DEFAULT_SPACE_TYPE = "l2"
TEXT_TYPES = frozenset({"text", "match_only_text", "search_as_you_type"})
SPARSE_TYPES = frozenset({"rank_features", "rank_feature", "sparse_vector"})
STRUCTURED_TYPES = frozenset({"object", "nested"})
SKIPPED_TYPES = frozenset({"alias"})
EXACT_DUPLICATES_MAX_DOCS = 100_000
SAMPLE_VALUES = 3
SCAN_PAGE = 5000
PIT_KEEP_ALIVE = "10m"


@dataclass(frozen=True, slots=True)
class IntrospectOptions:
    """Knobs for one introspection run."""

    indices: tuple[str, ...] | None = None
    sample_docs: int = 1000
    workload_window_s: float = 300.0
    full_scan: bool = False
    throughput_docs: int = 2000
    include_system: bool = False


def metric_from_space_type(space_type: str | None) -> MetricGuess:
    """l2 -> euclidean, cosinesimil -> cosine, innerproduct -> dotproduct."""
    if space_type is None:
        return MetricGuess.UNKNOWN
    return SPACE_TYPE_METRIC.get(space_type, MetricGuess.UNSUPPORTED)


def source_setting_for(
    field: str, source_meta: dict[str, Any], index_settings: dict[str, Any]
) -> str | None:
    """The mapping or index setting that keeps *field* out of ``_source``, if any."""
    if source_meta.get("enabled") is False:
        return "_source.enabled: false"
    excludes = source_meta.get("excludes") or []
    for pattern in excludes:
        if fnmatch.fnmatchcase(field, str(pattern)):
            return f"_source.excludes: {pattern}"
    includes = source_meta.get("includes") or []
    if includes and not any(fnmatch.fnmatchcase(field, str(p)) for p in includes):
        return f"_source.includes: {includes}"
    if _truthy(index_settings.get("index.knn.derived_source.enabled")):
        return "index.knn.derived_source.enabled: true"
    return None


def _truthy(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _flatten_settings(settings: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in settings.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(_flatten_settings(value, path))
        else:
            out[path] = value
    return out


def _render_sample(value: Any) -> str:
    text = (
        json.dumps(value, default=str, ensure_ascii=False)
        if isinstance(value, dict | list)
        else str(value)
    )
    return text if len(text) <= 80 else text[:77] + "..."


def _json_depth(value: Any) -> int:
    if isinstance(value, dict):
        return 1 + max((_json_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_json_depth(v) for v in value), default=0)
    return 0


def _dig(doc: Any, path: str) -> Any:
    for key in path.split("."):
        if isinstance(doc, list):
            doc = [d.get(key) for d in doc if isinstance(d, dict)]
            if not doc:
                return None
            continue
        if not isinstance(doc, dict) or key not in doc:
            return None
        doc = doc[key]
    return doc


@dataclass(frozen=True, slots=True)
class _FieldSpec:
    path: str
    mapping: dict[str, Any]
    parent_nested: bool


class OpenSearchIntrospector:
    """Read-only discovery against one OpenSearch cluster or Amazon OpenSearch Service domain."""

    def __init__(
        self,
        client: OpenSearchClient,
        options: IntrospectOptions | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._opts = options or IntrospectOptions()
        self._sleep = sleep
        self._clock = clock

    @property
    def kind(self) -> str:
        """Always ``opensearch``."""
        return "opensearch"

    @property
    def audit(self) -> Any:
        """The client's request audit."""
        return self._client.audit

    def close(self) -> None:
        """Close the client."""
        self._client.close()

    def introspect(self) -> SourceSchema:
        """SourceConnector entry point: structural schema for plan.lock."""
        return self.inventory().to_source_schema()

    def plan_partitions(self, mapping: Mapping) -> list[Partition]:
        """The export connector plans partitions; the introspector only reads."""
        raise NotImplementedError("use OpenSearchSource for export")

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """The export connector exports; the introspector only reads."""
        raise NotImplementedError("use OpenSearchSource for export")

    def change_feed(self) -> Iterator[Change]:
        """The export connector owns the change feed."""
        raise NotImplementedError("use OpenSearchSource for change capture")

    def inventory(self) -> Inventory:
        """Run every discovery step and return the inventory."""
        root = self._client.get("/")
        version = str(root.get("version", {}).get("number", ""))
        distribution = str(root.get("version", {}).get("distribution", "opensearch"))
        plugins = self._plugins()
        security = self._security_roles()
        names = self._select_indices()
        aliases = self._aliases()
        tables = [self._describe_index(name, aliases.get(name, []), security) for name in names]
        before = self._workload_snapshot(names)
        if self._opts.workload_window_s > 0 and tables:
            self._sleep(self._opts.workload_window_s)
        after = self._workload_snapshot(names)
        cluster_workload: dict[str, Any] = {}
        for table in tables:
            table.workload = self._workload_delta(table.name, before, after)
        if before.get("_cluster") and after.get("_cluster"):
            cluster_workload = self._cluster_delta(before["_cluster"], after["_cluster"])
        return Inventory(
            kind="opensearch",
            server_version=version,
            sampled_at=datetime.now(UTC),
            read_only_session=self._client.read_only,
            tables=tables,
            plugin_versions=plugins,
            cluster={
                "distribution": distribution,
                "security_plugin": security is not None,
                "workload": cluster_workload,
                "endpoint_is_aws": self._client.options.is_aws,
            },
        )

    def _plugins(self) -> dict[str, str]:
        try:
            rows = self._client.get("/_cat/plugins", format="json")
        except Exception:
            return {}
        out: dict[str, str] = {}
        for row in rows or []:
            component = str(row.get("component", ""))
            if component:
                out[component] = str(row.get("version", ""))
        return out

    def _select_indices(self) -> list[str]:
        rows = self._client.get("/_cat/indices", format="json", h="index,status", s="index")
        names = [str(r["index"]) for r in rows or [] if str(r.get("status", "open")) == "open"]
        if not self._opts.include_system:
            names = [n for n in names if not n.startswith(".")]
        wanted = self._opts.indices
        if wanted is None:
            return names
        aliases = self._aliases()
        by_alias: dict[str, list[str]] = {}
        for index, alias_names in aliases.items():
            for alias in alias_names:
                by_alias.setdefault(alias, []).append(index)
        selected: list[str] = []
        for pattern in wanted:
            matches = [n for n in names if fnmatch.fnmatchcase(n, pattern)]
            matches += [n for n in by_alias.get(pattern, []) if n in names]
            for name in matches:
                if name not in selected:
                    selected.append(name)
        missing = [
            p
            for p in wanted
            if not any(fnmatch.fnmatchcase(n, p) for n in selected) and p not in by_alias
        ]
        if missing:
            raise LookupError(f"no open index or alias matches {missing}")
        return selected

    def _aliases(self) -> dict[str, list[str]]:
        try:
            data = self._client.get("/_alias")
        except Exception:
            return {}
        return {
            str(index): sorted(str(a) for a in (entry.get("aliases") or {}))
            for index, entry in (data or {}).items()
        }

    def _describe_index(
        self, name: str, aliases: list[str], security: list[dict[str, Any]] | None
    ) -> ObjectInfo:
        mapping = self._client.get(f"/{name}/_mapping")[name]["mappings"]
        raw_settings = self._client.get(f"/{name}/_settings")[name]["settings"]
        settings = _flatten_settings(raw_settings)
        cat = self._client.get(
            "/_cat/indices/" + name, format="json", bytes="b", h="docs.count,store.size,pri"
        )
        row = cat[0] if cat else {}
        table = ObjectInfo(
            schema_name="",
            name=name,
            relkind="i",
            aliases=aliases,
            shards=int(row.get("pri") or settings.get("index.number_of_shards") or 0) or None,
            row_estimate=int(row.get("docs.count") or 0),
            total_bytes=int(row.get("store.size") or 0),
            primary_key=["_id"],
        )
        table.exact_rows = self._client.count(name)
        source_meta = mapping.get("_source") or {}
        analysis = (raw_settings.get("index") or {}).get("analysis") or {}
        table.settings = {
            "knn": _truthy(settings.get("index.knn")),
            "derived_source": _truthy(settings.get("index.knn.derived_source.enabled")),
            "source_enabled": source_meta.get("enabled", True) is not False,
            "source_excludes": list(source_meta.get("excludes") or []),
            "source_includes": list(source_meta.get("includes") or []),
        }
        if analysis:
            table.settings["analysis"] = analysis
        specs = list(_walk_properties(mapping.get("properties") or {}, "", False))
        position = 0
        for spec in specs:
            kind = str(spec.mapping.get("type", "object"))
            if kind in SKIPPED_TYPES:
                continue
            if kind == "knn_vector":
                table.vectors.append(self._vector_field(spec, source_meta, settings))
                continue
            if kind in SPARSE_TYPES:
                table.vectors.append(
                    VectorField(
                        column=spec.path,
                        kind=VectorKind.RANK_FEATURES,
                        dimension=None,
                        nullable=True,
                    )
                )
                continue
            position += 1
            if kind in TEXT_TYPES:
                table.text_search.append(
                    TextSearchField(
                        column=spec.path,
                        kind=kind,
                        analyzer=spec.mapping.get("analyzer"),
                        search_analyzer=spec.mapping.get("search_analyzer"),
                        multi_fields={
                            str(sub): str((sub_mapping or {}).get("type", "keyword"))
                            for sub, sub_mapping in (spec.mapping.get("fields") or {}).items()
                        },
                        mapping={
                            k: v
                            for k, v in spec.mapping.items()
                            if k not in ("type", "analyzer", "search_analyzer", "fields")
                        },
                    )
                )
            type_text = kind
            if kind == "date" and spec.mapping.get("format"):
                type_text = f"date ({spec.mapping['format']})"
            if spec.parent_nested:
                type_text = f"{kind} (in nested)"
            table.columns.append(
                ColumnInfo(
                    name=spec.path,
                    type=type_text,
                    base_type=kind,
                    nullable=True,
                    position=position,
                    analyzer=spec.mapping.get("analyzer"),
                )
            )
        self._sample(table)
        table.policies = _policies_for(name, security or [])
        table.rls_enabled = bool(table.policies)
        table.throughput = self._throughput(table)
        return table

    def _vector_field(
        self, spec: _FieldSpec, source_meta: dict[str, Any], settings: dict[str, Any]
    ) -> VectorField:
        method = spec.mapping.get("method") or {}
        space_type = spec.mapping.get("space_type") or method.get("space_type")
        model_id = spec.mapping.get("model_id")
        if space_type is None and model_id is None:
            space_type = DEFAULT_SPACE_TYPE
        engine = method.get("engine")
        metric = (
            metric_from_space_type(space_type)
            if model_id is None or space_type
            else MetricGuess.UNKNOWN
        )
        dimension = spec.mapping.get("dimension")
        return VectorField(
            column=spec.path,
            kind=VectorKind.KNN_VECTOR,
            dimension=int(dimension) if dimension is not None else None,
            nullable=True,
            metric=metric,
            metric_index=str(method.get("name"))
            if method.get("name")
            else (f"model {model_id}" if model_id else None),
            index_method=str(engine) if engine else None,
            space_type=str(space_type) if space_type else None,
            engine=str(engine) if engine else None,
            data_type=str(spec.mapping.get("data_type", "float")),
            source_setting=source_setting_for(spec.path, source_meta, settings),
        )

    def _sample_hits(
        self, index: str, size: int, *, dimension: int | None, fields: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        """A random sample, fetched in vector-sized pages so no single search is heavy."""
        if size <= 0:
            return []
        size = min(size, 10_000)
        page = vector_page_size(dimension)
        hits: list[dict[str, Any]] = []
        search_after: list[Any] | None = None
        while len(hits) < size:
            body: dict[str, Any] = {
                "size": min(page, size - len(hits)),
                "query": {
                    "function_score": {
                        "query": {"match_all": {}},
                        "random_score": {"seed": 42, "field": "_seq_no"},
                    }
                },
                "sort": ["_score", {"_id": "asc"}],
                "_source": list(fields) if fields else True,
            }
            if search_after is not None:
                body["search_after"] = search_after
            batch: list[dict[str, Any]] = self._client.search(index, body)["hits"]["hits"]
            if not batch:
                break
            hits.extend(batch)
            search_after = batch[-1]["sort"]
        return hits

    def _sample(self, table: ObjectInfo) -> None:
        widest = max((v.dimension or 0 for v in table.vectors), default=0) or None
        hits = self._sample_hits(table.name, self._opts.sample_docs, dimension=widest)
        docs = [h.get("_source") or {} for h in hits]
        for column in table.columns:
            values = [_dig(d, column.name) for d in docs]
            present = [v for v in values if v is not None]
            column.null_frac = (1 - len(present) / len(values)) if values else None
            if present:
                column.avg_width = round(
                    sum(len(json.dumps(v, default=str)) for v in present) / len(present)
                )
                column.sample_values = [_render_sample(v) for v in present[:SAMPLE_VALUES]]
                if column.base_type in STRUCTURED_TYPES:
                    column.sample_json_depth = max(_json_depth(v) for v in present)
                elif any(isinstance(v, list) for v in present):
                    column.base_type = f"{column.base_type}_array"
                    column.type = f"{column.type}[]"
        for vector in table.vectors:
            if vector.kind is not VectorKind.KNN_VECTOR:
                continue
            values = [_dig(d, vector.column) for d in docs]
            vectors = [v for v in values if isinstance(v, list)]
            vector.sampled_rows = len(values)
            vector.null_rows = len(values) - len(vectors)
            vector.distinct_dimensions = sorted({len(v) for v in vectors})
            if values:
                vector.in_source = bool(vectors)
            if vector.in_source is False and vector.source_setting is None:
                vector.source_setting = (
                    "vector missing from sampled _source (no setting identified)"
                )
            table.duplicates[vector.column] = self._duplicates(table, vector, vectors)

    def _duplicates(
        self, table: ObjectInfo, vector: VectorField, sampled: list[list[float]]
    ) -> DuplicateEstimate:
        if vector.in_source is False:
            return DuplicateEstimate(method="skipped", sampled_rows=0, duplicate_rows_in_sample=0)
        if self._opts.full_scan or table.rows <= EXACT_DUPLICATES_MAX_DOCS:
            counts: dict[int, int] = {}
            total = 0
            for doc in self.scan(
                table.name, fields=[vector.column], page=vector_page_size(vector.dimension)
            ):
                value = _dig(doc.get("_source") or {}, vector.column)
                if isinstance(value, list):
                    total += 1
                    key = vector_hash([float(x) for x in value])
                    counts[key] = counts.get(key, 0) + 1
            groups = sum(1 for c in counts.values() if c > 1)
            extra = sum(c - 1 for c in counts.values() if c > 1)
            return DuplicateEstimate(
                method="exact",
                sampled_rows=total,
                duplicate_rows_in_sample=extra,
                duplicate_groups=groups,
                estimated_duplicate_rows=extra,
            )
        hashes = [vector_hash([float(x) for x in v]) for v in sampled]
        extra = len(hashes) - len(set(hashes))
        scale = table.rows / len(hashes) if hashes else 0.0
        return DuplicateEstimate(
            method="sampled",
            sampled_rows=len(hashes),
            duplicate_rows_in_sample=extra,
            estimated_duplicate_rows=round(extra * scale),
        )

    def scan(
        self, index: str, *, fields: Sequence[str] | None = None, page: int = SCAN_PAGE
    ) -> Iterator[dict[str, Any]]:
        """Every document (hit) of *index* through a point in time, cheapest fields first."""
        pit = self._client.request(
            "POST", f"/{index}/_search/point_in_time", params={"keep_alive": PIT_KEEP_ALIVE}
        )
        pit_id = pit["pit_id"]
        search_after: list[Any] | None = None
        try:
            while True:
                body: dict[str, Any] = {
                    "size": page,
                    "query": {"match_all": {}},
                    "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
                    "sort": [{"_doc": "asc"}],
                    "_source": (list(fields) or False) if fields is not None else False,
                }
                if search_after is not None:
                    body["search_after"] = search_after
                hits: list[dict[str, Any]] = self._client.request("POST", "/_search", body=body)[
                    "hits"
                ]["hits"]
                if not hits:
                    return
                yield from hits
                search_after = hits[-1]["sort"]
        finally:
            self._client.request("DELETE", "/_search/point_in_time", body={"pit_id": [pit_id]})

    def _throughput(self, table: ObjectInfo) -> ThroughputSample | None:
        dense = table.dense
        if dense is None or table.rows == 0 or dense.in_source is False:
            return None
        limit = min(self._opts.throughput_docs, table.rows, 10_000)
        if limit <= 0:
            return None
        started = self._clock()
        rows = 0
        size = 0
        try:
            for hit in self.scan(
                table.name, fields=[dense.column], page=vector_page_size(dense.dimension)
            ):
                rows += 1
                size += len(json.dumps(hit.get("_source") or {}))
                if rows >= limit:
                    break
        except Exception as err:
            if not is_backpressure(err):
                raise
            return None
        elapsed = self._clock() - started
        return ThroughputSample(rows=rows, seconds=elapsed, bytes=size)

    def _workload_snapshot(self, names: list[str]) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        if not names:
            return out
        stats = self._client.get("/" + ",".join(names) + "/_stats/indexing,search")
        now = self._clock()
        for name, entry in (stats.get("indices") or {}).items():
            total = entry.get("total") or {}
            indexing = total.get("indexing") or {}
            search = total.get("search") or {}
            out[str(name)] = {
                "index_total": float(indexing.get("index_total", 0)),
                "delete_total": float(indexing.get("delete_total", 0)),
                "query_total": float(search.get("query_total", 0)),
                "at": now,
            }
        try:
            nodes = self._client.get("/_nodes/stats/indices/indexing,search")
        except Exception:
            return out
        index_total = delete_total = query_total = 0.0
        for node in (nodes.get("nodes") or {}).values():
            indices = node.get("indices") or {}
            index_total += float((indices.get("indexing") or {}).get("index_total", 0))
            delete_total += float((indices.get("indexing") or {}).get("delete_total", 0))
            query_total += float((indices.get("search") or {}).get("query_total", 0))
        out["_cluster"] = {
            "index_total": index_total,
            "delete_total": delete_total,
            "query_total": query_total,
            "at": now,
        }
        return out

    def _workload_delta(
        self, name: str, before: dict[str, dict[str, float]], after: dict[str, dict[str, float]]
    ) -> WorkloadSample:
        b = before.get(name)
        a = after.get(name)
        if not b or not a or self._opts.workload_window_s <= 0:
            return WorkloadSample(window_s=0.0, measured=False, statements_available=True)
        window = max(0.0, a["at"] - b["at"])
        if window <= 0:
            return WorkloadSample(window_s=0.0, measured=False, statements_available=True)
        per_min = 60.0 / window
        queries = a["query_total"] - b["query_total"]
        return WorkloadSample(
            window_s=window,
            measured=True,
            inserts_per_min=(a["index_total"] - b["index_total"]) * per_min,
            updates_per_min=0.0,
            deletes_per_min=(a["delete_total"] - b["delete_total"]) * per_min,
            idx_scans_per_min=queries * per_min,
            read_qps=queries / window,
            statements_available=True,
        )

    @staticmethod
    def _cluster_delta(b: dict[str, float], a: dict[str, float]) -> dict[str, float]:
        window = max(0.0, a["at"] - b["at"])
        if window <= 0:
            return {}
        return {
            "window_s": window,
            "index_ops_per_min": (a["index_total"] - b["index_total"]) * 60 / window,
            "deletes_per_min": (a["delete_total"] - b["delete_total"]) * 60 / window,
            "read_qps": (a["query_total"] - b["query_total"]) / window,
        }

    def _security_roles(self) -> list[dict[str, Any]] | None:
        try:
            roles = self._client.get("/_plugins/_security/api/roles")
        except Exception:
            return None
        if not isinstance(roles, dict):
            return None
        out: list[dict[str, Any]] = []
        for name, role in roles.items():
            for perm in role.get("index_permissions") or []:
                if perm.get("dls") or perm.get("fls"):
                    out.append(
                        {
                            "role": str(name),
                            "patterns": [str(p) for p in perm.get("index_patterns") or []],
                            "dls": perm.get("dls"),
                            "fls": perm.get("fls"),
                        }
                    )
        return out


def _walk_properties(
    properties: dict[str, Any], prefix: str, parent_nested: bool
) -> Iterator[_FieldSpec]:
    for name, mapping in properties.items():
        path = f"{prefix}.{name}" if prefix else str(name)
        if not isinstance(mapping, dict):
            continue
        kind = str(mapping.get("type", "object"))
        if kind == "nested":
            yield _FieldSpec(
                path,
                {"type": "nested", "properties": mapping.get("properties") or {}},
                parent_nested,
            )
            continue
        if kind == "object" and "properties" in mapping:
            if "." in path and path.count(".") + 1 >= limits.JSON_FLATTEN_MAX_DEPTH:
                yield _FieldSpec(
                    path, {"type": "object", "properties": mapping["properties"]}, parent_nested
                )
                continue
            yield from _walk_properties(mapping["properties"], path, parent_nested)
            continue
        yield _FieldSpec(path, mapping, parent_nested)


_PARAM = re.compile(r"\$\{[^}]+\}")


def _policies_for(index: str, security: list[dict[str, Any]]) -> list[RlsPolicy]:
    policies: list[RlsPolicy] = []
    for entry in security:
        if not any(fnmatch.fnmatchcase(index, p) for p in entry["patterns"]):
            continue
        dls = entry.get("dls")
        fls = entry.get("fls")
        classification, column, note = (
            _classify_dls(dls)
            if dls
            else (
                RlsClass.DOES_NOT_MAP,
                None,
                "field-level security hides fields; drop them in mapping.yaml or enforce them "
                "in the application",
            )
        )
        policies.append(
            RlsPolicy(
                name=entry["role"],
                cmd="READ",
                roles=[entry["role"]],
                qual=str(dls) if dls else None,
                with_check=json.dumps(fls) if fls else None,
                classification=classification,
                column=column,
                note=note,
            )
        )
    return policies


def _classify_dls(dls: str) -> tuple[RlsClass, str | None, str]:
    try:
        query = json.loads(dls)
    except ValueError:
        return RlsClass.DOES_NOT_MAP, None, "document-level security query is not JSON"
    inner = query
    for wrapper in ("bool", "filter", "must"):
        if isinstance(inner, dict) and wrapper in inner:
            inner = inner[wrapper]
            if isinstance(inner, list) and len(inner) == 1:
                inner = inner[0]
    if isinstance(inner, dict) and len(inner) == 1 and next(iter(inner)) in ("term", "match"):
        clause = next(iter(inner.values()))
        if isinstance(clause, dict) and len(clause) == 1:
            column, value = next(iter(clause.items()))
            raw = value.get("value", value) if isinstance(value, dict) else value
            if isinstance(raw, str) and _PARAM.search(raw):
                return (
                    RlsClass.NAMESPACE,
                    str(column),
                    f"one namespace per {column} (value from {raw})",
                )
            return RlsClass.FILTER, str(column), f"server-side metadata filter {column} == {raw!r}"
    return RlsClass.DOES_NOT_MAP, None, "document-level security query is more than one equality"
