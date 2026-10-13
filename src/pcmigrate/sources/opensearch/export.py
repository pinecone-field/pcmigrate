"""OpenSearchSource: the SourceConnector for OpenSearch, plus ground truth for validation.

The production export runs through OSIS and Glue (ADR-009); this connector's ``export`` is
the direct path for laptop runs and tests, scanning one point-in-time slice per partition
and translating documents with the same ``DocumentTranslator`` the Glue job uses. Ground
truth for recall is exact kNN through ``script_score`` with the k-NN plugin's ``knn_score``
script and the space type that matches the mapped metric.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import Mapping as MappingDoc
from pcmigrate.config.mapping import ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord, JsonLinesPartWriter, Reject, SparseVector
from pcmigrate.sources.base import Change, Partition, RecordSink, SourceSchema
from pcmigrate.sources.opensearch.client import (
    ConnectionOptions,
    OpenSearchClient,
    is_backpressure,
    vector_page_size,
)
from pcmigrate.sources.opensearch.glue_mapping import DocumentTranslator
from pcmigrate.sources.opensearch.introspect import (
    METRIC_SPACE_TYPE,
    PIT_KEEP_ALIVE,
    IntrospectOptions,
    OpenSearchIntrospector,
)
from pcmigrate.sources.pgvector.records import RecordBuildError

if TYPE_CHECKING:
    from pcmigrate.sources.opensearch.change_feed import RescanFeed

PAGE_SIZE = 1000
ID_SCAN_PAGE = 10_000
PAINLESS_SCORE = {
    "cosinesimil": "cosineSimilarity(params.query_value, doc[params.field]) + 1.0",
    "l2": "1 / (1 + l2Squared(params.query_value, doc[params.field]))",
}


def slug(index: str) -> str:
    """A partition-id-safe form of an index name."""
    return re.sub(r"[^a-z0-9]+", "-", index.lower()).strip("-") or "index"


@dataclass(frozen=True, slots=True)
class IdTemplate:
    """``prefix{_id}suffix``: render and reverse the rendered id back to the document id."""

    prefix: str
    suffix: str

    @classmethod
    def parse(cls, template: str) -> IdTemplate:
        """Only templates built on ``_id`` can be reversed for fetches."""
        head, sep, tail = template.partition("{_id}")
        if not sep or "{" in head or "{" in tail:
            raise ValueError(f"id_template {template!r} must be prefix{{_id}}suffix for OpenSearch")
        return cls(head, tail)

    def render(self, doc_id: str) -> str:
        """Document id to record id."""
        return f"{self.prefix}{doc_id}{self.suffix}"

    def document_id(self, record_id: str) -> str:
        """Record id back to document id."""
        if not (record_id.startswith(self.prefix) and record_id.endswith(self.suffix)):
            raise ValueError(f"{record_id!r} does not match the id template")
        end = len(record_id) - len(self.suffix) if self.suffix else len(record_id)
        return record_id[len(self.prefix) : end]


@dataclass
class OpenSearchSource:
    """SourceConnector and GroundTruthSource for OpenSearch."""

    options: ConnectionOptions
    mapping: MappingDoc
    target_rows_per_partition: int = 2_000_000
    page_size: int = PAGE_SIZE
    bucket: str | None = None
    rejects_prefix: str | None = None
    vocabulary_prefix: str | None = None
    region: str | None = None
    rejected: dict[str, int] = field(default_factory=dict)
    vocabularies: dict[str, dict[str, int]] = field(default_factory=dict)
    _client: OpenSearchClient | None = field(default=None, repr=False)

    @property
    def kind(self) -> str:
        """Always ``opensearch``."""
        return "opensearch"

    @property
    def client(self) -> OpenSearchClient:
        """A shared read-only client; opensearch-py's pool is thread-safe."""
        if self._client is None:
            self._client = OpenSearchClient(self.options, read_only=True)
        return self._client

    def close(self) -> None:
        """Close the client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def object_for(self, index: str) -> ObjectMapping:
        """The mapping entry for *index*."""
        for obj in self.mapping.objects:
            if obj.source == index:
                return obj
        raise KeyError(f"{index} is not in the mapping")

    def object_for_namespace(self, namespace: str) -> ObjectMapping:
        """The mapping entry whose namespace is *namespace* (or its namespace_by prefix)."""
        base = namespace.split(":", 1)[0]
        for obj in self.mapping.objects:
            if obj.namespace == base:
                return obj
        raise KeyError(f"no object targets namespace {namespace!r}")

    def translator(self, obj: ObjectMapping) -> DocumentTranslator:
        """The translator for one object."""
        return DocumentTranslator(obj)

    def introspect(self) -> SourceSchema:
        """Structural schema of the mapped indices, with no workload window."""
        introspector = OpenSearchIntrospector(
            self.client,
            IntrospectOptions(
                indices=tuple(o.source for o in self.mapping.objects),
                workload_window_s=0,
                sample_docs=200,
                throughput_docs=0,
            ),
        )
        return introspector.introspect()

    def plan_partitions(self, mapping: MappingDoc) -> list[Partition]:
        """Point-in-time slices per index, about ``target_rows_per_partition`` documents each."""
        plan: list[Partition] = []
        for obj in mapping.objects:
            if obj.dense is None:
                continue
            count = self.client.count(obj.source)
            slices = max(1, -(-count // self.target_rows_per_partition))
            per_slice = -(-count // slices)
            for index in range(slices):
                plan.append(
                    Partition(
                        id=f"{slug(obj.source)}-s{index:04d}",
                        object=obj.source,
                        namespace=obj.namespace,
                        lower=str(index),
                        upper=str(slices),
                        estimated_rows=per_slice,
                    )
                )
        return plan

    def _reject_writer(self, partition: Partition) -> JsonLinesPartWriter | None:
        if self.bucket is None or self.rejects_prefix is None:
            return None
        from pcmigrate.core.s3 import make_client

        prefix = f"{self.rejects_prefix}/{partition.namespace}/direct/{partition.id}"
        return JsonLinesPartWriter(make_client(self.region or "us-east-1"), self.bucket, prefix)

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """Scan one slice through a point in time, translating as it goes."""
        obj = self.object_for(partition.object)
        translator = self.translator(obj)
        slice_id = int(partition.lower or 0)
        slice_max = int(partition.upper or 1)
        rejects = self._reject_writer(partition)
        count = 0
        rejected = 0
        page = min(self.page_size, vector_page_size(obj.dense.dimension if obj.dense else None))
        for hit in self._scan(
            obj.source, slice_id=slice_id, slice_max=slice_max, fields=None, page=page
        ):
            try:
                record = translator.translate(str(hit["_id"]), hit.get("_source") or {})
            except RecordBuildError as err:
                rejected += 1
                if rejects is not None:
                    rejects.write(
                        Reject(str(hit["_id"]), obj.namespace, str(hit["_id"]), str(err)).to_json()
                    )
                continue
            writer.write(record)
            count += 1
        if rejects is not None:
            rejects.close()
        self.rejected[partition.id] = rejected
        if translator.builder.vocabulary and self.bucket is not None and self.vocabulary_prefix:
            from pcmigrate.core.s3 import make_client

            make_client(self.region or "us-east-1").put_object(
                Bucket=self.bucket,
                Key=f"{self.vocabulary_prefix}/{obj.source}/{partition.id}.json",
                Body=json.dumps(translator.builder.vocabulary, sort_keys=True).encode("utf-8"),
            )
        return count

    def _scan(
        self,
        index: str,
        *,
        slice_id: int = 0,
        slice_max: int = 1,
        fields: Sequence[str] | None,
        page: int | None = None,
        query: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        pit = self.client.request(
            "POST", f"/{index}/_search/point_in_time", params={"keep_alive": PIT_KEEP_ALIVE}
        )
        pit_id = pit["pit_id"]
        search_after: list[Any] | None = None
        size = page or self.page_size
        try:
            while True:
                body: dict[str, Any] = {
                    "size": size,
                    "query": dict(query) if query else {"match_all": {}},
                    "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
                    "sort": [{"_doc": "asc"}],
                    "_source": (list(fields) or False) if fields is not None else True,
                }
                if slice_max > 1:
                    body["slice"] = {"id": slice_id, "max": slice_max}
                if search_after is not None:
                    body["search_after"] = search_after
                try:
                    hits: list[dict[str, Any]] = self.client.request("POST", "/_search", body=body)[
                        "hits"
                    ]["hits"]
                except Exception as err:
                    if not is_backpressure(err) or size <= 10:
                        raise
                    size = max(10, size // 2)
                    continue
                if not hits:
                    return
                yield from hits
                search_after = hits[-1]["sort"]
        finally:
            self.client.request("DELETE", "/_search/point_in_time", body={"pit_id": [pit_id]})

    def change_feed(self) -> Iterator[Change]:
        """Dual-writes reach the buffer directly; the optional rescan feed is polled by sync."""
        yield from ()

    def rescan_feed(self) -> RescanFeed | None:
        """The ``updated_at`` rescan feed when the mapping asks for one."""
        from pcmigrate.sources.opensearch.change_feed import make_feed

        return make_feed(self, self.mapping)

    def _ids(self, obj: ObjectMapping) -> IdTemplate:
        return IdTemplate.parse(obj.id_template)

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """Random record ids, deterministic for a seed."""
        obj = self.object_for_namespace(namespace)
        ids = self._ids(obj)
        hits = self.client.search(
            obj.source,
            {
                "size": min(count, 10_000),
                "_source": False,
                "query": {
                    "function_score": {
                        "query": {"match_all": {}},
                        "random_score": {"seed": seed, "field": "_seq_no"},
                    }
                },
            },
        )["hits"]["hits"]
        return [ids.render(str(h["_id"])) for h in hits]

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """Current records for *ids*, built through the mapping."""
        obj = self.object_for_namespace(namespace)
        template = self._ids(obj)
        translator = self.translator(obj)
        out: dict[str, CanonicalRecord] = {}
        wanted = list(ids)
        for start in range(0, len(wanted), 500):
            chunk = wanted[start : start + 500]
            docs = self.client.request(
                "POST",
                f"/{obj.source}/_mget",
                body={"ids": [template.document_id(i) for i in chunk]},
            )["docs"]
            for doc in docs:
                if not doc.get("found"):
                    continue
                try:
                    record = translator.translate(str(doc["_id"]), doc.get("_source") or {})
                except RecordBuildError:
                    continue
                out[record.id] = record
        return out

    def _filter_query(self, obj: ObjectMapping, flt: Mapping[str, Any] | None) -> dict[str, Any]:
        if not flt:
            return {"match_all": {}}
        clauses: list[dict[str, Any]] = []
        for key, cond in flt.items():
            column = self._source_field(obj, key)
            if isinstance(cond, dict) and "$eq" in cond:
                clauses.append({"term": {column: cond["$eq"]}})
            elif isinstance(cond, dict) and "$in" in cond:
                clauses.append({"terms": {column: list(cond["$in"])}})
            else:
                clauses.append({"term": {column: cond}})
        return {"bool": {"filter": clauses}}

    @staticmethod
    def _source_field(obj: ObjectMapping, target: str) -> str:
        for spec in obj.metadata:
            if spec.action == "map" and (spec.target or spec.source) == target:
                return spec.source
        return target

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact nearest neighbours by the mapped metric via script_score."""
        obj = self.object_for_namespace(namespace)
        assert obj.dense is not None
        space_type = METRIC_SPACE_TYPE[obj.dense.metric.value]
        template = self._ids(obj)
        params = {"field": obj.dense.column, "query_value": list(vector), "space_type": space_type}
        scripts: list[dict[str, Any]] = [{"source": "knn_score", "lang": "knn", "params": params}]
        if space_type in PAINLESS_SCORE:
            scripts.append(
                {"source": PAINLESS_SCORE[space_type], "lang": "painless", "params": params}
            )
        last_error: Exception | None = None
        for script in scripts:
            body = {
                "size": k,
                "_source": False,
                "query": {
                    "script_score": {"query": self._filter_query(obj, filter), "script": script}
                },
            }
            try:
                hits = self.client.search(obj.source, body)["hits"]["hits"]
            except Exception as err:
                last_error = err
                continue
            return [template.render(str(h["_id"])) for h in hits]
        raise RuntimeError(f"exact kNN failed for {obj.source}: {last_error}")

    def knn_sparse(
        self,
        namespace: str,
        sparse: SparseVector,
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact sparse inner product through rank_feature clauses with linear scoring.

        Each query index is turned back into its token through the recorded vocabulary and
        scored as ``boost x stored weight``; the bool ``should`` sum is the dot product.
        """
        obj = self.object_for_namespace(namespace)
        if obj.sparse is None:
            raise RuntimeError(f"{obj.source} maps no sparse field")
        template = self._ids(obj)
        tokens = self.vocabulary_for(obj.source)
        by_index = {index: token for token, index in tokens.items()}
        clauses: list[dict[str, Any]] = []
        for index, weight in zip(sparse.indices, sparse.values, strict=True):
            token = by_index.get(index)
            if token is None:
                continue
            clauses.append(
                {
                    "rank_feature": {
                        "field": f"{obj.sparse.column}.{token}",
                        "boost": float(weight),
                        "linear": {},
                    }
                }
            )
        if not clauses:
            return []
        query: dict[str, Any] = {"bool": {"should": clauses, "minimum_should_match": 1}}
        if filter:
            query["bool"]["filter"] = self._filter_query(obj, filter)["bool"]["filter"]
        hits = self.client.search(obj.source, {"size": k, "_source": False, "query": query})[
            "hits"
        ]["hits"]
        return [template.render(str(h["_id"])) for h in hits]

    def vocabulary_for(self, index: str) -> dict[str, int]:
        """Token-to-index mapping for *index*: the attached one, else rebuilt from samples."""
        cached = self.vocabularies.get(index)
        if cached is not None:
            return cached
        from pcmigrate.core.sparse import token_index

        obj = self.object_for(index)
        if obj.sparse is None or obj.sparse.column is None:
            return {}
        column = obj.sparse.column
        vocabulary: dict[str, int] = {}
        for hit in self._scan(index, fields=[column], page=1000):
            value = (hit.get("_source") or {}).get(column)
            if isinstance(value, dict):
                for token in value:
                    vocabulary[str(token)] = token_index(str(token))
        self.vocabularies[index] = vocabulary
        return vocabulary

    def all_ids(self, namespace: str) -> Iterator[str]:
        """Every record id in the namespace."""
        obj = self.object_for_namespace(namespace)
        template = self._ids(obj)
        for hit in self._scan(obj.source, fields=[], page=ID_SCAN_PAGE):
            yield template.render(str(hit["_id"]))

    def common_filters(self, namespace: str, count: int) -> list[dict[str, Any]]:
        """Most frequent values of low-cardinality mapped string fields, via terms aggregations."""
        obj = self.object_for_namespace(namespace)
        candidates = [
            f
            for f in obj.metadata
            if f.action == "map" and f.type is not None and f.type.value == "string"
        ]
        ranked: list[tuple[int, str, list[Any]]] = []
        for spec in candidates:
            try:
                result = self.client.search(
                    obj.source,
                    {
                        "size": 0,
                        "aggs": {
                            "top": {"terms": {"field": spec.source, "size": 5, "min_doc_count": 2}}
                        },
                    },
                )
            except Exception:
                continue
            buckets = [
                b
                for b in result.get("aggregations", {}).get("top", {}).get("buckets", [])
                if len(str(b.get("key", ""))) <= 200
            ]
            if buckets:
                ranked.append(
                    (
                        -int(buckets[0]["doc_count"]),
                        spec.target or spec.source,
                        [b["key"] for b in buckets],
                    )
                )
        ranked.sort(key=lambda item: (item[0], item[1]))
        filters: list[dict[str, Any]] = []
        for _, target, values in ranked:
            for value in values:
                filters.append({target: {"$eq": value}})
                if len(filters) >= count:
                    return filters
        return filters
