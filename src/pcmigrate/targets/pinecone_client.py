"""The one place the Pinecone SDK is imported. Pinned to API version 2026-07 (ADR-001).

Everything else in targets/ talks to a PineconeGateway, so unit tests substitute a fake.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import boto3
import pinecone

from pcmigrate.config.mapping import PINECONE_API_VERSION
from pcmigrate.core.canonical import SparseVector

TERMINAL_IMPORT_STATES = frozenset({"Completed", "Failed", "Cancelled"})
DOCUMENT_UPSERT_MAX_DOCS = 200
DOCUMENT_UPSERT_MAX_BYTES = 1_500_000
"""One Documents API upsert request: the data plane rejects bodies over 2 MB (413), and a
1024-dim document is about 24 KB of JSON, so requests are cut by serialized size too."""


def document_batches(
    docs: Sequence[dict[str, Any]],
    *,
    max_docs: int = DOCUMENT_UPSERT_MAX_DOCS,
    max_bytes: int = DOCUMENT_UPSERT_MAX_BYTES,
) -> Iterator[list[dict[str, Any]]]:
    """Split documents into request-sized batches by count and serialized JSON size."""
    batch: list[dict[str, Any]] = []
    size = 0
    for doc in docs:
        doc_bytes = len(json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()) + 1
        if batch and (len(batch) >= max_docs or size + doc_bytes > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(doc)
        size += doc_bytes
    if batch:
        yield batch


@dataclass(frozen=True, slots=True)
class ReadCapacity:
    """The index's read capacity as the control plane reports it."""

    mode: str
    node_type: str | None = None
    shards: int | None = None
    replicas: int | None = None
    state: str | None = None
    error: str | None = None

    @property
    def dedicated(self) -> bool:
        """True for dedicated read nodes."""
        return self.mode.lower() == "dedicated"

    @property
    def settled(self) -> bool:
        """True once the control plane reports the capacity ready (or reports no state)."""
        return self.state in (None, "Ready")

    def matches(self, wanted: Mapping[str, Any] | None) -> bool:
        """True when *wanted* (a ``read_capacity`` body, None for on-demand) describes this."""
        if wanted is None or str(wanted.get("mode", "")).lower() == "ondemand":
            return not self.dedicated
        manual = (wanted.get("dedicated") or {}).get("manual") or {}
        return (
            self.dedicated
            and self.node_type == (wanted.get("dedicated") or {}).get("node_type")
            and self.shards == manual.get("shards")
            and self.replicas == manual.get("replicas")
        )


@dataclass(frozen=True, slots=True)
class DocumentSchema:
    """The searchable fields of a document index, as the control plane reports them.

    ``full_text`` holds each text field's resolved ``full_text_search`` object (language,
    stemming, stop_words, ngram) when the control plane reported it.
    """

    dense_field: str | None
    dimension: int | None
    metric: str | None
    sparse_field: str | None
    text_fields: tuple[str, ...] = ()
    full_text: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IndexInfo:
    """What the tool needs to know about an index."""

    name: str
    host: str
    dimension: int | None
    metric: str | None
    cloud: str | None
    region: str | None
    ready: bool
    read_capacity: ReadCapacity | None = None
    schema: DocumentSchema | None = None

    @property
    def document(self) -> bool:
        """True for an index with a document schema."""
        return self.schema is not None


@dataclass(frozen=True, slots=True)
class ImportInfo:
    """A snapshot of one bulk import."""

    id: str
    status: str
    percent_complete: float | None = None
    records_imported: int | None = None
    error: str | None = None

    @property
    def terminal(self) -> bool:
        """True for Completed, Failed, and Cancelled."""
        return self.status in TERMINAL_IMPORT_STATES


@dataclass(frozen=True, slots=True)
class VectorRecord:
    """One record as the data plane sees it.

    ``sparse`` and ``text_fields`` (keyed by schema field name) are set on document indexes.
    """

    id: str
    values: list[float]
    metadata: dict[str, Any] = field(default_factory=dict)
    sparse: SparseVector | None = None
    text_fields: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Match:
    """One query hit."""

    id: str
    score: float


class PineconeGateway(Protocol):
    """The subset of the Pinecone API the shared stages use."""

    def describe_index(self, name: str) -> IndexInfo | None:
        """The index, or None when it does not exist."""
        ...

    def create_index(
        self,
        name: str,
        *,
        dimension: int,
        metric: str,
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
        read_capacity: Mapping[str, Any] | None = None,
    ) -> IndexInfo:
        """Create a serverless dense vector index and wait until it is ready.

        ``read_capacity`` is the control-plane body for dedicated read nodes; None creates an
        on-demand index.
        """
        ...

    def create_document_index(
        self,
        name: str,
        *,
        fields: Mapping[str, Mapping[str, Any]],
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
        read_capacity: Mapping[str, Any] | None = None,
    ) -> IndexInfo:
        """Create a serverless index with a document schema and wait until it is ready.

        ``fields`` is the schema's field map: ``{"dense": {"type": "dense_vector",
        "dimension": 768, "metric": "cosine"}, "sparse": {"type": "sparse_vector"}}``.
        """
        ...

    def delete_index(self, name: str) -> None:
        """Delete the index. Only tests and prepare_index rollback call this."""
        ...

    def namespace_counts(self, host: str) -> dict[str, int]:
        """Record count per namespace from index stats."""
        ...

    def list_namespaces(self, host: str) -> list[str]:
        """Every namespace name the index currently has."""
        ...

    def start_import(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        """Start a bulk import with error_mode=abort; returns the import id."""
        ...

    def describe_import(self, host: str, import_id: str) -> ImportInfo:
        """Current state of one import."""
        ...

    def upsert(self, host: str, namespace: str, records: Sequence[VectorRecord]) -> int:
        """Upsert records through the data API; returns the count accepted."""
        ...

    def fetch(self, host: str, namespace: str, ids: Sequence[str]) -> dict[str, VectorRecord]:
        """Fetch records by id; absent ids are missing from the result."""
        ...

    def delete(self, host: str, namespace: str, ids: Sequence[str]) -> int:
        """Delete records by id; returns how many ids were sent."""
        ...

    def query(
        self,
        host: str,
        namespace: str,
        vector: Sequence[float],
        *,
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """Nearest neighbours by the index metric."""
        ...

    def query_sparse(
        self,
        host: str,
        namespace: str,
        sparse: SparseVector,
        *,
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """Nearest neighbours by sparse inner product; document indexes only."""
        ...

    def query_text(
        self,
        host: str,
        namespace: str,
        query: str,
        *,
        fields: Sequence[str],
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """BM25 over full-text fields (one ``text`` clause, token OR); document indexes only.

        ``filter`` may carry text-match operators (``$match_phrase``, ``$match_all``,
        ``$match_any``) next to metadata conditions.
        """
        ...

    def list_ids(self, host: str, namespace: str) -> Iterator[str]:
        """Every id in the namespace, following pagination."""
        ...


def api_key_from_secret(secret_arn: str, region: str) -> str:
    """Read the Pinecone API key from Secrets Manager. The value never touches config."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    secret = response.get("SecretString")
    if not secret:
        raise RuntimeError(f"secret {secret_arn} has no string value")
    return str(secret).strip()


def _dense_field(schema: Any) -> tuple[int | None, str | None]:
    fields = getattr(schema, "fields", None) or {}
    for spec in fields.values():
        dimension = getattr(spec, "dimension", None)
        if dimension is not None:
            return int(dimension), getattr(spec, "metric", None)
    return None, None


LEGACY_VECTOR_FIELD = "values"
_FIELD_CLASS_KINDS = {
    "DenseVectorField": "dense_vector",
    "SparseVectorField": "sparse_vector",
    "StringField": "string",
}


def document_schema_from(schema: Any) -> DocumentSchema | None:
    """The document schema, or None for a plain vector index.

    A vector index reports one dense field named ``values``; anything else (another dense
    field name, a sparse field, or a full-text field) means a document schema.
    """
    fields = getattr(schema, "fields", None) or {}
    dense_field = dimension = metric = sparse_field = None
    text: list[str] = []
    full_text: dict[str, dict[str, Any]] = {}
    for name, spec in fields.items():
        kind = str(_get(spec, "type") or "")
        if not kind:
            kind = _FIELD_CLASS_KINDS.get(type(spec).__name__, "")
        if kind == "dense_vector" or _get(spec, "dimension") is not None:
            dense_field = str(name)
            dim = _get(spec, "dimension")
            dimension = int(dim) if dim is not None else None
            met = _get(spec, "metric")
            metric = str(met) if met is not None else None
        elif kind == "sparse_vector":
            sparse_field = str(name)
        elif kind == "string" and _get(spec, "full_text_search") is not None:
            text.append(str(name))
            full_text[str(name)] = _full_text_config(_get(spec, "full_text_search"))
    if sparse_field is None and not text and dense_field in (None, LEGACY_VECTOR_FIELD):
        return None
    return DocumentSchema(dense_field, dimension, metric, sparse_field, tuple(text), full_text)


def _full_text_config(config: Any) -> dict[str, Any]:
    """The ``full_text_search`` object as a plain dict with only the keys that were set."""
    out: dict[str, Any] = {}
    for key in ("language", "stemming", "stop_words"):
        value = _get(config, key)
        if value is not None:
            out[key] = value
    ngram = _get(config, "ngram")
    if ngram is not None:
        out["ngram"] = {
            k: v
            for k in ("min_gram", "max_gram", "prefix_only")
            if (v := _get(ngram, k)) is not None
        }
    return out


def _get(obj: Any, key: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key)
    return getattr(obj, key, None)


def read_capacity_from(model: Any) -> ReadCapacity | None:
    """Parse the control plane's ``read_capacity`` (object or dict), None when absent."""
    raw = _get(model, "read_capacity")
    if raw is None:
        return None
    dedicated = _get(raw, "dedicated")
    mode = str(_get(raw, "mode") or ("Dedicated" if dedicated is not None else "OnDemand"))
    manual = _get(dedicated, "manual") if dedicated is not None else None
    status = _get(raw, "status")
    shards = _get(manual, "shards") if manual is not None else None
    replicas = _get(manual, "replicas") if manual is not None else None
    return ReadCapacity(
        mode=mode,
        node_type=_get(dedicated, "node_type") if dedicated is not None else None,
        shards=int(shards) if shards is not None else None,
        replicas=int(replicas) if replicas is not None else None,
        state=str(_get(status, "state")) if status is not None and _get(status, "state") else None,
        error=_get(status, "error_message") if status is not None else None,
    )


def _to_info(model: Any) -> IndexInfo:
    dimension, metric = _dense_field(getattr(model, "schema", None))
    deployment = getattr(model, "deployment", None)
    status = getattr(model, "status", None)
    return IndexInfo(
        name=model.name,
        host=model.host or "",
        dimension=dimension,
        metric=metric,
        cloud=getattr(deployment, "cloud", None),
        region=getattr(deployment, "region", None),
        ready=bool(getattr(status, "ready", False)),
        read_capacity=read_capacity_from(model),
        schema=document_schema_from(getattr(model, "schema", None)),
    )


class SdkGateway:
    """PineconeGateway backed by the official SDK."""

    def __init__(self, api_key: str, *, source_tag: str = "pcmigrate") -> None:
        self._pc = pinecone.Pinecone(api_key=api_key, source_tag=source_tag)
        self._indexes: dict[str, Any] = {}
        self._schemas: dict[str, DocumentSchema | None] = {}

    def _remember(self, info: IndexInfo) -> IndexInfo:
        if info.host:
            self._schemas[info.host] = info.schema
        return info

    def _schema(self, host: str) -> DocumentSchema | None:
        """The document schema behind *host*, looked up once per host."""
        if host not in self._schemas:
            found: DocumentSchema | None = None
            for model in self._pc.list_indexes():
                if getattr(model, "host", None) == host:
                    found = document_schema_from(getattr(model, "schema", None))
                    break
            self._schemas[host] = found
        return self._schemas[host]

    @property
    def api_version(self) -> str:
        """The API version the SDK is pinned to."""
        return PINECONE_API_VERSION

    def _index(self, host: str) -> Any:
        if host not in self._indexes:
            self._indexes[host] = self._pc.index(host=host)
        return self._indexes[host]

    def describe_index(self, name: str) -> IndexInfo | None:
        """See PineconeGateway."""
        if not self._pc.has_index(name):
            return None
        return self._remember(_to_info(self._pc.describe_index(name)))

    def create_index(
        self,
        name: str,
        *,
        dimension: int,
        metric: str,
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
        read_capacity: Mapping[str, Any] | None = None,
    ) -> IndexInfo:
        """See PineconeGateway."""
        model = self._pc.create_index(
            name=name,
            dimension=dimension,
            metric=metric,
            spec=pinecone.ServerlessSpec(
                cloud=cloud,
                region=region,
                read_capacity=dict(read_capacity) if read_capacity is not None else None,
            ),
            tags=tags,
        )
        return self._remember(_to_info(model))

    def create_document_index(
        self,
        name: str,
        *,
        fields: Mapping[str, Mapping[str, Any]],
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
        read_capacity: Mapping[str, Any] | None = None,
    ) -> IndexInfo:
        """See PineconeGateway."""
        model = self._pc.indexes.create(
            name=name,
            schema={"fields": {k: dict(v) for k, v in fields.items()}},
            deployment={"deployment_type": "managed", "cloud": cloud, "region": region},
            read_capacity=dict(read_capacity) if read_capacity is not None else None,
            tags=tags,
        )
        return self._remember(_to_info(model))

    def delete_index(self, name: str) -> None:
        """See PineconeGateway."""
        self._pc.delete_index(name)

    def namespace_counts(self, host: str) -> dict[str, int]:
        """See PineconeGateway."""
        stats = self._index(host).describe_index_stats()
        return {name: int(summary.vector_count) for name, summary in stats.namespaces.items()}

    def list_namespaces(self, host: str) -> list[str]:
        """See PineconeGateway."""
        names: list[str] = []
        for page in self._index(host).list_namespaces():
            names.extend(ns.name for ns in page.namespaces)
        return names

    def start_import(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        """See PineconeGateway."""
        response = self._index(host).start_import(
            uri=uri, error_mode="abort", integration_id=integration_id
        )
        return str(response.id)

    def describe_import(self, host: str, import_id: str) -> ImportInfo:
        """See PineconeGateway."""
        model = self._index(host).describe_import(import_id)
        return ImportInfo(
            id=str(model.id),
            status=str(model.status),
            percent_complete=model.percent_complete,
            records_imported=model.records_imported,
            error=model.error,
        )

    def _document(self, record: VectorRecord, schema: DocumentSchema) -> dict[str, Any]:
        doc: dict[str, Any] = {"_id": record.id, **record.metadata}
        for name, text in record.text_fields.items():
            if name in schema.text_fields and text:
                doc[name] = text
        if schema.dense_field and record.values:
            doc[schema.dense_field] = list(record.values)
        if schema.sparse_field and record.sparse is not None:
            doc[schema.sparse_field] = {
                "indices": list(record.sparse.indices),
                "values": list(record.sparse.values),
            }
        return doc

    def _from_document(self, doc: Any, schema: DocumentSchema) -> VectorRecord:
        data = dict(doc.to_dict()) if hasattr(doc, "to_dict") else dict(doc)
        values = data.pop(schema.dense_field, None) if schema.dense_field else None
        raw_sparse = data.pop(schema.sparse_field, None) if schema.sparse_field else None
        record_id = str(data.pop("_id", None) or data.pop("id", ""))
        data.pop("_score", None)
        text_fields = {
            name: str(data.pop(name)) for name in schema.text_fields if data.get(name) is not None
        }
        sparse = (
            SparseVector(
                [int(i) for i in raw_sparse["indices"]], [float(v) for v in raw_sparse["values"]]
            )
            if isinstance(raw_sparse, Mapping)
            else None
        )
        return VectorRecord(
            id=record_id,
            values=[float(v) for v in (values or [])],
            metadata={k: v for k, v in data.items() if k not in ("id", "score")},
            sparse=sparse,
            text_fields=text_fields,
        )

    def upsert(self, host: str, namespace: str, records: Sequence[VectorRecord]) -> int:
        """See PineconeGateway."""
        if not records:
            return 0
        schema = self._schema(host)
        if schema is not None:
            total = 0
            docs = [self._document(r, schema) for r in records]
            for chunk in document_batches(
                docs, max_docs=DOCUMENT_UPSERT_MAX_DOCS, max_bytes=DOCUMENT_UPSERT_MAX_BYTES
            ):
                response = self._index(host).documents.upsert(namespace=namespace, documents=chunk)
                count = getattr(response, "upserted_count", None)
                total += int(count) if count is not None else len(chunk)
            return total
        vectors = [
            {"id": r.id, "values": list(r.values), "metadata": dict(r.metadata)} for r in records
        ]
        response = self._index(host).upsert(
            vectors=vectors, namespace=namespace, show_progress=False
        )
        count = getattr(response, "upserted_count", None)
        return int(count) if count is not None else len(records)

    def delete(self, host: str, namespace: str, ids: Sequence[str]) -> int:
        """See PineconeGateway."""
        schema = self._schema(host)
        for start in range(0, len(ids), 1000):
            chunk = list(ids[start : start + 1000])
            if schema is not None:
                self._index(host).documents.delete(namespace=namespace, ids=chunk)
            else:
                self._index(host).delete(ids=chunk, namespace=namespace)
        return len(ids)

    def fetch(self, host: str, namespace: str, ids: Sequence[str]) -> dict[str, VectorRecord]:
        """See PineconeGateway."""
        out: dict[str, VectorRecord] = {}
        schema = self._schema(host)
        if schema is not None:
            for start in range(0, len(ids), 1000):
                response = self._index(host).documents.fetch(
                    namespace=namespace, ids=list(ids[start : start + 1000]), include_fields=["*"]
                )
                documents = response.documents
                items = (
                    documents.items() if isinstance(documents, Mapping) else enumerate(documents)
                )
                for key, doc in items:
                    record = self._from_document(doc, schema)
                    out[record.id or str(key)] = record
            return out
        for start in range(0, len(ids), 100):
            response = self._index(host).fetch(
                ids=list(ids[start : start + 100]), namespace=namespace
            )
            for vector_id, vector in response.vectors.items():
                out[str(vector_id)] = VectorRecord(
                    id=str(vector_id),
                    values=[float(v) for v in (vector.values or [])],
                    metadata=dict(vector.metadata or {}),
                )
        return out

    def query(
        self,
        host: str,
        namespace: str,
        vector: Sequence[float],
        *,
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """See PineconeGateway."""
        schema = self._schema(host)
        if schema is not None:
            if not schema.dense_field:
                raise RuntimeError(f"index at {host} has no dense field to query")
            clause = {"type": "dense_vector", "field": schema.dense_field, "values": list(vector)}
            return self._search_documents(host, namespace, clause, top_k, filter)
        response = self._index(host).query(
            vector=list(vector), top_k=top_k, namespace=namespace, filter=filter
        )
        return [Match(id=str(m.id), score=float(m.score)) for m in response.matches]

    def query_sparse(
        self,
        host: str,
        namespace: str,
        sparse: SparseVector,
        *,
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """See PineconeGateway."""
        schema = self._schema(host)
        if schema is None or not schema.sparse_field:
            raise RuntimeError(f"index at {host} has no sparse field to query")
        clause = {
            "type": "sparse_vector",
            "field": schema.sparse_field,
            "sparse_values": {"indices": list(sparse.indices), "values": list(sparse.values)},
        }
        return self._search_documents(host, namespace, clause, top_k, filter)

    def query_text(
        self,
        host: str,
        namespace: str,
        query: str,
        *,
        fields: Sequence[str],
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        """See PineconeGateway."""
        schema = self._schema(host)
        if schema is None or not schema.text_fields:
            raise RuntimeError(f"index at {host} has no full-text field to query")
        unknown = [f for f in fields if f not in schema.text_fields]
        if unknown:
            raise RuntimeError(f"index at {host} has no full-text fields {unknown}")
        clause = {"type": "text", "fields": list(fields), "query": query}
        return self._search_documents(host, namespace, clause, top_k, filter)

    def _search_documents(
        self,
        host: str,
        namespace: str,
        clause: Mapping[str, Any],
        top_k: int,
        filter: Mapping[str, Any] | None,
    ) -> list[Match]:
        response = self._index(host).documents.search(
            namespace=namespace, score_by=[clause], top_k=top_k, filter=filter
        )
        return [
            Match(id=str(doc.id), score=float(doc.score) if doc.score is not None else 0.0)
            for doc in response.matches
        ]

    def list_ids(self, host: str, namespace: str) -> Iterator[str]:
        """See PineconeGateway."""
        if self._schema(host) is not None:
            for item in self._index(host).documents.list(namespace=namespace, limit=100):
                doc_id = getattr(item, "id", None) or getattr(item, "_id", None)
                if doc_id is not None:
                    yield str(doc_id)
            return
        for page in self._index(host).list(namespace=namespace, limit=100):
            for item in page.vectors:
                if item.id is not None:
                    yield str(item.id)
