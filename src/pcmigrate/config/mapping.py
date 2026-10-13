"""Mapping: the shape of mapping.yaml, the FDE-editable proposed source-to-target mapping."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pcmigrate.config._secrets import require_secret_arn

PINECONE_API_VERSION = "2026-07"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceKind(StrEnum):
    """Supported source connectors."""

    PGVECTOR = "pgvector"
    OPENSEARCH = "opensearch"
    SYNTHETIC = "synthetic"


class Metric(StrEnum):
    """Pinecone distance metrics."""

    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    DOTPRODUCT = "dotproduct"


class MetadataType(StrEnum):
    """Scalar metadata types a mapped field may take in the target."""

    STRING = "string"
    FLOAT = "float"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    EPOCH_SECONDS = "epoch_seconds"
    STRING_LIST = "string_list"


class SourceSpec(_Strict):
    """Where the data comes from. Credentials are referenced by secret ARN only."""

    kind: SourceKind
    dsn_secret: str | None = None
    snapshot: Literal["exported", "none"] = "exported"
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("dsn_secret")
    @classmethod
    def _dsn_is_arn(cls, value: str | None) -> str | None:
        return None if value is None else require_secret_arn(value, "source.dsn_secret")


class ReadNodesSpec(_Strict):
    """Dedicated read nodes for the target index (capacity: dedicated).

    Shards hold the data (250 GB each); replicas add query throughput. Multiple namespaces
    on a dedicated index need the project to have that enabled by Pinecone;
    ``multi_namespace: true`` records that the FDE confirmed it.
    """

    node_type: Literal["b1", "t1"] = "b1"
    shards: int = Field(default=1, ge=1)
    replicas: int = Field(default=1, ge=1)
    multi_namespace: bool = False

    def read_capacity(self) -> dict[str, Any]:
        """The control-plane ``read_capacity`` body."""
        return {
            "mode": "Dedicated",
            "dedicated": {
                "node_type": self.node_type,
                "scaling": "Manual",
                "manual": {"shards": self.shards, "replicas": self.replicas},
            },
        }


class TargetSpec(_Strict):
    """The Pinecone index to create or adopt."""

    index: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,44}$")
    mode: Literal["create", "adopt"] = "create"
    type: Literal["vector", "document"] = "vector"
    cloud: Literal["aws"] = "aws"
    region: str
    capacity: Literal["on_demand", "dedicated"] = "on_demand"
    read_nodes: ReadNodesSpec | None = None
    dense_field: str = Field(default="dense", pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    sparse_field: str = Field(default="sparse", pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    api_version: str = PINECONE_API_VERSION

    @model_validator(mode="after")
    def _read_nodes_match_capacity(self) -> Self:
        if self.capacity == "dedicated" and self.read_nodes is None:
            raise ValueError("target.capacity dedicated needs target.read_nodes")
        if self.capacity == "on_demand" and self.read_nodes is not None:
            raise ValueError("target.read_nodes is only valid with target.capacity dedicated")
        return self

    def read_capacity(self) -> dict[str, Any] | None:
        """The ``read_capacity`` body to create the index with; None means on-demand."""
        return self.read_nodes.read_capacity() if self.read_nodes is not None else None


class DenseSpec(_Strict):
    """The dense vector column or field of one source object."""

    column: str
    dimension: int = Field(gt=0, le=20_000)
    metric: Metric


class SparseKind(StrEnum):
    """Where a sparse vector comes from."""

    SPARSEVEC = "sparsevec"
    RANK_FEATURES = "rank_features"
    GENERATED = "generated"


DEFAULT_SPARSE_MODEL = "pinecone-sparse-english-v0"


class SparseModelSpec(_Strict):
    """The hosted sparse embedding model that encodes text at migration time (ADR-017).

    ``api_key_secret`` is a Secrets Manager ARN for providers that need their own key; the
    ``pinecone`` provider reuses the migration's Pinecone key when it is unset.
    """

    provider: Literal["pinecone"] = "pinecone"
    name: str = DEFAULT_SPARSE_MODEL
    truncate: Literal["END", "NONE"] = "END"
    max_tokens: int | None = Field(default=None, gt=0, le=2048)
    api_key_secret: str | None = None

    @field_validator("api_key_secret")
    @classmethod
    def _key_is_arn(cls, value: str | None) -> str | None:
        return None if value is None else require_secret_arn(value, "sparse.model.api_key_secret")


class SparseSpec(_Strict):
    """The sparse vector of one source object (hybrid wave, ADR-016 and ADR-017).

    ``sparsevec`` carries integer indices already; ``rank_features`` (including neural
    sparse fields) carries token weights, and tokens become indices through the
    ``vocabulary`` function, recorded under ``manifest/vocabulary/`` so query-time
    encoding can use the same mapping. ``generated`` has no source column: the
    ``text_fields`` are concatenated in order with ``separator`` and encoded by ``model``
    during transform, so query-time encoding must call the same model.
    """

    kind: SparseKind
    column: str | None = None
    vocabulary: Literal["identity", "xxh32", "model"] = "identity"
    dimension: int | None = Field(default=None, gt=0)
    text_fields: list[str] = Field(default_factory=list)
    separator: str = "\n"
    model: SparseModelSpec | None = None

    @model_validator(mode="before")
    @classmethod
    def _generated_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("kind") == "generated":
            data = dict(data)
            data.setdefault("vocabulary", "model")
            data.setdefault("model", {})
        return data

    @model_validator(mode="after")
    def _fields_match_kind(self) -> Self:
        if self.kind is SparseKind.GENERATED:
            if self.column is not None:
                raise ValueError("generated sparse vectors take text_fields, not a column")
            if not self.text_fields:
                raise ValueError("generated sparse vectors need at least one text field")
            if len(set(self.text_fields)) != len(self.text_fields):
                raise ValueError("generated sparse text_fields repeat a field")
            if self.vocabulary != "model":
                raise ValueError("generated sparse vectors use vocabulary: model")
            if self.model is None:
                raise ValueError("generated sparse vectors need a model")
            return self
        if self.column is None:
            raise ValueError(f"{self.kind.value} sparse fields need a column")
        if self.text_fields or self.model is not None:
            raise ValueError("text_fields and model apply to kind: generated only")
        if self.kind is SparseKind.RANK_FEATURES and self.vocabulary != "xxh32":
            raise ValueError("rank_features sparse fields need vocabulary: xxh32")
        if self.kind is SparseKind.SPARSEVEC and self.vocabulary != "identity":
            raise ValueError("sparsevec sparse fields use vocabulary: identity")
        return self

    @property
    def generated(self) -> bool:
        """True when the sparse vector is encoded from text at migration time."""
        return self.kind is SparseKind.GENERATED


FTS_LANGUAGES = frozenset(
    {
        "ar",
        "da",
        "de",
        "el",
        "en",
        "es",
        "fi",
        "fr",
        "hu",
        "it",
        "nl",
        "no",
        "pt",
        "ro",
        "ru",
        "sv",
        "ta",
        "tr",
    }
)
TEXT_FIELD_NAME = r"^[A-Za-z][A-Za-z0-9_-]{0,63}$"


class NgramSpec(_Strict):
    """Character n-gram tokenization for a full-text field (substring and prefix matching)."""

    min_gram: int = Field(ge=1)
    max_gram: int = Field(ge=1)
    prefix_only: bool = False

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.max_gram < self.min_gram:
            raise ValueError("ngram.max_gram must be at least ngram.min_gram")
        return self


class FullTextSearchSpec(_Strict):
    """The ``full_text_search`` configuration of one string field (API 2026-07, ADR-018).

    ``language`` is a two-letter code Pinecone analyzes; ``stop_words`` needs ``stemming``;
    ``ngram`` replaces word tokenization and excludes both.
    """

    language: str = "en"
    stemming: bool = False
    stop_words: bool = False
    ngram: NgramSpec | None = None

    @field_validator("language")
    @classmethod
    def _known_language(cls, value: str) -> str:
        if value not in FTS_LANGUAGES:
            raise ValueError(
                f"full_text_search.language {value!r} is not one of {sorted(FTS_LANGUAGES)}"
            )
        return value

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.stop_words and not self.stemming:
            raise ValueError("full_text_search.stop_words requires stemming: true")
        if self.ngram is not None and (self.stemming or self.stop_words):
            raise ValueError(
                "full_text_search.ngram cannot be combined with stemming or stop_words"
            )
        return self

    def to_schema(self) -> dict[str, Any]:
        """The control-plane body of the field's ``full_text_search`` object."""
        body: dict[str, Any] = {"language": self.language}
        if self.ngram is not None:
            body["ngram"] = self.ngram.model_dump(mode="json")
        else:
            body["stemming"] = self.stemming
            body["stop_words"] = self.stop_words
        return body


class TextFieldSpec(_Strict):
    """One source text column or field that lands as a ``full_text_search`` string field.

    ``source`` is the column or ``_source`` field whose text is carried (for pgvector, the
    column a tsvector is generated from, never the tsvector). ``derived_from`` names the
    tsvector column or GIN expression index, or the OpenSearch analyzer, the configuration was
    translated from, and ``expression`` keeps the pgvector ``to_tsvector`` expression that
    ground-truth ranking reuses. ``max_chars`` truncates long values before emit.
    """

    source: str
    target: str | None = Field(default=None, pattern=TEXT_FIELD_NAME)
    full_text_search: FullTextSearchSpec = Field(default_factory=FullTextSearchSpec)
    derived_from: str | None = None
    expression: str | None = None
    analyzer: str | None = None
    max_chars: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _default_target(self) -> Self:
        if self.target is None:
            object.__setattr__(self, "target", self.source)
        return self

    @property
    def field_name(self) -> str:
        """The schema field name (``target`` resolved)."""
        return self.target or self.source


class MetadataField(_Strict):
    """One source field, either mapped into target metadata or dropped with a reason."""

    source: str
    target: str | None = None
    type: MetadataType | None = None
    action: Literal["map", "drop"] = "map"
    reason: str | None = None
    transform: Literal["stringify", "flatten"] | None = None
    max_chars: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.action == "drop":
            if not self.reason:
                raise ValueError(f"dropped field {self.source!r} needs a reason")
            return self
        if self.type is None:
            raise ValueError(f"mapped field {self.source!r} needs a type")
        if self.target is None:
            object.__setattr__(self, "target", self.source)
        return self


class ObjectMapping(_Strict):
    """One source table or index and how it lands in a namespace.

    ``namespace_by`` names a column whose value selects the namespace per record, for
    tenant-style RLS policies; when unset every record lands in ``namespace``.
    """

    source: str
    namespace: str
    namespace_by: str | None = None
    id_template: str
    dense: DenseSpec | None = None
    sparse: SparseSpec | None = None
    text: list[TextFieldSpec] = Field(default_factory=list)
    metadata: list[MetadataField] = Field(default_factory=list)

    @model_validator(mode="after")
    def _no_duplicate_targets(self) -> Self:
        targets = [f.target or f.source for f in self.metadata if f.action == "map"]
        duplicates = sorted({t for t in targets if targets.count(t) > 1})
        if duplicates:
            raise ValueError(f"object {self.source!r} maps several fields to {duplicates}")
        text_targets = [t.field_name for t in self.text]
        repeated = sorted({t for t in text_targets if text_targets.count(t) > 1})
        if repeated:
            raise ValueError(f"object {self.source!r} maps several text fields to {repeated}")
        clash = sorted(set(text_targets) & set(targets))
        if clash:
            raise ValueError(
                f"object {self.source!r} uses {clash} as both a metadata field and a full-text "
                "field; a Pinecone string field is searchable or filterable, never both"
            )
        return self

    @model_validator(mode="after")
    def _generated_text_is_not_the_vector(self) -> Self:
        sparse, dense = self.sparse, self.dense
        if sparse is None or dense is None or not sparse.generated:
            return self
        if dense.column in sparse.text_fields:
            raise ValueError(
                f"object {self.source!r} lists the dense column as a sparse text field"
            )
        return self

    @property
    def generated_sparse(self) -> SparseSpec | None:
        """The sparse spec when it is generated from text, else None."""
        return self.sparse if self.sparse is not None and self.sparse.generated else None

    @property
    def text_sources(self) -> dict[str, str]:
        """Source column -> schema field name for every full-text field."""
        return {t.source: t.field_name for t in self.text}

    @property
    def text_columns(self) -> list[str]:
        """Every source column whose text the record carries (FTS plus generated sparse)."""
        seen: dict[str, None] = dict.fromkeys(t.source for t in self.text)
        if self.generated_sparse is not None:
            for name in self.generated_sparse.text_fields:
                seen.setdefault(name, None)
        return list(seen)


class ChangeCaptureSpec(_Strict):
    """How post-snapshot changes reach the buffer (ADR-007, ADR-008).

    ``none`` provisions no buffer, applier, or write path and skips sync; the run goes from
    validate straight to cutover, where the final reconcile still runs.
    """

    tier: Literal["none", "watermark", "trigger_outbox", "logical", "dual_write"]
    buffer: Literal["sqs_fifo", "kinesis"] = "sqs_fifo"
    watermark_column: str | None = None
    rescan: bool = False

    @property
    def enabled(self) -> bool:
        """True for every tier but none."""
        return self.tier != "none"


class RecallAtK(_Strict):
    """Recall@k check parameters."""

    k: int = Field(default=10, gt=0)
    queries: int = Field(default=200, gt=0)
    threshold: float = Field(default=0.95, ge=0.0, le=1.0)


class HybridCheck(_Strict):
    """Sparse-only and fused (dense + sparse, RRF) overlap@k against the source (ADR-016)."""

    k: int = Field(default=10, gt=0)
    queries: int = Field(default=100, gt=0)
    threshold: float = Field(default=0.9, ge=0.0, le=1.0)
    rrf_k: int = Field(default=60, gt=0)


FTS_QUERY_CLASSES = ("single_term", "multi_term", "phrase", "filtered")


class FullTextThresholds(_Strict):
    """Overlap@k the FDE requires per query class (ADR-018).

    Source ranking is not BM25 on pgvector, so the defaults sit below the recall threshold.
    """

    single_term: float = Field(default=0.8, ge=0.0, le=1.0)
    multi_term: float = Field(default=0.5, ge=0.0, le=1.0)
    phrase: float = Field(default=0.8, ge=0.0, le=1.0)
    filtered: float = Field(default=0.6, ge=0.0, le=1.0)

    def for_class(self, name: str) -> float:
        """The threshold of one query class."""
        return float(getattr(self, name))


class FullTextCheck(_Strict):
    """Full-text overlap@k per query class against the source's text ranking (ADR-018)."""

    k: int = Field(default=10, gt=0)
    queries: int = Field(default=50, gt=0)
    thresholds: FullTextThresholds = Field(default_factory=FullTextThresholds)


class ValidationSpec(_Strict):
    """Checks and thresholds the validate stage runs exactly as approved."""

    recall_at_k: RecallAtK = Field(default_factory=RecallAtK)
    fidelity_sample: int = Field(default=1000, gt=0)
    hybrid: HybridCheck | None = None
    full_text: FullTextCheck | None = None


class Mapping(_Strict):
    """The complete mapping.yaml document."""

    migration_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    source: SourceSpec
    target: TargetSpec
    objects: list[ObjectMapping] = Field(min_length=1)
    duplicates: Literal["upsert_after_import", "drop"] = "upsert_after_import"
    change_capture: ChangeCaptureSpec | None = None
    validation: ValidationSpec = Field(default_factory=ValidationSpec)

    @model_validator(mode="after")
    def _namespaces_unique(self) -> Self:
        namespaces = [o.namespace for o in self.objects]
        duplicates = sorted({n for n in namespaces if namespaces.count(n) > 1})
        if duplicates:
            raise ValueError(f"several objects target the same namespace: {duplicates}")
        return self

    @property
    def namespaces(self) -> list[str]:
        """Target namespaces in object order."""
        return [o.namespace for o in self.objects]

    @property
    def change_capture_enabled(self) -> bool:
        """False when change_capture is absent or its tier is none."""
        return self.change_capture is not None and self.change_capture.enabled

    @property
    def hybrid(self) -> bool:
        """True when any object carries a sparse vector into the target."""
        return any(o.sparse is not None for o in self.objects)

    @property
    def generates_sparse(self) -> bool:
        """True when any object encodes its sparse vector from text (ADR-017)."""
        return any(o.generated_sparse is not None for o in self.objects)

    @property
    def full_text(self) -> bool:
        """True when any object carries full-text fields into the target (ADR-018)."""
        return any(o.text for o in self.objects)

    @property
    def text_fields(self) -> dict[str, FullTextSearchSpec]:
        """Every full-text schema field across objects with its configuration."""
        fields: dict[str, FullTextSearchSpec] = {}
        for obj in self.objects:
            for spec in obj.text:
                fields.setdefault(spec.field_name, spec.full_text_search)
        return fields

    @model_validator(mode="after")
    def _sparse_needs_document_index(self) -> Self:
        if self.hybrid and self.target.type != "document":
            raise ValueError("sparse vectors need target.type: document (ADR-004)")
        if self.full_text and self.target.type != "document":
            raise ValueError("full-text fields need target.type: document (ADR-018)")
        return self

    @model_validator(mode="after")
    def _text_fields_agree(self) -> Self:
        seen: dict[str, FullTextSearchSpec] = {}
        for obj in self.objects:
            for spec in obj.text:
                name = spec.field_name
                if name in (self.target.dense_field, self.target.sparse_field):
                    raise ValueError(f"text field {name!r} collides with a vector field name")
                earlier = seen.setdefault(name, spec.full_text_search)
                if earlier != spec.full_text_search:
                    raise ValueError(
                        f"text field {name!r} has different full_text_search settings across "
                        "objects; one index schema holds one configuration per field"
                    )
        if len(seen) > 100:
            raise ValueError("a document index takes at most 100 full-text fields")
        return self

    @classmethod
    def from_yaml(cls, text: str) -> Mapping:
        """Parse a mapping.yaml document."""
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            raise ValueError("mapping.yaml must be a YAML mapping at the top level")
        return cls.model_validate(data)

    def to_yaml(self) -> str:
        """Render as YAML in field order, in the documented shape.

        ``action: map`` and empty ``options`` are implied, so they are left out; everything
        else keeps its explicit value so the FDE sees what will run.
        """
        data = self.model_dump(mode="json", exclude_none=True)
        if not data["source"].get("options"):
            data["source"].pop("options", None)
        for obj in data["objects"]:
            if not obj.get("text"):
                obj.pop("text", None)
            for field_ in obj.get("metadata", []):
                if field_.get("action") == "map":
                    field_.pop("action")
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)

    def normalized(self) -> str:
        """Canonical JSON used for hashing: sorted keys, no None values, no whitespace."""
        data = self.model_dump(mode="json", exclude_none=True)
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
