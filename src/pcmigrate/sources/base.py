"""SourceConnector protocol: introspect, plan_partitions, export(partition, writer), change_feed."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.plan_lock import schema_fingerprint
from pcmigrate.core.canonical import CanonicalRecord, SparseVector


@dataclass(frozen=True, slots=True)
class SourceSchema:
    """What introspection learned about the source, enough to fingerprint it."""

    kind: str
    objects: dict[str, Any] = field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        """SHA-256 over the schema, independent of key order."""
        return schema_fingerprint({"kind": self.kind, "objects": self.objects})


@dataclass(frozen=True, slots=True)
class Partition:
    """One unit of export work. Bounds are source-specific strings, inclusive-exclusive."""

    id: str
    object: str
    namespace: str
    lower: str | None = None
    upper: str | None = None
    estimated_rows: int = 0


@dataclass(frozen=True, slots=True)
class Change:
    """One post-snapshot change from a source change feed."""

    op: Literal["upsert", "delete"]
    id: str
    namespace: str
    sequence: int
    record: CanonicalRecord | None = None


@dataclass(frozen=True, slots=True)
class TextQuery:
    """One full-text query the validate stage runs on both sides (ADR-018).

    ``field`` is the schema field name; sources map it back to their column or field.
    ``mode`` is ``any`` (token OR, like a Pinecone ``text`` clause), ``all`` (every term),
    or ``phrase`` (terms in order). ``filter`` is a Pinecone metadata filter.
    """

    field: str
    text: str
    mode: Literal["any", "all", "phrase"] = "any"
    filter: MappingABC[str, Any] | None = None


class RecordSink(Protocol):
    """Where export() puts records. The canonical writer satisfies it."""

    def write(self, record: CanonicalRecord) -> None:
        """Accept one record."""
        ...


class SourceConnector(Protocol):
    """One source kind. All source access goes through an implementation of this."""

    @property
    def kind(self) -> str:
        """Short name, matching Mapping.source.kind."""
        ...

    def introspect(self) -> SourceSchema:
        """Read-only discovery of objects, fields, and types."""
        ...

    def plan_partitions(self, mapping: Mapping) -> list[Partition]:
        """Split every mapped object into export partitions."""
        ...

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """Stream one partition's records into *writer*; return the row count."""
        ...

    def change_feed(self) -> Iterator[Change]:
        """Post-snapshot changes, in sequence order."""
        ...


class GroundTruthSource(Protocol):
    """What the validate stage needs from a source to compute ground truth."""

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """Random record ids from the namespace, deterministic for a seed."""
        ...

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """Current records for *ids*, built through the mapping."""
        ...

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: MappingABC[str, Any] | None = None,
    ) -> list[str]:
        """Exact nearest-neighbour ids by the mapped metric, index scans disabled."""
        ...

    def knn_sparse(
        self,
        namespace: str,
        sparse: SparseVector,
        k: int,
        *,
        filter: MappingABC[str, Any] | None = None,
    ) -> list[str]:
        """Exact nearest neighbours by sparse inner product (hybrid wave, ADR-016)."""
        ...

    def knn_text(self, namespace: str, query: TextQuery, k: int) -> list[str]:
        """Top-k ids by the source's own text ranking for one full-text query (ADR-018)."""
        ...

    def all_ids(self, namespace: str) -> Iterator[str]:
        """Every record id in the namespace."""
        ...

    def common_filters(self, namespace: str, count: int) -> list[dict[str, Any]]:
        """The most common equality filters, as Pinecone filter dicts."""
        ...
