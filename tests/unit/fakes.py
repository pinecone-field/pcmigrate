"""In-memory PineconeGateway for unit tests."""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.targets.pinecone_client import ImportInfo, IndexInfo, Match, VectorRecord


@dataclass
class FakeImport:
    id: str
    uri: str
    states: list[ImportInfo]
    cursor: int = 0

    def current(self) -> ImportInfo:
        info = self.states[min(self.cursor, len(self.states) - 1)]
        self.cursor += 1
        return info


@dataclass
class FakeGateway:
    indexes: dict[str, IndexInfo] = field(default_factory=dict)
    namespaces: dict[str, dict[str, int]] = field(default_factory=dict)
    imports: dict[str, FakeImport] = field(default_factory=dict)
    created: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    import_script: list[list[ImportInfo]] = field(default_factory=list)
    import_records: int = 0
    stats_history: list[dict[str, int]] = field(default_factory=list)

    def describe_index(self, name: str) -> IndexInfo | None:
        return self.indexes.get(name)

    def create_index(
        self,
        name: str,
        *,
        dimension: int,
        metric: str,
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
    ) -> IndexInfo:
        info = IndexInfo(name, f"{name}.fake.pinecone.io", dimension, metric, cloud, region, True)
        self.indexes[name] = info
        self.namespaces.setdefault(info.host, {})
        self.created.append(name)
        return info

    def delete_index(self, name: str) -> None:
        info = self.indexes.pop(name)
        self.namespaces.pop(info.host, None)
        self.deleted.append(name)

    def namespace_counts(self, host: str) -> dict[str, int]:
        if self.stats_history:
            return dict(self.stats_history.pop(0))
        return dict(self.namespaces.get(host, {}))

    def list_namespaces(self, host: str) -> list[str]:
        return sorted(self.namespaces.get(host, {}))

    def start_import(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        import_id = f"imp-{len(self.imports) + 1}"
        states = (
            self.import_script.pop(0)
            if self.import_script
            else [
                ImportInfo(import_id, "Pending"),
                ImportInfo(import_id, "InProgress", 50.0, self.import_records // 2),
                ImportInfo(import_id, "Completed", 100.0, self.import_records),
            ]
        )
        states = [
            ImportInfo(import_id, s.status, s.percent_complete, s.records_imported, s.error)
            for s in states
        ]
        self.imports[import_id] = FakeImport(import_id, uri, states)
        return import_id

    def describe_import(self, host: str, import_id: str) -> ImportInfo:
        info = self.imports[import_id].current()
        if info.status == "Completed":
            ns = self.namespaces.setdefault(host, {})
            for name, count in self.pending_namespace_counts.items():
                ns[name] = count
            for name, records in self.pending_records.items():
                self.data.setdefault(host, {}).setdefault(name, {}).update(records)
                ns[name] = len(self.data[host][name])
        return info

    pending_namespace_counts: dict[str, int] = field(default_factory=dict)
    pending_records: dict[str, dict[str, VectorRecord]] = field(default_factory=dict)
    data: dict[str, dict[str, dict[str, VectorRecord]]] = field(default_factory=dict)
    metric: str = "cosine"
    upserts: list[tuple[str, int]] = field(default_factory=list)

    def upsert(self, host: str, namespace: str, records: Sequence[VectorRecord]) -> int:
        store = self.data.setdefault(host, {}).setdefault(namespace, {})
        for record in records:
            store[record.id] = record
        self.namespaces.setdefault(host, {})[namespace] = len(store)
        self.upserts.append((namespace, len(records)))
        return len(records)

    def fetch(self, host: str, namespace: str, ids: Sequence[str]) -> dict[str, VectorRecord]:
        store = self.data.get(host, {}).get(namespace, {})
        return {i: store[i] for i in ids if i in store}

    def _score(self, a: Sequence[float], b: Sequence[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b, strict=True))
        if self.metric == "dotproduct":
            return dot
        if self.metric == "euclidean":
            return -sum((x - y) ** 2 for x, y in zip(a, b, strict=True))
        na = math.sqrt(sum(x * x for x in a)) or 1.0
        nb = math.sqrt(sum(y * y for y in b)) or 1.0
        return dot / (na * nb)

    @staticmethod
    def _matches_filter(metadata: Mapping[str, Any], flt: Mapping[str, Any] | None) -> bool:
        if not flt:
            return True
        for key, cond in flt.items():
            value = metadata.get(key)
            if isinstance(cond, dict):
                if "$eq" in cond and value != cond["$eq"]:
                    return False
                if "$in" in cond and value not in cond["$in"]:
                    return False
            elif value != cond:
                return False
        return True

    def query(
        self,
        host: str,
        namespace: str,
        vector: Sequence[float],
        *,
        top_k: int,
        filter: Mapping[str, Any] | None = None,
    ) -> list[Match]:
        store = self.data.get(host, {}).get(namespace, {})
        scored = [
            Match(id=r.id, score=self._score(vector, r.values))
            for r in store.values()
            if self._matches_filter(r.metadata, filter)
        ]
        scored.sort(key=lambda m: (-m.score, m.id))
        return scored[:top_k]

    def list_ids(self, host: str, namespace: str) -> Iterator[str]:
        yield from sorted(self.data.get(host, {}).get(namespace, {}))
