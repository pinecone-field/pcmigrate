"""Hybrid checks: sparse-only and fused (dense + sparse) overlap@k against the source (ADR-016).

The Documents API ranks by one vector clause per request, so the fused comparison runs the
dense and sparse searches separately on both sides and applies the same reciprocal rank
fusion before measuring overlap.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from pcmigrate.core.canonical import CanonicalRecord, SparseVector
from pcmigrate.core.sparse import overlap_at_k, rrf_fuse
from pcmigrate.targets.pinecone_client import PineconeGateway
from pcmigrate.validate.fidelity import _sparse_equal


class HybridTruth(Protocol):
    """What the hybrid checks need: a source, or the canonical zone for generated sparse."""

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """A seeded random sample of ids."""
        ...

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """Records with dense and sparse vectors."""
        ...

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact dense top-k."""
        ...

    def knn_sparse(
        self,
        namespace: str,
        sparse: SparseVector,
        k: int,
        *,
        filter: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Exact sparse top-k."""
        ...


@dataclass
class HybridResult:
    """Per-query overlap for the sparse-only and the fused rankings."""

    namespace: str
    k: int
    threshold: float
    sparse_overlap: list[float] = field(default_factory=list)
    fused_overlap: list[float] = field(default_factory=list)
    skipped: int = 0
    sparse_mismatches: list[str] = field(default_factory=list)
    model: str | None = None

    @staticmethod
    def _mean(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @property
    def sparse_mean(self) -> float:
        """Mean sparse-only overlap@k."""
        return self._mean(self.sparse_overlap)

    @property
    def fused_mean(self) -> float:
        """Mean fused overlap@k."""
        return self._mean(self.fused_overlap)

    @property
    def sparse_passed(self) -> bool:
        """Sparse-only overlap at or above the threshold, and every stored vector as generated."""
        return (
            bool(self.sparse_overlap)
            and self.sparse_mean >= self.threshold
            and not self.sparse_mismatches
        )

    @property
    def fused_passed(self) -> bool:
        """Fused overlap at or above the threshold, with at least one query."""
        return bool(self.fused_overlap) and self.fused_mean >= self.threshold

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "threshold": self.threshold,
            "queries": len(self.sparse_overlap),
            "skipped": self.skipped,
            "sparse_mean": round(self.sparse_mean, 4),
            "sparse_min": round(min(self.sparse_overlap), 4) if self.sparse_overlap else None,
            "fused_mean": round(self.fused_mean, 4),
            "fused_min": round(min(self.fused_overlap), 4) if self.fused_overlap else None,
            "sparse_passed": self.sparse_passed,
            "fused_passed": self.fused_passed,
            "sparse_mismatches": len(self.sparse_mismatches),
            "model": self.model,
        }


def check_hybrid(
    gateway: PineconeGateway,
    host: str,
    source: HybridTruth,
    namespace: str,
    *,
    k: int,
    queries: int,
    threshold: float,
    rrf_k: int = 60,
    seed: int = 3,
    model: str | None = None,
) -> HybridResult:
    """Query with the records' dense and sparse vectors; compare sparse-only and fused top-k.

    With *model* set (generated sparse, ADR-017) the truth is the canonical zone, so the
    check also fetches the sample from Pinecone and requires the stored sparse vectors to
    equal the generated ones.
    """
    result = HybridResult(namespace=namespace, k=k, threshold=threshold, model=model)
    ids = source.sample_ids(namespace, queries, seed=seed)
    records = source.fetch_records(namespace, ids)
    depth = max(k, 2 * k)
    usable = [
        r for i in ids if (r := records.get(i)) is not None and r.sparse is not None and r.dense
    ]
    result.skipped = len(ids) - len(usable)
    if model is not None and usable:
        stored = gateway.fetch(host, namespace, [r.id for r in usable])
        for record in usable:
            got = stored.get(record.id)
            if got is None or got.sparse is None or not _sparse_equal(record.sparse, got.sparse):
                result.sparse_mismatches.append(record.id)
    truth_sparse_all, truth_dense_all = _truth_rankings(source, namespace, usable, depth)
    for record, truth_sparse, truth_dense in zip(
        usable, truth_sparse_all, truth_dense_all, strict=True
    ):
        assert record.sparse is not None and record.dense is not None
        got_sparse = [
            m.id for m in gateway.query_sparse(host, namespace, record.sparse, top_k=depth)
        ]
        result.sparse_overlap.append(overlap_at_k(truth_sparse, got_sparse, k))
        got_dense = [m.id for m in gateway.query(host, namespace, record.dense, top_k=depth)]
        truth_fused = rrf_fuse([truth_dense, truth_sparse], k=rrf_k, top=k)
        got_fused = rrf_fuse([got_dense, got_sparse], k=rrf_k, top=k)
        result.fused_overlap.append(overlap_at_k(truth_fused, got_fused, k))
    return result


def _truth_rankings(
    source: HybridTruth, namespace: str, records: Sequence[CanonicalRecord], depth: int
) -> tuple[list[list[str]], list[list[str]]]:
    """Sparse and dense reference rankings, in one pass when the truth supports batches."""
    knn_sparse_many = getattr(source, "knn_sparse_many", None)
    knn_many = getattr(source, "knn_many", None)
    if callable(knn_sparse_many) and callable(knn_many):
        sparse_rankings: list[list[str]] = knn_sparse_many(
            namespace, [r.sparse for r in records], depth
        )
        dense_rankings: list[list[str]] = knn_many(namespace, [r.dense for r in records], depth)
        return sparse_rankings, dense_rankings
    sparse_rankings = []
    dense_rankings = []
    for record in records:
        assert record.sparse is not None and record.dense is not None
        sparse_rankings.append(source.knn_sparse(namespace, record.sparse, depth))
        dense_rankings.append(source.knn(namespace, record.dense, depth))
    return sparse_rankings, dense_rankings
