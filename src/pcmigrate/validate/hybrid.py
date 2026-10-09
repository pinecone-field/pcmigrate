"""Hybrid checks: sparse-only and fused (dense + sparse) overlap@k against the source (ADR-016).

The Documents API ranks by one vector clause per request, so the fused comparison runs the
dense and sparse searches separately on both sides and applies the same reciprocal rank
fusion before measuring overlap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pcmigrate.core.sparse import overlap_at_k, rrf_fuse
from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class HybridResult:
    """Per-query overlap for the sparse-only and the fused rankings."""

    namespace: str
    k: int
    threshold: float
    sparse_overlap: list[float] = field(default_factory=list)
    fused_overlap: list[float] = field(default_factory=list)
    skipped: int = 0

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
        """Sparse-only overlap at or above the threshold, with at least one query."""
        return bool(self.sparse_overlap) and self.sparse_mean >= self.threshold

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
        }


def check_hybrid(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    k: int,
    queries: int,
    threshold: float,
    rrf_k: int = 60,
    seed: int = 3,
) -> HybridResult:
    """Query with stored dense and sparse vectors; compare sparse-only and RRF-fused top-k."""
    result = HybridResult(namespace=namespace, k=k, threshold=threshold)
    ids = source.sample_ids(namespace, queries, seed=seed)
    records = source.fetch_records(namespace, ids)
    depth = max(k, 2 * k)
    for record_id in ids:
        record = records.get(record_id)
        if record is None or record.sparse is None or not record.dense:
            result.skipped += 1
            continue
        truth_sparse = source.knn_sparse(namespace, record.sparse, depth)
        got_sparse = [
            m.id for m in gateway.query_sparse(host, namespace, record.sparse, top_k=depth)
        ]
        result.sparse_overlap.append(overlap_at_k(truth_sparse, got_sparse, k))
        truth_dense = source.knn(namespace, record.dense, depth)
        got_dense = [m.id for m in gateway.query(host, namespace, record.dense, top_k=depth)]
        truth_fused = rrf_fuse([truth_dense, truth_sparse], k=rrf_k, top=k)
        got_fused = rrf_fuse([got_dense, got_sparse], k=rrf_k, top=k)
        result.fused_overlap.append(overlap_at_k(truth_fused, got_fused, k))
    return result
