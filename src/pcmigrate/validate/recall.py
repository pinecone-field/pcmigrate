"""Recall@k against exact-kNN ground truth from the source."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class RecallResult:
    """Recall per query and the mean."""

    namespace: str
    k: int
    threshold: float
    per_query: list[float] = field(default_factory=list)
    skipped: int = 0

    @property
    def mean(self) -> float:
        """Mean recall, 0 with no queries."""
        return sum(self.per_query) / len(self.per_query) if self.per_query else 0.0

    @property
    def passed(self) -> bool:
        """Mean at or above the approved threshold, with at least one query."""
        return bool(self.per_query) and self.mean >= self.threshold

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "threshold": self.threshold,
            "queries": len(self.per_query),
            "skipped": self.skipped,
            "mean": round(self.mean, 4),
            "min": round(min(self.per_query), 4) if self.per_query else None,
            "passed": self.passed,
        }


def check_recall(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    k: int,
    queries: int,
    threshold: float,
    seed: int = 1,
) -> RecallResult:
    """Query with stored vectors and compare the top-k id sets."""
    result = RecallResult(namespace=namespace, k=k, threshold=threshold)
    ids = source.sample_ids(namespace, queries, seed=seed)
    records = source.fetch_records(namespace, ids)
    for record_id in ids:
        record = records.get(record_id)
        if record is None or not record.dense:
            result.skipped += 1
            continue
        truth = set(source.knn(namespace, record.dense, k))
        got = {m.id for m in gateway.query(host, namespace, record.dense, top_k=k)}
        result.per_query.append(len(truth & got) / k if k else 0.0)
    return result
