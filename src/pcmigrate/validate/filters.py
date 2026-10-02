"""Filter parity for the most common metadata filters.

Pinecone serverless search is approximate, so the pass rule is mean overlap@k at or above
the approved recall threshold rather than exact set equality. Each filter's exact-match
status is still reported for the FDE.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class FilterParityResult:
    """Per-filter outcomes."""

    namespace: str
    k: int
    threshold: float = 0.95
    results: list[dict[str, Any]] = field(default_factory=list)

    @property
    def mean_overlap(self) -> float:
        """Mean of per-filter overlap@k, 1.0 when no filters ran."""
        if not self.results:
            return 1.0
        return float(sum(float(r["overlap_at_k"]) for r in self.results) / len(self.results))

    @property
    def passed(self) -> bool:
        """Mean overlap at or above the threshold."""
        return self.mean_overlap >= self.threshold

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "threshold": self.threshold,
            "filters": len(self.results),
            "exact_matches": sum(1 for r in self.results if r["equal"]),
            "mean_overlap": round(self.mean_overlap, 4),
            "results": self.results,
            "passed": self.passed,
        }


def check_filters(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    k: int,
    filters: int = 20,
    threshold: float = 0.95,
    seed: int = 2,
) -> FilterParityResult:
    """Run each common filter with a sampled query vector on both sides."""
    result = FilterParityResult(namespace=namespace, k=k, threshold=threshold)
    candidates = source.common_filters(namespace, filters)
    if not candidates:
        return result
    ids = source.sample_ids(namespace, len(candidates), seed=seed)
    records = source.fetch_records(namespace, ids)
    vectors = [r.dense for r in records.values() if r.dense]
    for index, flt in enumerate(candidates):
        if not vectors:
            break
        vector = vectors[index % len(vectors)]
        truth = set(source.knn(namespace, vector, k, filter=flt))
        got = {m.id for m in gateway.query(host, namespace, vector, top_k=k, filter=flt)}
        denominator = max(1, min(k, len(truth))) if truth or got else 1
        result.results.append(
            {
                "filter": flt,
                "equal": truth == got,
                "source": len(truth),
                "target": len(got),
                "overlap": len(truth & got),
                "overlap_at_k": len(truth & got) / denominator if (truth or got) else 1.0,
            }
        )
    return result
