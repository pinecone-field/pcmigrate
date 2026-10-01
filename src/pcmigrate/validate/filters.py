"""Filter parity for the most common metadata filters."""

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
    results: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Every filtered result set equal at k."""
        return all(r["equal"] for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "filters": len(self.results),
            "failed": sum(1 for r in self.results if not r["equal"]),
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
    seed: int = 2,
) -> FilterParityResult:
    """Run each common filter with a sampled query vector on both sides."""
    result = FilterParityResult(namespace=namespace, k=k)
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
        result.results.append(
            {
                "filter": flt,
                "equal": truth == got,
                "source": len(truth),
                "target": len(got),
                "overlap": len(truth & got),
            }
        )
    return result
