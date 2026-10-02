"""Shadow reads: sampled live-like queries against both systems, with overlap and latency."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class ShadowResult:
    """Per-query overlap and latencies."""

    namespace: str
    k: int
    overlaps: list[float] = field(default_factory=list)
    source_ms: list[float] = field(default_factory=list)
    target_ms: list[float] = field(default_factory=list)

    @staticmethod
    def _pct(values: list[float], pct: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, round(pct * (len(ordered) - 1)))
        return ordered[index]

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "queries": len(self.overlaps),
            "mean_overlap": round(statistics.fmean(self.overlaps), 4) if self.overlaps else None,
            "min_overlap": min(self.overlaps) if self.overlaps else None,
            "source_p50_ms": self._pct(self.source_ms, 0.5),
            "source_p95_ms": self._pct(self.source_ms, 0.95),
            "target_p50_ms": self._pct(self.target_ms, 0.5),
            "target_p95_ms": self._pct(self.target_ms, 0.95),
        }


def shadow_read(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    queries: int,
    k: int,
    seed: int = 7,
    clock: Callable[[], float] = time.perf_counter,
) -> ShadowResult:
    """Run *queries* sampled vectors through both systems and time each side."""
    result = ShadowResult(namespace=namespace, k=k)
    ids = source.sample_ids(namespace, queries, seed=seed)
    records = source.fetch_records(namespace, ids)
    for record_id in ids:
        record = records.get(record_id)
        if record is None or not record.dense:
            continue
        started = clock()
        truth = set(source.knn(namespace, record.dense, k))
        result.source_ms.append((clock() - started) * 1000)
        started = clock()
        got = {m.id for m in gateway.query(host, namespace, record.dense, top_k=k)}
        result.target_ms.append((clock() - started) * 1000)
        result.overlaps.append(len(truth & got) / k if k else 0.0)
    return result
