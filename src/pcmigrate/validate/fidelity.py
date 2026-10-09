"""Vector and metadata fidelity on a random id sample."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway

COSINE_THRESHOLD = 0.9999


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity, 0 when either vector is all zeros or lengths differ."""
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


@dataclass
class FidelityResult:
    """Per-record outcomes for one namespace."""

    namespace: str
    sampled: int
    missing: list[str] = field(default_factory=list)
    vector_failures: list[tuple[str, float]] = field(default_factory=list)
    metadata_failures: list[tuple[str, str]] = field(default_factory=list)
    min_cosine: float | None = None

    @property
    def vectors_passed(self) -> bool:
        """Every fetched vector within tolerance and nothing missing."""
        return not self.missing and not self.vector_failures

    @property
    def metadata_passed(self) -> bool:
        """Every metadata field equal."""
        return not self.missing and not self.metadata_failures

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "sampled": self.sampled,
            "missing": self.missing[:50],
            "missing_count": len(self.missing),
            "vector_failures": self.vector_failures[:50],
            "vector_failure_count": len(self.vector_failures),
            "metadata_failures": self.metadata_failures[:50],
            "metadata_failure_count": len(self.metadata_failures),
            "min_cosine": self.min_cosine,
            "vectors_passed": self.vectors_passed,
            "metadata_passed": self.metadata_passed,
        }


def _sparse_equal(a: Any, b: Any, *, tolerance: float = 1e-6) -> bool:
    if list(a.indices) != list(b.indices) or len(a.values) != len(b.values):
        return False
    return all(
        abs(float(x) - float(y)) <= tolerance * max(1.0, abs(float(x)))
        for x, y in zip(a.values, b.values, strict=True)
    )


def _normalize(value: Any) -> Any:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def compare_metadata(expected: dict[str, Any], actual: dict[str, Any]) -> str | None:
    """None when equal after numeric normalisation, else a description of the first difference."""
    for key in sorted(set(expected) | set(actual)):
        if key not in actual:
            return f"{key}: missing in target"
        if key not in expected:
            return f"{key}: unexpected in target"
        if _normalize(expected[key]) != _normalize(actual[key]):
            return f"{key}: {expected[key]!r} != {actual[key]!r}"
    return None


def check_fidelity(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    sample: int,
    seed: int = 0,
    cosine_threshold: float = COSINE_THRESHOLD,
) -> FidelityResult:
    """Fetch a random sample from both sides and compare vectors and metadata."""
    ids = source.sample_ids(namespace, sample, seed=seed)
    expected = source.fetch_records(namespace, ids)
    actual = gateway.fetch(host, namespace, ids)
    result = FidelityResult(namespace=namespace, sampled=len(ids))
    for record_id in ids:
        truth = expected.get(record_id)
        got = actual.get(record_id)
        if truth is None or got is None:
            result.missing.append(record_id)
            continue
        similarity = cosine(truth.dense or [], got.values)
        result.min_cosine = (
            similarity if result.min_cosine is None else min(result.min_cosine, similarity)
        )
        if similarity < cosine_threshold:
            result.vector_failures.append((record_id, similarity))
        if truth.sparse is not None:
            if got.sparse is None:
                result.vector_failures.append((record_id, -1.0))
            elif not _sparse_equal(truth.sparse, got.sparse):
                result.vector_failures.append((record_id, -2.0))
        difference = compare_metadata(truth.metadata, got.metadata)
        if difference:
            result.metadata_failures.append((record_id, difference))
    return result
