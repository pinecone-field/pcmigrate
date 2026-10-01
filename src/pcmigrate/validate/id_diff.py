"""Full id-set diff between the source and each target namespace."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class IdDiffResult:
    """Missing and extra ids."""

    namespace: str
    source_count: int
    target_count: int
    missing: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Zero missing, zero extra."""
        return not self.missing and not self.extra

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly, with samples of the differences."""
        return {
            "namespace": self.namespace,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "missing_count": len(self.missing),
            "extra_count": len(self.extra),
            "missing_sample": self.missing[:50],
            "extra_sample": self.extra[:50],
            "passed": self.passed,
        }


def check_id_diff(
    gateway: PineconeGateway, host: str, source: GroundTruthSource, namespace: str
) -> IdDiffResult:
    """Compare every id. Holds both id sets in memory; fine up to tens of millions."""
    source_ids = set(source.all_ids(namespace))
    target_ids = set(gateway.list_ids(host, namespace))
    return IdDiffResult(
        namespace=namespace,
        source_count=len(source_ids),
        target_count=len(target_ids),
        missing=sorted(source_ids - target_ids),
        extra=sorted(target_ids - source_ids),
    )
