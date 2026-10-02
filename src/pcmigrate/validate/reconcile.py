"""Reconcile: full id diff with the source, then upsert what is missing and delete what is extra."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway, VectorRecord
from pcmigrate.validate.id_diff import IdDiffResult, check_id_diff

UPSERT_BATCH = 200
DELETE_BATCH = 1000


@dataclass
class ReconcileResult:
    """What reconcile found and did."""

    namespace: str
    diff: IdDiffResult
    upserted: int = 0
    deleted: int = 0
    unresolvable: list[str] = field(default_factory=list)

    @property
    def converged(self) -> bool:
        """Every difference was repaired."""
        return not self.unresolvable

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "missing_before": len(self.diff.missing),
            "extra_before": len(self.diff.extra),
            "upserted": self.upserted,
            "deleted": self.deleted,
            "unresolvable": self.unresolvable[:50],
            "converged": self.converged,
        }


def reconcile(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    *,
    apply: bool = True,
) -> ReconcileResult:
    """Diff, then repair. With apply=False only report."""
    diff = check_id_diff(gateway, host, source, namespace)
    result = ReconcileResult(namespace=namespace, diff=diff)
    if not apply:
        result.unresolvable = diff.missing + diff.extra
        return result
    for start in range(0, len(diff.missing), UPSERT_BATCH):
        wanted = diff.missing[start : start + UPSERT_BATCH]
        records = source.fetch_records(namespace, wanted)
        batch = [VectorRecord(r.id, r.dense or [], r.metadata) for r in records.values()]
        if batch:
            result.upserted += gateway.upsert(host, namespace, batch)
        result.unresolvable.extend(i for i in wanted if i not in records)
    for start in range(0, len(diff.extra), DELETE_BATCH):
        result.deleted += gateway.delete(host, namespace, diff.extra[start : start + DELETE_BATCH])
    return result
