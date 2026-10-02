"""Drain the change buffer, collapse to the last op per id, then upsert and delete (ADR-007)."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pcmigrate.core.buffer import ChangeBuffer, ChangeEnvelope, Received
from pcmigrate.core.events import Event, EventBus
from pcmigrate.core.state import Stage
from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.pinecone_client import PineconeGateway, VectorRecord

UPSERT_BATCH = 200
DELETE_BATCH = 1000


class ApplierLag(Event):
    """Buffer depth and oldest message age after a drain round."""

    type: str = "applier_lag"
    depth: int = 0
    oldest_age_s: float | None = None
    applied: int = 0
    upserts: int = 0
    deletes: int = 0


def collapse(envelopes: Sequence[ChangeEnvelope]) -> dict[tuple[str, str], ChangeEnvelope]:
    """Keep the last op per (namespace, id), in arrival order."""
    last: dict[tuple[str, str], ChangeEnvelope] = {}
    for env in envelopes:
        last[(env.namespace, env.id)] = env
    return last


@dataclass
class ApplyResult:
    """What one drain round did."""

    received: int = 0
    collapsed: int = 0
    upserts: int = 0
    deletes: int = 0
    missing_rows: list[str] = field(default_factory=list)
    oldest_age_s: float | None = None


@dataclass
class Applier:
    """Applies buffered changes to the index. Idle until the sync stage says validate passed."""

    gateway: PineconeGateway
    host: str
    buffer: ChangeBuffer
    source: GroundTruthSource | None = None
    events: EventBus | None = None
    batch_size: int = 500
    clock: Callable[[], float] = time.monotonic

    def drain_once(self, *, wait_s: float = 0.0) -> ApplyResult:
        """Receive one batch, collapse, apply, acknowledge. Returns what happened."""
        received: list[Received] = []
        while len(received) < self.batch_size:
            chunk = self.buffer.receive(
                max_messages=min(10, self.batch_size - len(received)),
                wait_s=wait_s if not received else 0.0,
            )
            if not chunk:
                break
            received.extend(chunk)
        result = ApplyResult(received=len(received))
        if not received:
            return result
        now = datetime.now(UTC)
        ages = [r.age_s(now) for r in received if r.age_s(now) is not None]
        result.oldest_age_s = max(ages) if ages else None  # type: ignore[type-var]
        final = collapse([r.envelope for r in received])
        result.collapsed = len(final)
        by_namespace: dict[str, list[ChangeEnvelope]] = {}
        for env in final.values():
            by_namespace.setdefault(env.namespace, []).append(env)
        for namespace, envs in by_namespace.items():
            result.upserts += self._upsert(namespace, [e for e in envs if e.op == "upsert"], result)
            result.deletes += self._delete(namespace, [e for e in envs if e.op == "delete"])
        self.buffer.ack([r.receipt for r in received])
        if self.events is not None:
            self.events.emit(
                ApplierLag(
                    stage=Stage.SYNC,
                    depth=self.buffer.approximate_depth(),
                    oldest_age_s=result.oldest_age_s,
                    applied=result.collapsed,
                    upserts=result.upserts,
                    deletes=result.deletes,
                )
            )
        return result

    def _upsert(self, namespace: str, envs: list[ChangeEnvelope], result: ApplyResult) -> int:
        records: list[VectorRecord] = []
        reread = [e for e in envs if e.needs_reread]
        if reread:
            if self.source is None:
                raise RuntimeError("CDC envelopes need a source to re-read rows from")
            current = self.source.fetch_records(namespace, [e.id for e in reread])
            for env in reread:
                rec = current.get(env.id)
                if rec is None:
                    result.missing_rows.append(env.id)
                    continue
                records.append(VectorRecord(rec.id, rec.dense or [], rec.metadata))
        for env in envs:
            if not env.needs_reread:
                records.append(VectorRecord(env.id, env.values or [], env.metadata or {}))
        count = 0
        for start in range(0, len(records), UPSERT_BATCH):
            count += self.gateway.upsert(
                self.host, namespace, records[start : start + UPSERT_BATCH]
            )
        if result.missing_rows:
            deleted_meanwhile = [i for i in result.missing_rows if i in {e.id for e in envs}]
            if deleted_meanwhile:
                self.gateway.delete(self.host, namespace, deleted_meanwhile)
        return count

    def _delete(self, namespace: str, envs: list[ChangeEnvelope]) -> int:
        ids = [e.id for e in envs]
        count = 0
        for start in range(0, len(ids), DELETE_BATCH):
            count += self.gateway.delete(self.host, namespace, ids[start : start + DELETE_BATCH])
        return count

    def drain_until_empty(self, *, max_rounds: int = 1000, idle_rounds: int = 2) -> ApplyResult:
        """Drain until the buffer stays empty for *idle_rounds* consecutive rounds."""
        total = ApplyResult()
        idle = 0
        for _ in range(max_rounds):
            result = self.drain_once(wait_s=1.0)
            total.received += result.received
            total.collapsed += result.collapsed
            total.upserts += result.upserts
            total.deletes += result.deletes
            total.missing_rows.extend(result.missing_rows)
            if result.received == 0 and self.buffer.approximate_depth() == 0:
                idle += 1
                if idle >= idle_rounds:
                    break
            else:
                idle = 0
        return total


def forward(
    feed_poll: Callable[[int], Sequence[ChangeEnvelope]], buffer: ChangeBuffer, *, limit: int = 1000
) -> int:
    """One forwarding step: poll the feed and push into the buffer."""
    batch = feed_poll(limit)
    return buffer.send(batch) if batch else 0


def envelope_summary(envs: Sequence[ChangeEnvelope]) -> dict[str, Any]:
    """Counts by op, for logs."""
    return {
        "upserts": sum(1 for e in envs if e.op == "upsert"),
        "deletes": sum(1 for e in envs if e.op == "delete"),
    }
