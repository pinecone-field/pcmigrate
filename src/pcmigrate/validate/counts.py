"""Count parity with a settle-and-retry window on namespace stats."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass(frozen=True, slots=True)
class CountExpectation:
    """Source-side numbers for one namespace. Rejects and deferred duplicates never import."""

    namespace: str
    exported: int
    rejected: int = 0
    duplicates_deferred: int = 0

    @property
    def expected(self) -> int:
        """Records the import should have produced."""
        return self.exported - self.rejected - self.duplicates_deferred


@dataclass(frozen=True, slots=True)
class NamespaceCount:
    """Expected versus observed for one namespace."""

    namespace: str
    expected: int
    actual: int

    @property
    def passed(self) -> bool:
        """True when the numbers agree."""
        return self.expected == self.actual


@dataclass
class CountParityResult:
    """Outcome of the count check."""

    passed: bool
    stable: bool
    polls: int
    namespaces: list[NamespaceCount] = field(default_factory=list)
    records_imported: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly form for manifests and events."""
        return {
            "passed": self.passed,
            "stable": self.stable,
            "polls": self.polls,
            "records_imported": self.records_imported,
            "namespaces": [
                {"namespace": n.namespace, "expected": n.expected, "actual": n.actual}
                for n in self.namespaces
            ],
        }


def _compare(
    expectations: Sequence[CountExpectation], counts: Mapping[str, int]
) -> list[NamespaceCount]:
    return [
        NamespaceCount(e.namespace, e.expected, counts.get(e.namespace, 0)) for e in expectations
    ]


def check_counts(
    gateway: PineconeGateway,
    host: str,
    expectations: Sequence[CountExpectation],
    *,
    records_imported: int | None = None,
    settle_window_s: float = 180.0,
    poll_interval_s: float = 5.0,
    stable_reads: int = 2,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> CountParityResult:
    """Poll namespace stats until they match and hold still, or the window closes.

    Stats lag writes, so a single read is not evidence. The check passes once ``stable_reads``
    consecutive polls return identical counts that equal the expectations. It fails when the
    window closes, reporting the last counts seen and whether they had settled.
    """
    deadline = clock() + settle_window_s
    history: list[dict[str, int]] = []
    polls = 0
    while True:
        counts = dict(gateway.namespace_counts(host))
        polls += 1
        history.append(counts)
        recent = history[-stable_reads:]
        stable = len(recent) == stable_reads and all(r == recent[0] for r in recent)
        comparison = _compare(expectations, counts)
        matched = all(n.passed for n in comparison)
        if matched and stable:
            return CountParityResult(True, True, polls, comparison, records_imported)
        if clock() >= deadline:
            return CountParityResult(False, stable, polls, comparison, records_imported)
        sleep(poll_interval_s)
