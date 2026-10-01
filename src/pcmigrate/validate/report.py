"""Render validation.md and validation.json from check results."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class CheckOutcome:
    """One check, one namespace."""

    check: str
    namespace: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """All outcomes for the run."""

    migration_id: str
    index: str
    outcomes: list[CheckOutcome] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def passed(self) -> bool:
        """Every check passed."""
        return all(o.passed for o in self.outcomes)

    def failures(self) -> list[CheckOutcome]:
        """The checks that missed."""
        return [o for o in self.outcomes if not o.passed]

    def to_json(self) -> str:
        """validation.json."""
        return (
            json.dumps(
                {
                    "migration_id": self.migration_id,
                    "index": self.index,
                    "generated_at": self.generated_at.isoformat(),
                    "passed": self.passed,
                    "checks": [
                        {
                            "check": o.check,
                            "namespace": o.namespace,
                            "passed": o.passed,
                            "details": o.details,
                        }
                        for o in self.outcomes
                    ],
                },
                indent=2,
                default=str,
            )
            + "\n"
        )

    def to_markdown(self) -> str:
        """validation.md."""
        lines = [
            f"# Validation: {self.migration_id} -> {self.index}",
            "",
            f"Generated {self.generated_at.strftime('%Y-%m-%d %H:%M UTC')}. "
            + (
                "**All checks passed.**"
                if self.passed
                else f"**{len(self.failures())} check(s) failed.**"
            ),
            "",
            "| Check | Namespace | Result | Detail |",
            "| --- | --- | --- | --- |",
        ]
        for o in self.outcomes:
            lines.append(
                f"| {o.check} | {o.namespace} | {'pass' if o.passed else 'FAIL'} | {_summary(o)} |"
            )
        lines.append("")
        return "\n".join(lines)


def _summary(outcome: CheckOutcome) -> str:
    d = outcome.details
    if outcome.check == "count_parity":
        return ", ".join(
            f"{n['namespace']} {n['actual']}/{n['expected']}" for n in d.get("namespaces", [])
        )
    if outcome.check == "vector_fidelity":
        return f"{d.get('sampled')} sampled, min cosine {d.get('min_cosine')}, {d.get('vector_failure_count')} below threshold, {d.get('missing_count')} missing"
    if outcome.check == "metadata_fidelity":
        return f"{d.get('sampled')} sampled, {d.get('metadata_failure_count')} differ"
    if outcome.check == "recall_at_k":
        return f"mean recall@{d.get('k')} {d.get('mean')} over {d.get('queries')} queries (threshold {d.get('threshold')})"
    if outcome.check == "filter_parity":
        return f"{d.get('filters')} filters, {d.get('failed')} differ"
    if outcome.check == "id_diff":
        return f"{d.get('missing_count')} missing, {d.get('extra_count')} extra of {d.get('source_count')}"
    return ""
