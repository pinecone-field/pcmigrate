"""Stage `validate`: run the approved checks. Phase 1 ships count parity only."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.events import ValidationResult
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage
from pcmigrate.validate.counts import CountExpectation, check_counts


class ValidationFailedError(RuntimeError):
    """A check missed its approved threshold."""


class ValidateStage:
    """Fails the run when any check fails; the manifest keeps the numbers."""

    name = Stage.VALIDATE

    def __init__(self, *, settle_window_s: float = 180.0, poll_interval_s: float = 5.0) -> None:
        self._window = settle_window_s
        self._poll = poll_interval_s

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Count parity between exported rows and namespace stats."""
        exported: dict[str, int] = ctx.manifest.stage(Stage.EXPORT).outputs.get(
            "rows_by_namespace", {}
        )
        expectations = [CountExpectation(ns, rows) for ns, rows in sorted(exported.items())]
        imported = ctx.manifest.stage(Stage.IMPORT).outputs.get("records_imported")
        result = check_counts(
            ctx.require_target(),
            ctx.index_host(),
            expectations,
            records_imported=None if imported is None else int(imported),
            settle_window_s=self._window,
            poll_interval_s=self._poll,
        )
        ctx.events.emit(
            ValidationResult(
                stage=self.name,
                check="count_parity",
                passed=result.passed,
                details=result.to_dict(),
            )
        )
        if not result.passed:
            mismatches = [
                f"{n.namespace} expected {n.expected} got {n.actual}"
                for n in result.namespaces
                if not n.passed
            ]
            detail = ", ".join(mismatches) if mismatches else "counts never settled in the window"
            raise ValidationFailedError(f"count parity failed: {detail}")
        return {"count_parity": result.to_dict()}
