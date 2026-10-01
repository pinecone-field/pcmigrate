"""Stage `validate`: run every approved check and write validation.md and validation.json."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.artifacts import S3ArtifactStore
from pcmigrate.core.events import ValidationResult
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage
from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.validate.counts import CountExpectation, check_counts
from pcmigrate.validate.fidelity import check_fidelity
from pcmigrate.validate.filters import check_filters
from pcmigrate.validate.id_diff import check_id_diff
from pcmigrate.validate.recall import check_recall
from pcmigrate.validate.report import CheckOutcome, ValidationReport


class ValidationFailedError(RuntimeError):
    """A check missed its approved threshold."""


def _is_ground_truth(source: object) -> bool:
    return all(
        hasattr(source, m)
        for m in ("sample_ids", "fetch_records", "knn", "all_ids", "common_filters")
    )


class ValidateStage:
    """Fails the run when any check fails; the manifest and validation.json keep the numbers."""

    name = Stage.VALIDATE

    def __init__(
        self,
        *,
        settle_window_s: float = 180.0,
        poll_interval_s: float = 5.0,
        filter_queries: int = 20,
        checks: tuple[str, ...] = ("counts", "fidelity", "recall", "filters", "id_diff"),
    ) -> None:
        self._window = settle_window_s
        self._poll = poll_interval_s
        self._filter_queries = filter_queries
        self._checks = checks

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def _expectations(self, ctx: RunContext) -> list[CountExpectation]:
        exported: dict[str, int] = ctx.manifest.stage(Stage.EXPORT).outputs.get(
            "rows_by_namespace", {}
        )
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        rejected: dict[str, int] = transform.get("rejected_by_namespace", {})
        duplicates: dict[str, int] = transform.get("duplicates_by_namespace", {})
        upserted: dict[str, int] = ctx.manifest.stage(Stage.IMPORT).outputs.get(
            "duplicates_upserted", {}
        )
        expectations = []
        for namespace, rows in sorted(exported.items()):
            deferred = duplicates.get(namespace, 0) - upserted.get(namespace, 0)
            expectations.append(
                CountExpectation(
                    namespace,
                    rows,
                    rejected=rejected.get(namespace, 0),
                    duplicates_deferred=max(0, deferred),
                )
            )
        return expectations

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Run the checks the approved mapping asks for and write the report."""
        gateway = ctx.require_target()
        host = ctx.index_host()
        report = ValidationReport(
            migration_id=ctx.config.migration_id, index=ctx.mapping.target.index
        )
        expectations = self._expectations(ctx)
        imported = ctx.manifest.stage(Stage.IMPORT).outputs.get("records_imported")

        if "counts" in self._checks:
            counts = check_counts(
                gateway,
                host,
                expectations,
                records_imported=None if imported is None else int(imported),
                settle_window_s=self._window,
                poll_interval_s=self._poll,
            )
            report.outcomes.append(
                CheckOutcome("count_parity", "*", counts.passed, counts.to_dict())
            )

        source = ctx.source
        validation = ctx.mapping.validation
        if source is not None and _is_ground_truth(source):
            truth: GroundTruthSource = source  # type: ignore[assignment]
            for expectation in expectations:
                namespace = expectation.namespace
                if "fidelity" in self._checks:
                    fidelity = check_fidelity(
                        gateway, host, truth, namespace, sample=validation.fidelity_sample
                    )
                    report.outcomes.append(
                        CheckOutcome(
                            "vector_fidelity",
                            namespace,
                            fidelity.vectors_passed,
                            fidelity.to_dict(),
                        )
                    )
                    report.outcomes.append(
                        CheckOutcome(
                            "metadata_fidelity",
                            namespace,
                            fidelity.metadata_passed,
                            fidelity.to_dict(),
                        )
                    )
                if "recall" in self._checks:
                    recall = check_recall(
                        gateway,
                        host,
                        truth,
                        namespace,
                        k=validation.recall_at_k.k,
                        queries=validation.recall_at_k.queries,
                        threshold=validation.recall_at_k.threshold,
                    )
                    report.outcomes.append(
                        CheckOutcome("recall_at_k", namespace, recall.passed, recall.to_dict())
                    )
                if "filters" in self._checks:
                    parity = check_filters(
                        gateway,
                        host,
                        truth,
                        namespace,
                        k=validation.recall_at_k.k,
                        filters=self._filter_queries,
                    )
                    report.outcomes.append(
                        CheckOutcome("filter_parity", namespace, parity.passed, parity.to_dict())
                    )
                if "id_diff" in self._checks:
                    diff = check_id_diff(gateway, host, truth, namespace)
                    report.outcomes.append(
                        CheckOutcome("id_diff", namespace, diff.passed, diff.to_dict())
                    )

        for outcome in report.outcomes:
            ctx.events.emit(
                ValidationResult(
                    stage=self.name,
                    check=outcome.check,
                    passed=outcome.passed,
                    details=outcome.details,
                )
            )
        store = S3ArtifactStore(ctx.s3, ctx.config.bucket, ctx.config.zone("manifest"))
        store.write_text("validation.json", report.to_json())
        store.write_text("validation.md", report.to_markdown())
        summary = {
            o.check if o.namespace == "*" else f"{o.check}:{o.namespace}": o.passed
            for o in report.outcomes
        }
        if not report.passed:
            failed = ", ".join(f"{o.check}[{o.namespace}]" for o in report.failures())
            raise ValidationFailedError(
                f"validation failed: {failed}; see {store.location('validation.md')}"
            )
        return {"passed": True, "checks": summary, "report": store.location("validation.json")}
