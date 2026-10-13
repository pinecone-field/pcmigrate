"""Stage `cutover`: shadow reads, then the final drain, feed removal, and reconcile."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.artifacts import S3ArtifactStore
from pcmigrate.core.events import LogMessage, ValidationResult
from pcmigrate.core.stages import RunContext
from pcmigrate.core.stages.sync import (
    change_capture_enabled,
    resolve_buffer,
    resolve_enrichers,
    resolve_feed,
)
from pcmigrate.core.state import Manifest, Stage
from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.targets.applier import Applier, forward
from pcmigrate.validate.reconcile import reconcile
from pcmigrate.validate.shadow import shadow_read


class CutoverNotConvergedError(RuntimeError):
    """Reconcile could not repair every difference."""


def run_shadow(ctx: RunContext, *, queries: int) -> dict[str, Any]:
    """`cutover --shadow N`: sampled queries on both systems, overlap and latency per namespace."""
    source = ctx.source
    if source is None or not hasattr(source, "knn"):
        raise RuntimeError("shadow reads need a ground-truth source")
    truth: GroundTruthSource = source  # type: ignore[assignment]
    k = ctx.mapping.validation.recall_at_k.k
    results = {
        ns: shadow_read(
            ctx.require_target(), ctx.index_host(), truth, ns, queries=queries, k=k
        ).to_dict()
        for ns in ctx.mapping.namespaces
    }
    record = ctx.manifest.stage(Stage.CUTOVER)
    record.outputs["shadow"] = results
    ctx.checkpoint()
    for ns, result in results.items():
        ctx.events.emit(
            ValidationResult(stage=Stage.CUTOVER, check=f"shadow:{ns}", passed=True, details=result)
        )
    return results


class CutoverStage:
    """`cutover --confirm`: drain, stop capture, reconcile. Tier none runs only the reconcile."""

    name = Stage.CUTOVER

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once reconcile converged."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Final convergence."""
        source = ctx.source
        if source is None or not hasattr(source, "all_ids"):
            raise RuntimeError("cutover needs a ground-truth source for the final reconcile")
        truth: GroundTruthSource = source  # type: ignore[assignment]
        gateway = ctx.require_target()
        host = ctx.index_host()
        outputs: dict[str, Any] = {
            "change_capture": ctx.mapping.change_capture.tier
            if ctx.mapping.change_capture
            else "none"
        }
        enrichers = resolve_enrichers(ctx)
        if change_capture_enabled(ctx):
            buffer = resolve_buffer(ctx)
            feed = resolve_feed(ctx)
            applier = Applier(
                gateway, host, buffer, source=truth, events=ctx.events, enrichers=enrichers
            )
            forwarded = forward(feed.poll, buffer) if feed is not None else 0
            drained = applier.drain_until_empty()
            outputs["final_drain"] = {
                "forwarded": forwarded,
                "applied": drained.collapsed,
                "upserts": drained.upserts,
                "deletes": drained.deletes,
            }
            if feed is not None:
                feed.uninstall()
                outputs["feed_removed"] = feed.tier
            ctx.events.emit(
                LogMessage(
                    stage=self.name,
                    message=f"change capture stopped after draining {drained.collapsed} changes",
                )
            )
        results = {
            ns: reconcile(gateway, host, truth, ns, enricher=enrichers.get(ns)).to_dict()
            for ns in ctx.mapping.namespaces
        }
        outputs["reconcile"] = results
        store = S3ArtifactStore(ctx.s3, ctx.config.bucket, ctx.config.zone("manifest"))
        import json

        store.write_text("cutover.json", json.dumps(outputs, indent=2, default=str) + "\n")
        for ns, result in results.items():
            ctx.events.emit(
                ValidationResult(
                    stage=self.name,
                    check=f"reconcile:{ns}",
                    passed=bool(result["converged"]),
                    details=result,
                )
            )
        failed = [ns for ns, r in results.items() if not r["converged"]]
        if failed:
            raise CutoverNotConvergedError(
                f"reconcile left differences in {failed}; see {store.location('cutover.json')}"
            )
        return outputs
