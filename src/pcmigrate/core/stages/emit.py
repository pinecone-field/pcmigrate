"""Stage `emit`: canonical parts to import files, planned into batches within ADR-012 limits."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.targets.emitters.vector_parquet import (
    ImportBatch,
    ImportLimits,
    NamespaceInput,
    VectorParquetEmitter,
    plan_import_batches,
)


def source_parts(ctx: RunContext) -> dict[str, list[str]]:
    """Clean parts from transform when it ran, else the raw export parts."""
    transformed = ctx.manifest.stage(Stage.TRANSFORM)
    if transformed.status is StageStatus.COMPLETE and "parts_by_namespace" in transformed.outputs:
        parts: dict[str, list[str]] = transformed.outputs["parts_by_namespace"]
        return parts
    raw: dict[str, list[str]] = ctx.manifest.stage(Stage.EXPORT).outputs.get(
        "parts_by_namespace", {}
    )
    return raw


def namespace_inputs(ctx: RunContext) -> list[NamespaceInput]:
    """Canonical parts and byte sizes per namespace."""
    parts_by_namespace = source_parts(ctx)
    inputs: list[NamespaceInput] = []
    for namespace, keys in sorted(parts_by_namespace.items()):
        total = 0
        for key in keys:
            total += int(ctx.s3.head_object(Bucket=ctx.config.bucket, Key=key)["ContentLength"])
        inputs.append(NamespaceInput(namespace=namespace, keys=tuple(keys), bytes=total))
    return inputs


class EmitStage:
    """Each batch is a partition in the manifest, so a killed emit resumes per batch."""

    name = Stage.EMIT

    def __init__(self, limits: ImportLimits | None = None) -> None:
        self._limits = limits

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def _limits_for(self, ctx: RunContext) -> ImportLimits:
        if self._limits is not None:
            return self._limits
        return ImportLimits(target_part_bytes=ctx.config.import_part_bytes)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Plan batches, then emit the ones the manifest does not show complete."""
        limits = self._limits_for(ctx)
        inputs = namespace_inputs(ctx)
        batches = plan_import_batches(inputs, limits, capacity=ctx.mapping.target.capacity)
        emitter = VectorParquetEmitter(
            ctx.s3, ctx.config.bucket, ctx.config.zone("import"), limits=limits, events=ctx.events
        )
        record = ctx.manifest.stage(self.name)
        record.outputs["batches"] = [
            {"id": b.id, "uri": emitter.batch_uri(b), "namespaces": list(b.namespaces)}
            for b in batches
        ]
        ctx.checkpoint()
        for batch in batches:
            progress = ctx.manifest.partition(self.name, batch.id)
            if progress.status is StageStatus.COMPLETE:
                continue
            self._discard_stale_batch(ctx, batch)
            result = emitter.emit(batch, inputs)
            ctx.manifest.mark_partition_complete(
                self.name, batch.id, rows=result.rows, parts=[p.key for p in result.parts]
            )
            ctx.checkpoint()
        rows_by_namespace: dict[str, int] = {}
        for batch in batches:
            progress = ctx.manifest.partition(self.name, batch.id)
            rows_by_namespace[batch.id] = progress.rows
        return {
            "files": sum(len(p.parts) for p in record.partitions.values()),
            "rows": rows_by_namespace,
        }

    @staticmethod
    def _discard_stale_batch(ctx: RunContext, batch: ImportBatch) -> None:
        from pcmigrate.core.s3 import list_keys

        prefix = f"{ctx.config.zone('import')}/{batch.id}/"
        for key in list_keys(ctx.s3, ctx.config.bucket, prefix):
            ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=key)
