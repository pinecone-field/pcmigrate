"""Stage `export`: stream every partition into canonical Parquet parts under canonical/."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pcmigrate.core.canonical import CanonicalWriter
from pcmigrate.core.events import PartitionDone
from pcmigrate.core.s3 import list_keys
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.sources.base import Partition


def partition_prefix(ctx: RunContext, partition: Partition) -> str:
    """Where one partition's canonical parts live."""
    return f"{ctx.config.zone('canonical')}/{partition.object}/{partition.id}"


class ExportStage:
    """Partitions complete in the manifest are skipped; incomplete ones restart from scratch."""

    name = Stage.EXPORT

    def __init__(self, *, workers: int | None = None) -> None:
        self._workers = workers

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Export incomplete partitions in parallel, checkpointing after each one."""
        source = ctx.require_source()
        partitions = source.plan_partitions(ctx.mapping)
        record = ctx.manifest.stage(self.name)
        record.outputs["partitions"] = [p.id for p in partitions]
        ctx.checkpoint()
        pending = [
            p
            for p in partitions
            if ctx.manifest.partition(self.name, p.id).status is not StageStatus.COMPLETE
        ]
        guard = threading.Lock()

        def work(partition: Partition) -> None:
            prefix = partition_prefix(ctx, partition)
            self._discard_stale_parts(ctx, prefix)
            writer = CanonicalWriter(
                ctx.s3,
                ctx.config.bucket,
                prefix,
                target_part_bytes=ctx.config.canonical_part_bytes,
            )
            with writer:
                rows = source.export(partition, writer)
            parts = writer.close()
            with guard:
                ctx.manifest.mark_partition_complete(
                    self.name, partition.id, rows=rows, parts=[p.key for p in parts]
                )
                ctx.manifest.partition(self.name, partition.id).error = None
                ctx.checkpoint()
                ctx.events.emit(
                    PartitionDone(
                        stage=self.name,
                        partition=partition.id,
                        rows=rows,
                        parts=[p.key for p in parts],
                    )
                )

        workers = self._workers or ctx.config.workers
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for _ in pool.map(work, pending):
                pass

        rows_by_namespace: dict[str, int] = {}
        parts_by_namespace: dict[str, list[str]] = {}
        for partition in partitions:
            progress = ctx.manifest.partition(self.name, partition.id)
            rows_by_namespace[partition.namespace] = (
                rows_by_namespace.get(partition.namespace, 0) + progress.rows
            )
            parts_by_namespace.setdefault(partition.namespace, []).extend(progress.parts)
        return {
            "rows": sum(rows_by_namespace.values()),
            "rows_by_namespace": rows_by_namespace,
            "parts_by_namespace": parts_by_namespace,
        }

    @staticmethod
    def _discard_stale_parts(ctx: RunContext, prefix: str) -> None:
        stale = list_keys(ctx.s3, ctx.config.bucket, prefix + "/")
        for key in stale:
            ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=key)
