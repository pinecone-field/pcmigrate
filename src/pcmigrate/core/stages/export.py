"""Stage `export`: stream every partition into canonical Parquet parts under canonical/.

Partitions run in a process pool when the source is picklable (pgvector), else in threads.
Each partition is its own checkpoint. A pgvector source gets one shared snapshot for the
attempt, recorded in the manifest with the WAL position and updated_at watermarks.
"""

from __future__ import annotations

import threading
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any

from pcmigrate.core.canonical import CanonicalWriter, PartInfo
from pcmigrate.core.events import PartitionDone
from pcmigrate.core.s3 import list_keys, make_client
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.sources.base import Partition, SourceConnector


def partition_prefix(ctx: RunContext, partition: Partition) -> str:
    """Where one partition's canonical parts live."""
    return f"{ctx.config.zone('canonical')}/{partition.object}/{partition.id}"


def export_partition(
    source: SourceConnector,
    partition: Partition,
    *,
    bucket: str,
    prefix: str,
    region: str,
    target_part_bytes: int,
) -> tuple[int, list[PartInfo]]:
    """Export one partition into fresh parts. Runs in a worker process or thread."""
    s3 = make_client(region)
    for key in list_keys(s3, bucket, prefix + "/"):
        s3.delete_object(Bucket=bucket, Key=key)
    writer = CanonicalWriter(s3, bucket, prefix, target_part_bytes=target_part_bytes)
    try:
        rows = source.export(partition, writer)
    except Exception:
        writer.abort()
        raise
    return rows, writer.close()


class ExportStage:
    """Partitions complete in the manifest are skipped; incomplete ones restart from scratch."""

    name = Stage.EXPORT

    def __init__(self, *, workers: int | None = None, use_processes: bool | None = None) -> None:
        self._workers = workers
        self._use_processes = use_processes

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def _executor(self, source: SourceConnector, workers: int) -> Executor:
        use_processes = self._use_processes
        if use_processes is None:
            use_processes = hasattr(source, "dsn")
        if use_processes:
            return ProcessPoolExecutor(max_workers=workers)
        return ThreadPoolExecutor(max_workers=workers)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Export incomplete partitions in parallel, checkpointing after each one."""
        source = ctx.require_source()
        record = ctx.manifest.stage(self.name)
        snapshot = self._open_snapshot(ctx, source)
        try:
            partitions = source.plan_partitions(ctx.mapping)
            record.outputs["partitions"] = [p.id for p in partitions]
            ctx.checkpoint()
            pending = [
                p
                for p in partitions
                if ctx.manifest.partition(self.name, p.id).status is not StageStatus.COMPLETE
            ]
            guard = threading.Lock()
            workers = self._workers or ctx.config.workers
            with self._executor(source, workers) as pool:
                futures: dict[Future[tuple[int, list[PartInfo]]], Partition] = {
                    pool.submit(
                        export_partition,
                        source,
                        partition,
                        bucket=ctx.config.bucket,
                        prefix=partition_prefix(ctx, partition),
                        region=ctx.config.aws_region,
                        target_part_bytes=ctx.config.canonical_part_bytes,
                    ): partition
                    for partition in pending
                }
                for future in futures:
                    partition = futures[future]
                    rows, parts = future.result()
                    with guard:
                        ctx.manifest.mark_partition_complete(
                            self.name, partition.id, rows=rows, parts=[p.key for p in parts]
                        )
                        ctx.checkpoint()
                        ctx.events.emit(
                            PartitionDone(
                                stage=self.name,
                                partition=partition.id,
                                rows=rows,
                                parts=[p.key for p in parts],
                            )
                        )
        finally:
            if snapshot is not None:
                snapshot.close()

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
    def _open_snapshot(ctx: RunContext, source: SourceConnector) -> Any:
        """For pgvector, open the shared snapshot and record it; other sources need none."""
        dsn = getattr(source, "dsn", None)
        if not isinstance(dsn, str):
            return None
        from pcmigrate.sources.pgvector.snapshot import SnapshotCoordinator

        watermarks: dict[str, str] = {}
        for obj in ctx.mapping.objects:
            for spec in obj.metadata:
                if spec.action == "map" and spec.source in ("updated_at", "modified_at"):
                    watermarks[obj.source] = spec.source
        coordinator = SnapshotCoordinator(dsn)
        info = coordinator.open(watermarks)
        source.snapshot_id = info.snapshot_id  # type: ignore[attr-defined]
        record = ctx.manifest.stage(Stage.EXPORT)
        record.outputs.setdefault("snapshots", []).append(info.model_dump(mode="json"))
        record.outputs.setdefault("snapshot_lsn", info.wal_lsn)
        record.outputs.setdefault("watermarks", info.watermarks)
        ctx.checkpoint()
        return coordinator
