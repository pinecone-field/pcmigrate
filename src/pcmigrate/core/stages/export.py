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
        """For pgvector, open the shared snapshot and record it with the change boundary.

        With the trigger outbox the triggers are installed first and the outbox position is
        read inside the snapshot. With logical replication the slot is created with
        EXPORT_SNAPSHOT and the coordinator attaches to that snapshot, so export and CDC share
        one boundary (ADR-008). Other sources need none of this.
        """
        dsn = getattr(source, "dsn", None)
        if not isinstance(dsn, str):
            return None
        from pcmigrate.sources.pgvector.change_feed import LogicalFeed, make_feed
        from pcmigrate.sources.pgvector.snapshot import SnapshotCoordinator

        watermarks: dict[str, str] = {}
        for obj in ctx.mapping.objects:
            for spec in obj.metadata:
                if spec.action == "map" and spec.source in ("updated_at", "modified_at"):
                    watermarks[obj.source] = spec.source
        record = ctx.manifest.stage(Stage.EXPORT)
        feed = make_feed(dsn, ctx.mapping)
        install_outputs: dict[str, Any] = {}
        external: tuple[str, str] | None = None
        slot_conn: Any = None
        if feed is not None and "feed_installed" not in record.outputs:
            install_outputs = feed.install()
            record.outputs["feed_installed"] = {"tier": feed.tier, **install_outputs}
            ctx.checkpoint()
        if isinstance(feed, LogicalFeed) and "slot_created" not in record.outputs:
            slot_conn, snapshot_name, consistent_point = feed.create_slot_with_snapshot()
            external = (snapshot_name, consistent_point)
            record.outputs["slot_created"] = {
                "slot": feed.slot,
                "consistent_point": consistent_point,
            }
        coordinator = SnapshotCoordinator(dsn)
        info = coordinator.open(
            watermarks,
            boundary_reader=feed.boundary if feed is not None else None,
            external_snapshot=external,
        )
        coordinator.slot_connection = slot_conn  # type: ignore[attr-defined]
        source.snapshot_id = info.snapshot_id  # type: ignore[attr-defined]
        record.outputs.setdefault("snapshots", []).append(info.model_dump(mode="json"))
        record.outputs.setdefault("snapshot_lsn", info.wal_lsn)
        record.outputs.setdefault("watermarks", info.watermarks)
        if info.change_boundary and "change_boundary" not in record.outputs:
            record.outputs["change_boundary"] = info.change_boundary
        ctx.checkpoint()
        return _SnapshotHandle(coordinator, slot_conn)


class _SnapshotHandle:
    """Closes the coordinator and, for logical replication, the slot connection."""

    def __init__(self, coordinator: Any, slot_conn: Any) -> None:
        self._coordinator = coordinator
        self._slot_conn = slot_conn

    def close(self) -> None:
        self._coordinator.close()
        if self._slot_conn is not None:
            self._slot_conn.close()
