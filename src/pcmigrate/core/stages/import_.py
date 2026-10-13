"""Stage `import`: one bulk import per emitted batch, resumed by import id after a kill."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.events import LogMessage
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.core.transform import read_duplicates
from pcmigrate.targets.applier import vector_record
from pcmigrate.targets.importer import Importer
from pcmigrate.targets.pinecone_client import VectorRecord

UPSERT_BATCH_RECORDS = 200
UPSERT_BATCH_BYTES = 1_800_000


class ImportStage:
    """Records the import id before waiting, so a resumed run polls instead of re-importing."""

    name = Stage.IMPORT

    def __init__(self, *, poll_interval_s: float = 15.0, timeout_s: float | None = None) -> None:
        self._poll = poll_interval_s
        self._timeout = timeout_s

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Start or resume every batch's import and wait for completion."""
        importer = Importer(
            ctx.require_target(),
            ctx.events,
            poll_interval_s=self._poll,
            timeout_s=self._timeout,
        )
        host = ctx.index_host()
        batches: list[dict[str, Any]] = ctx.manifest.stage(Stage.EMIT).outputs.get("batches", [])
        if not batches:
            raise RuntimeError("emit recorded no batches to import")
        record = ctx.manifest.stage(self.name)
        total = 0
        for batch in batches:
            batch_id = str(batch["id"])
            progress = ctx.manifest.partition(self.name, batch_id)
            if progress.status is StageStatus.COMPLETE:
                total += progress.rows
                continue
            import_id = record.outputs.get("import_ids", {}).get(batch_id)
            if import_id is None:
                import_id = importer.start(
                    host, str(batch["uri"]), integration_id=ctx.config.storage_integration_id
                )
                record.outputs.setdefault("import_ids", {})[batch_id] = import_id
                ctx.checkpoint()
            info = importer.wait(host, str(import_id))
            imported = info.records_imported or 0
            ctx.manifest.mark_partition_complete(self.name, batch_id, rows=imported, parts=[])
            ctx.checkpoint()
            total += imported
        upserted = self._upsert_duplicates(ctx, host)
        return {"records_imported": total, "duplicates_upserted": upserted}

    def _upsert_duplicates(self, ctx: RunContext, host: str) -> dict[str, int]:
        """ADR-011: duplicates skipped by import load through the data API afterwards."""
        if ctx.mapping.duplicates != "upsert_after_import":
            return {}
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        parts_by_namespace: dict[str, list[str]] = transform.get("duplicate_parts_by_namespace", {})
        record = ctx.manifest.stage(self.name)
        done: dict[str, int] = record.outputs.get("duplicates_upserted", {})
        gateway = ctx.require_target()
        objects = {o.namespace: o for o in ctx.mapping.objects}
        for namespace, keys in sorted(parts_by_namespace.items()):
            if not keys or namespace in done:
                continue
            obj = objects.get(namespace.split(":", 1)[0])
            text_map = obj.text_sources if obj is not None else {}
            batch: list[VectorRecord] = []
            batch_bytes = 0
            count = 0

            def flush(ns: str = namespace) -> None:
                nonlocal batch, batch_bytes, count
                if batch:
                    count += gateway.upsert(host, ns, batch)
                    batch = []
                    batch_bytes = 0

            for dup in read_duplicates(ctx.s3, ctx.config.bucket, keys):
                vector = vector_record(dup, text_map)
                size = len(vector.values) * 4 + len(str(vector.metadata)) + len(vector.id) + 64
                if batch and (
                    len(batch) >= UPSERT_BATCH_RECORDS or batch_bytes + size > UPSERT_BATCH_BYTES
                ):
                    flush()
                batch.append(vector)
                batch_bytes += size
            flush()
            done[namespace] = count
            record.outputs["duplicates_upserted"] = done
            ctx.checkpoint()
            ctx.events.emit(
                LogMessage(
                    stage=self.name, message=f"upserted {count} duplicate records into {namespace}"
                )
            )
        return done
