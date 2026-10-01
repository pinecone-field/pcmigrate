"""Stage `import`: one bulk import per emitted batch, resumed by import id after a kill."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.targets.importer import Importer


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
        return {"records_imported": total}
