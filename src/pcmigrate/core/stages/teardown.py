"""Stage `teardown`: destroy the migration's AWS stack and staged data. Never the Pinecone index."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.s3 import list_keys
from pcmigrate.core.stages import RunContext
from pcmigrate.core.stages.provision import PulumiDriver, StackDriver
from pcmigrate.core.state import Manifest, Stage

KEEP_PREFIXES = ("manifest/",)


class TeardownStage:
    """pulumi destroy, then delete staged objects unless retain_data. The index is untouched."""

    name = Stage.TEARDOWN

    def __init__(self, driver: StackDriver | None = None, *, retain_data: bool = False) -> None:
        self._driver: StackDriver = driver or PulumiDriver()
        self._retain = retain_data

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once marked complete."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Destroy infrastructure, then staged data. Keeps manifest/ for the audit trail."""
        if ctx.target is not None:
            ctx.extras["teardown_target_untouched"] = True

        def on_output(line: str) -> None:
            text = line.rstrip()
            if text:
                ctx.events.log(text, stage=self.name)

        self._driver.destroy(ctx, on_output)
        deleted = 0
        if not self._retain:
            for zone in ("raw", "canonical", "import", "rejects"):
                prefix = ctx.config.zone(zone) + "/"
                for key in list_keys(ctx.s3, ctx.config.bucket, prefix):
                    ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=key)
                    deleted += 1
        return {
            "stack_destroyed": True,
            "staged_objects_deleted": deleted,
            "retained_data": self._retain,
            "pinecone_index_touched": False,
        }
