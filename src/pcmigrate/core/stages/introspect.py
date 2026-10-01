"""Stage `introspect`: read-only discovery. Records the schema fingerprint in the manifest."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage


class IntrospectStage:
    """Runs the connector's introspect() and records the fingerprint plan.lock was built from."""

    name = Stage.INTROSPECT

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once a fingerprint is recorded."""
        record = manifest.stage(self.name)
        return manifest.is_complete(self.name) and bool(record.outputs.get("schema_fingerprint"))

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Introspect and record the fingerprint."""
        schema = ctx.require_source().introspect()
        return {"schema_fingerprint": schema.fingerprint, "kind": schema.kind}
