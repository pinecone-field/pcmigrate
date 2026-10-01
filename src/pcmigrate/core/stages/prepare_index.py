"""Stage `prepare_index`: create or adopt the target index and verify it is ready for import."""

from __future__ import annotations

from typing import Any

from pcmigrate.config.mapping import DenseSpec, Mapping
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage
from pcmigrate.targets.index_manager import IndexManager


def shared_dense_spec(mapping: Mapping) -> DenseSpec:
    """The one dense spec every object agrees on; a vector index has a single dimension."""
    specs = [o.dense for o in mapping.objects if o.dense is not None]
    if not specs:
        raise ValueError("no object in the mapping has a dense vector")
    first = specs[0]
    for spec in specs[1:]:
        if (spec.dimension, spec.metric) != (first.dimension, first.metric):
            raise ValueError(
                "objects disagree on dimension or metric; a vector index needs one of each"
            )
    return first


class PrepareIndexStage:
    """Idempotent: an index created by an earlier attempt is verified, not recreated."""

    name = Stage.PREPARE_INDEX

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the manifest holds a host."""
        record = manifest.stage(self.name)
        return manifest.is_complete(self.name) and bool(record.outputs.get("host"))

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Create or adopt per mapping.target and record name, host, and mode."""
        manager = IndexManager(ctx.require_target(), ctx.events)
        info = manager.prepare(
            ctx.mapping.target, shared_dense_spec(ctx.mapping), ctx.mapping.namespaces
        )
        return {
            "index": info.name,
            "host": info.host,
            "mode": ctx.mapping.target.mode,
            "dimension": info.dimension,
            "metric": info.metric,
        }
