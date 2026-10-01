"""Stage `transform`: pre-validate canonical records, split rejects and duplicates (ADR-011)."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.s3 import list_keys
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.core.transform import transform_namespace


class TransformStage:
    """One manifest partition per namespace, so a killed transform resumes per namespace."""

    name = Stage.TRANSFORM

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once the stage is marked complete."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Validate every namespace's canonical parts into clean, rejects, and duplicates."""
        export_outputs = ctx.manifest.stage(Stage.EXPORT).outputs
        parts_by_namespace: dict[str, list[str]] = export_outputs.get("parts_by_namespace", {})
        objects = {o.namespace: o for o in ctx.mapping.objects}
        record = ctx.manifest.stage(self.name)
        for namespace, keys in sorted(parts_by_namespace.items()):
            progress = ctx.manifest.partition(self.name, namespace)
            if progress.status is StageStatus.COMPLETE:
                continue
            obj = objects.get(namespace.split(":", 1)[0])
            if obj is None:
                raise RuntimeError(f"no mapping object for namespace {namespace!r}")
            prefixes = {
                "clean": f"{ctx.config.zone('canonical')}/_clean/{namespace}",
                "rejects": f"{ctx.config.zone('rejects')}/{namespace}",
                "duplicates": f"{ctx.config.zone('manifest')}/duplicates/{namespace}",
            }
            for prefix in prefixes.values():
                for stale in list_keys(ctx.s3, ctx.config.bucket, prefix + "/"):
                    ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=stale)
            result = transform_namespace(
                ctx.s3,
                ctx.config.bucket,
                namespace=namespace,
                obj=obj,
                canonical_keys=keys,
                clean_prefix=prefixes["clean"],
                rejects_prefix=prefixes["rejects"],
                duplicates_prefix=prefixes["duplicates"],
                drop_duplicates=ctx.mapping.duplicates == "drop",
                target_part_bytes=ctx.config.canonical_part_bytes,
            )
            record.outputs.setdefault("namespaces", {})[namespace] = result.to_dict()
            ctx.manifest.mark_partition_complete(
                self.name, namespace, rows=result.clean_rows, parts=result.clean_parts
            )
            ctx.checkpoint()
        namespaces: dict[str, dict[str, Any]] = record.outputs.get("namespaces", {})
        return {
            "rows_by_namespace": {ns: v["clean_rows"] for ns, v in namespaces.items()},
            "rejected_by_namespace": {ns: v["rejected_rows"] for ns, v in namespaces.items()},
            "duplicates_by_namespace": {ns: v["duplicate_rows"] for ns, v in namespaces.items()},
            "parts_by_namespace": {ns: v["clean_parts"] for ns, v in namespaces.items()},
            "duplicate_parts_by_namespace": {
                ns: v["duplicate_parts"] for ns, v in namespaces.items()
            },
            "reject_parts_by_namespace": {ns: v["reject_parts"] for ns, v in namespaces.items()},
        }
