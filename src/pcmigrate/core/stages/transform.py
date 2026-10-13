"""Stage `transform`: pre-validate canonical records, split rejects and duplicates (ADR-011).

Rejects land under ``rejects/<namespace>/transform/``; the export paths keep their own
``direct/`` and ``glue/`` sub-prefixes there, which this stage never clears.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import SparseModelSpec, SparseSpec
from pcmigrate.core.events import SparseEncoded
from pcmigrate.core.s3 import list_keys
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.core.transform import (
    SparseCache,
    SparseEnricher,
    SparseStats,
    transform_namespace,
)

if TYPE_CHECKING:
    from pcmigrate.targets.sparse_encoder import SparseEncoder

SPARSE_CACHE_ZONE = "sparse_cache"


def resolve_sparse_encoder(ctx: RunContext, model: SparseModelSpec) -> SparseEncoder:
    """The encoder attached to the run context, or the provider's client for *model*.

    Tests attach one on ``ctx.extras["sparse_encoder"]``. Otherwise the ``pinecone``
    provider takes the migration's Pinecone key (``ctx.extras["pinecone_api_key"]``) unless
    the model names its own secret or the settings name an environment variable.
    """
    attached = ctx.extras.get("sparse_encoder")
    if attached is not None:
        return attached  # type: ignore[no-any-return]
    from pcmigrate.targets import PineconeSparseEncoder, api_key_from_secret

    api_key = ctx.extras.get("embedding_api_key")
    if not api_key and model.api_key_secret:
        api_key = api_key_from_secret(model.api_key_secret, ctx.config.aws_region)
    if not api_key:
        api_key = ctx.extras.get("pinecone_api_key")
    if not api_key:
        raise RuntimeError(
            "generated sparse vectors need an API key: attach ctx.extras['pinecone_api_key'] "
            "or set sparse.model.api_key_secret"
        )
    return PineconeSparseEncoder.from_spec(str(api_key), model, workers=ctx.config.workers)


def sparse_enricher(ctx: RunContext, spec: SparseSpec | None) -> SparseEnricher | None:
    """An enricher for a generated sparse spec, or None for stored and absent ones."""
    if spec is None or not spec.generated or spec.model is None:
        return None
    return SparseEnricher(spec, resolve_sparse_encoder(ctx, spec.model))


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
                "rejects": f"{ctx.config.zone('rejects')}/{namespace}/transform",
                "duplicates": f"{ctx.config.zone('manifest')}/duplicates/{namespace}",
            }
            for prefix in prefixes.values():
                for stale in list_keys(ctx.s3, ctx.config.bucket, prefix + "/"):
                    ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=stale)
            enricher = sparse_enricher(ctx, obj.sparse)
            cache = (
                SparseCache(
                    ctx.s3,
                    ctx.config.bucket,
                    f"{ctx.config.zone('manifest')}/{SPARSE_CACHE_ZONE}/{namespace}",
                )
                if enricher is not None
                else None
            )

            def on_part(
                key: str, rows: int, stats: SparseStats, namespace: str = namespace
            ) -> None:
                ctx.events.emit(
                    SparseEncoded(
                        stage=self.name,
                        namespace=namespace,
                        part=key.rsplit("/", 1)[-1],
                        records=rows,
                        cached=stats.cached,
                        no_text=stats.no_text,
                        tokens=stats.tokens,
                        requests=stats.requests,
                    )
                )

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
                enricher=enricher,
                cache=cache,
                on_part=on_part if enricher is not None else None,
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
            "sparse_by_namespace": {
                ns: v["sparse"] for ns, v in namespaces.items() if v.get("sparse") is not None
            },
        }
