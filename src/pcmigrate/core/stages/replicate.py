"""Stage `replicate`: copy emitted import batches to the import bucket (ADR-019).

Pinecone only imports into a BYOC index from a bucket in the BYOC deployment's own cloud
account. When that is not the account the runner and staging bucket live in, the customer
names a bucket there as ``migration.import_bucket``; this stage copies every emitted batch
to it server-side and records the URIs the import stage must use. Without an import bucket
the stage marks itself skipped. Each batch is a partition, and an object whose copy already
carries the source ETag is not copied again, so a killed run resumes where it stopped.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from pcmigrate.core.events import LogMessage, PartDone, PartitionDone
from pcmigrate.core.s3 import is_not_found, list_keys, make_client
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage, StageStatus

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

    from pcmigrate.config.migration import ImportBucket

SOURCE_ETAG_METADATA = "pcmigrate-source-etag"


class ReplicateStage:
    """Server-side copies, one partition per batch, idempotent per object."""

    name = Stage.REPLICATE

    def __init__(self, *, workers: int | None = None) -> None:
        self._workers = workers

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once marked complete (including the skipped case)."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Copy every batch the manifest does not show complete."""
        target = ctx.config.import_bucket
        if target is None:
            return {"skipped": True, "reason": "no import_bucket configured"}
        batches: list[dict[str, Any]] = ctx.manifest.stage(Stage.EMIT).outputs.get("batches", [])
        if not batches:
            raise RuntimeError("emit recorded no batches to replicate")
        dest = self._destination_client(ctx, target)
        objects = 0
        copied_bytes = 0
        replicated: list[dict[str, Any]] = []
        for batch in batches:
            batch_id = str(batch["id"])
            uri = ctx.config.import_batch_uri(batch_id)
            replicated.append({"id": batch_id, "uri": uri, "namespaces": batch.get("namespaces")})
            progress = ctx.manifest.partition(self.name, batch_id)
            if progress.status is StageStatus.COMPLETE:
                objects += len(progress.parts)
                continue
            keys, total = self._copy_batch(ctx, dest, target, batch_id)
            ctx.manifest.mark_partition_complete(self.name, batch_id, rows=len(keys), parts=keys)
            ctx.checkpoint()
            ctx.events.emit(
                PartitionDone(stage=self.name, partition=batch_id, rows=len(keys), parts=keys)
            )
            objects += len(keys)
            copied_bytes += total
        ctx.events.emit(
            LogMessage(
                stage=self.name,
                message=f"{objects} import objects available in s3://{target.name}/"
                f"{ctx.config.import_zone()}/",
            )
        )
        return {
            "bucket": target.name,
            "batches": replicated,
            "objects": objects,
            "bytes_copied": copied_bytes,
        }

    @staticmethod
    def _destination_client(ctx: RunContext, target: ImportBucket) -> S3Client:
        injected = ctx.extras.get("import_bucket_s3")
        if injected is not None:
            client: S3Client = injected
            return client
        return make_client(target.region or ctx.config.aws_region)

    def _copy_batch(
        self, ctx: RunContext, dest: S3Client, target: ImportBucket, batch_id: str
    ) -> tuple[list[str], int]:
        source_prefix = f"{ctx.config.zone('import')}/{batch_id}/"
        dest_prefix = f"{ctx.config.import_zone()}/{batch_id}/"
        keys = list_keys(ctx.s3, ctx.config.bucket, source_prefix)
        if not keys:
            raise RuntimeError(f"batch {batch_id} has no import objects under {source_prefix}")
        copied: list[str] = []
        total = 0

        def one(key: str) -> tuple[str, int]:
            dest_key = dest_prefix + key[len(source_prefix) :]
            size, done = self._copy_object(ctx, dest, target, key, dest_key)
            ctx.events.emit(
                PartDone(stage=self.name, key=dest_key, rows=0, bytes=size if done else 0)
            )
            return dest_key, size if done else 0

        workers = self._workers or max(1, min(ctx.config.workers, 16))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for dest_key, size in pool.map(one, keys):
                copied.append(dest_key)
                total += size
        return copied, total

    @staticmethod
    def _copy_object(
        ctx: RunContext, dest: S3Client, target: ImportBucket, key: str, dest_key: str
    ) -> tuple[int, bool]:
        """Copy one object unless the destination already carries its source ETag.

        Returns the source size and whether a copy was made.
        """
        head = ctx.s3.head_object(Bucket=ctx.config.bucket, Key=key)
        size = int(head["ContentLength"])
        etag = str(head["ETag"]).strip('"')
        try:
            existing = dest.head_object(Bucket=target.name, Key=dest_key)
        except ClientError as err:
            if not is_not_found(err):
                raise
        else:
            if (
                int(existing["ContentLength"]) == size
                and existing.get("Metadata", {}).get(SOURCE_ETAG_METADATA) == etag
            ):
                return size, False
        extra: dict[str, Any] = {
            "MetadataDirective": "REPLACE",
            "Metadata": {SOURCE_ETAG_METADATA: etag},
        }
        if target.kms_key_arn:
            extra["ServerSideEncryption"] = "aws:kms"
            extra["SSEKMSKeyId"] = target.kms_key_arn
        dest.copy(
            {"Bucket": ctx.config.bucket, "Key": key},
            target.name,
            dest_key,
            ExtraArgs=extra,
            SourceClient=ctx.s3,
        )
        return size, True
