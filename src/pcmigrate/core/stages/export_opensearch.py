"""Stage `export` for OpenSearch: T0, the OSIS scan into raw/, then Glue into canonical/ (ADR-009).

The stage records T0 and the source counts before anything moves, then releases the armed
OSIS pipeline by setting its start time to T0, waits until raw/ stops growing, stops the
pipeline, and runs one Glue job per index. Every step is a checkpoint, so a killed run
resumes at the step it was on and a Glue run in flight is polled rather than restarted.
``mode="direct"`` keeps T0 and the counts but scans from the runner through the shared
export stage, for laptop runs and tests.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pcmigrate.core.events import LogMessage, PartitionDone
from pcmigrate.core.s3 import list_keys
from pcmigrate.core.stages import RunContext
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.state import Manifest, Stage, StageStatus
from pcmigrate.sources.opensearch.change_feed import record_t0
from pcmigrate.sources.opensearch.export import OpenSearchSource, slug
from pcmigrate.sources.opensearch.glue_mapping import PartitionResult, merge_results
from pcmigrate.sources.opensearch.osis_config import with_start_time

GLUE_TERMINAL = frozenset({"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"})
OSIS_FAILED = frozenset({"CREATE_FAILED", "UPDATE_FAILED", "START_FAILED"})


class OsisControl(Protocol):
    """What the stage needs from OpenSearch Ingestion."""

    def status(self, name: str) -> str:
        """Pipeline status, e.g. ACTIVE, UPDATING, STOPPED."""
        ...

    def configuration(self, name: str) -> str:
        """The current pipeline configuration body."""
        ...

    def update(self, name: str, body: str) -> None:
        """Replace the configuration body."""
        ...

    def stop(self, name: str) -> None:
        """Stop the pipeline."""
        ...


class GlueControl(Protocol):
    """What the stage needs from Glue."""

    def start(self, job: str, arguments: dict[str, str]) -> str:
        """Start a run and return its id."""
        ...

    def state(self, job: str, run_id: str) -> tuple[str, str | None]:
        """``(state, error_message)`` of one run."""
        ...


class Boto3Osis:
    """OsisControl over boto3."""

    def __init__(self, region: str) -> None:
        import boto3

        self._osis = boto3.client("osis", region_name=region)

    def status(self, name: str) -> str:
        """See OsisControl."""
        return str(self._osis.get_pipeline(PipelineName=name)["Pipeline"]["Status"])

    def configuration(self, name: str) -> str:
        """See OsisControl."""
        return str(
            self._osis.get_pipeline(PipelineName=name)["Pipeline"]["PipelineConfigurationBody"]
        )

    def update(self, name: str, body: str) -> None:
        """See OsisControl."""
        self._osis.update_pipeline(PipelineName=name, PipelineConfigurationBody=body)

    def stop(self, name: str) -> None:
        """See OsisControl."""
        self._osis.stop_pipeline(PipelineName=name)


class Boto3Glue:
    """GlueControl over boto3."""

    def __init__(self, region: str) -> None:
        import boto3

        self._glue = boto3.client("glue", region_name=region)

    def start(self, job: str, arguments: dict[str, str]) -> str:
        """See GlueControl."""
        return str(self._glue.start_job_run(JobName=job, Arguments=arguments)["JobRunId"])

    def state(self, job: str, run_id: str) -> tuple[str, str | None]:
        """See GlueControl."""
        run = self._glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
        return str(run["JobRunState"]), run.get("ErrorMessage")


@dataclass(frozen=True, slots=True)
class ScanSettings:
    """How long to wait for the OSIS scan."""

    poll_s: float = 30.0
    quiet_s: float = 180.0
    timeout_s: float = 6 * 3600
    glue_poll_s: float = 30.0
    glue_timeout_s: float = 12 * 3600
    osis_timeout_s: float = 30 * 60


class OpenSearchExportStage:
    """OSIS plus Glue, or the direct scan; T0 and source counts are recorded first either way."""

    name = Stage.EXPORT

    def __init__(
        self,
        *,
        mode: Literal["osis_glue", "direct"] = "osis_glue",
        scan: ScanSettings | None = None,
        osis: OsisControl | None = None,
        glue: GlueControl | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        workers: int | None = None,
    ) -> None:
        self._mode = mode
        self._scan = scan or ScanSettings()
        self._osis = osis
        self._glue = glue
        self._sleep = sleep
        self._clock = clock
        self._workers = workers

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once marked complete."""
        return manifest.is_complete(self.name)

    def _source(self, ctx: RunContext) -> OpenSearchSource:
        source = ctx.require_source()
        if not isinstance(source, OpenSearchSource):
            raise RuntimeError("the OpenSearch export stage needs an OpenSearchSource")
        return source

    def _record_t0(self, ctx: RunContext, source: OpenSearchSource) -> None:
        record = ctx.manifest.stage(self.name)
        if "t0" in record.outputs:
            return
        indices = [o.source for o in ctx.mapping.objects if o.dense is not None]
        for index in indices:
            for key in list_keys(ctx.s3, ctx.config.bucket, f"{ctx.config.zone('raw')}/{index}/"):
                ctx.s3.delete_object(Bucket=ctx.config.bucket, Key=key)
        t0 = record_t0()
        record.outputs["t0"] = t0
        record.outputs["change_boundary"] = {"t0": t0, "watermarks": dict.fromkeys(indices, t0)}
        record.outputs["source_counts"] = {index: source.client.count(index) for index in indices}
        record.outputs["mode"] = self._mode
        ctx.checkpoint()
        ctx.events.emit(
            LogMessage(stage=self.name, message=f"T0 recorded at {t0}; dual-writes start now")
        )

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """T0, then the scan and transform for the configured mode."""
        source = self._source(ctx)
        self._record_t0(ctx, source)
        if self._mode == "direct":
            return self._run_direct(ctx, source)
        return self._run_osis_glue(ctx, source)

    def _run_direct(self, ctx: RunContext, source: OpenSearchSource) -> dict[str, Any]:
        source.bucket = ctx.config.bucket
        source.rejects_prefix = ctx.config.zone("rejects")
        source.region = ctx.config.aws_region
        outputs = ExportStage(workers=self._workers, use_processes=False).run(ctx)
        rejected: dict[str, int] = {}
        for partition in source.plan_partitions(ctx.mapping):
            rejected[partition.namespace] = rejected.get(
                partition.namespace, 0
            ) + source.rejected.get(partition.id, 0)
        outputs["rejected_by_namespace"] = rejected
        return outputs

    def _osis_control(self, ctx: RunContext) -> OsisControl:
        if self._osis is not None:
            return self._osis
        attached = ctx.extras.get("osis")
        if attached is not None:
            return attached  # type: ignore[no-any-return]
        return Boto3Osis(ctx.config.aws_region)

    def _glue_control(self, ctx: RunContext) -> GlueControl:
        if self._glue is not None:
            return self._glue
        attached = ctx.extras.get("glue")
        if attached is not None:
            return attached  # type: ignore[no-any-return]
        return Boto3Glue(ctx.config.aws_region)

    def _provision_output(self, ctx: RunContext, key: str) -> str:
        value = ctx.manifest.stage(Stage.PROVISION).outputs.get(key)
        if not value:
            raise RuntimeError(
                f"provision recorded no {key}; run provision or use export_mode: direct"
            )
        return str(value)

    def _wait_osis(self, osis: OsisControl, name: str, wanted: frozenset[str]) -> str:
        deadline = self._clock() + self._scan.osis_timeout_s
        while True:
            status = osis.status(name)
            if status in wanted:
                return status
            if status in OSIS_FAILED:
                raise RuntimeError(f"OSIS pipeline {name} is {status}")
            if self._clock() >= deadline:
                raise TimeoutError(f"OSIS pipeline {name} still {status} after the wait")
            self._sleep(self._scan.poll_s)

    def _run_osis_glue(self, ctx: RunContext, source: OpenSearchSource) -> dict[str, Any]:
        record = ctx.manifest.stage(self.name)
        outputs = record.outputs
        pipeline = self._provision_output(ctx, "osis_pipeline_name")
        job = self._provision_output(ctx, "glue_job_name")
        osis = self._osis_control(ctx)
        indices = [o.source for o in ctx.mapping.objects if o.dense is not None]

        if not outputs.get("osis_started"):
            self._wait_osis(osis, pipeline, frozenset({"ACTIVE", "STOPPED"}))
            osis.update(pipeline, with_start_time(osis.configuration(pipeline), str(outputs["t0"])))
            outputs["osis_started"] = True
            outputs["osis_started_at"] = record_t0()
            ctx.checkpoint()
            ctx.events.emit(
                LogMessage(stage=self.name, message=f"OSIS pipeline {pipeline} released at T0")
            )
        if not outputs.get("osis_scan_complete"):
            self._wait_osis(osis, pipeline, frozenset({"ACTIVE"}))
            outputs["raw_objects"] = self._wait_for_scan(ctx, indices)
            outputs["osis_scan_complete"] = True
            ctx.checkpoint()
        if not outputs.get("osis_stopped"):
            osis.stop(pipeline)
            outputs["osis_stopped"] = True
            ctx.checkpoint()
            ctx.events.emit(
                LogMessage(stage=self.name, message=f"OSIS pipeline {pipeline} stopped")
            )

        self._stage_mapping(ctx)
        glue = self._glue_control(ctx)
        outputs.setdefault("glue_runs", {})
        for obj in ctx.mapping.objects:
            if obj.dense is None:
                continue
            partition_id = slug(obj.source)
            if ctx.manifest.partition(self.name, partition_id).status is StageStatus.COMPLETE:
                continue
            runs: dict[str, Any] = outputs["glue_runs"]
            run_id = runs.get(obj.source)
            if run_id is None:
                run_id = glue.start(job, self._glue_arguments(ctx, obj.source, obj.namespace))
                runs[obj.source] = run_id
                ctx.checkpoint()
                ctx.events.emit(
                    LogMessage(
                        stage=self.name, message=f"Glue run {run_id} started for {obj.source}"
                    )
                )
            self._wait_glue(glue, job, str(run_id), obj.source)
            result = self._read_result(ctx, obj.source)
            merged = merge_results(
                PartitionResult.from_dict(r) for r in result["namespaces"].values()
            )
            total = merge_results(merged.values())
            rows = sum(r.rows for r in total.values())
            parts = sorted(p for r in total.values() for p in r.parts)
            outputs.setdefault("glue_results", {})[obj.source] = result
            ctx.manifest.mark_partition_complete(self.name, partition_id, rows=rows, parts=parts)
            ctx.checkpoint()
            ctx.events.emit(
                PartitionDone(stage=self.name, partition=partition_id, rows=rows, parts=parts)
            )

        rows_by_namespace: dict[str, int] = {}
        parts_by_namespace: dict[str, list[str]] = {}
        rejected_by_namespace: dict[str, int] = {}
        for obj in ctx.mapping.objects:
            if obj.dense is None:
                continue
            progress = ctx.manifest.partition(self.name, slug(obj.source))
            rows_by_namespace[obj.namespace] = (
                rows_by_namespace.get(obj.namespace, 0) + progress.rows
            )
            parts_by_namespace.setdefault(obj.namespace, []).extend(progress.parts)
            result = outputs.get("glue_results", {}).get(obj.source, {})
            rejected_by_namespace[obj.namespace] = rejected_by_namespace.get(
                obj.namespace, 0
            ) + int(result.get("rejected", 0))
            source_count = int(outputs.get("source_counts", {}).get(obj.source, 0))
            scanned = progress.rows + int(result.get("rejected", 0))
            if scanned != source_count:
                ctx.events.emit(
                    LogMessage(
                        stage=self.name,
                        level="warning",
                        message=(
                            f"{obj.source}: OSIS scanned {scanned} documents but the index held "
                            f"{source_count} at T0; documents written or deleted during the scan "
                            "arrive through dual-write and the final reconcile"
                        ),
                    )
                )
        return {
            "rows": sum(rows_by_namespace.values()),
            "rows_by_namespace": rows_by_namespace,
            "parts_by_namespace": parts_by_namespace,
            "rejected_by_namespace": rejected_by_namespace,
        }

    def _wait_for_scan(self, ctx: RunContext, indices: list[str]) -> dict[str, int]:
        """Poll raw/ until every index prefix has objects and none has changed for quiet_s."""
        started = self._clock()
        last_change = started
        seen: dict[str, tuple[tuple[str, int], ...]] = {}
        while True:
            snapshot: dict[str, tuple[tuple[str, int], ...]] = {}
            for index in indices:
                prefix = f"{ctx.config.zone('raw')}/{index}/"
                objects = tuple(
                    (obj["Key"], int(obj["Size"]))
                    for page in ctx.s3.get_paginator("list_objects_v2").paginate(
                        Bucket=ctx.config.bucket, Prefix=prefix
                    )
                    for obj in page.get("Contents", [])
                )
                snapshot[index] = objects
            now = self._clock()
            if snapshot != seen:
                seen = snapshot
                last_change = now
                ctx.events.emit(
                    LogMessage(
                        stage=self.name,
                        message="raw/ growing: "
                        + ", ".join(f"{i}={len(o)} objects" for i, o in snapshot.items()),
                    )
                )
            complete = all(snapshot[i] for i in indices) and now - last_change >= self._scan.quiet_s
            if complete:
                return {i: len(o) for i, o in snapshot.items()}
            if now - started >= self._scan.timeout_s:
                raise TimeoutError("OSIS scan did not settle before scan_timeout_s")
            self._sleep(self._scan.poll_s)

    def _stage_mapping(self, ctx: RunContext) -> None:
        key = f"{ctx.config.zone('manifest')}/glue/mapping.yaml"
        ctx.s3.put_object(
            Bucket=ctx.config.bucket, Key=key, Body=ctx.mapping.to_yaml().encode("utf-8")
        )

    def _glue_arguments(self, ctx: RunContext, index: str, namespace: str) -> dict[str, str]:
        bucket = ctx.config.bucket
        return {
            "--mapping_uri": f"s3://{bucket}/{ctx.config.zone('manifest')}/glue/mapping.yaml",
            "--index": index,
            "--raw_uri": f"s3://{bucket}/{ctx.config.zone('raw')}/{index}/",
            "--bucket": bucket,
            "--canonical_prefix": f"{ctx.config.zone('canonical')}/{index}",
            "--rejects_prefix": f"{ctx.config.zone('rejects')}/{namespace}/glue",
            "--result_uri": f"s3://{bucket}/{ctx.config.zone('manifest')}/glue/{index}/result.json",
            "--part_bytes": str(ctx.config.canonical_part_bytes),
            "--region": ctx.config.aws_region,
            "--min_partitions": str(max(2, (self._workers or ctx.config.workers) * 2)),
        }

    def _wait_glue(self, glue: GlueControl, job: str, run_id: str, index: str) -> None:
        deadline = self._clock() + self._scan.glue_timeout_s
        while True:
            state, error = glue.state(job, run_id)
            if state == "SUCCEEDED":
                return
            if state in GLUE_TERMINAL:
                raise RuntimeError(f"Glue run {run_id} for {index} ended {state}: {error}")
            if self._clock() >= deadline:
                raise TimeoutError(f"Glue run {run_id} for {index} still {state} after the wait")
            self._sleep(self._scan.glue_poll_s)

    def _read_result(self, ctx: RunContext, index: str) -> dict[str, Any]:
        key = f"{ctx.config.zone('manifest')}/glue/{index}/result.json"
        body = ctx.s3.get_object(Bucket=ctx.config.bucket, Key=key)["Body"].read()
        result: dict[str, Any] = json.loads(body)
        return result
