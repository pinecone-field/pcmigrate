"""One module per pipeline stage, plus the Pipeline that runs them in ADR-006 order."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.migration import MigrationConfig
from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.core.approval import require_approval
from pcmigrate.core.artifacts import ArtifactStore
from pcmigrate.core.events import (
    EventBus,
    StageCompleted,
    StageFailed,
    StageSkipped,
    StageStarted,
)
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage, StageProtocol

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

    from pcmigrate.sources.base import SourceConnector
    from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass
class RunContext:
    """Everything a stage may touch. Connectors and gateways are attached by the caller."""

    config: MigrationConfig
    mapping: Mapping
    manifest: Manifest
    store: ManifestStore
    events: EventBus
    s3: S3Client
    source: SourceConnector | None = None
    target: PineconeGateway | None = None
    plan_lock: PlanLock | None = None
    approvals: ArtifactStore | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def require_source(self) -> SourceConnector:
        """The source connector, or a clear error when the caller forgot to attach one."""
        if self.source is None:
            raise RuntimeError("this stage needs a source connector on the run context")
        return self.source

    def require_target(self) -> PineconeGateway:
        """The Pinecone gateway, or a clear error when the caller forgot to attach one."""
        if self.target is None:
            raise RuntimeError("this stage needs a Pinecone gateway on the run context")
        return self.target

    def require_plan_lock(self) -> PlanLock:
        """The plan.lock this run executes, or a clear error."""
        if self.plan_lock is None:
            raise RuntimeError("this stage needs the plan.lock on the run context")
        return self.plan_lock

    def require_approvals(self) -> ArtifactStore:
        """Where manifest/approvals.json lives, or a clear error."""
        if self.approvals is None:
            raise RuntimeError("this stage needs an approvals store on the run context")
        return self.approvals

    def index_host(self) -> str:
        """The host recorded by prepare_index."""
        host = self.manifest.stage(Stage.PREPARE_INDEX).outputs.get("host")
        if not host:
            raise RuntimeError("prepare_index has not recorded an index host")
        return str(host)

    def checkpoint(self) -> None:
        """Persist the manifest now, so a kill after this point resumes from here."""
        self.store.save(self.manifest)


class PipelineError(RuntimeError):
    """A stage failed. The manifest records which one and why."""

    def __init__(self, stage: Stage, cause: BaseException) -> None:
        super().__init__(f"stage {stage.value} failed: {cause}")
        self.stage = stage
        self.cause = cause


class Pipeline:
    """Runs stages in order under the run lock, skipping ones the manifest shows complete."""

    def __init__(
        self,
        stages: Sequence[StageProtocol],
        *,
        after_stage: Callable[[Stage], None] | None = None,
    ) -> None:
        ordered = sorted(stages, key=lambda s: s.name.position)
        names = [s.name for s in ordered]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate stages: {names}")
        self._stages = ordered
        self._after_stage = after_stage

    @property
    def stages(self) -> tuple[StageProtocol, ...]:
        """Stages in pipeline order."""
        return tuple(self._stages)

    def run(self, ctx: RunContext, lock: S3Lock, *, until: Stage | None = None) -> Manifest:
        """Run every stage up to and including *until* that is not already complete."""
        with lock:
            for stage in self._stages:
                if until is not None and until.precedes(stage.name):
                    break
                self._run_one(stage, ctx)
                lock.refresh()
                if self._after_stage is not None:
                    self._after_stage(stage.name)
        return ctx.manifest

    @staticmethod
    def _gate(stage: Stage, ctx: RunContext) -> None:
        """ADR-005: every stage after approve refuses to run without a matching approval."""
        if not Stage.APPROVE.precedes(stage):
            return
        lock = ctx.require_plan_lock()
        require_approval(ctx.require_approvals(), lock)
        if ctx.manifest.plan_hash not in (None, lock.plan_hash):
            raise RuntimeError(
                f"manifest was approved for plan {str(ctx.manifest.plan_hash)[:12]} but the "
                f"current plan is {lock.plan_hash[:12]}; re-run introspect and approve"
            )
        ctx.manifest.plan_hash = lock.plan_hash

    def _run_one(self, stage: StageProtocol, ctx: RunContext) -> None:
        name = stage.name
        manifest = ctx.manifest
        if stage.is_complete(manifest):
            ctx.events.emit(StageSkipped(stage=name))
            return
        self._gate(name, ctx)
        record = manifest.mark_running(name)
        ctx.checkpoint()
        ctx.events.emit(StageStarted(stage=name, attempt=record.attempts))
        started = time.monotonic()
        try:
            outputs = stage.run(ctx)
        except Exception as err:
            manifest.mark_failed(name, f"{type(err).__name__}: {err}")
            ctx.checkpoint()
            ctx.events.emit(StageFailed(stage=name, error=f"{type(err).__name__}: {err}"))
            raise PipelineError(name, err) from err
        manifest.mark_complete(name, **outputs)
        ctx.checkpoint()
        ctx.events.emit(
            StageCompleted(stage=name, duration_s=time.monotonic() - started, outputs=outputs)
        )
