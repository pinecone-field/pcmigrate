import io
import json
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.conftest import BUCKET, REGION

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


class FakeStage:
    def __init__(self, name: Stage, *, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.runs = 0

    def is_complete(self, manifest: Manifest) -> bool:
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        self.runs += 1
        if self.fail:
            raise RuntimeError("kaboom")
        return {"ran": self.runs}


@pytest.fixture
def ctx(s3: S3Client) -> tuple[RunContext, io.StringIO]:
    out = io.StringIO()
    cfg = MigrationConfig(
        migration_id="mig", bucket=BUCKET, aws_region=REGION, pinecone_api_key_secret=ARN
    )
    store = ManifestStore(s3, BUCKET, "mig")
    manifest = Manifest(migration_id="mig")
    context = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(EXAMPLE),
        manifest=manifest,
        store=store,
        events=EventBus("mig", [JsonLinesSink(out)]),
        s3=s3,
    )
    return context, out


def _events(out: io.StringIO) -> list[str]:
    return [f"{e['type']}:{e.get('stage')}" for e in map(json.loads, out.getvalue().splitlines())]


def test_runs_in_order_and_checkpoints(ctx: tuple[RunContext, io.StringIO], s3: S3Client) -> None:
    context, out = ctx
    export = FakeStage(Stage.EXPORT)
    emit = FakeStage(Stage.EMIT)
    pipeline = Pipeline([emit, export])
    assert [s.name for s in pipeline.stages] == [Stage.EXPORT, Stage.EMIT]
    pipeline.run(context, S3Lock(s3, BUCKET, "mig"))
    assert context.manifest.is_complete(Stage.EXPORT)
    assert context.manifest.stage(Stage.EMIT).outputs == {"ran": 1}
    stored = ManifestStore(s3, BUCKET, "mig").load()
    assert stored is not None and stored.is_complete(Stage.EMIT)
    assert _events(out) == [
        "stage_started:export",
        "stage_completed:export",
        "stage_started:emit",
        "stage_completed:emit",
    ]


def test_skips_completed_stages_on_resume(
    ctx: tuple[RunContext, io.StringIO], s3: S3Client
) -> None:
    context, out = ctx
    export = FakeStage(Stage.EXPORT)
    emit = FakeStage(Stage.EMIT)
    Pipeline([export]).run(context, S3Lock(s3, BUCKET, "mig"))
    Pipeline([export, emit]).run(context, S3Lock(s3, BUCKET, "mig"))
    assert export.runs == 1 and emit.runs == 1
    assert "stage_skipped:export" in _events(out)


def test_failure_is_recorded_and_lock_released(
    ctx: tuple[RunContext, io.StringIO], s3: S3Client
) -> None:
    context, out = ctx
    bad = FakeStage(Stage.EXPORT, fail=True)
    lock = S3Lock(s3, BUCKET, "mig")
    with pytest.raises(PipelineError) as info:
        Pipeline([bad, FakeStage(Stage.EMIT)]).run(context, lock)
    assert info.value.stage is Stage.EXPORT
    stored = ManifestStore(s3, BUCKET, "mig").load()
    assert stored is not None
    assert stored.stage(Stage.EXPORT).error == "RuntimeError: kaboom"
    assert not stored.is_complete(Stage.EMIT)
    assert not lock.held
    assert _events(out)[-1] == "stage_failed:export"


def test_until_stops_early(ctx: tuple[RunContext, io.StringIO], s3: S3Client) -> None:
    context, _ = ctx
    emit = FakeStage(Stage.EMIT)
    Pipeline([FakeStage(Stage.EXPORT), emit]).run(
        context, S3Lock(s3, BUCKET, "mig"), until=Stage.EXPORT
    )
    assert emit.runs == 0


def test_after_stage_hook(ctx: tuple[RunContext, io.StringIO], s3: S3Client) -> None:
    context, _ = ctx
    seen: list[Stage] = []
    Pipeline([FakeStage(Stage.EXPORT)], after_stage=seen.append).run(
        context, S3Lock(s3, BUCKET, "mig")
    )
    assert seen == [Stage.EXPORT]


def test_duplicate_stage_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        Pipeline([FakeStage(Stage.EXPORT), FakeStage(Stage.EXPORT)])
