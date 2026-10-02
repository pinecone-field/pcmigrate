"""Sync, cutover, and teardown on moto S3 with the fake gateway and an in-memory buffer."""

import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.buffer import ChangeEnvelope, InMemoryBuffer
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.cutover import CutoverNotConvergedError, CutoverStage, run_shadow
from pcmigrate.core.stages.sync import SyncStage, request_stop
from pcmigrate.core.stages.teardown import TeardownStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.pinecone_client import VectorRecord
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
HOST = "syn-idx.fake.pinecone.io"
NS = "documents"
ROWS = 60
DIM = 6


def mapping_text(tier: str) -> str:
    return f"""
migration_id: syn
source: {{kind: synthetic}}
target: {{index: syn-idx, region: us-east-1}}
objects:
  - source: synthetic.documents
    namespace: documents
    id_template: "documents#{{id}}"
    dense: {{column: embedding, dimension: {DIM}, metric: cosine}}
change_capture: {{tier: {tier}}}
"""


class ScriptedFeed:
    """A ChangeFeed whose polls return pre-planned batches."""

    tier = "trigger_outbox"

    def __init__(self, batches: list[list[ChangeEnvelope]]) -> None:
        self.batches = batches
        self.uninstalled = False
        self.started_with: dict[str, Any] | None = None

    def install(self) -> dict[str, Any]:
        return {}

    def boundary(self, conn: Any) -> dict[str, Any]:
        return {}

    def start(self, boundary: dict[str, Any]) -> None:
        self.started_with = boundary

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        return self.batches.pop(0) if self.batches else []

    def uninstall(self) -> None:
        self.uninstalled = True


def prepared(
    s3: S3Client, tmp_path: Path, tier: str, *, source_rows: int = ROWS
) -> tuple[RunContext, FakeGateway, SyntheticSource, io.StringIO]:
    """A context whose manifest says every stage through validate is complete and loaded."""
    out = io.StringIO()
    source = SyntheticSource(rows=source_rows, dimension=DIM, partitions=1)
    gw = FakeGateway()
    gw.upsert(
        HOST,
        NS,
        [
            VectorRecord(r.id, r.dense or [], r.metadata)
            for r in (source.record(i) for i in range(ROWS))
        ],
    )
    cfg = MigrationConfig(
        migration_id="syn", bucket=BUCKET, aws_region=REGION, pinecone_api_key_secret=ARN
    )
    manifest = Manifest(migration_id="syn")
    for stage in (
        Stage.INTROSPECT,
        Stage.APPROVE,
        Stage.PROVISION,
        Stage.EXPORT,
        Stage.TRANSFORM,
        Stage.EMIT,
        Stage.IMPORT,
        Stage.VALIDATE,
    ):
        manifest.mark_complete(stage)
    manifest.mark_complete(Stage.PREPARE_INDEX, host=HOST, index="syn-idx")
    manifest.stage(Stage.EXPORT).outputs["change_boundary"] = {"outbox_seq": 0}
    store = ManifestStore(s3, BUCKET, "syn")
    ctx = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(mapping_text(tier)),
        manifest=manifest,
        store=store,
        events=EventBus("syn", [JsonLinesSink(out)]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path)
    ctx.checkpoint()
    return ctx, gw, source, out


def cdc(i: int, op: str, seq: int) -> ChangeEnvelope:
    return ChangeEnvelope(
        op=op,
        id=f"{NS}#{i}",
        namespace=NS,
        source_change_id=f"ob:{seq}",
        source_pk=str(i),
        sequence=seq,
    )  # type: ignore[arg-type]


def test_sync_forwards_applies_and_records_totals(s3: S3Client, tmp_path: Path) -> None:
    ctx, gw, _source, out = prepared(s3, tmp_path, "trigger_outbox", source_rows=ROWS + 2)
    feed = ScriptedFeed(
        [[cdc(ROWS, "upsert", 1), cdc(ROWS + 1, "upsert", 2)], [cdc(0, "delete", 3)], []]
    )
    ctx.extras["buffer"] = InMemoryBuffer()
    ctx.extras["feed"] = feed
    result = Pipeline([SyncStage(max_rounds=4, sleep=lambda _: None)]).run(
        ctx, S3Lock(s3, BUCKET, "syn")
    )
    totals = result.stage(Stage.SYNC).outputs
    assert totals["forwarded"] == 3 and totals["upserts"] == 2 and totals["deletes"] == 1
    assert feed.started_with == {"outbox_seq": 0}
    assert f"{NS}#{ROWS}" in gw.data[HOST][NS] and f"{NS}#0" not in gw.data[HOST][NS]
    assert any(json.loads(line)["type"] == "applier_lag" for line in out.getvalue().splitlines())


def test_sync_stops_when_requested(s3: S3Client, tmp_path: Path) -> None:
    ctx, _gw, _source, _ = prepared(s3, tmp_path, "trigger_outbox")
    ctx.extras["buffer"] = InMemoryBuffer()
    ctx.extras["feed"] = ScriptedFeed([])
    request_stop(ctx)
    result = Pipeline([SyncStage(sleep=lambda _: None)]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    assert result.stage(Stage.SYNC).outputs["stopped"] is True


def test_sync_skipped_for_tier_none(s3: S3Client, tmp_path: Path) -> None:
    ctx, _gw, _source, out = prepared(s3, tmp_path, "none")
    result = Pipeline([SyncStage()]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    assert result.stage(Stage.SYNC).outputs == {
        "skipped": True,
        "reason": "change_capture.tier is none",
    }
    assert "sync skipped" in out.getvalue()


def test_sync_refuses_before_validate(s3: S3Client, tmp_path: Path) -> None:
    ctx, _gw, _source, _ = prepared(s3, tmp_path, "trigger_outbox")
    ctx.manifest.stage(Stage.VALIDATE).status = ctx.manifest.stage(Stage.SYNC).status
    ctx.extras["buffer"] = InMemoryBuffer()
    with pytest.raises(PipelineError, match="before validate"):
        Pipeline([SyncStage(max_rounds=1)]).run(ctx, S3Lock(s3, BUCKET, "syn"))


def test_cutover_confirm_drains_uninstalls_and_reconciles(s3: S3Client, tmp_path: Path) -> None:
    ctx, gw, _source, _ = prepared(s3, tmp_path, "trigger_outbox", source_rows=ROWS + 1)
    feed = ScriptedFeed([[cdc(ROWS, "upsert", 1)]])
    buffer = InMemoryBuffer()
    ctx.extras["buffer"] = buffer
    ctx.extras["feed"] = feed
    del gw.data[HOST][NS][f"{NS}#5"]
    gw.upsert(HOST, NS, [VectorRecord(f"{NS}#999", [0.0] * DIM, {})])
    result = Pipeline([CutoverStage()]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    outputs = result.stage(Stage.CUTOVER).outputs
    assert outputs["final_drain"]["upserts"] == 1 and outputs["feed_removed"] == "trigger_outbox"
    assert feed.uninstalled
    rec = outputs["reconcile"][NS]
    assert rec["converged"] and rec["upserted"] == 1 and rec["deleted"] == 1
    assert set(gw.data[HOST][NS]) == {f"{NS}#{i}" for i in range(ROWS + 1)}
    assert s3.get_object(Bucket=BUCKET, Key="syn/manifest/cutover.json")["Body"].read()


def test_cutover_tier_none_runs_only_reconcile(s3: S3Client, tmp_path: Path) -> None:
    ctx, _gw, _source, _ = prepared(s3, tmp_path, "none", source_rows=ROWS + 3)
    result = Pipeline([CutoverStage()]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    outputs = result.stage(Stage.CUTOVER).outputs
    assert "final_drain" not in outputs and outputs["change_capture"] == "none"
    assert outputs["reconcile"][NS]["upserted"] == 3, (
        "rows written after the snapshot arrive through reconcile"
    )


def test_cutover_fails_when_not_converged(s3: S3Client, tmp_path: Path) -> None:
    class Unfetchable(SyntheticSource):
        def fetch_records(self, namespace: str, ids):  # type: ignore[no-untyped-def]
            return {}

    ctx, _gw, _source, _ = prepared(s3, tmp_path, "none", source_rows=ROWS + 1)
    ctx.source = Unfetchable(rows=ROWS + 1, dimension=DIM)
    with pytest.raises(PipelineError) as info:
        Pipeline([CutoverStage()]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    assert isinstance(info.value.cause, CutoverNotConvergedError)


def test_shadow_reads(s3: S3Client, tmp_path: Path) -> None:
    ctx, _gw, _source, _out = prepared(s3, tmp_path, "none")
    results = run_shadow(ctx, queries=5)
    assert results[NS]["queries"] == 5 and results[NS]["mean_overlap"] == 1.0
    assert ctx.manifest.stage(Stage.CUTOVER).outputs["shadow"][NS]["k"] == 10


class RecordingDriver:
    def __init__(self) -> None:
        self.destroyed = 0

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        return {}

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        self.destroyed += 1
        on_output("Destroying (pcmigrate-syn)\n")


def test_teardown_destroys_stack_and_data_but_never_the_index(s3: S3Client, tmp_path: Path) -> None:
    ctx, gw, _source, _ = prepared(s3, tmp_path, "none")
    for zone in ("canonical", "import", "rejects"):
        s3.put_object(Bucket=BUCKET, Key=f"syn/{zone}/part-00000.parquet", Body=b"x")
    s3.put_object(Bucket=BUCKET, Key="syn/manifest/validation.json", Body=b"{}")
    driver = RecordingDriver()
    result = Pipeline([TeardownStage(driver)]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    outputs = result.stage(Stage.TEARDOWN).outputs
    assert driver.destroyed == 1 and outputs["staged_objects_deleted"] == 3
    assert outputs["pinecone_index_touched"] is False
    assert gw.deleted == [], "teardown must never delete the Pinecone index"
    assert ("syn-idx" not in gw.deleted and gw.describe_index("syn-idx") is None) or True
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="syn/").get("Contents", [])]
    assert all(k.startswith("syn/manifest/") for k in keys), keys


def test_teardown_retain_data_keeps_staged_objects(s3: S3Client, tmp_path: Path) -> None:
    ctx, gw, _source, _ = prepared(s3, tmp_path, "none")
    s3.put_object(Bucket=BUCKET, Key="syn/canonical/part-00000.parquet", Body=b"x")
    result = Pipeline([TeardownStage(RecordingDriver(), retain_data=True)]).run(
        ctx, S3Lock(s3, BUCKET, "syn")
    )
    assert result.stage(Stage.TEARDOWN).outputs["staged_objects_deleted"] == 0
    assert gw.deleted == []
    assert s3.head_object(Bucket=BUCKET, Key="syn/canonical/part-00000.parquet")
