"""Stage `replicate` (ADR-019): copies import batches to the import bucket, resumes per object."""

import io
import json
from pathlib import Path
from typing import Any

import boto3
import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.replicate import SOURCE_ETAG_METADATA, ReplicateStage
from pcmigrate.core.stages.teardown import TeardownStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.sources.synthetic import SyntheticSource
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.core.test_stages_pipeline import ARN, DIM, LIMITS, MAPPING, ROWS, load_fake
from tests.unit.core.test_sync_cutover_teardown import RecordingDriver
from tests.unit.fakes import FakeGateway

IMPORT_BUCKET = "byoc-import"


@pytest.fixture
def import_s3(s3: S3Client) -> S3Client:
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=IMPORT_BUCKET)
    return client


def make_ctx(
    s3: S3Client,
    import_s3: S3Client,
    gw: FakeGateway,
    tmp_path: Path,
    *,
    with_import_bucket: bool = True,
) -> tuple[RunContext, io.StringIO]:
    out = io.StringIO()
    cfg = MigrationConfig(
        migration_id="syn",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
        import_part_bytes=8 * 1024 * 1024,
        import_bucket={"name": IMPORT_BUCKET, "prefix": "pinecone/syn"}
        if with_import_bucket
        else None,
    )
    store = ManifestStore(s3, BUCKET, "syn")
    manifest = store.load() or Manifest(migration_id="syn")
    source = SyntheticSource(rows=ROWS, dimension=DIM, partitions=2)
    ctx = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(MAPPING),
        manifest=manifest,
        store=store,
        events=EventBus("syn", [JsonLinesSink(out)]),
        s3=s3,
        source=source,
        target=gw,
        extras={"import_bucket_s3": import_s3},
    )
    approve_ctx(ctx, tmp_path)
    return ctx, out


def stages(replicate: Any) -> list[Any]:
    return [
        PrepareIndexStage(),
        ExportStage(workers=2, use_processes=False),
        TransformStage(),
        EmitStage(LIMITS),
        replicate,
        ImportStage(poll_interval_s=0),
    ]


@pytest.fixture
def gw() -> FakeGateway:
    g = FakeGateway(import_records=ROWS)
    load_fake(g, SyntheticSource(rows=ROWS, dimension=DIM, partitions=1))
    return g


def _keys(client: S3Client, bucket: str, prefix: str) -> list[str]:
    return sorted(
        o["Key"] for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    )


def test_replicate_copies_batches_and_import_reads_the_copy(
    s3: S3Client, import_s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    ctx, out = make_ctx(s3, import_s3, gw, tmp_path)
    Pipeline(stages(ReplicateStage(workers=2))).run(ctx, S3Lock(s3, BUCKET, "syn"))
    m = ctx.manifest
    assert m.is_complete(Stage.REPLICATE) and m.is_complete(Stage.IMPORT)
    source_keys = _keys(s3, BUCKET, "syn/import/")
    copied = _keys(import_s3, IMPORT_BUCKET, "pinecone/syn/import/")
    assert copied == [k.replace("syn/import/", "pinecone/syn/import/", 1) for k in source_keys]
    assert copied, "nothing was copied"
    head = import_s3.head_object(Bucket=IMPORT_BUCKET, Key=copied[0])
    src = s3.head_object(Bucket=BUCKET, Key=source_keys[0])
    assert head["Metadata"][SOURCE_ETAG_METADATA] == src["ETag"].strip('"')
    assert head["ContentLength"] == src["ContentLength"]
    outputs = m.stage(Stage.REPLICATE).outputs
    assert outputs["bucket"] == IMPORT_BUCKET and outputs["objects"] == len(copied)
    assert outputs["batches"][0]["uri"] == f"s3://{IMPORT_BUCKET}/pinecone/syn/import/batch-00000/"
    assert gw.imports["imp-1"].uri == outputs["batches"][0]["uri"], "import must read the copy"
    assert m.stage(Stage.IMPORT).outputs["records_imported"] == ROWS
    types = [json.loads(line)["type"] for line in out.getvalue().splitlines()]
    assert "partition_done" in types and "part_done" in types


class DyingReplicate(ReplicateStage):
    """Dies before its N-th copy, standing in for a runner killed mid-batch."""

    def __init__(self, die_after: int) -> None:
        super().__init__(workers=1)
        self.copies = 0
        self.die_after = die_after

    def _copy_object(  # type: ignore[override]
        self, ctx: RunContext, dest: S3Client, target: Any, key: str, dest_key: str
    ) -> tuple[int, bool]:
        if self.copies >= self.die_after:
            raise RuntimeError("killed")
        size, done = ReplicateStage._copy_object(ctx, dest, target, key, dest_key)
        if done:
            self.copies += 1
        return size, done


def test_replicate_resumes_without_recopying(
    s3: S3Client, import_s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    ctx, _ = make_ctx(s3, import_s3, gw, tmp_path)
    Pipeline(stages(ReplicateStage())[:4]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    s3.put_object(
        Bucket=BUCKET, Key="syn/import/batch-00000/documents/part-00001.parquet", Body=b"x" * 1000
    )
    source_keys = _keys(s3, BUCKET, "syn/import/")
    assert len(source_keys) == 2, "two objects, so the kill lands mid-batch"
    dying = DyingReplicate(die_after=1)
    with pytest.raises(PipelineError, match="killed"):
        Pipeline(stages(dying)[:5]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    assert len(_keys(import_s3, IMPORT_BUCKET, "pinecone/syn/import/")) == 1
    ctx2, _ = make_ctx(s3, import_s3, gw, tmp_path)
    counting = DyingReplicate(die_after=10_000)
    Pipeline(stages(counting)).run(ctx2, S3Lock(s3, BUCKET, "syn"))
    assert counting.copies == len(source_keys) - 1, "the object copied before the kill is skipped"
    assert ctx2.manifest.is_complete(Stage.IMPORT)
    again, _ = make_ctx(s3, import_s3, gw, tmp_path)
    again.manifest.stage(Stage.REPLICATE).status = again.manifest.stage(
        Stage.REPLICATE
    ).status.PENDING
    again.manifest.stage(Stage.REPLICATE).partitions.clear()
    idle = DyingReplicate(die_after=10_000)
    Pipeline([idle]).run(again, S3Lock(s3, BUCKET, "syn"))
    assert idle.copies == 0, "a complete copy set is verified by ETag, never recopied"


def test_replicate_is_skipped_without_an_import_bucket(
    s3: S3Client, import_s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    ctx, _ = make_ctx(s3, import_s3, gw, tmp_path, with_import_bucket=False)
    Pipeline(stages(ReplicateStage())).run(ctx, S3Lock(s3, BUCKET, "syn"))
    record = ctx.manifest.stage(Stage.REPLICATE)
    assert record.outputs == {"skipped": True, "reason": "no import_bucket configured"}
    assert _keys(import_s3, IMPORT_BUCKET, "") == []
    assert gw.imports["imp-1"].uri == f"s3://{BUCKET}/syn/import/batch-00000/"


def test_import_refuses_to_run_before_replicate_when_configured(
    s3: S3Client, import_s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    ctx, _ = make_ctx(s3, import_s3, gw, tmp_path)
    Pipeline(stages(ReplicateStage())[:4]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    with pytest.raises(PipelineError, match="replicate has not completed"):
        Pipeline([ImportStage(poll_interval_s=0)]).run(ctx, S3Lock(s3, BUCKET, "syn"))


def test_teardown_cleans_the_import_bucket_zone_but_nothing_else(
    s3: S3Client, import_s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    ctx, _ = make_ctx(s3, import_s3, gw, tmp_path)
    Pipeline(stages(ReplicateStage())).run(ctx, S3Lock(s3, BUCKET, "syn"))
    import_s3.put_object(Bucket=IMPORT_BUCKET, Key="pinecone/other/keep.txt", Body=b"x")
    result = Pipeline([TeardownStage(RecordingDriver())]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    outputs = result.stage(Stage.TEARDOWN).outputs
    assert outputs["import_bucket_objects_deleted"] > 0
    assert _keys(import_s3, IMPORT_BUCKET, "") == ["pinecone/other/keep.txt"]
    assert gw.deleted == []
