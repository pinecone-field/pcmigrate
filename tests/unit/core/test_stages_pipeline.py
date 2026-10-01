"""The shared load path end to end on moto S3 and a fake Pinecone, including kill-and-resume."""

import io
import json
from pathlib import Path
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.canonical import read_canonical
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage, shared_dense_spec
from pcmigrate.core.stages.validate import ValidateStage, ValidationFailedError
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage, StageStatus
from pcmigrate.sources.base import Partition, RecordSink
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.emitters import ImportLimits
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
ROWS = 2_000
DIM = 16

MAPPING = f"""
migration_id: syn
source: {{kind: synthetic}}
target: {{index: syn-idx, region: us-east-1}}
objects:
  - source: synthetic.documents
    namespace: documents
    id_template: "documents#{{id}}"
    dense: {{column: embedding, dimension: {DIM}, metric: cosine}}
"""

LIMITS = ImportLimits(target_part_bytes=512 * 1024 * 1024)


def stages() -> list[Any]:
    return [
        PrepareIndexStage(),
        ExportStage(workers=2),
        EmitStage(LIMITS),
        ImportStage(poll_interval_s=0),
        ValidateStage(settle_window_s=30, poll_interval_s=0),
    ]


def make_ctx(
    s3: S3Client, source: SyntheticSource, gw: FakeGateway, tmp_path: Path
) -> tuple[RunContext, io.StringIO]:
    out = io.StringIO()
    cfg = MigrationConfig(
        migration_id="syn",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
    )
    store = ManifestStore(s3, BUCKET, "syn")
    manifest = store.load() or Manifest(migration_id="syn")
    ctx = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(MAPPING),
        manifest=manifest,
        store=store,
        events=EventBus("syn", [JsonLinesSink(out)]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path)
    return ctx, out


@pytest.fixture
def gw() -> FakeGateway:
    g = FakeGateway(import_records=ROWS)
    g.pending_namespace_counts = {"documents": ROWS}
    return g


def test_full_pipeline(s3: S3Client, gw: FakeGateway, tmp_path: Path) -> None:
    source = SyntheticSource(rows=ROWS, dimension=DIM, partitions=3)
    ctx, out = make_ctx(s3, source, gw, tmp_path)
    Pipeline(stages()).run(ctx, S3Lock(s3, BUCKET, "syn"))
    m = ctx.manifest
    assert all(
        m.is_complete(s)
        for s in (Stage.PREPARE_INDEX, Stage.EXPORT, Stage.EMIT, Stage.IMPORT, Stage.VALIDATE)
    )
    assert m.stage(Stage.EXPORT).outputs["rows_by_namespace"] == {"documents": ROWS}
    assert len(m.stage(Stage.EXPORT).complete_partitions) == 3
    batches = m.stage(Stage.EMIT).outputs["batches"]
    assert batches[0]["uri"] == f"s3://{BUCKET}/syn/import/batch-00000/"
    assert gw.imports["imp-1"].uri == batches[0]["uri"]
    assert m.stage(Stage.IMPORT).outputs["records_imported"] == ROWS
    assert m.stage(Stage.VALIDATE).outputs["count_parity"]["passed"] is True
    keys = [k for ks in m.stage(Stage.EXPORT).outputs["parts_by_namespace"].values() for k in ks]
    assert len(list(read_canonical(s3, BUCKET, keys))) == ROWS
    types = [json.loads(line)["type"] for line in out.getvalue().splitlines()]
    assert "index_prepared" in types and "import_started" in types and "validation_result" in types


class DyingSource(SyntheticSource):
    """Raises after exporting some partitions, standing in for a killed process."""

    die_after: int = 1

    def export(self, partition: Partition, writer: RecordSink) -> int:
        if len(self.exported_partitions) >= self.die_after:
            raise RuntimeError("killed")
        return super().export(partition, writer)


def test_export_resumes_only_incomplete_partitions(
    s3: S3Client, gw: FakeGateway, tmp_path: Path
) -> None:
    dying = DyingSource(rows=ROWS, dimension=DIM, partitions=4)
    ctx, _ = make_ctx(s3, dying, gw, tmp_path)
    with pytest.raises(PipelineError):
        Pipeline([PrepareIndexStage(), ExportStage(workers=1)]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    stored = ManifestStore(s3, BUCKET, "syn").load()
    assert stored is not None
    assert stored.stage(Stage.EXPORT).status is StageStatus.FAILED
    done_before = stored.stage(Stage.EXPORT).complete_partitions
    assert done_before == ["p0000"]

    fresh = SyntheticSource(rows=ROWS, dimension=DIM, partitions=4)
    ctx2, out = make_ctx(s3, fresh, gw, tmp_path)
    assert ctx2.manifest.is_complete(Stage.PREPARE_INDEX)
    Pipeline(stages()).run(ctx2, S3Lock(s3, BUCKET, "syn"))
    assert sorted(fresh.exported_partitions) == ["p0001", "p0002", "p0003"]
    assert gw.created == ["syn-idx"]
    assert ctx2.manifest.stage(Stage.EXPORT).outputs["rows"] == ROWS
    assert ctx2.manifest.is_complete(Stage.VALIDATE)
    keys = [
        k
        for ks in ctx2.manifest.stage(Stage.EXPORT).outputs["parts_by_namespace"].values()
        for k in ks
    ]
    ids = [r.id for r in read_canonical(s3, BUCKET, keys)]
    assert len(ids) == ROWS and len(set(ids)) == ROWS
    assert "stage_skipped" in out.getvalue()


def test_import_resume_polls_existing_import(s3: S3Client, gw: FakeGateway, tmp_path: Path) -> None:
    source = SyntheticSource(rows=ROWS, dimension=DIM, partitions=1)
    ctx, _ = make_ctx(s3, source, gw, tmp_path)
    Pipeline(stages()[:3]).run(ctx, S3Lock(s3, BUCKET, "syn"))
    import_id = gw.start_import(
        "syn-idx.fake.pinecone.io", ctx.manifest.stage(Stage.EMIT).outputs["batches"][0]["uri"]
    )
    ctx.manifest.stage(Stage.IMPORT).outputs["import_ids"] = {"batch-00000": import_id}
    ctx.manifest.mark_running(Stage.IMPORT)
    ctx.manifest.mark_failed(Stage.IMPORT, "killed while polling")
    ctx.checkpoint()
    ctx2, _ = make_ctx(s3, source, gw, tmp_path)
    Pipeline(stages()).run(ctx2, S3Lock(s3, BUCKET, "syn"))
    assert list(gw.imports) == [import_id]
    assert ctx2.manifest.stage(Stage.IMPORT).outputs["records_imported"] == ROWS


def test_validate_fails_on_count_mismatch(s3: S3Client, tmp_path: Path) -> None:
    gw = FakeGateway(import_records=ROWS)
    gw.pending_namespace_counts = {"documents": ROWS - 1}
    ctx, _ = make_ctx(s3, SyntheticSource(rows=ROWS, dimension=DIM, partitions=1), gw, tmp_path)
    with pytest.raises(PipelineError) as info:
        Pipeline(stages()).run(ctx, S3Lock(s3, BUCKET, "syn"))
    assert isinstance(info.value.cause, ValidationFailedError)
    assert ctx.manifest.stage(Stage.VALIDATE).status is StageStatus.FAILED


def test_shared_dense_spec_requires_agreement() -> None:
    mapping = Mapping.from_yaml(MAPPING)
    assert shared_dense_spec(mapping).dimension == DIM
    other = mapping.model_copy(
        update={
            "objects": [
                *mapping.objects,
                mapping.objects[0].model_copy(
                    update={
                        "namespace": "x",
                        "dense": mapping.objects[0].dense.model_copy(update={"dimension": 32}),
                    }  # type: ignore[union-attr]
                ),
            ]
        }
    )
    with pytest.raises(ValueError, match="disagree"):
        shared_dense_spec(other)
