"""The real PgvectorSource against the golden Docker database, with S3 and Pinecone faked.

Needs only `docker compose up pgvector`. Exercises partitioning, the shared snapshot, the
named-cursor export, transform, emit, import, and every validation check, in-process.
"""

import io
import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.canonical import read_canonical
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.base import TextQuery
from pcmigrate.sources.pgvector.export import PgvectorSource
from pcmigrate.sources.pgvector.introspect import IntrospectOptions, PgvectorIntrospector
from pcmigrate.targets.applier import vector_record
from pcmigrate.targets.emitters import ImportLimits
from pcmigrate.targets.pinecone_client import VectorRecord
from tests.unit.conftest import approve_ctx
from tests.unit.fakes import FakeGateway

pytestmark = pytest.mark.integration

DSN = os.environ.get("PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden")
BUCKET = "pcmigrate-docker-test"
REGION = "us-east-1"
ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


@pytest.fixture
def mapping() -> Mapping:
    introspector = PgvectorIntrospector(
        DSN, IntrospectOptions(workload_window_s=0, tables=("documents",))
    )
    bundle = build_report(
        introspector.inventory(),
        MappingOptions(migration_id="golden-docker", index="golden-docker", region=REGION),
    )
    introspector.close()
    return bundle.mapping


def _ctx(s3, source: PgvectorSource, gw: FakeGateway, mapping: Mapping, tmp_path: Path):  # type: ignore[no-untyped-def]
    cfg = MigrationConfig(
        migration_id="golden-docker",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
        partition_rows=1000,
        workers=2,
    )
    source.target_rows_per_partition = cfg.partition_rows
    store = ManifestStore(s3, BUCKET, cfg.migration_id)
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=store.load() or Manifest(migration_id=cfg.migration_id),
        store=store,
        events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path, fingerprint="golden")
    return ctx


def test_golden_documents_end_to_end_in_process(
    mapping: Mapping, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        partitions = source.plan_partitions(mapping)
        assert len(partitions) >= 2, "1005 rows at 1000 per partition should give 2 ranges"
        assert partitions[0].lower is None and partitions[-1].upper is None

        gw = FakeGateway(import_records=1001)
        ctx = _ctx(s3, source, gw, mapping, tmp_path)
        pipeline = Pipeline(
            [
                PrepareIndexStage(),
                ExportStage(use_processes=False),
                TransformStage(),
                EmitStage(ImportLimits()),
                ImportStage(poll_interval_s=0),
            ]
        )
        pipeline.run(ctx, S3Lock(s3, BUCKET, "golden-docker"))
        export = ctx.manifest.stage(Stage.EXPORT).outputs
        assert export["rows_by_namespace"] == {"documents": 1005}
        assert export["snapshots"][0]["snapshot_id"] and export["snapshot_lsn"]
        assert "public.documents" in export["watermarks"]
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["duplicates_by_namespace"] == {"documents": 4}
        assert transform["rejected_by_namespace"] == {"documents": 0}
        assert transform["rows_by_namespace"] == {"documents": 1001}

        records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["documents"]))
        sample = next(r for r in records if r.id == "documents#1")
        assert len(sample.dense or []) == 768
        assert sample.metadata["title"] == "Document 1" and isinstance(
            sample.metadata["published_at"], int
        )
        assert sample.metadata["attrs.nested.lang"] == "en" and sample.metadata["tags"] == [
            "tag-1",
            "tag-1",
        ]

        host = "golden-docker.fake.pinecone.io"
        gw.upsert(
            host, "documents", [VectorRecord(r.id, r.dense or [], r.metadata) for r in records]
        )
        assert ctx.manifest.stage(Stage.IMPORT).outputs["duplicates_upserted"] == {"documents": 4}
        assert len(gw.data[host]["documents"]) == 1005

        ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=5)
        ctx2 = _ctx(s3, source, gw, mapping, tmp_path)
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=5)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-docker")
        )
        checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert all(checks.values()), checks
        report = (
            s3.get_object(Bucket=BUCKET, Key="golden-docker/manifest/validation.md")["Body"]
            .read()
            .decode()
        )
        assert "recall_at_k | documents | pass" in report


def test_ground_truth_queries(mapping: Mapping) -> None:
    source = PgvectorSource(DSN, mapping)
    ids = source.sample_ids("documents", 5, seed=3)
    assert len(ids) == 5 and all(i.startswith("documents#") for i in ids)
    records = source.fetch_records("documents", ids)
    assert set(records) == set(ids)
    probe = records[ids[0]]
    neighbours = source.knn("documents", probe.dense or [], 3)
    assert neighbours[0] == probe.id
    filtered = source.knn("documents", probe.dense or [], 3, filter={"category": {"$eq": "news"}})
    assert len(filtered) == 3
    all_ids = list(source.all_ids("documents"))
    assert len(all_ids) == 1005 and len(set(all_ids)) == 1005
    filters = source.common_filters("documents", 20)
    assert filters and all("$eq" in next(iter(f.values())) for f in filters)


def test_hybrid_roadmap_migrates_sparsevec_into_a_document_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Roadmap hybrid: the sparsevec column rides along, JSONL.gz emit, hybrid checks pass."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    introspector = PgvectorIntrospector(
        DSN, IntrospectOptions(workload_window_s=0, tables=("documents",))
    )
    bundle = build_report(
        introspector.inventory(),
        MappingOptions(
            migration_id="golden-hybrid", index="golden-hybrid", region=REGION, roadmap="hybrid"
        ),
    )
    introspector.close()
    mapping = bundle.mapping.model_copy(
        update={
            "validation": bundle.mapping.validation.model_copy(
                update={
                    "fidelity_sample": 100,
                    "recall_at_k": bundle.mapping.validation.recall_at_k.model_copy(
                        update={"queries": 20}
                    ),
                    "hybrid": bundle.mapping.validation.hybrid.model_copy(  # type: ignore[union-attr]
                        update={"queries": 20}
                    ),
                }
            )
        }
    )
    obj = mapping.objects[0]
    assert (
        obj.sparse is not None
        and obj.sparse.column == "sparse"
        and mapping.target.type == "document"
    )
    assert any(
        f.check == "hybrid_readiness" and f.severity.value == "info" for f in bundle.findings
    )
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        gw = FakeGateway(import_records=1001)
        cfg = MigrationConfig(
            migration_id="golden-hybrid",
            bucket=BUCKET,
            aws_region=REGION,
            pinecone_api_key_secret=ARN,
            canonical_part_bytes=8 * 1024 * 1024,
            partition_rows=1000,
            workers=2,
        )
        store = ManifestStore(s3, BUCKET, cfg.migration_id)
        ctx = RunContext(
            config=cfg,
            mapping=mapping,
            manifest=Manifest(migration_id=cfg.migration_id),
            store=store,
            events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
            s3=s3,
            source=source,
            target=gw,
        )
        approve_ctx(ctx, tmp_path, fingerprint="golden-hybrid")
        Pipeline(
            [
                PrepareIndexStage(),
                ExportStage(use_processes=False),
                TransformStage(),
                EmitStage(ImportLimits()),
                ImportStage(poll_interval_s=0),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, "golden-hybrid"))
        assert ctx.manifest.stage(Stage.PREPARE_INDEX).outputs["type"] == "document"
        assert ctx.manifest.stage(Stage.EMIT).outputs["format"] == "jsonl.gz"
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["rejected_by_namespace"] == {"documents": 0}
        records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["documents"]))
        assert all(r.sparse is not None and len(r.sparse.indices) >= 1 for r in records)
        host = "golden-hybrid.fake.pinecone.io"
        gw.upsert(
            host,
            "documents",
            [VectorRecord(r.id, r.dense or [], r.metadata, sparse=r.sparse) for r in records],
        )
        ctx2 = RunContext(
            config=cfg,
            mapping=mapping,
            manifest=store.load() or ctx.manifest,
            store=store,
            events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
            s3=s3,
            source=source,
            target=gw,
        )
        approve_ctx(ctx2, tmp_path, fingerprint="golden-hybrid")
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=3)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-hybrid")
        )
        checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
        assert all(checks.values()), checks
        probe = source.fetch_records("documents", ["documents#1"])["documents#1"]
        assert probe.sparse is not None
        assert source.knn_sparse("documents", probe.sparse, 3)[0] == probe.id


def test_fts_roadmap_carries_body_and_summary_into_full_text_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Roadmap fts: body (english tsvector) and summary (simple GIN expression index) become
    full_text_search fields; validation reports overlap per query class against ts_rank_cd."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    introspector = PgvectorIntrospector(
        DSN, IntrospectOptions(workload_window_s=0, tables=("documents",))
    )
    bundle = build_report(
        introspector.inventory(),
        MappingOptions(migration_id="golden-fts", index="golden-fts", region=REGION, roadmap="fts"),
    )
    introspector.close()
    obj = bundle.mapping.objects[0]
    assert bundle.mapping.target.type == "document" and bundle.mapping.full_text
    assert {t.source: (t.field_name, t.derived_from) for t in obj.text} == {
        "body": ("body", "body_tsv"),
        "summary": ("summary", "documents_summary_fts_idx"),
    }
    by_source = {t.source: t.full_text_search for t in obj.text}
    assert by_source["body"].stemming and by_source["body"].stop_words
    assert by_source["body"].language == "en" and not by_source["summary"].stemming
    assert not any(f.source in ("body", "summary") and f.action == "map" for f in obj.metadata)
    assert {f.check for f in bundle.findings} >= {"fts_fields", "fts_validation"}
    assert "Full-text field" in bundle.report_md
    mapping = bundle.mapping.model_copy(
        update={
            "validation": bundle.mapping.validation.model_copy(
                update={
                    "fidelity_sample": 100,
                    "recall_at_k": bundle.mapping.validation.recall_at_k.model_copy(
                        update={"queries": 20}
                    ),
                    "full_text": bundle.mapping.validation.full_text.model_copy(  # type: ignore[union-attr]
                        update={"queries": 40}
                    ),
                }
            )
        }
    )
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        gw = FakeGateway(import_records=1001)
        cfg = MigrationConfig(
            migration_id="golden-fts",
            bucket=BUCKET,
            aws_region=REGION,
            pinecone_api_key_secret=ARN,
            canonical_part_bytes=8 * 1024 * 1024,
            partition_rows=1000,
            workers=2,
        )
        store = ManifestStore(s3, BUCKET, cfg.migration_id)
        ctx = RunContext(
            config=cfg,
            mapping=mapping,
            manifest=Manifest(migration_id=cfg.migration_id),
            store=store,
            events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
            s3=s3,
            source=source,
            target=gw,
        )
        approve_ctx(ctx, tmp_path, fingerprint="golden-fts")
        Pipeline(
            [
                PrepareIndexStage(),
                ExportStage(use_processes=False),
                TransformStage(),
                EmitStage(ImportLimits()),
                ImportStage(poll_interval_s=0),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, "golden-fts"))
        prepared = ctx.manifest.stage(Stage.PREPARE_INDEX).outputs
        assert prepared["type"] == "document" and prepared["text_fields"] == ["body", "summary"]
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["rejected_by_namespace"] == {"documents": 0}
        records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["documents"]))
        with_body = [r for r in records if "body" in r.text_fields]
        assert len(with_body) == 1000 - 1000 // 6 and all("body" not in r.metadata for r in records)
        assert sum("summary" in r.text_fields for r in records) == 1000 - 1000 // 9
        host = "golden-fts.fake.pinecone.io"
        gw.upsert(host, "documents", [vector_record(r, obj.text_sources) for r in records])
        ctx2 = RunContext(
            config=cfg,
            mapping=mapping,
            manifest=store.load() or ctx.manifest,
            store=store,
            events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
            s3=s3,
            source=source,
            target=gw,
        )
        approve_ctx(ctx2, tmp_path, fingerprint="golden-fts")
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=3)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-fts")
        )
        checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert checks["full_text_overlap:documents"], checks
        assert all(checks.values()), checks
        validation = json.loads(
            s3.get_object(Bucket=BUCKET, Key="golden-fts/manifest/validation.json")["Body"].read()
        )
        outcome = next(c for c in validation["checks"] if c["check"] == "full_text_overlap")
        classes = outcome["details"]["classes"]
        assert all(c["queries"] > 0 for c in classes.values()), classes
        probe = source.fetch_records("documents", ["documents#5"])["documents#5"]
        rare = next(
            w
            for w in probe.text_fields["body"].lower().replace(".", "").split()
            if w.endswith(
                ("wren", "moth", "finch", "lark", "stag", "hare", "vole", "newt", "crane", "pike")
            )
        )
        assert source.knn_text("documents", TextQuery("body", rare), 5)[0] == probe.id
        phrase = " ".join(probe.text_fields["summary"].lower().rstrip(".").split()[-2:])
        assert probe.id in source.knn_text(
            "documents", TextQuery("summary", phrase, mode="phrase"), 10
        )


def test_generated_sparse_encodes_title_and_body_from_the_golden_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--sparse-from title body: text rides through canonical, transform encodes it (ADR-017)."""
    from pcmigrate.validate.ground_truth import CanonicalGroundTruth
    from tests.unit.fakes import FakeSparseEncoder

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    introspector = PgvectorIntrospector(
        DSN, IntrospectOptions(workload_window_s=0, tables=("documents",))
    )
    bundle = build_report(
        introspector.inventory(),
        MappingOptions(
            migration_id="golden-gen",
            index="golden-gen",
            region=REGION,
            roadmap="hybrid",
            sparse_from=("title", "body"),
        ),
    )
    introspector.close()
    obj = bundle.mapping.objects[0]
    assert obj.sparse is not None and obj.sparse.generated
    assert obj.sparse.text_fields == ["title", "body"] and bundle.mapping.target.type == "document"
    assert {f.check for f in bundle.findings} >= {"sparse_generation_cost", "sparse_query_encoding"}
    assert "hybrid (generated)" in bundle.report_md
    mapping = bundle.mapping.model_copy(
        update={
            "validation": bundle.mapping.validation.model_copy(
                update={
                    "fidelity_sample": 100,
                    "recall_at_k": bundle.mapping.validation.recall_at_k.model_copy(
                        update={"queries": 20}
                    ),
                    "hybrid": bundle.mapping.validation.hybrid.model_copy(  # type: ignore[union-attr]
                        update={"queries": 20}
                    ),
                }
            )
        }
    )
    encoder = FakeSparseEncoder()
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        gw = FakeGateway(import_records=1001)
        cfg = MigrationConfig(
            migration_id="golden-gen",
            bucket=BUCKET,
            aws_region=REGION,
            pinecone_api_key_secret=ARN,
            canonical_part_bytes=8 * 1024 * 1024,
            partition_rows=1000,
            workers=2,
        )
        store = ManifestStore(s3, BUCKET, cfg.migration_id)

        def context(manifest: Manifest) -> RunContext:
            ctx = RunContext(
                config=cfg,
                mapping=mapping,
                manifest=manifest,
                store=store,
                events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
                s3=s3,
                source=source,
                target=gw,
            )
            ctx.extras["sparse_encoder"] = encoder
            approve_ctx(ctx, tmp_path, fingerprint="golden-gen")
            return ctx

        ctx = context(Manifest(migration_id=cfg.migration_id))
        Pipeline(
            [
                PrepareIndexStage(),
                ExportStage(use_processes=False),
                TransformStage(),
                EmitStage(ImportLimits()),
                ImportStage(poll_interval_s=0),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, "golden-gen"))
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["rejected_by_namespace"] == {"documents": 0}, transform
        stats = transform["sparse_by_namespace"]["documents"]
        assert stats["generated"] == 1005 and stats["no_text"] == 0 and stats["requests"] >= 1
        records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["documents"]))
        assert all(r.sparse is not None and r.text_fields == {} for r in records)
        raw = source.fetch_records("documents", [records[0].id])[records[0].id]
        assert raw.sparse is None and set(raw.text_fields) <= {"title", "body"}
        text = "\n".join(raw.text_fields[f] for f in ("title", "body") if f in raw.text_fields)
        assert records[0].sparse == encoder.vector(text)
        host = "golden-gen.fake.pinecone.io"
        gw.upsert(
            host,
            "documents",
            [VectorRecord(r.id, r.dense or [], r.metadata, sparse=r.sparse) for r in records],
        )
        ctx2 = context(store.load() or ctx.manifest)
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=3)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-gen")
        )
        outputs = ctx2.manifest.stage(Stage.VALIDATE).outputs
        checks = outputs["checks"]
        assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
        assert all(checks.values()), checks
        assert outputs["sparse_models"] == {"documents": "pinecone-sparse-english-v0"}
        truth = CanonicalGroundTruth(
            s3, BUCKET, {"documents": transform["parts_by_namespace"]["documents"]}
        )
        probe = truth.fetch_records("documents", [records[5].id])[records[5].id]
        assert probe.sparse is not None
        assert truth.knn_sparse("documents", probe.sparse, 3)[0] == probe.id
