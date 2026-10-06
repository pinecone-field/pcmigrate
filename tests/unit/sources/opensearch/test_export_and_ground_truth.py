"""OpenSearchSource against the fake transport: partitions, export, rescan feed, ground truth."""

import io
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.canonical import read_canonical
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export_opensearch import OpenSearchExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.sync import resolve_feed
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage, StageStatus
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.change_feed import RescanFeed, make_feed
from pcmigrate.sources.opensearch.client import ConnectionOptions
from pcmigrate.sources.opensearch.export import IdTemplate, OpenSearchSource
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from pcmigrate.targets.emitters import ImportLimits
from pcmigrate.targets.pinecone_client import VectorRecord
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway
from tests.unit.sources.opensearch.fakes import FakeTransport, make_client

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


def _mapping(transport: FakeTransport) -> Mapping:
    introspector = OpenSearchIntrospector(
        make_client(transport), IntrospectOptions(workload_window_s=0, sample_docs=100)
    )
    return build_report(
        introspector.inventory(),
        MappingOptions(migration_id="golden-os", index="golden-os", region=REGION),
    ).mapping


def _source(transport: FakeTransport, mapping: Mapping, **kw: object) -> OpenSearchSource:
    source = OpenSearchSource(ConnectionOptions("http://fake:9200", auth="none"), mapping, **kw)  # type: ignore[arg-type]
    source._client = make_client(transport)
    return source


def test_id_template_round_trip() -> None:
    template = IdTemplate.parse("golden-documents#{_id}")
    assert template.render("doc-1") == "golden-documents#doc-1"
    assert template.document_id("golden-documents#doc-1") == "doc-1"
    with pytest.raises(ValueError):
        IdTemplate.parse("docs#{doc_id}")
    with pytest.raises(ValueError):
        template.document_id("other#doc-1")


def test_partitions_and_direct_export(s3: S3Client) -> None:
    transport = FakeTransport.golden()
    mapping = _mapping(transport)
    source = _source(transport, mapping, target_rows_per_partition=400)
    partitions = source.plan_partitions(mapping)
    assert [p.id for p in partitions] == [
        "golden-documents-s0000",
        "golden-documents-s0001",
        "golden-documents-s0002",
    ]
    assert all(p.upper == "3" for p in partitions) and partitions[1].lower == "1"

    class Sink:
        def __init__(self) -> None:
            self.records = []  # type: ignore[var-annotated]

        def write(self, record) -> None:  # type: ignore[no-untyped-def]
            self.records.append(record)

    total = 0
    ids = set()
    for partition in partitions:
        sink = Sink()
        total += source.export(partition, sink)
        ids |= {r.id for r in sink.records}
    assert total == 1005 and len(ids) == 1005
    assert source.rejected == {p.id: 0 for p in partitions}


def test_ground_truth_queries() -> None:
    transport = FakeTransport.golden()
    mapping = _mapping(transport)
    source = _source(transport, mapping)
    ids = source.sample_ids("golden-documents", 5, seed=3)
    assert len(ids) == 5 and all(i.startswith("golden-documents#") for i in ids)
    assert source.sample_ids("golden-documents", 5, seed=3) == ids
    records = source.fetch_records("golden-documents", [*ids, "golden-documents#missing"])
    assert set(records) == set(ids)
    probe = records[ids[0]]
    neighbours = source.knn("golden-documents", probe.dense or [], 3)
    assert neighbours[0] == probe.id and len(neighbours) == 3
    filtered = source.knn(
        "golden-documents", probe.dense or [], 3, filter={"category": {"$eq": "news"}}
    )
    assert len(filtered) == 3
    news = source.fetch_records("golden-documents", filtered)
    assert all(r.metadata["category"] == "news" for r in news.values())
    all_ids = list(source.all_ids("golden-documents"))
    assert len(all_ids) == 1005 and len(set(all_ids)) == 1005
    filters = source.common_filters("golden-documents", 20)
    assert (filters and filters[0] == {"category": {"$eq": "blog"}}) or "category" in filters[0]
    assert all("$eq" in next(iter(f.values())) for f in filters)
    knn_requests = [p for m, p in transport.requests if p.endswith("/_search")]
    assert knn_requests


def test_knn_falls_back_to_painless_when_knn_score_is_unavailable() -> None:
    transport = FakeTransport.golden(fail_knn_score=True)
    mapping = _mapping(transport)
    source = _source(transport, mapping)
    probe = source.fetch_records("golden-documents", ["golden-documents#doc-00001"])[
        "golden-documents#doc-00001"
    ]
    assert source.knn("golden-documents", probe.dense or [], 1) == [probe.id]


def test_rescan_feed_polls_updated_documents() -> None:
    transport = FakeTransport.golden()
    index = transport.indices["golden-documents"]
    index.mapping["properties"]["updated_at"] = {"type": "date"}
    for n, doc in enumerate(index.docs.values()):
        doc["updated_at"] = f"2026-10-05T09:00:{n % 60:02d}Z"
    mapping = _mapping(transport)
    assert mapping.change_capture is not None and mapping.change_capture.rescan
    assert mapping.change_capture.watermark_column == "updated_at"
    source = _source(transport, mapping)
    feed = make_feed(source, mapping)
    assert isinstance(feed, RescanFeed) and source.rescan_feed() is not None
    boundary = feed.boundary()
    assert boundary["t0"] and boundary["watermarks"] == {"golden-documents": boundary["t0"]}
    feed.start(
        {"t0": "2026-10-05T09:00:30Z", "watermarks": {"golden-documents": "2026-10-05T09:00:30Z"}}
    )
    first = feed.poll(limit=10)
    assert len(first) == 10 and all(e.op == "upsert" and e.values is None for e in first)
    assert first[0].id.startswith("golden-documents#") and first[0].source_change_id.startswith(
        "rs:"
    )
    later = feed.watermarks["golden-documents"]
    assert isinstance(later, list) and later[0] > "2026-10-05T09:00:30Z"
    rest = []
    while batch := feed.poll(limit=500):
        rest.extend(batch)
    expected = sum(1 for d in index.docs.values() if d["updated_at"] > "2026-10-05T09:00:30Z")
    assert len(first) + len(rest) == expected
    assert feed.poll() == []
    assert make_feed(source, mapping.model_copy(update={"change_capture": None})) is None


def _ctx(
    s3: S3Client, source: OpenSearchSource, gw: FakeGateway, mapping: Mapping, tmp_path: Path
) -> RunContext:
    cfg = MigrationConfig(
        migration_id="golden-os",
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
        manifest=store.load() or Manifest(migration_id=cfg.migration_id),
        store=store,
        events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path, fingerprint="golden-os")
    return ctx


def test_direct_mode_pipeline_end_to_end_with_fakes(s3: S3Client, tmp_path: Path) -> None:
    transport = FakeTransport.golden()
    mapping = _mapping(transport)
    broken = transport.indices["golden-documents"].docs["doc-00007"]
    original_embedding = broken.pop("embedding")
    source = _source(transport, mapping, target_rows_per_partition=400)
    gw = FakeGateway(import_records=1000)
    ctx = _ctx(s3, source, gw, mapping, tmp_path)
    Pipeline(
        [
            PrepareIndexStage(),
            OpenSearchExportStage(mode="direct"),
            TransformStage(),
            EmitStage(ImportLimits()),
            ImportStage(poll_interval_s=0),
        ]
    ).run(ctx, S3Lock(s3, BUCKET, "golden-os"))
    export = ctx.manifest.stage(Stage.EXPORT).outputs
    assert (
        export["mode"] == "direct"
        and export["t0"]
        and export["source_counts"] == {"golden-documents": 1005}
    )
    assert export["change_boundary"]["watermarks"] == {"golden-documents": export["t0"]}
    assert export["rows_by_namespace"] == {"golden-documents": 1004}
    assert export["rejected_by_namespace"] == {"golden-documents": 1}
    reject_keys = [
        o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="golden-os/rejects/")["Contents"]
    ]
    assert any("/direct/golden-documents-s" in k for k in reject_keys)
    transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
    assert transform["duplicates_by_namespace"] == {"golden-documents": 4}
    assert transform["rows_by_namespace"] == {"golden-documents": 1000}
    records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["golden-documents"]))
    sample = next(r for r in records if r.id == "golden-documents#doc-00001")
    assert len(sample.dense or []) == 768 and isinstance(sample.metadata["published_at"], int)
    host = "golden-os.fake.pinecone.io"
    gw.upsert(
        host, "golden-documents", [VectorRecord(r.id, r.dense or [], r.metadata) for r in records]
    )
    assert ctx.manifest.stage(Stage.IMPORT).outputs["duplicates_upserted"] == {
        "golden-documents": 4
    }
    assert len(gw.data[host]["golden-documents"]) == 1004
    ctx_missing = _ctx(s3, source, gw, mapping, tmp_path)
    with pytest.raises(PipelineError, match="id_diff"):
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=5)]).run(
            ctx_missing, S3Lock(s3, BUCKET, "golden-os")
        )
    broken["embedding"] = original_embedding
    ctx2 = _ctx(s3, source, gw, mapping, tmp_path)
    ctx2.manifest.stage(Stage.VALIDATE).status = StageStatus.PENDING
    gw.upsert(
        host,
        "golden-documents",
        [VectorRecord("golden-documents#doc-00007", original_embedding, {})],
    )
    fixed = source.fetch_records("golden-documents", ["golden-documents#doc-00007"])
    gw.upsert(
        host,
        "golden-documents",
        [VectorRecord(r.id, r.dense or [], r.metadata) for r in fixed.values()],
    )
    Pipeline(
        [
            ValidateStage(
                settle_window_s=10,
                poll_interval_s=0,
                filter_queries=5,
                checks=("fidelity", "recall", "filters", "id_diff"),
            )
        ]
    ).run(ctx2, S3Lock(s3, BUCKET, "golden-os"))
    checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
    assert all(checks.values()) and "recall_at_k:golden-documents" in checks, checks
    assert resolve_feed(ctx2) is None, "dual-write without rescan needs no source feed"
