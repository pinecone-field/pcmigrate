"""The real OpenSearchSource against the golden Docker index, with S3 and Pinecone faked.

Needs `docker compose up -d --wait opensearch && docker compose run --rm opensearch-seed`.
Exercises introspection (read-only audit, findings, snapshot), the direct export through
point-in-time slices, transform, emit, import, every validation check with script_score
ground truth, the rescan feed, and the _source blocker on a scratch index.
"""

import io
import json
import os
import time
import uuid
from pathlib import Path

import boto3
import pytest
import requests
from moto import mock_aws

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.canonical import read_canonical
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export_opensearch import OpenSearchExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.change_feed import make_feed
from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient
from pcmigrate.sources.opensearch.export import OpenSearchSource
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from pcmigrate.targets.emitters import ImportLimits
from pcmigrate.targets.pinecone_client import VectorRecord
from tests.unit.conftest import approve_ctx
from tests.unit.fakes import FakeGateway

pytestmark = pytest.mark.integration

URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
INDEX = "golden-documents"
BUCKET = "pcmigrate-docker-test"
REGION = "us-east-1"
ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
EXPECTED = Path(__file__).resolve().parents[1] / "fixtures" / "golden" / "opensearch" / "expected"
OPTIONS = MappingOptions(migration_id="golden-os", index="golden-os", region=REGION)


def _options() -> ConnectionOptions:
    return ConnectionOptions(URL, auth="none")


def _introspector(**kw: object) -> OpenSearchIntrospector:
    opts = IntrospectOptions(workload_window_s=0, sample_docs=500, indices=(INDEX,), **kw)  # type: ignore[arg-type]
    return OpenSearchIntrospector(OpenSearchClient(_options()), opts)


@pytest.fixture(scope="module")
def mapping() -> Mapping:
    introspector = _introspector()
    bundle = build_report(introspector.inventory(), OPTIONS)
    introspector.close()
    return bundle.mapping


def test_introspection_is_read_only_and_matches_snapshot() -> None:
    introspector = _introspector()
    inventory = introspector.inventory()
    bundle = build_report(inventory, OPTIONS)
    introspector.close()
    for method, path in introspector.audit.requests:
        assert method in ("GET", "HEAD", "POST", "DELETE"), (method, path)
        if method == "POST":
            assert path.endswith(("/_search", "/_count", "/_search/point_in_time", "/_mget")), path
        if method == "DELETE":
            assert path == "/_search/point_in_time"
    table = inventory.tables[0]
    snapshot = {
        "findings": [
            {"check": f.check, "severity": f.severity.value, "object": f.object, "field": f.field}
            for f in bundle.findings
        ],
        "index": {
            "name": table.qualified_name,
            "rows": table.rows,
            "shards": table.shards,
            "primary_key": table.primary_key,
            "settings": table.settings,
            "columns": {c.name: c.base_type for c in table.columns},
            "vectors": [
                [
                    v.column,
                    v.kind.value,
                    v.dimension,
                    v.metric.value,
                    v.space_type,
                    v.engine,
                    v.in_source,
                ]
                for v in table.vectors
            ],
            "text_search": [[t.column, t.kind, t.analyzer] for t in table.text_search],
            "duplicates": {k: d.estimated_duplicate_rows for k, d in table.duplicates.items()},
        },
    }
    mapping_yaml = bundle.mapping.to_yaml()
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        EXPECTED.mkdir(parents=True, exist_ok=True)
        (EXPECTED / "snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n")
        (EXPECTED / "mapping.yaml").write_text(mapping_yaml)
    assert snapshot == json.loads((EXPECTED / "snapshot.json").read_text())
    assert Mapping.from_yaml(mapping_yaml) == Mapping.from_yaml(
        (EXPECTED / "mapping.yaml").read_text()
    )
    assert not [f for f in bundle.findings if f.severity.value == "blocker"]
    assert "OpenSearch 2." in bundle.report_md and "script_score with knn_score" in bundle.report_md


def test_vector_excluded_from_source_is_a_blocker() -> None:
    name = f"pcm-scratch-{uuid.uuid4().hex[:6]}"
    body = json.loads((EXPECTED.parent / "index.json").read_text())
    body["mappings"]["_source"] = {"excludes": ["embedding"]}
    requests.put(f"{URL}/{name}", json=body, timeout=30).raise_for_status()
    try:
        docs = requests.post(
            f"{URL}/{INDEX}/_search", json={"size": 20, "_source": True}, timeout=30
        ).json()["hits"]["hits"]
        lines = []
        for hit in docs:
            lines.append(json.dumps({"index": {"_index": name, "_id": hit["_id"]}}))
            lines.append(json.dumps(hit["_source"]))
        requests.post(
            f"{URL}/_bulk?refresh=wait_for",
            data="\n".join(lines) + "\n",
            headers={"Content-Type": "application/json"},
            timeout=60,
        ).raise_for_status()
        introspector = OpenSearchIntrospector(
            OpenSearchClient(_options()),
            IntrospectOptions(workload_window_s=0, sample_docs=50, indices=(name,)),
        )
        bundle = build_report(introspector.inventory(), OPTIONS)
        introspector.close()
        vector = bundle.inventory.tables[0].vectors[0]
        assert vector.in_source is False and vector.source_setting == "_source.excludes: embedding"
        blocker = next(f for f in bundle.findings if f.check == "vector_source")
        assert (
            blocker.severity.value == "blocker" and "_source.excludes: embedding" in blocker.message
        )
    finally:
        requests.delete(f"{URL}/{name}", timeout=30)


def _ctx(s3, source: OpenSearchSource, gw: FakeGateway, mapping: Mapping, tmp_path: Path):  # type: ignore[no-untyped-def]
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


def test_golden_index_end_to_end_in_process(
    mapping: Mapping, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = OpenSearchSource(_options(), mapping, target_rows_per_partition=400)
        partitions = source.plan_partitions(mapping)
        assert len(partitions) == 3, "1005 documents at 400 per slice give three slices"
        gw = FakeGateway(import_records=1001)
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
        assert export["rows_by_namespace"] == {INDEX: 1005} and export["source_counts"] == {
            INDEX: 1005
        }
        assert export["t0"] and export["change_boundary"]["watermarks"] == {INDEX: export["t0"]}
        assert export["rejected_by_namespace"] == {INDEX: 0}
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["duplicates_by_namespace"] == {INDEX: 4}
        assert transform["rejected_by_namespace"] == {INDEX: 0}
        assert transform["rows_by_namespace"] == {INDEX: 1001}
        records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"][INDEX]))
        sample = next(r for r in records if r.id == f"{INDEX}#doc-00001")
        assert len(sample.dense or []) == 768
        assert sample.metadata["title"] == "Document 1" and isinstance(
            sample.metadata["published_at"], int
        )
        assert json.loads(sample.metadata["author"])["name"] == "author-1"
        assert sample.metadata["views"] == 3 and sample.metadata["category"] == "blog"

        host = "golden-os.fake.pinecone.io"
        gw.upsert(host, INDEX, [VectorRecord(r.id, r.dense or [], r.metadata) for r in records])
        assert ctx.manifest.stage(Stage.IMPORT).outputs["duplicates_upserted"] == {INDEX: 4}
        assert len(gw.data[host][INDEX]) == 1005

        ctx2 = _ctx(s3, source, gw, mapping, tmp_path)
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=5)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-os")
        )
        checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert all(checks.values()), checks
        report = (
            s3.get_object(Bucket=BUCKET, Key="golden-os/manifest/validation.md")["Body"]
            .read()
            .decode()
        )
        assert f"recall_at_k | {INDEX} | pass" in report
        source.close()


def test_ground_truth_queries_use_script_score(mapping: Mapping) -> None:
    source = OpenSearchSource(_options(), mapping)
    ids = source.sample_ids(INDEX, 5, seed=3)
    assert len(ids) == 5 and all(i.startswith(f"{INDEX}#") for i in ids)
    records = source.fetch_records(INDEX, ids)
    assert set(records) == set(ids)
    probe = records[ids[0]]
    neighbours = source.knn(INDEX, probe.dense or [], 3)
    assert neighbours[0] == probe.id
    filtered = source.knn(INDEX, probe.dense or [], 3, filter={"category": {"$eq": "news"}})
    assert len(filtered) == 3
    assert all(
        r.metadata["category"] == "news" for r in source.fetch_records(INDEX, filtered).values()
    )
    all_ids = list(source.all_ids(INDEX))
    assert len(all_ids) == 1005 and len(set(all_ids)) == 1005
    filters = source.common_filters(INDEX, 20)
    assert filters and all("$eq" in next(iter(f.values())) for f in filters)
    searches = [p for m, p in source.client.audit.requests if p.endswith("/_search")]
    assert searches, "ground truth went through _search"
    source.close()


def test_rescan_feed_sees_documents_updated_after_t0(mapping: Mapping) -> None:
    name = f"pcm-rescan-{uuid.uuid4().hex[:6]}"
    body = json.loads((EXPECTED.parent / "index.json").read_text())
    body["mappings"]["properties"]["updated_at"] = {"type": "date"}
    requests.put(f"{URL}/{name}", json=body, timeout=30).raise_for_status()
    try:
        hits = requests.post(
            f"{URL}/{INDEX}/_search", json={"size": 30, "_source": True}, timeout=30
        ).json()["hits"]["hits"]
        t0 = "2026-10-05T10:00:00.000Z"
        lines = []
        for n, hit in enumerate(hits):
            doc = {
                **hit["_source"],
                "updated_at": "2026-10-05T09:59:00Z" if n < 10 else "2026-10-05T10:00:01Z",
            }
            lines.append(json.dumps({"index": {"_index": name, "_id": hit["_id"]}}))
            lines.append(json.dumps(doc))
        requests.post(
            f"{URL}/_bulk?refresh=wait_for",
            data="\n".join(lines) + "\n",
            headers={"Content-Type": "application/json"},
            timeout=60,
        ).raise_for_status()
        introspector = OpenSearchIntrospector(
            OpenSearchClient(_options()),
            IntrospectOptions(workload_window_s=0, sample_docs=50, indices=(name,)),
        )
        scratch = build_report(introspector.inventory(), OPTIONS).mapping
        introspector.close()
        assert (
            scratch.change_capture is not None
            and scratch.change_capture.watermark_column == "updated_at"
        )
        source = OpenSearchSource(_options(), scratch)
        feed = make_feed(source, scratch)
        assert feed is not None
        feed.start({"t0": t0, "watermarks": {name: t0}})
        seen = []
        while batch := feed.poll(limit=7):
            seen.extend(batch)
        assert len(seen) == 20 and all(e.op == "upsert" and e.values is None for e in seen)
        assert len({e.id for e in seen}) == 20
        time.sleep(0.1)
        source.close()
    finally:
        requests.delete(f"{URL}/{name}", timeout=30)


def test_hybrid_roadmap_migrates_rank_features_into_a_document_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Roadmap hybrid: rank_features tokens hash into sparse indices, vocabulary is written."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    introspector = _introspector()
    bundle = build_report(
        introspector.inventory(),
        MappingOptions(
            migration_id="golden-os-hybrid",
            index="golden-os-hybrid",
            region=REGION,
            roadmap="hybrid",
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
                    "hybrid": bundle.mapping.validation.hybrid.model_copy(update={"queries": 20}),  # type: ignore[union-attr]
                }
            )
        }
    )
    obj = mapping.objects[0]
    assert obj.sparse is not None and obj.sparse.kind.value == "rank_features"
    assert any(f.check == "sparse_vocabulary" for f in bundle.findings)
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = OpenSearchSource(_options(), mapping, target_rows_per_partition=400)
        gw = FakeGateway(import_records=1001)
        cfg = MigrationConfig(
            migration_id="golden-os-hybrid",
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
        approve_ctx(ctx, tmp_path, fingerprint="golden-os-hybrid")
        Pipeline(
            [
                PrepareIndexStage(),
                OpenSearchExportStage(mode="direct"),
                TransformStage(),
                EmitStage(ImportLimits()),
                ImportStage(poll_interval_s=0),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, "golden-os-hybrid"))
        export = ctx.manifest.stage(Stage.EXPORT).outputs
        vocabulary = export["vocabulary"][INDEX]
        assert vocabulary["tokens"] == 200 and vocabulary["collisions"] == 0
        stored = json.loads(s3.get_object(Bucket=BUCKET, Key=vocabulary["key"])["Body"].read())
        assert stored["function"] == "xxh32" and len(stored["tokens"]) == 200
        records = list(
            read_canonical(
                s3, BUCKET, ctx.manifest.stage(Stage.TRANSFORM).outputs["parts_by_namespace"][INDEX]
            )
        )
        assert all(r.sparse is not None and len(r.sparse.indices) == 6 for r in records)
        host = "golden-os-hybrid.fake.pinecone.io"
        gw.upsert(
            host,
            INDEX,
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
        approve_ctx(ctx2, tmp_path, fingerprint="golden-os-hybrid")
        Pipeline([ValidateStage(settle_window_s=10, poll_interval_s=0, filter_queries=3)]).run(
            ctx2, S3Lock(s3, BUCKET, "golden-os-hybrid")
        )
        checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert checks["sparse_overlap:" + INDEX] and checks["hybrid_overlap:" + INDEX], checks
        assert all(checks.values()), checks
        probe = source.fetch_records(INDEX, [f"{INDEX}#doc-00001"])[f"{INDEX}#doc-00001"]
        assert probe.sparse is not None
        assert source.knn_sparse(INDEX, probe.sparse, 3)[0] == probe.id
        source.close()
