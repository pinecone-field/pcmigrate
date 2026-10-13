"""Generated sparse vectors from text (ADR-017): config, encoding, transform, validation."""

from __future__ import annotations

import io
import math
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.config.mapping import ObjectMapping, SparseKind, SparseModelSpec
from pcmigrate.core.buffer import ChangeEnvelope, InMemoryBuffer
from pcmigrate.core.canonical import CanonicalRecord, CanonicalWriter, SparseVector
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.sparse import concat_text, sparse_dot, text_hash
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.core.transform import (
    NO_TEXT,
    SparseCache,
    SparseEnricher,
    json_to_record,
    record_to_json,
    transform_namespace,
)
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping, hybrid_readiness
from pcmigrate.sources.pgvector.records import RecordBuilder
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.applier import Applier
from pcmigrate.targets.emitters.vector_parquet import ImportLimits
from pcmigrate.targets.pinecone_client import VectorRecord
from pcmigrate.targets.sparse_encoder import (
    PineconeSparseEncoder,
    SparseEncodingError,
    TokenThrottle,
)
from pcmigrate.validate.ground_truth import CanonicalGroundTruth
from pcmigrate.validate.hybrid import check_hybrid
from pcmigrate.validate.reconcile import reconcile
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway, FakeSparseEncoder
from tests.unit.report.fixtures import documents_table, inventory

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def _generated_object(**overrides: Any) -> ObjectMapping:
    data: dict[str, Any] = {
        "source": "public.documents",
        "namespace": "documents",
        "id_template": "documents#{id}",
        "dense": {"column": "embedding", "dimension": 4, "metric": "cosine"},
        "sparse": {"kind": "generated", "text_fields": ["title", "body"]},
    }
    data.update(overrides)
    return ObjectMapping.model_validate(data)


def _mapping(obj: ObjectMapping, **validation: Any) -> Mapping:
    return Mapping(
        migration_id="gen",
        source={"kind": "synthetic"},  # type: ignore[arg-type]
        target={"index": "gen", "region": REGION, "type": "document"},  # type: ignore[arg-type]
        objects=[obj],
        validation=validation,  # type: ignore[arg-type]
    )


# --- config ------------------------------------------------------------------------------


def test_generated_spec_defaults_and_round_trip() -> None:
    obj = _generated_object()
    spec = obj.sparse
    assert spec is not None and spec.generated and spec.kind is SparseKind.GENERATED
    assert spec.vocabulary == "model" and spec.column is None
    assert spec.model == SparseModelSpec() and spec.model.name == "pinecone-sparse-english-v0"
    mapping = _mapping(obj)
    assert mapping.hybrid and mapping.generates_sparse
    again = Mapping.from_yaml(mapping.to_yaml())
    assert again == mapping and "kind: generated" in mapping.to_yaml()
    assert obj.generated_sparse is spec


@pytest.mark.parametrize(
    ("sparse", "message"),
    [
        ({"kind": "generated"}, "at least one text field"),
        ({"kind": "generated", "column": "x", "text_fields": ["t"]}, "not a column"),
        ({"kind": "generated", "text_fields": ["t", "t"]}, "repeat"),
        ({"kind": "generated", "text_fields": ["t"], "vocabulary": "xxh32"}, "vocabulary: model"),
        ({"kind": "sparsevec", "column": "s", "text_fields": ["t"]}, "kind: generated only"),
        ({"kind": "sparsevec"}, "need a column"),
        ({"kind": "generated", "text_fields": ["t"], "model": {"api_key_secret": "pcsk_x"}}, "arn"),
    ],
)
def test_generated_spec_validation(sparse: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _generated_object(sparse=sparse)


def test_generated_text_cannot_be_the_dense_column() -> None:
    with pytest.raises(ValueError, match="dense column"):
        _generated_object(sparse={"kind": "generated", "text_fields": ["embedding"]})


def test_generated_sparse_still_needs_a_document_index() -> None:
    with pytest.raises(ValueError, match="document"):
        Mapping(
            migration_id="gen",
            source={"kind": "synthetic"},  # type: ignore[arg-type]
            target={"index": "gen", "region": REGION},  # type: ignore[arg-type]
            objects=[_generated_object()],
        )


# --- records and text ------------------------------------------------------------------


def test_record_builder_carries_text_fields_and_no_sparse() -> None:
    builder = RecordBuilder(_generated_object(), ("id",))
    assert builder.source_columns == ["id", "embedding", "title", "body"]
    record = builder.build(
        {"id": 1, "embedding": [0.1, 0.2, 0.3, 0.4], "title": "Hello", "body": None}
    )
    assert record.text_fields == {"title": "Hello"} and record.sparse is None
    listy = builder.build(
        {"id": 2, "embedding": [0.1, 0.2, 0.3, 0.4], "title": ["a", None, "b"], "body": "  "}
    )
    assert listy.text_fields == {"title": "a b"}
    line = record_to_json(record)
    assert json_to_record(line).text_fields == {"title": "Hello"}


def test_concat_text_order_separator_and_hash() -> None:
    fields = {"body": "second", "title": " first ", "other": "ignored"}
    assert concat_text(fields, ["title", "body"], "\n") == "first\nsecond"
    assert concat_text(fields, ["body", "title"], " | ") == "second | first"
    assert concat_text({"title": "  "}, ["title", "body"], "\n") == ""
    assert text_hash("m1", "x") != text_hash("m2", "x") != text_hash("m2", "y")


# --- enricher, cache, transform ---------------------------------------------------------


def _record(i: int, title: str = "", body: str = "") -> CanonicalRecord:
    fields = {k: v for k, v in (("title", title), ("body", body)) if v}
    return CanonicalRecord(
        id=f"documents#{i}",
        namespace="documents",
        source_pk=str(i),
        dense=[float(i), 1.0, 0.0, 0.5],
        text_fields=fields,
    )


def test_enricher_encodes_rejects_empty_and_uses_cache() -> None:
    spec = _generated_object().sparse
    assert spec is not None
    encoder = FakeSparseEncoder()
    enricher = SparseEnricher(spec, encoder)
    records = [_record(1, "apple pie", "sweet apple"), _record(2), _record(3, "pear")]
    remember: dict[str, tuple[int, SparseVector]] = {}
    problems = enricher.enrich(records, remember=remember)
    assert problems == {"documents#2": NO_TEXT}
    assert records[0].sparse == FakeSparseEncoder.vector("apple pie\nsweet apple")
    assert records[2].sparse is not None and records[1].sparse is None
    assert all(r.text_fields == {} for r in records)
    assert set(remember) == {"documents#1", "documents#3"}
    assert enricher.stats.generated == 2 and enricher.stats.no_text == 1
    assert enricher.stats.requests == 1 and enricher.stats.tokens == 5
    again = [_record(1, "apple pie", "sweet apple"), _record(3, "changed")]
    calls = len(encoder.calls)
    enricher.enrich(again, remember)
    assert enricher.stats.cached == 1 and enricher.stats.generated == 3
    assert len(encoder.calls) == calls + 1 and encoder.calls[-1][0] == 1


def test_sparse_cache_round_trip(s3: S3Client) -> None:
    cache = SparseCache(s3, BUCKET, "gen/manifest/sparse_cache/documents")
    assert cache.load("gen/canonical/documents/part-00000.parquet") == {}
    entries = {"a": (7, SparseVector([3, 9], [0.5, 1.5])), "b": (8, SparseVector([1], [2.0]))}
    cache.store("gen/canonical/documents/part-00000.parquet", entries)
    assert (
        cache.key("x/part-00000.parquet")
        == "gen/manifest/sparse_cache/documents/part-00000.parquet"
    )
    loaded = cache.load("gen/canonical/documents/part-00000.parquet")
    assert loaded == entries


def test_transform_namespace_generates_caches_and_rejects(s3: S3Client) -> None:
    obj = _generated_object()
    assert obj.sparse is not None
    writer = CanonicalWriter(s3, BUCKET, "gen/canonical/documents", target_part_bytes=8 << 20)
    for i in range(1, 7):
        writer.write(_record(i, f"word{i} shared", "" if i == 4 else "body text"))
    keys = [p.key for p in writer.close()]
    encoder = FakeSparseEncoder()
    seen: list[tuple[str, int]] = []

    def run() -> Any:
        return transform_namespace(
            s3,
            BUCKET,
            namespace="documents",
            obj=obj,
            canonical_keys=keys,
            clean_prefix="gen/canonical/_clean/documents",
            rejects_prefix="gen/rejects/documents/transform",
            duplicates_prefix="gen/manifest/duplicates/documents",
            drop_duplicates=False,
            target_part_bytes=8 << 20,
            enricher=SparseEnricher(obj.sparse, encoder),
            cache=SparseCache(s3, BUCKET, "gen/manifest/sparse_cache/documents"),
            on_part=lambda key, rows, stats: seen.append((key, rows)),
        )

    first = run()
    assert first.clean_rows == 6 and first.rejected_rows == 0
    assert first.sparse is not None and first.sparse.generated == 6 and first.sparse.no_text == 0
    assert seen == [(keys[0], 6)]
    calls = len(encoder.calls)
    second = run()
    assert len(encoder.calls) == calls and second.sparse is not None and second.sparse.cached == 6
    # a record with no text at all is rejected with the documented reason
    writer = CanonicalWriter(s3, BUCKET, "gen/canonical/empty", target_part_bytes=8 << 20)
    writer.write(_record(9))
    empty_keys = [p.key for p in writer.close()]
    result = transform_namespace(
        s3,
        BUCKET,
        namespace="documents",
        obj=obj,
        canonical_keys=empty_keys,
        clean_prefix="gen/canonical/_clean/e",
        rejects_prefix="gen/rejects/e/transform",
        duplicates_prefix="gen/manifest/duplicates/e",
        drop_duplicates=False,
        target_part_bytes=8 << 20,
        enricher=SparseEnricher(obj.sparse, encoder),
    )
    assert result.rejected_rows == 1 and result.reject_reasons == {NO_TEXT: 1}
    reject = s3.get_object(Bucket=BUCKET, Key=result.reject_parts[0])["Body"].read().decode()
    assert NO_TEXT in reject and "missing sparse" not in reject


# --- encoder ------------------------------------------------------------------------------


class _StubInference:
    def __init__(self, vector_type: str = "sparse", max_batch: int = 2) -> None:
        self.vector_type = vector_type
        self.max_batch = max_batch
        self.calls: list[dict[str, Any]] = []

    def get_model(self, *, model: str) -> Any:
        return SimpleNamespace(
            vector_type=self.vector_type, max_batch_size=self.max_batch, max_sequence_length=512
        )

    def embed(self, *, model: str, inputs: list[str], parameters: dict[str, Any]) -> Any:
        self.calls.append({"model": model, "inputs": list(inputs), "parameters": parameters})
        data = [
            SimpleNamespace(sparse_indices=[len(t), 99], sparse_values=[1.0, 0.5]) for t in inputs
        ]
        return SimpleNamespace(
            vector_type=self.vector_type,
            data=data,
            usage=SimpleNamespace(total_tokens=sum(len(t.split()) for t in inputs)),
        )


def test_pinecone_encoder_batches_throttles_and_reports_usage() -> None:
    stub = _StubInference()
    clock = [0.0]
    sleeps: list[float] = []
    encoder = PineconeSparseEncoder(
        "key",
        client=SimpleNamespace(inference=stub),
        max_tokens=2048,
        tokens_per_minute=2,
        workers=1,
        clock=lambda: clock[0],
        sleep=lambda s: (sleeps.append(s), clock.__setitem__(0, clock[0] + s)),
    )
    assert encoder.limits().max_batch == 2 and encoder.limits().max_tokens == 512
    vectors = encoder.encode(["a b", "c d e", "f"], input_type="passage")
    assert [v.indices for v in vectors] == [[3, 99], [5, 99], [1, 99]]
    assert len(stub.calls) == 2 and stub.calls[0]["parameters"] == {
        "input_type": "passage",
        "truncate": "END",
        "max_tokens_per_sequence": 2048,
    }
    assert encoder.usage().requests == 2 and encoder.usage().tokens == 6
    assert sleeps, "the second batch had to wait for the next minute's token budget"
    stub = _StubInference(vector_type="dense")
    with pytest.raises(SparseEncodingError, match="dense"):
        PineconeSparseEncoder("key", client=SimpleNamespace(inference=stub)).encode(
            ["x"], input_type="query"
        )


def test_token_throttle_resets_each_minute() -> None:
    clock = [0.0]
    waits: list[float] = []
    throttle = TokenThrottle(100, clock=lambda: clock[0], sleep=lambda s: _advance(clock, waits, s))
    throttle.acquire(80)
    throttle.acquire(30)
    assert waits and clock[0] >= 60.0
    throttle.acquire(100)
    assert len(waits) == 2


def _advance(clock: list[float], waits: list[float], seconds: float) -> None:
    waits.append(seconds)
    clock[0] += seconds


# --- ground truth and hybrid check ------------------------------------------------------


def _write_clean(s3: S3Client, records: list[CanonicalRecord]) -> list[str]:
    writer = CanonicalWriter(s3, BUCKET, "gt/canonical/_clean/documents", target_part_bytes=8 << 20)
    for record in records:
        writer.write(record)
    return [p.key for p in writer.close()]


def test_canonical_ground_truth_matches_brute_force(s3: S3Client) -> None:
    source = SyntheticSource(rows=120, dimension=8, partitions=3, text_words=6)
    encoder = FakeSparseEncoder()
    records = [source.record(i) for i in range(120)]
    for record in records:
        record.sparse = encoder.vector(" ".join(record.text_fields.values()))
    keys = _write_clean(s3, records)
    truth = CanonicalGroundTruth(s3, BUCKET, {"documents": keys}, metric="cosine")
    ids = truth.sample_ids("documents", 5, seed=3)
    assert ids == truth.sample_ids("documents", 5, seed=3) and len(set(ids)) == 5
    fetched = truth.fetch_records("documents", ids)
    assert set(fetched) == set(ids) and all(r.sparse is not None for r in fetched.values())
    queries = [fetched[i] for i in ids]
    by_id = {r.id: r for r in records}

    def cosine(a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b, strict=True))
        return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))

    for query, dense_rank, sparse_rank in zip(
        queries,
        truth.knn_many("documents", [q.dense or [] for q in queries], 7),
        truth.knn_sparse_many("documents", [q.sparse for q in queries if q.sparse], 7),
        strict=True,
    ):
        assert query.dense is not None and query.sparse is not None
        expected_dense = sorted(
            by_id, key=lambda i: (-cosine(query.dense or [], by_id[i].dense or []), i)
        )[:7]
        assert dense_rank == expected_dense
        expected_sparse = sorted(
            by_id,
            key=lambda i: (-sparse_dot(query.sparse, by_id[i].sparse or SparseVector([], [])), i),  # type: ignore[arg-type]
        )[:7]
        assert sparse_rank == expected_sparse and sparse_rank[0] == query.id
        assert truth.knn("documents", query.dense, 7) == dense_rank
        assert truth.knn_sparse("documents", query.sparse, 7) == sparse_rank


def test_check_hybrid_flags_stored_sparse_that_differs_from_generated(s3: S3Client) -> None:
    source = SyntheticSource(rows=40, dimension=8, partitions=1, text_words=6)
    encoder = FakeSparseEncoder()
    records = [source.record(i) for i in range(40)]
    for record in records:
        record.sparse = encoder.vector(" ".join(record.text_fields.values()))
    keys = _write_clean(s3, records)
    truth = CanonicalGroundTruth(s3, BUCKET, {"documents": keys})
    gw = FakeGateway()
    host = "gt.fake.pinecone.io"
    gw.upsert(
        host,
        "documents",
        [VectorRecord(r.id, r.dense or [], r.metadata, sparse=r.sparse) for r in records],
    )
    good = check_hybrid(gw, host, truth, "documents", k=5, queries=6, threshold=0.9, model="fake")
    assert good.sparse_passed and good.fused_passed and good.to_dict()["model"] == "fake"
    tampered = records[0]
    gw.upsert(
        host,
        "documents",
        [
            VectorRecord(
                tampered.id,
                tampered.dense or [],
                tampered.metadata,
                sparse=SparseVector([1], [1.0]),
            )
        ],
    )
    bad = check_hybrid(gw, host, truth, "documents", k=5, queries=40, threshold=0.0, model="fake")
    assert bad.sparse_mismatches == [tampered.id] and not bad.sparse_passed


# --- applier and reconcile ---------------------------------------------------------------


def test_applier_rereads_and_generates_sparse_for_every_envelope() -> None:
    source = SyntheticSource(rows=10, dimension=4, partitions=1, text_words=5)
    obj = _generated_object(dense={"column": "embedding", "dimension": 4, "metric": "cosine"})
    assert obj.sparse is not None
    encoder = FakeSparseEncoder()
    gw = FakeGateway()
    host = "ap.fake.pinecone.io"
    buffer = InMemoryBuffer()
    buffer.send(
        [
            ChangeEnvelope(
                op="upsert", id="documents#1", namespace="documents", source_change_id="c1"
            ),
            ChangeEnvelope(
                op="upsert",
                id="documents#2",
                namespace="documents",
                source_change_id="c2",
                values=[1.0, 0.0, 0.0, 0.0],
                metadata={"category": "news"},
            ),
        ]
    )
    applier = Applier(
        gw,
        host,
        buffer,
        source=source,
        enrichers={"documents": SparseEnricher(obj.sparse, encoder)},
    )
    result = applier.drain_once()
    assert result.upserts == 2 and not result.unencodable
    stored = gw.fetch(host, "documents", ["documents#1", "documents#2"])
    expected = encoder.vector(" ".join(source.record(2).text_fields.values()))
    assert stored["documents#2"].sparse == expected and stored["documents#1"].sparse is not None
    # the dual-write payload was replaced by the re-read row, so the sparse vector is present
    assert stored["documents#2"].values == source.record(2).dense


def test_reconcile_generates_sparse_for_missing_records() -> None:
    source = SyntheticSource(rows=6, dimension=4, partitions=1, text_words=5, empty_text_every=5)
    obj = _generated_object()
    assert obj.sparse is not None
    gw = FakeGateway()
    host = "rc.fake.pinecone.io"
    result = reconcile(
        gw, host, source, "documents", enricher=SparseEnricher(obj.sparse, FakeSparseEncoder())
    )
    assert result.upserted == 4 and result.unresolvable == ["documents#0", "documents#5"]
    assert all(r.sparse is not None for r in gw.fetch(host, "documents", ["documents#1"]).values())


# --- introspect, findings, report -------------------------------------------------------


def test_sparse_from_generates_a_hybrid_mapping_and_findings() -> None:
    options = replace(OPTIONS, roadmap="hybrid", sparse_from=("title", "body", "nope"))
    mapping = generate_mapping(inventory(documents_table()), options)
    (obj,) = mapping.objects
    assert obj.sparse is not None and obj.sparse.generated
    assert obj.sparse.text_fields == ["title", "body"] and mapping.target.type == "document"
    assert mapping.validation.hybrid is not None
    assert hybrid_readiness(documents_table(), obj) == "generated"
    bundle = build_report(inventory(documents_table()), options)
    checks = {f.check: f for f in bundle.findings}
    assert "sparse_generation_cost" in checks and "sparse_query_encoding" in checks
    assert "input_type: query" in checks["sparse_query_encoding"].message
    assert "$" in checks["sparse_generation_cost"].message
    assert "hybrid (generated)" in bundle.report_md and "ADR-017" in bundle.report_md
    assert "hybrid (generated)" in bundle.report_html
    assert "kind: generated" in bundle.report_md


def test_sparse_from_non_text_field_blocks_and_long_text_warns() -> None:
    options = replace(OPTIONS, roadmap="hybrid", sparse_from=("price", "body"))
    bundle = build_report(inventory(documents_table()), options)
    blockers = [f for f in bundle.findings if f.check == "sparse_text_field"]
    assert blockers and blockers[0].field == "price" and "numeric" in blockers[0].message
    wide = documents_table()
    from tests.unit.report.fixtures import column

    wide = wide.model_copy(
        update={"columns": [*wide.columns, column("essay", "text", avg_width=5000)]}
    )
    bundle = build_report(inventory(wide), replace(OPTIONS, sparse_from=("essay",)))
    assert any(f.check == "sparse_truncation" for f in bundle.findings)
    assert bundle.mapping.objects[0].sparse is not None


# --- end to end with fakes ----------------------------------------------------------------


def test_generated_sparse_pipeline_end_to_end_with_fakes(s3: S3Client, tmp_path: Path) -> None:
    obj = _generated_object(
        source="synthetic.documents",
        dense={"column": "embedding", "dimension": 16, "metric": "cosine"},
    )
    mapping = _mapping(
        obj,
        hybrid={"k": 10, "queries": 20, "threshold": 0.9},
        fidelity_sample=50,
        recall_at_k={"k": 10, "queries": 20},
    )
    source = SyntheticSource(rows=300, dimension=16, partitions=2, text_words=10)
    gw = FakeGateway(import_records=300)
    encoder = FakeSparseEncoder()
    cfg = MigrationConfig(
        migration_id="gen",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
    )
    store = ManifestStore(s3, BUCKET, "gen")
    log = io.StringIO()

    def context(manifest: Manifest) -> RunContext:
        ctx = RunContext(
            config=cfg,
            mapping=mapping,
            manifest=manifest,
            store=store,
            events=EventBus("gen", [JsonLinesSink(log)]),
            s3=s3,
            source=source,
            target=gw,
        )
        ctx.extras["sparse_encoder"] = encoder
        approve_ctx(ctx, tmp_path)
        return ctx

    ctx = context(Manifest(migration_id="gen"))
    Pipeline(
        [
            PrepareIndexStage(),
            ExportStage(use_processes=False),
            TransformStage(),
            EmitStage(ImportLimits()),
            ImportStage(poll_interval_s=0),
        ]
    ).run(ctx, S3Lock(s3, BUCKET, "gen"))
    transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
    assert transform["sparse_by_namespace"]["documents"]["generated"] == 300
    assert transform["rejected_by_namespace"]["documents"] == 0
    assert '"type":"sparse_encoded"' in log.getvalue()
    assert ctx.manifest.stage(Stage.EMIT).outputs["format"] == "jsonl.gz"
    cache_keys = s3.list_objects_v2(Bucket=BUCKET, Prefix="gen/manifest/sparse_cache/")
    assert cache_keys["KeyCount"] >= 1
    from pcmigrate.core.canonical import read_canonical

    host = "gen.fake.pinecone.io"
    records = list(read_canonical(s3, BUCKET, transform["parts_by_namespace"]["documents"]))
    assert all(r.sparse is not None and r.text_fields == {} for r in records)
    gw.upsert(
        host,
        "documents",
        [VectorRecord(r.id, r.dense or [], r.metadata, sparse=r.sparse) for r in records],
    )
    ctx2 = context(store.load() or ctx.manifest)
    Pipeline([ValidateStage(settle_window_s=5, poll_interval_s=0, filter_queries=3)]).run(
        ctx2, S3Lock(s3, BUCKET, "gen")
    )
    outputs = ctx2.manifest.stage(Stage.VALIDATE).outputs
    checks = outputs["checks"]
    assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
    assert all(checks.values()), checks
    assert outputs["sparse_models"] == {"documents": "pinecone-sparse-english-v0"}
    assert "input_type: query" in outputs["query_encoding"]
