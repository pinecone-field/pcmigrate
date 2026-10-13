"""Hybrid wave: sparse parsing and vocabulary, document emitter and planner, document indexes,
hybrid validation, mapping generation, and the end-to-end synthetic pipeline with fakes."""

import gzip
import io
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.config.mapping import (
    DenseSpec,
    HybridCheck,
    MetadataField,
    MetadataType,
    Metric,
    ObjectMapping,
    SparseKind,
    SparseSpec,
    TargetSpec,
    ValidationSpec,
)
from pcmigrate.core.canonical import CanonicalRecord, SparseVector
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.sparse import (
    format_sparsevec,
    merge_vocabularies,
    overlap_at_k,
    parse_sparsevec,
    rrf_fuse,
    sparse_dot,
    token_index,
    tokens_to_sparse,
)
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.core.transform import RecordValidator, json_to_record, record_to_json
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping, hybrid_readiness
from pcmigrate.sources.inventory import VectorField, VectorKind
from pcmigrate.sources.opensearch.glue_mapping import DocumentTranslator
from pcmigrate.sources.pgvector.records import RecordBuilder
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.emitters import (
    DocumentFields,
    DocumentIndexEmitter,
    ImportLimits,
    NamespaceInput,
    plan_import_batches,
)
from pcmigrate.targets.emitters.document_jsonl import document_line
from pcmigrate.targets.index_manager import IndexManager, IndexSpecMismatchError
from pcmigrate.targets.pinecone_client import DocumentSchema, IndexInfo, document_schema_from
from pcmigrate.validate.hybrid import check_hybrid
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.fakes import FakeGateway
from tests.unit.report.fixtures import documents_table, inventory, text_search_documents

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def test_sparsevec_round_trip_and_hashing() -> None:
    sparse, dimension = parse_sparsevec("{1:0.5,7:0.25,3:1}/1024")
    assert dimension == 1024 and sparse.indices == [0, 2, 6] and sparse.values == [0.5, 1.0, 0.25]
    assert format_sparsevec(sparse, 1024) == "{1:0.5,3:1,7:0.25}/1024"
    empty, _ = parse_sparsevec("{}/8")
    assert empty.indices == []
    with pytest.raises(ValueError):
        parse_sparsevec("[1,2,3]")
    vec, vocab = tokens_to_sparse({"apple": 1.5, "pear": 0.5, "zero": 0.0})
    assert set(vocab) == {"apple", "pear", "zero"} and vocab["apple"] == token_index("apple")
    assert vec.indices == sorted(vec.indices) and len(vec.indices) == 2
    assert all(0 <= i <= 2**32 - 1 for i in vec.indices)
    assert sparse_dot(vec, vec) == pytest.approx(1.5**2 + 0.5**2)
    merged, collisions = merge_vocabularies([{"a": 1, "b": 2}, {"c": 2, "a": 1}])
    assert merged == {"a": 1, "b": 2, "c": 2} and collisions == ["b / c -> 2"]
    assert rrf_fuse([["x", "y", "z"], ["y", "x"]], k=60, top=2) == ["x", "y"]
    assert overlap_at_k(["a", "b", "c"], ["c", "a", "d"], 3) == pytest.approx(2 / 3)


def test_mapping_requires_document_index_for_sparse() -> None:
    obj = ObjectMapping(
        source="t",
        namespace="t",
        id_template="t#{id}",
        dense=DenseSpec(column="embedding", dimension=4, metric=Metric.COSINE),
        sparse=SparseSpec(column="sparse", kind=SparseKind.SPARSEVEC),
    )
    with pytest.raises(ValueError, match=r"target\.type: document"):
        Mapping(
            migration_id="m",
            source={"kind": "pgvector"},  # type: ignore[arg-type]
            target=TargetSpec(index="i", region="us-east-1"),
            objects=[obj],
        )
    mapping = Mapping(
        migration_id="m",
        source={"kind": "pgvector"},  # type: ignore[arg-type]
        target=TargetSpec(index="i", region="us-east-1", type="document"),
        objects=[obj],
        validation=ValidationSpec(hybrid=HybridCheck()),
    )
    assert mapping.hybrid and Mapping.from_yaml(mapping.to_yaml()) == mapping
    with pytest.raises(ValueError, match="xxh32"):
        SparseSpec(column="s", kind=SparseKind.RANK_FEATURES)


def test_record_builder_parses_sparsevec_and_rank_features() -> None:
    obj = ObjectMapping(
        source="public.documents",
        namespace="documents",
        id_template="documents#{id}",
        dense=DenseSpec(column="embedding", dimension=2, metric=Metric.COSINE),
        sparse=SparseSpec(column="sparse", kind=SparseKind.SPARSEVEC),
        metadata=[MetadataField(source="title", type=MetadataType.STRING)],
    )
    builder = RecordBuilder(obj, ("id",))
    assert builder.source_columns == ["id", "title", "embedding", "sparse"]
    record = builder.build(
        {"id": 1, "title": "t", "embedding": [0.1, 0.2], "sparse": "{2:0.5,9:1.5}/100"}
    )
    assert record.sparse == SparseVector([1, 8], [0.5, 1.5])
    assert builder.vocabulary == {}
    features = obj.model_copy(
        update={
            "id_template": "{_id}",
            "sparse": SparseSpec(
                column="sparse", kind=SparseKind.RANK_FEATURES, vocabulary="xxh32"
            ),
        }
    )
    translator = DocumentTranslator(features)
    doc = translator.translate(
        "d1", {"title": "t", "embedding": [0.1, 0.2], "sparse": {"apple": 2.0, "pear": 1.0}}
    )
    assert doc.sparse is not None and len(doc.sparse.indices) == 2
    assert translator.builder.vocabulary == {
        "apple": token_index("apple"),
        "pear": token_index("pear"),
    }
    line = record_to_json(doc)
    assert json_to_record(line).sparse == doc.sparse


def test_transform_validates_sparse_vectors() -> None:
    obj = ObjectMapping(
        source="t",
        namespace="t",
        id_template="t#{id}",
        dense=DenseSpec(column="embedding", dimension=2, metric=Metric.COSINE),
        sparse=SparseSpec(column="sparse", kind=SparseKind.SPARSEVEC),
    )
    validator = RecordValidator(obj)
    ok = CanonicalRecord(
        id="a",
        namespace="t",
        source_pk="a",
        dense=[0.1, 0.2],
        sparse=SparseVector([1, 5], [0.5, 1.0]),
    )
    assert validator.violations(ok) == []
    missing = CanonicalRecord(id="b", namespace="t", source_pk="b", dense=[0.1, 0.2])
    assert "missing sparse vector" in validator.violations(missing)
    bad = CanonicalRecord(
        id="c",
        namespace="t",
        source_pk="c",
        dense=[0.1, 0.2],
        sparse=SparseVector([3, 3], [float("nan"), 1.0]),
    )
    problems = validator.violations(bad)
    assert (
        "sparse vector repeats an index" in problems
        and "sparse vector contains NaN or infinity" in problems
    )
    huge = CanonicalRecord(
        id="d", namespace="t", source_pk="d", dense=[0.1, 0.2], sparse=SparseVector([2**32], [1.0])
    )
    assert "sparse index outside the uint32 range" in validator.violations(huge)


def test_document_line_and_emitter(s3: S3Client) -> None:
    record = CanonicalRecord(
        id="doc-1",
        namespace="docs",
        source_pk="1",
        dense=[0.5, 0.25],
        sparse=SparseVector([3, 9], [1.0, 0.5]),
        metadata={"title": "T", "tags": ["a"]},
    )
    line = document_line(record, DocumentFields(dense="embedding", sparse="keywords"))
    assert line == {
        "_id": "doc-1",
        "embedding": [0.5, 0.25],
        "keywords": {"indices": [3, 9], "values": [1.0, 0.5]},
        "title": "T",
        "tags": ["a"],
    }
    from pcmigrate.core.canonical import CanonicalWriter

    writer = CanonicalWriter(s3, BUCKET, "m/canonical/docs", target_part_bytes=8 * 1024 * 1024)
    for n in range(50):
        writer.write(
            CanonicalRecord(
                id=f"doc-{n}",
                namespace="docs",
                source_pk=str(n),
                dense=[0.1 * n, 1.0],
                sparse=SparseVector([n, n + 100], [1.0, 2.0]),
                metadata={"n": n},
            )
        )
    keys = [p.key for p in writer.close()]
    emitter = DocumentIndexEmitter(
        s3, BUCKET, "m/import", fields=DocumentFields(), limits=ImportLimits()
    )
    inputs = [NamespaceInput(namespace="docs", keys=tuple(keys), bytes=1000)]
    batches = plan_import_batches(inputs, ImportLimits(), file_format="jsonl.gz")
    result = emitter.emit(batches[0], inputs)
    assert result.rows == 50 and len(result.parts) == 1
    assert result.parts[0].key == "m/import/batch-00000/docs/part-00000.jsonl.gz"
    body = s3.get_object(Bucket=BUCKET, Key=result.parts[0].key)["Body"].read()
    lines = gzip.decompress(body).decode().splitlines()
    assert len(lines) == 50
    first = json.loads(lines[0])
    assert (
        first["_id"] == "doc-0"
        and first["sparse"] == {"indices": [0, 100], "values": [1.0, 2.0]}
        and first["n"] == 0
    )


def test_gzip_counts_ten_times_in_import_planning() -> None:
    limits = ImportLimits(target_part_bytes=512 * 1024 * 1024, size_safety_factor=1.0)
    inputs = [NamespaceInput(namespace=f"ns{i}", keys=(), bytes=60 * 1024**3) for i in range(4)]
    parquet = plan_import_batches(inputs, limits, file_format="parquet")
    assert len(parquet) == 1
    gz = plan_import_batches(inputs, limits, file_format="jsonl.gz")
    assert len(gz) == 4, "600 GB effective per namespace -> one namespace per 1 TB import"
    assert all(b.estimated_bytes <= limits.max_bytes_on_demand for b in gz)
    dedicated = plan_import_batches(inputs, limits, capacity="dedicated", file_format="jsonl.gz")
    assert len(dedicated) == 1


def test_document_schema_parsing_and_index_manager() -> None:
    plain = SimpleNamespace(
        fields={"values": SimpleNamespace(type="dense_vector", dimension=4, metric="cosine")}
    )
    assert document_schema_from(plain) is None
    hybrid = SimpleNamespace(
        fields={
            "dense": SimpleNamespace(
                type="dense_vector", dimension=4, metric="cosine", full_text_search=None
            ),
            "sparse": SimpleNamespace(type="sparse_vector", dimension=None, full_text_search=None),
            "body": SimpleNamespace(
                type="string", dimension=None, full_text_search=SimpleNamespace(language="en")
            ),
        }
    )
    schema = document_schema_from(hybrid)
    assert schema == DocumentSchema(
        "dense", 4, "cosine", "sparse", ("body",), {"body": {"language": "en"}}
    )

    gw = FakeGateway()
    target = TargetSpec(index="acme-docs", region="us-east-1", type="document")
    dense = DenseSpec(column="embedding", dimension=4, metric=Metric.COSINE)
    info = IndexManager(gw).prepare(target, dense, ["docs"], sparse=True)
    assert info.document and info.schema == DocumentSchema("dense", 4, "cosine", "sparse", ())
    assert gw.create_calls[0]["fields"] == {
        "dense": {"type": "dense_vector", "dimension": 4, "metric": "cosine"},
        "sparse": {"type": "sparse_vector"},
    }
    adopt = target.model_copy(update={"mode": "adopt", "sparse_field": "keywords"})
    with pytest.raises(IndexSpecMismatchError, match="sparse field 'sparse' != 'keywords'"):
        IndexManager(gw).prepare(adopt, dense, ["other"], sparse=True)
    vector_target = TargetSpec(index="acme-docs", region="us-east-1", mode="adopt")
    with pytest.raises(
        IndexSpecMismatchError, match=r"document schema, mapping says target\.type vector"
    ):
        IndexManager(gw).prepare(vector_target, dense, ["other"])

    class SparseVectorField:
        description = None

    class DenseVectorField:
        dimension = 4
        metric = "cosine"

    sdk_like = SimpleNamespace(fields={"dense": DenseVectorField(), "sparse": SparseVectorField()})
    assert document_schema_from(sdk_like) == DocumentSchema("dense", 4, "cosine", "sparse", ())
    gw.indexes["plain"] = IndexInfo("plain", "p", 4, "cosine", "aws", "us-east-1", True)
    with pytest.raises(
        IndexSpecMismatchError, match=r"vector index, mapping says target\.type document"
    ):
        IndexManager(gw).prepare(
            target.model_copy(update={"index": "plain", "mode": "adopt"}), dense, ["x"]
        )


def test_generated_mapping_and_report_for_hybrid_sources() -> None:
    sparse_table = documents_table(
        vectors=[
            *documents_table().vectors,
            VectorField(
                column="sparse",
                kind=VectorKind.SPARSEVEC,
                dimension=30000,
                nullable=True,
                sampled_rows=100,
            ),
        ]
    )
    dense_only = generate_mapping(inventory(sparse_table), OPTIONS)
    assert dense_only.objects[0].sparse is None and dense_only.target.type == "vector"
    assert any(f.source == "sparse" and f.action == "drop" for f in dense_only.objects[0].metadata)
    hybrid = generate_mapping(inventory(sparse_table), replace(OPTIONS, roadmap="hybrid"))
    obj = hybrid.objects[0]
    assert obj.sparse == SparseSpec(column="sparse", kind=SparseKind.SPARSEVEC, dimension=30000)
    assert hybrid.target.type == "document" and hybrid.hybrid
    assert hybrid.validation.hybrid == HybridCheck()
    assert not any(f.source == "sparse" for f in obj.metadata)
    assert hybrid_readiness(sparse_table) == "hybrid"
    assert hybrid_readiness(text_search_documents()) == "fts"
    assert hybrid_readiness(documents_table()) == "dense"

    bundle = build_report(inventory(sparse_table), replace(OPTIONS, roadmap="hybrid"))
    checks = {(f.check, f.severity.value) for f in bundle.findings}
    assert ("hybrid_readiness", "info") in checks
    assert "hybrid-ready" in bundle.report_md and "ADR-016" in bundle.report_md
    fts = build_report(inventory(text_search_documents()), replace(OPTIONS, roadmap="hybrid"))
    warning = next(f for f in fts.findings if f.check == "hybrid_readiness")
    assert warning.severity.value == "warning" and "wave 3" in warning.message
    assert "FTS wave" in fts.report_md

    features_table = documents_table(
        schema_name="",
        name="docs-index",
        relkind="i",
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.KNN_VECTOR,
                dimension=768,
                nullable=True,
                sampled_rows=10,
                in_source=True,
                metric=documents_table().vectors[0].metric,
            ),
            VectorField(
                column="sparse", kind=VectorKind.RANK_FEATURES, dimension=None, nullable=True
            ),
        ],
    )
    os_inventory = inventory(features_table).model_copy(update={"kind": "opensearch"})
    os_bundle = build_report(os_inventory, replace(OPTIONS, roadmap="hybrid"))
    assert os_bundle.mapping.objects[0].sparse == SparseSpec(
        column="sparse", kind=SparseKind.RANK_FEATURES, vocabulary="xxh32"
    )
    vocab = next(f for f in os_bundle.findings if f.check == "sparse_vocabulary")
    assert vocab.severity.value == "warning" and "manifest/vocabulary/" in vocab.message


def _hybrid_mapping() -> Mapping:
    return Mapping.model_validate(
        {
            "migration_id": "hyb",
            "source": {"kind": "synthetic"},
            "target": {"index": "hyb", "region": REGION, "type": "document"},
            "objects": [
                {
                    "source": "synthetic.documents",
                    "namespace": "documents",
                    "id_template": "documents#{id}",
                    "dense": {"column": "embedding", "dimension": 16, "metric": "cosine"},
                    "sparse": {"column": "sparse", "kind": "sparsevec"},
                }
            ],
            "validation": {
                "hybrid": {"k": 10, "queries": 20, "threshold": 0.9},
                "fidelity_sample": 50,
                "recall_at_k": {"k": 10, "queries": 20},
            },
        }
    )


def test_hybrid_synthetic_pipeline_end_to_end_with_fakes(s3: S3Client, tmp_path: Path) -> None:
    mapping = _hybrid_mapping()
    source = SyntheticSource(
        rows=300, dimension=16, partitions=2, sparse_terms=5, sparse_vocabulary=200
    )
    gw = FakeGateway(import_records=300)
    cfg = MigrationConfig(
        migration_id="hyb",
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
    )
    store = ManifestStore(s3, BUCKET, "hyb")
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=Manifest(migration_id="hyb"),
        store=store,
        events=EventBus("hyb", [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx, tmp_path)
    Pipeline(
        [
            PrepareIndexStage(),
            ExportStage(use_processes=False),
            TransformStage(),
            EmitStage(ImportLimits()),
            ImportStage(poll_interval_s=0),
        ]
    ).run(ctx, S3Lock(s3, BUCKET, "hyb"))
    prepared = ctx.manifest.stage(Stage.PREPARE_INDEX).outputs
    assert prepared["type"] == "document" and prepared["sparse_field"] == "sparse"
    emit = ctx.manifest.stage(Stage.EMIT).outputs
    assert emit["format"] == "jsonl.gz" and emit["batches"][0]["uri"].endswith(
        "/import/batch-00000/"
    )
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="hyb/import/")["Contents"]]
    assert all(k.endswith(".jsonl.gz") for k in keys)
    host = "hyb.fake.pinecone.io"
    from pcmigrate.core.canonical import read_canonical
    from pcmigrate.targets.pinecone_client import VectorRecord

    records = list(
        read_canonical(
            s3,
            BUCKET,
            ctx.manifest.stage(Stage.TRANSFORM).outputs["parts_by_namespace"]["documents"],
        )
    )
    gw.upsert(
        host,
        "documents",
        [VectorRecord(r.id, r.dense or [], r.metadata, sparse=r.sparse) for r in records],
    )
    fetched = gw.fetch(host, "documents", [records[0].id])[records[0].id]
    assert fetched.sparse == records[0].sparse
    ctx2 = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=store.load() or ctx.manifest,
        store=store,
        events=EventBus("hyb", [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    approve_ctx(ctx2, tmp_path)
    Pipeline([ValidateStage(settle_window_s=5, poll_interval_s=0, filter_queries=3)]).run(
        ctx2, S3Lock(s3, BUCKET, "hyb")
    )
    checks = ctx2.manifest.stage(Stage.VALIDATE).outputs["checks"]
    assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
    assert all(checks.values()), checks
    result = check_hybrid(gw, host, source, "documents", k=10, queries=5, threshold=0.9)
    assert result.sparse_mean == 1.0 and result.fused_mean == 1.0 and result.skipped == 0
    report = s3.get_object(Bucket=BUCKET, Key="hyb/manifest/validation.md")["Body"].read().decode()
    assert "hybrid_overlap | documents | pass" in report


def test_sdk_gateway_fetch_reads_id_keyed_documents() -> None:
    """The 2026-07 fetch response keys documents by id; the gateway must not iterate keys."""
    from pcmigrate.targets.pinecone_client import SdkGateway

    class Doc:
        def __init__(self, data: dict[str, object]) -> None:
            self._data = data

        def to_dict(self) -> dict[str, object]:
            return dict(self._data)

    documents = {
        "a": Doc({"_id": "a", "dense": [0.1, 0.2], "sparse": {"indices": [3], "values": [1.5]}}),
        "b": Doc({"_id": "b", "dense": [0.3, 0.4], "title": "t"}),
    }
    index = SimpleNamespace(
        documents=SimpleNamespace(
            fetch=lambda **_: SimpleNamespace(documents=documents, namespace="ns")
        )
    )
    gateway = SdkGateway.__new__(SdkGateway)
    gateway._indexes = {"host": index}
    gateway._schemas = {"host": DocumentSchema("dense", 2, "cosine", "sparse", ())}
    fetched = gateway.fetch("host", "ns", ["a", "b"])
    assert fetched["a"].sparse == SparseVector([3], [1.5]) and fetched["a"].values == [0.1, 0.2]
    assert fetched["b"].sparse is None and fetched["b"].metadata == {"title": "t"}
