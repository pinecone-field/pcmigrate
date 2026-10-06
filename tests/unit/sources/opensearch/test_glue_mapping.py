"""Mapping translation for every OpenSearch field type in the golden fixture, plus the extras."""

import json

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config.mapping import (
    DenseSpec,
    Mapping,
    MetadataField,
    MetadataType,
    Metric,
    ObjectMapping,
    SourceKind,
    SourceSpec,
    TargetSpec,
)
from pcmigrate.core.canonical import read_canonical
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.glue_mapping import (
    DocumentError,
    DocumentTranslator,
    PartitionResult,
    merge_results,
    transform_lines,
)
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from pcmigrate.sources.pgvector.records import RecordBuildError, to_epoch_seconds
from tests.fixtures.golden.opensearch.documents import golden_documents
from tests.unit.conftest import BUCKET
from tests.unit.sources.opensearch.fakes import FakeTransport, make_client

OPTIONS = MappingOptions(migration_id="golden-os", index="golden-os", region="us-east-1")


@pytest.fixture(scope="module")
def golden_mapping() -> Mapping:
    introspector = OpenSearchIntrospector(
        make_client(FakeTransport.golden()), IntrospectOptions(workload_window_s=0, sample_docs=300)
    )
    return build_report(introspector.inventory(), OPTIONS).mapping


def _translator(golden_mapping: Mapping) -> DocumentTranslator:
    return DocumentTranslator.for_index(golden_mapping, "golden-documents")


def test_generated_mapping_covers_every_fixture_field(golden_mapping: Mapping) -> None:
    obj = golden_mapping.objects[0]
    assert golden_mapping.source.kind is SourceKind.OPENSEARCH
    assert obj.source == "golden-documents" and obj.namespace == "golden-documents"
    assert obj.id_template == "golden-documents#{_id}"
    assert obj.dense == DenseSpec(column="embedding", dimension=768, metric=Metric.COSINE)
    fields = {f.source: f for f in obj.metadata}
    assert fields["doc_id"].type is MetadataType.STRING
    assert (
        fields["title"].type is MetadataType.STRING and fields["body"].type is MetadataType.STRING
    )
    assert fields["category"].type is MetadataType.STRING
    assert fields["published_at"].type is MetadataType.EPOCH_SECONDS
    assert fields["views"].type is MetadataType.INTEGER
    assert (
        fields["author"].type is MetadataType.STRING and fields["author"].transform == "stringify"
    )
    assert all(f.action == "map" for f in obj.metadata)
    assert golden_mapping.change_capture is not None
    assert golden_mapping.change_capture.tier == "dual_write"
    assert golden_mapping.change_capture.rescan is False


def test_translate_a_golden_document(golden_mapping: Mapping) -> None:
    translator = _translator(golden_mapping)
    doc = next(d for d in golden_documents() if d["doc_id"] == "doc-00003")
    record = translator.translate("doc-00003", doc)
    assert record.id == "golden-documents#doc-00003"
    assert record.namespace == "golden-documents" and record.source_pk == "doc-00003"
    assert record.dense == pytest.approx(doc["embedding"]) and len(record.dense or []) == 768
    assert record.vector_hash is not None
    assert record.metadata["title"] == "Document 3" and record.metadata["views"] == 9
    assert record.metadata["category"] == "manual"
    assert record.metadata["published_at"] == to_epoch_seconds(doc["published_at"])
    assert isinstance(record.metadata["published_at"], int)
    assert json.loads(record.metadata["author"]) == doc["author"]
    assert record.metadata["doc_id"] == "doc-00003"


def test_missing_optional_fields_are_omitted(golden_mapping: Mapping) -> None:
    translator = _translator(golden_mapping)
    no_category = next(d for d in golden_documents() if "category" not in d)
    no_date = next(d for d in golden_documents() if "published_at" not in d)
    assert "category" not in translator.translate("x", no_category).metadata
    assert "published_at" not in translator.translate("y", no_date).metadata


def test_rejects_missing_or_wrong_vector(golden_mapping: Mapping) -> None:
    translator = _translator(golden_mapping)
    doc = dict(golden_documents()[0])
    del doc["embedding"]
    with pytest.raises(RecordBuildError, match="embedding is NULL"):
        translator.translate("a", doc)
    doc["embedding"] = "not a vector"
    with pytest.raises(RecordBuildError, match="numeric array"):
        translator.translate("a", doc)
    doc["embedding"] = [0.1] * 3
    short = translator.translate("a", doc)
    assert len(short.dense or []) == 3, "dimension is enforced by the shared transform stage"


def test_translate_line_requires_the_injected_id(golden_mapping: Mapping) -> None:
    translator = _translator(golden_mapping)
    doc = {**golden_documents()[0], "_id": "doc-00001", "_index": "golden-documents"}
    record = translator.translate_line(json.dumps(doc))
    assert record.id == "golden-documents#doc-00001"
    assert "_index" not in record.metadata and "_id" not in record.metadata
    with pytest.raises(DocumentError, match="no _id"):
        translator.translate_line(json.dumps(golden_documents()[0]))
    with pytest.raises(DocumentError, match="not JSON"):
        translator.translate_line("{nope")
    with pytest.raises(DocumentError, match="not a JSON object"):
        translator.translate_line("[1, 2]")


def _object(*fields: MetadataField, dense: str = "embedding", dimension: int = 4) -> ObjectMapping:
    return ObjectMapping(
        source="idx",
        namespace="idx",
        id_template="idx#{_id}",
        dense=DenseSpec(column=dense, dimension=dimension, metric=Metric.COSINE),
        metadata=list(fields),
    )


def test_every_other_opensearch_type_rule() -> None:
    obj = _object(
        MetadataField(source="flag", type=MetadataType.BOOLEAN),
        MetadataField(source="score", type=MetadataType.FLOAT),
        MetadataField(source="count", type=MetadataType.INTEGER),
        MetadataField(source="tags", type=MetadataType.STRING_LIST),
        MetadataField(source="numbers", type=MetadataType.STRING, transform="stringify"),
        MetadataField(
            source="meta.lang", target="meta.lang", type=MetadataType.STRING, transform="flatten"
        ),
        MetadataField(
            source="meta.rank", target="meta.rank", type=MetadataType.INTEGER, transform="flatten"
        ),
        MetadataField(source="deep", type=MetadataType.STRING, transform="stringify"),
        MetadataField(source="created_millis", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="created_seconds", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="created_text", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="created_date_only", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="body", type=MetadataType.STRING, max_chars=10),
        MetadataField(source="_hidden", target="f_hidden", type=MetadataType.STRING),
        MetadataField(source="ip", type=MetadataType.STRING),
        MetadataField(source="location", action="drop", reason="geo_point has no scalar form"),
        MetadataField(source="blob", action="drop", reason="binary has no scalar form"),
    )
    translator = DocumentTranslator(obj)
    record = translator.translate(
        "k1",
        {
            "embedding": [1, 2, 3, 4],
            "flag": True,
            "score": 2.5,
            "count": 7,
            "tags": ["a", "b"],
            "numbers": [1, 2, 3],
            "meta": {"lang": "en", "rank": 3, "ignored": "x"},
            "deep": {"a": {"b": {"c": 1}}},
            "created_millis": 1704067200000,
            "created_seconds": 1704067200,
            "created_text": "2024-01-01T00:00:00Z",
            "created_date_only": "2024-01-01",
            "body": "abcdefghijklmnopqrstuvwxyz",
            "_hidden": "renamed",
            "ip": "10.0.0.1",
            "location": {"lat": 1.0, "lon": 2.0},
            "blob": "AAAA",
        },
    )
    assert record.metadata == {
        "flag": True,
        "score": 2.5,
        "count": 7,
        "tags": ["a", "b"],
        "numbers": "[1, 2, 3]",
        "meta.lang": "en",
        "meta.rank": 3,
        "deep": '{"a": {"b": {"c": 1}}}',
        "created_millis": 1704067200,
        "created_seconds": 1704067200,
        "created_text": 1704067200,
        "created_date_only": 1704067200,
        "body": "abcdefghij",
        "f_hidden": "renamed",
        "ip": "10.0.0.1",
    }
    assert record.dense == [1.0, 2.0, 3.0, 4.0]


def test_type_mismatches_are_build_errors() -> None:
    obj = _object(
        MetadataField(source="count", type=MetadataType.INTEGER),
        MetadataField(source="flag", type=MetadataType.BOOLEAN),
        MetadataField(source="when", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="tags", type=MetadataType.STRING_LIST),
    )
    translator = DocumentTranslator(obj)
    base = {"embedding": [1, 2, 3, 4]}
    with pytest.raises(RecordBuildError):
        translator.translate("a", {**base, "count": "seven"})
    with pytest.raises(RecordBuildError):
        translator.translate("a", {**base, "flag": "yes"})
    with pytest.raises(RecordBuildError):
        translator.translate("a", {**base, "when": "last tuesday"})
    with pytest.raises(RecordBuildError):
        translator.translate("a", {**base, "tags": "single"})


def test_dotted_vector_path_and_namespace_by() -> None:
    obj = ObjectMapping(
        source="idx",
        namespace="idx",
        namespace_by="tenant",
        id_template="idx#{_id}",
        dense=DenseSpec(column="vectors.dense", dimension=2, metric=Metric.EUCLIDEAN),
        metadata=[MetadataField(source="tenant", type=MetadataType.STRING)],
    )
    record = DocumentTranslator(obj).translate(
        "z", {"vectors": {"dense": [0.5, 0.25]}, "tenant": "acme"}
    )
    assert record.namespace == "idx:acme" and record.dense == [0.5, 0.25]


def test_transform_lines_writes_canonical_parts_and_rejects(
    s3: S3Client, golden_mapping: Mapping
) -> None:
    translator = _translator(golden_mapping)
    docs = golden_documents()[:20]
    lines = [json.dumps({**d, "_id": d["doc_id"], "_index": "golden-documents"}) for d in docs]
    broken = dict(docs[0])
    broken["_id"] = "broken-1"
    del broken["embedding"]
    lines.append(json.dumps(broken))
    lines.append("not json")
    lines.append("")
    result = transform_lines(
        lines,
        translator=translator,
        s3=s3,
        bucket=BUCKET,
        canonical_prefix="mig/canonical/golden-documents/p00000",
        rejects_prefix="mig/rejects/golden-documents/glue/p00000",
        target_part_bytes=8 * 1024 * 1024,
    )
    assert result.rows == 20 and result.rejected == 2
    assert result.parts == ["mig/canonical/golden-documents/p00000/part-00000.parquet"]
    assert result.reject_parts == ["mig/rejects/golden-documents/glue/p00000/part-00000.jsonl"]
    assert result.reject_reasons == {"embedding is NULL": 1, "line is not JSON": 1}
    records = list(read_canonical(s3, BUCKET, result.parts))
    assert [r.id for r in records][:2] == [
        "golden-documents#doc-00001",
        "golden-documents#doc-00002",
    ]
    rejects = (
        s3.get_object(Bucket=BUCKET, Key=result.reject_parts[0])["Body"]
        .read()
        .decode()
        .splitlines()
    )
    assert json.loads(rejects[0])["id"] == "broken-1"
    merged = merge_results([result, PartitionResult.from_dict(result.to_dict())])
    assert merged["golden-documents"].rows == 40 and merged["golden-documents"].rejected == 4
    assert merged["golden-documents"].reject_reasons["line is not JSON"] == 2


def test_mapping_yaml_round_trips_opensearch_fields(golden_mapping: Mapping) -> None:
    text = golden_mapping.to_yaml()
    assert text.startswith("migration_id: golden-os\nsource:\n  kind: opensearch\n")
    assert "rescan: false" in text
    assert Mapping.from_yaml(text) == golden_mapping
    edited = golden_mapping.model_copy(
        update={
            "source": SourceSpec(kind=SourceKind.OPENSEARCH),
            "target": TargetSpec(index="golden-os", region="us-east-1"),
        }
    )
    assert Mapping.from_yaml(edited.to_yaml()) == edited
