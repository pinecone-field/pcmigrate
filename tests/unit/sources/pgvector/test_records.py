from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from pcmigrate.config.mapping import DenseSpec, MetadataField, MetadataType, Metric, ObjectMapping
from pcmigrate.sources.pgvector.records import RecordBuilder, RecordBuildError, convert, render_id

OBJ = ObjectMapping(
    source="public.documents",
    namespace="documents",
    id_template="documents#{id}",
    dense=DenseSpec(column="embedding", dimension=3, metric=Metric.COSINE),
    metadata=[
        MetadataField(source="title", type=MetadataType.STRING),
        MetadataField(source="published_at", type=MetadataType.EPOCH_SECONDS),
        MetadataField(source="price", type=MetadataType.FLOAT),
        MetadataField(source="tags", type=MetadataType.STRING_LIST),
        MetadataField(source="attrs.nested.lang", type=MetadataType.STRING, transform="flatten"),
        MetadataField(source="attrs.views", type=MetadataType.INTEGER, transform="flatten"),
        MetadataField(
            source="scores", target="scores", type=MetadataType.STRING, transform="stringify"
        ),
        MetadataField(source="body", action="drop", reason="too big"),
    ],
)


def test_render_id_and_composite() -> None:
    assert render_id("documents#{id}", {"id": 7}) == "documents#7"
    assert (
        render_id("events#{id}:{occurred_on}", {"id": 1, "occurred_on": date(2025, 3, 1)})
        == "events#1:2025-03-01"
    )
    with pytest.raises(RecordBuildError):
        render_id("x#{missing}", {"id": 1})


def test_convert_rules() -> None:
    assert convert(datetime(2024, 1, 1, tzinfo=UTC), MetadataType.EPOCH_SECONDS) == 1704067200
    assert convert(date(2024, 1, 1), MetadataType.EPOCH_SECONDS) == 1704067200
    assert convert(Decimal("12.50"), MetadataType.FLOAT) == 12.5
    assert convert(["a", None, "b"], MetadataType.STRING_LIST) == ["a", "b"]
    assert convert(Decimal("3"), MetadataType.INTEGER) == 3
    assert convert(None, MetadataType.STRING) is None
    with pytest.raises(RecordBuildError):
        convert(float("nan"), MetadataType.FLOAT)
    with pytest.raises(RecordBuildError):
        convert("yes", MetadataType.BOOLEAN)


def test_builder_columns_and_record() -> None:
    builder = RecordBuilder(OBJ, ["id"])
    assert builder.source_columns == [
        "id",
        "title",
        "published_at",
        "price",
        "tags",
        "attrs",
        "scores",
        "embedding",
    ]
    row = {
        "id": 42,
        "title": "Hello",
        "published_at": datetime(2024, 5, 1, tzinfo=UTC),
        "price": Decimal("9.99"),
        "tags": ["a", "b"],
        "attrs": {"views": 3, "nested": {"lang": "en", "deep": {"x": 1}}},
        "scores": [1.5, 2.5],
        "body": "ignored",
        "embedding": [0.1, 0.2, 0.3],
    }
    record = builder.build(row)
    assert (
        record.id == "documents#42" and record.namespace == "documents" and record.source_pk == "42"
    )
    assert record.dense == [0.1, 0.2, 0.3] and record.vector_hash is not None
    assert record.metadata == {
        "title": "Hello",
        "published_at": 1714521600,
        "price": 9.99,
        "tags": ["a", "b"],
        "attrs.nested.lang": "en",
        "attrs.views": 3,
        "scores": "[1.5, 2.5]",
    }


def test_builder_handles_nulls_and_json_strings() -> None:
    builder = RecordBuilder(OBJ, ["id"])
    record = builder.build(
        {
            "id": 1,
            "title": None,
            "published_at": None,
            "price": None,
            "tags": None,
            "attrs": '{"views": 9}',
            "scores": None,
            "embedding": [1.0, 0.0, 0.0],
        }
    )
    assert record.metadata == {"attrs.views": 9}
    with pytest.raises(RecordBuildError, match="NULL"):
        builder.build({"id": 1, "embedding": None})


def test_namespace_by() -> None:
    obj = OBJ.model_copy(update={"namespace_by": "tenant_id", "metadata": []})
    builder = RecordBuilder(obj, ["id"])
    assert "tenant_id" in builder.source_columns
    assert (
        builder.build({"id": 1, "tenant_id": "acme", "embedding": [1.0, 0.0, 0.0]}).namespace
        == "documents:acme"
    )
    with pytest.raises(RecordBuildError):
        builder.build({"id": 1, "tenant_id": None, "embedding": [1.0, 0.0, 0.0]})
