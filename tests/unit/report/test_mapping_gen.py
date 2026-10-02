from dataclasses import replace

import pytest

from pcmigrate.config.mapping import Mapping, MetadataType
from pcmigrate.report.mapping_gen import (
    MappingOptions,
    generate_mapping,
    id_template,
    recommend_change_capture,
)
from pcmigrate.sources.pgvector.models import MetricGuess, VectorField, VectorKind
from tests.unit.report.fixtures import column, documents_table, inventory, tenants_table

OPTIONS = MappingOptions(
    migration_id="acme",
    index="acme-docs",
    region="us-east-1",
    dsn_secret="arn:aws:secretsmanager:us-east-1:1:secret:x",
)


def fields(mapping: Mapping, source: str) -> dict[str, object]:
    (obj,) = [o for o in mapping.objects if o.source == source]
    return {f.source: f for f in obj.metadata}


def test_type_rules() -> None:
    mapping = generate_mapping(inventory(documents_table()), OPTIONS)
    f = fields(mapping, "public.documents")
    assert f["published_at"].type is MetadataType.EPOCH_SECONDS  # type: ignore[attr-defined]
    assert f["price"].type is MetadataType.FLOAT  # type: ignore[attr-defined]
    assert f["tags"].type is MetadataType.STRING_LIST  # type: ignore[attr-defined]
    assert (
        f["attrs.nested.lang"].type is MetadataType.STRING
        and f["attrs.nested.lang"].transform == "flatten"
    )  # type: ignore[attr-defined]
    assert f["attrs.views"].type is MetadataType.INTEGER  # type: ignore[attr-defined]
    assert "attrs" not in f
    assert "id" not in f, "primary key is not metadata"


def test_deep_json_is_stringified() -> None:
    deep = documents_table(
        columns=[
            column("deep", "jsonb", sample_values=['{"a": {"b": {"c": 1}}}'], sample_json_depth=3)
        ]
    )
    f = fields(generate_mapping(inventory(deep), OPTIONS), "public.documents")
    assert f["deep"].transform == "stringify" and f["deep"].type is MetadataType.STRING  # type: ignore[attr-defined]


def test_oversized_and_unsupported_fields_are_dropped_with_reasons() -> None:
    table = documents_table(
        columns=[
            column("blob", "text", avg_width=60_000),
            column("raw", "bytea"),
            column("scores", "_float8"),
        ],
        text_search=[],
        vectors=[
            *documents_table().vectors,
            VectorField(column="sparse", kind=VectorKind.SPARSEVEC, dimension=30000, nullable=True),
        ],
    )
    f = fields(generate_mapping(inventory(table), OPTIONS), "public.documents")
    assert f["blob"].action == "drop" and "exceeds metadata size limit" in f["blob"].reason  # type: ignore[attr-defined]
    assert f["raw"].action == "drop" and "no scalar" in f["raw"].reason  # type: ignore[attr-defined]
    assert f["scores"].transform == "stringify"  # type: ignore[attr-defined]
    assert f["sparse"].action == "drop" and "additional sparsevec" in f["sparse"].reason  # type: ignore[attr-defined]


def test_id_template_and_composite_keys() -> None:
    assert id_template(documents_table()) == "documents#{id}"
    assert (
        id_template(documents_table(primary_key=["id", "occurred_on"]))
        == "documents#{id}:{occurred_on}"
    )


def test_metric_and_unknown_default() -> None:
    mapping = generate_mapping(inventory(documents_table()), OPTIONS)
    assert (
        mapping.objects[0].dense is not None and mapping.objects[0].dense.metric.value == "cosine"
    )
    noidx = documents_table(
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.VECTOR,
                dimension=768,
                nullable=False,
                metric=MetricGuess.UNKNOWN,
            )
        ]
    )
    fallback = generate_mapping(inventory(noidx), OPTIONS)
    assert (
        fallback.objects[0].dense is not None and fallback.objects[0].dense.metric.value == "cosine"
    )


def test_namespace_by_from_rls_policy() -> None:
    mapping = generate_mapping(inventory(tenants_table()), OPTIONS)
    assert (
        mapping.objects[0].namespace == "tenants" and mapping.objects[0].namespace_by == "tenant_id"
    )


def test_roadmap_selects_document_index_and_change_capture() -> None:
    mapping = generate_mapping(inventory(documents_table()), replace(OPTIONS, roadmap="fts"))
    assert mapping.target.type == "document"
    assert recommend_change_capture(inventory(documents_table())).tier == "trigger_outbox"
    with_updated = documents_table(
        columns=[*documents_table().columns, column("updated_at", "timestamptz")]
    )
    assert recommend_change_capture(inventory(with_updated)).tier == "watermark"


def test_yaml_is_in_documented_shape_and_round_trips() -> None:
    mapping = generate_mapping(inventory(documents_table()), OPTIONS)
    text = mapping.to_yaml()
    assert "action: map" not in text and "options:" not in text
    assert text.startswith("migration_id: acme\nsource:\n  kind: pgvector\n")
    assert Mapping.from_yaml(text) == mapping


def test_no_vector_table_is_an_error() -> None:
    with pytest.raises(ValueError, match="no table"):
        generate_mapping(inventory(documents_table(vectors=[])), OPTIONS)


def test_change_capture_none_when_no_writes_measured() -> None:
    from pcmigrate.sources.pgvector.models import WorkloadSample

    quiet = documents_table(workload=WorkloadSample(window_s=300, measured=True))
    assert recommend_change_capture(inventory(quiet)).tier == "none"
    busy = documents_table(
        workload=WorkloadSample(window_s=300, measured=True, inserts_per_min=3.0)
    )
    assert recommend_change_capture(inventory(busy)).tier == "trigger_outbox"
    mapping = generate_mapping(inventory(quiet), OPTIONS)
    assert not mapping.change_capture_enabled
    assert "tier: none" in mapping.to_yaml()
