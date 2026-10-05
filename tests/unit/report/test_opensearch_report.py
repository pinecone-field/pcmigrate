"""The shared report stack rendering an OpenSearch inventory."""

from datetime import UTC, datetime

from pcmigrate.config.mapping import MetadataType
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions, scalar_type_of
from pcmigrate.sources.inventory import (
    ColumnInfo,
    Inventory,
    MetricGuess,
    ObjectInfo,
    TextSearchField,
    VectorField,
    VectorKind,
)
from tests.unit.report.fixtures import inventory as pg_inventory

OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def _column(name: str, base: str, **kw: object) -> ColumnInfo:
    return ColumnInfo(name=name, type=base, base_type=base, nullable=True, position=1, **kw)  # type: ignore[arg-type]


def _inventory() -> Inventory:
    index = ObjectInfo(
        schema_name="",
        name="products",
        relkind="i",
        shards=3,
        aliases=["products-live"],
        row_estimate=50_000,
        exact_rows=50_000,
        total_bytes=400_000_000,
        primary_key=["_id"],
        columns=[
            _column("sku", "keyword", avg_width=12),
            _column("title", "text", avg_width=40, analyzer="english"),
            _column("price", "scaled_float", avg_width=6),
            _column("in_stock", "boolean"),
            _column("updated_at", "date", avg_width=24),
            _column("tags", "keyword_array", avg_width=20),
            _column("ratings", "integer_array", avg_width=10),
            _column(
                "specs",
                "object",
                sample_values=['{"color": "red", "weight": 2}'],
                sample_json_depth=1,
            ),
            _column(
                "reviews",
                "nested",
                sample_values=['[{"user": "a", "stars": 5}]'],
                sample_json_depth=2,
            ),
            _column("location", "geo_point"),
            _column("description", "text", avg_width=60_000),
            _column("owner_email", "keyword"),
            _column("$weird", "keyword"),
        ],
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.KNN_VECTOR,
                dimension=384,
                nullable=True,
                metric=MetricGuess.DOTPRODUCT,
                space_type="innerproduct",
                engine="faiss",
                metric_index="hnsw",
                sampled_rows=500,
                in_source=True,
                distinct_dimensions=[384],
            ),
            VectorField(
                column="sparse_terms", kind=VectorKind.RANK_FEATURES, dimension=None, nullable=True
            ),
        ],
        text_search=[TextSearchField(column="title", kind="text", analyzer="english")],
    )
    return Inventory(
        kind="opensearch",
        server_version="2.19.1",
        sampled_at=datetime(2026, 10, 5, tzinfo=UTC),
        read_only_session=True,
        tables=[index],
        plugin_versions={"opensearch-knn": "2.19.1.0"},
    )


def test_opensearch_type_rules() -> None:
    assert scalar_type_of("keyword") is MetadataType.STRING
    assert scalar_type_of("unsigned_long") is MetadataType.INTEGER
    assert scalar_type_of("half_float") is MetadataType.FLOAT
    assert scalar_type_of("boolean") is MetadataType.BOOLEAN
    assert scalar_type_of("date_nanos") is MetadataType.EPOCH_SECONDS
    assert scalar_type_of("keyword_array") is MetadataType.STRING_LIST
    assert scalar_type_of("long_array") is None
    assert scalar_type_of("geo_point") is None


def test_generated_mapping_and_report_for_an_index() -> None:
    bundle = build_report(_inventory(), OPTIONS)
    obj = bundle.mapping.objects[0]
    assert (
        obj.source == "products"
        and obj.namespace == "products"
        and obj.id_template == "products#{_id}"
    )
    assert (
        obj.dense is not None
        and obj.dense.metric.value == "dotproduct"
        and obj.dense.dimension == 384
    )
    fields = {f.source: f for f in obj.metadata}
    assert (
        fields["price"].type is MetadataType.FLOAT
        and fields["in_stock"].type is MetadataType.BOOLEAN
    )
    assert fields["updated_at"].type is MetadataType.EPOCH_SECONDS
    assert fields["tags"].type is MetadataType.STRING_LIST
    assert fields["ratings"].transform == "stringify"
    assert (
        fields["specs.color"].transform == "flatten"
        and fields["specs.weight"].type is MetadataType.INTEGER
    )
    assert fields["reviews"].transform == "stringify"
    assert fields["location"].action == "drop" and "no scalar metadata form" in str(
        fields["location"].reason
    )
    assert fields["description"].action == "drop" and "size limit" in str(
        fields["description"].reason
    )
    assert fields["sparse_terms"].action == "drop" and "additional rank_features" in str(
        fields["sparse_terms"].reason
    )
    assert fields["$weird"].target == "f_weird"
    assert "title" in fields and fields["title"].type is MetadataType.STRING
    assert bundle.mapping.target.type == "vector"
    checks = {(f.check, f.severity.value) for f in bundle.findings}
    assert ("pii", "info") in checks and ("metadata_size", "warning") in checks
    assert ("unsupported_shapes", "warning") in checks
    assert ("roadmap", "info") in checks
    assert not any(c == "unknown_metric" for c, _ in checks), "rank_features has no metric to guess"
    assert not any(c == "vector_source" for c, _ in checks)
    assert (
        bundle.mapping.change_capture is not None
        and bundle.mapping.change_capture.tier == "dual_write"
    )
    assert (
        bundle.mapping.change_capture.watermark_column == "updated_at"
        and bundle.mapping.change_capture.rescan
    )
    md = bundle.report_md
    assert "Generated" in md and "OpenSearch 2.19.1 (k-NN plugin 2.19.1.0)" in md
    assert "| `products` | index (3 shards) |" in md and "Aliases: products-live." in md
    assert "| `title` | text |" in md and "analyzer english" in md
    assert "script_score with knn_score" in md
    assert "dual-writes into the buffer from T0" in md and "updated_at rescan" in md
    assert "No row- or document-level access rules on the inventoried indexs" not in md
    html = bundle.report_html
    assert "index (3 shards)" in html and "knn_score" in html


def test_pgvector_report_still_renders_the_same_header() -> None:
    bundle = build_report(pg_inventory(), OPTIONS)
    assert "PostgreSQL 16.15 (pgvector 0.8.7)" in bundle.report_md
    assert "| `public.documents` | table |" in bundle.report_md
    assert "index scans disabled" in bundle.report_md
    assert "No row- or document-level access rules" not in bundle.report_md
