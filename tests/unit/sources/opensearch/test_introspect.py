import pytest

from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.inventory import MetricGuess, RlsClass, VectorKind
from pcmigrate.sources.opensearch.client import ReadOnlyViolationError
from pcmigrate.sources.opensearch.introspect import (
    IntrospectOptions,
    OpenSearchIntrospector,
    metric_from_space_type,
    source_setting_for,
)
from tests.unit.sources.opensearch.fakes import FakeIndex, FakeTransport, make_client

OPTIONS = MappingOptions(migration_id="golden-os", index="golden-os", region="us-east-1")


def _introspector(transport: FakeTransport, **kw: object) -> OpenSearchIntrospector:
    opts = IntrospectOptions(workload_window_s=0, sample_docs=200, **kw)  # type: ignore[arg-type]
    return OpenSearchIntrospector(make_client(transport), opts, sleep=lambda s: None)


@pytest.mark.parametrize(
    ("space", "metric"),
    [
        ("l2", MetricGuess.EUCLIDEAN),
        ("cosinesimil", MetricGuess.COSINE),
        ("innerproduct", MetricGuess.DOTPRODUCT),
        ("l1", MetricGuess.UNSUPPORTED),
        ("linf", MetricGuess.UNSUPPORTED),
        ("hamming", MetricGuess.UNSUPPORTED),
        (None, MetricGuess.UNKNOWN),
    ],
)
def test_space_type_mapping(space: str | None, metric: MetricGuess) -> None:
    assert metric_from_space_type(space) is metric


def test_golden_inventory_shape() -> None:
    transport = FakeTransport.golden()
    inventory = _introspector(transport).inventory()
    assert inventory.kind == "opensearch" and inventory.server_version == "2.19.1"
    assert inventory.source_label == "OpenSearch 2.19.1 (k-NN plugin 2.19.1.0)"
    (table,) = inventory.tables
    assert table.qualified_name == "golden-documents" and table.relkind == "i"
    assert (
        table.primary_key == ["_id"]
        and table.rows == 1005
        and table.kind_label == "index (1 shards)"
    )
    assert table.settings["knn"] is True and table.settings["source_excludes"] == []
    vector = table.dense
    assert vector is not None
    assert vector.kind is VectorKind.KNN_VECTOR and vector.dimension == 768
    (sparse,) = table.sparse
    assert sparse.kind is VectorKind.RANK_FEATURES and sparse.column == "sparse"
    assert vector.metric is MetricGuess.COSINE and vector.space_type == "cosinesimil"
    assert vector.engine == "lucene" and vector.metric_index == "hnsw"
    assert vector.in_source is True and vector.source_setting is None
    assert vector.distinct_dimensions == [768] and vector.null_rows == 0
    by_name = {c.name: c for c in table.columns}
    assert by_name["author"].base_type == "nested" and by_name["author"].sample_json_depth == 2
    assert (
        by_name["published_at"].base_type == "date"
        and 0 < (by_name["published_at"].null_frac or 0) < 0.2
    )
    assert by_name["category"].null_frac and by_name["views"].base_type == "integer"
    assert {t.column: t.analyzer for t in table.text_search} == {
        "title": "english_stem",
        "body": None,
        "summary": "summary_stop",
    }
    title = next(t for t in table.text_search if t.column == "title")
    assert title.multi_fields == {"raw": "keyword"}
    assert "summary_stop" in table.settings["analysis"]["analyzer"]
    assert table.duplicates["embedding"].estimated_duplicate_rows == 4
    assert table.duplicates["embedding"].method == "exact"
    assert inventory.fingerprint == _introspector(FakeTransport.golden()).inventory().fingerprint


def test_introspection_is_read_only() -> None:
    transport = FakeTransport.golden()
    introspector = _introspector(transport)
    introspector.inventory()
    verbs = {m for m, _ in introspector.audit.requests}
    assert verbs <= {"GET", "POST", "DELETE", "HEAD"}
    for method, path in introspector.audit.requests:
        if method == "POST":
            assert path.endswith(("/_search", "/_count", "/_search/point_in_time", "/_mget"))
        if method == "DELETE":
            assert path == "/_search/point_in_time"
    client = make_client(transport)
    with pytest.raises(ReadOnlyViolationError):
        client.request("PUT", "/golden-documents/_settings", body={})
    with pytest.raises(ReadOnlyViolationError):
        client.request("POST", "/golden-documents/_doc", body={})
    with pytest.raises(ReadOnlyViolationError):
        client.request("DELETE", "/golden-documents")


def test_vector_excluded_from_source_is_a_blocker_naming_the_setting() -> None:
    transport = FakeTransport.golden()
    index = transport.indices["golden-documents"]
    index.mapping["_source"] = {"excludes": ["embedding"]}
    inventory = _introspector(transport).inventory()
    vector = inventory.tables[0].vectors[0]
    assert vector.in_source is False and vector.source_setting == "_source.excludes: embedding"
    bundle = build_report(inventory, OPTIONS)
    blocker = next(f for f in bundle.findings if f.check == "vector_source")
    assert blocker.severity.value == "blocker" and "_source.excludes: embedding" in blocker.message
    assert inventory.tables[0].duplicates["embedding"].method == "skipped"


def test_source_disabled_and_derived_source_settings() -> None:
    assert source_setting_for("embedding", {"enabled": False}, {}) == "_source.enabled: false"
    assert source_setting_for("embedding", {"excludes": ["emb*"]}, {}) == "_source.excludes: emb*"
    assert (
        source_setting_for("embedding", {"includes": ["title"]}, {})
        == "_source.includes: ['title']"
    )
    assert (
        source_setting_for("embedding", {}, {"index.knn.derived_source.enabled": "true"})
        == "index.knn.derived_source.enabled: true"
    )
    assert source_setting_for("embedding", {}, {}) is None
    transport = FakeTransport.golden()
    transport.indices["golden-documents"].settings["index"]["knn.derived_source.enabled"] = "true"
    inventory = _introspector(transport).inventory()
    vector = inventory.tables[0].vectors[0]
    assert (
        vector.in_source is True
        and vector.source_setting == "index.knn.derived_source.enabled: true"
    )
    findings = build_report(inventory, OPTIONS).findings
    derived = next(f for f in findings if f.check == "vector_source")
    assert derived.severity.value == "warning"


def test_unsupported_space_type_and_byte_vectors_are_blockers() -> None:
    transport = FakeTransport.golden()
    props = transport.indices["golden-documents"].mapping["properties"]
    props["embedding"]["method"]["space_type"] = "l1"
    props["embedding"]["data_type"] = "byte"
    inventory = _introspector(transport).inventory()
    checks = {f.check: f.severity.value for f in build_report(inventory, OPTIONS).findings}
    assert checks["vector_space_type"] == "blocker" and checks["vector_data_type"] == "blocker"


def test_default_space_type_is_l2_and_model_id_is_unknown() -> None:
    transport = FakeTransport.golden()
    props = transport.indices["golden-documents"].mapping["properties"]
    del props["embedding"]["method"]
    inventory = _introspector(transport).inventory()
    assert inventory.tables[0].vectors[0].metric is MetricGuess.EUCLIDEAN
    props["embedding"]["model_id"] = "trained-1"
    inventory = _introspector(FakeTransport(indices=transport.indices)).inventory()
    assert inventory.tables[0].vectors[0].metric is MetricGuess.UNKNOWN
    findings = build_report(inventory, OPTIONS).findings
    assert any(f.check == "unknown_metric" and "trained model" in f.message for f in findings)


def test_aliases_select_indices_and_appear_in_the_inventory() -> None:
    transport = FakeTransport.golden()
    transport.indices["golden-documents"].aliases = ["docs-live"]
    transport.indices[".kibana"] = FakeIndex(mapping={"properties": {}})
    inventory = _introspector(transport, indices=("docs-live",)).inventory()
    assert [t.name for t in inventory.tables] == ["golden-documents"]
    assert inventory.tables[0].aliases == ["docs-live"]
    everything = _introspector(transport).inventory()
    assert [t.name for t in everything.tables] == ["golden-documents"], "system indices are skipped"
    with pytest.raises(LookupError):
        _introspector(transport, indices=("nope-*",)).inventory()


def test_workload_rates_from_stats_deltas() -> None:
    transport = FakeTransport.golden(writes_per_stats_call=30)
    ticks = iter([0.0, 0.1, 0.2, 60.3, 60.4, 60.5, 60.6, 60.7, 60.8, 61.0, 61.1, 61.2])
    introspector = OpenSearchIntrospector(
        make_client(transport),
        IntrospectOptions(workload_window_s=60, sample_docs=50),
        sleep=lambda s: None,
        clock=lambda: next(ticks),
    )
    inventory = introspector.inventory()
    workload = inventory.tables[0].workload
    assert workload is not None and workload.measured
    assert workload.inserts_per_min > 0 and workload.read_qps and workload.read_qps > 0
    assert inventory.cluster["workload"]["index_ops_per_min"] > 0
    mapping = build_report(inventory, OPTIONS).mapping
    assert mapping.change_capture is not None and mapping.change_capture.tier == "dual_write"


def test_quiet_window_recommends_tier_none() -> None:
    transport = FakeTransport.golden(writes_per_stats_call=0)
    ticks = iter([float(i) * 10 for i in range(40)])
    introspector = OpenSearchIntrospector(
        make_client(transport),
        IntrospectOptions(workload_window_s=30, sample_docs=50),
        sleep=lambda s: None,
        clock=lambda: next(ticks),
    )
    mapping = build_report(introspector.inventory(), OPTIONS).mapping
    assert mapping.change_capture is not None and mapping.change_capture.tier == "none"


def test_document_level_security_classification() -> None:
    transport = FakeTransport.golden(
        security_roles={
            "tenant_reader": {
                "index_permissions": [
                    {
                        "index_patterns": ["golden-*"],
                        "dls": '{"term": {"category": "${user.name}"}}',
                        "fls": None,
                    }
                ]
            },
            "news_only": {
                "index_permissions": [
                    {
                        "index_patterns": ["golden-documents"],
                        "dls": '{"term": {"category": "news"}}',
                    }
                ]
            },
            "hidden_email": {
                "index_permissions": [
                    {"index_patterns": ["golden-documents"], "fls": ["~author.email"]}
                ]
            },
            "other": {
                "index_permissions": [{"index_patterns": ["logs-*"], "dls": '{"match_all": {}}'}]
            },
        }
    )
    inventory = _introspector(transport).inventory()
    policies = {p.name: p for p in inventory.tables[0].policies}
    assert set(policies) == {"tenant_reader", "news_only", "hidden_email"}
    assert policies["tenant_reader"].classification is RlsClass.NAMESPACE
    assert policies["tenant_reader"].column == "category"
    assert policies["news_only"].classification is RlsClass.FILTER
    assert policies["hidden_email"].classification is RlsClass.DOES_NOT_MAP
    assert inventory.cluster["security_plugin"] is True
    mapping = build_report(inventory, OPTIONS).mapping
    assert mapping.objects[0].namespace_by == "category"


def test_sampled_duplicates_above_exact_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    from pcmigrate.sources.opensearch import introspect as module

    monkeypatch.setattr(module, "EXACT_DUPLICATES_MAX_DOCS", 10)
    inventory = _introspector(FakeTransport.golden()).inventory()
    estimate = inventory.tables[0].duplicates["embedding"]
    assert estimate.method == "sampled" and estimate.sampled_rows == 200
    assert estimate.estimated_duplicate_rows >= 0
