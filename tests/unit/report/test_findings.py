from dataclasses import replace

from pcmigrate.config.mapping import Mapping
from pcmigrate.report.findings import (
    FindingsContext,
    Severity,
    blockers,
    check_duplicate_vectors,
    check_export_duration,
    check_import_size,
    check_metadata_size,
    check_pii,
    check_primary_key,
    check_rls,
    check_rls_visibility,
    check_roadmap,
    check_target_cloud,
    check_unknown_metric,
    check_unsupported_shapes,
    check_vector_dimensions,
    run_checks,
)
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping
from pcmigrate.report.sizing import SizingPlan, TableSizing, size_plan
from pcmigrate.sources.pgvector.models import MetricGuess, RlsClass, VectorField, VectorKind
from tests.unit.report.fixtures import (
    column,
    documents_table,
    inventory,
    tenants_table,
    text_search_documents,
)

OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def ctx(
    *tables, roadmap="dense", mapping: Mapping | None = None, sizing: SizingPlan | None = None
) -> FindingsContext:  # type: ignore[no-untyped-def]
    inv = inventory(*tables)
    mapping = mapping or generate_mapping(inv, replace(OPTIONS, roadmap=roadmap))
    sizing = sizing or size_plan(inv, {o.source: o.namespace for o in mapping.objects}, {})
    return FindingsContext(inventory=inv, mapping=mapping, sizing=sizing, roadmap=roadmap)


def test_vector_dimensions_blocker_on_nulls_and_mixed_dims() -> None:
    bad = documents_table(
        vectors=[
            VectorField(
                column="embedding",
                kind=VectorKind.VECTOR,
                dimension=768,
                nullable=True,
                sampled_rows=100,
                null_rows=3,
                distinct_dimensions=[768, 512],
            )
        ]
    )
    (finding,) = check_vector_dimensions(ctx(bad))
    assert finding.severity is Severity.BLOCKER
    assert "3 of 100" in finding.message and "[768, 512]" in finding.message
    assert check_vector_dimensions(ctx(documents_table())) == []


def test_target_cloud_blocker() -> None:
    mapping = generate_mapping(inventory(documents_table()), OPTIONS)
    assert check_target_cloud(ctx(documents_table(), mapping=mapping)) == []
    forced = mapping.model_copy(
        update={"target": mapping.target.model_copy(update={"cloud": "gcp"})}
    )
    (finding,) = check_target_cloud(ctx(documents_table(), mapping=forced))
    assert finding.severity is Severity.BLOCKER


def test_primary_key_blocker() -> None:
    (finding,) = check_primary_key(ctx(documents_table(primary_key=[])))
    assert finding.severity is Severity.BLOCKER and finding.object == "public.documents"


def test_duplicate_vectors_warning_with_method() -> None:
    (finding,) = check_duplicate_vectors(ctx(documents_table()))
    assert finding.severity is Severity.WARNING
    assert "about 4 rows" in finding.message and "exact count" in finding.message
    none = documents_table(duplicates={})
    assert check_duplicate_vectors(ctx(none)) == []


def test_metadata_size_warning() -> None:
    wide = documents_table(columns=[column("blob", "text", avg_width=50_000)])
    (finding,) = check_metadata_size(ctx(wide))
    assert finding.field == "blob" and "40960-byte" in finding.message


def test_unsupported_shapes() -> None:
    odd = documents_table(
        columns=[
            column("scores", "_float8", type_text="double precision[]"),
            column("deep", "jsonb", sample_json_depth=4),
            column("_internal", "text"),
            column("x" * 70, "text"),
        ]
    )
    findings = check_unsupported_shapes(ctx(odd))
    messages = {f.field: f.message for f in findings}
    assert "numeric arrays" in messages["scores"]
    assert "deeper than 2" in messages["deep"]
    assert "reserved" in messages["_internal"]
    assert "longer than 64" in messages["x" * 70]


def test_roadmap_warning_and_signal_info() -> None:
    vector_mapping = generate_mapping(inventory(text_search_documents()), OPTIONS)
    findings = check_roadmap(ctx(text_search_documents(), roadmap="hybrid", mapping=vector_mapping))
    severities = sorted(f.severity.value for f in findings)
    assert severities == ["info", "warning"]
    assert check_roadmap(ctx(text_search_documents(), roadmap="hybrid")) == [], (
        "document index satisfies the roadmap"
    )
    assert check_roadmap(ctx(documents_table(), roadmap="dense")) == []
    assert all(
        f.severity is Severity.INFO
        for f in check_roadmap(ctx(text_search_documents(), roadmap="dense"))
    )


def test_export_duration_warning() -> None:
    slow = SizingPlan(
        tables=[
            TableSizing("public.documents", "documents", 10, 768, 0, 1, 1, 1, export_eta_s=3 * 3600)
        ]
    )
    (finding,) = check_export_duration(ctx(documents_table(), sizing=slow))
    assert "3.0 hours" in finding.message
    fast = SizingPlan(
        tables=[TableSizing("public.documents", "documents", 10, 768, 0, 1, 1, 1, export_eta_s=60)]
    )
    assert check_export_duration(ctx(documents_table(), sizing=fast)) == []


def test_import_size_warning_only_on_demand() -> None:
    huge = SizingPlan(
        tables=[TableSizing("public.documents", "documents", 10, 768, 0, 1, 2 * 1024**4, 1, None)]
    )
    (finding,) = check_import_size(ctx(documents_table(), sizing=huge))
    assert "exceeds the 1 TB" in finding.message
    mapping = generate_mapping(
        inventory(documents_table()),
        replace(OPTIONS, capacity="dedicated"),
    )
    assert check_import_size(ctx(documents_table(), mapping=mapping, sizing=huge)) == []


def test_rls_info_and_warning() -> None:
    (info,) = check_rls(ctx(tenants_table()))
    assert info.severity is Severity.INFO and "namespace" in info.message
    bad = tenants_table()
    bad.policies[0] = bad.policies[0].model_copy(update={"classification": RlsClass.DOES_NOT_MAP})
    (warning,) = check_rls(ctx(bad))
    assert warning.severity is Severity.WARNING


def test_rls_visibility_blocker() -> None:
    hidden = tenants_table().model_copy(update={"rls_hides_rows": True, "exact_rows": 0})
    (finding,) = check_rls_visibility(ctx(hidden))
    assert finding.severity is Severity.BLOCKER and "BYPASSRLS" in finding.fix
    assert check_rls_visibility(ctx(tenants_table())) == []


def test_unknown_metric_warning() -> None:
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
    (finding,) = check_unknown_metric(ctx(noidx))
    assert finding.severity is Severity.WARNING and "<=> cosine" in finding.fix


def test_pii_info() -> None:
    (finding,) = check_pii(ctx(documents_table()))
    assert finding.severity is Severity.INFO and "author_email" in (finding.field or "")


def test_run_checks_orders_blockers_first() -> None:
    hidden = tenants_table().model_copy(update={"rls_hides_rows": True, "exact_rows": 0})
    findings = run_checks(ctx(documents_table(), hidden))
    assert findings[0].severity is Severity.BLOCKER
    assert [f.check for f in blockers(findings)] == ["rls_visibility"]
