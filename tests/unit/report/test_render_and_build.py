from pathlib import Path

from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.core.artifacts import LocalArtifactStore
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.report.render import duration, human_bytes, percent, thousands
from tests.unit.report.fixtures import inventory

OPTIONS = MappingOptions(
    migration_id="acme", index="acme-docs", region="us-east-1", roadmap="hybrid"
)


def test_filters() -> None:
    assert human_bytes(1536) == "1.5 KB" and human_bytes(10) == "10 B" and human_bytes(None) == ""
    assert thousands(1234567) == "1,234,567"
    assert percent(0.1234) == "12.3%"
    assert duration(5400) == "1h 30m" and duration(61) == "1m 1s" and duration(None) == "unknown"


def test_build_report_sections_in_order(tmp_path: Path) -> None:
    bundle = build_report(inventory(), OPTIONS)
    md = bundle.report_md
    positions = [md.index(f"## {n}.") for n in range(1, 10)]
    assert positions == sorted(positions)
    for heading in (
        "Inventory",
        "Capability readiness",
        "Workload",
        "Access control",
        "Proposed mapping",
        "Findings",
        "Plan and sizing",
        "Validation plan",
        "Approval checklist",
    ):
        assert heading in md
    assert "tenants_isolation" in md
    assert "<table>" in bundle.report_html and "acme-docs" in bundle.report_html
    assert (
        bundle.plan_lock.plan_hash
        == PlanLock.compute(bundle.mapping, bundle.inventory.fingerprint).plan_hash
    )
    written = bundle.write(LocalArtifactStore(tmp_path))
    assert set(written) == {
        "report.md",
        "report.html",
        "mapping.yaml",
        "plan.lock",
        "inventory.json",
        "findings.json",
    }
    assert (
        PlanLock.from_text((tmp_path / "plan.lock").read_text()).plan_hash
        == bundle.plan_lock.plan_hash
    )


def test_report_reflects_mapping_edits() -> None:
    first = build_report(inventory(), OPTIONS)
    edited = first.mapping.model_copy(
        update={
            "objects": [
                first.mapping.objects[0].model_copy(update={"namespace": "renamed"}),
                *first.mapping.objects[1:],
            ]
        }
    )
    second = build_report(inventory(), OPTIONS, mapping=edited)
    assert second.plan_lock.plan_hash != first.plan_lock.plan_hash
    assert "namespace: renamed" in second.report_md
