"""Turn an inventory into the three approval artifacts: report, mapping.yaml, plan.lock."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.core.approval import MAPPING_NAME, PLAN_LOCK_NAME
from pcmigrate.core.artifacts import ArtifactStore
from pcmigrate.report.findings import Finding, FindingsContext, run_checks
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping
from pcmigrate.report.render import ReportInputs, render_html, render_markdown
from pcmigrate.report.sizing import SizingPlan, size_plan
from pcmigrate.sources.inventory import Inventory
from pcmigrate.targets.emitters.vector_parquet import ImportLimits

INVENTORY_NAME = "inventory.json"
FINDINGS_NAME = "findings.json"
REPORT_MD_NAME = "report.md"
REPORT_HTML_NAME = "report.html"

Roadmap = Literal["dense", "hybrid", "fts"]


@dataclass
class ReportBundle:
    """Everything introspection produces."""

    inventory: Inventory
    mapping: Mapping
    plan_lock: PlanLock
    findings: list[Finding]
    sizing: SizingPlan
    report_md: str
    report_html: str

    def write(self, store: ArtifactStore) -> dict[str, str]:
        """Persist every artifact; returns name -> location."""
        import json

        written = {
            REPORT_MD_NAME: store.write_text(REPORT_MD_NAME, self.report_md),
            REPORT_HTML_NAME: store.write_text(REPORT_HTML_NAME, self.report_html),
            MAPPING_NAME: store.write_text(MAPPING_NAME, self.mapping.to_yaml()),
            PLAN_LOCK_NAME: store.write_text(PLAN_LOCK_NAME, self.plan_lock.to_text()),
            INVENTORY_NAME: store.write_text(
                INVENTORY_NAME, self.inventory.model_dump_json(indent=2) + "\n"
            ),
            FINDINGS_NAME: store.write_text(
                FINDINGS_NAME,
                json.dumps([f.model_dump(mode="json") for f in self.findings], indent=2) + "\n",
            ),
        }
        return written


def change_capture_reason(mapping: Mapping) -> str:
    """One line explaining the recommended tier."""
    if mapping.change_capture is None:
        return "no change capture configured"
    if mapping.change_capture.tier == "none":
        return (
            "the workload window measured zero writes; sync is skipped and cutover runs the "
            "final reconcile"
        )
    if mapping.change_capture.tier == "watermark":
        return (
            "every table has an updated_at column, so no DDL is needed; deletes come from reconcile"
        )
    if mapping.change_capture.tier == "trigger_outbox":
        return (
            "a table lacks an updated_at column, so a trigger outbox captures inserts, "
            "updates, and deletes"
        )
    if mapping.change_capture.tier == "dual_write":
        rescan = (
            f", plus an {mapping.change_capture.watermark_column} rescan for anything the "
            "application missed"
            if mapping.change_capture.rescan
            else "; deletes come from the final reconcile"
        )
        return (
            "OpenSearch has no change stream the tool can read, so the application dual-writes "
            f"into the buffer from T0{rescan}"
        )
    return "chosen by the FDE"


def mapped_columns(mapping: Mapping) -> dict[str, list[str]]:
    """Metadata source columns per object, for sizing."""
    out: dict[str, list[str]] = {}
    for obj in mapping.objects:
        columns = [f.source.split(".", 1)[0] for f in obj.metadata if f.action == "map"]
        for text in obj.text:
            base = text.source.split(".", 1)[0]
            if base not in columns:
                columns.append(base)
        out[obj.source] = columns
    return out


def build_report(
    inventory: Inventory,
    options: MappingOptions,
    *,
    mapping: Mapping | None = None,
    workers: int = 4,
    limits: ImportLimits | None = None,
) -> ReportBundle:
    """Generate (or accept) the mapping, then size, check, lock, and render."""
    mapping = mapping or generate_mapping(inventory, options)
    sizing = size_plan(
        inventory,
        {o.source: o.namespace for o in mapping.objects},
        mapped_columns(mapping),
        limits=limits,
        capacity=mapping.target.capacity,
        workers=workers,
    )
    findings = run_checks(
        FindingsContext(
            inventory=inventory, mapping=mapping, sizing=sizing, roadmap=options.roadmap
        )
    )
    plan_lock = PlanLock.compute(mapping, inventory.fingerprint)
    inputs = ReportInputs(
        inventory=inventory,
        mapping=mapping,
        plan_lock=plan_lock,
        findings=findings,
        sizing=sizing,
        roadmap=options.roadmap,
        change_capture_reason=change_capture_reason(mapping),
    )
    return ReportBundle(
        inventory=inventory,
        mapping=mapping,
        plan_lock=plan_lock,
        findings=findings,
        sizing=sizing,
        report_md=render_markdown(inputs),
        report_html=render_html(inputs),
    )
