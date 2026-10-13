"""Jinja2 rendering of report.md and report.html in the section order from the design."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from jinja2 import Environment, PackageLoader, select_autoescape

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.report.findings import Finding, Severity
from pcmigrate.report.sizing import SizingPlan
from pcmigrate.sources.inventory import Inventory


def human_bytes(value: float | int | None) -> str:
    """1536 -> '1.5 KB'."""
    if value is None:
        return ""
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if size < 1024 or unit == "PB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def thousands(value: int | float | None) -> str:
    """1234567 -> '1,234,567'."""
    return "" if value is None else f"{int(value):,}"


def percent(value: float | None) -> str:
    """0.1234 -> '12.3%'."""
    return "" if value is None else f"{value * 100:.1f}%"


def duration(seconds: float | None) -> str:
    """5400 -> '1h 30m'."""
    if seconds is None:
        return "unknown"
    total = round(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def code(value: Any) -> str:
    """Wrap in backticks for Markdown."""
    return f"`{value}`"


@dataclass(frozen=True, slots=True)
class ReportInputs:
    """Everything the templates need."""

    inventory: Inventory
    mapping: Mapping
    plan_lock: PlanLock
    findings: Sequence[Finding]
    sizing: SizingPlan
    roadmap: str
    change_capture_reason: str


def _environment() -> Environment:
    env = Environment(
        loader=PackageLoader("pcmigrate.report", "templates"),
        autoescape=select_autoescape(enabled_extensions=("html",), default_for_string=False),
        trim_blocks=False,
        lstrip_blocks=False,
        keep_trailing_newline=True,
    )
    env.filters.update(
        human_bytes=human_bytes, thousands=thousands, percent=percent, duration=duration, code=code
    )
    return env


def _context(inputs: ReportInputs) -> dict[str, Any]:
    findings = list(inputs.findings)
    return {
        "inventory": inputs.inventory,
        "mapping": inputs.mapping,
        "mapping_yaml": inputs.mapping.to_yaml(),
        "plan_lock": inputs.plan_lock,
        "findings": findings,
        "blockers": [f for f in findings if f.severity is Severity.BLOCKER],
        "warnings": [f for f in findings if f.severity is Severity.WARNING],
        "infos": [f for f in findings if f.severity is Severity.INFO],
        "sizing": inputs.sizing,
        "roadmap": inputs.roadmap,
        "generated_sparse": {
            o.source: o.sparse for o in inputs.mapping.objects if o.generated_sparse is not None
        },
        "text_fields": {o.source: o.text for o in inputs.mapping.objects if o.text},
        "change_capture_reason": inputs.change_capture_reason,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
    }


def render_markdown(inputs: ReportInputs) -> str:
    """report.md."""
    return _environment().get_template("report.md.j2").render(_context(inputs))


def render_html(inputs: ReportInputs) -> str:
    """report.html."""
    return _environment().get_template("report.html.j2").render(_context(inputs))
