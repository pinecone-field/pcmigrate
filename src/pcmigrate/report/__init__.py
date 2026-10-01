"""Introspection report: findings, mapping, sizing, approval checklist as Markdown and HTML."""

from pcmigrate.report.build import ReportBundle, build_report
from pcmigrate.report.findings import Finding, Severity, run_checks
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping

__all__ = [
    "Finding",
    "MappingOptions",
    "ReportBundle",
    "Severity",
    "build_report",
    "generate_mapping",
    "run_checks",
]
