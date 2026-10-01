"""Pydantic models for configuration: MigrationConfig, Mapping, and PlanLock (ADR-005)."""

from pcmigrate.config.mapping import (
    ChangeCaptureSpec,
    DenseSpec,
    Mapping,
    MetadataField,
    ObjectMapping,
    SourceSpec,
    TargetSpec,
    ValidationSpec,
)
from pcmigrate.config.migration import MigrationConfig
from pcmigrate.config.plan_lock import PlanLock, schema_fingerprint

__all__ = [
    "ChangeCaptureSpec",
    "DenseSpec",
    "Mapping",
    "MetadataField",
    "MigrationConfig",
    "ObjectMapping",
    "PlanLock",
    "SourceSpec",
    "TargetSpec",
    "ValidationSpec",
    "schema_fingerprint",
]
