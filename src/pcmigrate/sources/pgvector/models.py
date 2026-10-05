"""Inventory models produced by pgvector introspection and consumed by the report.

The models themselves are source-neutral and live in ``pcmigrate.sources.inventory``; this
module keeps the pgvector names so the pgvector connector and its tests read naturally.
"""

from __future__ import annotations

from pcmigrate.sources.inventory import (
    ColumnInfo,
    DuplicateEstimate,
    IndexInfo,
    Inventory,
    MetricGuess,
    ObjectInfo,
    RlsClass,
    RlsPolicy,
    TextSearchField,
    ThroughputSample,
    VectorField,
    VectorKind,
    WorkloadSample,
)

PgInventory = Inventory
TableInfo = ObjectInfo

__all__ = [
    "ColumnInfo",
    "DuplicateEstimate",
    "IndexInfo",
    "Inventory",
    "MetricGuess",
    "ObjectInfo",
    "PgInventory",
    "RlsClass",
    "RlsPolicy",
    "TableInfo",
    "TextSearchField",
    "ThroughputSample",
    "VectorField",
    "VectorKind",
    "WorkloadSample",
]
