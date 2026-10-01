"""Emitters turn canonical Parquet into Pinecone import files (ADR-004, ADR-012)."""

from pcmigrate.targets.emitters.vector_parquet import (
    EmitResult,
    ImportBatch,
    ImportLimits,
    ImportPlanError,
    NamespaceInput,
    VectorParquetEmitter,
    plan_import_batches,
)

__all__ = [
    "EmitResult",
    "ImportBatch",
    "ImportLimits",
    "ImportPlanError",
    "NamespaceInput",
    "VectorParquetEmitter",
    "plan_import_batches",
]
