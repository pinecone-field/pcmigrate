"""Plan and sizing: canonical bytes, import files and batches, export ETA (ADR-012)."""

from __future__ import annotations

from dataclasses import dataclass, field

from pcmigrate.sources.inventory import Inventory, ObjectInfo
from pcmigrate.targets.emitters.vector_parquet import (
    ImportBatch,
    ImportLimits,
    NamespaceInput,
    plan_import_batches,
)

RECORD_OVERHEAD_BYTES = 96
PARQUET_COMPRESSION_RATIO = 0.85


@dataclass(frozen=True, slots=True)
class TableSizing:
    """Estimates for one table."""

    object: str
    namespace: str
    rows: int
    dimension: int
    metadata_bytes_per_row: int
    canonical_bytes: int
    import_bytes: int
    import_files: int
    export_eta_s: float | None


@dataclass
class SizingPlan:
    """Estimates for the whole run."""

    tables: list[TableSizing] = field(default_factory=list)
    batches: list[ImportBatch] = field(default_factory=list)
    workers: int = 4

    @property
    def total_rows(self) -> int:
        """Rows across tables."""
        return sum(t.rows for t in self.tables)

    @property
    def total_canonical_bytes(self) -> int:
        """Canonical zone size."""
        return sum(t.canonical_bytes for t in self.tables)

    @property
    def total_import_bytes(self) -> int:
        """Import zone size."""
        return sum(t.import_bytes for t in self.tables)

    @property
    def total_import_files(self) -> int:
        """Import files across namespaces."""
        return sum(t.import_files for t in self.tables)

    @property
    def export_eta_s(self) -> float | None:
        """Wall-clock export estimate with ``workers`` partitions in parallel."""
        etas = [t.export_eta_s for t in self.tables if t.export_eta_s is not None]
        if not etas:
            return None
        return sum(etas) / max(1, self.workers)


def metadata_bytes_per_row(table: ObjectInfo, mapped_columns: list[str]) -> int:
    """Average width of the mapped metadata columns, from pg_stats, plus JSON overhead."""
    total = 0
    for name in mapped_columns:
        column = table.column(name)
        width = column.avg_width if column and column.avg_width is not None else 16
        total += width + len(name) + 6
    return total


def size_table(
    table: ObjectInfo,
    namespace: str,
    mapped_columns: list[str],
    *,
    limits: ImportLimits,
) -> TableSizing:
    """Estimate one table."""
    dense = table.dense
    dimension = dense.dimension if dense and dense.dimension else 0
    meta = metadata_bytes_per_row(table, mapped_columns)
    per_row = dimension * 4 + meta + RECORD_OVERHEAD_BYTES
    canonical = int(table.rows * per_row * PARQUET_COMPRESSION_RATIO)
    import_bytes = int(table.rows * (dimension * 4 + meta + 32) * PARQUET_COMPRESSION_RATIO)
    files = max(1, -(-import_bytes // limits.target_part_bytes)) if table.rows else 0
    eta = None
    if table.throughput is not None and table.throughput.rows_per_s > 0:
        eta = table.rows / table.throughput.rows_per_s
    return TableSizing(
        object=table.qualified_name,
        namespace=namespace,
        rows=table.rows,
        dimension=dimension,
        metadata_bytes_per_row=meta,
        canonical_bytes=canonical,
        import_bytes=import_bytes,
        import_files=files,
        export_eta_s=eta,
    )


def size_plan(
    inventory: Inventory,
    namespaces: dict[str, str],
    mapped_columns: dict[str, list[str]],
    *,
    limits: ImportLimits | None = None,
    capacity: str = "on_demand",
    workers: int = 4,
) -> SizingPlan:
    """Estimate every mapped table and plan the import batches."""
    limits = limits or ImportLimits()
    plan = SizingPlan(workers=workers)
    inputs: list[NamespaceInput] = []
    for table in inventory.tables:
        if table.qualified_name not in namespaces:
            continue
        sizing = size_table(
            table,
            namespaces[table.qualified_name],
            mapped_columns.get(table.qualified_name, []),
            limits=limits,
        )
        plan.tables.append(sizing)
        inputs.append(
            NamespaceInput(namespace=sizing.namespace, keys=(), bytes=sizing.import_bytes)
        )
    if inputs:
        plan.batches = plan_import_batches(inputs, limits, capacity=capacity)
    return plan
