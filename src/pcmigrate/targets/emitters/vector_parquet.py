"""VectorIndexEmitter: canonical records to vector-index import Parquet (id, values, metadata).

Import files land as ``import/<batch>/<namespace>/part-N.parquet`` so one import URI
(``import/<batch>/``) carries several namespaces, each a subdirectory, as Pinecone expects.
Batches are planned up front from canonical sizes so every import stays inside ADR-012.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from pcmigrate.core.canonical import PartInfo, RollingParquetWriter, read_parquet_batches
from pcmigrate.core.events import EventBus, PartDone
from pcmigrate.core.state import Stage

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

MiB = 1024 * 1024
GiB = 1024 * MiB
TiB = 1024 * GiB

IMPORT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("values", pa.list_(pa.float32()), nullable=False),
        pa.field("metadata", pa.string()),
    ]
)

CANONICAL_TO_IMPORT_COLUMNS = ["id", "namespace", "dense", "metadata"]


@dataclass(frozen=True, slots=True)
class ImportLimits:
    """Pinecone's published import limits, per ADR-012."""

    target_part_bytes: int = 768 * MiB
    max_part_bytes: int = 10 * GiB
    max_files_per_import: int = 100_000
    max_namespaces_per_import: int = 10_000
    max_bytes_on_demand: int = 1 * TiB
    size_safety_factor: float = 1.2

    def __post_init__(self) -> None:
        if not 512 * MiB <= self.target_part_bytes <= 1 * GiB:
            raise ValueError("target_part_bytes must be between 512 MiB and 1 GiB (ADR-012)")


@dataclass(frozen=True, slots=True)
class NamespaceInput:
    """Canonical parts that feed one namespace."""

    namespace: str
    keys: tuple[str, ...]
    bytes: int

    def estimated_files(self, limits: ImportLimits) -> int:
        """How many import parts this namespace will produce, at least one."""
        estimated = int(self.bytes * limits.size_safety_factor)
        return max(1, -(-estimated // limits.target_part_bytes))


@dataclass(frozen=True, slots=True)
class ImportBatch:
    """One import: a set of namespaces whose combined size fits the limits."""

    id: str
    namespaces: tuple[str, ...]
    estimated_bytes: int
    estimated_files: int


class ImportPlanError(ValueError):
    """A single namespace exceeds what one import can take."""


def plan_import_batches(
    inputs: Sequence[NamespaceInput], limits: ImportLimits, *, capacity: str = "on_demand"
) -> list[ImportBatch]:
    """First-fit packing of namespaces into imports under the file, namespace, and byte limits."""
    byte_cap = limits.max_bytes_on_demand if capacity == "on_demand" else None
    batches: list[list[NamespaceInput]] = []
    totals: list[tuple[int, int]] = []
    for item in sorted(inputs, key=lambda i: i.bytes, reverse=True):
        est_bytes = int(item.bytes * limits.size_safety_factor)
        est_files = item.estimated_files(limits)
        if est_files > limits.max_files_per_import or (byte_cap and est_bytes > byte_cap):
            raise ImportPlanError(
                f"namespace {item.namespace!r} needs {est_files} files / {est_bytes} bytes, "
                "over the per-import limit; split it across namespaces or use dedicated capacity"
            )
        for index, (batch_bytes, batch_files) in enumerate(totals):
            if (
                len(batches[index]) < limits.max_namespaces_per_import
                and batch_files + est_files <= limits.max_files_per_import
                and (byte_cap is None or batch_bytes + est_bytes <= byte_cap)
            ):
                batches[index].append(item)
                totals[index] = (batch_bytes + est_bytes, batch_files + est_files)
                break
        else:
            batches.append([item])
            totals.append((est_bytes, est_files))
    return [
        ImportBatch(
            id=f"batch-{i:05d}",
            namespaces=tuple(sorted(n.namespace for n in members)),
            estimated_bytes=totals[i][0],
            estimated_files=totals[i][1],
        )
        for i, members in enumerate(batches)
    ]


@dataclass
class EmitResult:
    """What one batch emission wrote."""

    batch: ImportBatch
    uri: str
    parts: list[PartInfo] = field(default_factory=list)
    rows_by_namespace: dict[str, int] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        """Total rows across namespaces."""
        return sum(self.rows_by_namespace.values())


def canonical_batch_to_import(batch: pa.RecordBatch) -> dict[str, pa.RecordBatch]:
    """Project a canonical batch onto the import schema, split by namespace."""
    table = pa.Table.from_batches([batch]).select(CANONICAL_TO_IMPORT_COLUMNS)
    out: dict[str, pa.RecordBatch] = {}
    for namespace in table.column("namespace").unique().to_pylist():
        subset = table.filter(pa.compute.equal(table.column("namespace"), namespace))
        if subset.column("dense").null_count:
            raise ValueError(f"namespace {namespace!r} has records without a dense vector")
        projected = pa.table(
            {
                "id": subset.column("id"),
                "values": subset.column("dense"),
                "metadata": subset.column("metadata"),
            },
            schema=IMPORT_SCHEMA,
        )
        out[namespace] = projected.combine_chunks().to_batches()[0]
    return out


class VectorParquetEmitter:
    """Writes import Parquet for one batch, one rolling writer per namespace."""

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        import_prefix: str,
        *,
        limits: ImportLimits | None = None,
        events: EventBus | None = None,
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = import_prefix.rstrip("/")
        self._limits = limits or ImportLimits()
        self._events = events

    def batch_uri(self, batch: ImportBatch) -> str:
        """The import URI for *batch*: the directory whose subdirectories are namespaces."""
        return f"s3://{self._bucket}/{self._prefix}/{batch.id}/"

    def emit(self, batch: ImportBatch, inputs: Iterable[NamespaceInput]) -> EmitResult:
        """Read canonical parts for every namespace in *batch* and write import parts."""
        result = EmitResult(batch=batch, uri=self.batch_uri(batch))
        wanted = set(batch.namespaces)
        for item in inputs:
            if item.namespace not in wanted:
                continue
            writer = RollingParquetWriter(
                self._s3,
                self._bucket,
                f"{self._prefix}/{batch.id}/{item.namespace}",
                IMPORT_SCHEMA,
                target_part_bytes=self._limits.target_part_bytes,
            )
            rows = 0
            try:
                for key in item.keys:
                    for canonical in read_parquet_batches(
                        self._s3, self._bucket, key, columns=CANONICAL_TO_IMPORT_COLUMNS
                    ):
                        for namespace, projected in canonical_batch_to_import(canonical).items():
                            if namespace != item.namespace:
                                raise ValueError(
                                    f"canonical part {key} holds namespace {namespace!r}, "
                                    f"expected {item.namespace!r}"
                                )
                            writer.write_batch(projected)
                            rows += projected.num_rows
            except Exception:
                writer.abort()
                raise
            parts = writer.close()
            result.parts.extend(parts)
            result.rows_by_namespace[item.namespace] = rows
            if self._events is not None:
                for part in parts:
                    self._events.emit(
                        PartDone(stage=Stage.EMIT, key=part.key, rows=part.rows, bytes=part.bytes)
                    )
        return result
