import pyarrow as pa
import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.core.canonical import CanonicalRecord, CanonicalWriter, read_parquet_batches
from pcmigrate.targets.emitters import (
    ImportLimits,
    ImportPlanError,
    NamespaceInput,
    VectorParquetEmitter,
    plan_import_batches,
)
from pcmigrate.targets.emitters.vector_parquet import IMPORT_SCHEMA
from tests.unit.conftest import BUCKET

MiB = 1024 * 1024
GiB = 1024 * MiB
TiB = 1024 * GiB
LIMITS = ImportLimits()


def test_limits_enforce_adr_012_target_window() -> None:
    with pytest.raises(ValueError):
        ImportLimits(target_part_bytes=100 * MiB)
    ImportLimits(target_part_bytes=512 * MiB)
    ImportLimits(target_part_bytes=1 * GiB)


def test_single_small_batch() -> None:
    inputs = [NamespaceInput("a", ("k1",), 10 * MiB), NamespaceInput("b", ("k2",), 20 * MiB)]
    (batch,) = plan_import_batches(inputs, LIMITS)
    assert batch.id == "batch-00000" and batch.namespaces == ("a", "b")
    assert batch.estimated_files == 2


def test_on_demand_byte_cap_splits_batches() -> None:
    inputs = [NamespaceInput(f"ns{i}", ("k",), 600 * GiB) for i in range(3)]
    batches = plan_import_batches(inputs, LIMITS)
    assert len(batches) == 3
    assert all(b.estimated_bytes <= TiB for b in batches)
    dedicated = plan_import_batches(inputs, LIMITS, capacity="dedicated")
    assert len(dedicated) == 1


def test_file_cap_splits_batches() -> None:
    limits = ImportLimits(max_files_per_import=15)
    inputs = [NamespaceInput(f"ns{i}", ("k",), 4 * GiB) for i in range(3)]
    batches = plan_import_batches(inputs, limits)
    assert len(batches) == 2
    assert sum(b.estimated_files for b in batches) == 3 * 7


def test_namespace_cap_splits_batches() -> None:
    limits = ImportLimits(max_namespaces_per_import=2)
    inputs = [NamespaceInput(f"ns{i}", ("k",), MiB) for i in range(5)]
    assert len(plan_import_batches(inputs, limits)) == 3


def test_oversized_namespace_is_an_error() -> None:
    with pytest.raises(ImportPlanError):
        plan_import_batches([NamespaceInput("huge", ("k",), 2 * TiB)], LIMITS)


def _seed_canonical(s3: S3Client, namespace: str, rows: int, prefix: str) -> list[str]:
    writer = CanonicalWriter(s3, BUCKET, prefix, target_part_bytes=30_000, batch_rows=64)
    for i in range(rows):
        writer.write(
            CanonicalRecord(
                id=f"{namespace}#{i}",
                namespace=namespace,
                source_pk=str(i),
                dense=[float(i)] * 8,
                metadata={"i": i},
            )
        )
    return [p.key for p in writer.close()]


def test_emitter_writes_import_schema_per_namespace(s3: S3Client) -> None:
    keys_a = _seed_canonical(s3, "a", 500, "mig/canonical/ta/p0")
    keys_b = _seed_canonical(s3, "b", 120, "mig/canonical/tb/p0")
    inputs = [NamespaceInput("a", tuple(keys_a), 1), NamespaceInput("b", tuple(keys_b), 1)]
    (batch,) = plan_import_batches(inputs, LIMITS)
    emitter = VectorParquetEmitter(s3, BUCKET, "mig/import", limits=LIMITS)
    result = emitter.emit(batch, inputs)
    assert result.uri == f"s3://{BUCKET}/mig/import/batch-00000/"
    assert result.rows_by_namespace == {"a": 500, "b": 120}
    keys = [p.key for p in result.parts]
    assert all(k.startswith("mig/import/batch-00000/") for k in keys)
    assert {k.split("/")[3] for k in keys} == {"a", "b"}
    batches = list(read_parquet_batches(s3, BUCKET, keys[0]))
    assert batches[0].schema.equals(IMPORT_SCHEMA)
    assert batches[0].column("values").type == pa.list_(pa.float32())
    assert sum(b.num_rows for k in keys for b in read_parquet_batches(s3, BUCKET, k)) == 620


def test_emitter_rejects_records_without_dense(s3: S3Client) -> None:
    writer = CanonicalWriter(s3, BUCKET, "mig/canonical/tn/p0")
    writer.write(CanonicalRecord(id="n#1", namespace="n", source_pk="1"))
    keys = [p.key for p in writer.close()]
    inputs = [NamespaceInput("n", tuple(keys), 1)]
    (batch,) = plan_import_batches(inputs, LIMITS)
    with pytest.raises(ValueError, match="without a dense vector"):
        VectorParquetEmitter(s3, BUCKET, "mig/import", limits=LIMITS).emit(batch, inputs)
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="mig/import/").get("KeyCount", 0) == 0
