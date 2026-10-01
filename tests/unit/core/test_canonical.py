import struct

import pyarrow as pa
import pytest
import xxhash
from mypy_boto3_s3 import S3Client

from pcmigrate.core.canonical import (
    CANONICAL_SCHEMA,
    CanonicalRecord,
    CanonicalWriter,
    MultipartUploadStream,
    SparseVector,
    batch_to_records,
    read_canonical,
    read_parquet_batches,
    records_to_batch,
    vector_hash,
)
from tests.unit.conftest import BUCKET


def test_vector_hash_is_xxhash64_of_float32_bytes() -> None:
    values = [0.1, -0.2, 0.3]
    expected = xxhash.xxh64_intdigest(struct.pack("<3f", *values))
    assert vector_hash(values) == expected
    assert vector_hash(values) != vector_hash([0.1, -0.2, 0.30001])


def test_record_derives_hash_and_round_trips() -> None:
    rec = CanonicalRecord(
        id="a#1",
        namespace="a",
        source_pk="1",
        dense=[1.0, 2.0],
        sparse=SparseVector([3, 9], [0.5, 0.25]),
        text_fields={"title": "hello"},
        metadata={"k": "v", "n": 2, "b": True},
    )
    assert rec.vector_hash == vector_hash([1.0, 2.0])
    batch = records_to_batch([rec, CanonicalRecord(id="a#2", namespace="a", source_pk="2")])
    assert batch.schema.equals(CANONICAL_SCHEMA)
    back = list(batch_to_records(batch))
    assert back[0] == rec
    assert back[1].dense is None and back[1].sparse is None and back[1].vector_hash is None


def test_sparse_lengths_must_match() -> None:
    with pytest.raises(ValueError):
        SparseVector([1], [0.1, 0.2])


def test_multipart_stream_small_object_uses_put(s3: S3Client) -> None:
    stream = MultipartUploadStream(s3, BUCKET, "k/small.bin")
    stream.write(b"hello")
    stream.close()
    assert s3.get_object(Bucket=BUCKET, Key="k/small.bin")["Body"].read() == b"hello"
    assert stream.bytes_written == 5


def test_multipart_stream_rolls_parts(s3: S3Client) -> None:
    chunk = 5 * 1024 * 1024
    stream = MultipartUploadStream(s3, BUCKET, "k/big.bin", chunk_bytes=chunk)
    payload = b"x" * (chunk + 10)
    stream.write(payload[: chunk // 2])
    stream.write(payload[chunk // 2 :])
    stream.close()
    head = s3.head_object(Bucket=BUCKET, Key="k/big.bin")
    assert head["ContentLength"] == len(payload)


def test_multipart_stream_abort_leaves_nothing(s3: S3Client) -> None:
    chunk = 5 * 1024 * 1024
    stream = MultipartUploadStream(s3, BUCKET, "k/aborted.bin", chunk_bytes=chunk)
    stream.write(b"y" * (chunk + 1))
    stream.abort()
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="k/aborted").get("KeyCount", 0) == 0
    assert not s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads")


def _records(n: int, dim: int = 16) -> list[CanonicalRecord]:
    return [
        CanonicalRecord(
            id=f"ns#{i}",
            namespace="ns",
            source_pk=str(i),
            dense=[float(i)] * dim,
            metadata={"i": i},
        )
        for i in range(n)
    ]


def test_canonical_writer_rolls_at_target_size(s3: S3Client) -> None:
    writer = CanonicalWriter(
        s3, BUCKET, "mig/canonical/t/p0", target_part_bytes=8 * 1024 * 1024, batch_rows=100
    )
    writer_small = CanonicalWriter(
        s3, BUCKET, "mig/canonical/t/p1", target_part_bytes=20_000, batch_rows=100
    )
    for rec in _records(1000):
        writer.write(rec)
        writer_small.write(rec)
    parts = writer.close()
    small_parts = writer_small.close()
    assert len(parts) == 1 and parts[0].rows == 1000
    assert len(small_parts) > 1
    assert sum(p.rows for p in small_parts) == 1000
    assert [p.key for p in small_parts] == sorted(p.key for p in small_parts)
    back = list(read_canonical(s3, BUCKET, [p.key for p in small_parts]))
    assert [r.id for r in back] == [f"ns#{i}" for i in range(1000)]
    assert back[7].metadata == {"i": 7}


def test_canonical_writer_context_manager_aborts_on_error(s3: S3Client) -> None:
    with pytest.raises(RuntimeError), CanonicalWriter(s3, BUCKET, "mig/canonical/t/p2") as w:
        for rec in _records(10):
            w.write(rec)
        w.flush()
        raise RuntimeError("source died")
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="mig/canonical/t/p2").get("KeyCount", 0) == 0


def test_read_parquet_batches_projects_columns(s3: S3Client) -> None:
    writer = CanonicalWriter(s3, BUCKET, "mig/canonical/t/p3")
    for rec in _records(5):
        writer.write(rec)
    (part,) = writer.close()
    batches = list(read_parquet_batches(s3, BUCKET, part.key, columns=["id", "dense"]))
    assert batches[0].schema.names == ["id", "dense"]
    assert batches[0].column("dense").type == pa.list_(pa.float32())
