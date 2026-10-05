"""Canonical Parquet schema and streaming writer shared by sources and the Glue job (ADR-004)."""

from __future__ import annotations

import contextlib
import io
import json
import struct
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

import pyarrow as pa
import pyarrow.parquet as pq
import xxhash
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

MiB = 1024 * 1024
MULTIPART_CHUNK_BYTES = 8 * MiB

CANONICAL_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("namespace", pa.string(), nullable=False),
        pa.field("dense", pa.list_(pa.float32())),
        pa.field(
            "sparse",
            pa.struct([("indices", pa.list_(pa.int64())), ("values", pa.list_(pa.float32()))]),
        ),
        pa.field("text_fields", pa.map_(pa.string(), pa.string())),
        pa.field("metadata", pa.string()),
        pa.field("source_pk", pa.string()),
        pa.field("vector_hash", pa.uint64()),
    ]
)


def vector_hash(values: Sequence[float]) -> int:
    """xxhash64 of the vector as little-endian float32 bytes."""
    return xxhash.xxh64_intdigest(struct.pack(f"<{len(values)}f", *values))


def dumps_metadata(metadata: dict[str, Any]) -> str:
    """Metadata as compact JSON with sorted keys, so equal dicts hash equal."""
    return json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(slots=True)
class SparseVector:
    """Sparse indices and values, same length."""

    indices: list[int]
    values: list[float]

    def __post_init__(self) -> None:
        if len(self.indices) != len(self.values):
            raise ValueError("sparse indices and values differ in length")


@dataclass(slots=True)
class CanonicalRecord:
    """One record in the canonical zone. ``vector_hash`` is derived from ``dense`` when unset."""

    id: str
    namespace: str
    source_pk: str
    dense: list[float] | None = None
    sparse: SparseVector | None = None
    text_fields: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    vector_hash: int | None = None

    def __post_init__(self) -> None:
        if self.vector_hash is None and self.dense is not None:
            self.vector_hash = vector_hash(self.dense)


def records_to_batch(records: Sequence[CanonicalRecord]) -> pa.RecordBatch:
    """Build an Arrow batch in the canonical schema."""
    columns: dict[str, list[Any]] = {
        "id": [r.id for r in records],
        "namespace": [r.namespace for r in records],
        "dense": [r.dense for r in records],
        "sparse": [
            None if r.sparse is None else {"indices": r.sparse.indices, "values": r.sparse.values}
            for r in records
        ],
        "text_fields": [list(r.text_fields.items()) for r in records],
        "metadata": [dumps_metadata(r.metadata) for r in records],
        "source_pk": [r.source_pk for r in records],
        "vector_hash": [r.vector_hash for r in records],
    }
    return pa.RecordBatch.from_pydict(columns, schema=CANONICAL_SCHEMA)


def batch_to_records(batch: pa.RecordBatch) -> Iterator[CanonicalRecord]:
    """Rebuild records from a canonical batch."""
    for row in batch.to_pylist():
        sparse = row["sparse"]
        yield CanonicalRecord(
            id=row["id"],
            namespace=row["namespace"],
            source_pk=row["source_pk"],
            dense=row["dense"],
            sparse=None if sparse is None else SparseVector(sparse["indices"], sparse["values"]),
            text_fields=dict(row["text_fields"] or []),
            metadata=json.loads(row["metadata"]) if row["metadata"] else {},
            vector_hash=row["vector_hash"],
        )


@dataclass(frozen=True, slots=True)
class PartInfo:
    """One uploaded file."""

    key: str
    rows: int
    bytes: int


class MultipartUploadStream(io.RawIOBase):
    """A write-only file object that streams into one S3 object with multipart upload.

    Bytes are buffered until a chunk is full, then uploaded as a part. ``close()`` completes
    the upload; an upload that received no bytes is aborted and replaced by an empty object.
    """

    def __init__(
        self, s3: S3Client, bucket: str, key: str, *, chunk_bytes: int = MULTIPART_CHUNK_BYTES
    ) -> None:
        super().__init__()
        if chunk_bytes < 5 * MiB:
            raise ValueError("S3 multipart parts must be at least 5 MiB")
        self._s3 = s3
        self._bucket = bucket
        self._key = key
        self._chunk_bytes = chunk_bytes
        self._buffer = bytearray()
        self._parts: list[dict[str, Any]] = []
        self._upload_id: str | None = None
        self._bytes_written = 0

    @property
    def key(self) -> str:
        """Destination object key."""
        return self._key

    @property
    def bytes_written(self) -> int:
        """Bytes accepted so far, uploaded or buffered."""
        return self._bytes_written

    def writable(self) -> bool:
        """Always true."""
        return True

    def write(self, data: Any) -> int:
        """Buffer *data*, uploading full chunks as parts."""
        view = memoryview(data)
        self._buffer.extend(view)
        self._bytes_written += len(view)
        while len(self._buffer) >= self._chunk_bytes:
            self._upload_part(bytes(self._buffer[: self._chunk_bytes]))
            del self._buffer[: self._chunk_bytes]
        return len(view)

    def _upload_part(self, body: bytes) -> None:
        if self._upload_id is None:
            created = self._s3.create_multipart_upload(Bucket=self._bucket, Key=self._key)
            self._upload_id = created["UploadId"]
        number = len(self._parts) + 1
        response = self._s3.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            PartNumber=number,
            Body=body,
        )
        self._parts.append({"ETag": response["ETag"], "PartNumber": number})

    def abort(self) -> None:
        """Abandon the upload, leaving no object behind."""
        if self._upload_id is not None:
            with contextlib.suppress(ClientError):
                self._s3.abort_multipart_upload(
                    Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
                )
            self._upload_id = None
        self._buffer.clear()
        super().close()

    def close(self) -> None:
        """Flush the tail and complete the upload."""
        if self.closed:
            return
        try:
            if self._upload_id is None:
                self._s3.put_object(Bucket=self._bucket, Key=self._key, Body=bytes(self._buffer))
            else:
                if self._buffer:
                    self._upload_part(bytes(self._buffer))
                self._s3.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._upload_id,
                    MultipartUpload={"Parts": self._parts},  # type: ignore[typeddict-item]
                )
        except Exception:
            self.abort()
            raise
        finally:
            self._buffer.clear()
            super().close()


@dataclass(frozen=True, slots=True)
class Reject:
    """One record that will not import, and why."""

    id: str
    namespace: str
    source_pk: str
    reason: str

    def to_json(self) -> str:
        """One JSON line."""
        return json.dumps(
            {
                "id": self.id,
                "namespace": self.namespace,
                "source_pk": self.source_pk,
                "reason": self.reason,
            },
            ensure_ascii=False,
        )


class JsonLinesPartWriter:
    """Rolling JSON-lines parts, for rejects and duplicates."""

    def __init__(
        self, s3: S3Client, bucket: str, prefix: str, *, target_bytes: int = 64 * 1024 * 1024
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._target = target_bytes
        self._stream: MultipartUploadStream | None = None
        self.keys: list[str] = []
        self.rows = 0

    def write(self, line: str) -> None:
        """Append one line."""
        if self._stream is None:
            key = f"{self._prefix}/part-{len(self.keys):05d}.jsonl"
            self._stream = MultipartUploadStream(self._s3, self._bucket, key)
            self.keys.append(key)
        self._stream.write((line + "\n").encode("utf-8"))
        self.rows += 1
        if self._stream.bytes_written >= self._target:
            self._stream.close()
            self._stream = None

    def close(self) -> list[str]:
        """Finish the open part."""
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        return list(self.keys)


class RollingParquetWriter:
    """Writes Arrow batches to numbered Parquet parts, rolling at a target size.

    Used by the canonical writer and by the import emitters, which differ only in schema and
    destination prefix.
    """

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        prefix: str,
        schema: pa.Schema,
        *,
        target_part_bytes: int,
        batch_rows: int = 1024,
        part_name: str = "part-{index:05d}.parquet",
        compression: str = "zstd",
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._schema = schema
        self._target = target_part_bytes
        self._batch_rows = batch_rows
        self._part_name = part_name
        self._compression = compression
        self._parts: list[PartInfo] = []
        self._stream: MultipartUploadStream | None = None
        self._writer: pq.ParquetWriter | None = None
        self._part_rows = 0
        self._closed = False

    @property
    def parts(self) -> list[PartInfo]:
        """Parts completed so far."""
        return list(self._parts)

    @property
    def batch_rows(self) -> int:
        """Rows per Arrow batch callers should aim for."""
        return self._batch_rows

    def _open_part(self) -> None:
        key = f"{self._prefix}/{self._part_name.format(index=len(self._parts))}"
        self._stream = MultipartUploadStream(self._s3, self._bucket, key)
        self._writer = pq.ParquetWriter(
            pa.PythonFile(self._stream, mode="w"), self._schema, compression=self._compression
        )
        self._part_rows = 0

    def _close_part(self) -> None:
        if self._writer is None or self._stream is None:
            return
        self._writer.close()
        self._stream.close()
        self._parts.append(
            PartInfo(key=self._stream.key, rows=self._part_rows, bytes=self._stream.bytes_written)
        )
        self._writer = None
        self._stream = None

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Append one batch, rolling to a new part once the target size is reached."""
        if self._closed:
            raise RuntimeError("writer is closed")
        if batch.num_rows == 0:
            return
        if self._writer is None:
            self._open_part()
        assert self._writer is not None and self._stream is not None
        self._writer.write_batch(batch)
        self._part_rows += batch.num_rows
        if self._stream.bytes_written >= self._target:
            self._close_part()

    def abort(self) -> None:
        """Drop the open part without completing it."""
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
            self._writer = None
        if self._stream is not None:
            self._stream.abort()
            self._stream = None
        self._closed = True

    def close(self) -> list[PartInfo]:
        """Finish the open part and return every part written."""
        if not self._closed:
            self._close_part()
            self._closed = True
        return self.parts


class CanonicalWriter:
    """Streams canonical records into rolling Parquet parts under one S3 prefix."""

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        prefix: str,
        *,
        target_part_bytes: int = 256 * MiB,
        batch_rows: int = 1024,
    ) -> None:
        self._writer = RollingParquetWriter(
            s3,
            bucket,
            prefix,
            CANONICAL_SCHEMA,
            target_part_bytes=target_part_bytes,
            batch_rows=batch_rows,
        )
        self._pending: list[CanonicalRecord] = []
        self._rows = 0

    @property
    def rows(self) -> int:
        """Records accepted so far."""
        return self._rows

    def write(self, record: CanonicalRecord) -> None:
        """Accept one record."""
        self._pending.append(record)
        self._rows += 1
        if len(self._pending) >= self._writer.batch_rows:
            self.flush()

    def flush(self) -> None:
        """Push buffered records into the current part."""
        if self._pending:
            self._writer.write_batch(records_to_batch(self._pending))
            self._pending.clear()

    def abort(self) -> None:
        """Discard buffered records and the open part."""
        self._pending.clear()
        self._writer.abort()

    def close(self) -> list[PartInfo]:
        """Flush and finish; returns the parts written."""
        self.flush()
        return self._writer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if exc_type is not None:
            self.abort()
        else:
            self.close()


def read_parquet_batches(
    s3: S3Client, bucket: str, key: str, *, batch_rows: int = 4096, columns: list[str] | None = None
) -> Iterator[pa.RecordBatch]:
    """Stream the batches of one Parquet object."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    parquet = pq.ParquetFile(pa.BufferReader(body))
    yield from parquet.iter_batches(batch_size=batch_rows, columns=columns)


def read_canonical(s3: S3Client, bucket: str, keys: Sequence[str]) -> Iterator[CanonicalRecord]:
    """Iterate every record across canonical parts, in key order."""
    for key in keys:
        for batch in read_parquet_batches(s3, bucket, key):
            yield from batch_to_records(batch)
