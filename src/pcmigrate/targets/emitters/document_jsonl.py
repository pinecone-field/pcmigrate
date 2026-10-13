"""DocumentIndexEmitter: canonical records to document-index import JSONL.gz (ADR-004, ADR-012).

Each line is one document as the Documents API upserts it: ``_id``, the declared dense and
sparse fields, the full-text string fields (ADR-018), and every metadata key as its own
filterable field. Files land as
``import/<batch>/<namespace>/part-N.jsonl.gz`` and roll at ``target_part_bytes`` of
compressed output; the planner counts them at ten times their compressed size.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pcmigrate.core.canonical import (
    CanonicalRecord,
    MultipartUploadStream,
    PartInfo,
    read_parquet_batches,
)
from pcmigrate.core.canonical import batch_to_records as _batch_to_records
from pcmigrate.core.events import EventBus, PartDone
from pcmigrate.core.state import Stage
from pcmigrate.targets.emitters.vector_parquet import (
    EmitResult,
    ImportBatch,
    ImportLimits,
    NamespaceInput,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

JSONL_GZ_MULTIPLIER = 10


@dataclass(frozen=True, slots=True)
class DocumentFields:
    """The schema field names documents are written with.

    ``text`` maps canonical text-field keys (source columns) to full-text schema field
    names; text fields the mapping does not name are not written.
    """

    dense: str = "dense"
    sparse: str = "sparse"
    text: Mapping[str, str] = field(default_factory=dict)


def document_line(record: CanonicalRecord, fields: DocumentFields) -> dict[str, Any]:
    """One canonical record as a Documents API document."""
    doc: dict[str, Any] = {"_id": record.id}
    if record.dense is not None:
        doc[fields.dense] = record.dense
    if record.sparse is not None:
        doc[fields.sparse] = {"indices": record.sparse.indices, "values": record.sparse.values}
    for source, target in fields.text.items():
        value = record.text_fields.get(source)
        if value:
            doc[target] = value
    for key, value in record.metadata.items():
        doc[key] = value
    return doc


@dataclass
class RollingJsonlGzWriter:
    """Gzip-compressed JSON lines, rolling to a new object when the compressed size is reached."""

    s3: Any
    bucket: str
    prefix: str
    target_part_bytes: int
    parts: list[PartInfo] = field(default_factory=list)
    _stream: MultipartUploadStream | None = None
    _gzip: gzip.GzipFile | None = None
    _rows: int = 0

    def _open(self) -> None:
        key = f"{self.prefix.rstrip('/')}/part-{len(self.parts):05d}.jsonl.gz"
        self._stream = MultipartUploadStream(self.s3, self.bucket, key)
        self._gzip = gzip.GzipFile(fileobj=self._stream, mode="wb", compresslevel=6)
        self._rows = 0

    def write(self, doc: dict[str, Any]) -> None:
        """Append one document."""
        if self._gzip is None:
            self._open()
        assert self._gzip is not None and self._stream is not None
        self._gzip.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode())
        self._gzip.write(b"\n")
        self._rows += 1
        if self._stream.bytes_written >= self.target_part_bytes:
            self._close_part()

    def _close_part(self) -> None:
        if self._gzip is None or self._stream is None:
            return
        self._gzip.close()
        self._stream.close()
        self.parts.append(
            PartInfo(key=self._stream.key, rows=self._rows, bytes=self._stream.bytes_written)
        )
        self._gzip = None
        self._stream = None

    def abort(self) -> None:
        """Drop the open part."""
        if self._gzip is not None:
            self._gzip.close()
            self._gzip = None
        if self._stream is not None:
            self._stream.abort()
            self._stream = None

    def close(self) -> list[PartInfo]:
        """Finish the open part and return every part written."""
        self._close_part()
        return list(self.parts)


class DocumentIndexEmitter:
    """Writes import JSONL.gz for one batch, one rolling writer per namespace."""

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        import_prefix: str,
        *,
        fields: DocumentFields | None = None,
        fields_by_namespace: Mapping[str, DocumentFields] | None = None,
        limits: ImportLimits | None = None,
        events: EventBus | None = None,
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = import_prefix.rstrip("/")
        self._fields = fields or DocumentFields()
        self._by_namespace = dict(fields_by_namespace or {})
        self._limits = limits or ImportLimits()
        self._events = events

    def fields_for(self, namespace: str) -> DocumentFields:
        """The field names for one namespace (``namespace_by`` suffixes share the base)."""
        return self._by_namespace.get(namespace.split(":", 1)[0], self._fields)

    def batch_uri(self, batch: ImportBatch) -> str:
        """The import URI for *batch*."""
        return f"s3://{self._bucket}/{self._prefix}/{batch.id}/"

    def emit(self, batch: ImportBatch, inputs: Iterable[NamespaceInput]) -> EmitResult:
        """Read canonical parts for every namespace in *batch* and write JSONL.gz parts."""
        result = EmitResult(batch=batch, uri=self.batch_uri(batch))
        wanted = set(batch.namespaces)
        for item in inputs:
            if item.namespace not in wanted:
                continue
            writer = RollingJsonlGzWriter(
                self._s3,
                self._bucket,
                f"{self._prefix}/{batch.id}/{item.namespace}",
                self._limits.target_part_bytes // JSONL_GZ_MULTIPLIER,
            )
            rows = 0
            fields = self.fields_for(item.namespace)
            try:
                for key in item.keys:
                    for canonical in read_parquet_batches(self._s3, self._bucket, key):
                        for record in _batch_to_records(canonical):
                            if record.namespace != item.namespace:
                                raise ValueError(
                                    f"canonical part {key} holds namespace {record.namespace!r}, "
                                    f"expected {item.namespace!r}"
                                )
                            writer.write(document_line(record, fields))
                            rows += 1
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
