"""Pre-validation of canonical records against the target's rules (ADR-011).

Every violation becomes a reject with a reason. Duplicate vectors within a namespace keep
their first occurrence and route the rest to manifest/duplicates/ for a post-import upsert,
or to rejects/ when the mapping says to drop them.
"""

from __future__ import annotations

import io
import json
import math
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from pcmigrate.config.mapping import DenseSpec, MetadataType, ObjectMapping, SparseSpec
from pcmigrate.core.canonical import (
    CanonicalRecord,
    CanonicalWriter,
    JsonLinesPartWriter,
    Reject,
    SparseVector,
    batch_to_records,
    dumps_metadata,
    read_canonical,
    read_parquet_batches,
)
from pcmigrate.core.s3 import is_not_found
from pcmigrate.core.sparse import concat_text, text_hash
from pcmigrate.report import limits

if TYPE_CHECKING:
    from botocore.exceptions import ClientError
    from mypy_boto3_s3 import S3Client

    from pcmigrate.targets.sparse_encoder import SparseEncoder

NO_TEXT = "no text for sparse generation"
MISSING_SPARSE = "missing sparse vector"

SCALAR_TYPES = (str, int, float, bool)


def _expected_type(kind: MetadataType) -> tuple[type, ...]:
    if kind is MetadataType.STRING:
        return (str,)
    if kind in (MetadataType.INTEGER, MetadataType.EPOCH_SECONDS):
        return (int,)
    if kind is MetadataType.FLOAT:
        return (int, float)
    if kind is MetadataType.BOOLEAN:
        return (bool,)
    return (list,)


class RecordValidator:
    """Checks one record against dimension, metadata types, reserved names, and size limits."""

    def __init__(self, obj: ObjectMapping) -> None:
        self._dense: DenseSpec | None = obj.dense
        self._sparse = obj.sparse is not None
        self._types = {
            (f.target or f.source): f.type for f in obj.metadata if f.action == "map" and f.type
        }

    def violations(self, record: CanonicalRecord) -> list[str]:
        """Every rule the record breaks; empty when it is clean."""
        problems: list[str] = []
        if not record.id:
            problems.append("empty id")
        elif len(record.id) > limits.RECORD_ID_MAX_CHARS:
            problems.append(f"id longer than {limits.RECORD_ID_MAX_CHARS} characters")
        if record.dense is None:
            problems.append("missing dense vector")
        else:
            if self._dense is not None and len(record.dense) != self._dense.dimension:
                problems.append(f"dimension {len(record.dense)} != {self._dense.dimension}")
            if any(math.isnan(v) or math.isinf(v) for v in record.dense):
                problems.append("vector contains NaN or infinity")
        if self._sparse and record.sparse is None:
            problems.append(MISSING_SPARSE)
        if record.sparse is not None:
            if not record.sparse.indices:
                problems.append("sparse vector is empty")
            if len(record.sparse.indices) > limits.SPARSE_MAX_NONZERO:
                problems.append(
                    f"sparse vector has {len(record.sparse.indices)} non-zero values, "
                    f"over the {limits.SPARSE_MAX_NONZERO} limit"
                )
            if any(i < 0 or i > limits.SPARSE_INDEX_MAX for i in record.sparse.indices):
                problems.append("sparse index outside the uint32 range")
            if len(set(record.sparse.indices)) != len(record.sparse.indices):
                problems.append("sparse vector repeats an index")
            if any(math.isnan(v) or math.isinf(v) for v in record.sparse.values):
                problems.append("sparse vector contains NaN or infinity")
        for key, value in record.metadata.items():
            if key.startswith(limits.RESERVED_FIELD_PREFIXES):
                problems.append(f"metadata field {key!r} starts with a reserved prefix")
            if len(key.encode()) > limits.FIELD_NAME_MAX_BYTES:
                problems.append(
                    f"metadata field {key!r} name exceeds {limits.FIELD_NAME_MAX_BYTES} bytes"
                )
            if isinstance(value, SCALAR_TYPES):
                pass
            elif isinstance(value, list):
                if not all(isinstance(v, str) for v in value):
                    problems.append(f"metadata field {key!r} is a non-string list")
            else:
                problems.append(
                    f"metadata field {key!r} has non-scalar value {type(value).__name__}"
                )
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                problems.append(f"metadata field {key!r} is NaN or infinity")
            expected = self._types.get(key)
            if expected is not None and not isinstance(value, _expected_type(expected)):
                actual = type(value).__name__
                problems.append(
                    f"metadata field {key!r} is {actual}, mapping says {expected.value}"
                )
            if (
                expected is not None
                and expected is not MetadataType.BOOLEAN
                and isinstance(value, bool)
            ):
                problems.append(
                    f"metadata field {key!r} is a boolean, mapping says {expected.value}"
                )
        size = len(dumps_metadata(record.metadata).encode("utf-8"))
        if size > limits.METADATA_BYTES_PER_RECORD:
            problems.append(
                f"metadata is {size} bytes, over the {limits.METADATA_BYTES_PER_RECORD}-byte limit"
            )
        return problems


@dataclass
class SparseStats:
    """What generating sparse vectors cost (ADR-017)."""

    generated: int = 0
    cached: int = 0
    no_text: int = 0
    tokens: int = 0
    requests: int = 0

    def to_dict(self) -> dict[str, int]:
        """JSON-friendly."""
        return {
            "generated": self.generated,
            "cached": self.cached,
            "no_text": self.no_text,
            "tokens": self.tokens,
            "requests": self.requests,
        }

    def since(self, earlier: SparseStats) -> SparseStats:
        """The difference between two snapshots."""
        return SparseStats(
            generated=self.generated - earlier.generated,
            cached=self.cached - earlier.cached,
            no_text=self.no_text - earlier.no_text,
            tokens=self.tokens - earlier.tokens,
            requests=self.requests - earlier.requests,
        )


CacheEntries = dict[str, tuple[int, SparseVector]]
_CACHE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("text_hash", pa.uint64()),
        pa.field("indices", pa.list_(pa.uint32())),
        pa.field("values", pa.list_(pa.float32())),
    ]
)


class SparseCache:
    """Generated sparse vectors per canonical input part, so a rerun does not pay twice.

    Entries are keyed by record id and checked against the hash of the encoded text under
    the model, so a changed text or model misses the cache.
    """

    def __init__(self, s3: S3Client, bucket: str, prefix: str) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    def key(self, part_key: str) -> str:
        """Cache object for one canonical part."""
        stem = part_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return f"{self._prefix}/{stem}.parquet"

    def load(self, part_key: str) -> CacheEntries:
        """Cached entries for the part, empty when none were written."""
        try:
            body = self._s3.get_object(Bucket=self._bucket, Key=self.key(part_key))["Body"].read()
        except Exception as err:
            if _not_found(err):
                return {}
            raise
        table = pq.read_table(pa.BufferReader(body))
        entries: CacheEntries = {}
        for row in table.to_pylist():
            entries[str(row["id"])] = (
                int(row["text_hash"]),
                SparseVector([int(i) for i in row["indices"]], [float(v) for v in row["values"]]),
            )
        return entries

    def store(self, part_key: str, entries: Mapping[str, tuple[int, SparseVector]]) -> None:
        """Write the part's entries, replacing any earlier object."""
        table = pa.table(
            {
                "id": list(entries),
                "text_hash": [h for h, _ in entries.values()],
                "indices": [v.indices for _, v in entries.values()],
                "values": [v.values for _, v in entries.values()],
            },
            schema=_CACHE_SCHEMA,
        )
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        self._s3.put_object(Bucket=self._bucket, Key=self.key(part_key), Body=buffer.getvalue())


def _not_found(err: Exception) -> bool:
    from botocore.exceptions import ClientError

    client_error: ClientError | None = err if isinstance(err, ClientError) else None
    return client_error is not None and is_not_found(client_error)


@dataclass
class SparseEnricher:
    """Encodes an object's text fields into its sparse vector (ADR-017)."""

    spec: SparseSpec
    encoder: SparseEncoder
    stats: SparseStats = field(default_factory=SparseStats)

    @property
    def model(self) -> str:
        """The model name the cache keys include."""
        return self.encoder.model

    def enrich(
        self,
        records: Sequence[CanonicalRecord],
        cached: Mapping[str, tuple[int, SparseVector]] | None = None,
        *,
        remember: CacheEntries | None = None,
    ) -> dict[str, str]:
        """Set every record's sparse vector from its text; return id -> reason for failures.

        Text fields are cleared afterwards: they were carried only to be encoded.
        """
        problems: dict[str, str] = {}
        pending: list[tuple[CanonicalRecord, int]] = []
        texts: list[str] = []
        for record in records:
            text = concat_text(record.text_fields, self.spec.text_fields, self.spec.separator)
            record.text_fields = {}
            if not text:
                problems[record.id] = NO_TEXT
                record.sparse = None
                self.stats.no_text += 1
                continue
            digest = text_hash(self.model, text)
            hit = cached.get(record.id) if cached else None
            if hit is not None and hit[0] == digest:
                record.sparse = hit[1]
                self.stats.cached += 1
                if remember is not None:
                    remember[record.id] = hit
                continue
            pending.append((record, digest))
            texts.append(text)
        if texts:
            usage = self.encoder.usage()
            tokens_before, requests_before = usage.tokens, usage.requests
            vectors = self.encoder.encode(texts, input_type="passage")
            usage = self.encoder.usage()
            self.stats.tokens += usage.tokens - tokens_before
            self.stats.requests += usage.requests - requests_before
            for (record, digest), vector in zip(pending, vectors, strict=True):
                record.sparse = vector
                self.stats.generated += 1
                if remember is not None:
                    remember[record.id] = (digest, vector)
        return problems


@dataclass
class NamespaceTransformResult:
    """Counts and keys for one namespace."""

    namespace: str
    clean_rows: int = 0
    rejected_rows: int = 0
    duplicate_rows: int = 0
    clean_parts: list[str] = field(default_factory=list)
    reject_parts: list[str] = field(default_factory=list)
    duplicate_parts: list[str] = field(default_factory=list)
    reject_reasons: dict[str, int] = field(default_factory=dict)
    sparse: SparseStats | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "clean_rows": self.clean_rows,
            "rejected_rows": self.rejected_rows,
            "duplicate_rows": self.duplicate_rows,
            "clean_parts": self.clean_parts,
            "reject_parts": self.reject_parts,
            "duplicate_parts": self.duplicate_parts,
            "reject_reasons": self.reject_reasons,
            "sparse": None if self.sparse is None else self.sparse.to_dict(),
        }


def record_to_json(record: CanonicalRecord) -> str:
    """A duplicate record as one JSON line, enough to upsert it later."""
    return json.dumps(
        {
            "id": record.id,
            "namespace": record.namespace,
            "source_pk": record.source_pk,
            "values": record.dense,
            "sparse": None
            if record.sparse is None
            else {"indices": record.sparse.indices, "values": record.sparse.values},
            "metadata": record.metadata,
            "text_fields": record.text_fields or None,
            "vector_hash": record.vector_hash,
        },
        ensure_ascii=False,
    )


def json_to_record(line: str) -> CanonicalRecord:
    """Inverse of record_to_json."""
    data = json.loads(line)
    sparse = data.get("sparse")
    return CanonicalRecord(
        id=data["id"],
        namespace=data["namespace"],
        source_pk=data["source_pk"],
        dense=data["values"],
        sparse=SparseVector(sparse["indices"], sparse["values"]) if sparse else None,
        text_fields=data.get("text_fields") or {},
        metadata=data.get("metadata") or {},
        vector_hash=data.get("vector_hash"),
    )


def transform_namespace(
    s3: S3Client,
    bucket: str,
    *,
    namespace: str,
    obj: ObjectMapping,
    canonical_keys: Sequence[str],
    clean_prefix: str,
    rejects_prefix: str,
    duplicates_prefix: str,
    drop_duplicates: bool,
    target_part_bytes: int,
    enricher: SparseEnricher | None = None,
    cache: SparseCache | None = None,
    on_part: Callable[[str, int, SparseStats], None] | None = None,
) -> NamespaceTransformResult:
    """Validate every record of one namespace, splitting clean, rejects, and duplicates.

    With an *enricher*, each input part's records get their sparse vectors generated before
    validation (cached per part so a rerun skips the model), and *on_part* hears about it.
    """
    validator = RecordValidator(obj)
    result = NamespaceTransformResult(namespace=namespace)
    clean = CanonicalWriter(s3, bucket, clean_prefix, target_part_bytes=target_part_bytes)
    rejects = JsonLinesPartWriter(s3, bucket, rejects_prefix)
    duplicates = JsonLinesPartWriter(s3, bucket, duplicates_prefix)
    seen_hashes: set[int] = set()
    try:
        for records, extra_problems in _enriched_batches(
            s3, bucket, canonical_keys, enricher, cache, on_part
        ):
            for record in records:
                _route(
                    record,
                    validator,
                    extra_problems.get(record.id),
                    result=result,
                    clean=clean,
                    rejects=rejects,
                    duplicates=duplicates,
                    seen_hashes=seen_hashes,
                    drop_duplicates=drop_duplicates,
                )
    except Exception:
        clean.abort()
        raise
    result.clean_parts = [p.key for p in clean.close()]
    result.reject_parts = rejects.close()
    result.duplicate_parts = duplicates.close()
    if enricher is not None:
        result.sparse = replace(enricher.stats)
    return result


def _enriched_batches(
    s3: S3Client,
    bucket: str,
    canonical_keys: Sequence[str],
    enricher: SparseEnricher | None,
    cache: SparseCache | None,
    on_part: Callable[[str, int, SparseStats], None] | None,
) -> Iterator[tuple[list[CanonicalRecord], dict[str, str]]]:
    """Yield (records, id -> enrichment problem) one Arrow batch at a time."""
    if enricher is None:
        for record in read_canonical(s3, bucket, canonical_keys):
            yield [record], {}
        return
    for key in canonical_keys:
        cached = cache.load(key) if cache is not None else {}
        remember: CacheEntries = {}
        before = replace(enricher.stats)
        rows = 0
        for batch in read_parquet_batches(s3, bucket, key):
            records = list(batch_to_records(batch))
            rows += len(records)
            problems = enricher.enrich(records, cached, remember=remember)
            yield records, problems
        if cache is not None and remember:
            cache.store(key, remember)
        if on_part is not None:
            on_part(key, rows, enricher.stats.since(before))


def _route(
    record: CanonicalRecord,
    validator: RecordValidator,
    extra_problem: str | None,
    *,
    result: NamespaceTransformResult,
    clean: CanonicalWriter,
    rejects: JsonLinesPartWriter,
    duplicates: JsonLinesPartWriter,
    seen_hashes: set[int],
    drop_duplicates: bool,
) -> None:
    """Send one record to the clean zone, the rejects, or the duplicates."""
    problems = validator.violations(record)
    if extra_problem is not None:
        problems = [extra_problem, *[p for p in problems if p != MISSING_SPARSE]]
    if problems:
        reason = "; ".join(problems)
        rejects.write(Reject(record.id, record.namespace, record.source_pk, reason).to_json())
        result.rejected_rows += 1
        for problem in problems:
            key = (
                problem.split(" ", 2)[0]
                if problem.startswith("metadata")
                else problem.split(":")[0]
            )
            result.reject_reasons[key] = result.reject_reasons.get(key, 0) + 1
        return
    if record.vector_hash is not None:
        if record.vector_hash in seen_hashes:
            if drop_duplicates:
                rejects.write(
                    Reject(
                        record.id, record.namespace, record.source_pk, "duplicate vector"
                    ).to_json()
                )
                result.rejected_rows += 1
                result.reject_reasons["duplicate vector"] = (
                    result.reject_reasons.get("duplicate vector", 0) + 1
                )
            else:
                duplicates.write(record_to_json(record))
                result.duplicate_rows += 1
            return
        seen_hashes.add(record.vector_hash)
    clean.write(record)
    result.clean_rows += 1


def read_duplicates(s3: S3Client, bucket: str, keys: Sequence[str]) -> Iterator[CanonicalRecord]:
    """Stream duplicate records back from manifest/duplicates/."""
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if line.strip():
                yield json_to_record(line)
