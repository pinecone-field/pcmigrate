"""Pre-validation of canonical records against the target's rules (ADR-011).

Every violation becomes a reject with a reason. Duplicate vectors within a namespace keep
their first occurrence and route the rest to manifest/duplicates/ for a post-import upsert,
or to rejects/ when the mapping says to drop them.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import DenseSpec, MetadataType, ObjectMapping
from pcmigrate.core.canonical import (
    CanonicalRecord,
    CanonicalWriter,
    JsonLinesPartWriter,
    Reject,
    dumps_metadata,
    read_canonical,
)
from pcmigrate.report import limits

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

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
        }


def record_to_json(record: CanonicalRecord) -> str:
    """A duplicate record as one JSON line, enough to upsert it later."""
    return json.dumps(
        {
            "id": record.id,
            "namespace": record.namespace,
            "source_pk": record.source_pk,
            "values": record.dense,
            "metadata": record.metadata,
            "vector_hash": record.vector_hash,
        },
        ensure_ascii=False,
    )


def json_to_record(line: str) -> CanonicalRecord:
    """Inverse of record_to_json."""
    data = json.loads(line)
    return CanonicalRecord(
        id=data["id"],
        namespace=data["namespace"],
        source_pk=data["source_pk"],
        dense=data["values"],
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
) -> NamespaceTransformResult:
    """Validate every record of one namespace, splitting clean, rejects, and duplicates."""
    validator = RecordValidator(obj)
    result = NamespaceTransformResult(namespace=namespace)
    clean = CanonicalWriter(s3, bucket, clean_prefix, target_part_bytes=target_part_bytes)
    rejects = JsonLinesPartWriter(s3, bucket, rejects_prefix)
    duplicates = JsonLinesPartWriter(s3, bucket, duplicates_prefix)
    seen_hashes: set[int] = set()
    try:
        for record in read_canonical(s3, bucket, canonical_keys):
            problems = validator.violations(record)
            if problems:
                reason = "; ".join(problems)
                rejects.write(
                    Reject(record.id, record.namespace, record.source_pk, reason).to_json()
                )
                result.rejected_rows += 1
                for problem in problems:
                    key = (
                        problem.split(" ", 2)[0]
                        if problem.startswith("metadata")
                        else problem.split(":")[0]
                    )
                    result.reject_reasons[key] = result.reject_reasons.get(key, 0) + 1
                continue
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
                    continue
                seen_hashes.add(record.vector_hash)
            clean.write(record)
            result.clean_rows += 1
    except Exception:
        clean.abort()
        raise
    result.clean_parts = [p.key for p in clean.close()]
    result.reject_parts = rejects.close()
    result.duplicate_parts = duplicates.close()
    return result


def read_duplicates(s3: S3Client, bucket: str, keys: Sequence[str]) -> Iterator[CanonicalRecord]:
    """Stream duplicate records back from manifest/duplicates/."""
    for key in keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if line.strip():
                yield json_to_record(line)
