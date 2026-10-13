"""Apply the approved mapping to OpenSearch documents; Glue and the runner share this code.

An OSIS raw line is the document ``_source`` plus ``_id`` and ``_index`` copied in from event
metadata. ``DocumentTranslator`` turns one such document into a canonical record through the
same ``RecordBuilder`` the pgvector export uses, and ``transform_lines`` streams a batch of
lines into canonical Parquet parts and reject lines. Both are importable without pyspark,
opensearch-py, or any report code, so the Glue job needs only the pcmigrate wheel.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import Mapping as MappingDoc
from pcmigrate.config.mapping import ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord, CanonicalWriter, JsonLinesPartWriter, Reject
from pcmigrate.sources.pgvector.records import RecordBuilder, RecordBuildError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

DOC_ID_KEY = "_id"
INDEX_KEY = "_index"


class DocumentError(ValueError):
    """A raw line could not be read as an OpenSearch document."""


@dataclass
class DocumentTranslator:
    """Documents of one index to canonical records, per the approved object mapping."""

    obj: ObjectMapping
    builder: RecordBuilder = field(init=False)

    def __post_init__(self) -> None:
        self.builder = RecordBuilder(self.obj, (DOC_ID_KEY,))

    @classmethod
    def for_index(cls, mapping: MappingDoc, index: str) -> DocumentTranslator:
        """The translator for the object whose source is *index*."""
        for obj in mapping.objects:
            if obj.source == index:
                return cls(obj)
        raise KeyError(f"index {index!r} is not in the mapping")

    def translate(self, doc_id: str, source: Mapping[str, Any]) -> CanonicalRecord:
        """One document to one record; raises RecordBuildError when the mapping cannot apply."""
        row = dict(source)
        row[DOC_ID_KEY] = doc_id
        return self.builder.build(row)

    def translate_line(self, line: str) -> CanonicalRecord:
        """One raw NDJSON line (with ``_id`` injected by OSIS) to one record."""
        try:
            doc = json.loads(line)
        except ValueError as err:
            raise DocumentError(f"line is not JSON: {line[:80]!r}") from err
        if not isinstance(doc, dict):
            raise DocumentError("line is not a JSON object")
        doc_id = doc.get(DOC_ID_KEY)
        if not doc_id:
            raise DocumentError("document has no _id; the OSIS add_entries processor is missing")
        source = {k: v for k, v in doc.items() if k not in (DOC_ID_KEY, INDEX_KEY)}
        return self.translate(str(doc_id), source)


@dataclass
class PartitionResult:
    """What one Glue partition (or one runner batch) produced."""

    namespace: str
    rows: int = 0
    rejected: int = 0
    parts: list[str] = field(default_factory=list)
    reject_parts: list[str] = field(default_factory=list)
    reject_reasons: dict[str, int] = field(default_factory=dict)
    vocabulary: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "rows": self.rows,
            "rejected": self.rejected,
            "parts": self.parts,
            "reject_parts": self.reject_parts,
            "reject_reasons": self.reject_reasons,
            "vocabulary": self.vocabulary,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PartitionResult:
        """Inverse of to_dict."""
        return cls(
            namespace=str(data["namespace"]),
            rows=int(data.get("rows", 0)),
            rejected=int(data.get("rejected", 0)),
            parts=list(data.get("parts", [])),
            reject_parts=list(data.get("reject_parts", [])),
            reject_reasons=dict(data.get("reject_reasons", {})),
            vocabulary={str(k): int(v) for k, v in dict(data.get("vocabulary", {})).items()},
        )


def merge_results(results: Iterable[PartitionResult]) -> dict[str, PartitionResult]:
    """Fold partition results into one per namespace."""
    merged: dict[str, PartitionResult] = {}
    for result in results:
        total = merged.setdefault(result.namespace, PartitionResult(namespace=result.namespace))
        total.rows += result.rows
        total.rejected += result.rejected
        total.parts.extend(result.parts)
        total.reject_parts.extend(result.reject_parts)
        for reason, count in result.reject_reasons.items():
            total.reject_reasons[reason] = total.reject_reasons.get(reason, 0) + count
        total.vocabulary.update(result.vocabulary)
    for total in merged.values():
        total.parts.sort()
        total.reject_parts.sort()
    return merged


def _reason_key(message: str) -> str:
    head = message.split(":", 1)[0]
    return head if len(head) <= 60 else head[:57] + "..."


def transform_lines(
    lines: Iterable[str],
    *,
    translator: DocumentTranslator,
    s3: S3Client,
    bucket: str,
    canonical_prefix: str,
    rejects_prefix: str,
    target_part_bytes: int,
) -> PartitionResult:
    """Stream raw lines into canonical parts under *canonical_prefix*; rejects go to rejects/.

    A record whose namespace differs from the object's (``namespace_by``) still lands under
    the object's prefix; the shared transform stage splits namespaces afterwards.
    """
    result = PartitionResult(namespace=translator.obj.namespace)
    clean = CanonicalWriter(s3, bucket, canonical_prefix, target_part_bytes=target_part_bytes)
    rejects = JsonLinesPartWriter(s3, bucket, rejects_prefix)
    try:
        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            try:
                record = translator.translate_line(line)
            except (RecordBuildError, DocumentError) as err:
                doc_id = _doc_id_of(line)
                rejects.write(Reject(doc_id, translator.obj.namespace, doc_id, str(err)).to_json())
                result.rejected += 1
                key = _reason_key(str(err))
                result.reject_reasons[key] = result.reject_reasons.get(key, 0) + 1
                continue
            clean.write(record)
            result.rows += 1
    except Exception:
        clean.abort()
        raise
    result.parts = [p.key for p in clean.close()]
    result.reject_parts = rejects.close()
    result.vocabulary = dict(translator.builder.vocabulary)
    return result


def _doc_id_of(line: str) -> str:
    try:
        doc = json.loads(line)
    except ValueError:
        return ""
    return str(doc.get(DOC_ID_KEY, "")) if isinstance(doc, dict) else ""
