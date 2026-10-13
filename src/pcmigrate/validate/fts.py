"""Full-text overlap@k per query class against the source's own text ranking (ADR-018).

Queries are drawn from the migrated records' text so every query has matches: a single
term, several terms (token OR, the semantics of a Pinecone ``text`` clause), a phrase
(``$match_phrase`` on both sides), and a single term under a common metadata filter. Each
class reports mean and minimum overlap@k against its own approved threshold, because the
source ranks with ``ts_rank_cd`` or its own BM25 and Pinecone with its BM25: agreement is
expected to be high for single terms and lower for multi-term queries.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.config.mapping import FTS_QUERY_CLASSES, FullTextThresholds, ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord
from pcmigrate.core.sparse import overlap_at_k
from pcmigrate.sources.base import GroundTruthSource, TextQuery
from pcmigrate.targets.pinecone_client import PineconeGateway
from pcmigrate.validate.bm25 import ENGLISH_STOP_WORDS

WORD = re.compile(r"[^\W\d_]{3,}", re.UNICODE)
MULTI_TERMS = 3
PHRASE_TERMS = 2


@dataclass
class ClassResult:
    """Overlap@k of every query of one class."""

    name: str
    threshold: float
    overlaps: list[float] = field(default_factory=list)
    examples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def mean(self) -> float:
        """Mean overlap@k, 0 with no queries."""
        return sum(self.overlaps) / len(self.overlaps) if self.overlaps else 0.0

    @property
    def minimum(self) -> float | None:
        """Lowest overlap@k seen."""
        return min(self.overlaps) if self.overlaps else None

    @property
    def passed(self) -> bool:
        """At or above the threshold; a class that ran no query passes with a note."""
        return not self.overlaps or self.mean >= self.threshold

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "class": self.name,
            "queries": len(self.overlaps),
            "threshold": self.threshold,
            "mean": round(self.mean, 4),
            "min": round(self.minimum, 4) if self.minimum is not None else None,
            "passed": self.passed,
            "worst": sorted(self.examples, key=lambda e: e["overlap"])[:3],
        }


@dataclass
class FullTextResult:
    """Per-field, per-class overlap for one namespace."""

    namespace: str
    k: int
    classes: dict[str, ClassResult] = field(default_factory=dict)
    fields: list[str] = field(default_factory=list)
    sampled: int = 0
    skipped: int = 0

    @property
    def passed(self) -> bool:
        """Every class at or above its threshold."""
        return all(c.passed for c in self.classes.values())

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly."""
        return {
            "namespace": self.namespace,
            "k": self.k,
            "fields": self.fields,
            "sampled": self.sampled,
            "skipped": self.skipped,
            "queries": sum(len(c.overlaps) for c in self.classes.values()),
            "classes": {name: c.to_dict() for name, c in self.classes.items()},
            "passed": self.passed,
        }


def tokens_of(text: str, stop_words: frozenset[str] = ENGLISH_STOP_WORDS) -> list[str]:
    """Lowercase alphabetic words of three letters or more, stop words removed."""
    return [w.lower() for w in WORD.findall(text) if w.lower() not in stop_words]


def _document_frequency(texts: Sequence[str]) -> dict[str, int]:
    df: dict[str, int] = {}
    for text in texts:
        for token in set(tokens_of(text)):
            df[token] = df.get(token, 0) + 1
    return df


def build_queries(
    records: Sequence[CanonicalRecord],
    source: str,
    field_name: str,
    *,
    filters: Sequence[Mapping[str, Any]] = (),
) -> dict[str, list[TextQuery]]:
    """One query per class per record that carries text in *source*.

    The single term is the record's rarest token across the sample (so rankings are
    selective); the multi-term query adds the next two rarest; the phrase is the first
    two consecutive content words; the filtered query pairs the single term with one of
    the common filters, round robin.
    """
    texts = [r.text_fields[source] for r in records if r.text_fields.get(source)]
    df = _document_frequency(texts)
    out: dict[str, list[TextQuery]] = {name: [] for name in FTS_QUERY_CLASSES}
    for position, record in enumerate(records):
        text = record.text_fields.get(source)
        if not text:
            continue
        counts: dict[str, int] = {}
        for token in tokens_of(text):
            counts[token] = counts.get(token, 0) + 1
        if not counts:
            continue
        ranked = sorted(counts, key=lambda t: (df.get(t, 0), -counts[t], t))
        single = ranked[0]
        out["single_term"].append(TextQuery(field_name, single, source=source))
        if len(ranked) >= 2:
            out["multi_term"].append(
                TextQuery(field_name, " ".join(ranked[:MULTI_TERMS]), source=source)
            )
        words = tokens_of(text, frozenset())
        for i in range(len(words) - PHRASE_TERMS + 1):
            window = words[i : i + PHRASE_TERMS]
            if all(w not in ENGLISH_STOP_WORDS for w in window):
                out["phrase"].append(
                    TextQuery(field_name, " ".join(window), mode="phrase", source=source)
                )
                break
        if filters:
            flt = filters[position % len(filters)]
            out["filtered"].append(TextQuery(field_name, single, filter=dict(flt), source=source))
    return out


def pinecone_request(query: TextQuery) -> tuple[str, dict[str, Any] | None]:
    """The ``text`` clause query and filter a TextQuery becomes on the Pinecone side."""
    filter_body: dict[str, Any] = dict(query.filter or {})
    if query.mode == "phrase":
        filter_body[query.field] = {"$match_phrase": query.text}
    elif query.mode == "all":
        filter_body[query.field] = {"$match_all": query.text}
    return query.text, (filter_body or None)


def check_full_text(
    gateway: PineconeGateway,
    host: str,
    source: GroundTruthSource,
    namespace: str,
    obj: ObjectMapping,
    *,
    k: int,
    queries: int,
    thresholds: FullTextThresholds,
    filters: int = 5,
    seed: int = 5,
) -> FullTextResult:
    """Run every query class on every full-text field; compare top-k with the source."""
    result = FullTextResult(namespace=namespace, k=k, fields=[t.field_name for t in obj.text])
    result.classes = {
        name: ClassResult(name, thresholds.for_class(name)) for name in FTS_QUERY_CLASSES
    }
    if not obj.text:
        return result
    ids = source.sample_ids(namespace, queries, seed=seed)
    records = source.fetch_records(namespace, ids)
    sample = [r for i in ids if (r := records.get(i)) is not None]
    result.sampled = len(sample)
    result.skipped = len(ids) - len(sample)
    common = source.common_filters(namespace, filters)
    per_field = max(1, queries // max(1, len(obj.text)))
    for spec in obj.text:
        classes = build_queries(sample, spec.source, spec.field_name, filters=common)
        for name, class_queries in classes.items():
            outcome = result.classes[name]
            for query in class_queries[:per_field]:
                truth = source.knn_text(namespace, query, k)
                if not truth:
                    continue
                text, flt = pinecone_request(query)
                got = [
                    m.id
                    for m in gateway.query_text(
                        host, namespace, text, fields=[query.field], top_k=k, filter=flt
                    )
                ]
                depth = min(k, len(truth))
                overlap = overlap_at_k(truth, got, depth) if depth else 0.0
                outcome.overlaps.append(overlap)
                outcome.examples.append(
                    {
                        "field": query.field,
                        "query": query.text,
                        "filter": flt,
                        "overlap": round(overlap, 4),
                        "truth": len(truth),
                    }
                )
    return result
