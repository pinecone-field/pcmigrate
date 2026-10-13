"""A small analyzer and BM25 ranker, shared by the synthetic source and the test gateway.

It is not Pinecone's implementation. It exists so in-process ground truth and the unit-test
fake agree on tokenization and scoring, and so the Docker tests have a target to rank
against. ``Analyzer`` mirrors the four ``full_text_search`` knobs: language stop words,
stemming (through an injected stemmer), and character n-grams.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

TOKEN = re.compile(r"[^\W_]+", re.UNICODE)
ENGLISH_STOP_WORDS = frozenset(
    {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
        "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
        "there", "these", "they", "this", "to", "was", "will", "with",
    }
)  # fmt: skip


@dataclass(frozen=True)
class Analyzer:
    """Lowercase word tokens, optional stop-word removal and stemming, or character n-grams."""

    stemming: bool = False
    stop_words: bool = False
    ngram: tuple[int, int, bool] | None = None
    stem: Callable[[str], str] | None = None
    stop_list: frozenset[str] = ENGLISH_STOP_WORDS

    def tokens(self, text: str) -> list[str]:
        """Index-time and query-time tokens of *text*."""
        words = [w.lower() for w in TOKEN.findall(text)]
        if self.ngram is not None:
            low, high, prefix = self.ngram
            grams: list[str] = []
            for word in words:
                limit = 1 if prefix else len(word)
                for start in range(limit):
                    for size in range(low, high + 1):
                        if start + size <= len(word):
                            grams.append(word[start : start + size])
            return grams
        if self.stop_words:
            words = [w for w in words if w not in self.stop_list]
        if self.stemming and self.stem is not None:
            words = [self.stem(w) for w in words]
        return words


@dataclass
class Bm25Index:
    """BM25 (k1 = 1.2, b = 0.75) over one text field of many documents."""

    analyzer: Analyzer = field(default_factory=Analyzer)
    k1: float = 1.2
    b: float = 0.75
    _docs: dict[str, dict[str, int]] = field(default_factory=dict)
    _lengths: dict[str, int] = field(default_factory=dict)
    _df: dict[str, int] = field(default_factory=dict)
    _positions: dict[str, list[str]] = field(default_factory=dict)

    def add(self, doc_id: str, text: str) -> None:
        """Index (or replace) one document's text."""
        self.remove(doc_id)
        tokens = self.analyzer.tokens(text)
        counts: dict[str, int] = {}
        for token in tokens:
            counts[token] = counts.get(token, 0) + 1
        self._docs[doc_id] = counts
        self._lengths[doc_id] = len(tokens)
        self._positions[doc_id] = tokens
        for token in counts:
            self._df[token] = self._df.get(token, 0) + 1

    def remove(self, doc_id: str) -> None:
        """Forget one document."""
        counts = self._docs.pop(doc_id, None)
        if counts is None:
            return
        self._lengths.pop(doc_id, None)
        self._positions.pop(doc_id, None)
        for token in counts:
            self._df[token] -= 1
            if self._df[token] <= 0:
                del self._df[token]

    def __len__(self) -> int:
        return len(self._docs)

    def __contains__(self, doc_id: object) -> bool:
        return doc_id in self._docs

    def _idf(self, token: str) -> float:
        n = len(self._docs)
        df = self._df.get(token, 0)
        return math.log(1.0 + (n - df + 0.5) / (df + 0.5))

    def score(self, doc_id: str, query_tokens: Sequence[str]) -> float:
        """BM25 score of one document for the query tokens (token OR)."""
        counts = self._docs.get(doc_id)
        if not counts:
            return 0.0
        avg = sum(self._lengths.values()) / max(1, len(self._lengths))
        length = self._lengths[doc_id]
        total = 0.0
        for token in query_tokens:
            tf = counts.get(token, 0)
            if tf == 0:
                continue
            norm = tf * (self.k1 + 1) / (tf + self.k1 * (1 - self.b + self.b * length / avg))
            total += self._idf(token) * norm
        return total

    def matches_phrase(self, doc_id: str, phrase_tokens: Sequence[str]) -> bool:
        """True when the phrase tokens appear consecutively in the document."""
        tokens = self._positions.get(doc_id, [])
        size = len(phrase_tokens)
        if size == 0:
            return True
        return any(
            tokens[i : i + size] == list(phrase_tokens) for i in range(len(tokens) - size + 1)
        )

    def matches_all(self, doc_id: str, query_tokens: Sequence[str]) -> bool:
        """True when every query token appears in the document."""
        counts = self._docs.get(doc_id, {})
        return all(t in counts for t in query_tokens)

    def matches_any(self, doc_id: str, query_tokens: Sequence[str]) -> bool:
        """True when any query token appears in the document."""
        counts = self._docs.get(doc_id, {})
        return any(t in counts for t in query_tokens)

    def rank(
        self,
        query: str,
        k: int,
        *,
        candidates: Iterable[str] | None = None,
        mode: str = "any",
    ) -> list[tuple[str, float]]:
        """Top-k (id, score) by BM25; ``mode`` restricts candidates to any/all/phrase matches."""
        tokens = self.analyzer.tokens(query)
        if not tokens:
            return []
        pool = list(candidates) if candidates is not None else list(self._docs)
        scored: list[tuple[str, float]] = []
        for doc_id in pool:
            if mode == "phrase" and not self.matches_phrase(doc_id, tokens):
                continue
            if mode == "all" and not self.matches_all(doc_id, tokens):
                continue
            if not self.matches_any(doc_id, tokens):
                continue
            scored.append((doc_id, self.score(doc_id, tokens)))
        scored.sort(key=lambda s: (-round(s[1], 9), s[0]))
        return scored[:k]


def rank_fields(
    indexes: Mapping[str, Bm25Index],
    fields: Sequence[str],
    query: str,
    k: int,
    *,
    candidates: Iterable[str] | None = None,
    phrase: Mapping[str, str] | None = None,
    match_all: Mapping[str, str] | None = None,
    match_any: Mapping[str, str] | None = None,
) -> list[tuple[str, float]]:
    """Rank by the sum of BM25 over several fields (equal weight), after text-match filters."""
    pool: list[str] | None = list(candidates) if candidates is not None else None
    for name, text in (phrase or {}).items():
        index = indexes.get(name)
        if index is None:
            return []
        tokens = index.analyzer.tokens(text)
        pool = [
            d
            for d in (pool if pool is not None else list(index._docs))
            if index.matches_phrase(d, tokens)
        ]
    for name, text in (match_all or {}).items():
        index = indexes.get(name)
        if index is None:
            return []
        tokens = index.analyzer.tokens(text)
        pool = [
            d
            for d in (pool if pool is not None else list(index._docs))
            if index.matches_all(d, tokens)
        ]
    for name, text in (match_any or {}).items():
        index = indexes.get(name)
        if index is None:
            return []
        tokens = index.analyzer.tokens(text)
        pool = [
            d
            for d in (pool if pool is not None else list(index._docs))
            if index.matches_any(d, tokens)
        ]
    totals: dict[str, float] = {}
    for name in fields:
        index = indexes.get(name)
        if index is None:
            continue
        for doc_id, score in index.rank(query, len(index), candidates=pool):
            totals[doc_id] = totals.get(doc_id, 0.0) + score
    ranked = sorted(totals.items(), key=lambda s: (-round(s[1], 9), s[0]))
    return ranked[:k]
