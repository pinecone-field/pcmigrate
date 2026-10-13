"""Sparse vectors: parsing, vocabulary hashing, dot products, and rank fusion (ADR-016).

Importable by the Glue job, so nothing here touches the report, a database, or Pinecone.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence

import xxhash

from pcmigrate.core.canonical import SparseVector

_SPARSEVEC = re.compile(r"^\{(?P<entries>[^}]*)\}/(?P<dim>\d+)$")
UINT32_MAX = 2**32 - 1


def parse_sparsevec(text: str) -> tuple[SparseVector, int]:
    """Parse pgvector's ``{1:0.5,7:0.25}/1024`` text into a zero-based SparseVector and dimension.

    pgvector indices are one-based in text; Pinecone indices are zero-based integers, so
    one is subtracted. Entries come back sorted by index.
    """
    match = _SPARSEVEC.match(text.strip())
    if match is None:
        raise ValueError(f"not a sparsevec literal: {text[:60]!r}")
    pairs: list[tuple[int, float]] = []
    entries = match.group("entries").strip()
    if entries:
        for entry in entries.split(","):
            index_text, _, value_text = entry.partition(":")
            index = int(index_text) - 1
            value = float(value_text)
            if index < 0 or math.isnan(value) or math.isinf(value):
                raise ValueError(f"bad sparsevec entry {entry!r}")
            pairs.append((index, value))
    pairs.sort()
    return SparseVector([i for i, _ in pairs], [v for _, v in pairs]), int(match.group("dim"))


def format_sparsevec(sparse: SparseVector, dimension: int) -> str:
    """Inverse of parse_sparsevec, for pgvector query literals."""
    body = ",".join(f"{i + 1}:{v:.9g}" for i, v in zip(sparse.indices, sparse.values, strict=True))
    return f"{{{body}}}/{dimension}"


def token_index(token: str) -> int:
    """A stable 32-bit index for a token (xxh32), the ``xxh32`` vocabulary."""
    return xxhash.xxh32_intdigest(token.encode("utf-8"))


def tokens_to_sparse(weights: Mapping[str, float]) -> tuple[SparseVector, dict[str, int]]:
    """rank_features token weights to a SparseVector plus the token-to-index mapping used.

    Two tokens that hash to one index are summed, which the vocabulary merge reports as a
    collision. Zero, NaN, and infinite weights are dropped.
    """
    by_index: dict[int, float] = {}
    vocabulary: dict[str, int] = {}
    for token, weight in weights.items():
        index = token_index(str(token))
        vocabulary[str(token)] = index
        value = float(weight)
        if value == 0.0 or math.isnan(value) or math.isinf(value):
            continue
        by_index[index] = by_index.get(index, 0.0) + value
    items = sorted(by_index.items())
    return SparseVector([i for i, _ in items], [v for _, v in items]), vocabulary


def merge_vocabularies(parts: Iterable[Mapping[str, int]]) -> tuple[dict[str, int], list[str]]:
    """Union of partial token maps and every index shared by two different tokens."""
    merged: dict[str, int] = {}
    owners: dict[int, str] = {}
    collisions: set[str] = set()
    for part in parts:
        for token, index in part.items():
            merged[token] = int(index)
            owner = owners.setdefault(int(index), token)
            if owner != token:
                collisions.add(f"{owner} / {token} -> {index}")
    return merged, sorted(collisions)


def sparse_dot(a: SparseVector, b: SparseVector) -> float:
    """Inner product of two sparse vectors."""
    lookup = dict(zip(a.indices, a.values, strict=True))
    return sum(lookup.get(i, 0.0) * v for i, v in zip(b.indices, b.values, strict=True))


def rrf_fuse(
    rankings: Sequence[Sequence[str]], *, k: int = 60, top: int | None = None
) -> list[str]:
    """Reciprocal rank fusion of several ranked id lists (ADR-016)."""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for position, item in enumerate(ranking):
            scores[item] = scores.get(item, 0.0) + 1.0 / (k + position + 1)
    fused = sorted(scores.items(), key=lambda s: (-s[1], s[0]))
    ids = [item for item, _ in fused]
    return ids[:top] if top is not None else ids


def overlap_at_k(truth: Sequence[str], got: Sequence[str], k: int) -> float:
    """|truth[:k] ∩ got[:k]| / k."""
    return len(set(truth[:k]) & set(got[:k])) / k if k else 0.0


def concat_text(text_fields: Mapping[str, str], fields: Sequence[str], separator: str) -> str:
    """The text a generated sparse vector encodes: the named fields, in order, joined.

    Missing, empty, and whitespace-only fields are skipped, so the result is empty when
    there is nothing to encode (ADR-017).
    """
    parts = [str(text_fields.get(name, "")).strip() for name in fields]
    return separator.join(p for p in parts if p)


def text_hash(model: str, text: str) -> int:
    """Cache key of one encoded text under one model (xxh64)."""
    return xxhash.xxh64_intdigest(f"{model}\x00{text}".encode())
