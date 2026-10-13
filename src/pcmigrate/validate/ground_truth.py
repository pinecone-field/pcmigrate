"""Exact ground truth from the clean canonical zone (ADR-017).

When sparse vectors are generated during the migration, the source holds nothing to rank
sparse queries against. The clean canonical parts hold exactly what was imported, so this
module streams them once per batch of queries and computes brute-force top-k for dense,
sparse, and both, bounded in memory by one Arrow batch.
"""

from __future__ import annotations

import heapq
import random
from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Mapping as MappingABC
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pcmigrate.config.mapping import Metric
from pcmigrate.core.canonical import (
    CanonicalRecord,
    SparseVector,
    batch_to_records,
    read_parquet_batches,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

BATCH_ROWS = 4096


class CanonicalGroundTruth:
    """Ground truth for the hybrid checks, read from ``canonical/_clean/<namespace>/``."""

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        parts_by_namespace: Mapping[str, Sequence[str]],
        *,
        metric: Metric | str = Metric.COSINE,
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._parts = {ns: list(keys) for ns, keys in parts_by_namespace.items()}
        self._metric = Metric(metric)

    def _batches(
        self, namespace: str, columns: list[str] | None = None
    ) -> Iterator[pa.RecordBatch]:
        for key in self._parts.get(namespace, []):
            yield from read_parquet_batches(
                self._s3, self._bucket, key, batch_rows=BATCH_ROWS, columns=columns
            )

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """A seeded random sample of ids."""
        ids: list[str] = []
        for batch in self._batches(namespace, ["id"]):
            ids.extend(batch.column("id").to_pylist())
        return random.Random(seed).sample(ids, min(count, len(ids)))

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """The migrated records for *ids*, with their generated sparse vectors."""
        wanted = pa.array(sorted(set(ids)), type=pa.string())
        out: dict[str, CanonicalRecord] = {}
        for batch in self._batches(namespace):
            mask = pc.is_in(batch.column("id"), value_set=wanted)
            if not pc.any(mask).as_py():
                continue
            for record in batch_to_records(batch.filter(mask)):
                out[record.id] = record
        return out

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: MappingABC[str, Any] | None = None,
    ) -> list[str]:
        """Exact dense top-k by the mapped metric (filters are not supported here)."""
        return self.knn_many(namespace, [vector], k)[0]

    def knn_sparse(
        self,
        namespace: str,
        sparse: SparseVector,
        k: int,
        *,
        filter: MappingABC[str, Any] | None = None,
    ) -> list[str]:
        """Exact sparse top-k by inner product."""
        return self.knn_sparse_many(namespace, [sparse], k)[0]

    def knn_many(
        self, namespace: str, vectors: Sequence[Sequence[float]], k: int
    ) -> list[list[str]]:
        """Dense top-k for every query in one pass over the namespace."""
        if not vectors:
            return []
        queries: Any = np.asarray(vectors, dtype=np.float32)
        if self._metric is Metric.COSINE:
            queries = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
        heaps: list[list[tuple[float, _Last]]] = [[] for _ in vectors]
        for batch in self._batches(namespace, ["id", "dense"]):
            dense = batch.column("dense")
            flat = np.asarray(dense.flatten().to_numpy(zero_copy_only=False), dtype=np.float32)
            lengths = np.diff(np.asarray(dense.offsets.to_numpy(zero_copy_only=False)))
            if not len(lengths) or lengths.min() != lengths.max():
                raise ValueError("dense vectors in the clean zone differ in dimension")
            matrix = flat.reshape(len(lengths), int(lengths[0]))
            scores = self._dense_scores(queries, matrix)
            self._merge(heaps, scores, batch.column("id").to_pylist(), k)
        return [self._ranked(heap) for heap in heaps]

    def knn_sparse_many(
        self, namespace: str, sparses: Sequence[SparseVector], k: int
    ) -> list[list[str]]:
        """Sparse top-k (inner product) for every query in one pass over the namespace."""
        if not sparses:
            return []
        query_index = [np.asarray(s.indices, dtype=np.int64) for s in sparses]
        query_value = [np.asarray(s.values, dtype=np.float32) for s in sparses]
        order = [np.argsort(q) for q in query_index]
        query_index = [q[o] for q, o in zip(query_index, order, strict=True)]
        query_value = [v[o] for v, o in zip(query_value, order, strict=True)]
        heaps: list[list[tuple[float, _Last]]] = [[] for _ in sparses]
        for batch in self._batches(namespace, ["id", "sparse"]):
            struct = batch.column("sparse")
            indices = struct.field("indices")
            values = struct.field("values")
            offsets = np.asarray(indices.offsets.to_numpy(zero_copy_only=False))
            lengths = np.diff(offsets)
            rows = np.repeat(np.arange(len(lengths)), lengths)
            flat_index = np.asarray(
                indices.flatten().to_numpy(zero_copy_only=False), dtype=np.int64
            )
            flat_value = np.asarray(
                values.flatten().to_numpy(zero_copy_only=False), dtype=np.float32
            )
            scores = np.zeros((len(sparses), len(lengths)), dtype=np.float32)
            for q, (qi, qv) in enumerate(zip(query_index, query_value, strict=True)):
                if not len(qi) or not len(flat_index):
                    continue
                position = np.minimum(np.searchsorted(qi, flat_index), len(qi) - 1)
                hit = qi[position] == flat_index
                np.add.at(scores[q], rows[hit], flat_value[hit] * qv[position[hit]])
            self._merge(heaps, scores, batch.column("id").to_pylist(), k)
        return [self._ranked(heap) for heap in heaps]

    def _dense_scores(self, queries: Any, matrix: Any) -> Any:
        if self._metric is Metric.EUCLIDEAN:
            q2 = np.sum(queries * queries, axis=1, keepdims=True)
            d2 = np.sum(matrix * matrix, axis=1)[None, :]
            return 2.0 * (queries @ matrix.T) - q2 - d2
        if self._metric is Metric.COSINE:
            norms = np.maximum(np.linalg.norm(matrix, axis=1), 1e-12)
            return (queries @ matrix.T) / norms[None, :]
        return queries @ matrix.T

    @staticmethod
    def _merge(heaps: list[list[tuple[float, _Last]]], scores: Any, ids: list[str], k: int) -> None:
        """Keep the best k of every query: highest score, then lowest id, like the sources."""
        width = scores.shape[1]
        take = min(k, width)
        if take == 0:
            return
        id_array = np.asarray(ids)
        for q, heap in enumerate(heaps):
            row = scores[q]
            top = np.lexsort((id_array, -row))[:take]
            for i in top:
                item = (float(row[i]), _Last(ids[int(i)]))
                if len(heap) < k:
                    heapq.heappush(heap, item)
                elif item > heap[0]:
                    heapq.heapreplace(heap, item)

    @staticmethod
    def _ranked(heap: list[tuple[float, _Last]]) -> list[str]:
        return [last.value for _, last in sorted(heap, key=lambda s: (-s[0], s[1].value))]


class _Last:
    """An id that compares in reverse, so a min-heap of (score, id) evicts the larger id first."""

    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def __lt__(self, other: _Last) -> bool:
        return self.value > other.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Last) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)
