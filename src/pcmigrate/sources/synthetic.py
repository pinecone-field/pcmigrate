"""SyntheticSource: deterministic generated data for tests of the shared pipeline."""

from __future__ import annotations

import math
import random
from collections.abc import Iterator, Sequence
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from typing import Any

from pcmigrate.config.mapping import Mapping, SourceKind
from pcmigrate.core.canonical import CanonicalRecord
from pcmigrate.sources.base import Change, Partition, RecordSink, SourceSchema

CATEGORIES = ("news", "blog", "paper", "manual")


@dataclass
class SyntheticSource:
    """Generates ``rows`` records with ``dimension``-wide unit-ish vectors across partitions.

    Every record is a pure function of its row number and the seed, so re-exporting a
    partition after a crash writes identical data.
    """

    rows: int
    dimension: int
    partitions: int = 4
    object_name: str = "synthetic.documents"
    namespace: str = "documents"
    seed: int = 7
    exported_partitions: list[str] = field(default_factory=list)

    @property
    def kind(self) -> str:
        """Always ``synthetic``."""
        return SourceKind.SYNTHETIC.value

    def introspect(self) -> SourceSchema:
        """A one-table schema."""
        return SourceSchema(
            kind=self.kind,
            objects={
                self.object_name: {
                    "rows": self.rows,
                    "columns": {"id": "bigint", "embedding": f"vector({self.dimension})"},
                }
            },
        )

    def plan_partitions(self, mapping: Mapping) -> list[Partition]:
        """Equal row ranges, one per partition."""
        size = -(-self.rows // self.partitions)
        plan: list[Partition] = []
        for index in range(self.partitions):
            lower = index * size
            upper = min(self.rows, lower + size)
            if lower >= upper:
                break
            plan.append(
                Partition(
                    id=f"p{index:04d}",
                    object=self.object_name,
                    namespace=self.namespace,
                    lower=str(lower),
                    upper=str(upper),
                    estimated_rows=upper - lower,
                )
            )
        return plan

    def record(self, row: int) -> CanonicalRecord:
        """The record for one row number."""
        rng = random.Random(self.seed * 1_000_003 + row)
        values = [rng.uniform(-1.0, 1.0) for _ in range(self.dimension)]
        return CanonicalRecord(
            id=f"{self.namespace}#{row}",
            namespace=self.namespace,
            source_pk=str(row),
            dense=values,
            metadata={
                "category": CATEGORIES[row % len(CATEGORIES)],
                "row": row,
                "even": row % 2 == 0,
            },
        )

    def export(self, partition: Partition, writer: RecordSink) -> int:
        """Write every row in the partition's range."""
        assert partition.lower is not None and partition.upper is not None
        count = 0
        for row in range(int(partition.lower), int(partition.upper)):
            writer.write(self.record(row))
            count += 1
        self.exported_partitions.append(partition.id)
        return count

    def change_feed(self) -> Iterator[Change]:
        """No changes."""
        yield from ()

    def expected_counts(self) -> dict[str, int]:
        """Rows per namespace."""
        return {self.namespace: self.rows}

    def _row_of(self, record_id: str) -> int:
        return int(record_id.rsplit("#", 1)[1])

    def sample_ids(self, namespace: str, count: int, *, seed: int = 0) -> list[str]:
        """GroundTruthSource."""
        rng = random.Random(seed)
        rows = rng.sample(range(self.rows), min(count, self.rows))
        return [f"{namespace}#{r}" for r in rows]

    def fetch_records(self, namespace: str, ids: Sequence[str]) -> dict[str, CanonicalRecord]:
        """GroundTruthSource."""
        return {i: self.record(self._row_of(i)) for i in ids}

    def knn(
        self,
        namespace: str,
        vector: Sequence[float],
        k: int,
        *,
        filter: MappingABC[str, Any] | None = None,
    ) -> list[str]:
        """Exact cosine kNN over every row."""
        norm_q = math.sqrt(sum(x * x for x in vector)) or 1.0
        scored: list[tuple[float, str]] = []
        for row in range(self.rows):
            record = self.record(row)
            if filter and any(record.metadata.get(k_) != v.get("$eq") for k_, v in filter.items()):
                continue
            values = record.dense or []
            dot = sum(x * y for x, y in zip(vector, values, strict=True))
            norm = math.sqrt(sum(y * y for y in values)) or 1.0
            scored.append((dot / (norm_q * norm), record.id))
        scored.sort(key=lambda s: (-s[0], s[1]))
        return [i for _, i in scored[:k]]

    def all_ids(self, namespace: str) -> Iterator[str]:
        """GroundTruthSource."""
        for row in range(self.rows):
            yield f"{namespace}#{row}"

    def common_filters(self, namespace: str, count: int) -> list[dict[str, Any]]:
        """One filter per category."""
        return [{"category": {"$eq": c}} for c in CATEGORIES[:count]]
