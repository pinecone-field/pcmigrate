"""PK-range partitioning for parallel export under one exported snapshot (ADR-008).

Integer keys split evenly between min and max. Other keys (text, uuid, composite) take
their boundaries from an ordered sample bucketed with ntile, so each range holds about the
same number of rows. Bounds are inclusive below and exclusive above; the first partition has
no lower bound and the last no upper bound, so every row lands exactly once.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from pcmigrate.sources.base import Partition

INTEGER_TYPES = {"int2", "int4", "int8", "smallint", "integer", "bigint"}
DEFAULT_TARGET_ROWS = 2_000_000
SAMPLE_ROWS_PER_BOUNDARY = 200


@dataclass(frozen=True, slots=True)
class KeyInfo:
    """The partitioning column: the first primary key column and its type."""

    column: str
    type_name: str

    @property
    def is_integer(self) -> bool:
        """True for the integer families."""
        return self.type_name in INTEGER_TYPES


def partition_count(rows: int, target_rows: int = DEFAULT_TARGET_ROWS) -> int:
    """At least one partition; otherwise rows / target rounded up."""
    if rows <= 0:
        return 1
    return max(1, -(-rows // max(1, target_rows)))


def integer_bounds(low: int, high: int, count: int) -> list[tuple[int | None, int | None]]:
    """Split [low, high] into *count* half-open ranges with open ends."""
    if count <= 1 or high <= low:
        return [(None, None)]
    width = (high - low + 1) / count
    cuts = sorted({low + round(width * i) for i in range(1, count)})
    cuts = [c for c in cuts if low < c <= high]
    bounds: list[tuple[int | None, int | None]] = []
    previous: int | None = None
    for cut in cuts:
        bounds.append((previous, cut))
        previous = cut
    bounds.append((previous, None))
    return bounds


def sampled_bounds(sorted_sample: Sequence[Any], count: int) -> list[tuple[Any, Any]]:
    """Quantile boundaries of an ordered sample, deduplicated."""
    if count <= 1 or len(sorted_sample) < 2:
        return [(None, None)]
    cuts: list[Any] = []
    n = len(sorted_sample)
    for i in range(1, count):
        value = sorted_sample[min(n - 1, (n * i) // count)]
        if not cuts or value != cuts[-1]:
            cuts.append(value)
    bounds: list[tuple[Any, Any]] = []
    previous: Any = None
    for cut in cuts:
        bounds.append((previous, cut))
        previous = cut
    bounds.append((previous, None))
    return bounds


def range_predicate(
    key: KeyInfo, lower: str | None, upper: str | None
) -> tuple[sql.Composable, list[Any]]:
    """WHERE fragment and parameters for one partition's bounds."""
    clauses: list[sql.Composable] = []
    params: list[Any] = []
    cast = sql.SQL(key.type_name)
    if lower is not None:
        clauses.append(sql.SQL("{} >= %s::{}").format(sql.Identifier(key.column), cast))
        params.append(lower)
    if upper is not None:
        clauses.append(sql.SQL("{} < %s::{}").format(sql.Identifier(key.column), cast))
        params.append(upper)
    if not clauses:
        return sql.SQL("TRUE"), params
    return sql.SQL(" AND ").join(clauses), params


def build_partitions(
    object_name: str,
    namespace: str,
    bounds: Sequence[tuple[Any, Any]],
    estimated_rows: int,
) -> list[Partition]:
    """Partition objects from bounds, with bounds rendered as text."""
    per = max(1, estimated_rows // max(1, len(bounds)))
    return [
        Partition(
            id=f"p{index:04d}",
            object=object_name,
            namespace=namespace,
            lower=None if lower is None else str(lower),
            upper=None if upper is None else str(upper),
            estimated_rows=per,
        )
        for index, (lower, upper) in enumerate(bounds)
    ]
