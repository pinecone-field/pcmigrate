from itertools import pairwise

from psycopg import sql

from pcmigrate.sources.pgvector.partition import (
    KeyInfo,
    build_partitions,
    integer_bounds,
    partition_count,
    range_predicate,
    sampled_bounds,
)


def test_partition_count() -> None:
    assert partition_count(0) == 1
    assert partition_count(1_999_999) == 1
    assert partition_count(2_000_001) == 2
    assert partition_count(10_000, 2_500) == 4


def test_integer_bounds_cover_range_without_gaps() -> None:
    bounds = integer_bounds(1, 1000, 4)
    assert bounds[0][0] is None and bounds[-1][1] is None
    assert len(bounds) == 4
    for (_, upper), (lower, _) in pairwise(bounds):
        assert upper == lower
    assert integer_bounds(5, 5, 3) == [(None, None)]
    assert integer_bounds(1, 3, 10) == [(None, 2), (2, 3), (3, None)]


def test_sampled_bounds_dedupes_and_opens_ends() -> None:
    sample = sorted(["a", "b", "b", "b", "c", "d", "e", "f", "g", "h"])
    bounds = sampled_bounds(sample, 3)
    assert bounds[0][0] is None and bounds[-1][1] is None
    cuts = [b[1] for b in bounds[:-1]]
    assert cuts == sorted(set(cuts))
    assert sampled_bounds(["x"], 4) == [(None, None)]


def test_range_predicate() -> None:
    key = KeyInfo("id", "bigint")
    assert key.is_integer
    clause, params = range_predicate(key, "10", "20")
    assert isinstance(clause, sql.Composed) and params == ["10", "20"]
    clause, params = range_predicate(key, None, None)
    assert params == [] and isinstance(clause, sql.SQL)
    _, params = range_predicate(KeyInfo("sku", "text"), None, "SKU-00500")
    assert params == ["SKU-00500"]


def test_build_partitions() -> None:
    parts = build_partitions("public.documents", "documents", [(None, 500), (500, None)], 1000)
    assert [p.id for p in parts] == ["p0000", "p0001"]
    assert parts[0].lower is None and parts[0].upper == "500"
    assert parts[1].lower == "500" and parts[1].upper is None
    assert all(p.estimated_rows == 500 for p in parts)
