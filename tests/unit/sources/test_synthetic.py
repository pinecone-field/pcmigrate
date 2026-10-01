from pcmigrate.config import Mapping
from pcmigrate.core.canonical import CanonicalRecord
from pcmigrate.sources.synthetic import SyntheticSource
from tests.unit.config.test_mapping import EXAMPLE


class ListSink:
    def __init__(self) -> None:
        self.records: list[CanonicalRecord] = []

    def write(self, record: CanonicalRecord) -> None:
        self.records.append(record)


def test_partitions_cover_rows_exactly_once() -> None:
    source = SyntheticSource(rows=1003, dimension=8, partitions=4)
    plan = source.plan_partitions(Mapping.from_yaml(EXAMPLE))
    assert [p.id for p in plan] == ["p0000", "p0001", "p0002", "p0003"]
    seen: list[str] = []
    for partition in plan:
        sink = ListSink()
        assert source.export(partition, sink) == partition.estimated_rows
        seen.extend(r.id for r in sink.records)
    assert len(seen) == 1003 and len(set(seen)) == 1003


def test_export_is_deterministic() -> None:
    a = SyntheticSource(rows=50, dimension=4, partitions=1)
    b = SyntheticSource(rows=50, dimension=4, partitions=1)
    sink_a, sink_b = ListSink(), ListSink()
    (part,) = a.plan_partitions(Mapping.from_yaml(EXAMPLE))
    a.export(part, sink_a)
    b.export(part, sink_b)
    assert sink_a.records == sink_b.records
    assert all(len(r.dense or []) == 4 for r in sink_a.records)
    assert sink_a.records[0].metadata["category"] == "news"


def test_schema_fingerprint_changes_with_dimension() -> None:
    assert (
        SyntheticSource(rows=1, dimension=8).introspect().fingerprint
        != SyntheticSource(rows=1, dimension=16).introspect().fingerprint
    )
    assert SyntheticSource(rows=1, dimension=8).kind == "synthetic"
    assert list(SyntheticSource(rows=1, dimension=8).change_feed()) == []
