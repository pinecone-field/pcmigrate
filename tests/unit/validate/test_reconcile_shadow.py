from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.pinecone_client import VectorRecord
from pcmigrate.validate.reconcile import reconcile
from pcmigrate.validate.shadow import shadow_read
from tests.unit.fakes import FakeGateway

HOST = "h"
NS = "documents"


def loaded(rows: int = 40) -> tuple[SyntheticSource, FakeGateway]:
    source = SyntheticSource(rows=rows, dimension=6, partitions=1)
    gw = FakeGateway()
    gw.upsert(
        HOST,
        NS,
        [
            VectorRecord(r.id, r.dense or [], r.metadata)
            for r in (source.record(i) for i in range(rows))
        ],
    )
    return source, gw


def test_reconcile_repairs_missing_and_extra() -> None:
    source, gw = loaded()
    del gw.data[HOST][NS]["documents#3"]
    del gw.data[HOST][NS]["documents#4"]
    gw.upsert(HOST, NS, [VectorRecord("documents#999", [0.0] * 6, {})])
    result = reconcile(gw, HOST, source, NS)
    assert result.converged and result.upserted == 2 and result.deleted == 1
    assert len(gw.data[HOST][NS]) == 40 and "documents#999" not in gw.data[HOST][NS]
    assert reconcile(gw, HOST, source, NS).to_dict()["missing_before"] == 0


def test_reconcile_report_only() -> None:
    source, gw = loaded()
    del gw.data[HOST][NS]["documents#1"]
    result = reconcile(gw, HOST, source, NS, apply=False)
    assert not result.converged and result.unresolvable == ["documents#1"] and result.upserted == 0


def test_shadow_read_reports_overlap_and_latency() -> None:
    source, gw = loaded()
    ticks = iter([float(i) for i in range(1000)])
    result = shadow_read(gw, HOST, source, NS, queries=5, k=3, clock=lambda: next(ticks))
    d = result.to_dict()
    assert d["queries"] == 5 and d["mean_overlap"] == 1.0
    assert d["source_p50_ms"] == 1000.0 and d["target_p95_ms"] == 1000.0
