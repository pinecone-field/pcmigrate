import io
import json

from pcmigrate.core.buffer import ChangeEnvelope, InMemoryBuffer
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.applier import Applier, collapse, forward
from pcmigrate.targets.pinecone_client import VectorRecord
from tests.unit.fakes import FakeGateway

HOST = "h"
NS = "documents"


def dw(i: int, op: str = "upsert", change: str | None = None) -> ChangeEnvelope:
    return ChangeEnvelope(
        op=op,  # type: ignore[arg-type]
        id=f"{NS}#{i}",
        namespace=NS,
        source_change_id=change or f"c{i}-{op}",
        values=[float(i), 1.0] if op == "upsert" else None,
        metadata={"v": i} if op == "upsert" else None,
        origin="dual_write",
    )


def cdc(i: int, op: str = "upsert", seq: int = 0) -> ChangeEnvelope:
    return ChangeEnvelope(
        op=op,
        id=f"{NS}#{i}",
        namespace=NS,
        source_change_id=f"ob:{seq}",
        source_pk=str(i),
        sequence=seq,
    )  # type: ignore[arg-type]


def test_collapse_keeps_last_op_per_id() -> None:
    final = collapse([dw(1), dw(2), dw(1, "delete"), dw(2, change="later")])
    assert final[(NS, "documents#1")].op == "delete"
    assert final[(NS, "documents#2")].source_change_id == "later"


def test_applier_applies_dual_writes_and_acks() -> None:
    gw = FakeGateway()
    buffer = InMemoryBuffer()
    out = io.StringIO()
    buffer.send([dw(1), dw(2), dw(3), dw(2, "delete"), dw(1, change="again")])
    applier = Applier(gw, HOST, buffer, events=EventBus("m", [JsonLinesSink(out)]))
    result = applier.drain_once()
    assert result.received == 5 and result.collapsed == 3
    assert result.upserts == 2 and result.deletes == 1
    assert set(gw.data[HOST][NS]) == {"documents#1", "documents#3"}
    assert buffer.approximate_depth() == 0
    lag = json.loads(out.getvalue().splitlines()[-1])
    assert lag["type"] == "applier_lag" and lag["depth"] == 0 and lag["applied"] == 3


def test_applier_rereads_cdc_rows_from_source() -> None:
    source = SyntheticSource(rows=10, dimension=4, partitions=1)
    gw = FakeGateway()
    buffer = InMemoryBuffer()
    buffer.send([cdc(3, seq=1), cdc(4, seq=2), cdc(3, seq=3)])
    applier = Applier(gw, HOST, buffer, source=source)
    result = applier.drain_once()
    assert result.collapsed == 2 and result.upserts == 2
    assert gw.data[HOST][NS]["documents#3"].values == source.record(3).dense


def test_applier_turns_missing_rereads_into_deletes() -> None:
    source = SyntheticSource(rows=5, dimension=4, partitions=1)
    gw = FakeGateway()
    gw.upsert(HOST, NS, [VectorRecord("documents#99", [0.0] * 4, {})])

    class Vanishing(SyntheticSource):
        def fetch_records(self, namespace: str, ids):  # type: ignore[no-untyped-def]
            return {}

    buffer = InMemoryBuffer()
    buffer.send([cdc(99, seq=1)])
    Applier(gw, HOST, buffer, source=Vanishing(rows=5, dimension=4)).drain_once()
    assert "documents#99" not in gw.data[HOST][NS]
    assert source.rows == 5


def test_forward_and_drain_until_empty() -> None:
    gw = FakeGateway()
    buffer = InMemoryBuffer()
    batches = [[dw(1), dw(2)], [dw(3)], []]
    assert forward(lambda limit: batches.pop(0), buffer) == 2
    assert forward(lambda limit: batches.pop(0), buffer) == 1
    assert forward(lambda limit: batches.pop(0), buffer) == 0
    total = Applier(gw, HOST, buffer).drain_until_empty(max_rounds=5)
    assert total.collapsed == 3 and len(gw.data[HOST][NS]) == 3
