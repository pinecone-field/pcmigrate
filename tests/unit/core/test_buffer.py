import boto3
import pytest
from moto import mock_aws

from pcmigrate.core.buffer import (
    ChangeEnvelope,
    InMemoryBuffer,
    KinesisBuffer,
    SqsFifoBuffer,
    dual_write_change_id,
    envelope_bytes,
)


def env(i: int, op: str = "upsert", change: str | None = None) -> ChangeEnvelope:
    return ChangeEnvelope(
        op=op,  # type: ignore[arg-type]
        id=f"documents#{i}",
        namespace="documents",
        source_change_id=change or f"c{i}",
        values=[1.0, 2.0] if op == "upsert" else None,
        metadata={"n": i} if op == "upsert" else None,
    )


def test_envelope_round_trip_and_reread_flag() -> None:
    e = env(1)
    assert ChangeEnvelope.from_json(e.to_json()) == e
    assert not e.needs_reread
    assert ChangeEnvelope(op="upsert", id="x", namespace="d", source_change_id="k").needs_reread
    assert envelope_bytes(e) > 0
    assert dual_write_change_id().startswith("dw-")


def test_in_memory_buffer_dedups_and_requeues() -> None:
    b = InMemoryBuffer()
    assert b.send([env(1), env(2), env(1)]) == 2
    got = b.receive(max_messages=10)
    assert [r.envelope.id for r in got] == ["documents#1", "documents#2"]
    assert b.approximate_depth() == 2
    b.requeue_inflight()
    again = b.receive()
    assert len(again) == 2
    b.ack([r.receipt for r in again])
    assert b.approximate_depth() == 0


def test_sqs_fifo_buffer_round_trip() -> None:
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        url = sqs.create_queue(
            QueueName="pcm-test.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )["QueueUrl"]
        buffer = SqsFifoBuffer(sqs, url)
        assert buffer.send([env(i) for i in range(25)]) == 25
        assert buffer.approximate_depth() == 25
        received = buffer.receive(max_messages=10)
        assert len(received) == 10 and received[0].sent_at is not None
        assert received[0].envelope.id == "documents#0"
        buffer.ack([r.receipt for r in received])
        rest = []
        while True:
            chunk = buffer.receive(max_messages=10)
            if not chunk:
                break
            rest.extend(chunk)
            buffer.ack([r.receipt for r in chunk])
        assert len(rest) == 15
        assert buffer.approximate_depth() == 0


def test_sqs_fifo_dedups_on_source_change_id() -> None:
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        url = sqs.create_queue(QueueName="pcm-dedup.fifo", Attributes={"FifoQueue": "true"})[
            "QueueUrl"
        ]
        buffer = SqsFifoBuffer(sqs, url)
        buffer.send([env(1, change="same")])
        buffer.send([env(1, change="same")])
        got = buffer.receive(max_messages=10)
        assert len(got) == 1


def test_kinesis_buffer_round_trip() -> None:
    with mock_aws():
        kinesis = boto3.client("kinesis", region_name="us-east-1")
        kinesis.create_stream(StreamName="pcm-test", ShardCount=1)
        buffer = KinesisBuffer(kinesis, "pcm-test")
        assert buffer.send([env(i) for i in range(5)]) == 5
        got = buffer.receive(max_messages=10, wait_s=1.0)
        assert [r.envelope.id for r in got] == [f"documents#{i}" for i in range(5)]
        buffer.ack([r.receipt for r in got])
        assert buffer.checkpoints and buffer.approximate_depth() == 0
        assert buffer.receive(max_messages=10) == []


@pytest.mark.parametrize("op", ["upsert", "delete"])
def test_envelope_ops(op: str) -> None:
    e = env(3, op=op)
    assert e.op == op
