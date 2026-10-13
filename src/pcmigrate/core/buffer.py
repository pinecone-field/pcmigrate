"""The durable change buffer between change capture (or dual-write) and the applier (ADR-007).

Every post-snapshot change is one ChangeEnvelope. SQS FIFO is the default: MessageGroupId is
the record id so one record's changes stay ordered, and MessageDeduplicationId is the
source change id so a retried send is dropped. Kinesis sits behind the same interface for
high-volume sources but is not wired to infra yet.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from mypy_boto3_kinesis import KinesisClient
    from mypy_boto3_sqs import SQSClient

SQS_MAX_BATCH = 10
SQS_MAX_MESSAGE_BYTES = 256 * 1024
KINESIS_MAX_BATCH = 500


class ChangeEnvelope(BaseModel):
    """One change to one record. Dual-writes carry the payload; CDC may carry only the key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    op: Literal["upsert", "delete"]
    id: str
    namespace: str
    source_change_id: str
    source_pk: str | None = None
    sequence: int | None = None
    values: list[float] | None = None
    metadata: dict[str, Any] | None = None
    text_fields: dict[str, str] | None = None
    origin: Literal["cdc", "dual_write", "reconcile"] = "cdc"
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def to_json(self) -> str:
        """Wire form."""
        return self.model_dump_json(exclude_none=True)

    @classmethod
    def from_json(cls, text: str) -> ChangeEnvelope:
        """Inverse of to_json."""
        return cls.model_validate_json(text)

    @property
    def needs_reread(self) -> bool:
        """True when the applier must fetch the current row before upserting."""
        return self.op == "upsert" and self.values is None


@dataclass(frozen=True, slots=True)
class Received:
    """A message pulled from the buffer, with the handle needed to acknowledge it."""

    envelope: ChangeEnvelope
    receipt: str
    sent_at: datetime | None = None

    def age_s(self, now: datetime | None = None) -> float | None:
        """Seconds since the message was sent, when the buffer reports it."""
        if self.sent_at is None:
            return None
        return ((now or datetime.now(UTC)) - self.sent_at).total_seconds()


class ChangeBuffer(Protocol):
    """What the forwarder, the write path, and the applier use."""

    def send(self, envelopes: Sequence[ChangeEnvelope]) -> int:
        """Enqueue; returns how many were accepted."""
        ...

    def receive(self, *, max_messages: int = 10, wait_s: float = 0.0) -> list[Received]:
        """Pull up to max_messages, waiting up to wait_s for the first."""
        ...

    def ack(self, receipts: Sequence[str]) -> None:
        """Delete acknowledged messages. Only after Pinecone confirmed the write."""
        ...

    def approximate_depth(self) -> int:
        """Messages waiting, as the service estimates it."""
        ...


class InMemoryBuffer:
    """A FIFO in memory, for unit tests and the Docker integration tests."""

    def __init__(self) -> None:
        self._queue: deque[tuple[str, ChangeEnvelope, datetime]] = deque()
        self._inflight: dict[str, tuple[ChangeEnvelope, datetime]] = {}
        self._seen: set[str] = set()
        self.sent: list[ChangeEnvelope] = []

    def send(self, envelopes: Sequence[ChangeEnvelope]) -> int:
        """Deduplicate on source_change_id like SQS FIFO does."""
        accepted = 0
        for env in envelopes:
            if env.source_change_id in self._seen:
                continue
            self._seen.add(env.source_change_id)
            self._queue.append((uuid.uuid4().hex, env, datetime.now(UTC)))
            self.sent.append(env)
            accepted += 1
        return accepted

    def receive(self, *, max_messages: int = 10, wait_s: float = 0.0) -> list[Received]:
        """Pop up to max_messages; wait_s is ignored."""
        out: list[Received] = []
        while self._queue and len(out) < max_messages:
            receipt, env, sent_at = self._queue.popleft()
            self._inflight[receipt] = (env, sent_at)
            out.append(Received(env, receipt, sent_at))
        return out

    def ack(self, receipts: Sequence[str]) -> None:
        """Forget acknowledged messages."""
        for receipt in receipts:
            self._inflight.pop(receipt, None)

    def requeue_inflight(self) -> None:
        """Put unacknowledged messages back, as a visibility timeout would."""
        for receipt, (env, sent_at) in list(self._inflight.items()):
            self._queue.appendleft((receipt, env, sent_at))
        self._inflight.clear()

    def approximate_depth(self) -> int:
        """Waiting plus in flight."""
        return len(self._queue) + len(self._inflight)


class SqsFifoBuffer:
    """SQS FIFO: ordered per record id, deduplicated per source change id."""

    def __init__(self, sqs: SQSClient, queue_url: str, *, visibility_timeout_s: int = 120) -> None:
        self._sqs = sqs
        self._url = queue_url
        self._visibility = visibility_timeout_s

    @property
    def queue_url(self) -> str:
        """The queue."""
        return self._url

    def send(self, envelopes: Sequence[ChangeEnvelope]) -> int:
        """SendMessageBatch in groups of ten."""
        accepted = 0
        for start in range(0, len(envelopes), SQS_MAX_BATCH):
            chunk = envelopes[start : start + SQS_MAX_BATCH]
            entries = [
                {
                    "Id": str(i),
                    "MessageBody": env.to_json(),
                    "MessageGroupId": _group_id(env),
                    "MessageDeduplicationId": _dedup_id(env),
                }
                for i, env in enumerate(chunk)
            ]
            response = self._sqs.send_message_batch(QueueUrl=self._url, Entries=entries)  # type: ignore[arg-type]
            failed = response.get("Failed", [])
            if failed:
                raise RuntimeError(
                    f"SQS rejected {len(failed)} messages: {failed[0].get('Message')}"
                )
            accepted += len(response.get("Successful", []))
        return accepted

    def receive(self, *, max_messages: int = 10, wait_s: float = 0.0) -> list[Received]:
        """ReceiveMessage with SentTimestamp for lag."""
        response = self._sqs.receive_message(
            QueueUrl=self._url,
            MaxNumberOfMessages=min(10, max_messages),
            WaitTimeSeconds=int(min(20, wait_s)),
            VisibilityTimeout=self._visibility,
            AttributeNames=["SentTimestamp"],
        )
        out: list[Received] = []
        for message in response.get("Messages", []):
            sent_ms = message.get("Attributes", {}).get("SentTimestamp")
            sent_at = datetime.fromtimestamp(int(sent_ms) / 1000, UTC) if sent_ms else None
            out.append(
                Received(
                    ChangeEnvelope.from_json(message["Body"]), message["ReceiptHandle"], sent_at
                )
            )
        return out

    def ack(self, receipts: Sequence[str]) -> None:
        """DeleteMessageBatch in groups of ten."""
        for start in range(0, len(receipts), SQS_MAX_BATCH):
            chunk = receipts[start : start + SQS_MAX_BATCH]
            self._sqs.delete_message_batch(
                QueueUrl=self._url,
                Entries=[{"Id": str(i), "ReceiptHandle": r} for i, r in enumerate(chunk)],
            )

    def approximate_depth(self) -> int:
        """ApproximateNumberOfMessages plus in flight."""
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=self._url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        return int(attrs.get("ApproximateNumberOfMessages", 0)) + int(
            attrs.get("ApproximateNumberOfMessagesNotVisible", 0)
        )


def _group_id(env: ChangeEnvelope) -> str:
    return f"{env.namespace}/{env.id}"[:128]


def _dedup_id(env: ChangeEnvelope) -> str:
    import hashlib

    raw = f"{env.namespace}/{env.id}/{env.source_change_id}"
    return (
        raw if len(raw) <= 128 and raw.isprintable() else hashlib.sha256(raw.encode()).hexdigest()
    )


@dataclass
class KinesisBuffer:
    """Kinesis behind the ChangeBuffer interface: partition key is the record id.

    Kinesis has no server-side dedup or delete, so this reader keeps shard iterators and a
    seen-set of source change ids, and ``ack`` advances an in-memory checkpoint. Good enough
    for a single applier; a shared checkpoint store comes with infra wiring later.
    """

    kinesis: KinesisClient
    stream: str
    _iterators: dict[str, str] = field(default_factory=dict)
    _seen: set[str] = field(default_factory=set)
    _pending: dict[str, tuple[str, str]] = field(default_factory=dict)
    checkpoints: dict[str, str] = field(default_factory=dict)

    def send(self, envelopes: Sequence[ChangeEnvelope]) -> int:
        """PutRecords in groups of 500."""
        accepted = 0
        for start in range(0, len(envelopes), KINESIS_MAX_BATCH):
            chunk = envelopes[start : start + KINESIS_MAX_BATCH]
            response = self.kinesis.put_records(
                StreamName=self.stream,
                Records=[
                    {"Data": env.to_json().encode("utf-8"), "PartitionKey": _group_id(env)}
                    for env in chunk
                ],
            )
            accepted += len(chunk) - int(response.get("FailedRecordCount", 0))
        return accepted

    def _shards(self) -> Iterable[str]:
        return [
            s["ShardId"]
            for s in self.kinesis.describe_stream(StreamName=self.stream)["StreamDescription"][
                "Shards"
            ]
        ]

    def _iterator(self, shard: str) -> str:
        if shard not in self._iterators:
            kwargs: dict[str, Any] = {"StreamName": self.stream, "ShardId": shard}
            if shard in self.checkpoints:
                kwargs.update(
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=self.checkpoints[shard],
                )
            else:
                kwargs.update(ShardIteratorType="TRIM_HORIZON")
            self._iterators[shard] = self.kinesis.get_shard_iterator(**kwargs)["ShardIterator"]
        return self._iterators[shard]

    def receive(self, *, max_messages: int = 10, wait_s: float = 0.0) -> list[Received]:
        """Round-robin GetRecords over shards."""
        deadline = time.monotonic() + wait_s
        out: list[Received] = []
        while True:
            for shard in self._shards():
                response = self.kinesis.get_records(
                    ShardIterator=self._iterator(shard), Limit=max_messages
                )
                self._iterators[shard] = response.get("NextShardIterator", self._iterators[shard])
                for record in response.get("Records", []):
                    env = ChangeEnvelope.from_json(record["Data"].decode("utf-8"))
                    if env.source_change_id in self._seen:
                        continue
                    receipt = f"{shard}:{record['SequenceNumber']}"
                    self._pending[receipt] = (shard, record["SequenceNumber"])
                    out.append(Received(env, receipt, record.get("ApproximateArrivalTimestamp")))
                    if len(out) >= max_messages:
                        return out
            if out or time.monotonic() >= deadline:
                return out
            time.sleep(0.2)

    def ack(self, receipts: Sequence[str]) -> None:
        """Advance the per-shard checkpoint and remember the change ids."""
        for receipt in receipts:
            pending = self._pending.pop(receipt, None)
            if pending is None:
                continue
            shard, sequence = pending
            current = self.checkpoints.get(shard)
            if current is None or int(sequence) > int(current):
                self.checkpoints[shard] = sequence

    def approximate_depth(self) -> int:
        """Kinesis exposes no depth; report in-flight only."""
        return len(self._pending)


def dual_write_change_id() -> str:
    """A fresh id for a change that has no source sequence."""
    return f"dw-{uuid.uuid4().hex}"


def envelope_bytes(env: ChangeEnvelope) -> int:
    """Approximate wire size, to keep under the SQS 256 KB limit."""
    return len(json.dumps(env.model_dump(mode="json", exclude_none=True)).encode("utf-8"))
