"""Stage state machine, S3 manifest (manifest/state.json), and single-writer lock (ADR-006)."""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, Self

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.core.s3 import is_not_found, is_precondition_failed

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

MANIFEST_VERSION = 1


def _now() -> datetime:
    return datetime.now(UTC)


class Stage(StrEnum):
    """Pipeline stages in ADR-006 order."""

    INTROSPECT = "introspect"
    APPROVE = "approve"
    PROVISION = "provision"
    PREPARE_INDEX = "prepare_index"
    EXPORT = "export"
    TRANSFORM = "transform"
    EMIT = "emit"
    IMPORT = "import"
    VALIDATE = "validate"
    SYNC = "sync"
    CUTOVER = "cutover"
    TEARDOWN = "teardown"

    @classmethod
    def ordered(cls) -> tuple[Stage, ...]:
        """All stages in pipeline order."""
        return tuple(cls)

    @property
    def position(self) -> int:
        """Position in the pipeline, starting at zero."""
        return Stage.ordered().index(self)

    def next(self) -> Stage | None:
        """The stage after this one, or None after teardown."""
        order = Stage.ordered()
        return order[self.position + 1] if self.position + 1 < len(order) else None

    def precedes(self, other: Stage) -> bool:
        """True when this stage runs before *other*."""
        return self.position < other.position


class StageStatus(StrEnum):
    """Lifecycle of one stage or partition."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class PartitionProgress(BaseModel):
    """Per-partition progress inside a stage."""

    model_config = ConfigDict(extra="forbid")

    status: StageStatus = StageStatus.PENDING
    rows: int = 0
    parts: list[str] = Field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None


class StageRecord(BaseModel):
    """Status, timing, partitions, and outputs of one stage."""

    model_config = ConfigDict(extra="forbid")

    status: StageStatus = StageStatus.PENDING
    attempts: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    partitions: dict[str, PartitionProgress] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)

    @property
    def complete_partitions(self) -> list[str]:
        """Ids of partitions that finished."""
        return [k for k, v in self.partitions.items() if v.status is StageStatus.COMPLETE]


class Manifest(BaseModel):
    """The run's durable state, stored at manifest/state.json."""

    model_config = ConfigDict(extra="forbid")

    version: int = MANIFEST_VERSION
    migration_id: str
    plan_hash: str | None = None
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)
    stages: dict[Stage, StageRecord] = Field(
        default_factory=lambda: {s: StageRecord() for s in Stage.ordered()}
    )

    def stage(self, stage: Stage) -> StageRecord:
        """The record for *stage*, created on first access."""
        return self.stages.setdefault(stage, StageRecord())

    def is_complete(self, stage: Stage) -> bool:
        """True when *stage* finished."""
        return self.stage(stage).status is StageStatus.COMPLETE

    def next_stage(self) -> Stage | None:
        """The first stage that has not completed, or None when every stage has."""
        for stage in Stage.ordered():
            if not self.is_complete(stage):
                return stage
        return None

    def mark_running(self, stage: Stage) -> StageRecord:
        """Record a new attempt at *stage*."""
        record = self.stage(stage)
        record.status = StageStatus.RUNNING
        record.attempts += 1
        record.started_at = _now()
        record.finished_at = None
        record.error = None
        self.updated_at = _now()
        return record

    def mark_complete(self, stage: Stage, **outputs: Any) -> StageRecord:
        """Record that *stage* finished, merging *outputs* into its record."""
        record = self.stage(stage)
        record.status = StageStatus.COMPLETE
        record.finished_at = _now()
        record.error = None
        record.outputs.update(outputs)
        self.updated_at = _now()
        return record

    def mark_failed(self, stage: Stage, error: str) -> StageRecord:
        """Record that *stage* failed with *error*."""
        record = self.stage(stage)
        record.status = StageStatus.FAILED
        record.finished_at = _now()
        record.error = error
        self.updated_at = _now()
        return record

    def partition(self, stage: Stage, partition_id: str) -> PartitionProgress:
        """Progress for one partition of *stage*, created on first access."""
        return self.stage(stage).partitions.setdefault(partition_id, PartitionProgress())

    def mark_partition_complete(
        self, stage: Stage, partition_id: str, *, rows: int, parts: list[str]
    ) -> PartitionProgress:
        """Record that one partition of *stage* finished."""
        progress = self.partition(stage, partition_id)
        progress.status = StageStatus.COMPLETE
        progress.rows = rows
        progress.parts = list(parts)
        progress.finished_at = _now()
        progress.error = None
        self.updated_at = _now()
        return progress


class ManifestConflictError(RuntimeError):
    """Another writer changed the manifest since it was loaded."""


class ManifestStore:
    """Loads and saves the manifest with S3 conditional writes."""

    def __init__(self, s3: S3Client, bucket: str, migration_id: str) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._key = f"{migration_id}/manifest/state.json"
        self._etag: str | None = None

    @property
    def key(self) -> str:
        """Object key of state.json."""
        return self._key

    @property
    def etag(self) -> str | None:
        """ETag of the last loaded or saved version, None before the first save."""
        return self._etag

    def load(self) -> Manifest | None:
        """Fetch the manifest, or None when the run has no state yet."""
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=self._key)
        except ClientError as err:
            if is_not_found(err):
                self._etag = None
                return None
            raise
        self._etag = response["ETag"]
        return Manifest.model_validate_json(response["Body"].read())

    def save(self, manifest: Manifest) -> None:
        """Write the manifest. Creates with If-None-Match, updates with If-Match."""
        manifest.updated_at = _now()
        body = manifest.model_dump_json(indent=2).encode("utf-8")
        condition: dict[str, str] = (
            {"IfNoneMatch": "*"} if self._etag is None else {"IfMatch": self._etag}
        )
        try:
            response = self._s3.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=body,
                ContentType="application/json",
                **condition,  # type: ignore[arg-type]
            )
        except ClientError as err:
            if is_precondition_failed(err):
                raise ManifestConflictError(
                    f"{self._key} changed since it was loaded; reload and retry"
                ) from err
            raise
        self._etag = response["ETag"]


class LockRecord(BaseModel):
    """Contents of manifest/lock.json."""

    model_config = ConfigDict(extra="forbid")

    owner: str
    acquired_at: datetime
    expires_at: datetime

    def expired(self, at: datetime | None = None) -> bool:
        """True once the holder's lease has lapsed."""
        return (at or _now()) >= self.expires_at


class LockHeldError(RuntimeError):
    """Another live writer holds the run lock."""

    def __init__(self, record: LockRecord) -> None:
        super().__init__(f"run lock held by {record.owner} until {record.expires_at.isoformat()}")
        self.record = record


def default_owner() -> str:
    """hostname:pid, enough to tell writers apart in a lock record."""
    return f"{socket.gethostname()}:{os.getpid()}"


class S3Lock:
    """Single-writer lease at manifest/lock.json using S3 conditional writes."""

    def __init__(
        self,
        s3: S3Client,
        bucket: str,
        migration_id: str,
        *,
        owner: str | None = None,
        ttl: timedelta = timedelta(minutes=5),
        wait_for_expiry: bool = True,
        poll_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._key = f"{migration_id}/manifest/lock.json"
        self._owner = owner or default_owner()
        self._ttl = ttl
        self._wait = wait_for_expiry
        self._poll = poll_s
        self._sleep = sleep
        self._clock = clock
        self._etag: str | None = None

    @property
    def key(self) -> str:
        """Object key of lock.json."""
        return self._key

    @property
    def owner(self) -> str:
        """This writer's identity."""
        return self._owner

    @property
    def held(self) -> bool:
        """True while this instance holds the lease."""
        return self._etag is not None

    def _record(self) -> LockRecord:
        now = _now()
        return LockRecord(owner=self._owner, acquired_at=now, expires_at=now + self._ttl)

    def _put(self, record: LockRecord, **condition: str) -> None:
        response = self._s3.put_object(
            Bucket=self._bucket,
            Key=self._key,
            Body=record.model_dump_json().encode("utf-8"),
            ContentType="application/json",
            **condition,  # type: ignore[arg-type]
        )
        self._etag = response["ETag"]

    def current(self) -> tuple[LockRecord, str] | None:
        """The lock record and its ETag, or None when nobody holds the lock."""
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=self._key)
        except ClientError as err:
            if is_not_found(err):
                return None
            raise
        return LockRecord.model_validate_json(response["Body"].read()), response["ETag"]

    def acquire(self) -> None:
        """Take the lock, or take over an expired lease.

        A live lease belongs to a writer that may have died a moment ago: its record stays
        until the TTL lapses. With ``wait_for_expiry`` the call waits for that lapse, bounded by
        one TTL, so a resume after a crash succeeds without an operator deleting the lock. A
        holder that keeps refreshing its lease never expires, and LockHeldError is raised once
        the bound is reached.
        """
        deadline = self._clock() + self._ttl.total_seconds() + self._poll
        while True:
            try:
                self._try_acquire()
                return
            except LockHeldError as err:
                if not self._wait or self._clock() >= deadline:
                    raise
                remaining = (err.record.expires_at - _now()).total_seconds()
                self._sleep(max(0.1, min(self._poll, remaining)))

    def _try_acquire(self) -> None:
        try:
            self._put(self._record(), IfNoneMatch="*")
            return
        except ClientError as err:
            if not is_precondition_failed(err):
                raise
        existing = self.current()
        if existing is None:
            self._put(self._record(), IfNoneMatch="*")
            return
        record, etag = existing
        if not record.expired():
            raise LockHeldError(record)
        try:
            self._put(self._record(), IfMatch=etag)
        except ClientError as err:
            if is_precondition_failed(err):
                refreshed = self.current()
                raise LockHeldError(refreshed[0] if refreshed else record) from err
            raise

    def refresh(self) -> None:
        """Extend the lease. Raises LockHeldError if the lease was lost."""
        if self._etag is None:
            raise RuntimeError("refresh() called without holding the lock")
        try:
            self._put(self._record(), IfMatch=self._etag)
        except ClientError as err:
            if is_precondition_failed(err):
                self._etag = None
                existing = self.current()
                raise LockHeldError(existing[0] if existing else self._record()) from err
            raise

    def release(self) -> None:
        """Delete the lock object if this instance still holds it."""
        if self._etag is None:
            return
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=self._key, IfMatch=self._etag)
        except ClientError as err:
            if not (is_precondition_failed(err) or is_not_found(err)):
                raise
        finally:
            self._etag = None

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class StageProtocol(Protocol):
    """What every stage module exposes: an identity, a done-check, and run()."""

    @property
    def name(self) -> Stage:
        """Which pipeline stage this implements."""
        ...

    def is_complete(self, manifest: Manifest) -> bool:
        """True when the manifest shows no work left for this stage."""
        ...

    def run(self, ctx: Any) -> dict[str, Any]:
        """Do the stage's work and return outputs to record in the manifest."""
        ...
