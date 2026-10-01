from datetime import UTC, datetime, timedelta

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.core.state import (
    LockHeldError,
    LockRecord,
    Manifest,
    ManifestConflictError,
    ManifestStore,
    S3Lock,
    Stage,
    StageStatus,
)
from tests.unit.conftest import BUCKET

MIG = "mig-1"


def test_stage_order_matches_adr_006() -> None:
    assert [s.value for s in Stage.ordered()] == [
        "introspect",
        "approve",
        "provision",
        "prepare_index",
        "export",
        "transform",
        "emit",
        "import",
        "validate",
        "sync",
        "cutover",
        "teardown",
    ]
    assert Stage.INTROSPECT.next() is Stage.APPROVE
    assert Stage.TEARDOWN.next() is None
    assert Stage.EXPORT.precedes(Stage.EMIT)
    assert not Stage.EMIT.precedes(Stage.EXPORT)
    assert Stage.IMPORT.position == 7


def test_manifest_tracks_stage_lifecycle() -> None:
    m = Manifest(migration_id=MIG)
    assert m.next_stage() is Stage.INTROSPECT
    m.mark_complete(Stage.INTROSPECT)
    m.mark_complete(Stage.APPROVE, approver="fde@example.com")
    assert m.next_stage() is Stage.PROVISION
    rec = m.mark_running(Stage.PROVISION)
    assert rec.attempts == 1 and rec.status is StageStatus.RUNNING
    m.mark_failed(Stage.PROVISION, "boom")
    assert m.stage(Stage.PROVISION).error == "boom"
    m.mark_running(Stage.PROVISION)
    assert m.stage(Stage.PROVISION).attempts == 2
    assert m.stage(Stage.PROVISION).error is None
    assert m.stage(Stage.APPROVE).outputs == {"approver": "fde@example.com"}


def test_manifest_partitions() -> None:
    m = Manifest(migration_id=MIG)
    m.mark_partition_complete(Stage.EXPORT, "p0", rows=10, parts=["a.parquet"])
    m.partition(Stage.EXPORT, "p1")
    assert m.stage(Stage.EXPORT).complete_partitions == ["p0"]
    again = Manifest.model_validate_json(m.model_dump_json())
    assert again.stage(Stage.EXPORT).partitions["p0"].rows == 10


def test_store_creates_then_updates(s3: S3Client) -> None:
    store = ManifestStore(s3, BUCKET, MIG)
    assert store.load() is None
    m = Manifest(migration_id=MIG)
    store.save(m)
    first_etag = store.etag
    m.mark_complete(Stage.INTROSPECT)
    store.save(m)
    assert store.etag != first_etag
    loaded = ManifestStore(s3, BUCKET, MIG).load()
    assert loaded is not None and loaded.is_complete(Stage.INTROSPECT)


def test_store_create_conflicts_if_object_exists(s3: S3Client) -> None:
    ManifestStore(s3, BUCKET, MIG).save(Manifest(migration_id=MIG))
    other = ManifestStore(s3, BUCKET, MIG)
    with pytest.raises(ManifestConflictError):
        other.save(Manifest(migration_id=MIG))


def test_store_update_conflicts_on_stale_etag(s3: S3Client) -> None:
    a = ManifestStore(s3, BUCKET, MIG)
    a.save(Manifest(migration_id=MIG))
    b = ManifestStore(s3, BUCKET, MIG)
    mb = b.load()
    assert mb is not None
    b.save(mb)
    with pytest.raises(ManifestConflictError):
        a.save(Manifest(migration_id=MIG))


def test_lock_excludes_second_writer(s3: S3Client) -> None:
    first = S3Lock(s3, BUCKET, MIG, owner="a")
    second = S3Lock(s3, BUCKET, MIG, owner="b")
    first.acquire()
    with pytest.raises(LockHeldError) as info:
        second.acquire()
    assert info.value.record.owner == "a"
    first.release()
    second.acquire()
    assert second.held
    second.release()
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{MIG}/manifest/lock").get("KeyCount", 0) == 0


def test_expired_lock_can_be_taken_over(s3: S3Client) -> None:
    stale = S3Lock(s3, BUCKET, MIG, owner="dead", ttl=timedelta(seconds=-1))
    stale.acquire()
    fresh = S3Lock(s3, BUCKET, MIG, owner="alive")
    fresh.acquire()
    current = fresh.current()
    assert current is not None and current[0].owner == "alive"
    stale.release()
    assert fresh.current() is not None


def test_refresh_detects_lost_lease(s3: S3Client) -> None:
    a = S3Lock(s3, BUCKET, MIG, owner="a", ttl=timedelta(seconds=-1))
    a.acquire()
    b = S3Lock(s3, BUCKET, MIG, owner="b")
    b.acquire()
    with pytest.raises(LockHeldError):
        a.refresh()
    b.refresh()
    b.release()


def test_lock_context_manager(s3: S3Client) -> None:
    with S3Lock(s3, BUCKET, MIG, owner="ctx") as lock:
        assert lock.held
    assert not lock.held


def test_lock_record_expiry() -> None:
    now = datetime.now(UTC)
    rec = LockRecord(owner="x", acquired_at=now, expires_at=now + timedelta(seconds=10))
    assert not rec.expired(now)
    assert rec.expired(now + timedelta(seconds=11))
