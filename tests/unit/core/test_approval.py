from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, PlanLock, schema_fingerprint
from pcmigrate.core.approval import (
    ApprovalRequiredError,
    current_approval,
    load_approvals,
    record_approval,
    require_approval,
)
from pcmigrate.core.artifacts import LocalArtifactStore, S3ArtifactStore
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.conftest import BUCKET

FP = schema_fingerprint({"tables": 1})


def test_record_then_require(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    lock = PlanLock.compute(Mapping.from_yaml(EXAMPLE), FP)
    with pytest.raises(ApprovalRequiredError, match="no approval recorded"):
        require_approval(store, lock)
    approval = record_approval(store, lock, "fde@example.com")
    assert require_approval(store, lock) == approval
    assert (tmp_path / "manifest" / "approvals.json").exists()


def test_editing_mapping_invalidates_approval(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    lock = PlanLock.compute(Mapping.from_yaml(EXAMPLE), FP)
    record_approval(store, lock, "fde@example.com")
    edited = PlanLock.compute(
        Mapping.from_yaml(EXAMPLE.replace("threshold: 0.95", "threshold: 0.9")), FP
    )
    assert current_approval(store, edited) is None
    with pytest.raises(ApprovalRequiredError, match=r"latest approval by fde@example\.com"):
        require_approval(store, edited)
    record_approval(store, edited, "lead@example.com")
    assert len(load_approvals(store).approvals) == 2
    assert require_approval(store, edited).approver == "lead@example.com"
    assert require_approval(store, lock).approver == "fde@example.com"


def test_schema_change_invalidates_approval(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    mapping = Mapping.from_yaml(EXAMPLE)
    record_approval(store, PlanLock.compute(mapping, FP), "fde@example.com")
    with pytest.raises(ApprovalRequiredError):
        require_approval(store, PlanLock.compute(mapping, schema_fingerprint({"tables": 2})))


def test_s3_store_round_trip(s3: S3Client) -> None:
    store = S3ArtifactStore(s3, BUCKET, "mig")
    assert store.read_text("plan.lock") is None
    assert store.write_text("plan.lock", "x") == f"s3://{BUCKET}/mig/plan.lock"
    assert store.read_text("plan.lock") == "x"
    lock = PlanLock.compute(Mapping.from_yaml(EXAMPLE), FP)
    record_approval(store, lock, "fde@example.com")
    assert require_approval(store, lock).approver == "fde@example.com"
