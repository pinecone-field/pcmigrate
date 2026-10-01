import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3 import S3Client

BUCKET = "pcmigrate-unit"
REGION = "us-east-1"


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    os.environ.pop("AWS_PROFILE", None)


@pytest.fixture
def s3() -> Iterator[S3Client]:
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


def approve_ctx(ctx: object, tmp_path: Path, fingerprint: str = "test-schema") -> None:
    """Give a RunContext a plan lock and a matching approval so post-approve stages run."""
    from pcmigrate.config.plan_lock import PlanLock, schema_fingerprint
    from pcmigrate.core.approval import record_approval
    from pcmigrate.core.artifacts import LocalArtifactStore

    store = LocalArtifactStore(tmp_path / "artifacts")
    lock = PlanLock.compute(ctx.mapping, schema_fingerprint({"schema": fingerprint}))  # type: ignore[attr-defined]
    record_approval(store, lock, "tests@example.com")
    ctx.plan_lock = lock  # type: ignore[attr-defined]
    ctx.approvals = store  # type: ignore[attr-defined]
