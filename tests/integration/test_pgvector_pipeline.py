"""Golden pgvector end to end against real S3 and Pinecone, with kills after export, emit, and import.

Needs PGVECTOR_DSN (golden Docker DB), PINECONE_API_KEY, TEST_BUCKET,
PCMIGRATE_STORAGE_INTEGRATION_ID, and AWS credentials. Every run deletes its index and prefix.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest

from pcmigrate.core.state import Manifest, ManifestStore, Stage, StageStatus
from pcmigrate.targets import SdkGateway

pytestmark = [pytest.mark.integration, pytest.mark.slow]

RUNNER = Path(__file__).with_name("pgvector_runner.py")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.fixture
def run_id() -> str:
    for name in (
        "PINECONE_API_KEY",
        "TEST_BUCKET",
        "PCMIGRATE_STORAGE_INTEGRATION_ID",
        "PGVECTOR_DSN",
    ):
        _require(name)
    return uuid.uuid4().hex[:8]


@pytest.fixture
def index_name(run_id: str) -> Iterator[str]:
    name = f"pcm-pg-{run_id}"
    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    try:
        yield name
    finally:
        if gateway.describe_index(name) is not None:
            gateway.delete_index(name)


@pytest.fixture
def bucket_prefix(run_id: str) -> Iterator[tuple[str, str]]:
    bucket = os.environ["TEST_BUCKET"]
    migration_id = f"pg-{run_id}"
    try:
        yield bucket, migration_id
    finally:
        s3 = boto3.client("s3", region_name=REGION)
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{migration_id}/"
        ):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def _spawn(env: dict[str, str], log: Path) -> subprocess.Popen[str]:
    handle = log.open("w")
    return subprocess.Popen(
        [sys.executable, str(RUNNER)],
        env={**os.environ, **env},
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_stage(
    proc: subprocess.Popen[str], log: Path, store: ManifestStore, stage: Stage, timeout_s: float
) -> Manifest:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        manifest = store.load()
        if manifest is not None and manifest.is_complete(stage):
            return manifest
        if proc.poll() is not None:
            tail = log.read_text()[-6000:]
            raise AssertionError(
                f"runner exited with {proc.returncode} before {stage.value} completed:\n{tail}"
            )
        time.sleep(2)
    raise AssertionError(
        f"{stage.value} did not complete within {timeout_s}s:\n{log.read_text()[-3000:]}"
    )


def _kill_after(
    env: dict[str, str], store: ManifestStore, stage: Stage, timeout_s: float, log: Path
) -> Manifest:
    proc = _spawn({**env, "PCMIGRATE_PAUSE_AFTER_STAGE": stage.value}, log)
    try:
        return _wait_for_stage(proc, log, store, stage, timeout_s)
    finally:
        proc.kill()
        proc.wait()


def test_golden_documents_with_kills_at_export_emit_and_import(
    index_name: str, bucket_prefix: tuple[str, str], tmp_path: Path
) -> None:
    bucket, migration_id = bucket_prefix
    env = {
        "AWS_REGION": REGION,
        "PCMIGRATE_INDEX": index_name,
        "PCMIGRATE_MIGRATION_ID": migration_id,
        "PCMIGRATE_TABLES": "documents",
        "PCMIGRATE_PARTITION_ROWS": "1000",
        "PCMIGRATE_WORKERS": "3",
        "PCMIGRATE_FIDELITY_SAMPLE": "100",
        "PCMIGRATE_RECALL_QUERIES": "25",
    }
    store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)

    after_export = _kill_after(env, store, Stage.EXPORT, 900, tmp_path / "run1.log")
    assert len(after_export.stage(Stage.EXPORT).complete_partitions) >= 2
    assert after_export.stage(Stage.TRANSFORM).status is StageStatus.PENDING

    after_emit = _kill_after(env, store, Stage.EMIT, 900, tmp_path / "run2.log")
    assert after_emit.is_complete(Stage.TRANSFORM)
    assert after_emit.stage(Stage.TRANSFORM).outputs["duplicates_by_namespace"] == {"documents": 4}
    assert after_emit.stage(Stage.IMPORT).status is StageStatus.PENDING

    after_import = _kill_after(env, store, Stage.IMPORT, 3600, tmp_path / "run3.log")
    assert after_import.stage(Stage.IMPORT).outputs["records_imported"] == 1001
    assert after_import.stage(Stage.IMPORT).outputs["duplicates_upserted"] == {"documents": 4}

    final_log = tmp_path / "run4.log"
    final_run = _spawn(env, final_log)
    final_run.wait(timeout=3600)
    output = final_log.read_text()
    assert final_run.returncode == 0, output[-6000:]

    final = store.load()
    assert final is not None and final.is_complete(Stage.VALIDATE)
    checks = final.stage(Stage.VALIDATE).outputs["checks"]
    assert all(checks.values()), checks
    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    skipped = {e["stage"] for e in events if e["type"] == "stage_skipped"}
    assert {"prepare_index", "export", "transform", "emit", "import"} <= skipped

    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    info = gateway.describe_index(index_name)
    assert info is not None and info.dimension == 768 and info.metric == "cosine"
    assert gateway.namespace_counts(info.host) == {"documents": 1005}
