"""SyntheticSource (10k rows) through canonical, emit, import, and count validation.

Needs PINECONE_API_KEY, TEST_BUCKET, PCMIGRATE_STORAGE_INTEGRATION_ID, and AWS credentials
for that bucket. The run is killed
after export and resumed; the index is deleted afterwards, whatever happened.
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

RUNNER = Path(__file__).with_name("synthetic_runner.py")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.fixture
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture
def index_name(run_id: str) -> Iterator[str]:
    api_key = _require("PINECONE_API_KEY")
    name = f"pcm-syn-{run_id}"
    gateway = SdkGateway(api_key)
    try:
        yield name
    finally:
        if gateway.describe_index(name) is not None:
            gateway.delete_index(name)


@pytest.fixture
def bucket_prefix(run_id: str) -> Iterator[tuple[str, str]]:
    bucket = _require("TEST_BUCKET")
    migration_id = f"syn-{run_id}"
    try:
        yield bucket, migration_id
    finally:
        s3 = boto3.client("s3", region_name=REGION)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{migration_id}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def _spawn(env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, str(RUNNER)],
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_stage(store: ManifestStore, stage: Stage, timeout_s: float) -> Manifest:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        manifest = store.load()
        if manifest is not None and manifest.is_complete(stage):
            return manifest
        time.sleep(2)
    raise AssertionError(f"{stage.value} did not complete within {timeout_s}s")


def test_kill_after_export_then_resume(index_name: str, bucket_prefix: tuple[str, str]) -> None:
    _require("PCMIGRATE_STORAGE_INTEGRATION_ID")
    bucket, migration_id = bucket_prefix
    env = {
        "AWS_REGION": REGION,
        "PCMIGRATE_INDEX": index_name,
        "PCMIGRATE_MIGRATION_ID": migration_id,
    }
    s3 = boto3.client("s3", region_name=REGION)
    store = ManifestStore(s3, bucket, migration_id)

    first = _spawn({**env, "PCMIGRATE_PAUSE_AFTER_STAGE": "export"})
    try:
        manifest = _wait_for_stage(store, Stage.EXPORT, timeout_s=900)
    finally:
        first.kill()
        first.wait()
    assert manifest.is_complete(Stage.PREPARE_INDEX)
    assert len(manifest.stage(Stage.EXPORT).complete_partitions) == 4
    assert manifest.stage(Stage.EMIT).status is StageStatus.PENDING

    second = _spawn(env)
    output, _ = second.communicate(timeout=3600)
    assert second.returncode == 0, output[-4000:]

    final = store.load()
    assert final is not None
    assert final.is_complete(Stage.VALIDATE)
    assert final.stage(Stage.EXPORT).outputs["rows"] == 10_000
    assert final.stage(Stage.IMPORT).outputs["records_imported"] == 10_000
    assert final.stage(Stage.VALIDATE).outputs["count_parity"]["passed"] is True

    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    skipped = {e["stage"] for e in events if e["type"] == "stage_skipped"}
    assert {"prepare_index", "export"} <= skipped

    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    info = gateway.describe_index(index_name)
    assert info is not None and info.dimension == 128
    assert gateway.namespace_counts(info.host) == {"documents": 10_000}


def test_dedicated_read_nodes_end_to_end(index_name: str, bucket_prefix: tuple[str, str]) -> None:
    """Create the index with dedicated read nodes, import, validate; opt in with PCMIGRATE_TEST_DEDICATED=1."""
    if os.environ.get("PCMIGRATE_TEST_DEDICATED") != "1":
        pytest.skip("set PCMIGRATE_TEST_DEDICATED=1 to create a billed dedicated-read-node index")
    _require("PCMIGRATE_STORAGE_INTEGRATION_ID")
    bucket, migration_id = bucket_prefix
    namespace = os.environ.get("PCMIGRATE_NAMESPACE", "documents")
    env = {
        "AWS_REGION": REGION,
        "PCMIGRATE_INDEX": index_name,
        "PCMIGRATE_MIGRATION_ID": migration_id,
        "PCMIGRATE_CAPACITY": "dedicated",
        "PCMIGRATE_NODE_TYPE": os.environ.get("PCMIGRATE_NODE_TYPE", "b1"),
        "PCMIGRATE_NAMESPACE": namespace,
    }
    run = _spawn(env)
    output, _ = run.communicate(timeout=5400)
    assert run.returncode == 0, output[-4000:]
    store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)
    final = store.load()
    assert final is not None and final.is_complete(Stage.VALIDATE)
    assert final.stage(Stage.IMPORT).outputs["records_imported"] == 10_000
    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    info = gateway.describe_index(index_name)
    assert info is not None and info.read_capacity is not None
    assert info.read_capacity.dedicated and info.read_capacity.state == "Ready"
    assert info.read_capacity.node_type == env["PCMIGRATE_NODE_TYPE"]
    assert gateway.namespace_counts(info.host) == {namespace: 10_000}
    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    assert any("waiting for dedicated read nodes" in e.get("message", "") for e in events)


def test_hybrid_document_index_end_to_end(index_name: str, bucket_prefix: tuple[str, str]) -> None:
    """Dense plus sparse into a document index through JSONL.gz import; hybrid checks pass."""
    _require("PCMIGRATE_STORAGE_INTEGRATION_ID")
    bucket, migration_id = bucket_prefix
    env = {
        "AWS_REGION": REGION,
        "PCMIGRATE_INDEX": index_name,
        "PCMIGRATE_MIGRATION_ID": migration_id,
        "PCMIGRATE_HYBRID": "1",
    }
    run = _spawn(env)
    output, _ = run.communicate(timeout=5400)
    assert run.returncode == 0, output[-4000:]
    store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)
    final = store.load()
    assert final is not None and final.is_complete(Stage.VALIDATE)
    prepared = final.stage(Stage.PREPARE_INDEX).outputs
    assert prepared["type"] == "document" and prepared["sparse_field"] == "sparse"
    assert final.stage(Stage.EMIT).outputs["format"] == "jsonl.gz"
    assert final.stage(Stage.IMPORT).outputs["records_imported"] == 10_000
    checks = final.stage(Stage.VALIDATE).outputs["checks"]
    assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
    assert all(checks.values()), checks
    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    info = gateway.describe_index(index_name)
    assert info is not None and info.document and info.schema is not None
    assert info.schema.dense_field == "dense" and info.schema.sparse_field == "sparse"
    assert gateway.namespace_counts(info.host) == {"documents": 10_000}
    sample = gateway.fetch(info.host, "documents", ["documents#7"])["documents#7"]
    assert sample.sparse is not None and len(sample.sparse.indices) == 8


def test_generated_sparse_end_to_end(index_name: str, bucket_prefix: tuple[str, str]) -> None:
    """Sparse vectors generated by Pinecone Inference from synthetic text; hybrid checks pass."""
    _require("PCMIGRATE_STORAGE_INTEGRATION_ID")
    bucket, migration_id = bucket_prefix
    env = {
        "AWS_REGION": REGION,
        "PCMIGRATE_INDEX": index_name,
        "PCMIGRATE_MIGRATION_ID": migration_id,
        "PCMIGRATE_GENERATED_SPARSE": "1",
    }
    run = _spawn(env)
    output, _ = run.communicate(timeout=5400)
    assert run.returncode == 0, output[-4000:]
    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    encoded = [e for e in events if e.get("type") == "sparse_encoded"]
    assert encoded and sum(e["records"] for e in encoded) == 10_000
    assert sum(e["tokens"] for e in encoded) > 0 and all(e["no_text"] == 0 for e in encoded)
    store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)
    final = store.load()
    assert final is not None and final.is_complete(Stage.VALIDATE)
    transform = final.stage(Stage.TRANSFORM).outputs
    stats = transform["sparse_by_namespace"]["documents"]
    assert stats["generated"] == 10_000 and stats["cached"] == 0 and stats["requests"] >= 105
    assert final.stage(Stage.IMPORT).outputs["records_imported"] == 10_000
    validate = final.stage(Stage.VALIDATE).outputs
    checks = validate["checks"]
    assert checks["sparse_overlap:documents"] and checks["hybrid_overlap:documents"], checks
    assert all(checks.values()), checks
    assert validate["sparse_models"] == {"documents": "pinecone-sparse-english-v0"}
    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    info = gateway.describe_index(index_name)
    assert info is not None and info.document and info.schema is not None
    assert info.schema.sparse_field == "sparse"
    # the stored sparse vector is what a fresh encoding of the same text produces; the model's
    # values move by ~1e-3 with the batch a text was encoded in, so compare at 1% relative
    from pcmigrate.sources.synthetic import SyntheticSource
    from pcmigrate.targets import PineconeSparseEncoder

    source = SyntheticSource(rows=10_000, dimension=128, partitions=4, text_words=12)
    record = source.record(7)
    text = "\n".join(record.text_fields[f] for f in ("title", "body"))
    (fresh,) = PineconeSparseEncoder(os.environ["PINECONE_API_KEY"]).encode(
        [text], input_type="passage"
    )
    stored = gateway.fetch(info.host, "documents", ["documents#7"])["documents#7"]
    assert stored.sparse is not None and stored.sparse.indices == fresh.indices
    assert all(
        abs(a - b) <= 0.01 * max(abs(a), 1e-6)
        for a, b in zip(stored.sparse.values, fresh.values, strict=True)
    )
