"""Golden OpenSearch index end to end through OSIS, Glue, S3, and Pinecone in the test account.

Needs an Amazon OpenSearch Service domain reachable with the test credentials
(OPENSEARCH_URL, OPENSEARCH_DOMAIN_ARN, OPENSEARCH_DOMAIN_NAME; the test seeds the golden
index into it), PINECONE_API_KEY, TEST_BUCKET, PCMIGRATE_STORAGE_INTEGRATION_ID, Pulumi with
a local backend (PULUMI_CONFIG_PASSPHRASE), and AWS credentials. Set
PCMIGRATE_OPENSEARCH_AWS=1 to opt in: the run creates an OSIS pipeline and a Glue job and
takes 20 to 40 minutes. Every run tears the stack down and deletes its index and prefix.
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
from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient
from pcmigrate.targets import SdkGateway
from tests.fixtures.golden.opensearch.documents import golden_documents, golden_mapping

pytestmark = [pytest.mark.integration, pytest.mark.slow]

RUNNER = Path(__file__).with_name("opensearch_runner.py")
REGION = os.environ.get("AWS_REGION", "us-east-1")
SOURCE_INDEX = "golden-documents"


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.fixture
def run_id() -> str:
    if os.environ.get("PCMIGRATE_OPENSEARCH_AWS") != "1":
        pytest.skip("set PCMIGRATE_OPENSEARCH_AWS=1 to run OSIS and Glue in the test account")
    for name in (
        "PINECONE_API_KEY",
        "TEST_BUCKET",
        "PCMIGRATE_STORAGE_INTEGRATION_ID",
        "OPENSEARCH_URL",
        "OPENSEARCH_DOMAIN_ARN",
        "OPENSEARCH_DOMAIN_NAME",
        "PULUMI_CONFIG_PASSPHRASE",
    ):
        _require(name)
    return uuid.uuid4().hex[:8]


@pytest.fixture
def seeded_index(run_id: str) -> Iterator[str]:
    """The golden index in the AWS domain, created for the run and deleted afterwards."""
    client = OpenSearchClient(
        ConnectionOptions(os.environ["OPENSEARCH_URL"], auth="sigv4", region=REGION),
        read_only=False,
    )
    name = f"{SOURCE_INDEX}-{run_id}"
    try:
        client.request("PUT", f"/{name}", body=golden_mapping())
        docs = golden_documents()
        for start in range(0, len(docs), 200):
            lines = []
            for doc in docs[start : start + 200]:
                lines.append(json.dumps({"index": {"_index": name, "_id": doc["doc_id"]}}))
                lines.append(json.dumps(doc))
            result = client.request(
                "POST", "/_bulk", params={"refresh": "wait_for"}, body="\n".join(lines) + "\n"
            )
            assert not result.get("errors"), result["items"][:2]
        assert client.count(name) == len(docs)
        yield name
    finally:
        try:
            client.request("DELETE", f"/{name}")
        finally:
            client.close()


@pytest.fixture
def index_name(run_id: str) -> Iterator[str]:
    name = f"pcm-os-{run_id}"
    gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
    try:
        yield name
    finally:
        if gateway.describe_index(name) is not None:
            gateway.delete_index(name)


@pytest.fixture
def bucket_prefix(run_id: str) -> Iterator[tuple[str, str]]:
    bucket = os.environ["TEST_BUCKET"]
    migration_id = f"os-{run_id}"
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


def _env(
    index: str, migration_id: str, source_index: str, work_dir: Path, **extra: str
) -> dict[str, str]:
    return {
        **os.environ,
        "PCMIGRATE_INDEX": index,
        "PCMIGRATE_MIGRATION_ID": migration_id,
        "PCMIGRATE_SOURCE_INDEX": source_index,
        "PULUMI_WORK_DIR": str(work_dir),
        "PULUMI_BACKEND_URL": os.environ.get("PULUMI_BACKEND_URL", f"file://{work_dir}/state"),
        **extra,
    }


def _run(env: dict[str, str], log: Path, *, timeout: float) -> int:
    with log.open("a") as handle:
        return subprocess.run(
            [sys.executable, str(RUNNER)],
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        ).returncode


def _spawn(env: dict[str, str], log: Path) -> subprocess.Popen[str]:
    handle = log.open("a")
    return subprocess.Popen(
        [sys.executable, str(RUNNER)], env=env, stdout=handle, stderr=subprocess.STDOUT, text=True
    )


def _manifest(bucket: str, migration_id: str) -> Manifest:
    store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)
    manifest = store.load()
    assert manifest is not None
    return manifest


def _wait_for_stage(bucket: str, migration_id: str, stage: Stage, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        store = ManifestStore(boto3.client("s3", region_name=REGION), bucket, migration_id)
        manifest = store.load()
        if manifest is not None and manifest.is_complete(stage):
            return
        time.sleep(15)
    raise AssertionError(f"{stage.value} did not complete within {timeout}s")


def test_osis_glue_end_to_end_with_a_kill_after_export(
    seeded_index: str, index_name: str, bucket_prefix: tuple[str, str], tmp_path: Path
) -> None:
    bucket, migration_id = bucket_prefix
    log = tmp_path / "runner.log"
    (tmp_path / "state").mkdir()
    env = _env(index_name, migration_id, seeded_index, tmp_path)
    try:
        assert _run({**env, "PCMIGRATE_PHASE": "provision"}, log, timeout=1800) == 0, (
            log.read_text()[-4000:]
        )
        provision = _manifest(bucket, migration_id).stage(Stage.PROVISION).outputs
        assert provision["osis_pipeline_name"].startswith("pcm-os-") and provision["glue_job_name"]
        assert "domain_access_statement" in provision

        first = _spawn(
            {**env, "PCMIGRATE_PHASE": "load", "PCMIGRATE_PAUSE_AFTER_STAGE": "export"}, log
        )
        try:
            _wait_for_stage(bucket, migration_id, Stage.EXPORT, timeout=3600)
        finally:
            first.kill()
            first.wait()
        export = _manifest(bucket, migration_id).stage(Stage.EXPORT).outputs
        assert export["mode"] == "osis_glue" and export["t0"] and export["osis_stopped"]
        assert export["source_counts"] == {seeded_index: 1005}
        assert export["rows_by_namespace"] == {seeded_index: 1005}
        assert export["rejected_by_namespace"] == {seeded_index: 0}
        assert export["raw_objects"][seeded_index] >= 1
        assert export["glue_runs"][seeded_index].startswith("jr_")

        assert _run({**env, "PCMIGRATE_PHASE": "load"}, log, timeout=5400) == 0, log.read_text()[
            -4000:
        ]
        manifest = _manifest(bucket, migration_id)
        assert manifest.stage(Stage.EXPORT).attempts == 1, "the resumed run skipped export"
        assert manifest.stage(Stage.TRANSFORM).outputs["duplicates_by_namespace"] == {
            seeded_index: 4
        }
        assert manifest.stage(Stage.IMPORT).status is StageStatus.COMPLETE
        checks = manifest.stage(Stage.VALIDATE).outputs["checks"]
        assert all(checks.values()), checks
        gateway = SdkGateway(os.environ["PINECONE_API_KEY"])
        info = gateway.describe_index(index_name)
        assert info is not None
        assert gateway.namespace_counts(info.host) == {seeded_index: 1005}
    finally:
        rc = _run({**env, "PCMIGRATE_PHASE": "teardown"}, log, timeout=1800)
        assert rc == 0, log.read_text()[-4000:]
        glue = boto3.client("glue", region_name=REGION)
        with pytest.raises(glue.exceptions.EntityNotFoundException):
            glue.get_job(JobName=f"pcmigrate-{migration_id}-opensearch-transform")
        osis = boto3.client("osis", region_name=REGION)
        with pytest.raises(osis.exceptions.ResourceNotFoundException):
            osis.get_pipeline(PipelineName=f"pcm-{migration_id}")
