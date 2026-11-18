"""`pcmigrate preflight`: the checks a run depends on, each as the identity the stages use."""

from pathlib import Path

import boto3
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping
from pcmigrate.config.migration import ImportBucket
from pcmigrate.config.settings import RunSettings
from pcmigrate.core.preflight import (
    check_import_bucket,
    check_opensearch,
    check_staging_bucket,
    check_target_index,
    run_preflight,
)
from pcmigrate.targets.pinecone_client import IndexInfo
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.conftest import BUCKET, REGION
from tests.unit.fakes import FakeGateway
from tests.unit.test_cli_run import settings_file

IMPORT_BUCKET = "byoc-import"


def test_staging_bucket_check(s3: S3Client) -> None:
    assert check_staging_bucket(s3, BUCKET).ok
    missing = check_staging_bucket(s3, "no-such-bucket-here")
    assert not missing.ok and "no-such-bucket-here" in missing.detail


def test_import_bucket_probe_writes_lists_reads_and_deletes(s3: S3Client) -> None:
    dest = boto3.client("s3", region_name=REGION)
    dest.create_bucket(Bucket=IMPORT_BUCKET)
    result = check_import_bucket(dest, IMPORT_BUCKET, "pinecone/mig", None)
    assert result.ok, result.detail
    assert "put, list, get, delete ok" in result.detail
    assert not dest.list_objects_v2(Bucket=IMPORT_BUCKET, Prefix="pinecone/mig/").get("Contents")
    denied = check_import_bucket(dest, "no-such-import-bucket", "pinecone/mig", None)
    assert not denied.ok and "NoSuchBucket" in denied.detail


def test_import_bucket_probe_checks_the_kms_key(s3: S3Client) -> None:
    dest = boto3.client("s3", region_name=REGION)
    kms = boto3.client("kms", region_name=REGION)
    dest.create_bucket(Bucket=IMPORT_BUCKET)
    key = kms.create_key()["KeyMetadata"]["Arn"]
    assert check_import_bucket(dest, IMPORT_BUCKET, "pinecone/mig", key).ok


def test_opensearch_check_reports_hidden_distribution() -> None:
    ok = check_opensearch(lambda: {"version": {"number": "2.19.0", "distribution": "opensearch"}})
    assert ok.ok and ok.detail == "opensearch 2.19.0"
    compat = check_opensearch(lambda: {"version": {"number": "7.10.2"}})
    assert compat.ok and "no distribution field" in compat.detail
    down = check_opensearch(lambda: (_ for _ in ()).throw(ConnectionError("refused")))
    assert not down.ok and "refused" in down.detail


def test_target_index_check_follows_the_mode() -> None:
    gw = FakeGateway()
    mapping = Mapping.from_yaml(EXAMPLE)
    create = check_target_index(gw, mapping)
    assert create.ok and "will be created" in create.detail
    gw.indexes[mapping.target.index] = IndexInfo(
        mapping.target.index, "h", 768, "cosine", "aws", "us-east-1", True
    )
    assert not check_target_index(gw, mapping).ok, "create mode with an existing index"
    adopt = mapping.model_copy(
        update={"target": mapping.target.model_copy(update={"mode": "adopt"})}
    )
    assert check_target_index(gw, adopt).ok
    byoc = adopt.model_copy(
        update={
            "target": adopt.target.model_copy(
                update={"deployment": "byoc", "environment": "aws-us-east-1-abcd.byoc"}
            )
        }
    )
    wrong = check_target_index(gw, byoc)
    assert not wrong.ok and "mapping says byoc" in wrong.detail


def test_run_preflight_with_injected_clients(s3: S3Client, tmp_path: Path) -> None:
    dest = boto3.client("s3", region_name=REGION)
    dest.create_bucket(Bucket=IMPORT_BUCKET)
    path = settings_file(tmp_path)
    settings = RunSettings.load(path)
    settings = settings.model_copy(
        update={
            "migration": settings.migration.model_copy(
                update={"import_bucket": ImportBucket(name=IMPORT_BUCKET, prefix="pinecone/mig")}
            )
        }
    )
    results = run_preflight(settings, s3=s3, import_s3=dest, gateway=FakeGateway())
    assert [r.name for r in results] == ["staging bucket", "import bucket", "pinecone index"]
    assert all(r.ok for r in results), [r.detail for r in results if not r.ok]
