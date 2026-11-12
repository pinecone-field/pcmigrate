"""ensure_bucket: the staging bucket exists before the manifest is first read (ADR-003)."""

import boto3
from mypy_boto3_s3 import S3Client

from pcmigrate.core.s3 import ensure_bucket
from tests.unit.conftest import BUCKET, REGION


def test_ensure_bucket_creates_once_with_public_access_blocked(s3: S3Client) -> None:
    assert ensure_bucket(s3, "fresh-bucket", REGION, {"pcmigrate:migration_id": "m"}) is True
    assert ensure_bucket(s3, "fresh-bucket", REGION) is False
    assert ensure_bucket(s3, BUCKET, REGION) is False, "an existing bucket is left alone"
    block = s3.get_public_access_block(Bucket="fresh-bucket")["PublicAccessBlockConfiguration"]
    assert all(block.values())
    tags = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket="fresh-bucket")["TagSet"]}
    assert tags == {"pcmigrate:migration_id": "m"}


def test_ensure_bucket_sets_the_location_outside_us_east_1(s3: S3Client) -> None:
    west = boto3.client("s3", region_name="us-west-2")
    assert ensure_bucket(west, "west-bucket", "us-west-2") is True
    assert west.get_bucket_location(Bucket="west-bucket")["LocationConstraint"] == "us-west-2"
