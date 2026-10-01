"""Small boto3 helpers shared by the manifest store, canonical writer, and emitters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@dataclass(frozen=True, slots=True)
class S3Location:
    """A bucket and key or key prefix."""

    bucket: str
    key: str

    @property
    def uri(self) -> str:
        """The s3:// form."""
        return f"s3://{self.bucket}/{self.key}"

    def join(self, *parts: str) -> S3Location:
        """Append path segments to the key."""
        key = "/".join([self.key.rstrip("/"), *[p.strip("/") for p in parts]])
        return S3Location(self.bucket, key)

    @classmethod
    def parse(cls, uri: str) -> S3Location:
        """Parse an s3://bucket/key URI."""
        if not uri.startswith("s3://"):
            raise ValueError(f"not an S3 URI: {uri!r}")
        bucket, _, key = uri[len("s3://") :].partition("/")
        if not bucket:
            raise ValueError(f"S3 URI has no bucket: {uri!r}")
        return cls(bucket, key)


def make_client(region: str) -> S3Client:
    """Create an S3 client for *region*."""
    return boto3.client("s3", region_name=region)


def error_code(err: ClientError) -> str:
    """The AWS error code carried by a botocore ClientError."""
    code = err.response.get("Error", {}).get("Code", "")
    return str(code)


def is_precondition_failed(err: ClientError) -> bool:
    """True for the 412 that S3 returns when If-Match or If-None-Match is not satisfied."""
    return error_code(err) in {"PreconditionFailed", "412"}


def is_not_found(err: ClientError) -> bool:
    """True when the object does not exist."""
    return error_code(err) in {"NoSuchKey", "404", "NotFound"}


def list_keys(s3: S3Client, bucket: str, prefix: str) -> list[str]:
    """Every key under *prefix*, in lexical order."""
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)
