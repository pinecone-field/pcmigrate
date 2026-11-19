"""`pcmigrate preflight`: prove the pieces a run depends on before a stage spends an hour on them.

Each check runs as the identity the stages run as, which is what matters for cross-account
buckets: a probe object written, listed, read back, and deleted under the import prefix
exercises the operator role's own policy, the bucket policy, and the key policy together.
What cannot be checked from the source account, the BYOC data importer's own access, is
reported as a reminder rather than a pass.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from pcmigrate.config.settings import RunSettings
from pcmigrate.core.s3 import make_client

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

    from pcmigrate.config.mapping import Mapping
    from pcmigrate.targets.pinecone_client import PineconeGateway


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One preflight check."""

    name: str
    ok: bool
    detail: str


def _error_text(err: Exception) -> str:
    if isinstance(err, ClientError):
        error = err.response.get("Error", {})
        return f"{error.get('Code', 'error')}: {str(error.get('Message', ''))[:160]}"
    return f"{type(err).__name__}: {str(err)[:160]}"


def check_staging_bucket(s3: S3Client, bucket: str) -> CheckResult:
    """The staging bucket must be reachable with the current identity."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as err:
        return CheckResult("staging bucket", False, f"{bucket}: {_error_text(err)}")
    return CheckResult("staging bucket", True, bucket)


def check_import_bucket(
    s3: S3Client, bucket: str, prefix: str, kms_key_arn: str | None
) -> CheckResult:
    """Write, list, read, and delete a probe object under the import prefix."""
    key = f"{prefix}/_preflight/probe-{int(time.time())}.txt"
    put: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": b"pcmigrate preflight"}
    if kms_key_arn:
        put["ServerSideEncryption"] = "aws:kms"
        put["SSEKMSKeyId"] = kms_key_arn
    try:
        s3.put_object(**put)
        head = s3.head_object(Bucket=bucket, Key=key)
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/_preflight/")
        s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        s3.delete_object(Bucket=bucket, Key=key)
    except ClientError as err:
        return CheckResult("import bucket", False, f"s3://{bucket}/{prefix}/: {_error_text(err)}")
    if not any(o.get("Key") == key for o in listed.get("Contents", [])):
        return CheckResult(
            "import bucket", False, f"s3://{bucket}/{prefix}/: ListBucket is not allowed"
        )
    if kms_key_arn and head.get("SSEKMSKeyId") != kms_key_arn:
        return CheckResult(
            "import bucket",
            False,
            f"object encrypted with {head.get('SSEKMSKeyId')!r}, settings say {kms_key_arn!r}",
        )
    return CheckResult("import bucket", True, f"s3://{bucket}/{prefix}/ put, list, get, delete ok")


def check_opensearch(get_root: Callable[[], Any]) -> CheckResult:
    """GET / must answer, and the distribution it reports decides the pipeline's client."""
    try:
        root = get_root()
    except Exception as err:
        return CheckResult("opensearch", False, _error_text(err))
    version = root.get("version", {}) if isinstance(root, dict) else {}
    number = version.get("number", "?")
    if "distribution" not in version:
        return CheckResult(
            "opensearch",
            True,
            f"version {number}, no distribution field (compatibility mode or Elasticsearch "
            "engine): the export pipeline uses opensearch.distribution_version",
        )
    return CheckResult("opensearch", True, f"{version.get('distribution')} {number}")


def check_target_index(gateway: PineconeGateway, mapping: Mapping) -> CheckResult:
    """The index must exist for adopt mode and match its deployment, or be absent for create."""
    target = mapping.target
    try:
        info = gateway.describe_index(target.index)
    except Exception as err:
        return CheckResult("pinecone index", False, _error_text(err))
    if target.mode == "adopt":
        if info is None:
            return CheckResult("pinecone index", False, f"{target.index} does not exist")
        if target.deployment == "byoc" and info.environment != target.environment:
            return CheckResult(
                "pinecone index",
                False,
                f"{target.index} is in {info.deployment} {info.environment!r}, "
                f"mapping says byoc {target.environment!r}",
            )
        return CheckResult(
            "pinecone index",
            True,
            f"{target.index}: {info.deployment} {info.environment or ''} "
            f"{info.dimension} {info.metric}, {'document' if info.document else 'vector'}",
        )
    if info is not None:
        return CheckResult(
            "pinecone index", False, f"{target.index} already exists but target.mode is create"
        )
    return CheckResult("pinecone index", True, f"{target.index} will be created")


def byoc_reminder(mapping: Mapping | None, import_bucket: bool) -> CheckResult | None:
    """What this account cannot verify: the BYOC data importer's access to the import bucket."""
    if mapping is None or mapping.target.deployment != "byoc" or not import_bucket:
        return None
    return CheckResult(
        "byoc import role",
        True,
        "not verifiable from here: the BYOC account's pc-storage-integration-<suffix> role "
        "must be in the import bucket policy and its KMS key policy",
    )


def run_preflight(
    settings: RunSettings,
    *,
    s3: S3Client | None = None,
    import_s3: S3Client | None = None,
    get_root: Callable[[], Any] | None = None,
    gateway: PineconeGateway | None = None,
) -> list[CheckResult]:
    """Every check the settings make applicable; callers inject clients for tests."""
    config = settings.migration
    source = s3 or make_client(config.aws_region)
    results = [check_staging_bucket(source, config.bucket)]
    target_bucket = config.import_bucket
    if target_bucket is not None:
        dest = import_s3 or make_client(target_bucket.region or config.aws_region)
        results.append(
            check_import_bucket(
                dest, target_bucket.name, config.import_zone(), target_bucket.kms_key_arn
            )
        )
    mapping: Mapping | None = None
    try:
        mapping = settings.mapping()
    except (FileNotFoundError, ValueError):
        results.append(CheckResult("mapping", True, "no mapping.yaml yet; run introspect first"))
    if mapping is not None and mapping.source.kind == "opensearch":
        results.append(check_opensearch(get_root or _opensearch_root(settings)))
    if mapping is not None and gateway is not None:
        results.append(check_target_index(gateway, mapping))
    elif mapping is not None:
        results.append(check_target_index(_pinecone_gateway(settings), mapping))
    reminder = byoc_reminder(mapping, target_bucket is not None)
    if reminder is not None:
        results.append(reminder)
    return results


def _opensearch_root(settings: RunSettings) -> Callable[[], Any]:
    def get_root() -> Any:
        from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient

        client = OpenSearchClient(
            ConnectionOptions(
                settings.opensearch_url(),
                auth=settings.opensearch.auth,
                basic_auth=settings.opensearch_basic_auth(),
                region=settings.migration.aws_region,
                verify_certs=not settings.opensearch.insecure_tls,
            )
        )
        try:
            return client.get("/")
        finally:
            client.close()

    return get_root


def _pinecone_gateway(settings: RunSettings) -> PineconeGateway:
    import os

    from pcmigrate.targets import SdkGateway, api_key_from_secret

    env = settings.pinecone_api_key_env
    api_key = (
        os.environ[env]
        if env and os.environ.get(env)
        else api_key_from_secret(
            settings.migration.pinecone_api_key_secret, settings.migration.aws_region
        )
    )
    return SdkGateway(api_key)
