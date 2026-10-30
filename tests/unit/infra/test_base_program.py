"""Runs the base program under Pulumi's mock engine and inspects the declared resources."""

import json
from typing import Any

import pulumi
import pytest
from infra.base import BaseStackConfig, build
from infra.base.program import bucket_scoped_policy, storage_integration_trust_policy

CFG = BaseStackConfig(
    migration_id="acme-2026-10",
    region="us-east-1",
    bucket_name="acme-pcmigrate",
    pinecone_principal_arn="arn:aws:iam::123456789012:role/pinecone-import",
    pinecone_external_id="ext-abc",
    tags={"CostCenter": "4711", "Owner": "data-platform"},
)
TAGGABLE = {
    "aws:s3/bucket:Bucket",
    "aws:kms/key:Key",
    "aws:secretsmanager/secret:Secret",
    "aws:iam/role:Role",
    "aws:iam/instanceProfile:InstanceProfile",
    "aws:cloudwatch/logGroup:LogGroup",
    "aws:sqs/queue:Queue",
}


class Mocks(pulumi.runtime.Mocks):
    def __init__(self) -> None:
        self.resources: list[tuple[str, str, dict[str, Any]]] = []

    def new_resource(self, args: pulumi.runtime.MockResourceArgs) -> tuple[str, dict[str, Any]]:
        self.resources.append((args.typ, args.name, args.inputs))
        outputs = dict(args.inputs)
        outputs.setdefault("arn", f"arn:aws:mock:::{args.name}")
        outputs.setdefault("id", f"{args.name}-id")
        outputs.setdefault("keyId", f"{args.name}-keyid")
        outputs.setdefault("bucket", args.inputs.get("bucket", args.name))
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:
        if "getCallerIdentity" in args.token:
            return {
                "accountId": "111122223333",
                "arn": "arn:aws:iam::111122223333:user/t",
                "userId": "u",
            }
        if "getBucket" in args.token:
            name = args.args["bucket"]
            return {"bucket": name, "arn": f"arn:aws:s3:::{name}", "id": name}
        return {}


@pytest.fixture(scope="module")
def declared() -> list[tuple[str, str, dict[str, Any]]]:
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=CFG.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(CFG)

    run()
    return mocks.resources


def _of(declared: list[tuple[str, str, dict[str, Any]]], typ: str) -> list[dict[str, Any]]:
    return [inputs for t, _, inputs in declared if t == typ]


def _run(cfg: BaseStackConfig) -> list[tuple[str, str, dict[str, Any]]]:
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=cfg.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    return mocks.resources


def test_every_taggable_resource_carries_customer_and_tool_tags(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    tagged = [(t, inputs) for t, _, inputs in declared if t in TAGGABLE]
    assert {t for t, _ in tagged} == TAGGABLE
    for typ, inputs in tagged:
        assert inputs.get("tags") == {
            "CostCenter": "4711",
            "Owner": "data-platform",
            "pcmigrate:managed_by": "pcmigrate",
            "pcmigrate:migration_id": "acme-2026-10",
        }, typ


def test_name_prefix_renames_every_physical_resource_but_no_logical_one() -> None:
    cfg = CFG.model_copy(update={"name_prefix": "Corp-Prod"})
    declared = _run(cfg)
    logical = {name for _, name, _ in declared}
    assert logical == {name for _, name, _ in _run(CFG)}, "logical names never change"
    roles = {r["name"] for r in _of(declared, "aws:iam/role:Role")}
    assert roles == {
        "Corp-Prod-pcmigrate-acme-2026-10-runner",
        "Corp-Prod-pcmigrate-acme-2026-10-pinecone-import",
    }
    (profile,) = _of(declared, "aws:iam/instanceProfile:InstanceProfile")
    assert profile["name"] == "Corp-Prod-pcmigrate-acme-2026-10-runner"
    (alias,) = _of(declared, "aws:kms/alias:Alias")
    assert alias["name"] == "alias/Corp-Prod-pcmigrate-acme-2026-10"
    assert sorted(s["name"] for s in _of(declared, "aws:secretsmanager/secret:Secret")) == [
        "Corp-Prod-pcmigrate/acme-2026-10/pinecone-api-key",
        "Corp-Prod-pcmigrate/acme-2026-10/source-dsn",
    ]
    (logs,) = _of(declared, "aws:cloudwatch/logGroup:LogGroup")
    assert logs["name"] == "/Corp-Prod-pcmigrate/acme-2026-10"
    (queue,) = _of(declared, "aws:sqs/queue:Queue")
    assert queue["name"] == "Corp-Prod-pcmigrate-acme-2026-10-buffer.fifo"
    (key,) = _of(declared, "aws:kms/key:Key")
    logs_statement = next(
        s for s in json.loads(key["policy"])["Statement"] if s["Sid"] == "CloudWatchLogs"
    )
    assert logs_statement["Condition"]["ArnLike"]["kms:EncryptionContext:aws:logs:arn"].endswith(
        ":log-group:/Corp-Prod-pcmigrate/acme-2026-10"
    )
    (bucket,) = _of(declared, "aws:s3/bucket:Bucket")
    assert bucket["bucket"] == "acme-pcmigrate", "the bucket name is the customer's own"


def test_bucket_is_locked_down(declared: list[tuple[str, str, dict[str, Any]]]) -> None:
    (bucket,) = _of(declared, "aws:s3/bucket:Bucket")
    assert bucket["bucket"] == "acme-pcmigrate"
    (pab,) = _of(declared, "aws:s3/bucketPublicAccessBlock:BucketPublicAccessBlock")
    assert all(
        pab[k]
        for k in (
            "blockPublicAcls",
            "blockPublicPolicy",
            "ignorePublicAcls",
            "restrictPublicBuckets",
        )
    )
    (sse,) = _of(
        declared,
        "aws:s3/bucketServerSideEncryptionConfiguration:BucketServerSideEncryptionConfiguration",
    )
    default = sse["rules"][0]["applyServerSideEncryptionByDefault"]
    assert default["sseAlgorithm"] == "aws:kms" and default["kmsMasterKeyId"]
    (lifecycle,) = _of(declared, "aws:s3/bucketLifecycleConfiguration:BucketLifecycleConfiguration")
    rule = lifecycle["rules"][0]
    assert rule["expiration"]["days"] == 30
    assert rule["filter"]["prefix"] == "acme-2026-10/"
    assert rule["abortIncompleteMultipartUpload"]["daysAfterInitiation"] == 2


def test_kms_key_rotates(declared: list[tuple[str, str, dict[str, Any]]]) -> None:
    (key,) = _of(declared, "aws:kms/key:Key")
    assert key["enableKeyRotation"] is True
    (alias,) = _of(declared, "aws:kms/alias:Alias")
    assert alias["name"] == "alias/pcmigrate-acme-2026-10"


def test_secrets_are_containers_without_values(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    secrets = _of(declared, "aws:secretsmanager/secret:Secret")
    assert sorted(s["name"] for s in secrets) == [
        "pcmigrate/acme-2026-10/pinecone-api-key",
        "pcmigrate/acme-2026-10/source-dsn",
    ]
    assert not _of(declared, "aws:secretsmanager/secretVersion:SecretVersion")


def test_roles(declared: list[tuple[str, str, dict[str, Any]]]) -> None:
    roles = {r["name"]: r for r in _of(declared, "aws:iam/role:Role")}
    runner = json.loads(roles["pcmigrate-acme-2026-10-runner"]["assumeRolePolicy"])
    assert runner["Statement"][0]["Principal"] == {"Service": "ec2.amazonaws.com"}
    integration = json.loads(roles["pcmigrate-acme-2026-10-pinecone-import"]["assumeRolePolicy"])
    statement = integration["Statement"][0]
    assert statement["Principal"] == {"AWS": CFG.pinecone_principal_arn}
    assert statement["Condition"] == {"StringEquals": {"sts:ExternalId": "ext-abc"}}
    attachments = _of(declared, "aws:iam/rolePolicyAttachment:RolePolicyAttachment")
    assert any(a["policyArn"].endswith("AmazonSSMManagedInstanceCore") for a in attachments)
    assert _of(declared, "aws:iam/instanceProfile:InstanceProfile")


def test_policies_are_prefix_scoped() -> None:
    policy = json.loads(
        bucket_scoped_policy("arn:aws:s3:::b", "arn:aws:kms:::k", prefix="mig", write=False)
    )
    resources = [s["Resource"] for s in policy["Statement"]]
    assert "arn:aws:s3:::b/mig/*" in resources
    actions = {a for s in policy["Statement"] for a in s["Action"]}
    assert "s3:DeleteObject" not in actions and "kms:Encrypt" not in actions
    assert "s3:PutObject" in actions
    writable = json.loads(
        bucket_scoped_policy("arn:aws:s3:::b", "arn:aws:kms:::k", prefix="mig", write=True)
    )
    assert "s3:PutObject" in {a for s in writable["Statement"] for a in s["Action"]}


def test_trust_policy_external_id_is_optional() -> None:
    with_id = json.loads(storage_integration_trust_policy("arn:aws:iam::1:root", "x"))
    assert with_id["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"] == "x"
    without = json.loads(storage_integration_trust_policy("arn:aws:iam::1:root", None))
    assert "Condition" not in without["Statement"][0]


def test_default_principal_is_pinecones_account() -> None:
    cfg = BaseStackConfig(migration_id="m", region="us-east-1", bucket_name="some-bucket")
    assert cfg.pinecone_principal_arn == "arn:aws:iam::713131977538:root"


def test_buffer_queue_is_fifo_and_omitted_for_tier_none(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (queue,) = _of(declared, "aws:sqs/queue:Queue")
    assert queue["fifoQueue"] is True and queue["name"].endswith(".fifo")
    assert queue["contentBasedDeduplication"] is False
    assert queue["messageRetentionSeconds"] == CFG.buffer_retention_seconds
    assert _of(declared, "aws:cloudwatch/logGroup:LogGroup")
    cfg = CFG.model_copy(update={"change_capture_enabled": False})
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=cfg.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    assert not [t for t, _, _ in mocks.resources if t == "aws:sqs/queue:Queue"]


def test_retention_sized_from_import_eta() -> None:
    from pcmigrate.config import MigrationConfig

    config = MigrationConfig(
        migration_id="m",
        bucket="some-bucket",
        aws_region="us-east-1",
        pinecone_api_key_secret="arn:aws:secretsmanager:us-east-1:1:secret:x",
    )
    short = BaseStackConfig.from_migration(config, import_eta_s=3600)
    assert short.buffer_retention_seconds == 4 * 24 * 3600
    long = BaseStackConfig.from_migration(config, import_eta_s=5 * 24 * 3600)
    assert long.buffer_retention_seconds == 11 * 24 * 3600
    capped = BaseStackConfig.from_migration(config, import_eta_s=30 * 24 * 3600)
    assert capped.buffer_retention_seconds == 14 * 24 * 3600
    assert (
        BaseStackConfig.from_migration(config, change_capture_enabled=False).change_capture_enabled
        is False
    )


def test_adopt_bucket_reads_the_existing_bucket_and_declares_no_bucket_resources() -> None:
    cfg = CFG.model_copy(update={"adopt_bucket": True})
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=cfg.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    types = {t for t, _, _ in mocks.resources}
    assert not any(t.startswith("aws:s3/") for t in types), types
    runner_policy = next(
        inputs["policy"]
        for t, name, inputs in mocks.resources
        if t == "aws:iam/rolePolicy:RolePolicy" and name.endswith("-runner-bucket")
    )
    assert f"arn:aws:s3:::{cfg.bucket_name}/{cfg.migration_id}/*" in runner_policy


def test_bucket_mode_flows_from_migration_config() -> None:
    from pcmigrate.config import MigrationConfig

    config = MigrationConfig(
        migration_id="m",
        bucket="shared",
        bucket_mode="adopt",
        aws_region="us-east-1",
        pinecone_api_key_secret="arn:aws:secretsmanager:us-east-1:1:secret:k",
    )
    assert BaseStackConfig.from_migration(config).adopt_bucket is True
    prefixed = config.model_copy(update={"name_prefix": "acme", "tags": {"CostCenter": "1"}})
    stack = BaseStackConfig.from_migration(prefixed)
    assert stack.name_prefix == "acme" and stack.tags == {"CostCenter": "1"}
    assert stack.secret_name("source-dsn") == "acme-pcmigrate/m/source-dsn"
    assert (
        BaseStackConfig.from_migration(
            config.model_copy(update={"bucket_mode": "create"})
        ).adopt_bucket
        is False
    )


def test_kms_key_policy_lets_cloudwatch_logs_use_the_key(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (key,) = [inputs for t, _, inputs in declared if t == "aws:kms/key:Key"]
    policy = json.loads(key["policy"])
    logs = next(s for s in policy["Statement"] if s["Sid"] == "CloudWatchLogs")
    assert logs["Principal"] == {"Service": f"logs.{CFG.region}.amazonaws.com"}
    assert logs["Condition"]["ArnLike"]["kms:EncryptionContext:aws:logs:arn"] == (
        f"arn:aws:logs:{CFG.region}:111122223333:log-group:/pcmigrate/{CFG.migration_id}"
    )
    admin = next(s for s in policy["Statement"] if s["Sid"] == "AccountAdministration")
    assert admin["Principal"] == {"AWS": "arn:aws:iam::111122223333:root"}
