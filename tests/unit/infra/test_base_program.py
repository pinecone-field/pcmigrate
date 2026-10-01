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
)


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
    assert "s3:PutObject" not in actions and "kms:Encrypt" not in actions
    writable = json.loads(
        bucket_scoped_policy("arn:aws:s3:::b", "arn:aws:kms:::k", prefix="mig", write=True)
    )
    assert "s3:PutObject" in {a for s in writable["Statement"] for a in s["Action"]}


def test_trust_policy_requires_external_id() -> None:
    policy = json.loads(storage_integration_trust_policy("arn:aws:iam::1:root", "x"))
    assert policy["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"] == "x"


def test_stack_without_pinecone_principal_skips_integration_role() -> None:
    cfg = CFG.model_copy(update={"pinecone_principal_arn": None, "pinecone_external_id": None})
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=cfg.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    names = [n for t, n, _ in mocks.resources if t == "aws:iam/role:Role"]
    assert names == ["pcmigrate-acme-2026-10-runner-role"]
