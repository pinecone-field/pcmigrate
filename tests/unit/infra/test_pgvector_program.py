from typing import Any

import pulumi
import pytest
from infra.pgvector import PgvectorRunnerConfig, build, user_data

CFG = PgvectorRunnerConfig(
    migration_id="acme-2026-10",
    region="us-east-1",
    vpc_id="vpc-123",
    subnet_id="subnet-456",
    db_port=5432,
    db_cidr="10.0.0.0/16",
    instance_profile_name="pcmigrate-acme-2026-10-runner",
    bucket_name="acme-pcmigrate",
    ami_id="ami-abc",
    wheel_s3_uri="s3://acme-pcmigrate/acme-2026-10/manifest/dist/pcmigrate.whl",
    tags={"CostCenter": "4711"},
)
EXPECTED_TAGS = {
    "CostCenter": "4711",
    "pcmigrate:managed_by": "pcmigrate",
    "pcmigrate:migration_id": "acme-2026-10",
}


class Mocks(pulumi.runtime.Mocks):
    def __init__(self) -> None:
        self.resources: list[tuple[str, str, dict[str, Any]]] = []

    def new_resource(self, args: pulumi.runtime.MockResourceArgs) -> tuple[str, dict[str, Any]]:
        self.resources.append((args.typ, args.name, args.inputs))
        outputs = {**args.inputs, "id": f"{args.name}-id", "privateIp": "10.0.1.5"}
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:
        return {"value": "ami-from-ssm"}


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


def test_security_group_has_no_ingress_and_scoped_egress(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (sg,) = _of(declared, "aws:ec2/securityGroup:SecurityGroup")
    assert sg["vpcId"] == "vpc-123"
    assert sg.get("ingress", []) == []
    ports = sorted((e["fromPort"], e["toPort"], tuple(e["cidrBlocks"])) for e in sg["egress"])
    assert ports == [(443, 443, ("0.0.0.0/0",)), (5432, 5432, ("10.0.0.0/16",))]


def test_instance_is_private_ssm_only_and_scoped(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (instance,) = _of(declared, "aws:ec2/instance:Instance")
    assert instance["subnetId"] == "subnet-456"
    assert instance["iamInstanceProfile"] == "pcmigrate-acme-2026-10-runner"
    assert instance["associatePublicIpAddress"] is False
    assert instance.get("keyName") is None, "no SSH key pair"
    assert instance["metadataOptions"]["httpTokens"] == "required"
    assert instance["rootBlockDevice"]["encrypted"] is True
    assert instance["ami"] == "ami-abc"
    assert "pcmigrate.whl" in instance["userData"]


def test_instance_volume_and_security_group_carry_the_tags(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (sg,) = _of(declared, "aws:ec2/securityGroup:SecurityGroup")
    assert sg["tags"] == EXPECTED_TAGS and sg["name"] == "pcmigrate-acme-2026-10-runner"
    (instance,) = _of(declared, "aws:ec2/instance:Instance")
    named = {**EXPECTED_TAGS, "Name": "pcmigrate-acme-2026-10-runner"}
    assert instance["tags"] == named and instance["volumeTags"] == named


def test_name_prefix_renames_the_runner() -> None:
    cfg = CFG.model_copy(update={"name_prefix": "corp"})
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="pcmigrate", stack=cfg.stack_name, preview=False)

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    (sg,) = _of(mocks.resources, "aws:ec2/securityGroup:SecurityGroup")
    (instance,) = _of(mocks.resources, "aws:ec2/instance:Instance")
    assert sg["name"] == "corp-pcmigrate-acme-2026-10-runner"
    assert instance["tags"]["Name"] == "corp-pcmigrate-acme-2026-10-runner"


def test_user_data_installs_wheel_when_given() -> None:
    assert "aws s3 cp s3://acme-pcmigrate" in user_data(CFG)
    plain = CFG.model_copy(update={"wheel_s3_uri": None})
    assert "aws s3 cp" not in user_data(plain) and "pcmigrate-runner-ready" in user_data(plain)
