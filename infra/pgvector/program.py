"""Pulumi program for the pgvector runner: EC2 via SSM, egress to the DB port and HTTPS only."""

from __future__ import annotations

from collections.abc import Callable

import pulumi
import pulumi_aws as aws

from infra.pgvector.config import PgvectorRunnerConfig
from pcmigrate.config.aws import resource_tags

AL2023_SSM_PARAMETER = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"


def user_data(cfg: PgvectorRunnerConfig) -> str:
    """Install uv and pcmigrate on first boot. The SSM agent ships with AL2023."""
    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        "dnf install -y python3.12 git",
        "curl -LsSf https://astral.sh/uv/install.sh | sh",
        "install -d -o ec2-user /opt/pcmigrate",
    ]
    if cfg.wheel_s3_uri:
        lines += [
            f"aws s3 cp {cfg.wheel_s3_uri} /opt/pcmigrate/pcmigrate.whl",
            "/root/.local/bin/uv venv --python 3.12 /opt/pcmigrate/.venv",
            "/root/.local/bin/uv pip install --python /opt/pcmigrate/.venv/bin/python "
            "/opt/pcmigrate/pcmigrate.whl",
        ]
    lines.append("echo pcmigrate-runner-ready > /var/log/pcmigrate-ready")
    return "\n".join(lines) + "\n"


def build(cfg: PgvectorRunnerConfig) -> None:
    """Declare the runner stack."""
    tags = resource_tags(cfg.migration_id, cfg.tags)
    names = cfg.names

    security_group = aws.ec2.SecurityGroup(
        f"{cfg.resource_prefix}-runner-sg",
        name=names.runner_role,
        description="pcmigrate runner: no ingress; egress to the database port and HTTPS",
        vpc_id=cfg.vpc_id,
        ingress=[],
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                description="source database",
                from_port=cfg.db_port,
                to_port=cfg.db_port,
                protocol="tcp",
                cidr_blocks=[cfg.db_cidr],
            ),
            aws.ec2.SecurityGroupEgressArgs(
                description="HTTPS to S3, Secrets Manager, SSM, and Pinecone",
                from_port=443,
                to_port=443,
                protocol="tcp",
                cidr_blocks=["0.0.0.0/0"],
            ),
        ],
        tags=tags,
    )

    ami_id: pulumi.Input[str] = (
        cfg.ami_id or aws.ssm.get_parameter_output(name=AL2023_SSM_PARAMETER).value
    )

    instance = aws.ec2.Instance(
        f"{cfg.resource_prefix}-runner",
        ami=ami_id,
        instance_type=cfg.instance_type,
        subnet_id=cfg.subnet_id,
        vpc_security_group_ids=[security_group.id],
        iam_instance_profile=cfg.instance_profile_name,
        associate_public_ip_address=False,
        key_name=None,
        user_data=user_data(cfg),
        metadata_options=aws.ec2.InstanceMetadataOptionsArgs(
            http_tokens="required", http_endpoint="enabled"
        ),
        root_block_device=aws.ec2.InstanceRootBlockDeviceArgs(
            volume_size=cfg.root_volume_gb, volume_type="gp3", encrypted=True
        ),
        tags={**tags, "Name": names.runner_role},
        volume_tags={**tags, "Name": names.runner_role},
    )

    pulumi.export("runner_instance_id", instance.id)
    pulumi.export("runner_security_group_id", security_group.id)
    pulumi.export("runner_private_ip", instance.private_ip)
    pulumi.export(
        "ssm_command",
        instance.id.apply(lambda i: f"aws ssm start-session --target {i} --region {cfg.region}"),
    )


def program_for(cfg: PgvectorRunnerConfig) -> Callable[[], None]:
    """Zero-argument program for the Automation API."""
    return lambda: build(cfg)
