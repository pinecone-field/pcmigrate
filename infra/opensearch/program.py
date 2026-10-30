"""Pulumi program for the OpenSearch add-on: OSIS pipeline, Glue job, roles, domain access.

The pipeline role is trusted by OpenSearch Ingestion and may read the domain, write raw/
under the migration prefix, use the KMS key, and read the self-managed credentials secret.
The Glue role may read raw/ and the staged job assets and write canonical/ and rejects/.
For a managed domain the stack emits the access-policy statement the domain needs and,
when asked, applies it as the domain's resource policy.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pulumi
import pulumi_aws as aws

from infra.opensearch.config import OpenSearchStackConfig
from pcmigrate.config.aws import resource_tags
from pcmigrate.sources.opensearch.osis_config import with_role

GLUE_SERVICE_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"


def osis_trust_policy(account_id: str, region: str, pipeline_name: str) -> str:
    """Trust OpenSearch Ingestion, confined to this account and pipeline."""
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "osis-pipelines.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {"aws:SourceAccount": account_id},
                        "ArnLike": {
                            "aws:SourceArn": (
                                f"arn:aws:osis:{region}:{account_id}:pipeline/{pipeline_name}"
                            )
                        },
                    },
                }
            ],
        }
    )


def _service_trust(service: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}
            ],
        }
    )


def pipeline_policy(cfg: OpenSearchStackConfig, bucket_arn: str, key_arn: str | None) -> str:
    """Least privilege for the pipeline role: domain reads, raw/ writes, KMS, secret."""
    prefix = cfg.migration_id
    statements: list[dict[str, Any]] = [
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": bucket_arn,
            "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/raw/*"]}},
        },
        {
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
            "Resource": f"{bucket_arn}/{prefix}/raw/*",
        },
    ]
    if key_arn:
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["kms:Encrypt", "kms:GenerateDataKey*", "kms:Decrypt", "kms:DescribeKey"],
                "Resource": key_arn,
            }
        )
    if cfg.domain_arn:
        statements += [
            {"Effect": "Allow", "Action": ["es:DescribeDomain"], "Resource": cfg.domain_arn},
            {
                "Effect": "Allow",
                "Action": ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpHead", "es:ESHttpDelete"],
                "Resource": f"{cfg.domain_arn}/*",
            },
        ]
    if cfg.credentials_secret_arn:
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:GetSecretValue"],
                "Resource": cfg.credentials_secret_arn,
            }
        )
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


def domain_access_statement(domain_arn: str, principals: list[str]) -> dict[str, Any]:
    """The resource-policy statement a managed domain needs for the pipeline role."""
    return {
        "Effect": "Allow",
        "Principal": {"AWS": principals},
        "Action": ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpHead", "es:ESHttpDelete"],
        "Resource": f"{domain_arn}/*",
    }


def glue_policy(cfg: OpenSearchStackConfig, bucket_arn: str, key_arn: str | None) -> str:
    """Least privilege for the Glue role: read raw/ and manifest/, write canonical/ and rejects/."""
    prefix = cfg.migration_id
    statements: list[dict[str, Any]] = [
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": bucket_arn,
            "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*", prefix]}},
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [f"{bucket_arn}/{prefix}/raw/*", f"{bucket_arn}/{prefix}/manifest/*"],
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts",
            ],
            "Resource": [
                f"{bucket_arn}/{prefix}/canonical/*",
                f"{bucket_arn}/{prefix}/rejects/*",
                f"{bucket_arn}/{prefix}/manifest/glue/*",
            ],
        },
        {
            "Effect": "Allow",
            "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": "arn:aws:logs:*:*:*",
        },
    ]
    if key_arn:
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["kms:Encrypt", "kms:GenerateDataKey*", "kms:Decrypt", "kms:DescribeKey"],
                "Resource": key_arn,
            }
        )
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


def glue_default_arguments(cfg: OpenSearchStackConfig) -> dict[str, str]:
    """Job arguments fixed at provision time; the export stage adds the per-run ones."""
    return {
        "--job-language": "python",
        "--additional-python-modules": cfg.glue_python_modules,
        "--python-modules-installer-option": cfg.glue_installer_options,
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-metrics": "true",
        "--TempDir": f"s3://{cfg.bucket_name}/{cfg.migration_id}/manifest/glue/tmp/",
        "--region": cfg.region,
        "--bucket": cfg.bucket_name,
    }


def build(cfg: OpenSearchStackConfig) -> None:
    """Declare the add-on stack. Runs inside the Pulumi engine."""
    tags = resource_tags(cfg.migration_id, cfg.tags)
    names = cfg.names
    bucket_arn = f"arn:aws:s3:::{cfg.bucket_name}"
    identity = aws.get_caller_identity_output()

    pipeline_role = aws.iam.Role(
        f"{cfg.resource_prefix}-osis-role",
        name=names.osis_pipeline_role,
        assume_role_policy=identity.account_id.apply(
            lambda account: osis_trust_policy(account, cfg.region, cfg.pipeline_name)
        ),
        tags=tags,
    )
    aws.iam.RolePolicy(
        f"{cfg.resource_prefix}-osis-policy",
        role=pipeline_role.id,
        policy=pipeline_policy(cfg, bucket_arn, cfg.kms_key_arn),
    )

    log_group = aws.cloudwatch.LogGroup(
        f"{cfg.resource_prefix}-osis-logs",
        name=cfg.osis_log_group,
        retention_in_days=30,
        tags=tags,
    )
    pipeline_args: dict[str, Any] = {
        "pipeline_name": cfg.pipeline_name,
        "min_units": cfg.osis_min_units,
        "max_units": cfg.osis_max_units,
        "pipeline_configuration_body": pipeline_role.arn.apply(
            lambda arn: with_role(cfg.pipeline_body, arn)
        ),
        "log_publishing_options": aws.opensearchingest.PipelineLogPublishingOptionsArgs(
            is_logging_enabled=True,
            cloudwatch_log_destination=(
                aws.opensearchingest.PipelineLogPublishingOptionsCloudwatchLogDestinationArgs(
                    log_group=log_group.name
                )
            ),
        ),
        "tags": tags,
    }
    if cfg.kms_key_arn:
        pipeline_args["encryption_at_rest_options"] = (
            aws.opensearchingest.PipelineEncryptionAtRestOptionsArgs(kms_key_arn=cfg.kms_key_arn)
        )
    if cfg.subnet_ids:
        pipeline_args["vpc_options"] = aws.opensearchingest.PipelineVpcOptionsArgs(
            subnet_ids=list(cfg.subnet_ids),
            security_group_ids=list(cfg.security_group_ids),
        )
    pipeline = aws.opensearchingest.Pipeline(
        f"{cfg.resource_prefix}-osis",
        **pipeline_args,
        opts=pulumi.ResourceOptions(depends_on=[pipeline_role]),
    )

    glue_role = aws.iam.Role(
        f"{cfg.resource_prefix}-glue-role",
        name=names.glue_role,
        assume_role_policy=_service_trust("glue.amazonaws.com"),
        tags=tags,
    )
    aws.iam.RolePolicyAttachment(
        f"{cfg.resource_prefix}-glue-service",
        role=glue_role.name,
        policy_arn=GLUE_SERVICE_POLICY_ARN,
    )
    aws.iam.RolePolicy(
        f"{cfg.resource_prefix}-glue-policy",
        role=glue_role.id,
        policy=glue_policy(cfg, bucket_arn, cfg.kms_key_arn),
    )
    job = aws.glue.Job(
        f"{cfg.resource_prefix}-glue-job",
        name=cfg.glue_job_name,
        role_arn=glue_role.arn,
        glue_version=cfg.glue_version,
        worker_type=cfg.glue_worker_type,
        number_of_workers=cfg.glue_workers,
        timeout=cfg.glue_timeout_minutes,
        max_retries=0,
        execution_property=aws.glue.JobExecutionPropertyArgs(max_concurrent_runs=4),
        command=aws.glue.JobCommandArgs(
            name="glueetl", python_version="3", script_location=cfg.glue_script_uri
        ),
        default_arguments=glue_default_arguments(cfg),
        tags=tags,
    )

    pulumi.export("osis_pipeline_name", pipeline.pipeline_name)
    pulumi.export("osis_pipeline_arn", pipeline.pipeline_arn)
    pulumi.export("osis_pipeline_role_arn", pipeline_role.arn)
    pulumi.export("osis_log_group", log_group.name)
    pulumi.export("glue_job_name", job.name)
    pulumi.export("glue_role_arn", glue_role.arn)
    pulumi.export("source_indices", list(cfg.indices))

    if cfg.domain_arn:
        principals = pipeline_role.arn.apply(lambda arn: [arn, *cfg.extra_domain_principals])
        statement = principals.apply(
            lambda arns: json.dumps(domain_access_statement(str(cfg.domain_arn), list(arns)))
        )
        pulumi.export("domain_access_statement", statement)
        if cfg.manage_domain_policy and cfg.domain_name:
            aws.opensearch.DomainPolicy(
                f"{cfg.resource_prefix}-domain-policy",
                domain_name=cfg.domain_name,
                access_policies=statement.apply(
                    lambda s: json.dumps({"Version": "2012-10-17", "Statement": [json.loads(s)]})
                ),
            )


def program_for(cfg: OpenSearchStackConfig) -> Callable[[], None]:
    """Zero-argument program for the Automation API."""
    return lambda: build(cfg)
