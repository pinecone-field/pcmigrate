"""Pulumi program for the base stack: bucket, KMS key, secrets, runner and integration roles.

Called by the Automation API from core/stages/provision.py with a BaseStackConfig closed
over. The Pinecone index is never declared here (ADR-010). With ``adopt_bucket`` the stack
reads an existing bucket instead of creating one, so a shared staging bucket and its
registered storage integration can be reused; roles are still scoped to the migration prefix.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pulumi
import pulumi_aws as aws

from infra.base.config import BaseStackConfig

SSM_CORE_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"


def _tags(cfg: BaseStackConfig, **extra: str) -> dict[str, str]:
    return {"managed_by": "pcmigrate", "migration_id": cfg.migration_id, **cfg.tags, **extra}


def _assume_role_policy(service: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": service},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )


def storage_integration_trust_policy(principal_arn: str, external_id: str | None) -> str:
    """Trust policy letting Pinecone's principal assume the role, with an optional external id."""
    statement: dict[str, Any] = {
        "Effect": "Allow",
        "Principal": {"AWS": principal_arn},
        "Action": "sts:AssumeRole",
    }
    if external_id:
        statement["Condition"] = {"StringEquals": {"sts:ExternalId": external_id}}
    return json.dumps({"Version": "2012-10-17", "Statement": [statement]})


def bucket_scoped_policy(bucket_arn: str, key_arn: str, *, prefix: str, write: bool) -> str:
    """Least-privilege S3 and KMS access to one migration's prefix (ADR-014).

    Pinecone's integration role needs PutObject as well as read, per its documentation, so
    PutObject is granted in both modes; delete and multipart management stay runner-only.
    """
    actions = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"]
    if write:
        actions += [
            "s3:DeleteObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
        ]
    statements: list[dict[str, Any]] = [
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": bucket_arn,
            "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*", prefix]}},
        },
        {
            "Effect": "Allow",
            "Action": actions,
            "Resource": f"{bucket_arn}/{prefix}/*",
        },
        {
            "Effect": "Allow",
            "Action": ["kms:Decrypt", "kms:DescribeKey"]
            + (["kms:Encrypt", "kms:GenerateDataKey*"] if write else []),
            "Resource": key_arn,
        },
    ]
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


def secrets_read_policy(secret_arns: list[str]) -> str:
    """Read access to exactly the migration's secrets."""
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["secretsmanager:GetSecretValue"],
                    "Resource": secret_arns,
                }
            ],
        }
    )


def _managed_bucket(
    cfg: BaseStackConfig, key: aws.kms.Key, tags: dict[str, str], prefix: str
) -> tuple[pulumi.Output[str], pulumi.Output[str]]:
    """The bucket the stack owns: SSE-KMS, versioning, public access block, lifecycle."""
    bucket = aws.s3.Bucket(
        f"{cfg.resource_prefix}-bucket",
        bucket=cfg.bucket_name,
        force_destroy=True,
        tags=tags,
    )
    aws.s3.BucketPublicAccessBlock(
        f"{cfg.resource_prefix}-bucket-public-access",
        bucket=bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )
    aws.s3.BucketServerSideEncryptionConfiguration(
        f"{cfg.resource_prefix}-bucket-sse",
        bucket=bucket.id,
        rules=[
            aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
                apply_server_side_encryption_by_default=(
                    aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                        sse_algorithm="aws:kms", kms_master_key_id=key.arn
                    )
                ),
                bucket_key_enabled=True,
            )
        ],
    )
    aws.s3.BucketVersioning(
        f"{cfg.resource_prefix}-bucket-versioning",
        bucket=bucket.id,
        versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
            status="Enabled"
        ),
    )
    aws.s3.BucketLifecycleConfiguration(
        f"{cfg.resource_prefix}-bucket-lifecycle",
        bucket=bucket.id,
        rules=[
            aws.s3.BucketLifecycleConfigurationRuleArgs(
                id="expire-staged-data",
                status="Enabled",
                filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix=f"{prefix}/"),
                expiration=aws.s3.BucketLifecycleConfigurationRuleExpirationArgs(
                    days=cfg.staged_data_retention_days
                ),
                noncurrent_version_expiration=(
                    aws.s3.BucketLifecycleConfigurationRuleNoncurrentVersionExpirationArgs(
                        noncurrent_days=7
                    )
                ),
                abort_incomplete_multipart_upload=(
                    aws.s3.BucketLifecycleConfigurationRuleAbortIncompleteMultipartUploadArgs(
                        days_after_initiation=2
                    )
                ),
            )
        ],
    )
    return bucket.arn, bucket.bucket


def build(cfg: BaseStackConfig) -> None:
    """Declare the base stack. Runs inside the Pulumi engine."""
    tags = _tags(cfg)
    prefix = cfg.migration_id

    key = aws.kms.Key(
        f"{cfg.resource_prefix}-key",
        description=f"pcmigrate {cfg.migration_id} staged data",
        enable_key_rotation=True,
        deletion_window_in_days=7,
        tags=tags,
    )
    aws.kms.Alias(
        f"{cfg.resource_prefix}-key-alias",
        name=f"alias/{cfg.resource_prefix}",
        target_key_id=key.key_id,
    )

    bucket_arn: pulumi.Output[str]
    bucket_name: pulumi.Output[str]
    if cfg.adopt_bucket:
        existing = aws.s3.get_bucket_output(bucket=cfg.bucket_name)
        bucket_arn = existing.arn
        bucket_name = existing.bucket
    else:
        bucket_arn, bucket_name = _managed_bucket(cfg, key, tags, prefix)

    secrets: dict[str, aws.secretsmanager.Secret] = {}
    for short in cfg.secret_names:
        secrets[short] = aws.secretsmanager.Secret(
            f"{cfg.resource_prefix}-secret-{short}",
            name=cfg.secret_name(short),
            description=f"pcmigrate {cfg.migration_id}: {short} (value set by the FDE)",
            kms_key_id=key.arn,
            recovery_window_in_days=0,
            tags=tags,
        )
    secret_arns = pulumi.Output.all(*[s.arn for s in secrets.values()])

    runner_role = aws.iam.Role(
        f"{cfg.resource_prefix}-runner-role",
        name=f"{cfg.resource_prefix}-runner",
        assume_role_policy=_assume_role_policy("ec2.amazonaws.com"),
        tags=tags,
    )
    aws.iam.RolePolicyAttachment(
        f"{cfg.resource_prefix}-runner-ssm",
        role=runner_role.name,
        policy_arn=SSM_CORE_POLICY_ARN,
    )
    aws.iam.RolePolicy(
        f"{cfg.resource_prefix}-runner-bucket",
        role=runner_role.id,
        policy=pulumi.Output.all(bucket_arn, key.arn).apply(
            lambda args: bucket_scoped_policy(args[0], args[1], prefix=prefix, write=True)
        ),
    )
    aws.iam.RolePolicy(
        f"{cfg.resource_prefix}-runner-secrets",
        role=runner_role.id,
        policy=secret_arns.apply(lambda arns: secrets_read_policy(list(arns))),
    )
    aws.iam.InstanceProfile(
        f"{cfg.resource_prefix}-runner-profile",
        name=f"{cfg.resource_prefix}-runner",
        role=runner_role.name,
    )

    aws.cloudwatch.LogGroup(
        f"{cfg.resource_prefix}-logs",
        name=f"/pcmigrate/{cfg.migration_id}",
        retention_in_days=30,
        kms_key_id=key.arn,
        tags=tags,
    )

    if cfg.change_capture_enabled:
        queue = aws.sqs.Queue(
            f"{cfg.resource_prefix}-buffer",
            name=f"{cfg.resource_prefix}-buffer.fifo",
            fifo_queue=True,
            content_based_deduplication=False,
            deduplication_scope="messageGroup",
            fifo_throughput_limit="perMessageGroupId",
            message_retention_seconds=cfg.buffer_retention_seconds,
            visibility_timeout_seconds=120,
            kms_master_key_id=key.arn,
            tags=tags,
        )
        aws.iam.RolePolicy(
            f"{cfg.resource_prefix}-runner-buffer",
            role=runner_role.id,
            policy=queue.arn.apply(
                lambda arn: json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "sqs:SendMessage",
                                    "sqs:ReceiveMessage",
                                    "sqs:DeleteMessage",
                                    "sqs:GetQueueAttributes",
                                ],
                                "Resource": arn,
                            }
                        ],
                    }
                )
            ),
        )
        pulumi.export("buffer_queue_url", queue.url)
        pulumi.export("buffer_queue_arn", queue.arn)

    pulumi.export("bucket_name", bucket_name)
    pulumi.export("bucket_arn", bucket_arn)
    pulumi.export("kms_key_arn", key.arn)
    pulumi.export("secret_arns", pulumi.Output.all(**{k: v.arn for k, v in secrets.items()}))
    pulumi.export("runner_role_arn", runner_role.arn)

    if cfg.pinecone_principal_arn:
        integration_role = aws.iam.Role(
            f"{cfg.resource_prefix}-storage-integration-role",
            name=f"{cfg.resource_prefix}-pinecone-import",
            assume_role_policy=storage_integration_trust_policy(
                cfg.pinecone_principal_arn, cfg.pinecone_external_id
            ),
            tags=tags,
        )
        aws.iam.RolePolicy(
            f"{cfg.resource_prefix}-storage-integration-read",
            role=integration_role.id,
            policy=pulumi.Output.all(bucket_arn, key.arn).apply(
                lambda args: bucket_scoped_policy(args[0], args[1], prefix=prefix, write=False)
            ),
        )
        pulumi.export("storage_integration_role_arn", integration_role.arn)


def program_for(cfg: BaseStackConfig) -> Callable[[], None]:
    """The zero-argument program the Automation API expects."""
    return lambda: build(cfg)
