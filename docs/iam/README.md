# IAM roles for a pcmigrate migration

Five roles take part in a migration. The customer creates one, the operator role, before
anything runs; `pcmigrate provision` creates the other four with Pulumi, scoped to one
migration id, and `teardown` deletes them. Every created resource is named
`pcmigrate-<migration_id>-...`, which is what the operator policy's resource ARNs match.
With `migration.name_prefix: <prefix>` in `pcmigrate.yaml` the names become
`<prefix>-pcmigrate-<migration_id>-...` (secrets `<prefix>-pcmigrate/<migration_id>/...`,
the log group `/<prefix>-pcmigrate/<migration_id>`, the OSIS pipeline `<prefix>-pcm-<id>`);
widen the `pcmigrate-*`, `pcmigrate/*`, and `job/pcmigrate-*` patterns in the policy to match.
Every taggable resource carries `migration.tags` plus `pcmigrate:managed_by` and
`pcmigrate:migration_id`; the policy already holds each service's tagging action.

| Role | Created by | Trusted by | Used for |
|---|---|---|---|
| `pcmigrate-operator` (any name) | customer, from the files in this directory | `ec2.amazonaws.com` | the EC2 runner: Pulumi provision and teardown, every stage, Session Manager |
| `pcmigrate-<id>-runner` | base stack | `ec2.amazonaws.com` | the runner when the pgvector stack launches it; the role an OpenSearch-source runner needs at minimum |
| `pcmigrate-<id>-pinecone-import` | base stack | Pinecone's AWS account `713131977538` (optional external id) | the Pinecone storage integration that reads the import prefix |
| `pcmigrate-<id>-osis-pipeline` | OpenSearch stack | `osis-pipelines.amazonaws.com`, limited to this account and pipeline | the OSIS export pipeline |
| `pcmigrate-<id>-glue-transform` | OpenSearch stack | `glue.amazonaws.com` | the Glue transform job |

## The operator role (what the customer creates)

Files: [`ec2-trust-policy.json`](ec2-trust-policy.json) and
[`pcmigrate-operator-policy.json`](pcmigrate-operator-policy.json). Replace `<ACCOUNT_ID>`,
`<REGION>`, `<BUCKET>`, and `<DOMAIN_NAME>` in the policy, then:

```sh
aws iam create-role --role-name pcmigrate-operator --assume-role-policy-document file://ec2-trust-policy.json
aws iam attach-role-policy --role-name pcmigrate-operator --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name pcmigrate-operator --policy-name pcmigrate-operator --policy-document file://pcmigrate-operator-policy.filled.json
aws iam create-instance-profile --instance-profile-name pcmigrate-operator
aws iam add-role-to-instance-profile --instance-profile-name pcmigrate-operator --role-name pcmigrate-operator
```

Statement by statement:

| Sid | Grants | Needed by |
|---|---|---|
| Identity | `sts:GetCallerIdentity` | account detection in provision and the CLI |
| MigrationRoles | role, inline policy, attachment, and instance profile lifecycle plus `iam:PassRole`, on `role/pcmigrate-*` and `instance-profile/pcmigrate-*` | the four roles above |
| ServiceLinkedRoles | `iam:CreateServiceLinkedRole` for OSIS and Glue | first pipeline or job in the account |
| StagingBucket, StagingObjects | bucket configuration (create, policy, encryption, public access block, lifecycle, tags) and object get, put, delete on `<BUCKET>` | base stack (`bucket_mode: create` or `adopt`), every stage, teardown's zone cleanup |
| KmsKeys | key and alias lifecycle, key policy, encrypt and decrypt, on `*` (the key ARN does not exist before provision) | the migration key that protects the bucket, secrets, logs, and OSIS buffer |
| Secrets | create, describe, put, get, delete on `secret:pcmigrate/*` | the Pinecone key and source credential entries |
| OpenSearchIngestion | pipeline lifecycle, start, stop, update, validate | the export pipeline (armed at creation, started at T0, stopped after the scan) |
| GlueJobs | job lifecycle and job runs on `job/pcmigrate-*` | the transform job |
| Logs | log group lifecycle, retention, KMS association, resource policy, log writes, and the log delivery actions (`logs:CreateLogDelivery`, Get, Update, Delete, List) | `/pcmigrate/<id>`, the OSIS vended log group, and the pipeline's log publishing, which OSIS sets up as the caller |
| SourceDomain | `es:DescribeDomain`, `es:DescribeDomainConfig`, `es:UpdateDomainConfig`, the four `es:ESHttp*` actions on `domain/<DOMAIN_NAME>*` | introspection, direct scans, ground truth, and the access policy update when `manage_domain_policy: true` |
| VpcLookupsForPipeline | describe VPCs, subnets, security groups | only when the pipeline is placed in a VPC (self-managed source) |

For a pgvector source add the SQS queue lifecycle (`sqs:*Queue*` on `pcmigrate-*`) and the
EC2 actions the runner stack needs (security group lifecycle, `ec2:RunInstances`,
`ec2:TerminateInstances`, `ec2:Describe*`, `ec2:CreateTags`, `ssm:GetParameter` for the AMI
lookup).

## Tags and naming rules

- **Tags.** `migration.tags` is a map of up to 48 key-value pairs following AWS's rules
  (keys 1 to 128 characters, values up to 256, letters, digits, spaces and `_ . : / = + - @`,
  no `aws:` prefix). The `pcmigrate:` key namespace is reserved for the tool. The stacks put
  the merged set on every taggable resource and also as the AWS provider's default tags, and
  record it in the provision outputs of the manifest. Not taggable in AWS, so left bare:
  inline role policies, the KMS alias, S3 objects, Glue job runs (cost allocation uses the
  job's tags), and the `/aws-glue/...` log groups Glue creates itself.
- **Tags required at creation.** Organization policies that condition `Create*` actions on
  `aws:RequestTag` are satisfied for every resource except the bucket, which Pulumi creates
  and then tags. Under such a policy create the bucket first and use `bucket_mode: adopt`.
- **Name prefix.** `migration.name_prefix` is one to 32 characters of letters, digits, `-`
  and `_`, starting with a letter; the config is rejected when the prefix plus the migration
  id pushes any role name past IAM's 64 characters. The bucket keeps the name you give it.
  The prefix changes physical names only; Pulumi logical names and the stack name stay the
  same, so adding a prefix to a migration that was already provisioned renames resources in
  place on the next `provision`, which recreates IAM roles and the SQS queue (their names are
  immutable). Set the prefix before the first provision.

## The roles provision creates

**Runner** (`pcmigrate-<id>-runner`, with an instance profile of the same name):
`AmazonSSMManagedInstanceCore`; list and get, put, delete objects under the migration prefix;
decrypt with the migration key; `secretsmanager:GetSecretValue` on the migration's secrets;
with a change-capture buffer, send, receive, delete, and get attributes on the SQS queue.
The pgvector stack launches an instance with this profile; for an OpenSearch source the
customer launches the instance (see [the runner runbook](../runbooks/ec2-runner.md)) with
the operator role, which is a superset.

**Storage integration** (`pcmigrate-<id>-pinecone-import`): trust policy for Pinecone's
account, with `sts:ExternalId` when `pinecone_storage_external_id` is set; `s3:ListBucket`,
`s3:GetBucketLocation`, `s3:GetObject`, and `s3:PutObject` on the migration prefix; decrypt
with the migration key. Registered with Pinecone as in
[storage-integration.md](../runbooks/storage-integration.md).

**OSIS pipeline** (`pcmigrate-<id>-osis-pipeline`): trust limited to
`osis-pipelines.amazonaws.com` for this account and pipeline ARN; `es:DescribeDomain` and
the four `es:ESHttp*` actions on the domain; put objects under `raw/`; encrypt and decrypt
with the key; `GetSecretValue` on the credentials secret for self-managed clusters. The
domain's access policy must also allow this role (or `manage_domain_policy: true` writes
the statement).

**Glue transform** (`pcmigrate-<id>-glue-transform`): `AWSGlueServiceRole`; get objects
under `raw/` and `manifest/`; get, put, delete under `canonical/`, `rejects/`, and
`manifest/glue/`; CloudWatch Logs writes; encrypt and decrypt with the key.

## Outside AWS

A Pinecone API key for the project with control-plane rights (describe, create the index),
data-plane rights (import, upsert, query, fetch, list), and Inference when sparse vectors
are generated; and the storage integration registered in the project.
