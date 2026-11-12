# Migrating an OpenSearch index into a Pinecone BYOC index

This is the complete, stand-alone step list for the shape where the OpenSearch domain lives
in one AWS account (the *source account*) and the Pinecone BYOC environment lives in another
(the *BYOC account*). It assumes the domain, the BYOC environment, and the target index
already exist, and covers everything else: the IAM role and EC2 runner in the source
account, the bucket in the BYOC account, and the migration itself. Every IAM and S3 step is
given both as CLI commands and as console clicks. The same steps apply when both sides are
in one account; then the import bucket and its policies are the only pieces that go away.

Verified end to end on 2026-11-12 (`byoc-cross-account-live-test.md` has the evidence).

## What you need before starting

| Item | Where | Notes |
|---|---|---|
| Administrator access to the source account, or at least IAM, EC2, and S3 rights | source account | for Part A |
| Someone with IAM, S3, and KMS rights in the BYOC account | BYOC account | for Part B and step 5; a one-time task |
| OpenSearch domain endpoint, ARN, and name | source account | `aws opensearch describe-domain --domain-name <domain> --query "DomainStatus.[Endpoint,ARN]"` |
| Pinecone API key | Pinecone console | a project in the BYOC-enabled organization |
| BYOC environment name | Pinecone console, or the `pulumi up` output of the BYOC deployment | looks like `aws-us-west-2-fdf3.byoc` |
| Target index | BYOC environment | dimension and metric matching the source field; dedicated read nodes if wanted; it may already hold data in other namespaces |
| The BYOC data-importer role ARN | BYOC account IAM | an IAM role Pinecone's BYOC deployment created, named `pc-storage-integration-<suffix>` despite not being a Pinecone storage integration; `aws iam list-roles --query "Roles[?starts_with(RoleName,'pc-storage-integration')].Arn"` |
| A GitHub token that can read this repository, or a deploy key | | pasted into the runner session, never written to disk |

No Pinecone storage integration is needed, and `storage_integration_id` stays unset. A
BYOC data plane reads buckets in its own account as the data-importer IAM role above; the
role only has to be allowed on the import bucket and its key (step 5).

Names used below, change them as you like: operator role and instance profile
`pcmigrate-operator`; staging bucket `<staging bucket>` in the source account; import bucket
`<import bucket>` in the BYOC account.

---

# Part A. Source account: the operator role and the runner

## A1. The operator IAM role

One role does everything in the source account: it is the instance role of the runner, the
identity Pulumi provisions with, and the identity every stage runs as. It needs the AWS
managed policy `AmazonSSMManagedInstanceCore` (for Session Manager) plus the inline policy
[`docs/iam/pcmigrate-operator-policy.json`](../iam/pcmigrate-operator-policy.json), which is
least privilege for exactly the resources the tool creates (statement by statement in
[`docs/iam/README.md`](../iam/README.md)).

**Prepare the policy document.** Copy `docs/iam/pcmigrate-operator-policy.json` to
`pcmigrate-operator-policy.filled.json` and replace:

| Placeholder | Value |
|---|---|
| `<ACCOUNT_ID>` | the source account id |
| `<REGION>` | the region of the domain and the runner |
| `<BUCKET>` | the staging bucket name you will put in `migration.bucket` |
| `<DOMAIN_NAME>` | the OpenSearch domain name |

Then add one statement for the import bucket in the BYOC account, so the runner may copy
into it. Append it to the `Statement` array:

```json
{
  "Sid": "ImportBucket",
  "Effect": "Allow",
  "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
  "Resource": ["arn:aws:s3:::<import bucket>", "arn:aws:s3:::<import bucket>/*"]
},
{
  "Sid": "ImportBucketKey",
  "Effect": "Allow",
  "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
  "Resource": "arn:aws:kms:<REGION>:<BYOC account id>:key/<key id>"
}
```

Drop `ImportBucketKey` if the import bucket uses SSE-S3. If you plan to set
`migration.name_prefix`, change every `pcmigrate-*`, `pcmigrate/*`, and `job/pcmigrate-*` in
the resource ARNs to `<prefix>-pcmigrate-*` and so on.

### A1 by CLI

```sh
aws iam create-role --role-name pcmigrate-operator \
  --assume-role-policy-document file://docs/iam/ec2-trust-policy.json
aws iam attach-role-policy --role-name pcmigrate-operator \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name pcmigrate-operator --policy-name pcmigrate-operator \
  --policy-document file://pcmigrate-operator-policy.filled.json
aws iam create-instance-profile --instance-profile-name pcmigrate-operator
aws iam add-role-to-instance-profile --instance-profile-name pcmigrate-operator \
  --role-name pcmigrate-operator
```

`docs/iam/ec2-trust-policy.json` is the standard EC2 trust policy:

```json
{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
```

### A1 by console

1. **IAM** > **Roles** > **Create role**.
2. Trusted entity type **AWS service**, use case **EC2**, **Next**.
3. In the permissions list search `AmazonSSMManagedInstanceCore`, tick it, **Next**.
4. Role name `pcmigrate-operator`, **Create role**. The console creates the instance profile
   of the same name for an EC2 role automatically.
5. Open the role, **Permissions** tab, **Add permissions** > **Create inline policy**.
6. Switch the editor to **JSON**, replace the content with
   `pcmigrate-operator-policy.filled.json`, **Next**.
7. Policy name `pcmigrate-operator`, **Create policy**.

Verify either way:

```sh
aws iam get-instance-profile --instance-profile-name pcmigrate-operator --query "InstanceProfile.Roles[].RoleName"
```

## A2. The runner instance

Sizing and placement: Amazon Linux 2023, 8 vCPU and 16 GB (`c6i.2xlarge` or
`c7g.2xlarge`), 100 GB gp3, in the domain's region, in a subnet with outbound HTTPS to S3,
OpenSearch, Glue, OpenSearch Ingestion, Secrets Manager, GitHub, `api.pinecone.io`, and the
index host (a public subnet with a public IP, or a private subnet behind a NAT gateway). No
key pair, no inbound security group rules. Nothing large lands on disk; the staged data
streams through S3.

### A2 by CLI

```sh
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
SG=$(aws ec2 create-security-group --group-name pcmigrate-runner --description "pcmigrate runner, egress only" --vpc-id <vpc-id> --query GroupId --output text)
aws ec2 run-instances --image-id "$AMI" --instance-type c6i.2xlarge \
  --subnet-id <subnet-id> --security-group-ids "$SG" \
  --iam-instance-profile Name=pcmigrate-operator \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
  --metadata-options HttpTokens=required \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pcmigrate-runner}]' \
  --query "Instances[0].InstanceId" --output text
```

Add `--associate-public-ip-address` for a public subnet. A new security group has no
inbound rules and allows all outbound traffic, which is what the runner needs.

### A2 by console

1. **EC2** > **Instances** > **Launch instances**.
2. Name `pcmigrate-runner`. AMI **Amazon Linux 2023** (64-bit x86). Instance type
   `c6i.2xlarge`.
3. Key pair: **Proceed without a key pair**.
4. Network settings > **Edit**: the VPC and subnet with outbound HTTPS; auto-assign public IP
   **Enable** for a public subnet; **Create security group** with no inbound rules.
5. Storage: 100 GiB gp3.
6. **Advanced details** > **IAM instance profile**: `pcmigrate-operator`. Metadata version
   **V2 only (token required)**.
7. **Launch instance**.

Wait until the instance shows in **Systems Manager** > **Fleet Manager** (one to two minutes
after launch), or:

```sh
aws ssm describe-instance-information --query "InstanceInformationList[].InstanceId"
```

## A3. Connect and install

From a laptop with the AWS CLI and the Session Manager plugin, or from the console with
**EC2** > select the instance > **Connect** > **Session Manager**:

```sh
aws ssm start-session --target <instance-id>
sudo su - ec2-user
export AWS_REGION=<region>
aws sts get-caller-identity                 # expect .../pcmigrate-operator/i-...

sudo dnf install -y git tmux
curl -LsSf https://astral.sh/uv/install.sh | sh
curl -fsSL https://get.pulumi.com | sh
echo 'export PATH=$HOME/.local/bin:$HOME/.pulumi/bin:$PATH' >> ~/.bashrc && source ~/.bashrc
uv python install 3.12

git clone https://github.com/pinecone-field/pcmigrate.git ~/pcmigrate    # paste the token when asked
cd ~/pcmigrate && uv sync && uv run pcmigrate --help
```

## A4. Domain access

The OpenSearch domain's access policy must allow the four `es:ESHttp*` actions for two
roles that provision creates: `pcmigrate-<migration_id>-runner` and
`pcmigrate-<migration_id>-osis-pipeline` (with `name_prefix`, `<prefix>-pcmigrate-...`).
Either the policy already allows the account's principals, or set
`manage_domain_policy: true` in the settings so the stack writes the statement (step 4), or
paste the `domain_access_statement` output of provision into the policy by hand. With
fine-grained access control, set `fine_grained_access: true` so the pipeline role is mapped
to `all_access`.

---

# Part B. BYOC account: the import bucket

Pinecone imports into a BYOC index only from a bucket in the BYOC account, so the migration
copies its import files into one. Create it once, in the index's region, empty, with public
access blocked and object ownership "bucket owner enforced" (the default), encrypted with
SSE-S3 or with a KMS key in the BYOC account. Its bucket policy and key policy are filled in
at step 5, after provision has created the roles they name.

### B by CLI

```sh
aws s3api create-bucket --bucket <import bucket> --region <region> \
  --create-bucket-configuration LocationConstraint=<region>        # omit the configuration in us-east-1
aws s3api put-public-access-block --bucket <import bucket> \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# SSE-KMS, optional:
KEY=$(aws kms create-key --description "pcmigrate import bucket" --query KeyMetadata.Arn --output text)
aws kms create-alias --alias-name alias/pcmigrate-import --target-key-id "$KEY"
aws s3api put-bucket-encryption --bucket <import bucket> --server-side-encryption-configuration \
  "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$KEY\"},\"BucketKeyEnabled\":true}]}"
```

### B by console

1. **S3** > **Create bucket**: name, the index's region, **Block all public access** ticked,
   Object Ownership **ACLs disabled**, default encryption **SSE-S3** or **SSE-KMS** with a
   key from this account (create one under **KMS** > **Customer managed keys** > **Create
   key**, symmetric, encrypt and decrypt, if needed), **Create bucket**.

---

# Part C. The migration (on the runner)

## 1. Session

Every login:

```sh
tmux new -s pcm                                   # later: tmux attach -t pcm
mkdir -p ~/migrations/<migration_id> && cd ~/migrations/<migration_id>
export AWS_REGION=<region> PULUMI_CONFIG_PASSPHRASE=<passphrase>    # keep the passphrase; resume and teardown need it
export PINECONE_API_KEY=<key>
export OPENSEARCH_URL=https://<domain endpoint>
alias P="uv --project ~/pcmigrate run pcmigrate"
pulumi login --local
```

Long stages survive a dropped session inside tmux; detach with `Ctrl-b d`.

## 2. Settings file

`cp ~/pcmigrate/examples/pcmigrate.example.yaml pcmigrate.yaml` and edit:

```yaml
migration:
  migration_id: <slug>                       # names the S3 prefix, the stacks, and every resource
  bucket: <staging bucket in the source account>
  bucket_mode: create                        # the CLI creates it; adopt reuses an existing one
  aws_region: <region>
  pinecone_api_key_secret: arn:aws:secretsmanager:<region>:000000000000:secret:placeholder
  workers: 8
  partition_rows: 2000000
  canonical_part_bytes: 268435456
  # name_prefix: <prefix>                    # optional: <prefix>-pcmigrate-<id>-... on every resource
  # tags:                                    # optional: on every AWS resource the stacks create
  #   CostCenter: "..."
  import_bucket:
    name: <import bucket in the BYOC account>
    prefix: pinecone/<migration_id>          # optional, defaults to the migration id
    kms_key_arn: arn:aws:kms:<region>:<BYOC account>:key/<id>   # only with SSE-KMS
mapping_path: pcmigrate-out/mapping.yaml
artifacts_dir: pcmigrate-out
pinecone_api_key_env: PINECONE_API_KEY

opensearch:
  url_env: OPENSEARCH_URL
  auth: sigv4
  export_mode: osis_glue
  domain_arn: arn:aws:es:<region>:<source account>:domain/<domain>
  domain_name: <domain>
  fine_grained_access: false                 # true maps the pipeline role into all_access
  manage_domain_policy: true                 # false if the domain policy already allows the account's roles
  osis_min_units: 2
  osis_max_units: 8
  glue_workers: 8
  glue_worker_type: G.1X
```

Leave `storage_integration_id` out. Keep `pinecone_api_key_env`; the key comes from the
session until provision creates the secret.

## 3. Introspect, review, approve

```sh
P introspect --source opensearch --migration-id <migration_id> --index <target index> \
  --table <source index> --window 60 --region <region> \
  --byoc-environment <environment name> \
  [--capacity dedicated --node-type b1 --shards N --replicas M] \
  --out ./pcmigrate-out
less pcmigrate-out/report.md
```

Pass `--capacity dedicated` with the node type, shards, and replicas the index actually has;
adopt mode refuses a plan whose read capacity differs from the index. Resolve every blocker
in the report, then edit `pcmigrate-out/mapping.yaml`:

- `target.mode: adopt` (the index exists). Confirm `target.deployment: byoc` and
  `target.environment` are present.
- `objects[].namespace`: must not already exist in the index. Import refuses an existing
  namespace.
- `objects[].id_template: "{_id}"` to keep the source document ids.

```sh
P approve --approver <you@company.com> --out ./pcmigrate-out
```

Any later edit to the mapping needs a new approval.

## 4. Provision

```sh
P run --until provision
```

About three minutes: the base stack (KMS key, secrets, roles, log group, staging bucket
configuration) and the OpenSearch stack (OSIS pipeline armed for later, Glue job, roles).
If the domain access policy update fails with `InvalidTypeException` right after the roles
were created, that is IAM propagation; run `P resume --until provision`.

Note these outputs from the summary (`P status` shows them under provision):
`runner_role_arn`, `storage_integration_role_arn`, `domain_access_statement`.

If `manage_domain_policy` is false, add `domain_access_statement` to the domain's access
policy by hand now.

## 5. Grant the import bucket (BYOC account)

The owner of the BYOC account applies two policies. Fill
[`docs/iam/import-bucket-policy.json`](../iam/import-bucket-policy.json) with:

| Placeholder | Value |
|---|---|
| `<IMPORT_BUCKET>` | the import bucket name |
| `<PREFIX>` | `import_bucket.prefix`, or the migration id |
| `<RUNNER_ROLE_ARN>` | `runner_role_arn` from step 4 |
| `<OPERATOR_ROLE_ARN>` | the runner instance's operator role |
| `<STORAGE_INTEGRATION_ROLE_ARN>` | the BYOC data-importer role, `pc-storage-integration-<suffix>` |

By CLI:

```sh
aws s3api put-bucket-policy --bucket <import bucket> --policy file://import-bucket-policy.filled.json
```

By console: **S3** > the bucket > **Permissions** > **Bucket policy** > **Edit**, paste the
filled document, **Save changes**.

With SSE-KMS, the key policy must keep the account-root administration statement and add:

```json
{
  "Sid": "PcmigrateWriters",
  "Effect": "Allow",
  "Principal": {"AWS": ["<RUNNER_ROLE_ARN>", "<OPERATOR_ROLE_ARN>"]},
  "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
  "Resource": "*"
},
{
  "Sid": "PineconeImportReader",
  "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::<BYOC account>:role/pc-storage-integration-<suffix>"},
  "Action": ["kms:Decrypt", "kms:DescribeKey"],
  "Resource": "*"
}
```

By CLI:

```sh
aws kms put-key-policy --key-id <key arn> --policy-name default --policy file://key-policy.json
```

By console: **KMS** > **Customer managed keys** > the key > **Key policy** > **Switch to
policy view** > **Edit**, add the two statements inside the existing `Statement` array,
**Save changes**.

Check from the runner before going on:

```sh
echo probe > probe.txt
aws s3 cp probe.txt s3://<import bucket>/<prefix>/probe.txt && aws s3 rm s3://<import bucket>/<prefix>/probe.txt
```

## 6. Export, transform, emit, replicate

```sh
P resume --until replicate
P status
```

For 1M documents of 1,024 dimensions this took 27 min export, 25 min transform, 11 min
emit, and 7 s for the cross-account copy. Confirm the replicate row shows the batch count
and bytes, and that the objects are visible in the import bucket under
`<prefix>/import/batch-*/`.

## 7. Import and validate

```sh
P resume --until validate
P status
```

Import polls Pinecone until the batch completes; validation then checks count parity,
vector and metadata fidelity, recall, filter parity, and the id diff, and writes
`manifest/validation.json` plus the report to the staging bucket.

If the import ends `Failed: User error: Object inaccessible`, the BYOC data-importer role
is missing from the key policy or the bucket policy (step 5). Fix
the policy and run `P resume --until validate` again; the stage starts a fresh import.

## 8. Sync and cutover (only with change capture)

With `change_capture.tier` other than `none` in the mapping, `P resume` keeps the index
current until `P cutover --confirm`. With tier `none`, the migration is complete after
validation.

## 9. Teardown

```sh
P teardown
```

Destroys both stacks, deletes the staged zones in the staging bucket and the copies in the
import bucket, keeps `manifest/` in the staging bucket, and never touches the Pinecone
index. Delete the staging bucket yourself once the manifest is no longer needed. The import
bucket and its key are the BYOC account owner's to remove.

## Errors seen in practice

| Symptom | Cause | Action |
|---|---|---|
| `IndexSpecMismatchError ... cloud None != 'aws'` | the plan says managed, the index is BYOC | add `--byoc-environment` at introspect, or `target.deployment: byoc` and `target.environment` in the mapping, re-approve |
| `read capacity is on-demand, mapping says dedicated` (or the reverse) | `--capacity` does not match the index | re-run introspect with the index's real capacity, re-approve |
| `index has a document schema, mapping says target.type vector` | hybrid or full-text roadmap against a plain vector index | sparse and text need a document index; dense only into this one, or a new document index |
| `already has namespaces [...]` | the target namespace exists | choose a new namespace name or empty the old one |
| `InvalidTypeException: Error setting policy` during provision | roles not yet visible to OpenSearch | `P resume --until provision` |
| `AccessDenied ... iam:CreateRole` during provision | the runner role lacks the operator policy | attach `docs/iam/pcmigrate-operator-policy.json` to it |
| `User error: Object inaccessible` on import | BYOC data-importer role not granted on the key or bucket | step 5, then `P resume --until validate` |
