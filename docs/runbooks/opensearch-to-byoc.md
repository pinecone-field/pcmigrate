# Migrating an OpenSearch index into a Pinecone BYOC index

This is the step list for the shape where the OpenSearch domain lives in one AWS account
(the *source account*) and the Pinecone BYOC environment lives in another (the *BYOC
account*). It assumes the domain, the BYOC environment, and the target index already
exist. The same steps apply when both are in one account; then the import bucket and its
policies are the only cross-account pieces that go away.

Verified end to end on 2026-11-12 (`byoc-cross-account-live-test.md` has the evidence).

## What you need before starting

| Item | Where | Notes |
|---|---|---|
| Runner EC2 instance with the operator role | source account | [`ec2-runner.md`](ec2-runner.md) sections 1 to 3; the role carries [`docs/iam/pcmigrate-operator-policy.json`](../iam/pcmigrate-operator-policy.json) |
| OpenSearch domain endpoint, ARN, and name | source account | the runner must reach the endpoint over HTTPS |
| Pinecone API key | Pinecone console | a project in the BYOC-enabled organization |
| BYOC environment name | `pulumi up` output or the Pinecone console | looks like `aws-us-west-2-fdf3.byoc` |
| Target index | BYOC environment | dimension and metric matching the source field; dedicated read nodes if wanted; it may already hold data in other namespaces |
| Import bucket | BYOC account, same region as the index | SSE-KMS or SSE-S3, public access blocked, object ownership "bucket owner enforced"; empty prefix for this migration |
| Someone who can edit the import bucket's policy and its KMS key policy | BYOC account | needed once, in step 5 |
| The BYOC environment's storage-integration role ARN | BYOC account IAM | named `pc-storage-integration-<suffix>`; `aws iam list-roles --query "Roles[?starts_with(RoleName,'pc-storage-integration')].Arn"` |

No Pinecone storage integration is needed. A BYOC data plane reads buckets in its own
account with the `pc-storage-integration-<suffix>` role, so `storage_integration_id` stays
unset.

## 1. Session on the runner

```sh
tmux new -s pcm                                   # later: tmux attach -t pcm
mkdir -p ~/migrations/<migration_id> && cd ~/migrations/<migration_id>
export AWS_REGION=<region> PULUMI_CONFIG_PASSPHRASE=<passphrase>    # keep the passphrase; resume and teardown need it
export PINECONE_API_KEY=<key>
export OPENSEARCH_URL=https://<domain endpoint>
alias P="uv --project ~/pcmigrate run pcmigrate"
pulumi login --local
```

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
| `<STORAGE_INTEGRATION_ROLE_ARN>` | the BYOC environment's `pc-storage-integration-<suffix>` role |

```sh
aws s3api put-bucket-policy --bucket <import bucket> --policy file://import-bucket-policy.filled.json
```

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

```sh
aws kms put-key-policy --key-id <key arn> --policy-name default --policy file://key-policy.json
```

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

If the import ends `Failed: User error: Object inaccessible`, the BYOC environment's
storage-integration role is missing from the key policy or the bucket policy (step 5). Fix
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
| `User error: Object inaccessible` on import | BYOC storage-integration role not granted on the key or bucket | step 5, then `P resume --until validate` |
