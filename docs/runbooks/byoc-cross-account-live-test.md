# Live test: OpenSearch in one AWS account to Pinecone BYOC dedicated read nodes in another

Run on 2026-11-12 to verify the shape a customer hit: the source OpenSearch domain, the
runner, and the staging bucket in one AWS account; the Pinecone BYOC environment, the
dedicated-read-node index, and the import bucket in another (ADR-002, ADR-019). Every
command below was run as written; outputs and timings are recorded where they matter.

| Piece | Account | Region | Value |
|---|---|---|---|
| OpenSearch domain `pcm-byoc-src`, index `passages`, 1,000,000 docs x 1,024 dims | 429835537556 (source) | us-east-1 | 2 x r6g.2xlarge.search, 150 GB gp3 |
| Runner, Pulumi stacks, staging bucket | 429835537556 | us-east-1 | this laptop with administrator SSO stands in for the EC2 runner |
| Pinecone BYOC environment | 317871535823 (target) | us-east-1 | deployed with `pulumi-pinecone-byoc`, VPC 10.50.0.0/16 |
| Import bucket | 317871535823 | us-east-1 | `pcmigrate-byoc-import-317871535823`, SSE-KMS `alias/pcmigrate-byoc-import` |
| Target index | BYOC environment | | dedicated read nodes, b1, adopted with data already in two other namespaces |

The migration tool assumes the domain and the BYOC environment exist; sections 1 and 2
are the customer's prerequisites and run independently of each other.

## 1. Source: the OpenSearch domain (source account)

`tools/opensearch_dataset/configs/byoc-1m.yaml` describes the domain and the 1M-row
parquet set (`pcmigrate-data/2M_2indexes/index1`). From `tools/opensearch_dataset`, with
the source account's credentials exported:

```sh
uv run seed_opensearch.py --config configs/byoc-1m.yaml plan
uv run seed_opensearch.py --config configs/byoc-1m.yaml create-domain      # 15 to 25 min
uv run seed_opensearch.py --config configs/byoc-1m.yaml create-indices
uv run seed_opensearch.py --config configs/byoc-1m.yaml load               # resumable
uv run seed_opensearch.py --config configs/byoc-1m.yaml verify
```

`plan` reported 1,000,000 documents, 12.8 GB of bulk JSON (5.7 GB gzipped on the wire),
6.9 GB per index copy. Timings: `create-domain` 17 min (13:26 to 13:43); `load` 1,512 s
from a laptop at about 650 documents per second (upload bandwidth bound); `verify` reported
`passages expected=1,000,000 actual=1,000,000 ok`. The access policy is the account root,
so the migration sets `manage_domain_policy: true` and the stack adds the pipeline role.

## 2. Target: the Pinecone BYOC environment and the import bucket (target account)

### 2.1 Import bucket and key

Created before anything else; the bucket policy comes later because it names roles that
provision creates. With the target account's credentials:

```sh
KEY=$(aws kms create-key --description "pcmigrate BYOC import bucket" --query KeyMetadata.Arn --output text)
aws kms create-alias --alias-name alias/pcmigrate-byoc-import --target-key-id "$KEY"
aws s3api create-bucket --bucket pcmigrate-byoc-import-317871535823 --region us-east-1
aws s3api put-public-access-block --bucket pcmigrate-byoc-import-317871535823 \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket pcmigrate-byoc-import-317871535823 --server-side-encryption-configuration \
  "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$KEY\"},\"BucketKeyEnabled\":true}]}"
aws s3api put-bucket-ownership-controls --bucket pcmigrate-byoc-import-317871535823 --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
```

### 2.2 BYOC environment

Pinecone's deployment tooling (`pulumi-pinecone-byoc`) generates a Pulumi project from a
wizard and deploys it. The organization must be BYOC-enabled and the API key must belong to
a project in it. Prerequisites on the machine: `uv`, `pulumi`, `kubectl`, the AWS CLI, and
administrator credentials for the target account.

The wizard runs non-interactively from environment variables; the answers used here:

| Variable | Value | Why |
|---|---|---|
| `PINECONE_API_KEY` | the project key | validated by the wizard, stored as a Pulumi secret |
| `PINECONE_REGION` / `PINECONE_AZS` | `us-east-1` / `us-east-1a,us-east-1b` | same region as the source |
| `PINECONE_VPC_CIDR` | `10.50.0.0/16` | the account already had 10.0.0.0/16 and 172.31.0.0/16 |
| `PINECONE_PUBLIC_ACCESS` | `true` | the runner reaches the data plane over the internet |
| `PINECONE_DELETION_PROTECTION` | `false` | a test environment is torn down afterwards |
| `PINECONE_PROJECT_NAME` | `pcmigrate-byoc-test` | |
| `PINECONE_TAGS` | `pcmigrate:managed_by=live-test,Owner=...` | |
| `PULUMI_BACKEND` / `PULUMI_CONFIG_PASSPHRASE` | `local` / a generated passphrase | state under `~/.pulumi`; keep the passphrase, teardown needs it |

```sh
git clone https://github.com/pinecone-io/pulumi-pinecone-byoc.git
cd pulumi-pinecone-byoc
uv run python setup/wizard.py --cloud aws --non-interactive --stack-name pcmigrate-byoc --output-dir ../byoc-project
cd ../byoc-project && pulumi up --yes
```

The wizard's preflight checks (VPC, EIP, NAT, IGW, EKS quotas, instance types, CIDR)
passed. `pulumi up` output ends with the BYOC environment name, which is what
`--byoc-environment` and `target.environment` take.

What happened here: the first `pulumi up` ran 20 min and created 115 resources, then one
Helm release (cluster-autoscaler) failed on a transient `connection reset by peer`
downloading its chart from GitHub. A second `pulumi up` picked up where it stopped and
created the remaining 66 resources in another 20 min, including the load balancers, the
PrivateLink endpoint service, and the installer job (17 min on its own). Transient
`AccessDenied: ec2:GetSecurityGroupsForVpc` warnings from the ALB controller during that
run resolved themselves once its IAM policy propagated. Outputs:

```
environment               : "preprod-aws-us-east-1-143b.byoc"
cell_fqdn                 : "preprod-aws-us-east-1-143b.byoc.pinecone.io"
update_kubeconfig_command : "aws eks update-kubeconfig --region us-east-1 --name cluster-pineconefieldeng-byoc-143b"
```

So: expect about 40 minutes, and re-run `pulumi up` on a transient failure rather than
destroying.

### 2.3 The target index, with data already in it

The customer's index existed before the migration and held data in other namespaces, so
the test does the same: a dedicated-read-node (b1, 1 shard, 1 replica) vector index of
1,024 cosine dimensions created in the BYOC environment with the SDK, then 5,000 random
vectors upserted into each of two namespaces the migration does not target
(`legacy-a`, `legacy-b`). `scratchpad/preload_index.py` in the session notes does this;
the create call is:

```python
pc.indexes.create(
    name="pcm-byoc-passages",
    schema={"fields": {"_values": {"type": "dense_vector", "dimension": 1024, "metric": "cosine"}}},
    deployment={"deployment_type": "byoc", "environment": "preprod-aws-us-east-1-143b.byoc"},
    read_capacity={"mode": "Dedicated", "dedicated": {"node_type": "b1", "scaling": "Manual", "manual": {"shards": 1, "replicas": 1}}},
)
```

The control plane rejects a `read_capacity.dedicated` body without `scaling` and `manual`
(422 `missing field scaling`); the tool's own `ReadNodesSpec.read_capacity()` already
sends that shape. The nodes went `Migrating` to `Ready` in about 8 minutes, the host came
back as `pcm-byoc-passages-vjxlb1i.svc.preprod-aws-us-east-1-143b.byoc.pinecone.io`, and
both namespaces accepted writes: dedicated read nodes in this organization serve several
namespaces, so the migration's `passages` namespace will be the third.

## 3. The migration (source account)

Run directory `pcmigrate-runs/byoc-live/` with `pcmigrate.yaml` (full file in section 6).
Session variables for every command: the source account's AWS credentials, `AWS_REGION`,
`OPENSEARCH_URL`, `PINECONE_API_KEY`, and `PULUMI_CONFIG_PASSPHRASE`.

### 3.1 Introspect and approve

```sh
P introspect --source opensearch --migration-id byoc-live-01 --index pcm-byoc-passages \
  --table passages --window 60 --region us-east-1 \
  --byoc-environment preprod-aws-us-east-1-143b.byoc \
  --capacity dedicated --node-type b1 --shards 1 --replicas 1 --out ./pcmigrate-out
```

1 min 45 s; no blockers, one warning (the 3.2-hour single-snapshot export estimate). The
generated `target` block carried `deployment: byoc`, the environment, and the b1 read nodes.
Edited `target.mode: create` to `adopt`, then:

```sh
P approve --approver vamshi.e@pinecone.io --out ./pcmigrate-out
```

### 3.2 Provision

**Finding 1 (defect).** With `bucket_mode: create` the first `run --until provision` failed
before provisioning anything:

```
NoSuchBucket: An error occurred (NoSuchBucket) when calling the GetObject operation
```

`cli._context` loads the manifest from the staging bucket before the provision stage has
created it (`state.py:209 in load`). The bucket the stack is supposed to own must therefore
already exist, which `create` mode then collides with outside us-east-1. Workaround used
here, and what a customer should do until the fix lands: create the bucket by hand with a
public access block, and run with `bucket_mode: adopt`:

```sh
aws s3api create-bucket --bucket pcmigrate-byoc-live-429835537556 --region us-east-1
aws s3api put-public-access-block --bucket pcmigrate-byoc-live-429835537556 \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

```sh
P run --until provision
```

**Finding 2 (transient).** The base stack came up, then the OpenSearch add-on failed on
its first attempt setting the domain access policy (`manage_domain_policy: true`):

```
aws:opensearch:DomainPolicy ... InvalidTypeException: Error setting policy: [...]  (409)
```

The policy names the pipeline and runner roles the stack had created seconds earlier, and
OpenSearch could not resolve them yet. `pcmigrate resume --until provision` succeeded two
minutes later (provision 144 s in total). Expect this once; resume rather than teardown.

Provision outputs that the next steps need (`P status` shows them under provision):

| Output | Value |
|---|---|
| `runner_role_arn` | `arn:aws:iam::429835537556:role/byoc-pcmigrate-byoc-live-01-runner` |
| `storage_integration_role_arn` | `arn:aws:iam::429835537556:role/byoc-pcmigrate-byoc-live-01-pinecone-import` |
| `osis_pipeline_name` / `glue_job_name` | `byoc-pcm-byoc-live-01` / `byoc-pcmigrate-byoc-live-01-opensearch-transform` |
| `tags`, `resource_names` | the merged tag set and every prefixed physical name |

The name prefix and tags showed up on every resource as planned: roles
`byoc-pcmigrate-byoc-live-01-*`, secrets `byoc-pcmigrate/byoc-live-01/*`, log group
`/byoc-pcmigrate/byoc-live-01`, pipeline `byoc-pcm-byoc-live-01`.

### 3.3 Cross-account permissions (target account)

Two policies in the target account name three principals from the source account: the
runner role and the storage-integration role from the provision outputs, and the operator
identity the stages run as (here the laptop's SSO administrator role; on an EC2 runner, the
operator role from `docs/iam/`). Filled copies are kept next to the run directory.

- Bucket policy: `docs/iam/import-bucket-policy.json` with `<IMPORT_BUCKET>`, `<PREFIX>`
  (`pinecone/byoc-live-01`), and the three ARNs, applied with
  `aws s3api put-bucket-policy --bucket pcmigrate-byoc-import-317871535823 --policy file://...`.
- Key policy: the account-root administration statement plus encrypt, decrypt, re-encrypt,
  generate-data-key, and describe for the runner and operator, and decrypt plus describe for
  the integration role, applied with `aws kms put-key-policy --key-id <key arn> --policy-name default --policy file://...`.

**Finding 3 (defect, fixed in this PR).** The first version of
`docs/iam/import-bucket-policy.json` put `s3:GetBucketLocation` in the same statement as
`s3:ListBucket` under an `s3:prefix` condition, and S3 rejected it:

```
MalformedPolicy: Conditions do not apply to combination of actions and resources in statement
```

`GetBucketLocation` takes no prefix condition; it now has its own statement. After applying
the corrected policy, a probe from the source identity (`aws s3 cp`, `ls`, `rm` under the
prefix) succeeded, which is the check to run before starting the replicate stage.

### 3.4 Prepare the index through replicate

```sh
P resume --until replicate
```

| Stage | Result | Duration |
|---|---|---|
| prepare_index | adopted `pcm-byoc-passages`: BYOC environment, 1,024 cosine, b1 x 1 x 1 all matched; `passages` namespace absent, the two legacy namespaces ignored | 3 s |
| export | T0 recorded, OSIS pipeline released, `raw/passages/` grew to 218 objects, pipeline stopped, one Glue run; 1,000,000 rows into canonical parts | 27 min |
| transform | 1,000,000 rows, one partition | 25 min |
| emit | one batch of three parquet parts, 2.1 GB | 11 min |
| replicate | 3 objects, 2,100,209,215 bytes copied server-side into `s3://pcmigrate-byoc-import-317871535823/pinecone/byoc-live-01/import/batch-00000/` | 7 s |

The replicate copies carry the import bucket's SSE-KMS key and the `pcmigrate-source-etag`
metadata the stage uses to skip objects on a resume.

### 3.5 Import and validate

```sh
P resume --until validate
```

**Finding 4 (the BYOC import identity).** The first import, started with no storage
integration id, was accepted and failed 30 s later:

```
ImportFailedError: import 1 ended Failed: User error: Object inaccessible
```

A BYOC environment does not read customer buckets through Pinecone's managed account.
Its data importer assumes a role the BYOC stack created in the BYOC account,
`pc-storage-integration-<cell suffix>` (here `pc-storage-integration-143b`, trusted by the
account root, with `s3:ListBucket` and `s3:GetObject` on `*`; the EKS node role may assume
it). So no storage integration is needed, and the customer's registered integration role
is irrelevant for BYOC. What is needed: that role must be able to decrypt the import
bucket's KMS key, and be named in the bucket policy if the bucket policy restricts
principals. Added `arn:aws:iam::317871535823:role/pc-storage-integration-143b` to the
`PineconeImportReader` statement of the key policy (`kms:Decrypt`, `kms:DescribeKey`) and
to the read and list statements of the bucket policy, verified with an `assume-role` probe
that reads and decrypts one object, then resumed.

**Finding 5 (defect, fixed in PR #31).** The resume after fixing the policies
failed instantly with the same `Object inaccessible` message: the import stage resumed by
the recorded import id, and that import was already Failed. The stage now discards a
terminal Failed or Cancelled id (kept under `failed_import_ids` in the manifest) and
starts a fresh import. The rest of this run used that fix.

With the policies fixed and the restart fix in place, `resume --until validate`:

| Stage | Result | Duration |
|---|---|---|
| import | import 2 from `s3://pcmigrate-byoc-import-317871535823/pinecone/byoc-live-01/import/batch-00000/`, no storage integration id; Completed in 7.5 min, Pinecone reported 971,440 records imported while the namespace holds exactly 1,000,000 (the import progress counter under-reports; count parity is what matters) | 11.7 min |
| validate | every check passed, see below | 8.9 min |

| Check | Result |
|---|---|
| count_parity | `passages` expected 1,000,000, actual 1,000,000, stable over 2 polls |
| vector_fidelity | 1,000 sampled ids, 0 missing, 0 vector failures, min cosine 1.0 |
| metadata_fidelity | 1,000 sampled ids, 0 metadata failures |
| recall_at_k | k=10, 200 queries, mean 0.979 (threshold 0.95) |
| filter_parity | 20 filters, 17 exact, mean overlap 0.98 |
| id_diff | 0 missing, 0 extra, 0 rejected |

The two legacy namespaces kept their 5,000 vectors each; the index ended with three
namespaces on its dedicated read nodes.

## 4. Teardown, in order

1. `P teardown --yes` in the source account: destroys the OpenSearch add-on stack and the
   base stack, deletes the staged zones in the staging bucket and the replicated zone in
   the import bucket, keeps `manifest/`.
2. Delete every index in the BYOC environment (Pinecone requires this before the
   environment goes), then `pulumi destroy --yes` in the BYOC project directory with the
   same passphrase. With `deletion_protection: false` nothing has to be unprotected first.
3. Delete the import bucket (now empty) and schedule its KMS key for deletion.
4. `seed_opensearch.py --config configs/byoc-1m.yaml delete-domain --yes`.
5. Copy `manifest/state.json` out of the staging bucket for the record, then delete the
   bucket; the adopted bucket is never deleted by the tool.

What happened here: teardown took 86 s and reported `stack_destroyed: true`,
`staged_objects_deleted: 339`, `import_bucket_objects_deleted: 3`,
`pinecone_index_touched: false`; afterwards no role, pipeline, Glue job, log group, KMS
alias, or secret with the migration's name was left in the source account. The import
bucket was empty and deleted, its key scheduled for deletion, the domain delete requested,
and the staging bucket deleted after saving the manifest next to the run directory.
`pulumi destroy` of the BYOC environment removed 181 resources in 30 min 41 s with no
errors, after the index had been deleted through the API; `pulumi stack rm` then dropped
the state. The bill for the environment covered about three hours.

## 5. Findings and timings at a glance

| # | What | Outcome |
|---|---|---|
| 1 | `bucket_mode: create` fails before provision (manifest read first) | defect, fixed in PR #30 (merged) |
| 2 | domain access policy rejected seconds after role creation | transient, `resume` |
| 3 | import bucket policy template rejected (`GetBucketLocation` under a prefix condition) | defect, fixed with this runbook |
| 4 | BYOC imports run as the BYOC stack's `pc-storage-integration-<suffix>` role, not through a storage integration | document: grant that role on the import key and bucket |
| 5 | resume re-polled a Failed import | defect, fixed in PR #31 |

| Step | Time |
|---|---|
| OpenSearch domain create, 1M load | 17 min, 25 min |
| BYOC environment | 2 x 20 min (`pulumi up` twice) |
| dedicated read nodes Ready | 8 min |
| introspect | 2 min |
| provision (two attempts) | 2.5 min |
| export, transform, emit | 27, 25, 11 min |
| replicate (2.1 GB cross-account) | 7 s |
| import, validate | 12, 9 min |

## 6. Settings file used

```yaml
# BYOC cross-account live test (2026-11-12). Source account 429835537556, target account 317871535823.
migration:
  migration_id: byoc-live-01
  bucket: pcmigrate-byoc-live-429835537556
  bucket_mode: adopt                     # the CLI needs the bucket for the manifest before provision runs (see runbook finding 1)
  aws_region: us-east-1
  pinecone_api_key_secret: arn:aws:secretsmanager:us-east-1:000000000000:secret:placeholder
  workers: 8
  partition_rows: 2000000
  canonical_part_bytes: 268435456
  lock_ttl_seconds: 300
  name_prefix: byoc
  tags:
    "tmna:app-id": "LIVE-TEST"
    CostCenter: "pcmigrate"
    Owner: vamshi.e
  import_bucket:
    name: pcmigrate-byoc-import-317871535823
    prefix: pinecone/byoc-live-01
    kms_key_arn: arn:aws:kms:us-east-1:317871535823:key/4fecf3d0-30ce-4049-ae99-3f76b34e504f
mapping_path: pcmigrate-out/mapping.yaml
artifacts_dir: pcmigrate-out
pinecone_api_key_env: PINECONE_API_KEY

opensearch:
  url_env: OPENSEARCH_URL
  auth: sigv4
  export_mode: osis_glue
  domain_arn: arn:aws:es:us-east-1:429835537556:domain/pcm-byoc-src
  domain_name: pcm-byoc-src
  fine_grained_access: false
  manage_domain_policy: true
  osis_min_units: 2
  osis_max_units: 8
  scan_timeout_s: 21600
  glue_workers: 8
  glue_worker_type: G.1X
```
