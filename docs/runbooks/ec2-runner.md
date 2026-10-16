# Running a migration from an EC2 runner in the customer account

The production shape of ADR-002: the migration runs on an EC2 instance in the customer's
account, reached through Session Manager (no SSH), using an IAM role instead of user
credentials. This runbook assumes the source already exists (an Amazon OpenSearch Service
domain holding the data) and walks from a fresh instance to a validated index. The pgvector
path differs only in the source block of the settings file and the stack it provisions.

The two live runs of this procedure are summarised in
[opensearch-live-test-results.md](opensearch-live-test-results.md) (dense) and
[opensearch-hybrid-live-test-results.md](opensearch-hybrid-live-test-results.md) (hybrid).

## 0. What the customer creates

**IAM role for the instance.** Create a role from
[`docs/iam/ec2-trust-policy.json`](../iam/ec2-trust-policy.json), attach the AWS managed
policy `AmazonSSMManagedInstanceCore`, and add
[`docs/iam/pcmigrate-operator-policy.json`](../iam/pcmigrate-operator-policy.json) as an
inline policy after replacing `<ACCOUNT_ID>`, `<REGION>`, `<BUCKET>`, and `<DOMAIN_NAME>`.
The policy covers both the operator work (Pulumi creates the roles, bucket, key, secrets,
OSIS pipeline, Glue job, and log groups, all named `pcmigrate-*`) and the stages themselves.
[`docs/iam/README.md`](../iam/README.md) describes every role involved, statement by statement.

```sh
aws iam create-role --role-name pcmigrate-operator --assume-role-policy-document file://docs/iam/ec2-trust-policy.json
aws iam attach-role-policy --role-name pcmigrate-operator --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name pcmigrate-operator --policy-name pcmigrate-operator --policy-document file://pcmigrate-operator-policy.filled.json
aws iam create-instance-profile --instance-profile-name pcmigrate-operator
aws iam add-role-to-instance-profile --instance-profile-name pcmigrate-operator --role-name pcmigrate-operator
```

**The instance.** Amazon Linux 2023, 8 vCPU and 16 GB (c6i.2xlarge or c7g.2xlarge), 100 GB
gp3, in the source's region, with outbound HTTPS (public subnet or NAT) to S3, OpenSearch,
Glue, OSIS, Secrets Manager, GitHub, `api.pinecone.io`, and the index host. Instance profile
`pcmigrate-operator`, no key pair, no inbound security group rules. Nothing large lands on
disk: the staged data streams through S3.

**Access to this repository.** The runner clones it over HTTPS with a GitHub token that can
read the repository, or over SSH with a deploy key; the token is pasted into the session,
never written to the instance.

**OpenSearch domain access.** The domain's access policy must allow `es:ESHttpGet`,
`es:ESHttpPost`, `es:ESHttpHead`, and `es:ESHttpDelete` for the runner role and the pipeline
role that provision creates (`pcmigrate-<migration_id>-runner` and
`pcmigrate-<migration_id>-osis-pipeline`). Either the policy already allows the account's
principals, or set `manage_domain_policy: true` so the stack writes the statement, or paste
the `domain_access_statement` output of provision into the policy by hand. With fine-grained
access control, set `fine_grained_access: true` so the pipeline role is mapped to `all_access`.

**Pinecone.** A project API key (control plane, data plane, and Inference when sparse
vectors are generated), and either an existing index to adopt or room to create one.

## 1. Connect and install (5 min)

```sh
aws ssm start-session --target <instance-id>        # from a laptop with the Session Manager plugin
sudo su - ec2-user
export AWS_REGION=<region>
aws sts get-caller-identity                            # expect .../pcmigrate-operator/i-...

sudo dnf install -y git tmux
curl -LsSf https://astral.sh/uv/install.sh | sh
curl -fsSL https://get.pulumi.com | sh
echo 'export PATH=$HOME/.local/bin:$HOME/.pulumi/bin:$PATH' >> ~/.bashrc && source ~/.bashrc
uv python install 3.12

git clone https://github.com/pinecone-field/pcmigrate.git ~/pcmigrate   # paste the token when asked
cd ~/pcmigrate && uv sync && uv run pcmigrate --help
```

## 2. Working directory and settings

```sh
mkdir -p ~/migrations/<migration_id> && cd ~/migrations/<migration_id>
cp ~/pcmigrate/examples/pcmigrate.example.yaml pcmigrate.yaml
```

Edit `pcmigrate.yaml`:

- `migration_id`: a fresh slug; it names the S3 prefix, the Pulumi stacks, and every AWS resource.
- `bucket` and `bucket_mode`: `create` lets the base stack own a new bucket; `adopt` reuses one.
- `storage_integration_id`: leave the placeholder until step 5.
- `pinecone_api_key_env: PINECONE_API_KEY`: keep it; the key is exported in the session
  (step 3). Step 5 shows how to move it into the secret that provision creates.
- `partition_rows: 2000000` and `canonical_part_bytes: 268435456` for production sizes.
- `opensearch`: `domain_arn`, `domain_name`, `export_mode: osis_glue`, `fine_grained_access`
  and `manage_domain_policy` as decided above, and OSIS and Glue sizing (`osis_min_units: 2`,
  `osis_max_units: 8`, `glue_workers: 8` moved 2M documents in 31 min). Remove `dsn_env`.

## 3. Session environment (every login)

```sh
tmux new -s pcm                      # later: tmux attach -t pcm
cd ~/migrations/<migration_id>
export AWS_REGION=<region> PULUMI_CONFIG_PASSPHRASE=<passphrase>
export PINECONE_API_KEY=<key>        # pasted, never written to a file
export OPENSEARCH_URL=https://$(aws opensearch describe-domain --domain-name <domain> --query DomainStatus.Endpoint --output text)
alias P="uv --project ~/pcmigrate run pcmigrate"
pulumi login --local                 # state under ~/.pulumi; back it up with the run directory
```

Long stages survive a dropped session inside tmux; detach with `Ctrl-b d`.

## 4. Introspect, review, approve (read-only)

```sh
P introspect --source opensearch --migration-id <migration_id> --index <index> \
   --table <index-a> --table <index-b> --window 60 --out ./pcmigrate-out \
   [--roadmap hybrid --sparse-from <text field> ...] \
   [--capacity dedicated --node-type b1 --shards N --replicas M --multi-namespace]
less pcmigrate-out/report.md
```

Resolve every blocker in the report. Then edit `pcmigrate-out/mapping.yaml` where needed:
`target.mode: adopt` plus the existing index name to migrate into an index the customer
created (its schema and read capacity must match the plan), `id_template: "{_id}"` to keep
the source document ids, metadata fields to drop. Approve to lock the plan:

```sh
P approve --approver <you@pinecone.io> --out ./pcmigrate-out
```

## 5. Provision, then register the storage integration and the key

```sh
P run --until provision              # base stack, then the OpenSearch stack: about 3 min
```

The outputs include `storage_integration_role_arn`, `secret_arns`, and
`domain_access_statement`. Register the integration as in
[storage-integration.md](storage-integration.md) and put its `id` into
`storage_integration_id` in `pcmigrate.yaml`. Optionally store the Pinecone key in the
secret provision created and drop `pinecone_api_key_env` from the settings, so later resumes
read it from Secrets Manager:

```sh
aws secretsmanager put-secret-value --secret-id pcmigrate/<migration_id>/pinecone-api-key --secret-string "$PINECONE_API_KEY"
```

If the domain policy is managed by hand, add the `domain_access_statement` now.

## 6. Run through validate

```sh
P resume --until validate
P status
```

Stages: prepare_index (creates or adopts the index; dedicated read nodes wait for Ready),
export (OSIS scan, then one Glue job per index), transform (pre-validation, duplicate routing,
sparse encoding when generated), emit, import (bulk import plus duplicate upserts), validate.
Every stage is idempotent: after a failure, fix the cause and run the same `resume`. Expect
about 2 h for 2M dense records and 5 h for the hybrid shape; the report lands in
`s3://<bucket>/<migration_id>/manifest/validation.md`.

## 7. Cutover and teardown

With `change_capture.tier: none`, `P resume` continues to sync, which is skipped, and cutover
runs the final reconcile. For the tiers that capture changes, see
[manual-live-test.md](manual-live-test.md) (`cutover --shadow`, `cutover --confirm`).

```sh
P teardown --yes                     # destroys the stacks and staged zones; keeps manifest/; never touches the index
```

Copy `pcmigrate-out/` and the Pulumi state off the instance before terminating it:

```sh
aws s3 sync pcmigrate-out s3://<bucket>/<migration_id>/artifacts/
```

## Troubleshooting

- `AccessDenied` during provision: the action is missing from the inline policy; add it and
  rerun `P run --until provision`, which is idempotent.
- `IndexSpecMismatchError` on prepare_index: the adopted index's schema, metric, or read
  capacity differs from the mapping; pcmigrate never reconfigures an index.
- `403 restricted to a single namespace` on a dedicated index: the project is not enabled for
  multi-namespace dedicated read nodes; one object per index, or ask Pinecone to enable it.
- The OSIS pipeline stays at 0 documents: the pipeline role is not allowed on the domain;
  check the access policy (step 0).
