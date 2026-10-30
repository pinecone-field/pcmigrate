# ADR-002: Run inside the customer's AWS account

**Status:** Proposed

## Context

Source data and database credentials are the customer's most sensitive assets. The first question every enterprise security review asks is whether data or credentials leave the customer's boundary during the migration.

## Decision

The runner (EC2 via SSM, no SSH), the S3 bucket, OSIS, and Glue all live in the customer's account and VPC. Pinecone reads staged files through a storage integration.

Because the resources are the customer's, they follow the customer's tagging and naming rules (amended 2026-10-30):

- **Tags.** `migration.tags` in `pcmigrate.yaml` is a map of up to 48 key-value pairs. Every taggable resource the stacks create carries them, twice over: as explicit `tags` on each resource and as the AWS provider's `default_tags`, so a resource added later cannot be missed. The tool adds `pcmigrate:managed_by` and `pcmigrate:migration_id`; the `pcmigrate:` key namespace is reserved and `aws:` is rejected. Keys and values follow AWS's character and length rules and pass the secret-value guard (ADR-014). The applied set is recorded in the provision stage's manifest outputs.
- **Names.** Every physical resource name starts with `pcmigrate`, or with `<name_prefix>-pcmigrate` when `migration.name_prefix` is set: roles and the instance profile `…-<id>-runner`, `…-<id>-pinecone-import`, `…-<id>-osis-pipeline`, `…-<id>-glue-transform`; the Glue job `…-<id>-opensearch-transform`; the SQS buffer `…-<id>-buffer.fifo`; the KMS alias `alias/…-<id>`; secrets `…/<id>/<name>`; the log group `/…/<id>`; and the OSIS pipeline `[<prefix>-]pcm-<id>`, lowercased and cut to 28 characters. The prefix is one to 32 characters of letters, digits, `-` and `_`, starting with a letter, and the combination with the migration id must keep every role name within IAM's 64 characters. The bucket keeps the name the customer gave it. Pulumi logical names and the stack name do not change, so adding a prefix to an existing migration renames resources in place rather than recreating a stack.

## Consequences

- Source data and credentials never leave the customer's boundary.
- The target index must be on AWS. Pinecone doesn't import from S3 into GCP- or Azure-hosted indexes.
- Resources not taggable in AWS stay untagged: inline role policies, the KMS alias, S3 objects, Glue job runs (which inherit the job's tags for cost allocation), and the log groups Glue creates on its own.
- Organization policies that require tags at creation time (`aws:RequestTag`) are satisfied for every resource except the bucket, which is created and then tagged in a second call; customers under such a policy use `bucket_mode: adopt`.
- The operator policy's resource ARNs match `pcmigrate-*`, `pcmigrate/*`, and `job/pcmigrate-*`; with a name prefix the customer widens them to `<prefix>-pcmigrate-*` and so on.
