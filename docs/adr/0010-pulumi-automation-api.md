# ADR-010: Pulumi Automation API; the index stays out of IaC

**Status:** Proposed

## Context

Each migration needs a set of AWS resources created and later destroyed. Driving IaC from the CLI keeps provisioning in one command. The Pinecone index is the customer's production asset after cutover, and `teardown` must never be able to delete it.

## Decision

`pcmigrate provision` drives Pulumi from Python, with one stack per migration. It creates:

- VPC lookups and security groups
- the runner
- the S3 bucket and KMS key
- Secrets Manager entries
- IAM roles, including the Pinecone storage-integration role
- the OSIS pipeline and Glue job
- the SQS buffer and CloudWatch log groups

The Pinecone index is managed by the tool, never by IaC.

## Consequences

- `teardown` cannot delete the customer's production index.
- Index lifecycle lives in `prepare_index`, which has create and adopt modes.
