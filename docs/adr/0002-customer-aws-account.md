# ADR-002: Run inside the customer's AWS account

**Status:** Proposed

## Context

Source data and database credentials are the customer's most sensitive assets. The first question every enterprise security review asks is whether data or credentials leave the customer's boundary during the migration.

## Decision

The runner (EC2 via SSM, no SSH), the S3 bucket, OSIS, and Glue all live in the customer's account and VPC. Pinecone reads staged files through a storage integration.

## Consequences

- Source data and credentials never leave the customer's boundary.
- The target index must be on AWS. Pinecone doesn't import from S3 into GCP- or Azure-hosted indexes.
