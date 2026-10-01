# ADR-003: Stage everything in S3 zones; never stage on local disk

**Status:** Proposed

## Context

Pinecone bulk import reads from object storage. Staging on a runner's local disk would tie the run to one host, block parallel workers, and force a fresh source read after any host loss or re-import.

## Decision

Every migration run writes under `s3://<bucket>/<migration_id>/`:

- `raw/` holds source-native exports.
- `canonical/` holds canonical Parquet.
- `import/` holds emitted import files.
- `rejects/` holds failed records with reasons.
- `manifest/` holds state, schema snapshot, approvals, and counts.

Export streams parts directly to S3.

## Consequences

- Resume after host loss, parallel workers, an audit trail, and re-import without re-reading the source.
- Buckets use SSE-KMS, block public access, and a lifecycle rule.
- Staged data is a copy of customer data, so teardown deletes it unless the customer asks to retain it.
