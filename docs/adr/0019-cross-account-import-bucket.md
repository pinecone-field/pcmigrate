# ADR-019: Replicate import batches to a bucket in the Pinecone BYOC account

**Status:** Accepted (2026-11-03)

## Context

Pinecone imports into a BYOC index only from a bucket in the same cloud account as the BYOC deployment. Some customers run the BYOC data plane in one AWS account and the source database, and therefore the runner and the staging bucket (ADR-002, ADR-003), in another. Moving the whole migration into the BYOC account is the simplest answer when the source is reachable from there, but it often is not.

## Decision

`migration.import_bucket` names a bucket in the BYOC account (`name`, optional `region`, `prefix`, and `kms_key_arn`). When it is set:

- A new stage, `replicate`, runs between `emit` and `import`. It copies every emitted batch from the staging bucket's `import/` zone to `<prefix>/import/<batch>/` in the import bucket with server-side copies, one manifest partition per batch, and records the copied URIs. An object whose copy already carries the source ETag in its metadata is not copied again, so a killed run resumes per object.
- `import` hands Pinecone the replicated URIs and refuses to run until `replicate` is complete.
- `teardown` deletes the replicated zone unless data is retained.
- The base stack grants the runner role write access and the storage-integration role read access to that prefix, and to its KMS key when one is named. The customer grants the same principals in the import bucket's policy and key policy, which can only name the roles after provision has created them.

Without `import_bucket`, `replicate` marks itself skipped and nothing else changes. The manifest, the canonical and rejects zones, the change buffer, validation, and sync stay with the staging bucket and the data API.

## Consequences

- One more stage in the ADR-006 order; the manifest gains a `replicate` record and `--until replicate` becomes valid.
- Staged import data exists twice until teardown, and the copy adds a few minutes per 100 GB; same-region copies carry no transfer cost.
- Which role Pinecone's BYOC data plane assumes for the storage integration is not documented. The customer may register the stack's integration role (now granted read on the import bucket) or a role created in the BYOC account; the integration must show Validated before import.
- Unit tests cover the copy, resume, skip, import gating, and teardown on two moto buckets; true cross-account permissions are exercised only by a real run, so the first customer run should start with a small table.
