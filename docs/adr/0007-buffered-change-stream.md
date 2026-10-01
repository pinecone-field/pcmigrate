# ADR-007: Buffered change stream for both CDC and dual-write

**Status:** Proposed

## Context

Bulk import only writes to namespaces that don't exist yet. A direct dual-write before import completes would create the namespace and fail the import. Post-snapshot changes from the source and from the customer's application both need somewhere durable to wait.

## Decision

All post-snapshot changes flow into one durable buffer:

- Source CDC feeds it.
- Customer dual-writes feed it through our SDK shim or HTTP endpoint.

The buffer is SQS FIFO by default, grouped by record id, with Kinesis as the high-volume option. An applier drains it only after import validation passes. It collapses changes to the last operation per id, then applies upserts and deletes.

## Consequences

- Changes stay buffered until import finishes, at least 10 minutes and often hours, so buffer retention is sized to the import ETA.
- A final id-diff reconcile removes records deleted mid-export.
