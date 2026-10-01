# ADR-007: Optional buffered change stream for CDC and dual-write

**Status:** Proposed

## Context

Bulk import only writes to namespaces that don't exist yet. A direct dual-write before import completes would create the namespace and fail the import. Post-snapshot changes from the source and from the customer's application both need somewhere durable to wait. Not every source keeps changing during a migration, though: static or batch-loaded datasets, and customers who agree a write freeze, gain nothing from a change path.

## Decision

Change capture is optional. When it's on, all post-snapshot changes flow into one durable buffer:

- Source CDC feeds it.
- Customer dual-writes feed it through our SDK shim or HTTP endpoint.

The buffer is SQS FIFO by default, grouped by record id, with Kinesis as the high-volume option. An applier drains it only after import validation passes. It collapses changes to the last operation per id, then applies upserts and deletes.

**When change capture is off.** `tier: none` provisions no buffer, applier, or write path, and skips `sync`, so the run goes straight from `validate` to `cutover`. It fits three cases:

- static or batch-loaded sources
- a planned write freeze from snapshot to cutover
- customers who can replay their own ingestion into Pinecone after cutover

Introspection warns if it measured writes and the tier is `none`, and the pre-cutover id diff still catches anything written after the snapshot.

## Consequences

- Changes stay buffered until import finishes, at least 10 minutes and often hours, so buffer retention is sized to the import ETA.
- A final id-diff reconcile removes records deleted mid-export.
- With `tier: none`, writes after the snapshot are missed unless the customer freezes writes or replays them; the report captures that agreement.
