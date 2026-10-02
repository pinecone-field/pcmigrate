# ADR-008: pgvector change capture: none, or one of three tiers

**Status:** Proposed

## Context

Customers differ in what their DBA permits: some allow no DDL, some allow triggers, some allow logical replication, and some need no change capture at all (ADR-007). The export and the change stream must share one consistent boundary so no change is lost or double-counted at the handoff.

## Decision

The tier is chosen in `mapping.yaml`, based on what the customer's DBA permits:

1. **Watermark/ledger.** No DDL; deletes are caught by reconcile.
2. **Trigger outbox.** Ported from the field repo, with the PK column made configurable.
3. **Logical replication.** Uses `pgoutput`. On RDS, this needs `rds.logical_replication` and a restart.

`none` is also valid (ADR-007). Introspection proposes `none` when the workload window measures zero writes, and `trigger_outbox` otherwise.

Export workers share one `REPEATABLE READ` snapshot via `pg_export_snapshot()`. CDC starts from the LSN or watermark recorded at that instant.

## Consequences

- One consistent boundary between snapshot and CDC.
- The snapshot transaction stays open for the whole export, so long exports hold back vacuum. Introspection estimates export duration and warns.
