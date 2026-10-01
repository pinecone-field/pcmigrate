# ADR-006: Checkpointed stage state machine, manifest in S3

**Status:** Proposed

## Context

Migrations run for hours and fail for mundane reasons: a lost runner, an expired credential, a network blip. Restarting from zero is unacceptable, and introducing a database for coordination adds infrastructure the customer has to approve.

## Decision

The stages run in order:

`introspect → approve → provision → prepare_index → export → transform → emit → import → validate → sync → cutover → teardown`

Each stage is idempotent and records per-partition progress in `manifest/state.json`. A single-writer lock uses S3 conditional writes (`If-None-Match` / `If-Match`).

## Consequences

- Any host can resume a run. No DynamoDB is needed in v1.
- Each stage needs a defined "already done" check (`is_complete(manifest)`), and tests must kill and resume each stage.
