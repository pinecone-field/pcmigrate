# ADR-009: OpenSearch export via OSIS, transform via Glue

**Status:** Proposed

## Context

OpenSearch exports can be large, and sizing a runner for them is guesswork. AWS already offers a serverless scan-and-sink path (OpenSearch Ingestion, OSIS) and a serverless transform engine (Glue). OSIS's OpenSearch source scans the index; it is not a change stream.

## Decision

An OSIS pipeline with the OpenSearch source writes NDJSON to `raw/`. A Glue PySpark job maps it to canonical Parquet. The emit stage is shared.

OpenSearch changes come through dual-write into the buffer (ADR-007), optionally with `updated_at`-filtered rescans.

## Consequences

- Serverless export and transform that scale without runner sizing.
- Introspection must check that vectors are present in `_source`. If vectors are excluded from `_source` or served from derived source, export can't retrieve them, and the run stops at the gate.
