# ADR-011: Pre-validate every record; import with abort

**Status:** Proposed

## Context

With `error_mode=continue`, Pinecone bulk import skips bad records without saying which. Bulk import also drops records whose vector values duplicate another record's. Either behaviour leaves gaps in the counts that nobody can explain.

## Decision

Transform enforces every target rule and writes violations to `rejects/`:

- dimension
- scalar-only metadata
- reserved names (`_` and `$` prefixes in document indexes)
- size limits
- duplicate vectors

Import then runs with `error_mode=abort`.

## Consequences

- Every gap in the counts is explainable from `rejects/` and the duplicate report.
- Duplicate-vector groups are reported at introspection. After the FDE decides, remaining duplicates load through the data API after import, not through import.
