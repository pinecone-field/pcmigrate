# ADR-012: Parquet and JSONL file sizing rules

**Status:** Proposed

## Context

Pinecone publishes import limits on file size, file count, namespace count, and total input per import. Files that are too small make import slow and file counts high; files that are too large make retries expensive.

## Decision

Import files target 512 MB to 1 GB each, never more than 10 GB. Each import stays under 100,000 files and 10,000 namespaces.

On-demand indexes have a 1 TB total input limit per import, and `.jsonl.gz` is counted at 10× its compressed size. Larger datasets split into multiple imports over disjoint namespaces, or use dedicated read nodes.

## Consequences

- Mid-size files keep import parallel and retries cheap.
- The planner computes file counts and import batches at introspection time, so they appear in the report.
