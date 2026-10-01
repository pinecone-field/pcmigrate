# ADR-015: Test against real services in Docker and a Pinecone test project

**Status:** Proposed

## Context

Bulk import must read from real object storage, and source behaviour (snapshots, RLS, `_source` handling) differs from any mock. Mocks can't cover the critical path.

## Decision

- Unit tests run on every PR.
- Integration tests run against Docker Compose pgvector and OpenSearch, a real S3 test bucket, and a Pinecone test project.
- Golden datasets include edge cases: nulls, nested objects, duplicate vectors, wide metadata, deletes during export.
- Every stage gets a kill-and-resume test.

## Consequences

- CI needs Pinecone and AWS test credentials as GitHub secrets, and test indexes are deleted after each run.
- Integration tests are gated behind manual dispatch and the `integration` pytest marker.
