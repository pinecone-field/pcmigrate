# Architecture decision records

Sixteen decisions shape the build. The first fifteen need sign-off before Phase 1; ADR-016 records the answer to the hybrid open question before Phase 6. The format is Status, Context, Decision, Consequences.

| ADR | Decision |
| --- | --- |
| [001](0001-python-cli.md) | Python 3.12 CLI, packaged with `uv` |
| [002](0002-customer-aws-account.md) | Run inside the customer's AWS account |
| [003](0003-s3-zones.md) | Stage everything in S3 zones; never stage on local disk |
| [004](0004-canonical-format-and-emitters.md) | Canonical record format, pluggable target emitters |
| [005](0005-introspection-first-approval-gate.md) | Introspection-first, with a hash-locked FDE approval gate |
| [006](0006-checkpointed-state-machine.md) | Checkpointed stage state machine, manifest in S3 |
| [007](0007-buffered-change-stream.md) | Optional buffered change stream for CDC and dual-write |
| [008](0008-pgvector-change-capture-tiers.md) | pgvector change capture: none, or one of three tiers |
| [009](0009-opensearch-osis-glue.md) | OpenSearch export via OSIS, transform via Glue |
| [010](0010-pulumi-automation-api.md) | Pulumi Automation API; the index stays out of IaC |
| [011](0011-prevalidate-import-abort.md) | Pre-validate every record; import with `abort` |
| [012](0012-file-sizing.md) | Parquet and JSONL file sizing rules |
| [013](0013-structured-events.md) | Structured events, terminal view first |
| [014](0014-security-baseline.md) | Least-privilege security baseline |
| [015](0015-testing-against-real-services.md) | Test against real services in Docker and a Pinecone test project |
| [016](0016-hybrid-query-combination.md) | Hybrid queries on document indexes: one ranker per request, fused client-side |
