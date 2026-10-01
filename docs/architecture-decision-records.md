# Architecture decision records

Fifteen decisions shape the build of `pcmigrate`. All are proposed and need sign-off before Phase 1. Each record follows the standard Context / Decision / Consequences form.

| ADR | Decision |
| --- | --- |
| [001](#adr-001-python-312-cli) | Python 3.12 CLI, packaged with `uv` |
| [002](#adr-002-run-inside-the-customers-aws-account) | Run inside the customer's AWS account |
| [003](#adr-003-s3-zones-no-local-staging) | Stage everything in S3 zones; never stage on local disk |
| [004](#adr-004-canonical-record-format-and-target-emitters) | Canonical record format, pluggable target emitters |
| [005](#adr-005-introspection-first-approval-gate) | Introspection-first, with a hash-locked FDE approval gate |
| [006](#adr-006-checkpointed-stage-state-machine) | Checkpointed stage state machine, manifest in S3 |
| [007](#adr-007-buffered-change-stream) | Buffered change stream for both CDC and dual-write |
| [008](#adr-008-pgvector-change-capture-tiers) | pgvector change capture in three tiers |
| [009](#adr-009-opensearch-via-osis-and-glue) | OpenSearch export via OSIS, transform via Glue |
| [010](#adr-010-pulumi-automation-api) | Pulumi Automation API; the index stays out of IaC |
| [011](#adr-011-pre-validate-then-import-with-abort) | Pre-validate every record; import with `abort` |
| [012](#adr-012-file-sizing) | Parquet and JSONL file sizing rules |
| [013](#adr-013-structured-events-terminal-first) | Structured events, terminal view first |
| [014](#adr-014-security-baseline) | Least-privilege security baseline |
| [015](#adr-015-testing-against-real-services) | Test against real services in Docker and a Pinecone test project |

## ADR-001: Python 3.12 CLI

**Decision.** Python 3.12. Built on `typer` for the CLI, `pydantic` v2 for config and schema models, `pyarrow`, `psycopg` 3, `opensearch-py`, `boto3`, `rich`, and the Pinecone Python SDK on API version 2026-07. Packaged with `uv`.

**Why.** The Pinecone SDK, pyarrow, and Glue PySpark share one language, so transform logic is reused between the runner and Glue.

**Consequences.** The `pcmigrate` package must be importable by the Glue job, so it can't depend on anything Glue can't install.

## ADR-002: Run inside the customer's AWS account

**Decision.** The runner (EC2 via SSM, no SSH), the S3 bucket, OSIS, and Glue all live in the customer's account and VPC. Pinecone reads staged files through a storage integration.

**Why.** Source data and credentials never leave the customer's boundary. That's the first question every enterprise security review asks.

**Consequences.** The target index must be on AWS. Pinecone doesn't import from S3 into GCP- or Azure-hosted indexes.

## ADR-003: S3 zones, no local staging

**Decision.** Every migration run writes under `s3://<bucket>/<migration_id>/`:

- `raw/` holds source-native exports.
- `canonical/` holds canonical Parquet.
- `import/` holds emitted import files.
- `rejects/` holds failed records with reasons.
- `manifest/` holds state, schema snapshot, approvals, and counts.

Export streams parts directly to S3.

**Why.** Bulk import reads from object storage anyway. S3 staging gives resume after host loss, parallel workers, an audit trail, and re-import without re-reading the source.

**Consequences.** Buckets use SSE-KMS, block public access, and a lifecycle rule. Staged data is a copy of customer data, so teardown deletes it unless the customer asks to retain it.

## ADR-004: Canonical record format and target emitters

**Decision.** Exporters write one canonical Parquet schema:

- `id`, `namespace`
- `dense` (list\<float32>), `sparse` (struct of indices/values)
- `text_fields` (map\<string,string>), `metadata` (JSON string)
- `source_pk`, `vector_hash`

Emitters turn canonical files into a target format:

- `VectorIndexEmitter` writes Parquet.
- `DocumentIndexEmitter` writes JSONL.gz.

The dense wave defaults to a vector index. Hybrid and FTS use a document index. A dense customer with hybrid or FTS on their roadmap should start on a document index; the report flags this.

**Why.** Index type is the costliest choice to undo. With a canonical layer, switching is a re-emit from S3, not a new source export.

**Consequences.** One extra pass over the data, in exchange for adding targets without touching exporters.

## ADR-005: Introspection-first approval gate

**Decision.** `pcmigrate introspect` is read-only. It writes three artifacts:

- `report.md` and `report.html`, the findings.
- `mapping.yaml`, the proposed mapping.
- `plan.lock`, a SHA-256 of the normalized mapping plus the source schema fingerprint.

`pcmigrate approve` records approver, time, and hash. Every later stage refuses to run unless an approval matches the current `plan.lock`.

**Why.** The mapping is where silent data loss happens. A human checks it once, and the tool enforces that the approved plan is the one that runs.

**Consequences.** Any mapping edit or source schema change invalidates approval, and a new review is needed.

## ADR-006: Checkpointed stage state machine

**Decision.** The stages run in order:

`introspect → approve → provision → prepare_index → export → transform → emit → import → validate → sync → cutover → teardown`

Each stage is idempotent and records per-partition progress in `manifest/state.json`. A single-writer lock uses S3 conditional writes (`If-None-Match` / `If-Match`).

**Why.** Any host can resume a run. No DynamoDB is needed in v1.

**Consequences.** Each stage needs a defined "already done" check, and tests must kill and resume each stage.

## ADR-007: Buffered change stream

**Decision.** All post-snapshot changes flow into one durable buffer:

- Source CDC feeds it.
- Customer dual-writes feed it through our SDK shim or HTTP endpoint.

The buffer is SQS FIFO by default, grouped by record id, with Kinesis as the high-volume option. An applier drains it only after import validation passes. It collapses changes to the last operation per id, then applies upserts and deletes.

**Why.** Bulk import only writes to namespaces that don't exist yet. A direct dual-write before import completes would create the namespace and fail the import.

**Consequences.** Changes stay buffered until import finishes, at least 10 minutes and often hours, so buffer retention is sized to the import ETA. A final id-diff reconcile removes records deleted mid-export.

## ADR-008: pgvector change capture tiers

**Decision.** The tier is chosen in `mapping.yaml`, based on what the customer's DBA permits:

1. **Watermark/ledger.** No DDL; deletes are caught by reconcile.
2. **Trigger outbox.** Ported from the field repo, with the PK column made configurable.
3. **Logical replication.** Uses `pgoutput`. On RDS, this needs `rds.logical_replication` and a restart.

Export workers share one `REPEATABLE READ` snapshot via `pg_export_snapshot()`. CDC starts from the LSN or watermark recorded at that instant.

**Why.** One consistent boundary means no change is lost or double-counted at the handoff.

**Consequences.** The snapshot transaction stays open for the whole export, so long exports hold back vacuum. Introspection estimates export duration and warns.

## ADR-009: OpenSearch via OSIS and Glue

**Decision.** An OSIS pipeline with the OpenSearch source writes NDJSON to `raw/`. A Glue PySpark job maps it to canonical Parquet. The emit stage is shared.

OpenSearch changes come through dual-write into the buffer (ADR-007), optionally with `updated_at`-filtered rescans.

**Why.** This follows the agreed pattern, is serverless, and scales without runner sizing. OSIS's OpenSearch source scans the index; it is not a change stream.

**Consequences.** Introspection must check that vectors are present in `_source`. If vectors are excluded from `_source` or served from derived source, export can't retrieve them, and the run stops at the gate.

## ADR-010: Pulumi Automation API

**Decision.** `pcmigrate provision` drives Pulumi from Python, with one stack per migration. It creates:

- VPC lookups and security groups
- the runner
- the S3 bucket and KMS key
- Secrets Manager entries
- IAM roles, including the Pinecone storage-integration role
- the OSIS pipeline and Glue job
- the SQS buffer and CloudWatch log groups

The Pinecone index is managed by the tool, never by IaC.

**Why.** `teardown` must never be able to delete the customer's production index.

**Consequences.** Index lifecycle lives in `prepare_index`, which has create and adopt modes.

## ADR-011: Pre-validate, then import with abort

**Decision.** Transform enforces every target rule and writes violations to `rejects/`:

- dimension
- scalar-only metadata
- reserved names (`_` and `$` prefixes in document indexes)
- size limits
- duplicate vectors

Import then runs with `error_mode=abort`.

**Why.** With `continue`, Pinecone skips bad records without saying which. Bulk import also drops records whose vector values duplicate another record's. Catching both ourselves makes every gap in the counts explainable.

**Consequences.** Duplicate-vector groups are reported at introspection. After the FDE decides, remaining duplicates load through the data API after import, not through import.

## ADR-012: File sizing

**Decision.** Import files target 512 MB to 1 GB each, never more than 10 GB. Each import stays under 100,000 files and 10,000 namespaces.

On-demand indexes have a 1 TB total input limit per import, and `.jsonl.gz` is counted at 10× its compressed size. Larger datasets split into multiple imports over disjoint namespaces, or use dedicated read nodes.

**Why.** These are Pinecone's published import limits. Mid-size files keep import parallel and retries cheap.

**Consequences.** The planner computes file counts and import batches at introspection time, so they appear in the report.

## ADR-013: Structured events, terminal first

**Decision.** Stages emit typed JSON events (`stage_started`, `partition_done`, `import_progress`, `validation_result`, and so on). Events go to stdout logs, to `manifest/events/`, and to a `rich` live view.

**Why.** The terminal UI ships in v1. CloudWatch dashboards or a web UI can subscribe later without touching stage code.

**Consequences.** No stage prints directly; everything goes through the event bus.

## ADR-014: Security baseline

**Decision.** Security rests on six rules:

- Source access uses read-only roles. CDC tiers 2 and 3 get separately scoped grants.
- Secrets live in Secrets Manager, never in config files.
- The runner uses SSM only, with no SSH.
- IAM roles are least-privilege, one per component.
- Data is encrypted with KMS at rest and TLS in transit.
- The report flags likely PII fields for the FDE.

**Why.** These clear standard enterprise reviews without exceptions.

**Consequences.** CDC tiers 2 and 3 need explicit DBA sign-off, which the report captures.

## ADR-015: Testing against real services

**Decision.**

- Unit tests run on every PR.
- Integration tests run against Docker Compose pgvector and OpenSearch, a real S3 test bucket, and a Pinecone test project.
- Golden datasets include edge cases: nulls, nested objects, duplicate vectors, wide metadata, deletes during export.
- Every stage gets a kill-and-resume test.

**Why.** Bulk import must read from real object storage, so mocks can't cover the critical path.

**Consequences.** CI needs Pinecone and AWS test credentials as GitHub secrets, and test indexes are deleted after each run.
