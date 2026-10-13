# Pinecone Migration Tool: ADRs, Design, and Build Plan

Oct 1, 2026 · @Vamshi Krishna Enabothala

## Context and scope

`pcmigrate` moves customer data from pgvector and OpenSearch into Pinecone in three capability waves: dense first, then hybrid, then full-text search (FTS). Every run stops after introspection and publishes a findings report. Nothing is created or moved until an FDE approves it.

The tool runs in the customer's AWS account, stages data in S3, and loads it with Pinecone bulk import. An optional buffered change path keeps Pinecone current until cutover, for sources that stay live during the migration.

### Capability waves

| Wave | Source signals | Pinecone target | Import file |
| --- | --- | --- | --- |
| 1. Dense | pgvector `vector`, `halfvec`; OpenSearch `knn_vector` | Vector index (dense) | Parquet: `id`, `values`, `metadata` |
| 2. Hybrid | Wave 1 plus pgvector `sparsevec`; OpenSearch `rank_features` / neural sparse fields; or sparse vectors generated from chosen text fields with a hosted sparse model (ADR-017) | Document index with `dense_vector` + `sparse_vector` fields (ADR-004) | JSONL / JSONL.gz |
| 3. FTS | PostgreSQL `tsvector` columns and GIN text indexes; OpenSearch `text` fields and analyzers | Document index with `full_text_search` string fields, API 2026-07 | JSONL / JSONL.gz |

Pinecone's [import guide](https://docs.pinecone.io/guides/index-data/import-data) confirms both formats: Parquet for vector indexes, JSONL for indexes with a document schema. FTS and the Documents API are [generally available](https://www.pinecone.io/learn/pinecone-documents-api/) as of September 2026.

### Goals

- One CLI that takes a migration from introspection to cutover for a supported source.
- An introspection report an FDE can approve, edit, or reject before any write.
- Restartable stages: any failure resumes from the last checkpoint, not from zero.
- Provable correctness: counts, sampled fidelity, recall, and an id-level diff before cutover.
- New sources added by implementing one connector interface.

### Non-goals for v1

- Re-embedding the dense vectors or changing the dense embedding model. Generating a sparse vector from text for the hybrid wave is in scope (ADR-017).
- Rewriting the customer's query layer. We document query mappings; we don't rewrite application code.
- OpenSearch aggregations, dashboards, ingest pipelines, or security roles.
- Pinecone indexes on GCP or Azure. S3 import only works into AWS-hosted indexes.

### Source order

pgvector first, then OpenSearch. The [pgvector-migration repo](https://github.com/pinecone-field/pgvector-migration) gives us tested discovery and sync logic, and pgvector exercises the hardest framework piece (the snapshot-to-CDC handoff) without OSIS and Glue.

## High-level design and architecture

A single orchestrator runs a gated, checkpointed stage pipeline. Source connectors sit behind one interface. Everything from the canonical S3 zone onward is shared across sources.

![pcmigrate architecture: control plane, full-load path, change path](images/pcmigrate_high_level_design.png)

The top row is the control plane. Introspection is read-only, and the FDE approves one hash. The lower lanes run in the customer's account: the full load (top lane) lands through bulk import, and changes (bottom lane) wait in the buffer until validation passes, because import can't write into an existing namespace.

### Components

| Component | Responsibility | Shared or per source |
| --- | --- | --- |
| Orchestrator (`core/`) | Stage state machine, S3 manifest and lock, resume, event bus | Shared |
| Source connector (`sources/<kind>/`) | `introspect`, `plan_partitions`, `export`, `change_feed` | Per source |
| Canonical writer | One Parquet schema for dense, sparse, text, and metadata | Shared |
| Emitters (`targets/emitters/`) | Canonical to vector-index Parquet or document-index JSONL | Shared |
| Index manager and importer | Create or adopt the index, start and poll imports, batch by limits | Shared |
| Validator | Counts, fidelity, recall, filters, id diff, hybrid and FTS overlap | Shared, with per-source ground truth |
| Buffer and applier | Durable change queue, collapse per id, upsert and delete | Shared |
| Write path (`writepath/`) | SDK shim and HTTP endpoint for customer dual-writes | Shared |
| Infra (`infra/`) | Pulumi stacks: base, pgvector runner, OpenSearch OSIS and Glue | Base shared, add-ons per source |

### Stage lifecycle

Stages run in order: `introspect → approve → provision → prepare_index → export → transform → emit → import → validate → sync → cutover → teardown`.

- `introspect` and `approve` need no infrastructure, so an FDE can run them from a laptop with network access to the source.
- `provision` onward runs on the runner inside the VPC.
- `sync` is skipped when change capture is off; otherwise it runs continuously until `cutover --confirm`.

## Architecture decision records

Fifteen decisions shape the build. All are proposed and need sign-off before Phase 1. In the repo, each becomes `docs/adr/NNNN-<slug>.md` in the standard Context / Decision / Consequences form.

| ADR | Decision |
| --- | --- |
| 001 | Python 3.12 CLI, packaged with `uv` |
| 002 | Run inside the customer's AWS account |
| 003 | Stage everything in S3 zones; never stage on local disk |
| 004 | Canonical record format, pluggable target emitters |
| 005 | Introspection-first, with a hash-locked FDE approval gate |
| 006 | Checkpointed stage state machine, manifest in S3 |
| 007 | Optional buffered change stream for CDC and dual-write |
| 008 | pgvector change capture: none, or one of three tiers |
| 009 | OpenSearch export via OSIS, transform via Glue |
| 010 | Pulumi Automation API; the index stays out of IaC |
| 011 | Pre-validate every record; import with `abort` |
| 012 | Parquet and JSONL file sizing rules |
| 013 | Structured events, terminal view first |
| 014 | Least-privilege security baseline |
| 015 | Test against real services in Docker and a Pinecone test project |

### ADR-001: Python 3.12 CLI

**Decision.** Python 3.12. Built on `typer` for the CLI, `pydantic` v2 for config and schema models, `pyarrow`, `psycopg` 3, `opensearch-py`, `boto3`, `rich`, and the Pinecone Python SDK on API version 2026-07. Packaged with `uv`.

**Why.** The Pinecone SDK, pyarrow, and Glue PySpark share one language, so transform logic is reused between the runner and Glue.

**Consequences.** The `pcmigrate` package must be importable by the Glue job, so it can't depend on anything Glue can't install.

### ADR-002: Run inside the customer's AWS account

**Decision.** The runner (EC2 via SSM, no SSH), the S3 bucket, OSIS, and Glue all live in the customer's account and VPC. Pinecone reads staged files through a storage integration.

**Why.** Source data and credentials never leave the customer's boundary. That's the first question every enterprise security review asks.

**Consequences.** The target index must be on AWS. Pinecone doesn't import from S3 into GCP- or Azure-hosted indexes.

### ADR-003: S3 zones, no local staging

**Decision.** Every migration run writes under `s3://<bucket>/<migration_id>/`:

- `raw/` holds source-native exports.
- `canonical/` holds canonical Parquet.
- `import/` holds emitted import files.
- `rejects/` holds failed records with reasons.
- `manifest/` holds state, schema snapshot, approvals, and counts.

Export streams parts directly to S3.

**Why.** Bulk import reads from object storage anyway. S3 staging gives resume after host loss, parallel workers, an audit trail, and re-import without re-reading the source.

**Consequences.** Buckets use SSE-KMS, block public access, and a lifecycle rule. Staged data is a copy of customer data, so teardown deletes it unless the customer asks to retain it.

### ADR-004: Canonical record format and target emitters

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

### ADR-005: Introspection-first approval gate

**Decision.** `pcmigrate introspect` is read-only. It writes three artifacts:

- `report.md` and `report.html`, the findings.
- `mapping.yaml`, the proposed mapping.
- `plan.lock`, a SHA-256 of the normalized mapping plus the source schema fingerprint.

`pcmigrate approve` records approver, time, and hash. Every later stage refuses to run unless an approval matches the current `plan.lock`.

**Why.** The mapping is where silent data loss happens. A human checks it once, and the tool enforces that the approved plan is the one that runs.

**Consequences.** Any mapping edit or source schema change invalidates approval, and a new review is needed.

### ADR-006: Checkpointed stage state machine

**Decision.** The stages run in order:

`introspect → approve → provision → prepare_index → export → transform → emit → import → validate → sync → cutover → teardown`

Each stage is idempotent and records per-partition progress in `manifest/state.json`. A single-writer lock uses S3 conditional writes (`If-None-Match` / `If-Match`).

**Why.** Any host can resume a run. No DynamoDB is needed in v1.

**Consequences.** Each stage needs a defined "already done" check, and tests must kill and resume each stage.

### ADR-007: Buffered change stream

**Decision.** Change capture is optional (see "When change capture is off" below). When it's on, all post-snapshot changes flow into one durable buffer:

- Source CDC feeds it.
- Customer dual-writes feed it through our SDK shim or HTTP endpoint.

The buffer is SQS FIFO by default, grouped by record id, with Kinesis as the high-volume option. An applier drains it only after import validation passes. It collapses changes to the last operation per id, then applies upserts and deletes.

**Why.** Bulk import only writes to namespaces that don't exist yet. A direct dual-write before import completes would create the namespace and fail the import.

**Consequences.** Changes stay buffered until import finishes, at least 10 minutes and often hours, so buffer retention is sized to the import ETA. A final id-diff reconcile removes records deleted mid-export.

**When change capture is off.** `tier: none` provisions no buffer, applier, or write path, and skips `sync`, so the run goes straight from `validate` to `cutover`. It fits three cases:

- static or batch-loaded sources
- a planned write freeze from snapshot to cutover
- customers who can replay their own ingestion into Pinecone after cutover

Introspection warns if it measured writes and the tier is `none`, and the pre-cutover id diff still catches anything written after the snapshot.

### ADR-008: pgvector change capture tiers

**Decision.** The tier is chosen in `mapping.yaml`, based on what the customer's DBA permits:

1. **Watermark/ledger.** No DDL; deletes are caught by reconcile.
2. **Trigger outbox.** Ported from the field repo, with the PK column made configurable.
3. **Logical replication.** Uses `pgoutput`. On RDS, this needs `rds.logical_replication` and a restart.

`none` is also valid (ADR-007). Introspection proposes `none` when the workload window measures zero writes, and `trigger_outbox` otherwise.

Export workers share one `REPEATABLE READ` snapshot via `pg_export_snapshot()`. CDC starts from the LSN or watermark recorded at that instant.

**Why.** One consistent boundary means no change is lost or double-counted at the handoff.

**Consequences.** The snapshot transaction stays open for the whole export, so long exports hold back vacuum. Introspection estimates export duration and warns.

### ADR-009: OpenSearch via OSIS and Glue

**Decision.** An OSIS pipeline with the OpenSearch source writes NDJSON to `raw/`. A Glue PySpark job maps it to canonical Parquet. The emit stage is shared.

OpenSearch changes come through dual-write into the buffer (ADR-007), optionally with `updated_at`-filtered rescans.

**Why.** This follows the agreed pattern, is serverless, and scales without runner sizing. OSIS's OpenSearch source scans the index; it is not a change stream.

**Consequences.** Introspection must check that vectors are present in `_source`. If vectors are excluded from `_source` or served from derived source, export can't retrieve them, and the run stops at the gate.

### ADR-010: Pulumi Automation API

**Decision.** `pcmigrate provision` drives Pulumi from Python, with one stack per migration. It creates:

- VPC lookups and security groups
- the runner
- the S3 bucket and KMS key
- Secrets Manager entries
- IAM roles, including the Pinecone storage-integration role
- the OSIS pipeline and Glue job
- the SQS buffer (omitted when change capture is off) and CloudWatch log groups

The Pinecone index is managed by the tool, never by IaC.

**Why.** `teardown` must never be able to delete the customer's production index.

**Consequences.** Index lifecycle lives in `prepare_index`, which has create and adopt modes.

### ADR-011: Pre-validate, then import with abort

**Decision.** Transform enforces every target rule and writes violations to `rejects/`:

- dimension
- scalar-only metadata
- reserved names (`_` and `$` prefixes in document indexes)
- size limits
- duplicate vectors

Import then runs with `error_mode=abort`.

**Why.** With `continue`, Pinecone skips bad records without saying which. Bulk import also drops records whose vector values duplicate another record's. Catching both ourselves makes every gap in the counts explainable.

**Consequences.** Duplicate-vector groups are reported at introspection. After the FDE decides, remaining duplicates load through the data API after import, not through import.

### ADR-012: File sizing

**Decision.** Import files target 512 MB to 1 GB each, never more than 10 GB. Each import stays under 100,000 files and 10,000 namespaces.

On-demand indexes have a 1 TB total input limit per import, and `.jsonl.gz` is counted at 10× its compressed size. Larger datasets split into multiple imports over disjoint namespaces, or use dedicated read nodes.

**Why.** These are Pinecone's published import limits. Mid-size files keep import parallel and retries cheap.

**Consequences.** The planner computes file counts and import batches at introspection time, so they appear in the report.

### ADR-013: Structured events, terminal first

**Decision.** Stages emit typed JSON events (`stage_started`, `partition_done`, `import_progress`, `validation_result`, and so on). Events go to stdout logs, to `manifest/events/`, and to a `rich` live view.

**Why.** The terminal UI ships in v1. CloudWatch dashboards or a web UI can subscribe later without touching stage code.

**Consequences.** No stage prints directly; everything goes through the event bus.

### ADR-014: Security baseline

**Decision.** Security rests on six rules:

- Source access uses read-only roles. CDC tiers 2 and 3 get separately scoped grants.
- Secrets live in Secrets Manager, never in config files.
- The runner uses SSM only, with no SSH.
- IAM roles are least-privilege, one per component.
- Data is encrypted with KMS at rest and TLS in transit.
- The report flags likely PII fields for the FDE.

**Why.** These clear standard enterprise reviews without exceptions.

**Consequences.** CDC tiers 2 and 3 need explicit DBA sign-off, which the report captures.

### ADR-015: Testing against real services

**Decision.**

- Unit tests run on every PR.
- Integration tests run against Docker Compose pgvector and OpenSearch, a real S3 test bucket, and a Pinecone test project.
- Golden datasets include edge cases: nulls, nested objects, duplicate vectors, wide metadata, deletes during export.
- Every stage gets a kill-and-resume test.

**Why.** Bulk import must read from real object storage, so mocks can't cover the critical path.

**Consequences.** CI needs Pinecone and AWS test credentials as GitHub secrets, and test indexes are deleted after each run.

## Introspection report and approval gate

Introspection is read-only, needs only a read-only source role and a Pinecone key, and finishes in minutes on a sample, or longer with `--full-scan`. It produces `report.html`, `mapping.yaml`, and `plan.lock`. The FDE approves the exact hash that later stages will run.

### What the report contains

| Section | Contents | pgvector source | OpenSearch source |
| --- | --- | --- | --- |
| 1. Inventory | Per table or index: count, bytes, PK, vector fields (type, dimension, metric), other fields with types, null rates, sample values | `pg_attribute`, `pg_indexes`, `pg_class`, `pg_stats` | `_mapping`, `_cat/indices`, `_stats`, sampled `_search` |
| 2. Capability readiness | Dense, hybrid, and FTS fields found per object, and what each wave needs | `vector` / `halfvec`, `sparsevec`, `tsvector` columns, GIN indexes | `knn_vector`, `rank_features` / sparse fields, `text` fields with analyzers |
| 3. Workload | Read QPS and write rate (inserts, updates, deletes per minute) over a measurement window | `pg_stat_statements`, `pg_stat_user_tables` deltas | `_nodes/stats` search and indexing deltas |
| 4. Access control | RLS policies classified into namespace, filter, denormalize, or does-not-map | `pg_policies` | Document- and field-level security, if the security plugin is present |
| 5. Proposed mapping | Index type and spec, namespace strategy, id template, field-by-field mapping, dropped fields with reasons | Generated `mapping.yaml` | Generated `mapping.yaml` |
| 6. Findings | Every risk, graded blocker, warning, or info, with the fix | Checks below | Checks below |
| 7. Plan and sizing | Canonical bytes, import file count, import batches, ETA per stage, recommended change-capture tier, including none | Computed | Computed |
| 8. Validation plan | Checks, sample sizes, thresholds, and the ground-truth method | See below | See below |
| 9. Approval checklist | What the FDE must confirm before `approve` | Fixed list | Fixed list |

### Findings checks

| Check | Severity | Why it matters |
| --- | --- | --- |
| Vectors absent from `_source` (OpenSearch) | Blocker | Export can't retrieve them |
| Mixed or null vector dimensions in one field | Blocker | Import aborts on the first mismatch |
| Target region not on AWS | Blocker | S3 import only reaches AWS-hosted indexes |
| Duplicate vector values across ids | Warning | Bulk import drops duplicates; they load via the data API instead |
| Metadata over the per-record size limit | Warning | Field must be truncated, moved, or dropped |
| Nested objects, numeric arrays, `_` / `$` field names | Warning | Must be flattened, stringified, or renamed |
| Dense-only customer with hybrid or FTS planned | Warning | Choose a document index now to avoid re-import later |
| Estimated export longer than 2 hours on one snapshot | Warning | Holds back vacuum (pgvector); split or schedule off-peak |
| Input over 1 TB for one on-demand import | Warning | Multiple imports, or dedicated read nodes |
| RLS policy that doesn't map | Warning | The app must enforce it at query time |
| Analyzer features with no Pinecone equivalent (FTS) | Warning | Relevance will differ; listed per field |
| Likely PII field names or patterns | Info | FDE confirms the field should migrate |
| Change capture set to none while writes are measured | Warning | Writes after the snapshot are missed; agree a write freeze or pick a tier |

### Proposed mapping file

The FDE edits this file directly. Any edit changes `plan.lock` and invalidates earlier approval.

```yaml
migration_id: acme-docs-2026-10
source:
  kind: pgvector
  dsn_secret: arn:aws:secretsmanager:us-east-1:111122223333:secret:acme-pg-ro
  snapshot: exported            # pg_export_snapshot shared by all workers
target:
  index: acme-docs
  mode: create                  # create | adopt
  type: vector                  # vector | document  (ADR-004)
  cloud: aws
  region: us-east-1
  capacity: on_demand           # on_demand | dedicated
  api_version: "2026-07"
objects:
  - source: public.documents
    namespace: documents
    id_template: "documents#{id}"
    dense: {column: embedding, dimension: 768, metric: cosine}
    metadata:
      - {source: title, target: title, type: string}
      - {source: category, target: category, type: string}
      - {source: published_at, target: published_at, type: epoch_seconds}
      - {source: body, action: drop, reason: "exceeds metadata size limit"}
duplicates: upsert_after_import  # upsert_after_import | drop
change_capture:
  tier: trigger_outbox          # none | watermark | trigger_outbox | logical
  buffer: sqs_fifo
validation:
  recall_at_k: {k: 10, queries: 200, threshold: 0.95}
  fidelity_sample: 1000
```

### Recommended validation

The report fixes these checks and thresholds up front, and the validate stage runs exactly what was approved.

| Check | Method | Pass rule |
| --- | --- | --- |
| Count parity | Source count at snapshot vs. canonical rows vs. `records_imported` vs. namespace stats, re-read until stable | Equal after subtracting reported rejects and adding duplicates loaded via upsert |
| Vector fidelity | Fetch N random ids and compare with the source vector | Cosine ≥ 0.9999 per record (float32 cast tolerance) |
| Metadata fidelity | Same sample, field-by-field after the mapped transform | 100% equal |
| Recall@k | M queries drawn from stored vectors; ground truth is exact kNN in the source (index scan disabled for pgvector, `script_score` for OpenSearch) | Mean recall@10 ≥ approved threshold, default 0.95 |
| Filter parity | 20 queries with the most common metadata filters | Same result set as the source at k |
| Id diff | Full id set from source vs. `index.list` per namespace | Zero missing, zero extra |
| Hybrid (wave 2) | Sparse-only and fused queries vs. source hybrid results | Overlap@10 ≥ approved threshold |
| FTS (wave 3) | Keyword and phrase queries vs. source BM25 results | Overlap@10 reported per query class; threshold set by the FDE |

### FDE approval checklist

- [ ] No blockers open
- [ ] Index type matches the customer's 12-month roadmap (dense, hybrid, FTS)
- [ ] Every dropped or transformed field is acknowledged
- [ ] Namespace layout and RLS mapping agreed with the customer's security owner
- [ ] Change-capture tier approved by the customer's DBA, or a write freeze agreed if the tier is none
- [ ] Validation thresholds agreed with the customer
- [ ] Cost and duration estimates shared with the customer

## Repository layout and conventions

One GitHub repo, `pinecone-field/pcmigrate`, holds the package, IaC, the Glue job, tests, and docs. `CLAUDE.md` at the root carries the rules below, so every Claude Code session starts from the same constraints.

```
pcmigrate/
  CLAUDE.md                     # rules for Claude Code sessions
  pyproject.toml  uv.lock
  docs/
    adr/0001-python-cli.md ...  # one file per ADR
    design.md                   # this document, exported
    runbooks/                   # FDE operating guides per source
  src/pcmigrate/
    cli.py                      # typer app: introspect, approve, run, status, resume, teardown
    config/                     # pydantic models: MigrationConfig, Mapping, PlanLock
    core/
      state.py                  # stage state machine + S3 manifest + lock
      events.py                 # event bus, JSON log sink, rich live view
      canonical.py              # canonical Parquet schema + writer
      stages/                   # one module per stage
    sources/
      base.py                   # SourceConnector protocol
      pgvector/                 # introspect, partition, export, change_feed
      opensearch/               # introspect, osis config, glue mapping, change_feed
    targets/
      index_manager.py          # create / adopt, spec checks
      emitters/                 # vector_parquet.py, document_jsonl.py
      importer.py               # start, poll, describe import
      applier.py                # buffer drain, collapse, upsert/delete
    validate/                   # counts, fidelity, recall, filters, id diff, report
    report/                     # introspection report: jinja templates -> md + html
    writepath/                  # dual-write SDK shim + HTTP endpoint -> buffer
  infra/                        # Pulumi programs, driven by core/stages/provision.py
    base/  pgvector/  opensearch/
  glue/opensearch_transform.py  # imports pcmigrate.core.canonical
  tests/
    unit/  integration/  fixtures/golden/
  docker-compose.yml            # pgvector + OpenSearch for local and CI
  .github/workflows/            # ci.yml (unit, lint, types), integration.yml (gated)
```

### Conventions

- **Toolchain.** `uv` for environments, `ruff` for lint and format, `mypy --strict` on `src/`, `pytest` with `integration` and `slow` markers.
- **Workflow.** One PR per phase task, with conventional commits. CI must be green, and every PR links the ADR it implements or changes.
- **No stage prints.** Every user-visible output is an event (ADR-013).
- **Idempotency.** Every stage has a `is_complete(manifest)` check and a kill-and-resume test.
- **Secrets.** Secrets never appear in config, logs, events, or the report. Config holds secret ARNs only.
- **Pinecone access.** All Pinecone calls go through `targets/`, pinned to API version 2026-07. No other module imports the Pinecone SDK.
- **Source access.** All source access goes through `sources/<kind>/`. Introspection opens connections in read-only mode.
- **Test cleanup.** Integration tests create uniquely named indexes and buckets and always delete them, even on failure.

## Implementation phases

Eight phases take the tool from an empty repo to FTS. Phases 0 to 4 deliver a production pgvector dense migration. Phase 5 adds OpenSearch dense, and can start once Phase 3 is merged. Phases 6 and 7 add hybrid and FTS for both sources. Each phase ends with a merged PR set and a demo against the golden datasets.

1. **Phase 0: Repo bootstrap.** Create the skeleton:
   - the layout above, `CLAUDE.md`, and the ADRs as `docs/adr/` files
   - CI for lint, types, and unit tests
   - Docker Compose with pgvector and OpenSearch seeded from golden fixtures

   Exit: `pcmigrate --help` works, CI is green, and `docker compose up` seeds both sources.
2. **Phase 1: Core framework.** Build the shared machinery:
   - config and mapping models, plus the stage state machine with its S3 manifest and lock
   - the event bus and terminal view, and the canonical Parquet writer
   - the `SourceConnector` protocol and a synthetic test source
   - index manager (create and adopt), vector Parquet emitter, and importer
   - the count-validation skeleton, and the base Pulumi stack (bucket, KMS, secrets, IAM, storage-integration role)

   Exit: synthetic data goes through canonical, emit, import, and count validation into a test index, and a killed run resumes.
3. **Phase 2: pgvector introspection and the approval gate.** Build introspection and the report:
   - discovery for `vector`, `halfvec`, `sparsevec`, and `tsvector`, including partitioned tables
   - metric inference, RLS classification, and workload sampling
   - the findings engine and the mapping generator
   - Markdown and HTML report rendering
   - `plan.lock` and `approve`, with every later stage refusing to run without a matching approval

   Exit: the report on the golden database matches a snapshot test of expected findings, and an FDE can review and approve it.
4. **Phase 3: pgvector dense full load.** Build the load path:
   - PK-range partitioning and the shared exported snapshot
   - parallel workers streaming Parquet parts to S3
   - transform with pre-validation and rejects
   - duplicate-vector handling, emit and import, and every dense validation check
   - the Pulumi runner stack

   Exit: a 1M-row synthetic table migrates with validation passing, and a kill at any stage resumes cleanly.
5. **Phase 4: Change capture and cutover.** Build the post-snapshot path:
   - all three pgvector change-capture tiers, plus the none mode that skips sync
   - the SQS FIFO buffer and the applier
   - the dual-write SDK shim and HTTP endpoint
   - reconcile, a shadow-read comparison report, and the `cutover` and `teardown` commands

   Exit: inserts, updates, and deletes made during export converge, and the id diff is zero.
6. **Phase 5: OpenSearch dense.** Build the second source:
   - OpenSearch introspection, including the `_source` vector check and space-type mapping
   - OSIS pipeline and Glue job Pulumi modules
   - the Glue transform reusing `pcmigrate.core.canonical`
   - dual-write as the change path, and exact-kNN ground truth via `script_score`

   Exit: the golden OpenSearch index migrates with the same validation passing.
7. **Phase 6: Hybrid.** Add sparse vectors:
   - the document-index JSONL emitter
   - sparse extraction from `sparsevec` and OpenSearch sparse fields
   - hybrid readiness in the report, and hybrid validation

   Exit: a dense plus sparse dataset from each source migrates into one document index, and hybrid overlap meets the threshold.
8. **Phase 7: FTS.** Add full-text fields:
   - text-field mapping into `full_text_search` fields
   - an analyzer translation table (language, stemming, stop words, n-grams), with unmappable features reported
   - FTS relevance comparison
   - a query-translation guide for the customer's app team

   Exit: keyword and phrase queries on the golden corpus report overlap per query class, and the FDE signs off the analyzer gaps.

## Open questions to verify

Eight items need an answer during the build. The first three block a phase.

- [ ] **Hybrid query combination (blocks Phase 6).** How does the 2026-07 Documents API combine multiple `score_by` clauses: fused, or one ranker plus filters? This decides how hybrid validation and query translation work.
- [ ] **Duplicate-vector handling (blocks Phase 3).** Is bulk import's duplicate-vector skip scoped per namespace or per import? Does the data API's upsert accept the skipped duplicates? Confirm with Pinecone engineering.
- [ ] **Size limits (blocks Phase 2 findings).** What are the current per-record metadata and per-document size limits for vector and document indexes on API 2026-07?
- [ ] **Storage integration.** Can the S3 storage integration be created through an API, or must it stay one manual console step in the runbook?
- [ ] **OSIS behaviour.** Confirm the OpenSearch source's behaviour on self-managed clusters and older OpenSearch or Elasticsearch versions, and with derived source enabled.
- [ ] **Logical replication.** Confirm tier-3 feasibility on Aurora PostgreSQL and on RDS without a maintenance window.
- [ ] **Repo home.** Is it `pinecone-field/pcmigrate`, private until the first customer run, or another org?
- [ ] **Attribution.** Logic is ported from the MIT-licensed [pgvector-migration](https://github.com/pinecone-field/pgvector-migration) repo; keep its notice in `NOTICE`.

### Sources

- [Pinecone: Import records](https://docs.pinecone.io/guides/index-data/import-data)
- [Pinecone: Introducing the Documents API](https://www.pinecone.io/learn/pinecone-documents-api/)
- [Pinecone: Full-text search guide](https://docs.pinecone.io/guides/search/full-text-search)
- [Pinecone 2026 changelog](https://docs.pinecone.io/release-notes/2026)
- [pinecone-field/pgvector-migration](https://github.com/pinecone-field/pgvector-migration)
- [AWS: OpenSearch Ingestion data migration](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/creating-opensearch-service-pipeline.html)
