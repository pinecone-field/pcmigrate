# Pinecone Migration Tool: ADRs, Design, and Build Plan

Oct 1, 2026 · @Vamshi Krishna Enabothala

## Context and scope

`pcmigrate` moves customer data from pgvector and OpenSearch into Pinecone in three capability waves: dense first, then hybrid, then full-text search (FTS). Every run stops after introspection and publishes a findings report. Nothing is created or moved until an FDE approves it.

The tool runs in the customer's AWS account, stages data in S3, and loads it with Pinecone bulk import. A buffered change path keeps Pinecone current until cutover.

### Capability waves

| Wave | Source signals | Pinecone target | Import file |
| --- | --- | --- | --- |
| 1. Dense | pgvector `vector`, `halfvec`; OpenSearch `knn_vector` | Vector index (dense) | Parquet: `id`, `values`, `metadata` |
| 2. Hybrid | Wave 1 plus pgvector `sparsevec`; OpenSearch `rank_features` / neural sparse fields | Document index with `dense_vector` + `sparse_vector` fields (ADR-004) | JSONL / JSONL.gz |
| 3. FTS | PostgreSQL `tsvector` columns and GIN text indexes; OpenSearch `text` fields and analyzers | Document index with `full_text_search` string fields, API 2026-07 | JSONL / JSONL.gz |

Pinecone's [import guide](https://docs.pinecone.io/guides/index-data/import-data) confirms both formats: Parquet for vector indexes, JSONL for indexes with a document schema. FTS and the Documents API are [generally available](https://www.pinecone.io/learn/pinecone-documents-api/) as of September 2026.

### Goals

- One CLI that takes a migration from introspection to cutover for a supported source.
- An introspection report an FDE can approve, edit, or reject before any write.
- Restartable stages: any failure resumes from the last checkpoint, not from zero.
- Provable correctness: counts, sampled fidelity, recall, and an id-level diff before cutover.
- New sources added by implementing one connector interface.

### Non-goals for v1

- Re-embedding data or changing embedding models.
- Rewriting the customer's query layer. We document query mappings; we don't rewrite application code.
- OpenSearch aggregations, dashboards, ingest pipelines, or security roles.
- Pinecone indexes on GCP or Azure. S3 import only works into AWS-hosted indexes.

### Source order

pgvector first, then OpenSearch. The [pgvector-migration repo](https://github.com/pinecone-field/pgvector-migration) gives us tested discovery and sync logic, and pgvector exercises the hardest framework piece (the snapshot-to-CDC handoff) without OSIS and Glue.

## High-level design and architecture

A single orchestrator runs a gated, checkpointed stage pipeline. Source connectors sit behind one interface. Everything from the canonical S3 zone onward is shared across sources.

&#91;embedded content: pcmigrate architecture · control plane, full-load path, change path\]

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
- `sync` runs continuously until `cutover --confirm`.

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
| 007 | Buffered change stream for both CDC and dual-write |
| 008 | pgvector change capture in three tiers |
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

**Decision.** All post-snapshot changes flow into one durable buffer:

- Source CDC feeds it.
- Customer dual-writes feed it through our SDK shim or HTTP endpoint.

The buffer is SQS FIFO by default, grouped by record id, with Kinesis as the high-volume option. An applier drains it only after import validation passes. It collapses changes to the last operation per id, then applies upserts and deletes.

**Why.** Bulk import only writes to namespaces that don't exist yet. A direct dual-write before import completes would create the namespace and fail the import.

**Consequences.** Changes stay buffered until import finishes, at least 10 minutes and often hours, so buffer retention is sized to the import ETA. A final id-diff reconcile removes records deleted mid-export.

### ADR-008: pgvector change capture tiers

**Decision.** The tier is chosen in `mapping.yaml`, based on what the customer's DBA permits:

1. **Watermark/ledger.** No DDL; deletes are caught by reconcile.
2. **Trigger outbox.** Ported from the field repo, with the PK column made configurable.
3. **Logical replication.** Uses `pgoutput`. On RDS, this needs `rds.logical_replication` and a restart.

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
- the SQS buffer and CloudWatch log groups

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
| 7. Plan and sizing | Canonical bytes, import file count, import batches, ETA per stage, recommended change-capture tier | Computed | Computed |
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
  tier: trigger_outbox          # watermark | trigger_outbox | logical
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
- [ ] Change-capture tier approved by the customer's DBA
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
   - all three pgvector change-capture tiers
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

## Claude Code prompts

Run one prompt per phase, in a fresh Claude Code session at the repo root, starting in plan mode. Review the plan, then let it implement.

Before Phase 0, export this doc as Markdown and save it as `docs/design.md`. Every prompt points Claude Code at that file and the ADRs, rather than restating them.

### Phase 0: Repo bootstrap

```text
You are bootstrapping a new repo for pcmigrate, a CLI that migrates pgvector and OpenSearch data into Pinecone. docs/design.md is the approved design; read it fully before planning.

Tasks:
1. Create the GitHub repo pinecone-field/pcmigrate (private) with `gh`, and push an initial commit.
2. Create the layout from "Repository layout and conventions" in docs/design.md, with empty modules that have docstrings only.
3. Write CLAUDE.md with the conventions section, plus: run `uv run ruff check`, `uv run mypy src`, and `uv run pytest -m "not integration"` before every commit.
4. Split the ADR section into docs/adr/0001-*.md through 0015-*.md, each with Status: Proposed, Context, Decision, Consequences.
5. Set up pyproject.toml with uv: Python 3.12, typer, pydantic v2, pyarrow, psycopg[binary], pgvector, opensearch-py, boto3, rich, jinja2, pinecone (latest SDK supporting API 2026-07), pulumi, pulumi-aws.
6. Add a typer app with stub commands: introspect, approve, run, status, resume, teardown. Each prints "not implemented" via a placeholder.
7. Add docker-compose.yml with pgvector (pg16) and OpenSearch 2.x (security disabled for local). Add tests/fixtures/golden/ seed scripts that create:
   - a documents table: 768-dim cosine, title, category, published_at, nullable fields, and 5 duplicate-vector rows
   - a products table: 384-dim L2, with a numeric price
   - a tenants table with an RLS policy
   - an OpenSearch index with a knn_vector field, a nested object, and a text field
8. Add .github/workflows/ci.yml for lint, mypy, and unit tests on PRs. Add integration.yml to run on manual dispatch, using secrets PINECONE_API_KEY, AWS_ROLE_ARN, and TEST_BUCKET.

Acceptance: `uv run pcmigrate --help` lists the commands, CI passes, and `docker compose up` seeds both sources. Open a PR titled "Phase 0: bootstrap".
```

### Phase 1: Core framework

```text
Read CLAUDE.md, docs/design.md, and ADRs 003, 004, 006, 010, 011, 012, and 013. Implement the source-agnostic core.

1. config/: pydantic models MigrationConfig, Mapping (the shape of the mapping.yaml example in design.md), and PlanLock (SHA-256 over the normalized mapping plus the source schema fingerprint).
2. core/state.py: the stage enum in ADR-006 order. Manifest in s3://<bucket>/<migration_id>/manifest/state.json with per-stage and per-partition status. Single-writer lock using S3 conditional writes (If-None-Match on create, If-Match on update). Each stage exposes run() and is_complete().
3. core/events.py: typed events, JSON-lines sink to stdout and to manifest/events/, and a rich live view. Add a lint test that fails if any module under src/ calls print().
4. core/canonical.py: the canonical Parquet schema from ADR-004 and a streaming writer that rolls parts at a target size and uploads each part with multipart upload. vector_hash = xxhash64 of the float32 bytes.
5. sources/base.py: a SourceConnector Protocol with introspect(), plan_partitions(), export(partition, writer), and change_feed(). Add a SyntheticSource for tests.
6. targets/:
   - index_manager: create and adopt. Adopt verifies dimension, metric, and cloud, and checks that target namespaces are absent.
   - emitters/vector_parquet.py: Pinecone import Parquet (id string, values list<float32>, metadata JSON string), sized per ADR-012.
   - importer: start_import with error_mode abort, poll describe_import, and emit progress events.
   Only targets/ may import the Pinecone SDK.
7. validate/counts.py: count parity with a settle-and-retry window on namespace stats.
8. infra/base: a Pulumi program for the S3 bucket (SSE-KMS, block public access, lifecycle), the KMS key, Secrets Manager entries, the runner IAM role, and the Pinecone storage-integration IAM role (trust policy parameterized by Pinecone principal and external ID). Wire it to core/stages/provision.py via the Automation API.

Tests: unit tests for every module. One integration test runs SyntheticSource (10k rows) through canonical, emit, import, and count validation against a uniquely named test index, kills the process after export, resumes, and passes. Always delete the index. Open one PR per numbered item, or group 1-3 and 4-7.
```

### Phase 2: pgvector introspection and approval gate

```text
Read CLAUDE.md, docs/design.md (especially "Introspection report and approval gate"), and ADRs 005, 008, 011, and 014. Implement sources/pgvector/introspect.py, report/, and the approve command.

Port the discovery and RLS logic from https://github.com/pinecone-field/pgvector-migration (README and sync.py), fixing these gaps:
- Detect vector, halfvec, sparsevec, and tsvector columns, not only `vector`.
- Include partitioned tables (relkind 'p'), and report partitions.
- Detect the PK from pg_index rather than assuming `id`.
- Infer the metric from the opclass (vector_cosine_ops, vector_ip_ops, vector_l2_ops, and the halfvec_* variants). When no index exists, mark it unknown, as a warning.

Collect every inventory, workload, and RLS item in the report table. Workload: sample pg_stat_statements and pg_stat_user_tables twice over a configurable window (default 5 minutes, --window).

Findings engine: one function per check in the findings table, each returning a Finding(severity, object, field, message, fix). Duplicate vectors: sampled estimate by default; exact count via a hash GROUP BY with --full-scan.

Mapping generator: emit mapping.yaml exactly in the documented shape. Metadata type rules:
- timestamps become epoch_seconds
- numeric/Decimal becomes float
- text[] becomes list of strings
- json/jsonb objects get flattened to dotted keys, up to depth 2, else stringified
- fields over the metadata size limit are dropped with a reason

Sizing: estimate canonical bytes, import file count, import batches, and export ETA from table sizes and a measured read throughput sample.

Render report.md and report.html with jinja2, in the section order from the design. Write plan.lock. `pcmigrate approve --approver <email>` validates there are no blockers, records the approval in manifest/approvals.json, and every stage after approve refuses to run unless the approval hash equals the current plan.lock.

Safety: open the connection with default_transaction_read_only=on and assert no DDL or DML is issued (add a test that runs introspect with a role lacking write grants).

Tests: a snapshot test of the golden DB report (findings and mapping). Unit tests per finding. A test that editing mapping.yaml invalidates approval.
```

### Phase 3: pgvector dense full load

```text
Read CLAUDE.md, docs/design.md, and ADRs 003, 004, 006, 008, 011, and 012. Implement export, transform, emit, import, and validate for pgvector dense, plus infra/pgvector.

1. Partitioning: split each table into PK ranges targeting ~2M rows per partition (configurable), using min/max for integers and ntile sampling for others.
2. Snapshot: a coordinator connection opens REPEATABLE READ, calls pg_export_snapshot(), and records the snapshot id plus pg_current_wal_lsn() and max(updated_at) where present in the manifest. Each worker sets the same snapshot with SET TRANSACTION SNAPSHOT.
3. Export: N worker processes (configurable) stream rows with a named server-side cursor, build canonical records through the mapping, and write rolling Parquet parts to canonical/ via core/canonical.py. Never hold a full partition in memory. Mark partitions complete in the manifest.
4. Transform and pre-validation: enforce dimension, metadata types, reserved names, and size limits. Write violations to rejects/ with a reason. Detect duplicate vector_hash within each namespace; keep the first occurrence for import, and write the others to manifest/duplicates/ for post-import upsert when mapping.duplicates == upsert_after_import.
5. Emit and import: run the Phase 1 emitter and importer, splitting into multiple imports when ADR-012 limits require. After import, upsert duplicates through the data API in size-aware batches.
6. Validate: implement counts, vector fidelity, metadata fidelity, recall@k (ground truth via exact kNN with SET enable_indexscan = off), filter parity, and id diff via index.list. Write validation.md and validation.json. The stage fails if any check misses its approved threshold.
7. infra/pgvector: an EC2 runner in the customer subnet, reached via SSM only. Security group egress to the DB port and HTTPS. Instance profile scoped to the migration bucket prefix and secrets.

Tests: an integration test migrating golden pgvector end to end. A performance test seeding 1M rows at 768 dims, recording throughput in the test output. Kill-and-resume tests at export (mid-partition), emit, and import.
```

### Phase 4: Change capture and cutover

```text
Read CLAUDE.md, docs/design.md, and ADRs 007, 008, and 014. Implement change capture, the buffer, the applier, dual-write, reconcile, cutover, and teardown.

1. Buffer: SQS FIFO in infra/base, with MessageGroupId = record id and dedup id = source change id. Retention is sized from the import ETA in the plan. Add a Kinesis adapter behind the same interface, without wiring it to infra yet.
2. pgvector change_feed() tiers, selected by mapping.change_capture.tier:
   - watermark: poll updated_at > last watermark from the snapshot boundary; deletes come from reconcile.
   - trigger_outbox: port the changelog table and trigger from pgvector-migration with a configurable PK column. Install it before export starts, and only start forwarding rows with seq after the snapshot boundary.
   - logical: a pgoutput replication slot created at the snapshot (CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT), so export and CDC share one boundary. Decode inserts, updates, and deletes.
3. Applier: stays idle until validate has passed, then drains the buffer, collapses to the last op per id within each batch, upserts (re-reading the current row for outbox and watermark tiers) and deletes, and deletes messages only after Pinecone acknowledges. Emit lag events.
4. writepath/: a Python SDK shim with the same upsert/delete signature as the Pinecone SDK that writes to the buffer, plus a small FastAPI endpoint doing the same. Document both in docs/runbooks/dual-write.md.
5. Reconcile: a full id diff with the source. Upsert what's missing, delete what's extra, and report both.
6. `pcmigrate cutover --shadow N` runs N sampled live-like queries against both systems and reports overlap and latency. `pcmigrate cutover --confirm` stops change capture after the buffer drains, removes triggers and slots, and runs a final reconcile.
7. teardown: pulumi destroy for the migration stack, deleting staged data unless --retain-data. It must never touch the Pinecone index; add a test asserting that.

Tests: an integration test that runs inserts, updates, and deletes against golden pgvector throughout export and import, for each tier, and asserts a zero id diff and field equality after the applier drains.
```

### Phase 5: OpenSearch dense

```text
Read CLAUDE.md, docs/design.md, and ADRs 004, 007, and 009. Implement sources/opensearch/, infra/opensearch, and glue/opensearch_transform.py.

1. Introspection: indices and aliases, _mapping, and knn_vector fields with dimension, space_type (l2 -> euclidean, cosinesimil -> cosine, innerproduct -> dotproduct), and method/engine. Sample documents and verify each vector field is present in _source; if not, raise a blocker naming the mapping setting responsible. Also cover nested and object fields, date fields, counts, and indexing/search rates from _nodes/stats. Reuse the findings engine and report.
2. infra/opensearch: an OSIS pipeline with the OpenSearch source for the approved indices, an S3 sink writing NDJSON to raw/, the pipeline role, and the domain access mapping. Support both AWS-managed domains and self-managed clusters reachable from the VPC.
3. Glue: a PySpark job that reads raw/, applies the approved mapping (flatten objects, dates to epoch, drop or truncate per mapping), and writes canonical Parquet through pcmigrate.core.canonical. Package pcmigrate as a wheel for Glue's --additional-python-modules. Rejects go to rejects/.
4. Change path: dual-write via writepath/ into the buffer from T0, with T0 recorded before the OSIS pipeline starts. Optionally, an updated_at rescan using a second OSIS run with a range query.
5. Validation: exact-kNN ground truth via script_score with knn_score or a painless cosine, consistent with the space type.

Tests: an integration test migrating the golden OpenSearch index end to end, with OSIS and Glue run in the test AWS account. Unit tests for the mapping translation of every OpenSearch field type in the fixture.
```

### Phase 6: Hybrid

```text
Read CLAUDE.md, docs/design.md, and ADR-004. Pinecone docs: https://docs.pinecone.io/guides/index-data/import-data (document schema JSONL) and the Documents API reference for API 2026-07. Add hybrid (dense + sparse) support.

1. targets/emitters/document_jsonl.py: canonical to JSONL.gz with _id, the declared dense_vector and sparse_vector fields, and other fields as metadata. Reject numeric arrays and field names starting with _ or $ at transform time. Account for .jsonl.gz counting 10x toward the 1 TB limit when batching imports.
2. index_manager: create document-schema indexes from the mapping (dense_vector with dimension and metric, sparse_vector), and verify the schema in adopt mode.
3. Sources: pgvector sparsevec -> sparse indices/values. OpenSearch rank_features and neural sparse fields -> token-to-index mapping. Store the vocabulary mapping in manifest/, and flag in the report that query-time encoding must use the same vocabulary.
4. Report: a hybrid readiness section per object. If the source does BM25 + kNN hybrid with no stored sparse vectors, mark it as Wave 3 (FTS), not hybrid.
5. Validation: sparse-only and combined queries vs. the source, with overlap@10 against the approved threshold.

Before coding the query side, confirm from the 2026-07 Documents API reference how multiple score_by clauses are combined. Record the answer in a new ADR-016.
```

### Phase 7: FTS

```text
Read CLAUDE.md, docs/design.md, and ADR-016. Pinecone docs: https://docs.pinecone.io/guides/search/full-text-search. Add full-text search support on document indexes.

1. Introspection:
   - pgvector: tsvector columns, their generating expressions and text search configuration, and GIN indexes on to_tsvector(...).
   - OpenSearch: text fields with analyzers, tokenizers, and filters, plus multi-fields.
   Map each field to a full_text_search string field (language, stemming, stop_words, ngram where configured), using an analyzer translation table in sources/<kind>/fts_map.py. Report every feature with no equivalent (synonyms, custom char filters, per-field boosts, etc.) as a warning.
2. Export: carry the source text (not the tsvector) into canonical text_fields. For pgvector, use the column the tsvector is generated from.
3. Emitter and index: declare FTS fields in the document schema alongside dense and sparse fields from earlier waves.
4. Validation: a query set per class (single term, multi-term, phrase via $match_phrase, filtered), comparing source top-10 with Pinecone top-10. Report overlap per class; the FDE sets thresholds in mapping.yaml.
5. docs/runbooks/query-translation.md: how common OpenSearch query DSL and PostgreSQL tsquery patterns map to Documents API search requests.

Tests: the golden corpus with a known query set, plus snapshot tests for analyzer translation.
```

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
