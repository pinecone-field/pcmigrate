# pcmigrate

`pcmigrate` is a CLI that moves customer data from pgvector and OpenSearch into Pinecone. It runs inside the customer's AWS account, stages data in S3, loads it with Pinecone bulk import, and, for sources that stay live during the migration, keeps Pinecone current through an optional buffered change path until cutover.

Every run stops after introspection and publishes a findings report. Nothing is created or moved until a Pinecone field engineer (FDE) approves the proposed plan.

## Scope

The tool ships in three capability waves:

| Wave | Source signals | Pinecone target | Import file |
| --- | --- | --- | --- |
| 1. Dense | pgvector `vector`, `halfvec`; OpenSearch `knn_vector` | Vector index (dense) | Parquet: `id`, `values`, `metadata` |
| 2. Hybrid | Wave 1 plus pgvector `sparsevec`; OpenSearch `rank_features` / neural sparse fields | Document index with `dense_vector` + `sparse_vector` fields | JSONL / JSONL.gz |
| 3. FTS | PostgreSQL `tsvector` columns and GIN text indexes; OpenSearch `text` fields and analyzers | Document index with `full_text_search` string fields, API 2026-07 | JSONL / JSONL.gz |

Sources are built in order: pgvector first, then OpenSearch.

**Goals**

- One CLI that takes a migration from introspection to cutover for a supported source.
- An introspection report an FDE can approve, edit, or reject before any write.
- Restartable stages: any failure resumes from the last checkpoint, not from zero.
- Provable correctness: counts, sampled fidelity, recall, and an id-level diff before cutover.
- New sources added by implementing one connector interface.

**Non-goals for v1**

- Re-embedding data or changing embedding models.
- Rewriting the customer's query layer. Query mappings are documented, application code is not rewritten.
- OpenSearch aggregations, dashboards, ingest pipelines, or security roles.
- Pinecone indexes on GCP or Azure. S3 import only works into AWS-hosted indexes.

## High-level design and architecture

A single orchestrator runs a gated, checkpointed stage pipeline. Source connectors sit behind one interface. Everything from the canonical S3 zone onward is shared across sources.

![pcmigrate architecture: control plane, full-load path, change path](docs/images/pcmigrate_high_level_design.png)

The top row is the control plane. Introspection is read-only, and the FDE approves one hash that every later stage checks. The lower lanes run in the customer's account: the full load lands through bulk import from the S3 staging zones, and changes wait in the buffer until validation passes, because import can't write into an existing namespace. With `change_capture.tier: none` the change path is never provisioned and the run goes from validate straight to cutover, where the final reconcile still runs. The manifest in S3 holds the stage state, the single-writer lock, approvals, and the event log, so any host can resume a run.

## Components

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

## Stage lifecycle

Stages run in order: `introspect → approve → provision → prepare_index → export → transform → emit → import → validate → sync → cutover → teardown`.

- `introspect` and `approve` need no infrastructure, so an FDE can run them from a laptop with network access to the source.
- `provision` onward runs on the runner inside the VPC.
- `sync` is skipped when change capture is off; otherwise it runs continuously until `cutover --confirm`.

## Getting started

```sh
uv sync                      # Python 3.12 environment with all dependencies
uv run pcmigrate --help      # introspect (--source pgvector|opensearch), approve, run, status, resume, cutover, teardown
docker compose up -d --wait  # pgvector (pg16) on :5432 and OpenSearch 2.x on :9200, seeded
uv run pytest                # unit tests, plus integration tests when compose is up
```

Before every commit run `uv run ruff check`, `uv run mypy src infra`, and `uv run pytest -m "not integration"`. See `CLAUDE.md` for the full conventions.

## Hybrid (dense plus sparse)

With `--roadmap hybrid`, introspection maps a pgvector `sparsevec` column or an OpenSearch
`rank_features` (neural sparse) field as the record's sparse vector, proposes a document
index with one `dense_vector` and one `sparse_vector` field, and emits `.jsonl.gz` import
files. The 2026-07 Documents API ranks by one vector clause per request, so hybrid queries
fuse client-side ([ADR-016](docs/adr/0016-hybrid-query-combination.md)); validation checks
sparse-only and RRF-fused overlap@k against the source. Token vocabularies for hashed sparse
indices are written under `manifest/vocabulary/`.

When the source stores no sparse vectors, `--sparse-from title --sparse-from body` generates
them from the chosen text fields with Pinecone Inference (`pinecone-sparse-english-v0`)
during transform ([ADR-017](docs/adr/0017-generated-sparse-vectors.md)). The mapping gets
`sparse: {kind: generated, text_fields: [...], model: {...}}`, the report estimates tokens,
cost, and time, generated vectors are cached per part under `manifest/sparse_cache/` so a
rerun pays once, and validation ranks against the migrated data itself. Queries must be
encoded with the same model and `input_type: query`.

## Target capacity

The dense wave lands on a serverless index. `target.capacity: on_demand` (default) uses
shared read capacity; `target.capacity: dedicated` with `target.read_nodes` creates the
index on dedicated read nodes (`b1` or `t1`, shards of 250 GB, replicas for throughput),
waits for them to become ready, and refuses to adopt an index whose read capacity differs
from the plan. Multi-namespace dedicated indexes need enabling by Pinecone per project;
the report warns until `read_nodes.multi_namespace: true` confirms it.

## Further reading

- [Design, ADRs, and build plan](docs/design.md), the approved design this repo implements
- [Architecture decision records](docs/adr/README.md), one file per ADR
- [Implementation phases](docs/implementation-phases.md)
- [Testing a migration in laptop mode](docs/runbooks/laptop-mode-test.md), a 30-minute end-to-end walkthrough
- [Manual live test](docs/runbooks/manual-live-test.md), the same plus full-stack mode and change capture under live writes
- [OpenSearch live test](docs/runbooks/opensearch-live-test.md), Amazon OpenSearch Service through OSIS and Glue into Pinecone, plus the laptop-mode variant
- [OpenSearch live test results](docs/runbooks/opensearch-live-test-results.md), the 2M-document run of 2026-10-06: timings, validation, and recall against the source
