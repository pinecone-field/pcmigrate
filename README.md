# pcmigrate

`pcmigrate` is a CLI that moves customer data from pgvector and OpenSearch into Pinecone. It runs inside the customer's AWS account, stages data in S3, loads it with Pinecone bulk import, and keeps Pinecone current through a buffered change path until cutover.

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

The top row is the control plane. Introspection is read-only, and the FDE approves one hash. The lower lanes run in the customer's account: the full load (top lane) lands through bulk import, and changes (bottom lane) wait in the buffer until validation passes, because import can't write into an existing namespace.

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
- `sync` runs continuously until `cutover --confirm`.

## Further reading

- [Architecture decision records](docs/architecture-decision-records.md)
- [Implementation phases](docs/implementation-phases.md)
