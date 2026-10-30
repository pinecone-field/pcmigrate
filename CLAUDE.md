# pcmigrate

CLI that migrates pgvector and OpenSearch data into Pinecone. The approved design is `docs/design.md`; the decisions it rests on are `docs/adr/0001-*.md` through `docs/adr/0015-*.md`. Read the design and the ADRs a task touches before planning.

## Before every commit

Run all three and fix what they report:

```sh
uv run ruff check
uv run mypy src infra
uv run pytest -m "not integration"
```

`uv run ruff format` keeps formatting consistent. CI runs the same three commands on every PR and must be green before merge.

## Conventions

- **Toolchain.** `uv` for environments, `ruff` for lint and format, `mypy --strict` on `src/`, `pytest` with `integration` and `slow` markers.
- **Workflow.** One PR per phase task, with conventional commits. CI must be green, and every PR links the ADR it implements or changes.
- **No stage prints.** Every user-visible output is an event (ADR-013). Ruff's `T20` rule rejects `print()` under `src/`.
- **Idempotency.** Every stage has an `is_complete(manifest)` check and a kill-and-resume test.
- **Secrets.** Secrets never appear in config, logs, events, or the report. Config holds secret ARNs only.
- **Pinecone access.** All Pinecone calls go through `targets/`, pinned to API version 2026-07. No other module imports the Pinecone SDK.
- **Source access.** All source access goes through `sources/<kind>/`. Introspection opens connections in read-only mode.
- **Test cleanup.** Integration tests create uniquely named indexes and buckets and always delete them, even on failure.

## Layout

```
src/pcmigrate/
  cli.py          typer app: introspect, approve, run, status, resume, teardown
  config/         pydantic models: MigrationConfig, Mapping, PlanLock
  core/           state machine, events, canonical writer, stages/
  sources/        base.py protocol, pgvector/, opensearch/
  targets/        index_manager, emitters/, importer, applier
  validate/       counts, fidelity, recall, filters, id diff, report
  report/         findings engine and jinja rendering
  writepath/      dual-write SDK shim and HTTP endpoint
infra/            Pulumi programs: base/, pgvector/, opensearch/
glue/             Glue PySpark transform, imports pcmigrate.core.canonical
tests/            unit/, integration/, fixtures/golden/
```

## Local services

`docker compose up` starts pgvector (Postgres 16, with `pg_stat_statements`) on port 5432 and OpenSearch 2.x on port 9200 with security disabled, and seeds both from `tests/fixtures/golden/`. Integration tests expect these to be running. OpenSearch seeds through `docker compose run --rm opensearch-seed`; `tests/fixtures/golden/opensearch/documents.py` yields the same documents in-process for unit tests.

The golden database has a read-only role `pcmigrate_ro` for introspection tests, a partitioned `events` table with a `halfvec` column, and a forced RLS policy on `tenants` that hides rows from the read-only role on purpose: introspection must report that as a blocker.

Snapshot tests under `tests/fixtures/golden/pgvector/expected/` are regenerated with `UPDATE_SNAPSHOTS=1 uv run pytest -m integration tests/integration/test_golden_introspection.py`. Only do that after confirming the change in output is intended.

## Phase 2 commands

```sh
PGVECTOR_DSN=postgresql://pcmigrate:pcmigrate@localhost:5432/golden \
  uv run pcmigrate introspect --migration-id demo --index demo --window 0 --out ./pcmigrate-out
uv run pcmigrate approve --approver you@pinecone.io --out ./pcmigrate-out
```

`introspect` is read-only by construction: the session sets `default_transaction_read_only=on` and a statement audit rejects anything but SELECT, WITH, SET, SHOW, and EXPLAIN.

## Phase 3 notes

- Export runs partitions in a process pool when the source has a `dsn` (pgvector) and in
  threads otherwise (synthetic). Under moto, use `ExportStage(use_processes=False)`: worker
  processes create their own boto3 clients, which the mock cannot see.
- `tests/integration/test_pgvector_source_docker.py` runs the real pgvector source through
  every stage with moto S3 and the fake gateway. It needs only the Docker database and is the
  fastest end-to-end check for load-path changes.
- `tests/integration/test_pgvector_pipeline.py` runs against real S3 and Pinecone and kills the
  runner after export, emit, and import. `tests/integration/test_pgvector_performance.py` seeds
  `PCMIGRATE_PERF_ROWS` rows (default 1,000,000) at 768 dims and prints throughput; set the
  variable lower for a quick harness check.
- Transform keeps the first record per `vector_hash` in a namespace and routes the rest to
  `manifest/duplicates/`, which the import stage upserts through the data API afterwards.
  Count parity expects `exported - rejected - (duplicates - upserted)`.

## Phase 4 notes

- `change_capture.tier: none` is a first-class path (ADR-007): provision creates no SQS queue,
  export installs no feed, sync is skipped, and cutover runs only the final reconcile. The
  provision stage refuses a stack whose buffer presence disagrees with the tier.
- Tiers: `watermark` polls `updated_at`; `trigger_outbox` installs `pinecone_changelog` plus
  one trigger per table before export and reads the outbox position inside the snapshot;
  `logical` creates a pgoutput slot with EXPORT_SNAPSHOT on a replication connection
  (psycopg `ClientCursor`, simple query protocol) and the coordinator attaches to that
  snapshot, then polls `pg_logical_slot_get_binary_changes` and decodes pgoutput in
  `sources/pgvector/pgoutput.py`. The Docker database runs with `wal_level=logical`.
- CDC envelopes carry only the key; the applier re-reads the row through
  `GroundTruthSource.fetch_records` before upserting. Dual-write envelopes carry the payload.
- The applier never applies before `validate` is complete, collapses to the last op per id per
  batch, and acknowledges only after the gateway returns. `applier_lag` events carry depth
  and oldest-message age.
- Tests inject buffers and feeds through `ctx.extras["buffer"]` and `ctx.extras["feed"]`;
  `tests/integration/test_pgvector_cdc_docker.py` runs every tier under a concurrent writer
  thread on a scratch copy of `documents`, in CI.
- `cutover --shadow N` and `cutover --confirm`, and `teardown`, read `pcmigrate.yaml`
  (`config/settings.py`). Teardown destroys the stack and staged zones, keeps `manifest/`,
  and has a test asserting the Pinecone index is never touched.

## Phase 5 notes (OpenSearch dense)

- Amazon OpenSearch Service is the primary source: `introspect --source opensearch` reads the
  endpoint from `OPENSEARCH_URL` and signs with SigV4; `--os-auth basic` plus
  `OPENSEARCH_BASIC_AUTH=user:password` covers self-managed clusters. All HTTP goes through
  `sources/opensearch/client.py`, whose read-only audit allows GET, HEAD, search-family POSTs,
  and point-in-time DELETE only.
- The report stack is source-neutral: `sources/inventory.py` holds `Inventory` and
  `ObjectInfo`; `sources/pgvector/models.py` keeps `PgInventory` and `TableInfo` as aliases.
  OpenSearch-only facts (`space_type`, `engine`, `in_source`, `source_setting`, aliases, index
  settings) are optional fields, and the findings `vector_source`, `vector_space_type`, and
  `vector_data_type` only fire for `knn_vector`.
- `sources/opensearch/glue_mapping.py` is the one translation (documents to canonical records)
  shared by the Glue job, the direct export, ground truth, and the applier re-read. It must
  stay importable on Glue 5.0 (Python 3.11, no pydantic-free path): no report, Pinecone,
  psycopg, or opensearch-py imports, no 3.12-only syntax; a unit test checks both.
- Export runs through OSIS and Glue (`core/stages/export_opensearch.py`). The pipeline is
  provisioned armed with `scheduling.start_time` in 2099; the stage records T0 and the source
  counts, moves the start time to T0, waits until `raw/<index>/` stops growing, stops the
  pipeline, then runs one Glue job per index and reads `manifest/glue/<index>/result.json`.
  `opensearch.export_mode: direct` scans from the runner instead (laptop mode and the Docker
  test). The Data Prepper OpenSearch source takes no query, so the optional `updated_at`
  rescan is a ranged search from the runner (`RescanFeed`), not a second pipeline.
- The wheel is built with `uv build` (or the path in `opensearch.wheel_path`), uploaded with
  the Glue script under `manifest/glue/`, and installed with `--no-deps` plus pinned
  pydantic, xxhash, and pyyaml; `requires-python` allows 3.11 for that reason.
- Rejects: transform writes `rejects/<namespace>/transform/`, Glue `rejects/<namespace>/glue/`,
  the direct export `rejects/<namespace>/direct/`; transform clears only its own prefix.
- `tests/integration/test_opensearch_source_docker.py` runs introspection, the direct export,
  validation with `script_score` ground truth, and the rescan against the Docker index.
  `tests/integration/test_opensearch_pipeline.py` provisions the OSIS pipeline and Glue job
  in the test account (`PCMIGRATE_OPENSEARCH_AWS=1`, an existing domain, Pulumi local backend).
  Snapshots live in `tests/fixtures/golden/opensearch/expected/` and refresh with
  `UPDATE_SNAPSHOTS=1`.

## Phase 6 notes (hybrid)

- ADR-016: the 2026-07 Documents API ranks by one `dense_vector` or `sparse_vector` clause
  per request (only `text` clauses combine), so hybrid is fused client-side. The validate
  stage's `sparse_overlap` and `hybrid_overlap` checks compare sparse-only and RRF-fused
  (k=60) top-k between the source and Pinecone; thresholds live in `validation.hybrid`.
- A sparse vector in the mapping (`objects[].sparse`, kinds `sparsevec` and `rank_features`)
  forces `target.type: document`; prepare_index creates the schema `{dense_field:
  dense_vector, sparse_field: sparse_vector}` and verifies it in adopt mode (a sparse field
  cannot be added after creation). The gateway dispatches to the Documents API per host.
- `core/sparse.py` is shared by sources, Glue, and validation: pgvector `sparsevec` text is
  one-based and becomes zero-based indices; `rank_features` (and neural sparse) tokens hash
  to uint32 indices with xxh32, the vocabulary is written to `manifest/vocabulary/<object>.json`
  with any collisions, and the report warns that query-time encoding must use the same map.
- Document imports are `.jsonl.gz` under `import/<batch>/<namespace>/`, planned at ten
  times their compressed size against the 1 TB on-demand limit (ADR-012). Transform rejects
  numeric arrays, `_` / `$` field names, and bad sparse vectors before emit.
- Golden fixtures carry sparse data: pgvector `documents.sparse sparsevec(30000)` and the
  OpenSearch `sparse` rank_features field; recreate Docker volumes after changing them.
- Generated sparse (ADR-017): `sparse: {kind: generated, text_fields: [...], model: {...}}`
  (`introspect --sparse-from f1 --sparse-from f2`). The record builders carry the fields as
  `text_fields`; `core/transform.py` encodes them per input part through a `SparseEncoder`
  (`targets/sparse_encoder.py`, Pinecone Inference first), caches the vectors under
  `manifest/sparse_cache/<ns>/`, rejects records with no text, and emits `sparse_encoded`
  events. Tests attach `ctx.extras["sparse_encoder"] = FakeSparseEncoder()`; the real
  encoder needs `ctx.extras["pinecone_api_key"]`, which `cli._context` sets. The applier
  and reconcile re-read and encode every change for a generated object. Validation uses
  `validate/ground_truth.py` (exact top-k streamed from `canonical/_clean/`) in place of
  the source's `knn_sparse`, and also requires the stored sparse vectors to equal the
  generated ones. OpenAI has no sparse model; it is not a provider.

## Phase 7 notes (full-text search)

- ADR-018: `--roadmap fts` (or `--fts-from col`) maps text into `full_text_search` string
  fields on a document index. The export carries the source text in `text_fields` keyed by
  source column; the emitter, applier, reconcile, and duplicates upsert rename them to
  `objects[].text[].target`. A string field is searchable or filterable, never both, so a
  text source leaves metadata (OpenSearch keyword multi-fields become `<field>_<sub>`).
- Translation tables live in `sources/pgvector/fts_map.py` (tsvector expressions, GIN
  expression indexes, `pg_ts_config_map` dictionaries, `default_text_search_config`) and
  `sources/opensearch/fts_map.py` (built-in, language, and custom analyzers from the
  index's `analysis` settings, multi-fields, field parameters). Both produce
  `sources/fts.py` `FtsTranslation` objects; gaps become `fts_unsupported` warnings.
  Snapshot tests: `tests/unit/fts/test_fts_map_snapshots.py` with fixtures under
  `tests/fixtures/golden/*/analyzers.json` / `tsvector_expressions.json`, refreshed with
  `UPDATE_SNAPSHOTS=1`.
- Pinecone facts (SDK 10, API 2026-07): languages ar da de el en es fi fr hu it nl no pt
  ro ru sv ta tr; `stop_words` needs `stemming`; `ngram` excludes both; at most 100 FTS
  fields; fields are fixed at creation (adopt mode verifies names and configuration).
- Validation (`validate/fts.py`): queries drawn from the sampled records' text, rarest
  terms first; classes single_term, multi_term (token OR), phrase (`$match_phrase`),
  filtered; thresholds in `validation.full_text.thresholds`. Ground truth is
  `GroundTruthSource.knn_text`: `ts_rank_cd(..., 1)` with `to_tsquery` /
  `plainto_tsquery` / `phraseto_tsquery` on pgvector, `match` / `match_phrase` on
  OpenSearch, in-process BM25 (`validate/bm25.py`) for the synthetic source. The test
  fake ranks with the same BM25 and a snowball stemmer (dev dependency).
- The golden corpus is generated by `tests/fixtures/golden/corpus.py` (stdlib only):
  pgvector `003_fts_corpus.sql` (regenerate with `uv run python -m
  tests.fixtures.golden.corpus`) and the OpenSearch seeder. `documents.summary` has a GIN
  expression index with the `simple` configuration; the OpenSearch `summary` field uses a
  stop-only analyzer. Recreate the Docker volumes after pulling.

## Dedicated read nodes

- `target.capacity: dedicated` needs `target.read_nodes: {node_type: b1|t1, shards, replicas}`
  in mapping.yaml (`introspect --capacity dedicated --node-type t1 --shards 2 --replicas 2`
  generates it). prepare_index creates the index with that `read_capacity`, waits until the
  control plane reports the nodes Ready, and in adopt mode refuses an index whose read
  capacity differs from the plan; pcmigrate never reconfigures an existing index.
- Several namespaces on one dedicated-read-node index need Pinecone to enable that for the
  project (the test project is not enabled: the second namespace is rejected with 403). The
  findings engine warns on a multi-namespace plan until `target.read_nodes.multi_namespace:
  true` (`introspect --multi-namespace`) records the confirmation, warns when the data needs
  more 250 GB shards than configured, and lists the node count and hourly billing.
- `tests/integration/test_synthetic_pipeline.py` runs the dedicated variant when
  `PCMIGRATE_TEST_DEDICATED=1`; nodes bill from creation, so the test deletes the index.

## AWS tags and resource names

- `migration.tags` and `migration.name_prefix` in `pcmigrate.yaml` (ADR-002) are validated in
  `config/aws.py`: tags follow AWS's rules and stay out of the reserved `pcmigrate:` and `aws:`
  namespaces; the prefix plus the migration id must keep every IAM role name within 64
  characters. `ResourceNames` is the only place physical names are spelled; the three stack
  configs expose it as `names` and the programs use it for every `name=`. Pulumi logical names
  (`cfg.resource_prefix`) never carry the prefix, so a prefix never replaces a stack.
- Every taggable resource receives `resource_tags(migration_id, cfg.tags)` explicitly, and the
  provision drivers also set `aws:defaultTags` on the stack. The infra unit tests assert both
  the tag set on every taggable resource type and the prefixed names, so a new resource must
  be added to `TAGGABLE` in its program's test.

## Running it by hand

`docs/runbooks/manual-live-test.md` walks every command end to end against the golden
database, a real bucket, and a live index. `docs/runbooks/ec2-runner.md` is the customer
shape: an EC2 runner with the IAM policy from `docs/iam/`, reached over SSM, cloning the repo. `examples/pcmigrate.example.yaml` is the settings
file `run`, `status`, `resume`, `cutover`, and `teardown` read. `run --skip-provision` is the
laptop mode: it records introspect and approve from the local artifacts directory and marks
provision as skipped, then runs prepare_index through sync. `--until validate` stops before
sync. `teardown --no-stack` cleans staged zones when no Pulumi stack was provisioned.
