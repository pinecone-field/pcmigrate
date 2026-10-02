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

`docker compose up` starts pgvector (Postgres 16, with `pg_stat_statements`) on port 5432 and OpenSearch 2.x on port 9200 with security disabled, and seeds both from `tests/fixtures/golden/`. Integration tests expect these to be running.

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
