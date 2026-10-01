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
