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

`docker compose up` starts pgvector (Postgres 16) on port 5432 and OpenSearch 2.x on port 9200 with security disabled, and seeds both from `tests/fixtures/golden/`. Integration tests expect these to be running.
