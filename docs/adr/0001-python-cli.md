# ADR-001: Python 3.12 CLI, packaged with uv

**Status:** Proposed

## Context

The tool has to talk to Pinecone, read Parquet, drive AWS, and share transform logic with an AWS Glue PySpark job. The Pinecone SDK, pyarrow, and Glue PySpark are all Python, so one language covers the runner and the Glue job without a second implementation of the canonical transform.

## Decision

Python 3.12. Built on `typer` for the CLI, `pydantic` v2 for config and schema models, `pyarrow`, `psycopg` 3, `opensearch-py`, `boto3`, `rich`, and the Pinecone Python SDK on API version 2026-07. Packaged with `uv`.

## Consequences

- Transform logic is written once and reused between the runner and Glue.
- The `pcmigrate` package must be importable by the Glue job, so it can't depend on anything Glue can't install.
