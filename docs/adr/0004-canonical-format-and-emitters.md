# ADR-004: Canonical record format and pluggable target emitters

**Status:** Proposed

## Context

Index type (vector index versus document index) is the costliest choice to undo. If exporters wrote the target format directly, changing the index type would mean a new export from the source.

## Decision

Exporters write one canonical Parquet schema:

- `id`, `namespace`
- `dense` (list\<float32>), `sparse` (struct of indices/values)
- `text_fields` (map\<string,string>), `metadata` (JSON string)
- `source_pk`, `vector_hash`

Emitters turn canonical files into a target format:

- `VectorIndexEmitter` writes Parquet.
- `DocumentIndexEmitter` writes JSONL.gz.

The dense wave defaults to a vector index. Hybrid and FTS use a document index. A dense customer with hybrid or FTS on their roadmap should start on a document index; the report flags this.

## Consequences

- Switching index type is a re-emit from S3, not a new source export.
- One extra pass over the data, in exchange for adding targets without touching exporters.
