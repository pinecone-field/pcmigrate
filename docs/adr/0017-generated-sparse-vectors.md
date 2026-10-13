# ADR-017: Sparse vectors generated from text at migration time

**Status:** Accepted (2026-10-13)

## Context

The hybrid wave (ADR-004, ADR-016) carries a sparse vector the source already stores:
pgvector `sparsevec` or an OpenSearch `rank_features` field. Most customers have neither.
They run BM25 plus kNN, or dense only, and want the Pinecone side to be hybrid from day
one. For them the sparse vector has to be produced during the migration from the record's
text.

Three facts bound how that can work:

- The 2026-07 Documents API embeds nothing server-side for a `sparse_vector` field: the
  schema field takes no model, bulk import takes vectors only, and `documents.upsert`
  takes `{indices, values}`. Server-side embedding exists only for `semantic_text` fields
  created through `create_for_model`, which is a different index shape.
- OpenAI's embeddings API produces dense vectors only. There is no OpenAI sparse model.
- Pinecone Inference hosts `pinecone-sparse-english-v0` (DeepImpact): `input_type`
  `passage` for writes and `query` for reads, 512 tokens per input by default (2048 with
  `max_tokens_per_sequence`), 96 inputs per request, $0.08 per million tokens, and
  250K passage tokens per minute on Starter or 3M on Standard.

`docs/design.md` lists "re-embedding data or changing embedding models" as a v1 non-goal.
That non-goal was about the dense vector, which this decision leaves untouched.

## Decision

- **Generation is client-side on the runner, in the transform stage.** The mapping
  declares `objects[].sparse` with `kind: generated`, the ordered `text_fields` to encode,
  a `separator` (newline by default), and a `model`. The record builders carry the named
  columns or `_source` fields as `text_fields` on the canonical record; transform
  concatenates them in order, encodes the result with `input_type: passage`, stores the
  sparse vector, clears the text, and only then validates the record. Export, emit, import,
  and the index schema are the stored-sparse hybrid path unchanged.
- **Pinecone Inference is the first provider**, behind a `SparseEncoder` protocol
  (`targets/sparse_encoder.py`) so another sparse provider can be added without touching
  the pipeline. The `pinecone` provider reuses the migration's Pinecone key; another
  provider names its own Secrets Manager ARN in `sparse.model.api_key_secret` (ADR-014).
  Dense re-embedding stays out of scope.
- **A record with no text is rejected** with the reason `no text for sparse generation`,
  because a document in a hybrid index that no sparse query can reach is a silent recall
  loss. The count is reported per namespace.
- **A rerun pays once.** Transform writes the generated vectors per canonical input part
  under `manifest/sparse_cache/<namespace>/`, keyed by record id and a hash of the text
  under the model, and reads them back before calling the model again.
- **The change path re-reads.** CDC and dual-write envelopes carry no text, so for a
  generated object the applier and the final reconcile fetch the current row from the
  source and encode it before upserting, the same way CDC envelopes already re-read.
- **Ground truth comes from the migrated data.** The source has no sparse index, so the
  validate stage streams the clean canonical zone (dense plus generated sparse) and
  computes exact top-k for dense, sparse, and the RRF fusion of ADR-016, then compares
  Pinecone's rankings with the approved `sparse_overlap` and `hybrid_overlap` thresholds.
  It also fetches the sample from Pinecone and requires every stored sparse vector to equal
  the generated one.
- **Queries must be encoded with the same model.** The report, the validation outputs,
  and the query-translation guidance all say so: `input_type: query` with the model named
  in the mapping.

## Consequences

- `introspect --roadmap hybrid --sparse-from title --sparse-from body` generates the
  mapping; the report shows the object as "hybrid (generated)", estimates tokens, cost,
  and wall time at both plan throughputs, warns when the concatenated text exceeds the
  model's input, and blocks on a field that is not text or is excluded from `_source`.
- ADR-016's closing consequence is amended: a source whose hybrid is BM25 plus kNN with no
  stored sparse vectors can still be a hybrid migration when the FDE chooses to generate
  the sparse side; otherwise it remains the FTS wave.
- The design's non-goal is narrowed to dense re-embedding. Sparse generation is additive:
  the dense vector, metadata, and ids are migrated exactly as before.
- Transform becomes the stage that talks to an external service and is rate-limited by
  the plan's token throughput. The per-part cache and the SDK's retry on 429 keep a long
  run resumable; the report's time estimate tells the FDE what to expect before approval.
- Generated vectors are only as good as the text chosen. The FDE picks the fields queries
  match on; the report warns when a long body will be truncated at the end.
