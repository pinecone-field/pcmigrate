# ADR-016: Hybrid queries on document indexes are one ranker per request, fused client-side

**Status:** Accepted (verified against the 2026-07 Documents API reference on 2026-10-08)

## Context

The hybrid wave migrates a dense vector and a sparse vector per record into a document
index (ADR-004). Validation must compare hybrid results between the source and Pinecone, and
the query-translation guide must tell the customer's application how to run hybrid queries.
Both depend on how the Documents API combines scoring clauses.

The design listed this as the open question that blocks Phase 6: does the 2026-07 Documents
API fuse several `score_by` clauses, or is it one ranker plus filters?

## Decision

The 2026-07 `search` request takes `score_by` as a list of one to one hundred clauses, but
the reference states: "A single clause of any type is always valid. Several clauses may be
combined only when every one of them is `text` or `query_string`; a `dense_vector` or
`sparse_vector` clause must appear on its own." The response carries one `_score` per
match and no fusion parameters. The SDK enforces the same rule client-side.

Therefore:

- A hybrid search is **one ranker per request**. A dense or a sparse clause ranks; metadata
  filters restrict; `text` clauses may combine with each other (BM25 over several fields)
  but never with a vector clause.
- Dense plus sparse fusion is **client-side**. pcmigrate's validation runs the dense search
  and the sparse search as separate requests on both the source and Pinecone and fuses each
  side with reciprocal rank fusion (k = 60) before comparing overlap@k. The same fusion is
  what the query-translation guide recommends to the application, with the weighting left
  to the application.
- Sparse scoring is a dot product over `{indices, values}` with no configurable metric, so
  the source-side exact ground truth is the inner product of the stored sparse vectors
  (pgvector `sparsevec <#>`; OpenSearch `rank_feature` clauses with linear scoring and the
  query weight as boost).
- Sparse indices come from a token-to-index vocabulary that the exporter records under
  `manifest/vocabulary/`, and the introspection report tells the FDE that query-time
  encoding must use the same vocabulary.

## Consequences

- The validate stage's hybrid check (`hybrid_overlap`) measures overlap@k between two
  client-side fusions, not against a server-side hybrid score. Its threshold is approved in
  `mapping.yaml` like recall@k.
- If a later API version adds server-side fusion, the comparison method changes here and
  in the query-translation guide; the canonical data and the import files do not.
- A source whose "hybrid" is BM25 plus kNN with no stored sparse vectors is not a hybrid
  migration by itself: its text ranking belongs to the FTS wave (`text` clauses), which
  the report marks per object. ADR-017 adds the alternative of generating the sparse
  vectors from the text during the migration.
