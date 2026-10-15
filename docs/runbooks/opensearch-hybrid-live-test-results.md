# OpenSearch hybrid live test: 2M documents, generated sparse vectors (2026-10-15)

The dense rehearsal of [opensearch-live-test-results.md](opensearch-live-test-results.md)
repeated for the hybrid wave: the same two OpenSearch indices (1M documents each, 1,024-dim
cosine vectors plus `title`, `review`, and `text`), migrated into one Pinecone document index
with a sparse vector generated from the three text fields (ADR-016, ADR-017). Run with main at
`c5fc03d` plus the two fixes listed at the end. Settings and the command sequence are in
`pcmigrate-runs/passages-2m-hybrid/` (outside the repo).

| Step | Result |
|---|---|
| Domain | `pcm-passages-2m`, 2 x r6g.2xlarge.search, created in 10 min |
| Load into OpenSearch | 2,000,000 documents in 945 s (2,118 docs/s), counts exact |
| Introspect | `--roadmap hybrid --sparse-from title --sparse-from review --sparse-from text`; 0 blockers, 5 warnings (cost per index, query-time encoding, duplicate estimate) |
| Target | document index `passages-2m`, schema `dense: dense_vector(1024, cosine)` + `sparse: sparse_vector`, on-demand, namespaces `index1` and `index2` |
| Encoding | `pinecone-sparse-english-v0`, Standard tier: 2,000,000 texts, 182,449,256 tokens, 21,102 requests, about $14.60, 2.0M tokens/min with 4 workers, no 429s |

## Stage times

| Stage | Time | Notes |
|---|---|---|
| prepare_index | 6 s | document index created |
| export | 31 min | OSIS scan 25 min, one Glue job per index at 3 min each |
| transform | 92 min | encoding is the whole of it: 360 records/s; the dense run took 31 min |
| emit | 90 min | gzipped JSONL, single thread, 352 rows/s, 7.6 KB per record (the dense parquet emit took 22 min) |
| import | 15 min | bulk import of 1,924,876 records in 11.5 min, then 75,007 duplicate upserts |
| validate | 69 min | exact dense truth from OpenSearch, exact sparse and hybrid truth from the clean zone |

Total runner time about 5 hours against 2 hours for the dense migration; the encoding and
the JSONL emit account for the difference.

## Validation

| Check | index1 | index2 |
|---|---|---|
| count parity | 999,935 | 999,948 |
| vector and metadata fidelity (1,000 ids) | exact | exact |
| recall@10, 200 queries | 0.995 | 0.984 |
| filter parity, 20 filters | 1.0 | 1.0 |
| sparse overlap@10, 100 queries | 0.988 | 0.970 |
| hybrid (RRF) overlap@10 | 0.976 | 0.968 |
| stored vs generated sparse vectors | 0 mismatches | 0 mismatches |
| id diff | 0 missing, 0 extra, 65 rejected | 0 missing, 0 extra, 52 rejected |

`seed_opensearch.py recall --target pinecone --queries 500` (Documents API, dense clause):
recall@10 0.989 / 0.983, recall@100 0.980 / 0.970, p50 52 ms, p95 130 ms.

## Rejects

117 records (65 and 52) were rejected in transform with `sparse vector is empty`: the model
returned no terms for their text (stop words, emoji, or non-English only). They are listed
under `rejects/<namespace>/transform/` and excluded from count parity and the id diff.
Importing such records without a sparse field would be the alternative.

## Fixes made during the run

- Documents API upserts (duplicates, applier) are cut by serialized size as well as count:
  200 documents of 1,024 dims are 4.8 MB of JSON and the data plane answered
  `413 PAYLOAD_TOO_LARGE`. The import stage resumed after the fix without re-importing.
- The id diff reports ids found under `rejects/` as `rejected` instead of `missing`, matching
  count parity.
- `tools/opensearch_dataset` recall detects a document index and searches it through the
  Documents API.

## Dedicated read nodes

The run was meant for b1 dedicated read nodes. A probe through `create_document_index` with
`read_capacity` created the index and reached Ready in 67 s, but the project still rejects a
second namespace on a dedicated index (`403 restricted to a single namespace`), so the test
ran on-demand. Repeat with `--capacity dedicated --node-type b1 --shards 1 --replicas 1
--multi-namespace` once the project is enabled.
