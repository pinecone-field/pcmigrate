# ADR-018: Full-text search fields carry the source text, with analyzers translated and gaps reported

**Status:** Accepted (2026-10-13)

## Context

The FTS wave (ADR-004, wave 3) migrates PostgreSQL `tsvector` columns and OpenSearch
`text` fields into a document index. The 2026-07 Documents API offers one kind of
full-text field: a `string` field with a `full_text_search` object. Four facts about it
decide the design:

- A field's analysis has four knobs: `language` (eighteen two-letter codes, English by
  default), `stemming` (off by default), `stop_words` (off by default, and only valid with
  stemming on), and `ngram` (`min_gram`, `max_gram`, `prefix_only`; excludes stemming and
  stop words). There are no synonyms, char filters, custom stop lists, accent folding,
  shingles, per-field weights, or separate search analyzers. Schema fields are fixed at
  index creation.
- A string field is searchable or filterable, never both. Declaring `filterable: true`
  next to `full_text_search` keeps the filter and silently drops the search.
- Ranking is BM25. A `text` clause ranks by token OR over one or more fields with equal
  weight; `query_string` takes Lucene syntax; `$match_phrase`, `$match_all`, and
  `$match_any` are filters that narrow candidates before ranking. `text` and
  `query_string` clauses combine with each other but never with a vector clause
  (ADR-016).
- The source stores analysed tokens (a tsvector, a Lucene inverted index), not something
  Pinecone can ingest. Only the original text can be re-analysed on the Pinecone side.

PostgreSQL ranks with `ts_rank` / `ts_rank_cd` (term frequency and cover density, no
inverse document frequency), OpenSearch with BM25. Agreement between the source and
Pinecone is therefore a property of the query class, not a single number.

## Decision

- **Export carries the source text, never the tsvector.** For pgvector the mapping
  names the column the tsvector is generated from (parsed from the generated column's
  expression or from a GIN `to_tsvector(...)` expression index); several columns
  concatenated in one tsvector become several full-text fields, and a JSON path becomes a
  dotted source. A tsvector maintained by a trigger has no derivable source and is
  reported; the FDE names the column with `--fts-from`. OpenSearch fields carry their
  `_source` value. The canonical record's `text_fields` map, keyed by source name, is
  what the emitter, the applier, the reconcile, and the duplicates upsert write into the
  document under the schema field name.
- **One translation table per source** (`sources/pgvector/fts_map.py`,
  `sources/opensearch/fts_map.py`) turns a text search configuration or an analyzer
  chain into `{language, stemming, stop_words, ngram}`. Snowball configurations and the
  built-in language analyzers become language plus stemming plus stop words; `simple` and
  `standard` become English without either; `ngram` and `edge_ngram` tokenizers become
  character n-grams; a stop filter without a stemmer keeps its stop words (Pinecone
  requires stemming for `stop_words`, and stop words carry little BM25 weight) and says
  so. Every component with no equivalent is a `fts_unsupported` warning per field, and
  the FDE signs the gaps off at approval. A keyword analyzer or `index: false` field is
  not searchable and stays metadata.
- **A full-text source column leaves metadata.** Because a string field cannot be both,
  the generator removes the column from the metadata list; an OpenSearch keyword
  multi-field (`title.raw`) becomes `title_raw` filterable metadata so filters survive.
- **The schema declares the fields at creation** next to the dense and sparse fields of
  the earlier waves, and adopt mode refuses an index whose full-text fields are missing
  or configured differently.
- **Validation is overlap@k per query class**, with the FDE's thresholds in
  `validation.full_text.thresholds`: `single_term`, `multi_term` (token OR, the `text`
  clause semantics), `phrase` (`$match_phrase` on both sides), and `filtered` (a term
  under a common metadata filter). Queries are drawn from the migrated records' own
  text, rarest terms first, so every query has matches. The source side ranks with its
  own engine: `ts_rank_cd(..., 1)` over the tsvector (length-normalized) with
  `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery`, or `match` / `match_phrase` on
  OpenSearch. The defaults (0.8, 0.5, 0.8, 0.6) sit below the recall threshold on purpose; on the golden corpus pgvector multi-term agreement is about 0.62.
- **The query-translation guide** (`docs/runbooks/query-translation.md`) tells the
  application team how OpenSearch query DSL and PostgreSQL tsquery patterns map onto
  `text`, `query_string`, and the `$match_*` filters, and which patterns do not map.

## Consequences

- `introspect --roadmap fts` (or any `--fts-from`) proposes a document index with the
  text fields, the report lists each field's analysis and the gaps, and the approval
  checklist gains the analyzer sign-off.
- Relevance differs from the source by construction where features are lost; the report
  says where, and the per-class overlap says how much on the customer's own data.
- Dual-write payloads gain an optional `text_fields` map keyed by schema field name; CDC
  envelopes still carry only the key and the applier re-reads the row, so the text comes
  from the source.
- The dense and hybrid waves are unchanged: without text fields nothing in the schema,
  the emitter, or validation differs.
