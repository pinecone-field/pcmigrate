# Query translation: OpenSearch query DSL and PostgreSQL tsquery to the Documents API

This guide is for the application team after an FTS-wave migration (ADR-018). It maps the
query patterns most applications use against OpenSearch `text` fields or PostgreSQL
`tsvector` columns onto the 2026-07 Documents API `search` request, and says plainly which
patterns have no equivalent. The index name, host, namespace, and field names come from
`mapping.yaml`: `target.index`, each object's `namespace`, and `objects[].text[].target`.

## The shape of a search request

Every request has an optional `filter` that narrows candidates and a `score_by` list that
ranks what remains. A `text` clause ranks by BM25 over the named full-text fields with
token OR semantics and equal weight per field; a `query_string` clause takes Lucene
syntax; a `dense_vector` or `sparse_vector` clause ranks by similarity and must stand
alone (ADR-016). Text-match filters (`$match_phrase`, `$match_all`, `$match_any`) apply
to full-text fields; metadata filters (`$eq`, `$in`, `$gte`, ...) apply to filterable
fields. A full-text field is never filterable with `$eq`, and a filterable field is never
searchable with `text`.

```python
from pinecone import Pinecone

pc = Pinecone(api_key=...)
index = pc.Index(host="<host from manifest/prepare_index>")
hits = index.documents.search(
    namespace="documents",
    top_k=10,
    score_by=[{"type": "text", "fields": ["body"], "query": "harbor lighthouse"}],
    filter={"category": {"$eq": "news"}, "body": {"$match_phrase": "harbor master"}},
    include_fields=["title", "category"],
)
for match in hits.matches:
    print(match.id, match.score)
```

Queries are analyzed with the field's own `full_text_search` configuration (language,
stemming, stop words, or n-grams). There is no search-time analyzer to choose.

## OpenSearch query DSL

| OpenSearch | Documents API | Notes |
| --- | --- | --- |
| `match: {body: "harbor lighthouse"}` (default `operator: or`) | `score_by: [{type: text, fields: [body], query: "harbor lighthouse"}]` | Same token-OR semantics, BM25 on both sides. |
| `match: {body: {query: ..., operator: and}}` | `filter: {body: {$match_all: "harbor lighthouse"}}` plus the `text` clause | Every term must appear; the clause still ranks. |
| `match: {body: {query: ..., minimum_should_match: "75%"}}` | `text` clause | No minimum-should-match; treat as OR or AND. |
| `match_phrase: {body: "harbor master"}` | `filter: {body: {$match_phrase: "harbor master"}}` plus `text` with the same words | Exact adjacent order; no `slop`. |
| `match_phrase_prefix` | n-gram field with `prefix_only: true`, then `text` | Only when the field was mapped with an `ngram` configuration. |
| `multi_match: {query, fields: [title, body]}` (`best_fields` / `most_fields`) | `text` with `fields: [title, body]` | Fields are summed with equal weight; `^2` boosts are ignored. |
| `multi_match` with `type: phrase` | `$match_phrase` on each field in an `$or` filter, then `text` | |
| `query_string: {query: "title:harbor AND body:(lighthouse OR beacon)"}` | `score_by: [{type: query_string, query: "title:(harbor) AND body:(lighthouse OR beacon)"}]` | Lucene operators and field qualifiers; no fuzziness or proximity. |
| `simple_query_string` | `query_string` | Same syntax subset; `+`/`-`/`|` become `AND`/`NOT`/`OR`. |
| `term: {category: "news"}` on a keyword field | `filter: {category: {$eq: "news"}}` | Keyword fields and `.raw` multi-fields are metadata (`title_raw`). |
| `terms: {category: [...]}` | `filter: {category: {$in: [...]}}` | |
| `range: {views: {gte: 10}}` | `filter: {views: {$gte: 10}}` | Dates are epoch seconds after the mapping. |
| `bool.filter` + `bool.must` text | `filter` for the filter part, `text` for the must part | Filters apply before ranking, as in OpenSearch. |
| `bool.should` of several `match` clauses | one `text` clause over several fields, or several `text` clauses | Several `text` clauses combine; scores add. |
| `bool.must_not: {match: ...}` | `query_string` with `NOT term` | No negative text filter; `NOT` lives in the Lucene string. |
| `wildcard`, `regexp`, `fuzzy`, `prefix` on text | no equivalent | Use an n-gram field for prefix or substring matching; there is no fuzziness. |
| `function_score`, `boosting`, `rank_feature` on text scores | no equivalent | Re-rank client-side from `_score`. |
| `highlight` | no equivalent | Fetch the text (`include_fields`) and highlight in the application. |
| `knn` + `match` in one query (hybrid) | two requests, fused client-side (ADR-016) | Dense and text never share a request. |
| `search_analyzer`, `analyzer` per request | none | The field's configuration applies at query time. |
| `synonym` / `synonym_graph` filters | none | Expand synonyms in the application before sending the query string. |

## PostgreSQL tsquery

| PostgreSQL | Documents API | Notes |
| --- | --- | --- |
| `body_tsv @@ to_tsquery('english', 'harbor | lighthouse')` ordered by `ts_rank_cd` | `score_by: [{type: text, fields: [body], query: "harbor lighthouse"}]` | OR semantics; BM25 replaces cover density, so the order within a result set differs. |
| `plainto_tsquery('english', 'harbor lighthouse')` (AND) | `filter: {body: {$match_all: "harbor lighthouse"}}` plus the `text` clause | |
| `websearch_to_tsquery('english', '"harbor master" -beacon')` | `filter: {body: {$match_phrase: "harbor master"}}` and `query_string: 'body:(harbor master) NOT beacon'` | Quoted phrases become `$match_phrase`; `-term` becomes `NOT`; `or` becomes `OR`. |
| `phraseto_tsquery('english', 'harbor master')` | `filter: {body: {$match_phrase: "harbor master"}}` | Adjacent terms only; `<N>` distance operators have no equivalent. |
| `to_tsquery('english', 'harb:*')` (prefix) | n-gram field with `prefix_only: true` | Only when the field was mapped with an `ngram` configuration. |
| `to_tsquery('english', 'harbor & !beacon')` | `query_string: 'body:(harbor) NOT beacon'` | |
| `setweight(...)` weights and `ts_rank(weights, ...)` | none | Rank the separate fields with one `text` clause over all of them (equal weight) or re-rank client-side. |
| `ts_headline(...)` | none | Highlight in the application from the fetched text. |
| `WHERE category = 'news' AND body_tsv @@ q` | `filter: {category: {$eq: "news"}}` plus the `text` clause | |
| `ts_rank_cd(..., 1)` (length normalization) | BM25 `b = 0.75` | Both penalize long documents; the curves differ. |
| `unaccent(...)`, synonym and thesaurus dictionaries, custom configurations | none | Pre-process the query the same way the migration report lists for the field. |
| `tsvector` concatenating several columns | `text` with `fields: [col_a, col_b]` | The migration splits the columns into separate full-text fields. |

## Pinecone-side checklist

1. Read the field configuration from `mapping.yaml` (`objects[].text[].full_text_search`)
   or `pcmigrate` validation output; the application cannot change it after creation.
2. Send `text` for keyword queries, `query_string` for boolean logic, `$match_phrase` for
   phrases, and metadata filters for everything that was a `term`, `range`, or `WHERE`.
3. Expect `_score` to be BM25. Thresholds tuned on `ts_rank` or on an OpenSearch
   `function_score` do not carry over; re-tune on the migrated index.
4. For hybrid search run the dense (or sparse) request and the text request separately and
   fuse with reciprocal rank fusion (ADR-016); the validate stage does the same.
5. Review the `fts_unsupported` findings in the introspection report for every field: each
   one is a feature the application has to apply itself (synonyms, accent folding, prefix
   matching without an n-gram field) or accept losing.

## What the migration validated

The validate stage reports overlap@k per query class on the customer's own data
(`manifest/validation.md`): single term, multi-term (token OR), phrase, and filtered. The
thresholds are the FDE's (`validation.full_text.thresholds`). A low multi-term overlap
against pgvector is expected (`ts_rank_cd` has no IDF); a low single-term overlap points
at an analyzer gap listed in the report.
