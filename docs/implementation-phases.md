# Implementation phases

Eight phases take the tool from an empty repo to FTS. Phases 0 to 4 deliver a production pgvector dense migration. Phase 5 adds OpenSearch dense, and can start once Phase 3 is merged. Phases 6 and 7 add hybrid and FTS for both sources. Each phase ends with a merged PR set and a demo against the golden datasets.

1. **Phase 0: Repo bootstrap.** Create the skeleton:
   - the layout above, `CLAUDE.md`, and the ADRs as `docs/adr/` files
   - CI for lint, types, and unit tests
   - Docker Compose with pgvector and OpenSearch seeded from golden fixtures

   Exit: `pcmigrate --help` works, CI is green, and `docker compose up` seeds both sources.
2. **Phase 1: Core framework.** Build the shared machinery:
   - config and mapping models, plus the stage state machine with its S3 manifest and lock
   - the event bus and terminal view, and the canonical Parquet writer
   - the `SourceConnector` protocol and a synthetic test source
   - index manager (create and adopt), vector Parquet emitter, and importer
   - the count-validation skeleton, and the base Pulumi stack (bucket, KMS, secrets, IAM, storage-integration role)

   Exit: synthetic data goes through canonical, emit, import, and count validation into a test index, and a killed run resumes.
3. **Phase 2: pgvector introspection and the approval gate.** Build introspection and the report:
   - discovery for `vector`, `halfvec`, `sparsevec`, and `tsvector`, including partitioned tables
   - metric inference, RLS classification, and workload sampling
   - the findings engine and the mapping generator
   - Markdown and HTML report rendering
   - `plan.lock` and `approve`, with every later stage refusing to run without a matching approval

   Exit: the report on the golden database matches a snapshot test of expected findings, and an FDE can review and approve it.
4. **Phase 3: pgvector dense full load.** Build the load path:
   - PK-range partitioning and the shared exported snapshot
   - parallel workers streaming Parquet parts to S3
   - transform with pre-validation and rejects
   - duplicate-vector handling, emit and import, and every dense validation check
   - the Pulumi runner stack

   Exit: a 1M-row synthetic table migrates with validation passing, and a kill at any stage resumes cleanly.
5. **Phase 4: Change capture and cutover.** Build the post-snapshot path:
   - all three pgvector change-capture tiers
   - the SQS FIFO buffer and the applier
   - the dual-write SDK shim and HTTP endpoint
   - reconcile, a shadow-read comparison report, and the `cutover` and `teardown` commands

   Exit: inserts, updates, and deletes made during export converge, and the id diff is zero.
6. **Phase 5: OpenSearch dense.** Build the second source:
   - OpenSearch introspection, including the `_source` vector check and space-type mapping
   - OSIS pipeline and Glue job Pulumi modules
   - the Glue transform reusing `pcmigrate.core.canonical`
   - dual-write as the change path, and exact-kNN ground truth via `script_score`

   Exit: the golden OpenSearch index migrates with the same validation passing.
7. **Phase 6: Hybrid.** Add sparse vectors:
   - the document-index JSONL emitter
   - sparse extraction from `sparsevec` and OpenSearch sparse fields
   - hybrid readiness in the report, and hybrid validation

   Exit: a dense plus sparse dataset from each source migrates into one document index, and hybrid overlap meets the threshold.
8. **Phase 7: FTS.** Add full-text fields:
   - text-field mapping into `full_text_search` fields
   - an analyzer translation table (language, stemming, stop words, n-grams), with unmappable features reported
   - FTS relevance comparison
   - a query-translation guide for the customer's app team

   Exit: keyword and phrase queries on the golden corpus report overlap per query class, and the FDE signs off the analyzer gaps.

