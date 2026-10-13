# Manual live test: pgvector to Pinecone end to end

A hands-on walkthrough of every command, against the golden Docker database, a real S3
bucket, and a real Pinecone index. Budget about 30 minutes. Nothing here touches
production: the index is created fresh, and you delete it at the end.

This runbook was executed end to end on 2026-10-02 against the golden database, the
`pcmigrate` test bucket, and a live Pinecone index; the timings and outputs quoted below
are from that run.

For a shorter, laptop-only version of this guide see `laptop-mode-test.md`.

Two modes are described. **Laptop mode** skips `provision` and uses a bucket and storage
integration you already have; it exercises every stage except the Pulumi stack. **Full
mode** adds `provision` and `teardown` of a real stack and is optional.

## 0. Prerequisites

- Docker Desktop running, and `uv sync` done in the repo.
- A Pinecone API key for a test project (`PINECONE_API_KEY`).
- An S3 bucket in `us-east-1` you can write to (`TEST_BUCKET`), and AWS credentials for it
  (`aws sts get-caller-identity` must work).
- A Pinecone storage integration whose IAM role can read that bucket. The id is in the
  console under Manage, Storage integrations, or create one as `docs/runbooks/storage-integration.md`
  describes. Put it in `PCMIGRATE_STORAGE_INTEGRATION_ID`.

```sh
cd <repo>
docker compose up -d --wait pgvector            # golden DB on :5432, seeded on first start
export PINECONE_API_KEY=... TEST_BUCKET=... AWS_REGION=us-east-1
export PCMIGRATE_STORAGE_INTEGRATION_ID=...
export PGVECTOR_DSN=postgresql://pcmigrate:pcmigrate@localhost:5432/golden
```

Checks before you start:

```sh
aws s3 ls "s3://$TEST_BUCKET/" >/dev/null && echo bucket ok
docker compose exec pgvector psql -U pcmigrate -d golden -c 'select count(*) from documents'   # 1005
```

## 1. Make a working directory with run settings

Everything the CLI reads lives in one directory: `pcmigrate.yaml` plus the artifacts that
`introspect` writes.

```sh
mkdir -p ~/pcm-live && cd ~/pcm-live
cp <repo>/examples/pcmigrate.example.yaml pcmigrate.yaml
alias pcm='pcm'      # every command below uses this alias
```

Edit `pcmigrate.yaml`:

- `migration_id`: a fresh slug, for example `golden-live-01`. It is the S3 prefix.
- `bucket`: your `TEST_BUCKET`.
- `storage_integration_id`: your integration id.
- Leave `pinecone_api_key_env: PINECONE_API_KEY` so the key comes from your shell. On a
  runner you would remove that line and point `pinecone_api_key_secret` at Secrets Manager.
- `partition_rows: 1000` is deliberately small so the 1,005-row table splits in two.

## 2. Introspect (read-only)

```sh
pcm introspect \
  --migration-id golden-live-01 --index pcm-live-01 --region us-east-1 \
  --table documents --window 60 --out pcmigrate-out
```

`--window 60` samples the workload for a minute. With no writes it proposes
`change_capture.tier: none`. To see the trigger-outbox path instead, keep writes flowing in
another terminal during the window (step 7 shows a writer) or edit the tier by hand.

What to look at:

- `pcmigrate-out/report.html` in a browser: nine sections, one duplicate-vector warning
  (five golden rows share a vector) and an info item about the `tsvector` column.
- `pcmigrate-out/mapping.yaml`: the proposed mapping. Edit anything you like; the plan hash
  changes and approval below must be redone.
- The command exits 3 if blockers are open. Try `--table tenants` once to see the forced-RLS
  blocker, then go back to `documents`.

Confirm it was read-only: the introspector connects with `default_transaction_read_only=on`
and refuses non-SELECT statements. `docker compose exec pgvector psql -U pcmigrate -d golden -c "select query from pg_stat_statements where query ~* '^(insert|update|delete|create|alter|drop)' and query !~* 'pg_stat|golden_random|documents|products|tenants|events'"`
should list nothing new.

### Variant: generate sparse vectors from text (ADR-017)

The golden table stores a `sparsevec` column, so `--roadmap hybrid` maps it. To exercise
the generated path instead, name the text fields:

```sh
pcm introspect \
  --migration-id golden-gen-01 --index pcm-gen-01 --region us-east-1 \
  --table documents --window 0 --roadmap hybrid \
  --sparse-from title --sparse-from body --out pcmigrate-out
```

The mapping now carries `sparse: {kind: generated, text_fields: [title, body], model:
{provider: pinecone, name: pinecone-sparse-english-v0, truncate: END}}` and a document
target. Section 2 of the report shows the object as "hybrid (generated)"; the findings
estimate the tokens, cost, and minutes at Starter and Standard throughput, and remind you
that queries must be encoded with the same model and `input_type: query`. Transform calls
Pinecone Inference with the migration's Pinecone key (1,001 rows, a few thousand tokens,
well under a cent), writes `manifest/sparse_cache/documents/`, and emits `sparse_encoded`
events; validation then reports `sparse_overlap` and `hybrid_overlap` against exact
rankings computed from the clean canonical zone. Kill and resume transform to watch the
cache keep the model calls at zero on the second pass.

## 3. Approve

```sh
pcm approve --approver you@pinecone.io --out pcmigrate-out
cat pcmigrate-out/manifest/approvals.json
```

Now edit `pcmigrate-out/mapping.yaml` (change `threshold: 0.95` to `0.9`), re-run
introspect, and watch `pcmigrate run` refuse with exit code 4 until you approve again.
Revert the edit and re-approve before continuing.

## 4. Run the load path

Laptop mode, stopping before sync:

```sh
pcm run --skip-provision --until validate
```

The first thing `run` does is record `introspect` and `approve` in the S3 manifest from your
local artifacts (and `provision` as skipped), so `status` shows the whole lifecycle. Then a
live stage table follows the pipeline. Expect, in order: `prepare_index` creates
`pcm-live-01` (768 dims, cosine, aws us-east-1); `export` writes two partitions to
`s3://$TEST_BUCKET/golden-live-01/canonical/`; `transform` reports 4 duplicates routed to
`manifest/duplicates/`; `emit` writes `import/batch-00000/documents/`; `import` starts a
Pinecone import with `error_mode=abort` and polls it (4m43s on the recorded run), then
upserts the 4 duplicates; `validate` runs count parity, vector and metadata fidelity,
recall@10, filter parity, and the id diff (about 50 seconds). The whole run took just over
six minutes.

Filter parity and recall are approximate-search checks: Pinecone serverless returns
approximate neighbours, so both pass on mean overlap@k against the approved threshold
(0.95 by default), and `validation.json` lists each filter's exact-match status. On the
recorded run recall@10 averaged 0.99 and 19 of 20 filters matched exactly.

Watch progress from a second terminal:

```sh
pcm status
aws s3 ls "s3://$TEST_BUCKET/golden-live-01/" --recursive | head -30
```

Read the results:

```sh
aws s3 cp "s3://$TEST_BUCKET/golden-live-01/manifest/validation.md" -
```

## 5. Kill and resume

Run step 4 again with `--until export` on a fresh `migration_id`, or simply press Ctrl-C
during `import`, then:

```sh
pcm status            # the failed or running stage is visible
pcm resume --skip-provision --until validate
```

`status` reads only the S3 manifest, so it works from any machine with bucket access while
a run is in progress.

Completed stages are skipped (`stage_skipped` events), an import that was already started
is polled by its recorded id rather than restarted, and the lock lease of the dead process
expires within `lock_ttl_seconds`.

## 6. Shadow reads and cutover

```sh
pcm cutover --shadow 25
```

A table shows mean overlap@10 (expect 1.0 on the golden data) and p50/p95 latency for the
source and for Pinecone.

```sh
pcm cutover --confirm
```

With `tier: none` this runs only the final reconcile. To see it repair a gap, insert a row
first:

```sh
docker compose exec pgvector psql -U pcmigrate -d golden -c \
  "insert into documents (title, category, published_at, embedding) select 'late row', 'late', now(), embedding from documents where id = 1"
```

Then `cutover --confirm` reports one upsert in `manifest/cutover.json` and the namespace
count becomes 1,006. Delete that row afterwards so the golden database stays clean:
`delete from documents where title = 'late row'`.

## 7. Optional: a change-capture tier under live writes

The mapping is yours to edit. Set `change_capture.tier` in `pcmigrate-out/mapping.yaml` to
`watermark`, `trigger_outbox`, or `logical`, re-run `approve` (the plan hash changed), and
use a new `migration_id` in `pcmigrate.yaml` so the manifest starts clean. Run the load
path with `--until validate`, then start sync for a bounded number of rounds while a writer
changes rows:

```sh
pcm run --skip-provision --sync-rounds 30 &
docker compose exec pgvector psql -U pcmigrate -d golden -c \
  "update documents set title = title || ' *', updated_at = now() where id between 1 and 20"
wait
pcm cutover --confirm
```

`applier_lag` events show buffer depth and oldest message age; `cutover.json` shows the
final drain, the removed trigger or slot, and a converged reconcile. In laptop mode there is
no SQS queue, so sync needs a buffer: run the Docker integration test
`uv run pytest -m integration tests/integration/test_pgvector_cdc_docker.py -v` to see all
three tiers converge with an in-memory buffer, or use full mode below.

## 8. Clean up

```sh
pcm teardown --no-stack --yes        # deletes canonical/, import/, rejects/; keeps manifest/
aws s3 rm "s3://$TEST_BUCKET/golden-live-01/" --recursive          # remove the audit trail too, for a test run
```

Teardown never touches the Pinecone index. Delete the test index yourself:

```sh
uv --project <repo> run python -c "
import os; from pinecone import Pinecone
Pinecone(api_key=os.environ['PINECONE_API_KEY']).delete_index('pcm-live-01')"
```

## Full mode: provision and teardown a real stack

Adds the Pulumi base stack (bucket, KMS key, secrets, roles, SQS when the tier is not none).

1. `pulumi login --local` and `export PULUMI_CONFIG_PASSPHRASE=...`, or log in to your
   Pulumi backend.
2. In `pcmigrate.yaml`, either set `bucket` to a name that does not exist yet (the stack
   creates it) and remove `storage_integration_id` for now, or keep the shared test bucket
   with `bucket_mode: adopt` and its existing `storage_integration_id` (the stack then
   creates everything except the bucket).
3. `pcmigrate run --until provision`. Outputs land in the manifest: bucket, KMS key,
   runner role, and `storage_integration_role_arn`.
4. Register that role as a Pinecone storage integration (`docs/runbooks/storage-integration.md`),
   put the returned id into `storage_integration_id`, then `pcmigrate resume --until validate`.
5. Finish with steps 6 and 8, using `pcmigrate teardown --yes` without `--no-stack` so
   Pulumi destroys the stack.

## What good looks like

| Step | Expect |
| --- | --- |
| introspect | exit 0, 0 blockers, 1 warning (duplicates), tier none after a quiet window |
| approve | `approvals.json` with your email and the plan hash |
| run | all stages complete; `validation.md` says all checks passed; namespace count 1,005 |
| resume | `stage_skipped` for completed stages; same final counts |
| cutover --shadow | mean overlap 1.0 |
| cutover --confirm | reconcile converged; with a late row, one upsert |
| teardown | staged zones gone, `manifest/` kept, index still present |
