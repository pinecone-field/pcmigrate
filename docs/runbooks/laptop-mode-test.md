# Testing a migration in laptop mode

Laptop mode runs every stage except `provision` from your own machine: the golden Docker
database is the source, an S3 bucket you already have is the staging area, and a fresh
Pinecone index is the target. It takes about 30 minutes and leaves nothing behind. Use it
to see the tool work end to end before touching customer infrastructure.

For the full-stack variant (Pulumi `provision` and a real `teardown`) and for change
capture under live writes, see `manual-live-test.md`.

## What you need

| Item | Where it comes from |
| --- | --- |
| Docker Desktop and `uv sync` in the repo | local |
| `PINECONE_API_KEY` | a Pinecone test project |
| `TEST_BUCKET` in `us-east-1` plus AWS credentials that can read, write, list, and delete in it | your AWS account |
| `PCMIGRATE_STORAGE_INTEGRATION_ID` | Pinecone console, Manage, Storage integrations; or create one per `storage-integration.md` |

## 1. Start the source and set your shell

```sh
cd <repo>
docker compose up -d --wait pgvector
export PINECONE_API_KEY=... TEST_BUCKET=... AWS_REGION=us-east-1
export PCMIGRATE_STORAGE_INTEGRATION_ID=...
export PGVECTOR_DSN=postgresql://pcmigrate:pcmigrate@localhost:5432/golden

aws s3 ls "s3://$TEST_BUCKET/" >/dev/null && echo bucket ok
docker compose exec pgvector psql -U pcmigrate -d golden -tAc 'select count(*) from documents'   # 1005
```

## 2. Create a working directory

```sh
mkdir -p ~/pcm-live && cd ~/pcm-live
cp <repo>/examples/pcmigrate.example.yaml pcmigrate.yaml
alias pcm='uv --project <repo> run pcmigrate'
```

Edit `pcmigrate.yaml`:

| Field | Set to |
| --- | --- |
| `migration_id` | a fresh slug, for example `golden-live-01`; it becomes the S3 prefix |
| `bucket` | your `TEST_BUCKET` |
| `storage_integration_id` | your integration id |
| `pinecone_api_key_env` | leave as `PINECONE_API_KEY` so the key comes from your shell |
| `partition_rows` | leave at `1000` so the 1,005-row table splits into two partitions |

## 3. Introspect

```sh
pcm introspect --migration-id golden-live-01 --index pcm-live-01 \
  --table documents --window 60 --out pcmigrate-out
```

Expect exit code 0, zero blockers, one warning (five golden rows share a vector), and one
info item (a `tsvector` column the dense wave does not migrate). After a quiet 60-second
window the mapping proposes `change_capture.tier: none`.

Look at:

- `pcmigrate-out/report.html` in a browser: nine sections ending with the approval checklist.
- `pcmigrate-out/mapping.yaml`: the proposed mapping you are about to approve.

Optional detours:

- `pcm introspect ... --table tenants` exits 3: forced row-level security hides every row
  from the connecting role, reported as a blocker. Go back to `documents`.
- Introspection is read-only by construction. This should print nothing new:
  `docker compose exec pgvector psql -U pcmigrate -d golden -c "select query from pg_stat_statements s join pg_roles r on r.oid = s.userid where r.rolname = 'pcmigrate' and query ~* '^(insert|update|delete|create|alter|drop)' and query !~* 'pg_stat|golden_random|documents|products|tenants|events|late row'"`

## 4. Approve, and prove the gate

```sh
pcm approve --approver you@pinecone.io --out pcmigrate-out
cat pcmigrate-out/manifest/approvals.json
```

Now change `threshold: 0.95` to `0.9` in `pcmigrate-out/mapping.yaml`, run the introspect
command again (it recomputes `plan.lock`), and run step 5. It refuses with exit code 4 and
names the plan hash that is not approved. Revert the edit, re-run introspect, and approve
again before continuing.

## 5. Run the load path

```sh
pcm run --skip-provision --until validate
```

The first thing `run` does is record `introspect` and `approve` in the S3 manifest from
your local artifacts and mark `provision` as skipped. Then a live table follows the stages:

| Stage | What happens | Recorded run |
| --- | --- | --- |
| `prepare_index` | creates `pcm-live-01`: 768 dims, cosine, aws us-east-1 | 7s |
| `export` | one shared snapshot, two PK-range partitions to `canonical/` | a few seconds |
| `transform` | validates every record; 4 duplicate vectors go to `manifest/duplicates/` | 3s |
| `emit` | writes `import/batch-00000/documents/part-00000.parquet` | 2s |
| `import` | Pinecone bulk import with `error_mode=abort`, then upserts the 4 duplicates | 4m43s |
| `validate` | counts, vector and metadata fidelity, recall@10, filter parity, id diff | 50s |

From a second terminal while it runs:

```sh
pcm status
aws s3 ls "s3://$TEST_BUCKET/golden-live-01/" --recursive | head -30
```

When it finishes, read the validation report:

```sh
aws s3 cp "s3://$TEST_BUCKET/golden-live-01/manifest/validation.md" -
```

Recall and filter parity are approximate-search checks: they pass on mean overlap@10 at or
above the approved threshold (0.95), and `validation.json` lists each filter's exact-match
status. On the recorded run recall averaged 0.99 and 19 of 20 filters matched exactly.

## 6. Kill it and resume

Run step 5 again on a fresh `migration_id` and press Ctrl-C while `import` is polling.
Then:

```sh
pcm status                                      # import shows failed or running
pcm resume --skip-provision --until validate
```

Completed stages are skipped (`stage_skipped` events). An import that was already started
is polled by its recorded id rather than started again. If the killed process still held
the run lock, resume waits for the lease to lapse (`lock_ttl_seconds`, 60s in the example
file) instead of failing.

## 7. Shadow reads

```sh
pcm cutover --shadow 25
```

A table shows mean overlap@10 between exact kNN on Postgres and Pinecone's results, plus
p50 and p95 latency on each side. Expect overlap 1.0 on the golden data.

## 8. Cutover

Insert a row the import never saw, then confirm:

```sh
docker compose exec pgvector psql -U pcmigrate -d golden -c \
  "insert into documents (title, category, published_at, embedding) select 'late row', 'late', now(), embedding from documents where id = 1"
pcm cutover --confirm
aws s3 cp "s3://$TEST_BUCKET/golden-live-01/manifest/cutover.json" -
```

With `tier: none` there is no buffer to drain, so cutover runs only the final reconcile. It
reports one missing id, upserts it, and converges; the namespace count becomes 1,006. Put
the golden database back afterwards:

```sh
docker compose exec pgvector psql -U pcmigrate -d golden -c "delete from documents where title = 'late row'"
```

## 9. Clean up

```sh
pcm teardown --no-stack --yes                                   # deletes canonical/, import/, rejects/; keeps manifest/
aws s3 rm "s3://$TEST_BUCKET/golden-live-01/" --recursive       # drop the audit trail too, for a test run
uv --project <repo> run python -c "
import os; from pinecone import Pinecone
Pinecone(api_key=os.environ['PINECONE_API_KEY']).delete_index('pcm-live-01')"
```

Teardown never touches the Pinecone index, by design (ADR-010), which is why the last
command is yours to run.

## What good looks like

| Step | Expect |
| --- | --- |
| introspect | exit 0, 0 blockers, 1 warning, tier none |
| approve | your email and the plan hash in `approvals.json` |
| run | six stages complete, `validation.md` says every check passed, namespace count 1,005 |
| resume | `stage_skipped` for finished stages, same final counts |
| cutover --shadow | mean overlap 1.0 |
| cutover --confirm | reconcile converged, one upsert for the late row |
| teardown | staged zones gone, `manifest/` kept, index still present until you delete it |

## If something fails

- `pcm status` shows the failed stage and its error. Fix the cause and `pcm resume`.
- `aws s3 cp s3://$TEST_BUCKET/<migration_id>/manifest/state.json -` is the full manifest.
- Events for every run are under `manifest/events/` as JSON lines.
- Exit codes: 1 generic, 2 not implemented, 3 blockers open, 4 approval missing.
