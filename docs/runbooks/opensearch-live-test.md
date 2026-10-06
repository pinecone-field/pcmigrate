# Manual live test: Amazon OpenSearch Service to Pinecone end to end

Walks every command against a managed OpenSearch domain seeded with the golden index, the
test bucket, OSIS and Glue in the same account, and a live Pinecone index. Laptop mode
(`export_mode: direct`) needs none of the AWS pieces beyond the bucket and is the quickest
check; the OSIS and Glue path is what a customer migration runs.

Results of a 2M-document run of this procedure are in
[opensearch-live-test-results.md](opensearch-live-test-results.md).

## 0. Prerequisites

- An Amazon OpenSearch Service domain (OpenSearch 2.x) your AWS identity can read and write,
  with fine-grained access control off or your identity mapped to `all_access`. Note the
  endpoint, the domain ARN, and the domain name.
- `.env` with `PINECONE_API_KEY`, `TEST_BUCKET`, `AWS_REGION`, fresh AWS credentials, and
  `PCMIGRATE_STORAGE_INTEGRATION_ID`; then `set -a && source .env && set +a`.
- `pulumi login --local` and `export PULUMI_CONFIG_PASSPHRASE=...`.
- `export OPENSEARCH_URL=https://search-<domain>.<region>.es.amazonaws.com`.

Seed the golden index into the domain (SigV4, 1,005 documents, 5 sharing one vector):

```sh
uv run python - <<'EOF'
import json, os
from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient
from tests.fixtures.golden.opensearch.documents import golden_documents, golden_mapping
c = OpenSearchClient(ConnectionOptions(os.environ["OPENSEARCH_URL"], auth="sigv4"), read_only=False)
c.request("PUT", "/golden-documents", body=golden_mapping())
docs = golden_documents()
for start in range(0, len(docs), 200):
    lines = []
    for d in docs[start:start + 200]:
        lines.append(json.dumps({"index": {"_index": "golden-documents", "_id": d["doc_id"]}}))
        lines.append(json.dumps(d))
    c.request("POST", "/_bulk", params={"refresh": "wait_for"}, body="\n".join(lines) + "\n")
print(c.count("golden-documents"))
EOF
```

## 1. Working directory and settings

```sh
mkdir -p ~/pcm-os && cd ~/pcm-os
cp <repo>/examples/pcmigrate.example.yaml pcmigrate.yaml
```

Edit `pcmigrate.yaml`: `migration_id: golden-os-01`, `bucket: <TEST_BUCKET>` with
`bucket_mode: adopt` (the bucket and its storage integration already exist), the storage
integration id, `pinecone_api_key_env: PINECONE_API_KEY`, and in the `opensearch:` block the
`domain_arn` and `domain_name`. Keep `export_mode: osis_glue` for the full path.

## 2. Introspect (read-only)

```sh
uv --project <repo> run pcmigrate introspect --source opensearch \
  --migration-id golden-os-01 --index golden-os-01 --table golden-documents \
  --window 60 --out ./pcmigrate-out
```

Expect exit 0, no blockers, one `duplicate_vectors` warning (4 rows), an
`unsupported_shapes` warning for the nested `author` field, and `change_capture.tier:
dual_write` when the window saw writes, `none` otherwise. `report.md` names the k-NN plugin
version, the `cosinesimil` space type mapped to `cosine`, and the `_source` check. To see
the blocker, create a scratch index with `"_source": {"excludes": ["embedding"]}` and
introspect it: the finding names `_source.excludes: embedding`.

## 3. Approve

```sh
uv --project <repo> run pcmigrate approve --approver you@pinecone.io --out ./pcmigrate-out
```

## 4. Provision and load through OSIS and Glue

```sh
uv --project <repo> run pcmigrate run --until provision
```

The add-on stack creates the pipeline role, an OSIS pipeline `pcm-golden-os-01` armed with a
2099 start time, the Glue job `pcmigrate-golden-os-01-opensearch-transform`, and uploads the
wheel and `opensearch_transform.py` under `manifest/glue/`. The manifest's provision outputs
carry `domain_access_statement`: add it to the domain's access policy (or set
`manage_domain_policy: true` to let the stack replace the policy, which is only safe on a
test domain). With fine-grained access control, `fine_grained_access: true` maps the pipeline
role into `all_access` for you.

```sh
uv --project <repo> run pcmigrate resume --until validate
```

Watch for `T0 recorded at ...` (start dual-writes now in a real migration), `OSIS pipeline
released at T0`, `raw/ growing`, `OSIS pipeline stopped`, `Glue run jr_... started`, then
transform, emit, import, and validate. Expect `rows_by_namespace: {golden-documents: 1005}`,
4 duplicates upserted after import, and every validation check passing with recall@10 of
1.0 against `script_score` ground truth.

## 5. Kill and resume

Start `resume` again after export, kill it during Glue, and resume: the manifest holds the
Glue run id and the resumed run polls it instead of starting a second one.

## 6. Laptop mode

Set `export_mode: direct` and run `run --skip-provision --until validate`: the runner scans
the index through point-in-time slices and the same validation passes. No OSIS or Glue is
created.

## 7. Clean up

```sh
uv --project <repo> run pcmigrate teardown --yes
```

Destroys the OSIS pipeline, the Glue job, and the roles, deletes `raw/`, `canonical/`,
`import/`, and `rejects/`, keeps `manifest/`, and never touches the Pinecone index. Delete
the index and the golden index in the domain by hand afterwards.
