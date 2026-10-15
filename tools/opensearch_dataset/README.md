# Seeding an OpenSearch domain with the 10M passages dataset

`seed_opensearch.py` is a stand-alone tool (its dependencies are declared inline, so
`uv run` needs nothing from the pcmigrate environment) that stands up an Amazon OpenSearch
Service domain and loads a parquet vector dataset into it, one index per file. It
simulates a customer who already runs OpenSearch; pcmigrate then migrates each index into
its own Pinecone namespace.

The dataset under `pcmigrate-data/` is 10 parquet files of 1,000,000 rows each
(`id`, `metadata` JSON string, `values` as 1,024 doubles) plus 10,000 queries with their
top-100 ground-truth neighbours. The vectors are unit-norm and the ground truth is ordered by
cosine similarity, so the indices use `cosinesimil` and Pinecone `cosine`.

## Steps

```sh
cd tools/opensearch_dataset
cp config.example.yaml config.yaml            # edit paths, domain size, region
set -a && source ../../.env && set +a          # AWS credentials

uv run seed_opensearch.py --config config.yaml plan            # rows, bytes, index names
uv run seed_opensearch.py --config config.yaml create-domain   # 15-25 min, prints the endpoint
uv run seed_opensearch.py --config config.yaml create-indices
uv run seed_opensearch.py --config config.yaml load            # resumable; see sizing below
uv run seed_opensearch.py --config config.yaml verify          # counts per index vs parquet rows
uv run seed_opensearch.py --config config.yaml recall --queries 500 --k 10,100
```

Then migrate with pcmigrate (`docs/runbooks/opensearch-live-test.md`), with
`--table "passages-*"` at introspection so all ten indices are inventoried. The generated
`mapping.yaml` gives each index its own namespace named after the index. To keep the original
document ids in Pinecone, set every object's `id_template` to `{_id}` before approving;
`recall --target pinecone` strips an `index#` prefix either way. The target may be a vector
index (query API) or the document index a hybrid migration creates (Documents API, one
`dense_vector` clause on `pinecone.dense_field`, default `dense`); the tool detects which.

After the migration:

```sh
uv run seed_opensearch.py --config config.yaml recall --target pinecone --queries 500
uv run seed_opensearch.py --config config.yaml delete-domain --yes
```

## Sizing and where to run the load

- Bulk JSON for 10M documents at 1,024 dims is about 120 GB uncompressed, roughly 50 GB
  gzipped on the wire (level-1 gzip, 0.1 ms per document; the client's built-in level-9
  compression was 30 times slower). A laptop with 8 workers serializes about 3,000 documents
  per second, so the upload link decides: at 100 Mbit/s the 50 GB alone takes about 70
  minutes, so for the full 10M run the load from an EC2 instance in the domain's region
  (`c6i.4xlarge` or similar, Python 3.12 and `uv`, the data synced to S3 and
  `data.dir: s3://bucket/prefix`). `load.workers` processes each own a parquet row group
  (20,000 rows) and send 500-document bulk requests.
- A 3 x `r6g.4xlarge.search` domain with 500 GB gp3 per node indexes about 3,000 to 6,000
  documents per second with the `lucene` engine; expect 30 to 60 minutes for 10M. Replicas
  and refresh are switched off during the load and restored by `finalize`.
- The `faiss` engine is faster to search but needs about 50 GB of off-heap memory across
  the cluster for 10M x 1,024; `lucene` (default) keeps the graph on disk.
- Small instances (t3) cancel searches over a few MB of heap; do not use them for this set.
- `data.max_rows_per_file` and `data.only_files` give a rehearsal at any scale, for example
  100,000 rows per file for a 1M-document run.
- The loader keeps `load.state_file`; rerunning `load` after a failure skips finished row
  groups. Delete the file to start over (`create-indices --recreate` drops the indices).

## Costs

3 x `r6g.4xlarge.search` is about $3.6 per hour plus EBS. Delete the domain when done.
