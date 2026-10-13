# Dual-write during a migration

Change capture keeps Pinecone current from the source's own change log. When the customer
prefers to send writes from the application instead, or the source has no change log
(OpenSearch), the application dual-writes into the migration buffer. The applier drains
the buffer only after import validation passes, because bulk import cannot write into a
namespace that already exists (ADR-007). Nothing here applies when `change_capture.tier` is
`none`.

## When to start: T0

For OpenSearch sources the export stage records T0 in the manifest
(`export.outputs.t0`, also logged as "T0 recorded at ...") before it releases the OSIS
pipeline. Dual-writes must be flowing into the buffer from that instant; anything written to
OpenSearch before T0 is covered by the scan, anything after it by the buffer. With
`change_capture.rescan: true` the sync stage also re-reads documents whose `updated_at`
is later than T0, which covers writes the application failed to mirror.

## Python: the SDK shim

`pcmigrate.writepath.BufferedIndex` has the same `upsert` and `delete` signatures as
`pinecone.Index`. Swap it in behind a feature flag:

```python
import boto3
from pcmigrate.core.buffer import SqsFifoBuffer
from pcmigrate.writepath import BufferedIndex

buffer = SqsFifoBuffer(boto3.client("sqs"), queue_url=os.environ["PCMIGRATE_BUFFER_URL"])
index = BufferedIndex(buffer, default_namespace="documents")

index.upsert(vectors=[{"id": "documents#42", "values": vec, "metadata": {"title": "x"}}])
index.delete(ids=["documents#41"], namespace="documents")
```

Rules:

- Keep writing to the source of truth as well. The shim only mirrors writes.
- On a document index with full-text fields (ADR-018), pass the text under `text_fields`,
  keyed by the schema field name from `mapping.yaml`:
  `{"id": ..., "values": vec, "metadata": {...}, "text_fields": {"body": "..."}}`. The
  HTTP endpoint takes the same key on each vector.
- Use the same ids the migration generates (`id_template` in `mapping.yaml`), or the
  reconcile step will treat your writes as extras.
- Reads still go to the source until cutover. The shim raises on `delete_all` and
  filter-based deletes; neither can be replayed safely.
- Each call needs `sqs:SendMessage` on the buffer queue.

## Other languages: the HTTP endpoint

`pcmigrate.writepath.http.create_app(buffer, token=...)` is a FastAPI app with two routes
shaped like the Pinecone data plane:

```sh
uvicorn 'pcmigrate.writepath.http:create_app' --factory  # or embed in your own service
curl -X POST https://<runner>/vectors/upsert -H "Api-Key: $TOKEN" -H "Content-Type: application/json" \
  -d '{"namespace":"documents","vectors":[{"id":"documents#42","values":[...],"metadata":{"title":"x"}}]}'
curl -X POST https://<runner>/vectors/delete -H "Api-Key: $TOKEN" -d '{"namespace":"documents","ids":["documents#41"]}'
```

Run it on the runner behind the customer's load balancer, or in the application's VPC with
`sqs:SendMessage` on the queue. `GET /healthz` reports buffer depth.

## What the applier does with dual-writes

Dual-write envelopes carry the full payload, so the applier upserts them as sent. It
collapses to the last operation per id within each batch and acknowledges messages only
after Pinecone confirms the write. If the same record also changes through CDC, the later
message wins; both paths converge on the final id diff at cutover.
