"""Seed PCMIGRATE_PERF_ROWS rows at 768 dims into the golden database and time the export.

Default 1,000,000 rows (about 3 GB of vectors). The seed itself takes minutes; the test
prints throughput for seeding, canonical export to real S3, and transform. Needs the
golden Docker database and TEST_BUCKET with AWS credentials. Set PCMIGRATE_PERF_ROWS lower
for a quick run. The table is dropped afterwards.
"""

import io
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import boto3
import psycopg
import pytest

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.sources.pgvector.export import PgvectorSource
from tests.unit.conftest import approve_ctx

pytestmark = [pytest.mark.integration, pytest.mark.slow]

DSN = os.environ.get("PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ROWS = int(os.environ.get("PCMIGRATE_PERF_ROWS", "1000000"))
DIM = 768
ARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:unused"


@pytest.fixture
def perf_table() -> Iterator[str]:
    name = f"perf_{uuid.uuid4().hex[:6]}"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            f"CREATE TABLE {name} (id bigserial PRIMARY KEY, category text NOT NULL, "
            f"score real, embedding vector({DIM}) NOT NULL)"
        )
        started = time.monotonic()
        step = 50_000
        for lower in range(1, ROWS + 1, step):
            upper = min(ROWS, lower + step - 1)
            conn.execute(
                f"INSERT INTO {name} (category, score, embedding) "
                f"SELECT (ARRAY['a','b','c','d'])[1 + g % 4], random(), "
                f"(SELECT array_agg(random()::real) FROM generate_series(1, {DIM} + g * 0))::vector "
                f"FROM generate_series({lower}, {upper}) g"
            )
        elapsed = time.monotonic() - started
        print(f"\nseeded {ROWS} rows x {DIM} dims in {elapsed:.0f}s ({ROWS / elapsed:.0f} rows/s)")
    try:
        yield name
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {name}")


def test_export_throughput_one_million_rows(perf_table: str, tmp_path: Path) -> None:
    bucket = os.environ.get("TEST_BUCKET")
    if not bucket:
        pytest.skip("TEST_BUCKET is not set")
    migration_id = f"perf-{uuid.uuid4().hex[:8]}"
    mapping = Mapping.model_validate(
        {
            "migration_id": migration_id,
            "source": {"kind": "pgvector"},
            "target": {"index": "perf-unused", "region": REGION},
            "objects": [
                {
                    "source": f"public.{perf_table}",
                    "namespace": "perf",
                    "id_template": "perf#{id}",
                    "dense": {"column": "embedding", "dimension": DIM, "metric": "cosine"},
                    "metadata": [
                        {"source": "category", "type": "string"},
                        {"source": "score", "type": "float"},
                    ],
                }
            ],
        }
    )
    workers = int(os.environ.get("PCMIGRATE_WORKERS", "4"))
    cfg = MigrationConfig(
        migration_id=migration_id,
        bucket=bucket,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        workers=workers,
        partition_rows=int(os.environ.get("PCMIGRATE_PARTITION_ROWS", "250000")),
        canonical_part_bytes=256 * 1024 * 1024,
    )
    s3 = boto3.client("s3", region_name=REGION)
    store = ManifestStore(s3, bucket, migration_id)
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=Manifest(migration_id=migration_id),
        store=store,
        events=EventBus(migration_id, [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=PgvectorSource(DSN, mapping, target_rows_per_partition=cfg.partition_rows),
    )
    approve_ctx(ctx, tmp_path, fingerprint="perf")
    try:
        started = time.monotonic()
        Pipeline([ExportStage()]).run(ctx, S3Lock(s3, bucket, migration_id), until=Stage.EXPORT)
        export_s = time.monotonic() - started
        export = ctx.manifest.stage(Stage.EXPORT).outputs
        assert export["rows"] == ROWS
        parts = export["parts_by_namespace"]["perf"]
        total_bytes = sum(s3.head_object(Bucket=bucket, Key=k)["ContentLength"] for k in parts)
        print(
            f"export: {ROWS} rows in {export_s:.0f}s = {ROWS / export_s:.0f} rows/s, "
            f"{total_bytes / 1024**2 / export_s:.1f} MB/s to S3 across {len(parts)} parts, "
            f"{len(export['partitions'])} partitions, {workers} workers"
        )
        started = time.monotonic()
        Pipeline([ExportStage(), TransformStage()]).run(ctx, S3Lock(s3, bucket, migration_id))
        transform_s = time.monotonic() - started
        transform = ctx.manifest.stage(Stage.TRANSFORM).outputs
        assert transform["rows_by_namespace"] == {"perf": ROWS}
        print(f"transform: {ROWS} rows in {transform_s:.0f}s = {ROWS / transform_s:.0f} rows/s")
    finally:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{migration_id}/"
        ):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
