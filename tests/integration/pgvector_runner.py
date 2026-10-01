"""Run the golden pgvector database through the full load against real S3 and Pinecone.

Driven by test_pgvector_pipeline.py as a subprocess. Environment: PGVECTOR_DSN,
PINECONE_API_KEY, TEST_BUCKET, AWS_REGION, PCMIGRATE_INDEX, PCMIGRATE_MIGRATION_ID,
PCMIGRATE_STORAGE_INTEGRATION_ID, optional PCMIGRATE_TABLES (comma list), PCMIGRATE_PARTITION_ROWS,
PCMIGRATE_WORKERS, PCMIGRATE_FIDELITY_SAMPLE, PCMIGRATE_RECALL_QUERIES, and
PCMIGRATE_PAUSE_AFTER_STAGE for the kill tests.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import timedelta

from pcmigrate.config import MigrationConfig, PlanLock
from pcmigrate.core.approval import record_approval
from pcmigrate.core.artifacts import S3ArtifactStore
from pcmigrate.core.events import EventBus, JsonLinesSink, S3EventSink
from pcmigrate.core.s3 import make_client
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.pgvector.export import PgvectorSource
from pcmigrate.sources.pgvector.introspect import IntrospectOptions, PgvectorIntrospector
from pcmigrate.targets import SdkGateway

SECRET_ARN_PLACEHOLDER = "arn:aws:secretsmanager:us-east-1:000000000000:secret:unused"


def main() -> int:
    region = os.environ.get("AWS_REGION", "us-east-1")
    bucket = os.environ["TEST_BUCKET"]
    dsn = os.environ["PGVECTOR_DSN"]
    index = os.environ["PCMIGRATE_INDEX"]
    migration_id = os.environ["PCMIGRATE_MIGRATION_ID"]
    tables = tuple(t for t in os.environ.get("PCMIGRATE_TABLES", "documents").split(",") if t)
    pause_after = os.environ.get("PCMIGRATE_PAUSE_AFTER_STAGE")

    introspector = PgvectorIntrospector(dsn, IntrospectOptions(workload_window_s=0, tables=tables))
    inventory = introspector.inventory()
    introspector.close()
    bundle = build_report(
        inventory, MappingOptions(migration_id=migration_id, index=index, region=region)
    )
    mapping = bundle.mapping.model_copy(
        update={
            "validation": bundle.mapping.validation.model_copy(
                update={
                    "fidelity_sample": int(os.environ.get("PCMIGRATE_FIDELITY_SAMPLE", "200")),
                    "recall_at_k": bundle.mapping.validation.recall_at_k.model_copy(
                        update={"queries": int(os.environ.get("PCMIGRATE_RECALL_QUERIES", "50"))}
                    ),
                }
            )
        }
    )
    config = MigrationConfig(
        migration_id=migration_id,
        bucket=bucket,
        aws_region=region,
        pinecone_api_key_secret=SECRET_ARN_PLACEHOLDER,
        storage_integration_id=os.environ.get("PCMIGRATE_STORAGE_INTEGRATION_ID") or None,
        canonical_part_bytes=int(
            os.environ.get("PCMIGRATE_CANONICAL_PART_BYTES", str(64 * 1024 * 1024))
        ),
        workers=int(os.environ.get("PCMIGRATE_WORKERS", "4")),
        partition_rows=int(os.environ.get("PCMIGRATE_PARTITION_ROWS", "250000")),
        lock_ttl_seconds=30,
    )
    s3 = make_client(region)
    store = ManifestStore(s3, bucket, migration_id)
    manifest = store.load() or Manifest(migration_id=migration_id)
    events = EventBus(
        migration_id, [JsonLinesSink(sys.stdout), S3EventSink(s3, bucket, migration_id)]
    )
    source = PgvectorSource(dsn, mapping, target_rows_per_partition=config.partition_rows)
    artifacts = S3ArtifactStore(s3, bucket, migration_id)
    plan_lock = PlanLock.compute(mapping, inventory.fingerprint)
    record_approval(artifacts, plan_lock, "integration-test@pinecone.io")
    ctx = RunContext(
        config=config,
        mapping=mapping,
        manifest=manifest,
        store=store,
        events=events,
        s3=s3,
        source=source,
        target=SdkGateway(os.environ["PINECONE_API_KEY"]),
        plan_lock=plan_lock,
        approvals=artifacts,
    )

    def after_stage(stage: Stage) -> None:
        if pause_after and stage.value == pause_after:
            events.log(f"pausing after {stage.value} for the test to kill this process")
            time.sleep(600)

    pipeline = Pipeline(
        [
            PrepareIndexStage(),
            ExportStage(),
            TransformStage(),
            EmitStage(),
            ImportStage(poll_interval_s=10.0, timeout_s=7200.0),
            ValidateStage(settle_window_s=300.0, poll_interval_s=10.0, filter_queries=10),
        ],
        after_stage=after_stage,
    )
    started = time.monotonic()
    try:
        pipeline.run(
            ctx, S3Lock(s3, bucket, migration_id, ttl=timedelta(seconds=config.lock_ttl_seconds))
        )
    finally:
        events.log(f"total wall time {time.monotonic() - started:.1f}s")
        events.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
