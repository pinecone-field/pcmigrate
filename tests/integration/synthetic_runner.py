"""Run SyntheticSource through the shared load path against real S3 and Pinecone.

Invoked as a subprocess by test_synthetic_pipeline.py so the test can kill it. Environment:
PINECONE_API_KEY, TEST_BUCKET, AWS_REGION, PCMIGRATE_INDEX, PCMIGRATE_MIGRATION_ID, and
optionally PCMIGRATE_PAUSE_AFTER_STAGE, which makes the runner sleep after that stage so the
parent can kill it at a known point.
"""

from __future__ import annotations

import os
import sys
import time

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink, S3EventSink
from pcmigrate.core.s3 import make_client
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets import SdkGateway

ROWS = 10_000
DIMENSION = 128
PARTITIONS = 4
SECRET_ARN_PLACEHOLDER = "arn:aws:secretsmanager:us-east-1:000000000000:secret:unused"


def build_mapping(migration_id: str, index: str, region: str) -> Mapping:
    return Mapping.model_validate(
        {
            "migration_id": migration_id,
            "source": {"kind": "synthetic"},
            "target": {"index": index, "region": region},
            "objects": [
                {
                    "source": "synthetic.documents",
                    "namespace": "documents",
                    "id_template": "documents#{id}",
                    "dense": {"column": "embedding", "dimension": DIMENSION, "metric": "cosine"},
                }
            ],
        }
    )


def main() -> int:
    region = os.environ.get("AWS_REGION", "us-east-1")
    bucket = os.environ["TEST_BUCKET"]
    index = os.environ["PCMIGRATE_INDEX"]
    migration_id = os.environ["PCMIGRATE_MIGRATION_ID"]
    pause_after = os.environ.get("PCMIGRATE_PAUSE_AFTER_STAGE")

    config = MigrationConfig(
        migration_id=migration_id,
        bucket=bucket,
        aws_region=region,
        pinecone_api_key_secret=SECRET_ARN_PLACEHOLDER,
        canonical_part_bytes=8 * 1024 * 1024,
        workers=PARTITIONS,
    )
    s3 = make_client(region)
    store = ManifestStore(s3, bucket, migration_id)
    manifest = store.load() or Manifest(migration_id=migration_id)
    events = EventBus(
        migration_id, [JsonLinesSink(sys.stdout), S3EventSink(s3, bucket, migration_id)]
    )
    ctx = RunContext(
        config=config,
        mapping=build_mapping(migration_id, index, region),
        manifest=manifest,
        store=store,
        events=events,
        s3=s3,
        source=SyntheticSource(rows=ROWS, dimension=DIMENSION, partitions=PARTITIONS),
        target=SdkGateway(os.environ["PINECONE_API_KEY"]),
    )

    def after_stage(stage: Stage) -> None:
        if pause_after and stage.value == pause_after:
            events.log(f"pausing after {stage.value} for the test to kill this process")
            time.sleep(600)

    pipeline = Pipeline(
        [
            PrepareIndexStage(),
            ExportStage(),
            EmitStage(),
            ImportStage(poll_interval_s=10.0, timeout_s=3600.0),
            ValidateStage(settle_window_s=300.0, poll_interval_s=10.0),
        ],
        after_stage=after_stage,
    )
    try:
        pipeline.run(ctx, S3Lock(s3, bucket, migration_id))
    finally:
        events.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
