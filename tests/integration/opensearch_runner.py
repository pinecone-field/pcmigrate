"""Run the golden OpenSearch index through OSIS, Glue, S3, and Pinecone in the test AWS account.

Driven by test_opensearch_pipeline.py as a subprocess. Environment: OPENSEARCH_URL (an
Amazon OpenSearch Service endpoint seeded with the golden index), OPENSEARCH_DOMAIN_ARN,
OPENSEARCH_DOMAIN_NAME, PINECONE_API_KEY, TEST_BUCKET, AWS_REGION, PCMIGRATE_INDEX,
PCMIGRATE_MIGRATION_ID, PCMIGRATE_STORAGE_INTEGRATION_ID, PCMIGRATE_KMS_KEY_ARN (optional),
PCMIGRATE_PHASE (provision | load | teardown), and PCMIGRATE_PAUSE_AFTER_STAGE for kill tests.

The runner provisions only the OpenSearch add-on stack (OSIS pipeline, Glue job, roles) on
top of the existing test bucket; the base stack is not needed for the test account.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import timedelta
from typing import Any

from pcmigrate.config import ChangeCaptureSpec, MigrationConfig, PlanLock
from pcmigrate.config.settings import OpenSearchSettings
from pcmigrate.core.approval import record_approval
from pcmigrate.core.artifacts import S3ArtifactStore
from pcmigrate.core.events import EventBus, JsonLinesSink, S3EventSink
from pcmigrate.core.s3 import make_client
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export_opensearch import OpenSearchExportStage, ScanSettings
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.provision import ProvisionStage
from pcmigrate.core.stages.teardown import TeardownStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient
from pcmigrate.sources.opensearch.export import OpenSearchSource
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from pcmigrate.sources.opensearch.provision import OpenSearchAddon
from pcmigrate.targets import SdkGateway

SECRET_ARN_PLACEHOLDER = "arn:aws:secretsmanager:us-east-1:000000000000:secret:unused"


class AddonOnlyDriver:
    """Brings up just the OpenSearch add-on, using the existing test bucket as the base."""

    def __init__(self, addon: OpenSearchAddon, *, bucket: str, kms_key_arn: str | None) -> None:
        self._addon = addon
        self._bucket = bucket
        self._kms = kms_key_arn

    def up(self, ctx: RunContext, on_output: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "bucket_name": self._bucket,
            "bucket_arn": f"arn:aws:s3:::{self._bucket}",
            "kms_key_arn": self._kms or "",
            "runner_role_arn": "",
            "secret_arns": {},
        }
        base.update(self._addon.up(ctx, base, on_output))
        return base

    def destroy(self, ctx: RunContext, on_output: Any) -> None:
        self._addon.destroy(ctx, on_output)


def main() -> int:
    region = os.environ.get("AWS_REGION", "us-east-1")
    bucket = os.environ["TEST_BUCKET"]
    url = os.environ["OPENSEARCH_URL"]
    index = os.environ["PCMIGRATE_INDEX"]
    migration_id = os.environ["PCMIGRATE_MIGRATION_ID"]
    source_index = os.environ.get("PCMIGRATE_SOURCE_INDEX", "golden-documents")
    phase = os.environ.get("PCMIGRATE_PHASE", "load")
    pause_after = os.environ.get("PCMIGRATE_PAUSE_AFTER_STAGE")

    options = ConnectionOptions(url, auth="sigv4", region=region)
    settings = OpenSearchSettings(
        domain_arn=os.environ.get("OPENSEARCH_DOMAIN_ARN") or None,
        domain_name=os.environ.get("OPENSEARCH_DOMAIN_NAME") or None,
        manage_domain_policy=os.environ.get("PCMIGRATE_MANAGE_DOMAIN_POLICY", "0") == "1",
        fine_grained_access=os.environ.get("PCMIGRATE_FGAC", "0") == "1",
        osis_min_units=1,
        osis_max_units=2,
        glue_workers=2,
        scan_timeout_s=int(os.environ.get("PCMIGRATE_SCAN_TIMEOUT_S", "3600")),
    )

    introspector = OpenSearchIntrospector(
        OpenSearchClient(options),
        IntrospectOptions(workload_window_s=0, indices=(source_index,), sample_docs=300),
    )
    inventory = introspector.inventory()
    introspector.close()
    bundle = build_report(
        inventory, MappingOptions(migration_id=migration_id, index=index, region=region)
    )
    mapping = bundle.mapping.model_copy(
        update={
            "change_capture": ChangeCaptureSpec(tier="none"),
            "validation": bundle.mapping.validation.model_copy(
                update={
                    "fidelity_sample": int(os.environ.get("PCMIGRATE_FIDELITY_SAMPLE", "200")),
                    "recall_at_k": bundle.mapping.validation.recall_at_k.model_copy(
                        update={"queries": int(os.environ.get("PCMIGRATE_RECALL_QUERIES", "50"))}
                    ),
                }
            ),
        }
    )
    config = MigrationConfig(
        migration_id=migration_id,
        bucket=bucket,
        aws_region=region,
        pinecone_api_key_secret=SECRET_ARN_PLACEHOLDER,
        storage_integration_id=os.environ.get("PCMIGRATE_STORAGE_INTEGRATION_ID") or None,
        kms_key_arn=os.environ.get("PCMIGRATE_KMS_KEY_ARN") or None,
        canonical_part_bytes=64 * 1024 * 1024,
        workers=2,
        partition_rows=1000,
        lock_ttl_seconds=60,
    )
    s3 = make_client(region)
    store = ManifestStore(s3, bucket, migration_id)
    manifest = store.load() or Manifest(migration_id=migration_id)
    events = EventBus(
        migration_id, [JsonLinesSink(sys.stdout), S3EventSink(s3, bucket, migration_id)]
    )
    source = OpenSearchSource(options, mapping, target_rows_per_partition=config.partition_rows)
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
    ctx.extras["export_mode"] = "osis_glue"
    driver = AddonOnlyDriver(
        OpenSearchAddon(settings, endpoint=url, work_dir=os.environ.get("PULUMI_WORK_DIR")),
        bucket=bucket,
        kms_key_arn=config.kms_key_arn,
    )

    def after_stage(stage: Stage) -> None:
        if pause_after and stage.value == pause_after:
            events.log(f"pausing after {stage.value} for the test to kill this process")
            time.sleep(600)

    lock = S3Lock(s3, bucket, migration_id, ttl=timedelta(seconds=config.lock_ttl_seconds))
    stages: list[Any]
    if phase == "provision":
        stages = [ProvisionStage(driver)]
    elif phase == "teardown":
        stages = [TeardownStage(driver, retain_data=False)]
    else:
        stages = [
            PrepareIndexStage(),
            OpenSearchExportStage(
                mode="osis_glue",
                scan=ScanSettings(
                    poll_s=30.0,
                    quiet_s=float(os.environ.get("PCMIGRATE_QUIET_S", "180")),
                    timeout_s=float(settings.scan_timeout_s),
                ),
            ),
            TransformStage(),
            EmitStage(),
            ImportStage(poll_interval_s=10.0, timeout_s=7200.0),
            ValidateStage(settle_window_s=300.0, poll_interval_s=10.0, filter_queries=10),
        ]
    started = time.monotonic()
    try:
        Pipeline(stages, after_stage=after_stage).run(ctx, lock)
    finally:
        events.log(f"total wall time {time.monotonic() - started:.1f}s")
        events.close()
        source.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
