import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.provision import REQUIRED_OUTPUTS, ProvisionStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.conftest import BUCKET, REGION, approve_ctx

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


class FakeDriver:
    def __init__(self, outputs: dict[str, Any]) -> None:
        self.outputs = outputs
        self.ups = 0

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        self.ups += 1
        on_output("Updating (pcmigrate-mig)\n")
        on_output("    + aws:s3:Bucket bucket created\n")
        return dict(self.outputs)

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        pass


GOOD = {
    "bucket_name": BUCKET,
    "bucket_arn": f"arn:aws:s3:::{BUCKET}",
    "kms_key_arn": "arn:aws:kms:us-east-1:1:key/k",
    "runner_role_arn": "arn:aws:iam::1:role/r",
    "secret_arns": {"source-dsn": "arn:aws:secretsmanager:us-east-1:1:secret:a"},
    "storage_integration_role_arn": "arn:aws:iam::1:role/p",
}


@pytest.fixture
def ctx(s3: S3Client, tmp_path: Path) -> tuple[RunContext, io.StringIO]:
    out = io.StringIO()
    cfg = MigrationConfig(
        migration_id="mig", bucket=BUCKET, aws_region=REGION, pinecone_api_key_secret=ARN
    )
    context = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(EXAMPLE),
        manifest=Manifest(migration_id="mig"),
        store=ManifestStore(s3, BUCKET, "mig"),
        events=EventBus("mig", [JsonLinesSink(out)]),
        s3=s3,
    )
    approve_ctx(context, tmp_path)
    return context, out


def test_provision_records_outputs_and_streams_logs(
    ctx: tuple[RunContext, io.StringIO], s3: S3Client
) -> None:
    context, out = ctx
    driver = FakeDriver(GOOD)
    stage = ProvisionStage(driver)
    Pipeline([stage]).run(context, S3Lock(s3, BUCKET, "mig"))
    record = context.manifest.stage(Stage.PROVISION)
    assert all(k in record.outputs for k in REQUIRED_OUTPUTS)
    assert stage.is_complete(context.manifest)
    logs = [json.loads(line) for line in out.getvalue().splitlines() if '"log"' in line]
    assert any("Bucket" in entry["message"] for entry in logs)
    Pipeline([stage]).run(context, S3Lock(s3, BUCKET, "mig"))
    assert driver.ups == 1


def test_provision_fails_on_missing_outputs(
    ctx: tuple[RunContext, io.StringIO], s3: S3Client
) -> None:
    context, _ = ctx
    stage = ProvisionStage(FakeDriver({"bucket_name": BUCKET}))
    with pytest.raises(PipelineError, match="without outputs"):
        Pipeline([stage]).run(context, S3Lock(s3, BUCKET, "mig"))


def test_provision_fails_on_bucket_mismatch(
    ctx: tuple[RunContext, io.StringIO], s3: S3Client
) -> None:
    context, _ = ctx
    stage = ProvisionStage(FakeDriver({**GOOD, "bucket_name": "someone-elses-bucket"}))
    with pytest.raises(PipelineError, match="differs from config"):
        Pipeline([stage]).run(context, S3Lock(s3, BUCKET, "mig"))
