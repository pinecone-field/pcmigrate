import io
from pathlib import Path

import pytest
import yaml
from mypy_boto3_s3 import S3Client
from typer.testing import CliRunner

from pcmigrate.cli import _load_stages, _record_gate_stages, app
from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.config.settings import RunSettings
from pcmigrate.core.approval import ApprovalRequiredError
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, ManifestStore, Stage
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.conftest import BUCKET, REGION, approve_ctx

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
runner = CliRunner()


def settings_file(tmp_path: Path, **extra: object) -> Path:
    (tmp_path / "pcmigrate-out").mkdir(exist_ok=True)
    (tmp_path / "pcmigrate-out" / "mapping.yaml").write_text(EXAMPLE)
    data = {
        "migration": {
            "migration_id": "mig",
            "bucket": BUCKET,
            "aws_region": REGION,
            "pinecone_api_key_secret": ARN,
        },
        "mapping_path": "pcmigrate-out/mapping.yaml",
        "artifacts_dir": "pcmigrate-out",
        "pinecone_api_key_env": "PINECONE_API_KEY",
        **extra,
    }
    path = tmp_path / "pcmigrate.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_settings_resolve_relative_paths(tmp_path: Path) -> None:
    settings = RunSettings.load(settings_file(tmp_path))
    assert settings.mapping_path == tmp_path / "pcmigrate-out" / "mapping.yaml"
    assert settings.artifacts_dir == tmp_path / "pcmigrate-out"
    assert settings.mapping().migration_id == "acme-docs-2026-10"
    with pytest.raises(RuntimeError, match="PGVECTOR_DSN"):
        settings.dsn()


def _ctx(s3: S3Client, tmp_path: Path) -> RunContext:
    cfg = MigrationConfig(
        migration_id="mig", bucket=BUCKET, aws_region=REGION, pinecone_api_key_secret=ARN
    )
    ctx = RunContext(
        config=cfg,
        mapping=Mapping.from_yaml(EXAMPLE),
        manifest=Manifest(migration_id="mig"),
        store=ManifestStore(s3, BUCKET, "mig"),
        events=EventBus("mig", [JsonLinesSink(io.StringIO())]),
        s3=s3,
    )
    return ctx


def test_load_stages_respects_until_and_skip_provision(s3: S3Client, tmp_path: Path) -> None:
    ctx = _ctx(s3, tmp_path)
    names = [
        s.name for s in _load_stages(ctx, skip_provision=True, until="validate", sync_rounds=None)
    ]
    assert names == [
        Stage.PREPARE_INDEX,
        Stage.EXPORT,
        Stage.TRANSFORM,
        Stage.EMIT,
        Stage.IMPORT,
        Stage.VALIDATE,
    ]
    full = [s.name for s in _load_stages(ctx, skip_provision=False, until=None, sync_rounds=3)]
    assert full[0] is Stage.PROVISION and full[-1] is Stage.SYNC


def test_record_gate_stages_marks_introspect_approve_and_skipped_provision(
    s3: S3Client, tmp_path: Path
) -> None:
    ctx = _ctx(s3, tmp_path)
    approve_ctx(ctx, tmp_path)
    _record_gate_stages(ctx, skip_provision=True)
    m = ctx.manifest
    assert (
        m.is_complete(Stage.INTROSPECT)
        and m.is_complete(Stage.APPROVE)
        and m.is_complete(Stage.PROVISION)
    )
    assert m.stage(Stage.APPROVE).outputs["approver"] == "tests@example.com"
    assert m.plan_hash == ctx.plan_lock.plan_hash  # type: ignore[union-attr]
    assert m.stage(Stage.PROVISION).outputs["skipped"] is True
    stored = ManifestStore(s3, BUCKET, "mig").load()
    assert stored is not None and stored.is_complete(Stage.APPROVE)


def test_record_gate_stages_requires_approval(s3: S3Client, tmp_path: Path) -> None:
    from pcmigrate.config.plan_lock import PlanLock, schema_fingerprint
    from pcmigrate.core.artifacts import LocalArtifactStore

    ctx = _ctx(s3, tmp_path)
    ctx.plan_lock = PlanLock.compute(ctx.mapping, schema_fingerprint({"x": 1}))
    ctx.approvals = LocalArtifactStore(tmp_path / "empty")
    with pytest.raises(ApprovalRequiredError):
        _record_gate_stages(ctx, skip_provision=False)
    assert not ctx.manifest.is_complete(Stage.APPROVE)


def test_status_reports_missing_manifest(s3: S3Client, tmp_path: Path) -> None:
    result = runner.invoke(app, ["status", "--settings", str(settings_file(tmp_path))])
    assert result.exit_code == 1 and "no manifest" in result.output


def test_status_renders_manifest(s3: S3Client, tmp_path: Path) -> None:
    store = ManifestStore(s3, BUCKET, "mig")
    manifest = Manifest(migration_id="mig")
    manifest.mark_complete(Stage.PREPARE_INDEX, index="acme-docs", host="h")
    manifest.mark_running(Stage.EXPORT)
    manifest.mark_failed(Stage.EXPORT, "RuntimeError: boom")
    store.save(manifest)
    result = runner.invoke(app, ["status", "--settings", str(settings_file(tmp_path))])
    assert result.exit_code == 0, result.output
    assert "prepare_index" in result.output and "boom" in result.output
    assert "next stage: introspect" in result.output


def test_cutover_requires_a_mode(tmp_path: Path) -> None:
    result = runner.invoke(app, ["cutover", "--settings", str(settings_file(tmp_path))])
    assert result.exit_code == 1 and "--shadow" in result.output
