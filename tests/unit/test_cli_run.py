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
        Stage.REPLICATE,
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


def test_status_stage_prints_the_full_record(s3: S3Client, tmp_path: Path) -> None:
    store = ManifestStore(s3, BUCKET, "mig")
    manifest = Manifest(migration_id="mig")
    manifest.mark_running(Stage.EXPORT)
    manifest.mark_failed(Stage.EXPORT, "RuntimeError: " + "x" * 300)
    manifest.stage(Stage.EXPORT).outputs["t0"] = "2026-11-18T21:19:42.317Z"
    store.save(manifest)
    result = runner.invoke(
        app, ["status", "--settings", str(settings_file(tmp_path)), "--stage", "export"]
    )
    assert result.exit_code == 0, result.output
    assert "2026-11-18T21:19:42.317Z" in result.output and "x" * 200 in result.output


def test_context_writes_events_to_the_staging_bucket(
    s3: S3Client, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pcmigrate.cli import _context

    monkeypatch.setenv("PINECONE_API_KEY", "pcsk_test_key")
    monkeypatch.setenv("PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden")
    ctx = _context(settings_file(tmp_path))
    ctx.events.log("hello from the runner", stage=Stage.EXPORT)
    ctx.events.close()
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="mig/manifest/events/").get(
            "Contents", []
        )
    ]
    assert len(keys) == 1, keys
    body = s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read().decode()
    assert "hello from the runner" in body


def test_preflight_command_exits_nonzero_on_a_failed_check(
    s3: S3Client, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pcmigrate.core import preflight
    from pcmigrate.core.preflight import CheckResult

    monkeypatch.setattr(
        preflight,
        "run_preflight",
        lambda settings: [
            CheckResult("staging bucket", True, BUCKET),
            CheckResult("import bucket", False, "AccessDenied: no identity-based policy"),
        ],
    )
    result = runner.invoke(app, ["preflight", "--settings", str(settings_file(tmp_path))])
    assert result.exit_code == 1 and "AccessDenied" in result.output and "FAILED" in result.output
