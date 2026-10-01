import json
from pathlib import Path

from typer.testing import CliRunner

from pcmigrate.cli import APPROVAL_EXIT_CODE, BLOCKERS_EXIT_CODE, app
from pcmigrate.core.approval import load_approvals
from pcmigrate.core.artifacts import LocalArtifactStore
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from tests.unit.report.fixtures import inventory, tenants_table

runner = CliRunner()
OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def test_approve_records_when_no_blockers(tmp_path: Path) -> None:
    build_report(inventory(), OPTIONS).write(LocalArtifactStore(tmp_path))
    result = runner.invoke(
        app, ["approve", "--approver", "fde@example.com", "--out", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    approvals = load_approvals(LocalArtifactStore(tmp_path)).approvals
    assert [a.approver for a in approvals] == ["fde@example.com"]


def test_approve_refuses_with_blockers(tmp_path: Path) -> None:
    hidden = tenants_table().model_copy(update={"rls_hides_rows": True, "exact_rows": 0})
    build_report(inventory(hidden), OPTIONS).write(LocalArtifactStore(tmp_path))
    result = runner.invoke(
        app, ["approve", "--approver", "fde@example.com", "--out", str(tmp_path)]
    )
    assert result.exit_code == BLOCKERS_EXIT_CODE
    assert not (tmp_path / "manifest" / "approvals.json").exists()


def test_approve_without_plan_lock(tmp_path: Path) -> None:
    result = runner.invoke(app, ["approve", "--approver", "x@example.com", "--out", str(tmp_path)])
    assert result.exit_code == 1


def test_introspect_requires_dsn_env(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("PGVECTOR_DSN", raising=False)
    result = runner.invoke(
        app, ["introspect", "--migration-id", "m", "--index", "i", "--out", str(tmp_path)]
    )
    assert result.exit_code == 1


def test_exit_codes_are_distinct() -> None:
    assert len({BLOCKERS_EXIT_CODE, APPROVAL_EXIT_CODE, 1, 2}) == 4
    assert json.dumps({"ok": True})
