from typer.testing import CliRunner

from pcmigrate.cli import NOT_IMPLEMENTED_EXIT_CODE, app

COMMANDS = ["introspect", "approve", "run", "status", "resume", "teardown"]
STUBS = ["run", "status", "resume"]

runner = CliRunner()


def test_help_lists_every_command() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in COMMANDS:
        assert command in result.output


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.output.startswith("pcmigrate ")


def test_each_stub_reports_not_implemented() -> None:
    for command in STUBS:
        result = runner.invoke(app, [command])
        assert result.exit_code == NOT_IMPLEMENTED_EXIT_CODE, command
        assert "not implemented" in result.output
