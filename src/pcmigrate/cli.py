"""Typer application for pcmigrate.

Commands: introspect, approve, run, status, resume, teardown. Phase 0 ships stubs only;
each command reports that it is not implemented and exits non-zero.
"""

from __future__ import annotations

import typer
from rich.console import Console

from pcmigrate import __version__

app = typer.Typer(
    name="pcmigrate",
    help="Migrate pgvector and OpenSearch data into Pinecone.",
    no_args_is_help=True,
    add_completion=False,
)

_console = Console(stderr=True)

NOT_IMPLEMENTED_EXIT_CODE = 2


def _not_implemented(command: str) -> None:
    _console.print(f"[yellow]pcmigrate {command}[/yellow]: not implemented")
    raise typer.Exit(code=NOT_IMPLEMENTED_EXIT_CODE)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pcmigrate {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the version and exit.",
    ),
) -> None:
    """Migrate pgvector and OpenSearch data into Pinecone."""


@app.command()
def introspect() -> None:
    """Read-only discovery of the source. Writes report, mapping.yaml, and plan.lock."""
    _not_implemented("introspect")


@app.command()
def approve() -> None:
    """Record FDE approval of the current plan.lock hash."""
    _not_implemented("approve")


@app.command()
def run() -> None:
    """Run the approved stage pipeline from the next incomplete stage."""
    _not_implemented("run")


@app.command()
def status() -> None:
    """Show stage and partition progress from the S3 manifest."""
    _not_implemented("status")


@app.command()
def resume() -> None:
    """Resume an interrupted run from the last checkpoint."""
    _not_implemented("resume")


@app.command()
def teardown() -> None:
    """Destroy the migration's AWS stack. Never touches the Pinecone index."""
    _not_implemented("teardown")


if __name__ == "__main__":
    app()
