"""Typer application for pcmigrate.

Phase 2 implements `introspect` and `approve`, which run from a laptop against a local
artifact directory or an S3 manifest prefix. The remaining commands are stubs.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Any, Literal

import typer
from rich.console import Console
from rich.table import Table

from pcmigrate import __version__
from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.core.approval import PLAN_LOCK_NAME, record_approval
from pcmigrate.core.artifacts import ArtifactStore, LocalArtifactStore, S3ArtifactStore
from pcmigrate.report.findings import Severity, blockers

app = typer.Typer(
    name="pcmigrate",
    help="Migrate pgvector and OpenSearch data into Pinecone.",
    no_args_is_help=True,
    add_completion=False,
)

_console = Console(stderr=True)

NOT_IMPLEMENTED_EXIT_CODE = 2
BLOCKERS_EXIT_CODE = 3
APPROVAL_EXIT_CODE = 4


def _not_implemented(command: str) -> None:
    _console.print(f"[yellow]pcmigrate {command}[/yellow]: not implemented")
    raise typer.Exit(code=NOT_IMPLEMENTED_EXIT_CODE)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pcmigrate {__version__}")
        raise typer.Exit()


def _store(out: Path | None, manifest_uri: str | None) -> ArtifactStore:
    if manifest_uri:
        from pcmigrate.core.s3 import S3Location, make_client

        location = S3Location.parse(manifest_uri)
        region = os.environ.get("AWS_REGION", "us-east-1")
        return S3ArtifactStore(make_client(region), location.bucket, location.key)
    return LocalArtifactStore(out or Path("pcmigrate-out"))


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
def introspect(
    migration_id: Annotated[str, typer.Option(help="Slug for this migration run.")],
    index: Annotated[str, typer.Option(help="Target Pinecone index name.")],
    region: Annotated[str, typer.Option(help="AWS region of the target index.")] = "us-east-1",
    dsn_env: Annotated[
        str,
        typer.Option(
            help="Environment variable holding the source DSN (never put the DSN in config)."
        ),
    ] = "PGVECTOR_DSN",
    dsn_secret: Annotated[
        str | None,
        typer.Option(help="Secrets Manager ARN to record in mapping.yaml as source.dsn_secret."),
    ] = None,
    out: Annotated[
        Path, typer.Option(help="Directory for report, mapping.yaml, plan.lock.")
    ] = Path("pcmigrate-out"),
    manifest: Annotated[
        str | None,
        typer.Option(help="s3://bucket/<migration_id> to write artifacts there instead."),
    ] = None,
    schema: Annotated[list[str], typer.Option(help="Schemas to inventory.")] = ["public"],  # noqa: B006
    table: Annotated[
        list[str] | None, typer.Option(help="Restrict to these tables (name or schema.name).")
    ] = None,
    window: Annotated[
        float, typer.Option(help="Seconds to sample workload counters over (0 skips).")
    ] = 300.0,
    full_scan: Annotated[
        bool,
        typer.Option(
            "--full-scan", help="Exact duplicate counts and row counts, whatever the size."
        ),
    ] = False,
    roadmap: Annotated[
        str,
        typer.Option(
            help="Customer roadmap: dense, hybrid, or fts. Drives the index-type recommendation."
        ),
    ] = "dense",
    workers: Annotated[int, typer.Option(help="Export workers assumed for the ETA.")] = 4,
    sample_rows: Annotated[
        int, typer.Option(help="Rows sampled per table for dimension and duplicate checks.")
    ] = 1000,
) -> None:
    """Read-only discovery of the source. Writes report, mapping.yaml, and plan.lock."""
    from pcmigrate.report.build import build_report
    from pcmigrate.report.mapping_gen import MappingOptions
    from pcmigrate.sources.pgvector.introspect import IntrospectOptions, PgvectorIntrospector

    dsn = os.environ.get(dsn_env)
    if not dsn:
        _console.print(f"[red]environment variable {dsn_env} is not set[/red]")
        raise typer.Exit(code=1)
    if roadmap not in ("dense", "hybrid", "fts"):
        _console.print("[red]--roadmap must be dense, hybrid, or fts[/red]")
        raise typer.Exit(code=1)
    roadmap_value: Literal["dense", "hybrid", "fts"] = roadmap  # type: ignore[assignment]

    introspector = PgvectorIntrospector(
        dsn,
        IntrospectOptions(
            schemas=tuple(schema),
            tables=tuple(table) if table else None,
            sample_rows=sample_rows,
            workload_window_s=window,
            full_scan=full_scan,
        ),
    )
    with _console.status(f"introspecting (workload window {window:.0f}s)..."):
        inventory = introspector.inventory()
    introspector.close()
    bundle = build_report(
        inventory,
        MappingOptions(
            migration_id=migration_id,
            index=index,
            region=region,
            dsn_secret=dsn_secret,
            roadmap=roadmap_value,
        ),
        workers=workers,
    )
    written = bundle.write(_store(out, manifest))

    summary = Table(title=f"pcmigrate introspect: {migration_id}")
    summary.add_column("severity")
    summary.add_column("count", justify="right")
    for severity in Severity:
        summary.add_row(
            severity.value, str(sum(1 for f in bundle.findings if f.severity is severity))
        )
    _console.print(summary)
    for name, where in written.items():
        _console.print(f"  {name:<16} {where}")
    _console.print(f"plan hash {bundle.plan_lock.plan_hash}")
    if blockers(bundle.findings):
        _console.print("[red]blockers open; approval is not possible until they are resolved[/red]")
        raise typer.Exit(code=BLOCKERS_EXIT_CODE)


@app.command()
def approve(
    approver: Annotated[str, typer.Option(help="Email of the FDE approving the plan.")],
    out: Annotated[
        Path, typer.Option(help="Directory holding plan.lock and findings.json.")
    ] = Path("pcmigrate-out"),
    manifest: Annotated[
        str | None, typer.Option(help="s3://bucket/<migration_id> when artifacts live in S3.")
    ] = None,
) -> None:
    """Record FDE approval of the current plan.lock hash."""
    import json

    from pcmigrate.report.build import FINDINGS_NAME
    from pcmigrate.report.findings import Finding

    store = _store(out, manifest)
    lock_text = store.read_text(PLAN_LOCK_NAME)
    if lock_text is None:
        where = store.location(PLAN_LOCK_NAME)
        _console.print(f"[red]no {PLAN_LOCK_NAME} at {where}; run introspect first[/red]")
        raise typer.Exit(code=1)
    lock = PlanLock.from_text(lock_text)
    findings_text = store.read_text(FINDINGS_NAME)
    findings = (
        [Finding.model_validate(f) for f in json.loads(findings_text)] if findings_text else []
    )
    open_blockers = blockers(findings)
    if open_blockers:
        for finding in open_blockers:
            _console.print(
                f"[red]blocker[/red] {finding.object} {finding.field or ''}: {finding.message}"
            )
        raise typer.Exit(code=BLOCKERS_EXIT_CODE)
    approval = record_approval(store, lock, approver)
    _console.print(
        f"approved plan {approval.plan_hash[:12]} by {approval.approver} at "
        f"{approval.approved_at.isoformat(timespec='seconds')}"
    )


def _context(settings_path: Path) -> Any:
    """Build a RunContext for cutover and teardown from pcmigrate.yaml and the environment."""
    from pcmigrate.config.plan_lock import PlanLock
    from pcmigrate.config.settings import RunSettings
    from pcmigrate.core.events import EventBus, JsonLinesSink, RichLiveSink
    from pcmigrate.core.s3 import make_client
    from pcmigrate.core.stages import RunContext
    from pcmigrate.core.state import Manifest, ManifestStore
    from pcmigrate.sources.pgvector.export import PgvectorSource
    from pcmigrate.targets import SdkGateway, api_key_from_secret

    settings = RunSettings.load(settings_path)
    config = settings.migration
    mapping = settings.mapping()
    s3 = make_client(config.aws_region)
    store = ManifestStore(s3, config.bucket, config.migration_id)
    manifest = store.load() or Manifest(migration_id=config.migration_id)
    api_key = (
        os.environ[settings.pinecone_api_key_env]
        if settings.pinecone_api_key_env and os.environ.get(settings.pinecone_api_key_env)
        else api_key_from_secret(config.pinecone_api_key_secret, config.aws_region)
    )
    artifacts = S3ArtifactStore(s3, config.bucket, config.migration_id)
    lock_text = artifacts.read_text(PLAN_LOCK_NAME) or LocalArtifactStore(
        settings.artifacts_dir
    ).read_text(PLAN_LOCK_NAME)
    plan_lock = PlanLock.from_text(lock_text) if lock_text else None
    return RunContext(
        config=config,
        mapping=mapping,
        manifest=manifest,
        store=store,
        events=EventBus(config.migration_id, [JsonLinesSink(), RichLiveSink()]),
        s3=s3,
        source=PgvectorSource(
            settings.dsn(), mapping, target_rows_per_partition=config.partition_rows
        ),
        target=SdkGateway(api_key),
        plan_lock=plan_lock,
        approvals=artifacts,
    )


@app.command()
def cutover(
    settings: Annotated[
        Path, typer.Option("--settings", help="pcmigrate.yaml for this migration.")
    ] = Path("pcmigrate.yaml"),
    shadow: Annotated[
        int | None, typer.Option(help="Run N sampled shadow queries on both systems and report.")
    ] = None,
    confirm: Annotated[
        bool,
        typer.Option(
            "--confirm", help="Drain the buffer, stop change capture, and run the final reconcile."
        ),
    ] = False,
) -> None:
    """Shadow-read comparison, or the final convergence with --confirm."""
    from datetime import timedelta

    from pcmigrate.core.stages import Pipeline
    from pcmigrate.core.stages.cutover import CutoverStage, run_shadow
    from pcmigrate.core.stages.sync import request_stop
    from pcmigrate.core.state import S3Lock

    if shadow is None and not confirm:
        _console.print("[red]pass --shadow N, --confirm, or both[/red]")
        raise typer.Exit(code=1)
    ctx = _context(settings)
    try:
        if shadow is not None:
            results = run_shadow(ctx, queries=shadow)
            table = Table(title="shadow reads")
            for column in (
                "namespace",
                "queries",
                "mean_overlap",
                "source_p50_ms",
                "target_p50_ms",
                "source_p95_ms",
                "target_p95_ms",
            ):
                table.add_column(column)
            for ns, r in results.items():
                table.add_row(
                    ns,
                    *[
                        str(r.get(c))
                        for c in (
                            "queries",
                            "mean_overlap",
                            "source_p50_ms",
                            "target_p50_ms",
                            "source_p95_ms",
                            "target_p95_ms",
                        )
                    ],
                )
            _console.print(table)
        if confirm:
            request_stop(ctx)
            lock = S3Lock(
                ctx.s3,
                ctx.config.bucket,
                ctx.config.migration_id,
                ttl=timedelta(seconds=ctx.config.lock_ttl_seconds),
            )
            Pipeline([CutoverStage()]).run(ctx, lock)
            _console.print(
                "[green]cutover complete: change capture stopped and reconcile converged[/green]"
            )
    finally:
        ctx.events.close()


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
def teardown(
    settings: Annotated[
        Path, typer.Option("--settings", help="pcmigrate.yaml for this migration.")
    ] = Path("pcmigrate.yaml"),
    retain_data: Annotated[
        bool, typer.Option("--retain-data", help="Keep staged data in S3.")
    ] = False,
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation prompt.")] = False,
) -> None:
    """Destroy the migration's AWS stack. Never touches the Pinecone index."""
    from datetime import timedelta

    from pcmigrate.core.stages import Pipeline
    from pcmigrate.core.stages.teardown import TeardownStage
    from pcmigrate.core.state import S3Lock

    ctx = _context(settings)
    if not yes and not typer.confirm(
        f"Destroy stack pcmigrate-{ctx.config.migration_id} and "
        + ("keep" if retain_data else "delete")
        + " staged data? The Pinecone index is never touched."
    ):
        raise typer.Exit(code=1)
    try:
        lock = S3Lock(
            ctx.s3,
            ctx.config.bucket,
            ctx.config.migration_id,
            ttl=timedelta(seconds=ctx.config.lock_ttl_seconds),
        )
        Pipeline([TeardownStage(retain_data=retain_data)]).run(ctx, lock)
        _console.print("[green]teardown complete[/green]")
    finally:
        ctx.events.close()


if __name__ == "__main__":
    app()
