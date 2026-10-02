"""Stage `provision`: drive the base Pulumi stack through the Automation API (ADR-010)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage

REQUIRED_OUTPUTS = ("bucket_name", "kms_key_arn", "runner_role_arn", "secret_arns")


class StackDriver(Protocol):
    """Brings a stack up or down. The Pulumi driver is the real one; tests use a fake."""

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        """Create or update the stack and return its outputs."""
        ...

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        """Destroy the stack."""
        ...


class PulumiDriver:
    """Automation API driver for infra/base. Imports Pulumi lazily so unit tests stay light."""

    def __init__(self, *, work_dir: str | None = None) -> None:
        self._work_dir = work_dir

    def _stack(self, ctx: RunContext) -> Any:
        import pulumi.automation as auto
        from infra.base import PROJECT_NAME, BaseStackConfig, program_for

        cfg = BaseStackConfig.from_migration(
            ctx.config,
            change_capture_enabled=ctx.mapping.change_capture_enabled,
            import_eta_s=ctx.extras.get("import_eta_s"),
        )
        stack = auto.create_or_select_stack(
            stack_name=cfg.stack_name,
            project_name=PROJECT_NAME,
            program=program_for(cfg),
            opts=auto.LocalWorkspaceOptions(
                work_dir=self._work_dir,
                project_settings=auto.ProjectSettings(
                    name=PROJECT_NAME, runtime="python", description="pcmigrate base stack"
                ),
            ),
        )
        stack.set_config("aws:region", auto.ConfigValue(value=cfg.region))
        return stack

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        """See StackDriver."""
        stack = self._stack(ctx)
        result = stack.up(on_output=on_output, refresh=True)
        return {name: value.value for name, value in result.outputs.items()}

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        """See StackDriver."""
        stack = self._stack(ctx)
        stack.destroy(on_output=on_output, remove=True)


class ProvisionStage:
    """Idempotent by construction: Pulumi up on an existing stack is a no-op diff."""

    name = Stage.PROVISION

    def __init__(self, driver: StackDriver | None = None) -> None:
        self._driver: StackDriver = driver or PulumiDriver()

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once every required output is recorded."""
        outputs = manifest.stage(self.name).outputs
        return manifest.is_complete(self.name) and all(k in outputs for k in REQUIRED_OUTPUTS)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Bring the base stack up and record its outputs."""

        def on_output(line: str) -> None:
            text = line.rstrip()
            if text:
                ctx.events.log(text, stage=self.name)

        outputs = self._driver.up(ctx, on_output)
        missing = [k for k in REQUIRED_OUTPUTS if k not in outputs]
        if missing:
            raise RuntimeError(f"stack came up without outputs {missing}")
        if outputs["bucket_name"] != ctx.config.bucket:
            raise RuntimeError(
                f"stack bucket {outputs['bucket_name']!r} differs from config {ctx.config.bucket!r}"
            )
        if ctx.mapping.change_capture_enabled and "buffer_queue_url" not in outputs:
            raise RuntimeError("change capture is on but the stack produced no buffer_queue_url")
        if not ctx.mapping.change_capture_enabled and "buffer_queue_url" in outputs:
            raise RuntimeError("change capture is none but the stack provisioned a buffer")
        return dict(outputs)
