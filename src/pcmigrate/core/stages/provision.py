"""Stage `provision`: drive the base Pulumi stack through the Automation API (ADR-010)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from pcmigrate.config.aws import default_tags_config
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


class AddonStack(Protocol):
    """A per-source stack brought up after the base stack and destroyed before it."""

    def up(
        self, ctx: RunContext, base_outputs: dict[str, Any], on_output: Callable[[str], None]
    ) -> dict[str, Any]:
        """Create or update the add-on and return its outputs."""
        ...

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        """Destroy the add-on."""
        ...


class PulumiDriver:
    """Automation API driver for infra/base plus add-ons. Imports Pulumi lazily."""

    def __init__(self, *, work_dir: str | None = None, addons: Sequence[AddonStack] = ()) -> None:
        self._work_dir = work_dir
        self._addons = tuple(addons)

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
        stack.set_config(
            "aws:defaultTags",
            auto.ConfigValue(value=default_tags_config(ctx.config.resource_tags)),
        )
        return stack

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        """See StackDriver."""
        stack = self._stack(ctx)
        result = stack.up(on_output=on_output, refresh=True)
        outputs = {name: value.value for name, value in result.outputs.items()}
        for addon in self._addons:
            outputs.update(addon.up(ctx, dict(outputs), on_output))
        return outputs

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        """See StackDriver."""
        for addon in reversed(self._addons):
            addon.destroy(ctx, on_output)
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
        if (
            ctx.mapping.source.kind == "opensearch"
            and ctx.extras.get("export_mode", "osis_glue") == "osis_glue"
        ):
            for key in ("osis_pipeline_name", "glue_job_name"):
                if key not in outputs:
                    raise RuntimeError(f"OpenSearch source but the stack produced no {key}")
        return dict(outputs)
