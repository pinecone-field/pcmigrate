"""The OpenSearch add-on for the provision stage: stage Glue assets, bring up the stack, map access.

Runs after the base stack: it packages pcmigrate as a wheel, uploads it with the Glue script
under ``manifest/glue/``, renders the armed OSIS pipeline body, brings up
``infra/opensearch`` through the Automation API, and for fine-grained-access-control domains
maps the pipeline role into the security plugin. Its outputs join the base outputs in the
manifest, which is where the export stage finds the pipeline and job names.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pcmigrate.config.settings import OpenSearchSettings
from pcmigrate.core.stages import RunContext
from pcmigrate.sources.opensearch.client import ConnectionOptions, OpenSearchClient
from pcmigrate.sources.opensearch.glue_package import (
    INSTALLER_OPTIONS,
    GlueAssets,
    build_wheel,
    upload_assets,
)
from pcmigrate.sources.opensearch.osis_config import OsisPipelineSpec, pipeline_body, pipeline_name

PROJECT_NAME = "pcmigrate-opensearch"


def stage_glue_assets(ctx: RunContext, settings: OpenSearchSettings) -> GlueAssets:
    """Build (or take) the wheel and upload it with the Glue script."""
    if settings.wheel_path is not None:
        wheel = Path(settings.wheel_path)
    else:
        wheel = build_wheel(Path(tempfile.mkdtemp(prefix="pcmigrate-wheel-")))
    return upload_assets(ctx.s3, ctx.config.bucket, ctx.config.zone("manifest"), wheel=wheel)


def stack_config(
    ctx: RunContext,
    settings: OpenSearchSettings,
    *,
    endpoint: str,
    base_outputs: dict[str, Any],
    assets: GlueAssets,
) -> Any:
    """Derive the add-on stack inputs from the run, the base outputs, and the staged assets."""
    from infra.opensearch import OpenSearchStackConfig

    indices = tuple(o.source for o in ctx.mapping.objects if o.dense is not None)
    spec = OsisPipelineSpec(
        migration_id=ctx.config.migration_id,
        endpoint=endpoint,
        indices=indices,
        region=ctx.config.aws_region,
        bucket=ctx.config.bucket,
        raw_prefix=ctx.config.zone("raw"),
        serverless=endpoint.endswith(".aoss.amazonaws.com") or ".aoss." in endpoint,
        credentials_secret_arn=settings.credentials_secret_arn
        if settings.domain_arn is None
        else None,
    )
    return OpenSearchStackConfig(
        migration_id=ctx.config.migration_id,
        region=ctx.config.aws_region,
        bucket_name=ctx.config.bucket,
        kms_key_arn=base_outputs.get("kms_key_arn") or ctx.config.kms_key_arn,
        pipeline_name=pipeline_name(ctx.config.migration_id),
        pipeline_body=pipeline_body(spec),
        indices=indices,
        domain_arn=settings.domain_arn,
        domain_name=settings.domain_name,
        manage_domain_policy=settings.manage_domain_policy,
        extra_domain_principals=tuple(
            p for p in (base_outputs.get("runner_role_arn"),) if isinstance(p, str) and p
        ),
        credentials_secret_arn=settings.credentials_secret_arn,
        subnet_ids=tuple(settings.subnet_ids),
        security_group_ids=tuple(settings.security_group_ids),
        osis_min_units=settings.osis_min_units,
        osis_max_units=settings.osis_max_units,
        glue_script_uri=assets.script_uri,
        glue_python_modules=assets.python_modules,
        glue_installer_options=INSTALLER_OPTIONS,
        glue_worker_type=settings.glue_worker_type,
        glue_workers=settings.glue_workers,
        glue_timeout_minutes=settings.glue_timeout_minutes,
        tags={"pcmigrate:migration_id": ctx.config.migration_id},
    )


class OpenSearchAddon:
    """AddonStack for the provision driver."""

    def __init__(
        self,
        settings: OpenSearchSettings,
        *,
        endpoint: str,
        basic_auth: tuple[str, str] | None = None,
        work_dir: str | None = None,
        assets: GlueAssets | None = None,
    ) -> None:
        self._settings = settings
        self._endpoint = endpoint
        self._basic_auth = basic_auth
        self._work_dir = work_dir
        self._assets = assets

    @property
    def settings(self) -> OpenSearchSettings:
        """The settings block this add-on was built from."""
        return self._settings

    def _stack(self, ctx: RunContext, base_outputs: dict[str, Any]) -> Any:
        import pulumi.automation as auto
        from infra.opensearch import program_for

        assets = self._assets or stage_glue_assets(ctx, self._settings)
        cfg = stack_config(
            ctx, self._settings, endpoint=self._endpoint, base_outputs=base_outputs, assets=assets
        )
        stack = auto.create_or_select_stack(
            stack_name=cfg.stack_name,
            project_name=PROJECT_NAME,
            program=program_for(cfg),
            opts=auto.LocalWorkspaceOptions(
                work_dir=self._work_dir,
                project_settings=auto.ProjectSettings(
                    name=PROJECT_NAME, runtime="python", description="pcmigrate OpenSearch add-on"
                ),
            ),
        )
        stack.set_config("aws:region", auto.ConfigValue(value=cfg.region))
        return stack

    def up(
        self, ctx: RunContext, base_outputs: dict[str, Any], on_output: Callable[[str], None]
    ) -> dict[str, Any]:
        """Bring the add-on up and map the pipeline role; returns its outputs."""
        stack = self._stack(ctx, base_outputs)
        result = stack.up(on_output=on_output, refresh=True)
        outputs = {name: value.value for name, value in result.outputs.items()}
        if self._settings.fine_grained_access and "osis_pipeline_role_arn" in outputs:
            from pcmigrate.sources.opensearch.access import map_backend_role

            client = OpenSearchClient(
                ConnectionOptions(
                    self._endpoint,
                    auth=self._settings.auth,
                    basic_auth=self._basic_auth,
                    region=ctx.config.aws_region,
                    verify_certs=not self._settings.insecure_tls,
                ),
                read_only=False,
            )
            try:
                outputs["domain_role_mapping"] = map_backend_role(
                    client, str(outputs["osis_pipeline_role_arn"])
                )
            finally:
                client.close()
        return outputs

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        """Destroy the add-on stack."""
        from pcmigrate.sources.opensearch.glue_package import GlueAssets

        placeholder = GlueAssets(
            script_uri="s3://unused/unused.py", wheel_uri="s3://unused/unused.whl"
        )
        self._assets = self._assets or placeholder
        stack = self._stack(ctx, {})
        stack.destroy(on_output=on_output, remove=True)
