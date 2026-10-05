"""Inputs to the OpenSearch add-on stack. Names and ARNs only, never secret values (ADR-014)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class OpenSearchStackConfig(BaseModel):
    """An OSIS pipeline, a Glue job, their roles, and the domain access mapping (ADR-009).

    ``domain_arn`` set means an Amazon OpenSearch Service domain reached with IAM; unset means
    a self-managed cluster reached through the VPC with basic auth from
    ``credentials_secret_arn``. ``pipeline_body`` is the YAML from
    ``sources/opensearch/osis_config.py`` with the role placeholder still in it.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    region: str
    bucket_name: str
    kms_key_arn: str | None = None
    pipeline_name: str = Field(pattern=r"^[a-z][a-z0-9-]{2,27}$")
    pipeline_body: str
    indices: tuple[str, ...]
    domain_arn: str | None = None
    domain_name: str | None = None
    manage_domain_policy: bool = False
    extra_domain_principals: tuple[str, ...] = ()
    credentials_secret_arn: str | None = None
    subnet_ids: tuple[str, ...] = ()
    security_group_ids: tuple[str, ...] = ()
    osis_min_units: int = Field(default=1, ge=1, le=96)
    osis_max_units: int = Field(default=4, ge=1, le=96)
    glue_script_uri: str
    glue_python_modules: str
    glue_installer_options: str = "--no-deps"
    glue_version: str = "5.0"
    glue_worker_type: Literal["G.1X", "G.2X", "G.4X", "G.8X"] = "G.1X"
    glue_workers: int = Field(default=4, ge=2, le=299)
    glue_timeout_minutes: int = Field(default=12 * 60, ge=1)
    tags: dict[str, str] = Field(default_factory=dict)

    @property
    def resource_prefix(self) -> str:
        """Prefix for every named resource."""
        return f"pcmigrate-{self.migration_id}"

    @property
    def stack_name(self) -> str:
        """One add-on stack per migration."""
        return f"pcmigrate-{self.migration_id}-opensearch"

    @property
    def self_managed(self) -> bool:
        """True when there is no AWS domain to grant access on."""
        return self.domain_arn is None

    @property
    def glue_job_name(self) -> str:
        """The Glue job the export stage starts."""
        return f"{self.resource_prefix}-opensearch-transform"

    @property
    def osis_log_group(self) -> str:
        """OSIS only publishes to log groups under /aws/vendedlogs/."""
        return f"/aws/vendedlogs/OpenSearchIngestion/{self.pipeline_name}/audit-logs"
