"""Inputs to the base stack. Names and ARNs only, never secret values (ADR-014)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.migration import MigrationConfig

PROJECT_NAME = "pcmigrate"
PINECONE_AWS_ACCOUNT = "713131977538"
PINECONE_DEFAULT_PRINCIPAL = f"arn:aws:iam::{PINECONE_AWS_ACCOUNT}:root"
SOURCE_DSN_SECRET = "source-dsn"
PINECONE_API_KEY_SECRET = "pinecone-api-key"
DEFAULT_SECRETS = (SOURCE_DSN_SECRET, PINECONE_API_KEY_SECRET)


class BaseStackConfig(BaseModel):
    """What the base Pulumi program needs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    region: str
    bucket_name: str = Field(min_length=3, max_length=63)
    adopt_bucket: bool = False
    secret_names: tuple[str, ...] = DEFAULT_SECRETS
    pinecone_principal_arn: str = PINECONE_DEFAULT_PRINCIPAL
    pinecone_external_id: str | None = None
    staged_data_retention_days: int = Field(default=30, ge=1)
    change_capture_enabled: bool = True
    buffer_retention_seconds: int = Field(default=4 * 24 * 3600, ge=60, le=14 * 24 * 3600)
    tags: dict[str, str] = Field(default_factory=dict)

    @property
    def stack_name(self) -> str:
        """One Pulumi stack per migration (ADR-010)."""
        return f"{PROJECT_NAME}-{self.migration_id}"

    @property
    def resource_prefix(self) -> str:
        """Prefix for every named AWS resource."""
        return f"pcmigrate-{self.migration_id}"

    def secret_name(self, short: str) -> str:
        """Full Secrets Manager name for one of ``secret_names``."""
        return f"pcmigrate/{self.migration_id}/{short}"

    @classmethod
    def from_migration(
        cls,
        config: MigrationConfig,
        *,
        change_capture_enabled: bool = True,
        import_eta_s: float | None = None,
    ) -> BaseStackConfig:
        """Derive the stack inputs from the run configuration.

        Buffer retention is sized from the import ETA (ADR-007): twice the ETA plus a day,
        clamped to SQS's 14-day maximum, and never below the 4-day default.
        """
        retention = 4 * 24 * 3600
        if import_eta_s:
            retention = int(min(14 * 24 * 3600, max(retention, 2 * import_eta_s + 24 * 3600)))
        return cls(
            migration_id=config.migration_id,
            region=config.aws_region,
            bucket_name=config.bucket,
            adopt_bucket=config.bucket_mode == "adopt",
            pinecone_principal_arn=config.pinecone_storage_principal_arn
            or PINECONE_DEFAULT_PRINCIPAL,
            pinecone_external_id=config.pinecone_storage_external_id,
            change_capture_enabled=change_capture_enabled,
            buffer_retention_seconds=retention,
            tags={"pcmigrate:migration_id": config.migration_id},
        )
