"""MigrationConfig: top-level run configuration. Holds secret ARNs, never secrets (ADR-014)."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pcmigrate.config._secrets import reject_secret_values, require_secret_arn

MiB = 1024 * 1024


class MigrationConfig(BaseModel):
    """Everything a run needs besides the mapping: where to stage, which secrets, how wide."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    bucket: str = Field(min_length=3, max_length=63)
    bucket_mode: Literal["create", "adopt"] = "create"
    aws_region: str
    pinecone_api_key_secret: str
    kms_key_arn: str | None = None
    storage_integration_id: str | None = None
    pinecone_storage_principal_arn: str | None = None
    pinecone_storage_external_id: str | None = None
    workers: int = Field(default=4, ge=1, le=64)
    partition_rows: int = Field(default=2_000_000, ge=1000)
    canonical_part_bytes: int = Field(default=256 * MiB, ge=8 * MiB)
    import_part_bytes: int = Field(default=768 * MiB, ge=8 * MiB)
    lock_ttl_seconds: int = Field(default=300, ge=30)

    @field_validator("pinecone_api_key_secret")
    @classmethod
    def _api_key_is_arn(cls, value: str) -> str:
        return require_secret_arn(value, "pinecone_api_key_secret")

    @model_validator(mode="after")
    def _no_secret_values(self) -> Self:
        for name, value in self.model_dump().items():
            if isinstance(value, str):
                reject_secret_values(value, name)
        return self

    @property
    def root_prefix(self) -> str:
        """Key prefix under the bucket for this run, without a trailing slash."""
        return self.migration_id

    def zone(self, name: str) -> str:
        """Key prefix for one S3 zone from ADR-003: raw, canonical, import, rejects, manifest."""
        return f"{self.root_prefix}/{name}"

    def zone_uri(self, name: str) -> str:
        """S3 URI for one zone, with a trailing slash."""
        return f"s3://{self.bucket}/{self.zone(name)}/"
