"""RunSettings: the on-disk file `pcmigrate run`, `cutover`, and `teardown` read.

Holds the MigrationConfig fields plus where mapping.yaml lives and which environment
variable carries the source DSN. Secret values never appear here (ADR-014).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from pcmigrate.config._secrets import reject_secret_values, require_secret_arn
from pcmigrate.config.mapping import Mapping
from pcmigrate.config.migration import MigrationConfig


class OpenSearchSettings(BaseModel):
    """How the run reaches an OpenSearch source and which AWS pieces move its data (ADR-009).

    ``auth: sigv4`` is the Amazon OpenSearch Service default; ``basic`` reads ``user:password``
    from the environment variable in ``basic_auth_env`` for self-managed clusters on a laptop,
    and OSIS reads the same credentials from ``credentials_secret_arn``. ``export_mode: direct``
    scans the index from the runner instead of OSIS and Glue, for laptop runs and tests.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    url_env: str = "OPENSEARCH_URL"
    auth: Literal["sigv4", "basic", "none"] = "sigv4"
    basic_auth_env: str | None = None
    insecure_tls: bool = False
    export_mode: Literal["osis_glue", "direct"] = "osis_glue"
    domain_arn: str | None = None
    domain_name: str | None = None
    fine_grained_access: bool = False
    manage_domain_policy: bool = False
    subnet_ids: list[str] = Field(default_factory=list)
    security_group_ids: list[str] = Field(default_factory=list)
    credentials_secret_arn: str | None = None
    osis_min_units: int = Field(default=1, ge=1, le=96)
    osis_max_units: int = Field(default=4, ge=1, le=96)
    scan_timeout_s: int = Field(default=6 * 3600, ge=60)
    glue_workers: int = Field(default=4, ge=2, le=299)
    glue_worker_type: Literal["G.1X", "G.2X", "G.4X", "G.8X"] = "G.1X"
    glue_timeout_minutes: int = Field(default=12 * 60, ge=1)
    wheel_path: Path | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.credentials_secret_arn is not None:
            require_secret_arn(self.credentials_secret_arn, "opensearch.credentials_secret_arn")
        if self.osis_max_units < self.osis_min_units:
            raise ValueError("opensearch.osis_max_units must be at least osis_min_units")
        for name in ("domain_arn", "domain_name", "url_env", "basic_auth_env"):
            value = getattr(self, name)
            if isinstance(value, str):
                reject_secret_values(value, f"opensearch.{name}")
        return self


class RunSettings(BaseModel):
    """pcmigrate.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration: MigrationConfig
    mapping_path: Path = Field(default=Path("pcmigrate-out/mapping.yaml"))
    artifacts_dir: Path = Field(default=Path("pcmigrate-out"))
    dsn_env: str = "PGVECTOR_DSN"
    pinecone_api_key_env: str | None = None
    opensearch: OpenSearchSettings = Field(default_factory=OpenSearchSettings)

    @classmethod
    def load(cls, path: Path) -> RunSettings:
        """Parse the settings file."""
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict):
            raise ValueError(f"{path} must be a YAML mapping")
        settings = cls.model_validate(data)
        base = path.parent
        return settings.model_copy(
            update={
                "mapping_path": settings.mapping_path
                if settings.mapping_path.is_absolute()
                else base / settings.mapping_path,
                "artifacts_dir": settings.artifacts_dir
                if settings.artifacts_dir.is_absolute()
                else base / settings.artifacts_dir,
            }
        )

    def mapping(self) -> Mapping:
        """The approved mapping."""
        return Mapping.from_yaml(self.mapping_path.read_text())

    def dsn(self) -> str:
        """The source DSN from the environment."""
        value = os.environ.get(self.dsn_env)
        if not value:
            raise RuntimeError(f"environment variable {self.dsn_env} is not set")
        return value

    def opensearch_url(self) -> str:
        """The OpenSearch endpoint from the environment."""
        value = os.environ.get(self.opensearch.url_env)
        if not value:
            raise RuntimeError(f"environment variable {self.opensearch.url_env} is not set")
        return value

    def opensearch_basic_auth(self) -> tuple[str, str] | None:
        """``(user, password)`` from ``basic_auth_env`` when auth is basic, else None."""
        if self.opensearch.auth != "basic":
            return None
        name = self.opensearch.basic_auth_env or "OPENSEARCH_BASIC_AUTH"
        value = os.environ.get(name)
        if not value or ":" not in value:
            raise RuntimeError(f"environment variable {name} must hold user:password")
        user, _, password = value.partition(":")
        return user, password
