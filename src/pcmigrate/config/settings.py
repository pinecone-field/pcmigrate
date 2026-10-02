"""RunSettings: the on-disk file `pcmigrate run`, `cutover`, and `teardown` read.

Holds the MigrationConfig fields plus where mapping.yaml lives and which environment
variable carries the source DSN. Secret values never appear here (ADR-014).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.migration import MigrationConfig


class RunSettings(BaseModel):
    """pcmigrate.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration: MigrationConfig
    mapping_path: Path = Field(default=Path("pcmigrate-out/mapping.yaml"))
    artifacts_dir: Path = Field(default=Path("pcmigrate-out"))
    dsn_env: str = "PGVECTOR_DSN"
    pinecone_api_key_env: str | None = None

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
