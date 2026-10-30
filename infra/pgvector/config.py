"""Inputs to the pgvector runner stack."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.aws import ResourceNames


class PgvectorRunnerConfig(BaseModel):
    """An EC2 runner in the customer's subnet, reached only through SSM (ADR-014)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    migration_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    region: str
    vpc_id: str
    subnet_id: str
    db_port: int = Field(default=5432, ge=1, le=65535)
    db_cidr: str = "0.0.0.0/0"
    instance_type: str = "m6i.2xlarge"
    ami_id: str | None = None
    root_volume_gb: int = Field(default=100, ge=20)
    instance_profile_name: str
    bucket_name: str
    wheel_s3_uri: str | None = None
    name_prefix: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @property
    def resource_prefix(self) -> str:
        """Pulumi logical-name prefix; fixed so a name_prefix change never replaces resources."""
        return f"pcmigrate-{self.migration_id}"

    @property
    def names(self) -> ResourceNames:
        """Physical AWS names, with the customer's ``name_prefix`` applied."""
        return ResourceNames(self.migration_id, self.name_prefix)

    @property
    def stack_name(self) -> str:
        """One add-on stack per migration."""
        return f"pcmigrate-{self.migration_id}-pgvector"
