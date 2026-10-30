"""Names and tags for the AWS resources a migration creates (ADR-002).

Every physical resource name starts with ``pcmigrate`` or, when the customer sets
``name_prefix``, with ``<prefix>-pcmigrate``. Every taggable resource carries the customer's
tags plus two of the tool's own under the reserved ``pcmigrate:`` key namespace.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from pcmigrate.config._secrets import reject_secret_values

TOOL_NAME = "pcmigrate"
TOOL_TAG_NAMESPACE = "pcmigrate:"
NAME_PREFIX_PATTERN = r"^[A-Za-z](?:[A-Za-z0-9_-]{0,30}[A-Za-z0-9])?$"
MAX_IAM_ROLE_NAME = 64
MAX_OSIS_PIPELINE_NAME = 28
MAX_TAGS = 50
MAX_TAG_KEY = 128
MAX_TAG_VALUE = 256
_TAG_CHARS = re.compile(r"^[\w .:/=+@-]*$")
_OSIS_SAFE = re.compile(r"[^a-z0-9-]+")


@dataclass(frozen=True, slots=True)
class ResourceNames:
    """The physical AWS names for one migration, with the optional customer prefix applied.

    Pulumi logical names are separate and never change, so adding a prefix to an existing
    migration is a rename of the physical resources, not a new stack.
    """

    migration_id: str
    prefix: str | None = None

    @property
    def family(self) -> str:
        """``pcmigrate`` or ``<prefix>-pcmigrate``: the token every name starts with."""
        return f"{self.prefix}-{TOOL_NAME}" if self.prefix else TOOL_NAME

    @property
    def base(self) -> str:
        """``<family>-<migration_id>``."""
        return f"{self.family}-{self.migration_id}"

    def suffixed(self, suffix: str) -> str:
        """``<family>-<migration_id>-<suffix>``."""
        return f"{self.base}-{suffix}"

    @property
    def runner_role(self) -> str:
        """IAM role, instance profile, security group, and instance Name of the runner."""
        return self.suffixed("runner")

    @property
    def storage_integration_role(self) -> str:
        """IAM role Pinecone assumes to read the import prefix."""
        return self.suffixed("pinecone-import")

    @property
    def osis_pipeline_role(self) -> str:
        """IAM role of the OSIS export pipeline."""
        return self.suffixed("osis-pipeline")

    @property
    def glue_role(self) -> str:
        """IAM role of the Glue transform job."""
        return self.suffixed("glue-transform")

    @property
    def glue_job(self) -> str:
        """The Glue transform job."""
        return self.suffixed("opensearch-transform")

    @property
    def buffer_queue(self) -> str:
        """The FIFO change buffer."""
        return self.suffixed("buffer.fifo")

    @property
    def kms_alias(self) -> str:
        """Alias of the migration's KMS key."""
        return f"alias/{self.base}"

    def secret(self, short: str) -> str:
        """Secrets Manager name for one of the migration's secrets."""
        return f"{self.family}/{self.migration_id}/{short}"

    @property
    def log_group(self) -> str:
        """The runner's CloudWatch log group."""
        return f"/{self.family}/{self.migration_id}"

    @property
    def osis_pipeline(self) -> str:
        """``[<prefix>-]pcm-<migration_id>`` fitted to OSIS's 28-character lowercase names."""
        raw = f"pcm-{self.migration_id}"
        if self.prefix:
            raw = f"{self.prefix}-{raw}"
        name = _OSIS_SAFE.sub("-", raw.lower())[:MAX_OSIS_PIPELINE_NAME].strip("-")
        return name if re.fullmatch(r"[a-z][a-z0-9-]{2,27}", name) else "pcm-migration"

    @property
    def osis_log_group(self) -> str:
        """OSIS only publishes to log groups under /aws/vendedlogs/."""
        return f"/aws/vendedlogs/OpenSearchIngestion/{self.osis_pipeline}/audit-logs"

    @property
    def longest_role(self) -> str:
        """The IAM role name that hits the 64-character limit first."""
        return max(
            (
                self.runner_role,
                self.storage_integration_role,
                self.osis_pipeline_role,
                self.glue_role,
            ),
            key=len,
        )

    def as_dict(self) -> dict[str, str]:
        """The names worth recording in the manifest."""
        return {
            "runner_role": self.runner_role,
            "storage_integration_role": self.storage_integration_role,
            "osis_pipeline_role": self.osis_pipeline_role,
            "glue_role": self.glue_role,
            "glue_job": self.glue_job,
            "buffer_queue": self.buffer_queue,
            "kms_alias": self.kms_alias,
            "log_group": self.log_group,
            "osis_pipeline": self.osis_pipeline,
            "secret_prefix": self.secret(""),
        }


def validate_name_prefix(prefix: str | None, migration_id: str) -> str | None:
    """Return *prefix* when it is well formed and every role name still fits in 64 characters."""
    if prefix is None:
        return None
    if not re.fullmatch(NAME_PREFIX_PATTERN, prefix):
        raise ValueError(
            "name_prefix must start with a letter, use only letters, digits, '-' and '_', "
            "not end with a separator, and be at most 32 characters"
        )
    longest = ResourceNames(migration_id, prefix).longest_role
    if len(longest) > MAX_IAM_ROLE_NAME:
        raise ValueError(
            f"name_prefix {prefix!r} makes the role name {longest!r} "
            f"{len(longest)} characters; IAM allows {MAX_IAM_ROLE_NAME}. "
            "Shorten the prefix or the migration_id"
        )
    return prefix


def validate_tags(tags: dict[str, str]) -> dict[str, str]:
    """Return *tags* when they satisfy AWS's tag rules and stay out of the reserved namespaces."""
    if len(tags) > MAX_TAGS - len(tool_tags("x")):
        raise ValueError(f"at most {MAX_TAGS - len(tool_tags('x'))} tags; AWS allows {MAX_TAGS}")
    for key, value in tags.items():
        if not 1 <= len(key) <= MAX_TAG_KEY:
            raise ValueError(f"tag key {key!r} must be 1 to {MAX_TAG_KEY} characters")
        if len(value) > MAX_TAG_VALUE:
            raise ValueError(f"tag {key!r} value must be at most {MAX_TAG_VALUE} characters")
        if key.lower().startswith("aws:"):
            raise ValueError(f"tag key {key!r}: the aws: prefix is reserved by AWS")
        if key.lower().startswith(TOOL_TAG_NAMESPACE):
            raise ValueError(f"tag key {key!r}: the {TOOL_TAG_NAMESPACE} prefix is reserved")
        for text in (key, value):
            if not _TAG_CHARS.match(text):
                raise ValueError(
                    f"tag {key!r}: only letters, digits, spaces and _ . : / = + - @ are allowed"
                )
            reject_secret_values(text, f"tags.{key}")
    return dict(tags)


def tool_tags(migration_id: str) -> dict[str, str]:
    """The tags pcmigrate puts on everything it creates."""
    return {
        f"{TOOL_TAG_NAMESPACE}managed_by": TOOL_NAME,
        f"{TOOL_TAG_NAMESPACE}migration_id": migration_id,
    }


def resource_tags(migration_id: str, customer_tags: dict[str, str] | None = None) -> dict[str, str]:
    """Customer tags plus the tool's own; the tool's keys cannot be overridden."""
    return {**(customer_tags or {}), **tool_tags(migration_id)}


def default_tags_config(tags: dict[str, str]) -> str:
    """The ``aws:defaultTags`` stack config value: the provider stamps these on every resource."""
    return json.dumps({"tags": tags}, sort_keys=True)
