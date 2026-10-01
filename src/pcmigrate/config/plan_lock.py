"""PlanLock: SHA-256 over the normalized mapping plus the source schema fingerprint (ADR-005)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping as MappingABC
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.mapping import Mapping

PLAN_LOCK_VERSION = 1


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def schema_fingerprint(schema: MappingABC[str, Any]) -> str:
    """SHA-256 of a source schema snapshot, independent of key order."""
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"), default=str)
    return _sha256(canonical)


class PlanLock(BaseModel):
    """The hash an FDE approves and every later stage checks against."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = PLAN_LOCK_VERSION
    migration_id: str
    mapping_sha256: str
    schema_fingerprint: str
    plan_hash: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def compute(cls, mapping: Mapping, fingerprint: str) -> PlanLock:
        """Derive the lock from a mapping and the fingerprint of the schema it was built from."""
        mapping_hash = _sha256(mapping.normalized())
        plan_hash = _sha256(f"{PLAN_LOCK_VERSION}\n{mapping_hash}\n{fingerprint}\n")
        return cls(
            migration_id=mapping.migration_id,
            mapping_sha256=mapping_hash,
            schema_fingerprint=fingerprint,
            plan_hash=plan_hash,
        )

    def matches(self, approved_hash: str) -> bool:
        """True when an approval recorded *approved_hash* for this exact plan."""
        return approved_hash == self.plan_hash

    def to_text(self) -> str:
        """Serialize for plan.lock."""
        return self.model_dump_json(indent=2) + "\n"

    @classmethod
    def from_text(cls, text: str) -> PlanLock:
        """Parse plan.lock."""
        return cls.model_validate_json(text)
