"""The approval gate: manifest/approvals.json must hold the hash of the current plan.lock."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from pcmigrate.config.plan_lock import PlanLock
from pcmigrate.core.artifacts import ArtifactStore

APPROVALS_NAME = "manifest/approvals.json"
PLAN_LOCK_NAME = "plan.lock"
MAPPING_NAME = "mapping.yaml"


class Approval(BaseModel):
    """One recorded approval."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    approver: str
    approved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    migration_id: str
    plan_hash: str
    mapping_sha256: str
    schema_fingerprint: str


class ApprovalsFile(BaseModel):
    """Every approval ever recorded for the run, newest last."""

    model_config = ConfigDict(extra="forbid")

    approvals: list[Approval] = Field(default_factory=list)

    def for_plan(self, lock: PlanLock) -> Approval | None:
        """The newest approval whose hash equals the lock's, if any."""
        for approval in reversed(self.approvals):
            if lock.matches(approval.plan_hash):
                return approval
        return None


class ApprovalRequiredError(RuntimeError):
    """No approval matches the current plan.lock."""


def load_approvals(store: ArtifactStore) -> ApprovalsFile:
    """Read approvals.json, empty when absent."""
    text = store.read_text(APPROVALS_NAME)
    return ApprovalsFile.model_validate_json(text) if text else ApprovalsFile()


def record_approval(store: ArtifactStore, lock: PlanLock, approver: str) -> Approval:
    """Append an approval for *lock* and persist."""
    approvals = load_approvals(store)
    approval = Approval(
        approver=approver,
        migration_id=lock.migration_id,
        plan_hash=lock.plan_hash,
        mapping_sha256=lock.mapping_sha256,
        schema_fingerprint=lock.schema_fingerprint,
    )
    approvals.approvals.append(approval)
    store.write_text(APPROVALS_NAME, approvals.model_dump_json(indent=2) + "\n")
    return approval


def current_approval(store: ArtifactStore, lock: PlanLock) -> Approval | None:
    """The approval matching *lock*, or None."""
    return load_approvals(store).for_plan(lock)


def require_approval(store: ArtifactStore, lock: PlanLock) -> Approval:
    """Raise unless an approval matches the current plan."""
    approval = current_approval(store, lock)
    if approval is None:
        latest = load_approvals(store).approvals
        hint = (
            f"latest approval by {latest[-1].approver} was for plan {latest[-1].plan_hash[:12]}"
            if latest
            else "no approval recorded"
        )
        raise ApprovalRequiredError(
            f"plan {lock.plan_hash[:12]} is not approved ({hint}); review the report and run "
            "`pcmigrate approve`"
        )
    return approval
