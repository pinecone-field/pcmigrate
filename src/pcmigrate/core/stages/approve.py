"""Stage `approve`: verify an approval exists for the current plan and record who gave it."""

from __future__ import annotations

from typing import Any

from pcmigrate.core.approval import require_approval
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage


class ApproveStage:
    """Never records an approval itself; `pcmigrate approve` does. This stage only checks."""

    name = Stage.APPROVE

    def is_complete(self, manifest: Manifest) -> bool:
        """Done once an approver is recorded for the current plan hash."""
        record = manifest.stage(self.name)
        return (
            manifest.is_complete(self.name)
            and bool(record.outputs.get("approver"))
            and record.outputs.get("plan_hash") == manifest.plan_hash
        )

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Look up the approval for ctx.plan_lock."""
        lock = ctx.require_plan_lock()
        approval = require_approval(ctx.require_approvals(), lock)
        ctx.manifest.plan_hash = lock.plan_hash
        return {
            "approver": approval.approver,
            "approved_at": approval.approved_at.isoformat(),
            "plan_hash": approval.plan_hash,
        }
