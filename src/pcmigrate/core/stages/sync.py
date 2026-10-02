"""Stage `sync`: forward source changes into the buffer and apply them after validate passed.

Skipped entirely when change_capture.tier is none (ADR-007). Otherwise runs rounds of
forward-then-drain until a stop is requested through the manifest (cutover --confirm) or
the configured round limit is reached, which tests use.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast

from pcmigrate.core.buffer import ChangeBuffer
from pcmigrate.core.events import LogMessage
from pcmigrate.core.stages import RunContext
from pcmigrate.core.state import Manifest, Stage
from pcmigrate.sources.base import GroundTruthSource
from pcmigrate.sources.pgvector.change_feed import ChangeFeed, make_feed
from pcmigrate.targets.applier import Applier, forward

STOP_FLAG = "stop_requested"


def change_capture_enabled(ctx: RunContext) -> bool:
    """True unless the mapping's tier is none."""
    return ctx.mapping.change_capture_enabled


def resolve_buffer(ctx: RunContext) -> ChangeBuffer:
    """The buffer attached to the run context, or an SQS FIFO client from provision's outputs."""
    attached = ctx.extras.get("buffer")
    if attached is not None:
        return attached  # type: ignore[no-any-return]
    url = ctx.manifest.stage(Stage.PROVISION).outputs.get("buffer_queue_url")
    if not url:
        raise RuntimeError(
            "no buffer: attach one on ctx.extras['buffer'] or provision the SQS queue"
        )
    import boto3

    from pcmigrate.core.buffer import SqsFifoBuffer

    return SqsFifoBuffer(boto3.client("sqs", region_name=ctx.config.aws_region), str(url))


def resolve_feed(ctx: RunContext) -> ChangeFeed | None:
    """The source's change feed for the mapped tier, started at the recorded boundary."""
    attached = ctx.extras.get("feed")
    if attached is not None:
        feed: ChangeFeed = attached
    else:
        dsn = getattr(ctx.source, "dsn", None)
        if not isinstance(dsn, str):
            return None
        built = make_feed(dsn, ctx.mapping)
        if built is None:
            return None
        feed = built
    boundary = ctx.manifest.stage(Stage.EXPORT).outputs.get("change_boundary", {})
    if "watermarks" not in boundary and ctx.manifest.stage(Stage.EXPORT).outputs.get("watermarks"):
        boundary = {
            **boundary,
            "watermarks": ctx.manifest.stage(Stage.EXPORT).outputs["watermarks"],
        }
    feed.start(boundary)
    return feed


class SyncStage:
    """Forward and apply in rounds. ``max_rounds`` bounds a run; None runs until stop."""

    name = Stage.SYNC

    def __init__(
        self,
        *,
        max_rounds: int | None = None,
        idle_sleep_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._max_rounds = max_rounds
        self._idle_sleep = idle_sleep_s
        self._sleep = sleep

    def is_complete(self, manifest: Manifest) -> bool:
        """Done when marked complete, which happens on stop or when the tier is none."""
        return manifest.is_complete(self.name)

    def run(self, ctx: RunContext) -> dict[str, Any]:
        """Skip for tier none; otherwise forward and drain until stopped."""
        if not change_capture_enabled(ctx):
            ctx.events.emit(
                LogMessage(stage=self.name, message="change capture is none; sync skipped")
            )
            return {"skipped": True, "reason": "change_capture.tier is none"}
        if not ctx.manifest.is_complete(Stage.VALIDATE):
            raise RuntimeError("sync must not apply changes before validate has passed (ADR-007)")
        buffer = resolve_buffer(ctx)
        feed = resolve_feed(ctx)
        truth = cast("GroundTruthSource | None", ctx.source)
        applier = Applier(
            ctx.require_target(), ctx.index_host(), buffer, source=truth, events=ctx.events
        )
        record = ctx.manifest.stage(self.name)
        totals = record.outputs.setdefault(
            "totals", {"forwarded": 0, "applied": 0, "upserts": 0, "deletes": 0, "rounds": 0}
        )
        rounds = 0
        while True:
            forwarded = forward(feed.poll, buffer) if feed is not None else 0
            result = applier.drain_once(wait_s=0.0)
            totals["forwarded"] += forwarded
            totals["applied"] += result.collapsed
            totals["upserts"] += result.upserts
            totals["deletes"] += result.deletes
            totals["rounds"] += 1
            rounds += 1
            if rounds % 10 == 0 or forwarded or result.received:
                ctx.checkpoint()
            if record.outputs.get(STOP_FLAG) or self._stop_requested(ctx):
                break
            if self._max_rounds is not None and rounds >= self._max_rounds:
                return {**totals, "stopped": False}
            if not forwarded and not result.received:
                self._sleep(self._idle_sleep)
        return {**totals, "stopped": True}

    @staticmethod
    def _stop_requested(ctx: RunContext) -> bool:
        latest = ctx.store.load()
        if latest is None:
            return False
        return bool(latest.stage(Stage.SYNC).outputs.get(STOP_FLAG))


def request_stop(ctx: RunContext) -> None:
    """Ask a running sync to finish its round and exit (cutover --confirm)."""
    ctx.manifest.stage(Stage.SYNC).outputs[STOP_FLAG] = True
    ctx.checkpoint()
