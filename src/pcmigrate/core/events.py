"""Typed event bus: JSON-lines sinks (stdout, manifest/events/) and a rich live view (ADR-013)."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, Self, TextIO

from pydantic import BaseModel, ConfigDict, Field
from rich.console import Console
from rich.live import Live
from rich.table import Table

from pcmigrate.core.state import Stage, StageStatus

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


LogLevel = Literal["debug", "info", "warning", "error"]


def _now() -> datetime:
    return datetime.now(UTC)


class Event(BaseModel):
    """Base of every event. Subclasses fix ``type`` and add fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: str
    ts: datetime = Field(default_factory=_now)
    migration_id: str = ""
    stage: Stage | None = None

    def to_json(self) -> str:
        """One JSON line, without a trailing newline."""
        return self.model_dump_json(exclude_none=True)


class StageStarted(Event):
    """A stage began an attempt."""

    type: Literal["stage_started"] = "stage_started"
    attempt: int = 1


class StageSkipped(Event):
    """A stage was already complete and was not re-run."""

    type: Literal["stage_skipped"] = "stage_skipped"


class StageCompleted(Event):
    """A stage finished."""

    type: Literal["stage_completed"] = "stage_completed"
    duration_s: float = 0.0
    outputs: dict[str, Any] = Field(default_factory=dict)


class StageFailed(Event):
    """A stage raised."""

    type: Literal["stage_failed"] = "stage_failed"
    error: str


class PartitionDone(Event):
    """One partition of a stage finished."""

    type: Literal["partition_done"] = "partition_done"
    partition: str
    rows: int = 0
    parts: list[str] = Field(default_factory=list)


class PartDone(Event):
    """One output file was written."""

    type: Literal["part_done"] = "part_done"
    key: str
    rows: int
    bytes: int


class IndexPrepared(Event):
    """The target index was created or adopted."""

    type: Literal["index_prepared"] = "index_prepared"
    index: str
    mode: str
    host: str | None = None


class ImportStarted(Event):
    """A bulk import was accepted."""

    type: Literal["import_started"] = "import_started"
    import_id: str
    uri: str


class ImportProgress(Event):
    """A poll of a running import."""

    type: Literal["import_progress"] = "import_progress"
    import_id: str
    status: str
    percent_complete: float | None = None
    records_imported: int | None = None


class ValidationResult(Event):
    """Outcome of one validation check."""

    type: Literal["validation_result"] = "validation_result"
    check: str
    passed: bool
    details: dict[str, Any] = Field(default_factory=dict)


class SparseEncoded(Event):
    """One canonical part had its sparse vectors generated from text (ADR-017)."""

    type: Literal["sparse_encoded"] = "sparse_encoded"
    namespace: str = ""
    part: str = ""
    records: int = 0
    cached: int = 0
    no_text: int = 0
    tokens: int = 0
    requests: int = 0


class LogMessage(Event):
    """Free-form message. The only event with prose in it."""

    type: Literal["log"] = "log"
    level: LogLevel = "info"
    message: str


class EventSink(Protocol):
    """Receives every event the bus emits."""

    def handle(self, event: Event) -> None:
        """Consume one event."""
        ...

    def close(self) -> None:
        """Flush and release resources."""
        ...


class JsonLinesSink:
    """Writes one JSON line per event to a text stream, stdout by default."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def handle(self, event: Event) -> None:
        """Write the event as one line."""
        self._stream.write(event.to_json() + "\n")
        self._stream.flush()

    def close(self) -> None:
        """Flush the stream."""
        self._stream.flush()


class S3EventSink:
    """Buffers events and writes them as JSON-lines objects under manifest/events/."""

    def __init__(
        self, s3: S3Client, bucket: str, migration_id: str, *, flush_every: int = 200
    ) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = f"{migration_id}/manifest/events"
        self._flush_every = flush_every
        self._buffer: list[str] = []
        self._sequence = 0
        self._run_started = _now().strftime("%Y%m%dT%H%M%S")

    @property
    def prefix(self) -> str:
        """Key prefix the sink writes under."""
        return self._prefix

    def handle(self, event: Event) -> None:
        """Buffer the event, flushing when the batch is full."""
        self._buffer.append(event.to_json())
        if len(self._buffer) >= self._flush_every:
            self.flush()

    def flush(self) -> None:
        """Write buffered events as one object."""
        if not self._buffer:
            return
        key = f"{self._prefix}/{self._run_started}-{self._sequence:06d}.jsonl"
        body = ("\n".join(self._buffer) + "\n").encode("utf-8")
        self._s3.put_object(
            Bucket=self._bucket, Key=key, Body=body, ContentType="application/x-ndjson"
        )
        self._sequence += 1
        self._buffer.clear()

    def close(self) -> None:
        """Flush whatever is buffered."""
        self.flush()


class RichLiveSink:
    """A live table of stage status plus the latest message, for the terminal."""

    _STATUS_STYLE: ClassVar[dict[StageStatus, str]] = {
        StageStatus.PENDING: "dim",
        StageStatus.RUNNING: "yellow",
        StageStatus.COMPLETE: "green",
        StageStatus.FAILED: "red",
    }

    def __init__(self, console: Console | None = None) -> None:
        self._console = console or Console(stderr=True)
        self._status: dict[Stage, StageStatus] = dict.fromkeys(Stage.ordered(), StageStatus.PENDING)
        self._detail: dict[Stage, str] = {}
        self._last_message = ""
        self._live = Live(
            self._render(), console=self._console, refresh_per_second=4, transient=False
        )
        self._live.start()

    def _render(self) -> Table:
        table = Table(title="pcmigrate", expand=False)
        table.add_column("stage")
        table.add_column("status")
        table.add_column("detail")
        for stage in Stage.ordered():
            status = self._status[stage]
            table.add_row(
                stage.value,
                f"[{self._STATUS_STYLE[status]}]{status.value}[/]",
                self._detail.get(stage, ""),
            )
        if self._last_message:
            table.caption = self._last_message
        return table

    def handle(self, event: Event) -> None:
        """Update the table from the event."""
        stage = event.stage
        if isinstance(event, StageStarted) and stage is not None:
            self._status[stage] = StageStatus.RUNNING
            self._detail[stage] = f"attempt {event.attempt}"
        elif isinstance(event, StageSkipped | StageCompleted) and stage is not None:
            self._status[stage] = StageStatus.COMPLETE
            if isinstance(event, StageCompleted):
                self._detail[stage] = f"{event.duration_s:.1f}s"
        elif isinstance(event, StageFailed) and stage is not None:
            self._status[stage] = StageStatus.FAILED
            self._detail[stage] = event.error[:60]
        elif isinstance(event, PartitionDone) and stage is not None:
            self._detail[stage] = f"partition {event.partition}: {event.rows} rows"
        elif isinstance(event, ImportProgress) and stage is not None:
            pct = f"{event.percent_complete:.0f}%" if event.percent_complete is not None else ""
            self._detail[stage] = f"{event.status} {pct}".strip()
        elif isinstance(event, ValidationResult) and stage is not None:
            verdict = "pass" if event.passed else "FAIL"
            self._detail[stage] = f"{event.check}: {verdict}"
        elif isinstance(event, LogMessage):
            self._last_message = f"[{event.level}] {event.message}"
        self._live.update(self._render())

    def close(self) -> None:
        """Render the final state and stop the live display."""
        self._live.update(self._render())
        self._live.stop()


class EventBus:
    """Fans every event out to its sinks, stamping the migration id."""

    def __init__(self, migration_id: str, sinks: list[EventSink] | None = None) -> None:
        self._migration_id = migration_id
        self._sinks: list[EventSink] = list(sinks or [])

    @property
    def migration_id(self) -> str:
        """The run every emitted event is stamped with."""
        return self._migration_id

    def add_sink(self, sink: EventSink) -> None:
        """Attach another sink."""
        self._sinks.append(sink)

    def emit(self, event: Event) -> Event:
        """Stamp the migration id onto *event* and deliver it to every sink."""
        if event.migration_id != self._migration_id:
            event = event.model_copy(update={"migration_id": self._migration_id})
        for sink in self._sinks:
            sink.handle(event)
        return event

    def log(self, message: str, *, level: LogLevel = "info", stage: Stage | None = None) -> None:
        """Emit a LogMessage."""
        self.emit(LogMessage(message=message, level=level, stage=stage))

    def close(self) -> None:
        """Close every sink."""
        for sink in self._sinks:
            sink.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
