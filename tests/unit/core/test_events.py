import io
import json

from mypy_boto3_s3 import S3Client
from rich.console import Console

from pcmigrate.core.events import (
    EventBus,
    ImportProgress,
    JsonLinesSink,
    LogMessage,
    PartitionDone,
    RichLiveSink,
    S3EventSink,
    StageCompleted,
    StageFailed,
    StageStarted,
    ValidationResult,
)
from pcmigrate.core.state import Stage
from tests.unit.conftest import BUCKET


def test_json_lines_sink_writes_one_line_per_event() -> None:
    out = io.StringIO()
    bus = EventBus("mig", [JsonLinesSink(out)])
    bus.emit(StageStarted(stage=Stage.EXPORT))
    bus.emit(PartitionDone(stage=Stage.EXPORT, partition="p0", rows=5))
    lines = out.getvalue().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["type"] == "stage_started"
    assert first["migration_id"] == "mig"
    assert first["stage"] == "export"
    assert json.loads(lines[1])["rows"] == 5


def test_bus_stamps_migration_id() -> None:
    out = io.StringIO()
    bus = EventBus("mig", [JsonLinesSink(out)])
    returned = bus.emit(LogMessage(message="hi"))
    assert returned.migration_id == "mig"
    bus.log("bye", level="warning")
    assert json.loads(out.getvalue().splitlines()[1])["level"] == "warning"


def test_s3_sink_flushes_in_batches_and_on_close(s3: S3Client) -> None:
    sink = S3EventSink(s3, BUCKET, "mig", flush_every=2)
    bus = EventBus("mig", [sink])
    bus.emit(StageStarted(stage=Stage.EXPORT))
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=sink.prefix).get("KeyCount", 0) == 0
    bus.emit(StageCompleted(stage=Stage.EXPORT, duration_s=1.5))
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=sink.prefix)["KeyCount"] == 1
    bus.emit(StageFailed(stage=Stage.EMIT, error="x"))
    bus.close()
    keys = sorted(
        o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=sink.prefix)["Contents"]
    )
    assert len(keys) == 2
    assert keys[0].startswith("mig/manifest/events/")
    body = s3.get_object(Bucket=BUCKET, Key=keys[1])["Body"].read().decode()
    assert json.loads(body.strip())["type"] == "stage_failed"


def test_rich_sink_renders_without_error() -> None:
    console = Console(file=io.StringIO(), force_terminal=False, width=100)
    sink = RichLiveSink(console)
    bus = EventBus("mig", [sink])
    bus.emit(StageStarted(stage=Stage.IMPORT))
    bus.emit(
        ImportProgress(stage=Stage.IMPORT, import_id="i1", status="InProgress", percent_complete=42)
    )
    bus.emit(ValidationResult(stage=Stage.VALIDATE, check="counts", passed=True))
    bus.emit(StageFailed(stage=Stage.VALIDATE, error="threshold missed"))
    bus.log("done")
    bus.close()
    text = console.file.getvalue()  # type: ignore[attr-defined]
    assert "import" in text
    assert "threshold missed" in text
