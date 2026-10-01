import io
import json

import pytest

from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.targets.importer import Importer, ImportFailedError, ImportTimeoutError
from pcmigrate.targets.pinecone_client import ImportInfo
from tests.unit.fakes import FakeGateway


def _importer(gw: FakeGateway, out: io.StringIO, **kw: object) -> Importer:
    return Importer(
        gw, EventBus("mig", [JsonLinesSink(out)]), poll_interval_s=0, sleep=lambda _: None, **kw
    )  # type: ignore[arg-type]


def test_run_polls_to_completion_and_emits_progress() -> None:
    gw = FakeGateway(import_records=100)
    out = io.StringIO()
    info = _importer(gw, out).run("h", "s3://b/mig/import/batch-00000/")
    assert info.status == "Completed" and info.records_imported == 100
    events = [json.loads(line) for line in out.getvalue().splitlines()]
    assert events[0]["type"] == "import_started"
    assert [e["status"] for e in events[1:]] == ["Pending", "InProgress", "Completed"]


def test_failed_import_raises() -> None:
    gw = FakeGateway()
    gw.import_script.append(
        [ImportInfo("x", "InProgress"), ImportInfo("x", "Failed", error="bad parquet")]
    )
    with pytest.raises(ImportFailedError, match="bad parquet"):
        _importer(gw, io.StringIO()).run("h", "s3://b/x/")


def test_timeout_raises() -> None:
    gw = FakeGateway()
    gw.import_script.append([ImportInfo("x", "InProgress")])
    clock = iter([0.0, 0.0, 100.0, 200.0, 300.0])
    importer = Importer(
        gw, None, poll_interval_s=0, timeout_s=50, sleep=lambda _: None, clock=lambda: next(clock)
    )
    with pytest.raises(ImportTimeoutError):
        importer.run("h", "s3://b/x/")


def test_progress_only_emitted_on_change() -> None:
    gw = FakeGateway()
    gw.import_script.append(
        [
            ImportInfo("x", "InProgress", 10.0),
            ImportInfo("x", "InProgress", 10.0),
            ImportInfo("x", "Completed", 100.0, 5),
        ]
    )
    out = io.StringIO()
    _importer(gw, out).run("h", "s3://b/x/")
    assert len(out.getvalue().splitlines()) == 3
