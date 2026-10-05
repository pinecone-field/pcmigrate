"""The OSIS + Glue export stage with fake OSIS and Glue controls and moto S3."""

import io
import json
from pathlib import Path
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, PipelineError, RunContext
from pcmigrate.core.stages.export_opensearch import OpenSearchExportStage, ScanSettings
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.opensearch.client import ConnectionOptions
from pcmigrate.sources.opensearch.export import OpenSearchSource
from pcmigrate.sources.opensearch.introspect import IntrospectOptions, OpenSearchIntrospector
from pcmigrate.sources.opensearch.osis_config import (
    ARMED_START_TIME,
    OsisPipelineSpec,
    pipeline_body,
)
from tests.unit.conftest import BUCKET, REGION, approve_ctx
from tests.unit.sources.opensearch.fakes import FakeTransport, make_client

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
INDEX = "golden-documents"


class FakeOsis:
    def __init__(self, body: str) -> None:
        self.bodies = [body]
        self.statuses = ["ACTIVE"]
        self.stopped = 0
        self.updates: list[str] = []

    def status(self, name: str) -> str:
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    def configuration(self, name: str) -> str:
        return self.bodies[-1]

    def update(self, name: str, body: str) -> None:
        self.updates.append(body)
        self.bodies.append(body)

    def stop(self, name: str) -> None:
        self.stopped += 1


class FakeGlue:
    def __init__(self, s3: S3Client, *, states: list[str] | None = None) -> None:
        self.s3 = s3
        self.runs: dict[str, dict[str, str]] = {}
        self.states = states or ["RUNNING", "SUCCEEDED"]
        self.polls: dict[str, int] = {}

    def start(self, job: str, arguments: dict[str, str]) -> str:
        run_id = f"jr_{len(self.runs) + 1}"
        self.runs[run_id] = arguments
        return run_id

    def state(self, job: str, run_id: str) -> tuple[str, str | None]:
        n = self.polls.get(run_id, 0)
        self.polls[run_id] = n + 1
        state = self.states[min(n, len(self.states) - 1)]
        if state == "SUCCEEDED":
            args = self.runs[run_id]
            result = {
                "index": args["--index"],
                "partitions": 1,
                "namespaces": {
                    INDEX: {
                        "namespace": INDEX,
                        "rows": 999,
                        "rejected": 5,
                        "parts": [f"{args['--canonical_prefix']}/p00000/part-00000.parquet"],
                        "reject_parts": [f"{args['--rejects_prefix']}/p00000/part-00000.jsonl"],
                        "reject_reasons": {"embedding is NULL": 5},
                    }
                },
                "rows": 999,
                "rejected": 5,
            }
            uri = args["--result_uri"].removeprefix(f"s3://{BUCKET}/")
            self.s3.put_object(Bucket=BUCKET, Key=uri, Body=json.dumps(result).encode())
            return "SUCCEEDED", None
        return state, "boom" if state in ("FAILED", "ERROR") else None


@pytest.fixture
def setup(s3: S3Client, tmp_path: Path) -> dict[str, Any]:
    transport = FakeTransport.golden()
    introspector = OpenSearchIntrospector(
        make_client(transport), IntrospectOptions(workload_window_s=0, sample_docs=50)
    )
    mapping: Mapping = build_report(
        introspector.inventory(),
        MappingOptions(migration_id="os-mig", index="os-mig", region=REGION),
    ).mapping
    source = OpenSearchSource(ConnectionOptions("http://fake:9200", auth="none"), mapping)
    source._client = make_client(transport)
    cfg = MigrationConfig(
        migration_id="os-mig", bucket=BUCKET, aws_region=REGION, pinecone_api_key_secret=ARN
    )
    store = ManifestStore(s3, BUCKET, "os-mig")
    out = io.StringIO()
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=Manifest(migration_id="os-mig"),
        store=store,
        events=EventBus("os-mig", [JsonLinesSink(out)]),
        s3=s3,
        source=source,
    )
    approve_ctx(ctx, tmp_path, fingerprint="os")
    ctx.manifest.mark_complete(
        Stage.PROVISION,
        osis_pipeline_name="pcm-os-mig",
        glue_job_name="pcmigrate-os-mig-opensearch-transform",
    )
    ctx.manifest.mark_complete(Stage.PREPARE_INDEX, host="h")
    body = pipeline_body(
        OsisPipelineSpec(
            migration_id="os-mig",
            endpoint="https://x",
            indices=(INDEX,),
            region=REGION,
            bucket=BUCKET,
            raw_prefix="os-mig/raw",
            pipeline_role_arn="arn:aws:iam::1:role/p",
        )
    )
    s3.put_object(Bucket=BUCKET, Key=f"os-mig/raw/{INDEX}/stale.ndjson", Body=b"{}\n")
    return {
        "ctx": ctx,
        "osis": FakeOsis(body),
        "glue": FakeGlue(s3),
        "out": out,
        "transport": transport,
    }


def _stage(setup: dict[str, Any], **kw: Any) -> OpenSearchExportStage:
    clock = {"t": 0.0}

    def sleep(seconds: float) -> None:
        clock["t"] += seconds

    def now() -> float:
        return clock["t"]

    return OpenSearchExportStage(
        osis=setup["osis"],
        glue=setup["glue"],
        scan=ScanSettings(poll_s=10, quiet_s=30, timeout_s=600, glue_poll_s=5),
        sleep=sleep,
        clock=now,
        **kw,
    )


def _grow_raw(s3: S3Client, setup: dict[str, Any], objects: int) -> None:
    for n in range(objects):
        s3.put_object(
            Bucket=BUCKET, Key=f"os-mig/raw/{INDEX}/events-{n}.ndjson", Body=b'{"_id": "x"}\n'
        )


def _grow_on_release(s3: S3Client, setup: dict[str, Any], objects: int) -> None:
    osis: FakeOsis = setup["osis"]
    original = osis.update

    def update(name: str, body: str) -> None:
        original(name, body)
        _grow_raw(s3, setup, objects)

    osis.update = update  # type: ignore[method-assign]


def test_t0_is_recorded_before_the_pipeline_is_released(
    s3: S3Client, setup: dict[str, Any]
) -> None:
    ctx: RunContext = setup["ctx"]
    osis: FakeOsis = setup["osis"]
    order: list[str] = []
    original_update = osis.update

    def update(name: str, body: str) -> None:
        order.append("update")
        assert "t0" in ctx.manifest.stage(Stage.EXPORT).outputs, (
            "T0 must precede the pipeline start"
        )
        original_update(name, body)
        _grow_raw(s3, setup, 3)

    osis.update = update  # type: ignore[method-assign]
    Pipeline([_stage(setup)]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))
    outputs = ctx.manifest.stage(Stage.EXPORT).outputs
    assert (
        order == ["update"]
        and outputs["osis_started"]
        and outputs["osis_stopped"]
        and osis.stopped == 1
    )
    assert ARMED_START_TIME not in osis.updates[0] and outputs["t0"] in osis.updates[0]
    assert outputs["source_counts"] == {INDEX: 1005}
    assert outputs["raw_objects"] == {INDEX: 3}
    assert not s3.list_objects_v2(Bucket=BUCKET, Prefix=f"os-mig/raw/{INDEX}/stale").get(
        "Contents"
    ), "stale raw cleared"
    assert outputs["rows_by_namespace"] == {INDEX: 999} and outputs["rejected_by_namespace"] == {
        INDEX: 5
    }
    assert outputs["parts_by_namespace"] == {
        INDEX: [f"os-mig/canonical/{INDEX}/p00000/part-00000.parquet"]
    }
    glue: FakeGlue = setup["glue"]
    (args,) = glue.runs.values()
    assert args["--mapping_uri"] == f"s3://{BUCKET}/os-mig/manifest/glue/mapping.yaml"
    assert args["--raw_uri"] == f"s3://{BUCKET}/os-mig/raw/{INDEX}/" and args["--index"] == INDEX
    assert args["--rejects_prefix"] == f"os-mig/rejects/{INDEX}/glue"
    assert s3.get_object(Bucket=BUCKET, Key="os-mig/manifest/glue/mapping.yaml")["Body"].read()
    events = setup["out"].getvalue()
    assert "T0 recorded" in events and "held 1005 at T0" in events


def test_resume_polls_the_glue_run_in_flight_instead_of_restarting(
    s3: S3Client, setup: dict[str, Any]
) -> None:
    ctx: RunContext = setup["ctx"]
    _grow_on_release(s3, setup, 2)
    glue: FakeGlue = setup["glue"]
    glue.states = ["RUNNING", "RUNNING", "RUNNING", "FAILED"]
    with pytest.raises(PipelineError, match="ended FAILED"):
        Pipeline([_stage(setup)]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))
    outputs = ctx.manifest.stage(Stage.EXPORT).outputs
    assert outputs["glue_runs"] == {INDEX: "jr_1"} and outputs["osis_stopped"]
    glue.states = ["SUCCEEDED"]
    resumed = _stage(setup)
    Pipeline([resumed]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))
    assert list(glue.runs) == ["jr_1"], "no second Glue run was started"
    assert setup["osis"].stopped == 1 and len(setup["osis"].updates) == 1
    assert ctx.manifest.is_complete(Stage.EXPORT)


def test_scan_timeout_and_pipeline_failure(s3: S3Client, setup: dict[str, Any]) -> None:
    ctx: RunContext = setup["ctx"]
    with pytest.raises(PipelineError, match="did not settle"):
        Pipeline([_stage(setup)]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))
    setup["osis"].statuses = ["UPDATE_FAILED"]
    ctx.manifest.stage(Stage.EXPORT).outputs.pop("osis_started", None)
    with pytest.raises(PipelineError, match="UPDATE_FAILED"):
        Pipeline([_stage(setup)]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))


def test_missing_provision_outputs_point_at_direct_mode(
    s3: S3Client, setup: dict[str, Any]
) -> None:
    ctx: RunContext = setup["ctx"]
    ctx.manifest.stage(Stage.PROVISION).outputs.clear()
    with pytest.raises(PipelineError, match="export_mode: direct"):
        Pipeline([_stage(setup)]).run(ctx, S3Lock(s3, BUCKET, "os-mig"))
