"""Change capture against the golden Docker database, per tier, with S3 and Pinecone faked.

Each tier migrates a scratch copy of `documents` while a writer thread inserts, updates, and
deletes rows throughout export and import. After the applier drains and reconcile runs, the id
diff is zero and every record's fields equal the source. The tier-none run asserts no buffer is
provisioned, sync is skipped, and the pre-cutover id diff reports rows written after the snapshot.
"""

import io
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import psycopg
import pytest
from moto import mock_aws

from pcmigrate.config import Mapping, MigrationConfig
from pcmigrate.core.buffer import InMemoryBuffer
from pcmigrate.core.events import EventBus, JsonLinesSink
from pcmigrate.core.stages import Pipeline, RunContext
from pcmigrate.core.stages.cutover import CutoverStage
from pcmigrate.core.stages.emit import EmitStage
from pcmigrate.core.stages.export import ExportStage
from pcmigrate.core.stages.import_ import ImportStage
from pcmigrate.core.stages.prepare_index import PrepareIndexStage
from pcmigrate.core.stages.provision import ProvisionStage
from pcmigrate.core.stages.sync import SyncStage
from pcmigrate.core.stages.transform import TransformStage
from pcmigrate.core.stages.validate import ValidateStage
from pcmigrate.core.state import Manifest, ManifestStore, S3Lock, Stage
from pcmigrate.sources.pgvector.export import PgvectorSource
from pcmigrate.targets.emitters import ImportLimits
from pcmigrate.targets.pinecone_client import VectorRecord
from pcmigrate.validate.fidelity import check_fidelity
from pcmigrate.validate.id_diff import check_id_diff
from tests.unit.conftest import approve_ctx
from tests.unit.fakes import FakeGateway

pytestmark = [pytest.mark.integration, pytest.mark.slow]

DSN = os.environ.get("PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden")
BUCKET = "pcmigrate-cdc-test"
REGION = "us-east-1"
ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"
HOST = "cdc-idx.fake.pinecone.io"


@pytest.fixture
def scratch_table() -> Iterator[str]:
    name = f"cdc_{uuid.uuid4().hex[:6]}"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            f"CREATE TABLE {name} (id bigserial PRIMARY KEY, title text NOT NULL, category text, "
            f"updated_at timestamptz NOT NULL DEFAULT now(), embedding vector(768) NOT NULL)"
        )
        conn.execute(
            f"INSERT INTO {name} (title, category, embedding) "
            f"SELECT title, category, embedding FROM documents WHERE id <= 300"
        )
        conn.execute(f"CREATE INDEX ON {name} USING hnsw (embedding vector_cosine_ops)")
    try:
        yield name
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f"DROP TRIGGER IF EXISTS trg_pcmigrate_{name} ON {name}")
            conn.execute(f"DROP FUNCTION IF EXISTS public.pcmigrate_log_{name}()")
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'pcm_cdc%'"
            )
            conn.execute("DROP PUBLICATION IF EXISTS pcmigrate_pub")
            conn.execute(f"DROP TABLE IF EXISTS {name}")
            conn.execute("DROP TABLE IF EXISTS pinecone_changelog")


def mapping_for(table: str, tier: str) -> Mapping:
    return Mapping.model_validate(
        {
            "migration_id": f"cdc-{table[4:]}",
            "source": {"kind": "pgvector"},
            "target": {"index": "cdc-idx", "region": REGION},
            "objects": [
                {
                    "source": f"public.{table}",
                    "namespace": "docs",
                    "id_template": "docs#{id}",
                    "dense": {"column": "embedding", "dimension": 768, "metric": "cosine"},
                    "metadata": [
                        {"source": "title", "type": "string"},
                        {"source": "category", "type": "string"},
                        {"source": "updated_at", "type": "epoch_seconds"},
                    ],
                }
            ],
            "validation": {
                "recall_at_k": {"k": 5, "queries": 10, "threshold": 0.9},
                "fidelity_sample": 50,
            },
            "change_capture": {"tier": tier, "watermark_column": "updated_at"},
        }
    )


class Writer(threading.Thread):
    """Inserts, updates, and deletes against the scratch table until stopped."""

    def __init__(self, table: str) -> None:
        super().__init__(daemon=True)
        self.table = table
        self.stop = threading.Event()
        self.inserted: list[int] = []
        self.updated: list[int] = []
        self.deleted: list[int] = []

    def run(self) -> None:
        with psycopg.connect(DSN, autocommit=True) as conn:
            tick = 0
            while not self.stop.is_set():
                tick += 1
                row = conn.execute(
                    f"INSERT INTO {self.table} (title, category, embedding) "
                    f"SELECT 'live ' || %s, 'live', embedding FROM documents WHERE id = %s RETURNING id",
                    [tick, 500 + tick],
                ).fetchone()
                if row:
                    self.inserted.append(int(row[0]))
                victim = 1 + (tick * 7) % 300
                conn.execute(
                    f"UPDATE {self.table} SET title = 'updated ' || %s, updated_at = now() WHERE id = %s",
                    [tick, victim],
                )
                self.updated.append(victim)
                if tick % 3 == 0:
                    target = 1 + (tick * 11) % 300
                    conn.execute(f"DELETE FROM {self.table} WHERE id = %s", [target])
                    self.deleted.append(target)
                time.sleep(0.05)


class FakeDriver:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def up(self, ctx: RunContext, on_output: Callable[[str], None]) -> dict[str, Any]:
        outputs: dict[str, Any] = {
            "bucket_name": BUCKET,
            "kms_key_arn": "arn:k",
            "runner_role_arn": "arn:r",
            "secret_arns": {},
        }
        if self.enabled:
            outputs["buffer_queue_url"] = "https://sqs/fake.fifo"
        return outputs

    def destroy(self, ctx: RunContext, on_output: Callable[[str], None]) -> None:
        pass


def _ctx(
    s3,
    source: PgvectorSource,
    gw: FakeGateway,
    mapping: Mapping,
    tmp_path: Path,
    buffer: InMemoryBuffer | None,
):  # type: ignore[no-untyped-def]
    cfg = MigrationConfig(
        migration_id=mapping.migration_id,
        bucket=BUCKET,
        aws_region=REGION,
        pinecone_api_key_secret=ARN,
        canonical_part_bytes=8 * 1024 * 1024,
        partition_rows=1000,
        workers=2,
    )
    store = ManifestStore(s3, BUCKET, cfg.migration_id)
    ctx = RunContext(
        config=cfg,
        mapping=mapping,
        manifest=store.load() or Manifest(migration_id=cfg.migration_id),
        store=store,
        events=EventBus(cfg.migration_id, [JsonLinesSink(io.StringIO())]),
        s3=s3,
        source=source,
        target=gw,
    )
    if buffer is not None:
        ctx.extras["buffer"] = buffer
    approve_ctx(ctx, tmp_path, fingerprint="cdc")
    return ctx


def _land_import(gw: FakeGateway, s3, ctx: RunContext) -> None:  # type: ignore[no-untyped-def]
    """The fake import 'lands' the emitted import files by reading the clean canonical parts."""
    from pcmigrate.core.canonical import read_canonical

    parts = ctx.manifest.stage(Stage.TRANSFORM).outputs["parts_by_namespace"]["docs"]
    records = list(read_canonical(s3, BUCKET, parts))
    gw.pending_records = {
        "docs": {r.id: VectorRecord(r.id, r.dense or [], r.metadata) for r in records}
    }
    gw.import_records = len(records)


@pytest.mark.parametrize("tier", ["watermark", "trigger_outbox", "logical"])
def test_tier_converges_under_concurrent_writes(
    tier: str, scratch_table: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    mapping = mapping_for(scratch_table, tier)
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        gw = FakeGateway()
        buffer = InMemoryBuffer()
        ctx = _ctx(s3, source, gw, mapping, tmp_path, buffer)
        writer = Writer(scratch_table)
        writer.start()
        try:
            Pipeline(
                [
                    ProvisionStage(FakeDriver(True)),
                    PrepareIndexStage(),
                    ExportStage(use_processes=False),
                    TransformStage(),
                    EmitStage(ImportLimits()),
                ]
            ).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
            assert ctx.manifest.stage(Stage.EXPORT).outputs["feed_installed"]["tier"] == tier
            if tier == "trigger_outbox":
                assert "outbox_seq" in ctx.manifest.stage(Stage.EXPORT).outputs["change_boundary"]
            if tier == "logical":
                assert (
                    ctx.manifest.stage(Stage.EXPORT).outputs["snapshots"][0]["source"]
                    == "replication_slot"
                )
            _land_import(gw, s3, ctx)
            Pipeline(
                [
                    ImportStage(poll_interval_s=0),
                    ValidateStage(settle_window_s=10, poll_interval_s=0, checks=("counts",)),
                ]
            ).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
            time.sleep(1.0)
        finally:
            writer.stop.set()
            writer.join(timeout=10)
        assert writer.inserted and writer.updated and writer.deleted, (
            "the writer must have changed rows during the load"
        )

        diverged = check_id_diff(gw, HOST, source, "docs")
        assert not diverged.passed, "changes during export and import must show up before sync"

        Pipeline([SyncStage(max_rounds=6, idle_sleep_s=0.1)]).run(
            ctx, S3Lock(s3, BUCKET, mapping.migration_id)
        )
        totals = ctx.manifest.stage(Stage.SYNC).outputs
        assert totals["forwarded"] > 0 and totals["applied"] > 0

        Pipeline([CutoverStage()]).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
        cut = ctx.manifest.stage(Stage.CUTOVER).outputs
        assert cut["feed_removed"] == tier and cut["reconcile"]["docs"]["converged"]

        final = check_id_diff(gw, HOST, source, "docs")
        assert final.passed, final.to_dict()
        fidelity = check_fidelity(gw, HOST, source, "docs", sample=60)
        assert fidelity.vectors_passed and fidelity.metadata_passed, fidelity.to_dict()
        with psycopg.connect(DSN) as conn:
            (count,) = conn.execute(f"SELECT count(*) FROM {scratch_table}").fetchone()
        assert len(gw.data[HOST]["docs"]) == count
        if tier == "trigger_outbox":
            with psycopg.connect(DSN) as conn:
                (triggers,) = conn.execute(
                    "SELECT count(*) FROM pg_trigger WHERE tgname = %s",
                    [f"trg_pcmigrate_{scratch_table}"],
                ).fetchone()
            assert triggers == 0, "cutover must remove the trigger"
        if tier == "logical":
            with psycopg.connect(DSN) as conn:
                (slots,) = conn.execute("SELECT count(*) FROM pg_replication_slots").fetchone()
            assert slots == 0, "cutover must drop the slot"


def test_tier_none_skips_sync_and_reports_late_rows(
    scratch_table: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    mapping = mapping_for(scratch_table, "none")
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        source = PgvectorSource(DSN, mapping, target_rows_per_partition=1000)
        gw = FakeGateway()
        ctx = _ctx(s3, source, gw, mapping, tmp_path, None)
        Pipeline(
            [
                ProvisionStage(FakeDriver(False)),
                PrepareIndexStage(),
                ExportStage(use_processes=False),
                TransformStage(),
                EmitStage(ImportLimits()),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
        assert "buffer_queue_url" not in ctx.manifest.stage(Stage.PROVISION).outputs
        assert "feed_installed" not in ctx.manifest.stage(Stage.EXPORT).outputs
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                f"INSERT INTO {scratch_table} (title, category, embedding) SELECT 'late', 'late', embedding FROM documents WHERE id BETWEEN 900 AND 904"
            )
        _land_import(gw, s3, ctx)
        Pipeline(
            [
                ImportStage(poll_interval_s=0),
                ValidateStage(settle_window_s=10, poll_interval_s=0, checks=("counts",)),
                SyncStage(),
            ]
        ).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
        assert ctx.manifest.stage(Stage.SYNC).outputs["skipped"] is True
        pre_cutover = check_id_diff(gw, HOST, source, "docs")
        assert len(pre_cutover.missing) == 5 and not pre_cutover.passed
        Pipeline([CutoverStage()]).run(ctx, S3Lock(s3, BUCKET, mapping.migration_id))
        cut = ctx.manifest.stage(Stage.CUTOVER).outputs
        assert "final_drain" not in cut and cut["reconcile"]["docs"]["upserted"] == 5
        assert check_id_diff(gw, HOST, source, "docs").passed
