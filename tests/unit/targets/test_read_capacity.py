"""Dedicated read nodes: config, gateway parsing, index manager create/adopt/wait, findings, CLI."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from pcmigrate.config.mapping import DenseSpec, Mapping, Metric, ReadNodesSpec, TargetSpec
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions, generate_mapping
from pcmigrate.targets.index_manager import (
    IndexManager,
    IndexSpecMismatchError,
    ReadCapacityError,
)
from pcmigrate.targets.pinecone_client import IndexInfo, ReadCapacity, read_capacity_from
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.fakes import FakeGateway
from tests.unit.report.fixtures import inventory

DENSE = DenseSpec(column="embedding", dimension=768, metric=Metric.COSINE)
DEDICATED = TargetSpec(
    index="acme-docs",
    region="us-east-1",
    capacity="dedicated",
    read_nodes=ReadNodesSpec(node_type="t1", shards=2, replicas=3),
)
OPTIONS = MappingOptions(migration_id="acme", index="acme-docs", region="us-east-1")


def test_read_nodes_spec_and_capacity_agree() -> None:
    assert DEDICATED.read_capacity() == {
        "mode": "Dedicated",
        "dedicated": {
            "node_type": "t1",
            "scaling": "Manual",
            "manual": {"shards": 2, "replicas": 3},
        },
    }
    assert TargetSpec(index="x", region="us-east-1").read_capacity() is None
    with pytest.raises(ValueError, match=r"needs target\.read_nodes"):
        TargetSpec(index="x", region="us-east-1", capacity="dedicated")
    with pytest.raises(ValueError, match="only valid with"):
        TargetSpec(index="x", region="us-east-1", read_nodes=ReadNodesSpec())
    with pytest.raises(ValueError):
        ReadNodesSpec(shards=0)
    with pytest.raises(ValueError):
        ReadNodesSpec(node_type="c1")  # type: ignore[arg-type]


def test_mapping_yaml_round_trips_read_nodes() -> None:
    text = EXAMPLE.replace(
        "  capacity: on_demand\n",
        "  capacity: dedicated\n  read_nodes:\n    node_type: b1\n    shards: 1\n    replicas: 2\n",
    )
    mapping = Mapping.from_yaml(text)
    assert mapping.target.read_nodes == ReadNodesSpec(node_type="b1", shards=1, replicas=2)
    assert Mapping.from_yaml(mapping.to_yaml()) == mapping
    assert "read_nodes:" in mapping.to_yaml()


def test_read_capacity_parsing_from_dicts_and_objects() -> None:
    body = {
        "read_capacity": {
            "mode": "Dedicated",
            "dedicated": {
                "node_type": "b1",
                "scaling": "Manual",
                "manual": {"shards": 1, "replicas": 2},
            },
            "status": {"state": "Scaling", "current_replicas": 1, "current_shards": 1},
        }
    }
    parsed = read_capacity_from(body)
    assert parsed == ReadCapacity("Dedicated", "b1", 1, 2, "Scaling", None)
    assert parsed.dedicated and not parsed.settled
    model = SimpleNamespace(
        read_capacity=SimpleNamespace(
            mode="OnDemand",
            dedicated=None,
            status=SimpleNamespace(state="Ready", error_message=None),
        )
    )
    on_demand = read_capacity_from(model)
    assert on_demand == ReadCapacity("OnDemand", None, None, None, "Ready", None)
    assert on_demand.matches(None) and on_demand.matches({"mode": "OnDemand"})
    assert not on_demand.matches(DEDICATED.read_capacity())
    assert read_capacity_from({"read_capacity": None}) is None
    sdk_shape = SimpleNamespace(
        read_capacity=SimpleNamespace(
            dedicated=SimpleNamespace(
                node_type="b1", scaling="Manual", manual=SimpleNamespace(shards=1, replicas=1)
            ),
            status=SimpleNamespace(state="Migrating", error_message=None),
        )
    )
    inferred = read_capacity_from(sdk_shape)
    assert inferred.dedicated and inferred.state == "Migrating" and not inferred.settled
    ready = ReadCapacity("Dedicated", "t1", 2, 3, "Ready")
    assert ready.matches(DEDICATED.read_capacity())
    assert not ready.matches(ReadNodesSpec(node_type="t1", shards=2, replicas=1).read_capacity())


def test_create_passes_read_capacity_and_waits_for_ready() -> None:
    gw = FakeGateway(read_capacity_states=["Scaling", "Scaling", "Ready"])
    slept: list[float] = []
    manager = IndexManager(gw, sleep=slept.append, clock=lambda: 0.0, poll_interval_s=5.0)
    info = manager.prepare(DEDICATED, DENSE, ["documents"])
    assert gw.create_calls[0]["read_capacity"] == DEDICATED.read_capacity()
    assert info.read_capacity is not None and info.read_capacity.state == "Ready"
    assert slept == [5.0, 5.0], "polled twice before Ready"
    on_demand = IndexManager(FakeGateway()).prepare(
        TargetSpec(index="plain", region="us-east-1"), DENSE, ["documents"]
    )
    assert on_demand.read_capacity is None


def test_wait_times_out_or_errors() -> None:
    gw = FakeGateway(read_capacity_states=["Scaling"] * 50)
    ticks = iter(range(0, 10_000, 100))
    manager = IndexManager(
        gw, sleep=lambda s: None, clock=lambda: float(next(ticks)), read_capacity_timeout_s=300
    )
    with pytest.raises(ReadCapacityError, match="still Scaling"):
        manager.prepare(DEDICATED, DENSE, ["documents"])
    gw = FakeGateway(read_capacity_states=["Error"])
    with pytest.raises(ReadCapacityError, match="errored"):
        IndexManager(gw, sleep=lambda s: None).prepare(
            DEDICATED.model_copy(update={"index": "bad"}), DENSE, ["documents"]
        )


def test_adopt_checks_read_capacity_against_the_plan() -> None:
    gw = FakeGateway()
    gw.indexes["acme-docs"] = IndexInfo(
        "acme-docs", "h", 768, "cosine", "aws", "us-east-1", True, ReadCapacity("OnDemand")
    )
    adopt = DEDICATED.model_copy(update={"mode": "adopt"})
    with pytest.raises(
        IndexSpecMismatchError, match="read capacity is on-demand, mapping says dedicated t1"
    ):
        IndexManager(gw).prepare(adopt, DENSE, ["documents"])
    gw.indexes["acme-docs"] = IndexInfo(
        "acme-docs",
        "h",
        768,
        "cosine",
        "aws",
        "us-east-1",
        True,
        ReadCapacity("Dedicated", "t1", 2, 3, "Ready"),
    )
    IndexManager(gw).prepare(adopt, DENSE, ["documents"])
    plain_adopt = TargetSpec(index="acme-docs", region="us-east-1", mode="adopt")
    with pytest.raises(IndexSpecMismatchError, match="mapping says on-demand"):
        IndexManager(gw).prepare(plain_adopt, DENSE, ["documents"])


def test_generate_mapping_and_findings_for_dedicated() -> None:
    options = replace(OPTIONS, capacity="dedicated", node_type="t1", shards=1, replicas=2)
    mapping = generate_mapping(inventory(), options)
    assert mapping.target.capacity == "dedicated"
    assert mapping.target.read_nodes == ReadNodesSpec(node_type="t1", shards=1, replicas=2)
    bundle = build_report(inventory(), options)
    checks = {f.check: f for f in bundle.findings}
    assert checks["dedicated_namespaces"].severity.value == "blocker", (
        "two tables -> two namespaces"
    )
    assert "2 namespaces" in checks["dedicated_namespaces"].message
    assert checks["dedicated_read_nodes"].severity.value == "info"
    assert "t1, 1 shard(s) x 2 replica(s) = 2 node(s)" in checks["dedicated_read_nodes"].message
    assert "Read capacity: **dedicated** (t1, 1 shard(s) x 2 replica(s)" in bundle.report_md
    single = build_report(inventory(inventory().tables[0]), options)
    assert not any(f.check == "dedicated_namespaces" for f in single.findings)
    assert not any(f.check == "import_size" for f in single.findings)
    on_demand = build_report(inventory(), OPTIONS)
    assert not any(f.check.startswith("dedicated") for f in on_demand.findings)
    assert "Read capacity: **on_demand**." in on_demand.report_md


def test_shard_warning_from_sizing() -> None:
    from pcmigrate.report.findings import FindingsContext, check_dedicated_read_nodes
    from pcmigrate.report.sizing import SizingPlan, TableSizing

    options = replace(OPTIONS, capacity="dedicated")
    inv = inventory(inventory().tables[0])
    mapping = generate_mapping(inv, options)
    big = SizingPlan(
        tables=[
            TableSizing(
                object="public.documents",
                namespace="documents",
                rows=1,
                dimension=768,
                metadata_bytes_per_row=1,
                canonical_bytes=1,
                import_bytes=600 * 1024**3,
                import_files=1,
                export_eta_s=None,
            )
        ]
    )
    findings = check_dedicated_read_nodes(
        FindingsContext(inventory=inv, mapping=mapping, sizing=big)
    )
    shards = next(f for f in findings if f.check == "dedicated_shards")
    assert "at least 3 shard(s)" in shards.message and shards.severity.value == "warning"
