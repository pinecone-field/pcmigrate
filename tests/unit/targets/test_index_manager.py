import pytest

from pcmigrate.config.mapping import DenseSpec, Metric, TargetSpec
from pcmigrate.targets.index_manager import (
    IndexManager,
    IndexNotFoundError,
    IndexSpecMismatchError,
    NamespaceOccupiedError,
)
from pcmigrate.targets.pinecone_client import IndexInfo
from tests.unit.fakes import FakeGateway

TARGET = TargetSpec(index="acme-docs", region="us-east-1")
DENSE = DenseSpec(column="embedding", dimension=768, metric=Metric.COSINE)


def test_create_mode_creates_and_is_idempotent() -> None:
    gw = FakeGateway()
    manager = IndexManager(gw)
    info = manager.prepare(TARGET, DENSE, ["documents"])
    assert gw.created == ["acme-docs"]
    assert info.dimension == 768 and info.metric == "cosine"
    again = manager.prepare(TARGET, DENSE, ["documents"])
    assert again == info and gw.created == ["acme-docs"]


def test_adopt_requires_existing_index() -> None:
    with pytest.raises(IndexNotFoundError):
        IndexManager(FakeGateway()).prepare(TARGET.model_copy(update={"mode": "adopt"}), DENSE, [])


def test_adopt_verifies_dimension_metric_cloud_region() -> None:
    gw = FakeGateway()
    gw.indexes["acme-docs"] = IndexInfo(
        "acme-docs", "h", 1024, "euclidean", "gcp", "us-central1", True
    )
    with pytest.raises(IndexSpecMismatchError) as info:
        IndexManager(gw).prepare(TARGET.model_copy(update={"mode": "adopt"}), DENSE, [])
    message = str(info.value)
    assert "dimension 1024 != 768" in message
    assert "metric 'euclidean' != 'cosine'" in message
    assert "cloud 'gcp' != 'aws'" in message
    assert "region" in message


def test_adopt_rejects_occupied_namespace() -> None:
    gw = FakeGateway()
    gw.indexes["acme-docs"] = IndexInfo("acme-docs", "h", 768, "cosine", "aws", "us-east-1", True)
    gw.namespaces["h"] = {"documents": 10, "other": 1}
    with pytest.raises(NamespaceOccupiedError, match=r"\['documents'\]"):
        IndexManager(gw).prepare(
            TARGET.model_copy(update={"mode": "adopt"}), DENSE, ["documents", "new"]
        )
    IndexManager(gw).prepare(TARGET.model_copy(update={"mode": "adopt"}), DENSE, ["new"])


def test_reserved_vector_fields_are_a_plain_vector_index() -> None:
    from types import SimpleNamespace

    from pcmigrate.targets.pinecone_client import document_schema_from

    reserved = SimpleNamespace(
        fields={
            "_values": {"type": "dense_vector", "dimension": 3072, "metric": "dotproduct"},
            "_sparse_values": {"type": "sparse_vector"},
        }
    )
    assert document_schema_from(reserved) is None
    assert document_schema_from(SimpleNamespace(fields={"values": {"dimension": 8}})) is None
    named = SimpleNamespace(
        fields={
            "dense": {"type": "dense_vector", "dimension": 8, "metric": "cosine"},
            "sparse": {"type": "sparse_vector"},
        }
    )
    schema = document_schema_from(named)
    assert schema is not None and schema.dense_field == "dense"


def test_adopt_byoc_index_checks_environment_not_region() -> None:
    byoc = TargetSpec(
        index="acme-docs",
        region="us-west-2",
        mode="adopt",
        deployment="byoc",
        environment="aws-us-west-2-fdf3.byoc",
    )
    gw = FakeGateway()
    gw.indexes["acme-docs"] = IndexInfo(
        "acme-docs",
        "h",
        768,
        "cosine",
        None,
        None,
        True,
        deployment="byoc",
        environment="aws-us-west-2-fdf3.byoc",
    )
    assert IndexManager(gw).prepare(byoc, DENSE, ["documents"]).environment == byoc.environment
    gw.indexes["acme-docs"] = IndexInfo(
        "acme-docs",
        "h",
        768,
        "cosine",
        None,
        None,
        True,
        deployment="byoc",
        environment="aws-us-east-1-0000.byoc",
    )
    with pytest.raises(
        IndexSpecMismatchError, match=r"BYOC environment 'aws-us-east-1-0000\.byoc'"
    ):
        IndexManager(gw).prepare(byoc, DENSE, [])
    with pytest.raises(IndexSpecMismatchError, match=r"mapping says target\.deployment managed"):
        IndexManager(gw).prepare(TARGET.model_copy(update={"mode": "adopt"}), DENSE, [])
    gw.indexes["acme-docs"] = IndexInfo("acme-docs", "h", 768, "cosine", "aws", "us-east-1", True)
    with pytest.raises(
        IndexSpecMismatchError, match=r"managed deployment, mapping says target\.deployment byoc"
    ):
        IndexManager(gw).prepare(byoc, DENSE, [])


def test_create_mode_passes_the_byoc_environment() -> None:
    byoc = TargetSpec(
        index="acme-byoc",
        region="us-west-2",
        deployment="byoc",
        environment="aws-us-west-2-fdf3.byoc",
    )
    gw = FakeGateway()
    info = IndexManager(gw).prepare(byoc, DENSE, ["documents"])
    assert gw.create_calls[-1]["environment"] == "aws-us-west-2-fdf3.byoc"
    assert info.deployment == "byoc" and info.cloud is None
    assert IndexManager(gw).prepare(byoc, DENSE, ["documents"]) == info


def test_byoc_target_needs_an_environment() -> None:
    with pytest.raises(ValueError, match=r"needs target\.environment"):
        TargetSpec(index="x", region="us-west-2", deployment="byoc")
    with pytest.raises(ValueError, match=r"only valid with target\.deployment byoc"):
        TargetSpec(index="x", region="us-west-2", environment="aws-us-west-2-fdf3.byoc")
