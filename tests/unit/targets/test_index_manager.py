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
