from fastapi.testclient import TestClient

from pcmigrate.core.buffer import InMemoryBuffer
from pcmigrate.writepath import BufferedIndex
from pcmigrate.writepath.http import create_app


def test_shim_buffers_upserts_and_deletes() -> None:
    buffer = InMemoryBuffer()
    index = BufferedIndex(buffer, default_namespace="documents")
    result = index.upsert(
        vectors=[
            {"id": "documents#1", "values": [1, 2], "metadata": {"a": "b"}},
            ("documents#2", [3.0, 4.0]),
            ("documents#3", [5.0, 6.0], {"c": 1}),
        ]
    )
    assert result == {"upserted_count": 3}
    index.delete(ids=["documents#1"], namespace="other")
    sent = buffer.sent
    assert [e.op for e in sent] == ["upsert", "upsert", "upsert", "delete"]
    assert sent[0].namespace == "documents" and sent[3].namespace == "other"
    assert sent[0].values == [1.0, 2.0] and sent[2].metadata == {"c": 1}
    assert all(e.origin == "dual_write" and not e.needs_reread for e in sent[:3])
    import pytest

    with pytest.raises(NotImplementedError):
        index.delete(delete_all=True)


def test_http_endpoint_requires_token_and_buffers() -> None:
    buffer = InMemoryBuffer()
    client = TestClient(create_app(buffer, token="secret"))
    assert client.post("/vectors/upsert", json={"vectors": [], "namespace": "d"}).status_code == 401
    response = client.post(
        "/vectors/upsert",
        headers={"Api-Key": "secret"},
        json={
            "namespace": "documents",
            "vectors": [{"id": "documents#9", "values": [0.1, 0.2], "metadata": {"k": "v"}}],
        },
    )
    assert response.status_code == 200 and response.json() == {"upsertedCount": 1}
    response = client.post(
        "/vectors/delete",
        headers={"Authorization": "Bearer secret"},
        json={"namespace": "documents", "ids": ["documents#8"]},
    )
    assert response.status_code == 200
    assert [e.op for e in buffer.sent] == ["upsert", "delete"]
    assert client.get("/healthz").json() == {"ok": True, "depth": 2}
