"""Fetches shrink their batch when Pinecone says the response would be too large."""

from types import SimpleNamespace

import pytest

from pcmigrate.targets.pinecone_client import FETCH_BATCH, SdkGateway, fetch_in_batches


def test_fetch_in_batches_halves_on_too_large_and_covers_every_id() -> None:
    calls: list[int] = []

    def call(chunk: list[str]) -> None:
        if len(chunk) > 30:
            raise RuntimeError("ApiError: [400 OUT_OF_RANGE] Response is too large. To reduce ...")
        calls.append(len(chunk))

    ids = [f"id-{i}" for i in range(250)]
    fetch_in_batches(ids, call)
    assert sum(calls) == 250 and max(calls) <= 25, calls
    assert FETCH_BATCH == 100


def test_fetch_in_batches_raises_other_errors_unchanged() -> None:
    def call(chunk: list[str]) -> None:
        raise RuntimeError("[401] Unauthorized")

    with pytest.raises(RuntimeError, match="Unauthorized"):
        fetch_in_batches(["a", "b"], call)


def test_fetch_in_batches_gives_up_at_one_id() -> None:
    def call(chunk: list[str]) -> None:
        raise RuntimeError("Response is too large")

    with pytest.raises(RuntimeError, match="too large"):
        fetch_in_batches(["a", "b"], call, batch=4)


class FakeVectorIndex:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.calls: list[int] = []

    def fetch(self, ids: list[str], namespace: str) -> SimpleNamespace:
        if len(ids) > self.limit:
            raise RuntimeError("ApiError: [400 OUT_OF_RANGE] Response is too large.")
        self.calls.append(len(ids))
        return SimpleNamespace(
            vectors={i: SimpleNamespace(values=[0.5] * 4, metadata={"n": i}) for i in ids}
        )


def test_sdk_gateway_fetch_adapts_the_batch_for_a_vector_index() -> None:
    gateway = SdkGateway("pcsk_offline_test_key")
    index = FakeVectorIndex(limit=40)
    gateway._indexes["h"] = index
    gateway._schemas["h"] = None
    ids = [f"v{i}" for i in range(130)]
    out = gateway.fetch("h", "ns", ids)
    assert set(out) == set(ids) and out["v7"].metadata == {"n": "v7"}
    assert max(index.calls) <= 25 and sum(index.calls) == 130
