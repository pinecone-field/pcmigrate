"""The golden OpenSearch documents as Python, for unit tests that run without Docker.

Mirrors seed.py exactly (same seed, same shapes) so unit fixtures and the Docker index agree.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from tests.fixtures.golden.opensearch import seed

HERE = Path(__file__).parent


def golden_mapping() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((HERE / "index.json").read_text())
    return data


def golden_documents(dimension: int | None = None) -> list[dict[str, Any]]:
    rng = random.Random(42)
    docs = seed.documents(rng)
    if dimension is not None:
        for doc in docs:
            doc["embedding"] = doc["embedding"][:dimension]
    return docs
