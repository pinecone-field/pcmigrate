"""Seed the golden OpenSearch index.

Runs inside the `opensearch-seed` compose service with only the standard library,
so it must not import anything outside Python 3.12's stdlib.
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from corpus import body_text, summary_text

URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
INDEX = "golden-documents"
DIMENSION = 768
DOC_COUNT = 1000
DUPLICATE_VECTOR_COUNT = 5
CATEGORIES = ["news", "blog", "paper", "manual"]
ROLES = ["editor", "reviewer", "admin"]
SPARSE_VOCABULARY = [f"term{n:03d}" for n in range(200)]
SPARSE_TERMS = 6


def request(method: str, path: str, body: bytes | None = None) -> dict:
    req = urllib.request.Request(
        f"{URL}{path}", data=body, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read() or b"{}")


def wait_for_cluster(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            health = request("GET", "/_cluster/health")
            if health.get("status") in ("green", "yellow"):
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(3)
    sys.exit(f"OpenSearch at {URL} did not become healthy within {timeout_s}s")


def vector(rng: random.Random) -> list[float]:
    return [round(rng.random(), 6) for _ in range(DIMENSION)]


def sparse(rng: random.Random) -> dict[str, float]:
    tokens = rng.sample(SPARSE_VOCABULARY, SPARSE_TERMS)
    return {token: round(rng.uniform(0.1, 3.0), 4) for token in sorted(tokens)}


def documents(rng: random.Random) -> list[dict]:
    docs = []
    for i in range(1, DOC_COUNT + 1):
        doc: dict = {
            "doc_id": f"doc-{i:05d}",
            "title": f"Document {i}",
            "body": body_text(i),
            "category": CATEGORIES[i % len(CATEGORIES)],
            "views": i * 3,
            "author": {
                "name": f"author-{i % 13}",
                "email": f"author{i % 13}@example.com",
                "roles": ROLES[: 1 + i % len(ROLES)],
            },
            "embedding": vector(rng),
            "sparse": sparse(rng),
        }
        if i % 9 != 0:
            doc["summary"] = summary_text(i)
        if i % 11 != 0:
            doc["published_at"] = f"2024-{1 + i % 12:02d}-{1 + i % 28:02d}T00:00:00Z"
        if i % 7 == 0:
            del doc["category"]
        docs.append(doc)
    shared = vector(rng)
    for i in range(1, DUPLICATE_VECTOR_COUNT + 1):
        docs.append(
            {
                "doc_id": f"dup-{i}",
                "title": f"Duplicate vector {i}",
                "body": "same vector as its siblings",
                "category": "dup",
                "views": 0,
                "author": {"name": "dup-author", "email": "dup@example.com", "roles": ["editor"]},
                "embedding": shared,
                "sparse": sparse(rng),
            }
        )
    return docs


def main() -> None:
    wait_for_cluster()
    try:
        request("HEAD", f"/{INDEX}")
        print(f"index {INDEX} already exists; nothing to do")
        return
    except urllib.error.HTTPError as err:
        if err.code != 404:
            raise

    mapping = (Path(__file__).parent / "index.json").read_bytes()
    request("PUT", f"/{INDEX}", mapping)

    rng = random.Random(42)
    lines = []
    for doc in documents(rng):
        lines.append(json.dumps({"index": {"_index": INDEX, "_id": doc["doc_id"]}}))
        lines.append(json.dumps(doc))
    body = ("\n".join(lines) + "\n").encode()
    result = request("POST", "/_bulk?refresh=wait_for", body)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in item["index"]]
        sys.exit(f"bulk indexing reported {len(failed)} failures: {failed[:3]}")

    count = request("GET", f"/{INDEX}/_count")["count"]
    print(f"seeded {count} documents into {INDEX}")


if __name__ == "__main__":
    main()
