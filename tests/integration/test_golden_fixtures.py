"""Checks that `docker compose up` seeded both golden sources as the design requires."""

import json
import os
import urllib.request

import psycopg
import pytest

pytestmark = pytest.mark.integration

PGVECTOR_DSN = os.environ.get(
    "PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden"
)
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")


@pytest.fixture(scope="module")
def pg() -> psycopg.Connection:
    with psycopg.connect(PGVECTOR_DSN) as conn:
        yield conn


def test_documents_table(pg: psycopg.Connection) -> None:
    row = pg.execute(
        "SELECT atttypmod FROM pg_attribute"
        " WHERE attrelid = 'documents'::regclass AND attname = 'embedding'"
    ).fetchone()
    assert row is not None and row[0] == 768
    (count,) = pg.execute("SELECT count(*) FROM documents").fetchone()
    assert count == 1005
    (dups,) = pg.execute(
        "SELECT max(n) FROM (SELECT count(*) AS n FROM documents GROUP BY embedding) s"
    ).fetchone()
    assert dups == 5
    (nulls,) = pg.execute("SELECT count(*) FROM documents WHERE category IS NULL").fetchone()
    assert nulls > 0
    opclass = pg.execute(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'documents_embedding_cosine_idx'"
    ).fetchone()
    assert opclass is not None and "vector_cosine_ops" in opclass[0]


def test_products_table(pg: psycopg.Connection) -> None:
    row = pg.execute(
        "SELECT atttypmod FROM pg_attribute"
        " WHERE attrelid = 'products'::regclass AND attname = 'embedding'"
    ).fetchone()
    assert row is not None and row[0] == 384
    price_type = pg.execute(
        "SELECT data_type FROM information_schema.columns"
        " WHERE table_name = 'products' AND column_name = 'price'"
    ).fetchone()
    assert price_type is not None and price_type[0] == "numeric"
    opclass = pg.execute(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'products_embedding_l2_idx'"
    ).fetchone()
    assert opclass is not None and "vector_l2_ops" in opclass[0]


def test_tenants_rls_policy(pg: psycopg.Connection) -> None:
    policies = pg.execute(
        "SELECT policyname FROM pg_policies WHERE tablename = 'tenants'"
    ).fetchall()
    assert ("tenants_isolation",) in policies
    (rls,) = pg.execute("SELECT relrowsecurity FROM pg_class WHERE relname = 'tenants'").fetchone()
    assert rls is True


def test_opensearch_index() -> None:
    with urllib.request.urlopen(f"{OPENSEARCH_URL}/golden-documents/_mapping") as resp:
        mapping = json.load(resp)["golden-documents"]["mappings"]["properties"]
    assert mapping["embedding"]["type"] == "knn_vector"
    assert mapping["embedding"]["dimension"] == 768
    assert mapping["author"]["type"] == "nested"
    assert mapping["title"]["type"] == "text"
    with urllib.request.urlopen(f"{OPENSEARCH_URL}/golden-documents/_count") as resp:
        assert json.load(resp)["count"] == 1005
