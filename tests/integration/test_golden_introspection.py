"""Introspection against the golden database: read-only guarantee and a snapshot of the output.

Set UPDATE_SNAPSHOTS=1 to rewrite tests/fixtures/golden/pgvector/expected/ from a run.
"""

import json
import os
from pathlib import Path

import psycopg
import pytest

from pcmigrate.config.mapping import Mapping
from pcmigrate.report.build import build_report
from pcmigrate.report.mapping_gen import MappingOptions
from pcmigrate.sources.pgvector.introspect import (
    READ_ONLY_VERBS,
    IntrospectOptions,
    PgvectorIntrospector,
)

pytestmark = pytest.mark.integration

RO_DSN = os.environ.get(
    "PGVECTOR_RO_DSN", "postgresql://pcmigrate_ro:pcmigrate_ro@localhost:5432/golden"
)
OWNER_DSN = os.environ.get("PGVECTOR_DSN", "postgresql://pcmigrate:pcmigrate@localhost:5432/golden")
EXPECTED = Path(__file__).resolve().parents[1] / "fixtures" / "golden" / "pgvector" / "expected"
OPTIONS = MappingOptions(migration_id="golden", index="golden-docs", region="us-east-1")


def _introspect(dsn: str) -> PgvectorIntrospector:
    return PgvectorIntrospector(dsn, IntrospectOptions(workload_window_s=0, sample_rows=500))


def test_read_only_role_runs_no_writes() -> None:
    introspector = _introspect(RO_DSN)
    inventory = introspector.inventory()
    introspector.close()
    assert inventory.read_only_session
    assert introspector.audit.statements
    for statement in introspector.audit.statements:
        assert statement.lstrip().split(None, 1)[0].upper() in READ_ONLY_VERBS, statement[:80]
    with psycopg.connect(OWNER_DSN) as conn:
        (writes,) = conn.execute(
            "SELECT count(*) FROM pg_stat_statements s JOIN pg_roles r ON r.oid = s.userid "
            "WHERE r.rolname = 'pcmigrate_ro' AND s.query !~* '^\\s*(select|with|set|show|explain)'"
        ).fetchone()
    assert writes == 0


def test_forced_rls_is_reported_as_a_blocker() -> None:
    introspector = _introspect(RO_DSN)
    bundle = build_report(introspector.inventory(), OPTIONS)
    introspector.close()
    tenants = bundle.inventory.table("public.tenants")
    assert tenants is not None and tenants.rls_hides_rows
    assert any(
        f.check == "rls_visibility" and f.object == "public.tenants" for f in bundle.findings
    )


def _snapshot(bundle) -> dict:  # type: ignore[no-untyped-def]
    return {
        "findings": [
            {"check": f.check, "severity": f.severity.value, "object": f.object, "field": f.field}
            for f in bundle.findings
        ],
        "tables": {
            t.qualified_name: {
                "relkind": t.relkind,
                "partitions": t.partitions,
                "rows": t.rows,
                "primary_key": t.primary_key,
                "vectors": [
                    [v.column, v.kind.value, v.dimension, v.metric.value, v.metric_index]
                    for v in t.vectors
                ],
                "text_search": [[x.column, x.gin_index] for x in t.text_search],
                "policies": [[p.name, p.classification.value, p.column] for p in t.policies],
                "duplicates": {k: d.estimated_duplicate_rows for k, d in t.duplicates.items()},
            }
            for t in bundle.inventory.tables
        },
    }


def test_golden_report_matches_snapshot() -> None:
    introspector = _introspect(OWNER_DSN)
    bundle = build_report(introspector.inventory(), OPTIONS)
    introspector.close()
    snapshot = _snapshot(bundle)
    mapping_yaml = bundle.mapping.to_yaml()
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        EXPECTED.mkdir(parents=True, exist_ok=True)
        (EXPECTED / "snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n")
        (EXPECTED / "mapping.yaml").write_text(mapping_yaml)
    expected = json.loads((EXPECTED / "snapshot.json").read_text())
    assert snapshot == expected
    assert Mapping.from_yaml(mapping_yaml) == Mapping.from_yaml(
        (EXPECTED / "mapping.yaml").read_text()
    )
    documents = bundle.inventory.table("public.documents")
    assert documents is not None and documents.duplicates["embedding"].estimated_duplicate_rows == 4
    events = bundle.inventory.table("public.events")
    assert events is not None and events.relkind == "p" and len(events.partitions) == 2
    assert events.dense is not None and events.dense.metric.value == "dotproduct"
