import pytest

from pcmigrate.sources.pgvector.models import RlsClass
from pcmigrate.sources.pgvector.rls import classify_qual

COLS = ["id", "tenant_id", "visibility", "region", "status", "allowed_groups", "owner"]


@pytest.mark.parametrize(
    ("qual", "expected", "column"),
    [
        (
            "(tenant_id = current_setting('app.tenant_id'::text, true))",
            RlsClass.NAMESPACE,
            "tenant_id",
        ),
        (
            "(tenant_id = (current_setting('app.tenant'::text))::uuid)",
            RlsClass.NAMESPACE,
            "tenant_id",
        ),
        ("(status = 'active'::text)", RlsClass.FILTER, "status"),
        (
            "(visibility = ANY (ARRAY['public'::text, (CURRENT_USER)::text]))",
            RlsClass.FILTER,
            "visibility",
        ),
        (
            "(allowed_groups && current_setting('app.groups')::text[])",
            RlsClass.FILTER,
            "allowed_groups",
        ),
        ("((region = 'eu'::text) AND (status = 'active'::text))", RlsClass.FILTER, None),
        (
            "(EXISTS (SELECT 1 FROM memberships m WHERE m.user_id = owner))",
            RlsClass.DENORMALIZE,
            None,
        ),
        ("is_member(owner)", RlsClass.DOES_NOT_MAP, None),
        ("(owner = CURRENT_USER)", RlsClass.NAMESPACE, "owner"),
        (None, RlsClass.FILTER, None),
    ],
)
def test_classification(qual: str | None, expected: RlsClass, column: str | None) -> None:
    classification, got_column, note = classify_qual(qual, COLS)
    assert classification is expected, note
    assert got_column == column


def test_unknown_column_does_not_map() -> None:
    classification, _, _ = classify_qual("(mystery = 'x')", COLS)
    assert classification is RlsClass.DOES_NOT_MAP
