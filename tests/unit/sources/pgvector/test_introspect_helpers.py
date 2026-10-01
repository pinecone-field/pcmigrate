import pytest

from pcmigrate.sources.pgvector.introspect import (
    ReadOnlyViolationError,
    StatementAudit,
    metric_from_opclass,
)
from pcmigrate.sources.pgvector.models import MetricGuess


@pytest.mark.parametrize(
    ("opclass", "metric"),
    [
        ("vector_cosine_ops", MetricGuess.COSINE),
        ("vector_ip_ops", MetricGuess.DOTPRODUCT),
        ("vector_l2_ops", MetricGuess.EUCLIDEAN),
        ("halfvec_cosine_ops", MetricGuess.COSINE),
        ("halfvec_ip_ops", MetricGuess.DOTPRODUCT),
        ("halfvec_l2_ops", MetricGuess.EUCLIDEAN),
        ("sparsevec_cosine_ops", MetricGuess.COSINE),
        ("vector_l1_ops", MetricGuess.UNKNOWN),
        ("text_ops", MetricGuess.UNKNOWN),
    ],
)
def test_metric_from_opclass(opclass: str, metric: MetricGuess) -> None:
    assert metric_from_opclass(opclass) is metric


def test_statement_audit_allows_reads_only() -> None:
    audit = StatementAudit()
    for ok in (
        "SELECT 1",
        "  with s as (select 1) select * from s",
        "SET x = 1",
        "SHOW y",
        "EXPLAIN SELECT 1",
    ):
        audit.check(ok)
    assert len(audit.statements) == 5
    for bad in (
        "CREATE TABLE t (x int)",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "ANALYZE t",
        "VACUUM",
    ):
        with pytest.raises(ReadOnlyViolationError):
            audit.check(bad)
