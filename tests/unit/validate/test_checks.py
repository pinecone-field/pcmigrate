from pcmigrate.sources.synthetic import SyntheticSource
from pcmigrate.targets.pinecone_client import VectorRecord
from pcmigrate.validate import check_fidelity, check_filters, check_id_diff, check_recall
from pcmigrate.validate.fidelity import compare_metadata, cosine
from pcmigrate.validate.report import CheckOutcome, ValidationReport
from tests.unit.fakes import FakeGateway

HOST = "h"
NS = "documents"


def loaded(rows: int = 60, dim: int = 8) -> tuple[SyntheticSource, FakeGateway]:
    source = SyntheticSource(rows=rows, dimension=dim, partitions=1)
    gw = FakeGateway()
    gw.upsert(
        HOST,
        NS,
        [
            VectorRecord(r.id, r.dense or [], r.metadata)
            for r in (source.record(i) for i in range(rows))
        ],
    )
    return source, gw


def test_cosine_and_metadata_compare() -> None:
    assert abs(cosine([1, 0], [1, 0]) - 1.0) < 1e-9
    assert cosine([1, 0], [0, 1]) == 0.0 and cosine([], []) == 0.0
    assert compare_metadata({"a": 1, "b": 2.0}, {"a": 1, "b": 2}) is None
    assert compare_metadata({"a": 1}, {}) == "a: missing in target"
    assert compare_metadata({"a": 1}, {"a": 2}) == "a: 1 != 2"


def test_fidelity_passes_on_exact_copy_and_catches_drift() -> None:
    source, gw = loaded()
    result = check_fidelity(gw, HOST, source, NS, sample=20)
    assert result.vectors_passed and result.metadata_passed and result.sampled == 20
    broken = source.record(3)
    gw.upsert(
        HOST,
        NS,
        [
            VectorRecord(
                broken.id, [v * -1 for v in broken.dense or []], {**broken.metadata, "row": 999}
            )
        ],
    )
    del gw.data[HOST][NS]["documents#5"]
    result = check_fidelity(gw, HOST, source, NS, sample=60)
    assert not result.vectors_passed and not result.metadata_passed
    assert "documents#5" in result.missing
    assert any(i == "documents#3" for i, _ in result.vector_failures)
    assert any(i == "documents#3" and d.startswith("row:") for i, d in result.metadata_failures)


def test_recall_against_exact_knn() -> None:
    source, gw = loaded()
    result = check_recall(gw, HOST, source, NS, k=5, queries=10, threshold=0.95)
    assert result.passed and result.mean == 1.0
    for i in range(30):
        del gw.data[HOST][NS][f"documents#{i}"]
    degraded = check_recall(gw, HOST, source, NS, k=5, queries=10, threshold=0.95)
    assert not degraded.passed and degraded.mean < 1.0


def test_filter_parity() -> None:
    source, gw = loaded()
    result = check_filters(gw, HOST, source, NS, k=5, filters=4)
    assert result.passed and len(result.results) == 4 and result.mean_overlap == 1.0
    assert all(
        r["filter"]["category"]["$eq"] in ("news", "blog", "paper", "manual")
        for r in result.results
    )
    for record in list(gw.data[HOST][NS].values()):
        if record.metadata["category"] == "news":
            gw.data[HOST][NS][record.id] = VectorRecord(
                record.id, record.values, {**record.metadata, "category": "blog"}
            )
    assert not check_filters(gw, HOST, source, NS, k=5, filters=4).passed


def test_id_diff() -> None:
    source, gw = loaded()
    assert check_id_diff(gw, HOST, source, NS).passed
    del gw.data[HOST][NS]["documents#7"]
    gw.upsert(HOST, NS, [VectorRecord("documents#999", [0.0] * 8, {})])
    diff = check_id_diff(gw, HOST, source, NS)
    assert diff.missing == ["documents#7"] and diff.extra == ["documents#999"] and not diff.passed


def test_report_rendering() -> None:
    report = ValidationReport(migration_id="m", index="i")
    report.outcomes.append(
        CheckOutcome(
            "count_parity",
            "*",
            True,
            {"namespaces": [{"namespace": "d", "expected": 1, "actual": 1}]},
        )
    )
    report.outcomes.append(
        CheckOutcome(
            "recall_at_k", "d", False, {"k": 10, "mean": 0.9, "queries": 5, "threshold": 0.95}
        )
    )
    assert not report.passed and [o.check for o in report.failures()] == ["recall_at_k"]
    md = report.to_markdown()
    assert "| recall_at_k | d | FAIL |" in md and "1 check(s) failed" in md
    assert '"passed": false' in report.to_json()
