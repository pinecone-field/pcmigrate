from pcmigrate.validate.counts import CountExpectation, check_counts
from tests.unit.fakes import FakeGateway


def _clock(values: list[float]):  # type: ignore[no-untyped-def]
    it = iter(values)
    return lambda: next(it)


def test_passes_once_counts_match_and_settle() -> None:
    gw = FakeGateway()
    gw.stats_history = [{"docs": 10}, {"docs": 90}, {"docs": 100}, {"docs": 100}]
    result = check_counts(
        gw,
        "h",
        [CountExpectation("docs", 100)],
        poll_interval_s=0,
        sleep=lambda _: None,
        clock=_clock([0, 1, 2, 3, 4, 5]),
        settle_window_s=100,
    )
    assert result.passed and result.stable and result.polls == 4
    assert result.to_dict()["namespaces"] == [{"namespace": "docs", "expected": 100, "actual": 100}]


def test_subtracts_rejects_and_deferred_duplicates() -> None:
    gw = FakeGateway()
    gw.stats_history = [{"docs": 95}, {"docs": 95}]
    result = check_counts(
        gw,
        "h",
        [CountExpectation("docs", 100, rejected=2, duplicates_deferred=3)],
        poll_interval_s=0,
        sleep=lambda _: None,
        clock=_clock([0, 1, 2, 3]),
        settle_window_s=100,
    )
    assert result.passed


def test_fails_when_window_closes() -> None:
    gw = FakeGateway()
    gw.stats_history = [{"docs": 90}, {"docs": 90}, {"docs": 90}]
    result = check_counts(
        gw,
        "h",
        [CountExpectation("docs", 100), CountExpectation("other", 0)],
        poll_interval_s=0,
        sleep=lambda _: None,
        clock=_clock([0, 1, 2, 100]),
        settle_window_s=50,
    )
    assert not result.passed and result.stable
    assert [n.passed for n in result.namespaces] == [False, True]


def test_matching_but_unsettled_keeps_polling() -> None:
    gw = FakeGateway()
    gw.stats_history = [{"docs": 100}, {"docs": 99}, {"docs": 100}, {"docs": 100}]
    result = check_counts(
        gw,
        "h",
        [CountExpectation("docs", 100)],
        poll_interval_s=0,
        sleep=lambda _: None,
        clock=_clock([0, 1, 2, 3, 4, 5]),
        settle_window_s=100,
    )
    assert result.passed and result.polls == 4
