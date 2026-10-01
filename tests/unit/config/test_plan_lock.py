from pcmigrate.config import Mapping, PlanLock, schema_fingerprint
from tests.unit.config.test_mapping import EXAMPLE


def test_hash_is_deterministic() -> None:
    mapping = Mapping.from_yaml(EXAMPLE)
    fp = schema_fingerprint({"tables": {"public.documents": {"embedding": "vector(768)"}}})
    a = PlanLock.compute(mapping, fp)
    b = PlanLock.compute(mapping, fp)
    assert a.plan_hash == b.plan_hash
    assert len(a.plan_hash) == 64
    assert a.matches(b.plan_hash)


def test_mapping_edit_invalidates_hash() -> None:
    fp = schema_fingerprint({"x": 1})
    before = PlanLock.compute(Mapping.from_yaml(EXAMPLE), fp)
    after = PlanLock.compute(
        Mapping.from_yaml(EXAMPLE.replace("threshold: 0.95", "threshold: 0.9")), fp
    )
    assert not before.matches(after.plan_hash)


def test_schema_change_invalidates_hash() -> None:
    mapping = Mapping.from_yaml(EXAMPLE)
    before = PlanLock.compute(mapping, schema_fingerprint({"dim": 768}))
    after = PlanLock.compute(mapping, schema_fingerprint({"dim": 1024}))
    assert before.plan_hash != after.plan_hash


def test_schema_fingerprint_ignores_key_order() -> None:
    assert schema_fingerprint({"a": 1, "b": [1, 2]}) == schema_fingerprint({"b": [1, 2], "a": 1})


def test_text_round_trip() -> None:
    lock = PlanLock.compute(Mapping.from_yaml(EXAMPLE), schema_fingerprint({}))
    assert PlanLock.from_text(lock.to_text()) == lock
