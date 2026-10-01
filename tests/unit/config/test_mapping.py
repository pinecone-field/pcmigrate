import pytest
import yaml
from pydantic import ValidationError

from pcmigrate.config import Mapping

EXAMPLE = """
migration_id: acme-docs-2026-10
source:
  kind: pgvector
  dsn_secret: arn:aws:secretsmanager:us-east-1:111122223333:secret:acme-pg-ro
  snapshot: exported
target:
  index: acme-docs
  mode: create
  type: vector
  cloud: aws
  region: us-east-1
  capacity: on_demand
  api_version: "2026-07"
objects:
  - source: public.documents
    namespace: documents
    id_template: "documents#{id}"
    dense: {column: embedding, dimension: 768, metric: cosine}
    metadata:
      - {source: title, target: title, type: string}
      - {source: category, target: category, type: string}
      - {source: published_at, target: published_at, type: epoch_seconds}
      - {source: body, action: drop, reason: "exceeds metadata size limit"}
duplicates: upsert_after_import
change_capture:
  tier: trigger_outbox
  buffer: sqs_fifo
validation:
  recall_at_k: {k: 10, queries: 200, threshold: 0.95}
  fidelity_sample: 1000
"""


def test_parses_the_design_example() -> None:
    mapping = Mapping.from_yaml(EXAMPLE)
    assert mapping.migration_id == "acme-docs-2026-10"
    assert mapping.target.api_version == "2026-07"
    obj = mapping.objects[0]
    assert obj.dense is not None and obj.dense.dimension == 768
    assert [f.action for f in obj.metadata] == ["map", "map", "map", "drop"]
    assert mapping.change_capture is not None and mapping.change_capture.tier == "trigger_outbox"
    assert mapping.namespaces == ["documents"]


def test_yaml_round_trip_is_stable() -> None:
    first = Mapping.from_yaml(EXAMPLE)
    second = Mapping.from_yaml(first.to_yaml())
    assert first == second
    assert first.normalized() == second.normalized()


def test_normalized_ignores_key_order_and_whitespace() -> None:
    reordered = EXAMPLE.replace(
        "  mode: create\n  type: vector\n", "  type: vector\n  mode: create\n"
    )
    assert Mapping.from_yaml(EXAMPLE).normalized() == Mapping.from_yaml(reordered).normalized()


def test_edit_changes_normalized_form() -> None:
    edited = EXAMPLE.replace("threshold: 0.95", "threshold: 0.9")
    assert Mapping.from_yaml(EXAMPLE).normalized() != Mapping.from_yaml(edited).normalized()


def test_mapped_field_defaults_target_to_source() -> None:
    text = EXAMPLE.replace(
        "{source: title, target: title, type: string}", "{source: title, type: string}"
    )
    field = Mapping.from_yaml(text).objects[0].metadata[0]
    assert field.target == "title"


def test_dropped_field_requires_reason() -> None:
    text = EXAMPLE.replace(
        '{source: body, action: drop, reason: "exceeds metadata size limit"}',
        "{source: body, action: drop}",
    )
    with pytest.raises(ValidationError, match="needs a reason"):
        Mapping.from_yaml(text)


def test_mapped_field_requires_type() -> None:
    text = EXAMPLE.replace("{source: title, target: title, type: string}", "{source: title}")
    with pytest.raises(ValidationError, match="needs a type"):
        Mapping.from_yaml(text)


def test_dsn_secret_must_be_an_arn() -> None:
    text = EXAMPLE.replace(
        "arn:aws:secretsmanager:us-east-1:111122223333:secret:acme-pg-ro",
        "postgresql://user:pw@host/db",
    )
    with pytest.raises(ValidationError, match="Secrets Manager ARN"):
        Mapping.from_yaml(text)


def test_unknown_keys_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Mapping.from_yaml(EXAMPLE + "\nextra: true\n")


def test_duplicate_namespaces_are_rejected() -> None:
    data = yaml.safe_load(EXAMPLE)
    data["objects"].append(
        {"source": "public.other", "namespace": "documents", "id_template": "other#{id}"}
    )
    with pytest.raises(ValidationError, match="same namespace"):
        Mapping.model_validate(data)


def test_duplicate_metadata_targets_are_rejected() -> None:
    text = EXAMPLE.replace(
        "{source: category, target: category, type: string}",
        "{source: category, target: title, type: string}",
    )
    with pytest.raises(ValidationError, match="several fields"):
        Mapping.from_yaml(text)
