"""Resource names and tags for the AWS resources a migration creates (ADR-002)."""

import json

import pytest

from pcmigrate.config.aws import (
    ResourceNames,
    default_tags_config,
    resource_tags,
    tool_tags,
    validate_name_prefix,
    validate_tags,
)


def test_default_names_start_with_pcmigrate() -> None:
    names = ResourceNames("acme-2026-10")
    assert names.runner_role == "pcmigrate-acme-2026-10-runner"
    assert names.storage_integration_role == "pcmigrate-acme-2026-10-pinecone-import"
    assert names.osis_pipeline_role == "pcmigrate-acme-2026-10-osis-pipeline"
    assert names.glue_role == "pcmigrate-acme-2026-10-glue-transform"
    assert names.glue_job == "pcmigrate-acme-2026-10-opensearch-transform"
    assert names.buffer_queue == "pcmigrate-acme-2026-10-buffer.fifo"
    assert names.kms_alias == "alias/pcmigrate-acme-2026-10"
    assert names.secret("source-dsn") == "pcmigrate/acme-2026-10/source-dsn"
    assert names.log_group == "/pcmigrate/acme-2026-10"
    assert names.osis_pipeline == "pcm-acme-2026-10"
    assert names.osis_log_group == "/aws/vendedlogs/OpenSearchIngestion/pcm-acme-2026-10/audit-logs"


def test_prefix_lands_in_front_of_every_name() -> None:
    names = ResourceNames("acme-2026-10", "Corp_Prod")
    assert names.runner_role == "Corp_Prod-pcmigrate-acme-2026-10-runner"
    assert names.secret("source-dsn") == "Corp_Prod-pcmigrate/acme-2026-10/source-dsn"
    assert names.log_group == "/Corp_Prod-pcmigrate/acme-2026-10"
    assert names.kms_alias == "alias/Corp_Prod-pcmigrate-acme-2026-10"
    assert names.osis_pipeline == "corp-prod-pcm-acme-2026-10", "OSIS names are lowercase a-z0-9-"
    assert set(names.as_dict()) >= {"runner_role", "glue_job", "osis_pipeline", "secret_prefix"}
    assert all(
        v.startswith(("Corp_Prod-pcmigrate", "alias/Corp_Prod", "/Corp_Prod", "corp-prod-pcm"))
        for v in names.as_dict().values()
    )


def test_osis_pipeline_name_fits_28_characters_with_a_prefix() -> None:
    name = ResourceNames("a" * 40, "acme-platform-prod").osis_pipeline
    assert len(name) <= 28 and name.startswith("acme-platform-prod-pcm-")
    assert ResourceNames("x" * 23 + "-----").osis_pipeline == "pcm-" + "x" * 23


@pytest.mark.parametrize("prefix", ["acme", "Acme-Prod", "a", "team_42"])
def test_well_formed_prefixes_pass(prefix: str) -> None:
    assert validate_name_prefix(prefix, "mig") == prefix
    assert validate_name_prefix(None, "mig") is None


@pytest.mark.parametrize(
    "prefix", ["acme-", "-acme", "9acme", "acme prod", "acme/prod", "a" * 33, ""]
)
def test_malformed_prefixes_are_rejected(prefix: str) -> None:
    with pytest.raises(ValueError, match="name_prefix must"):
        validate_name_prefix(prefix, "mig")


def test_prefix_plus_id_must_fit_an_iam_role_name() -> None:
    with pytest.raises(ValueError, match="IAM allows 64"):
        validate_name_prefix("a" * 32, "m" * 20)
    assert validate_name_prefix("a" * 32, "m" * 5) == "a" * 32


def test_tool_tags_win_over_customer_tags() -> None:
    merged = resource_tags("mig", {"CostCenter": "4711", "pcmigrate:migration_id": "spoof"})
    assert merged["CostCenter"] == "4711"
    assert merged["pcmigrate:migration_id"] == "mig"
    assert merged["pcmigrate:managed_by"] == "pcmigrate"
    assert set(tool_tags("mig")) == {"pcmigrate:managed_by", "pcmigrate:migration_id"}


def test_valid_tags_pass_through() -> None:
    tags = {"CostCenter": "4711", "Owner": "data-platform@example.com", "Env": "", "k:v/x": "a b"}
    assert validate_tags(tags) == tags


@pytest.mark.parametrize(
    ("tags", "message"),
    [
        ({"aws:cloudformation:stack": "x"}, "reserved by AWS"),
        ({"AWS:anything": "x"}, "reserved by AWS"),
        ({"pcmigrate:migration_id": "x"}, "prefix is reserved"),
        ({"": "x"}, "1 to 128 characters"),
        ({"k" * 129: "x"}, "1 to 128 characters"),
        ({"k": "v" * 257}, "at most 256 characters"),
        ({"bad key!": "x"}, "only letters, digits"),
        ({"k": "tab\there"}, "only letters, digits"),
        ({"dsn": "postgresql://u:pw@h/db"}, "secret value"),
        ({f"k{i}": "v" for i in range(49)}, "at most 48 tags"),
    ],
)
def test_bad_tags_are_rejected(tags: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_tags(tags)


def test_default_tags_config_is_the_provider_object() -> None:
    assert json.loads(default_tags_config({"b": "2", "a": "1"})) == {"tags": {"a": "1", "b": "2"}}
