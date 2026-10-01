import pytest
from pydantic import ValidationError

from pcmigrate.config import MigrationConfig

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


def make(**overrides: object) -> MigrationConfig:
    base: dict[str, object] = {
        "migration_id": "acme-docs-2026-10",
        "bucket": "acme-pcmigrate",
        "aws_region": "us-east-1",
        "pinecone_api_key_secret": ARN,
    }
    base.update(overrides)
    return MigrationConfig.model_validate(base)


def test_zone_layout_follows_adr_003() -> None:
    cfg = make()
    assert cfg.zone("canonical") == "acme-docs-2026-10/canonical"
    assert cfg.zone_uri("import") == "s3://acme-pcmigrate/acme-docs-2026-10/import/"


def test_api_key_must_be_secret_arn() -> None:
    with pytest.raises(ValidationError, match="Secrets Manager ARN"):
        make(pinecone_api_key_secret="pcsk_abc123")


@pytest.mark.parametrize(
    "value",
    [
        "postgresql://user:pw@host/db",
        "host=db password=hunter2",
        "AKIAABCDEFGHIJKLMNOP",
    ],
)
def test_secret_looking_values_are_rejected(value: str) -> None:
    with pytest.raises(ValidationError, match="secret value"):
        make(migration_id="acme", bucket="acme-pcmigrate", aws_region=value)


def test_defaults_are_sane() -> None:
    cfg = make()
    assert cfg.workers == 4
    assert cfg.canonical_part_bytes < cfg.import_part_bytes
    assert cfg.lock_ttl_seconds >= 30
