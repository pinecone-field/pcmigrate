import pytest

from pcmigrate.config.mapping import Mapping
from pcmigrate.config.migration import MigrationConfig
from pcmigrate.config.settings import OpenSearchSettings, RunSettings
from pcmigrate.core.stages.provision import PulumiDriver
from pcmigrate.sources.opensearch.access import map_backend_role
from pcmigrate.sources.opensearch.glue_package import GlueAssets
from pcmigrate.sources.opensearch.provision import stack_config
from tests.unit.config.test_mapping import EXAMPLE
from tests.unit.sources.opensearch.fakes import FakeTransport, make_client

ARN = "arn:aws:secretsmanager:us-east-1:111122223333:secret:pinecone"


def test_map_backend_role_is_idempotent_and_needs_a_writable_client() -> None:
    transport = FakeTransport.golden()
    transport.role_mappings["all_access"] = {
        "backend_roles": ["admin"],
        "hosts": [],
        "users": ["kibanaserver"],
    }
    with pytest.raises(RuntimeError, match="writable"):
        map_backend_role(make_client(transport), "arn:aws:iam::1:role/p")
    client = make_client(transport, read_only=False)
    first = map_backend_role(client, "arn:aws:iam::1:role/p")
    assert first["changed"] and transport.role_mappings["all_access"]["backend_roles"] == [
        "admin",
        "arn:aws:iam::1:role/p",
    ]
    assert transport.role_mappings["all_access"]["users"] == ["kibanaserver"]
    second = map_backend_role(client, "arn:aws:iam::1:role/p")
    assert not second["changed"]
    assert ("PUT", "/_plugins/_security/api/rolesmapping/all_access") in client.audit.requests
    missing = map_backend_role(client, "arn:aws:iam::1:role/q", security_role="readall")
    assert missing["backend_roles"] == ["arn:aws:iam::1:role/q"]


def test_opensearch_settings_validation(tmp_path) -> None:  # type: ignore[no-untyped-def]
    settings = OpenSearchSettings(
        domain_arn="arn:aws:es:us-east-1:1:domain/acme",
        domain_name="acme",
        fine_grained_access=True,
    )
    assert settings.auth == "sigv4" and settings.export_mode == "osis_glue"
    with pytest.raises(ValueError, match="credentials_secret_arn"):
        OpenSearchSettings(credentials_secret_arn="user:password")
    with pytest.raises(ValueError, match="osis_max_units"):
        OpenSearchSettings(osis_min_units=4, osis_max_units=2)
    path = tmp_path / "pcmigrate.yaml"
    path.write_text(
        "migration:\n  migration_id: m\n  bucket: bkt\n  aws_region: us-east-1\n"
        f"  pinecone_api_key_secret: {ARN}\n"
        "opensearch:\n  url_env: OS_URL\n  auth: basic\n  basic_auth_env: OS_AUTH\n  export_mode: direct\n"
    )
    loaded = RunSettings.load(path)
    assert loaded.opensearch.export_mode == "direct"
    import os

    os.environ["OS_URL"] = "http://localhost:9200"
    os.environ["OS_AUTH"] = "admin:secret"
    try:
        assert loaded.opensearch_url() == "http://localhost:9200"
        assert loaded.opensearch_basic_auth() == ("admin", "secret")
    finally:
        del os.environ["OS_URL"], os.environ["OS_AUTH"]
    assert (
        RunSettings.load(path)
        .model_copy(update={"opensearch": OpenSearchSettings()})
        .opensearch_basic_auth()
        is None
    )


def test_stack_config_from_run_context() -> None:
    from pcmigrate.core.stages import RunContext

    mapping = Mapping.from_yaml(
        EXAMPLE.replace("kind: pgvector", "kind: opensearch").replace("public.documents", "docs")
    )
    cfg = MigrationConfig(
        migration_id="m",
        bucket="bkt",
        aws_region="us-east-1",
        pinecone_api_key_secret=ARN,
        name_prefix="corp",
        tags={"CostCenter": "4711"},
    )
    ctx = RunContext(config=cfg, mapping=mapping, manifest=None, store=None, events=None, s3=None)  # type: ignore[arg-type]
    settings = OpenSearchSettings(
        domain_arn="arn:aws:es:us-east-1:1:domain/d", domain_name="d", glue_workers=8
    )
    stack = stack_config(
        ctx,
        settings,
        endpoint="https://search-d.us-east-1.es.amazonaws.com",
        base_outputs={
            "kms_key_arn": "arn:aws:kms:us-east-1:1:key/k",
            "runner_role_arn": "arn:aws:iam::1:role/r",
        },
        assets=GlueAssets(
            script_uri="s3://bkt/m/manifest/glue/opensearch_transform.py",
            wheel_uri="s3://bkt/m/manifest/glue/pcmigrate-0.0.1-py3-none-any.whl",
        ),
    )
    assert stack.pipeline_name == "corp-pcm-m" and stack.indices == ("docs",)
    assert stack.name_prefix == "corp" and stack.tags == {"CostCenter": "4711"}
    assert stack.glue_job_name == "corp-pcmigrate-m-opensearch-transform"
    assert (
        stack.kms_key_arn == "arn:aws:kms:us-east-1:1:key/k"
        and stack.extra_domain_principals == ("arn:aws:iam::1:role/r",)
    )
    assert stack.glue_workers == 8 and "index_name_regex: ^docs$" in stack.pipeline_body
    assert stack.credentials_secret_arn is None
    driver = PulumiDriver(addons=[])
    assert driver._addons == ()
