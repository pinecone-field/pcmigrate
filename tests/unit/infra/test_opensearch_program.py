import json
from typing import Any

import pulumi
import pytest
from infra.opensearch import (
    OpenSearchStackConfig,
    build,
    domain_access_statement,
    glue_default_arguments,
    glue_policy,
    osis_trust_policy,
    pipeline_policy,
)

from pcmigrate.sources.opensearch.osis_config import OsisPipelineSpec, pipeline_body

BODY = pipeline_body(
    OsisPipelineSpec(
        migration_id="acme-2026-10",
        endpoint="https://search-acme.us-east-1.es.amazonaws.com",
        indices=("documents",),
        region="us-east-1",
        bucket="acme-pcmigrate",
        raw_prefix="acme-2026-10/raw",
    )
)
CFG = OpenSearchStackConfig(
    migration_id="acme-2026-10",
    region="us-east-1",
    bucket_name="acme-pcmigrate",
    kms_key_arn="arn:aws:kms:us-east-1:1:key/k",
    pipeline_name="pcm-acme-2026-10",
    pipeline_body=BODY,
    indices=("documents",),
    domain_arn="arn:aws:es:us-east-1:1:domain/acme",
    domain_name="acme",
    manage_domain_policy=True,
    extra_domain_principals=("arn:aws:iam::1:role/runner",),
    glue_script_uri="s3://acme-pcmigrate/acme-2026-10/manifest/glue/opensearch_transform.py",
    glue_python_modules="s3://acme-pcmigrate/acme-2026-10/manifest/glue/pcmigrate-0.0.1-py3-none-any.whl,pydantic==2.13.5",
    glue_workers=6,
    tags={"CostCenter": "4711"},
)
TAGGABLE = {
    "aws:iam/role:Role",
    "aws:cloudwatch/logGroup:LogGroup",
    "aws:opensearchingest/pipeline:Pipeline",
    "aws:glue/job:Job",
}
EXPECTED_TAGS = {
    "CostCenter": "4711",
    "pcmigrate:managed_by": "pcmigrate",
    "pcmigrate:migration_id": "acme-2026-10",
}


class Mocks(pulumi.runtime.Mocks):
    def __init__(self) -> None:
        self.resources: list[tuple[str, str, dict[str, Any]]] = []

    def new_resource(self, args: pulumi.runtime.MockResourceArgs) -> tuple[str, dict[str, Any]]:
        self.resources.append((args.typ, args.name, args.inputs))
        outputs = {
            **args.inputs,
            "id": f"{args.name}-id",
            "arn": f"arn:aws:iam::1:role/{args.name}",
        }
        if args.typ.endswith("Pipeline"):
            outputs["pipelineArn"] = "arn:aws:osis:us-east-1:1:pipeline/pcm-acme-2026-10"
        return f"{args.name}-id", outputs

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:
        if "getCallerIdentity" in args.token:
            return {
                "accountId": "111122223333",
                "arn": "arn:aws:iam::111122223333:user/t",
                "userId": "u",
            }
        return {}


@pytest.fixture(scope="module")
def declared() -> list[tuple[str, str, dict[str, Any]]]:
    mocks = Mocks()
    pulumi.runtime.set_mocks(
        mocks, project="pcmigrate-opensearch", stack=CFG.stack_name, preview=False
    )

    @pulumi.runtime.test
    def run() -> None:
        build(CFG)

    run()
    return mocks.resources


def _of(declared: list[tuple[str, str, dict[str, Any]]], typ: str) -> list[dict[str, Any]]:
    return [inputs for t, _, inputs in declared if t == typ]


def test_every_taggable_resource_carries_the_tags(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    tagged = [(t, inputs) for t, _, inputs in declared if t in TAGGABLE]
    assert {t for t, _ in tagged} == TAGGABLE
    for typ, inputs in tagged:
        assert inputs.get("tags") == EXPECTED_TAGS, typ


def test_name_prefix_renames_roles_job_and_pipeline() -> None:
    cfg = CFG.model_copy(update={"name_prefix": "corp", "pipeline_name": "corp-pcm-acme-2026-10"})
    mocks = Mocks()
    pulumi.runtime.set_mocks(
        mocks, project="pcmigrate-opensearch", stack=cfg.stack_name, preview=False
    )

    @pulumi.runtime.test
    def run() -> None:
        build(cfg)

    run()
    roles = {r["name"] for r in _of(mocks.resources, "aws:iam/role:Role")}
    assert roles == {
        "corp-pcmigrate-acme-2026-10-osis-pipeline",
        "corp-pcmigrate-acme-2026-10-glue-transform",
    }
    (job,) = _of(mocks.resources, "aws:glue/job:Job")
    assert job["name"] == "corp-pcmigrate-acme-2026-10-opensearch-transform"
    (logs,) = _of(mocks.resources, "aws:cloudwatch/logGroup:LogGroup")
    assert logs["name"] == "/aws/vendedlogs/OpenSearchIngestion/corp-pcm-acme-2026-10/audit-logs"
    assert {name for _, name, _ in mocks.resources} == {name for _, name, _ in _declared_default()}


def _declared_default() -> list[tuple[str, str, dict[str, Any]]]:
    mocks = Mocks()
    pulumi.runtime.set_mocks(
        mocks, project="pcmigrate-opensearch", stack=CFG.stack_name, preview=False
    )

    @pulumi.runtime.test
    def run() -> None:
        build(CFG)

    run()
    return mocks.resources


def test_pipeline_is_armed_with_the_role_filled_in(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (pipeline,) = _of(declared, "aws:opensearchingest/pipeline:Pipeline")
    assert pipeline["pipelineName"] == "pcm-acme-2026-10"
    assert pipeline["minUnits"] == 1 and pipeline["maxUnits"] == 4
    body = pipeline["pipelineConfigurationBody"]
    assert "{pipeline_role_arn}" not in body and "arn:aws:iam::1:role/" in body
    assert "start_time: '2099-01-01T00:00:00Z'" in body
    assert pipeline["logPublishingOptions"]["isLoggingEnabled"] is True
    assert pipeline["encryptionAtRestOptions"]["kmsKeyArn"] == "arn:aws:kms:us-east-1:1:key/k"
    assert "vpcOptions" not in pipeline


def test_roles_are_least_privilege(declared: list[tuple[str, str, dict[str, Any]]]) -> None:
    roles = {r["name"]: r for r in _of(declared, "aws:iam/role:Role")}
    assert set(roles) == {
        "pcmigrate-acme-2026-10-osis-pipeline",
        "pcmigrate-acme-2026-10-glue-transform",
    }
    trust = json.loads(roles["pcmigrate-acme-2026-10-osis-pipeline"]["assumeRolePolicy"])
    statement = trust["Statement"][0]
    assert statement["Principal"] == {"Service": "osis-pipelines.amazonaws.com"}
    assert statement["Condition"]["StringEquals"] == {"aws:SourceAccount": "111122223333"}
    assert statement["Condition"]["ArnLike"]["aws:SourceArn"].endswith(":pipeline/pcm-acme-2026-10")
    policies = _of(declared, "aws:iam/rolePolicy:RolePolicy")
    osis = next(json.loads(p["policy"]) for p in policies if '"es:ESHttpGet"' in p["policy"])
    resources = {str(s["Resource"]) for s in osis["Statement"]}
    assert "arn:aws:s3:::acme-pcmigrate/acme-2026-10/raw/*" in resources
    assert "arn:aws:es:us-east-1:1:domain/acme/*" in resources
    assert not any("s3:DeleteObject" in json.dumps(s) for s in osis["Statement"])
    glue = next(json.loads(p["policy"]) for p in policies if "canonical/*" in p["policy"])
    writes = next(s for s in glue["Statement"] if "s3:PutObject" in s["Action"])
    assert set(writes["Resource"]) == {
        "arn:aws:s3:::acme-pcmigrate/acme-2026-10/canonical/*",
        "arn:aws:s3:::acme-pcmigrate/acme-2026-10/rejects/*",
        "arn:aws:s3:::acme-pcmigrate/acme-2026-10/manifest/glue/*",
    }


def test_glue_job_installs_the_wheel_without_dependencies(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (job,) = _of(declared, "aws:glue/job:Job")
    assert job["name"] == "pcmigrate-acme-2026-10-opensearch-transform"
    assert (
        job["glueVersion"] == "5.0" and job["workerType"] == "G.1X" and job["numberOfWorkers"] == 6
    )
    assert job["command"]["scriptLocation"].endswith("/glue/opensearch_transform.py")
    args = job["defaultArguments"]
    assert args["--additional-python-modules"].startswith(
        "s3://acme-pcmigrate/acme-2026-10/manifest/glue/pcmigrate-"
    )
    assert args["--python-modules-installer-option"] == "--no-deps"
    assert args["--TempDir"] == "s3://acme-pcmigrate/acme-2026-10/manifest/glue/tmp/"
    assert glue_default_arguments(CFG)["--region"] == "us-east-1"


def test_domain_policy_grants_the_pipeline_and_runner(
    declared: list[tuple[str, str, dict[str, Any]]],
) -> None:
    (policy,) = _of(declared, "aws:opensearch/domainPolicy:DomainPolicy")
    assert policy["domainName"] == "acme"
    doc = json.loads(policy["accessPolicies"])
    (statement,) = doc["Statement"]
    assert statement["Resource"] == "arn:aws:es:us-east-1:1:domain/acme/*"
    assert "arn:aws:iam::1:role/runner" in statement["Principal"]["AWS"]
    assert any(p.endswith("-osis-role") for p in statement["Principal"]["AWS"])
    assert domain_access_statement("arn:aws:es:r:1:domain/d", ["a"])["Action"] == [
        "es:ESHttpGet",
        "es:ESHttpPost",
        "es:ESHttpHead",
        "es:ESHttpDelete",
    ]


def test_self_managed_config_uses_vpc_and_secret() -> None:
    cfg = CFG.model_copy(
        update={
            "domain_arn": None,
            "domain_name": None,
            "manage_domain_policy": False,
            "credentials_secret_arn": "arn:aws:secretsmanager:us-east-1:1:secret:os",
            "subnet_ids": ("subnet-1",),
            "security_group_ids": ("sg-1",),
        }
    )
    assert cfg.self_managed
    policy = json.loads(pipeline_policy(cfg, "arn:aws:s3:::b", None))
    actions = {a for s in policy["Statement"] for a in s["Action"]}
    assert "secretsmanager:GetSecretValue" in actions and "es:ESHttpGet" not in actions
    assert "kms:Encrypt" not in actions
    assert "logs:PutLogEvents" in {
        a
        for s in json.loads(glue_policy(cfg, "arn:aws:s3:::b", None))["Statement"]
        for a in s["Action"]
    }
    trust = json.loads(osis_trust_policy("1", "us-east-1", "pcm-x"))
    assert (
        trust["Statement"][0]["Condition"]["ArnLike"]["aws:SourceArn"]
        == "arn:aws:osis:us-east-1:1:pipeline/pcm-x"
    )
    assert cfg.osis_log_group == "/aws/vendedlogs/OpenSearchIngestion/pcm-acme-2026-10/audit-logs"
