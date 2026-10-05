import yaml

from pcmigrate.sources.opensearch.osis_config import (
    ARMED_START_TIME,
    PIPELINE_ROLE_PLACEHOLDER,
    OsisPipelineSpec,
    pipeline_body,
    pipeline_document,
    pipeline_name,
    with_role,
    with_start_time,
)

SPEC = OsisPipelineSpec(
    migration_id="acme-docs-2026-10",
    endpoint="https://search-acme.us-east-1.es.amazonaws.com",
    indices=("golden-documents", "Other.Index"),
    region="us-east-1",
    bucket="acme-pcmigrate",
    raw_prefix="acme-docs-2026-10/raw",
)


def test_pipeline_name_fits_osis_limits() -> None:
    assert pipeline_name("acme-docs-2026-10") == "pcm-acme-docs-2026-10"
    assert len(pipeline_name("a" * 70)) <= 28
    assert pipeline_name("x" * 23 + "-----") == "pcm-" + "x" * 23


def test_managed_domain_pipeline_body() -> None:
    doc = pipeline_document(SPEC)
    assert doc["version"] == "2" and "extension" not in doc
    names = [k for k in doc if k != "version"]
    assert names == ["idx0-golden-documents", "idx1-other-index"]
    first = doc["idx0-golden-documents"]
    source = first["source"]["opensearch"]
    assert source["hosts"] == [SPEC.endpoint]
    assert source["indices"] == {"include": [{"index_name_regex": "^golden\\-documents$"}]}
    assert source["aws"] == {
        "region": "us-east-1",
        "sts_role_arn": PIPELINE_ROLE_PLACEHOLDER,
        "serverless": False,
    }
    assert source["scheduling"] == {"index_read_count": 1, "start_time": ARMED_START_TIME}
    assert source["acknowledgments"] is True
    assert source["search_options"] == {"batch_size": 1000, "search_context_type": "point_in_time"}
    entries = first["processor"][0]["add_entries"]["entries"]
    assert [e["key"] for e in entries] == ["_id", "_index"]
    assert entries[0]["value_expression"] == 'getMetadata("opensearch-document_id")'
    (sink,) = first["sink"]
    assert sink["s3"]["bucket"] == "acme-pcmigrate"
    assert sink["s3"]["object_key"] == {"path_prefix": "acme-docs-2026-10/raw/golden-documents/"}
    assert sink["s3"]["codec"] == {"ndjson": {}} and sink["s3"]["compression"] == "none"
    assert sink["s3"]["aws"]["sts_role_arn"] == PIPELINE_ROLE_PLACEHOLDER
    second = doc["idx1-other-index"]["sink"][0]["s3"]["object_key"]["path_prefix"]
    assert second == "acme-docs-2026-10/raw/Other.Index/"


def test_self_managed_cluster_reads_credentials_from_the_secret() -> None:
    spec = OsisPipelineSpec(
        migration_id="m",
        endpoint="https://10.0.1.5:9200",
        indices=("docs",),
        region="us-east-1",
        bucket="b",
        raw_prefix="m/raw",
        credentials_secret_arn="arn:aws:secretsmanager:us-east-1:1:secret:os-creds",
        insecure_tls=True,
    )
    doc = pipeline_document(spec)
    assert doc["extension"]["aws"]["secrets"]["source-credentials"]["secret_id"].endswith(
        "os-creds"
    )
    source = doc["idx0-docs"]["source"]["opensearch"]
    assert source["username"] == "${{aws_secrets:source-credentials:username}}"
    assert source["password"] == "${{aws_secrets:source-credentials:password}}"
    assert source["connection"] == {"insecure": True} and "aws" not in source


def test_role_and_start_time_substitution_round_trip() -> None:
    body = pipeline_body(SPEC)
    assert PIPELINE_ROLE_PLACEHOLDER in body
    filled = with_role(body, "arn:aws:iam::1:role/pipeline")
    assert (
        PIPELINE_ROLE_PLACEHOLDER not in filled
        and filled.count("arn:aws:iam::1:role/pipeline") == 4
    )
    released = yaml.safe_load(with_start_time(filled, "2026-10-05T10:00:00.000+00:00"))
    for name, pipeline in released.items():
        if name == "version":
            continue
        scheduling = pipeline["source"]["opensearch"]["scheduling"]
        assert scheduling == {"index_read_count": 1, "start_time": "2026-10-05T10:00:00.000+00:00"}
    assert (
        released["idx0-golden-documents"]["sink"][0]["s3"]["aws"]["sts_role_arn"]
        == "arn:aws:iam::1:role/pipeline"
    )
