import pytest

from pcmigrate.sources.opensearch.client import (
    ConnectionOptions,
    ReadOnlyViolationError,
    RequestAudit,
)


def test_aws_endpoint_detection() -> None:
    aws = ConnectionOptions("https://search-golden-abc123.us-west-2.es.amazonaws.com")
    assert aws.is_aws and aws.aws_region == "us-west-2" and aws.aws_service == "es"
    serverless = ConnectionOptions("https://abc.us-east-1.aoss.amazonaws.com", region="eu-west-1")
    assert serverless.is_aws and serverless.aws_region == "eu-west-1"
    assert serverless.aws_service == "aoss"
    local = ConnectionOptions("http://localhost:9200", auth="none")
    assert not local.is_aws and local.aws_region is None


@pytest.mark.parametrize(
    ("method", "path", "ok"),
    [
        ("GET", "/_cat/indices", True),
        ("HEAD", "/golden", True),
        ("POST", "/golden/_search", True),
        ("POST", "/golden/_count", True),
        ("POST", "/golden/_mget", True),
        ("POST", "/golden/_search/point_in_time?keep_alive=1m", True),
        ("DELETE", "/_search/point_in_time", True),
        ("POST", "/golden/_doc", False),
        ("PUT", "/golden", False),
        ("DELETE", "/golden", False),
        ("POST", "/golden/_delete_by_query", False),
        ("POST", "/_plugins/_security/api/rolesmapping/x", False),
    ],
)
def test_audit_allows_only_reads(method: str, path: str, ok: bool) -> None:
    audit = RequestAudit()
    if ok:
        audit.check(method, path)
        assert audit.requests == [(method, path)]
    else:
        with pytest.raises(ReadOnlyViolationError):
            audit.check(method, path)
