"""The one place that talks HTTP to OpenSearch, with a read-only request audit.

Every request passes through ``OpenSearchClient.request``. In read-only mode the audit allows
GET and HEAD anywhere, POST only to search-family endpoints, and DELETE only for closing a
point-in-time, so introspection and export cannot write by construction. Provisioning opens
a separate writable client for the security role mapping.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

READ_POST_SUFFIXES = (
    "/_search",
    "/_count",
    "/_msearch",
    "/_mget",
    "/_search/point_in_time",
    "/_pit",
    "/_search/scroll",
    "/_field_caps",
    "/_validate/query",
)
READ_DELETE_PATHS = ("/_search/point_in_time", "/_search/scroll")
TARGET_PAGE_BYTES = 3 * 1024 * 1024
VECTOR_JSON_BYTES = 10


def vector_page_size(dimension: int | None, *, cap: int = 1000, floor: int = 25) -> int:
    """Documents per page so that a page of full vectors stays near TARGET_PAGE_BYTES.

    Amazon OpenSearch Service applies search backpressure per task (a few MB of heap on
    small instances), so pages are sized by vector width rather than by document count.
    """
    if not dimension:
        return cap
    return max(floor, min(cap, TARGET_PAGE_BYTES // (dimension * VECTOR_JSON_BYTES)))


def is_backpressure(err: Exception) -> bool:
    """True for the 429 OpenSearch returns when a search task exceeds its heap budget."""
    status = getattr(err, "status_code", None)
    return status == 429 or "heap usage exceeded" in str(err)


_AWS_HOST = re.compile(
    r"\.([a-z]{2}-[a-z]+-\d)\.(es|aoss)\.amazonaws\.com$|\.(aos)\.([a-z]{2}-[a-z]+-\d)\.on\.aws$"
)


class ReadOnlyViolationError(RuntimeError):
    """A read-only client was asked to send something other than a read."""


@dataclass
class RequestAudit:
    """Every request the client sent. Tests assert all of them are reads."""

    requests: list[tuple[str, str]] = field(default_factory=list)

    def check(self, method: str, path: str) -> None:
        """Record the request, raising if it could write."""
        verb = method.upper()
        clean = path.split("?", 1)[0].rstrip("/") or "/"
        allowed = (
            verb in ("GET", "HEAD")
            or (verb == "POST" and clean.endswith(READ_POST_SUFFIXES))
            or (verb == "DELETE" and clean in READ_DELETE_PATHS)
        )
        if not allowed:
            raise ReadOnlyViolationError(f"refusing non-read request {verb} {path}")
        self.requests.append((verb, path))


@dataclass(frozen=True, slots=True)
class ConnectionOptions:
    """How to reach the cluster. Credentials come from the environment or SigV4, never config."""

    url: str
    auth: str = "sigv4"
    basic_auth: tuple[str, str] | None = None
    region: str | None = None
    verify_certs: bool = True
    timeout_s: int = 120

    @property
    def is_aws(self) -> bool:
        """True for an Amazon OpenSearch Service or Serverless endpoint."""
        return _AWS_HOST.search(urlparse(self.url).hostname or "") is not None

    @property
    def aws_region(self) -> str | None:
        """Region from the endpoint hostname, or the one given."""
        if self.region:
            return self.region
        match = _AWS_HOST.search(urlparse(self.url).hostname or "")
        return (match.group(1) or match.group(4)) if match else None

    @property
    def aws_service(self) -> str:
        """``es`` for domains, ``aoss`` for serverless collections."""
        match = _AWS_HOST.search(urlparse(self.url).hostname or "")
        if not match:
            return "es"
        return "aoss" if match.group(2) == "aoss" else "es"


def _build(options: ConnectionOptions) -> Any:
    from opensearchpy import OpenSearch, RequestsHttpConnection

    parsed = urlparse(options.url)
    kwargs: dict[str, Any] = {
        "hosts": [options.url],
        "use_ssl": parsed.scheme == "https",
        "verify_certs": options.verify_certs,
        "ssl_show_warn": False,
        "timeout": options.timeout_s,
        "connection_class": RequestsHttpConnection,
    }
    if options.auth == "sigv4" and options.is_aws:
        import boto3
        from opensearchpy import AWSV4SignerAuth

        credentials = boto3.Session().get_credentials()
        if credentials is None:
            raise RuntimeError("no AWS credentials for SigV4 to OpenSearch")
        kwargs["http_auth"] = AWSV4SignerAuth(
            credentials, options.aws_region or "us-east-1", options.aws_service
        )
    elif options.auth == "basic" and options.basic_auth is not None:
        kwargs["http_auth"] = options.basic_auth
    return OpenSearch(**kwargs)


class OpenSearchClient:
    """A thin, audited wrapper over opensearch-py's low-level transport."""

    def __init__(
        self, options: ConnectionOptions, *, read_only: bool = True, transport: Any = None
    ) -> None:
        self.options = options
        self.read_only = read_only
        self.audit = RequestAudit()
        self._transport = transport

    def _perform(self, method: str, path: str, params: Any, body: Any) -> Any:
        if self._transport is None:
            self._transport = _build(self.options).transport
        return self._transport.perform_request(method, path, params=params, body=body)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any = None,
    ) -> Any:
        """Send one request and return the decoded JSON body."""
        if self.read_only:
            self.audit.check(method, path)
        else:
            self.audit.requests.append((method.upper(), path))
        return self._perform(method, path, dict(params or {}), body)

    def get(self, path: str, **params: Any) -> Any:
        """GET helper."""
        return self.request("GET", path, params=params)

    def exists(self, path: str) -> bool:
        """HEAD helper, False on 404."""
        from opensearchpy.exceptions import NotFoundError

        try:
            self.request("HEAD", path)
        except NotFoundError:
            return False
        return True

    def search(self, index: str, body: Mapping[str, Any], **params: Any) -> dict[str, Any]:
        """POST _search."""
        result: dict[str, Any] = self.request(
            "POST", f"/{index}/_search", params=params, body=dict(body)
        )
        return result

    def count(self, index: str, body: Mapping[str, Any] | None = None) -> int:
        """Document count, optionally under a query."""
        result = self.request("POST", f"/{index}/_count", body=dict(body) if body else None)
        return int(result["count"])

    def close(self) -> None:
        """Close the underlying connection pool."""
        if self._transport is not None:
            self._transport.close()
            self._transport = None
