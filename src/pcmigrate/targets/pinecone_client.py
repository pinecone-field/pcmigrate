"""The one place the Pinecone SDK is imported. Pinned to API version 2026-07 (ADR-001).

Everything else in targets/ talks to a PineconeGateway, so unit tests substitute a fake.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import boto3
import pinecone

from pcmigrate.config.mapping import PINECONE_API_VERSION

TERMINAL_IMPORT_STATES = frozenset({"Completed", "Failed", "Cancelled"})


@dataclass(frozen=True, slots=True)
class IndexInfo:
    """What the tool needs to know about an index."""

    name: str
    host: str
    dimension: int | None
    metric: str | None
    cloud: str | None
    region: str | None
    ready: bool


@dataclass(frozen=True, slots=True)
class ImportInfo:
    """A snapshot of one bulk import."""

    id: str
    status: str
    percent_complete: float | None = None
    records_imported: int | None = None
    error: str | None = None

    @property
    def terminal(self) -> bool:
        """True for Completed, Failed, and Cancelled."""
        return self.status in TERMINAL_IMPORT_STATES


class PineconeGateway(Protocol):
    """The subset of the Pinecone API the shared stages use."""

    def describe_index(self, name: str) -> IndexInfo | None:
        """The index, or None when it does not exist."""
        ...

    def create_index(
        self,
        name: str,
        *,
        dimension: int,
        metric: str,
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
    ) -> IndexInfo:
        """Create a serverless dense vector index and wait until it is ready."""
        ...

    def delete_index(self, name: str) -> None:
        """Delete the index. Only tests and prepare_index rollback call this."""
        ...

    def namespace_counts(self, host: str) -> dict[str, int]:
        """Record count per namespace from index stats."""
        ...

    def list_namespaces(self, host: str) -> list[str]:
        """Every namespace name the index currently has."""
        ...

    def start_import(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        """Start a bulk import with error_mode=abort; returns the import id."""
        ...

    def describe_import(self, host: str, import_id: str) -> ImportInfo:
        """Current state of one import."""
        ...


def api_key_from_secret(secret_arn: str, region: str) -> str:
    """Read the Pinecone API key from Secrets Manager. The value never touches config."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    secret = response.get("SecretString")
    if not secret:
        raise RuntimeError(f"secret {secret_arn} has no string value")
    return str(secret).strip()


def _dense_field(schema: Any) -> tuple[int | None, str | None]:
    fields = getattr(schema, "fields", None) or {}
    for spec in fields.values():
        dimension = getattr(spec, "dimension", None)
        if dimension is not None:
            return int(dimension), getattr(spec, "metric", None)
    return None, None


def _to_info(model: Any) -> IndexInfo:
    dimension, metric = _dense_field(getattr(model, "schema", None))
    deployment = getattr(model, "deployment", None)
    status = getattr(model, "status", None)
    return IndexInfo(
        name=model.name,
        host=model.host or "",
        dimension=dimension,
        metric=metric,
        cloud=getattr(deployment, "cloud", None),
        region=getattr(deployment, "region", None),
        ready=bool(getattr(status, "ready", False)),
    )


class SdkGateway:
    """PineconeGateway backed by the official SDK."""

    def __init__(self, api_key: str, *, source_tag: str = "pcmigrate") -> None:
        self._pc = pinecone.Pinecone(api_key=api_key, source_tag=source_tag)
        self._indexes: dict[str, Any] = {}

    @property
    def api_version(self) -> str:
        """The API version the SDK is pinned to."""
        return PINECONE_API_VERSION

    def _index(self, host: str) -> Any:
        if host not in self._indexes:
            self._indexes[host] = self._pc.index(host=host)
        return self._indexes[host]

    def describe_index(self, name: str) -> IndexInfo | None:
        """See PineconeGateway."""
        if not self._pc.has_index(name):
            return None
        return _to_info(self._pc.describe_index(name))

    def create_index(
        self,
        name: str,
        *,
        dimension: int,
        metric: str,
        cloud: str,
        region: str,
        tags: dict[str, str] | None = None,
    ) -> IndexInfo:
        """See PineconeGateway."""
        model = self._pc.create_index(
            name=name,
            dimension=dimension,
            metric=metric,
            spec=pinecone.ServerlessSpec(cloud=cloud, region=region),
            tags=tags,
        )
        return _to_info(model)

    def delete_index(self, name: str) -> None:
        """See PineconeGateway."""
        self._pc.delete_index(name)

    def namespace_counts(self, host: str) -> dict[str, int]:
        """See PineconeGateway."""
        stats = self._index(host).describe_index_stats()
        return {name: int(summary.vector_count) for name, summary in stats.namespaces.items()}

    def list_namespaces(self, host: str) -> list[str]:
        """See PineconeGateway."""
        names: list[str] = []
        for page in self._index(host).list_namespaces():
            names.extend(ns.name for ns in page.namespaces)
        return names

    def start_import(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        """See PineconeGateway."""
        response = self._index(host).start_import(
            uri=uri, error_mode="abort", integration_id=integration_id
        )
        return str(response.id)

    def describe_import(self, host: str, import_id: str) -> ImportInfo:
        """See PineconeGateway."""
        model = self._index(host).describe_import(import_id)
        return ImportInfo(
            id=str(model.id),
            status=str(model.status),
            percent_complete=model.percent_complete,
            records_imported=model.records_imported,
            error=model.error,
        )
