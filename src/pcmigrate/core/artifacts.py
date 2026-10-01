"""Artifact stores: a local directory for laptop runs, an S3 prefix for the manifest zone."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from botocore.exceptions import ClientError

from pcmigrate.core.s3 import is_not_found

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class ArtifactStore(Protocol):
    """Named text artifacts: report.md, mapping.yaml, plan.lock, manifest/approvals.json."""

    def read_text(self, name: str) -> str | None:
        """Contents, or None when absent."""
        ...

    def write_text(self, name: str, text: str) -> str:
        """Write and return where it landed (path or URI)."""
        ...

    def location(self, name: str) -> str:
        """Where *name* lives, for messages."""
        ...


class LocalArtifactStore:
    """Artifacts under one directory."""

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        """The directory."""
        return self._root

    def read_text(self, name: str) -> str | None:
        """See ArtifactStore."""
        path = self._root / name
        return path.read_text() if path.exists() else None

    def write_text(self, name: str, text: str) -> str:
        """See ArtifactStore."""
        path = self._root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return str(path)

    def location(self, name: str) -> str:
        """See ArtifactStore."""
        return str(self._root / name)


class S3ArtifactStore:
    """Artifacts under s3://bucket/prefix/."""

    def __init__(self, s3: S3Client, bucket: str, prefix: str) -> None:
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, name: str) -> str:
        return f"{self._prefix}/{name}" if self._prefix else name

    def read_text(self, name: str) -> str | None:
        """See ArtifactStore."""
        try:
            body = self._s3.get_object(Bucket=self._bucket, Key=self._key(name))["Body"].read()
        except ClientError as err:
            if is_not_found(err):
                return None
            raise
        return body.decode("utf-8")

    def write_text(self, name: str, text: str) -> str:
        """See ArtifactStore."""
        self._s3.put_object(Bucket=self._bucket, Key=self._key(name), Body=text.encode("utf-8"))
        return self.location(name)

    def location(self, name: str) -> str:
        """See ArtifactStore."""
        return f"s3://{self._bucket}/{self._key(name)}"
