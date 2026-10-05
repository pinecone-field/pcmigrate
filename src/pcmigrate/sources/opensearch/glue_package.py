"""Package pcmigrate as a wheel for Glue's --additional-python-modules and stage the job assets.

Glue 5.0 ships pyarrow and boto3 but not pydantic or xxhash, so the module list carries the
wheel plus the small runtime dependencies the Glue job imports, pinned to the versions in
the runner's environment. ``--no-deps`` keeps pip from pulling the CLI's heavy dependencies
(Pulumi, the Pinecone SDK, psycopg) onto Glue workers.
"""

from __future__ import annotations

import importlib.metadata
import shutil
import subprocess
import sys
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

GLUE_RUNTIME_MODULES = (
    "pydantic",
    "pydantic-core",
    "annotated-types",
    "typing-extensions",
    "typing-inspection",
    "xxhash",
    "pyyaml",
)
INSTALLER_OPTIONS = "--no-deps"
GLUE_VERSION = "5.0"
GLUE_SCRIPT_NAME = "opensearch_transform.py"


def pinned_modules() -> list[str]:
    """``name==version`` for every runtime module installed here."""
    out: list[str] = []
    for name in GLUE_RUNTIME_MODULES:
        try:
            out.append(f"{name}=={importlib.metadata.version(name)}")
        except importlib.metadata.PackageNotFoundError:
            continue
    return out


def additional_python_modules(wheel_uri: str) -> str:
    """The value for ``--additional-python-modules``."""
    return ",".join([wheel_uri, *pinned_modules()])


def project_root() -> Path | None:
    """The checkout holding pyproject.toml, when running from an editable install."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "pyproject.toml"
        if candidate.exists() and 'name = "pcmigrate"' in candidate.read_text():
            return parent
    return None


def build_wheel(dest: Path, *, root: Path | None = None) -> Path:
    """Build the pcmigrate wheel into *dest* with uv, falling back to pip."""
    root = root or project_root()
    if root is None:
        raise RuntimeError("not an editable checkout; set opensearch.wheel_path in pcmigrate.yaml")
    dest.mkdir(parents=True, exist_ok=True)
    if shutil.which("uv"):
        command = ["uv", "build", "--wheel", "--out-dir", str(dest), str(root)]
    else:
        command = [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", str(dest), str(root)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    wheels = sorted(dest.glob("pcmigrate-*.whl"), key=lambda p: p.stat().st_mtime)
    if not wheels:
        raise RuntimeError(f"no wheel produced in {dest}")
    return wheels[-1]


def glue_script_path() -> Path:
    """The Glue job script, shipped inside the package."""
    with resources.as_file(
        resources.files("pcmigrate.sources.opensearch").joinpath("glue_script", GLUE_SCRIPT_NAME)
    ) as path:
        if path.exists():
            return path
    root = project_root()
    if root is not None and (root / "glue" / GLUE_SCRIPT_NAME).exists():
        return root / "glue" / GLUE_SCRIPT_NAME
    raise RuntimeError("Glue script not found in the package or the checkout")


@dataclass(frozen=True, slots=True)
class GlueAssets:
    """Where the staged script and wheel live."""

    script_uri: str
    wheel_uri: str

    @property
    def python_modules(self) -> str:
        """The ``--additional-python-modules`` value."""
        return additional_python_modules(self.wheel_uri)


def upload_assets(
    s3: S3Client, bucket: str, prefix: str, *, wheel: Path, script: Path | None = None
) -> GlueAssets:
    """Put the wheel and script under ``<prefix>/glue/`` and return their URIs."""
    script = script or glue_script_path()
    base = f"{prefix.rstrip('/')}/glue"
    wheel_key = f"{base}/{wheel.name}"
    script_key = f"{base}/{GLUE_SCRIPT_NAME}"
    s3.put_object(Bucket=bucket, Key=wheel_key, Body=wheel.read_bytes())
    s3.put_object(Bucket=bucket, Key=script_key, Body=script.read_bytes())
    return GlueAssets(
        script_uri=f"s3://{bucket}/{script_key}", wheel_uri=f"s3://{bucket}/{wheel_key}"
    )
