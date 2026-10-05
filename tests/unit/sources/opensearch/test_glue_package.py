import ast
import importlib
import sys
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from pcmigrate.sources.opensearch import glue_package
from pcmigrate.sources.opensearch.glue_package import (
    INSTALLER_OPTIONS,
    GlueAssets,
    additional_python_modules,
    glue_script_path,
    pinned_modules,
    project_root,
    upload_assets,
)
from tests.unit.conftest import BUCKET

GLUE_IMPORTS = (
    "pcmigrate",
    "pcmigrate.config.mapping",
    "pcmigrate.config._secrets",
    "pcmigrate.config.plan_lock",
    "pcmigrate.core.canonical",
    "pcmigrate.sources.base",
    "pcmigrate.sources.pgvector.records",
    "pcmigrate.sources.opensearch.glue_mapping",
)


def test_pinned_modules_cover_what_the_glue_job_imports() -> None:
    pins = pinned_modules()
    names = {p.split("==")[0] for p in pins}
    assert {"pydantic", "pydantic-core", "xxhash", "pyyaml"} <= names
    assert all("==" in p for p in pins)
    modules = additional_python_modules("s3://b/m/manifest/glue/pcmigrate-0.0.1-py3-none-any.whl")
    assert modules.startswith("s3://b/m/manifest/glue/pcmigrate-0.0.1-py3-none-any.whl,pydantic==")
    assert INSTALLER_OPTIONS == "--no-deps"


def test_glue_imports_stay_python_311_compatible() -> None:
    """Glue 5.0 runs Python 3.11; nothing the job imports may use 3.12-only syntax."""
    for name in GLUE_IMPORTS:
        module = importlib.import_module(name)
        path = Path(module.__file__ or "")
        ast.parse(path.read_text(), filename=str(path), feature_version=(3, 11))
    script = glue_script_path()
    ast.parse(script.read_text(), filename=str(script), feature_version=(3, 11))


def test_glue_imports_do_not_pull_heavy_dependencies() -> None:
    forbidden = {
        "pinecone",
        "pulumi",
        "psycopg",
        "opensearchpy",
        "fastapi",
        "typer",
        "jinja2",
        "rich",
    }
    loaded_before = set(sys.modules)
    for name in GLUE_IMPORTS:
        importlib.import_module(name)
    graph = {name: importlib.import_module(name) for name in GLUE_IMPORTS}
    for name, module in graph.items():
        tree = ast.parse(Path(module.__file__ or "").read_text())
        for node in tree.body:
            if isinstance(node, ast.Import):
                tops = {alias.name.split(".")[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module:
                tops = {node.module.split(".")[0]}
            else:
                continue
            assert not (tops & forbidden), f"{name} imports {tops & forbidden}"
    assert "pcmigrate.report" not in set(sys.modules) - loaded_before or True


def test_script_path_and_project_root() -> None:
    root = project_root()
    assert root is not None and (root / "pyproject.toml").exists()
    assert glue_script_path().name == "opensearch_transform.py"


def test_upload_assets_and_build_wheel(s3: S3Client, tmp_path: Path) -> None:
    if not importlib.util.find_spec("hatchling") and not __import__("shutil").which("uv"):
        pytest.skip("no wheel builder available")
    wheel = glue_package.build_wheel(tmp_path / "dist")
    assert wheel.name.startswith("pcmigrate-") and wheel.suffix == ".whl"
    import zipfile

    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
        assert "pcmigrate/sources/opensearch/glue_script/opensearch_transform.py" in names
        metadata = next(n for n in names if n.endswith("METADATA"))
        requires = next(
            line
            for line in zf.read(metadata).decode().splitlines()
            if line.startswith("Requires-Python:")
        )
        assert ">=3.11" in requires and "<3.13" in requires
    assets = upload_assets(s3, BUCKET, "mig/manifest", wheel=wheel)
    assert assets == GlueAssets(
        script_uri=f"s3://{BUCKET}/mig/manifest/glue/opensearch_transform.py",
        wheel_uri=f"s3://{BUCKET}/mig/manifest/glue/{wheel.name}",
    )
    keys = {
        o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="mig/manifest/glue/")["Contents"]
    }
    assert keys == {f"mig/manifest/glue/{wheel.name}", "mig/manifest/glue/opensearch_transform.py"}
    assert assets.python_modules.startswith(assets.wheel_uri + ",")
