import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "pcmigrate"


def _modules() -> list[Path]:
    return sorted(p for p in SRC.rglob("*.py"))


def test_every_module_has_a_docstring() -> None:
    missing = [p for p in _modules() if ast.get_docstring(ast.parse(p.read_text())) is None]
    assert not missing, [str(p.relative_to(SRC)) for p in missing]


def test_only_targets_imports_the_pinecone_sdk() -> None:
    offenders: list[str] = []
    for path in _modules():
        if path.relative_to(SRC).parts[0] == "targets":
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            if any(n == "pinecone" or n.startswith("pinecone.") for n in names):
                offenders.append(str(path.relative_to(SRC)))
    assert not offenders, offenders
