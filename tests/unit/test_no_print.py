"""ADR-013: no stage prints. Every user-visible output goes through the event bus."""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"


def _print_calls(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"print", "pprint"}
    ]


def test_no_module_under_src_calls_print() -> None:
    offenders = {
        str(p.relative_to(SRC)): lines for p in SRC.rglob("*.py") if (lines := _print_calls(p))
    }
    assert not offenders, offenders
