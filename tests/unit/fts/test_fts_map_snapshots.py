"""Snapshot tests for the analyzer translation tables (ADR-018).

Set UPDATE_SNAPSHOTS=1 to rewrite the expected files after an intended change.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pcmigrate.sources.fts import FtsTranslation
from pcmigrate.sources.inventory import ColumnInfo, ObjectInfo, TextSearchField
from pcmigrate.sources.opensearch.fts_map import AnalysisSettings
from pcmigrate.sources.opensearch.fts_map import translate_field as translate_os_field
from pcmigrate.sources.pgvector.fts_map import parse_expression
from pcmigrate.sources.pgvector.fts_map import translate_field as translate_pg_field

GOLDEN = Path(__file__).resolve().parents[2] / "fixtures" / "golden"


def _as_dict(translation: FtsTranslation) -> dict[str, Any]:
    return {
        "source": translation.source,
        "searchable": translation.searchable,
        "full_text_search": translation.spec().model_dump(mode="json", exclude_none=True)
        if translation.searchable
        else None,
        "analyzer": translation.analyzer,
        "derived_from": translation.derived_from,
        "unsupported": list(translation.unsupported),
        "notes": list(translation.notes),
    }


def _check(name: str, actual: dict[str, Any], expected_path: Path) -> None:
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        expected_path.write_text(json.dumps(actual, indent=2) + "\n")
    expected = json.loads(expected_path.read_text())
    assert actual == expected, f"{name}: analyzer translation changed; UPDATE_SNAPSHOTS=1 to accept"


def test_opensearch_analyzer_translation_snapshot() -> None:
    fixture = json.loads((GOLDEN / "opensearch" / "analyzers.json").read_text())
    analysis = AnalysisSettings.from_settings(fixture["settings"]["analysis"])
    actual: dict[str, Any] = {}
    for name, mapping in fixture["fields"].items():
        text = TextSearchField(
            column=name,
            kind=str(mapping.get("type", "text")),
            analyzer=mapping.get("analyzer"),
            search_analyzer=mapping.get("search_analyzer"),
            multi_fields={
                sub: str(sub_mapping.get("type", "keyword"))
                for sub, sub_mapping in (mapping.get("fields") or {}).items()
            },
            mapping={
                k: v
                for k, v in mapping.items()
                if k not in ("type", "analyzer", "search_analyzer", "fields")
            },
        )
        actual[name] = _as_dict(translate_os_field(text, analysis))
    _check("opensearch", actual, GOLDEN / "opensearch" / "expected" / "fts_translation.json")
    assert actual["title"]["full_text_search"] == {
        "language": "en",
        "stemming": True,
        "stop_words": True,
    }
    assert actual["summary"]["full_text_search"]["stop_words"] is False
    assert any("stop" in u for u in actual["summary"]["unsupported"])
    assert actual["suggest"]["full_text_search"]["ngram"] == {
        "min_gram": 2,
        "max_gram": 10,
        "prefix_only": True,
    }
    assert actual["sku_text"]["searchable"] is False
    assert any("synonym" in u for u in actual["product"]["unsupported"])
    assert any("boost" in u for u in actual["html_body"]["unsupported"])


def test_pgvector_tsvector_translation_snapshot() -> None:
    fixture = json.loads((GOLDEN / "pgvector" / "tsvector_expressions.json").read_text())
    table = ObjectInfo(
        schema_name="public",
        name="documents",
        relkind="r",
        columns=[
            ColumnInfo(name=c, type="text", base_type="text", nullable=True, position=i)
            for i, c in enumerate(fixture["columns"], 1)
        ],
    )
    actual: dict[str, Any] = {}
    for case in fixture["cases"]:
        parsed = (
            parse_expression(case["expression"], fixture["columns"]) if case["expression"] else None
        )
        text = TextSearchField(
            column=case["column"],
            kind=case["kind"],
            expression=case["expression"],
            gin_index=case["gin_index"],
            config=parsed.config if parsed else None,
            source_columns=[*parsed.columns, *parsed.json_paths] if parsed else [],
            dictionaries=case.get("dictionaries", []),
        )
        actual[case["name"]] = {
            "parsed": None
            if parsed is None
            else {
                "configs": parsed.configs,
                "columns": parsed.columns,
                "json_paths": parsed.json_paths,
                "functions": parsed.functions,
                "weighted": parsed.weighted,
                "concatenated": parsed.concatenated,
            },
            "translations": [
                _as_dict(t)
                for t in translate_pg_field(text, table, default_config="pg_catalog.english")
            ],
        }
    _check("pgvector", actual, GOLDEN / "pgvector" / "expected" / "fts_translation.json")
    english = actual["english_body"]["translations"][0]
    assert english["source"] == "body" and english["full_text_search"] == {
        "language": "en",
        "stemming": True,
        "stop_words": True,
    }
    simple = actual["simple_summary_index"]["translations"][0]
    assert simple["source"] == "summary" and simple["full_text_search"]["stemming"] is False
    weighted = actual["weighted_title_body"]["translations"]
    assert [t["source"] for t in weighted] == ["title", "body"]
    assert any("setweight" in u for u in weighted[0]["unsupported"])
    assert actual["jsonb_path"]["translations"][0]["source"] == "attrs.abstract"
    assert actual["trigger_maintained"]["translations"][0]["searchable"] is False
    assert actual["quoted_identifier"]["translations"][0]["source"] == "Caption"
