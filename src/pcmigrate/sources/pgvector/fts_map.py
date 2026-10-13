"""Translate PostgreSQL text search into Pinecone ``full_text_search`` settings (ADR-018).

A tsvector column or GIN expression index names a text search configuration and the
columns it reads. The configuration maps to a language plus stemming and stop words: the
built-in snowball configurations stem and drop stop words, ``simple`` does neither, and a
custom configuration is judged by its dictionaries. Everything the expression does that
Pinecone's analyzer will not (``setweight`` weights, ``unaccent``, synonym and thesaurus
dictionaries, JSON extraction, concatenation) is reported as unsupported.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from pcmigrate.sources.fts import DEFAULT_LANGUAGE, FtsTranslation, language_code
from pcmigrate.sources.inventory import ObjectInfo, TextSearchField

SNOWBALL_CONFIGS = frozenset(
    {
        "arabic",
        "armenian",
        "basque",
        "catalan",
        "danish",
        "dutch",
        "english",
        "finnish",
        "french",
        "german",
        "greek",
        "hindi",
        "hungarian",
        "indonesian",
        "irish",
        "italian",
        "lithuanian",
        "nepali",
        "norwegian",
        "portuguese",
        "romanian",
        "russian",
        "serbian",
        "spanish",
        "swedish",
        "tamil",
        "turkish",
        "yiddish",
    }
)
TRANSPARENT_FUNCTIONS = frozenset(
    {"to_tsvector", "coalesce", "setweight", "char", "text", "regconfig"}
)
TSVECTOR_BUILDERS = frozenset({"jsonb_to_tsvector", "json_to_tsvector", "array_to_tsvector"})
SQL_KEYWORDS = frozenset(
    {"and", "or", "not", "null", "true", "false", "case", "when", "then", "else", "end", "as", "is"}
)

_CONFIG = re.compile(r"to_tsvector\(\s*'([^']+)'(?:::regconfig)?\s*,", re.IGNORECASE)
_JSON_PATH = re.compile(r"\(?\s*(\"?[A-Za-z_][A-Za-z0-9_]*\"?)\s*->>?\s*'([^']+)'")
_IDENTIFIER = re.compile(r'"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*)')
_CALL = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_STRING = re.compile(r"'(?:[^']|'')*'")


@dataclass
class ParsedExpression:
    """What a ``to_tsvector`` expression reads and does."""

    configs: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    functions: list[str] = field(default_factory=list)
    weighted: bool = False
    concatenated: bool = False
    json_paths: list[str] = field(default_factory=list)

    @property
    def config(self) -> str | None:
        """The first configuration named, without the ``pg_catalog.`` prefix."""
        return strip_schema(self.configs[0]) if self.configs else None


def strip_schema(config: str) -> str:
    """``pg_catalog.english`` to ``english``."""
    return config.rsplit(".", 1)[-1].strip().lower()


def parse_expression(expression: str, columns: Sequence[str]) -> ParsedExpression:
    """Read the regconfig, the table columns, JSON paths, and functions out of an expression."""
    parsed = ParsedExpression()
    for match in _CONFIG.finditer(expression):
        if match.group(1) not in parsed.configs:
            parsed.configs.append(match.group(1))
    known = {c.lower(): c for c in columns}
    for match in _JSON_PATH.finditer(expression):
        column = match.group(1).strip('"')
        if column.lower() in known:
            path = f"{known[column.lower()]}.{match.group(2)}"
            if path not in parsed.json_paths:
                parsed.json_paths.append(path)
    without_strings = _STRING.sub("''", _JSON_PATH.sub(" ", expression))
    for match in _CALL.finditer(without_strings):
        name = match.group(1).lower()
        if name not in TRANSPARENT_FUNCTIONS and name not in parsed.functions:
            parsed.functions.append(name)
    parsed.weighted = "setweight(" in without_strings.lower()
    parsed.concatenated = "||" in without_strings
    for match in _IDENTIFIER.finditer(without_strings):
        quoted, bare = match.group(1), match.group(2)
        name = quoted if quoted is not None else bare
        if name is None or name.lower() in SQL_KEYWORDS:
            continue
        end = match.end()
        if without_strings[end:].lstrip().startswith("("):
            continue
        column = known.get(name.lower()) if quoted is None else (name if name in columns else None)
        if column is not None and column not in parsed.columns:
            parsed.columns.append(column)
    return parsed


def translate_config(
    translation: FtsTranslation,
    config: str | None,
    dictionaries: Iterable[str] = (),
    default_config: str | None = None,
) -> None:
    """Set language, stemming, and stop words from a text search configuration name."""
    name = config
    if name is None:
        if default_config:
            name = strip_schema(default_config)
            translation.note(
                f"to_tsvector names no configuration; the server default {name!r} is assumed"
            )
        else:
            translation.note("to_tsvector names no configuration; english is assumed")
            name = "english"
    name = strip_schema(name)
    translation.analyzer = name
    if name == "simple":
        translation.language = DEFAULT_LANGUAGE
        translation.note("simple configuration: no stemming and no stop words")
        return
    if name in SNOWBALL_CONFIGS:
        code = language_code(name)
        if code is None:
            translation.language = DEFAULT_LANGUAGE
            translation.lose(
                f"text search configuration {name!r}: Pinecone has no {name} analysis; "
                "tokenized as en without stemming"
            )
            return
        translation.language = code
        translation.stemming = True
        translation.stop_words = True
        return
    translation.note(f"custom text search configuration {name!r}")
    stemmed = False
    for entry in dictionaries:
        dict_name, _, template = entry.partition(":")
        template = template or dict_name
        if template == "snowball" or dict_name.endswith("_stem"):
            code = language_code(dict_name.removesuffix("_stem"))
            if code is not None:
                translation.language = code
                translation.stemming = True
                translation.stop_words = True
                stemmed = True
            else:
                translation.lose(
                    f"dictionary {dict_name!r} ({template}) stems a language Pinecone does "
                    "not analyze"
                )
        elif template == "simple":
            continue
        else:
            translation.lose(f"dictionary {dict_name!r} ({template}) has no Pinecone equivalent")
    if not stemmed:
        translation.language = DEFAULT_LANGUAGE
        if not any(d for d in dictionaries):
            translation.lose(
                f"custom configuration {name!r}: dictionaries unknown, so its analysis is "
                "not reproduced"
            )


def translate_field(
    text: TextSearchField,
    table: ObjectInfo,
    *,
    default_config: str | None = None,
) -> list[FtsTranslation]:
    """One translation per source column of a tsvector column or GIN expression index."""
    columns = [c.name for c in table.columns]
    derived = text.column
    if text.expression is None:
        lone = FtsTranslation(source=text.column, derived_from=derived, searchable=False)
        lone.lose(
            "tsvector column is not generated from an expression (trigger or application "
            "maintained), so the source text column is unknown; name it with --fts-from"
        )
        return [lone]
    parsed = parse_expression(text.expression, columns)
    sources = [*parsed.columns, *parsed.json_paths] or list(text.source_columns)
    if not sources:
        lone = FtsTranslation(
            source=text.column, derived_from=derived, expression=text.expression, searchable=False
        )
        lone.lose("to_tsvector expression reads no table column the export can carry")
        return [lone]
    out: list[FtsTranslation] = []
    for source in sources:
        translation = FtsTranslation(
            source=source, derived_from=derived, expression=text.expression
        )
        translate_config(
            translation, parsed.config or text.config, text.dictionaries, default_config
        )
        translation.finalize()
        if len(parsed.configs) > 1:
            translation.lose(
                "several text search configurations in one expression "
                f"({', '.join(parsed.configs)}); "
                f"{translation.analyzer!r} is used for every field"
            )
        if parsed.weighted:
            translation.lose(
                "setweight() weights (A-D): BM25 in Pinecone weights every field equally and "
                "ts_rank weights are not applied"
            )
        if len(sources) > 1:
            translation.note(
                f"{len(sources)} columns concatenated in one tsvector become {len(sources)} "
                "full-text fields; rank them together with one text clause over all of them"
            )
        for name in parsed.functions:
            if name in TSVECTOR_BUILDERS:
                translation.lose(
                    f"{name}(): the JSON or array value is carried as text and tokenized whole"
                )
            else:
                translation.lose(
                    f"{name}() in the tsvector expression is not applied by Pinecone's analyzer"
                )
        if text.gin_index is None and text.kind == "tsvector":
            translation.note("no GIN index on the tsvector; source ranking scans the table")
        out.append(translation)
    return out


def translate_table(
    table: ObjectInfo, *, default_config: str | None = None, extra: Sequence[str] = ()
) -> list[FtsTranslation]:
    """Every full-text translation of a table, one per source column, first definition wins.

    ``extra`` names plain text columns the FDE asked to carry (``--fts-from``); they get the
    server default configuration.
    """
    out: list[FtsTranslation] = []
    seen: dict[str, FtsTranslation] = {}
    for text in table.text_search:
        for translation in translate_field(text, table, default_config=default_config):
            earlier = seen.get(translation.source)
            if earlier is not None:
                if earlier.spec() != translation.spec():
                    earlier.lose(
                        f"also analyzed by {translation.derived_from!r} with "
                        f"{translation.analyzer!r}; one configuration per field, "
                        f"{earlier.analyzer!r} from {earlier.derived_from!r} is kept"
                    )
                continue
            seen[translation.source] = translation
            out.append(translation)
    for name in extra:
        if name in seen or table.column(name) is None:
            continue
        translation = FtsTranslation(source=name, derived_from=None)
        translate_config(translation, None, (), default_config)
        translation.note("carried on request (--fts-from); no tsvector or index in the source")
        seen[name] = translation
        out.append(translation)
    return out
