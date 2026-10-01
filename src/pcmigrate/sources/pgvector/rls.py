"""Classify row-level security policies into namespace, filter, denormalize, or does-not-map.

Ported from pinecone-field/pgvector-migration (README, "Access control"), which sorts a
policy's USING expression by shape. The rules here are deliberately conservative: anything
the heuristics do not recognise is reported as not mapping, so a human looks at it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pcmigrate.sources.pgvector.models import RlsClass

_SESSION_VALUE = re.compile(
    r"current_setting\(|current_user|current_role|session_user|auth\.|request\.", re.IGNORECASE
)
_SUBQUERY = re.compile(r"\b(EXISTS|SELECT|IN\s*\()", re.IGNORECASE)
_IDENT = r"(?:\"[^\"]+\"|[a-zA-Z_][a-zA-Z0-9_]*)"
_EQUALITY = re.compile(
    rf"^\(?\s*(?P<col>{_IDENT})\s*(?:::\w+\s*)?=\s*(?P<rhs>.+?)\s*\)?$", re.DOTALL
)
_IN_LIST = re.compile(rf"^\(?\s*(?P<col>{_IDENT})\s*=\s*ANY\s*\(", re.IGNORECASE)
_ARRAY_CONTAINS = re.compile(rf"(?P<col>{_IDENT})\s*(?:&&|@>)", re.IGNORECASE)
_FUNCTION_CALL = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\s*\(")
_ALLOWED_FUNCTIONS = {"current_setting", "any", "array", "coalesce", "lower", "upper", "nullif"}
_SQL_KEYWORDS = {
    "and",
    "or",
    "not",
    "in",
    "all",
    "some",
    "exists",
    "case",
    "when",
    "then",
    "else",
    "end",
    "is",
    "null",
    "true",
    "false",
    "between",
    "like",
    "ilike",
}
_COMPARATORS = re.compile(r"\s(=|<>|!=|<=|>=|<|>)\s")


def _strip_casts(expr: str) -> str:
    return re.sub(r"::[a-zA-Z_][a-zA-Z0-9_ ]*(\[\])?", "", expr)


def _unquote(ident: str) -> str:
    return ident[1:-1] if ident.startswith('"') and ident.endswith('"') else ident


def _unknown_functions(expr: str) -> set[str]:
    names = {m.group(0).rstrip("( ").lower() for m in _FUNCTION_CALL.finditer(expr)}
    return {n for n in names if n not in _ALLOWED_FUNCTIONS and n not in _SQL_KEYWORDS}


def classify_qual(qual: str | None, columns: Iterable[str]) -> tuple[RlsClass, str | None, str]:
    """Return (classification, column, note) for one USING expression."""
    if not qual or qual.strip().lower() in ("true", "(true)"):
        return RlsClass.FILTER, None, "permissive policy; no restriction to carry over"
    expr = _strip_casts(qual.strip())
    known = set(columns)

    if _SUBQUERY.search(expr):
        return (
            RlsClass.DENORMALIZE,
            None,
            "policy joins another table; flatten the membership onto each record, then filter",
        )

    unknown = _unknown_functions(expr)
    if unknown:
        return (
            RlsClass.DOES_NOT_MAP,
            None,
            f"policy calls {sorted(unknown)}; keep the decision in the application",
        )

    in_match = _IN_LIST.match(expr)
    if in_match and _unquote(in_match.group("col")) in known:
        column = _unquote(in_match.group("col"))
        return RlsClass.FILTER, column, f"copy {column} to metadata and filter with $in"

    match = _EQUALITY.match(expr)
    if (
        match
        and _unquote(match.group("col")) in known
        and not _COMPARATORS.search(match.group("rhs"))
    ):
        column = _unquote(match.group("col"))
        if _SESSION_VALUE.search(match.group("rhs")):
            return (
                RlsClass.NAMESPACE,
                column,
                f"one namespace per {column}, or filter {{{column}: {{$eq: <session value>}}}}",
            )
        return RlsClass.FILTER, column, f"copy {column} to metadata and filter with $eq"

    contains = _ARRAY_CONTAINS.search(expr)
    if contains and _unquote(contains.group("col")) in known:
        column = _unquote(contains.group("col"))
        return RlsClass.FILTER, column, f"store {column} as a list field and filter with $in"

    referenced = [c for c in known if re.search(rf"\b{re.escape(c)}\b", expr)]
    if referenced and " AND " in expr.upper() and not _SESSION_VALUE.search(expr):
        return (
            RlsClass.FILTER,
            None,
            f"copy {sorted(referenced)} to metadata and combine $eq filters with $and",
        )
    if referenced and _SESSION_VALUE.search(expr):
        return (
            RlsClass.FILTER,
            referenced[0],
            f"compares {sorted(referenced)} with a session value; filter server-side",
        )
    return (
        RlsClass.DOES_NOT_MAP,
        None,
        "policy shape not recognised; review by hand and enforce in the application",
    )
