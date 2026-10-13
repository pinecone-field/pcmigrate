"""Build canonical records from pgvector rows through the approved mapping.

The type rules mirror the mapping generator: timestamps to epoch seconds, Decimal to float,
text[] to a string list, JSON flattened at dotted keys or stringified, everything else as
declared. The builder also renders ``id_template`` from the primary key columns.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping as MappingABC
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pcmigrate.config.mapping import MetadataField, MetadataType, ObjectMapping
from pcmigrate.core.canonical import CanonicalRecord, SparseVector
from pcmigrate.core.sparse import parse_sparsevec, tokens_to_sparse

_TEMPLATE_VAR = re.compile(r"\{([^{}]+)\}")
EPOCH_MILLIS_THRESHOLD = 100_000_000_000


class RecordBuildError(ValueError):
    """A row could not be turned into a canonical record."""


def render_id(template: str, values: MappingABC[str, Any]) -> str:
    """Fill ``documents#{id}`` style templates from the row's primary key values."""

    def sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise RecordBuildError(f"id_template references unknown column {name!r}")
        return _plain(values[name])

    return _TEMPLATE_VAR.sub(sub, template)


def _plain(value: Any) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def to_epoch_seconds(value: Any) -> int | None:
    """timestamp, timestamptz, date, ISO-8601 text, or epoch number to integer epoch seconds.

    Naive values are taken as UTC. Numbers at or above ``EPOCH_MILLIS_THRESHOLD`` are epoch
    milliseconds, which is how OpenSearch stores ``epoch_millis`` dates; smaller numbers are
    seconds.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp())
    if isinstance(value, date):
        return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp())
    if isinstance(value, bool):
        raise RecordBuildError("a boolean is not a date")
    if isinstance(value, int | float):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            raise RecordBuildError("NaN and infinity are not dates")
        return int(number / 1000) if abs(number) >= EPOCH_MILLIS_THRESHOLD else int(number)
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", text):
            return to_epoch_seconds(float(text))
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as err:
            raise RecordBuildError(f"{value!r} is not an ISO-8601 date") from err
        return to_epoch_seconds(parsed)
    raise RecordBuildError(f"cannot convert {type(value).__name__} to epoch_seconds")


def convert(value: Any, kind: MetadataType) -> Any:
    """Coerce one source value to the declared metadata type. None stays None."""
    if value is None:
        return None
    if kind is MetadataType.STRING:
        if isinstance(value, dict | list):
            return json.dumps(value, default=_plain, ensure_ascii=False)
        return _plain(value) if isinstance(value, UUID | datetime | date) else str(value)
    if kind is MetadataType.INTEGER:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float | Decimal) and float(value).is_integer():
            return int(value)
        raise RecordBuildError(f"{value!r} is not an integer")
    if kind is MetadataType.FLOAT:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            raise RecordBuildError("NaN and infinity are not valid metadata")
        return number
    if kind is MetadataType.BOOLEAN:
        if isinstance(value, bool):
            return value
        raise RecordBuildError(f"{value!r} is not a boolean")
    if kind is MetadataType.EPOCH_SECONDS:
        return to_epoch_seconds(value)
    if kind is MetadataType.STRING_LIST:
        if isinstance(value, list | tuple):
            return [str(v) for v in value if v is not None]
        raise RecordBuildError(f"{value!r} is not a list")
    raise RecordBuildError(f"unsupported metadata type {kind}")


def dig(value: Any, path: Sequence[str]) -> Any:
    """Walk ``a.b.c`` into nested JSON; None when any step is missing."""
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


@dataclass(frozen=True, slots=True)
class FieldPlan:
    """A mapped field resolved to the column it reads and the JSON path inside it."""

    column: str
    path: tuple[str, ...]
    target: str
    kind: MetadataType
    stringify: bool
    max_chars: int | None = None


@dataclass
class RecordBuilder:
    """Turns rows (as dicts keyed by column) into canonical records for one object."""

    mapping: ObjectMapping
    primary_key: Sequence[str]
    fields: list[FieldPlan] = field(init=False)
    vocabulary: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        plans: list[FieldPlan] = []
        for spec in self.mapping.metadata:
            if spec.action != "map" or spec.type is None:
                continue
            column, *path = spec.source.split(".")
            plans.append(
                FieldPlan(
                    column=column,
                    path=tuple(path),
                    target=spec.target or spec.source,
                    kind=spec.type,
                    stringify=spec.transform == "stringify",
                    max_chars=spec.max_chars,
                )
            )
        self.fields = plans

    @property
    def dense_column(self) -> str:
        """The vector column."""
        if self.mapping.dense is None:
            raise RecordBuildError(f"object {self.mapping.source} has no dense vector")
        return self.mapping.dense.column

    @property
    def sparse_column(self) -> str | None:
        """The stored sparse vector column, when the hybrid wave maps one."""
        return self.mapping.sparse.column if self.mapping.sparse is not None else None

    @property
    def text_columns(self) -> list[str]:
        """Columns whose text the record carries, keyed by source name.

        Full-text fields (ADR-018) and the columns a generated sparse vector is encoded
        from (ADR-017).
        """
        return self.mapping.text_columns

    @property
    def source_columns(self) -> list[str]:
        """Every column the SELECT must include, primary key first, vectors last."""
        seen: dict[str, None] = dict.fromkeys(self.primary_key)
        for plan in self.fields:
            seen.setdefault(plan.column, None)
        if self.mapping.namespace_by:
            seen.setdefault(self.mapping.namespace_by, None)
        seen.pop(self.dense_column, None)
        columns = [*seen, self.dense_column]
        if self.sparse_column and self.sparse_column not in columns:
            columns.append(self.sparse_column)
        for column in self.text_columns:
            base = column.split(".", 1)[0]
            if base not in columns:
                columns.append(base)
        return columns

    def text_fields(self, row: MappingABC[str, Any]) -> dict[str, str]:
        """The text columns of one row as strings, omitting nulls and empty values.

        Keys are the source names from the mapping (a column, or a dotted JSON path into
        one); the emitter renames full-text fields to their schema targets.
        """
        out: dict[str, str] = {}
        limits = {t.source: t.max_chars for t in self.mapping.text if t.max_chars}
        for column in self.text_columns:
            base, *path = column.split(".")
            raw = row.get(base)
            if path:
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except ValueError:
                        raw = None
                raw = dig(raw, path)
            if raw is None:
                continue
            if isinstance(raw, list | tuple):
                text = " ".join(str(v) for v in raw if v is not None)
            elif isinstance(raw, dict):
                text = json.dumps(raw, default=_plain, ensure_ascii=False)
            else:
                text = str(raw)
            cap = limits.get(column)
            if cap is not None and len(text) > cap:
                text = text[:cap]
            if text.strip():
                out[column] = text
        return out

    def sparse(self, row: MappingABC[str, Any]) -> tuple[SparseVector | None, dict[str, int]]:
        """The row's sparse vector through the mapped kind, plus any token-to-index mapping."""
        spec = self.mapping.sparse
        if spec is None or spec.column is None:
            return None, {}
        raw = row.get(spec.column)
        if raw is None:
            return None, {}
        if spec.kind.value == "sparsevec":
            sparse, _ = parse_sparsevec(str(raw))
            return sparse, {}
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict):
            raise RecordBuildError(f"{spec.column} is not a token-weight object")
        return tokens_to_sparse(raw)

    def metadata(self, row: MappingABC[str, Any]) -> dict[str, Any]:
        """Mapped metadata for one row, omitting nulls."""
        out: dict[str, Any] = {}
        for plan in self.fields:
            raw = row.get(plan.column)
            if plan.path:
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except ValueError:
                        raw = None
                raw = dig(raw, plan.path)
            if raw is None:
                continue
            value = (
                json.dumps(raw, default=_plain, ensure_ascii=False)
                if plan.stringify and not isinstance(raw, str)
                else convert(raw, plan.kind)
            )
            if (
                plan.max_chars is not None
                and isinstance(value, str)
                and len(value) > plan.max_chars
            ):
                value = value[: plan.max_chars]
            if value is not None:
                out[plan.target] = value
        return out

    def namespace(self, row: MappingABC[str, Any]) -> str:
        """Static namespace, or the row's ``namespace_by`` value when the mapping sets one."""
        if self.mapping.namespace_by:
            value = row.get(self.mapping.namespace_by)
            if value is None:
                raise RecordBuildError(f"namespace_by column {self.mapping.namespace_by!r} is NULL")
            return f"{self.mapping.namespace}:{_plain(value)}"
        return self.mapping.namespace

    def build_id(self, row: MappingABC[str, Any]) -> str:
        """Only the rendered id, for id-level checks."""
        return render_id(self.mapping.id_template, {k: row[k] for k in self.primary_key})

    def id_sql(self, primary_key: Sequence[str]) -> Any:
        """A SQL expression that renders id_template server-side (text concatenation)."""
        from psycopg import sql

        parts: list[Any] = []
        template = self.mapping.id_template
        pos = 0
        for match in _TEMPLATE_VAR.finditer(template):
            if match.start() > pos:
                parts.append(sql.Literal(template[pos : match.start()]))
            parts.append(sql.SQL("{}::text").format(sql.Identifier(match.group(1))))
            pos = match.end()
        if pos < len(template):
            parts.append(sql.Literal(template[pos:]))
        return sql.SQL("(") + sql.SQL(" || ").join(parts) + sql.SQL(")")

    def build(self, row: MappingABC[str, Any]) -> CanonicalRecord:
        """One canonical record. The vector column must already be a list of floats."""
        dense = row.get(self.dense_column)
        if dense is None and "." in self.dense_column:
            dense = dig(row, self.dense_column.split("."))
        if dense is None:
            raise RecordBuildError(f"{self.dense_column} is NULL")
        if not isinstance(dense, list | tuple) or not all(
            isinstance(v, int | float) and not isinstance(v, bool) for v in dense
        ):
            raise RecordBuildError(f"{self.dense_column} is not a numeric array")
        pk_values = {k: row[k] for k in self.primary_key}
        sparse, vocabulary = self.sparse(row)
        if vocabulary:
            self.vocabulary.update(vocabulary)
        return CanonicalRecord(
            id=render_id(self.mapping.id_template, pk_values),
            namespace=self.namespace(row),
            source_pk=":".join(_plain(v) for v in pk_values.values()),
            dense=[float(v) for v in dense],
            sparse=sparse,
            text_fields=self.text_fields(row),
            metadata=self.metadata(row),
        )


def mapped_fields(mapping: ObjectMapping) -> list[MetadataField]:
    """The metadata fields that land in the target."""
    return [f for f in mapping.metadata if f.action == "map"]
