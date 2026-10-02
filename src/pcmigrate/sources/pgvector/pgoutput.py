"""A minimal decoder for the pgoutput logical replication protocol (version 1).

Decodes Relation, Insert, Update, Delete, Begin, and Commit messages as returned by
``pg_logical_slot_get_binary_changes(... 'proto_version', '1', 'publication_names', ...)``.
Tuple data arrives in text format; column values are returned as strings (or None).
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class RelationColumn:
    """One column of a published relation."""

    name: str
    type_oid: int
    is_key: bool


@dataclass(frozen=True, slots=True)
class Relation:
    """A published table, as pgoutput describes it."""

    relation_id: int
    namespace: str
    name: str
    replica_identity: str
    columns: tuple[RelationColumn, ...]

    @property
    def key_columns(self) -> tuple[str, ...]:
        """The replica identity (primary key) columns."""
        return tuple(c.name for c in self.columns if c.is_key)


@dataclass(frozen=True, slots=True)
class RowChange:
    """One decoded insert, update, or delete."""

    op: str
    relation: Relation
    new: dict[str, str | None] | None
    key: dict[str, str | None] | None
    lsn: str


@dataclass
class PgoutputDecoder:
    """Stateful: relations arrive before the rows that reference them."""

    relations: dict[int, Relation] = field(default_factory=dict)

    def decode(self, lsn: str, data: bytes) -> RowChange | None:
        """Decode one message. Returns a RowChange for I/U/D, None for everything else."""
        kind = chr(data[0])
        if kind == "R":
            self._relation(data)
            return None
        if kind in ("I", "U", "D"):
            return self._row(kind, lsn, data)
        return None

    def _relation(self, data: bytes) -> None:
        offset = 1
        relation_id = struct.unpack_from("!i", data, offset)[0]
        offset += 4
        namespace, offset = _cstring(data, offset)
        name, offset = _cstring(data, offset)
        replica_identity = chr(data[offset])
        offset += 1
        count = struct.unpack_from("!h", data, offset)[0]
        offset += 2
        columns: list[RelationColumn] = []
        for _ in range(count):
            flags = data[offset]
            offset += 1
            col_name, offset = _cstring(data, offset)
            type_oid = struct.unpack_from("!i", data, offset)[0]
            offset += 4
            offset += 4
            columns.append(RelationColumn(col_name, type_oid, bool(flags & 1)))
        self.relations[relation_id] = Relation(
            relation_id, namespace, name, replica_identity, tuple(columns)
        )

    def _row(self, kind: str, lsn: str, data: bytes) -> RowChange:
        offset = 1
        relation_id = struct.unpack_from("!i", data, offset)[0]
        offset += 4
        relation = self.relations.get(relation_id)
        if relation is None:
            raise ValueError(f"row message for unknown relation {relation_id}")
        new: dict[str, str | None] | None = None
        key: dict[str, str | None] | None = None
        while offset < len(data):
            tag = chr(data[offset])
            offset += 1
            if tag in ("K", "O"):
                values, offset = _tuple(data, offset)
                key = dict(zip((c.name for c in relation.columns), values, strict=False))
            elif tag == "N":
                values, offset = _tuple(data, offset)
                new = dict(zip((c.name for c in relation.columns), values, strict=False))
            else:
                raise ValueError(f"unexpected tuple tag {tag!r} in {kind} message")
        op = {"I": "insert", "U": "update", "D": "delete"}[kind]
        if op == "delete" and key is None:
            raise ValueError(
                "delete without replica identity; set REPLICA IDENTITY DEFAULT or FULL"
            )
        return RowChange(op=op, relation=relation, new=new, key=key, lsn=lsn)


def _cstring(data: bytes, offset: int) -> tuple[str, int]:
    end = data.index(b"\x00", offset)
    return data[offset:end].decode("utf-8"), end + 1


def _tuple(data: bytes, offset: int) -> tuple[list[str | None], int]:
    count = struct.unpack_from("!h", data, offset)[0]
    offset += 2
    values: list[str | None] = []
    for _ in range(count):
        tag = chr(data[offset])
        offset += 1
        if tag == "n" or tag == "u":
            values.append(None)
        elif tag in ("t", "b"):
            length = struct.unpack_from("!i", data, offset)[0]
            offset += 4
            raw = data[offset : offset + length]
            offset += length
            values.append(raw.decode("utf-8") if tag == "t" else raw.hex())
        else:
            raise ValueError(f"unknown column tag {tag!r}")
    return values, offset
