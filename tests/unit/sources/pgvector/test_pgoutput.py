import struct

import pytest

from pcmigrate.sources.pgvector.pgoutput import PgoutputDecoder


def cstr(s: str) -> bytes:
    return s.encode() + b"\x00"


def relation(rel_id: int, schema: str, name: str, columns: list[tuple[str, bool]]) -> bytes:
    body = (
        b"R"
        + struct.pack("!i", rel_id)
        + cstr(schema)
        + cstr(name)
        + b"d"
        + struct.pack("!h", len(columns))
    )
    for col, key in columns:
        body += bytes([1 if key else 0]) + cstr(col) + struct.pack("!i", 25) + struct.pack("!i", -1)
    return body


def tuple_data(values: list[str | None]) -> bytes:
    out = struct.pack("!h", len(values))
    for v in values:
        if v is None:
            out += b"n"
        else:
            raw = v.encode()
            out += b"t" + struct.pack("!i", len(raw)) + raw
    return out


def test_decode_insert_update_delete() -> None:
    d = PgoutputDecoder()
    assert (
        d.decode("0/1", relation(7, "public", "documents", [("id", True), ("title", False)]))
        is None
    )
    insert = d.decode("0/2", b"I" + struct.pack("!i", 7) + b"N" + tuple_data(["1", "hello"]))
    assert (
        insert is not None and insert.op == "insert" and insert.new == {"id": "1", "title": "hello"}
    )
    update = d.decode(
        "0/3",
        b"U"
        + struct.pack("!i", 7)
        + b"K"
        + tuple_data(["1", None])
        + b"N"
        + tuple_data(["1", "bye"]),
    )
    assert (
        update is not None
        and update.op == "update"
        and update.key == {"id": "1", "title": None}
        and update.new == {"id": "1", "title": "bye"}
    )
    delete = d.decode("0/4", b"D" + struct.pack("!i", 7) + b"K" + tuple_data(["1", None]))
    assert delete is not None and delete.op == "delete" and delete.key == {"id": "1", "title": None}
    assert delete.relation.key_columns == ("id",)
    assert d.decode("0/5", b"B" + b"\x00" * 20) is None


def test_unknown_relation_and_missing_key_raise() -> None:
    d = PgoutputDecoder()
    with pytest.raises(ValueError, match="unknown relation"):
        d.decode("0/1", b"I" + struct.pack("!i", 99) + b"N" + tuple_data(["1"]))
    d.decode("0/1", relation(1, "public", "t", [("id", True)]))
    with pytest.raises(ValueError, match="replica identity"):
        d.decode("0/2", b"D" + struct.pack("!i", 1))
