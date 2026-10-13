"""A drop-in for the Pinecone SDK's index client that writes to the change buffer instead.

Customers dual-write during the migration by swapping ``pc.Index(...)`` for
``BufferedIndex(buffer)``. Upserts carry the full payload, so the applier never re-reads.
A document index with full-text fields takes them as ``text_fields`` on the vector dict,
keyed by schema field name (ADR-018).
Only upsert and delete are buffered; reads must still go to the source of truth.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pcmigrate.core.buffer import ChangeBuffer, ChangeEnvelope, dual_write_change_id

VectorLike = (
    Mapping[str, Any] | tuple[str, Sequence[float]] | tuple[str, Sequence[float], Mapping[str, Any]]
)


def _to_envelope(vector: VectorLike, namespace: str) -> ChangeEnvelope:
    text_fields: dict[str, str] | None = None
    if isinstance(vector, tuple):
        vector_id, values = vector[0], vector[1]
        metadata = dict(vector[2]) if len(vector) > 2 else {}
    else:
        vector_id = str(vector["id"])
        values = vector["values"]
        metadata = dict(vector.get("metadata") or {})
        raw_text = vector.get("text_fields")
        if isinstance(raw_text, Mapping):
            text_fields = {str(k): str(v) for k, v in raw_text.items()}
    return ChangeEnvelope(
        op="upsert",
        id=str(vector_id),
        namespace=namespace,
        source_change_id=dual_write_change_id(),
        values=[float(v) for v in values],
        metadata=metadata,
        text_fields=text_fields,
        origin="dual_write",
    )


class BufferedIndex:
    """Same upsert/delete signature as ``pinecone.Index``; writes go to the buffer."""

    def __init__(self, buffer: ChangeBuffer, *, default_namespace: str = "") -> None:
        self._buffer = buffer
        self._default = default_namespace

    def upsert(
        self,
        *,
        vectors: Sequence[VectorLike],
        namespace: str = "",
        batch_size: int | None = None,
        show_progress: bool = False,
        **_: Any,
    ) -> dict[str, int]:
        """Buffer upserts. Returns ``{"upserted_count": n}`` like the SDK."""
        target = namespace or self._default
        envelopes = [_to_envelope(v, target) for v in vectors]
        accepted = self._buffer.send(envelopes)
        return {"upserted_count": accepted}

    def delete(
        self,
        *,
        ids: Sequence[str] | None = None,
        namespace: str = "",
        delete_all: bool = False,
        filter: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """Buffer deletes by id. delete_all and filter deletes are not buffered."""
        if delete_all or filter:
            raise NotImplementedError("only deletes by id are buffered during migration")
        target = namespace or self._default
        envelopes = [
            ChangeEnvelope(
                op="delete",
                id=str(i),
                namespace=target,
                source_change_id=dual_write_change_id(),
                origin="dual_write",
            )
            for i in ids or []
        ]
        self._buffer.send(envelopes)
        return {}
