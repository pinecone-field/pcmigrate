"""A small FastAPI endpoint mirroring the Pinecone data plane's upsert and delete routes.

For customers who write from languages without the Python shim. Deploy it next to the
runner; it authenticates with a shared token and forwards every write into the buffer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from pcmigrate.core.buffer import ChangeBuffer, ChangeEnvelope, dual_write_change_id


class UpsertVector(BaseModel):
    """One vector in an upsert request."""

    id: str
    values: list[float]
    metadata: dict[str, Any] = Field(default_factory=dict)
    text_fields: dict[str, str] | None = None


class UpsertRequest(BaseModel):
    """Body of POST /vectors/upsert."""

    vectors: list[UpsertVector]
    namespace: str = ""


class DeleteRequest(BaseModel):
    """Body of POST /vectors/delete."""

    ids: list[str]
    namespace: str = ""


def create_app(buffer: ChangeBuffer, *, token: str) -> FastAPI:
    """Build the app around one buffer and one bearer token."""
    app = FastAPI(title="pcmigrate dual-write endpoint", version="1")

    def authorize(
        authorization: str | None = Header(default=None),
        api_key: str | None = Header(default=None, alias="Api-Key"),
    ) -> None:
        presented = api_key or (
            authorization.removeprefix("Bearer ").strip() if authorization else None
        )
        if presented != token:
            raise HTTPException(status_code=401, detail="invalid token")

    @app.post("/vectors/upsert", dependencies=[Depends(authorize)])
    def upsert(body: UpsertRequest) -> dict[str, int]:
        envelopes: Sequence[ChangeEnvelope] = [
            ChangeEnvelope(
                op="upsert",
                id=v.id,
                namespace=body.namespace,
                source_change_id=dual_write_change_id(),
                values=v.values,
                metadata=v.metadata,
                text_fields=v.text_fields,
                origin="dual_write",
            )
            for v in body.vectors
        ]
        return {"upsertedCount": buffer.send(envelopes)}

    @app.post("/vectors/delete", dependencies=[Depends(authorize)])
    def delete(body: DeleteRequest) -> dict[str, Any]:
        envelopes = [
            ChangeEnvelope(
                op="delete",
                id=i,
                namespace=body.namespace,
                source_change_id=dual_write_change_id(),
                origin="dual_write",
            )
            for i in body.ids
        ]
        buffer.send(envelopes)
        return {}

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {"ok": True, "depth": buffer.approximate_depth()}

    return app
