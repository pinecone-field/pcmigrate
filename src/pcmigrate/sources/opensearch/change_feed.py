"""OpenSearch change path: dual-write into the buffer, optional updated_at rescans (ADR-009).

OpenSearch has no change stream the tool can subscribe to, so post-snapshot changes arrive
through the customer's dual-writes (``writepath/``) from T0, the instant the export stage
records before the OSIS pipeline starts. When the mapping names an ``updated_at`` field and
sets ``rescan: true``, ``RescanFeed`` polls ``updated_at > watermark`` and forwards key-only
upserts, which the applier re-reads through ``fetch_records``. Deletes are caught by the
final reconcile.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pcmigrate.config.mapping import Mapping
from pcmigrate.core.buffer import ChangeEnvelope

if TYPE_CHECKING:
    from pcmigrate.sources.opensearch.export import OpenSearchSource

DATE_FORMAT = "epoch_millis||strict_date_optional_time"


def record_t0(now: datetime | None = None) -> str:
    """The ISO-8601 instant dual-writes must start from."""
    return (now or datetime.now(UTC)).isoformat(timespec="milliseconds")


@dataclass
class RescanFeed:
    """Poll ``column > watermark`` per mapped index and emit key-only upserts."""

    source: OpenSearchSource
    column: str
    watermarks: dict[str, Any] = field(default_factory=dict)

    @property
    def tier(self) -> str:
        """Always ``dual_write``; the rescan is an add-on to it."""
        return "dual_write"

    def install(self) -> dict[str, Any]:
        """Nothing to create."""
        return {}

    def boundary(self, conn: Any = None) -> dict[str, Any]:
        """T0 for every mapped index."""
        t0 = record_t0()
        return {"t0": t0, "watermarks": {o.source: t0 for o in self.source.mapping.objects}}

    def start(self, boundary: dict[str, Any]) -> None:
        """Begin at the recorded watermarks, or T0 when only that was recorded."""
        marks = dict(boundary.get("watermarks") or {})
        t0 = boundary.get("t0")
        for obj in self.source.mapping.objects:
            marks.setdefault(obj.source, t0 or record_t0())
        self.watermarks = marks

    def poll(self, limit: int = 1000) -> list[ChangeEnvelope]:
        """Documents updated since the watermark, oldest first, advancing it.

        The first poll after T0 asks for ``column > t0``. Later polls resume exactly where
        the previous page ended with ``search_after`` on ``(column, _id)``, so documents that
        share a timestamp across a page boundary are never skipped.
        """
        from pcmigrate.sources.opensearch.export import IdTemplate

        out: list[ChangeEnvelope] = []
        for obj in self.source.mapping.objects:
            template = IdTemplate.parse(obj.id_template)
            mark = self.watermarks.get(obj.source)
            body: dict[str, Any] = {
                "size": limit,
                "_source": False,
                "sort": [{self.column: "asc"}, {"_id": "asc"}],
            }
            if isinstance(mark, list):
                body["query"] = {"range": {self.column: {"gte": mark[0], "format": DATE_FORMAT}}}
                body["search_after"] = mark
            else:
                body["query"] = {"range": {self.column: {"gt": mark, "format": DATE_FORMAT}}}
            hits = self.source.client.search(obj.source, body)["hits"]["hits"]
            for hit in hits:
                doc_id = str(hit["_id"])
                stamp = hit["sort"][0]
                out.append(
                    ChangeEnvelope(
                        op="upsert",
                        id=template.render(doc_id),
                        namespace=obj.namespace,
                        source_change_id=f"rs:{obj.source}:{stamp}:{doc_id}",
                        source_pk=doc_id,
                    )
                )
            if hits:
                self.watermarks[obj.source] = list(hits[-1]["sort"])
        return out

    def uninstall(self) -> None:
        """Nothing to remove."""


def make_feed(source: OpenSearchSource, mapping: Mapping) -> RescanFeed | None:
    """A rescan feed when the mapping asks for one, else None (dual-write only)."""
    spec = mapping.change_capture
    if spec is None or spec.tier != "dual_write" or not spec.rescan or not spec.watermark_column:
        return None
    return RescanFeed(source, spec.watermark_column)
