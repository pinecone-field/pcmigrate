"""Start, poll, and describe bulk imports with error_mode=abort (ADR-011, ADR-012)."""

from __future__ import annotations

import time
from collections.abc import Callable

from pcmigrate.core.events import EventBus, ImportProgress, ImportStarted
from pcmigrate.core.state import Stage
from pcmigrate.targets.pinecone_client import ImportInfo, PineconeGateway


class ImportFailedError(RuntimeError):
    """The import ended in Failed or Cancelled."""

    def __init__(self, info: ImportInfo) -> None:
        super().__init__(f"import {info.id} ended {info.status}: {info.error or 'no detail'}")
        self.info = info


class ImportTimeoutError(TimeoutError):
    """The import did not reach a terminal state in time."""


class Importer:
    """Drives one bulk import at a time and reports progress as events."""

    def __init__(
        self,
        gateway: PineconeGateway,
        events: EventBus | None = None,
        *,
        poll_interval_s: float = 15.0,
        timeout_s: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._gateway = gateway
        self._events = events
        self._poll = poll_interval_s
        self._timeout = timeout_s
        self._sleep = sleep
        self._clock = clock

    def start(self, host: str, uri: str, *, integration_id: str | None = None) -> str:
        """Start an import with error_mode=abort and return its id."""
        import_id = self._gateway.start_import(host, uri, integration_id=integration_id)
        if self._events is not None:
            self._events.emit(ImportStarted(stage=Stage.IMPORT, import_id=import_id, uri=uri))
        return import_id

    def wait(self, host: str, import_id: str) -> ImportInfo:
        """Poll until the import is terminal. Raises on Failed or Cancelled."""
        deadline = None if self._timeout is None else self._clock() + self._timeout
        last: tuple[str, float | None, int | None] | None = None
        while True:
            info = self._gateway.describe_import(host, import_id)
            snapshot = (info.status, info.percent_complete, info.records_imported)
            if snapshot != last and self._events is not None:
                self._events.emit(
                    ImportProgress(
                        stage=Stage.IMPORT,
                        import_id=info.id,
                        status=info.status,
                        percent_complete=info.percent_complete,
                        records_imported=info.records_imported,
                    )
                )
            last = snapshot
            if info.terminal:
                if info.status != "Completed":
                    raise ImportFailedError(info)
                return info
            if deadline is not None and self._clock() >= deadline:
                raise ImportTimeoutError(f"import {import_id} still {info.status}")
            self._sleep(self._poll)

    def run(self, host: str, uri: str, *, integration_id: str | None = None) -> ImportInfo:
        """start() then wait()."""
        return self.wait(host, self.start(host, uri, integration_id=integration_id))
