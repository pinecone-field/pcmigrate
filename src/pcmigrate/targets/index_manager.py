"""Create or adopt the target index; check dimension, metric, cloud, and namespaces (ADR-010)."""

from __future__ import annotations

from collections.abc import Sequence

from pcmigrate.config.mapping import DenseSpec, TargetSpec
from pcmigrate.core.events import EventBus, IndexPrepared
from pcmigrate.core.state import Stage
from pcmigrate.targets.pinecone_client import IndexInfo, PineconeGateway


class IndexSpecMismatchError(RuntimeError):
    """The existing index does not match the approved target spec."""


class NamespaceOccupiedError(RuntimeError):
    """A target namespace already has data, so bulk import cannot write into it."""


class IndexNotFoundError(RuntimeError):
    """Adopt mode named an index that does not exist."""


class IndexManager:
    """Owns index lifecycle so that IaC never does (ADR-010)."""

    def __init__(self, gateway: PineconeGateway, events: EventBus | None = None) -> None:
        self._gateway = gateway
        self._events = events

    def prepare(self, target: TargetSpec, dense: DenseSpec, namespaces: Sequence[str]) -> IndexInfo:
        """Create or adopt the index named in *target* and verify it is ready for import."""
        if target.type != "vector":
            raise NotImplementedError("document indexes arrive with the hybrid wave (Phase 6)")
        existing = self._gateway.describe_index(target.index)
        if target.mode == "adopt":
            if existing is None:
                raise IndexNotFoundError(f"index {target.index!r} does not exist")
            info = existing
        elif existing is not None:
            info = existing
        else:
            info = self._gateway.create_index(
                target.index,
                dimension=dense.dimension,
                metric=dense.metric.value,
                cloud=target.cloud,
                region=target.region,
                tags={"managed_by": "pcmigrate"},
            )
        self.verify_spec(info, target, dense)
        self.verify_namespaces_absent(info, namespaces)
        if self._events is not None:
            self._events.emit(
                IndexPrepared(
                    stage=Stage.PREPARE_INDEX, index=info.name, mode=target.mode, host=info.host
                )
            )
        return info

    @staticmethod
    def verify_spec(info: IndexInfo, target: TargetSpec, dense: DenseSpec) -> None:
        """Raise unless dimension, metric, cloud, and region match the approved plan."""
        problems: list[str] = []
        if info.dimension != dense.dimension:
            problems.append(f"dimension {info.dimension} != {dense.dimension}")
        if info.metric != dense.metric.value:
            problems.append(f"metric {info.metric!r} != {dense.metric.value!r}")
        if info.cloud != target.cloud:
            problems.append(f"cloud {info.cloud!r} != {target.cloud!r}")
        if info.region != target.region:
            problems.append(f"region {info.region!r} != {target.region!r}")
        if problems:
            raise IndexSpecMismatchError(f"index {info.name!r}: " + "; ".join(problems))

    def verify_namespaces_absent(self, info: IndexInfo, namespaces: Sequence[str]) -> None:
        """Raise if any target namespace already exists (ADR-007: import needs a fresh one)."""
        present = set(self._gateway.list_namespaces(info.host))
        occupied = sorted(present.intersection(namespaces))
        if occupied:
            raise NamespaceOccupiedError(
                f"index {info.name!r} already has namespaces {occupied}; "
                "bulk import cannot write into an existing namespace"
            )
