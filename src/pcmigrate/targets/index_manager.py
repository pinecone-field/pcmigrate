"""Create or adopt the target index; check dimension, metric, cloud, and namespaces (ADR-010)."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence

from pcmigrate.config.mapping import DenseSpec, TargetSpec
from pcmigrate.core.events import EventBus, IndexPrepared, LogMessage
from pcmigrate.core.state import Stage
from pcmigrate.targets.pinecone_client import IndexInfo, PineconeGateway

READ_CAPACITY_TIMEOUT_S = 45 * 60


class IndexSpecMismatchError(RuntimeError):
    """The existing index does not match the approved target spec."""


class NamespaceOccupiedError(RuntimeError):
    """A target namespace already has data, so bulk import cannot write into it."""


class IndexNotFoundError(RuntimeError):
    """Adopt mode named an index that does not exist."""


class ReadCapacityError(RuntimeError):
    """Dedicated read nodes did not become ready, or errored."""


class IndexManager:
    """Owns index lifecycle so that IaC never does (ADR-010)."""

    def __init__(
        self,
        gateway: PineconeGateway,
        events: EventBus | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        poll_interval_s: float = 30.0,
        read_capacity_timeout_s: float = READ_CAPACITY_TIMEOUT_S,
    ) -> None:
        self._gateway = gateway
        self._events = events
        self._sleep = sleep
        self._clock = clock
        self._poll = poll_interval_s
        self._timeout = read_capacity_timeout_s

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
                read_capacity=target.read_capacity(),
            )
        self.verify_spec(info, target, dense)
        info = self.wait_for_read_capacity(info, target)
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
        wanted = target.read_capacity()
        capacity = info.read_capacity
        if capacity is not None and not capacity.matches(wanted):
            have = (
                f"dedicated {capacity.node_type} x {capacity.shards} shard(s) x "
                f"{capacity.replicas} replica(s)"
                if capacity.dedicated
                else "on-demand"
            )
            want = (
                f"dedicated {target.read_nodes.node_type} x {target.read_nodes.shards} "
                f"shard(s) x {target.read_nodes.replicas} replica(s)"
                if target.read_nodes is not None
                else "on-demand"
            )
            problems.append(
                f"read capacity is {have}, mapping says {want} (pcmigrate never reconfigures "
                "an existing index; change target.read_nodes or the index)"
            )
        elif capacity is None and wanted is not None:
            problems.append(
                "the control plane reported no read capacity but the mapping asks for "
                "dedicated read nodes"
            )
        if problems:
            raise IndexSpecMismatchError(f"index {info.name!r}: " + "; ".join(problems))

    def wait_for_read_capacity(self, info: IndexInfo, target: TargetSpec) -> IndexInfo:
        """Block until dedicated read nodes report Ready, so later queries hit them."""
        if target.read_nodes is None:
            return info
        deadline = self._clock() + self._timeout
        current = info
        while True:
            capacity = current.read_capacity
            if capacity is not None and capacity.state == "Error":
                raise ReadCapacityError(
                    f"index {current.name!r} read capacity errored: {capacity.error}"
                )
            if current.ready and (capacity is None or capacity.settled):
                return current
            if self._clock() >= deadline:
                raise ReadCapacityError(
                    f"index {current.name!r} read capacity still "
                    f"{capacity.state if capacity else 'unknown'} after {self._timeout:.0f}s"
                )
            if self._events is not None:
                self._events.emit(
                    LogMessage(
                        stage=Stage.PREPARE_INDEX,
                        message=f"waiting for dedicated read nodes on {current.name}: "
                        f"{capacity.state if capacity else 'unknown'}",
                    )
                )
            self._sleep(self._poll)
            refreshed = self._gateway.describe_index(current.name)
            if refreshed is None:
                raise IndexNotFoundError(f"index {current.name!r} disappeared while waiting")
            current = refreshed

    def verify_namespaces_absent(self, info: IndexInfo, namespaces: Sequence[str]) -> None:
        """Raise if any target namespace already exists (ADR-007: import needs a fresh one)."""
        present = set(self._gateway.list_namespaces(info.host))
        occupied = sorted(present.intersection(namespaces))
        if occupied:
            raise NamespaceOccupiedError(
                f"index {info.name!r} already has namespaces {occupied}; "
                "bulk import cannot write into an existing namespace"
            )
