"""Create or adopt the target index; check dimension, metric, cloud, and namespaces (ADR-010)."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pcmigrate.config.mapping import DenseSpec, FullTextSearchSpec, TargetSpec
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

    @staticmethod
    def document_fields(
        target: TargetSpec,
        dense: DenseSpec,
        *,
        sparse: bool,
        text: Mapping[str, FullTextSearchSpec] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """The document schema the mapping asks for.

        One dense field, one sparse field (ADR-004), and one ``full_text_search`` string
        field per text field (ADR-018).
        """
        fields: dict[str, dict[str, Any]] = {
            target.dense_field: {
                "type": "dense_vector",
                "dimension": dense.dimension,
                "metric": dense.metric.value,
            }
        }
        if sparse:
            fields[target.sparse_field] = {"type": "sparse_vector"}
        for name, spec in (text or {}).items():
            fields[name] = {"type": "string", "full_text_search": spec.to_schema()}
        return fields

    def prepare(
        self,
        target: TargetSpec,
        dense: DenseSpec,
        namespaces: Sequence[str],
        *,
        sparse: bool = False,
        text: Mapping[str, FullTextSearchSpec] | None = None,
    ) -> IndexInfo:
        """Create or adopt the index named in *target* and verify it is ready for import."""
        existing = self._gateway.describe_index(target.index)
        if target.mode == "adopt":
            if existing is None:
                raise IndexNotFoundError(f"index {target.index!r} does not exist")
            info = existing
        elif existing is not None:
            info = existing
        elif target.type == "document":
            info = self._gateway.create_document_index(
                target.index,
                fields=self.document_fields(target, dense, sparse=sparse, text=text),
                cloud=target.cloud,
                region=target.region,
                tags={"managed_by": "pcmigrate"},
                read_capacity=target.read_capacity(),
            )
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
        self.verify_spec(info, target, dense, sparse=sparse, text=text)
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
    def verify_spec(
        info: IndexInfo,
        target: TargetSpec,
        dense: DenseSpec,
        *,
        sparse: bool = False,
        text: Mapping[str, FullTextSearchSpec] | None = None,
    ) -> None:
        """Raise unless type, schema, dimension, metric, cloud, and region match the plan."""
        problems: list[str] = []
        if target.type == "document":
            if info.schema is None:
                problems.append("index is a vector index, mapping says target.type document")
            else:
                if info.schema.dense_field != target.dense_field:
                    problems.append(
                        f"dense field {info.schema.dense_field!r} != {target.dense_field!r}"
                    )
                if sparse and info.schema.sparse_field != target.sparse_field:
                    problems.append(
                        f"sparse field {info.schema.sparse_field!r} != {target.sparse_field!r} "
                        "(a sparse field cannot be added after creation)"
                    )
                for name, spec in (text or {}).items():
                    if name not in info.schema.text_fields:
                        problems.append(
                            f"full-text field {name!r} is missing from the index schema "
                            "(schema fields are fixed at creation)"
                        )
                        continue
                    reported = info.schema.full_text.get(name)
                    if reported:
                        planned = spec.to_schema()
                        differences = [
                            f"{key}: index {reported[key]!r}, mapping {planned[key]!r}"
                            for key in planned
                            if key in reported and reported[key] != planned[key]
                        ]
                        if differences:
                            problems.append(
                                f"full-text field {name!r} differs: " + "; ".join(differences)
                            )
        elif info.schema is not None:
            problems.append("index has a document schema, mapping says target.type vector")
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
