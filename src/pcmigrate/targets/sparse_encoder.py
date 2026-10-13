"""Sparse encoders: text to sparse vectors through a hosted embedding model (ADR-017).

The first provider is Pinecone Inference (``pinecone-sparse-english-v0``). Generation is
client-side on the runner because bulk import does not embed and a document index's
``sparse_vector`` field declares no model. Query-time encoding must use the same model with
``input_type: query``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import pinecone

from pcmigrate.config.mapping import DEFAULT_SPARSE_MODEL, SparseModelSpec
from pcmigrate.core.canonical import SparseVector
from pcmigrate.report import limits

InputType = Literal["passage", "query"]
DEFAULT_MAX_BATCH = 96


class SparseEncodingError(RuntimeError):
    """The model did not return sparse vectors, or returned the wrong number of them."""


@dataclass(frozen=True, slots=True)
class EncoderLimits:
    """What one request may carry."""

    max_batch: int
    max_tokens: int


@dataclass
class EncoderUsage:
    """Tokens and requests consumed so far."""

    texts: int = 0
    tokens: int = 0
    requests: int = 0

    def add(self, *, texts: int, tokens: int, requests: int = 1) -> None:
        """Accumulate one response."""
        self.texts += texts
        self.tokens += tokens
        self.requests += requests


class SparseEncoder(Protocol):
    """Text in, sparse vectors out, one per input and in input order."""

    model: str

    def limits(self) -> EncoderLimits:
        """Batch and sequence limits of the model."""
        ...

    def usage(self) -> EncoderUsage:
        """Consumption so far."""
        ...

    def encode(self, texts: Sequence[str], *, input_type: InputType) -> list[SparseVector]:
        """Encode every text; raises SparseEncodingError on a non-sparse response."""
        ...


def estimate_tokens(text: str) -> int:
    """A rough token count for throttling before the model reports the real one."""
    return len(text) // limits.CHARS_PER_TOKEN + 1


class TokenThrottle:
    """A per-minute token budget shared by the encoder's worker threads."""

    def __init__(
        self,
        tokens_per_minute: int | None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._budget = tokens_per_minute
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._window_start = clock()
        self._used = 0

    def acquire(self, tokens: int) -> None:
        """Block until *tokens* fit in the current minute."""
        if self._budget is None:
            return
        while True:
            with self._lock:
                now = self._clock()
                if now - self._window_start >= 60.0:
                    self._window_start = now
                    self._used = 0
                if self._used + tokens <= self._budget or self._used == 0:
                    self._used += tokens
                    return
                wait = 60.0 - (now - self._window_start)
            self._sleep(max(wait, 0.05))


class PineconeSparseEncoder:
    """Pinecone Inference sparse embeddings through the SDK, batched and throttled."""

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_SPARSE_MODEL,
        *,
        truncate: Literal["END", "NONE"] = "END",
        max_tokens: int | None = None,
        tokens_per_minute: int | None = None,
        workers: int = 4,
        client: Any = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.model = model
        self._pc = (
            client
            if client is not None
            else pinecone.Pinecone(api_key=api_key, source_tag="pcmigrate")
        )
        self._truncate = truncate
        self._max_tokens = max_tokens
        self._workers = max(1, workers)
        self._throttle = TokenThrottle(tokens_per_minute, clock=clock, sleep=sleep)
        self._usage = EncoderUsage()
        self._usage_lock = threading.Lock()
        self._limits: EncoderLimits | None = None

    @classmethod
    def from_spec(
        cls, api_key: str, spec: SparseModelSpec, *, workers: int = 4, **kwargs: Any
    ) -> PineconeSparseEncoder:
        """An encoder for the mapping's model spec."""
        return cls(
            api_key,
            spec.name,
            truncate=spec.truncate,
            max_tokens=spec.max_tokens,
            workers=workers,
            **kwargs,
        )

    def limits(self) -> EncoderLimits:
        """Read the model's batch and sequence limits once, and check it is sparse."""
        if self._limits is None:
            info = self._pc.inference.get_model(model=self.model)
            vector_type = getattr(info, "vector_type", None)
            if vector_type not in (None, "sparse"):
                raise SparseEncodingError(
                    f"{self.model} produces {vector_type} vectors, not sparse"
                )
            max_batch = int(getattr(info, "max_batch_size", None) or DEFAULT_MAX_BATCH)
            max_tokens = int(
                getattr(info, "max_sequence_length", None) or limits.SPARSE_MODEL_DEFAULT_TOKENS
            )
            self._limits = EncoderLimits(max_batch=max_batch, max_tokens=max_tokens)
        return self._limits

    def usage(self) -> EncoderUsage:
        """Consumption so far."""
        return self._usage

    def _parameters(self, input_type: InputType) -> dict[str, Any]:
        parameters: dict[str, Any] = {"input_type": input_type, "truncate": self._truncate}
        if self._max_tokens is not None:
            parameters["max_tokens_per_sequence"] = self._max_tokens
        return parameters

    def _embed(self, texts: Sequence[str], input_type: InputType) -> list[SparseVector]:
        self._throttle.acquire(sum(estimate_tokens(t) for t in texts))
        response = self._pc.inference.embed(
            model=self.model, inputs=list(texts), parameters=self._parameters(input_type)
        )
        if getattr(response, "vector_type", "sparse") != "sparse":
            raise SparseEncodingError(f"{self.model} returned {response.vector_type} vectors")
        data = list(response.data)
        if len(data) != len(texts):
            raise SparseEncodingError(f"{self.model} returned {len(data)} vectors for {len(texts)}")
        usage = getattr(response, "usage", None)
        with self._usage_lock:
            self._usage.add(texts=len(texts), tokens=int(getattr(usage, "total_tokens", 0) or 0))
        return [
            SparseVector(
                [int(i) for i in item.sparse_indices], [float(v) for v in item.sparse_values]
            )
            for item in data
        ]

    def encode(self, texts: Sequence[str], *, input_type: InputType) -> list[SparseVector]:
        """Encode in model-sized batches, several in flight, preserving order."""
        if not texts:
            return []
        size = self.limits().max_batch
        batches = [texts[i : i + size] for i in range(0, len(texts), size)]
        if len(batches) == 1 or self._workers == 1:
            return [v for batch in batches for v in self._embed(batch, input_type)]
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            results = list(pool.map(lambda b: self._embed(b, input_type), batches))
        return [v for batch in results for v in batch]
