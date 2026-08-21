"""Public contract objects for Inkpack.

This module owns the public API surface that has no I/O: progress events,
long-running operations, result types, errors, profiles and the small
dependency-injection protocols (Clock, CancelToken). It imports nothing
internal and performs no sqlite/zstd work.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol, TypeVar

T = TypeVar("T")

OpKind = Literal["start", "phase", "progress", "item", "log", "error", "done"]


@dataclass(frozen=True)
class ContentRef:
    """Reference to stored content: the canonical blob plus an encoding profile."""

    blob_key: str
    profile: str


@dataclass
class OpEvent:
    """Progress event yielded by long-running operations (spec 4.1)."""

    kind: OpKind
    op: str
    phase: str | None = None
    message: str | None = None
    metrics: dict[str, Any] | None = None


class Operation(Generic[T]):
    """A lazily-executed long operation (spec 4.2).

    Iterating the operation yields ``OpEvent`` objects and drives the
    operation to completion. After (or during) iteration, ``.result`` returns
    the operation's result value, or re-raises the exception that aborted it.

    The operation is single-use: the underlying generator is created on first
    iteration and consumed until exhaustion. The generator's ``return`` value
    is captured as the operation result.
    """

    def __init__(self, iterator_factory: Callable[[], Iterator[OpEvent]]) -> None:
        self._iterator_factory = iterator_factory
        self._iterator: Iterator[OpEvent] | None = None
        self._result: T | None = None
        self._error: BaseException | None = None
        self._done = False

    def __iter__(self) -> Iterator[OpEvent]:
        if self._iterator is None:
            self._iterator = self._iterator_factory()
        iterator = self._iterator
        try:
            while True:
                try:
                    event = next(iterator)
                except StopIteration as stop:
                    # A generator's `return value` arrives via StopIteration.value.
                    # Re-iterating an already-finished operation must not
                    # clobber the captured result (StopIteration.value is None
                    # on an exhausted generator).
                    if not self._done:
                        self._result = stop.value
                    self._done = True
                    return
                yield event
        except GeneratorExit:
            raise
        except BaseException as exc:
            self._error = exc
            self._done = True
            raise

    @property
    def result(self) -> T:
        if not self._done:
            for _ in self:
                pass
        if self._error is not None:
            raise self._error
        assert self._done, "operation was never fully iterated"
        return self._result  # type: ignore[return-value]


@dataclass(frozen=True)
class PutResult:
    ref: ContentRef
    raw_len: int
    stored_len: int
    codec: str
    zstd_dict_id: str | None


@dataclass(frozen=True)
class TrainDictResult:
    dict_id: str
    dict_size: int
    samples_used: int
    sample_bytes: int


@dataclass(frozen=True)
class VerifyResult:
    checked: int
    ok: int
    missing: int
    corrupt: int


@dataclass(frozen=True)
class GcResult:
    encodings_deleted: int
    payload_rows_deleted: int
    blobs_deleted: int
    dicts_deleted: int


@dataclass(frozen=True)
class CompactResult:
    mode: str
    targets: list[str]


@dataclass(frozen=True)
class ReencodeResult:
    targets: int
    reencoded: int
    skipped: int
    bytes_in: int
    bytes_out: int


class InkpackError(Exception):
    """Base class for all Inkpack errors."""


class NotFound(InkpackError):
    """A referenced entity does not exist."""


class MissingContent(NotFound):
    """Stored content (encoding, payload or dictionary) is missing."""


class CorruptContent(InkpackError):
    """Stored content failed to decode or failed identity verification."""


class Busy(InkpackError):
    """A writer operation failed because the database was locked/busy."""


class Cancelled(InkpackError):
    """An operation was aborted via its cancel token."""


@dataclass(frozen=True)
class Profile:
    """A named write policy (spec 3.4).

    Profiles decide how *new* writes are encoded and what ``reencode()``
    targets. They are never used to decode already-stored content (spec 6.2).
    """

    name: str
    codec: str
    params: dict[str, Any] = field(default_factory=dict[str, Any])
    zstd_dict_id: str | None = None


class Clock(Protocol):
    def now_iso(self) -> str: ...


CancelToken = Callable[[], bool]


def check_cancel(cancel: CancelToken | None) -> None:
    """Raise :class:`Cancelled` if the optional cancel token requests it."""
    if cancel is not None and cancel():
        raise Cancelled("operation cancelled")
