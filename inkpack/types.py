from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Generic, Iterator, Literal, Optional, Protocol, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class ContentRef:
    blob_key: str
    profile: str


@dataclass
class OpEvent:
    kind: Literal["start", "phase", "progress", "item", "log", "error", "done"]
    op: str
    phase: str | None = None
    message: str | None = None
    metrics: dict | None = None


class Operation(Generic[T]):
    def __init__(self, iterator_factory: Callable[[], Iterator[OpEvent]]):
        self._iterator_factory = iterator_factory
        self._iter: Optional[Iterator[OpEvent]] = None
        self._done = False
        self._result: Optional[T] = None
        self._error: Optional[BaseException] = None

    def __iter__(self) -> Iterator[OpEvent]:
        if self._iter is None:
            self._iter = self._iterator_factory()
        try:
            for event in self._iter:
                yield event
        except BaseException as exc:
            self._error = exc
            self._done = True
            raise
        else:
            self._done = True

    @property
    def result(self) -> T:
        if not self._done:
            for _ in self:
                pass
        if self._error is not None:
            raise self._error
        return self._result  # type: ignore[return-value]

    def _set_result(self, result: T) -> None:
        self._result = result


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
    pass


class NotFound(InkpackError):
    pass


class MissingContent(NotFound):
    pass


class CorruptContent(InkpackError):
    pass


class Busy(InkpackError):
    pass


class Cancelled(InkpackError):
    pass


@dataclass(frozen=True)
class Profile:
    name: str
    codec: str
    params: dict = field(default_factory=dict)
    zstd_dict_id: str | None = None


class Clock(Protocol):
    def now_iso(self) -> str: ...


CancelToken = Callable[[], bool]
