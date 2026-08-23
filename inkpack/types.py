"""Public contract objects for Inkpack.

This module owns the public API surface that has no I/O: progress events,
long-running operations, result types, errors, profiles and the small
dependency-injection protocols (Clock, CancelToken). It imports nothing
internal and performs no sqlite/zstd work.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Generator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic, Literal, Protocol, TypeVar, cast

T = TypeVar("T")

# Spec 4.1: the full union, including "error". Failures are surfaced as
# exceptions (Operation.result re-raises); "error" remains part of the
# published contract so callers can annotate against it.
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


class _OpIterator(Generic[T]):
    """Single driver over an Operation's inner generator (Workstream B).

    A real iterator object (not a generator wrapping a generator) so a stale
    outer iterator being closed can never clobber a completed operation's
    result (Issue 2). close()/GeneratorExit mark the operation Cancelled ONLY
    when it is not already done.
    """

    def __init__(self, op: Operation[T]) -> None:
        self.op = op
        if op.iterator is None:
            op.iterator = op.iterator_factory()
        self._inner = op.iterator

    def __iter__(self) -> _OpIterator[T]:
        return self

    def __next__(self) -> OpEvent:
        if self.op.done:
            raise StopIteration
        try:
            event = next(self._inner)
        except StopIteration as stop:
            self.op.result_value = cast("T", stop.value)
            self.op.done = True
            raise StopIteration from None
        except BaseException as exc:
            self.op.error = exc
            self.op.done = True
            self._inner.close()
            raise
        return event

    def close(self) -> None:
        if self.op.done:
            return
        self.op.done = True
        self.op.error = Cancelled("operation abandoned before completion")
        self._inner.close()


class Operation(Generic[T]):
    """A lazily-executed long operation (spec 4.2).

    Iterating the operation yields ``OpEvent`` objects and drives the
    operation to completion. After (or during) iteration, ``.result`` returns
    the operation's result value, or re-raises the exception that aborted it.

    The operation is single-use: the underlying generator is created on first
    iteration and consumed until exhaustion; its ``return`` value is captured
    as the operation result. Operations are not thread-safe: drive one
    operation from one thread (review L27).

    Abandonment (locked semantics S1 / Issue 22): ``close()``, ``with op:``
    and ``.result`` are deterministic on every interpreter. A discarded
    half-consumed iterator is best-effort.
    """

    _UNSET = object()

    def __init__(self, iterator_factory: Callable[[], Generator[OpEvent, None, T]]) -> None:
        self.iterator_factory = iterator_factory
        self.iterator: Generator[OpEvent, None, T] | None = None
        self.result_value: T | object = Operation._UNSET
        self.error: BaseException | None = None
        self.done = False

    def __iter__(self) -> _OpIterator[T]:
        return _OpIterator(self)  # exhausted immediately when _done

    def __enter__(self) -> Operation[T]:
        return self

    def __exit__(self, *exc_info: object) -> Literal[False]:
        self.close()
        return False

    def close(self, *, _warn: bool = True) -> None:
        """Abandon the operation early, releasing any resources immediately.

        After closing, ``.result`` raises :class:`Cancelled`. An explicit
        close of a never-started operation warns (Issue 21 / M23); ``__del__``
        suppresses the warning.
        """
        if self.done:
            return
        if self.iterator is None:
            if _warn:
                warnings.warn(
                    "Operation closed before it started (never started); work did not run. "
                    "Use op.result or iterate it.",
                    stacklevel=2,
                )
        else:
            self.iterator.close()
        self.done = True
        self.error = Cancelled("operation closed before completion")

    def __del__(self) -> None:
        with suppress(Exception):
            self.close(_warn=False)

    @property
    def result(self) -> T:
        if not self.done:
            for _ in self:
                pass
        if self.error is not None:
            raise self.error
        if not self.done:
            raise InkpackError("operation was never fully iterated")
        if self.result_value is Operation._UNSET:
            raise InkpackError("operation produced no result")
        return cast("T", self.result_value)


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


@dataclass(frozen=True)
class ChapterInfo:
    """Catalog metadata for one chapter row (never includes the body)."""

    id: int
    novel_id: int
    order_key: str | None
    blob_key: str
    profile: str
    media_type: str | None
    charset: str | None
    created_at: str | None
    updated_at: str | None


class InkpackError(Exception):
    """Base class for all Inkpack errors."""


class NotFound(InkpackError):
    """A referenced entity does not exist."""


class MissingContent(NotFound):
    """Stored content (encoding, payload or dictionary) is missing."""


class Retryable(InkpackError):
    """Transient contention or race condition; safe to retry the call.

    Raised when a concurrent operation changed the repository between two
    reads of the same call (e.g. an encoding vanished between prepare and
    persist, or a shard locator changed repeatedly during a read). Subclasses
    :class:`InkpackError` so existing handlers keep working (spec §11 —
    additions to the minimum set are sanctioned).
    """


class CorruptContent(InkpackError):
    """Stored content failed to decode or failed identity verification."""


class Busy(InkpackError):
    """A writer operation failed because the database was locked/busy."""


class Cancelled(InkpackError):
    """An operation was aborted via its cancel token."""


class UnknownProfile(NotFound, KeyError):
    """A profile name does not exist in the repo config.

    Subclasses both ``NotFound`` (typed error model) and ``KeyError``
    (existing callers that catch ``KeyError`` keep working).
    """

    def __str__(self) -> str:
        # KeyError.__str__ repr-quotes its args; the typed-error message must
        # stay plain (D4).
        return NotFound.__str__(self)


@dataclass(frozen=True)
class Profile:
    """A named write policy (spec 3.4).

    Profiles decide how *new* writes are encoded and what ``reencode()``
    targets. They are never used to decode already-stored content (spec 6.2).

    ``params`` is defensively copied and exposed read-only, so mutating the
    dict used to construct a profile cannot silently change a cached policy.
    """

    name: str
    codec: str
    params: Mapping[str, Any] = field(default_factory=dict[str, Any])
    zstd_dict_id: str | None = None

    def __hash__(self) -> int:
        # The dataclass-generated hash would include ``params`` (an arbitrary
        # Mapping, not hashable). Equal profiles hash equal: the hash uses the
        # hashable subset of the equality fields (D3).
        return hash((self.name, self.codec, self.zstd_dict_id))

    def __post_init__(self) -> None:
        params = self.params
        if not isinstance(cast("Any", params), Mapping):
            raise TypeError(f"profile params must be a mapping, got {type(params).__name__}")
        object.__setattr__(self, "params", MappingProxyType(dict(params)))


class Clock(Protocol):
    def now_iso(self) -> str: ...


CancelToken = Callable[[], bool]


def check_cancel(cancel: CancelToken | None) -> None:
    """Raise :class:`Cancelled` if the optional cancel token requests it."""
    if cancel is not None and cancel():
        raise Cancelled("operation cancelled")


def require_pk(value: Any, label: str) -> int:
    """Validate a primary-key argument (locked decision M16): ``int``, not
    ``bool``, non-negative. Single source for the backend and the Repository
    public wrappers (E2)."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TypeError(f"{label} must be a non-negative int, got {type(value).__name__}")
    return value


# -- profile config serialization -------------------------------------------
# Shared by Repository's profile wrappers, BlobStore and create_repo, so the
# stored `repo_config["profiles"]` shape has exactly one converter pair.
# Parsing is strict: a malformed stored config fails at open/set time instead
# of producing a broken Profile that fails later at encode time.


def profiles_to_config(profiles: dict[str, Profile]) -> dict[str, dict[str, Any]]:
    """Serialize ``Profile`` objects into the ``repo_config["profiles"]`` shape."""
    return {
        name: {
            "codec": p.codec,
            "params": dict(p.params),
            "zstd_dict_id": p.zstd_dict_id,
        }
        for name, p in profiles.items()
    }


def profiles_from_config(raw: Any) -> dict[str, Profile]:
    """Parse the stored ``repo_config["profiles"]`` value into ``Profile`` objects.

    Raises :class:`InkpackError` for malformed entries; semantic validation
    (codec membership, dict-only-with-zstd, key/name match) is done by
    :func:`validate_profiles`.
    """
    if not isinstance(raw, dict):
        # Strict at the top level too (review P1-2): a non-dict stored value is
        # repo corruption, never silently treated as "no profiles".
        raise InkpackError("invalid repo config: profiles must be a dict")
    mapping = cast("dict[str, Any]", raw)
    out: dict[str, Profile] = {}
    for name, cfg in mapping.items():
        if not isinstance(cfg, dict):
            raise InkpackError(f"invalid repo config: profile {name!r} is not a dict")
        cfg = cast("dict[str, Any]", cfg)
        codec = cfg.get("codec")
        if not isinstance(codec, str):
            raise InkpackError(f"invalid repo config: profile {name!r} has no string codec")
        params = cfg.get("params")
        if params is not None and not isinstance(params, dict):
            raise InkpackError(f"invalid repo config: profile {name!r} params must be a dict")
        zstd_dict_id = cfg.get("zstd_dict_id")
        if zstd_dict_id is not None and not isinstance(zstd_dict_id, str):
            raise InkpackError(f"invalid repo config: profile {name!r} zstd_dict_id must be a string or null")
        params_dict = cast("dict[str, Any]", params) if isinstance(params, dict) else {}
        out[name] = Profile(
            name=name,
            codec=codec,
            params=params_dict,
            zstd_dict_id=zstd_dict_id,
        )
    return out


def validate_profile_entry(name: Any, profile: Any) -> Profile:
    """Validate one caller-supplied profile definition.

    Includes codec-specific param constraints (review P2-7): ``zstd`` accepts
    only a ``level`` key, an integer in 1..22, so invalid policies fail at
    set/open time instead of surfacing later at encode time.
    """
    if not isinstance(name, str):
        raise TypeError("profile names must be strings")
    if not isinstance(profile, Profile):
        raise TypeError(f"profile {name!r} must be a Profile instance")
    if profile.name != name:
        raise ValueError(f"profile key {name!r} does not match Profile.name {profile.name!r}")
    if profile.codec not in ("none", "zstd"):
        raise ValueError(f"profile {name!r}: unsupported codec {profile.codec!r}")
    if profile.zstd_dict_id is not None and profile.codec != "zstd":
        raise ValueError(f"profile {name!r}: zstd_dict_id requires codec 'zstd'")
    if profile.codec == "none" and dict(profile.params):
        raise ValueError(f"profile {name!r}: codec 'none' takes no params")
    if profile.codec == "zstd":
        params = dict(profile.params)
        unknown = set(params) - {"level"}
        if unknown:
            raise ValueError(f"profile {name!r}: unknown zstd params {sorted(unknown)} (allowed: level)")
        level = params.get("level", 6)
        if not isinstance(level, int) or isinstance(level, bool) or not 1 <= level <= 22:
            raise ValueError(f"profile {name!r}: zstd level must be an integer in 1..22, got {level!r}")
    return profile


def validate_profiles(profiles: dict[str, Profile] | None) -> dict[str, Profile]:
    """Validate a full profile set; at least one profile is required."""
    if not profiles:
        raise ValueError("at least one profile is required")
    for name, profile in profiles.items():
        validate_profile_entry(name, profile)
    return profiles
