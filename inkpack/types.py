"""Public contract objects for Inkpack.

This module owns the public API surface that has no I/O: progress events,
long-running operations, result types, errors, profiles and the small
dependency-injection protocols (Clock, CancelToken). It imports nothing
internal and performs no sqlite/zstd work.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator, Mapping
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


class Operation(Generic[T]):
    """A lazily-executed long operation (spec 4.2).

    Iterating the operation yields ``OpEvent`` objects and drives the
    operation to completion. After (or during) iteration, ``.result`` returns
    the operation's result value, or re-raises the exception that aborted it.

    The operation is single-use: the underlying generator is created on first
    iteration and consumed until exhaustion. The generator's ``return`` value
    is captured as the operation result.

    Abandonment is deterministic (locked semantics S1): ``close()`` (or the
    ``with op:`` context manager, or discarding the iterator) prevents an
    unstarted operation from running, releases resources immediately, and
    makes ``.result`` raise :class:`Cancelled`.
    """

    _UNSET = object()

    def __init__(self, iterator_factory: Callable[[], Generator[OpEvent, None, T]]) -> None:
        self._iterator_factory = iterator_factory
        self._iterator: Generator[OpEvent, None, T] | None = None
        self._result: T | object = Operation._UNSET
        self._error: BaseException | None = None
        self._done = False

    def __iter__(self) -> Iterator[OpEvent]:
        if self._done:
            # Closed before starting (P0-OP-1): never start the generator.
            return
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
                    # clobber the captured result.
                    if not self._done:
                        self._result = cast("T", stop.value)
                    self._done = True
                    return
                yield event
        except GeneratorExit:
            # The consumer abandoned iteration (P0-OP-2): mark the operation
            # as cancelled deterministically — .result must not depend on GC
            # or resume a half-run generator.
            self._done = True
            self._error = Cancelled("operation abandoned before completion")
            iterator.close()
            raise
        except BaseException as exc:
            self._error = exc
            self._done = True
            raise

    def __enter__(self) -> Operation[T]:
        return self

    def __exit__(self, *exc_info: object) -> Literal[False]:
        self.close()
        return False

    def close(self) -> None:
        """Abandon the operation early, releasing any resources (e.g. the
        operation-scoped database session) immediately.

        Safe to call multiple times and on never-started operations. After
        closing, ``.result`` raises :class:`Cancelled`.
        """
        if self._done:
            return
        if self._iterator is not None:
            self._iterator.close()
        self._done = True
        self._error = Cancelled("operation closed before completion")

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    @property
    def result(self) -> T:
        if not self._done:
            for _ in self:
                pass
        if self._error is not None:
            raise self._error
        assert self._done, "operation was never fully iterated"
        if self._result is Operation._UNSET:
            # Never return None as a stand-in for a missing result (S1).
            raise InkpackError("operation produced no result")
        return cast("T", self._result)


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
