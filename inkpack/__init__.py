"""Inkpack: local storage library for text novel libraries.

Composition root. Builds the backend, codec engine, identity policy,
BlobStore and Repository, and re-exports the public API.

Factory contract (review §1): ``create_repo`` refuses an existing repository
and requires a validated profile set; ``open_repo`` detects the backend
layout from the filesystem (refusing ambiguous layouts), never creates files,
and validates the stored ``identity_policy`` / ``backend_mode`` / profiles.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .blobstore import BlobStore
from .codec import IKB1, CodecEngine, Identity
from .repo import Repository
from .sqlite import SqliteBackend
from .types import (
    Busy,
    Cancelled,
    ChapterInfo,
    Clock,
    CompactResult,
    ContentRef,
    CorruptContent,
    GcResult,
    InkpackError,
    MissingContent,
    NotFound,
    Operation,
    OpEvent,
    Profile,
    PutResult,
    ReencodeResult,
    TrainDictResult,
    UnknownProfile,
    VerifyResult,
    profiles_from_config,
    profiles_to_config,
    validate_profiles,
)

__version__ = "0.1.0"

DEFAULT_SHARD_CAP_BYTES = 2 << 30
DEFAULT_SHARD_MIN_BYTES = 256 << 20


def _build_repository(
    backend: SqliteBackend,
    *,
    identity: Identity | None,
    codec: CodecEngine | None,
) -> Repository:
    """Compose a Repository from a prepared backend (shared by create/open)."""
    return Repository(
        backend=backend,
        store=BlobStore(backend=backend, codec=codec or CodecEngine(), identity=identity or IKB1),
    )


def create_repo(
    path: str | os.PathLike[str],
    backend_mode: str = "sqlite_single",
    profiles: dict[str, Profile] | None = None,
    pragmas: dict[str, Any] | None = None,
    shard_cap_bytes: int = DEFAULT_SHARD_CAP_BYTES,
    shard_min_bytes: int = DEFAULT_SHARD_MIN_BYTES,
    verify_on_read: bool = False,
    *,
    identity: Identity | None = None,
    codec: CodecEngine | None = None,
    clock: Clock | None = None,
) -> Repository:
    """Create a new repository (spec 4.4) and return a ready-to-use Repository.

    Refuses to run over an existing repository; ``profiles`` is required
    (validated via :func:`validate_profiles`).
    """
    profiles = validate_profiles(profiles)
    identity = identity or IKB1
    root = Path(path)
    if (root / "index.sqlite").exists() or (root / "repo.sqlite").exists():
        raise InkpackError(f"repository already exists at {root}")
    backend = SqliteBackend.create(
        path=path,
        mode=backend_mode,
        pragmas=pragmas,
        shard_cap_bytes=shard_cap_bytes,
        shard_min_bytes=shard_min_bytes,
        clock=clock,
    )
    backend.config_set("identity_policy", identity.name)
    backend.config_set("backend_mode", backend_mode)
    backend.config_set("profiles", profiles_to_config(profiles))
    backend.config_set("verify_on_read", bool(verify_on_read))
    if backend_mode == "sqlite_sharded":
        backend.config_set("shard_cap_bytes", int(shard_cap_bytes))
        backend.config_set("shard_min_bytes", int(shard_min_bytes))
    return _build_repository(backend, identity=identity, codec=codec)


def open_repo(
    path: str | os.PathLike[str],
    pragmas: dict[str, Any] | None = None,
    *,
    identity: Identity | None = None,
    codec: CodecEngine | None = None,
    clock: Clock | None = None,
) -> Repository:
    """Open an existing repository, validating its stored config.

    Layout detection happens here, not in SQL: ``index.sqlite`` + ``payload/``
    implies ``sqlite_sharded``, ``repo.sqlite`` implies ``sqlite_single``,
    both raise (ambiguous), neither raises :class:`NotFound`. Stored
    ``identity_policy``, ``backend_mode`` and profiles are validated
    (decisions I; review §1, §9).
    """
    root = Path(path)
    has_index = (root / "index.sqlite").exists()
    has_single = (root / "repo.sqlite").exists()
    if has_index and has_single:
        raise InkpackError(f"ambiguous repository layout at {root}: both index.sqlite and repo.sqlite present")
    if has_index:
        mode = "sqlite_sharded"
    elif has_single:
        mode = "sqlite_single"
    else:
        raise NotFound(f"no inkpack repository found at {root}")
    backend = SqliteBackend.open(path=path, mode=mode, pragmas=pragmas, clock=clock)

    identity_policy = backend.config_get("identity_policy")
    if identity_policy is None:
        raise InkpackError(f"{root} is not an inkpack repository (repo_config.identity_policy missing)")
    if identity is not None:
        if identity.name != identity_policy:
            raise InkpackError(f"identity_policy mismatch: stored {identity_policy!r}, supplied {identity.name!r}")
    elif identity_policy != "ikb1":
        raise InkpackError(f"unsupported identity_policy: {identity_policy!r} (expected 'ikb1')")

    stored_profiles = profiles_from_config(backend.config_get("profiles"))
    if not stored_profiles:
        raise InkpackError("invalid repo config: profiles missing or empty")
    try:
        validate_profiles(stored_profiles)
    except (TypeError, ValueError) as exc:
        raise InkpackError(f"invalid repo config: {exc}") from exc

    configured_mode = backend.config_get("backend_mode")
    if configured_mode != mode:
        raise InkpackError(f"backend_mode mismatch: config says {configured_mode!r}, layout implies {mode!r}")

    if backend.mode == "sqlite_sharded":
        cap = backend.config_get("shard_cap_bytes")
        minimum = backend.config_get("shard_min_bytes")
        if isinstance(cap, int) and cap > 0:
            backend.shard_cap_bytes = cap
        if isinstance(minimum, int) and minimum > 0:
            backend.shard_min_bytes = minimum

    return _build_repository(backend, identity=identity, codec=codec)


__all__ = [
    "BlobStore",
    "Busy",
    "Cancelled",
    "ChapterInfo",
    "CompactResult",
    "ContentRef",
    "CorruptContent",
    "GcResult",
    "InkpackError",
    "MissingContent",
    "NotFound",
    "OpEvent",
    "Operation",
    "Profile",
    "PutResult",
    "ReencodeResult",
    "Repository",
    "TrainDictResult",
    "UnknownProfile",
    "VerifyResult",
    "__version__",
    "create_repo",
    "open_repo",
]
