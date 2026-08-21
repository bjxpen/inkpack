"""Inkpack: local storage library for text novel libraries.

Composition root. Builds the backend, codec engine, identity policy,
BlobStore and Repository, and re-exports the public API.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

from .blobstore import BlobStore
from .codec import IKB1, CodecEngine, Identity
from .repo import Repository
from .sqlite import SqliteBackend
from .types import (
    Busy,
    Cancelled,
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
    VerifyResult,
    profiles_to_config,
    validate_profiles,
)

__version__ = "0.1.0"

DEFAULT_SHARD_CAP_BYTES = 2 << 30
DEFAULT_SHARD_MIN_BYTES = 256 << 20


def _validate_stored_profiles(profiles: Any) -> dict[str, Any]:
    mapping = cast("dict[str, Any] | None", profiles)
    if not mapping:
        raise InkpackError("invalid repo config: profiles missing or empty")
    out: dict[str, Any] = {}
    for name, cfg_raw in mapping.items():
        if not isinstance(cfg_raw, dict):
            raise InkpackError("invalid repo config: malformed profile entry")
        cfg = cast("dict[str, Any]", cfg_raw)
        if cfg.get("codec") not in ("none", "zstd"):
            raise InkpackError("invalid repo config: malformed profile entry")
        out[name] = cfg
    return out


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
    """Create a new repository (spec 4.4) and return a ready-to-use Repository."""
    profiles = validate_profiles(profiles)
    identity = identity or IKB1
    backend = SqliteBackend.open(
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
    """Open an existing repository, validating its stored config (decisions I, backend-mode check)."""
    root = Path(path)
    if not (root / "index.sqlite").exists() and not (root / "repo.sqlite").exists():
        raise InkpackError(f"no inkpack repository found at {root}")
    mode = "sqlite_sharded" if (root / "index.sqlite").exists() else "sqlite_single"
    backend = SqliteBackend.open(path=path, mode=mode, pragmas=pragmas, clock=clock)

    identity_policy = backend.config_get("identity_policy")
    if identity_policy is None:
        raise InkpackError(f"{root} is not an inkpack repository (repo_config.identity_policy missing)")
    if identity is not None:
        if identity.name != identity_policy:
            raise InkpackError(f"identity_policy mismatch: stored {identity_policy!r}, supplied {identity.name!r}")
    elif identity_policy != "ikb1":
        raise InkpackError(f"unsupported identity_policy: {identity_policy!r} (expected 'ikb1')")

    _validate_stored_profiles(backend.config_get("profiles"))

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
    "VerifyResult",
    "__version__",
    "create_repo",
    "open_repo",
]
