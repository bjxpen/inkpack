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
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, cast

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


def _sweep_stale_creating_dirs(parent: Path) -> None:
    """Remove stale ``.inkpack-creating-*`` temp dirs (Issue 46).

    Only in ``create_repo`` (open has no filesystem side effects). Only
    siblings of the destination with the exact name pattern and mtime older
    than 1 hour are removed, so a concurrent create (different UUID) is
    never touched. The grace period is the mitigation, not a lock.
    """
    if not parent.is_dir():
        return
    cutoff = time.time() - 3600
    for child in parent.iterdir():
        name = child.name
        if not (name.startswith(".inkpack-creating-") and len(name) == len(".inkpack-creating-") + 32):
            continue
        try:
            if child.stat().st_mtime < cutoff:
                shutil.rmtree(child, ignore_errors=True)
        except OSError:
            continue


def _repo_markers_exist(root: Path) -> bool:
    """A path is already a repository if index/repo markers exist OR the
    payload dir holds any shard file (review H5)."""
    if (root / "index.sqlite").exists() or (root / "repo.sqlite").exists():
        return True
    payload = root / "payload"
    return payload.is_dir() and any(payload.glob("shard-*.sqlite"))


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

    All-or-nothing (locked decision D5/H3): the repository is built in a
    sibling temp directory (with the initial config committed in the same
    transaction as the migration) and atomically renamed into place; any
    failure leaves no ``repo.sqlite`` / ``index.sqlite`` / ``payload/`` and a
    later create/open behaves as on a fresh path. ``~/...`` is expanded
    (D11/M24). The exists-check (D11/H5) runs BEFORE profile validation.

    ``shard_min_bytes`` is the allowed floor for ``shard_cap_bytes`` (must be
    a positive int, ``min <= cap``); routing uses only ``shard_cap_bytes``
    (locked decision D4).
    """
    root = Path(path).expanduser().resolve()
    if _repo_markers_exist(root):
        raise InkpackError(f"repository already exists at {root}")
    if root.exists() and not root.is_dir():
        raise InkpackError(f"cannot create repository at {root}: path exists and is not a directory")
    if root.exists() and any(root.iterdir()):
        raise InkpackError(f"cannot create repository at {root}: directory exists and is not empty")
    _sweep_stale_creating_dirs(root.parent)  # Issue 46: only in create_repo
    profiles = validate_profiles(profiles)  # after the exists-check (D11/M24)
    for name, profile in profiles.items():
        if profile.zstd_dict_id is not None:
            # Issue 12: a dictionary cannot exist before the repo does;
            # attach dicts later via set_profile.
            raise ValueError(
                f"profile {name!r}: zstd_dict_id must be None when creating a repository "
                "(train the dictionary and attach it with set_profile)"
            )
    identity = identity or IKB1
    if not isinstance(cast("Any", verify_on_read), bool):
        raise TypeError(f"verify_on_read must be a bool, got {type(verify_on_read).__name__}")
    initial_config: dict[str, Any] = {
        "identity_policy": identity.name,
        "backend_mode": backend_mode,
        "profiles": profiles_to_config(profiles),
        "verify_on_read": verify_on_read,
    }
    if backend_mode == "sqlite_sharded":
        initial_config["shard_cap_bytes"] = int(shard_cap_bytes)
        initial_config["shard_min_bytes"] = int(shard_min_bytes)
    tmp = root.parent / f".inkpack-creating-{uuid.uuid4().hex}"
    try:
        # Create-phase failures propagate raw (D5/H3): the temp dir is removed
        # and the destination is untouched, so a retry behaves as fresh.
        backend = SqliteBackend.create(
            path=tmp,
            mode=backend_mode,
            pragmas=pragmas,
            shard_cap_bytes=shard_cap_bytes,
            shard_min_bytes=shard_min_bytes,
            clock=clock,
            initial_config=initial_config,
        )
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    try:
        if root.exists():
            root.rmdir()  # empty only (Issue 13: os.replace onto a dir is not portable)
        os.replace(tmp, root)  # atomic; root absent or an empty dir
        backend.root = root
        backend.index_path = root / ("index.sqlite" if backend_mode == "sqlite_sharded" else "repo.sqlite")
        if backend.payload_dir is not None:
            backend.payload_dir = root / "payload"
    except OSError as exc:
        shutil.rmtree(tmp, ignore_errors=True)
        raise InkpackError(f"cannot create repository at {root}: {exc}") from exc
    return _build_repository(backend, identity=identity, codec=codec)


def open_repo(
    path: str | os.PathLike[str],
    pragmas: dict[str, Any] | None = None,
    *,
    identity: Identity | None = None,
    codec: CodecEngine | None = None,
    clock: Clock | None = None,
) -> Repository:
    """Open an existing repository, validating its stored config (D8/M12).

    Layout detection happens here, not in SQL: ``index.sqlite`` + ``payload/``
    implies ``sqlite_sharded``, ``repo.sqlite`` implies ``sqlite_single``,
    both raise (ambiguous), neither raises :class:`NotFound`. A broken layout
    (``index.sqlite`` without ``payload/``) is ``InkpackError``, never
    ``NotFound`` (M13). ``~/...`` is expanded (D11/M24). Every config value is
    validated strictly at open time; shard caps are restored from config.
    """
    root = Path(path).expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise InkpackError(f"cannot open repository at {root}: path exists and is not a directory")
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
    backend = SqliteBackend.open(path=root, mode=mode, pragmas=pragmas, clock=clock)
    _load_repo_config(backend, root, mode, identity)
    return _build_repository(backend, identity=identity or IKB1, codec=codec)


def _load_repo_config(
    backend: SqliteBackend, root: Path, mode: str, identity: Identity | None
) -> dict[str, Any]:
    """Validate the stored repo config strictly at open time (locked decision
    D8 / review M12)."""
    identity_policy = backend.config_get("identity_policy")
    if identity_policy is None:
        raise InkpackError(f"{root} is not an inkpack repository (repo_config.identity_policy missing)")
    if identity is not None:
        if identity.name != identity_policy:
            raise InkpackError(f"identity_policy mismatch: stored {identity_policy!r}, supplied {identity.name!r}")
    elif identity_policy != "ikb1":
        raise InkpackError(f"unsupported identity_policy: {identity_policy!r} (expected 'ikb1')")

    profiles_raw = backend.config_get("profiles")
    if profiles_raw is None:
        raise InkpackError(f"invalid repo config: profiles missing at {root}")
    stored_profiles = profiles_from_config(profiles_raw)
    if not stored_profiles:
        raise InkpackError("invalid repo config: profiles missing or empty")
    try:
        validate_profiles(stored_profiles)
    except (TypeError, ValueError) as exc:
        raise InkpackError(f"invalid repo config: {exc}") from exc

    configured_mode = backend.config_get("backend_mode")
    if configured_mode != mode:
        raise InkpackError(f"backend_mode mismatch: config says {configured_mode!r}, layout implies {mode!r}")

    verify_on_read = backend.config_get("verify_on_read")
    if verify_on_read is None:
        verify_on_read = False
    if not isinstance(verify_on_read, bool):
        raise InkpackError(
            f"invalid repo config: verify_on_read must be a bool, got {verify_on_read!r}"
        )

    cap = backend.config_get("shard_cap_bytes")
    minimum = backend.config_get("shard_min_bytes")
    if backend.mode == "sqlite_sharded":
        if cap is not None:
            if isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0:
                raise InkpackError(f"invalid repo config: shard_cap_bytes must be a positive int, got {cap!r}")
            backend.shard_cap_bytes = cap
        if minimum is not None:
            if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum <= 0:
                raise InkpackError(f"invalid repo config: shard_min_bytes must be a positive int, got {minimum!r}")
            backend.shard_min_bytes = minimum
        if backend.shard_min_bytes > backend.shard_cap_bytes:
            raise InkpackError(
                f"invalid repo config: shard_min_bytes ({backend.shard_min_bytes}) exceeds "
                f"shard_cap_bytes ({backend.shard_cap_bytes})"
            )
    return {
        "identity_policy": identity_policy,
        "backend_mode": configured_mode,
        "profiles": stored_profiles,
        "verify_on_read": verify_on_read,
    }


__all__ = [
    "BlobStore",
    "Busy",
    "Cancelled",
    "ChapterInfo",
    "Clock",
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
