from __future__ import annotations

from .blobstore import BlobStore
from .codec import CodecEngine
from .repo import Repository
from .sqlite import SqliteBackend
from .types import (
    Busy,
    Cancelled,
    CompactResult,
    ContentRef,
    CorruptContent,
    GcResult,
    InkpackError,
    MissingContent,
    NotFound,
    OpEvent,
    Operation,
    Profile,
    PutResult,
    ReencodeResult,
    TrainDictResult,
    VerifyResult,
)


def create_repo(
    path,
    backend_mode="sqlite_single",
    profiles: dict[str, Profile] | None = None,
    pragmas=None,
    shard_cap_bytes=2 << 30,
    shard_min_bytes=256 << 20,
    verify_on_read=False,
):
    profiles = profiles or {"raw": Profile(name="raw", codec="none", params={})}
    backend = SqliteBackend.open(
        path=path,
        mode=backend_mode,
        pragmas=pragmas,
        shard_cap_bytes=shard_cap_bytes,
        shard_min_bytes=shard_min_bytes,
    )
    backend.config_set("identity_policy", "ikb1")
    backend.config_set("backend_mode", backend_mode)
    backend.config_set(
        "profiles",
        {
            name: {"codec": p.codec, "params": p.params, "zstd_dict_id": p.zstd_dict_id}
            for name, p in profiles.items()
        },
    )
    backend.config_set("verify_on_read", bool(verify_on_read))
    if backend_mode == "sqlite_sharded":
        backend.config_set("shard_cap_bytes", int(shard_cap_bytes))
        backend.config_set("shard_min_bytes", int(shard_min_bytes))
    store = BlobStore(backend=backend, codec=CodecEngine())
    return Repository(backend=backend, store=store)


def open_repo(path, pragmas=None):
    del pragmas
    from pathlib import Path

    root = Path(path)
    mode = "sqlite_sharded" if (root / "index.sqlite").exists() else "sqlite_single"
    backend = SqliteBackend.open(path=path, mode=mode)
    identity_policy = backend.config_get("identity_policy")
    if identity_policy != "ikb1":
        raise InkpackError("identity_policy mismatch; expected ikb1")
    if backend.config_get("profiles") is None:
        raise InkpackError("profiles missing in repo config")
    store = BlobStore(backend=backend, codec=CodecEngine())
    return Repository(backend=backend, store=store)


__all__ = [
    "create_repo",
    "open_repo",
    "Repository",
    "BlobStore",
    "Operation",
    "OpEvent",
    "ContentRef",
    "Profile",
    "PutResult",
    "TrainDictResult",
    "VerifyResult",
    "GcResult",
    "CompactResult",
    "ReencodeResult",
    "InkpackError",
    "NotFound",
    "MissingContent",
    "CorruptContent",
    "Busy",
    "Cancelled",
]
