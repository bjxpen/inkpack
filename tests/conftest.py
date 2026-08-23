"""Shared fixtures and whitebox helpers for the Inkpack test-suite."""

from __future__ import annotations

import io
import sqlite3

import pytest

from inkpack import ContentRef, Profile, create_repo

BACKENDS = ("sqlite_single", "sqlite_sharded")

# Canonical byte samples that prove "no transcoding" (spec 1.3).
CANONICAL_SAMPLES = [
    b"",
    b"\x00\xff\xfe\x00",
    b"hello\nworld",
    b"word " * 200_000,
]


def make_profiles() -> dict[str, Profile]:
    return {
        "raw": Profile(name="raw", codec="none", params={}),
        "zstd_nodict": Profile(name="zstd_nodict", codec="zstd", params={"level": 3}),
    }


def make_repo(tmp_path, mode: str = "sqlite_single", **kwargs):
    kwargs.setdefault("shard_cap_bytes", 2 << 20)
    kwargs.setdefault("min_shard_cap_bytes", 1 << 20)
    kwargs.setdefault("pragmas", {"busy_timeout_ms": 50})
    kwargs.setdefault("verify_on_read", False)
    profiles = kwargs.pop("profiles", None)
    return create_repo(
        path=tmp_path / f"repo-{mode}",
        backend_mode=mode,
        profiles=profiles or make_profiles(),
        **kwargs,
    )


@pytest.fixture(params=BACKENDS)
def repo(tmp_path, request):
    """Parameterized fixture: one repo per backend mode."""
    return make_repo(tmp_path, request.param)


@pytest.fixture
def repo_single(tmp_path):
    return make_repo(tmp_path, "sqlite_single")


@pytest.fixture
def repo_sharded(tmp_path):
    return make_repo(tmp_path, "sqlite_sharded")


# -- whitebox helpers --------------------------------------------------------


def enc_row(repo, ref: ContentRef):
    with repo.backend.txn(write=False) as conn:
        return conn.execute(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?", (ref.blob_key, ref.profile)
        ).fetchone()


def payload_bytes(repo, ref: ContentRef) -> bytes | None:
    row = enc_row(repo, ref)
    if row is None:
        return None
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=False) as conn:
            found = conn.execute(
                "SELECT data FROM payload WHERE blob_key=? AND profile=?", (ref.blob_key, ref.profile)
            ).fetchone()
            return None if found is None else bytes(found[0])
    with repo.backend.txn(write=False, attach_shard_id=row["shard_id"]) as conn:
        found = conn.execute(
            "SELECT data FROM p.payload WHERE blob_key=? AND profile=?", (ref.blob_key, ref.profile)
        ).fetchone()
        return None if found is None else bytes(found[0])


def set_payload(repo, ref: ContentRef, data: bytes) -> None:
    """Overwrite a payload row (used to simulate corruption)."""
    row = enc_row(repo, ref)
    assert row is not None, "no encoding row to corrupt"
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=True) as conn:
            conn.execute(
                "UPDATE payload SET data=? WHERE blob_key=? AND profile=?",
                (data, ref.blob_key, ref.profile),
            )
    else:
        with repo.backend.txn(write=True, attach_shard_id=row["shard_id"]) as conn:
            conn.execute(
                "UPDATE p.payload SET data=? WHERE blob_key=? AND profile=?",
                (data, ref.blob_key, ref.profile),
            )


def delete_payload_row(repo, ref: ContentRef) -> None:
    """Delete a payload row directly on the DB file (test corruption helper).

    Uses a separate ``sqlite3.connect`` on the file path (review §26), never
    the backend's session connection.
    """
    row = enc_row(repo, ref)
    assert row is not None, "no encoding row to delete payload for"
    if repo.backend.mode == "sqlite_single":
        conn = sqlite3.connect(str(repo.backend.index_path))
    else:
        conn = sqlite3.connect(str(repo.backend.shard_path(int(row["shard_id"]))))
    try:
        conn.execute(
            "DELETE FROM payload WHERE blob_key=? AND profile=?", (ref.blob_key, ref.profile)
        )
        conn.commit()
    finally:
        conn.close()


def delete_dict(repo, dict_id: str) -> None:
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (dict_id,))


def corrupt_shard_btree_page(repo, ref: ContentRef) -> None:
    """Deterministically corrupt the ``payload`` PK-index page for ``ref``.

    The database file stays a *valid* SQLite database at connect time (header
    and PRAGMAs all pass); the damage surfaces on the first statement that
    touches the corrupted page (``database disk image is malformed`` —
    ``sqlite3.DatabaseError``). This pins the present-but-unusable-shard
    classification on paths where connect succeeds (A1/A2).
    """
    backend = repo.backend
    if backend.mode == "sqlite_single":
        path = backend.index_path
    else:
        row = enc_row(repo, ref)
        assert row is not None, "no encoding row to corrupt"
        path = backend.shard_path(int(row["shard_id"]))
    probe = sqlite3.connect(str(path))
    try:
        row = probe.execute(
            "SELECT rootpage FROM sqlite_master WHERE type='index' AND tbl_name='payload'"
        ).fetchone()
        assert row is not None, "no payload index to corrupt"
        rootpage = int(row[0])
        page_size = int(probe.execute("PRAGMA page_size").fetchone()[0])
    finally:
        probe.close()
    data = bytearray(path.read_bytes())
    data[(rootpage - 1) * page_size] = 0xDE  # invalid b-tree page type
    path.write_bytes(bytes(data))
    # Verify the corruption is visible through a fresh connection (guards
    # against WAL replay restoring the page in sqlite_single mode).
    check = sqlite3.connect(str(path))
    try:
        check.execute("SELECT 1 FROM payload WHERE blob_key=? AND profile=?", ("k", "raw")).fetchone()
        raise AssertionError("corruption not visible through a fresh connection")
    except sqlite3.DatabaseError:
        pass
    finally:
        check.close()


def all_encodings(repo) -> list[sqlite3.Row]:
    with repo.backend.txn(write=False) as conn:
        return conn.execute("SELECT * FROM encodings ORDER BY blob_key, profile").fetchall()


def assert_repo_consistent(repo) -> None:
    """Spec 16 invariant helper: no broken references, stored_len matches payload."""
    from inkpack.codec import parse_blob_key_ikb1

    backend = repo.backend
    with backend.txn(write=False) as conn:
        encs = all_encodings(repo)
        blob_rows = conn.execute("SELECT blob_key, raw_len FROM blobs").fetchall()
        dict_ids = {r[0] for r in conn.execute("SELECT dict_id FROM dicts")}
    blob_lens = {r[0]: r[1] for r in blob_rows}
    enc_pairs: set[tuple[str, str]] = set()
    for e in encs:
        pair = (e["blob_key"], e["profile"])
        enc_pairs.add(pair)
        assert e["blob_key"] in blob_lens, f"encoding {pair} has no blobs row"
        assert blob_lens[e["blob_key"]] == parse_blob_key_ikb1(e["blob_key"])[0], (
            f"blobs.raw_len mismatch for {pair}"
        )
        if e["codec"] != "zstd":
            assert e["zstd_dict_id"] is None, f"non-zstd encoding {pair} has a dict"
        if e["zstd_dict_id"] is not None:
            assert e["zstd_dict_id"] in dict_ids, f"encoding {pair} references a missing dict"
        payload = payload_bytes(repo, ContentRef(*pair))
        assert payload is not None, f"encoding {pair} has no payload"
        assert e["stored_len"] == len(payload), f"stored_len mismatch for {pair}"
    # No payload rows without a matching encoding row.
    if backend.mode == "sqlite_single":
        with backend.txn(write=False) as conn:
            rows = conn.execute("SELECT blob_key, profile FROM payload").fetchall()
        for r in rows:
            assert (r[0], r[1]) in enc_pairs, f"orphan payload row {(r[0], r[1])}"
    else:
        for sid in backend.list_shards():
            with backend.txn(write=False, attach_shard_id=sid) as conn:
                rows = conn.execute("SELECT blob_key, profile FROM p.payload").fetchall()
            for r in rows:
                assert (r[0], r[1]) in enc_pairs, f"orphan payload row {(r[0], r[1])} in shard {sid}"


class NonseekableBytesIO(io.BytesIO):
    """A binary stream that refuses seek/tell, like a socket wrapper."""

    def seek(self, *args, **kwargs):  # type: ignore[override]
        raise OSError("seek not supported")

    def tell(self, *args, **kwargs):  # type: ignore[override]
        raise OSError("tell not supported")


class CountingCancel:
    """Cancel token that flips to True after ``limit`` calls."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        return self.calls > self.limit


class AlwaysCancel:
    def __call__(self) -> bool:
        return True


def hold_write_lock(index_path) -> sqlite3.Connection:
    """Hold BEGIN EXCLUSIVE on the index DB so writer ops go busy."""
    conn = sqlite3.connect(str(index_path))
    conn.execute("BEGIN EXCLUSIVE")
    return conn


@pytest.fixture
def connect_counter(monkeypatch):
    """Count opens via connect_file; track liveness via a Connection subclass
    (sqlite3.Connection is immutable, so the factory subclass is the hook)."""
    import sqlite3

    import inkpack.sqlite as sqlite_mod

    state = {"opens": 0, "live": 0}
    original_connect = sqlite3.connect

    class TrackedConnection(sqlite3.Connection):
        def close(self):
            if not getattr(self, "_inkpack_closed_tracked", False):
                self._inkpack_closed_tracked = True
                state["live"] -= 1
            super().close()

    def tracked_connect(*args, **kwargs):
        kwargs["factory"] = TrackedConnection
        conn = original_connect(*args, **kwargs)
        state["live"] += 1
        return conn

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    original = sqlite_mod.connect_file

    def counting(path, busy_timeout_ms, synchronous="NORMAL", *, create=False):
        state["opens"] += 1
        return original(path, busy_timeout_ms, synchronous, create=create)

    monkeypatch.setattr(sqlite_mod, "connect_file", counting)
    return state
