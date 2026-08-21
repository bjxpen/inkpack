"""PRAGMA policy, payload-size limit and creation-time pragma tests
(review §2, §14, §29)."""

from __future__ import annotations

import sqlite3

import pytest

from inkpack import Profile, create_repo

from .conftest import assert_repo_consistent, enc_row, make_repo

SYNC_VALUES = {"OFF": 0, "NORMAL": 1, "FULL": 2}


@pytest.mark.parametrize("mode", [("OFF", 0), ("NORMAL", 1), ("FULL", 2), (None, 1)])
def test_synchronous_applied_on_work_connections(tmp_path, mode):
    """§2: `synchronous` is per-connection; every work connection must carry it."""
    requested, expected = mode
    pragmas = None if requested is None else {"synchronous": requested}
    repo = create_repo(
        path=tmp_path / "sync",
        backend_mode="sqlite_single",
        profiles={"raw": Profile("raw", "none", {})},
        pragmas=pragmas,
    )
    with repo.backend.txn() as conn:
        value = conn.execute("PRAGMA synchronous").fetchone()[0]
    assert int(value) == expected


def test_synchronous_applied_on_attached_write_connection(tmp_path):
    """§2 (sharded): the index connection used for ATTACH writes has the policy too."""
    repo = make_repo(tmp_path, "sqlite_sharded", pragmas={"synchronous": "OFF", "busy_timeout_ms": 50})
    with repo.backend.txn(write=True, attach_shard_id=1) as conn:
        value = conn.execute("PRAGMA synchronous").fetchone()[0]
    assert int(value) == 0


def test_vacuum_connections_use_synchronous(tmp_path, monkeypatch):
    """§2: _vacuum connections go through connect_file (policy included)."""
    repo = make_repo(tmp_path, "sqlite_single")
    seen: list[str] = []
    import inkpack.sqlite as sqlite_mod

    original = sqlite_mod.connect_file

    def spy(path, busy_timeout_ms, synchronous="NORMAL"):
        seen.append(synchronous)
        return original(path, busy_timeout_ms, synchronous)

    monkeypatch.setattr(sqlite_mod, "connect_file", spy)
    repo.store.compact().result
    assert seen, "compact must open connections via connect_file"
    assert all(s == "NORMAL" for s in seen)


def test_journal_mode_not_rewritten_on_connect(tmp_path):
    """§2 negative: connecting must not silently rewrite journal_mode."""
    repo = make_repo(tmp_path, "sqlite_single")
    conn = sqlite3.connect(str(repo.backend.index_path))
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
    finally:
        conn.close()
    assert mode == "wal"  # untouched by plain connections


def test_unknown_pragma_rejected(tmp_path):
    with pytest.raises(ValueError):
        create_repo(
            tmp_path / "bad",
            backend_mode="sqlite_single",
            profiles={"raw": Profile("raw", "none", {})},
            pragmas={"cache_size": 1},
        )


# -- §14 payload size limit ---------------------------------------------------


def test_payload_over_limit_raises_no_write(repo, monkeypatch):
    repo.backend._blob_limit = 1024
    with pytest.raises(ValueError) as exc:
        repo.store.put_bytes(b"x" * 2048, profile="raw").result
    message = str(exc.value)
    assert "1024" in message and "2048" in message
    with repo.backend.txn(write=False) as conn:
        encodings = conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0]
        blobs = conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
    assert encodings == 0 and blobs == 0
    assert_repo_consistent(repo)


def test_payload_at_limit_minus_one_succeeds(repo, monkeypatch):
    repo.backend._blob_limit = 4096
    put = repo.store.put_bytes(b"y" * 4095, profile="raw").result
    assert put.stored_len == 4095
    assert repo.store.get_bytes(put.ref) == b"y" * 4095


def test_upsert_chapter_over_limit_raises_no_row(repo, monkeypatch):
    repo.backend._blob_limit = 1024
    novel_id = repo.create_novel("Limit")
    with pytest.raises(ValueError):
        repo.upsert_chapter(novel_id, "1", b"z" * 2048, "raw")
    assert repo.list_chapters(novel_id) == []
    assert_repo_consistent(repo)


def test_limit_uses_connection_getlimit(repo):
    assert repo.backend.blob_length_limit() == 1_000_000_000  # stock build default


# -- §29 creation-time policy -------------------------------------------------


def test_fresh_db_auto_vacuum_none(tmp_path):
    for mode in ("sqlite_single", "sqlite_sharded"):
        repo = create_repo(
            path=tmp_path / f"av-{mode}",
            backend_mode=mode,
            profiles={"raw": Profile("raw", "none", {})},
        )
        with repo.backend.txn() as conn:
            value = conn.execute("PRAGMA auto_vacuum").fetchone()[0]
        assert int(value) == 0, f"auto_vacuum must be NONE on fresh {mode}"


def test_checksum_null_on_put(repo):
    put = repo.store.put_bytes(b"no-checksum", profile="raw").result
    assert enc_row(repo, put.ref)["checksum"] is None
