"""Sharded-backend specific tests (spec 8.2, 15)."""

from __future__ import annotations

import sqlite3

import pytest

from inkpack import MissingContent

from .conftest import assert_repo_consistent, enc_row, make_repo


def test_layout_files(repo_sharded):
    root = repo_sharded.backend.root
    assert (root / "index.sqlite").exists()
    assert (root / "payload" / "shard-0001.sqlite").exists()


def test_rollback_journal_mode_enforced(repo_sharded):
    for label, path in [
        ("index", repo_sharded.backend.index_path),
        *[(f"shard-{sid}", repo_sharded.backend.shard_path(sid)) for sid in repo_sharded.backend.list_shards()],
    ]:
        conn = sqlite3.connect(str(path))
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
        finally:
            conn.close()
        assert mode in {"delete", "truncate"}, f"{label} must use rollback journal, got {mode}"


def test_single_mode_uses_wal(repo_single):
    conn = sqlite3.connect(str(repo_single.backend.index_path))
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
    finally:
        conn.close()
    assert mode == "wal"


def test_single_mode_has_no_shards(repo_single):
    assert repo_single.backend.list_shards() == []
    with pytest.raises(ValueError):
        repo_single.backend.shard_path(1)


def test_multi_shard_routing_under_cap(repo_sharded):
    """Payloads roll to fresh shards past the cap; all remain readable."""
    bodies = [b"a" * (700_000 + i) for i in range(4)]
    refs = [repo_sharded.store.put_bytes(body, profile="raw").result.ref for body in bodies]
    shards = repo_sharded.backend.list_shards()
    assert len(shards) >= 2, f"expected multiple shards, got {shards}"
    for ref, body in zip(refs, bodies, strict=True):
        assert repo_sharded.store.get_bytes(ref) == body
    verify = repo_sharded.store.verify().result
    assert verify.ok == len(bodies)


def test_payload_locator_is_shard_id(repo_sharded):
    """Spec 15.2: payload lives in exactly the shard named by encodings.shard_id."""
    bodies = [b"loc" * 500_000 + bytes([i]) * 100 for i in range(4)]
    refs = [repo_sharded.store.put_bytes(body, profile="raw").result.ref for body in bodies]
    assert len(repo_sharded.backend.list_shards()) >= 2
    for ref, body in zip(refs, bodies, strict=True):
        row = enc_row(repo_sharded, ref)
        assert row["shard_id"] is not None
        shard_id = int(row["shard_id"])
        with repo_sharded.backend.txn(write=False, attach_shard_id=shard_id) as conn:
            found = conn.execute(
                "SELECT data FROM p.payload WHERE blob_key=? AND profile=?",
                (ref.blob_key, ref.profile),
            ).fetchone()
        assert found is not None and bytes(found[0]) == body
        for other in repo_sharded.backend.list_shards():
            if other == shard_id:
                continue
            with repo_sharded.backend.txn(write=False, attach_shard_id=other) as conn:
                found = conn.execute(
                    "SELECT 1 FROM p.payload WHERE blob_key=? AND profile=?",
                    (ref.blob_key, ref.profile),
                ).fetchone()
            assert found is None, f"payload of {ref} duplicated in shard {other}"


def test_encodings_always_have_shard_id(repo_sharded):
    for i in range(3):
        repo_sharded.store.put_bytes(f"sid-{i}".encode() * 100, profile="raw").result
    with repo_sharded.backend.txn(write=False) as conn:
        rows = conn.execute("SELECT shard_id FROM encodings").fetchall()
    assert rows
    assert all(r[0] is not None for r in rows)


def test_gc_removes_payload_from_its_shard_atomically(repo_sharded):
    kept = repo_sharded.store.put_bytes(b"shard-keep" * 300_000, profile="raw").result
    dead = repo_sharded.store.put_bytes(b"shard-dead" * 300_000, profile="raw").result
    dead_row = enc_row(repo_sharded, dead.ref)
    dead_shard = int(dead_row["shard_id"])
    gc = repo_sharded.store.gc(live=[kept.ref]).result
    assert gc.encodings_deleted == 1
    with repo_sharded.backend.txn(write=False, attach_shard_id=dead_shard) as conn:
        row = conn.execute(
            "SELECT 1 FROM p.payload WHERE blob_key=? AND profile=?",
            (dead.ref.blob_key, dead.ref.profile),
        ).fetchone()
    assert row is None
    with repo_sharded.backend.txn(write=False) as conn:
        row = conn.execute(
            "SELECT 1 FROM encodings WHERE blob_key=? AND profile=?",
            (dead.ref.blob_key, dead.ref.profile),
        ).fetchone()
    assert row is None
    with pytest.raises(MissingContent):
        repo_sharded.store.get_bytes(dead.ref)
    assert repo_sharded.store.get_bytes(kept.ref) == b"shard-keep" * 300_000
    assert_repo_consistent(repo_sharded)


def test_sharded_reopen_keeps_layout_and_content(tmp_path):
    repo = make_repo(tmp_path, "sqlite_sharded")
    put = repo.store.put_bytes(b"reopen-shard" * 1000, profile="raw").result
    from inkpack import open_repo

    reopened = open_repo(tmp_path / "repo-sqlite_sharded")
    assert reopened.backend.mode == "sqlite_sharded"
    assert reopened.store.get_bytes(put.ref) == b"reopen-shard" * 1000
    assert_repo_consistent(reopened)


def test_sharded_choose_shard_respects_configured_cap(tmp_path):
    repo = make_repo(tmp_path, "sqlite_sharded", shard_cap_bytes=1 << 20)
    assert repo.backend.shard_cap_bytes == 1 << 20
    refs = [repo.store.put_bytes(bytes([i]) * 600_000, profile="raw").result.ref for i in range(4)]
    assert len(repo.backend.list_shards()) >= 3
    for ref in refs:
        assert repo.store.get_bytes(ref) is not None
