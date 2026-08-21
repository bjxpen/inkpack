"""Sharded-backend specific tests (spec 8.2, 15)."""

from __future__ import annotations

import random
import sqlite3

import pytest

from inkpack import MissingContent, Profile, open_repo

from .conftest import assert_repo_consistent, enc_row, make_repo


def _chapter_text(rng: random.Random, words: list[str], n: int = 60) -> bytes:
    return " ".join(rng.choice(words) for _ in range(n)).encode()


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


# -- end-to-end workflows on the sharded backend ------------------------------


def test_sharded_dedupe_across_shards(repo_sharded):
    """Dedupe still hits after a shard roll: one encoding, payload in one shard."""
    body = b"cross-shard-dedupe " * 100_000  # ~2 MB, fills shard 1
    first = repo_sharded.store.put_bytes(body, profile="raw").result
    repo_sharded.store.put_bytes(b"filler " * 60_000, profile="raw").result  # rolls to shard 2
    assert len(repo_sharded.backend.list_shards()) >= 2

    again = repo_sharded.store.put_bytes(body, profile="raw").result
    assert again.ref.blob_key == first.ref.blob_key
    with repo_sharded.backend.txn(write=False) as conn:
        n = int(
            conn.execute(
                "SELECT COUNT(*) FROM encodings WHERE blob_key=?", (first.ref.blob_key,)
            ).fetchone()[0]
        )
    assert n == 1
    row = enc_row(repo_sharded, first.ref)
    shard_id = int(row["shard_id"])
    with repo_sharded.backend.txn(write=False, attach_shard_id=shard_id) as conn:
        found = conn.execute(
            "SELECT 1 FROM p.payload WHERE blob_key=? AND profile=?",
            (first.ref.blob_key, "raw"),
        ).fetchone()
    assert found is not None
    verify = repo_sharded.store.verify().result
    assert verify.ok == 2 and verify.missing == 0 and verify.corrupt == 0


def test_sharded_end_to_end_workflow(repo_sharded):
    """Full novel-library lifecycle on the sharded backend."""
    rng = random.Random(77)
    words = ["shard", "index", "payload", "journal", "attach", "commit", "atomic", "rollback"]
    novel_id = repo_sharded.create_novel("Sharded Saga")
    bodies = [_chapter_text(rng, words) for _ in range(10)]
    for i, body in enumerate(bodies):
        repo_sharded.upsert_chapter(novel_id, f"{i:04d}", body, "raw")
    refs = list(repo_sharded.iter_live_content(scope=novel_id))
    assert refs

    # Build a dictionary from the novel and recompress with it.
    train = repo_sharded.store.train_dict(bodies).result
    repo_sharded.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=train.dict_id))
    repo_sharded.store.reencode(refs, options={"profile": "zstd_dict"}).result
    for ref in refs:
        assert enc_row(repo_sharded, ref)["zstd_dict_id"] == train.dict_id
        assert repo_sharded.store.get_bytes(ref) in bodies
    assert repo_sharded.store.verify().result.ok == len(refs)

    # Remove half the chapters, GC, confirm the rest is intact.
    with repo_sharded.backend.txn(write=True) as conn:
        rows = conn.execute("SELECT id FROM chapters").fetchall()
        for i, row in enumerate(rows):
            if i % 2:
                conn.execute("DELETE FROM chapters WHERE id=?", (row[0],))
    live = list(repo_sharded.iter_live_content())
    assert 0 < len(live) < len(refs)
    gc = repo_sharded.store.gc(live=live).result
    assert gc.encodings_deleted == len(refs) - len(live)
    for ref in live:
        assert repo_sharded.store.get_bytes(ref) in bodies
    verify = repo_sharded.store.verify().result
    assert verify.ok == len(live) and verify.missing == 0 and verify.corrupt == 0

    # Compact, reopen, verify again from the reopened handle.
    repo_sharded.store.compact().result
    reopened = open_repo(repo_sharded.backend.root)
    assert reopened.backend.mode == "sqlite_sharded"
    assert reopened.store.verify().result.ok == len(live)
    assert reopened.get_profile("zstd_dict").zstd_dict_id == train.dict_id
    assert_repo_consistent(reopened)


def test_sharded_repeated_upsert_and_meta(repo_sharded):
    novel = repo_sharded.create_novel("Sharded Repeat")
    first = repo_sharded.upsert_chapter(novel, "k", b"v1" * 100, "raw")
    second = repo_sharded.upsert_chapter(novel, "k", b"v2" * 100, "raw")
    assert first == second
    assert repo_sharded.get_chapter_bytes(second) == b"v2" * 100
    repo_sharded.meta_set("chapter", second, "k", 1)
    repo_sharded.meta_set("chapter", second, "k", 2)
    assert repo_sharded.meta_get("chapter", second, "k") == 2
    with repo_sharded.backend.txn(write=False) as conn:
        count = int(conn.execute("SELECT COUNT(*) FROM chapters").fetchone()[0])
    assert count == 1
    assert_repo_consistent(repo_sharded)
