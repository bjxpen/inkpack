"""Fail-first tests for the r2 issue register, section B (concurrency).

Concurrency scenarios use the H1 failpoint mechanism — no thread-timing
dependence. The one test with a real thread (B1) deterministically pins the
exclusive-window invariant: the put is spawned INSIDE gc's window, so it can
only observe pre- or post-gc state, never an in-between one.

- B1  GC lost-update race -> batched exclusive-window redesign
- B2  dict-existence checks must run inside the profile write txn
- B3  repair + chapter catalog must commit atomically
"""

from __future__ import annotations

import sqlite3
import threading

import pytest

from inkpack import ContentRef, Profile, failpoints

from .conftest import delete_payload_row, enc_row, payload_bytes

# ---------------------------------------------------------------------------
# B1 — gc must never delete content written after its snapshot
# ---------------------------------------------------------------------------


def test_gc_never_deletes_content_written_after_its_snapshot(repo_sharded, monkeypatch):
    """The lost-update race: gc snapshots a dead set, a concurrent put
    RESURRECTS that content (re-put of the same bytes after its payload was
    deleted), and gc's stale snapshot must not delete the put's row.

    Today (autocommit snapshot + per-shard delete txns): gc deletes the
    encoding the put just committed -> MissingContent. With the batched
    exclusive window: the put blocks on the window, re-probes inside its own
    txn, and self-heals as a new write.
    """
    ref = repo_sharded.store.put_bytes(b"victim-body", "raw").result.ref
    # Simulate the race: the payload vanishes, and the put thread re-puts the
    # same bytes DURING gc's exclusive window (spawned by the failpoint).
    delete_payload_row(repo_sharded, ref)
    spawned: list[threading.Thread] = []
    armed = {"once": True}

    def hook() -> None:
        if armed.pop("once", False):
            t = threading.Thread(
                target=lambda: repo_sharded.store.put_bytes(b"victim-body", "raw").result
            )
            t.start()
            spawned.append(t)

    monkeypatch.setitem(failpoints._REGISTRY, "gc.post_snapshot", hook)
    repo_sharded.store.gc(live=[]).result
    for t in spawned:
        t.join()
    assert repo_sharded.store.get_bytes(ref) == b"victim-body"  # today: MissingContent


def test_gc_may_collect_puts_completed_before_gc_started(repo):
    """Pin the AUTHORIZED direction (passes today; must keep passing): a put
    that completed before gc started is dead as of gc's snapshot and
    authorizable for collection."""
    ref = repo.store.put_bytes(b"pre-gc-body", "raw").result.ref
    repo.store.gc(live=[]).result
    from inkpack import MissingContent

    with pytest.raises(MissingContent):
        repo.store.get_bytes(ref)


def test_gc_batch_is_one_atomic_commit(repo_sharded, monkeypatch):
    """A crash inside the batch window rolls back the WHOLE batch: payloads
    and encodings of every shard in the batch survive together."""
    dead1 = repo_sharded.store.put_bytes(b"atomic-1 " * 200_000, "raw").result.ref
    dead2 = repo_sharded.store.put_bytes(b"atomic-2 " * 200_000, "raw").result.ref
    assert len(repo_sharded.backend.list_shards()) >= 2
    assert int(enc_row(repo_sharded, dead1)["shard_id"]) != int(enc_row(repo_sharded, dead2)["shard_id"])

    def crash() -> None:
        raise RuntimeError("crash in batch")

    monkeypatch.setitem(failpoints._REGISTRY, "gc.post_snapshot", crash)
    with pytest.raises(RuntimeError):
        repo_sharded.store.gc(live=[]).result

    # Nothing was deleted: both shards' payloads and encodings are intact.
    assert payload_bytes(repo_sharded, dead1) is not None
    assert payload_bytes(repo_sharded, dead2) is not None
    assert enc_row(repo_sharded, dead1) is not None
    assert enc_row(repo_sharded, dead2) is not None
    # A follow-up GC cleans everything (idempotent recovery).
    monkeypatch.delitem(failpoints._REGISTRY, "gc.post_snapshot")
    result = repo_sharded.store.gc(live=[]).result
    assert result.encodings_deleted == 2


# ---------------------------------------------------------------------------
# B2 — dict checks inside the profile write txn
# ---------------------------------------------------------------------------


def test_set_profile_checks_dicts_inside_the_write_txn(repo, monkeypatch):
    train = repo.store.train_dict([b"txn-dict " * 60] * 6).result
    events: list[str] = []

    class Recording(sqlite3.Connection):
        def execute(self, sql, *a, **k):
            u = str(sql).lstrip().upper()
            if u.startswith("BEGIN"):
                events.append("begin")
            elif u.startswith("COMMIT"):
                events.append("commit")
            elif "FROM dicts" in sql:
                events.append("dict_check")
            return super().execute(sql, *a, **k)

        def commit(self):  # txn_on commits via the method, not execute()
            events.append("commit")
            return super().commit()

    real = sqlite3.connect
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: real(*a, **k, factory=Recording))
    repo.set_profile(Profile("zstd_d", "zstd", {"level": 6}, train.dict_id))
    assert events.count("begin") == 1  # today: 2 (dict probe is its own txn)
    assert events == ["begin", "dict_check", "commit"]


def test_set_profiles_checks_dicts_inside_the_write_txn(repo, monkeypatch):
    """set_profiles routes through the same in-txn check (B2 covers both)."""
    events: list[str] = []

    class Recording(sqlite3.Connection):
        def execute(self, sql, *a, **k):
            u = str(sql).lstrip().upper()
            if u.startswith("BEGIN"):
                events.append("begin")
            elif u.startswith("COMMIT"):
                events.append("commit")
            elif "FROM dicts" in sql:
                events.append("dict_check")
            return super().execute(sql, *a, **k)

        def commit(self):  # txn_on commits via the method, not execute()
            events.append("commit")
            return super().commit()

    real = sqlite3.connect
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: real(*a, **k, factory=Recording))
    repo.set_profiles({"raw": Profile("raw", "none", {})})
    # No dict to check -> one txn, no dict probe.
    assert events == ["begin", "commit"]
    assert events.count("begin") == 1


# ---------------------------------------------------------------------------
# B3 — repair commits atomically with the chapter catalog
# ---------------------------------------------------------------------------


def test_upsert_repair_commits_atomically_with_catalog(repo_sharded, monkeypatch):
    novel_id = repo_sharded.create_novel("AtomicRepair")
    cid = repo_sharded.upsert_chapter(novel_id, "k", b"v1 " * 50, "raw")
    repo_sharded.upsert_chapter(novel_id, "k", b"v2 " * 50, "raw")
    info = repo_sharded.get_chapter(cid)

    def vanish() -> None:
        # The payload dies AFTER prepare's hit probe: the common path's
        # in-txn re-probe must find it gone and take the repair detour.
        delete_payload_row(repo_sharded, ContentRef(info.blob_key, info.profile))

    def crash() -> None:
        raise RuntimeError("crash")

    monkeypatch.setitem(failpoints._REGISTRY, "prepare.post_hit_probe", vanish)
    monkeypatch.setitem(failpoints._REGISTRY, "upsert.pre_commit", crash)
    with pytest.raises(RuntimeError):
        repo_sharded.upsert_chapter(novel_id, "k", b"v2 " * 50, "raw")

    # The crash rolled back the unified repair+catalog txn: the payload the
    # repair would have written is absent (today: the two-txn design leaves
    # it committed while the catalog rolls back).
    info2 = repo_sharded.get_chapter(cid)
    assert payload_bytes(repo_sharded, ContentRef(info2.blob_key, info2.profile)) is None
    monkeypatch.undo()
    repo_sharded.upsert_chapter(novel_id, "k", b"v2 " * 50, "raw")  # retry heals
    assert repo_sharded.get_chapter_bytes(cid) == b"v2 " * 50
