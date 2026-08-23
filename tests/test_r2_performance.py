"""Fail-first tests for the r2 issue register, section C (performance).

Deterministic counters/timing-free invariants — no thread-timing dependence:

- C1  per-row ATTACH/txn overhead -> session-managed read attaches (LRU)
- C2  zstd contexts rebuilt per call -> LRU context cache
- C3  iter_live_content materializes -> paged weakly-consistent streaming
- C4  serial sharded open validation -> thread-pool (bench-gated, slow)
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from inkpack import create_repo
from inkpack.sqlite import Session, SqliteBackend

from .conftest import make_profiles

# ---------------------------------------------------------------------------
# C4 — parallel sharded open validation (bench-gated)
# ---------------------------------------------------------------------------


def _make_multi_shard_repo(tmp_path, n_shards: int):
    """A sharded repo with exactly ~n_shards shards (small cap, one put each)."""
    root = tmp_path / "c4"
    repo = create_repo(
        root,
        "sqlite_sharded",
        profiles=make_profiles(),
        shard_cap_bytes=200 << 10,
        min_shard_cap_bytes=128 << 10,
    )
    for i in range(n_shards):
        repo.store.put_bytes(f"c4-fill-{i} ".encode() * 30_000, "raw").result
    return repo


@pytest.mark.slow
def test_sharded_open_validation_parallel_within_5x_serial(tmp_path):
    """C4 gate: parallel open validation stays within 5x of a measured serial
    baseline on ~24 shards (plus a fixed overhead allowance). Correctness is
    anchored by the existing shard/journal-mode suites; this bound only
    guards the parallel path from regressing badly.

    Run with: python -m pytest -m slow
    """
    n = 24
    repo = _make_multi_shard_repo(tmp_path, n)
    backend = repo.backend
    shards = backend.list_shards()
    assert len(shards) >= 20, f"need ~{n} shards for a meaningful baseline, got {len(shards)}"

    t0 = time.perf_counter()
    for sid in shards:  # measured serial baseline
        backend._validate_shard_journal(sid)
    serial = time.perf_counter() - t0

    t0 = time.perf_counter()
    backend._validate_shard_journals(shards)  # parallel path
    parallel = time.perf_counter() - t0

    assert parallel <= serial * 5 + 0.5, (
        f"parallel shard validation ({parallel:.3f}s) regressed past 5x the "
        f"serial baseline ({serial:.3f}s)"
    )


# ---------------------------------------------------------------------------
# C1 — session-managed read attaches
# ---------------------------------------------------------------------------


def test_verify_attaches_each_shard_once_not_per_row(repo_sharded, monkeypatch):
    for i in range(40):
        repo_sharded.store.put_bytes(f"att-{i}".encode(), "raw").result
    attaches = {"n": 0}

    class Counting(sqlite3.Connection):
        def execute(self, sql, *a, **k):
            if str(sql).lstrip().upper().startswith("ATTACH"):
                attaches["n"] += 1
            return super().execute(sql, *a, **k)

    real = sqlite3.connect
    monkeypatch.setattr(
        sqlite3, "connect", lambda *a, **k: real(*a, **k, factory=Counting)
    )
    assert repo_sharded.store.verify().result.ok == 40
    assert attaches["n"] <= 2  # O(shards), not O(rows) — today: ~40


def test_verify_evicts_lru_shards_and_stays_correct(repo_sharded, monkeypatch):
    """Forcing the attach capacity below the shard count must still verify
    everything correctly (LRU eviction is transparent)."""
    for i in range(3):
        repo_sharded.store.put_bytes(f"evict-{i} ".encode() * 600_000, "raw").result
    assert len(repo_sharded.backend.list_shards()) >= 3
    monkeypatch.setattr(Session, "_force_attach_limit", 1)
    result = repo_sharded.store.verify().result
    assert result.ok == 3
    # All content still readable after the eviction churn.
    for ref in repo_sharded.iter_live_content():
        repo_sharded.store.get_bytes(ref)


def test_gc_uses_session_attaches_not_extra_connections(repo_sharded, connect_counter):
    """C1/B1: GC's batch attaches ride on the operation session connection."""
    for i in range(6):
        repo_sharded.store.put_bytes(f"gc-att-{i}".encode(), "raw").result
    connect_counter["opens"] = 0
    result = repo_sharded.store.gc(live=[]).result
    assert result.encodings_deleted == 6
    # One session connection for the whole GC (plus the two dedicated txn
    # connections for orphan blobs / unreferenced dicts).
    assert connect_counter["opens"] <= 3
    assert connect_counter["live"] == 0


# ---------------------------------------------------------------------------
# C2 — zstd context reuse
# ---------------------------------------------------------------------------


def test_reencode_reuses_zstd_contexts(repo, monkeypatch):
    import inkpack.codec as cm

    refs = [
        repo.store.put_bytes(f"ctx-{i} ".encode() * 50, "raw").result.ref for i in range(30)
    ]
    made = {"n": 0}
    real_compressor = cm._zstd.ZstdCompressor

    def counting_factory(*args, **kwargs):
        # Count constructions WITHOUT subclassing the C type: subclassing
        # python-zstandard's C classes with an __init__ override corrupts the
        # heap on GC (upstream bug, zstandard 0.25).
        made["n"] += 1
        return real_compressor(*args, **kwargs)

    monkeypatch.setattr(cm._zstd, "ZstdCompressor", counting_factory)
    repo.store.reencode(refs, options={"codec": "zstd", "params": {"level": 9}}).result
    assert made["n"] <= 2  # one (level, dict id) context, not one per target


def test_context_cache_evicts_lru(repo, monkeypatch):
    """Filling the cache past its bound evicts the least-recent context."""
    import inkpack.codec as cm
    from inkpack import Profile

    engine = repo.store.codec
    monkeypatch.setattr(cm.CodecEngine, "_CONTEXT_CACHE_MAX", 2)
    train_a = repo.store.train_dict([b"ctx-a " * 80] * 6).result
    train_b = repo.store.train_dict([b"ctx-b " * 80] * 6).result
    repo.set_profile(Profile("d_a", "zstd", {"level": 6}, train_a.dict_id))
    repo.set_profile(Profile("d_b", "zstd", {"level": 6}, train_b.dict_id))
    # Two distinct dict contexts fill the size-2 cache; a third evicts the LRU.
    repo.store.put_bytes(b"ctx-a " * 100, "d_a").result
    repo.store.put_bytes(b"ctx-b " * 100, "d_b").result
    repo.store.put_bytes(b"ctx-a " * 200, "d_a").result
    assert len(engine._compressors) <= 2
    # Everything still round-trips through the evicted-and-rebuilt contexts.
    for ref in repo.iter_live_content():
        repo.store.get_bytes(ref)
    assert repo.store.verify().result.corrupt == 0


# ---------------------------------------------------------------------------
# C3 — paged, weakly-consistent iter_live_content
# ---------------------------------------------------------------------------


def test_iter_live_content_streams_without_full_materialization(repo, monkeypatch):
    novel_id = repo.create_novel("Stream")
    for i in range(5):
        repo.upsert_chapter(novel_id, f"{i:04d}", f"body-{i}".encode(), "raw")

    def boom(self, sql, params=()):
        raise AssertionError("iter_chapter_refs must page, not fetchall everything")

    monkeypatch.setattr(SqliteBackend, "_query_all", boom)
    assert len(list(repo.iter_live_content())) == 5  # today: AssertionError


def test_iter_live_content_order_is_global_and_deterministic(repo):
    """Keyset paging must produce the same global order as a single query:
    no per-page reordering, no duplicates across page boundaries."""
    n1 = repo.create_novel("A")
    n2 = repo.create_novel("B")
    repo.upsert_chapter(n1, "1", b"body-a1", "raw")
    repo.upsert_chapter(n1, "2", b"body-a2", "raw")
    repo.upsert_chapter(n2, "1", b"body-a1", "raw")  # shared body dedupes
    all_refs = [(r.blob_key, r.profile) for r in repo.iter_live_content()]
    scoped = [(r.blob_key, r.profile) for r in repo.iter_live_content(scope=n1)]
    assert len(scoped) == 2
    assert all(pair in all_refs for pair in scoped)
    # Deterministic across drains (page boundaries must not shuffle rows).
    again = [(r.blob_key, r.profile) for r in repo.iter_live_content()]
    assert again == all_refs
    assert len(all_refs) == len(set(all_refs))
