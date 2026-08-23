"""GC cancellation and set-based delete tests (review §12, §17)."""

from __future__ import annotations

import pytest

from inkpack import Cancelled, ContentRef

from .conftest import assert_repo_consistent, enc_row


def _enc_count(repo) -> int:
    with repo.backend.txn(write=False) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0])


def _payload_count(repo) -> int:
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=False) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM payload").fetchone()[0])
    total = 0
    for sid in repo.backend.list_shards():
        with repo.backend.txn(write=False, attach_shard_id=sid) as conn:
            total += int(conn.execute("SELECT COUNT(*) FROM p.payload").fetchone()[0])
    return total


def test_gc_cancel_before_start(repo):
    refs = [repo.store.put_bytes(f"gc-{i}".encode(), profile="raw").result.ref for i in range(5)]
    before = (_enc_count(repo), _payload_count(repo))
    op = repo.store.gc(live=refs, cancel=lambda: True)
    with pytest.raises(Cancelled):
        op.result
    assert (_enc_count(repo), _payload_count(repo)) == before
    assert_repo_consistent(repo)


def test_gc_cancel_during_live_staging(repo):
    """A 20k-ref live set can abort mid-staging (batched inserts + cancel)."""
    live = [
        ContentRef(blob_key=f"ikb1:{i}:" + "a" * 64 + "b" * 32, profile="raw") for i in range(20_000)
    ]
    calls = {"n": 0}

    def token() -> bool:
        calls["n"] += 1
        return calls["n"] > 3

    op = repo.store.gc(live=live, cancel=token)
    with pytest.raises(Cancelled):
        op.result
    assert_repo_consistent(repo)  # nothing was deleted


def test_gc_cancel_after_batch_commit_keeps_next_batch(repo_sharded, monkeypatch):
    """Decision G amendment (B1) + Cancelled: committed BATCHES stay deleted;
    the cancelled batch is untouched (both its payloads and encodings remain).

    Default batching would put both shards in one batch, so the batch size is
    pinned to 1 to retain per-batch commit coverage."""
    # Two payloads large enough to force a roll into shard 2.
    dead1 = repo_sharded.store.put_bytes(b"d" * 1_400_000, profile="raw").result
    dead2 = repo_sharded.store.put_bytes(b"e" * 1_400_000, profile="raw").result
    assert len(repo_sharded.backend.list_shards()) >= 2
    shard_of_1 = int(enc_row(repo_sharded, dead1.ref)["shard_id"])
    shard_of_2 = int(enc_row(repo_sharded, dead2.ref)["shard_id"])
    assert shard_of_1 != shard_of_2

    from inkpack.sqlite import SqliteBackend

    monkeypatch.setattr(SqliteBackend, "_gc_batch_size", lambda self, s: 1)

    calls = {"n": 0}

    def token() -> bool:
        calls["n"] += 1
        # Batch checkpoints: BlobStore start (1), backend start (2),
        # batch-1 start (3), post-batch-1 commit (4), batch-2 start (5 -> True).
        return calls["n"] > 4

    op = repo_sharded.store.gc(live=[], cancel=token)
    with pytest.raises(Cancelled):
        op.result

    # The first batch (lowest shard id) committed fully; the second batch is
    # untouched.
    first_gone = enc_row(repo_sharded, dead1.ref) is None
    second_gone = enc_row(repo_sharded, dead2.ref) is None
    assert first_gone != second_gone
    survivor_ref = dead2.ref if first_gone else dead1.ref
    assert enc_row(repo_sharded, survivor_ref) is not None
    from .conftest import payload_bytes

    assert payload_bytes(repo_sharded, survivor_ref) is not None
    assert_repo_consistent(repo_sharded)

    # A follow-up GC cleans the remainder.
    remaining = repo_sharded.store.gc(live=[]).result
    assert remaining.encodings_deleted == 1
    assert_repo_consistent(repo_sharded)


def test_gc_then_verify_after_cancel(repo):
    keep = repo.store.put_bytes(b"keep-after-cancel", profile="raw").result.ref
    repo.store.put_bytes(b"dead-after-cancel", profile="raw").result
    calls = {"n": 0}

    def token() -> bool:
        calls["n"] += 1
        # Cancels at the pre-shard check (call 3), before anything is deleted.
        return calls["n"] > 2

    with pytest.raises(Cancelled):
        repo.store.gc(live=[keep], cancel=token).result
    verify = repo.store.verify().result
    assert verify.ok == 2  # nothing lost
    repo.store.gc(live=[keep]).result
    verify = repo.store.verify().result
    assert verify.ok == 1
    assert repo.store.get_bytes(keep) == b"keep-after-cancel"


def test_gc_idempotent_second_run_zeros(repo):
    refs = [repo.store.put_bytes(f"idem-{i}".encode(), profile="raw").result.ref for i in range(3)]
    first = repo.store.gc(live=refs[:1]).result
    assert first.encodings_deleted == 2
    assert first.payload_rows_deleted == 2
    second = repo.store.gc(live=refs[:1]).result
    assert second.encodings_deleted == 0
    assert second.payload_rows_deleted == 0
    assert second.blobs_deleted == 0
    assert second.dicts_deleted == 0
    assert_repo_consistent(repo)


def test_gc_counts_match_dead_set(repo):
    keep = repo.store.put_bytes(b"count-keep", profile="raw").result.ref
    dead = repo.store.put_bytes(b"count-dead", profile="raw").result.ref
    gc = repo.store.gc(live=[keep]).result
    assert gc.encodings_deleted == 1
    assert gc.payload_rows_deleted == 1
    assert gc.blobs_deleted == 1
    from inkpack import MissingContent

    with pytest.raises(MissingContent):
        repo.store.get_bytes(dead)
    assert repo.store.get_bytes(keep) == b"count-keep"


def test_gc_does_not_materialize_dead_list(repo):
    """L26: the Python list helper is gone; GC stages dead rows in SQL only."""
    repo.store.put_bytes(b"keep-a", profile="raw").result
    repo.store.put_bytes(b"dead-a", profile="raw").result
    assert not hasattr(repo.backend, "list_dead_encodings")
    repo.store.gc(live=list(repo.iter_live_content())).result
    assert_repo_consistent(repo)

