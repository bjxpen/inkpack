"""Fail-first tests for r4-P3.1 (GC per-batch item events).

``SqliteBackend.gc_iter`` yields a frozen ``GcBatchSummary`` after each
batch's ``COMMIT``+``DETACH`` (never from inside the exclusive window),
and ``BlobStore.gc`` surfaces each one as an ``item`` OpEvent, then a
``done`` carrying the full-run totals. Every test here failed before the
fix — the pre-P3.1 ``gc`` operation emitted NO ``item`` events at all.
"""

from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError

import pytest

from inkpack.sqlite import SqliteBackend
from inkpack.types import GcBatchSummary

from .conftest import assert_repo_consistent, enc_row, payload_bytes

_GC_ITEM_KEYS = frozenset(
    {"index", "batches", "shards", "encodings_deleted", "payload_rows_deleted"}
)


def _gc_events(repo):
    """Run gc(live=[]) to completion; return (events, result)."""
    op = repo.store.gc(live=[])
    events = list(op)
    return events, op.result


def _gc_item_metrics(events) -> list[dict]:
    return [e.metrics for e in events if e.kind == "item" and e.op == "gc"]


def _gc_done_metrics(events) -> dict:
    done = [e for e in events if e.kind == "done" and e.op == "gc"]
    assert len(done) == 1
    return done[0].metrics


def test_gc_iter_yields_frozen_summaries(repo_single):
    """Whitebox: the backend generator yields frozen GcBatchSummary
    instances — one per batch (1-based index) plus the index==0 final
    totals sentinel (which is NOT a batch item)."""
    repo_single.store.put_bytes(b"dead", profile="raw").result
    summaries = list(repo_single.backend.gc_iter([]))
    # Frozen: mutation of a yielded summary must raise FrozenInstanceError.
    with pytest.raises(FrozenInstanceError):
        summaries[0].index = 99  # type: ignore[misc]
    assert len(summaries) == 2  # one batch + the final totals sentinel
    assert all(isinstance(s, GcBatchSummary) for s in summaries)
    batch, final = summaries
    assert (batch.index, batch.batches, batch.shards) == (1, 1, ())
    assert (batch.encodings_deleted, batch.payload_rows_deleted) == (1, 1)
    assert (final.index, final.batches, final.shards) == (0, 1, ())
    assert (final.encodings_deleted, final.payload_rows_deleted) == (1, 1)
    assert_repo_consistent(repo_single)


def test_gc_single_batch_yields_one_item(repo):
    """A small repo is one batch: exactly one item event (index 1,
    batches 1, this batch's shards) whose counts match the done totals."""
    repo.store.put_bytes(b"dead-1", profile="raw").result
    repo.store.put_bytes(b"dead-2", profile="raw").result
    events, result = _gc_events(repo)
    items = _gc_item_metrics(events)
    assert len(items) == 1
    m = items[0]
    assert set(m) == _GC_ITEM_KEYS
    assert m["index"] == 1
    assert m["batches"] == 1
    if repo.backend.mode == "sqlite_single":
        assert m["shards"] == ()  # single mode: the main DB is the batch
    else:
        assert m["shards"] == tuple(sorted(repo.backend.list_shards()))
    assert m["encodings_deleted"] == 2
    assert m["payload_rows_deleted"] == 2
    assert _gc_done_metrics(events) == {
        "encodings_deleted": 2,
        "payload_rows_deleted": 2,
        "blobs_deleted": 2,
        "dicts_deleted": 0,
    }
    assert result.encodings_deleted == 2
    assert result.blobs_deleted == 2
    assert_repo_consistent(repo)


def test_gc_multi_batch_yields_one_item_per_batch(repo_sharded, monkeypatch):
    """Two shards + pinned batch size 1: one item per batch with 1-based
    indices, per-batch shard tuples, and CUMULATIVE counts; the done
    totals equal the operation result."""
    monkeypatch.setattr(SqliteBackend, "_gc_batch_size", lambda self, s: 1)
    dead1 = repo_sharded.store.put_bytes(b"d" * 1_400_000, profile="raw").result
    dead2 = repo_sharded.store.put_bytes(b"e" * 1_400_000, profile="raw").result
    shard1 = int(enc_row(repo_sharded, dead1.ref)["shard_id"])
    shard2 = int(enc_row(repo_sharded, dead2.ref)["shard_id"])
    assert shard1 != shard2

    events, result = _gc_events(repo_sharded)
    items = _gc_item_metrics(events)
    assert len(items) == 2  # one per batch — the final sentinel is NOT an item
    first, second = items
    assert set(first) == _GC_ITEM_KEYS
    assert (first["index"], first["batches"]) == (1, 2)
    assert (second["index"], second["batches"]) == (2, 2)
    assert first["shards"] == (shard1,)
    assert second["shards"] == (shard2,)
    # Counts are cumulative THROUGH each batch.
    assert (first["encodings_deleted"], first["payload_rows_deleted"]) == (1, 1)
    assert (second["encodings_deleted"], second["payload_rows_deleted"]) == (2, 2)
    assert _gc_done_metrics(events) == {
        "encodings_deleted": 2,
        "payload_rows_deleted": 2,
        "blobs_deleted": 2,
        "dicts_deleted": 0,
    }
    assert (result.encodings_deleted, result.payload_rows_deleted) == (2, 2)
    assert (result.blobs_deleted, result.dicts_deleted) == (2, 0)
    assert_repo_consistent(repo_sharded)


def test_gc_final_sweep_deletions_land_only_in_done_totals(repo_sharded, monkeypatch):
    """The r3-A final convergence sweep is NOT a batch item: its deletion
    of the cross-batch parked copy must appear only in the final done /
    result totals, never in any batch's cumulative counts."""
    monkeypatch.setattr(SqliteBackend, "_gc_batch_size", lambda self, s: 1)
    repo_sharded.store.put_bytes(b"a" * 1_500_000, "raw").result  # fills shard 1
    ref = repo_sharded.store.put_bytes(b"b" * 700_000, "raw").result.ref  # rolls to shard 2
    old = int(enc_row(repo_sharded, ref)["shard_id"])
    assert old >= 2
    earlier = old - 1
    payload = payload_bytes(repo_sharded, ref)
    with repo_sharded.backend.txn(write=True, attach_shard_id=earlier) as conn:
        conn.execute(
            "INSERT INTO p.payload(blob_key, profile, data) VALUES(?,?,?)",
            (ref.blob_key, ref.profile, payload),
        )

    events, result = _gc_events(repo_sharded)
    items = _gc_item_metrics(events)
    assert len(items) == 2
    # Batch 1 reclaims shard-1's own dead content; the parked copy survives
    # its window (its encoding is still alive — it dies in batch 2).
    assert items[0]["shards"] == (earlier,)
    assert (items[0]["encodings_deleted"], items[0]["payload_rows_deleted"]) == (1, 1)
    # Batch 2 reclaims the locator shard's dead encoding + payload.
    assert items[1]["shards"] == (old,)
    assert (items[1]["encodings_deleted"], items[1]["payload_rows_deleted"]) == (2, 2)
    # The parked copy is reclaimed ONLY by the final sweep, so the done
    # payload total exceeds the last batch item's cumulative count.
    done = _gc_done_metrics(events)
    assert done["encodings_deleted"] == 2
    assert done["payload_rows_deleted"] == 3  # 2 in-batch + 1 from the sweep
    assert done["blobs_deleted"] == 2
    assert result.payload_rows_deleted == 3
    assert_repo_consistent(repo_sharded)


def test_gc_consumer_between_batches_runs_without_writer_lock(repo_sharded, monkeypatch):
    """P3.1: gc NEVER yields from inside the exclusive window — consumer
    code between batches runs without the writer lock. So immediately after
    the first item event, a second connection can take a BEGIN IMMEDIATE
    on the index without blocking (the batch's COMMIT+DETACH already ran)."""
    monkeypatch.setattr(SqliteBackend, "_gc_batch_size", lambda self, s: 1)
    repo_sharded.store.put_bytes(b"d" * 1_400_000, profile="raw").result
    repo_sharded.store.put_bytes(b"e" * 1_400_000, profile="raw").result

    op = repo_sharded.store.gc(live=[])
    it = iter(op)
    assert next(it).kind == "start"
    ev = next(it)  # suspended AFTER batch 1's commit+detach
    assert ev.kind == "item"
    assert ev.metrics["index"] == 1
    conn = sqlite3.connect(str(repo_sharded.backend.index_path), timeout=2)
    try:
        conn.execute("BEGIN IMMEDIATE")  # would block if the window were still open
        conn.rollback()
    finally:
        conn.close()
    result = op.result  # drive the rest to completion
    assert result.encodings_deleted == 2
    assert_repo_consistent(repo_sharded)
