"""Compact, Busy and cancellation tests (spec 10.3, 13, decisions F/H/K)."""

from __future__ import annotations

import io
import sqlite3

import pytest

from inkpack import Busy, Cancelled, InkpackError, open_repo

from .conftest import AlwaysCancel, CountingCancel, assert_repo_consistent, hold_write_lock


def test_compact_returns_nonempty_targets(repo):
    put = repo.store.put_bytes(b"compact-me" * 100, profile="raw").result
    assert repo.store.get_bytes(put.ref) == b"compact-me" * 100
    result = repo.store.compact().result
    assert result.mode  # non-empty mode (decision K)
    assert result.targets  # non-empty targets
    assert len(result.targets) == len(set(result.targets))
    for target in result.targets:
        assert target.endswith(".sqlite")
    assert repo.store.get_bytes(put.ref) == b"compact-me" * 100


def test_compact_vacuums_every_shard(repo_sharded):
    put = repo_sharded.store.put_bytes(b"vac" * 1000, profile="raw").result
    result = repo_sharded.store.compact().result
    names = {t.rsplit("/", 1)[-1] for t in result.targets}
    assert "index.sqlite" in names
    assert names == {"index.sqlite"} | {f"shard-{sid:04d}.sqlite" for sid in repo_sharded.backend.list_shards()}
    assert repo_sharded.store.get_bytes(put.ref) == b"vac" * 1000


def test_compact_options_restrict_shards(repo_sharded):
    repo_sharded.store.put_bytes(b"restrict" * 500, profile="raw").result
    result = repo_sharded.store.compact(options={"shard_ids": [1]}).result
    names = {t.rsplit("/", 1)[-1] for t in result.targets}
    assert names == {"index.sqlite", "shard-0001.sqlite"}


def test_compact_unknown_option_raises(repo):
    with pytest.raises(ValueError):
        repo.store.compact(options={"bogus": True}).result


@pytest.mark.parametrize(
    "op_name",
    ["put_bytes", "put_stream", "gc", "reencode", "compact", "train_dict"],
)
def test_writer_ops_raise_busy_when_locked(repo, op_name):
    """Decision F: writer ops must translate lock timeouts into Busy."""
    # Prepare any content needed by the op before taking the lock.
    if op_name == "reencode":
        put_ref = repo.store.put_bytes(b"reencode-busy", profile="raw").result.ref
    lock = hold_write_lock(repo.backend.index_path)
    try:
        if op_name == "put_bytes":
            op = repo.store.put_bytes(b"busy", profile="raw")
        elif op_name == "put_stream":
            op = repo.store.put_stream(io.BytesIO(b"busy"), profile="raw")
        elif op_name == "gc":
            op = repo.store.gc(live=[])
        elif op_name == "reencode":
            op = repo.store.reencode([put_ref])
        elif op_name == "compact":
            op = repo.store.compact()
        else:
            op = repo.store.train_dict([b"busy"] * 5)
        with pytest.raises(Busy):
            op.result
    finally:
        lock.rollback()
        lock.close()
    # After the lock is released, a fresh operation succeeds (a failed
    # Operation keeps its recorded error and re-raises it from .result).
    if op_name == "put_bytes":
        result = repo.store.put_bytes(b"busy", profile="raw").result
        assert repo.store.get_bytes(result.ref) == b"busy"
    elif op_name == "put_stream":
        result = repo.store.put_stream(io.BytesIO(b"busy"), profile="raw").result
        assert repo.store.get_bytes(result.ref) == b"busy"
    elif op_name == "gc":
        assert repo.store.gc(live=[]).result is not None
    elif op_name == "reencode":
        assert repo.store.reencode([]).result is not None
    elif op_name == "compact":
        assert repo.store.compact().result is not None
    else:
        assert repo.store.train_dict([b"busy"] * 5).result is not None


def test_cancel_verify_mid_run(repo):
    for i in range(300):
        repo.store.put_bytes(f"cancel-{i}".encode(), profile="raw").result
    op = repo.store.verify(cancel=CountingCancel(limit=10))
    with pytest.raises(Cancelled):
        op.result
    # verify is read-only: a re-run with no cancellation must be clean.
    verify = repo.store.verify().result
    assert verify.checked == 300
    assert verify.ok == 300 and verify.missing == 0 and verify.corrupt == 0


def test_cancel_gc_mid_run_keeps_consistency(repo):
    for i in range(5):
        repo.store.put_bytes(f"gc-cancel-{i}".encode(), profile="raw").result
    # Fires at the check between listing dead encodings and deleting them.
    op = repo.store.gc(live=[], cancel=CountingCancel(limit=1))
    with pytest.raises(Cancelled):
        op.result
    assert_repo_consistent(repo)
    gc = repo.store.gc(live=[]).result
    assert gc.encodings_deleted == 5


def test_cancel_reencode_partial_progress_is_consistent(repo):
    refs = []
    for i in range(5):
        body = f"re-cancel-{i}".encode() * 50
        refs.append((repo.store.put_bytes(body, profile="zstd_nodict").result.ref, body))
    # Cancel after the first two targets are re-encoded.
    op = repo.store.reencode([ref for ref, _ in refs], cancel=CountingCancel(limit=3))
    with pytest.raises(Cancelled):
        op.result
    for ref, body in refs:
        assert repo.store.get_bytes(ref) == body  # all still decodable
    verify = repo.store.verify().result
    assert verify.ok == 5 and verify.missing == 0 and verify.corrupt == 0
    assert_repo_consistent(repo)


def test_cancel_put_leaves_no_trace(repo):
    op = repo.store.put_bytes(b"cancelled-put", profile="raw", cancel=AlwaysCancel())
    with pytest.raises(Cancelled):
        op.result
    assert_repo_consistent(repo)
    assert repo.store.verify().result.checked == 0


def test_cancel_stream_put_mid_read(repo):
    op = repo.store.put_stream(io.BytesIO(b"x" * 4096), profile="raw", cancel=AlwaysCancel())
    with pytest.raises(Cancelled):
        op.result


def test_cancel_train_dict(repo):
    op = repo.store.train_dict([b"sample" * 10], cancel=AlwaysCancel())
    with pytest.raises(Cancelled):
        op.result


def test_open_repo_refuses_wal_in_sharded_mode(repo_sharded):
    """Spec 15.1: switching a shard DB to WAL must be refused on open."""
    index_path = repo_sharded.backend.index_path
    raw = sqlite3.connect(str(index_path))
    try:
        mode = raw.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower()
        assert mode == "wal"
    finally:
        raw.close()
    with pytest.raises(InkpackError):
        open_repo(repo_sharded.backend.root)
