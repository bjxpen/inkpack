"""Atomic chapter/novel persist tests (review §10, §11) and the streaming
chapter upsert (review §20)."""

from __future__ import annotations

import io

import pytest

from inkpack import Busy, NotFound, OpEvent

from .conftest import NonseekableBytesIO, assert_repo_consistent, hold_write_lock


def _count(repo, table: str, where: str = "") -> int:
    with repo.backend.txn(write=False) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0])


def test_upsert_missing_novel_does_not_leave_blob(repo):
    body = b"no-orphan" * 50
    key = repo.store.prepare_bytes(body, "raw").blob_key
    encodings_before = _count(repo, "encodings")
    with pytest.raises(NotFound, match="999"):
        repo.upsert_chapter(999, "1", body, "raw")
    assert _count(repo, "encodings") == encodings_before
    assert repo.store.has_blob(key) is False
    assert _count(repo, "chapters") == 0
    assert_repo_consistent(repo)


def test_upsert_chapter_atomic_with_meta(repo):
    novel_id = repo.create_novel("Atomic")
    chapter_id = repo.upsert_chapter(
        novel_id, "1", b"atomic-body", "raw", meta={"lang": "en", "n": 1}
    )
    assert repo.meta_get("chapter", chapter_id, "lang") == "en"
    assert repo.meta_get("chapter", chapter_id, "n") == 1
    assert_repo_consistent(repo)


def test_create_novel_meta_atomic(repo):
    with pytest.raises(ValueError):
        repo.create_novel("Broken", meta={"k": float("nan")})  # canonical_json refuses NaN
    assert repo.list_novels(search="Broken") == []


def test_upsert_replaces_body_same_id(repo):
    novel_id = repo.create_novel("Replace")
    first = repo.upsert_chapter(novel_id, "k", b"v1", "raw")
    second = repo.upsert_chapter(novel_id, "k", b"v2", "zstd_nodict")
    assert first == second
    assert repo.get_chapter_bytes(second) == b"v2"
    assert _count(repo, "chapters") == 1


def test_upsert_busy_no_partial_row(repo):
    novel_id = repo.create_novel("BusyNovel")
    lock = hold_write_lock(repo.backend.index_path)
    try:
        with pytest.raises(Busy):
            repo.upsert_chapter(novel_id, "1", b"partial?", "raw")
    finally:
        lock.rollback()
        lock.close()
    assert _count(repo, "chapters") == 0
    assert _count(repo, "encodings") == 0
    assert_repo_consistent(repo)


def test_upsert_missing_novel_message_contains_id(repo):
    with pytest.raises(NotFound) as exc:
        repo.upsert_chapter(999_999, "1", b"x", "raw")
    assert "999999" in str(exc.value)


# -- §20 streaming chapter upsert ---------------------------------------------


def test_upsert_chapter_stream_roundtrip_non_seekable(repo):
    novel_id = repo.create_novel("Stream")
    stream = NonseekableBytesIO(b"stream-body \x00\xff" * 200)
    chapter_id = repo.upsert_chapter_stream(stream, novel_id, "s1", "raw").result
    assert isinstance(chapter_id, int)
    assert repo.get_chapter_bytes(chapter_id) == b"stream-body \x00\xff" * 200
    assert_repo_consistent(repo)


def test_upsert_chapter_stream_is_operation(repo):
    novel_id = repo.create_novel("StreamOp")
    op = repo.upsert_chapter_stream(io.BytesIO(b"op-body" * 100), novel_id, "1", "raw")
    events = list(op)
    assert all(isinstance(e, OpEvent) for e in events)
    assert events[0].kind == "start" and events[-1].kind == "done"
    chapter_id = op.result
    assert repo.get_chapter_bytes(chapter_id) == b"op-body" * 100


def test_upsert_chapter_stream_replaces_same_key(repo):
    novel_id = repo.create_novel("StreamReplace")
    first = repo.upsert_chapter_stream(io.BytesIO(b"one"), novel_id, "k", "raw").result
    second = repo.upsert_chapter_stream(io.BytesIO(b"two"), novel_id, "k", "raw").result
    assert first == second
    assert repo.get_chapter_bytes(second) == b"two"
    assert _count(repo, "chapters") == 1


def test_upsert_chapter_stream_missing_novel_no_orphan(repo):
    body = b"stream-orphan" * 30
    key = repo.store.prepare_bytes(body, "raw").blob_key
    op = repo.upsert_chapter_stream(io.BytesIO(body), 777, "1", "raw")
    with pytest.raises(NotFound):
        op.result
    assert repo.store.has_blob(key) is False
    assert_repo_consistent(repo)


def test_upsert_chapter_stream_emits_progress_for_large_payload(repo, monkeypatch):
    import inkpack.blobstore as bs

    monkeypatch.setattr(bs, "PROGRESS_INTERVAL", 1024)
    novel_id = repo.create_novel("StreamProgress")
    events = list(
        repo.upsert_chapter_stream(io.BytesIO(b"p" * 8192), novel_id, "1", "raw")
    )
    progress = [e for e in events if e.kind == "progress"]
    assert progress
    assert progress[-1].metrics["bytes_in"] == 8192
    done = [e for e in events if e.kind == "done"][-1]
    assert done.metrics["bytes_in"] == 8192


def test_upsert_chapter_stream_cancel(repo):
    novel_id = repo.create_novel("StreamCancel")
    op = repo.upsert_chapter_stream(io.BytesIO(b"x" * 4096), novel_id, "1", "raw", cancel=lambda: True)
    from inkpack import Cancelled

    with pytest.raises(Cancelled):
        op.result
    assert _count(repo, "chapters") == 0
