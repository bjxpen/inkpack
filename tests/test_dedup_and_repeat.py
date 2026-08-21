"""Blob dedupe (same chapter content) and repeated chapter/novel submission tests."""

from __future__ import annotations

import io

from inkpack import open_repo

from .conftest import assert_repo_consistent


def _count(repo, table: str) -> int:
    with repo.backend.txn(write=False) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _payload_rows(repo) -> int:
    if repo.backend.mode == "sqlite_single":
        return _count(repo, "payload")
    total = 0
    for shard_id in repo.backend.list_shards():
        with repo.backend.txn(write=False, attach_shard_id=shard_id) as conn:
            total += int(conn.execute("SELECT COUNT(*) FROM p.payload").fetchone()[0])
    return total


def _live_pairs(repo) -> set[tuple[str, str]]:
    return {(r.blob_key, r.profile) for r in repo.iter_live_content()}


# -- blob dedupe: same chapter content ---------------------------------------


def test_same_chapter_content_across_novels_dedupes(repo):
    novel_a = repo.create_novel("A")
    novel_b = repo.create_novel("B")
    body = b"shared chapter body \x00\xff" * 50
    chapter_a = repo.upsert_chapter(novel_a, "001", body, "raw")
    chapter_b = repo.upsert_chapter(novel_b, "001", body, "raw")
    assert chapter_a != chapter_b  # distinct chapters...
    assert _count(repo, "blobs") == 1  # ...but one shared blob
    assert _count(repo, "encodings") == 1
    assert _payload_rows(repo) == 1
    assert len(_live_pairs(repo)) == 1
    assert repo.get_chapter_bytes(chapter_a) == body
    assert repo.get_chapter_bytes(chapter_b) == body


def test_same_chapter_content_same_novel_different_keys(repo):
    novel = repo.create_novel("N")
    body = b"same body, two chapters" * 10
    a = repo.upsert_chapter(novel, "ch1", body, "raw")
    b = repo.upsert_chapter(novel, "ch2", body, "raw")
    assert a != b
    assert _count(repo, "blobs") == 1
    assert _count(repo, "encodings") == 1
    assert _payload_rows(repo) == 1
    assert len(_live_pairs(repo)) == 1


def test_same_content_different_profiles_shares_blob(repo):
    novel = repo.create_novel("N")
    body = b"shared across profiles" * 30
    a = repo.upsert_chapter(novel, "p1", body, "raw")
    b = repo.upsert_chapter(novel, "p2", body, "zstd_nodict")
    assert _count(repo, "blobs") == 1  # blob-level dedupe
    assert _count(repo, "encodings") == 2  # one encoding per profile
    assert repo.get_chapter_bytes(a) == body
    assert repo.get_chapter_bytes(b) == body


# -- repeated chapter/novel submission ----------------------------------------


def test_repeated_upsert_same_key_same_body_is_stable(repo):
    novel = repo.create_novel("N")
    body = b"stable body" * 20
    first = repo.upsert_chapter(novel, "k", body, "raw")
    second = repo.upsert_chapter(novel, "k", body, "raw")
    third = repo.upsert_chapter(novel, "k", body, "raw")
    assert first == second == third  # same chapter id every time
    assert _count(repo, "chapters") == 1
    assert _count(repo, "blobs") == 1
    assert _count(repo, "encodings") == 1
    assert _payload_rows(repo) == 1
    assert repo.get_chapter_bytes(first) == body


def test_repeated_upsert_same_key_different_body_replaces(repo):
    novel = repo.create_novel("N")
    first = repo.upsert_chapter(novel, "k", b"version-1", "raw")
    second = repo.upsert_chapter(novel, "k", b"version-2", "raw")
    assert first == second
    assert repo.get_chapter_bytes(second) == b"version-2"
    assert _count(repo, "chapters") == 1
    assert _count(repo, "blobs") == 2  # old body orphaned until GC
    assert len(_live_pairs(repo)) == 1
    repo.store.gc(live=list(repo.iter_live_content())).result
    assert _count(repo, "blobs") == 1
    assert repo.get_chapter_bytes(second) == b"version-2"
    assert_repo_consistent(repo)


def test_repeated_upsert_cycle_reuses_original_blob(repo):
    novel = repo.create_novel("N")
    body_a, body_b = b"A" * 100, b"B" * 100
    chapter = repo.upsert_chapter(novel, "k", body_a, "raw")
    repo.upsert_chapter(novel, "k", body_b, "raw")
    repo.upsert_chapter(novel, "k", body_a, "raw")
    repo.upsert_chapter(novel, "k", body_a, "raw")
    assert repo.get_chapter_bytes(chapter) == body_a
    assert _count(repo, "chapters") == 1
    assert _count(repo, "blobs") == 2  # A and B only
    assert _count(repo, "encodings") == 2


def test_repeated_create_novel_same_title(repo):
    a = repo.create_novel("Same Title")
    b = repo.create_novel("Same Title")
    assert a != b  # novels are never deduped
    assert len(repo.list_novels(search="Same Title")) == 2
    repo.meta_set("novel", a, "k", "first")
    repo.meta_set("novel", b, "k", "second")
    assert repo.meta_get("novel", a, "k") == "first"
    assert repo.meta_get("novel", b, "k") == "second"


def test_repeated_put_bytes_same_content(repo):
    data = b"repeated content" * 100
    first = repo.store.put_bytes(data, profile="raw").result
    for _ in range(5):
        again = repo.store.put_bytes(data, profile="raw").result
        assert again.ref.blob_key == first.ref.blob_key
    assert _count(repo, "blobs") == 1
    assert _count(repo, "encodings") == 1
    assert _payload_rows(repo) == 1
    assert repo.store.verify().result.ok == 1


def test_repeated_mixed_stream_and_bytes_put(repo):
    data = b"mixed path content" * 50
    via_bytes = repo.store.put_bytes(data, profile="raw").result
    via_stream = repo.store.put_stream(io.BytesIO(data), profile="raw").result
    via_stream_again = repo.store.put_stream(io.BytesIO(data), profile="raw").result
    assert via_bytes.ref.blob_key == via_stream.ref.blob_key == via_stream_again.ref.blob_key
    assert _count(repo, "blobs") == 1


def test_repeated_meta_set_same_key(repo):
    novel = repo.create_novel("M")
    repo.meta_set("novel", novel, "k", {"v": 1})
    repo.meta_set("novel", novel, "k", {"v": 2})
    repo.meta_set("novel", novel, "k", {"v": 1})
    assert repo.meta_get("novel", novel, "k") == {"v": 1}
    with repo.backend.txn(write=False) as conn:
        count = int(conn.execute("SELECT COUNT(*) FROM meta WHERE key='k'").fetchone()[0])
    assert count == 1  # overwrite in place, not duplicate rows


def test_repeated_open_repo(tmp_path, repo):
    path = tmp_path / f"repo-{repo.backend.mode}"
    put = repo.store.put_bytes(b"persist", profile="raw").result
    for _ in range(3):
        reopened = open_repo(path)
        assert reopened.store.get_bytes(put.ref) == b"persist"
        assert reopened.get_profile("raw").codec == "none"


def test_dedupe_survives_gc_when_still_live(repo):
    novel = repo.create_novel("N")
    body = b"gc-shared body" * 20
    a = repo.upsert_chapter(novel, "1", body, "raw")
    b = repo.upsert_chapter(novel, "2", body, "raw")
    live = list(repo.iter_live_content())
    assert len(live) == 1
    repo.store.gc(live=live).result
    assert repo.get_chapter_bytes(a) == body
    assert repo.get_chapter_bytes(b) == body
    assert repo.store.has_blob(live[0].blob_key) is True
    # Removing one chapter keeps the blob alive via the other.
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM chapters WHERE id=?", (a,))
    repo.store.gc(live=list(repo.iter_live_content())).result
    assert repo.get_chapter_bytes(b) == body
    assert repo.store.has_blob(live[0].blob_key) is True
    assert_repo_consistent(repo)
