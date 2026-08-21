"""Repository catalog-listing/delete API tests (review §19)."""

from __future__ import annotations

import pytest

from inkpack import ChapterInfo, InkpackError, NotFound

from .conftest import assert_repo_consistent


def test_get_novel_and_not_found(repo):
    novel_id = repo.create_novel("Listed", meta={"genre": "x"})
    novel = repo.get_novel(novel_id)
    assert novel["title"] == "Listed"
    assert novel["id"] == novel_id
    with pytest.raises(NotFound):
        repo.get_novel(999_999)


def test_list_chapters_order_and_fields(repo):
    novel_id = repo.create_novel("Chapters")
    repo.upsert_chapter(novel_id, "002", b"two", "raw")
    repo.upsert_chapter(novel_id, "001", b"one", "zstd_nodict", hints={"charset": "utf-8"})
    chapters = repo.list_chapters(novel_id)
    # Catalog ordering is by order_key (text sort, review P1-7).
    assert [c.order_key for c in chapters] == ["001", "002"]
    info = chapters[0]
    assert isinstance(info, ChapterInfo)
    assert info.novel_id == novel_id
    assert info.profile == "zstd_nodict"
    assert info.charset == "utf-8"
    assert "body" not in str(info)  # never materialized in the info object
    assert repo.get_chapter_bytes(info.id) == b"one"


def test_list_chapters_missing_novel_vs_empty(repo):
    with pytest.raises(NotFound):
        repo.list_chapters(999_999)
    novel_id = repo.create_novel("Empty")
    assert repo.list_chapters(novel_id) == []


def test_get_chapter_info(repo):
    novel_id = repo.create_novel("Info")
    chapter_id = repo.upsert_chapter(novel_id, "1", b"body", "raw")
    info = repo.get_chapter(chapter_id)
    assert info.id == chapter_id
    assert info.order_key == "1"
    with pytest.raises(NotFound):
        repo.get_chapter(999_999)


def test_iter_live_content_matches_list_chapters(repo):
    novel_id = repo.create_novel("Live")
    repo.upsert_chapter(novel_id, "1", b"first", "raw")
    repo.upsert_chapter(novel_id, "2", b"second", "zstd_nodict")
    live = set(repo.iter_live_content(scope=novel_id))
    rows = repo.backend.list_chapters(novel_id)
    expected = {__import__("inkpack").ContentRef(blob_key=r["blob_key"], profile=r["profile"]) for r in rows}
    assert live == expected


def test_delete_chapter_keeps_blob_until_gc(repo):
    novel_id = repo.create_novel("Del")
    chapter_id = repo.upsert_chapter(novel_id, "1", b"delete-me", "raw")
    ref = next(r for r in repo.iter_live_content(scope=novel_id))
    repo.delete_chapter(chapter_id)
    with pytest.raises(NotFound):
        repo.get_chapter(chapter_id)
    assert repo.store.has_blob(ref.blob_key) is True  # catalog-only delete
    repo.store.gc(live=list(repo.iter_live_content())).result
    assert repo.store.has_blob(ref.blob_key) is False
    assert_repo_consistent(repo)


def test_delete_chapter_missing_raises(repo):
    with pytest.raises(NotFound):
        repo.delete_chapter(999_999)


def test_delete_novel_cascade_shared_blob_survives(repo):
    novel_a = repo.create_novel("A")
    novel_b = repo.create_novel("B")
    shared = b"shared-between-novels" * 20
    repo.upsert_chapter(novel_a, "1", shared, "raw")
    chapter_b = repo.upsert_chapter(novel_b, "1", shared, "raw")
    repo.delete_novel(novel_a)
    with pytest.raises(NotFound):
        repo.get_novel(novel_a)
    with pytest.raises(NotFound):
        repo.list_chapters(novel_a)
    live = list(repo.iter_live_content())
    assert len(live) == 1
    repo.store.gc(live=live).result
    assert repo.get_chapter_bytes(chapter_b) == shared  # shared blob survived
    assert_repo_consistent(repo)


def test_delete_novel_requires_cascade(repo):
    novel_id = repo.create_novel("N")
    repo.upsert_chapter(novel_id, "1", b"x", "raw")
    with pytest.raises(InkpackError, match="cascade"):
        repo.delete_novel(novel_id, cascade=False)
    assert repo.get_novel(novel_id)["title"] == "N"  # nothing deleted


def test_update_novel_title_and_slug(repo):
    novel_id = repo.create_novel("Old")
    repo.update_novel(novel_id, title="New", slug="new-slug")
    novel = repo.get_novel(novel_id)
    assert novel["title"] == "New"
    assert novel["slug"] == "new-slug"
    repo.update_novel(novel_id, title="Newer")
    assert repo.get_novel(novel_id)["slug"] == "new-slug"  # unchanged field kept
    with pytest.raises(NotFound):
        repo.update_novel(999_999, title="x")


def test_delete_novel_removes_meta(repo):
    novel_id = repo.create_novel("MetaDel", meta={"genre": "x"})
    chapter_id = repo.upsert_chapter(novel_id, "1", b"y", "raw", meta={"k": 1})
    repo.meta_set("chapter", chapter_id, "extra", 2)
    repo.delete_novel(novel_id)
    with repo.backend.txn(write=False) as conn:
        meta_rows = conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0]
        chapter_rows = conn.execute("SELECT COUNT(*) FROM chapters").fetchone()[0]
    assert meta_rows == 0
    assert chapter_rows == 0


def test_delete_novel_missing_raises(repo):
    with pytest.raises(NotFound):
        repo.delete_novel(999_999)
