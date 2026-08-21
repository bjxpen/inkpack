"""Repository model tests: novels, chapters, KV metadata, live refs (spec 4.4, 7.2)."""

from __future__ import annotations

import io

import pytest

from inkpack import ContentRef, MissingContent, NotFound

from .conftest import assert_repo_consistent


def test_create_and_list_novels(repo):
    ids = [repo.create_novel(f"Novel {i}") for i in range(3)]
    assert len(ids) == 3 and len(set(ids)) == 3
    novels = repo.list_novels()
    assert [n["id"] for n in novels] == sorted(ids, reverse=True)
    assert repo.list_novels(search="Novel 1")[0]["id"] == ids[1]
    assert repo.list_novels(search="no-such") == []


def test_create_novel_with_meta(repo):
    novel_id = repo.create_novel("Metad", meta={"genre": "fantasy", "nested": {"a": 1}})
    assert repo.meta_get("novel", novel_id, "genre") == "fantasy"
    assert repo.meta_get("novel", novel_id, "nested") == {"a": 1}


def test_upsert_chapter_roundtrip_with_hints_and_meta(repo):
    novel_id = repo.create_novel("Chapters")
    chapter_id = repo.upsert_chapter(
        novel_id,
        "ch-001",
        b"chapter-body\x00\xff",
        "raw",
        hints={"media_type": "text/plain", "charset": "utf-8"},
        meta={"lang": "en", "notes": [1, 2, 3]},
    )
    assert repo.get_chapter_bytes(chapter_id) == b"chapter-body\x00\xff"
    assert repo.open_chapter(chapter_id).read() == b"chapter-body\x00\xff"
    assert isinstance(repo.open_chapter(chapter_id), io.BytesIO)
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT * FROM chapters WHERE id=?", (chapter_id,)).fetchone()
    assert row["media_type"] == "text/plain"
    assert row["charset"] == "utf-8"
    assert repo.meta_get("chapter", chapter_id, "lang") == "en"
    assert repo.meta_get("chapter", chapter_id, "notes") == [1, 2, 3]


def test_upsert_chapter_same_key_updates_in_place(repo):
    novel_id = repo.create_novel("Upsert")
    first = repo.upsert_chapter(novel_id, "1", b"old-body", "raw")
    second = repo.upsert_chapter(novel_id, "1", b"new-body", "zstd_nodict")
    assert second == first  # same chapter id
    assert repo.get_chapter_bytes(second) == b"new-body"
    with repo.backend.txn(write=False) as conn:
        count = conn.execute("SELECT COUNT(*) FROM chapters WHERE novel_id=?", (novel_id,)).fetchone()[0]
    assert count == 1


def test_upsert_chapter_int_and_str_keys_are_equivalent(repo):
    novel_id = repo.create_novel("Keys")
    a = repo.upsert_chapter(novel_id, 7, b"seven", "raw")
    b = repo.upsert_chapter(novel_id, "7", b"seven-v2", "raw")
    assert a == b
    assert repo.get_chapter_bytes(a) == b"seven-v2"


def test_upsert_chapter_missing_novel_raises(repo):
    with pytest.raises(NotFound):
        repo.upsert_chapter(999_999, "1", b"x", "raw")


def test_get_chapter_missing_raises(repo):
    with pytest.raises(MissingContent):
        repo.get_chapter_bytes(999_999)
    with pytest.raises(MissingContent):
        repo.open_chapter(999_999)


def test_meta_entity_id_normalization(repo):
    novel_id = repo.create_novel("Norm")
    repo.meta_set("novel", novel_id, "k", 1)  # int accepted
    with repo.backend.txn(write=False) as conn:
        row = conn.execute(
            "SELECT entity_id FROM meta WHERE entity_type='novel' AND key='k'"
        ).fetchone()
    assert row[0] == str(novel_id)
    # String form of the same id reads the same row.
    assert repo.meta_get("novel", str(novel_id), "k") == 1
    # Non-decimal strings are rejected.
    for bad in ("id-not-decimal", "007", "-1", "1.5", "", "1e3"):
        with pytest.raises(ValueError):
            repo.meta_set("novel", bad, "x", 1)
    # Booleans are not integers here.
    with pytest.raises(ValueError):
        repo.meta_set("novel", True, "x", 1)
    with pytest.raises(ValueError):
        repo.meta_set("novel", 1.5, "x", 1)


def test_meta_entity_type_validation(repo):
    with pytest.raises(ValueError):
        repo.meta_set("chapters", 1, "k", 1)  # typo, not a valid entity type
    with pytest.raises(ValueError):
        repo.meta_get("novelx", 1, "k")


def test_meta_roundtrip_json_values(repo):
    novel_id = repo.create_novel("JSON")
    values = [
        {"nested": {"list": [1, 2, {"x": None}]}},
        [1, "two", 3.0],
        None,
        "text",
        42,
        True,
        "\u00e9\u4e2d\u6587",
    ]
    for i, value in enumerate(values):
        repo.meta_set("novel", novel_id, f"key-{i}", value)
        assert repo.meta_get("novel", novel_id, f"key-{i}") == value
    listed = repo.meta_list("novel", novel_id)
    assert len(listed) == len(values)
    assert listed["key-0"] == values[0]


def test_meta_overwrite_and_empty_list(repo):
    novel_id = repo.create_novel("Overwrite")
    assert repo.meta_list("novel", novel_id) == {}
    repo.meta_set("novel", novel_id, "k", 1)
    repo.meta_set("novel", novel_id, "k", 2)
    assert repo.meta_get("novel", novel_id, "k") == 2
    assert repo.meta_get("novel", novel_id, "missing") is None
    with repo.backend.txn(write=False) as conn:
        count = conn.execute("SELECT COUNT(*) FROM meta WHERE key='k'").fetchone()[0]
    assert count == 1


def test_iter_live_content_matches_chapters(repo):
    n1 = repo.create_novel("Live 1")
    n2 = repo.create_novel("Live 2")
    expected: set[tuple[str, str]] = set()
    for novel_id in (n1, n2):
        for i in range(3):
            body = f"live-{novel_id}-{i}".encode() * 5
            repo.upsert_chapter(novel_id, f"{i:02d}", body, "raw" if i % 2 else "zstd_nodict")
            ref = repo.store.put_bytes(body, profile="raw" if i % 2 else "zstd_nodict").result.ref
            expected.add((ref.blob_key, ref.profile))
    live = {(r.blob_key, r.profile) for r in repo.iter_live_content()}
    assert live == expected
    scoped = {(r.blob_key, r.profile) for r in repo.iter_live_content(scope=n1)}
    assert len(scoped) == 3
    assert scoped <= expected
    with pytest.raises(TypeError):
        list(repo.iter_live_content(scope="not-an-int"))


def test_shared_body_dedupes_across_chapters(repo):
    novel_id = repo.create_novel("Shared")
    body = b"shared-chapter-body" * 20
    a = repo.upsert_chapter(novel_id, "a", body, "raw")
    b = repo.upsert_chapter(novel_id, "b", body, "raw")
    assert repo.get_chapter_bytes(a) == body
    assert repo.get_chapter_bytes(b) == body
    refs = list(repo.iter_live_content())
    assert len(refs) == 1  # one blob_key/profile pair
    with repo.backend.txn(write=False) as conn:
        encodings = conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0]
    assert encodings == 1


def test_upsert_chapter_with_different_profiles_same_body(repo):
    novel_id = repo.create_novel("Profiles")
    body = b"same-body-two-profiles" * 10
    a = repo.upsert_chapter(novel_id, "a", body, "raw")
    b = repo.upsert_chapter(novel_id, "b", body, "zstd_nodict")
    assert repo.get_chapter_bytes(a) == body
    assert repo.get_chapter_bytes(b) == body
    with repo.backend.txn(write=False) as conn:
        count = conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0]
    assert count == 2


def test_chapter_ref_points_at_stored_encoding(repo):
    novel_id = repo.create_novel("Refs")
    body = b"ref-check" * 10
    chapter_id = repo.upsert_chapter(novel_id, "1", body, "zstd_nodict")
    row = repo.backend.get_chapter(chapter_id)
    assert row is not None
    assert row["profile"] == "zstd_nodict"
    live = list(repo.iter_live_content())
    assert live == [ContentRef(blob_key=row["blob_key"], profile=row["profile"])]
    assert repo.store.get_bytes(live[0]) == body
    assert_repo_consistent(repo)
