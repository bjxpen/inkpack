"""Misc contract tests (review §21 progress, §22 search escape, §23
UnknownProfile, §24 operation reuse, §27 constants/typing, §28 OpKind)."""

from __future__ import annotations

import io
from typing import get_args

import pytest

from inkpack import (
    MissingContent,
    NotFound,
    OpEvent,
    Profile,
)
from inkpack.types import OpKind, UnknownProfile

# -- §21 progress events --------------------------------------------------------


def test_put_stream_emits_progress_for_large_payload(repo, monkeypatch):
    import inkpack.blobstore as bs

    monkeypatch.setattr(bs, "PROGRESS_INTERVAL", 1024)
    events = list(repo.store.put_stream(io.BytesIO(b"x" * 8192), profile="raw"))
    progress = [e for e in events if e.kind == "progress"]
    assert progress, "large streams must emit progress events"
    bytes_in = [e.metrics["bytes_in"] for e in progress]
    assert bytes_in == sorted(bytes_in)  # monotonic
    done = [e for e in events if e.kind == "done"][-1]
    assert done.metrics["bytes_in"] == 8192
    assert done.metrics["bytes_out"] == 8192


def test_put_bytes_small_has_start_and_done_no_progress(repo):
    events = list(repo.store.put_bytes(b"tiny", profile="raw"))
    assert events[0].kind == "start"
    assert events[-1].kind == "done"
    assert all(e.kind != "progress" for e in events)


def test_progress_events_are_opevent(repo, monkeypatch):
    import inkpack.blobstore as bs

    monkeypatch.setattr(bs, "PROGRESS_INTERVAL", 1)
    events = list(repo.store.put_stream(io.BytesIO(b"z" * 2048), profile="raw"))
    assert all(isinstance(e, OpEvent) for e in events)
    assert any(e.kind == "progress" for e in events)


# -- §22 search escaping ----------------------------------------------------------


@pytest.fixture
def search_repo(repo):
    for title in ("100% real", "100 real", "a_b", "ab"):
        repo.create_novel(title)
    return repo


def test_search_escapes_percent(search_repo):
    titles = [n["title"] for n in search_repo.list_novels(search="100%")]
    assert titles == ["100% real"]


def test_search_plain_substring_matches_both(search_repo):
    titles = [n["title"] for n in search_repo.list_novels(search="100")]
    assert set(titles) == {"100% real", "100 real"}


def test_search_escapes_underscore(search_repo):
    assert [n["title"] for n in search_repo.list_novels(search="a_b")] == ["a_b"]
    assert [n["title"] for n in search_repo.list_novels(search="ab")] == ["ab"]


def test_search_empty_returns_all_desc(search_repo):
    titles = [n["title"] for n in search_repo.list_novels(search="")]
    assert titles == ["ab", "a_b", "100 real", "100% real"]  # ORDER BY id DESC


def test_search_lone_wildcards_do_not_raise(search_repo):
    for needle in ("\\", "%", "_", "%_\\"):
        search_repo.list_novels(search=needle)  # no exception


# -- §23 UnknownProfile ------------------------------------------------------------


def test_put_unknown_profile_raises_unknownprofile(repo):
    with pytest.raises(UnknownProfile) as exc:
        repo.store.put_bytes(b"x", profile="nope").result
    assert "nope" in str(exc.value)
    # Both catch-styles work.
    with pytest.raises(KeyError):
        repo.store.put_bytes(b"x", profile="nope").result
    with pytest.raises(NotFound):
        repo.store.put_bytes(b"x", profile="nope").result


def test_get_profile_unknown_raises_unknownprofile(repo):
    with pytest.raises(UnknownProfile):
        repo.get_profile("nope")


def test_reencode_unknown_profile_option(repo):
    put = repo.store.put_bytes(b"x" * 10, profile="raw").result
    with pytest.raises(UnknownProfile):
        repo.store.reencode([put.ref], options={"profile": "nope"}).result


def test_unknownprofile_is_typed_error():
    assert issubclass(UnknownProfile, NotFound)
    assert issubclass(UnknownProfile, KeyError)


# -- §24 operation reuse -----------------------------------------------------------


def test_failed_op_result_reraises_same_exception(repo):
    op = repo.store.put_bytes(b"x", profile="no-such-profile")
    with pytest.raises(UnknownProfile) as first:
        list(op)
    with pytest.raises(UnknownProfile) as second:
        op.result
    assert first.value is second.value


def test_reiterate_keeps_result(repo):
    op = repo.store.put_bytes(b"reuse", profile="raw")
    first = list(op)
    assert first
    assert list(op) == []  # second iteration yields nothing
    assert op.result.raw_len == 5


# -- §27 shared constants + Operation typing ----------------------------------------


def test_chunk_size_single_source():
    from inkpack import blobstore
    from inkpack.codec import CHUNK_SIZE

    assert blobstore.CHUNK_SIZE == CHUNK_SIZE == 128 * 1024


def test_operation_result_typed_putresult(repo):
    op = repo.store.put_bytes(b"typed", profile="raw")
    result = op.result
    # Static check: mypy/pyright assert the inferred type; runtime sanity here.
    assert result.ref.profile == "raw"


# -- §28 OpKind keeps the spec union ------------------------------------------------


def test_opevent_accepts_error_kind():
    event = OpEvent(kind="error", op="put", message="boom")
    assert event.kind == "error"
    assert "error" in get_args(OpKind)
    assert set(get_args(OpKind)) == {"start", "phase", "progress", "item", "log", "error", "done"}


# -- §5 missing dict classification is MissingContent (behavior lock) -------------


def test_missing_dict_is_missingcontent_not_corrupt(repo):
    train = repo.store.train_dict([b"class-lock " * 60] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"class-lock " * 200, profile="zstd_dict").result
    from .conftest import delete_dict

    delete_dict(repo, train.dict_id)
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)
    with pytest.raises(MissingContent):
        repo.store.open(put.ref)
    novel = repo.create_novel("N")
    chapter = repo.upsert_chapter(novel, "1", b"class-lock " * 200, "zstd_dict")
    with pytest.raises(MissingContent):
        repo.get_chapter_bytes(chapter)
    verify = repo.store.verify().result
    assert verify.missing == 1 and verify.corrupt == 0
