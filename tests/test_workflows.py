"""End-to-end workflow tests.

Covers the workflows that matter for a novel library:
- progress events emitted by long operations,
- dictionary building from a filled novel's chapters and recompression,
- a different dictionary per novel (own dict beats foreign dicts),
- random chapter replacement/removal with correct GC + verify,
- zstd-imposed errors surfacing as typed, actionable API errors.
"""

from __future__ import annotations

import io
import random

import pytest

from inkpack import (
    Cancelled,
    ContentRef,
    CorruptContent,
    MissingContent,
    OpEvent,
)

from .conftest import assert_repo_consistent, enc_row

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _chapter_text(rng: random.Random, words: list[str], n: int = 60) -> bytes:
    return " ".join(rng.choice(words) for _ in range(n)).encode()


def _put_profiles(repo, extra: dict[str, dict]) -> None:
    """Merge ad-hoc profile definitions into the repo config."""
    profiles = repo.backend.config_get("profiles")
    profiles.update(extra)
    repo.backend.config_set("profiles", profiles)


def _stored_len(repo, ref: ContentRef) -> int:
    row = enc_row(repo, ref)
    assert row is not None
    return int(row["stored_len"])


def _set_dict_profile(repo, name: str, dict_id: str, level: int = 6) -> None:
    _put_profiles(repo, {name: {"codec": "zstd", "params": {"level": level}, "zstd_dict_id": dict_id}})


def _sum_stored(repo, refs) -> int:
    return sum(_stored_len(repo, ref) for ref in refs)


# ---------------------------------------------------------------------------
# 1) progress events
# ---------------------------------------------------------------------------


def test_progress_events_verify_sequence_and_metrics(repo):
    for i in range(30):
        repo.store.put_bytes(f"prog-{i}".encode() * 20, profile="raw").result
    events = list(repo.store.verify())
    assert events[0].kind == "start" and events[-1].kind == "done"
    assert all(isinstance(e, OpEvent) for e in events)
    items = [e for e in events if e.kind == "item"]
    assert len(items) == 30
    checked = [e.metrics["checked"] for e in items]
    assert checked == list(range(1, 31))  # monotonic progress
    assert events[-1].metrics["checked"] == 30
    assert events[-1].metrics["ok"] == 30


def test_progress_events_reencode_sequence(repo):
    refs = [
        repo.store.put_bytes(f"re-prog-{i}".encode() * 50, profile="zstd_nodict").result.ref
        for i in range(12)
    ]
    events = list(repo.store.reencode(refs, options={"codec": "none"}))
    assert events[0].kind == "start" and events[-1].kind == "done"
    items = [e for e in events if e.kind == "item"]
    assert len(items) == 12
    assert [e.metrics["reencoded"] for e in items] == list(range(1, 13))
    assert events[-1].metrics["reencoded"] == 12
    assert events[-1].metrics["skipped"] == 0


def test_progress_events_train_dict_and_put_stream(repo):
    train_events = list(repo.store.train_dict([b"progress sample " * 30] * 6))
    assert train_events[0].kind == "start" and train_events[-1].kind == "done"
    assert train_events[-1].metrics["dict_size"] > 0
    assert train_events[-1].metrics["samples_used"] == 6

    stream_events = list(repo.store.put_stream(io.BytesIO(b"s" * 4096), profile="raw"))
    assert stream_events[0].kind == "start" and stream_events[-1].kind == "done"
    assert stream_events[0].phase == "stream_hash"
    assert stream_events[-1].metrics["bytes_in"] == 4096


def test_cancel_token_stops_long_progress(repo):
    for i in range(50):
        repo.store.put_bytes(f"cancel-prog-{i}".encode(), profile="raw").result
    calls = {"n": 0}

    def cancel() -> bool:
        calls["n"] += 1
        return calls["n"] > 15

    op = repo.store.verify(cancel=cancel)
    with pytest.raises(Cancelled):
        op.result
    # A fresh run completes cleanly (operation state never corrupts the repo).
    assert repo.store.verify().result.ok == 50


# ---------------------------------------------------------------------------
# 2) dictionary building from a filled novel + recompression
# ---------------------------------------------------------------------------


def test_dict_building_after_novel_chapters_filled(repo):
    rng = random.Random(11)
    words = ["knight", "dragon", "castle", "kingdom", "sword", "scroll", "wizard", "prophecy"]
    novel_id = repo.create_novel("Dict Novel")
    chapter_ids: list[int] = []
    for i in range(12):
        body = _chapter_text(rng, words, n=100)
        chapter_id = repo.upsert_chapter(novel_id, f"{i:04d}", body, "raw")
        chapter_ids.append(chapter_id)
    refs = list(repo.iter_live_content(scope=novel_id))

    # Build a dictionary from the novel's own chapters (as a user would).
    samples = [repo.get_chapter_bytes(cid) for cid in chapter_ids]
    train = repo.store.train_dict(samples).result
    assert train.samples_used == 12
    assert train.sample_bytes == sum(len(s) for s in samples)

    # Point a profile at the new dict and recompress the whole novel.
    _set_dict_profile(repo, "zstd_dict", train.dict_id)
    raw_total = _sum_stored(repo, refs)
    result = repo.store.reencode(refs, options={"profile": "zstd_dict"}).result
    assert result.reencoded == len(refs)
    dict_total = _sum_stored(repo, refs)
    assert dict_total < raw_total, "dictionary recompression must shrink repetitive prose"

    for cid, body in zip(chapter_ids, samples, strict=True):
        assert repo.get_chapter_bytes(cid) == body
    for ref in refs:
        row = enc_row(repo, ref)
        assert row["zstd_dict_id"] == train.dict_id
        assert repo.store.get_bytes(ref) in samples  # every ref decodes to a chapter

    verify = repo.store.verify().result
    assert verify.checked == len(refs) and verify.ok == len(refs)
    gc = repo.store.gc(live=refs).result
    assert gc.encodings_deleted == 0
    assert_repo_consistent(repo)


# ---------------------------------------------------------------------------
# 3) per-novel dictionaries
# ---------------------------------------------------------------------------


def test_per_novel_dictionaries(repo):
    rng = random.Random(23)
    words_a = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    words_b = ["winter", "spring", "summer", "autumn", "frost", "blossom", "harvest", "thaw"]
    novel_a = repo.create_novel("Novel A")
    novel_b = repo.create_novel("Novel B")

    chapters_a = [_chapter_text(rng, words_a, n=120) for _ in range(8)]
    chapters_b = [_chapter_text(rng, words_b, n=120) for _ in range(8)]
    for i, body in enumerate(chapters_a):
        repo.upsert_chapter(novel_a, f"{i:04d}", body, "raw")
    for i, body in enumerate(chapters_b):
        repo.upsert_chapter(novel_b, f"{i:04d}", body, "raw")
    refs_a = list(repo.iter_live_content(scope=novel_a))
    refs_b = list(repo.iter_live_content(scope=novel_b))

    # One dictionary per novel, built from that novel's chapters.
    train_a = repo.store.train_dict(chapters_a).result
    train_b = repo.store.train_dict(chapters_b).result
    assert train_a.dict_id != train_b.dict_id
    _set_dict_profile(repo, "zstd_dict_A", train_a.dict_id)
    _set_dict_profile(repo, "zstd_dict_B", train_b.dict_id)

    # Fresh probes written under each profile: a novel's own dict must beat
    # the other novel's dict for its own prose.
    probe_a = _chapter_text(rng, words_a, n=300)
    probe_b = _chapter_text(rng, words_b, n=300)
    ref_a_probe = repo.store.put_bytes(probe_a, profile="raw").result.ref
    ref_b_probe = repo.store.put_bytes(probe_b, profile="raw").result.ref

    repo.store.reencode([ref_a_probe], options={"profile": "zstd_dict_A"}).result
    repo.store.reencode([ref_b_probe], options={"profile": "zstd_dict_B"}).result
    size_a_with_a = _stored_len(repo, ref_a_probe)
    size_b_with_b = _stored_len(repo, ref_b_probe)

    repo.store.reencode([ref_a_probe], options={"profile": "zstd_dict_B"}).result
    repo.store.reencode([ref_b_probe], options={"profile": "zstd_dict_A"}).result
    size_a_with_b = _stored_len(repo, ref_a_probe)
    size_b_with_a = _stored_len(repo, ref_b_probe)

    assert size_a_with_a < size_a_with_b, "novel A's dict must compress A prose best"
    assert size_b_with_b < size_b_with_a, "novel B's dict must compress B prose best"

    # Everything still round-trips, and decoding uses the stored dict id.
    assert repo.store.get_bytes(ref_a_probe) == probe_a
    assert repo.store.get_bytes(ref_b_probe) == probe_b
    for ref, body in zip(refs_a + refs_b, chapters_a + chapters_b, strict=True):
        assert repo.store.get_bytes(ref) == body
    verify = repo.store.verify().result
    assert verify.checked == verify.ok
    assert_repo_consistent(repo)


# ---------------------------------------------------------------------------
# 4) random chapter replacement / removal
# ---------------------------------------------------------------------------


def test_random_chapter_replacement_and_removal(repo):
    rng = random.Random(42)
    words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit"]
    novel_id = repo.create_novel("Mutable Novel")

    # chapter_id -> order_key, so replacements target the same key/row.
    keys: dict[int, str] = {}
    current: dict[int, bytes] = {}
    for i in range(15):
        body = _chapter_text(rng, words)
        chapter_id = repo.upsert_chapter(novel_id, f"{i:03d}", body, "raw")
        keys[chapter_id] = f"{i:03d}"
        current[chapter_id] = body

    for _ in range(60):
        op = rng.randrange(10)
        if op < 6:
            # Replace a random chapter (same key -> same chapter id).
            chapter_id = rng.choice(list(current))
            body = _chapter_text(rng, words)
            replaced = repo.upsert_chapter(
                novel_id,
                keys[chapter_id],
                body,
                rng.choice(["raw", "zstd_nodict"]),
            )
            assert replaced == chapter_id
            current[chapter_id] = body
        elif op < 8:
            # Remove a chapter (no public delete API; remove the row directly).
            chapter_id = rng.choice(list(current))
            with repo.backend.txn(write=True) as conn:
                conn.execute("DELETE FROM chapters WHERE id=?", (chapter_id,))
            del current[chapter_id]
        else:
            # Read back a random chapter and verify byte fidelity.
            chapter_id = rng.choice(list(current))
            assert repo.get_chapter_bytes(chapter_id) == current[chapter_id]

    # Live content must equal exactly the distinct refs of surviving chapters.
    with repo.backend.txn(write=False) as conn:
        chapter_rows = conn.execute("SELECT blob_key, profile FROM chapters").fetchall()
    expected = {(str(r[0]), str(r[1])) for r in chapter_rows}
    live = {(r.blob_key, r.profile) for r in repo.iter_live_content()}
    assert live == expected
    assert len(live) <= len(current)  # shared bodies dedupe

    # GC reclaims replaced/removed content but keeps every live chapter.
    encodings_before = len(list(repo.backend.iter_encodings()))
    gc = repo.store.gc(live=list(repo.iter_live_content())).result
    assert gc.encodings_deleted == encodings_before - len(live)
    for chapter_id, body in current.items():
        assert repo.get_chapter_bytes(chapter_id) == body
    verify = repo.store.verify().result
    assert verify.ok == len(live) and verify.missing == 0 and verify.corrupt == 0
    assert_repo_consistent(repo)


def test_replaced_chapter_body_becomes_unreadable_after_gc(repo):
    novel_id = repo.create_novel("Replace GC")
    first = repo.upsert_chapter(novel_id, "1", b"old-body-version", "raw")
    old_ref = next(r for r in repo.iter_live_content(scope=novel_id))
    second = repo.upsert_chapter(novel_id, "1", b"new-body-version", "raw")
    assert first == second
    live = list(repo.iter_live_content(scope=novel_id))
    assert len(live) == 1
    assert live[0] != old_ref
    repo.store.gc(live=live).result
    assert repo.store.has_blob(old_ref.blob_key) is False
    with pytest.raises(MissingContent):
        repo.store.get_bytes(old_ref)
    assert repo.get_chapter_bytes(second) == b"new-body-version"
    assert_repo_consistent(repo)


# ---------------------------------------------------------------------------
# 5) recompression round-trips
# ---------------------------------------------------------------------------


def test_recompress_workflow_raw_to_dict_and_back(repo):
    rng = random.Random(5)
    words = ["forest", "river", "mountain", "valley", "stone", "bridge", "village", "road"]
    novel_id = repo.create_novel("Recompress")
    for i in range(8):
        repo.upsert_chapter(novel_id, f"{i:04d}", _chapter_text(rng, words), "raw")
    refs = list(repo.iter_live_content(scope=novel_id))

    raw_total = _sum_stored(repo, refs)

    train = repo.store.train_dict([repo.store.get_bytes(r) for r in refs]).result
    _set_dict_profile(repo, "zstd_dict", train.dict_id)
    repo.store.reencode(refs, options={"profile": "zstd_dict"}).result
    dict_total = _sum_stored(repo, refs)
    assert dict_total < raw_total
    for ref in refs:
        assert enc_row(repo, ref)["zstd_dict_id"] == train.dict_id

    # Recompress back to raw: sizes must return to the raw baseline.
    repo.store.reencode(refs, options={"codec": "none"}).result
    assert _sum_stored(repo, refs) == raw_total
    for ref in refs:
        assert enc_row(repo, ref)["codec"] == "none"
        assert enc_row(repo, ref)["zstd_dict_id"] is None

    verify = repo.store.verify().result
    assert verify.ok == len(refs) and verify.missing == 0 and verify.corrupt == 0
    repo.store.compact().result
    assert_repo_consistent(repo)


# ---------------------------------------------------------------------------
# 6) zstd-imposed errors surface as typed, actionable API errors
# ---------------------------------------------------------------------------


def test_train_dict_too_few_samples_guides_user(repo):
    with pytest.raises(ValueError) as exc:
        repo.store.train_dict([b"only two samples"] * 2).result
    message = str(exc.value)
    assert "5" in message and "samples" in message


def test_train_dict_too_little_sample_data(repo):
    with pytest.raises(ValueError) as exc:
        repo.store.train_dict([b"a"] * 5).result
    assert "8 bytes" in str(exc.value)


def test_train_dict_bad_dict_size_option(repo):
    with pytest.raises(ValueError) as exc:
        repo.store.train_dict([b"x" * 100] * 5, options={"dict_size": 100}).result
    assert "dict_size" in str(exc.value)
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x" * 100] * 5, options={"dict_size": "big"}).result


def test_train_dict_unknown_option(repo):
    with pytest.raises(ValueError) as exc:
        repo.store.train_dict([b"x" * 100] * 5, options={"bogus": 1}).result
    assert "bogus" in str(exc.value)


def test_put_with_invalid_zstd_level_is_typed_error(repo):
    _put_profiles(repo, {"bad_level": {"codec": "zstd", "params": {"level": 99}, "zstd_dict_id": None}})
    with pytest.raises(ValueError) as exc:
        repo.store.put_bytes(b"x" * 100, profile="bad_level").result
    assert "level" in str(exc.value)
    _put_profiles(repo, {"str_level": {"codec": "zstd", "params": {"level": "3"}, "zstd_dict_id": None}})
    with pytest.raises(ValueError):
        repo.store.put_bytes(b"x" * 100, profile="str_level").result


def test_reencode_with_invalid_zstd_level_is_typed_error(repo):
    put = repo.store.put_bytes(b"y" * 100, profile="raw").result
    with pytest.raises(ValueError) as exc:
        repo.store.reencode([put.ref], options={"codec": "zstd", "params": {"level": 99}}).result
    assert "level" in str(exc.value)


def test_put_with_unsupported_codec_is_typed_error(repo):
    _put_profiles(repo, {"lzma": {"codec": "lzma", "params": {}, "zstd_dict_id": None}})
    with pytest.raises(ValueError) as exc:
        repo.store.put_bytes(b"x", profile="lzma").result
    assert "unsupported codec" in str(exc.value)


def test_decode_failure_is_corruptcontent_not_raw_zstd(repo):
    put = repo.store.put_bytes(b"corrupt-me " * 100, profile="zstd_nodict").result
    from .conftest import set_payload

    set_payload(repo, put.ref, b"this is not a zstd frame at all")
    with pytest.raises(CorruptContent) as exc:
        repo.store.get_bytes(put.ref)
    assert "zstd" in str(exc.value).lower() or "decode" in str(exc.value).lower()
    # verify() classifies it as corrupt, not ok/missing.
    verify = repo.store.verify().result
    assert verify.corrupt == 1 and verify.ok == 0


def test_missing_dictionary_is_missingcontent(repo):
    _put_profiles(repo, {"ghost_dict": {"codec": "zstd", "params": {}, "zstd_dict_id": "ikd1:nope"}})
    with pytest.raises(MissingContent) as exc:
        repo.store.put_bytes(b"x" * 100, profile="ghost_dict").result
    assert "dictionary" in str(exc.value)
