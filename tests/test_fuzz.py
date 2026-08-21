"""Fuzz and property tests: many fake novels + KV metadata, random op sequences."""

from __future__ import annotations

import io
import os
import random

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from inkpack import ContentRef, Profile

from .conftest import assert_repo_consistent, make_repo

PROFILES = ("raw", "zstd_nodict")
GENRES = ("fantasy", "scifi", "romance", "mystery", "horror")


def _random_body(rng: random.Random, max_size: int) -> bytes:
    size = rng.randrange(max_size + 1)
    kind = rng.randrange(4)
    if kind == 0:
        return os.urandom(size)
    if kind == 1:
        return bytes(rng.randrange(256) for _ in range(size))
    if kind == 2:
        return b"".join(f"word{i} ".encode() for i in range(size // 5 + 1))[:size]
    base = "chapter text with \x00 binary \xff and unicode \u00e9 \u4e2d\u6587 ".encode()
    return (base * (size // len(base) + 1))[:size]


def _random_meta(rng: random.Random) -> dict:
    return {
        "ix": rng.randrange(1 << 30),
        "tags": [f"tag-{rng.randrange(20)}" for _ in range(rng.randrange(4))],
        "rating": rng.choice([None, rng.random(), rng.randrange(10)]),
        "text": "".join(rng.choice("abcxyz\u00e9\u4e2d") for _ in range(rng.randrange(20))),
    }


# -- many novels + metadata KV (the core fuzzy scenario) -----------------------


def test_fuzz_many_novels_and_metadata_kv(repo):
    """Create many fake novels and chapters with random bytes + KV metadata."""
    rng = random.Random(20260821)
    novel_ids = []
    for i in range(25):
        novel_id = repo.create_novel(f"Fuzz Novel {i}", meta=_random_meta(rng))
        repo.meta_set("novel", novel_id, "genre", rng.choice(GENRES))
        novel_ids.append(novel_id)

    bodies: dict[int, bytes] = {}
    chapter_refs: dict[int, ContentRef] = {}
    for novel_id in novel_ids:
        for chapter in range(rng.randrange(1, 6)):
            body = _random_body(rng, rng.choice([0, 1, 64, 4096, 60_000]))
            profile = rng.choice(PROFILES)
            chapter_id = repo.upsert_chapter(
                novel_id,
                f"{chapter:04d}",
                body,
                profile,
                hints={"media_type": "text/plain", "charset": rng.choice(["utf-8", None])},
                meta={"words": rng.randrange(100_000), "ch": chapter},
            )
            repo.meta_set("chapter", chapter_id, "score", rng.randrange(0, 11))
            bodies[chapter_id] = body
            chapter_refs[chapter_id] = ContentRef(
                blob_key=repo.store.put_bytes(body, profile=profile).result.ref.blob_key, profile=profile
            )

    # Every chapter round-trips byte-for-byte.
    for chapter_id, body in bodies.items():
        assert repo.get_chapter_bytes(chapter_id) == body
        assert repo.open_chapter(chapter_id).read() == body

    # Live content == the distinct refs behind chapters (bodies may dedupe,
    # so distinct refs can be fewer than chapters).
    live = set(repo.iter_live_content())
    expected = set(chapter_refs.values())
    assert live == expected
    assert len(live) == len(expected)

    # Full verify: everything ok.
    verify = repo.store.verify().result
    assert verify.checked == len(live)
    assert verify.ok == len(live)
    assert verify.missing == 0 and verify.corrupt == 0

    # GC with all live refs must delete nothing.
    gc = repo.store.gc(live=live).result
    assert gc.encodings_deleted == 0 and gc.blobs_deleted == 0

    # Search + scoped live refs.
    assert len(repo.list_novels(search="Fuzz Novel 12")) == 1
    assert repo.list_novels(search="absent") == []
    scoped = {r for r in repo.iter_live_content(scope=novel_ids[0])}
    assert scoped <= live and scoped

    # Metadata survived on both entity types.
    for novel_id in novel_ids[:5]:
        meta = repo.meta_list("novel", novel_id)
        assert "genre" in meta and "ix" in meta
    assert_repo_consistent(repo)


# -- randomized operation sequences -------------------------------------------


def test_fuzz_random_operation_sequence(repo):
    """Random put/get/verify/gc/reencode/train/compact sequences, both backends."""
    rng = random.Random(0xF00D)
    stored: dict[ContentRef, bytes] = {}
    dict_ids: list[str] = []
    actions = ["put", "get", "verify", "gc", "reencode", "train", "compact", "open", "has_blob"]
    weights = [34, 16, 10, 10, 10, 6, 4, 6, 4]

    for _ in range(120):
        action = rng.choices(actions, weights=weights, k=1)[0]
        if action == "put":
            profile = rng.choice(PROFILES)
            if dict_ids and rng.random() < 0.3:
                profile = "zstd_dict"
                repo.set_profile(
                    Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=rng.choice(dict_ids))
                )
            body = _random_body(rng, rng.randrange(0, 20_000))
            if rng.random() < 0.3:
                result = repo.store.put_stream(io.BytesIO(body), profile=profile).result
            else:
                result = repo.store.put_bytes(body, profile=profile).result
            stored[result.ref] = body
        elif action == "get" and stored:
            ref = rng.choice(list(stored))
            assert repo.store.get_bytes(ref) == stored[ref]
        elif action == "open" and stored:
            ref = rng.choice(list(stored))
            assert repo.store.open(ref).read() == stored[ref]
        elif action == "has_blob" and stored:
            ref = rng.choice(list(stored))
            assert repo.store.has_blob(ref.blob_key) is True
        elif action == "verify":
            verify = repo.store.verify().result
            assert verify.missing == 0 and verify.corrupt == 0
        elif action == "gc" and stored:
            live = rng.sample(list(stored), rng.randrange(len(stored) + 1))
            repo.store.gc(live=live).result
            # GC deletes unreferenced dictionaries; drop ids it reclaimed so
            # the zstd_dict profile never points at a missing dict.
            with repo.backend.txn(write=False) as conn:
                existing = {r[0] for r in conn.execute("SELECT dict_id FROM dicts")}
            dict_ids[:] = [d for d in dict_ids if d in existing]
            live_set = set(live)
            for ref in live_set:
                assert repo.store.get_bytes(ref) == stored[ref]
            stored = {ref: body for ref, body in stored.items() if ref in live_set}
        elif action == "reencode" and stored:
            targets = rng.sample(list(stored), min(rng.randrange(1, 6), len(stored)))
            repo.store.reencode(targets).result
            for ref in targets:
                assert repo.store.get_bytes(ref) == stored[ref]
        elif action == "train":
            if stored and len(stored) >= 5:
                samples = rng.sample(list(stored.values()), 5)
            else:
                samples = [b"default sample " * 20] * 5
            train = repo.store.train_dict(samples).result
            dict_ids.append(train.dict_id)
        elif action == "compact":
            repo.store.compact().result

    # Final state: everything still readable, verifiable, consistent.
    for ref, body in stored.items():
        assert repo.store.get_bytes(ref) == body
    verify = repo.store.verify().result
    assert verify.checked == len(stored)
    assert verify.ok == len(stored)
    assert verify.missing == 0 and verify.corrupt == 0
    assert_repo_consistent(repo)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_fuzz_repo_flow_with_novels_and_gc(tmp_path, seed):
    """Full lifecycle: novels -> chapters -> reencode -> partial gc -> verify."""
    rng = random.Random(seed)
    repo = make_repo(tmp_path, "sqlite_single")
    novel_id = repo.create_novel("Lifecycle")
    for i in range(20):
        body = _random_body(rng, 30_000)
        repo.upsert_chapter(novel_id, f"{i:04d}", body, rng.choice(PROFILES))
    # Capture the bytes behind every live ref before mutating anything.
    bodies = {ref: repo.store.get_bytes(ref) for ref in repo.iter_live_content()}
    live_refs = set(bodies)
    assert len(live_refs) <= 20
    repo.store.reencode(list(live_refs), options={"codec": "zstd", "params": {"level": 1}}).result
    survivors = set(rng.sample(sorted(live_refs, key=lambda r: r.blob_key), 8))
    repo.store.gc(live=survivors).result
    for ref in survivors:
        assert repo.store.get_bytes(ref) == bodies[ref]
    verify = repo.store.verify().result
    assert verify.ok == len(survivors)
    assert verify.missing == 0 and verify.corrupt == 0
    assert_repo_consistent(repo)


# -- hypothesis property tests ------------------------------------------------


@given(data=st.binary(max_size=200_000))
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_hypothesis_roundtrip_both_profiles(repo_single, data):
    for profile in PROFILES:
        result = repo_single.store.put_bytes(data, profile=profile).result
        assert repo_single.store.get_bytes(result.ref) == data


@given(data=st.binary(max_size=200_000))
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_hypothesis_stream_roundtrip(repo_single, data):
    result = repo_single.store.put_stream(io.BytesIO(data), profile="zstd_nodict").result
    assert repo_single.store.get_bytes(result.ref) == data


@given(data=st.binary(max_size=50_000))
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_hypothesis_dedupe_single_encoding(repo_single, data):
    a = repo_single.store.put_bytes(data, profile="raw").result
    b = repo_single.store.put_bytes(data, profile="raw").result
    assert a.ref.blob_key == b.ref.blob_key
    with repo_single.backend.txn(write=False) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM encodings WHERE blob_key=? AND profile=?",
            (a.ref.blob_key, "raw"),
        ).fetchone()[0]
    assert count == 1
    assert repo_single.store.get_bytes(a.ref) == data


@given(samples=st.lists(st.binary(min_size=2, max_size=2_000), min_size=5, max_size=8))
@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_hypothesis_train_dict_roundtrip(repo_single, samples):
    train = repo_single.store.train_dict(samples).result
    assert train.dict_size > 0
    assert train.samples_used == len(samples)
    repo_single.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=train.dict_id))
    probe = samples[0] * 3
    put = repo_single.store.put_bytes(probe, profile="zstd_dict").result
    assert put.zstd_dict_id is not None  # may be a dedupe hit from an earlier example
    assert repo_single.store.get_bytes(put.ref) == probe
