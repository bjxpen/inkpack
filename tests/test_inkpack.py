from __future__ import annotations

import io
import os
import random
import sqlite3

import pytest

from inkpack import ContentRef, CorruptContent, MissingContent, Profile, create_repo, open_repo


@pytest.fixture(params=["sqlite_single", "sqlite_sharded"])
def repo(tmp_path, request):
    mode = request.param
    profiles = {
        "raw": Profile(name="raw", codec="none", params={}),
        "zstd_nodict": Profile(name="zstd_nodict", codec="zstd", params={"level": 3}),
    }
    r = create_repo(
        path=tmp_path / mode,
        backend_mode=mode,
        profiles=profiles,
        shard_cap_bytes=2 << 20,
        shard_min_bytes=1 << 20,
        verify_on_read=False,
    )
    return r


def _enc_row(repo, ref: ContentRef):
    with repo.backend.txn(write=False) as conn:
        row = conn.execute(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?", (ref.blob_key, ref.profile)
        ).fetchone()
    return row


def test_operation_contract_and_roundtrip(repo):
    op = repo.store.put_bytes(b"abc\x00\xff", profile="raw")
    events = list(op)
    assert events
    assert op.result.ref.blob_key.startswith("ikb1:")
    data = repo.store.get_bytes(op.result.ref)
    assert data == b"abc\x00\xff"


def test_put_dedupe_same_bytes_same_blob_key(repo):
    r1 = repo.store.put_bytes(b"same-data", profile="raw").result
    r2 = repo.store.put_bytes(b"same-data", profile="raw").result
    assert r1.ref.blob_key == r2.ref.blob_key

    with repo.backend.txn(write=False) as conn:
        c1 = conn.execute("SELECT COUNT(*) FROM blobs WHERE blob_key=?", (r1.ref.blob_key,)).fetchone()[0]
        c2 = conn.execute(
            "SELECT COUNT(*) FROM encodings WHERE blob_key=? AND profile=?", (r1.ref.blob_key, "raw")
        ).fetchone()[0]
    assert c1 == 1
    assert c2 == 1


def test_decode_uses_stored_metadata_not_profile(repo):
    put = repo.store.put_bytes(b"hello world" * 100, profile="zstd_nodict").result
    profiles = repo.backend.config_get("profiles")
    profiles["zstd_nodict"] = {"codec": "none", "params": {}, "zstd_dict_id": None}
    repo.backend.config_set("profiles", profiles)
    got = repo.store.get_bytes(put.ref)
    assert got == b"hello world" * 100


def test_get_bytes_corrupt_payload_raises(repo):
    put = repo.store.put_bytes(b"payload-corrupt", profile="zstd_nodict").result
    row = _enc_row(repo, put.ref)
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=True) as conn:
            conn.execute(
                "UPDATE payload SET data=? WHERE blob_key=? AND profile=?",
                (b"not-valid-compressed-bytes", put.ref.blob_key, put.ref.profile),
            )
    else:
        with repo.backend.txn(write=True, attach_shard_id=row["shard_id"]) as conn:
            conn.execute(
                "UPDATE p.payload SET data=? WHERE blob_key=? AND profile=?",
                (b"not-valid-compressed-bytes", put.ref.blob_key, put.ref.profile),
            )
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)


def test_missing_payload_is_missingcontent(repo):
    put = repo.store.put_bytes(b"gone", profile="raw").result
    row = _enc_row(repo, put.ref)
    repo.backend.clear_payload(put.ref.blob_key, put.ref.profile, row["shard_id"])
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)


def test_open_is_stable_materialized(repo):
    put = repo.store.put_bytes(b"stable-open" * 30, profile="raw").result
    fh = repo.store.open(put.ref)
    _ = repo.store.gc(live=[]).result
    assert fh.read() == b"stable-open" * 30


def test_put_stream_non_seekable(repo):
    class NonSeek(io.BytesIO):
        def seek(self, *args, **kwargs):  # type: ignore[override]
            raise OSError("no seek")

        def tell(self):  # type: ignore[override]
            raise OSError("no tell")

    stream = NonSeek(b"stream-data\x00\xff" * 100)
    result = repo.store.put_stream(stream, profile="raw").result
    assert repo.store.get_bytes(result.ref) == b"stream-data\x00\xff" * 100


def test_verify_counts_and_identity_mismatch(repo):
    a = repo.store.put_bytes(b"A", profile="raw").result
    b = repo.store.put_bytes(b"B", profile="raw").result
    first = repo.store.verify().result
    assert first.checked >= 2
    assert first.ok >= 2

    row = _enc_row(repo, a.ref)
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=True) as conn:
            conn.execute(
                "UPDATE payload SET data=? WHERE blob_key=? AND profile=?",
                (b"X", a.ref.blob_key, a.ref.profile),
            )
    else:
        with repo.backend.txn(write=True, attach_shard_id=row["shard_id"]) as conn:
            conn.execute(
                "UPDATE p.payload SET data=? WHERE blob_key=? AND profile=?",
                (b"X", a.ref.blob_key, a.ref.profile),
            )
    v = repo.store.verify().result
    assert v.corrupt >= 1


def test_train_dict_put_with_dict_and_gc_dict_cleanup(repo):
    samples = [b"word " * 200, b"another sample " * 100]
    tr = repo.store.train_dict(samples).result
    profiles = repo.backend.config_get("profiles")
    profiles["zstd_dict"] = {"codec": "zstd", "params": {"level": 6}, "zstd_dict_id": tr.dict_id}
    repo.backend.config_set("profiles", profiles)

    put = repo.store.put_bytes(b"word " * 2000, profile="zstd_dict").result
    assert put.zstd_dict_id == tr.dict_id
    assert repo.store.get_bytes(put.ref) == b"word " * 2000

    gc = repo.store.gc(live=[]).result
    assert gc.encodings_deleted >= 1
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT 1 FROM dicts WHERE dict_id=?", (tr.dict_id,)).fetchone()
    assert row is None


def test_reencode_in_place_and_roundtrip(repo):
    put = repo.store.put_bytes(b"reencode-me" * 200, profile="zstd_nodict").result
    before = _enc_row(repo, put.ref)
    profiles = repo.backend.config_get("profiles")
    profiles["zstd_nodict"] = {"codec": "zstd", "params": {"level": 9}, "zstd_dict_id": None}
    repo.backend.config_set("profiles", profiles)
    rr = repo.store.reencode([put.ref]).result
    after = _enc_row(repo, put.ref)
    assert rr.reencoded == 1
    assert after["blob_key"] == before["blob_key"]
    assert after["profile"] == before["profile"]
    assert repo.store.get_bytes(put.ref) == b"reencode-me" * 200


def test_gc_keeps_live_and_deletes_dead(repo):
    keep = repo.store.put_bytes(b"keep", profile="raw").result.ref
    dead = repo.store.put_bytes(b"dead", profile="raw").result.ref
    _ = repo.store.gc(live=[keep]).result
    assert repo.store.get_bytes(keep) == b"keep"
    with pytest.raises(MissingContent):
        repo.store.get_bytes(dead)


def test_compact_and_has_blob_semantics(repo):
    put = repo.store.put_bytes(b"blob-present", profile="raw").result
    assert repo.store.has_blob(put.ref.blob_key) is True
    comp = repo.store.compact().result
    assert comp.mode
    assert comp.targets
    _ = repo.store.gc(live=[]).result
    assert repo.store.has_blob(put.ref.blob_key) is False


def test_repository_model_meta_and_live_content(repo):
    novel_id = repo.create_novel("Novel 1", meta={"genre": "fantasy"})
    chapter_id = repo.upsert_chapter(novel_id, "001", b"chapter-body", "raw", meta={"lang": "en"})
    assert repo.get_chapter_bytes(chapter_id) == b"chapter-body"

    repo.meta_set("novel", novel_id, "rating", 5)
    assert repo.meta_get("novel", novel_id, "rating") == 5
    all_meta = repo.meta_list("novel", novel_id)
    assert "rating" in all_meta
    assert list(repo.iter_live_content())

    with pytest.raises(ValueError):
        repo.meta_set("novel", "id-not-decimal", "x", 1)


def test_identity_policy_validation(tmp_path):
    profiles = {"raw": Profile(name="raw", codec="none", params={})}
    repo = create_repo(path=tmp_path / "single", backend_mode="sqlite_single", profiles=profiles)
    repo.backend.config_set("identity_policy", "other")
    with pytest.raises(Exception):
        open_repo(tmp_path / "single")


@pytest.mark.parametrize("count", [20])
def test_fuzzy_many_novels_and_metadata(repo, count):
    refs = []
    for i in range(count):
        nid = repo.create_novel(f"Novel {i}")
        for c in range(3):
            body = (f"chapter-{i}-{c}-".encode() + os.urandom(64))
            cid = repo.upsert_chapter(nid, f"{c:03d}", body, "raw", meta={"ix": i, "c": c})
            refs.append((cid, body))
            repo.meta_set("chapter", cid, "score", random.randint(1, 5))
    for cid, body in refs[:10]:
        assert repo.get_chapter_bytes(cid) == body
    verify = repo.store.verify().result
    assert verify.checked >= count
