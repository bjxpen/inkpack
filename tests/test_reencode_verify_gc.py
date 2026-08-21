"""Reencode / verify / gc semantics (spec 7.5, 10.1, 10.2, decisions D/G)."""

from __future__ import annotations

import pytest

from inkpack import ContentRef, MissingContent, Profile

from .conftest import assert_repo_consistent, enc_row, payload_bytes, set_payload

# -- reencode ----------------------------------------------------------------


def test_reencode_in_place_updates_same_pk_and_roundtrips(repo):
    data = b"reencode-me " * 2000
    put = repo.store.put_bytes(data, profile="zstd_nodict").result
    before = enc_row(repo, put.ref)
    before_payload = payload_bytes(repo, put.ref)
    result = repo.store.reencode([put.ref], options={"codec": "none"}).result
    after = enc_row(repo, put.ref)
    assert result.reencoded == 1 and result.skipped == 0
    assert after["blob_key"] == before["blob_key"]  # same PK
    assert after["profile"] == before["profile"]
    assert after["codec"] == "none"
    assert after["codec_params_json"] == "{}"
    assert after["stored_len"] == len(data)
    assert payload_bytes(repo, put.ref) == data  # in-place payload update
    assert payload_bytes(repo, put.ref) != before_payload
    assert repo.store.get_bytes(put.ref) == data
    assert_repo_consistent(repo)


def test_reencode_does_not_change_blob_key_and_verify_ok(repo):
    data = b"stable-key " * 100
    put = repo.store.put_bytes(data, profile="zstd_nodict").result
    blob_key = put.ref.blob_key
    repo.store.reencode([put.ref], options={"codec": "none"}).result
    repo.store.reencode([put.ref], options={"codec": "zstd", "params": {"level": 9}}).result
    assert enc_row(repo, put.ref)["blob_key"] == blob_key
    verify = repo.store.verify().result
    assert verify.ok == 1 and verify.missing == 0 and verify.corrupt == 0
    assert repo.store.get_bytes(put.ref) == data


def test_reencode_follows_profile_definition_changes(repo):
    put = repo.store.put_bytes(b"profile-driven " * 300, profile="zstd_nodict").result
    repo.set_profile(Profile(name="zstd_nodict", codec="none", params={}))
    result = repo.store.reencode([put.ref]).result
    assert result.reencoded == 1
    assert enc_row(repo, put.ref)["codec"] == "none"
    assert repo.store.get_bytes(put.ref) == b"profile-driven " * 300


def test_reencode_options_profile_override(repo):
    data = b"override-target " * 300
    put = repo.store.put_bytes(data, profile="raw").result
    result = repo.store.reencode([put.ref], options={"profile": "zstd_nodict"}).result
    assert result.reencoded == 1
    row = enc_row(repo, put.ref)
    assert row["codec"] == "zstd"
    assert row["blob_key"] == put.ref.blob_key  # key unchanged
    assert repo.store.get_bytes(put.ref) == data


def test_reencode_skips_missing_targets(repo):
    missing = ContentRef(blob_key="ikb1:5:" + "ab" * 32 + "cd" * 16, profile="raw")
    result = repo.store.reencode([missing]).result
    assert result.targets == 1 and result.reencoded == 0 and result.skipped == 1


def test_reencode_option_validation(repo):
    put = repo.store.put_bytes(b"opts", profile="raw").result
    with pytest.raises(ValueError):
        repo.store.reencode([put.ref], options={"unknown": 1}).result
    with pytest.raises(ValueError):
        repo.store.reencode([put.ref], options={"profile": "raw", "codec": "zstd"}).result
    with pytest.raises(TypeError):
        repo.store.reencode([put.ref], options={"params": "not-a-dict"}).result
    with pytest.raises(KeyError):
        repo.store.reencode([put.ref], options={"profile": "no-such-profile"}).result


def test_reencode_to_dict_profile_and_back(repo):
    train = repo.store.train_dict([b"cycle " * 100] * 5).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=train.dict_id))
    data = b"cycle " * 1000
    put = repo.store.put_bytes(data, profile="raw").result
    repo.store.reencode([put.ref], options={"profile": "zstd_dict"}).result
    assert enc_row(repo, put.ref)["zstd_dict_id"] == train.dict_id
    repo.store.reencode([put.ref], options={"codec": "none"}).result
    assert enc_row(repo, put.ref)["codec"] == "none"
    assert repo.store.get_bytes(put.ref) == data
    assert_repo_consistent(repo)


# -- verify -------------------------------------------------------------------


def test_verify_ok_counts(repo):
    for i in range(7):
        repo.store.put_bytes(f"v{i}".encode() * 10, profile="raw").result
    verify = repo.store.verify().result
    assert verify.checked == 7
    assert verify.ok == 7
    assert verify.missing == 0
    assert verify.corrupt == 0


def test_verify_limit(repo):
    for i in range(5):
        repo.store.put_bytes(f"l{i}".encode(), profile="raw").result
    assert repo.store.verify(limit=2).result.checked == 2
    assert repo.store.verify(limit=0).result.checked == 0
    assert repo.store.verify().result.checked == 5


def test_verify_detects_missing_payload(repo):
    put = repo.store.put_bytes(b"verify-missing", profile="raw").result
    row = enc_row(repo, put.ref)
    repo.backend.clear_payload(put.ref.blob_key, put.ref.profile, row["shard_id"])
    verify = repo.store.verify().result
    assert verify.missing == 1
    assert verify.ok == 0 and verify.corrupt == 0


def test_verify_detects_identity_mismatch(repo):
    put = repo.store.put_bytes(b"verify-corrupt", profile="raw").result
    set_payload(repo, put.ref, b"wrong-bytes")
    verify = repo.store.verify().result
    assert verify.corrupt == 1
    assert verify.ok == 0 and verify.missing == 0


def test_verify_detects_undecodable_payload(repo):
    put = repo.store.put_bytes(b"verify-undecodable" * 10, profile="zstd_nodict").result
    set_payload(repo, put.ref, b"garbage-not-zstd")
    verify = repo.store.verify().result
    assert verify.corrupt == 1
    assert verify.ok == 0 and verify.missing == 0


def test_verify_mixed_counts(repo):
    good = repo.store.put_bytes(b"good", profile="raw").result
    missing = repo.store.put_bytes(b"gone", profile="raw").result
    corrupt = repo.store.put_bytes(b"bad", profile="raw").result
    row = enc_row(repo, missing.ref)
    repo.backend.clear_payload(missing.ref.blob_key, missing.ref.profile, row["shard_id"])
    set_payload(repo, corrupt.ref, b"different")
    verify = repo.store.verify().result
    assert verify.checked == 3
    assert verify.ok == 1 and verify.missing == 1 and verify.corrupt == 1
    assert repo.store.get_bytes(good.ref) == b"good"


# -- gc -----------------------------------------------------------------------


def test_gc_keeps_live_and_deletes_dead(repo):
    keep = repo.store.put_bytes(b"keep", profile="raw").result
    dead = repo.store.put_bytes(b"dead", profile="raw").result
    gc = repo.store.gc(live=[keep.ref]).result
    assert gc.encodings_deleted == 1
    assert gc.payload_rows_deleted == 1
    assert gc.blobs_deleted == 1
    assert repo.store.get_bytes(keep.ref) == b"keep"
    with pytest.raises(MissingContent):
        repo.store.get_bytes(dead.ref)
    assert_repo_consistent(repo)


def test_gc_with_all_live_deletes_nothing(repo):
    stored = {
        repo.store.put_bytes(f"live-{i}".encode(), profile="raw").result.ref: f"live-{i}".encode()
        for i in range(3)
    }
    gc = repo.store.gc(live=stored).result
    assert gc.encodings_deleted == 0
    assert gc.payload_rows_deleted == 0
    assert gc.blobs_deleted == 0
    assert gc.dicts_deleted == 0
    for ref, body in stored.items():
        assert repo.store.get_bytes(ref) == body


def test_gc_live_from_repository_iter_live_content(repo):
    novel_id = repo.create_novel("GC Novel")
    kept = repo.upsert_chapter(novel_id, "1", b"gc-kept", "raw")
    dropped = repo.upsert_chapter(novel_id, "2", b"gc-dropped", "raw")
    refs = list(repo.iter_live_content())
    assert len(refs) == 2
    gc = repo.store.gc(live=refs).result
    assert gc.encodings_deleted == 0
    assert repo.get_chapter_bytes(kept) == b"gc-kept"
    assert repo.get_chapter_bytes(dropped) == b"gc-dropped"
    # Now drop one chapter from the repo and GC: its content must be reclaimed.
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM chapters WHERE id=?", (dropped,))
    refs = list(repo.iter_live_content())
    gc = repo.store.gc(live=refs).result
    assert gc.encodings_deleted == 1
    assert_repo_consistent(repo)


def test_gc_blob_survives_while_other_profile_encoding_live(repo):
    data = b"shared-blob" * 20
    raw_ref = repo.store.put_bytes(data, profile="raw").result.ref
    zstd_ref = repo.store.put_bytes(data, profile="zstd_nodict").result.ref
    gc = repo.store.gc(live=[zstd_ref]).result
    assert gc.encodings_deleted == 1
    assert gc.blobs_deleted == 0  # still referenced by the zstd encoding
    assert repo.store.has_blob(raw_ref.blob_key) is True
    assert repo.store.get_bytes(zstd_ref) == data
    with pytest.raises(MissingContent):
        repo.store.get_bytes(raw_ref)
    assert_repo_consistent(repo)


def test_gc_duplicate_live_refs_are_harmless(repo):
    ref = repo.store.put_bytes(b"dup-live", profile="raw").result.ref
    gc = repo.store.gc(live=[ref, ref, ref]).result
    assert gc.encodings_deleted == 0
    assert repo.store.get_bytes(ref) == b"dup-live"


def test_gc_empty_live_is_full_wipe(repo):
    refs = [repo.store.put_bytes(f"wipe-{i}".encode(), profile="raw").result for i in range(3)]
    gc = repo.store.gc(live=[]).result
    assert gc.encodings_deleted == 3
    assert gc.blobs_deleted == 3
    for r in refs:
        assert repo.store.has_blob(r.ref.blob_key) is False
        with pytest.raises(MissingContent):
            repo.store.get_bytes(r.ref)
    assert_repo_consistent(repo)


def test_gc_after_reencode_dedupes_correctly(repo):
    data = b"gc-reencode" * 200
    ref = repo.store.put_bytes(data, profile="zstd_nodict").result.ref
    repo.store.reencode([ref], options={"codec": "none"}).result
    assert enc_row(repo, ref)["codec"] == "none"
    gc = repo.store.gc(live=[ref]).result
    assert gc.encodings_deleted == 0
    assert repo.store.get_bytes(ref) == data
