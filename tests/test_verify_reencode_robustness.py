"""Verify classifier and reencode override-validation tests (review §3, §4)."""

from __future__ import annotations

import pytest

from inkpack import CorruptContent, MissingContent, Profile

from .conftest import enc_row, set_payload

# -- §3 verify robustness -----------------------------------------------------


def test_verify_bad_blob_key_is_corrupt_not_fatal(repo):
    """Decision D: an unparsable blob_key counts as corrupt, never aborts."""
    data = b"abc"
    if repo.backend.mode == "sqlite_single":
        with repo.backend.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id,"
                " stored_len, checksum, shard_id, updated_at) VALUES(?, ?, 'none', '{}', NULL, 3, NULL, NULL, ?)",
                ("not-ikb1:x", "raw", "now"),
            )
            conn.execute(
                "INSERT INTO payload(blob_key, profile, data) VALUES(?, ?, ?)",
                ("not-ikb1:x", "raw", data),
            )
    else:
        with repo.backend.txn(write=True, attach_shard_id=1) as conn:
            conn.execute(
                "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id,"
                " stored_len, checksum, shard_id, updated_at) VALUES(?, ?, 'none', '{}', NULL, 3, NULL, 1, ?)",
                ("not-ikb1:x", "raw", "now"),
            )
            conn.execute(
                "INSERT INTO p.payload(blob_key, profile, data) VALUES(?, ?, ?)",
                ("not-ikb1:x", "raw", data),
            )
    verify = repo.store.verify().result  # must return, not raise
    assert verify.corrupt >= 1
    assert verify.ok == 0


def test_verify_missing_dict_counts_missing_not_corrupt(repo):
    train = repo.store.train_dict([b"verify-dict " * 60] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"verify-dict " * 300, profile="zstd_dict").result
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (train.dict_id,))
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)
    verify = repo.store.verify().result
    assert verify.missing == 1
    assert verify.corrupt == 0
    assert verify.ok == 0


def test_verify_does_not_use_current_profile(repo):
    repo.store.put_bytes(b"profile-independent" * 100, profile="zstd_nodict").result
    repo.set_profile(Profile(name="zstd_nodict", codec="none", params={}))
    verify = repo.store.verify().result
    assert verify.ok == 1 and verify.missing == 0 and verify.corrupt == 0


def test_verify_missing_dict_restored_by_reput(repo):
    train = repo.store.train_dict([b"restore-dict " * 60] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"restore-dict " * 200, profile="zstd_dict").result
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (train.dict_id,))
    # Restoring the same content-addressed dict bytes is enough.
    again = repo.store.train_dict([b"restore-dict " * 60] * 6).result
    assert again.dict_id == train.dict_id
    assert repo.store.get_bytes(put.ref) == b"restore-dict " * 200
    assert repo.store.verify().result.ok == 1


# -- §4 reencode override validation -------------------------------------------


def test_reencode_override_none_with_dict_id_rejected(repo):
    put = repo.store.put_bytes(b"reject-me" * 50, profile="raw").result
    before = enc_row(repo, put.ref)
    with pytest.raises(ValueError, match="zstd_dict_id"):
        repo.store.reencode([put.ref], options={"codec": "none", "zstd_dict_id": "ikd1:dead"}).result
    after = enc_row(repo, put.ref)
    assert after["codec"] == before["codec"]
    assert after["codec_params_json"] == before["codec_params_json"]


def test_reencode_override_none_roundtrip(repo):
    data = b"to-none " * 200
    put = repo.store.put_bytes(data, profile="zstd_nodict").result
    result = repo.store.reencode([put.ref], options={"codec": "none", "params": {}}).result
    assert result.reencoded == 1
    row = enc_row(repo, put.ref)
    assert row["codec"] == "none"
    assert row["zstd_dict_id"] is None
    assert repo.store.get_bytes(put.ref) == data


def test_reencode_override_zstd_missing_dict(repo):
    put = repo.store.put_bytes(b"missing-dict" * 30, profile="raw").result
    before = enc_row(repo, put.ref)
    # Per-target data problems skip instead of aborting (review P1-3).
    result = repo.store.reencode(
        [put.ref], options={"codec": "zstd", "zstd_dict_id": "ikd1:missing"}
    ).result
    assert result.skipped == 1 and result.reencoded == 0
    after = enc_row(repo, put.ref)
    assert after["codec"] == before["codec"]  # no half-update (spec 7.5)
    assert repo.store.get_bytes(put.ref) == b"missing-dict" * 30


def test_reencode_uses_named_profile_policy(repo):
    data = b"named-policy " * 100
    put = repo.store.put_bytes(data, profile="raw").result
    repo.store.reencode([put.ref], options={"profile": "zstd_nodict"}).result
    row = enc_row(repo, put.ref)
    assert row["codec"] == "zstd"
    assert row["codec_params_json"] == '{"level":3}'
    assert row["blob_key"] == put.ref.blob_key  # PK unchanged
    assert repo.store.get_bytes(put.ref) == data


def test_reencode_override_policy_frozen_for_all_targets(repo):
    """§18: the ad-hoc policy is validated once and applied to every target."""
    refs = [
        repo.store.put_bytes(f"frozen-{i} ".encode() * 100, profile="raw").result.ref for i in range(3)
    ]
    repo.store.reencode(refs, options={"codec": "zstd", "params": {"level": 9}}).result
    params = {enc_row(repo, ref)["codec_params_json"] for ref in refs}
    assert params == {'{"level":9}'}


def test_corrupt_payload_still_corruptcontent_not_zstd_error(repo):
    put = repo.store.put_bytes(b"corrupt-class" * 50, profile="zstd_nodict").result
    set_payload(repo, put.ref, b"not a zstd frame")
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)
    assert repo.store.verify().result.corrupt == 1
