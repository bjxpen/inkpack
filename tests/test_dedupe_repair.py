"""Decision J (raw_len), decision E dedupe-hit semantics and payload repair
tests (review §6, §7)."""

from __future__ import annotations

import pytest

from inkpack import CorruptContent, MissingContent, Profile, create_repo

from .conftest import assert_repo_consistent, delete_payload_row, enc_row, make_profiles, payload_bytes


def test_new_blob_raw_len_matches_key(repo):
    data = b"raw-len-key" * 7
    put = repo.store.put_bytes(data, profile="raw").result
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT raw_len, created_at FROM blobs WHERE blob_key=?", (put.ref.blob_key,)).fetchone()
    assert row is not None
    assert row["raw_len"] == len(data) == int(put.ref.blob_key.split(":")[1])


def test_dedupe_hit_does_not_change_raw_len_or_created_at(repo):
    data = b"stable" * 100
    first = repo.store.put_bytes(data, profile="raw").result
    with repo.backend.txn(write=False) as conn:
        before = conn.execute(
            "SELECT raw_len, created_at FROM blobs WHERE blob_key=?", (first.ref.blob_key,)
        ).fetchone()
    repo.store.put_bytes(data, profile="raw").result
    with repo.backend.txn(write=False) as conn:
        after = conn.execute(
            "SELECT raw_len, created_at FROM blobs WHERE blob_key=?", (first.ref.blob_key,)
        ).fetchone()
    assert tuple(before) == tuple(after)


def test_blob_raw_len_mismatch_is_corrupt(repo):
    data = b"tamper-raw-len" * 10
    repo.store.put_bytes(data, profile="raw").result
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE blobs SET raw_len = raw_len + 1 WHERE blob_key IN "
            "(SELECT blob_key FROM blobs LIMIT 1)"
        )
    # A write for a *new* profile of the same key hits ensure_blob -> CorruptContent.
    with pytest.raises(CorruptContent):
        repo.store.put_bytes(data, profile="zstd_nodict").result


def test_identity_len_disagrees_with_bytes_raises_before_sql(tmp_path):
    from inkpack.codec import IKB1Identity

    class BrokenIdentity(IKB1Identity):
        def key_bytes(self, data: bytes):
            return "ikb1:1:" + "a" * 64 + ":" + "b" * 32, 1, "a" * 64, "b" * 32

    repo = create_repo(
        tmp_path / "broken-id",
        backend_mode="sqlite_single",
        profiles=make_profiles(),
        identity=BrokenIdentity(),
    )
    with pytest.raises(CorruptContent):
        repo.store.put_bytes(b"four-bytes", profile="raw").result
    with repo.backend.txn(write=False) as conn:
        assert conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0] == 0


# -- §7 dedupe-hit semantics ---------------------------------------------------


def test_dedupe_hit_does_not_rewrite(repo):
    data = b"no-rewrite " * 200
    first = repo.store.put_bytes(data, profile="zstd_nodict").result
    row = enc_row(repo, first.ref)
    payload_before = payload_bytes(repo, first.ref)
    updated_before = row["updated_at"]
    params_before = row["codec_params_json"]

    # Change the live profile definition: a hit must still report the stored row.
    repo.set_profile(Profile(name="zstd_nodict", codec="none", params={}))
    second = repo.store.put_bytes(data, profile="zstd_nodict").result

    assert second.codec == "zstd"  # stored metadata, not the mutated profile
    row_after = enc_row(repo, first.ref)
    assert row_after["codec_params_json"] == params_before
    assert row_after["updated_at"] == updated_before
    assert payload_bytes(repo, first.ref) == payload_before


def test_dedupe_hit_repairs_missing_payload(repo):
    data = b"repair-me " * 200
    first = repo.store.put_bytes(data, profile="zstd_nodict").result
    delete_payload_row(repo, first.ref)
    with pytest.raises(MissingContent):
        repo.store.get_bytes(first.ref)

    # Change the profile definition before the repair-put: repair must use the
    # STORED policy (zstd level 3), not the current one (none).
    repo.set_profile(Profile(name="zstd_nodict", codec="none", params={}))
    second = repo.store.put_bytes(data, profile="zstd_nodict").result

    assert repo.store.get_bytes(first.ref) == data
    row = enc_row(repo, first.ref)
    assert row["codec"] == "zstd"  # stored policy preserved
    assert row["codec_params_json"] == '{"level":3}'
    assert second.codec == "zstd"
    assert_repo_consistent(repo)


def test_dedupe_hit_missing_dict_does_not_invent_encoding(repo):
    train = repo.store.train_dict([b"no-invent " * 60] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"no-invent " * 300, profile="zstd_dict").result
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (train.dict_id,))
    delete_payload_row(repo, put.ref)
    with pytest.raises(MissingContent):
        repo.store.put_bytes(b"no-invent " * 300, profile="zstd_dict").result
    # The encoding row is still present (nothing was half-written); the dict
    # is gone, so this deliberately-corrupt state is NOT consistency-checked.
    assert enc_row(repo, put.ref) is not None


def test_repair_writes_to_same_shard(repo_sharded):
    data = b"same-shard-repair " * 20_000
    put = repo_sharded.store.put_bytes(data, profile="raw").result
    shard_before = enc_row(repo_sharded, put.ref)["shard_id"]
    delete_payload_row(repo_sharded, put.ref)
    repo_sharded.store.put_bytes(data, profile="raw").result
    assert enc_row(repo_sharded, put.ref)["shard_id"] == shard_before
    assert repo_sharded.store.get_bytes(put.ref) == data
    assert_repo_consistent(repo_sharded)
