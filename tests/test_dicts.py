"""Dictionary tests (spec 9, decision C)."""

from __future__ import annotations

import json

import pytest

from inkpack import MissingContent, Profile

from .conftest import delete_dict, enc_row


def _add_dict_profile(repo, dict_id: str, name: str = "zstd_dict", level: int = 6) -> None:
    repo.set_profile(Profile(name=name, codec="zstd", params={"level": level}, zstd_dict_id=dict_id))


def test_train_dict_persists_row(repo):
    samples = [b"word " * 200, b"another sample " * 100, b"third sample " * 150, b"fourth " * 250, b"fifth " * 90]
    result = repo.store.train_dict(samples).result
    assert result.dict_id.startswith("ikd1:")
    assert result.dict_size > 0
    assert result.samples_used == 5
    assert result.sample_bytes == sum(len(s) for s in samples)
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT * FROM dicts WHERE dict_id=?", (result.dict_id,)).fetchone()
    assert row is not None
    assert row["codec"] == "zstd"
    assert len(row["dict_bytes"]) == result.dict_size
    assert json.loads(row["params_json"])["samples_used"] == 5


def test_train_dict_deterministic_id(repo):
    samples = [b"deterministic " * 50, b"training " * 30, b"third " * 40, b"fourth " * 60, b"fifth " * 20]
    a = repo.store.train_dict(samples).result
    b = repo.store.train_dict(samples).result
    assert a.dict_id == b.dict_id
    assert a.dict_size == b.dict_size


def test_train_dict_skips_empty_samples(repo):
    result = repo.store.train_dict([b"", b"real", b"", b"more", b"", b"stuff", b"", b"here", b"", b"too"]).result
    assert result.samples_used == 5
    assert result.sample_bytes == len(b"real") + len(b"more") + len(b"stuff") + len(b"here") + len(b"too")


def test_train_dict_requires_five_non_empty_samples(repo):
    with pytest.raises(ValueError):
        repo.store.train_dict([]).result
    with pytest.raises(ValueError):
        repo.store.train_dict([b"", b""]).result
    with pytest.raises(ValueError):
        repo.store.train_dict([b"only-four"] * 4).result


def test_train_dict_option_validation(repo):
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x"], options={"dict_size": 10}).result  # below zstd min
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x"], options={"dict_size": "big"}).result
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x"], options={"mystery": 1}).result


def test_put_with_dict_sets_zstd_dict_id_and_decodes(repo):
    train = repo.store.train_dict([b"dict-sample " * 100] * 5).result
    _add_dict_profile(repo, train.dict_id)
    data = b"dict-sample " * 500
    put = repo.store.put_bytes(data, profile="zstd_dict").result
    assert put.zstd_dict_id == train.dict_id
    assert put.codec == "zstd"
    assert enc_row(repo, put.ref)["zstd_dict_id"] == train.dict_id
    assert repo.store.get_bytes(put.ref) == data


def test_dict_compresses_repetitive_text(repo):
    train = repo.store.train_dict([b"the quick brown fox " * 200] * 5).result
    _add_dict_profile(repo, train.dict_id)
    data = b"the quick brown fox " * 2000
    put = repo.store.put_bytes(data, profile="zstd_dict").result
    assert put.stored_len < put.raw_len
    assert repo.store.get_bytes(put.ref) == data


def test_profile_dict_id_swap_changes_encoding_not_decoding(repo):
    train_a = repo.store.train_dict([b"alpha " * 100] * 5).result
    train_b = repo.store.train_dict([b"beta " * 100] * 5).result
    _add_dict_profile(repo, train_a.dict_id)
    data = b"alpha " * 300
    put = repo.store.put_bytes(data, profile="zstd_dict").result
    assert put.zstd_dict_id == train_a.dict_id
    # Redefine the profile to use a different dict; decoding must still work
    # and reencode must migrate the stored row to the new dict.
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=train_b.dict_id))
    assert repo.store.get_bytes(put.ref) == data
    repo.store.reencode([put.ref]).result
    assert enc_row(repo, put.ref)["zstd_dict_id"] == train_b.dict_id
    assert repo.store.get_bytes(put.ref) == data


def test_gc_deletes_unreferenced_dicts_by_default(repo):
    train = repo.store.train_dict([b"gc-dict " * 100] * 5).result
    _add_dict_profile(repo, train.dict_id)
    put = repo.store.put_bytes(b"gc-dict " * 300, profile="zstd_dict").result
    gc = repo.store.gc(live=[put.ref]).result
    assert gc.dicts_deleted == 0  # still referenced
    # The profile still references the dict: GC must keep it (review §4.1
    # Option B) so the documented train -> set_profile -> gc -> put workflow
    # does not lose the dictionary.
    gc = repo.store.gc(live=[]).result
    assert gc.dicts_deleted == 0
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT 1 FROM dicts WHERE dict_id=?", (train.dict_id,)).fetchone()
    assert row is not None
    # Drop the profile reference, then GC reclaims the dict.
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}))
    gc = repo.store.gc(live=[]).result
    assert gc.dicts_deleted == 1
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT 1 FROM dicts WHERE dict_id=?", (train.dict_id,)).fetchone()
    assert row is None


def test_missing_dict_then_reencode_restores(repo):
    train = repo.store.train_dict([b"restore " * 100] * 5).result
    _add_dict_profile(repo, train.dict_id)
    put = repo.store.put_bytes(b"restore " * 300, profile="zstd_dict").result
    delete_dict(repo, train.dict_id)
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)
    # Reencode skips the unreadable target instead of aborting (review P1-3).
    skipped = repo.store.reencode([put.ref]).result
    assert skipped.skipped == 1 and skipped.reencoded == 0
    # Re-training the same dict and re-running reencode repairs the repo.
    again = repo.store.train_dict([b"restore " * 100] * 5).result
    assert again.dict_id == train.dict_id
    result = repo.store.reencode([put.ref]).result
    assert result.reencoded == 1
    assert repo.store.get_bytes(put.ref) == b"restore " * 300


def test_dicts_stored_only_in_index_db(repo_sharded):
    train = repo_sharded.store.train_dict([b"shard-dict " * 100] * 5).result
    _add_dict_profile(repo_sharded, train.dict_id)
    repo_sharded.store.put_bytes(b"shard-dict " * 200, profile="zstd_dict").result
    for sid in repo_sharded.backend.list_shards():
        with repo_sharded.backend.txn(write=False, attach_shard_id=sid) as conn:
            row = conn.execute(
                "SELECT name FROM p.sqlite_master WHERE type='table' AND name='dicts'"
            ).fetchone()
        assert row is None, f"dicts table must not exist in shard {sid}"
