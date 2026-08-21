"""Shard routing tests (review §8): max-id selection, numeric listing,
reencode keeps shard placement."""

from __future__ import annotations

from .conftest import assert_repo_consistent, enc_row, make_repo


def test_choose_shard_uses_max_id_not_lexical(tmp_path):
    """shard-10000.sqlite sorts before shard-9999.sqlite lexically; the write
    pointer must use the numeric max."""
    repo = make_repo(tmp_path, "sqlite_sharded")
    backend = repo.backend
    backend.ensure_shard_exists(2)
    backend.ensure_shard_exists(10)
    assert backend.choose_shard_for_write(1) == 10
    backend.ensure_shard_exists(10000)
    assert backend.choose_shard_for_write(1) == 10000


def test_list_shards_numeric(tmp_path):
    repo = make_repo(tmp_path, "sqlite_sharded")
    repo.backend.ensure_shard_exists(10)
    repo.backend.ensure_shard_exists(2)
    assert repo.backend.list_shards() == [1, 2, 10]


def test_shard_roll_on_cap(tmp_path):
    repo = make_repo(tmp_path, "sqlite_sharded", shard_cap_bytes=1 << 20)
    refs = [repo.store.put_bytes(bytes([i]) * 600_000, profile="raw").result.ref for i in range(3)]
    shard_ids = [int(enc_row(repo, ref)["shard_id"]) for ref in refs]
    assert shard_ids == [1, 2, 3]  # rolled on cap, strictly increasing
    assert len(repo.backend.list_shards()) >= 3
    for ref in refs:
        assert repo.store.get_bytes(ref) is not None
    assert_repo_consistent(repo)


def test_reencode_does_not_move_shard(repo_sharded):
    data = b"stay-put " * 100
    ref = repo_sharded.store.put_bytes(data, profile="raw").result.ref
    shard_before = int(enc_row(repo_sharded, ref)["shard_id"])
    repo_sharded.store.reencode([ref], options={"codec": "zstd", "params": {"level": 1}}).result
    shard_after = int(enc_row(repo_sharded, ref)["shard_id"])
    assert shard_after == shard_before
    assert repo_sharded.store.get_bytes(ref) == data


def test_reencode_keeps_shard_across_roll(repo_sharded):
    """A ref living in shard 2 stays in shard 2 after reencode."""
    repo_sharded.store.put_bytes(b"f" * 1_400_000, profile="raw").result
    ref = repo_sharded.store.put_bytes(b"g" * 1_400_000, profile="raw").result.ref
    repo_sharded.store.put_bytes(b"h" * 1_400_000, profile="raw").result  # roll to shard 3
    assert int(enc_row(repo_sharded, ref)["shard_id"]) == 2
    repo_sharded.store.reencode([ref], options={"codec": "zstd", "params": {"level": 1}}).result
    assert int(enc_row(repo_sharded, ref)["shard_id"]) == 2
    assert repo_sharded.store.get_bytes(ref) == b"g" * 1_400_000
