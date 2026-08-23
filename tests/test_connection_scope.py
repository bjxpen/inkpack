"""Operation-scoped connection tests (review §13, §15, §16, §18)."""

from __future__ import annotations

import pytest

from inkpack import Cancelled, CorruptContent, Profile

from .conftest import set_payload


def test_get_bytes_bounded_connects(repo, connect_counter):
    put = repo.store.put_bytes(b"bounded " * 100, profile="raw").result
    connect_counter["opens"] = 0
    assert repo.store.get_bytes(put.ref) == b"bounded " * 100
    # H1/D1: encodings + payload are read as ONE snapshot on the index
    # connection (sharded reads ATTACH the shard; no second connection).
    assert connect_counter["opens"] == 1
    assert connect_counter["live"] == 0  # closed after the call


def test_verify_connects_not_per_row(repo, connect_counter):
    for i in range(20):
        repo.store.put_bytes(f"conn-{i}".encode() * 10, profile="raw").result
    connect_counter["opens"] = 0
    verify = repo.store.verify().result
    assert verify.checked == 20
    # C1: O(1) connections (one session connection, plus at most the
    # blob-limit probe); sharded attaches ride on that session (O(shards)
    # attaches, not O(rows)) — see test_r2_performance for the attach bound.
    assert connect_counter["opens"] <= 2  # O(1), not O(rows)


def test_reencode_connects_not_per_row(repo, connect_counter):
    refs = [
        repo.store.put_bytes(f"re-conn-{i}".encode() * 10, profile="raw").result.ref for i in range(15)
    ]
    connect_counter["opens"] = 0
    result = repo.store.reencode(refs, options={"codec": "none"}).result
    assert result.reencoded == 15
    assert connect_counter["opens"] <= 2


def test_verify_closes_on_cancel(repo, connect_counter):
    for i in range(40):
        repo.store.put_bytes(f"close-{i}".encode(), profile="raw").result
    calls = {"n": 0}

    def token() -> bool:
        calls["n"] += 1
        return calls["n"] > 5

    connect_counter["opens"] = 0
    with pytest.raises(Cancelled):
        repo.store.verify(cancel=token).result
    assert connect_counter["opens"] > 0  # connections were opened...
    assert connect_counter["live"] == 0  # ...and all closed


def test_get_bytes_does_not_hold_connection(repo, connect_counter):
    put = repo.store.put_bytes(b"no-hold" * 50, profile="raw").result
    for _ in range(20):
        repo.store.get_bytes(put.ref)
    assert connect_counter["live"] == 0


def test_put_connects_once(repo, connect_counter):
    connect_counter["opens"] = 0
    put = repo.store.put_bytes(b"one-conn", profile="raw").result
    # G4 annotation: `== 1` holds because the blob-length probe is
    # SESSION-PLUMBED — check_blob_limit runs on the session connection
    # (backend.blob_length_limit(conn=s.conn)) instead of opening a dedicated
    # probe connection. A regression to a per-call probe connection would
    # make this 2.
    assert connect_counter["opens"] == 1
    assert repo.store.get_bytes(put.ref) == b"one-conn"


# -- §15 paging -----------------------------------------------------------------


def test_iter_encodings_pages_small_page(repo, monkeypatch):
    import inkpack.blobstore as bs

    monkeypatch.setattr(bs, "_PAGE_SIZE", 2)
    for i in range(5):
        repo.store.put_bytes(f"page-{i}".encode(), profile="raw").result
    verify = repo.store.verify().result
    assert verify.checked == 5 and verify.ok == 5  # paging is transparent


def test_verify_limit_stops_across_pages(repo, monkeypatch):
    import inkpack.blobstore as bs

    monkeypatch.setattr(bs, "_PAGE_SIZE", 2)
    for i in range(5):
        repo.store.put_bytes(f"limit-{i}".encode(), profile="raw").result
    assert repo.store.verify(limit=3).result.checked == 3


# -- §16 dict caching within an operation --------------------------------------


def test_dict_cached_within_verify(repo, monkeypatch):
    """G1: the dict-cache property is OBSERVABLE — a 10-row verify fetches
    the dictionary bytes exactly once (the old spy watched
    ``backend.get_dict``, which verify never calls: vacuous)."""
    from inkpack.sqlite import Session

    train = repo.store.train_dict([b"cache-dict " * 60] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    data = b"cache-dict " * 200
    refs = []
    for i in range(10):
        # Distinct content (different suffixes) but all compressed with the dict.
        body = data + bytes([i]) * 7
        refs.append(repo.store.put_bytes(body, profile="zstd_dict").result.ref)
    # Instrument the actual FETCH (the dicts query), not the cached accessor:
    # dict_bytes is called per row, but the underlying query must run once.
    calls = {"n": 0}
    original = Session.query_one

    def spy(self, sql, params=()):
        if "FROM dicts" in sql:
            calls["n"] += 1
        return original(self, sql, params)

    monkeypatch.setattr(Session, "query_one", spy)
    verify = repo.store.verify().result
    assert verify.ok == 10
    assert calls["n"] == 1  # dict bytes fetched once for the whole operation


# -- §18 operation-local snapshots ----------------------------------------------


def test_set_profile_visible_on_next_put(repo):
    repo.set_profile(Profile(name="zstd_lvl", codec="zstd", params={"level": 3}))
    first = repo.store.put_bytes(b"snapshot-1 " * 100, profile="zstd_lvl").result
    repo.set_profile(Profile(name="zstd_lvl", codec="zstd", params={"level": 9}))
    second = repo.store.put_bytes(b"snapshot-2 " * 100, profile="zstd_lvl").result
    from .conftest import enc_row

    assert enc_row(repo, first.ref)["codec_params_json"] == '{"level":3}'
    assert enc_row(repo, second.ref)["codec_params_json"] == '{"level":9}'


def test_get_bytes_reads_config_once(repo, monkeypatch, connect_counter):
    put = repo.store.put_bytes(b"cfg-once" * 20, profile="raw").result
    calls = {"n": 0}
    original = repo.backend.config_get

    def spy(key):
        calls["n"] += 1
        return original(key)

    monkeypatch.setattr(repo.backend, "config_get", spy)
    connect_counter["opens"] = 0
    repo.store.get_bytes(put.ref)
    assert calls["n"] == 0  # reads go through the session's connection
    assert connect_counter["opens"] <= 2


def test_verify_on_read_toggled_via_config_set(repo):
    put = repo.store.put_bytes(b"toggle-me", profile="raw").result
    repo.backend.config_set("verify_on_read", True)
    set_payload(repo, put.ref, b"toggled!!")  # same length, different content
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)
    repo.backend.config_set("verify_on_read", False)
    assert repo.store.get_bytes(put.ref) == b"toggled!!"  # no identity check
