"""Identity (ikb1) and dedupe tests (spec 3.2, decisions A/E/J)."""

from __future__ import annotations

import io
import re

import pytest

from inkpack import ContentRef
from inkpack.codec import (
    blob_key_ikb1_bytes,
    blob_key_ikb1_from_chunks,
    blob_key_ikb1_stream,
    parse_blob_key_ikb1,
)

BLOB_KEY_RE = re.compile(r"^ikb1:\d+:[0-9a-f]{64}:[0-9a-f]{32}$")


def test_blob_key_format_and_determinism():
    for data in (b"", b"abc", b"\x00\xff\xfe", os_urandom(10_000)):
        k1 = blob_key_ikb1_bytes(data)
        k2 = blob_key_ikb1_bytes(data)
        assert k1 == k2
        assert BLOB_KEY_RE.match(k1[0])
        assert k1[1] == len(data)


def test_blob_key_stream_and_chunks_match_bytes():
    data = b"stream-identity-check\x00\xff" * 1000
    by_bytes = blob_key_ikb1_bytes(data)
    by_stream = blob_key_ikb1_stream(io.BytesIO(data))
    by_chunks = blob_key_ikb1_from_chunks([data[i : i + 7] for i in range(0, len(data), 7)])
    assert by_bytes == by_stream == by_chunks


def test_blob_key_content_addressing():
    a = blob_key_ikb1_bytes(b"one")
    b = blob_key_ikb1_bytes(b"two")
    assert a[0] != b[0]
    assert parse_blob_key_ikb1(a[0]) == (3, a[2], a[3])


def test_parse_blob_key_validation():
    with pytest.raises(ValueError):
        parse_blob_key_ikb1("nope")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1("ikb1:3:zz:zz")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1("ikb1:abc:deadbeef:deadbeef")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1("ikb1:3:0000:0000")
    key = blob_key_ikb1_bytes(b"parse-me")[0]
    assert parse_blob_key_ikb1(key)[0] == 8


def test_put_dedupe_same_bytes_same_blob_key(repo):
    r1 = repo.store.put_bytes(b"same-data", profile="raw").result
    r2 = repo.store.put_bytes(b"same-data", profile="raw").result
    assert r1.ref.blob_key == r2.ref.blob_key
    with repo.backend.txn(write=False) as conn:
        blobs = conn.execute("SELECT COUNT(*) FROM blobs WHERE blob_key=?", (r1.ref.blob_key,)).fetchone()[0]
        encodings = conn.execute(
            "SELECT COUNT(*) FROM encodings WHERE blob_key=? AND profile=?", (r1.ref.blob_key, "raw")
        ).fetchone()[0]
    assert blobs == 1
    assert encodings == 1
    assert repo.store.get_bytes(r2.ref) == b"same-data"


def test_dedupe_different_bytes_different_blob_key(repo):
    a = repo.store.put_bytes(b"aaa", profile="raw").result
    b = repo.store.put_bytes(b"bbb", profile="raw").result
    assert a.ref.blob_key != b.ref.blob_key


def test_same_bytes_two_profiles_two_encodings(repo):
    data = b"multi-profile-data" * 50
    a = repo.store.put_bytes(data, profile="raw").result
    b = repo.store.put_bytes(data, profile="zstd_nodict").result
    assert a.ref.blob_key == b.ref.blob_key
    assert a.ref.profile != b.ref.profile
    assert repo.store.get_bytes(a.ref) == data
    assert repo.store.get_bytes(b.ref) == data
    with repo.backend.txn(write=False) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM encodings WHERE blob_key=?", (a.ref.blob_key,)
        ).fetchone()[0]
    assert n == 2


def test_dedupe_hit_reflects_stored_encoding(repo):
    """Decision E: on a dedupe hit, PutResult must describe the stored row."""
    data = b"dedupe-reflection" * 200
    first = repo.store.put_bytes(data, profile="zstd_nodict").result
    assert first.codec == "zstd"
    profiles = repo.backend.config_get("profiles")
    profiles["zstd_nodict"] = {"codec": "none", "params": {}, "zstd_dict_id": None}
    repo.backend.config_set("profiles", profiles)
    second = repo.store.put_bytes(data, profile="zstd_nodict").result
    assert second.codec == "zstd"  # stored metadata, not the mutated profile
    assert second.stored_len == first.stored_len
    assert repo.store.get_bytes(second.ref) == data


def test_has_blob_follows_blobs_catalog(repo):
    put = repo.store.put_bytes(b"blob-catalog", profile="raw").result
    assert repo.store.has_blob(put.ref.blob_key) is True
    assert repo.store.has_blob("ikb1:0:" + "0" * 64 + "0" * 32) is False
    repo.store.gc(live=[]).result
    assert repo.store.has_blob(put.ref.blob_key) is False


def test_blobs_raw_len_matches_blob_key(repo):
    put = repo.store.put_bytes(b"raw-len-check" * 10, profile="raw").result
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT raw_len FROM blobs WHERE blob_key=?", (put.ref.blob_key,)).fetchone()
    assert row is not None
    assert row[0] == parse_blob_key_ikb1(put.ref.blob_key)[0]
    assert row[0] == put.raw_len


def test_put_empty_bytes_roundtrip_and_dedupe(repo):
    a = repo.store.put_bytes(b"", profile="raw").result
    b = repo.store.put_bytes(b"", profile="raw").result
    assert a.ref.blob_key == b.ref.blob_key
    assert BLOB_KEY_RE.match(a.ref.blob_key)
    assert repo.store.get_bytes(a.ref) == b""
    v = repo.store.verify().result
    assert v.ok == 1


def test_put_ref_fields(repo):
    data = b"ref-fields"
    result = repo.store.put_bytes(data, profile="zstd_nodict").result
    assert result.ref == ContentRef(blob_key=result.ref.blob_key, profile="zstd_nodict")
    assert result.raw_len == len(data)
    assert result.stored_len > 0
    assert result.codec == "zstd"
    assert result.zstd_dict_id is None


def os_urandom(n: int) -> bytes:
    import os

    return os.urandom(n)
