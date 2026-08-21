"""Put/get/open read-semantics tests (spec 4.3, 6)."""

from __future__ import annotations

import io
import os

import pytest

from inkpack import ContentRef, CorruptContent, MissingContent, Profile, open_repo

from .conftest import CANONICAL_SAMPLES, NonseekableBytesIO, delete_payload_row, make_repo, set_payload


@pytest.mark.parametrize("profile", ["raw", "zstd_nodict"])
@pytest.mark.parametrize("sample", CANONICAL_SAMPLES)
def test_put_bytes_roundtrip_preserves_exact_bytes(repo, profile, sample):
    result = repo.store.put_bytes(sample, profile=profile).result
    assert repo.store.get_bytes(result.ref) == sample


@pytest.mark.parametrize("profile", ["raw", "zstd_nodict"])
def test_put_bytes_roundtrip_random_1mb(repo, profile):
    data = os.urandom(1_000_000)
    result = repo.store.put_bytes(data, profile=profile).result
    assert repo.store.get_bytes(result.ref) == data


@pytest.mark.parametrize("profile", ["raw", "zstd_nodict"])
def test_put_stream_roundtrip(repo, profile):
    data = b"stream-body\x00\xff\xfe" * 500
    result = repo.store.put_stream(io.BytesIO(data), profile=profile).result
    assert repo.store.get_bytes(result.ref) == data


def test_put_stream_non_seekable(repo):
    data = b"non-seekable\x00\xff" * 200
    result = repo.store.put_stream(NonseekableBytesIO(data), profile="raw").result
    assert repo.store.get_bytes(result.ref) == data


def test_put_stream_empty(repo):
    result = repo.store.put_stream(io.BytesIO(b""), profile="raw").result
    assert repo.store.get_bytes(result.ref) == b""


def test_put_stream_size_hint_ignored_but_accepted(repo):
    data = b"hinted" * 100
    result = repo.store.put_stream(io.BytesIO(data), profile="raw", size_hint=10_000).result
    assert repo.store.get_bytes(result.ref) == data


def test_put_unknown_profile_raises(repo):
    with pytest.raises(KeyError):
        repo.store.put_bytes(b"x", profile="missing").result


def test_put_with_missing_dict_raises(repo):
    repo.set_profile(Profile(name="broken", codec="zstd", params={"level": 3}, zstd_dict_id="ikd1:does-not-exist"))
    with pytest.raises(MissingContent):
        repo.store.put_bytes(b"x" * 100, profile="broken").result


def test_get_missing_encoding_raises(repo):
    ref = ContentRef(blob_key="ikb1:1:" + "ab" * 32 + "cd" * 16, profile="raw")
    with pytest.raises(MissingContent):
        repo.store.get_bytes(ref)


def test_get_missing_payload_raises(repo):
    put = repo.store.put_bytes(b"gone-payload", profile="raw").result
    delete_payload_row(repo, put.ref)
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)


def test_get_missing_dict_raises_and_verify_counts_missing(repo):
    """Decision C: missing dictionary => MissingContent (not corrupt)."""
    train = repo.store.train_dict([b"dict-sample " * 50] * 5).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 3}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"dict-sample " * 200, profile="zstd_dict").result
    assert put.zstd_dict_id == train.dict_id

    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (train.dict_id,))
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)
    verify = repo.store.verify().result
    assert verify.missing == 1
    assert verify.corrupt == 0


def test_get_corrupt_payload_raises(repo):
    put = repo.store.put_bytes(b"payload-corrupt", profile="zstd_nodict").result
    set_payload(repo, put.ref, b"definitely-not-zstd-frames")
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)


def test_open_is_materialized_and_survives_gc(repo):
    data = b"stable-open" * 30
    put = repo.store.put_bytes(data, profile="raw").result
    handle = repo.store.open(put.ref)
    assert isinstance(handle, io.BytesIO)
    # A writer op that would fail if a read txn were held.
    repo.store.gc(live=[]).result
    assert handle.read() == data
    handle.seek(0)
    assert handle.read() == data


def test_decode_uses_stored_encoding_metadata_not_profile(repo):
    """Spec 6.2 MUST: profile changes must not affect decoding."""
    data = b"hello world " * 500
    put = repo.store.put_bytes(data, profile="zstd_nodict").result
    repo.set_profile(Profile(name="zstd_nodict", codec="none", params={}))
    assert repo.store.get_bytes(put.ref) == data
    # Same for dict-based content: dropping the dict from the profile is fine,
    # the stored zstd_dict_id remains authoritative.
    train = repo.store.train_dict([b"dict-word " * 50] * 5).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 3}, zstd_dict_id=train.dict_id))
    dput = repo.store.put_bytes(b"dict-word " * 200, profile="zstd_dict").result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 3}))
    assert repo.store.get_bytes(dput.ref) == b"dict-word " * 200


def test_verify_on_read_detects_tamper(tmp_path):
    repo = make_repo(tmp_path, "sqlite_single", verify_on_read=True)
    put = repo.store.put_bytes(b"tamper-me", profile="raw").result
    set_payload(repo, put.ref, b"different-bytes")
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)


def test_verify_on_read_default_off(repo):
    put = repo.store.put_bytes(b"tamper-me", profile="raw").result
    set_payload(repo, put.ref, b"different-bytes")
    # No verify_on_read => tampered bytes are returned as-is...
    assert repo.store.get_bytes(put.ref) == b"different-bytes"
    # ...and verify() is what catches it.
    verify = repo.store.verify().result
    assert verify.corrupt == 1


def test_put_stream_then_gc_then_missing(repo):
    put = repo.store.put_stream(io.BytesIO(b"stream-gc"), profile="raw").result
    repo.store.gc(live=[]).result
    with pytest.raises(MissingContent):
        repo.store.get_bytes(put.ref)


def test_roundtrip_all_profiles_after_reopen(tmp_path, repo):
    data = b"persist-through-reopen\x00\xff" * 100
    put = repo.store.put_bytes(data, profile="zstd_nodict").result
    reopened = open_repo(tmp_path / f"repo-{repo.backend.mode}")
    assert reopened.store.get_bytes(put.ref) == data


def test_stream_and_bytes_put_produce_same_blob_key(repo):
    data = b"same-content-two-paths\x00" * 50
    via_bytes = repo.store.put_bytes(data, profile="raw").result
    via_stream = repo.store.put_stream(io.BytesIO(data), profile="raw").result
    assert via_bytes.ref.blob_key == via_stream.ref.blob_key
    assert repo.store.get_bytes(via_stream.ref) == data


def test_put_bytes_typed_rejects_str_input(repo):
    with pytest.raises(TypeError):
        repo.store.put_bytes("not-bytes", profile="raw").result  # type: ignore[arg-type]
