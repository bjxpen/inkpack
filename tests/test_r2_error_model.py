"""Fail-first tests for the r2 issue register, section A (error model).

Each test was designed to fail on the pre-fix code and locks the fixed
behavior (deterministic — no thread-timing dependence):

- A1  compact() leaks raw sqlite3.DatabaseError on a corrupt shard
- A2  sharded put leaks raw sqlite3.DatabaseError on a corrupt shard
- A3  verify() aborts on a damaged payload schema (both backends)
- A4  dedupe hit succeeds on undecodable stored metadata
- A5  verify() does not enforce stored_len / blobs.raw_len invariants
- A6  retryable conditions are indistinguishable (Retryable type)
- A7  MemoryError misclassified as corrupt/ValueError
- A8  text stream to put_stream fails opaquely
- A9  create_repo coerces bool shard caps
"""

from __future__ import annotations

import io
import sqlite3

import pytest

from inkpack import CorruptContent, InkpackError, Profile, create_repo

from .conftest import corrupt_shard_btree_page, enc_row, make_profiles

PROFILES = make_profiles()


# ---------------------------------------------------------------------------
# A1 — compact() must not leak raw sqlite3.DatabaseError on a corrupt shard
# ---------------------------------------------------------------------------


def test_compact_corrupt_shard_is_typed_error(repo_sharded):
    repo_sharded.store.put_bytes(b"x" * 100, "raw").result
    sid = repo_sharded.backend.list_shards()[0]
    repo_sharded.backend.shard_path(sid).write_bytes(b"not a sqlite db")
    with pytest.raises(InkpackError) as ei:  # today: raw sqlite3.DatabaseError
        repo_sharded.store.compact().result
    assert not isinstance(ei.value, sqlite3.Error)


def test_compact_vacuum_of_image_malformed_shard_is_typed_error(repo_sharded):
    """Valid header at connect time; the damage fires at VACUUM itself.

    This pins the broadened ``_vacuum`` catch (a DatabaseError raised past
    the old OperationalError-only handler).
    """
    ref = repo_sharded.store.put_bytes(b"vac" * 100, "raw").result.ref
    corrupt_shard_btree_page(repo_sharded, ref)
    with pytest.raises(InkpackError) as ei:
        repo_sharded.store.compact().result
    assert not isinstance(ei.value, sqlite3.Error)


def test_compact_corrupt_index_is_typed_error(repo_single):
    repo_single.store.put_bytes(b"x" * 100, "raw").result
    repo_single.backend.index_path.write_bytes(b"not a sqlite db")
    with pytest.raises(InkpackError) as ei:  # today: raw sqlite3.DatabaseError
        repo_single.store.compact().result
    assert not isinstance(ei.value, sqlite3.Error)


def test_open_repo_corrupt_shard_is_typed_error(repo_sharded):
    """S8: open-time shard validation must not leak raw sqlite errors either."""
    repo_sharded.store.put_bytes(b"x" * 100, "raw").result
    sid = repo_sharded.backend.list_shards()[0]
    repo_sharded.backend.shard_path(sid).write_bytes(b"junk")
    from inkpack import open_repo

    with pytest.raises(InkpackError) as ei:
        open_repo(repo_sharded.backend.root)
    assert not isinstance(ei.value, sqlite3.Error)


# ---------------------------------------------------------------------------
# A2 — sharded put must not leak raw sqlite3.DatabaseError on a corrupt shard
# ---------------------------------------------------------------------------


def test_put_over_unusable_shard_is_corruptcontent(repo_sharded):
    """Connect succeeds (valid header); the dedupe payload probe fails.

    This pins the broadened ``Session._shard_query_one`` catch — a
    ``DatabaseError`` raised there previously escaped raw (only
    ``OperationalError`` was translated).
    """
    ref = repo_sharded.store.put_bytes(b"hello" * 50, "raw").result.ref
    corrupt_shard_btree_page(repo_sharded, ref)
    with pytest.raises(CorruptContent):  # today: raw sqlite3.DatabaseError
        repo_sharded.store.put_bytes(b"hello" * 50, "raw").result


def test_put_over_junk_shard_is_corruptcontent(repo_sharded):
    """Garbage file (no valid header): connect-phase classification."""
    ref = repo_sharded.store.put_bytes(b"hello" * 50, "raw").result.ref
    row = enc_row(repo_sharded, ref)
    repo_sharded.backend.shard_path(int(row["shard_id"])).write_bytes(b"junk-junk")
    with pytest.raises(CorruptContent):
        repo_sharded.store.put_bytes(b"hello" * 50, "raw").result


def test_get_bytes_over_corrupt_shard_is_corruptcontent(repo_sharded):
    """The read path must classify the same corruption the same way (A2)."""
    ref = repo_sharded.store.put_bytes(b"hello" * 50, "raw").result.ref
    row = enc_row(repo_sharded, ref)
    repo_sharded.backend.shard_path(int(row["shard_id"])).write_bytes(b"junk-junk")
    with pytest.raises(CorruptContent):
        repo_sharded.store.get_bytes(ref)


# ---------------------------------------------------------------------------
# A3 — verify() must count a damaged payload schema as corrupt, not abort
# ---------------------------------------------------------------------------


def test_verify_counts_damaged_payload_schema_as_corrupt(repo):
    """Dropping the payload table (main DB in single mode, any shard in
    sharded mode) is a per-row content finding — never an abort."""
    refs = [repo.store.put_bytes(f"r{i}".encode(), "raw").result.ref for i in range(3)]
    target = (
        repo.backend.index_path
        if repo.backend.mode == "sqlite_single"
        else repo.backend.shard_path(repo.backend.list_shards()[0])
    )
    with sqlite3.connect(str(target)) as conn:
        conn.execute("DROP TABLE payload")
    result = repo.store.verify().result  # today: raises InkpackError
    assert result.corrupt == 3 and result.ok == 0
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(refs[0])


def test_damaged_payload_schema_is_per_shard_not_global(repo_sharded):
    """A payload schema damaged in ONE shard must not poison other shards."""
    keep = repo_sharded.store.put_bytes(b"keep" * 1_500_000, "raw").result.ref  # shard 1
    dead = repo_sharded.store.put_bytes(b"dead" * 1_500_000, "raw").result.ref  # shard 2
    assert len(repo_sharded.backend.list_shards()) >= 2
    keep_sid = int(enc_row(repo_sharded, keep)["shard_id"])
    dead_sid = int(enc_row(repo_sharded, dead)["shard_id"])
    assert keep_sid != dead_sid
    with sqlite3.connect(str(repo_sharded.backend.shard_path(dead_sid))) as conn:
        conn.execute("DROP TABLE payload")
    result = repo_sharded.store.verify().result
    assert result.ok == 1 and result.corrupt == 1
    assert repo_sharded.store.get_bytes(keep) == b"keep" * 1_500_000


# ---------------------------------------------------------------------------
# A4 — dedupe hit must reject undecodable stored metadata
# ---------------------------------------------------------------------------


def test_dedupe_hit_rejects_undecodable_stored_metadata(repo):
    """A tampered row (codec 'none' + zstd_dict_id) is unreadable, so the hit
    must not report success (N7)."""
    trained = repo.store.train_dict([b"probe " * 80] * 6).result
    ref = repo.store.put_bytes(b"probe " * 200, "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET zstd_dict_id=? WHERE blob_key=?",
            (trained.dict_id, ref.blob_key),
        )
    with pytest.raises(CorruptContent):  # today: returns PutResult
        repo.store.put_bytes(b"probe " * 200, "raw").result


def test_dedupe_hit_via_stream_rejects_undecodable_stored_metadata(repo):
    """The stream shortcut (stream_prepare) reaches the same validator via
    persist_prepared's dedupe branch."""
    trained = repo.store.train_dict([b"sprobe " * 80] * 6).result
    ref = repo.store.put_bytes(b"sprobe " * 200, "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET zstd_dict_id=? WHERE blob_key=?",
            (trained.dict_id, ref.blob_key),
        )
    with pytest.raises(CorruptContent):
        repo.store.put_stream(io.BytesIO(b"sprobe " * 200), "raw").result


# ---------------------------------------------------------------------------
# A5 — verify() must enforce stored_len / blobs.raw_len invariants
# ---------------------------------------------------------------------------


def test_verify_detects_stored_len_mismatch(repo):
    ref = repo.store.put_bytes(b"invariant " * 50, "zstd_nodict").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET stored_len=stored_len+1 WHERE blob_key=?", (ref.blob_key,)
        )
    assert repo.store.verify().result.corrupt == 1  # today: ok == 1


def test_verify_detects_blobs_raw_len_mismatch(repo):
    ref = repo.store.put_bytes(b"j-invariant", "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute("UPDATE blobs SET raw_len=raw_len+1 WHERE blob_key=?", (ref.blob_key,))
    assert repo.store.verify().result.corrupt == 1  # today: ok == 1


def test_verify_detects_encoding_without_blobs_row(repo):
    ref = repo.store.put_bytes(b"no-blob-row", "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM blobs WHERE blob_key=?", (ref.blob_key,))
    assert repo.store.verify().result.corrupt == 1  # today: ok == 1


def test_get_bytes_enforces_stored_len_on_read(repo):
    """Read layer: a lying stored_len is CorruptContent, not a silent read
    (A5 layer 1; changelogs as a get_bytes tightening)."""
    ref = repo.store.put_bytes(b"len-check " * 50, "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET stored_len=stored_len+1 WHERE blob_key=?", (ref.blob_key,)
        )
    with pytest.raises(CorruptContent, match="stored_len"):
        repo.store.get_bytes(ref)


# ---------------------------------------------------------------------------
# A6 — transient races are Retryable, not plain InkpackError
# ---------------------------------------------------------------------------


def test_persist_vanished_encoding_is_retryable(repo):
    from inkpack import Retryable

    data = b"retryable " * 20
    repo.store.put_bytes(data, "raw").result
    prepared = repo.store.prepare_bytes(data, "raw")
    assert prepared.enc is None  # healthy hit
    with repo.backend.txn(write=True) as conn:  # row vanishes after prepare
        conn.execute("DELETE FROM encodings")
    with pytest.raises(Retryable), repo.backend.session() as s:  # today: plain InkpackError
        repo.store._persist(s, prepared, None)


def test_retryable_is_an_inkpack_error():
    from inkpack import InkpackError, Retryable

    assert issubclass(Retryable, InkpackError)
    with pytest.raises(InkpackError):
        raise Retryable("retry me")


# ---------------------------------------------------------------------------
# A7 — MemoryError is never corruption or a policy error
# ---------------------------------------------------------------------------


def test_memory_error_propagates_from_encode(repo, monkeypatch):
    import inkpack.codec as cm

    class Boom:
        def __init__(self, *a, **k):
            pass

        def compress(self, data):
            raise MemoryError("oom")

    monkeypatch.setattr(cm._zstd, "ZstdCompressor", Boom)
    with pytest.raises(MemoryError):  # today: ValueError
        repo.store.put_bytes(b"x" * 10, "zstd_nodict").result


def test_memory_error_propagates_from_decode(repo, monkeypatch):
    import inkpack.codec as cm

    ref = repo.store.put_bytes(b"decode-oom" * 50, "zstd_nodict").result.ref

    class Boom:
        def __init__(self, *a, **k):
            pass

        def decompress(self, data, **k):
            raise MemoryError("oom")

    monkeypatch.setattr(cm._zstd, "ZstdDecompressor", Boom)
    with pytest.raises(MemoryError):  # today: CorruptContent
        repo.store.get_bytes(ref)


def test_memory_error_aborts_verify_not_counts_corrupt(repo, monkeypatch):
    repo.store.put_bytes(b"x", "raw").result

    def boom(*a, **k):
        raise MemoryError("oom")

    monkeypatch.setattr(type(repo.store), "_decode_from_row", boom)
    with pytest.raises(MemoryError):  # today: corrupt == 1
        repo.store.verify().result


# ---------------------------------------------------------------------------
# A8 — text streams fail fast with an actionable error
# ---------------------------------------------------------------------------


def test_put_stream_text_mode_fail_fast_with_actionable_error(repo):
    with pytest.raises(TypeError, match="binary"):
        repo.store.put_stream(io.StringIO("chapter text"), profile="raw").result


def test_upsert_chapter_stream_text_mode_fail_fast(repo):
    novel_id = repo.create_novel("t")
    with pytest.raises(TypeError, match="binary"):
        repo.upsert_chapter_stream(io.StringIO("chapter text"), novel_id, "1", "raw").result


def test_put_stream_binary_still_works(repo):
    result = repo.store.put_stream(io.BytesIO(b"binary is fine"), "raw").result
    assert repo.store.get_bytes(result.ref) == b"binary is fine"


# ---------------------------------------------------------------------------
# A9 — create_repo rejects bool shard caps at the factory
# ---------------------------------------------------------------------------


def test_create_repo_rejects_bool_shard_caps(tmp_path):
    with pytest.raises(ValueError, match="shard_cap_bytes must be a positive integer"):
        create_repo(
            tmp_path / "b",
            "sqlite_sharded",
            profiles={"raw": Profile("raw", "none", {})},
            shard_cap_bytes=True,
        )
    # legacy kwarg (D1): deprecation warning + validation under the new name
    with pytest.warns(DeprecationWarning), pytest.raises(
        ValueError, match="min_shard_cap_bytes must be a positive integer"
    ):
        create_repo(
            tmp_path / "c",
            "sqlite_sharded",
            profiles={"raw": Profile("raw", "none", {})},
            shard_min_bytes=False,
        )
