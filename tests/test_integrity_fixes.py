"""Fail-first tests for the second review round (P0/P1/P2 fixes).

Each test here was designed to fail on the pre-fix code and locks the fixed
behavior:

- P0-1  upsert repairs a missing payload under the stored policy
- P0-2  sharded open/read/compact never create files
- P0-3  GC is index-driven (NULL/missing shard encodings are reclaimed)
- P0-4  NULL shard locator yields a typed error, not a raw sqlite error
- P0-5  prepare_bytes binds a caller-supplied blob_key to the content
- P0-6  backend create refuses an existing repository
- P1-2  profiles_from_config is strict at the top level
- P1-3  reencode skips bad targets and continues
- P1-4  compact option correctness
- P1-5  dedupe hit fails when the required dict is missing
- P1-6  missing chapter row is NotFound (catalog miss)
- P1-7  list_chapters orders by order_key
- P2-5  Operation.close releases resources deterministically
- P2-6  decode output is bounded by the blob_key's raw_len
"""

from __future__ import annotations

import io

import pytest

from inkpack import (
    Cancelled,
    ContentRef,
    CorruptContent,
    InkpackError,
    MissingContent,
    NotFound,
    Profile,
)
from inkpack.sqlite import SqliteBackend
from inkpack.types import profiles_from_config

from .conftest import assert_repo_consistent, delete_payload_row, enc_row

# -- P0-1: upsert repairs missing payload -------------------------------------


@pytest.mark.parametrize("mode", ["sqlite_single", "sqlite_sharded"])
def test_upsert_repairs_missing_payload_without_policy_migration(tmp_path, mode):
    from .conftest import make_repo

    repo = make_repo(tmp_path, mode)
    novel_id = repo.create_novel("n")
    repo.set_profile(Profile("zstd_nodict", "zstd", {"level": 3}))
    body = b"once upon a time" * 20
    chapter_id = repo.upsert_chapter(novel_id, "ch-1", body, "zstd_nodict")

    info = repo.get_chapter(chapter_id)
    blob_key, profile = info.blob_key, info.profile
    row0 = repo.backend.get_encoding(blob_key, profile)
    codec0 = str(row0["codec"])
    params0 = str(row0["codec_params_json"])

    delete_payload_row(repo, ContentRef(blob_key, profile))
    with pytest.raises(MissingContent):
        repo.get_chapter_bytes(chapter_id)

    # Change the current profile: repair must use the STORED policy (level 3).
    repo.set_profile(Profile("zstd_nodict", "zstd", {"level": 19}))
    repo.upsert_chapter(novel_id, "ch-1", body, "zstd_nodict")

    assert repo.get_chapter_bytes(chapter_id) == body
    row1 = repo.backend.get_encoding(blob_key, profile)
    assert str(row1["codec"]) == codec0
    assert str(row1["codec_params_json"]) == params0
    assert_repo_consistent(repo)


# -- P0-2: open/read/compact never create files --------------------------------


def test_backend_open_sharded_does_not_create_payload_dir(tmp_path):
    root = tmp_path / "r"
    SqliteBackend.create(root, mode="sqlite_sharded")
    payload = root / "payload"
    assert payload.exists()

    for path in payload.glob("*"):
        path.unlink()
    payload.rmdir()

    with pytest.raises(InkpackError):
        SqliteBackend.open(root, mode="sqlite_sharded", create=False)
    assert not payload.exists()


def test_get_bytes_missing_shard_file_does_not_create(repo_sharded):
    put = repo_sharded.store.put_bytes(b"x" * 100, "raw").result
    row = repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile)
    shard_path = repo_sharded.backend.shard_path(int(row["shard_id"]))

    before = set(repo_sharded.backend.payload_dir.glob("shard-*.sqlite"))
    shard_path.unlink()

    with pytest.raises(MissingContent):
        repo_sharded.store.get_bytes(put.ref)

    after = set(repo_sharded.backend.payload_dir.glob("shard-*.sqlite"))
    assert after == before - {shard_path}
    assert not shard_path.exists()


def test_compact_unknown_shard_id_rejected_not_created(repo_sharded):
    missing = 999
    assert repo_sharded.backend.shard_path(missing).exists() is False
    with pytest.raises(ValueError, match="unknown shard"):
        repo_sharded.store.compact(options={"shard_ids": [missing]}).result
    assert repo_sharded.backend.shard_path(missing).exists() is False


# -- P0-3: GC is index-driven --------------------------------------------------


def test_gc_deletes_encodings_with_null_shard_id(repo_sharded):
    put = repo_sharded.store.put_bytes(b"dead-null", "raw").result
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=NULL WHERE blob_key=? AND profile=?",
            (put.ref.blob_key, put.ref.profile),
        )
    gc = repo_sharded.store.gc(live=[]).result
    assert gc.encodings_deleted == 1
    assert repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile) is None
    assert_repo_consistent(repo_sharded)


def test_gc_deletes_encoding_if_shard_file_missing(repo_sharded):
    put = repo_sharded.store.put_bytes(b"dead-missing-file", "raw").result
    row = repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile)
    shard_path = repo_sharded.backend.shard_path(int(row["shard_id"]))
    shard_path.unlink()

    gc = repo_sharded.store.gc(live=[]).result
    assert gc.encodings_deleted == 1
    assert repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile) is None
    assert not shard_path.exists()  # never recreated
    assert_repo_consistent(repo_sharded)


def test_gc_repairs_mixed_shard_locations(repo_sharded):
    """Dead rows across a valid shard, a NULL shard and a missing shard file
    are all reclaimed in one run."""
    good = repo_sharded.store.put_bytes(b"good" * 300_000, "raw").result.ref
    nulled = repo_sharded.store.put_bytes(b"nulled", "raw").result.ref
    missing_file = repo_sharded.store.put_bytes(b"missing" * 300_000, "raw").result.ref

    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=NULL WHERE blob_key=? AND profile=?",
            (nulled.blob_key, nulled.profile),
        )
    row = repo_sharded.backend.get_encoding(missing_file.blob_key, missing_file.profile)
    repo_sharded.backend.shard_path(int(row["shard_id"])).unlink()

    gc = repo_sharded.store.gc(live=[good]).result
    assert gc.encodings_deleted == 2
    assert repo_sharded.backend.get_encoding(nulled.blob_key, nulled.profile) is None
    assert repo_sharded.backend.get_encoding(missing_file.blob_key, missing_file.profile) is None
    assert repo_sharded.store.get_bytes(good) == b"good" * 300_000
    assert_repo_consistent(repo_sharded)


# -- P0-4: NULL shard repair is a typed error -----------------------------------


def test_null_shard_id_repair_rehomes(repo_sharded):
    """Locked semantics S2: with raw bytes available, a NULL shard locator is
    repaired by REHOMING — the payload is written to a writable shard and
    encodings.shard_id is updated in the same transaction. No raw sqlite
    errors, no junk shard."""
    put = repo_sharded.store.put_bytes(b"z" * 100, "raw").result
    row = repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile)
    with repo_sharded.backend.txn(write=True, attach_shard_id=int(row["shard_id"])) as conn:
        conn.execute(
            "DELETE FROM p.payload WHERE blob_key=? AND profile=?",
            (put.ref.blob_key, put.ref.profile),
        )
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=NULL WHERE blob_key=? AND profile=?",
            (put.ref.blob_key, put.ref.profile),
        )
    # Repair via rehoming: succeeds, locator repaired, content readable.
    repaired = repo_sharded.store.put_bytes(b"z" * 100, "raw").result
    row_after = repo_sharded.backend.get_encoding(repaired.ref.blob_key, repaired.ref.profile)
    assert row_after["shard_id"] is not None
    assert repo_sharded.backend.shard_path(int(row_after["shard_id"])).exists()
    assert repo_sharded.store.get_bytes(repaired.ref) == b"z" * 100


# -- P0-5: prepare_bytes binds blob_key to content ------------------------------


def test_prepare_bytes_rejects_forged_blob_key(repo_single):
    store = repo_single.store
    data = b"hello"
    real, *_ = store.identity.key_bytes(data)
    parts = real.split(":")
    fake = f"ikb1:{parts[1]}:{'ab' * 32}:{'cd' * 16}"  # same length, wrong hashes
    with pytest.raises(CorruptContent):
        store.prepare_bytes(data, "raw", blob_key=fake)
    # Nothing was written.
    with repo_single.backend.txn(write=False) as conn:
        assert conn.execute("SELECT COUNT(*) FROM encodings").fetchone()[0] == 0


def test_put_stream_still_accepts_its_own_key(repo_single):
    """The internal stream path passes the freshly-computed key: must work."""
    data = b"own-key" * 100
    result = repo_single.store.put_stream(io.BytesIO(data), profile="raw").result
    assert repo_single.store.get_bytes(result.ref) == data


# -- P0-6: backend create refuses existing --------------------------------------


def test_backend_create_refuses_existing(tmp_path):
    root = tmp_path / "r"
    SqliteBackend.create(root, "sqlite_single")
    with pytest.raises(InkpackError):
        SqliteBackend.create(root, "sqlite_single")
    sharded = tmp_path / "s"
    SqliteBackend.create(sharded, "sqlite_sharded")
    with pytest.raises(InkpackError):
        SqliteBackend.create(sharded, "sqlite_sharded")


# -- P1-2: profiles_from_config strict at top level -----------------------------


def test_profiles_from_config_rejects_non_dict():
    with pytest.raises(InkpackError):
        profiles_from_config(["not", "a", "dict"])
    with pytest.raises(InkpackError):
        profiles_from_config(None)


# -- P1-3: reencode skips bad targets and continues ------------------------------


def test_reencode_skips_missing_payload_and_continues(repo):
    store = repo.store
    a = store.put_bytes(b"a" * 100, "raw").result.ref
    b = store.put_bytes(b"b" * 100, "raw").result.ref
    delete_payload_row(repo, a)

    result = store.reencode([a, b]).result
    assert result.reencoded == 1
    assert result.skipped == 1
    assert store.get_bytes(b) == b"b" * 100


def test_reencode_skips_deleted_profile_and_continues(repo):
    put = repo.store.put_bytes(b"x" * 100, "raw").result.ref
    repo.set_profile(Profile("gone", "zstd", {"level": 3}))
    target = repo.store.put_bytes(b"y" * 100, "gone").result.ref
    repo.set_profiles({"raw": Profile("raw", "none", {})})  # profile "gone" removed
    result = repo.store.reencode([target, put]).result
    assert result.skipped == 1
    assert result.reencoded == 1
    assert repo.store.get_bytes(put) == b"x" * 100


# -- P1-4: compact options -------------------------------------------------------


def test_compact_shard_ids_rejected_in_single(repo_single):
    with pytest.raises(ValueError):
        repo_single.store.compact({"shard_ids": [1]}).result


def test_compact_empty_shard_ids_vacuums_index_only(repo_sharded, monkeypatch):
    vacuumed = []

    def spy(sid):
        vacuumed.append(int(sid))
        return "x"

    monkeypatch.setattr(repo_sharded.backend, "vacuum_shard", spy)
    result = repo_sharded.store.compact({"shard_ids": []}).result
    assert vacuumed == []
    assert len(result.targets) == 1  # index only


# -- P1-5: dedupe hit fails when the required dict is missing --------------------


def test_dedupe_hit_missing_dict_raises_not_silent_success(repo):
    train = repo.store.train_dict([b"hit-dict " * 60] * 6).result
    repo.set_profile(Profile("zstd_dict", "zstd", {"level": 6}, train.dict_id))
    put = repo.store.put_bytes(b"hit-dict " * 300, "zstd_dict").result
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM dicts WHERE dict_id=?", (train.dict_id,))
    with pytest.raises(MissingContent):
        repo.store.put_bytes(b"hit-dict " * 300, "zstd_dict").result
    # The encoding row is untouched (no rewrite on hit). The repo is
    # deliberately corrupt (encoding references a missing dict), so the
    # consistency helper is not applicable here.
    assert enc_row(repo, put.ref) is not None


# -- P1-6: missing chapter row is NotFound ---------------------------------------


def test_missing_chapter_row_is_notfound(repo):
    with pytest.raises(NotFound):
        repo.get_chapter_bytes(999_999)
    with pytest.raises(NotFound):
        repo.open_chapter(999_999)


# -- P1-7: list_chapters orders by order_key -------------------------------------


def test_list_chapters_order_by_order_key(repo):
    novel_id = repo.create_novel("Order")
    repo.upsert_chapter(novel_id, "10", b"ten", "raw")
    repo.upsert_chapter(novel_id, "2", b"two", "raw")
    repo.upsert_chapter(novel_id, "1", b"one", "raw")
    # Text sort: "1" < "10" < "2" (documented order_key semantics).
    assert [c.order_key for c in repo.list_chapters(novel_id)] == ["1", "10", "2"]


# -- P2-5: Operation.close releases resources -------------------------------------


def test_operation_close_releases_session(repo, connect_counter):
    for i in range(20):
        repo.store.put_bytes(f"close-{i}".encode(), profile="raw").result
    op = repo.store.verify()
    iterator = iter(op)
    assert next(iterator).kind == "start"  # session opens after the start event
    assert next(iterator).kind == "item"  # first item: session is now open
    assert connect_counter["live"] >= 1
    op.close()
    assert connect_counter["live"] == 0
    op.close()  # idempotent
    with pytest.raises(Cancelled):
        op.result


def test_operation_close_unstarted_is_safe(repo):
    op = repo.store.put_bytes(b"never", profile="raw")
    op.close()
    with pytest.raises(Cancelled):
        op.result


def test_operation_close_after_completion_is_noop(repo):
    op = repo.store.put_bytes(b"done", profile="raw")
    result = op.result
    op.close()
    assert op.result is result  # completed result preserved


# -- P2-6: decode output bound ----------------------------------------------------


def test_decode_output_bounded_by_raw_len(repo_single):
    """A zstd payload that decompresses beyond the blob_key's declared raw_len
    is corruption, and the bound prevents the decompression."""
    from inkpack.codec import blob_key_ikb1_bytes

    data = b"y" * 5000
    _, _, sha, blake = blob_key_ikb1_bytes(data)
    # Declare a raw_len far smaller than the real content: the decode bound
    # (max_output_size=raw_len) must reject the payload.
    forged = f"ikb1:100:{sha}:{blake}"
    encoded = __import__("zstandard").ZstdCompressor(level=3).compress(data)
    with repo_single.backend.txn(write=True) as conn:
        conn.execute(
            "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id,"
            " stored_len, checksum, shard_id, updated_at) VALUES(?, 'raw', 'zstd', '{}', NULL, ?, NULL, NULL, ?)",
            (forged, len(encoded), "now"),
        )
        conn.execute(
            "INSERT INTO payload(blob_key, profile, data) VALUES(?, 'raw', ?)", (forged, encoded)
        )
    with pytest.raises(CorruptContent):
        repo_single.store.get_bytes(ContentRef(forged, "raw"))
    verify = repo_single.store.verify().result
    assert verify.corrupt == 1


# -- §4.1 Option B: profile-referenced dicts are live ----------------------------


def test_gc_keeps_profile_referenced_dict(repo):
    train = repo.store.train_dict([b"pin " * 80] * 6).result
    repo.set_profile(Profile("pinned", "zstd", {"level": 3}, train.dict_id))
    # No encoding references the dict yet.
    gc = repo.store.gc(live=[]).result
    assert gc.dicts_deleted == 0  # profile reference keeps it alive
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT 1 FROM dicts WHERE dict_id=?", (train.dict_id,)).fetchone()
    assert row is not None
    # And the documented workflow still works afterwards.
    put = repo.store.put_bytes(b"pin " * 200, "pinned").result
    assert repo.store.get_bytes(put.ref) == b"pin " * 200
