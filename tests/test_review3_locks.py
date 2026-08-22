"""Fail-first tests for the third review round (locked semantics S1-S9 and
P0/P1/P2 fixes).

Each test here was designed to fail on the pre-fix code and locks the fixed
behavior:

- S1/P0-OP-1/2  Operation abandonment is deterministic (close prevents start,
                 context manager, abandoned iteration -> Cancelled)
- S2/P0-ATTACH-1 ATTACH is require-mode (never creates files); repair rehomes
- P0-URI-1       paths with spaces/#/%/? work (URI-escaped)
- P0-PREP-1      prepare has no filesystem side effects
- P0-CLOSE-1     connections are explicitly closed
- P0-ZSTD-1/P0-NONE-1 decode output bound enforced
- P1-UP-1        dedupe-hit upsert re-probes the payload in-txn
- P1-CFG-1       set_profile is an atomic read-modify-write
- P1-DEL-1       delete_novel uses subqueries (no unbounded IN)
- P1-REENC-1     reencode oversize skips the target
- P1-SHARD-1     corrupt shard -> CorruptContent (not raw sqlite error)
- P1-LIST-1/S9   list_shards ignores malformed filenames
- P1-PARSE-1/S4  ikb1 parse is strict ASCII canonical
- P1-COMPACT-1/S5 compact shard_ids type is strict
- P2-HASH-1      put_stream never re-hashes the spooled bytes
- P2-METRIC-1    stream upsert dedupe hit reports stored_len
- P2-OPT-1       options validated at call time
- P2-EXISTS-1    backend payload_exists is a SELECT 1 probe
- S6/S7/S8       entity ids, chapter keys, busy mapping on open
"""

from __future__ import annotations

import io
import sqlite3

import pytest

from inkpack import (
    Busy,
    Cancelled,
    ContentRef,
    CorruptContent,
    InkpackError,
    MissingContent,
    NotFound,
    Profile,
    create_repo,
    open_repo,
)
from inkpack.codec import blob_key_ikb1_bytes, parse_blob_key_ikb1
from inkpack.sqlite import SqliteBackend, connect_file

from .conftest import assert_repo_consistent, delete_payload_row, make_profiles

# -- S1 / P0-OP-1: close() prevents work from starting ------------------------


def test_operation_close_prevents_start(repo):
    data = b"must-not-land"
    from inkpack.codec import IKB1

    blob_key, *_ = IKB1.key_bytes(data)

    op = repo.store.put_bytes(data, profile="raw")
    op.close()

    assert list(op) == []  # must not start the generator
    with pytest.raises(Cancelled):
        _ = op.result
    assert repo.store.has_blob(blob_key) is False


# -- S1 / P0-OP-2: abandoning iteration is deterministic -----------------------


def test_operation_context_manager_abandons_deterministically(repo):
    repo.store.put_bytes(b"ctx", profile="raw").result
    op = repo.store.verify()
    with op:
        iterator = iter(op)
        next(iterator)  # start and yield one event
    # The with-block's __exit__ closed the operation: deterministic Cancelled.
    with pytest.raises(Cancelled):
        _ = op.result


def test_abandoned_iteration_raises_cancelled(repo):
    repo.store.put_bytes(b"abandon", profile="raw").result
    op = repo.store.verify()
    it = iter(op)
    next(it)  # start and yield one event
    it.close()  # explicit abandonment of the iterator
    with pytest.raises(Cancelled):
        _ = op.result


def test_operation_context_manager_success_path(repo):
    """A completed operation inside `with` still yields its result."""
    with repo.store.put_bytes(b"success", profile="raw") as op:
        result = op.result
    assert result.raw_len == 7


# -- S2 / P0-ATTACH-1: ATTACH is require-mode; repair rehomes ------------------


def test_attach_requires_existing_shard(repo_sharded):
    """ATTACH of a missing shard raises MissingContent and creates nothing."""
    backend = repo_sharded.backend
    missing_id = 9999
    assert not backend.shard_path(missing_id).exists()
    with pytest.raises(MissingContent), backend.txn(write=True, attach_shard_id=missing_id):
        pass  # pragma: no cover
    assert not backend.shard_path(missing_id).exists()


def _payload_table_exists(path, backend) -> bool:
    conn = connect_file(path, backend.busy_timeout_ms, backend.synchronous, create=False)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='payload'"
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def test_repair_rehomes_and_never_leaves_junk_shard(repo_sharded):
    """S2: put-repair with a missing shard file succeeds via rehoming, and any
    shard file that appears has a real payload schema (never a junk file)."""
    store = repo_sharded.store
    backend = repo_sharded.backend

    put = store.put_bytes(b"hello" * 50, "raw").result
    row = backend.get_encoding(put.ref.blob_key, put.ref.profile)
    backend.shard_path(int(row["shard_id"])).unlink()

    before = set(backend.payload_dir.glob("shard-*.sqlite"))

    put2 = store.put_bytes(b"hello" * 50, "raw").result  # must rehome, not leak
    row2 = backend.get_encoding(put2.ref.blob_key, put2.ref.profile)
    assert backend.shard_path(int(row2["shard_id"])).exists()

    after = set(backend.payload_dir.glob("shard-*.sqlite"))
    for path in after - before:
        assert _payload_table_exists(path, backend), f"junk shard created: {path}"
    assert store.get_bytes(put2.ref) == b"hello" * 50
    assert_repo_consistent(repo_sharded)


def test_upsert_repair_rehomes_when_shard_missing(repo_sharded):
    """S2: the atomic upsert path rehomes too."""
    novel_id = repo_sharded.create_novel("Rehome")
    body = b"upsert-rehome " * 100
    chapter_id = repo_sharded.upsert_chapter(novel_id, "1", body, "raw")
    row = repo_sharded.backend.get_encoding(
        repo_sharded.get_chapter(chapter_id).blob_key, "raw"
    )
    repo_sharded.backend.shard_path(int(row["shard_id"])).unlink()

    repo_sharded.upsert_chapter(novel_id, "1", body, "raw")
    assert repo_sharded.get_chapter_bytes(chapter_id) == body
    assert_repo_consistent(repo_sharded)


# -- P0-URI-1: unusual paths ----------------------------------------------------


@pytest.mark.parametrize("name", ["my novels", "hash#tag", "50%off"])
def test_create_open_unusual_paths(tmp_path, name):
    root = tmp_path / name
    repo = create_repo(root, backend_mode="sqlite_single", profiles=make_profiles())
    novel_id = repo.create_novel("t")
    chapter_id = repo.upsert_chapter(novel_id, "1", b"hello", "raw")
    reopened = open_repo(root)
    assert reopened.get_chapter_bytes(chapter_id) == b"hello"


def test_create_open_question_mark_path(tmp_path):
    root = tmp_path / "novels?backup"
    repo = create_repo(root, backend_mode="sqlite_single", profiles=make_profiles())
    repo.create_novel("t")
    open_repo(root)  # must not raise


# -- P0-PREP-1: prepare has no filesystem side effects -------------------------


def test_prepare_has_no_filesystem_side_effects(repo_sharded, monkeypatch):
    from inkpack.blobstore import BlobStore

    backend = repo_sharded.backend
    store = repo_sharded.store

    backend._write_shard_id = None
    backend.shard_cap_bytes = 1  # force rollover if routing ran early

    before = set(backend.payload_dir.glob("shard-*.sqlite"))

    def boom(self, stored_len, conn=None):
        raise ValueError("limit")

    monkeypatch.setattr(BlobStore, "check_blob_limit", boom)

    with pytest.raises(ValueError, match="limit"):
        store.put_bytes(b"hello" * 100, "raw").result

    after = set(backend.payload_dir.glob("shard-*.sqlite"))
    assert after == before, "prepare caused shard creation despite failing before the write txn"


# -- P0-CLOSE-1: connections are explicitly closed -----------------------------


def test_compact_closes_connections(repo_sharded, monkeypatch):
    import inkpack.sqlite as sql

    repo_sharded.store.put_bytes(b"x" * 100, "raw").result

    opened = []
    real = sql.connect_file

    def spy(*args, **kwargs):
        conn = real(*args, **kwargs)
        opened.append(conn)  # keep alive so GC cannot close it
        return conn

    monkeypatch.setattr(sql, "connect_file", spy)
    repo_sharded.store.compact().result

    import sqlite3 as _sqlite3

    for conn in opened:
        with pytest.raises(_sqlite3.Error):
            conn.execute("SELECT 1")  # closed connections reject statements


# -- P0-ZSTD-1 / P0-NONE-1: decode output bound ---------------------------------


def test_zstd_unknown_content_size_rejected(repo_single):
    """S3: a bounded decode of a frame without a declared content size is
    CorruptContent (cannot bound the allocation)."""
    import zstandard as z

    from inkpack.codec import blob_key_ikb1_bytes

    data = b"x" * 1000
    key, _, _sha, _blake = blob_key_ikb1_bytes(data)
    enc = z.ZstdCompressor(level=3, write_content_size=False).compress(data)
    assert z.frame_content_size(enc) < 0  # no declared size
    with repo_single.backend.txn(write=True) as conn:
        conn.execute(
            "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id,"
            " stored_len, checksum, shard_id, updated_at) VALUES(?, 'raw', 'zstd', '{}', NULL, ?, NULL, NULL, ?)",
            (key, len(enc), "now"),
        )
        conn.execute(
            "INSERT INTO payload(blob_key, profile, data) VALUES(?, 'raw', ?)", (key, enc)
        )
    with pytest.raises(CorruptContent):
        repo_single.store.get_bytes(ContentRef(key, "raw"))
    assert repo_single.store.verify().result.corrupt == 1


def test_codec_none_length_mismatch_is_corrupt(repo):
    """S3: codec='none' payload length must equal the blob_key's raw_len."""
    from .conftest import set_payload

    put = repo.store.put_bytes(b"len-bound", profile="raw").result
    set_payload(repo, put.ref, b"different-length-bytes")
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(put.ref)
    assert repo.store.verify().result.corrupt == 1


# -- P1-UP-1: dedupe-hit upsert re-probes the payload in-txn ---------------------


def test_dedupe_hit_upsert_checks_payload_in_txn(repo):

    novel_id = repo.create_novel("n")
    body = b"same-bytes"
    repo.upsert_chapter(novel_id, "a", body, "raw")

    prepared = repo.store.prepare_bytes(body, "raw")
    assert prepared.enc is None

    delete_payload_row(repo, ContentRef(prepared.blob_key, prepared.profile))

    with pytest.raises((InkpackError, MissingContent)):
        repo.backend.upsert_chapter_with_content(
            novel_id=novel_id,
            order_key="b",
            blob_key=prepared.blob_key,
            raw_len=prepared.raw_len,
            created_at=repo.backend.now(),
            profile=prepared.profile,
            media_type=None,
            charset=None,
            meta=None,
            enc=None,
            shard_id=None,
        )

    assert all(c.order_key != "b" for c in repo.list_chapters(novel_id))


# -- P1-CFG-1: set_profile is an atomic read-modify-write -----------------------


def test_config_update_is_atomic_merge(repo):
    repo.backend.config_set("list", [1])
    result = repo.backend.config_update("list", lambda current: (current or []) + [2])
    assert result == [1, 2]
    assert repo.backend.config_get("list") == [1, 2]
    # A failing update rolls back: no partial write.
    with pytest.raises(ValueError):
        repo.backend.config_update("list", lambda current: (_ for _ in ()).throw(ValueError("boom")))
    assert repo.backend.config_get("list") == [1, 2]


def test_set_profile_merges_not_replaces(repo):
    repo.set_profile(Profile(name="extra1", codec="none", params={}))
    repo.set_profile(Profile(name="extra2", codec="none", params={}))
    profiles = repo.get_profiles()
    assert "raw" in profiles and "extra1" in profiles and "extra2" in profiles


# -- P1-DEL-1: delete_novel scales to many chapters -----------------------------


def test_delete_novel_many_chapters(repo):
    novel_id = repo.create_novel("Many")
    for i in range(1200):
        repo.upsert_chapter(novel_id, f"{i:05d}", f"body-{i}".encode() * 5, "raw")
    repo.delete_novel(novel_id)
    with pytest.raises(NotFound):
        repo.get_novel(novel_id)
    with repo.backend.txn(write=False) as conn:
        assert conn.execute("SELECT COUNT(*) FROM chapters").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0] == 0
    assert_repo_consistent(repo)


# -- P1-REENC-1: reencode oversize skips the target -----------------------------


def test_reencode_oversize_skips_target(repo, monkeypatch):
    from inkpack.blobstore import BlobStore

    a = repo.store.put_bytes(b"a" * 100, "raw").result.ref
    b = repo.store.put_bytes(b"b" * 100, "raw").result.ref
    calls = {"n": 0}
    original = BlobStore.check_blob_limit

    def boom(self, stored_len, conn=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("too big")
        return original(self, stored_len, conn)

    monkeypatch.setattr(BlobStore, "check_blob_limit", boom)
    result = repo.store.reencode([a, b]).result
    assert result.skipped == 1
    assert result.reencoded == 1
    assert repo.store.get_bytes(b) == b"b" * 100


# -- P1-SHARD-1: corrupt shard -> CorruptContent ---------------------------------


def test_corrupt_shard_is_corrupt(repo_sharded):
    put = repo_sharded.store.put_bytes(b"hello" * 100, "raw").result
    row = repo_sharded.backend.get_encoding(put.ref.blob_key, put.ref.profile)
    path = repo_sharded.backend.shard_path(int(row["shard_id"]))
    path.write_bytes(b"not a sqlite db")

    with pytest.raises(CorruptContent):
        repo_sharded.store.get_bytes(put.ref)


# -- P1-LIST-1 / S9: list_shards ignores malformed files ------------------------


def test_list_shards_ignores_malformed(repo_sharded):
    payload_dir = repo_sharded.backend.payload_dir
    (payload_dir / "shard-bad.sqlite").write_bytes(b"")
    (payload_dir / "shard-xyz.sqlite").write_bytes(b"")
    (payload_dir / "notes.txt").write_bytes(b"")
    ids = repo_sharded.backend.list_shards()
    assert all(isinstance(i, int) for i in ids)
    # open_repo must not crash on stray files either.
    from inkpack import open_repo

    reopened = open_repo(repo_sharded.backend.root)
    assert reopened.backend.list_shards() == ids


# -- P1-PARSE-1 / S4: strict ikb1 parse ------------------------------------------


def test_ikb1_parse_strict():
    key, raw_len, sha, blake = blob_key_ikb1_bytes(b"abc")
    assert parse_blob_key_ikb1(key) == (raw_len, sha, blake)
    # Unicode digits rejected (e.g. Arabic-Indic ٣).
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"ikb1:\u0663:{sha}:{blake}")
    # Uppercase hex rejected.
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"ikb1:{raw_len}:{sha.upper()}:{blake}")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"ikb1:{raw_len}:{sha}:{blake.upper()}")
    # Other prefixes / shapes rejected.
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"IKB1:{raw_len}:{sha}:{blake}")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"ikb1:{raw_len}:{sha}")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(f"ikb1:0x{raw_len}:{sha}:{blake}")


# -- P1-COMPACT-1 / S5: compact shard_ids type is strict --------------------------


def test_compact_shard_ids_type_strict(repo_sharded):
    with pytest.raises(TypeError):
        repo_sharded.store.compact({"shard_ids": "12"})  # string iterates chars
    with pytest.raises(TypeError):
        repo_sharded.store.compact({"shard_ids": [1, True]})
    with pytest.raises(TypeError):
        repo_sharded.store.compact({"shard_ids": [1.5]})
    # tuple is fine; unknown ids still rejected.
    with pytest.raises(ValueError):
        repo_sharded.store.compact({"shard_ids": (1, 999)})


# -- P2-HASH-1: put_stream does not re-hash --------------------------------------


def test_put_stream_no_double_hash(repo, monkeypatch):
    data = b"no-rehash " * 5000
    calls = {"n": 0}
    original = repo.store.identity.key_bytes

    def spy(bytes_data):
        calls["n"] += 1
        return original(bytes_data)

    monkeypatch.setattr(repo.store.identity, "key_bytes", spy)
    result = repo.store.put_stream(io.BytesIO(data), profile="raw").result
    # D6/M9: the streaming hasher computes the identity; a miss RE-BINDS with
    # identity.key_bytes(raw) exactly once (never re-hashes inside prepare).
    assert calls["n"] == 1
    assert repo.store.get_bytes(result.ref) == data


# -- P2-METRIC-1: stream upsert dedupe hit reports bytes_out ----------------------


def test_upsert_chapter_stream_dedupe_reports_bytes_out(repo):
    novel_id = repo.create_novel("n")
    repo.upsert_chapter(novel_id, "ch-1", b"abc" * 10, "raw")

    op = repo.upsert_chapter_stream(io.BytesIO(b"abc" * 10), novel_id, "ch-2", "raw")
    events = list(op)
    done = next(e for e in events if e.kind == "done")
    assert done.metrics["bytes_out"] == 30  # stored_len, never 0


# -- P2-OPT-1: options validated at call time -------------------------------------


def test_options_validated_at_call_time(repo):
    # compact
    with pytest.raises(ValueError):
        repo.store.compact(options={"nope": 1})  # no .result, no iteration
    # reencode
    with pytest.raises(ValueError):
        repo.store.reencode([], options={"nope": 1})
    with pytest.raises(TypeError):
        repo.store.reencode([], options={"params": "not-a-dict"})
    # train_dict
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x" * 100] * 6, options={"nope": 1})
    with pytest.raises(ValueError):
        repo.store.train_dict([b"x" * 100] * 6, options={"dict_size": 10})


# -- P2-EXISTS-1: backend payload_exists is a SELECT 1 probe ----------------------


def test_payload_exists_does_not_fetch_blob(repo_single, monkeypatch):
    ref = repo_single.store.put_bytes(b"abc" * 10, "raw").result.ref

    def boom(*args, **kwargs):
        raise AssertionError("must not fetch the whole blob")

    monkeypatch.setattr(repo_single.backend, "get_payload", boom)
    with repo_single.backend.session() as s:
        assert s.payload_exists(ref.blob_key, ref.profile, None) is True


# -- S6: entity id normalization ---------------------------------------------------


def test_entity_id_rejects_negative_and_bool(repo):
    novel_id = repo.create_novel("S6")
    with pytest.raises(ValueError):
        repo.meta_set("novel", -1, "k", 1)
    with pytest.raises(ValueError):
        repo.meta_set("novel", True, "k", 1)
    with pytest.raises(ValueError):
        repo.meta_get("novel", -5, "k")
    # Valid non-negative ints and canonical strings work.
    repo.meta_set("novel", novel_id, "k", 1)
    assert repo.meta_get("novel", str(novel_id), "k") == 1


# -- S7: chapter_key type -----------------------------------------------------------


def test_chapter_key_rejects_none_and_bool(repo):
    novel_id = repo.create_novel("S7")
    with pytest.raises(TypeError):
        repo.upsert_chapter(novel_id, None, b"x", "raw")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        repo.upsert_chapter(novel_id, True, b"x", "raw")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        repo.upsert_chapter_stream(io.BytesIO(b"x"), novel_id, None, "raw")  # type: ignore[arg-type]
    assert repo.list_chapters(novel_id) == []  # nothing stored
    # int and str keys both work.
    a = repo.upsert_chapter(novel_id, 7, b"seven", "raw")
    b = repo.upsert_chapter(novel_id, "7", b"seven-v2", "raw")
    assert a == b


# -- S8: busy mapped on open/migrate/validation paths ------------------------------


def test_open_repo_busy_mapped(tmp_path):
    path = tmp_path / "busy-open"
    create_repo(path, backend_mode="sqlite_single", profiles=make_profiles())
    # Force open-time migrations to WRITE: reset user_version so the index
    # migration re-runs (CREATE TABLE IF NOT EXISTS is a write).
    raw = sqlite3.connect(str(path / "repo.sqlite"))
    raw.execute("PRAGMA user_version=0")
    raw.commit()
    raw.close()
    lock = sqlite3.connect(str(path / "repo.sqlite"))
    lock.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(Busy):
            open_repo(path, pragmas={"busy_timeout_ms": 50})
    finally:
        lock.rollback()
        lock.close()
    # After release, open works again (migrations idempotent).
    reopened = open_repo(path)
    assert reopened.get_profiles() == make_profiles()


def test_backend_open_busy_mapped_sharded(tmp_path):
    SqliteBackend.create(tmp_path / "bs", mode="sqlite_sharded")
    lock = sqlite3.connect(str(tmp_path / "bs" / "index.sqlite"))
    lock.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(Busy):
            SqliteBackend.open(tmp_path / "bs", mode="sqlite_sharded", pragmas={"busy_timeout_ms": 50})
    finally:
        lock.rollback()
        lock.close()
