"""Fail-first tests for the fourth review round (locked decisions D1-D12 and
H1-H5 / M6-M24 fixes).

Each test was designed to fail on the pre-fix code and locks the fixed
behavior:

- D1/H1  reads are one snapshot (BEGIN + ATTACH on the index connection)
- D3/H2  put commit point: dedupe hits re-probe payload+dict inside a write txn
- D5/H3  create_repo is all-or-nothing (temp dir + atomic rename)
- D2/H4  canonical shard filenames enforced on open; junk ignored
- H5     create refuses orphan payload shards
- M6     consumer exception closes the generator
- M7     reencode write-path errors skip the target
- M8     stored non-object codec_params_json is CorruptContent
- M9/D6  prepare_bytes has no key_tuple; stream re-binds
- M10    existing shards are migrated on open
- M11    GC aborts when the profiles config is not a mapping
- M12    open-time config validation (bool caps, min>cap, verify_on_read str)
- M13    no raw JSON/sqlite errors; broken layout is InkpackError
- M14    ATTACH of a vanished file is MissingContent
- M15    codec 'none' + zstd_dict_id is CorruptContent
- M16    bool novel/chapter ids rejected
- M17    ikb1 parse rejects trailing newline / leading zeros
- M18    ad-hoc reencode policy validated at call time
- M19    stream dedupe hit never materializes the spool
- M20    upsert uses one index connection
- M22    verify throttles item events
- M23    close of a never-started writer warns
- M24    tilde expansion; exists-check before profile validation
"""

from __future__ import annotations

import inspect
import io
import sqlite3
import warnings
from pathlib import Path

import pytest

from inkpack import (
    CorruptContent,
    InkpackError,
    MissingContent,
    NotFound,
    Profile,
    create_repo,
    open_repo,
)
from inkpack.blobstore import BlobStore
from inkpack.codec import IKB1, parse_blob_key_ikb1
from inkpack.sqlite import SqliteBackend, connect_file
from inkpack.types import Operation, OpEvent

from .conftest import assert_repo_consistent, enc_row, make_profiles, payload_bytes

PROFILES = {**make_profiles(), "zstd": Profile("zstd", "zstd", {"level": 3})}


@pytest.fixture(params=["sqlite_single", "sqlite_sharded"])
def repo(tmp_path, request):
    return create_repo(tmp_path / request.param, request.param, profiles=PROFILES)


# ---------------------------------------------------------------------------
# D1 / H1 — snapshot reads
# ---------------------------------------------------------------------------


def test_h1_get_bytes_reads_inside_a_transaction(repo, monkeypatch):
    ref = repo.store.put_bytes(b"body" * 20, "raw").result.ref
    begins: list[str] = []
    original_connect = sqlite3.connect

    class RecordingConnection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if str(sql).lstrip().upper().startswith("BEGIN"):
                begins.append(str(sql))
            return super().execute(sql, *args, **kwargs)

    def factory_connect(*args, **kwargs):
        kwargs["factory"] = RecordingConnection
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", factory_connect)
    assert repo.store.get_bytes(ref) == b"body" * 20
    assert begins, "get_bytes must BEGIN a snapshot before reading encodings+payload"


def test_h1_stale_row_plus_new_payload_is_not_a_public_success(repo):
    """Mechanism: decoding trusts the row it is given and reads the payload
    NOW; the public path reads both inside one fresh snapshot."""
    store = repo.store
    data = b"chapter prose " * 4000
    ref = store.put_bytes(data, "raw").result.ref
    with store.backend.session() as s:
        stale = s.query_one(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
            (ref.blob_key, ref.profile),
        )
    store.reencode([ref], options={"codec": "zstd", "params": {"level": 3}}).result
    # A stale row (codec 'none') paired with the NEW zstd payload must not
    # silently decode: the S3 length bound rejects it.
    with store.backend.session() as s:
        new_payload = payload_bytes(store, ref)
        with pytest.raises(CorruptContent):
            store._decode_from_row(s, stale, new_payload)
    # Public API returns the original bytes from a fresh snapshot.
    assert store.get_bytes(ref) == data


# ---------------------------------------------------------------------------
# D3 / H2 — put commit point
# ---------------------------------------------------------------------------


def test_h2_dedupe_persist_repairs_or_refuses_if_payload_vanished(repo):
    store = repo.store
    data = b"same-bytes"
    first = store.put_bytes(data, "raw").result
    prepared = store.prepare_bytes(data, "raw")
    assert prepared.enc is None

    with repo.backend.txn(write=True) as conn:
        if repo.backend.mode == "sqlite_single":
            conn.execute("DELETE FROM payload")
        else:
            sid = enc_row(repo, first.ref)["shard_id"]
            shard = connect_file(
                repo.backend.shard_path(int(sid)),
                repo.backend.busy_timeout_ms,
                repo.backend.synchronous,
            )
            try:
                shard.execute("DELETE FROM payload")
                shard.commit()
            finally:
                shard.close()

    with store.backend.session() as s:
        result = store._persist(s, prepared, data)

    assert store.get_bytes(result.ref) == data
    assert_repo_consistent(repo)


# ---------------------------------------------------------------------------
# D5 / H3 — create_repo is all-or-nothing
# ---------------------------------------------------------------------------


def test_h3_failed_create_leaves_no_marker_and_can_be_retried(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    real = SqliteBackend.create

    def create_then_fail(*args, **kwargs):
        real(*args, **kwargs)
        raise OSError("disk full")

    monkeypatch.setattr(SqliteBackend, "create", create_then_fail)
    with pytest.raises(OSError):
        create_repo(root, "sqlite_single", profiles=PROFILES)

    assert not (root / "repo.sqlite").exists()
    assert not (root / "index.sqlite").exists()
    assert not (root / "payload").exists()

    monkeypatch.undo()
    repo = create_repo(root, "sqlite_single", profiles=PROFILES)
    assert repo.get_profiles().keys() == PROFILES.keys()


# ---------------------------------------------------------------------------
# D2 / H4 — canonical shard filenames
# ---------------------------------------------------------------------------


def test_h4_noncanonical_shard_filename_is_typed_error_on_open(tmp_path):
    root = tmp_path / "sharded"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    src = repo.backend.shard_path(1)
    src.rename(src.with_name("shard-1.sqlite"))

    with pytest.raises(InkpackError, match=r"shard-1\.sqlite"):
        open_repo(root)


def test_h4_duplicate_numeric_names_are_refused(tmp_path):
    root = tmp_path / "sharded"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    canonical = repo.backend.shard_path(1)
    (canonical.parent / "shard-1.sqlite").write_bytes(canonical.read_bytes())
    with pytest.raises(InkpackError, match="shard"):
        open_repo(root)


def test_h4_junk_filenames_are_ignored(tmp_path):
    root = tmp_path / "sharded"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    ref = repo.store.put_bytes(b"body", "raw").result.ref
    (repo.backend.payload_dir / "shard-foo.sqlite").write_text("nope")
    (repo.backend.payload_dir / "notes.txt").write_text("x")
    repo2 = open_repo(root)
    assert repo2.store.get_bytes(ref) == b"body"
    assert repo2.backend.list_shards() == [1]


# ---------------------------------------------------------------------------
# D5 / H5 — create refuses orphan payload shards
# ---------------------------------------------------------------------------


def test_h5_create_refuses_orphan_payload_shards(tmp_path):
    root = tmp_path / "repo"
    payload = root / "payload"
    payload.mkdir(parents=True)
    conn = sqlite3.connect(payload / "shard-0007.sqlite")
    conn.execute("CREATE TABLE t(x)")
    conn.close()

    with pytest.raises(InkpackError, match="already exists"):
        create_repo(root, "sqlite_sharded", profiles=PROFILES)

    assert not (root / "index.sqlite").exists()


# ---------------------------------------------------------------------------
# D10 / M6 — consumer exception closes the generator
# ---------------------------------------------------------------------------


def test_m6_consumer_exception_propagates_and_op_stays_usable():
    """D10 with the iterator-object design (Issue 2/B): a consumer exception
    raised in the loop body propagates to the caller; the operation is not
    cancelled by it and can still complete via .result. The old generator-
    wrapper delivered GeneratorExit into __iter__; the real iterator object
    does not."""
    closed = {"n": 0}

    def factory():
        try:
            yield OpEvent(kind="start", op="verify")
            yield OpEvent(kind="item", op="verify")
            yield OpEvent(kind="done", op="verify")
            return 1
        finally:
            closed["n"] += 1

    op = Operation(factory)
    it = iter(op)
    assert next(it).kind == "start"
    with pytest.raises(RuntimeError, match="boom"):
        for ev in it:
            if ev.kind == "item":
                raise RuntimeError("boom")
    assert op.result == 1
    assert closed["n"] == 1


# ---------------------------------------------------------------------------
# M7 — reencode write-path errors skip the target
# ---------------------------------------------------------------------------


def test_m7_reencode_skips_corrupt_blob_catalog(repo):
    store = repo.store
    a = store.put_bytes(b"aaa", "raw").result.ref
    b = store.put_bytes(b"bbb", "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute("UPDATE blobs SET raw_len=0 WHERE blob_key=?", (a.blob_key,))

    result = store.reencode(
        [a, b], options={"codec": "zstd", "params": {"level": 3}}
    ).result
    assert result.targets == 2
    assert result.skipped == 1
    assert result.reencoded == 1
    assert store.get_bytes(b) == b"bbb"


# ---------------------------------------------------------------------------
# D7 / M8 — repair rejects non-object codec_params_json
# ---------------------------------------------------------------------------


def test_m8_repair_rejects_non_object_codec_params(repo):
    store = repo.store
    data = b"payload-bytes"
    ref = store.put_bytes(data, "zstd").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET codec_params_json=? WHERE blob_key=?",
            ("[]", ref.blob_key),
        )
        if repo.backend.mode == "sqlite_single":
            conn.execute("DELETE FROM payload")
    if repo.backend.mode == "sqlite_sharded":
        sid = enc_row(repo, ref)["shard_id"]
        shard = connect_file(
            repo.backend.shard_path(int(sid)),
            repo.backend.busy_timeout_ms,
            repo.backend.synchronous,
        )
        try:
            shard.execute("DELETE FROM payload")
            shard.commit()
        finally:
            shard.close()

    with pytest.raises(CorruptContent, match="codec_params"):
        store.put_bytes(data, "zstd").result


# ---------------------------------------------------------------------------
# D6 / M9 — no public key_tuple; stream re-binds
# ---------------------------------------------------------------------------


def test_m9_prepare_bytes_has_no_key_tuple():
    assert "key_tuple" not in inspect.signature(BlobStore.prepare_bytes).parameters


def test_m9_prepare_bytes_rejects_forged_key(repo):
    """D6/M9 regression lock (unmocked): a caller-supplied blob_key is bound
    to the content, so a forged key with the right length is refused."""
    store = repo.store
    real, *_ = IKB1.key_bytes(b"actual body")
    raw_len, _sha, _blake = IKB1.parse(real)
    forged = f"ikb1:{raw_len}:{'ab' * 32}:{'cd' * 16}"
    with pytest.raises(CorruptContent):
        store.prepare_bytes(b"forged body", "raw", blob_key=forged)
    with pytest.raises(CorruptContent):
        store.prepare_bytes(b"forged body", "raw", blob_key=real)


# ---------------------------------------------------------------------------
# M10 — existing shards are migrated on open
# ---------------------------------------------------------------------------


def test_m10_open_migrates_existing_shard_payload_schema(tmp_path, monkeypatch):
    root = tmp_path / "sharded"
    create_repo(root, "sqlite_sharded", profiles=PROFILES)
    from inkpack import sqlite as sqlite_mod

    monkeypatch.setattr(
        sqlite_mod,
        "PAYLOAD_MIGRATIONS",
        (*sqlite_mod.PAYLOAD_MIGRATIONS, (2, "CREATE TABLE IF NOT EXISTS payload_meta(x INTEGER);")),
    )
    repo = open_repo(root)
    for sid in repo.backend.list_shards():
        conn = connect_file(
            repo.backend.shard_path(sid),
            repo.backend.busy_timeout_ms,
            repo.backend.synchronous,
        )
        try:
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "payload_meta" in names
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# M11 — GC aborts when the profiles config is not a mapping
# ---------------------------------------------------------------------------


def test_m11_gc_aborts_if_profiles_config_is_not_a_mapping(repo):
    store = repo.store
    trained = store.train_dict([b"sample prose " * 80] * 5).result
    repo.set_profile(Profile("zstd_d", "zstd", {"level": 6}, trained.dict_id))
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE repo_config SET value_json=? WHERE key='profiles'",
            ("[]",),
        )
    with pytest.raises(InkpackError):
        store.gc(live=[]).result
    assert repo.backend.get_dict(trained.dict_id) is not None


# ---------------------------------------------------------------------------
# D8 / M12 — open-time config validation
# ---------------------------------------------------------------------------


def test_m12_bool_shard_cap_is_rejected(tmp_path):
    root = tmp_path / "s"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    repo.backend.config_set("shard_cap_bytes", True)
    repo.backend.config_set("shard_min_bytes", True)
    with pytest.raises(InkpackError, match="shard_"):
        open_repo(root)


def test_m12_min_exceeding_cap_is_rejected(tmp_path):
    root = tmp_path / "s"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    repo.backend.config_set("shard_cap_bytes", 1000)
    repo.backend.config_set("shard_min_bytes", 5000)
    with pytest.raises(InkpackError, match="shard_min"):
        open_repo(root)


def test_m12_verify_on_read_string_is_rejected(tmp_path):
    root = tmp_path / "s"
    repo = create_repo(root, "sqlite_single", profiles=PROFILES)
    repo.backend.config_set("verify_on_read", "false")
    with pytest.raises(InkpackError, match="verify_on_read"):
        open_repo(root)


def test_m12_missing_profiles_says_missing(tmp_path):
    root = tmp_path / "s"
    repo = create_repo(root, "sqlite_single", profiles=PROFILES)
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM repo_config WHERE key='profiles'")
    with pytest.raises(InkpackError, match="profiles missing"):
        open_repo(root)


# ---------------------------------------------------------------------------
# D9 / M13 — typed-error leaks
# ---------------------------------------------------------------------------


def test_m13_corrupt_config_json_is_inkpack_error(repo):
    with repo.backend.txn(write=True) as conn:
        conn.execute("UPDATE repo_config SET value_json='{' WHERE key='profiles'")
    with pytest.raises(InkpackError, match="profiles"):
        repo.backend.config_get("profiles")


def test_m13_create_on_a_file(tmp_path):
    path = tmp_path / "notadir"
    path.write_text("nope")
    with pytest.raises(InkpackError):
        create_repo(path, "sqlite_single", profiles=PROFILES)


def test_m13_index_without_payload_is_not_notfound(tmp_path):
    import shutil

    root = tmp_path / "s"
    create_repo(root, "sqlite_sharded", profiles=PROFILES)
    shutil.rmtree(root / "payload")
    with pytest.raises(InkpackError) as excinfo:
        open_repo(root)
    assert not isinstance(excinfo.value, NotFound)


# ---------------------------------------------------------------------------
# D9 / M14 — ATTACH of a vanished file is MissingContent
# ---------------------------------------------------------------------------


def test_m14_attach_unable_to_open_is_missingcontent(repo, monkeypatch):
    if repo.backend.mode != "sqlite_sharded":
        pytest.skip()
    from inkpack import sqlite as sqlite_mod

    store = repo.store
    prepared = store.prepare_bytes(b"unique-miss-path" * 10, "raw")
    assert prepared.enc is not None

    real_uri = sqlite_mod.sqlite_uri

    def uri_unlinking_shards(path: Path, mode: str) -> str:
        if path.name.startswith("shard-") and path.suffix == ".sqlite":
            path.unlink(missing_ok=True)
        return real_uri(path, mode)

    monkeypatch.setattr(sqlite_mod, "sqlite_uri", uri_unlinking_shards)
    with pytest.raises(MissingContent), store.backend.session() as s:
        store._persist(s, prepared, b"unique-miss-path" * 10)


# ---------------------------------------------------------------------------
# D7 / M15 — codec none + zstd_dict_id is CorruptContent
# ---------------------------------------------------------------------------


def test_m15_none_codec_with_dict_id_is_corrupt(repo):
    store = repo.store
    ref = store.put_bytes(b"raw-body", "raw").result.ref
    trained = store.train_dict([b"sample prose " * 80] * 5).result
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET zstd_dict_id=? WHERE blob_key=?",
            (trained.dict_id, ref.blob_key),
        )
    with pytest.raises(CorruptContent):
        store.get_bytes(ref)
    v = store.verify().result
    assert v.corrupt == 1 and v.missing == 0


# ---------------------------------------------------------------------------
# M16 — bool novel/chapter ids rejected
# ---------------------------------------------------------------------------


def test_m16_bool_ids_are_rejected(repo):
    repo.create_novel("n")
    with pytest.raises(TypeError):
        repo.get_novel(True)
    with pytest.raises(TypeError):
        repo.list_chapters(True)
    with pytest.raises(TypeError):
        repo.get_chapter(True)
    with pytest.raises(TypeError):
        repo.delete_novel(True)


# ---------------------------------------------------------------------------
# S4 / M17 — ikb1 parse strictness
# ---------------------------------------------------------------------------


def test_m17_parse_rejects_trailing_newline():
    key, *_ = IKB1.key_bytes(b"x")
    with pytest.raises(ValueError):
        parse_blob_key_ikb1(key + "\n")


def test_parse_accepts_s4_leading_zeros():
    """Issue 14: S4 allows leading zeros in the length field."""
    key = "ikb1:01:" + ("ab" * 32) + ":" + ("cd" * 16)
    raw_len, sha, blake = parse_blob_key_ikb1(key)
    assert raw_len == 1
    assert sha == "ab" * 32
    assert blake == "cd" * 16


# ---------------------------------------------------------------------------
# M18 — ad-hoc reencode policy validated eagerly
# ---------------------------------------------------------------------------


def test_m18_bad_adhoc_level_fails_before_operation(repo):
    with pytest.raises(ValueError, match="level"):
        repo.store.reencode([], options={"codec": "zstd", "params": {"level": 99}})


# ---------------------------------------------------------------------------
# D6 / M19 — stream dedupe hit does not materialize the spool
# ---------------------------------------------------------------------------


def test_m19_dedupe_hit_does_not_materialize_spool(repo, monkeypatch):
    store = repo.store
    data = b"x" * (3 * 1024 * 1024)
    store.put_bytes(data, "raw").result
    reads = {"n": 0}

    import tempfile

    orig = tempfile.SpooledTemporaryFile

    class SpySpool(orig):
        def read(self, *a, **k):
            chunk = super().read(*a, **k)
            reads["n"] += len(chunk)
            return chunk

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", SpySpool)
    store.put_stream(io.BytesIO(data), "raw").result
    assert reads["n"] == 0


# ---------------------------------------------------------------------------
# M20 — upsert uses one index connection
# ---------------------------------------------------------------------------


def test_m20_upsert_uses_one_index_connection(repo, monkeypatch):
    repo.store.put_bytes(b"warm-blob-limit-cache", "raw").result
    novel_id = repo.create_novel("n")
    opens = {"n": 0}
    real = repo.backend.connect_index

    def counted():
        opens["n"] += 1
        return real()

    monkeypatch.setattr(repo.backend, "connect_index", counted)
    repo.upsert_chapter(novel_id, "ch-1", b"body" * 10, "raw")
    assert opens["n"] == 1


# ---------------------------------------------------------------------------
# M22 — verify throttles item events
# ---------------------------------------------------------------------------


def test_m22_verify_throttles_item_events(repo):
    store = repo.store
    for i in range(30):
        store.put_bytes(f"c{i}".encode(), "raw").result
    items = [e for e in store.verify() if e.kind == "item"]
    assert 0 < len(items) < 30
    assert store.verify().result.checked == 30


# ---------------------------------------------------------------------------
# D10 / M23 — close of a never-started writer warns
# ---------------------------------------------------------------------------


def test_m23_close_of_never_started_writer_warns(repo):
    op = repo.store.put_bytes(b"lost", "raw")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        op.close()
    assert any(
        "never started" in str(w.message).lower() or "abandoned" in str(w.message).lower()
        for w in caught
    )
    assert repo.store.has_blob(IKB1.key_bytes(b"lost")[0]) is False


# ---------------------------------------------------------------------------
# D11 / M24 — factory footguns
# ---------------------------------------------------------------------------


def test_m24_tilde_is_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    create_repo("~/novels", "sqlite_single", profiles=PROFILES)
    assert (tmp_path / "novels" / "repo.sqlite").exists()


def test_m24_existing_repo_beats_bad_profiles(tmp_path):
    root = tmp_path / "r"
    create_repo(root, "sqlite_single", profiles=PROFILES)
    with pytest.raises(InkpackError, match="already exists"):
        create_repo(root, "sqlite_single", profiles=None)


def test_m24_set_profile_requires_existing_dict(repo):
    with pytest.raises(MissingContent):
        repo.set_profile(Profile("d", "zstd", {"level": 6}, "ikd1:" + "ab" * 32))
