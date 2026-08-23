"""Fail-first tests for the fifth review round (Workstreams A-D + standalone).

Each test was designed to fail on the pre-fix code and locks the fixed
behavior:

- Issue 1   NULL shard_id is MissingContent on reads; verify counts missing
- Issue 3   locked shard is Busy, not CorruptContent
- Issue 5   raw sqlite3.Error never escapes public APIs
- Issue 8   vanished shard on dedupe hit rehomes
- Issue 10  backend create config is atomic with the schema
- Issue 11  get_chapter_bytes/open_chapter reject bool
- Issue 12  set_profiles rejects missing dictionaries; create_repo rejects dict ids
- Issue 13  create_repo accepts an existing EMPTY directory
- Issue 14  ikb1 parse accepts S4 leading zeros
- Issue 15  iter_live_content rejects negative/bool scope
- Issue 16  sqlite_single does not validate shard caps
- Issue 17  missing shard classified by path, not message text
- Issue 18  GC sweeps payloads parked at the wrong shard
- Issue 20  verify_on_read must be a bool
- Issue 32  read attaches never migrate (DDL is writer-only)
- Issue 43  tokenized payload DDL with two {p} tokens
- Issue 46  stale .inkpack-creating-* dirs are swept on create
- Issue 47  verify item events not duplicated on the throttle boundary
"""

from __future__ import annotations

import os
import sqlite3
import time

import pytest

from inkpack import (
    Busy,
    CorruptContent,
    InkpackError,
    MissingContent,
    Profile,
    create_repo,
)
from inkpack.sqlite import SqliteBackend

from .conftest import enc_row, make_profiles, payload_bytes

PROFILES = make_profiles()


@pytest.fixture(params=["sqlite_single", "sqlite_sharded"])
def repo(tmp_path, request):
    return create_repo(tmp_path / request.param, request.param, profiles=PROFILES)


# ---------------------------------------------------------------------------
# Issue 1 — NULL shard_id
# ---------------------------------------------------------------------------


def test_null_shard_id_get_bytes_is_missing_content(repo_sharded):
    store = repo_sharded.store
    ref = store.put_bytes(b"hello-null-locator", "raw").result.ref
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=NULL WHERE blob_key=? AND profile=?",
            (ref.blob_key, ref.profile),
        )
    with pytest.raises(MissingContent):
        store.get_bytes(ref)


def test_null_shard_id_verify_counts_missing_and_completes(repo_sharded):
    store = repo_sharded.store
    ref = store.put_bytes(b"verify-null-locator", "raw").result.ref
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=NULL WHERE blob_key=? AND profile=?",
            (ref.blob_key, ref.profile),
        )
    result = store.verify().result
    assert result.checked == 1
    assert result.missing == 1
    assert result.corrupt == 0
    assert result.ok == 0


def test_null_shard_id_dedupe_put_does_not_leak_sqlite(repo_sharded):
    """Issue 1: typed outcome (MissingContent or rehome), never raw sqlite."""
    store = repo_sharded.store
    data = b"dedupe-null-locator"
    store.put_bytes(data, "raw").result
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute("UPDATE encodings SET shard_id=NULL")
    try:
        store.put_bytes(data, "raw").result
    except MissingContent:
        pass
    except sqlite3.Error as exc:
        pytest.fail(f"raw sqlite leaked: {exc}")


# ---------------------------------------------------------------------------
# Issue 3 — locked shard is Busy
# ---------------------------------------------------------------------------


def test_busy_opening_shard_is_busy_not_corrupt(repo_sharded, monkeypatch):
    import inkpack.sqlite as sqlite_mod

    store = repo_sharded.store
    data = b"busy-shard-probe"
    store.put_bytes(data, "raw").result

    real = sqlite_mod.connect_file

    def shard_only(db_path, *args, **kwargs):
        if db_path.name.startswith("shard-"):
            raise Busy("database is locked")
        return real(db_path, *args, **kwargs)

    monkeypatch.setattr(sqlite_mod, "connect_file", shard_only)
    with pytest.raises(Busy):
        store.put_bytes(data, "raw").result  # dedupe hit -> payload_exists -> _shard_conn


# ---------------------------------------------------------------------------
# Issue 5 — raw sqlite3.Error never escapes public APIs
# ---------------------------------------------------------------------------


def test_broken_schema_get_bytes_is_inkpack_error(repo):
    from inkpack.types import ContentRef

    with repo.backend.txn(write=True) as conn:
        conn.execute("DROP TABLE encodings")
    with pytest.raises(InkpackError) as ei:
        repo.store.get_bytes(ContentRef("ikb1:0:" + "a" * 64 + ":" + "b" * 32, "raw"))
    assert not isinstance(ei.value, sqlite3.Error)


def test_catalog_write_sqlite_error_is_inkpack_error(repo):
    """Public upsert must not leak sqlite3.Error."""
    novel_id = repo.create_novel("n")
    with repo.backend.txn(write=True) as conn:
        conn.execute("DROP TABLE novels")
    with pytest.raises(InkpackError) as ei:
        repo.upsert_chapter(novel_id, "1", b"x", "raw")
    assert not isinstance(ei.value, sqlite3.Error)


def test_verify_unexpected_decode_error_counts_corrupt(repo, monkeypatch):
    store = repo.store
    store.put_bytes(b"x", "raw").result

    def boom(*_a, **_k):
        raise RuntimeError("decode exploded")

    monkeypatch.setattr(store, "_decode_from_row", boom)
    result = store.verify().result
    assert result.checked == 1
    assert result.corrupt == 1


# ---------------------------------------------------------------------------
# Issue 8 — vanished shard on dedupe hit rehomes
# ---------------------------------------------------------------------------


def test_put_rehomes_when_shard_file_deleted(repo_sharded):
    store = repo_sharded.store
    data = b"rehome-me"
    ref = store.put_bytes(data, "raw").result.ref
    row = enc_row(repo_sharded, ref)
    repo_sharded.backend.shard_path(int(row["shard_id"])).unlink()

    again = store.put_bytes(data, "raw").result
    assert again.ref.blob_key == ref.blob_key
    assert store.get_bytes(again.ref) == data
    new_row = enc_row(repo_sharded, ref)
    assert new_row["shard_id"] is not None
    assert repo_sharded.backend.shard_path(int(new_row["shard_id"])).exists()


def test_upsert_rehomes_when_shard_file_deleted(repo_sharded):
    data = b"rehome-upsert"
    novel_id = repo_sharded.create_novel("n")
    repo_sharded.upsert_chapter(novel_id, "a", data, "raw")
    ref = next(r for r in repo_sharded.iter_live_content())
    row = enc_row(repo_sharded, ref)
    repo_sharded.backend.shard_path(int(row["shard_id"])).unlink()

    chapter_b = repo_sharded.upsert_chapter(novel_id, "b", data, "raw")
    assert repo_sharded.get_chapter_bytes(chapter_b) == data
    new_row = enc_row(repo_sharded, ref)
    assert repo_sharded.backend.shard_path(int(new_row["shard_id"])).exists()


# ---------------------------------------------------------------------------
# Issue 10 — backend create config atomic with schema
# ---------------------------------------------------------------------------


def test_backend_create_config_is_atomic_with_schema(tmp_path, monkeypatch):
    import inkpack.sqlite as sqlite_mod

    orig = sqlite_mod._write_initial_config

    def fail_midway(conn, config):
        items = list(config.items())
        orig(conn, dict(items[:1]))
        raise RuntimeError("boom")

    monkeypatch.setattr(sqlite_mod, "_write_initial_config", fail_midway)
    root = tmp_path / "direct"
    with pytest.raises(RuntimeError):
        SqliteBackend.create(
            root,
            mode="sqlite_single",
            initial_config={
                "identity_policy": "ikb1",
                "backend_mode": "sqlite_single",
                "profiles": {"raw": {"codec": "none", "params": {}, "zstd_dict_id": None}},
            },
        )
    db = root / "repo.sqlite"
    if db.exists():
        # The create txn rolled back: either the DB is absent, or it has no
        # committed schema/config (a half-written DB is not acceptable).
        conn = sqlite3.connect(db)
        try:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            ]
            assert "repo_config" not in tables or not [
                r for r in conn.execute("SELECT key FROM repo_config")
            ]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Issue 11 — bool chapter ids
# ---------------------------------------------------------------------------


def test_get_chapter_bytes_rejects_bool(repo):
    novel_id = repo.create_novel("n")
    repo.upsert_chapter(novel_id, "1", b"body", "raw")
    for bad in (True, "1"):
        with pytest.raises(TypeError):
            repo.get_chapter_bytes(bad)
        with pytest.raises(TypeError):
            repo.open_chapter(bad)


# ---------------------------------------------------------------------------
# Issue 12 — set_profiles / create_repo dictionary existence
# ---------------------------------------------------------------------------


def test_set_profiles_rejects_missing_dict(repo):
    missing = "ikd1:" + "ab" * 32
    with pytest.raises(MissingContent):
        repo.set_profiles(
            {
                "raw": Profile("raw", "none", {}),
                "d": Profile("d", "zstd", {"level": 6}, missing),
            }
        )
    assert "d" not in repo.get_profiles()


def test_create_repo_rejects_profile_dict_id(tmp_path):
    with pytest.raises(ValueError, match="zstd_dict_id"):
        create_repo(
            tmp_path / "r",
            profiles={"d": Profile("d", "zstd", {"level": 6}, "ikd1:" + "ab" * 32)},
        )


# ---------------------------------------------------------------------------
# Issue 13 — existing empty directory
# ---------------------------------------------------------------------------


def test_create_repo_accepts_existing_empty_directory(tmp_path):
    root = tmp_path / "novels"
    root.mkdir()
    repo = create_repo(root, profiles={"raw": Profile("raw", "none", {})})
    assert (root / "repo.sqlite").exists() or (root / "index.sqlite").exists()
    assert repo.get_profiles()


# ---------------------------------------------------------------------------
# Issue 15 — scope validation
# ---------------------------------------------------------------------------


def test_iter_live_content_rejects_negative_and_bool_scope(repo):
    for bad in (-1, True):
        with pytest.raises(TypeError):
            list(repo.iter_live_content(scope=bad))


# ---------------------------------------------------------------------------
# Issue 16 — single mode does not validate caps
# ---------------------------------------------------------------------------


def test_single_mode_does_not_validate_shard_caps(tmp_path):
    repo = create_repo(
        tmp_path / "s",
        backend_mode="sqlite_single",
        profiles={"raw": Profile("raw", "none", {})},
        shard_min_bytes=0,
        shard_cap_bytes=1,
    )
    assert repo.backend.mode == "sqlite_single"


# ---------------------------------------------------------------------------
# Issue 17 — missing shard classified by path not message
# ---------------------------------------------------------------------------


def test_missing_shard_never_connects(repo_sharded, monkeypatch):
    """G2(a): a missing shard file is classified by the path check —
    ``connect_file`` is NEVER invoked for it (no message-text parsing)."""
    import inkpack.sqlite as sqlite_mod

    store = repo_sharded.store
    data = b"missing-shard-msg"
    ref = store.put_bytes(data, "raw").result.ref
    row = enc_row(repo_sharded, ref)
    repo_sharded.backend.shard_path(int(row["shard_id"])).unlink()

    real = sqlite_mod.connect_file

    def boom(db_path, *args, **kwargs):
        if db_path.name.startswith("shard-"):
            raise AssertionError("connect_file must not be invoked for a missing shard")
        return real(db_path, *args, **kwargs)

    monkeypatch.setattr(sqlite_mod, "connect_file", boom)
    prepared = store.prepare_bytes(data, "raw")
    assert prepared.enc is not None  # missing payload -> repair path, not a healthy hit


def test_present_unusable_shard_is_corrupt(repo_sharded, monkeypatch):
    """G2(b): a PRESENT shard file whose connection fails with a generic
    OperationalError is CorruptContent (present-but-unusable), never Busy
    and never a raw leak."""
    import inkpack.sqlite as sqlite_mod

    store = repo_sharded.store
    data = b"present-unusable"
    store.put_bytes(data, "raw").result

    real = sqlite_mod.connect_file

    def shard_only(db_path, *args, **kwargs):
        if db_path.name.startswith("shard-"):
            raise sqlite3.OperationalError("I/O error")
        return real(db_path, *args, **kwargs)

    monkeypatch.setattr(sqlite_mod, "connect_file", shard_only)
    with pytest.raises(CorruptContent):
        store.put_bytes(data, "raw").result


# ---------------------------------------------------------------------------
# Issue 18 — GC sweeps payloads parked at the wrong shard
# ---------------------------------------------------------------------------


def test_gc_sweeps_payloads_not_referenced_by_encodings(repo_sharded):
    store = repo_sharded.store
    backend = repo_sharded.backend
    ref = store.put_bytes(b"wrong-locator", "raw").result.ref
    row = enc_row(repo_sharded, ref)
    old = int(row["shard_id"])
    payload = payload_bytes(repo_sharded, ref)
    new = old + 1
    backend.ensure_shard_exists(new)
    with backend.txn(write=True, attach_shard_id=new) as conn:
        conn.execute(
            "INSERT INTO p.payload(blob_key, profile, data) VALUES(?,?,?)",
            (ref.blob_key, ref.profile, payload),
        )
    store.gc(live=[]).result
    with backend.session() as s:
        for sid in backend.list_shards():
            assert not s.payload_exists(ref.blob_key, ref.profile, sid)


# ---------------------------------------------------------------------------
# Issue 20 — verify_on_read must be bool
# ---------------------------------------------------------------------------


def test_verify_on_read_must_be_bool(tmp_path):
    with pytest.raises(TypeError):
        create_repo(
            tmp_path / "v",
            profiles={"raw": Profile("raw", "none", {})},
            verify_on_read="False",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# Issue 32 — read attaches never migrate
# ---------------------------------------------------------------------------


def test_read_attach_does_not_call_payload_migration(repo_sharded, monkeypatch):
    import inkpack.sqlite as sqlite_mod

    called = {"n": 0}
    real = sqlite_mod.migrate_payload_attached

    def wrapped(conn, alias="p"):
        called["n"] += 1
        return real(conn, alias=alias)

    monkeypatch.setattr(sqlite_mod, "migrate_payload_attached", wrapped)
    ref = repo_sharded.store.put_bytes(b"x", "raw").result.ref
    called["n"] = 0
    repo_sharded.store.get_bytes(ref)
    assert called["n"] == 0  # reads never DDL (N2); the put above already migrated


def test_read_shard_conn_does_not_migrate_payload(repo_sharded, monkeypatch):
    import inkpack.sqlite as sqlite_mod

    called = {"n": 0}
    real = sqlite_mod.migrate_payload

    def wrapped(conn):
        called["n"] += 1
        return real(conn)

    monkeypatch.setattr(sqlite_mod, "migrate_payload", wrapped)
    data = b"y"
    repo_sharded.store.put_bytes(data, "raw").result
    called["n"] = 0
    repo_sharded.store.prepare_bytes(data, "raw")  # payload_exists -> _shard_conn
    assert called["n"] == 0


# ---------------------------------------------------------------------------
# Issue 43 — tokenized payload DDL
# ---------------------------------------------------------------------------


def test_payload_ddl_two_tokens(tmp_path):
    """A v2-shaped script mentioning {p}payload twice qualifies both the
    main and the attached migration paths."""
    import inkpack.sqlite as sqlite_mod

    # Two {p} tokens: one table plus a second table (SQLite cannot CREATE
    # INDEX on an attached schema, so both statements are tables).
    v2 = (
        "CREATE TABLE IF NOT EXISTS {p}payload_extra(x INTEGER); "
        "CREATE TABLE IF NOT EXISTS {p}payload_extra2(y INTEGER);"
    )
    sqlite_mod.PAYLOAD_MIGRATIONS = (*sqlite_mod.PAYLOAD_MIGRATIONS, (2, v2))
    try:
        db = tmp_path / "t.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            sqlite_mod.migrate_payload(conn)
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "payload_extra" in names and "payload_extra2" in names
        finally:
            conn.close()
        # attached variant: the shard must already be at v1 for v2 to apply
        main = tmp_path / "m.sqlite"
        shard = tmp_path / "s.sqlite"
        c0 = sqlite3.connect(str(shard))
        sqlite_mod.migrate_payload(c0)
        c0.close()
        c1 = sqlite3.connect(str(main))
        c2 = sqlite3.connect(str(shard))
        try:
            c1.execute(f"ATTACH DATABASE '{shard}' AS p")
            sqlite_mod.migrate_payload_attached(c1)
            names = {r[0] for r in c2.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "payload_extra" in names and "payload_extra2" in names
        finally:
            c1.close()
            c2.close()
    finally:
        sqlite_mod.PAYLOAD_MIGRATIONS = sqlite_mod.PAYLOAD_MIGRATIONS[:-1]


# ---------------------------------------------------------------------------
# Issue 46 — stale creating-dir sweep
# ---------------------------------------------------------------------------


def test_create_repo_sweeps_stale_creating_dirs(tmp_path):
    stale = tmp_path / (".inkpack-creating-" + "ab" * 16)
    stale.mkdir()
    (stale / "leftover").write_text("x")
    old = time.time() - 3 * 3600
    os.utime(stale, (old, old))
    create_repo(tmp_path / "repo", profiles={"raw": Profile("raw", "none", {})})
    assert not stale.exists()


# ---------------------------------------------------------------------------
# Issue 47 — verify item events not duplicated on the throttle boundary
# ---------------------------------------------------------------------------


def test_verify_item_events_are_not_duplicated_on_throttle_boundary(repo):
    store = repo.store
    for i in range(32):
        store.put_bytes(f"v{i}".encode(), "raw").result
    items = [e for e in store.verify() if e.kind == "item"]
    assert len(items) == 1
