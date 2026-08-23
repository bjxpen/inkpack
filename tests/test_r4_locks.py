"""r4.1 locks: fail-first tests (red on the pre-fix tree) and PINs (green,
lock a rule so it cannot flip silently) for the r4.1 change document.

Every test is labeled [FAIL-FIRST] or [PIN] in its docstring. Uses the
PROFILES / repo fixture pattern from test_review5_locks.py.
"""

from __future__ import annotations

import pytest

from inkpack import Busy, CorruptContent, InkpackError, MissingContent, Profile

from .conftest import delete_dict, delete_payload_row, enc_row, payload_bytes

PROFILES = {
    "raw": Profile("raw", "none", {}),
    "zstd_nodict": Profile("zstd_nodict", "zstd", {"level": 3}),
}


@pytest.fixture(params=["sqlite_single", "sqlite_sharded"])
def repo(tmp_path, request):
    from .conftest import make_repo

    return make_repo(tmp_path, request.param)


# ---------------------------------------------------------------------------
# P0.4 [PIN] — hit/read ignores codec_params_json (decode does; L27)
# ---------------------------------------------------------------------------


def test_hit_ignores_non_object_codec_params_json(repo):
    """[PIN] N7/L27: params are not a read/hit concern. A dedupe hit with
    non-object ``codec_params_json`` is genuinely readable and succeeds;
    verify agrees (counts ok). Repair still rejects unusable params (M8,
    locked in test_known_issues). Passes today; locks the rule."""
    data = b"params-hit " * 20
    ref = repo.store.put_bytes(data, "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET codec_params_json=? WHERE blob_key=?",
            ("[]", ref.blob_key),
        )
    again = repo.store.put_bytes(data, "raw").result
    assert again.ref == ref
    assert repo.store.get_bytes(ref) == data
    assert repo.store.verify().result.ok == 1  # verify agrees: not a read finding


# ---------------------------------------------------------------------------
# P0.3 [FAIL-FIRST] — "never miss(ed)" is gone from repo.py too
# ---------------------------------------------------------------------------
# (The guard lives in tests/test_r3_fixes.py::test_no_never_miss_live_ref_claim_anywhere,
# extending the r3-B guard to the module docstring surface.)


# ---------------------------------------------------------------------------
# P1.8 [FAIL-FIRST + PIN] — hints: None preserves, a provided dict replaces
# ---------------------------------------------------------------------------


def test_upsert_omitted_hints_preserve_media_type(repo):
    """[FAIL-FIRST] hints=None must NOT NULL out the stored media_type/
    charset (the old ``hints or {}`` wrote NULLs over preserved hints)."""
    novel_id = repo.create_novel("hints")
    cid = repo.upsert_chapter(
        novel_id, "1", b"v1", "raw", hints={"media_type": "text/plain", "charset": "utf-8"}
    )
    repo.upsert_chapter(novel_id, "1", b"v2", "raw")  # hints default None
    info = repo.get_chapter(cid)
    assert info.media_type == "text/plain"  # TODAY: both None
    assert info.charset == "utf-8"
    assert repo.get_chapter_bytes(cid) == b"v2"


def test_upsert_explicit_empty_hints_clears_media_type(repo):
    """[PIN] an EXPLICIT {} replaces the group (clears both); a partial dict
    writes present keys and NULLs the omitted ones."""
    novel_id = repo.create_novel("hints2")
    cid = repo.upsert_chapter(
        novel_id, "1", b"v1", "raw", hints={"media_type": "text/plain", "charset": "utf-8"}
    )
    repo.upsert_chapter(novel_id, "1", b"v2", "raw", hints={})  # explicit: clear both
    info = repo.get_chapter(cid)
    assert info.media_type is None and info.charset is None
    repo.upsert_chapter(novel_id, "1", b"v3", "raw", hints={"media_type": "image/png"})
    info = repo.get_chapter(cid)
    assert info.media_type == "image/png" and info.charset is None


def test_upsert_meta_merges(repo):
    """[PIN] meta always merges per key (independent upserts)."""
    novel_id = repo.create_novel("meta")
    cid = repo.upsert_chapter(novel_id, "1", b"v1", "raw", meta={"a": 1})
    repo.upsert_chapter(novel_id, "1", b"v2", "raw", meta={"b": 2})
    assert repo.meta_list("chapter", cid) == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# P1.9 [FAIL-FIRST] — omitted min must not make a small cap uncreatable
# ---------------------------------------------------------------------------


def test_create_repo_min_defaults_to_not_exceed_cap(tmp_path):
    from inkpack import create_repo

    repo = create_repo(
        tmp_path / "small-cap",
        "sqlite_sharded",
        profiles={"raw": Profile("raw", "none", {})},
        shard_cap_bytes=1 << 20,  # TODAY: ValueError (min 256MiB > cap 1MiB)
    )
    assert repo.backend.shard_cap_bytes == 1 << 20
    assert repo.backend.shard_min_bytes == 1 << 20  # derived: min(DEFAULT, cap)


def test_create_repo_explicit_min_still_enforced(tmp_path):
    """[PIN] an EXPLICIT min > cap still raises (derivation is omission-only)."""
    from inkpack import create_repo

    with pytest.raises(ValueError, match="must not exceed"):
        create_repo(
            tmp_path / "explicit",
            "sqlite_sharded",
            profiles={"raw": Profile("raw", "none", {})},
            shard_cap_bytes=1 << 20,
            min_shard_cap_bytes=1 << 25,
        )


# ---------------------------------------------------------------------------
# P1.1 [FAIL-FIRST x2] — N7: put must not commit an encoding whose dict is
# already gone (in-txn dict probe at the commit point)
# ---------------------------------------------------------------------------


def _count(repo, table: str) -> int:
    with repo.backend.txn(write=False) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_n7_new_write_does_not_commit_if_dict_vanished_after_prepare(repo, monkeypatch):
    """[FAIL-FIRST] prepare loaded D into the session cache; the last profile
    pin is dropped and gc deletes D; the WRITE path must re-probe the dicts
    table on the commit connection and fail (TODAY: PutResult returned,
    leaving an encoding that names a missing dict)."""
    train = repo.store.train_dict([b"n7-new " * 80] * 6).result
    repo.set_profile(Profile("zstd_d", "zstd", {"level": 6}, train.dict_id))
    data = b"n7-new-body " * 200
    real = repo.store._persist

    def drop_pin_then_gc(s, prepared, raw):
        repo.set_profile(Profile("zstd_d", "zstd", {"level": 6}))  # drop the pin
        repo.store.gc(live=[]).result  # D now unreferenced -> deleted
        return real(s, prepared, raw)

    monkeypatch.setattr(repo.store, "_persist", drop_pin_then_gc)
    encodings_before = _count(repo, "encodings")
    with pytest.raises(MissingContent):  # TODAY: PutResult returned
        repo.store.put_bytes(data, "zstd_d").result
    assert _count(repo, "encodings") == encodings_before


def test_n7_hit_rechecks_dict_in_the_write_txn(repo, monkeypatch):
    """[FAIL-FIRST] poison the cache the way the race actually does: delete
    the dict row AFTER a positive fetch, so THIS session's cache stays
    positive. The hit branch must probe the dicts table on the write-txn
    connection (TODAY: succeeds via the poisoned cache)."""
    from inkpack.sqlite import Session

    train = repo.store.train_dict([b"n7-hit " * 80] * 6).result
    repo.set_profile(Profile("zstd_d", "zstd", {"level": 6}, train.dict_id))
    data = b"n7-hit-body " * 200
    first = repo.store.put_bytes(data, "zstd_d").result

    orig = Session.dict_bytes
    fired = {"n": 0}

    def poison(self, dict_id):
        result = orig(self, dict_id)
        if dict_id == train.dict_id and result is not None and fired["n"] == 0:
            fired["n"] += 1
            delete_dict(repo, train.dict_id)  # DB row gone; cache stays positive
        return result

    monkeypatch.setattr(Session, "dict_bytes", poison)
    with pytest.raises(MissingContent):  # TODAY: succeeds via the poisoned cache
        repo.store.put_bytes(data, "zstd_d").result
    assert enc_row(repo, first.ref) is not None  # hit must not rewrite


# ---------------------------------------------------------------------------
# P1.2 [FAIL-FIRST] — S2: a present-but-unusable shard is rehomed off, not
# bricked; reads still classify it CorruptContent (A2)
# ---------------------------------------------------------------------------


def test_new_put_rolls_past_corrupt_highest_shard(repo_sharded):
    """[FAIL-FIRST] a junk HIGHEST shard must not brick new writes (no locator
    to rehome from) — the write rolls to max+1. TODAY: CorruptContent."""
    first = repo_sharded.store.put_bytes(b"keep-me" * 50, "raw").result
    sid = int(enc_row(repo_sharded, first.ref)["shard_id"])
    repo_sharded.backend.shard_path(sid).write_bytes(b"not a sqlite db")
    second = repo_sharded.store.put_bytes(b"fresh" * 50, "raw").result  # TODAY: CorruptContent
    assert repo_sharded.store.get_bytes(second.ref) == b"fresh" * 50
    assert int(enc_row(repo_sharded, second.ref)["shard_id"]) != sid


def test_repair_rehomes_off_unusable_locator(repo_sharded):
    """[FAIL-FIRST] a repair whose locator shard is unusable rehomes to a
    writable shard (the payload is gone, so this is the repair branch).
    TODAY: CorruptContent (payload_exists raised instead of missing)."""
    data = b"rehome-unusable " * 80
    put = repo_sharded.store.put_bytes(data, "raw").result
    delete_payload_row(repo_sharded, put.ref)
    sid = int(enc_row(repo_sharded, put.ref)["shard_id"])
    repo_sharded.backend.shard_path(sid).write_bytes(b"junk-junk")
    again = repo_sharded.store.put_bytes(data, "raw").result  # TODAY: CorruptContent
    assert repo_sharded.store.get_bytes(again.ref) == data
    assert int(enc_row(repo_sharded, again.ref)["shard_id"]) != sid


def test_get_bytes_on_unusable_shard_still_corruptcontent(repo_sharded):
    """[PIN] A2 read pin, unchanged: a read on a present-but-unusable shard
    still raises CorruptContent (only the WRITE path rehomes)."""
    ref = repo_sharded.store.put_bytes(b"read-me" * 50, "raw").result.ref
    sid = int(enc_row(repo_sharded, ref)["shard_id"])
    repo_sharded.backend.shard_path(sid).write_bytes(b"junk-junk")
    with pytest.raises(CorruptContent):
        repo_sharded.store.get_bytes(ref)


def test_busy_shard_not_treated_unusable(repo_sharded, monkeypatch):
    """[PIN] the except-ordering trap: a LOCKED (Busy) shard must raise Busy,
    not be silently rolled off as "unusable". Busy subclasses InkpackError,
    so a broad catch before the Busy catch would swallow it. P2.1: the
    write-side probe uses Session.attach (not connect_file for the shard), so
    the spy patches Session.attach."""
    from inkpack.sqlite import Session

    repo_sharded.store.put_bytes(b"busy-stay" * 50, "raw").result

    def busy_attach(self, shard_id):
        raise Busy("database is locked")

    monkeypatch.setattr(Session, "attach", busy_attach)
    with pytest.raises(Busy):
        repo_sharded.store.put_bytes(b"busy-stay" * 50, "raw").result


# ---------------------------------------------------------------------------
# P1.3 [PIN] — upsert dedupe-hit self-heals a moved shard locator.
# NOTE (discrepancy with the r4.1 doc): the doc labels the first test
# [FAIL-FIRST] "TODAY: MissingContent". Empirically it is GREEN on the
# pre-fix code, because the pre-fix dedupe path RE-PEEKS the locator (so a
# pre-call move is seen by the peek and the payload is found). The new
# code's compare/retry loop additionally handles a CONCURRENT move between
# the peek and the in-txn probe (mirroring _persist). These tests therefore
# PIN the moved-locator end-to-end behavior (green pre- and post-fix).
# ---------------------------------------------------------------------------


def _move_payload_to_new_shard(repo_sharded, ref, prepared):
    """Move the payload to a brand-new shard and repoint the locator, leaving
    the OLD shard's payload deleted (the locator moved; payload only on the
    new shard). Returns (old_sid, new_sid)."""
    old = int(enc_row(repo_sharded, ref)["shard_id"])
    new = old + 1
    repo_sharded.backend.ensure_shard_exists(new)
    payload = payload_bytes(repo_sharded, ref)
    with repo_sharded.backend.txn(write=True, attach_shard_id=new) as conn:
        conn.execute(
            "INSERT INTO p.payload(blob_key, profile, data) VALUES(?,?,?)",
            (ref.blob_key, ref.profile, payload),
        )
    with repo_sharded.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET shard_id=? WHERE blob_key=? AND profile=?",
            (new, ref.blob_key, ref.profile),
        )
    with repo_sharded.backend.txn(write=True, attach_shard_id=old) as conn:
        conn.execute(
            "DELETE FROM p.payload WHERE blob_key=? AND profile=?",
            (ref.blob_key, ref.profile),
        )
    return old, new


def test_upsert_stale_hit_locator_self_heals_without_raw(repo_sharded):
    """[PIN] a dedupe-hit upsert whose shard locator moved (payload now only
    on the new shard) commits even with raw=None — the content exists, so it
    must not raise MissingContent (see the P1.3 note above: green pre- and
    post-fix; the post-fix retry loop additionally covers a concurrent
    move between peek and probe)."""
    novel_id = repo_sharded.create_novel("n")
    body = b"stale-loc " * 80
    repo_sharded.upsert_chapter(novel_id, "k", body, "raw")
    prepared = repo_sharded.store.prepare_bytes(body, "raw")
    assert prepared.enc is None and prepared.shard_id is not None
    ref = repo_sharded.store.put_bytes(body, "raw").result.ref
    _move_payload_to_new_shard(repo_sharded, ref, prepared)
    with repo_sharded.backend.session() as s:
        cid = repo_sharded._upsert_prepared(s, novel_id, "k2", prepared, None, {}, None)
    assert repo_sharded.get_chapter_bytes(cid) == body


def test_upsert_stale_hit_locator_with_raw_still_commits(repo_sharded):
    """[PIN] the raw-present twin: a moved-locator dedupe hit with raw bytes
    commits (it self-heals today via the repair path; this guards against
    regressing it into a retry)."""
    novel_id = repo_sharded.create_novel("n2")
    body = b"stale-loc-raw " * 80
    repo_sharded.upsert_chapter(novel_id, "k", body, "raw")
    prepared = repo_sharded.store.prepare_bytes(body, "raw")
    assert prepared.enc is None and prepared.shard_id is not None
    ref = repo_sharded.store.put_bytes(body, "raw").result.ref
    _move_payload_to_new_shard(repo_sharded, ref, prepared)
    with repo_sharded.backend.session() as s:
        cid = repo_sharded._upsert_prepared(s, novel_id, "k2", prepared, body, {}, None)
    assert repo_sharded.get_chapter_bytes(cid) == body


def test_upsert_payload_gone_everywhere_without_raw_is_missingcontent(repo_sharded):
    """[PIN] boundary: the encoding row is present but the payload is deleted
    from EVERY shard and raw=None -> MissingContent (content is genuinely
    absent; Retryable is reserved for persistent locator churn)."""
    novel_id = repo_sharded.create_novel("n3")
    body = b"gone-everywhere " * 80
    repo_sharded.upsert_chapter(novel_id, "k", body, "raw")
    prepared = repo_sharded.store.prepare_bytes(body, "raw")
    assert prepared.enc is None
    ref = repo_sharded.store.put_bytes(body, "raw").result.ref
    for sid in repo_sharded.backend.list_shards():
        with repo_sharded.backend.txn(write=True, attach_shard_id=sid) as conn:
            conn.execute(
                "DELETE FROM p.payload WHERE blob_key=? AND profile=?",
                (ref.blob_key, ref.profile),
            )
    with repo_sharded.backend.session() as s, pytest.raises(MissingContent):
        repo_sharded._upsert_prepared(s, novel_id, "k2", prepared, None, {}, None)


# ---------------------------------------------------------------------------
# P1.4 [FAIL-FIRST] — create_repo must not delete a pre-existing empty dest
# when the rename fails
# ---------------------------------------------------------------------------


def test_create_repo_failed_rename_keeps_preexisting_empty_dir(tmp_path, monkeypatch):
    """[FAIL-FIRST] a pre-existing empty dest dir is rmdir'd before
    os.replace (os.replace onto a dir is not portable); on rename failure it
    must be restored. TODAY: the dir is gone."""
    import os

    from inkpack import InkpackError, create_repo

    root = tmp_path / "novels"
    root.mkdir()
    monkeypatch.setattr(
        os, "replace", lambda *_a, **_k: (_ for _ in ()).throw(OSError("simulated"))
    )
    with pytest.raises(InkpackError):
        create_repo(root, "sqlite_single", profiles=PROFILES)
    assert root.is_dir()  # restored
    assert not (root / "repo.sqlite").exists()
    assert not (root / "index.sqlite").exists()


# ---------------------------------------------------------------------------
# P1.6 [FAIL-FIRST] — missing sharded cap keys on open raise InkpackError
# ---------------------------------------------------------------------------


def test_open_missing_shard_caps_is_inkpack_error(tmp_path):
    """[FAIL-FIRST] absent shard_cap_bytes/shard_min_bytes keys silently fell
    back to factory defaults (2 GiB cap); every other required key raises.
    Deleting the caps is silent tampering and must raise."""
    from inkpack import create_repo, open_repo

    root = tmp_path / "s"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "DELETE FROM repo_config WHERE key IN ('shard_cap_bytes','shard_min_bytes')"
        )
    with pytest.raises(InkpackError, match="shard_"):  # TODAY: opens with defaults
        open_repo(root)


def test_open_missing_single_shard_cap_is_inkpack_error(tmp_path):
    """[PIN] partial deletion (one cap key present, one missing) also raises."""
    from inkpack import create_repo, open_repo

    root = tmp_path / "s2"
    repo = create_repo(root, "sqlite_sharded", profiles=PROFILES)
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM repo_config WHERE key = 'shard_cap_bytes'")
    with pytest.raises(InkpackError, match="shard_cap_bytes missing"):
        open_repo(root)


# ---------------------------------------------------------------------------
# P1.7 [FAIL-FIRST source guard + PIN smoke] — CodecEngine cache is
# lock-protected (process-wide, shared across threads)
# ---------------------------------------------------------------------------


def test_codec_engine_cache_is_synchronized():
    """[FAIL-FIRST] source guard: the context cache must be lock-protected.
    TODAY (pre-fix): no lock."""
    import inspect

    from inkpack.codec import CodecEngine

    src = inspect.getsource(CodecEngine._compressor) + inspect.getsource(
        CodecEngine._decompressor
    )
    assert "_cache_lock" in src or "Lock" in src


def test_codec_engine_concurrent_encode_roundtrip(repo):
    """[PIN] smoke: concurrent reads + reencodes through one shared engine
    complete without error (the lock keeps the LRU OrderedDict safe)."""
    import threading

    refs = [
        repo.store.put_bytes(f"t{i}".encode() * 50, "zstd_nodict").result.ref
        for i in range(8)
    ]
    errors: list[BaseException] = []

    def worker(ref):
        try:
            repo.store.get_bytes(ref)
            repo.store.reencode([ref], options={"codec": "zstd", "params": {"level": 3}}).result
        except BaseException as exc:  # smoke test collects all
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(r,)) for r in refs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


# ---------------------------------------------------------------------------
# P2.3 [PIN] — payload migrations are {p}-token-safe (str.replace, not
# str.format) and idempotent
# ---------------------------------------------------------------------------


def test_payload_migrations_are_format_safe_and_idempotent():
    """[PIN] a literal ``{`` in a future migration script (JSON default,
    trigger body) must not raise KeyError — the ``{p}`` token is substituted
    with ``str.replace``, and every statement is idempotent (IF NOT EXISTS /
    ON CONFLICT) since attached migrations run in autocommit."""
    import sqlite3

    from inkpack import sqlite as sm

    for _v, script in sm.PAYLOAD_MIGRATIONS:
        stripped = script.replace("{p}", "")
        assert "{" not in stripped and "}" not in stripped
        for stmt in sm._split_statements(script.replace("{p}", "p.")):
            assert sqlite3.complete_statement(stmt)
            u = stmt.upper()
            assert "IF NOT EXISTS" in u or "ON CONFLICT" in u


# ---------------------------------------------------------------------------
# P2.4 [FAIL-FIRST] — open skips payload DDL when the schema is already
# current
# ---------------------------------------------------------------------------


def test_open_skips_payload_migrate_when_already_current(tmp_path, monkeypatch):
    """[FAIL-FIRST] open/validation ran a no-op payload DDL (CREATE TABLE IF
    NOT EXISTS _inkpack_schema + version SELECT) on every open even when the
    schema was already current. P2.4 reads the version first (read-only) and
    skips migrate_payload when current. TODAY: >=1 migrate_payload call per
    open (per shard in sharded mode)."""
    from inkpack import create_repo, open_repo
    from inkpack import sqlite as sm

    called = {"n": 0}
    real = sm.migrate_payload

    def wrapped(conn):
        called["n"] += 1
        return real(conn)

    monkeypatch.setattr(sm, "migrate_payload", wrapped)
    for mode in ("sqlite_single", "sqlite_sharded"):
        root = tmp_path / mode
        create_repo(root, mode, profiles=PROFILES)
        called["n"] = 0
        open_repo(root)
        assert called["n"] == 0, mode


# ---------------------------------------------------------------------------
# P2.2 [PIN] — the repair DECISION (encode + limit + resolve + forget) is
# defined once, shared by repair_flow and Repository._upsert_repair
# ---------------------------------------------------------------------------


def test_repair_decision_is_shared_not_duplicated():
    """[PIN] P2.2 extracted the prepare-side repair decision into
    BlobStore.resolve_repair_write so repair_flow (put-repair) and
    Repository._upsert_repair (upsert-repair) cannot diverge. Pin that both
    call sites route through the shared helper (and neither inlines
    resolve_write_shard + forget_missing_shard themselves)."""
    import inspect

    from inkpack.blobstore import BlobStore
    from inkpack.repo import Repository

    assert hasattr(BlobStore, "resolve_repair_write")
    repair_flow_src = inspect.getsource(BlobStore.repair_flow)
    upsert_repair_src = inspect.getsource(Repository._upsert_repair)
    assert "resolve_repair_write" in repair_flow_src
    assert "resolve_repair_write" in upsert_repair_src
    # Neither caller inlines the resolve/forget decision itself.
    assert "resolve_write_shard" not in repair_flow_src
    assert "resolve_write_shard" not in upsert_repair_src
    assert "forget_missing_shard" not in repair_flow_src
    assert "forget_missing_shard" not in upsert_repair_src


# ---------------------------------------------------------------------------
# P2.1 [FAIL-FIRST] — the write-side probe uses the session ATTACH (one
# connection), not a per-shard connection
# ---------------------------------------------------------------------------


def test_sharded_dedupe_hit_put_opens_one_connection(repo_sharded, connect_counter):
    """[FAIL-FIRST] P2.1 unified the write-side existence probe onto the
    session ATTACH, so a sharded dedupe-hit put opens ONE connection (the
    session), not two (session + a per-shard probe connection). TODAY
    (pre-P2.1, probe via a per-shard connection): 2."""
    data = b"one-conn-hit" * 20
    repo_sharded.store.put_bytes(data, "raw").result  # warms the blob-limit cache
    connect_counter["opens"] = 0
    repo_sharded.store.put_bytes(data, "raw").result
    assert connect_counter["opens"] == 1
