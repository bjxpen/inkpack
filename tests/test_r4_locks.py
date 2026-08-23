"""r4.1 locks: fail-first tests (red on the pre-fix tree) and PINs (green,
lock a rule so it cannot flip silently) for the r4.1 change document.

Every test is labeled [FAIL-FIRST] or [PIN] in its docstring. Uses the
PROFILES / repo fixture pattern from test_review5_locks.py.
"""

from __future__ import annotations

import pytest

from inkpack import Busy, CorruptContent, MissingContent, Profile

from .conftest import delete_dict, delete_payload_row, enc_row, payload_bytes  # noqa: F401

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
    so a broad catch before the Busy catch would swallow it."""
    import inkpack.sqlite as sqlite_mod

    repo_sharded.store.put_bytes(b"busy-stay" * 50, "raw").result
    real = sqlite_mod.connect_file

    def shard_busy(db_path, *a, **k):
        if db_path.name.startswith("shard-"):
            raise Busy("database is locked")
        return real(db_path, *a, **k)

    monkeypatch.setattr(sqlite_mod, "connect_file", shard_busy)
    with pytest.raises(Busy):
        repo_sharded.store.put_bytes(b"busy-stay" * 50, "raw").result
