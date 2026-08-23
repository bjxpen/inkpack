"""r4.1 locks: fail-first tests (red on the pre-fix tree) and PINs (green,
lock a rule so it cannot flip silently) for the r4.1 change document.

Every test is labeled [FAIL-FIRST] or [PIN] in its docstring. Uses the
PROFILES / repo fixture pattern from test_review5_locks.py.
"""

from __future__ import annotations

import pytest

from inkpack import Profile

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
