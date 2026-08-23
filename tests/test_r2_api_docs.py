"""Fail-first tests for the r2 issue register, section D (API/UX) and the
E-section maintainability guards.

- D1  rename shard_min_bytes -> min_shard_cap_bytes (legacy kwarg deprecated)
- D2  Repository.set_verify_on_read
- D3  Profile is hashable
- D4  UnknownProfile.__str__ is plain (no KeyError repr-quoting)
- D5  CancelToken is exported
- D6  stream puts emit a phase event at the hash->persist boundary
- E1  dead/divergent code removal (guards)
- E2  validation/marker consolidation (guards)
- E3  ChapterInfo explicit construction survives future columns
- E4  migration splitter robustness (guard)
- E5  _RepairRequired is typed (InkpackError subclass)
"""

from __future__ import annotations

import io
import sqlite3

import pytest

import inkpack
from inkpack import (
    CancelToken,  # noqa: F401  (D5: import must not ImportError)
    CorruptContent,
    Profile,
    UnknownProfile,
    create_repo,
)

from .conftest import set_payload

PROFILES = {"raw": Profile("raw", "none", {})}


# ---------------------------------------------------------------------------
# D1 — min_shard_cap_bytes rename
# ---------------------------------------------------------------------------


def test_min_shard_cap_kwarg_and_deprecation(tmp_path):
    create_repo(
        tmp_path / "n",
        "sqlite_sharded",
        profiles=PROFILES,
        min_shard_cap_bytes=1 << 19,  # today: TypeError
    )
    with pytest.warns(DeprecationWarning):
        create_repo(
            tmp_path / "o",
            "sqlite_sharded",
            profiles=PROFILES,
            shard_min_bytes=1 << 19,  # today: no warning
        )


def test_min_shard_cap_persisted_key_and_backend_field_stable(tmp_path):
    """Storage stability: the config KEY and backend field keep the old name."""
    from inkpack import open_repo

    repo = create_repo(
        tmp_path / "s",
        "sqlite_sharded",
        profiles=PROFILES,
        min_shard_cap_bytes=1 << 19,
    )
    assert repo.backend.config_get("shard_min_bytes") == 1 << 19
    reopened = open_repo(tmp_path / "s")
    assert reopened.backend.shard_min_bytes == 1 << 19


def test_min_shard_cap_both_kwargs_rejected(tmp_path):
    with pytest.raises(ValueError):
        create_repo(
            tmp_path / "both",
            "sqlite_sharded",
            profiles=PROFILES,
            min_shard_cap_bytes=1 << 18,
            shard_min_bytes=1 << 19,
        )


# ---------------------------------------------------------------------------
# D2 — set_verify_on_read
# ---------------------------------------------------------------------------


def test_set_verify_on_read_toggles(repo):
    ref = repo.store.put_bytes(b"toggle-via-api", "raw").result.ref
    repo.set_verify_on_read(True)  # today: AttributeError
    set_payload(repo, ref, b"TOGGLE-VIA-API")  # same length, different content
    with pytest.raises(CorruptContent):
        repo.store.get_bytes(ref)
    repo.set_verify_on_read(False)
    assert repo.store.get_bytes(ref) == b"TOGGLE-VIA-API"


def test_set_verify_on_read_rejects_non_bool(repo):
    with pytest.raises(TypeError):
        repo.set_verify_on_read("yes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# D3 — Profile is hashable
# ---------------------------------------------------------------------------


def test_profile_is_hashable():
    a = Profile("a", "none", {}, None)
    b = Profile("a", "none", {}, None)
    assert hash(a) == hash(b)  # today: TypeError (unhashable MappingProxyType)
    assert len({a, b}) == 1
    d = {a: 1}
    assert d[b] == 1
    # Equal profiles (same name/codec/dict) hash equal even with different
    # param values; the hash uses the hashable subset of the eq fields.
    c = Profile("a", "none", {"level": 3}, None)
    assert c not in {a}  # c != a (params differ in eq)


# ---------------------------------------------------------------------------
# D4 — UnknownProfile.__str__ is plain
# ---------------------------------------------------------------------------


def test_unknown_profile_str_is_plain():
    err = UnknownProfile("profile not found: raw")
    assert str(err) == "profile not found: raw"  # today: "'profile not found: raw'"
    with pytest.raises(KeyError):
        raise err


# ---------------------------------------------------------------------------
# D5 — CancelToken exported
# ---------------------------------------------------------------------------


def test_cancel_token_exported():
    # The module-top `from inkpack import CancelToken` is the fail-first
    # assertion (today: ImportError); pin `__all__` membership too.
    assert "CancelToken" in inkpack.__all__
    assert inkpack.CancelToken is not None


# ---------------------------------------------------------------------------
# D6 — stream puts emit a phase event at the hash->persist boundary
# ---------------------------------------------------------------------------


def test_stream_put_emits_phase_event(repo):
    events = list(repo.store.put_stream(io.BytesIO(b"phase " * 50), "raw"))
    assert events[0].kind == "start" and events[-1].kind == "done"
    phases = [e for e in events if e.kind == "phase"]
    assert phases, "a phase event must be emitted at the hash->persist boundary"
    # The phase event sits between the start and the done events.
    assert 0 < events.index(phases[0]) < len(events) - 1


def test_upsert_chapter_stream_emits_phase_event(repo):
    novel_id = repo.create_novel("t")
    events = list(repo.upsert_chapter_stream(io.BytesIO(b"phase-stream"), novel_id, "1", "raw"))
    assert any(e.kind == "phase" for e in events)


# ---------------------------------------------------------------------------
# E1 — dead/divergent code removal (guards)
# ---------------------------------------------------------------------------


def test_removed_helpers_are_gone():
    from inkpack import blobstore as bs
    from inkpack import sqlite as sm

    assert not hasattr(sm.SqliteBackend, "upsert_chapter_with_content")
    assert not hasattr(sm.SqliteBackend, "store_encoding_and_payload")
    assert not hasattr(sm.SqliteBackend, "get_encoding")
    assert not hasattr(sm.SqliteBackend, "get_payload")
    assert not hasattr(bs, "require_shard_id")


# ---------------------------------------------------------------------------
# E2 — validation/marker consolidation (guards)
# ---------------------------------------------------------------------------


def test_repo_markers_single_source(tmp_path):
    from inkpack.sqlite import repo_markers_exist

    assert not repo_markers_exist(tmp_path)
    (tmp_path / "repo.sqlite").write_bytes(b"")
    assert repo_markers_exist(tmp_path)
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "shard-0001.sqlite").write_bytes(b"")
    (tmp_path / "repo.sqlite").unlink()
    assert repo_markers_exist(tmp_path)


def test_require_pk_single_source():
    from inkpack.types import require_pk

    assert require_pk(0, "x") == 0
    assert require_pk(7, "x") == 7
    for bad in (True, False, -1, "1", 1.5, None):
        with pytest.raises(TypeError):
            require_pk(bad, "x")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# E3 — ChapterInfo explicit construction
# ---------------------------------------------------------------------------


def test_list_chapters_survives_future_column_addition(repo):
    novel_id = repo.create_novel("Future")
    repo.upsert_chapter(novel_id, "1", b"b", "raw")
    with repo.backend.txn(write=True) as conn:
        conn.execute("ALTER TABLE chapters ADD COLUMN future_proof TEXT")
    chapters = repo.list_chapters(novel_id)  # today: TypeError unexpected kwarg
    assert len(chapters) == 1
    info = repo.get_chapter(chapters[0].id)
    assert info.blob_key


# ---------------------------------------------------------------------------
# E4 — migration splitter robustness (guard)
# ---------------------------------------------------------------------------


def test_migration_statements_are_complete():
    """Every produced statement must be a complete statement (passing today;
    protects future migration scripts containing ';' in literals/triggers)."""
    import inkpack.sqlite as sm

    scripts = [script for _v, script in sm.MIGRATIONS]
    scripts += [script for _v, script in sm.PAYLOAD_MIGRATIONS]
    assert scripts
    for script in scripts:
        for stmt in sm._split_statements(script):
            assert sqlite3.complete_statement(stmt), f"incomplete: {stmt!r}"


def test_split_statements_handles_semicolons_in_literals():
    import inkpack.sqlite as sm

    script = "CREATE TABLE t(x TEXT); INSERT INTO t VALUES('a;b');"
    stmts = sm._split_statements(script)
    assert len(stmts) == 2
    for stmt in stmts:
        assert sqlite3.complete_statement(stmt)


# ---------------------------------------------------------------------------
# E5 — _RepairRequired is typed
# ---------------------------------------------------------------------------


def test_repair_required_is_typed_internal():
    from inkpack.blobstore import _RepairRequired
    from inkpack.types import InkpackError

    assert issubclass(_RepairRequired, InkpackError)
