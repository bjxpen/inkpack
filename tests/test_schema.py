"""Schema, migrations and repo-config tests (spec 7, 7.7, 7.8)."""

from __future__ import annotations

import sqlite3

import pytest

from inkpack import InkpackError, Profile, create_repo, open_repo
from inkpack.sqlite import LATEST_USER_VERSION, migrate_index, migrate_payload

INDEX_TABLES = {"repo_config", "novels", "chapters", "meta", "blobs", "encodings", "dicts"}
ENCODING_COLUMNS = {
    "blob_key",
    "profile",
    "codec",
    "codec_params_json",
    "zstd_dict_id",
    "stored_len",
    "checksum",
    "shard_id",
    "updated_at",
}


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r[0] for r in rows}


def test_schema_tables_exist_index_db(repo):
    with repo.backend.txn(write=False) as conn:
        assert _table_names(conn) >= INDEX_TABLES


def test_encodings_columns_match_spec(repo):
    with repo.backend.txn(write=False) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(encodings)")}
    assert cols >= ENCODING_COLUMNS


def test_payload_table_exists_single(repo_single):
    with repo_single.backend.txn(write=False) as conn:
        assert "payload" in _table_names(conn)


def test_payload_table_exists_in_each_shard(repo_sharded):
    shards = repo_sharded.backend.list_shards()
    assert shards
    for sid in shards:
        conn = sqlite3.connect(str(repo_sharded.backend.shard_path(sid)))
        try:
            assert "payload" in _table_names(conn)
        finally:
            conn.close()


def test_required_indexes_exist(repo):
    with repo.backend.txn(write=False) as conn:
        indexes = {
            r[1] for r in conn.execute("PRAGMA index_list(chapters)")
        } | {r[1] for r in conn.execute("PRAGMA index_list(encodings)")}
    assert "idx_chapters_blob_profile" in indexes  # required (spec 7.7)
    assert "idx_chapters_novel_order" in indexes  # unique upsert key
    assert "idx_encodings_zstd_dict_id" in indexes  # recommended (spec 7.7)


def test_migrations_idempotent(tmp_path):
    profiles = {"raw": Profile(name="raw", codec="none", params={})}
    path = tmp_path / "mig"
    repo = create_repo(path=path, backend_mode="sqlite_single", profiles=profiles)
    repo.store.put_bytes(b"x", profile="raw").result
    with repo.backend.txn(write=False) as conn:
        version_before = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_before >= LATEST_USER_VERSION
    # Reopening runs migrations again: must be a no-op with an unchanged version.
    repo2 = open_repo(path)
    assert repo2.store.get_bytes(repo.store.put_bytes(b"x", profile="raw").result.ref) == b"x"
    with repo.backend.txn(write=False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == version_before


def test_user_version_tracks_index_schema(repo):
    """PRAGMA user_version reflects the index schema in both backend modes."""
    with repo.backend.txn(write=False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == LATEST_USER_VERSION


def test_payload_migrations_idempotent(tmp_path):
    path = tmp_path / "payload-mig.sqlite"
    conn = sqlite3.connect(str(path))
    try:
        # Order must not matter: payload-then-index and index-then-payload
        # both converge because the two chains keep separate counters.
        migrate_payload(conn)
        migrate_payload(conn)
        migrate_index(conn)
        migrate_index(conn)
        assert "payload" in _table_names(conn)
        assert "encodings" in _table_names(conn)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == LATEST_USER_VERSION
        row = conn.execute("SELECT version FROM _inkpack_schema WHERE key='payload'").fetchone()
        assert row is not None and row[0] == 1
    finally:
        conn.close()


def test_repo_config_stored_on_create(repo):
    assert repo.backend.config_get("identity_policy") == "ikb1"
    assert repo.backend.config_get("backend_mode") == repo.backend.mode
    assert repo.backend.config_get("verify_on_read") is False
    profiles = repo.backend.config_get("profiles")
    assert set(profiles) == {"raw", "zstd_nodict"}
    assert profiles["raw"] == {"codec": "none", "params": {}, "zstd_dict_id": None}
    if repo.backend.mode == "sqlite_sharded":
        assert repo.backend.config_get("shard_cap_bytes") == 2 << 20
        assert repo.backend.config_get("shard_min_bytes") == 1 << 20


def test_config_roundtrip(repo):
    repo.backend.config_set("custom", {"nested": [1, 2, {"b": 1, "a": 2}]})
    assert repo.backend.config_get("custom") == {"nested": [1, 2, {"b": 1, "a": 2}]}
    assert repo.backend.config_get("missing-key") is None


def test_open_repo_on_non_repo_raises(tmp_path):
    with pytest.raises(InkpackError):
        open_repo(tmp_path / "empty")


def test_open_repo_identity_policy_mismatch_raises(tmp_path):
    repo = create_repo(
        path=tmp_path / "id", backend_mode="sqlite_single", profiles={"raw": Profile("raw", "none")}
    )
    repo.backend.config_set("identity_policy", "other")
    with pytest.raises(InkpackError):
        open_repo(tmp_path / "id")


def test_open_repo_backend_mode_mismatch_raises(tmp_path):
    repo = create_repo(
        path=tmp_path / "bm", backend_mode="sqlite_single", profiles={"raw": Profile("raw", "none")}
    )
    repo.backend.config_set("backend_mode", "sqlite_sharded")
    with pytest.raises(InkpackError):
        open_repo(tmp_path / "bm")


def test_open_repo_missing_profiles_raises(tmp_path):
    repo = create_repo(
        path=tmp_path / "np", backend_mode="sqlite_single", profiles={"raw": Profile("raw", "none")}
    )
    with repo.backend.txn(write=True) as conn:
        conn.execute("DELETE FROM repo_config WHERE key='profiles'")
    with pytest.raises(InkpackError):
        open_repo(tmp_path / "np")


def test_create_repo_validation(tmp_path):
    with pytest.raises(ValueError):
        create_repo(tmp_path / "a", backend_mode="sqlite_single", profiles=None)
    with pytest.raises(ValueError):
        create_repo(tmp_path / "b", backend_mode="nope", profiles={"raw": Profile("raw", "none")})
    with pytest.raises(ValueError):
        create_repo(
            tmp_path / "c",
            backend_mode="sqlite_single",
            profiles={"bad": Profile("bad", "lzma", {})},
        )
    with pytest.raises(ValueError):
        create_repo(tmp_path / "d", backend_mode="sqlite_sharded", shard_cap_bytes=0, profiles={"raw": Profile("raw", "none")})
    with pytest.raises(ValueError):
        create_repo(tmp_path / "e", backend_mode="sqlite_sharded", shard_min_bytes=10, shard_cap_bytes=5, profiles={"raw": Profile("raw", "none")})
    with pytest.raises(ValueError):
        create_repo(tmp_path / "f", backend_mode="sqlite_single", pragmas={"journal_mode": "WAL"}, profiles={"raw": Profile("raw", "none")})
    with pytest.raises(ValueError):
        create_repo(tmp_path / "g", backend_mode="sqlite_single", pragmas={"synchronous": "WHATEVER"}, profiles={"raw": Profile("raw", "none")})
    with pytest.raises(ValueError):
        create_repo(tmp_path / "h", backend_mode="sqlite_single", pragmas={"busy_timeout_ms": -1}, profiles={"raw": Profile("raw", "none")})
    with pytest.raises(TypeError):
        create_repo(tmp_path / "i", backend_mode="sqlite_single", profiles={"raw": "not-a-profile"})


def test_open_repo_restores_shard_caps(tmp_path):
    create_repo(
        path=tmp_path / "caps",
        backend_mode="sqlite_sharded",
        profiles={"raw": Profile("raw", "none")},
        shard_cap_bytes=1 << 20,
        shard_min_bytes=1 << 19,
    )
    reopened = open_repo(tmp_path / "caps")
    assert reopened.backend.shard_cap_bytes == 1 << 20
    assert reopened.backend.shard_min_bytes == 1 << 19
