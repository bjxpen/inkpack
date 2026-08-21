"""Factory contract tests (review §1): create vs open, layout detection,
refusal semantics, NotFound on missing repos."""

from __future__ import annotations

import pytest

from inkpack import InkpackError, NotFound, create_repo, open_repo
from inkpack.sqlite import LATEST_USER_VERSION

from .conftest import make_profiles


@pytest.mark.parametrize("mode", ["sqlite_single", "sqlite_sharded"])
def test_create_then_open_roundtrip(tmp_path, mode):
    path = tmp_path / "rt"
    repo = create_repo(path=path, backend_mode=mode, profiles=make_profiles())
    put = repo.store.put_bytes(b"roundtrip \x00\xff" * 50, profile="raw").result
    assert repo.backend.config_get("identity_policy") == "ikb1"
    assert repo.backend.config_get("backend_mode") == mode
    assert set(repo.backend.config_get("profiles")) == {"raw", "zstd_nodict"}

    reopened = open_repo(path)
    assert reopened.backend.mode == mode
    assert reopened.store.get_bytes(put.ref) == b"roundtrip \x00\xff" * 50
    assert reopened.get_profiles() == make_profiles()


def test_create_repo_refuses_existing(tmp_path):
    path = tmp_path / "exists"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    with pytest.raises(InkpackError, match="already exists"):
        create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    # Sharded layout also refused by a single-mode create attempt and vice versa.
    sharded = tmp_path / "exists-sharded"
    create_repo(path=sharded, backend_mode="sqlite_sharded", profiles=make_profiles())
    with pytest.raises(InkpackError, match="already exists"):
        create_repo(path=sharded, backend_mode="sqlite_single", profiles=make_profiles())


def test_open_repo_missing_path_does_not_create(tmp_path):
    missing = tmp_path / "never-created"
    with pytest.raises(NotFound):
        open_repo(missing)
    assert not (missing / "repo.sqlite").exists()
    assert not (missing / "index.sqlite").exists()
    assert not missing.exists()  # nothing was mkdir'd either


def test_open_repo_empty_profiles_raises(tmp_path):
    repo = create_repo(path=tmp_path / "ep", backend_mode="sqlite_single", profiles=make_profiles())
    repo.backend.config_set("profiles", {})
    with pytest.raises(InkpackError):
        open_repo(tmp_path / "ep")


def test_open_repo_ambiguous_layout_raises(tmp_path):
    path = tmp_path / "amb"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    (path / "index.sqlite").touch()
    (path / "payload").mkdir(exist_ok=True)
    with pytest.raises(InkpackError, match="ambiguous"):
        open_repo(path)


def test_open_repo_runs_migrations_twice(tmp_path):
    path = tmp_path / "mig"
    repo = create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    repo.store.put_bytes(b"mig", profile="raw").result
    open_repo(path)
    reopened = open_repo(path)
    with reopened.backend.txn(write=False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == LATEST_USER_VERSION


def test_sqlite_backend_open_never_creates(tmp_path):
    from inkpack.sqlite import SqliteBackend

    with pytest.raises(NotFound):
        SqliteBackend.open(tmp_path / "nope", "sqlite_single")
    assert not (tmp_path / "nope").exists()


def test_sqlite_backend_create_then_open(tmp_path):
    from inkpack.sqlite import SqliteBackend

    path = tmp_path / "bc"
    backend = SqliteBackend.create(path, "sqlite_single")
    assert backend.index_path.exists()
    reopened = SqliteBackend.open(path, "sqlite_single")
    assert reopened.index_path.exists()
