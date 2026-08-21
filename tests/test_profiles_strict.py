"""Strict profile config parsing (review §9) and Profile immutability (review §25)."""

from __future__ import annotations

import pytest

from inkpack import InkpackError, Profile, create_repo, open_repo
from inkpack.types import profiles_from_config

from .conftest import make_profiles


def test_open_rejects_profile_codec_null(tmp_path):
    path = tmp_path / "c1"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    import sqlite3

    conn = sqlite3.connect(str(path / "repo.sqlite"))
    conn.execute("UPDATE repo_config SET value_json='{\"raw\": {\"codec\": null, \"params\": {}}}' WHERE key='profiles'")
    conn.commit()
    conn.close()
    with pytest.raises(InkpackError):
        open_repo(path)


def test_open_rejects_non_dict_entry(tmp_path):
    path = tmp_path / "c2"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    import sqlite3

    conn = sqlite3.connect(str(path / "repo.sqlite"))
    conn.execute("UPDATE repo_config SET value_json='{\"raw\": \"zstd\"}' WHERE key='profiles'")
    conn.commit()
    conn.close()
    with pytest.raises(InkpackError):
        open_repo(path)


def test_open_rejects_zstd_dict_on_none_codec(tmp_path):
    path = tmp_path / "c3"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    import sqlite3

    conn = sqlite3.connect(str(path / "repo.sqlite"))
    conn.execute(
        "UPDATE repo_config SET value_json='{\"raw\": {\"codec\": \"none\", \"params\": {}, "
        "\"zstd_dict_id\": \"ikd1:x\"}}' WHERE key='profiles'"
    )
    conn.commit()
    conn.close()
    with pytest.raises(InkpackError):
        open_repo(path)


def test_open_rejects_params_non_dict(tmp_path):
    path = tmp_path / "c4"
    create_repo(path=path, backend_mode="sqlite_single", profiles=make_profiles())
    import sqlite3

    conn = sqlite3.connect(str(path / "repo.sqlite"))
    conn.execute(
        "UPDATE repo_config SET value_json='{\"raw\": {\"codec\": \"zstd\", \"params\": 3}}' "
        "WHERE key='profiles'"
    )
    conn.commit()
    conn.close()
    with pytest.raises(InkpackError):
        open_repo(path)


def test_profiles_from_config_strict_unit():
    with pytest.raises(InkpackError):
        profiles_from_config({"raw": "zstd"})
    with pytest.raises(InkpackError):
        profiles_from_config({"raw": {"codec": None, "params": {}}})
    with pytest.raises(InkpackError):
        profiles_from_config({"raw": {"codec": "zstd", "params": [], "zstd_dict_id": None}})
    with pytest.raises(InkpackError):
        profiles_from_config({"raw": {"codec": "zstd", "params": {}, "zstd_dict_id": 5}})
    assert profiles_from_config(None) == {}
    assert profiles_from_config({}) == {}
    parsed = profiles_from_config({"raw": {"codec": "none", "params": {"a": 1}, "zstd_dict_id": None}})
    assert parsed["raw"].params["a"] == 1


def test_get_profiles_on_corrupt_config_raises(repo):
    repo.backend.config_set("profiles", {"bad": "not-a-dict"})
    with pytest.raises(InkpackError):
        repo.get_profiles()
    # And the write path refuses too (no silent half-parsed profile).
    with pytest.raises(InkpackError):
        repo.store.put_bytes(b"x", profile="bad").result


# -- §25 Profile immutability -------------------------------------------------


def test_profile_params_immutable():
    profile = Profile(name="z", codec="zstd", params={"level": 3})
    with pytest.raises(TypeError):
        profile.params["level"] = 9  # type: ignore[index]
    assert profile.params["level"] == 3


def test_profile_params_copied():
    params = {"level": 3}
    profile = Profile(name="z", codec="zstd", params=params)
    params["level"] = 9
    assert profile.params["level"] == 3


def test_set_profile_does_not_alias_caller_dict(repo):
    params = {"level": 3}
    repo.set_profile(Profile(name="zstd_alias", codec="zstd", params=params))
    params["level"] = 9
    assert repo.get_profile("zstd_alias").params["level"] == 3


def test_profile_equality_still_works():
    assert Profile(name="a", codec="none", params={}) == Profile(name="a", codec="none", params={})
    assert Profile(name="a", codec="zstd", params={"level": 3}) == Profile(
        name="a", codec="zstd", params={"level": 3}
    )
