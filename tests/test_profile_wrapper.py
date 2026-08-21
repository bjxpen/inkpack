"""Tests for the Repository profile-management wrappers.

These wrappers exist so normal usage never needs ``repo.backend.config_get/
config_set("profiles", ...)``.
"""

from __future__ import annotations

import pytest

from inkpack import Profile, open_repo

from .conftest import make_profiles


def test_get_profiles_returns_profile_objects(repo):
    profiles = repo.get_profiles()
    assert set(profiles) == {"raw", "zstd_nodict"}
    assert profiles["raw"] == Profile(name="raw", codec="none", params={})
    assert profiles["zstd_nodict"] == Profile(name="zstd_nodict", codec="zstd", params={"level": 3})
    assert all(isinstance(p, Profile) for p in profiles.values())


def test_get_profiles_roundtrips_initial_config(repo):
    assert repo.get_profiles() == make_profiles()


def test_get_profile_single(repo):
    assert repo.get_profile("raw").codec == "none"
    assert repo.get_profile("zstd_nodict").params == {"level": 3}
    with pytest.raises(KeyError):
        repo.get_profile("no-such-profile")


def test_set_profile_adds_and_persists(tmp_path, repo):
    repo.set_profile(Profile(name="zstd_hi", codec="zstd", params={"level": 9}))
    assert repo.get_profile("zstd_hi").params == {"level": 9}
    # Persists across reopen.
    reopened = open_repo(tmp_path / f"repo-{repo.backend.mode}")
    assert reopened.get_profile("zstd_hi").codec == "zstd"
    # Usable for writes immediately.
    put = repo.store.put_bytes(b"hi " * 100, profile="zstd_hi").result
    assert repo.store.get_bytes(put.ref) == b"hi " * 100


def test_set_profile_replaces_existing(repo):
    repo.set_profile(Profile(name="raw", codec="zstd", params={"level": 1}))
    profiles = repo.get_profiles()
    assert profiles["raw"] == Profile(name="raw", codec="zstd", params={"level": 1})
    assert len(profiles) == 2  # set grew, nothing else lost


def test_set_profile_validation(repo):
    with pytest.raises(ValueError):
        repo.set_profile(Profile(name="bad", codec="lzma", params={}))
    with pytest.raises(ValueError):
        repo.set_profile(Profile(name="bad", codec="none", params={}, zstd_dict_id="ikd1:x"))
    with pytest.raises(TypeError):
        repo.set_profile("not-a-profile")  # type: ignore[arg-type]


def test_set_profiles_replaces_all(repo):
    repo.set_profiles({"only": Profile(name="only", codec="none", params={})})
    assert set(repo.get_profiles()) == {"only"}
    put = repo.store.put_bytes(b"x", profile="only").result
    assert repo.store.get_bytes(put.ref) == b"x"
    with pytest.raises(KeyError):
        repo.store.put_bytes(b"y", profile="raw").result


def test_set_profiles_empty_raises(repo):
    with pytest.raises(ValueError):
        repo.set_profiles({})


def test_set_profiles_validation(repo):
    with pytest.raises(ValueError):
        repo.set_profiles({"a": Profile(name="b", codec="none", params={})})  # key/name mismatch
    with pytest.raises(TypeError):
        repo.set_profiles({"a": "nope"})  # type: ignore[dict-item]


def test_set_profiles_does_not_break_existing_content(repo):
    put = repo.store.put_bytes(b"stored-before", profile="zstd_nodict").result
    repo.set_profiles({"raw": Profile(name="raw", codec="none", params={})})
    # Decoding uses stored encoding metadata, never the profile set (spec 6.2).
    assert repo.store.get_bytes(put.ref) == b"stored-before"


def test_profile_wrapper_with_dictionary(repo):
    train = repo.store.train_dict([b"dict profile " * 50] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id=train.dict_id))
    assert repo.get_profile("zstd_dict").zstd_dict_id == train.dict_id
    put = repo.store.put_bytes(b"dict profile " * 500, profile="zstd_dict").result
    assert put.zstd_dict_id == train.dict_id
    # Stored config uses the canonical serialization shape.
    raw = repo.backend.config_get("profiles")
    assert raw["zstd_dict"] == {"codec": "zstd", "params": {"level": 6}, "zstd_dict_id": train.dict_id}


def test_set_profile_drop_dict_from_profile_keeps_decoding(repo):
    train = repo.store.train_dict([b"drop dict " * 50] * 6).result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}, zstd_dict_id=train.dict_id))
    put = repo.store.put_bytes(b"drop dict " * 300, profile="zstd_dict").result
    repo.set_profile(Profile(name="zstd_dict", codec="zstd", params={}))
    assert repo.get_profile("zstd_dict").zstd_dict_id is None
    # Stored encoding still references the dict; decoding unaffected.
    assert repo.store.get_bytes(put.ref) == b"drop dict " * 300
    verify = repo.store.verify().result
    assert verify.ok == 1


def test_get_profiles_after_reopen_roundtrip(tmp_path, repo):
    repo.set_profile(Profile(name="extra", codec="none", params={}))
    reopened = open_repo(tmp_path / f"repo-{repo.backend.mode}")
    assert reopened.get_profiles() == repo.get_profiles()
