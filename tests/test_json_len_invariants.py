"""Canonical JSON + stored_len invariant tests (spec 7.3, decision B)."""

from __future__ import annotations

import json

from .conftest import enc_row, payload_bytes


def _is_canonical(value: str) -> bool:
    obj = json.loads(value)
    expected = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return value == expected


def test_codec_params_json_is_canonical(repo):
    put = repo.store.put_bytes(b"canonical-params" * 50, profile="zstd_nodict").result
    row = enc_row(repo, put.ref)
    assert _is_canonical(row["codec_params_json"])
    assert json.loads(row["codec_params_json"]) == {"level": 3}


def test_none_codec_stores_empty_params_even_if_profile_has_params(repo):
    profiles = repo.backend.config_get("profiles")
    profiles["raw_noisy"] = {"codec": "none", "params": {"irrelevant": True}, "zstd_dict_id": None}
    repo.backend.config_set("profiles", profiles)
    put = repo.store.put_bytes(b"noisy", profile="raw_noisy").result
    row = enc_row(repo, put.ref)
    assert row["codec_params_json"] == "{}"


def test_stored_len_matches_payload_len_after_put(repo):
    for profile in ("raw", "zstd_nodict"):
        put = repo.store.put_bytes(b"len-check " * 300, profile=profile).result
        row = enc_row(repo, put.ref)
        payload = payload_bytes(repo, put.ref)
        assert row["stored_len"] == len(payload)
        assert row["stored_len"] == put.stored_len


def test_stored_len_matches_payload_len_after_reencode(repo):
    put = repo.store.put_bytes(b"reencode-len " * 300, profile="zstd_nodict").result
    repo.store.reencode([put.ref], options={"codec": "none"}).result
    row = enc_row(repo, put.ref)
    payload = payload_bytes(repo, put.ref)
    assert row["stored_len"] == len(payload)
    assert row["codec"] == "none"


def test_zstd_dict_id_null_unless_zstd_dict_used(repo):
    raw = repo.store.put_bytes(b"x" * 100, profile="raw").result
    plain = repo.store.put_bytes(b"y" * 100, profile="zstd_nodict").result
    assert enc_row(repo, raw.ref)["zstd_dict_id"] is None
    assert enc_row(repo, plain.ref)["zstd_dict_id"] is None
    assert enc_row(repo, plain.ref)["codec"] == "zstd"

    train = repo.store.train_dict([b"dicty " * 100] * 5).result
    profiles = repo.backend.config_get("profiles")
    profiles["zstd_dict"] = {"codec": "zstd", "params": {"level": 6}, "zstd_dict_id": train.dict_id}
    repo.backend.config_set("profiles", profiles)
    dicted = repo.store.put_bytes(b"dicty " * 400, profile="zstd_dict").result
    assert enc_row(repo, dicted.ref)["zstd_dict_id"] == train.dict_id


def test_meta_value_json_is_canonical(repo):
    repo.meta_set("novel", 1, "k", {"b": 1, "a": 2})
    with repo.backend.txn(write=False) as conn:
        row = conn.execute("SELECT value_json FROM meta WHERE key='k'").fetchone()
    assert row is not None
    assert _is_canonical(row[0])
    assert json.loads(row[0]) == {"a": 2, "b": 1}


def test_codec_params_roundtrip_via_reencode(repo):
    put = repo.store.put_bytes(b"params-roundtrip" * 100, profile="zstd_nodict").result
    before = enc_row(repo, put.ref)
    repo.store.reencode([put.ref], options={"params": {"level": 9}, "codec": "zstd"}).result
    after = enc_row(repo, put.ref)
    assert before["codec_params_json"] == '{"level":3}'
    assert after["codec_params_json"] == '{"level":9}'
    assert after["stored_len"] == len(payload_bytes(repo, put.ref))
    assert repo.store.get_bytes(put.ref) == b"params-roundtrip" * 100


def test_shard_id_locator_invariant(repo):
    """Every encoding has a locator consistent with its backend mode."""
    put = repo.store.put_bytes(b"locator", profile="raw").result
    row = enc_row(repo, put.ref)
    if repo.backend.mode == "sqlite_single":
        assert row["shard_id"] is None
    else:
        assert row["shard_id"] is not None
        assert repo.backend.shard_path(int(row["shard_id"])).exists()
