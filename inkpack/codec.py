from __future__ import annotations

import hashlib
import io
import json
import zlib
from dataclasses import dataclass
from typing import Iterable

from .types import Cancelled, Profile


def _check_cancel(cancel) -> None:
    if cancel and cancel():
        raise Cancelled("operation cancelled")


def canonical_json(obj) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def blob_key_ikb1_stream(fp, size_hint=None, cancel=None) -> tuple[str, int, str, str]:
    del size_hint
    h1 = hashlib.sha256()
    h2 = hashlib.blake2b(digest_size=16)
    raw_len = 0
    while True:
        _check_cancel(cancel)
        chunk = fp.read(1024 * 128)
        if not chunk:
            break
        raw_len += len(chunk)
        h1.update(chunk)
        h2.update(chunk)
    sha = h1.hexdigest()
    b2 = h2.hexdigest()
    return f"ikb1:{raw_len}:{sha}:{b2}", raw_len, sha, b2


def blob_key_ikb1_bytes(data: bytes) -> tuple[str, int, str, str]:
    return blob_key_ikb1_stream(io.BytesIO(data))


def parse_blob_key_ikb1(blob_key: str) -> tuple[int, str, str]:
    parts = blob_key.split(":")
    if len(parts) != 4 or parts[0] != "ikb1":
        raise ValueError("invalid blob_key")
    raw_len = int(parts[1])
    sha = parts[2]
    b2 = parts[3]
    if len(sha) != 64 or len(b2) != 32:
        raise ValueError("invalid blob_key digest lengths")
    return raw_len, sha, b2


@dataclass(frozen=True)
class Encoded:
    codec: str
    codec_params_json: str
    zstd_dict_id: str | None
    data: bytes
    stored_len: int


class CodecEngine:
    def encode(self, raw: bytes, profile: Profile, dict_bytes: bytes | None) -> Encoded:
        if profile.codec == "none":
            payload = raw
            params_json = canonical_json(profile.params or {})
            return Encoded(
                codec="none",
                codec_params_json=params_json,
                zstd_dict_id=None,
                data=payload,
                stored_len=len(payload),
            )
        if profile.codec != "zstd":
            raise ValueError(f"unsupported codec: {profile.codec}")
        level = int((profile.params or {}).get("level", 6))
        comp = zlib.compressobj(level=level, wbits=zlib.MAX_WBITS, zdict=dict_bytes or b"")
        payload = comp.compress(raw) + comp.flush()
        params_json = canonical_json({"level": level})
        return Encoded(
            codec="zstd",
            codec_params_json=params_json,
            zstd_dict_id=profile.zstd_dict_id if dict_bytes else None,
            data=payload,
            stored_len=len(payload),
        )

    def decode(self, encoded: bytes, *, codec: str, codec_params_json: str, dict_bytes: bytes | None) -> bytes:
        del codec_params_json
        if codec == "none":
            return encoded
        if codec != "zstd":
            raise ValueError(f"unsupported codec: {codec}")
        d = zlib.decompressobj(wbits=zlib.MAX_WBITS, zdict=dict_bytes or b"")
        return d.decompress(encoded) + d.flush()

    def train_dict(self, samples: Iterable[bytes], options=None, cancel=None) -> tuple[bytes, str | None, int, int]:
        options = options or {}
        dict_size = int(options.get("dict_size", 4096))
        chunks: list[bytes] = []
        sample_bytes = 0
        samples_used = 0
        for sample in samples:
            _check_cancel(cancel)
            if not sample:
                continue
            samples_used += 1
            sample_bytes += len(sample)
            tail = sample[-min(len(sample), 256) :]
            chunks.append(tail)
            if sum(len(x) for x in chunks) >= dict_size:
                break
        blob = b"".join(chunks)
        if len(blob) > dict_size:
            blob = blob[-dict_size:]
        params_json = canonical_json({"dict_size": len(blob)})
        return blob, params_json, samples_used, sample_bytes


def dict_id_for_bytes(dict_bytes: bytes) -> str:
    return "ikd1:" + hashlib.sha256(dict_bytes).hexdigest()
