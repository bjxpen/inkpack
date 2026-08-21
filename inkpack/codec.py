"""Identity policy (``ikb1``), canonical JSON and the codec engine.

Owns the single-pass streaming identity used for blob keys and dedupe, the
canonical JSON serialization stored in ``encodings.codec_params_json``, and
the pluggable codec engine (``none`` and dictionary-capable ``zstd``).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol

import zstandard as _zstd

from .types import CancelToken, CorruptContent, Profile, check_cancel

CHUNK_SIZE = 128 * 1024


def canonical_json(obj: Any) -> str:
    """Stable canonical JSON (sorted keys, compact separators; spec 7.3 / decision B)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


# ---------------------------------------------------------------------------
# ikb1 identity policy (spec 3.2)
# ---------------------------------------------------------------------------


class Identity(Protocol):
    """Repo-level identity policy: computes and parses blob keys."""

    name: str

    def key_bytes(self, data: bytes) -> tuple[str, int, str, str]:
        """Return ``(blob_key, raw_len, sha256_hex, blake2b128_hex)`` for bytes."""
        ...

    def key_stream(
        self,
        fp: BinaryIO,
        size_hint: int | None = None,
        cancel: CancelToken | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int, str, str]:
        """Single-pass identity over a binary stream."""
        ...

    def key_from_chunks(
        self,
        chunks: Iterable[bytes],
        cancel: CancelToken | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int, str, str]:
        """Single-pass identity over an iterable of byte chunks.

        ``on_bytes`` is invoked with the cumulative byte count after each
        chunk, letting callers surface progress without coupling the identity
        to the operation protocol.
        """
        ...

    def parse(self, blob_key: str) -> tuple[int, str, str]:
        """Extract ``(raw_len, sha256_hex, blake2b128_hex)``; raise on bad format."""
        ...

    def hasher(self) -> IdentityHasher:
        """Return an incremental hasher for single-pass hashing of a stream
        while the caller controls the loop (live progress, review P2-3)."""
        ...


class IdentityHasher(Protocol):
    def update(self, chunk: bytes) -> None: ...

    def digest(self) -> tuple[str, int, str, str]:
        """Finalize: ``(blob_key, raw_len, sha256_hex, blake2b128_hex)``."""
        ...


class _IKB1Hasher:
    def __init__(self) -> None:
        self._sha = hashlib.sha256()
        self._blake = hashlib.blake2b(digest_size=16)
        self._raw_len = 0

    def update(self, chunk: bytes) -> None:
        self._raw_len += len(chunk)
        self._sha.update(chunk)
        self._blake.update(chunk)

    def digest(self) -> tuple[str, int, str, str]:
        sha_hex, blake_hex = self._sha.hexdigest(), self._blake.hexdigest()
        return f"ikb1:{self._raw_len}:{sha_hex}:{blake_hex}", self._raw_len, sha_hex, blake_hex


class IKB1Identity:
    """Default ``ikb1`` policy: ``ikb1:<raw_len>:<sha256>:<blake2b-128>``."""

    name = "ikb1"

    def hasher(self) -> IdentityHasher:
        return _IKB1Hasher()

    def key_from_chunks(
        self,
        chunks: Iterable[bytes],
        cancel: CancelToken | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int, str, str]:
        sha = hashlib.sha256()
        blake = hashlib.blake2b(digest_size=16)
        raw_len = 0
        for chunk in chunks:
            check_cancel(cancel)
            raw_len += len(chunk)
            if on_bytes is not None:
                on_bytes(raw_len)
            sha.update(chunk)
            blake.update(chunk)
        sha_hex, blake_hex = sha.hexdigest(), blake.hexdigest()
        return f"ikb1:{raw_len}:{sha_hex}:{blake_hex}", raw_len, sha_hex, blake_hex

    def key_bytes(self, data: bytes) -> tuple[str, int, str, str]:
        return self.key_from_chunks([data])

    def key_stream(
        self,
        fp: BinaryIO,
        size_hint: int | None = None,
        cancel: CancelToken | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int, str, str]:
        del size_hint  # the stream is hashed exactly; the hint only reserves space
        return self.key_from_chunks(_iter_reads(fp), cancel, on_bytes=on_bytes)

    def parse(self, blob_key: str) -> tuple[int, str, str]:
        parts = blob_key.split(":")
        if len(parts) != 4 or parts[0] != "ikb1":
            raise ValueError(f"invalid blob_key: {blob_key!r}")
        _, raw_len_s, sha, blake = parts
        if not raw_len_s.isdigit() or len(sha) != 64 or len(blake) != 32:
            raise ValueError(f"invalid blob_key: {blob_key!r}")
        try:
            int(sha, 16)
            int(blake, 16)
        except ValueError as exc:
            raise ValueError(f"invalid blob_key: {blob_key!r}") from exc
        return int(raw_len_s), sha, blake


def _iter_reads(fp: BinaryIO) -> Iterator[bytes]:
    while True:
        chunk = fp.read(CHUNK_SIZE)
        if not chunk:
            return
        yield chunk


# Singleton used as the default identity; module-level functions kept as a
# convenience for callers that want plain function access (e.g. tests).
IKB1 = IKB1Identity()


def blob_key_ikb1_bytes(data: bytes) -> tuple[str, int, str, str]:
    return IKB1.key_bytes(data)


def blob_key_ikb1_stream(
    fp: BinaryIO,
    size_hint: int | None = None,
    cancel: CancelToken | None = None,
) -> tuple[str, int, str, str]:
    return IKB1.key_stream(fp, size_hint=size_hint, cancel=cancel)


def blob_key_ikb1_from_chunks(
    chunks: Iterable[bytes],
    cancel: CancelToken | None = None,
) -> tuple[str, int, str, str]:
    return IKB1.key_from_chunks(chunks, cancel=cancel)


def parse_blob_key_ikb1(blob_key: str) -> tuple[int, str, str]:
    return IKB1.parse(blob_key)


# ---------------------------------------------------------------------------
# Codec engine
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Encoded:
    codec: str
    codec_params_json: str
    zstd_dict_id: str | None
    data: bytes
    stored_len: int


def dict_id_for_bytes(dict_bytes: bytes) -> str:
    """Stable content-addressed id for a trained dictionary."""
    return "ikd1:" + hashlib.sha256(dict_bytes).hexdigest()


class CodecEngine:
    """Encoding engine: ``none`` (identity) and ``zstd`` (with optional dict).

    ``decode`` deliberately takes only *stored* encoding metadata (spec 6.2);
    it never receives a :class:`Profile`.

    All zstd engine failures are translated to typed API errors so callers
    never see raw ``zstandard`` exceptions:
    - encode-side problems (bad profile params, unusable dictionary) become
      :class:`ValueError` with an actionable message;
    - decode-side problems become :class:`CorruptContent`.
    """

    dict_codec = "zstd"

    def encode(self, raw: bytes, profile: Profile, dict_bytes: bytes | None) -> Encoded:
        if profile.codec == "none":
            return Encoded(
                codec="none",
                codec_params_json=canonical_json({}),
                zstd_dict_id=None,
                data=raw,
                stored_len=len(raw),
            )
        if profile.codec != "zstd":
            raise ValueError(f"unsupported codec: {profile.codec!r}")
        level = profile.params.get("level", 6)
        if not isinstance(level, int) or isinstance(level, bool) or not 1 <= level <= 22:
            raise ValueError(f"zstd level must be an integer in 1..22, got {level!r}")
        try:
            dict_data = _zstd.ZstdCompressionDict(dict_bytes) if dict_bytes is not None else None
            payload = _zstd.ZstdCompressor(level=level, dict_data=dict_data).compress(raw)
        except Exception as exc:
            raise ValueError(
                f"zstd encode failed (profile {profile.name!r}, level {level}): {exc}"
            ) from exc
        return Encoded(
            codec="zstd",
            codec_params_json=canonical_json({"level": level}),
            zstd_dict_id=profile.zstd_dict_id if dict_bytes is not None else None,
            data=payload,
            stored_len=len(payload),
        )

    def decode(
        self,
        encoded: bytes,
        *,
        codec: str,
        codec_params_json: str,
        dict_bytes: bytes | None,
        max_output_size: int | None = None,
    ) -> bytes:
        del codec_params_json  # kept in the contract; zstd decoding needs no params
        if codec == "none":
            return encoded
        if codec != "zstd":
            raise ValueError(f"unsupported codec: {codec!r}")
        if max_output_size is not None:
            # Bound decompressed output using the canonical raw_len from the
            # blob_key (review P2-6). One-shot decompress grows its output
            # buffer regardless of max_output_size, so the real guard is the
            # frame header's declared content size, checked BEFORE any
            # allocation happens. Frames without a declared size are only
            # produced by external tooling; our compressor always writes one.
            try:
                declared = _zstd.frame_content_size(encoded)
            except Exception as exc:
                raise CorruptContent(f"zstd decode failed: {exc}") from exc
            if declared and declared > max_output_size:
                raise CorruptContent(
                    f"zstd frame declares {declared} bytes, exceeding the bound of "
                    f"{max_output_size} declared by the blob_key"
                )
        try:
            dict_data = _zstd.ZstdCompressionDict(dict_bytes) if dict_bytes is not None else None
            return _zstd.ZstdDecompressor(dict_data=dict_data).decompress(encoded)
        except Exception as exc:
            raise CorruptContent(f"zstd decode failed: {exc}") from exc

    def train_dict(
        self,
        samples: Iterable[bytes],
        options: dict[str, Any] | None = None,
        cancel: CancelToken | None = None,
    ) -> tuple[bytes, str, int, int]:
        """Train a zstd dictionary; returns ``(dict_bytes, params_json, samples_used, sample_bytes)``."""
        options = dict(options or {})
        unknown = set(options) - {"dict_size"}
        if unknown:
            raise ValueError(f"unknown train_dict options: {sorted(unknown)}")
        dict_size = options.get("dict_size", 4096)
        if not isinstance(dict_size, int) or isinstance(dict_size, bool) or not 256 <= dict_size <= (1 << 26):
            raise ValueError(f"dict_size must be an integer in 256..67108864, got {dict_size!r}")

        sample_list: list[bytes | bytearray | memoryview] = []
        sample_bytes = 0
        for sample in samples:
            check_cancel(cancel)
            if not sample:
                continue
            sample_list.append(sample)
            sample_bytes += len(sample)
        # zstd's dictionary trainer (fastCover) has hard input minimums:
        # with split_point=1.0 (all samples used for training) it needs at
        # least 5 samples and at least 8 total bytes of sample data.
        if len(sample_list) < 5:
            raise ValueError(
                f"train_dict requires at least 5 non-empty samples (got {len(sample_list)}); "
                "zstd needs enough material to build a dictionary - train on the novel's "
                "chapter bodies, or aggregate shorter chapters into one sample each"
            )
        if sample_bytes < 8:
            raise ValueError(
                f"train_dict requires at least 8 bytes of sample data (got {sample_bytes})"
            )
        try:
            dict_bytes = _zstd.train_dictionary(dict_size, sample_list, split_point=1.0).as_bytes()
        except Exception as exc:
            raise ValueError(
                f"zstd dictionary training failed (dict_size={dict_size}, "
                f"{len(sample_list)} samples, {sample_bytes} bytes): {exc}"
            ) from exc
        params_json = canonical_json({"dict_size": len(dict_bytes), "samples_used": len(sample_list)})
        return dict_bytes, params_json, len(sample_list), sample_bytes
