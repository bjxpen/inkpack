"""Identity policy (``ikb1``), canonical JSON and the codec engine.

Owns the single-pass streaming identity used for blob keys and dedupe, the
canonical JSON serialization stored in ``encodings.codec_params_json``, and
the pluggable codec engine (``none`` and dictionary-capable ``zstd``).
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol

import zstandard as _zstd

from .types import CancelToken, CorruptContent, Profile, check_cancel

CHUNK_SIZE = 128 * 1024

# Strict ASCII-canonical ikb1 key format (locked semantics S4): ASCII digits
# only, lowercase hex, exact lengths. Unicode digits, uppercase hex, other
# prefixes and malformed lengths are all rejected.
# Strict ASCII-canonical ikb1 key format (locked semantics S4 / Issue 14):
# ASCII digits (leading zeros ALLOWED per S4), lowercase hex, exact lengths,
# and a hard end anchor so a trailing newline is rejected. Keys this process
# writes never have leading zeros; parse accepts the documented alphabet.
_IKB1_RE = re.compile(r"^ikb1:[0-9]+:[0-9a-f]{64}:[0-9a-f]{32}\Z")


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
        """Extract ``(raw_len, sha256_hex, blake2b128_hex)``.

        Strict canonical parse (locked semantics S4): only the exact
        ``ikb1:<ascii-digits>:<64 lowercase hex>:<32 lowercase hex>`` shape is
        accepted.
        """
        if not _IKB1_RE.fullmatch(blob_key):
            raise ValueError(f"invalid blob_key: {blob_key!r}")
        _, raw_len_s, sha, blake = blob_key.split(":")
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


def validate_train_dict_options(options: dict[str, Any] | None) -> dict[str, Any]:
    """Validate ``train_dict`` options (shared by call-time eager validation
    and the codec engine)."""
    options = dict(options or {})
    unknown = set(options) - {"dict_size"}
    if unknown:
        raise ValueError(f"unknown train_dict options: {sorted(unknown)}")
    dict_size = options.get("dict_size", 4096)
    if not isinstance(dict_size, int) or isinstance(dict_size, bool) or not 256 <= dict_size <= (1 << 26):
        raise ValueError(f"dict_size must be an integer in 256..67108864, got {dict_size!r}")
    return options


class CodecEngine:
    """Encoding engine: ``none`` (identity) and ``zstd`` (with optional dict).

    ``decode`` deliberately takes only *stored* encoding metadata (spec 6.2);
    it never receives a :class:`Profile`.

    All zstd engine failures are translated to typed API errors so callers
    never see raw ``zstandard`` exceptions:
    - encode-side problems (bad profile params, unusable dictionary) become
      :class:`ValueError` with an actionable message;
    - decode-side problems become :class:`CorruptContent`;
    - ``MemoryError`` propagates (it is a resource failure, never a content
      or policy error — A7).

    zstd contexts are cached per (level, dict id) / (dict id) in an LRU
    (C2): a bulk reencode over N targets builds one compressor instead of
    N. The one-shot ``compress``/``decompress`` APIs are share-safe in
    python-zstandard (the dependency is pinned for this reason); an engine
    is used from the threads that drive its repository's operations.
    """

    dict_codec = "zstd"
    _CONTEXT_CACHE_MAX = 32

    def __init__(self) -> None:
        self._compressors: OrderedDict[tuple[int, str | None], _zstd.ZstdCompressor] = OrderedDict()
        self._decompressors: OrderedDict[tuple[str | None], _zstd.ZstdDecompressor] = OrderedDict()
        # P1.7: the context cache is process-wide and lock-protected —
        # independent operations on different threads may share one engine.
        # The lock covers BOTH the get-or-create AND the cache-hit
        # move_to_end (the hit path mutates LRU order too).
        self._cache_lock = threading.Lock()

    # -- context cache (C2) ---------------------------------------------------

    def _compressor(
        self, level: int, dict_bytes: bytes | None, dict_cache_key: str | None
    ) -> _zstd.ZstdCompressor:
        """Get-or-create the (level, dict id) compressor; LRU at 32.

        A dict whose id is unknown to the caller is deliberately NOT cached
        (keying by ``id(bytes)`` would be wrong; an uncached context is only
        a performance loss, never a correctness one).
        """
        if dict_bytes is not None and dict_cache_key is None:
            dict_data_eager = _zstd.ZstdCompressionDict(dict_bytes)
            return _zstd.ZstdCompressor(level=level, dict_data=dict_data_eager)
        key = (level, dict_cache_key)
        with self._cache_lock:  # P1.7: covers get-or-create AND hit move_to_end
            compressor = self._compressors.get(key)
            if compressor is None:
                dict_data = _zstd.ZstdCompressionDict(dict_bytes) if dict_bytes is not None else None
                compressor = _zstd.ZstdCompressor(level=level, dict_data=dict_data)
                self._compressors[key] = compressor
                while len(self._compressors) > self._CONTEXT_CACHE_MAX:
                    self._compressors.popitem(last=False)
            else:
                self._compressors.move_to_end(key)
        return compressor

    def _decompressor(self, dict_bytes: bytes | None, dict_cache_key: str | None) -> _zstd.ZstdDecompressor:
        if dict_bytes is not None and dict_cache_key is None:
            dict_data_eager = _zstd.ZstdCompressionDict(dict_bytes)
            return _zstd.ZstdDecompressor(dict_data=dict_data_eager)
        key = (dict_cache_key,)
        with self._cache_lock:  # P1.7: covers get-or-create AND hit move_to_end
            decompressor = self._decompressors.get(key)
            if decompressor is None:
                dict_data = _zstd.ZstdCompressionDict(dict_bytes) if dict_bytes is not None else None
                decompressor = _zstd.ZstdDecompressor(dict_data=dict_data)
                self._decompressors[key] = decompressor
                while len(self._decompressors) > self._CONTEXT_CACHE_MAX:
                    self._decompressors.popitem(last=False)
            else:
                self._decompressors.move_to_end(key)
        return decompressor

    def encode(
        self,
        raw: bytes,
        profile: Profile,
        dict_bytes: bytes | None,
        *,
        dict_cache_key: str | None = None,
    ) -> Encoded:
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
            payload = self._compressor(level, dict_bytes, dict_cache_key).compress(raw)
        except MemoryError:
            raise  # OOM is a resource failure, never an encoding-policy error (A7)
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
        dict_cache_key: str | None = None,
    ) -> bytes:
        # codec_params_json is intentionally ignored for decode: 'none' and
        # zstd store everything needed in the frame; params only shape writes
        # (review L27).
        del codec_params_json
        if codec == "none":
            # Locked semantics S3: codec 'none' stores the raw bytes, so the
            # stored length must equal the blob_key's declared raw_len.
            if max_output_size is not None and len(encoded) != max_output_size:
                raise CorruptContent(
                    f"codec 'none' payload length {len(encoded)} does not match the "
                    f"declared raw_len {max_output_size}"
                )
            return encoded
        if codec != "zstd":
            raise ValueError(f"unsupported codec: {codec!r}")
        if max_output_size is not None:
            # Locked semantics S3: enforce the blob_key's raw_len as a hard
            # decode bound. The frame header's declared content size is
            # checked BEFORE any allocation; a frame with no declared size is
            # rejected when bounded (one-shot decompress would otherwise grow
            # its output buffer regardless of max_output_size).
            try:
                declared = _zstd.frame_content_size(encoded)
            except MemoryError:
                raise
            except Exception as exc:
                raise CorruptContent(f"zstd decode failed: {exc}") from exc
            if declared < 0:
                # zstandard reports -1 when the frame header carries no
                # content size; a bound then cannot be enforced.
                raise CorruptContent(
                    "zstd frame has no declared content size; cannot bound decode to "
                    f"{max_output_size} bytes"
                )
            if declared > max_output_size:
                raise CorruptContent(
                    f"zstd frame declares {declared} bytes, exceeding the bound of "
                    f"{max_output_size} declared by the blob_key"
                )
        decompressor = self._decompressor(dict_bytes, dict_cache_key)
        try:
            if max_output_size is not None:
                return decompressor.decompress(encoded, max_output_size=max_output_size)
            return decompressor.decompress(encoded)
        except MemoryError:
            raise  # A7
        except Exception as exc:
            raise CorruptContent(f"zstd decode failed: {exc}") from exc

    def train_dict(
        self,
        samples: Iterable[bytes],
        options: dict[str, Any] | None = None,
        cancel: CancelToken | None = None,
    ) -> tuple[bytes, str, int, int]:
        """Train a zstd dictionary; returns ``(dict_bytes, params_json, samples_used, sample_bytes)``."""
        options = validate_train_dict_options(options)
        dict_size = options.get("dict_size", 4096)

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
