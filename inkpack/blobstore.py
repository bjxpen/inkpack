"""BlobStore: the content-addressable store (spec 4.3, 6, 9, 10).

Depends only on an injected :class:`SqliteBackend`, a :class:`CodecEngine` and
an :class:`Identity` policy. All long operations return an :class:`Operation`
that yields :class:`OpEvent` objects; the generator's ``return`` value becomes
``Operation.result``.
"""

from __future__ import annotations

import io
import tempfile
from collections.abc import Generator, Iterable, Iterator
from typing import Any, BinaryIO, cast

from .codec import IKB1, CodecEngine, Encoded, Identity, dict_id_for_bytes
from .sqlite import SqliteBackend
from .types import (
    CancelToken,
    CompactResult,
    ContentRef,
    CorruptContent,
    GcResult,
    MissingContent,
    Operation,
    OpEvent,
    Profile,
    PutResult,
    ReencodeResult,
    TrainDictResult,
    VerifyResult,
    check_cancel,
)

_READ_CHUNK = 128 * 1024
_SPOOL_MAX_SIZE = 2 * 1024 * 1024

_REENCODE_OPTION_KEYS = frozenset({"profile", "codec", "params", "zstd_dict_id"})
_COMPACT_OPTION_KEYS = frozenset({"shard_ids"})


class BlobStore:
    def __init__(
        self,
        backend: SqliteBackend,
        codec: CodecEngine | None = None,
        identity: Identity | None = None,
    ) -> None:
        self.backend = backend
        self.codec = codec or CodecEngine()
        self.identity: Identity = identity or IKB1

    # -- helpers ------------------------------------------------------------

    def _profile(self, name: str) -> Profile:
        raw = self.backend.config_get("profiles")
        if not isinstance(raw, dict):
            raise KeyError(f"profile not found: {name}")
        cfg = cast("dict[str, Any]", raw).get(name)
        if not isinstance(cfg, dict):
            raise KeyError(f"profile not found: {name}")
        cfg = cast("dict[str, Any]", cfg)
        return Profile(
            name=name,
            codec=str(cfg.get("codec")),
            params=cast("dict[str, Any]", cfg.get("params") or {}),
            zstd_dict_id=cfg.get("zstd_dict_id"),
        )

    def _load_dict(self, profile: Profile) -> bytes | None:
        if profile.zstd_dict_id is None:
            return None
        dict_bytes = self.backend.get_dict(profile.zstd_dict_id)
        if dict_bytes is None:
            raise MissingContent(
                f"dictionary {profile.zstd_dict_id!r} referenced by profile {profile.name!r} is missing"
            )
        return dict_bytes

    def _encode(self, raw: bytes, profile: Profile) -> Encoded:
        """Encode with the profile's policy, resolving its dictionary."""
        return self.codec.encode(raw, profile, self._load_dict(profile))

    def _store(self, *, blob_key: str, raw: bytes, profile: str, cancel: CancelToken | None) -> PutResult:
        """Dedupe check + encode + persist for an already-hashed payload."""
        check_cancel(cancel)
        existing = self.backend.get_encoding(blob_key, profile)
        if existing is not None:
            # Dedupe hit (decision E): report the stored encoding as-is.
            return PutResult(
                ref=ContentRef(blob_key=blob_key, profile=profile),
                raw_len=len(raw),
                stored_len=int(existing["stored_len"]),
                codec=str(existing["codec"]),
                zstd_dict_id=existing["zstd_dict_id"],
            )
        prof = self._profile(profile)
        enc = self._encode(raw, prof)
        shard_id = None
        if self.backend.mode == "sqlite_sharded":
            shard_id = self.backend.choose_shard_for_write(enc.stored_len)
        now = self.backend.now()
        self.backend.store_encoding_and_payload(
            blob_key=blob_key,
            raw_len=len(raw),
            created_at=now,
            profile=profile,
            codec=enc.codec,
            codec_params_json=enc.codec_params_json,
            zstd_dict_id=enc.zstd_dict_id,
            stored_len=enc.stored_len,
            checksum=None,
            shard_id=shard_id,
            updated_at=now,
            payload=enc.data,
        )
        return PutResult(
            ref=ContentRef(blob_key=blob_key, profile=profile),
            raw_len=len(raw),
            stored_len=enc.stored_len,
            codec=enc.codec,
            zstd_dict_id=enc.zstd_dict_id,
        )

    def _emit_put_done(self, result: PutResult) -> OpEvent:
        return OpEvent(
            kind="done",
            op="put",
            metrics={"bytes_in": result.raw_len, "bytes_out": result.stored_len},
        )

    # -- core API (spec 4.3) --------------------------------------------------

    def put_bytes(self, data: bytes, profile: str, cancel: CancelToken | None = None) -> Operation[PutResult]:
        def _run() -> Generator[OpEvent, None, PutResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="hash")
            blob_key, _, _, _ = self.identity.key_bytes(data)
            result = self._store(blob_key=blob_key, raw=data, profile=profile, cancel=cancel)
            yield self._emit_put_done(result)
            return result

        return Operation(_run)

    def put_stream(
        self,
        fp: BinaryIO,
        profile: str,
        size_hint: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[PutResult]:
        del size_hint  # identity is computed exactly; the hint is only advisory

        def _run() -> Generator[OpEvent, None, PutResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            # Single pass over the source stream: spool to disk while hashing,
            # so non-seekable sources and multi-GB chapters both work.
            with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MAX_SIZE) as spool:

                def spool_and_yield() -> Iterator[bytes]:
                    while True:
                        chunk = fp.read(_READ_CHUNK)
                        if not chunk:
                            return
                        spool.write(chunk)
                        yield chunk

                blob_key, _, _, _ = self.identity.key_from_chunks(spool_and_yield(), cancel)
                spool.seek(0)
                raw = spool.read()
            result = self._store(blob_key=blob_key, raw=raw, profile=profile, cancel=cancel)
            yield self._emit_put_done(result)
            return result

        return Operation(_run)

    def get_bytes(self, ref: ContentRef) -> bytes:
        """Decode using *stored* encoding metadata only (spec 6.2, 6.4)."""
        row = self.backend.get_encoding(ref.blob_key, ref.profile)
        if row is None:
            raise MissingContent(f"encoding not found for {ref}")
        payload = self.backend.get_payload(ref.blob_key, ref.profile, row["shard_id"])
        if payload is None:
            raise MissingContent(f"payload missing for {ref}")
        dict_bytes = None
        if row["zstd_dict_id"] is not None:
            dict_bytes = self.backend.get_dict(row["zstd_dict_id"])
            if dict_bytes is None:
                raise MissingContent(f"dictionary {row['zstd_dict_id']!r} missing for {ref}")
        try:
            raw = self.codec.decode(
                payload,
                codec=str(row["codec"]),
                codec_params_json=str(row["codec_params_json"]),
                dict_bytes=dict_bytes,
            )
        except (MissingContent, CorruptContent):
            raise
        except Exception as exc:
            raise CorruptContent(f"failed to decode {ref}: {exc}") from exc
        if bool(self.backend.config_get("verify_on_read") or False):
            got_key, _, _, _ = self.identity.key_bytes(raw)
            if got_key != ref.blob_key:
                raise CorruptContent(f"identity mismatch on read for {ref}")
        return raw

    def open(self, ref: ContentRef) -> io.BytesIO:
        """Materialized stable read handle (spec 6.3 MUST: ``BytesIO(get_bytes(ref))``)."""
        return io.BytesIO(self.get_bytes(ref))

    def has_blob(self, blob_key: str) -> bool:
        """True iff the ``blobs`` catalog contains the key (decision A)."""
        return self.backend.get_blob(blob_key) is not None

    # -- compression tools ---------------------------------------------------

    def train_dict(
        self,
        samples: Iterable[bytes],
        options: dict[str, Any] | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[TrainDictResult]:
        def _run() -> Generator[OpEvent, None, TrainDictResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="train_dict")
            dict_bytes, params_json, samples_used, sample_bytes = self.codec.train_dict(
                samples, options=options, cancel=cancel
            )
            dict_id = dict_id_for_bytes(dict_bytes)
            self.backend.put_dict(dict_id, self.codec.dict_codec, dict_bytes, params_json, self.backend.now())
            result = TrainDictResult(
                dict_id=dict_id,
                dict_size=len(dict_bytes),
                samples_used=samples_used,
                sample_bytes=sample_bytes,
            )
            yield OpEvent(
                kind="done",
                op="train_dict",
                metrics={"dict_size": len(dict_bytes), "samples_used": samples_used},
            )
            return result

        return Operation(_run)

    def reencode(
        self,
        targets: Iterable[ContentRef],
        options: dict[str, Any] | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[ReencodeResult]:
        """Re-encode targets in place using the current profile (spec 7.5).

        ``options`` may override the target policy:
        - ``{"profile": name}`` re-encodes every target with that profile's policy,
        - ``{"codec": ..., "params": ..., "zstd_dict_id": ...}`` supplies an ad-hoc policy.
        The ``(blob_key, profile)`` key itself never changes.
        """

        def _target_policy() -> Profile | None:
            opts = options or {}
            unknown = set(opts) - _REENCODE_OPTION_KEYS
            if unknown:
                raise ValueError(f"unknown reencode options: {sorted(unknown)}")
            policy_keys = {"codec", "params", "zstd_dict_id"} & set(opts)
            if "profile" in opts:
                if policy_keys:
                    raise ValueError("reencode option 'profile' cannot be combined with codec/params/zstd_dict_id")
                return self._profile(str(opts["profile"]))
            if not policy_keys:
                return None
            codec = str(opts.get("codec", "zstd"))
            params_raw: Any = opts.get("params") or {}
            if not isinstance(params_raw, dict):
                raise TypeError("reencode option 'params' must be a dict")
            params = cast("dict[str, Any]", params_raw)
            zstd_dict_id = opts.get("zstd_dict_id")
            if zstd_dict_id is not None and not isinstance(zstd_dict_id, str):
                raise TypeError("reencode option 'zstd_dict_id' must be a string or None")
            return Profile(name="<reencode-override>", codec=codec, params=params, zstd_dict_id=zstd_dict_id)

        def _run() -> Generator[OpEvent, None, ReencodeResult]:
            check_cancel(cancel)
            policy = _target_policy()
            targets_seen = reencoded = skipped = 0
            bytes_in = bytes_out = 0
            yield OpEvent(kind="start", op="reencode")
            for ref in targets:
                check_cancel(cancel)
                targets_seen += 1
                row = self.backend.get_encoding(ref.blob_key, ref.profile)
                if row is None:
                    skipped += 1
                    continue
                raw = self.get_bytes(ref)
                prof = policy or self._profile(ref.profile)
                enc = self._encode(raw, prof)
                # In-place update of payload + encodings in the same transaction (spec 7.5).
                now = self.backend.now()
                self.backend.store_encoding_and_payload(
                    blob_key=ref.blob_key,
                    raw_len=len(raw),
                    created_at=now,
                    profile=ref.profile,
                    codec=enc.codec,
                    codec_params_json=enc.codec_params_json,
                    zstd_dict_id=enc.zstd_dict_id,
                    stored_len=enc.stored_len,
                    checksum=None,
                    shard_id=row["shard_id"],
                    updated_at=now,
                    payload=enc.data,
                )
                reencoded += 1
                bytes_in += len(raw)
                bytes_out += enc.stored_len
                yield OpEvent(
                    kind="item", op="reencode", metrics={"targets": targets_seen, "reencoded": reencoded}
                )
            result = ReencodeResult(
                targets=targets_seen,
                reencoded=reencoded,
                skipped=skipped,
                bytes_in=bytes_in,
                bytes_out=bytes_out,
            )
            yield OpEvent(kind="done", op="reencode", metrics={"reencoded": reencoded, "skipped": skipped})
            return result

        return Operation(_run)

    # -- maintenance (spec 10) -------------------------------------------------

    def verify(
        self,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[VerifyResult]:
        """Decode every encoding and compare against the ``ikb1`` identity (spec 10.1, decision D)."""

        def _run() -> Generator[OpEvent, None, VerifyResult]:
            checked = ok = missing = corrupt = 0
            yield OpEvent(kind="start", op="verify")
            for row in self.backend.iter_encodings(limit=limit):
                check_cancel(cancel)
                checked += 1
                ref = ContentRef(blob_key=str(row["blob_key"]), profile=str(row["profile"]))
                try:
                    data = self.get_bytes(ref)
                except MissingContent:
                    missing += 1
                except CorruptContent:
                    corrupt += 1
                else:
                    expected_len, expected_sha, expected_blake = self.identity.parse(ref.blob_key)
                    got_key, got_len, got_sha, got_blake = self.identity.key_bytes(data)
                    if (
                        got_key != ref.blob_key
                        or got_len != expected_len
                        or got_sha != expected_sha
                        or got_blake != expected_blake
                    ):
                        corrupt += 1
                    else:
                        ok += 1
                yield OpEvent(
                    kind="item",
                    op="verify",
                    metrics={"checked": checked, "ok": ok, "missing": missing, "corrupt": corrupt},
                )
            result = VerifyResult(checked=checked, ok=ok, missing=missing, corrupt=corrupt)
            yield OpEvent(kind="done", op="verify", metrics={"checked": checked, "ok": ok})
            return result

        return Operation(_run)

    def gc(self, live: Iterable[ContentRef], cancel: CancelToken | None = None) -> Operation[GcResult]:
        """Delete encodings/payloads not in ``live``, then orphan blobs and dicts (spec 10.2)."""

        def _run() -> Generator[OpEvent, None, GcResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="gc")
            dead = self.backend.list_dead_encodings(live)
            check_cancel(cancel)
            encodings_deleted, payload_rows_deleted = self.backend.delete_encoding_and_payload_group(dead)
            blobs_deleted = self.backend.delete_orphan_blobs()
            dicts_deleted = self.backend.delete_unreferenced_dicts()
            result = GcResult(
                encodings_deleted=encodings_deleted,
                payload_rows_deleted=payload_rows_deleted,
                blobs_deleted=blobs_deleted,
                dicts_deleted=dicts_deleted,
            )
            yield OpEvent(
                kind="done",
                op="gc",
                metrics={
                    "encodings_deleted": encodings_deleted,
                    "payload_rows_deleted": payload_rows_deleted,
                    "blobs_deleted": blobs_deleted,
                    "dicts_deleted": dicts_deleted,
                },
            )
            return result

        return Operation(_run)

    def compact(
        self,
        options: dict[str, Any] | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[CompactResult]:
        """VACUUM the index and (in sharded mode) selected/all shards (spec 10.3, decision H)."""

        def _run() -> Generator[OpEvent, None, CompactResult]:
            check_cancel(cancel)
            opts = options or {}
            unknown = set(opts) - _COMPACT_OPTION_KEYS
            if unknown:
                raise ValueError(f"unknown compact options: {sorted(unknown)}")
            targets: list[str] = []
            yield OpEvent(kind="start", op="compact")
            targets.append(self.backend.vacuum_index())
            if self.backend.mode == "sqlite_sharded":
                shard_ids = opts.get("shard_ids") or self.backend.list_shards()
                for shard_id in shard_ids:
                    check_cancel(cancel)
                    targets.append(self.backend.vacuum_shard(int(shard_id)))
            result = CompactResult(mode="vacuum", targets=targets)
            yield OpEvent(kind="done", op="compact", metrics={"targets": len(targets)})
            return result

        return Operation(_run)
