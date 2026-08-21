from __future__ import annotations

import io
import json
import tempfile
from collections.abc import Iterable

from .codec import CodecEngine, blob_key_ikb1_bytes, blob_key_ikb1_stream, dict_id_for_bytes, parse_blob_key_ikb1
from .types import (
    Busy,
    Cancelled,
    CompactResult,
    ContentRef,
    CorruptContent,
    GcResult,
    MissingContent,
    OpEvent,
    Operation,
    Profile,
    PutResult,
    ReencodeResult,
    TrainDictResult,
    VerifyResult,
)


def _cancel(cancel) -> None:
    if cancel and cancel():
        raise Cancelled("operation cancelled")


class BlobStore:
    def __init__(self, backend, codec: CodecEngine | None = None):
        self.backend = backend
        self.codec = codec or CodecEngine()

    def _profiles(self) -> dict[str, Profile]:
        raw = self.backend.config_get("profiles") or {}
        out: dict[str, Profile] = {}
        for name, cfg in raw.items():
            out[name] = Profile(name=name, codec=cfg["codec"], params=cfg.get("params") or {}, zstd_dict_id=cfg.get("zstd_dict_id"))
        return out

    def _profile(self, name: str) -> Profile:
        profiles = self._profiles()
        if name not in profiles:
            raise KeyError(f"profile not found: {name}")
        return profiles[name]

    def put_bytes(self, data: bytes, profile: str, cancel=None) -> Operation[PutResult]:
        op: Operation[PutResult]

        def _it():
            nonlocal op
            _cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="hash", message="computing ikb1")
            blob_key, raw_len, _, _ = blob_key_ikb1_bytes(data)
            self.backend.insert_blob_if_missing(blob_key, raw_len, self.backend.now())
            existing = self.backend.get_encoding(blob_key, profile)
            if existing:
                result = PutResult(
                    ref=ContentRef(blob_key=blob_key, profile=profile),
                    raw_len=int(raw_len),
                    stored_len=int(existing["stored_len"]),
                    codec=str(existing["codec"]),
                    zstd_dict_id=existing["zstd_dict_id"],
                )
                op._set_result(result)
                yield OpEvent(kind="done", op="put", message="dedupe hit")
                return

            yield OpEvent(kind="phase", op="put", phase="encode")
            prof = self._profile(profile)
            dict_bytes = None
            if prof.zstd_dict_id:
                dict_bytes = self.backend.get_dict(prof.zstd_dict_id)
                if dict_bytes is None:
                    raise MissingContent("required dict missing")
            enc = self.codec.encode(data, prof, dict_bytes)
            sid = None
            if self.backend.mode == "sqlite_sharded":
                sid = self.backend.choose_shard_for_write(enc.stored_len)
            self.backend.put_encoding_and_payload(
                blob_key=blob_key,
                profile=profile,
                codec=enc.codec,
                codec_params_json=enc.codec_params_json,
                zstd_dict_id=enc.zstd_dict_id,
                stored_len=enc.stored_len,
                checksum=None,
                shard_id=sid,
                updated_at=self.backend.now(),
                payload=enc.data,
            )
            result = PutResult(
                ref=ContentRef(blob_key=blob_key, profile=profile),
                raw_len=raw_len,
                stored_len=enc.stored_len,
                codec=enc.codec,
                zstd_dict_id=enc.zstd_dict_id,
            )
            op._set_result(result)
            yield OpEvent(kind="done", op="put", metrics={"bytes_in": raw_len, "bytes_out": enc.stored_len})

        op = Operation(_it)
        return op

    def put_stream(self, fp, profile: str, size_hint=None, cancel=None) -> Operation[PutResult]:
        op: Operation[PutResult]

        def _it():
            nonlocal op
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            with tempfile.SpooledTemporaryFile(max_size=2 * 1024 * 1024) as tmp:
                while True:
                    _cancel(cancel)
                    chunk = fp.read(1024 * 128)
                    if not chunk:
                        break
                    tmp.write(chunk)
                tmp.seek(0)
                blob_key, raw_len, _, _ = blob_key_ikb1_stream(tmp, size_hint=size_hint, cancel=cancel)
                self.backend.insert_blob_if_missing(blob_key, raw_len, self.backend.now())
                existing = self.backend.get_encoding(blob_key, profile)
                if existing:
                    result = PutResult(
                        ref=ContentRef(blob_key=blob_key, profile=profile),
                        raw_len=int(raw_len),
                        stored_len=int(existing["stored_len"]),
                        codec=str(existing["codec"]),
                        zstd_dict_id=existing["zstd_dict_id"],
                    )
                    op._set_result(result)
                    yield OpEvent(kind="done", op="put", message="dedupe hit")
                    return
                tmp.seek(0)
                raw = tmp.read()
            prof = self._profile(profile)
            dict_bytes = self.backend.get_dict(prof.zstd_dict_id) if prof.zstd_dict_id else None
            if prof.zstd_dict_id and dict_bytes is None:
                raise MissingContent("required dict missing")
            enc = self.codec.encode(raw, prof, dict_bytes)
            sid = None
            if self.backend.mode == "sqlite_sharded":
                sid = self.backend.choose_shard_for_write(enc.stored_len)
            self.backend.put_encoding_and_payload(
                blob_key=blob_key,
                profile=profile,
                codec=enc.codec,
                codec_params_json=enc.codec_params_json,
                zstd_dict_id=enc.zstd_dict_id,
                stored_len=enc.stored_len,
                checksum=None,
                shard_id=sid,
                updated_at=self.backend.now(),
                payload=enc.data,
            )
            result = PutResult(ContentRef(blob_key, profile), raw_len, enc.stored_len, enc.codec, enc.zstd_dict_id)
            op._set_result(result)
            yield OpEvent(kind="done", op="put")

        op = Operation(_it)
        return op

    def get_bytes(self, ref: ContentRef) -> bytes:
        row = self.backend.get_encoding(ref.blob_key, ref.profile)
        if not row:
            raise MissingContent("encoding not found")
        payload = self.backend.get_payload(ref.blob_key, ref.profile, row["shard_id"])
        if payload is None:
            raise MissingContent("payload missing")
        dict_bytes = None
        if row["zstd_dict_id"] is not None:
            dict_bytes = self.backend.get_dict(row["zstd_dict_id"])
            if dict_bytes is None:
                raise MissingContent("dictionary missing")
        try:
            raw = self.codec.decode(
                payload,
                codec=row["codec"],
                codec_params_json=row["codec_params_json"],
                dict_bytes=dict_bytes,
            )
        except MissingContent:
            raise
        except Exception as exc:
            raise CorruptContent(str(exc)) from exc

        verify_on_read = bool(self.backend.config_get("verify_on_read") or False)
        if verify_on_read:
            blob_key, _, _, _ = blob_key_ikb1_bytes(raw)
            if blob_key != ref.blob_key:
                raise CorruptContent("identity mismatch on read")
        return raw

    def open(self, ref: ContentRef):
        return io.BytesIO(self.get_bytes(ref))

    def has_blob(self, blob_key: str) -> bool:
        return self.backend.get_blob(blob_key) is not None

    def train_dict(self, samples: Iterable[bytes], options=None, cancel=None) -> Operation[TrainDictResult]:
        op: Operation[TrainDictResult]

        def _it():
            nonlocal op
            yield OpEvent(kind="start", op="train_dict")
            dict_bytes, params_json, samples_used, sample_bytes = self.codec.train_dict(samples, options=options, cancel=cancel)
            did = dict_id_for_bytes(dict_bytes)
            self.backend.put_dict(did, "zstd", dict_bytes, params_json, self.backend.now())
            result = TrainDictResult(dict_id=did, dict_size=len(dict_bytes), samples_used=samples_used, sample_bytes=sample_bytes)
            op._set_result(result)
            yield OpEvent(kind="done", op="train_dict")

        op = Operation(_it)
        return op

    def verify(self, limit=None, cancel=None) -> Operation[VerifyResult]:
        op: Operation[VerifyResult]

        def _it():
            nonlocal op
            checked = ok = missing = corrupt = 0
            yield OpEvent(kind="start", op="verify")
            for row in self.backend.iter_encodings(limit=limit):
                _cancel(cancel)
                checked += 1
                ref = ContentRef(blob_key=row["blob_key"], profile=row["profile"])
                try:
                    data = self.get_bytes(ref)
                    exp_len, exp_sha, exp_b2 = parse_blob_key_ikb1(ref.blob_key)
                    got_key, got_len, got_sha, got_b2 = blob_key_ikb1_bytes(data)
                    if got_len != exp_len or got_sha != exp_sha or got_b2 != exp_b2 or got_key != ref.blob_key:
                        corrupt += 1
                    else:
                        ok += 1
                except MissingContent:
                    missing += 1
                except CorruptContent:
                    corrupt += 1
                yield OpEvent(kind="item", op="verify", metrics={"checked": checked, "ok": ok, "missing": missing, "corrupt": corrupt})
            result = VerifyResult(checked=checked, ok=ok, missing=missing, corrupt=corrupt)
            op._set_result(result)
            yield OpEvent(kind="done", op="verify")

        op = Operation(_it)
        return op

    def reencode(self, targets: Iterable[ContentRef], options=None, cancel=None) -> Operation[ReencodeResult]:
        del options
        op: Operation[ReencodeResult]

        def _it():
            nonlocal op
            t = r = s = 0
            bytes_in = bytes_out = 0
            yield OpEvent(kind="start", op="reencode")
            for ref in targets:
                _cancel(cancel)
                t += 1
                row = self.backend.get_encoding(ref.blob_key, ref.profile)
                if not row:
                    s += 1
                    continue
                raw = self.get_bytes(ref)
                prof = self._profile(ref.profile)
                dict_bytes = self.backend.get_dict(prof.zstd_dict_id) if prof.zstd_dict_id else None
                if prof.zstd_dict_id and dict_bytes is None:
                    raise MissingContent("required dict missing for reencode")
                enc = self.codec.encode(raw, prof, dict_bytes)
                sid = row["shard_id"]
                self.backend.put_encoding_and_payload(
                    blob_key=ref.blob_key,
                    profile=ref.profile,
                    codec=enc.codec,
                    codec_params_json=enc.codec_params_json,
                    zstd_dict_id=enc.zstd_dict_id,
                    stored_len=enc.stored_len,
                    checksum=None,
                    shard_id=sid,
                    updated_at=self.backend.now(),
                    payload=enc.data,
                )
                r += 1
                bytes_in += len(raw)
                bytes_out += enc.stored_len
                yield OpEvent(kind="item", op="reencode", metrics={"reencoded": r, "targets": t})
            result = ReencodeResult(targets=t, reencoded=r, skipped=s, bytes_in=bytes_in, bytes_out=bytes_out)
            op._set_result(result)
            yield OpEvent(kind="done", op="reencode")

        op = Operation(_it)
        return op

    def gc(self, live: Iterable[ContentRef], cancel=None) -> Operation[GcResult]:
        op: Operation[GcResult]

        def _it():
            nonlocal op
            _cancel(cancel)
            yield OpEvent(kind="start", op="gc")
            dead = self.backend.list_dead_encodings(live)
            _cancel(cancel)
            enc_deleted, payload_deleted = self.backend.delete_encoding_and_payload_group(dead)
            blobs_deleted = self.backend.delete_orphan_blobs()
            dicts_deleted = self.backend.delete_unreferenced_dicts()
            result = GcResult(
                encodings_deleted=enc_deleted,
                payload_rows_deleted=payload_deleted,
                blobs_deleted=blobs_deleted,
                dicts_deleted=dicts_deleted,
            )
            op._set_result(result)
            yield OpEvent(kind="done", op="gc")

        op = Operation(_it)
        return op

    def compact(self, options=None, cancel=None) -> Operation[CompactResult]:
        options = options or {}
        op: Operation[CompactResult]

        def _it():
            nonlocal op
            _cancel(cancel)
            targets: list[str] = []
            yield OpEvent(kind="start", op="compact")
            targets.append(self.backend.vacuum_index())
            if self.backend.mode == "sqlite_sharded":
                shard_ids = options.get("shard_ids") or self.backend.list_shards()
                for sid in shard_ids:
                    _cancel(cancel)
                    targets.append(self.backend.vacuum_shard(int(sid)))
            result = CompactResult(mode="vacuum", targets=targets)
            op._set_result(result)
            yield OpEvent(kind="done", op="compact", metrics={"targets": len(targets)})

        op = Operation(_it)
        return op
