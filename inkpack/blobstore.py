"""BlobStore: the content-addressable store (spec 4.3, 6, 9, 10).

Depends only on an injected :class:`SqliteBackend`, a :class:`CodecEngine` and
an :class:`Identity` policy. All long operations return an :class:`Operation`
that yields :class:`OpEvent` objects; the generator's ``return`` value becomes
``Operation.result``.

Connection policy: one operation-scoped session (review §13 phase 1) per
public call, so reads never pay one connection per statement and GC can hold
TEMP tables.
"""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, cast

from .codec import (
    CHUNK_SIZE,
    IKB1,
    CodecEngine,
    Encoded,
    Identity,
    dict_id_for_bytes,
    validate_train_dict_options,
)
from .sqlite import Session, SqliteBackend
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
    UnknownProfile,
    VerifyResult,
    check_cancel,
    profiles_from_config,
    validate_profile_entry,
)

_SPOOL_MAX_SIZE = 2 * 1024 * 1024
PROGRESS_INTERVAL = 8 * 1024 * 1024
_PAGE_SIZE = 500

_REENCODE_OPTION_KEYS = frozenset({"profile", "codec", "params", "zstd_dict_id"})
_COMPACT_OPTION_KEYS = frozenset({"shard_ids"})


@dataclass(frozen=True)
class PreparedPut:
    """Outcome of hashing + dedupe-checking one payload, before persisting.

    ``enc is None`` signals a healthy dedupe hit: nothing may be rewritten
    (decision E). Otherwise ``enc`` holds the bytes to persist (either a new
    encoding or a repair of a missing payload under the *stored* policy).
    """

    blob_key: str
    raw_len: int
    profile: str
    enc: Encoded | None
    shard_id: int | None


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

    @staticmethod
    def _profile_from(snapshot: dict[str, Profile], name: str) -> Profile:
        try:
            return snapshot[name]
        except KeyError:
            raise UnknownProfile(f"profile not found: {name}") from None

    def _profiles_snapshot(self) -> dict[str, Profile]:
        """Load the profile map once per public call / operation (review §18)."""
        return profiles_from_config(self.backend.config_get("profiles"))

    def _load_dict(self, profile: Profile, s: Session) -> bytes | None:
        """Load the profile's dictionary on the operation session; codec-aware
        (review §4, P2-1).

        A dict is only relevant for ``zstd``, so ``none`` never probes the
        dict table even if a stale id is set. Uses the session's dict cache so
        one operation never re-reads the same dictionary.
        """
        if profile.codec != "zstd" or profile.zstd_dict_id is None:
            return None
        dict_bytes = s.dict_bytes(profile.zstd_dict_id)
        if dict_bytes is None:
            raise MissingContent(
                f"dictionary {profile.zstd_dict_id!r} referenced by profile {profile.name!r} is missing"
            )
        return dict_bytes

    def check_blob_limit(self, stored_len: int, conn: sqlite3.Connection | None = None) -> None:
        """Refuse payloads that cannot fit in one SQLite BLOB (review §14).

        ``conn`` is an open session connection used to probe the limit without
        an extra connection.
        """
        limit = self.backend.blob_length_limit(conn=conn)
        if stored_len > limit:
            raise ValueError(
                f"encoded payload of {stored_len} bytes exceeds the SQLite blob length limit "
                f"({limit} bytes); split the chapter into smaller parts"
            )

    # -- prepare / persist ---------------------------------------------------

    def _prepare(
        self,
        s: Session,
        raw: bytes,
        profile: str,
        blob_key: str | None = None,
        key_tuple: tuple[str, int, str, str] | None = None,
    ) -> PreparedPut:
        """Hash + dedupe-check + encode WITHOUT any filesystem side effects.

        Shard selection/creation never happens here (review P0-PREP-1): new
        encodings carry ``shard_id=None`` and the write path resolves the
        shard inside its own transaction after the blob-limit check.
        """
        # Profile map snapshot, read on the operation's connection (review §18).
        profiles = profiles_from_config(s.config("profiles"))
        if key_tuple is not None:
            # Internal streaming path (review P2-HASH-1): the identity was
            # already computed while hashing the stream; do not re-hash.
            blob_key, raw_len, _, _ = key_tuple
        elif blob_key is None:
            blob_key, raw_len, _, _ = self.identity.key_bytes(raw)
        else:
            raw_len = len(raw)
            # P0-5: a caller-supplied blob_key must be *bound* to the content —
            # recompute the full identity and require equality (a length-only
            # check would let forged keys with correct lengths be persisted).
            computed, _, _, _ = self.identity.key_bytes(raw)
            if computed != blob_key:
                raise CorruptContent("blob_key does not match content (identity mismatch)")
        # Decision J: the key is the identity; a broken identity implementation
        # must fail before any SQL is written.
        parsed_len, _, _ = self.identity.parse(blob_key)
        if parsed_len != len(raw):
            raise CorruptContent(
                f"identity length mismatch: blob_key declares {parsed_len} bytes "
                f"but {len(raw)} were supplied"
            )
        row = s.query_one(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
        )
        if row is not None:
            if s.payload_exists(blob_key, profile, row["shard_id"]):
                # Healthy dedupe hit (decision E): report the stored row as-is.
                # "put succeeded" must imply "content is readable": a required
                # dictionary that vanished is a typed error, not a silent hit
                # (review §4.2, P1-5).
                if row["zstd_dict_id"] is not None and s.dict_bytes(str(row["zstd_dict_id"])) is None:
                    raise MissingContent(
                        f"dictionary {row['zstd_dict_id']!r} missing for {(blob_key, profile)}"
                    )
                return PreparedPut(blob_key, raw_len, profile, enc=None, shard_id=row["shard_id"])
            # Repair: payload vanished but the encoding row survives. Re-encode
            # with the *stored* policy (never the current profile) so a put
            # does not silently migrate encoding policy. If the referenced
            # shard is gone/unusable, the persist path REHOMES (locked
            # semantics S2) — never attaches a missing shard.
            try:
                stored_params = json.loads(str(row["codec_params_json"]))
            except json.JSONDecodeError as exc:
                raise CorruptContent(f"stored codec_params_json is invalid for {(blob_key, profile)}") from exc
            stored_profile = Profile(
                name=profile,
                codec=str(row["codec"]),
                params=cast("dict[str, Any]", stored_params) if isinstance(stored_params, dict) else {},
                zstd_dict_id=row["zstd_dict_id"],
            )
            enc = self.codec.encode(raw, stored_profile, self._load_dict(stored_profile, s))
            return PreparedPut(blob_key, raw_len, profile, enc=enc, shard_id=row["shard_id"])
        prof = self._profile_from(profiles, profile)
        enc = self.codec.encode(raw, prof, self._load_dict(prof, s))
        return PreparedPut(blob_key, raw_len, profile, enc=enc, shard_id=None)

    def prepare_bytes(
        self,
        raw: bytes,
        profile: str,
        *,
        blob_key: str | None = None,
        key_tuple: tuple[str, int, str, str] | None = None,
        session: Session | None = None,
    ) -> PreparedPut:
        """Hash + dedupe-check + encode without writing (used by put paths and
        the Repository's atomic chapter upsert).

        ``key_tuple`` is the internal streaming path's precomputed identity
        (avoids re-hashing the spooled bytes, review P2-HASH-1).
        """
        if session is not None:
            return self._prepare(session, raw, profile, blob_key, key_tuple)
        with self.backend.session() as s:
            return self._prepare(s, raw, profile, blob_key, key_tuple)

    def _persist(self, s: Session, prepared: PreparedPut) -> PutResult:
        """Persist a prepared put on the session's connection (one write txn)."""
        if prepared.enc is None:
            row = s.query_one(
                "SELECT codec, stored_len, zstd_dict_id FROM encodings "
                "WHERE blob_key=? AND profile=?",
                (prepared.blob_key, prepared.profile),
            )
            if row is None:
                raise MissingContent(
                    f"encoding for {(prepared.blob_key, prepared.profile)} vanished between prepare and persist"
                )
            return PutResult(
                ref=ContentRef(prepared.blob_key, prepared.profile),
                raw_len=prepared.raw_len,
                stored_len=int(row["stored_len"]),
                codec=str(row["codec"]),
                zstd_dict_id=row["zstd_dict_id"],
            )
        self.check_blob_limit(prepared.enc.stored_len, s.conn)
        # Shard resolution happens here, in the write path, AFTER the
        # blob-limit check and never during prepare (review P0-PREP-1).
        # Repair rehoming (locked semantics S2): when the encoding's referenced
        # shard is missing or its locator is NULL, pick a writable shard
        # explicitly and update encodings.shard_id in the same transaction.
        shard_id = prepared.shard_id
        if self.backend.mode == "sqlite_sharded" and (
            shard_id is None or not self.backend.shard_path(shard_id).exists()
        ):
            # Rehome (locked semantics S2): explicitly select AND ensure the
            # writable shard — never leave it to ATTACH to create.
            shard_id = self.backend.choose_shard_for_write(prepared.enc.stored_len)
            self.backend.ensure_shard_exists(shard_id)
        now = self.backend.now()
        with self.backend.txn_on(
            s.conn,
            write=True,
            attach_shard_id=shard_id if self.backend.mode == "sqlite_sharded" else None,
        ) as conn:
            self.backend.store_encoding_and_payload_on(
                conn,
                blob_key=prepared.blob_key,
                raw_len=prepared.raw_len,
                created_at=now,
                profile=prepared.profile,
                codec=prepared.enc.codec,
                codec_params_json=prepared.enc.codec_params_json,
                zstd_dict_id=prepared.enc.zstd_dict_id,
                stored_len=prepared.enc.stored_len,
                checksum=None,
                shard_id=shard_id,
                updated_at=now,
                payload=prepared.enc.data,
            )
        return PutResult(
            ref=ContentRef(prepared.blob_key, prepared.profile),
            raw_len=prepared.raw_len,
            stored_len=prepared.enc.stored_len,
            codec=prepared.enc.codec,
            zstd_dict_id=prepared.enc.zstd_dict_id,
        )

    def hash_stream_events(
        self,
        fp: BinaryIO,
        cancel: CancelToken | None = None,
    ) -> Generator[OpEvent, None, tuple[tuple[str, int, str, str], bytes]]:
        """Single-pass spool + hash, yielding live progress events (review
        P2-3): the caller sees ``bytes_in`` grow *while* the source stream is
        being consumed, not replayed after hashing completes.

        Returns ``((blob_key, raw_len, sha, blake), raw)`` via the generator's
        return value; the digest tuple is threaded into prepare so the spooled
        bytes are never re-hashed (review P2-HASH-1).
        """
        hasher = self.identity.hasher()
        with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MAX_SIZE) as spool:
            total = 0
            last_mark = 0
            while True:
                check_cancel(cancel)
                chunk = fp.read(CHUNK_SIZE)
                if not chunk:
                    break
                spool.write(chunk)
                hasher.update(chunk)
                total += len(chunk)
                if total - last_mark >= PROGRESS_INTERVAL:
                    yield OpEvent(kind="progress", op="put", metrics={"bytes_in": total})
                    last_mark = total
            digest = hasher.digest()
            spool.seek(0)
            raw = spool.read()
        return digest, raw

    # -- core API (spec 4.3) --------------------------------------------------

    def put_bytes(self, data: bytes, profile: str, cancel: CancelToken | None = None) -> Operation[PutResult]:
        def _run() -> Generator[OpEvent, None, PutResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="hash")
            with self.backend.session() as s:
                prepared = self.prepare_bytes(data, profile, session=s)
                result = self._persist(s, prepared)
            yield OpEvent(
                kind="done",
                op="put",
                metrics={"bytes_in": result.raw_len, "bytes_out": result.stored_len},
            )
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
            digest, raw = yield from self.hash_stream_events(fp, cancel)
            with self.backend.session() as s:
                prepared = self.prepare_bytes(raw, profile, key_tuple=digest, session=s)
                result = self._persist(s, prepared)
            yield OpEvent(
                kind="done",
                op="put",
                metrics={"bytes_in": result.raw_len, "bytes_out": result.stored_len},
            )
            return result

        return Operation(_run)

    def _decode_row(self, s: Session, row: sqlite3.Row) -> bytes:
        """Decode one encodings row using *stored* metadata only (spec 6.2).

        The payload + dict are read on the same session (no extra connections,
        dict bytes cached per operation, review §13/§16). ``verify()`` uses
        this directly so classification never depends on ``get_bytes``.
        """
        blob_key = str(row["blob_key"])
        profile = str(row["profile"])
        ref = ContentRef(blob_key=blob_key, profile=profile)
        payload = s.payload(blob_key, profile, row["shard_id"])
        if payload is None:
            raise MissingContent(f"payload missing for {ref}")
        dict_bytes = None
        if row["zstd_dict_id"] is not None:
            dict_bytes = s.dict_bytes(str(row["zstd_dict_id"]))
            if dict_bytes is None:
                raise MissingContent(f"dictionary {row['zstd_dict_id']!r} missing for {ref}")
        # The blob_key declares the canonical length: use it to bound zstd
        # decompression output so a corrupt payload cannot balloon memory
        # (review P2-6). An unparsable key is corruption, not a raw ValueError.
        try:
            expected_len = self.identity.parse(blob_key)[0]
        except ValueError as exc:
            raise CorruptContent(f"invalid blob_key {blob_key!r}") from exc
        try:
            return self.codec.decode(
                payload,
                codec=str(row["codec"]),
                codec_params_json=str(row["codec_params_json"]),
                dict_bytes=dict_bytes,
                max_output_size=expected_len,
            )
        except (MissingContent, CorruptContent):
            raise
        except Exception as exc:
            raise CorruptContent(f"failed to decode {ref}: {exc}") from exc

    def get_bytes(self, ref: ContentRef) -> bytes:
        """Decode using *stored* encoding metadata only (spec 6.2, 6.4)."""
        with self.backend.session() as s:
            row = s.query_one(
                "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                (ref.blob_key, ref.profile),
            )
            if row is None:
                raise MissingContent(f"encoding not found for {ref}")
            raw = self._decode_row(s, row)
            if bool(s.config("verify_on_read") or False):
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
        # Eager option validation (review P2-OPT-1): bad options fail at call
        # time, before an Operation exists.
        validate_train_dict_options(options)

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
        - ``{"codec": ..., "params": ..., "zstd_dict_id": ...}`` supplies an ad-hoc policy,
          validated with the same rules as ``set_profile`` (review §4).
        The policy (and the profile map) is frozen at call time, so
        ``set_profile`` mid-run cannot change remaining targets (review §18).
        """

        opts = options or {}
        unknown = set(opts) - _REENCODE_OPTION_KEYS
        if unknown:
            raise ValueError(f"unknown reencode options: {sorted(unknown)}")
        policy_keys = {"codec", "params", "zstd_dict_id"} & set(opts)
        if "codec" in opts and not isinstance(opts["codec"], str):
            raise TypeError("reencode option 'codec' must be a string")
        if "params" in opts and not isinstance(opts["params"], dict):
            raise TypeError("reencode option 'params' must be a dict")
        if "zstd_dict_id" in opts and opts["zstd_dict_id"] is not None and not isinstance(opts["zstd_dict_id"], str):
            raise TypeError("reencode option 'zstd_dict_id' must be a string or None")

        def _target_policy(profiles: dict[str, Profile]) -> Profile | None:
            if "profile" in opts:
                return self._profile_from(profiles, str(opts["profile"]))
            if not policy_keys:
                return None
            profile = Profile(
                name="<reencode-override>",
                codec=str(opts.get("codec", "zstd")),
                params=cast("dict[str, Any]", opts.get("params") or {}),
                zstd_dict_id=opts.get("zstd_dict_id"),
            )
            validate_profile_entry("<reencode-override>", profile)
            return profile

        # Eager conflict check (review P2-OPT-1); the profile lookup itself
        # happens on the operation's session connection so call-time
        # validation never touches the DB.
        if "profile" in opts and policy_keys:
            raise ValueError("reencode option 'profile' cannot be combined with codec/params/zstd_dict_id")

        def _run() -> Generator[OpEvent, None, ReencodeResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="reencode")
            with self.backend.session() as s:
                profiles = profiles_from_config(s.config("profiles"))
                policy = _target_policy(profiles)
                targets_seen = reencoded = skipped = 0
                bytes_in = bytes_out = 0
                for ref in targets:
                    check_cancel(cancel)
                    targets_seen += 1
                    row = s.query_one(
                        "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                        (ref.blob_key, ref.profile),
                    )
                    if row is None:
                        skipped += 1
                        continue
                    try:
                        raw = self._decode_row(s, row)
                        prof = policy or self._profile_from(profiles, ref.profile)
                        enc = self.codec.encode(raw, prof, self._load_dict(prof, s))
                        self.check_blob_limit(enc.stored_len, s.conn)
                    except (MissingContent, CorruptContent, UnknownProfile, ValueError) as exc:
                        # Per-target problems (missing payload/dict, corrupt
                        # payload, deleted profile, payload over the blob
                        # limit) must not kill a bulk run (review P1-3,
                        # P1-REENC-1): skip and keep going.
                        skipped += 1
                        yield OpEvent(
                            kind="log",
                            op="reencode",
                            message=f"skipped {ref}: {exc}",
                            metrics={"targets": targets_seen, "skipped": skipped},
                        )
                        continue
                    # In-place update of payload + encodings in one transaction (spec 7.5).
                    now = self.backend.now()
                    with self.backend.txn_on(
                        s.conn,
                        write=True,
                        attach_shard_id=row["shard_id"] if self.backend.mode == "sqlite_sharded" else None,
                    ) as conn:
                        self.backend.store_encoding_and_payload_on(
                            conn,
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

    def _iter_encodings(self, s: Session, limit: int | None) -> Iterator[sqlite3.Row]:
        """Keyset-paged iteration over encodings (review §15): no fetchall."""
        last_key: str | None = None
        last_profile: str | None = None
        remaining = limit
        while True:
            if last_key is None:
                rows = s.query_all(
                    "SELECT * FROM encodings ORDER BY blob_key, profile LIMIT ?", (_PAGE_SIZE,)
                )
            else:
                rows = s.query_all(
                    "SELECT * FROM encodings WHERE (blob_key, profile) > (?, ?) "
                    "ORDER BY blob_key, profile LIMIT ?",
                    (last_key, last_profile, _PAGE_SIZE),
                )
            if not rows:
                return
            for row in rows:
                if remaining is not None:
                    if remaining <= 0:
                        return
                    remaining -= 1
                yield row
                last_key = str(row["blob_key"])
                last_profile = str(row["profile"])

    def verify(
        self,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[VerifyResult]:
        """Decode every encoding and compare against the identity (spec 10.1).

        Every row is classified as exactly one of ok / missing / corrupt
        (decision D): an unparsable ``blob_key`` or undecodable payload counts
        as corrupt and never aborts the run (review §3). Unexpected errors
        still propagate.
        """

        def _run() -> Generator[OpEvent, None, VerifyResult]:
            checked = ok = missing = corrupt = 0
            yield OpEvent(kind="start", op="verify")
            with self.backend.session() as s:
                for row in self._iter_encodings(s, limit):
                    check_cancel(cancel)
                    checked += 1
                    try:
                        raw = self._decode_row(s, row)
                    except MissingContent:
                        missing += 1
                    except CorruptContent:
                        corrupt += 1
                    else:
                        try:
                            expected_len, expected_sha, expected_blake = self.identity.parse(
                                str(row["blob_key"])
                            )
                            got_key, got_len, got_sha, got_blake = self.identity.key_bytes(raw)
                            if (
                                got_key != str(row["blob_key"])
                                or got_len != expected_len
                                or got_sha != expected_sha
                                or got_blake != expected_blake
                            ):
                                corrupt += 1
                            else:
                                ok += 1
                        except ValueError:
                            corrupt += 1
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
        """Delete encodings/payloads not in ``live``, then orphan blobs and dicts."""

        def _run() -> Generator[OpEvent, None, GcResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="gc")
            encodings_deleted, payload_rows_deleted = self.backend.gc(live, cancel)
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
        """VACUUM the index and (in sharded mode) selected/all shards (spec 10.3, decision H).

        Options are validated eagerly at call time (review P2-OPT-1):
        ``shard_ids`` must be a ``list[int] | tuple[int, ...]`` of non-bool
        ints (locked semantics S5), is rejected in ``sqlite_single`` mode, and
        unknown shard ids raise ``ValueError`` — compact never creates a shard
        file (review P0-2).
        """
        opts = options or {}
        unknown = set(opts) - _COMPACT_OPTION_KEYS
        if unknown:
            raise ValueError(f"unknown compact options: {sorted(unknown)}")
        requested: list[int] | None = None
        if "shard_ids" in opts:
            if self.backend.mode != "sqlite_sharded":
                raise ValueError("compact option 'shard_ids' is only valid for sqlite_sharded")
            shard_ids_value: Any = opts["shard_ids"]
            if not isinstance(shard_ids_value, (list, tuple)):
                raise TypeError(
                    "compact option 'shard_ids' must be a list or tuple of ints, "
                    f"got {type(shard_ids_value).__name__}"
                )
            requested = []
            for raw_sid in cast("tuple[Any, ...] | list[Any]", shard_ids_value):  # type: ignore[redundant-cast]  # pyright: ignore[reportUnknownVariableType]
                if isinstance(raw_sid, bool) or not isinstance(raw_sid, int):
                    raise TypeError(f"compact option 'shard_ids' must contain ints, got {raw_sid!r}")
                requested.append(raw_sid)
            existing = set(self.backend.list_shards())
            unknown_shards = [sid for sid in requested if sid not in existing]
            if unknown_shards:
                raise ValueError(f"unknown shard ids: {unknown_shards}")

        def _run() -> Generator[OpEvent, None, CompactResult]:
            check_cancel(cancel)
            targets: list[str] = []
            yield OpEvent(kind="start", op="compact")
            targets.append(self.backend.vacuum_index())
            if self.backend.mode == "sqlite_sharded":
                shard_ids = requested if requested is not None else sorted(self.backend.list_shards())
                for shard_id in shard_ids:
                    check_cancel(cancel)
                    targets.append(self.backend.vacuum_shard(shard_id))
            result = CompactResult(mode="vacuum", targets=targets)
            yield OpEvent(kind="done", op="compact", metrics={"targets": len(targets)})
            return result

        return Operation(_run)
