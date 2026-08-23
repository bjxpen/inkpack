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
from dataclasses import asdict, dataclass
from typing import IO, Any, BinaryIO, cast

from .codec import (
    CHUNK_SIZE,
    IKB1,
    CodecEngine,
    Encoded,
    Identity,
    dict_id_for_bytes,
    validate_train_dict_options,
)
from .failpoints import failpoint
from .sqlite import Session, SqliteBackend, _require_dict_row  # pyright: ignore[reportPrivateUsage]
from .types import (
    Busy,
    Cancelled,
    CancelToken,
    CompactResult,
    ContentRef,
    CorruptContent,
    GcResult,
    InkpackError,
    MissingContent,
    Operation,
    OpEvent,
    Profile,
    PutResult,
    ReencodeResult,
    Retryable,
    TrainDictResult,
    UnknownProfile,
    VerifyResult,
    check_cancel,
    profiles_from_config,
    validate_profile_entry,
)

SPOOL_MAX_SIZE = 2 * 1024 * 1024
PROGRESS_INTERVAL = 8 * 1024 * 1024
_PAGE_SIZE = 500
_VERIFY_ITEM_INTERVAL = 32


def _require_binary_stream(fp: BinaryIO, op: str) -> None:
    """A8: fail fast on a text stream with an actionable message.

    ``read(0)`` is legal on any stream — it consumes nothing and moves no
    position — yet reveals the mode (``str`` vs ``bytes``).
    """
    if isinstance(fp.read(0), str):
        raise TypeError(
            f"{op} requires a binary stream (open the source with 'rb'), got a text stream"
        )


class _RepairRequired(InkpackError):
    """Internal control-flow signal (E5): a dedupe hit found a missing payload
    and raw bytes are available; the caller runs the repair path. Subclasses
    :class:`InkpackError` so it satisfies the typed hierarchy if it ever
    escapes; it is never part of the public contract."""

    def __init__(self, row: sqlite3.Row) -> None:
        super().__init__("payload missing; repair required")
        self.row = row

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

    @staticmethod
    def _check_stored_codec_dict(row: sqlite3.Row, ref: ContentRef) -> None:
        """A4: stored metadata must be decodable — the exact rule decode enforces.

        ``zstd_dict_id`` requires codec ``zstd``. Deliberately does NOT
        validate ``codec_params_json``: decode ignores it (the frame is the
        source of truth), so checking it would exceed decode's real
        enforcement surface.
        """
        if str(row["codec"]) != "zstd" and row["zstd_dict_id"] is not None:
            raise CorruptContent(f"codec {row['codec']!r} cannot use zstd_dict_id for {ref}")

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

    @staticmethod
    def profile_from_encoding_row(row: sqlite3.Row) -> Profile:
        """Build the write policy encoded in a stored encodings row (D7/M8).

        Stored metadata that is not a usable policy is ``CorruptContent``:
        unparsable JSON, non-object params, or a codec the engine rejects.
        """
        profile = str(row["profile"])
        try:
            params = json.loads(str(row["codec_params_json"]))
        except json.JSONDecodeError as exc:
            raise CorruptContent(
                f"stored codec_params_json is invalid for {(row['blob_key'], profile)}: {exc}"
            ) from exc
        if not isinstance(params, dict):
            raise CorruptContent(
                f"stored codec_params_json is not an object for {(row['blob_key'], profile)}"
            )
        return Profile(
            name=profile,
            codec=str(row["codec"]),
            params=cast("dict[str, Any]", params),
            zstd_dict_id=row["zstd_dict_id"],
        )

    def _prepare(
        self,
        s: Session,
        raw: bytes,
        profile: str,
        blob_key: str | None = None,
        *,
        _verified_key: bool = False,
    ) -> PreparedPut:
        """Hash + dedupe-check + encode WITHOUT any filesystem side effects.

        Shard selection/creation never happens here (review P0-PREP-1): new
        encodings carry ``shard_id=None`` and the write path resolves the
        shard inside its own transaction after the blob-limit check.
        """
        # Profile map snapshot, read on the operation's connection (review §18).
        profiles = profiles_from_config(s.config("profiles"))
        if blob_key is None:
            blob_key, raw_len, _, _ = self.identity.key_bytes(raw)
        else:
            raw_len = len(raw)
            if not _verified_key:
                # P0-5: a caller-supplied blob_key must be *bound* to the
                # content — recompute the full identity and require equality.
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
                # H1: after the payload probe, before the hit is committed
                # (B3: the window a racing gc can exploit).
                failpoint("prepare.post_hit_probe")
                # Healthy dedupe hit (decision E): report the stored row as-is.
                # "put succeeded" must imply "content is readable" (N7): the
                # hit enforces exactly what DECODE enforces — the
                # codec/dict-id pairing (A4), the dict row's presence,
                # stored_len, and the S3 bound — and NOTHING more. It does not
                # parse ``codec_params_json``: decode ignores it (L27 — the
                # frame is the source of truth), so a hit with non-object
                # params is genuinely readable and must succeed. (Repair is
                # the opposite: it must ENCODE, so it does reject unusable
                # params via profile_from_encoding_row — M8.)
                self._check_stored_codec_dict(row, ContentRef(blob_key, profile))
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
            stored_profile = self.profile_from_encoding_row(row)
            try:
                enc = self.codec.encode(
                    raw,
                    stored_profile,
                    self._load_dict(stored_profile, s),
                    dict_cache_key=stored_profile.zstd_dict_id,
                )
            except ValueError as exc:
                # Stored metadata that is not a usable policy is corruption
                # (D7), not a caller error.
                raise CorruptContent(f"stored encoding policy unusable: {exc}") from exc
            return PreparedPut(blob_key, raw_len, profile, enc=enc, shard_id=row["shard_id"])
        prof = self._profile_from(profiles, profile)
        enc = self.codec.encode(
            raw, prof, self._load_dict(prof, s), dict_cache_key=prof.zstd_dict_id
        )
        return PreparedPut(blob_key, raw_len, profile, enc=enc, shard_id=None)

    def prepare_bytes(
        self,
        raw: bytes,
        profile: str,
        *,
        blob_key: str | None = None,
        session: Session | None = None,
    ) -> PreparedPut:
        """Hash + dedupe-check + encode without writing (used by put paths and
        the Repository's atomic chapter upsert)."""
        if session is not None:
            return self._prepare(session, raw, profile, blob_key)
        with self.backend.session() as s:
            return self._prepare(s, raw, profile, blob_key)

    def persist_prepared(
        self,
        conn: sqlite3.Connection,
        s: Session,
        prepared: PreparedPut,
        raw: bytes | None,
        now: str,
        *,
        alias: str = "p",
    ) -> PutResult:
        """Commit one prepared put inside an OPEN write transaction (D3).

        - ``enc is not None``: write the payload + encoding triple.
        - ``enc is None`` (dedupe hit): re-probe payload schema (A3), stored
          metadata (A4) and payload + required dict in this transaction; a
          healthy hit returns the stored row without rewriting; a vanished
          encoding raises :class:`Retryable`; a missing payload raises
          :class:`_RepairRequired` when ``raw`` is available (the caller
          repairs+rehomes), otherwise :class:`MissingContent`.

        The connection must already have the correct shard ATTACHed as
        ``alias`` (``p`` for whitebox txns, ``p<id>`` for session attaches).
        """
        ref = ContentRef(prepared.blob_key, prepared.profile)
        if prepared.enc is not None:
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
                shard_id=prepared.shard_id,
                updated_at=now,
                payload=prepared.enc.data,
                alias=alias,
            )
            return PutResult(
                ref=ref,
                raw_len=prepared.raw_len,
                stored_len=prepared.enc.stored_len,
                codec=prepared.enc.codec,
                zstd_dict_id=prepared.enc.zstd_dict_id,
            )
        row = conn.execute(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
            (prepared.blob_key, prepared.profile),
        ).fetchone()
        if row is None:
            raise Retryable(
                f"encoding for {(prepared.blob_key, prepared.profile)} vanished between prepare and persist; retry"
            )
        # A4: a hit must only succeed when the stored metadata is decodable —
        # the same rule the read path enforces (N7 symmetry).
        self._check_stored_codec_dict(row, ref)
        # N7 (r4-P1.1): probe the dicts table ON THIS CONNECTION (the
        # write-txn connection), not the session cache — a concurrent gc can
        # delete the dict after prepare filled the cache. Probing in the same
        # writer lock closes the window.
        _require_dict_row(
            conn, row["zstd_dict_id"], what=f"{(prepared.blob_key, prepared.profile)}"
        )
        if self.backend.mode == "sqlite_sharded":
            if prepared.shard_id is None:
                # A NULL locator cannot be probed: the payload is missing.
                payload_row = None
            else:
                if not s.payload_schema_ok(conn, int(prepared.shard_id), alias=alias):
                    raise CorruptContent(
                        f"payload schema missing in shard {prepared.shard_id} for {ref}"
                    )
                payload_row = conn.execute(
                    f"SELECT 1 FROM {alias}.payload WHERE blob_key=? AND profile=?",
                    (prepared.blob_key, prepared.profile),
                ).fetchone()
        else:
            if not s.payload_schema_ok(conn, None):
                raise CorruptContent(f"payload schema missing for {ref}")
            payload_row = conn.execute(
                "SELECT 1 FROM payload WHERE blob_key=? AND profile=?",
                (prepared.blob_key, prepared.profile),
            ).fetchone()
        if payload_row is not None:
            return PutResult(
                ref=ref,
                raw_len=prepared.raw_len,
                stored_len=int(row["stored_len"]),
                codec=str(row["codec"]),
                zstd_dict_id=row["zstd_dict_id"],
            )
        if raw is None:
            raise MissingContent(
                f"payload missing for {(prepared.blob_key, prepared.profile)} and no raw bytes to repair with"
            )
        raise _RepairRequired(row)

    def encode_with_stored_policy(self, s: Session, row: sqlite3.Row, raw: bytes) -> Encoded:
        """Re-encode with the policy encoded in a stored encodings row (B3).

        Shared by the put-repair flow and the Repository's atomic-upsert
        repair path so the two cannot diverge. Stored metadata that is not a
        usable policy is :class:`CorruptContent` (D7).
        """
        stored = self.profile_from_encoding_row(row)
        try:
            return self.codec.encode(
                raw, stored, self._load_dict(stored, s), dict_cache_key=stored.zstd_dict_id
            )
        except ValueError as exc:
            raise CorruptContent(f"stored encoding policy unusable: {exc}") from exc

    def resolve_repair_write(self, s: Session, prepared: PreparedPut, row: sqlite3.Row, raw: bytes) -> tuple[PreparedPut, Encoded]:
        """P2.2: the prepare-side repair DECISION (no commit) — encode under
        the stored policy, enforce the blob limit, resolve + ensure the write
        shard (rehome off a missing/unusable shard, P1.2), and return the
        ``(prepared, enc)`` to write. Shared by :meth:`repair_flow` and
        ``Repository._upsert_repair`` so the decision is defined once."""
        enc = self.encode_with_stored_policy(s, row, raw)
        self.check_blob_limit(enc.stored_len, s.conn)
        shard_id = self.backend.resolve_write_shard(enc.stored_len, row["shard_id"])
        if self.backend.mode == "sqlite_sharded":
            s.forget_missing_shard(shard_id)
        prepared = PreparedPut(
            prepared.blob_key, prepared.raw_len, prepared.profile, enc, shard_id
        )
        return prepared, enc

    def repair_flow(self, s: Session, prepared: PreparedPut, raw: bytes) -> PutResult:
        """Repair a missing payload using the STORED policy, rehoming to a
        writable shard when the referenced shard is missing/NULL (S2)."""
        row = s.query_one(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
            (prepared.blob_key, prepared.profile),
        )
        if row is None:
            raise MissingContent(
                f"encoding for {(prepared.blob_key, prepared.profile)} vanished between prepare and repair"
            )
        if row["zstd_dict_id"] is not None and s.dict_bytes(str(row["zstd_dict_id"])) is None:
            raise MissingContent(
                f"dictionary {row['zstd_dict_id']!r} missing for {(prepared.blob_key, prepared.profile)}"
            )
        prepared, enc = self.resolve_repair_write(s, prepared, row, raw)  # P2.2
        now = self.backend.now()
        if self.backend.mode == "sqlite_sharded":
            assert prepared.shard_id is not None  # resolve_repair_write set a concrete shard
            alias = s.alias_for(prepared.shard_id)
            attach = prepared.shard_id
        else:
            alias = "p"
            attach = None
        with self.backend.txn_on(
            s.conn,
            write=True,
            attach_shard_id=attach,
            session=s,
        ) as conn:
            self.backend.store_encoding_and_payload_on(
                conn,
                blob_key=prepared.blob_key,
                raw_len=prepared.raw_len,
                created_at=now,
                profile=prepared.profile,
                codec=enc.codec,
                codec_params_json=enc.codec_params_json,
                zstd_dict_id=enc.zstd_dict_id,
                stored_len=enc.stored_len,
                checksum=None,
                shard_id=prepared.shard_id,
                updated_at=now,
                payload=enc.data,
                alias=alias,
            )
        return PutResult(
            ref=ContentRef(prepared.blob_key, prepared.profile),
            raw_len=prepared.raw_len,
            stored_len=enc.stored_len,
            codec=enc.codec,
            zstd_dict_id=enc.zstd_dict_id,
        )

    def _persist(self, s: Session, prepared: PreparedPut, raw: bytes | None) -> PutResult:
        """Persist a prepared put on the session's connection.

        This is the commit point (D3): the dedupe hit is committed inside a
        write transaction that re-probes payload + required dict; success
        means readable at commit.
        """
        now = self.backend.now()
        if prepared.enc is not None:
            self.check_blob_limit(prepared.enc.stored_len, s.conn)
            shard_id = self.backend.resolve_write_shard(prepared.enc.stored_len, prepared.shard_id)
            if self.backend.mode == "sqlite_sharded":
                s.forget_missing_shard(shard_id)
            # Fold the resolved shard back into the prepared put so the
            # shared persist helper writes with the correct locator.
            prepared = PreparedPut(
                prepared.blob_key, prepared.raw_len, prepared.profile, prepared.enc, shard_id
            )
            alias = s.alias_for(shard_id) if self.backend.mode == "sqlite_sharded" else "p"
            with self.backend.txn_on(
                s.conn,
                write=True,
                attach_shard_id=shard_id if self.backend.mode == "sqlite_sharded" else None,
                session=s,
            ) as conn:
                return self.persist_prepared(conn, s, prepared, raw, now, alias=alias)
        # Dedupe-hit path: peek the shard, then re-probe inside a write txn.
        for _attempt in range(2):
            peek = s.query_one(
                "SELECT shard_id FROM encodings WHERE blob_key=? AND profile=?",
                (prepared.blob_key, prepared.profile),
            )
            if peek is None:
                # Row vanished between prepare and persist: treat as a new write.
                if raw is None:
                    raise Retryable(
                        f"encoding for {(prepared.blob_key, prepared.profile)} "
                        "vanished between prepare and persist; retry"
                    )
                prepared = self._prepare(s, raw, prepared.profile)
                if prepared.enc is None:
                    continue
                return self._persist(s, prepared, raw)
            attach = (
                int(peek["shard_id"])
                if (self.backend.mode == "sqlite_sharded" and peek["shard_id"] is not None)
                else None
            )
            try:
                with self.backend.txn_on(
                    s.conn, write=True, attach_shard_id=attach, session=s
                ) as conn:
                    row = conn.execute(
                        "SELECT shard_id FROM encodings WHERE blob_key=? AND profile=?",
                        (prepared.blob_key, prepared.profile),
                    ).fetchone()
                    if row is None:
                        continue  # retry (row vanished mid-txn)
                    if self.backend.mode == "sqlite_sharded" and row["shard_id"] != peek["shard_id"]:
                        continue  # retry with the new shard locator
                    alias = (
                        s.alias_for(int(row["shard_id"]))
                        if (self.backend.mode == "sqlite_sharded" and row["shard_id"] is not None)
                        else "p"
                    )
                    return self.persist_prepared(conn, s, prepared, raw, now, alias=alias)
            except _RepairRequired:
                if raw is None:
                    raise
                return self.repair_flow(s, prepared, raw)
        raise Retryable("persist retry budget exhausted")

    def hash_stream_events(
        self,
        fp: BinaryIO,
        spool: IO[bytes],
        cancel: CancelToken | None = None,
    ) -> Generator[OpEvent, None, tuple[str, int, str, str]]:
        """Hash ``fp`` into ``spool`` (one pass), yielding live progress events
        (review P2-3). Returns the identity digest via the generator.

        The caller owns the spool lifecycle, so a dedupe hit never needs to
        materialize the spooled bytes (review M19 / locked decision D6).
        """
        hasher = self.identity.hasher()
        total = 0
        last_mark = 0
        while True:
            check_cancel(cancel)
            chunk = fp.read(CHUNK_SIZE)
            if not chunk:
                break
            try:
                spool.write(chunk)
            except TypeError as exc:  # belt-and-braces behind the eager probe (A8)
                raise TypeError(
                    "stream source must yield bytes (open it in binary mode); a text stream was read"
                ) from exc
            hasher.update(chunk)
            total += len(chunk)
            if total - last_mark >= PROGRESS_INTERVAL:
                yield OpEvent(kind="progress", op="put", metrics={"bytes_in": total})
                last_mark = total
        spool.seek(0)
        return hasher.digest()

    def stream_prepare(
        self, s: Session, spool: IO[bytes], digest: tuple[str, int, str, str], profile: str
    ) -> tuple[PreparedPut, bytes | None]:
        """Resolve a streamed put inside the persist session (D6/M9/M19).

        Look up the digest first: a healthy dedupe hit returns without reading
        the spool. A miss/repair reads the spool, RE-BINDS with
        ``identity.key_bytes(raw)`` (forged digests are refused), then
        prepares normally.
        """
        blob_key, raw_len, _, _ = digest
        peek = s.query_one(
            "SELECT shard_id FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
        )
        if peek is not None and s.payload_exists(blob_key, profile, peek["shard_id"]):
            return PreparedPut(blob_key, raw_len, profile, enc=None, shard_id=peek["shard_id"]), None
        raw = spool.read()
        # D6: re-bind the spooled bytes exactly once; the verified key is
        # threaded into prepare so nothing re-hashes.
        computed = self.identity.key_bytes(raw)
        if computed[0] != blob_key:
            raise CorruptContent(
                "stream identity mismatch: hashed digest does not match the spooled bytes"
            )
        prepared = self._prepare(s, raw, profile, blob_key=computed[0], _verified_key=True)
        return prepared, raw

    # -- core API (spec 4.3) --------------------------------------------------

    def put_bytes(self, data: bytes, profile: str, cancel: CancelToken | None = None) -> Operation[PutResult]:
        def _run() -> Generator[OpEvent, None, PutResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="hash")
            with self.backend.session() as s:
                prepared = self.prepare_bytes(data, profile, session=s)
                result = self._persist(s, prepared, data)
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
        _require_binary_stream(fp, "put_stream")  # A8: actionable error, not a deep TypeError

        def _run() -> Generator[OpEvent, None, PutResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_SIZE) as spool:
                digest = yield from self.hash_stream_events(fp, spool, cancel)
                # D6: one phase event at the hash→persist boundary (doc §4.1).
                yield OpEvent(kind="phase", op="put", phase="persist")
                with self.backend.session() as s:
                    prepared, raw = self.stream_prepare(s, spool, digest, profile)
                    result = self._persist(s, prepared, raw)
            yield OpEvent(
                kind="done",
                op="put",
                metrics={"bytes_in": result.raw_len, "bytes_out": result.stored_len},
            )
            return result

        return Operation(_run)

    def _decode_from_row(self, s: Session, row: sqlite3.Row, payload: bytes) -> bytes:
        """Decode one encodings row using *stored* metadata only (spec 6.2).

        Runs inside the caller's snapshot transaction (H1/D1); the dict is
        loaded from the session cache (review §16).
        """
        blob_key = str(row["blob_key"])
        profile = str(row["profile"])
        ref = ContentRef(blob_key=blob_key, profile=profile)
        # M15 / D7 / A4: a non-zstd codec must not carry a dictionary.
        self._check_stored_codec_dict(row, ref)
        dict_bytes = None
        if row["zstd_dict_id"] is not None:
            dict_bytes = s.dict_bytes(str(row["zstd_dict_id"]))
            if dict_bytes is None:
                raise MissingContent(f"dictionary {row['zstd_dict_id']!r} missing for {ref}")
        # The blob_key declares the canonical length: use it to bound decode
        # (S3). An unparsable key is corruption, not a raw ValueError.
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
                dict_cache_key=row["zstd_dict_id"],
            )
        except (MissingContent, CorruptContent):
            raise
        except MemoryError:
            raise  # OOM is never corruption (A7)
        except Exception as exc:
            raise CorruptContent(f"failed to decode {ref}: {exc}") from exc

    def _read_snapshot(self, s: Session, blob_key: str, profile: str) -> bytes:
        """Read encodings + payload as ONE snapshot (locked decision D1/H1).

        The encodings row is read inside a transaction that also reads the
        payload: single-file reads BEGIN on the index connection; sharded
        reads ATTACH the shard to the SAME index connection (never a second
        shard connection). WAL (single) and rollback+ATTACH (sharded) both
        give a consistent view. A missing shard file is ``MissingContent``
        and is never created (S2).
        """
        ref = ContentRef(blob_key=blob_key, profile=profile)
        peek = s.query_one(
            "SELECT shard_id FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
        )
        if peek is None:
            raise MissingContent(f"encoding not found for {ref}")
        for _attempt in range(2):
            if self.backend.mode == "sqlite_sharded":
                if peek["shard_id"] is None:
                    # A.2 (Issue 1): a NULL locator is missing content on the
                    # read path; never probe p.payload without an attach.
                    raise MissingContent(f"encoding for {ref} has no shard locator")
                if not self.backend.shard_path(int(peek["shard_id"])).exists():
                    raise MissingContent(f"shard file missing for {ref}")
            attach = (
                int(peek["shard_id"])
                if (self.backend.mode == "sqlite_sharded" and peek["shard_id"] is not None)
                else None
            )
            with self.backend.txn_on(s.conn, write=False, attach_shard_id=attach, session=s) as conn:
                if self.backend.mode == "sqlite_sharded":
                    row = conn.execute(
                        "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                        (blob_key, profile),
                    ).fetchone()
                    if row is None:
                        raise MissingContent(f"encoding not found for {ref}")
                    if row["shard_id"] != peek["shard_id"]:
                        peek = row  # locator moved; re-ATTACH and retry
                        continue
                    shard_probe_id = int(row["shard_id"])
                    alias = s.alias_for(shard_probe_id)
                else:
                    row = conn.execute(
                        "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                        (blob_key, profile),
                    ).fetchone()
                    if row is None:
                        raise MissingContent(f"encoding not found for {ref}")
                    shard_probe_id = None
                    alias = "p"
                # A3: a damaged payload schema is a content finding, not an
                # abort — probe the structure before touching the table.
                if not s.payload_schema_ok(conn, shard_probe_id, alias=alias):
                    raise CorruptContent(
                        f"payload schema missing for {ref}"
                        + (f" (shard {shard_probe_id})" if shard_probe_id is not None else "")
                    )
                table = "payload" if shard_probe_id is None else f"{alias}.payload"
                payload_row = conn.execute(
                    f"SELECT data FROM {table} WHERE blob_key=? AND profile=?",
                    (blob_key, profile),
                ).fetchone()
                if payload_row is None:
                    raise MissingContent(f"payload missing for {ref}")
                payload_data = bytes(payload_row[0])
                # A5 (read layer): stored_len is a MUST (spec §7.3) — a lying
                # row is corruption, never a silent read.
                try:
                    stored_len = int(row["stored_len"])
                except (TypeError, ValueError) as exc:
                    raise CorruptContent(
                        f"encodings.stored_len is not an integer for {ref}"
                    ) from exc
                if len(payload_data) != stored_len:
                    raise CorruptContent(
                        f"payload length {len(payload_data)} does not match "
                        f"encodings.stored_len {stored_len} for {ref}"
                    )
                return self._decode_from_row(s, row, payload_data)
        # A6: a locator that keeps moving under us is transient contention,
        # not corruption — the call is safe to retry.
        raise Retryable(f"shard locator changed repeatedly for {ref}")

    def get_bytes(self, ref: ContentRef) -> bytes:
        """Decode using *stored* encoding metadata only (spec 6.2, 6.4)."""
        with self.backend.session() as s:
            raw = self._read_snapshot(s, ref.blob_key, ref.profile)
            verify_on_read = s.config("verify_on_read")
            if verify_on_read is None:
                verify_on_read = False
            if not isinstance(verify_on_read, bool):
                raise InkpackError(
                    f"invalid repo config: verify_on_read must be a bool, got {verify_on_read!r}"
                )
            if verify_on_read:
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
        """Train a zstd dictionary and store it in the index DB.

        Reclamation rule: the dictionary is reclaimable by the next ``gc()``
        until a profile or an encoding references it — ``train_dict`` alone
        pins nothing, so ``train_dict -> gc`` (without ``set_profile`` or a
        put under a dict profile) silently loses the dict.
        """
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
          validated eagerly at CALL time with the same rules as ``set_profile``
          (review M18).
        Options are frozen at call time; the profile map is frozen at
        operation start, so ``set_profile`` mid-run cannot change remaining
        targets (review §18). Per-target failures (missing payload/dict,
        corrupt payload, deleted profile, payload over the blob limit, or a
        write-path error) skip that target and continue (review M7); ``Busy``
        and ``Cancelled`` always propagate.
        """
        opts = dict(options or {})
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
        if "profile" in opts and policy_keys:
            raise ValueError("reencode option 'profile' cannot be combined with codec/params/zstd_dict_id")
        # M18: build + validate the ad-hoc policy NOW (no DB access).
        adhoc_policy: Profile | None = None
        if policy_keys:
            adhoc_policy = Profile(
                name="<reencode-override>",
                codec=str(opts.get("codec", "zstd")),
                params=cast("dict[str, Any]", opts.get("params") or {}),
                zstd_dict_id=opts.get("zstd_dict_id"),
            )
            validate_profile_entry("<reencode-override>", adhoc_policy)

        def _run() -> Generator[OpEvent, None, ReencodeResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="reencode")
            with self.backend.session() as s:
                # Profile map frozen at operation start (review §18).
                profiles = profiles_from_config(s.config("profiles"))
                policy = adhoc_policy
                if policy is None and "profile" in opts:
                    policy = self._profile_from(profiles, str(opts["profile"]))
                targets_seen = reencoded = skipped = 0
                bytes_in = bytes_out = 0
                for ref in targets:
                    check_cancel(cancel)
                    targets_seen += 1
                    try:
                        row = s.query_one(
                            "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                            (ref.blob_key, ref.profile),
                        )
                        if row is None:
                            skipped += 1
                            continue
                        # Snapshot read (H1): decode inside one transaction.
                        raw = self._read_snapshot(s, ref.blob_key, ref.profile)
                        prof = policy or self._profile_from(profiles, ref.profile)
                        enc = self.codec.encode(
                            raw, prof, self._load_dict(prof, s), dict_cache_key=prof.zstd_dict_id
                        )
                        self.check_blob_limit(enc.stored_len, s.conn)
                        # In-place update of payload + encodings in one txn (spec 7.5).
                        now = self.backend.now()
                        alias = (
                            s.alias_for(int(row["shard_id"]))
                            if (self.backend.mode == "sqlite_sharded" and row["shard_id"] is not None)
                            else "p"
                        )
                        with self.backend.txn_on(
                            s.conn,
                            write=True,
                            attach_shard_id=row["shard_id"]
                            if self.backend.mode == "sqlite_sharded"
                            else None,
                            session=s,
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
                                alias=alias,
                            )
                        reencoded += 1
                        bytes_in += len(raw)
                        bytes_out += enc.stored_len
                        yield OpEvent(
                            kind="item",
                            op="reencode",
                            metrics={"targets": targets_seen, "reencoded": reencoded},
                        )
                    except (MissingContent, CorruptContent, UnknownProfile, ValueError) as exc:
                        # Per-target problems must not kill a bulk run
                        # (review P1-3, P1-REENC-1, M7): skip and keep going.
                        # Busy / Cancelled / unexpected InkpackError propagate.
                        skipped += 1
                        yield OpEvent(
                            kind="log",
                            op="reencode",
                            message=f"skipped {ref}: {exc}",
                            metrics={"targets": targets_seen, "skipped": skipped},
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
        """Keyset-paged, shard-ordered iteration over encodings (A5/C1, r3-C).

        The iteration order is materialized ONCE per run into a WITHOUT ROWID
        temp table whose PK is the keyset ``(ord_shard, blob_key, profile)``,
        then paged with keyset seeks. The previous per-page
        ``ORDER BY COALESCE(shard_id, -1), ...`` matched no index, so EVERY
        page re-scanned and re-sorted the whole ``encodings`` table —
        O(pages · N log N), a cliff precisely at the scale C1 targets. Now:
        one sort total, and keyset pages seek the PK.

        - Carries ``blobs.raw_len`` (decision J) per row instead of
          materializing a blobs map (review §15).
        - Keeps ``(shard_id, blob_key, profile)`` order so consecutive rows
          hit the same shard: session attaches (C1) cost O(shards), not
          O(rows). NULL shard_ids (tampered locators) sort first via
          COALESCE, which keeps the keyset comparison total.

        Semantic delta (changelog): the checked set is FROZEN at
        materialization — encodings added mid-verify are picked up by the
        next run; encodings deleted mid-verify classify as ``missing``
        (previously the per-page snapshots could see either state).
        """
        s.execute("DROP TABLE IF EXISTS _verify_order")
        s.execute(
            "CREATE TEMP TABLE _verify_order("
            "  ord_shard     INTEGER NOT NULL,"
            "  blob_key      TEXT    NOT NULL,"
            "  profile       TEXT    NOT NULL,"
            "  blobs_raw_len INTEGER,"
            "  PRIMARY KEY (ord_shard, blob_key, profile)"
            ") WITHOUT ROWID"
        )
        s.execute(
            "INSERT INTO _verify_order(ord_shard, blob_key, profile, blobs_raw_len) "
            "SELECT COALESCE(e.shard_id, -1), e.blob_key, e.profile, b.raw_len "
            "FROM encodings e LEFT JOIN blobs b ON b.blob_key = e.blob_key"
        )
        try:
            last: tuple[int, str, str] | None = None
            remaining = limit
            while True:
                if last is None:
                    rows = s.query_all(
                        "SELECT ord_shard, blob_key, profile, blobs_raw_len "
                        "FROM _verify_order ORDER BY ord_shard, blob_key, profile LIMIT ?",
                        (_PAGE_SIZE,),
                    )
                else:
                    rows = s.query_all(
                        "SELECT ord_shard, blob_key, profile, blobs_raw_len "
                        "FROM _verify_order "
                        "WHERE (ord_shard, blob_key, profile) > (?, ?, ?) "
                        "ORDER BY ord_shard, blob_key, profile LIMIT ?",
                        (last[0], last[1], last[2], _PAGE_SIZE),
                    )
                if not rows:
                    return
                for row in rows:
                    if remaining is not None:
                        if remaining <= 0:
                            return
                        remaining -= 1
                    yield row
                    last = (int(row["ord_shard"]), str(row["blob_key"]), str(row["profile"]))
        finally:
            s.execute("DROP TABLE IF EXISTS _verify_order")

    def verify(
        self,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[VerifyResult]:
        """Decode every encoding and compare against the identity (spec 10.1).

        Every row is classified as exactly one of ok / missing / corrupt
        (decision D, A5 amendment): an unparsable ``blob_key``, an encoding
        without a ``blobs`` row, a ``blobs.raw_len`` that disagrees with the
        key, a lying ``stored_len``, a damaged payload schema, or an
        undecodable payload all count as corrupt and never abort the run
        (review §3). Decoding goes through the snapshot reader (H1/D1).
        ``MemoryError`` propagates (A7) — OOM is never a per-row finding.
        ``item`` events are throttled (review M22): one per
        ``_VERIFY_ITEM_INTERVAL`` rows plus a final exact one.
        """
        THROTTLE = _VERIFY_ITEM_INTERVAL

        def _run() -> Generator[OpEvent, None, VerifyResult]:
            checked = ok = missing = corrupt = 0

            def emit_item() -> OpEvent:
                return OpEvent(
                    kind="item",
                    op="verify",
                    metrics={"checked": checked, "ok": ok, "missing": missing, "corrupt": corrupt},
                )

            yield OpEvent(kind="start", op="verify")
            with self.backend.session() as s:
                for row in self._iter_encodings(s, limit):
                    check_cancel(cancel)
                    checked += 1
                    blob_key = str(row["blob_key"])
                    # A5: metadata invariants first (free integer compares),
                    # so a lying row is reported corrupt without a decode.
                    # Precedence (decision D): a key that fails to parse is
                    # corrupt on its own — blobs_raw_len is deliberately NOT
                    # consulted for such a row (there is no well-formed key
                    # to compare it against).
                    try:
                        expected_len, expected_sha, expected_blake = self.identity.parse(blob_key)
                    except ValueError:
                        corrupt += 1
                    else:
                        blobs_raw_len = row["blobs_raw_len"]
                        try:
                            metadata_ok = blobs_raw_len is not None and int(blobs_raw_len) == expected_len
                        except (TypeError, ValueError):
                            metadata_ok = False  # non-integer raw_len is corruption
                        if not metadata_ok:
                            corrupt += 1  # missing blobs row / raw_len mismatch (J/A5)
                        else:
                            try:
                                raw = self._read_snapshot(s, blob_key, str(row["profile"]))
                            except MemoryError:
                                raise  # OOM is never corruption (A7)
                            except Busy:
                                raise
                            except Cancelled:
                                raise
                            except MissingContent:
                                missing += 1
                            except CorruptContent:
                                corrupt += 1
                            except InkpackError:
                                raise  # schema/config — not a per-row finding
                            except Exception as exc:
                                corrupt += 1  # unexpected decode/identity bugs only
                                yield OpEvent(
                                    kind="log",
                                    op="verify",
                                    message=f"unexpected per-row error for {blob_key}: {exc}",
                                )
                            else:
                                try:
                                    got_key, got_len, got_sha, got_blake = self.identity.key_bytes(raw)
                                    if (
                                        got_key != blob_key
                                        or got_len != expected_len
                                        or got_sha != expected_sha
                                        or got_blake != expected_blake
                                    ):
                                        corrupt += 1
                                    else:
                                        ok += 1
                                except ValueError:
                                    corrupt += 1
                    if checked % THROTTLE == 0:
                        yield emit_item()
                if checked % THROTTLE != 0:
                    yield emit_item()  # final exact counts (Issue 47)
            result = VerifyResult(checked=checked, ok=ok, missing=missing, corrupt=corrupt)
            yield OpEvent(kind="done", op="verify", metrics={"checked": checked, "ok": ok})
            return result

        return Operation(_run)

    def gc(self, live: Iterable[ContentRef], cancel: CancelToken | None = None) -> Operation[GcResult]:
        """Delete encodings/payloads not in ``live``, then orphan blobs and dicts.

        Emits a per-batch ``item`` event (r4-P3.1) after each batch's commit,
        carrying the cumulative ``encodings_deleted``/``payload_rows_deleted``
        totals, the 1-based batch ``index``, the total ``batches``, and the
        batch's ``shards`` — then a ``done`` event with the full-run totals
        (including the final convergence sweep and the reclaimed orphan
        blobs / unreferenced dicts). The per-batch exclusive window can be
        long for large dead sets — see the GUARANTEES.md duration note;
        cancel between batches keeps committed batches.
        """

        def _run() -> Generator[OpEvent, None, GcResult]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="gc")
            encodings_deleted = 0
            payload_rows_deleted = 0
            for summary in self.backend.gc_iter(live, cancel):
                if summary.index == 0:  # final totals sentinel (not a batch item)
                    encodings_deleted = summary.encodings_deleted
                    payload_rows_deleted = summary.payload_rows_deleted
                else:
                    yield OpEvent(kind="item", op="gc", metrics=asdict(summary))
            # Orphan blobs + unreferenced dicts are reclaimed only AFTER every
            # batch has committed (and the final convergence sweep): each
            # delete is its own atomic txn and is safe against the puts a
            # committed batch authorized.
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
