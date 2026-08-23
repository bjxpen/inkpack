"""Repository: the text-novel model on top of BlobStore (spec 4.4, 7.2)."""

from __future__ import annotations

import io
import sqlite3
import tempfile
from collections.abc import Generator, Iterator
from typing import Any, BinaryIO, cast

from .blobstore import (
    SPOOL_MAX_SIZE,
    BlobStore,
    PreparedPut,
    # E5/A8: intentional cross-module internals (private within the package).
    _RepairRequired,  # pyright: ignore[reportPrivateUsage]
    _require_binary_stream,  # pyright: ignore[reportPrivateUsage]
)
from .failpoints import failpoint
from .sqlite import Session, SqliteBackend
from .types import (
    CancelToken,
    ChapterInfo,
    ContentRef,
    MissingContent,
    NotFound,
    Operation,
    OpEvent,
    Profile,
    Retryable,
    UnknownProfile,
    check_cancel,
    profiles_from_config,
    profiles_to_config,
    require_pk,
    validate_profiles,
)


class Repository:
    def __init__(self, backend: SqliteBackend, store: BlobStore) -> None:
        self.backend = backend
        self.store = store

    # -- profile management (thin wrappers over repo_config["profiles"]) -----

    def get_profiles(self) -> dict[str, Profile]:
        """Return all profiles as :class:`Profile` objects (write policies)."""
        return profiles_from_config(self.backend.config_get("profiles"))

    def get_profile(self, name: str) -> Profile:
        """Return one profile; raises :class:`UnknownProfile` when missing."""
        profiles = self.get_profiles()
        if name not in profiles:
            raise UnknownProfile(f"profile not found: {name}")
        return profiles[name]

    def set_profile(self, profile: Profile) -> None:
        """Add or replace a single profile and persist it in the repo config.

        The profile is validated (codec must be ``none``/``zstd``,
        ``zstd_dict_id`` only with ``zstd``) before being stored, so invalid
        definitions fail fast instead of surfacing later at write time. The
        dictionary-existence check AND the read-modify-write happen inside
        ONE write transaction (review P1-CFG-1, B2): concurrent
        ``set_profile`` calls cannot lose updates, and the check cannot be
        interleaved with gc's single-txn dict reclamation (check passes →
        gc deletes the dict → profile references a ghost) — with both in the
        same txn every interleaving serializes correctly.
        """
        if not isinstance(cast("Any", profile), Profile):
            raise TypeError(f"profile must be a Profile instance, got {type(profile).__name__}")
        validate_profiles({profile.name: profile})

        def _merge(current: Any, conn: sqlite3.Connection) -> dict[str, Any]:
            self._require_known_dicts({profile.name: profile}, conn=conn)
            profiles = profiles_from_config(current)
            profiles[profile.name] = profile
            return profiles_to_config(profiles)

        self.backend.config_update_in_txn("profiles", _merge)

    def set_profiles(self, profiles: dict[str, Profile]) -> None:
        """Replace the whole profile set and persist it in the repo config.

        The set must be non-empty, every entry valid, and every referenced
        dictionary must exist in this repository (Issue 12). The dictionary
        check and the replacement commit in ONE write transaction (B2).
        Existing stored content is unaffected (decoding never consults
        profiles, spec 6.2).
        """
        validate_profiles(profiles)

        def _replace(_current: Any, conn: sqlite3.Connection) -> dict[str, Any]:
            self._require_known_dicts(profiles, conn=conn)
            return profiles_to_config(profiles)

        self.backend.config_update_in_txn("profiles", _replace)

    def set_verify_on_read(self, flag: bool) -> None:
        """Toggle the ``verify_on_read`` repo config (D2).

        Strict ``bool``; the RMW goes through the in-txn config update (B2
        machinery) so concurrent toggles cannot lose updates. ``get_bytes``/
        ``open`` pick the new value up on their next call.
        """
        if not isinstance(cast("Any", flag), bool):
            raise TypeError(f"verify_on_read must be a bool, got {type(flag).__name__}")
        self.backend.config_update_in_txn("verify_on_read", lambda _current, _conn: flag)

    def _require_known_dicts(
        self, profiles: dict[str, Profile], conn: sqlite3.Connection | None = None
    ) -> None:
        """Raise MissingContent listing dictionary ids referenced by profiles
        that do not exist in this repository (Issue 12, B2).

        With ``conn`` supplied the checks run on that connection — inside the
        caller's write transaction — so the check is atomic with the profile
        write. Without it (diagnostic paths) a read probe is used.
        """
        missing: list[str] = []
        for p in profiles.values():
            if p.zstd_dict_id is None:
                continue
            if conn is not None:
                row = conn.execute(
                    "SELECT 1 FROM dicts WHERE dict_id=?", (p.zstd_dict_id,)
                ).fetchone()
                if row is None:
                    missing.append(p.zstd_dict_id)
            elif self.backend.get_dict(p.zstd_dict_id) is None:
                missing.append(p.zstd_dict_id)
        if missing:
            raise MissingContent(
                "dictionaries do not exist in this repository: " + ", ".join(sorted(missing))
            )

    # -- novels / chapters ---------------------------------------------------

    def create_novel(self, title: str, meta: dict[str, Any] | None = None) -> int:
        return self.backend.create_novel(title, meta=meta)

    def list_novels(self, search: str = "") -> list[dict[str, Any]]:
        return self.backend.list_novels(search=search)

    def get_novel(self, novel_id: int) -> dict[str, Any]:
        """Novel catalog row (no chapters); raises :class:`NotFound`."""
        row = self.backend.get_novel(self._require_novel_id(novel_id))
        if row is None:
            raise NotFound(f"novel {novel_id} not found")
        return dict(row)

    def update_novel(self, novel_id: int, title: str | None = None, slug: str | None = None) -> None:
        """Update the provided novel fields; raises :class:`NotFound`."""
        if not self.backend.update_novel(self._require_novel_id(novel_id), title=title, slug=slug):
            raise NotFound(f"novel {novel_id} not found")

    def delete_novel(self, novel_id: int, *, cascade: bool = True) -> None:
        """Delete a novel (and, with cascade, its chapters + metadata).

        Catalog-only: reclaim content with ``gc(iter_live_content())``.
        """
        if not self.backend.delete_novel(self._require_novel_id(novel_id), cascade=cascade):
            raise NotFound(f"novel {novel_id} not found")

    @staticmethod
    def _chapter_info(row: sqlite3.Row) -> ChapterInfo:
        """Explicit per-column construction (E3): tolerant of future column
        additions, loud on removals — unlike ``ChapterInfo(**dict(row))``,
        which breaks on any new catalog column."""
        return ChapterInfo(
            id=int(row["id"]),
            novel_id=int(row["novel_id"]),
            order_key=row["order_key"],
            blob_key=str(row["blob_key"]),
            profile=str(row["profile"]),
            media_type=row["media_type"],
            charset=row["charset"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def list_chapters(self, novel_id: int) -> list[ChapterInfo]:
        """Chapter catalog rows for a novel (no bodies); raises :class:`NotFound`
        for a missing novel, returns ``[]`` for a novel with no chapters."""
        novel_id = self._require_novel_id(novel_id)
        if self.backend.get_novel(novel_id) is None:
            raise NotFound(f"novel {novel_id} not found")
        return [self._chapter_info(row) for row in self.backend.list_chapters(novel_id)]

    def get_chapter(self, chapter_id: int) -> ChapterInfo:
        """One chapter's catalog row; raises :class:`NotFound`."""
        row = self.backend.get_chapter(self._require_pk_public(chapter_id, "chapter_id"))
        if row is None:
            raise NotFound(f"chapter {chapter_id} not found")
        return self._chapter_info(row)

    def delete_chapter(self, chapter_id: int) -> None:
        """Delete a chapter row + its metadata; raises :class:`NotFound`.

        Catalog-only: reclaim content with ``gc(iter_live_content())``.
        """
        if not self.backend.delete_chapter(self._require_pk_public(chapter_id, "chapter_id")):
            raise NotFound(f"chapter {chapter_id} not found")

    @staticmethod
    def _normalize_chapter_key(chapter_key: str | int) -> str:
        """Validate and normalize a chapter key (locked semantics S7): only
        ``str`` or ``int``; ``None`` and ``bool`` are rejected so the catalog
        never stores ``"None"`` / ``"True"``."""
        if isinstance(chapter_key, bool) or not isinstance(cast("Any", chapter_key), (str, int)):
            raise TypeError(
                f"chapter_key must be str or int, got {type(chapter_key).__name__}"
            )
        return str(chapter_key)

    @staticmethod
    def _require_pk_public(value: Any, label: str) -> int:
        """Validate a primary-key argument (M16: int, not bool, non-negative).

        Delegates to :func:`inkpack.types.require_pk` (E2: one validator).
        """
        return require_pk(value, label)

    @staticmethod
    def _require_novel_id(novel_id: Any) -> int:
        """Validate a novel id (M16: int, not bool, non-negative)."""
        return require_pk(novel_id, "novel_id")

    def upsert_chapter(
        self,
        novel_id: int,
        chapter_key: str | int,
        body_bytes: bytes,
        profile: str,
        hints: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> int:
        """Insert or replace a chapter; content + catalog commit atomically.

        A failure (e.g. missing novel, busy) leaves no orphan blob or empty
        shard behind.

        ``chapter_key`` is stored as ``order_key`` and ordered by a **TEXT
        sort** (``"1" < "10" < "2"``) — zero-pad for numeric order (e.g.
        ``"001"``). For multi-MB bodies prefer
        :meth:`upsert_chapter_stream` (cancel + live progress).
        """
        novel_id = self._require_novel_id(novel_id)
        order_key = self._normalize_chapter_key(chapter_key)
        hints = hints or {}
        with self.backend.session() as s:
            if s.query_one("SELECT 1 FROM novels WHERE id=?", (novel_id,)) is None:
                raise NotFound(f"novel {novel_id} not found")
            prepared = self.store.prepare_bytes(body_bytes, profile, session=s)
            return self._upsert_prepared(
                s, novel_id, order_key, prepared, body_bytes, hints, meta
            )

    def upsert_chapter_stream(
        self,
        fp: BinaryIO,
        novel_id: int,
        chapter_key: str | int,
        profile: str,
        hints: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        size_hint: int | None = None,
        cancel: CancelToken | None = None,
    ) -> Operation[int]:
        """Streaming upsert: hashes the stream (live progress events) then
        commits content + catalog atomically. Returns an Operation[int].

        ``chapter_key`` becomes ``order_key`` (TEXT sort — zero-pad for
        numeric order). The preferred form for multi-MB bodies: cancel +
        live progress, and the source is read in one pass.
        """
        del size_hint
        novel_id = self._require_novel_id(novel_id)
        order_key = self._normalize_chapter_key(chapter_key)
        hints = hints or {}
        _require_binary_stream(fp, "upsert_chapter_stream")  # A8

        def _run() -> Generator[OpEvent, None, int]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_SIZE) as spool:
                digest = yield from self.store.hash_stream_events(fp, spool, cancel)
                # D6: one phase event at the hash→persist boundary.
                yield OpEvent(kind="phase", op="put", phase="persist")
                with self.backend.session() as s:
                    if s.query_one("SELECT 1 FROM novels WHERE id=?", (novel_id,)) is None:
                        raise NotFound(f"novel {novel_id} not found")
                    prepared, raw = self.store.stream_prepare(s, spool, digest, profile)
                    if prepared.enc is not None:
                        bytes_out = prepared.enc.stored_len
                    else:
                        # Dedupe hit: report the stored encoding's length, never
                        # 0 (review P2-METRIC-1).
                        row = s.query_one(
                            "SELECT stored_len FROM encodings WHERE blob_key=? AND profile=?",
                            (prepared.blob_key, prepared.profile),
                        )
                        bytes_out = int(row["stored_len"]) if row is not None else 0
                    chapter_id = self._upsert_prepared(
                        s, novel_id, order_key, prepared, raw, hints, meta
                    )
            yield OpEvent(
                kind="done",
                op="put",
                metrics={"bytes_in": digest[1], "bytes_out": bytes_out},
            )
            return chapter_id

        return Operation(_run)

    @staticmethod
    def _shard_alias(s: Session, shard_id: int | None) -> str:
        return s.alias_for(shard_id) if shard_id is not None else "p"

    def _catalog_upsert(
        self,
        conn: sqlite3.Connection,
        novel_id: int,
        order_key: str,
        prepared: PreparedPut,
        hints: dict[str, Any],
        meta: dict[str, Any] | None,
        now: str,
    ) -> int:
        """Write the chapter catalog + metadata inside the caller's open
        write transaction, then fire the pre-commit failpoint (H1/B3)."""
        chapter_id = self.backend.upsert_chapter_catalog_on(
            conn,
            novel_id=novel_id,
            order_key=order_key,
            blob_key=prepared.blob_key,
            profile=prepared.profile,
            media_type=hints.get("media_type"),
            charset=hints.get("charset"),
            meta=meta,
            now=now,
        )
        failpoint("upsert.pre_commit")  # inside the unified txn, pre-commit
        return chapter_id

    def _upsert_prepared(
        self,
        s: Session,
        novel_id: int,
        order_key: str,
        prepared: PreparedPut,
        raw: bytes | None,
        hints: dict[str, Any],
        meta: dict[str, Any] | None,
    ) -> int:
        """Commit content + chapter catalog + metadata on ONE session (M20).

        Both the common path (healthy dedupe hit or new write) and the
        repair path (a payload that vanished between prepare's probe and the
        commit point, S2/H2) commit content + catalog in a SINGLE
        transaction (B3): a chapter never commits pointing at missing
        content, and no failure can leave a repaired payload without its
        catalog row (or vice versa).
        """
        backend = self.backend
        now = backend.now()
        shard_id: int | None = None
        if prepared.enc is not None:
            self.store.check_blob_limit(prepared.enc.stored_len, s.conn)
            shard_id = backend.resolve_write_shard(prepared.enc.stored_len, prepared.shard_id)
            if backend.mode == "sqlite_sharded":
                s.forget_missing_shard(shard_id)
            prepared = PreparedPut(
                prepared.blob_key, prepared.raw_len, prepared.profile, prepared.enc, shard_id
            )
        else:
            peek = s.query_one(
                "SELECT shard_id FROM encodings WHERE blob_key=? AND profile=?",
                (prepared.blob_key, prepared.profile),
            )
            if peek is not None and backend.mode == "sqlite_sharded" and peek["shard_id"] is not None:
                shard_id = int(peek["shard_id"])
        try:
            with backend.txn_on(
                s.conn,
                write=True,
                attach_shard_id=shard_id if (backend.mode == "sqlite_sharded" and shard_id is not None) else None,
                session=s,
            ) as conn:
                self.store.persist_prepared(
                    conn, s, prepared, raw, now, alias=self._shard_alias(s, shard_id)
                )
                return self._catalog_upsert(conn, novel_id, order_key, prepared, hints, meta, now)
        except _RepairRequired:
            if raw is None:
                raise
            # B3: fold the repair INTO the catalog transaction. Re-fetch the
            # row (it may have moved), encode under the STORED policy, rehome
            # when the referenced shard is missing/NULL, then write content +
            # catalog in one commit.
            row = s.query_one(
                "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                (prepared.blob_key, prepared.profile),
            )
            if row is None:
                # The encoding vanished under us (concurrent gc): transient
                # contention, safe to retry the upsert (A6/B3).
                raise Retryable(
                    f"encoding for {(prepared.blob_key, prepared.profile)} vanished "
                    "between prepare and persist; retry"
                ) from None
            enc = self.store.encode_with_stored_policy(s, row, raw)
            self.store.check_blob_limit(enc.stored_len, s.conn)
            shard_id = backend.resolve_write_shard(enc.stored_len, row["shard_id"])
            if backend.mode == "sqlite_sharded":
                s.forget_missing_shard(shard_id)
            prepared = PreparedPut(
                prepared.blob_key, prepared.raw_len, prepared.profile, enc, shard_id
            )
            with backend.txn_on(
                s.conn,
                write=True,
                # shard_id is always a resolved int here (resolve_write_shard).
                attach_shard_id=shard_id if backend.mode == "sqlite_sharded" else None,
                session=s,
            ) as conn:
                backend.store_encoding_and_payload_on(
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
                    shard_id=shard_id,
                    updated_at=now,
                    payload=enc.data,
                    alias=self._shard_alias(s, shard_id),
                )
                return self._catalog_upsert(conn, novel_id, order_key, prepared, hints, meta, now)

    def _chapter_ref(self, chapter_id: int) -> ContentRef:
        row = self.backend.get_chapter(self._require_pk_public(chapter_id, "chapter_id"))
        if row is None:
            # A missing catalog row is a catalog miss (review P1-6): NotFound,
            # not MissingContent (which is reserved for missing stored content).
            raise NotFound(f"chapter {chapter_id} not found")
        return ContentRef(blob_key=str(row["blob_key"]), profile=str(row["profile"]))

    def get_chapter_bytes(self, chapter_id: int) -> bytes:
        return self.store.get_bytes(self._chapter_ref(chapter_id))

    def open_chapter(self, chapter_id: int) -> io.BytesIO:
        return self.store.open(self._chapter_ref(chapter_id))

    def meta_set(self, entity_type: str, entity_id: int | str, key: str, value: Any) -> None:
        self.backend.meta_set(entity_type, entity_id, key, value)

    def meta_get(self, entity_type: str, entity_id: int | str, key: str) -> Any:
        """KV lookup. A missing key and a stored JSON ``null`` both return
        ``None`` (not distinguishable; use ``meta_list`` to tell them apart).
        """
        return self.backend.meta_get(entity_type, entity_id, key)

    def meta_list(self, entity_type: str, entity_id: int | str) -> dict[str, Any]:
        return self.backend.meta_list(entity_type, entity_id)

    def iter_live_content(self, scope: int | None = None) -> Iterator[ContentRef]:
        """Distinct content refs referenced by chapters (optionally one novel).

        Visibility (C3): **at least that of a single-snapshot fetch taken at
        drain start** — a row present at the first page's snapshot sits at a
        fixed key, and the ascending keyset sweep cannot skip it, even under
        concurrent writers; late additions whose key sorts above the cursor
        appear in a later page; mid-drain deletions cause safe over-retention.

        Live-set freshness obligation (normative for callers): the exposure
        window is **commits during or after the drain** — such chapters are
        not represented, and ``gc`` will reclaim their content (recovery:
        re-run the upsert). See GUARANTEES.md.

        Honest footnote: a start-present key can be *delayed* indefinitely if
        writers keep inserting keys that sort below it; a FINISHED drain
        still cannot skip it.
        """
        yield from self.backend.iter_chapter_refs(scope=scope)
