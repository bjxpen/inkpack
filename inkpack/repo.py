"""Repository: the text-novel model on top of BlobStore (spec 4.4, 7.2)."""

from __future__ import annotations

import io
import tempfile
from collections.abc import Generator, Iterator
from typing import Any, BinaryIO, cast

from .blobstore import BlobStore, PreparedPut, RepairRequired
from .sqlite import SqliteBackend
from .types import (
    CancelToken,
    ChapterInfo,
    ContentRef,
    MissingContent,
    NotFound,
    Operation,
    OpEvent,
    Profile,
    UnknownProfile,
    check_cancel,
    profiles_from_config,
    profiles_to_config,
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
        read-modify-write happens inside ONE write transaction (review
        P1-CFG-1): concurrent ``set_profile`` calls cannot lose updates.
        """
        if not isinstance(cast("Any", profile), Profile):
            raise TypeError(f"profile must be a Profile instance, got {type(profile).__name__}")
        validate_profiles({profile.name: profile})
        self._require_known_dicts({profile.name: profile})

        def _merge(current: Any) -> dict[str, Any]:
            profiles = profiles_from_config(current)
            profiles[profile.name] = profile
            return profiles_to_config(profiles)

        self.backend.config_update("profiles", _merge)

    def set_profiles(self, profiles: dict[str, Profile]) -> None:
        """Replace the whole profile set and persist it in the repo config.

        The set must be non-empty, every entry valid, and every referenced
        dictionary must exist in this repository (Issue 12). Existing stored
        content is unaffected (decoding never consults profiles, spec 6.2).
        """
        validate_profiles(profiles)
        self._require_known_dicts(profiles)
        self.backend.config_set("profiles", profiles_to_config(profiles))

    def _require_known_dicts(self, profiles: dict[str, Profile]) -> None:
        """Raise MissingContent listing dictionary ids referenced by profiles
        that do not exist in this repository (Issue 12)."""
        missing = sorted(
            p.zstd_dict_id
            for p in profiles.values()
            if p.zstd_dict_id is not None and self.backend.get_dict(p.zstd_dict_id) is None
        )
        if missing:
            raise MissingContent(
                "dictionaries do not exist in this repository: " + ", ".join(missing)
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

    def list_chapters(self, novel_id: int) -> list[ChapterInfo]:
        """Chapter catalog rows for a novel (no bodies); raises :class:`NotFound`
        for a missing novel, returns ``[]`` for a novel with no chapters."""
        novel_id = self._require_novel_id(novel_id)
        if self.backend.get_novel(novel_id) is None:
            raise NotFound(f"novel {novel_id} not found")
        return [ChapterInfo(**dict(row)) for row in self.backend.list_chapters(novel_id)]

    def get_chapter(self, chapter_id: int) -> ChapterInfo:
        """One chapter's catalog row; raises :class:`NotFound`."""
        row = self.backend.get_chapter(self._require_pk_public(chapter_id, "chapter_id"))
        if row is None:
            raise NotFound(f"chapter {chapter_id} not found")
        return ChapterInfo(**dict(row))

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
        """Validate a primary-key argument (M16: int, not bool, non-negative)."""
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TypeError(f"{label} must be a non-negative int, got {type(value).__name__}")
        return value

    @staticmethod
    def _require_novel_id(novel_id: Any) -> int:
        """Validate a novel id (M16: int, not bool, non-negative)."""
        if isinstance(novel_id, bool) or not isinstance(novel_id, int) or novel_id < 0:
            raise TypeError(f"novel_id must be a non-negative int, got {type(novel_id).__name__}")
        return novel_id

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
        commits content + catalog atomically. Returns an Operation[int]."""
        del size_hint
        novel_id = self._require_novel_id(novel_id)
        order_key = self._normalize_chapter_key(chapter_key)
        hints = hints or {}

        def _run() -> Generator[OpEvent, None, int]:
            check_cancel(cancel)
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            with tempfile.SpooledTemporaryFile(max_size=2 * 1024 * 1024) as spool:
                digest = yield from self.store.hash_stream_events(fp, spool, cancel)
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

    def _upsert_prepared(
        self,
        s: Any,
        novel_id: int,
        order_key: str,
        prepared: PreparedPut,
        raw: bytes | None,
        hints: dict[str, Any],
        meta: dict[str, Any] | None,
    ) -> int:
        """Commit content + chapter catalog + metadata on ONE session (M20).

        The common path (healthy dedupe hit or new write) commits in a single
        transaction; a missing payload with raw bytes available triggers the
        shared repair flow (S2/H2) before the catalog row commits, so a
        chapter never points at missing content.
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
            ) as conn:
                self.store.persist_prepared(conn, s, prepared, raw, now)
                return backend.upsert_chapter_catalog_on(
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
        except RepairRequired:
            if raw is None:
                raise
            self.store.repair_flow(s, prepared, raw)
            with backend.txn_on(s.conn, write=True) as conn:
                return backend.upsert_chapter_catalog_on(
                    conn,
                    novel_id=novel_id,
                    order_key=order_key,
                    blob_key=prepared.blob_key,
                    profile=prepared.profile,
                    media_type=hints.get("media_type"),
                    charset=hints.get("charset"),
                    meta=meta,
                    now=backend.now(),
                )

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
        return self.backend.meta_get(entity_type, entity_id, key)

    def meta_list(self, entity_type: str, entity_id: int | str) -> dict[str, Any]:
        return self.backend.meta_list(entity_type, entity_id)

    def iter_live_content(self, scope: int | None = None) -> Iterator[ContentRef]:
        """Distinct content refs referenced by chapters (optionally one novel)."""
        return self.backend.iter_chapter_refs(scope=scope)
