"""Repository: the text-novel model on top of BlobStore (spec 4.4, 7.2)."""

from __future__ import annotations

import io
from collections.abc import Generator, Iterator
from typing import Any, BinaryIO, cast

from .blobstore import BlobStore, PreparedPut
from .sqlite import SqliteBackend
from .types import (
    CancelToken,
    ChapterInfo,
    ContentRef,
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

        def _merge(current: Any) -> dict[str, Any]:
            profiles = profiles_from_config(current)
            profiles[profile.name] = profile
            return profiles_to_config(profiles)

        self.backend.config_update("profiles", _merge)

    def set_profiles(self, profiles: dict[str, Profile]) -> None:
        """Replace the whole profile set and persist it in the repo config.

        The set must be non-empty and every entry valid; existing stored
        content is unaffected (decoding never consults profiles, spec 6.2).
        """
        validate_profiles(profiles)
        self.backend.config_set("profiles", profiles_to_config(profiles))

    # -- novels / chapters ---------------------------------------------------

    def create_novel(self, title: str, meta: dict[str, Any] | None = None) -> int:
        return self.backend.create_novel(title, meta=meta)

    def list_novels(self, search: str = "") -> list[dict[str, Any]]:
        return self.backend.list_novels(search=search)

    def get_novel(self, novel_id: int) -> dict[str, Any]:
        """Novel catalog row (no chapters); raises :class:`NotFound`."""
        row = self.backend.get_novel(int(novel_id))
        if row is None:
            raise NotFound(f"novel {novel_id} not found")
        return dict(row)

    def update_novel(self, novel_id: int, title: str | None = None, slug: str | None = None) -> None:
        """Update the provided novel fields; raises :class:`NotFound`."""
        if not self.backend.update_novel(int(novel_id), title=title, slug=slug):
            raise NotFound(f"novel {novel_id} not found")

    def delete_novel(self, novel_id: int, *, cascade: bool = True) -> None:
        """Delete a novel (and, with cascade, its chapters + metadata).

        Catalog-only: reclaim content with ``gc(iter_live_content())``.
        """
        if not self.backend.delete_novel(int(novel_id), cascade=cascade):
            raise NotFound(f"novel {novel_id} not found")

    def list_chapters(self, novel_id: int) -> list[ChapterInfo]:
        """Chapter catalog rows for a novel (no bodies); raises :class:`NotFound`
        for a missing novel, returns ``[]`` for a novel with no chapters."""
        if self.backend.get_novel(int(novel_id)) is None:
            raise NotFound(f"novel {novel_id} not found")
        return [ChapterInfo(**dict(row)) for row in self.backend.list_chapters(int(novel_id))]

    def get_chapter(self, chapter_id: int) -> ChapterInfo:
        """One chapter's catalog row; raises :class:`NotFound`."""
        row = self.backend.get_chapter(int(chapter_id))
        if row is None:
            raise NotFound(f"chapter {chapter_id} not found")
        return ChapterInfo(**dict(row))

    def delete_chapter(self, chapter_id: int) -> None:
        """Delete a chapter row + its metadata; raises :class:`NotFound`.

        Catalog-only: reclaim content with ``gc(iter_live_content())``.
        """
        if not self.backend.delete_chapter(int(chapter_id)):
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

    def _require_novel(self, novel_id: int) -> None:
        """Fail fast on a missing novel BEFORE hashing/encoding/shard
        allocation, so a failed upsert has no side effects (review P1-1). The
        write transaction re-probes for race safety."""
        if self.backend.get_novel(int(novel_id)) is None:
            raise NotFound(f"novel {novel_id} not found")

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
        self._require_novel(novel_id)
        order_key = self._normalize_chapter_key(chapter_key)
        hints = hints or {}
        prepared = self.store.prepare_bytes(body_bytes, profile)
        if prepared.enc is not None:
            self.store.check_blob_limit(prepared.enc.stored_len)
        return self.backend.upsert_chapter_with_content(
            novel_id=int(novel_id),
            order_key=order_key,
            blob_key=prepared.blob_key,
            raw_len=prepared.raw_len,
            created_at=self.backend.now(),
            profile=prepared.profile,
            media_type=hints.get("media_type"),
            charset=hints.get("charset"),
            meta=meta,
            enc=prepared.enc,
            shard_id=prepared.shard_id,
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
        order_key = self._normalize_chapter_key(chapter_key)
        hints = hints or {}

        def _run() -> Generator[OpEvent, None, int]:
            check_cancel(cancel)
            self._require_novel(novel_id)
            yield OpEvent(kind="start", op="put", phase="stream_hash")
            digest, raw = yield from self.store.hash_stream_events(fp, cancel)
            prepared: PreparedPut = self.store.prepare_bytes(raw, profile, key_tuple=digest)
            if prepared.enc is not None:
                self.store.check_blob_limit(prepared.enc.stored_len)
                bytes_out = prepared.enc.stored_len
            else:
                # Dedupe hit: report the stored encoding's length, never 0
                # (review P2-METRIC-1).
                row = self.backend.get_encoding(prepared.blob_key, prepared.profile)
                bytes_out = int(row["stored_len"]) if row is not None else 0
            chapter_id = self.backend.upsert_chapter_with_content(
                novel_id=int(novel_id),
                order_key=order_key,
                blob_key=prepared.blob_key,
                raw_len=prepared.raw_len,
                created_at=self.backend.now(),
                profile=prepared.profile,
                media_type=hints.get("media_type"),
                charset=hints.get("charset"),
                meta=meta,
                enc=prepared.enc,
                shard_id=prepared.shard_id,
            )
            yield OpEvent(
                kind="done",
                op="put",
                metrics={"bytes_in": len(raw), "bytes_out": bytes_out},
            )
            return chapter_id

        return Operation(_run)

    def _chapter_ref(self, chapter_id: int) -> ContentRef:
        row = self.backend.get_chapter(chapter_id)
        if row is None:
            # A missing catalog row is a catalog miss (review P1-6): NotFound,
            # not MissingContent (which is reserved for missing stored content).
            raise NotFound(f"chapter {chapter_id} not found")
        return ContentRef(blob_key=str(row["blob_key"]), profile=str(row["profile"]))

    def get_chapter_bytes(self, chapter_id: int) -> bytes:
        return self.store.get_bytes(self._chapter_ref(int(chapter_id)))

    def open_chapter(self, chapter_id: int) -> io.BytesIO:
        return self.store.open(self._chapter_ref(int(chapter_id)))

    def meta_set(self, entity_type: str, entity_id: int | str, key: str, value: Any) -> None:
        self.backend.meta_set(entity_type, entity_id, key, value)

    def meta_get(self, entity_type: str, entity_id: int | str, key: str) -> Any:
        return self.backend.meta_get(entity_type, entity_id, key)

    def meta_list(self, entity_type: str, entity_id: int | str) -> dict[str, Any]:
        return self.backend.meta_list(entity_type, entity_id)

    def iter_live_content(self, scope: int | None = None) -> Iterator[ContentRef]:
        """Distinct content refs referenced by chapters (optionally one novel)."""
        return self.backend.iter_chapter_refs(scope=scope)
