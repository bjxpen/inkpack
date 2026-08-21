"""Repository: the text-novel model on top of BlobStore (spec 4.4, 7.2)."""

from __future__ import annotations

import io
from collections.abc import Iterator
from typing import Any, cast

from .blobstore import BlobStore
from .sqlite import SqliteBackend
from .types import (
    ContentRef,
    MissingContent,
    Profile,
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
        """Return one profile; raises ``KeyError`` when the name is unknown."""
        profiles = self.get_profiles()
        if name not in profiles:
            raise KeyError(f"profile not found: {name}")
        return profiles[name]

    def set_profile(self, profile: Profile) -> None:
        """Add or replace a single profile and persist it in the repo config.

        The profile is validated (codec must be ``none``/``zstd``,
        ``zstd_dict_id`` only with ``zstd``) before being stored, so invalid
        definitions fail fast instead of surfacing later at write time.
        """
        if not isinstance(cast("Any", profile), Profile):
            raise TypeError(f"profile must be a Profile instance, got {type(profile).__name__}")
        validate_profiles({profile.name: profile})
        profiles = self.get_profiles()
        profiles[profile.name] = profile
        self.backend.config_set("profiles", profiles_to_config(profiles))

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

    def upsert_chapter(
        self,
        novel_id: int,
        chapter_key: str | int,
        body_bytes: bytes,
        profile: str,
        hints: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> int:
        hints = hints or {}
        put = self.store.put_bytes(body_bytes, profile=profile)
        ref = put.result.ref
        return self.backend.upsert_chapter(
            novel_id=int(novel_id),
            order_key=str(chapter_key),
            blob_key=ref.blob_key,
            profile=ref.profile,
            media_type=hints.get("media_type"),
            charset=hints.get("charset"),
            meta=meta,
        )

    def _chapter_ref(self, chapter_id: int) -> ContentRef:
        row = self.backend.get_chapter(chapter_id)
        if row is None:
            raise MissingContent(f"chapter {chapter_id} not found")
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
