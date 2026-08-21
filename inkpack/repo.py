from __future__ import annotations

from .types import ContentRef, MissingContent


class Repository:
    def __init__(self, backend, store):
        self.backend = backend
        self.store = store

    def create_novel(self, title: str, meta: dict | None = None) -> int:
        return self.backend.create_novel(title, meta=meta)

    def list_novels(self, search: str = "") -> list:
        return self.backend.list_novels(search=search)

    def upsert_chapter(
        self,
        novel_id,
        chapter_key,
        body_bytes,
        profile,
        hints=None,
        meta=None,
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

    def get_chapter_bytes(self, chapter_id) -> bytes:
        row = self.backend.get_chapter(int(chapter_id))
        if not row:
            raise MissingContent("chapter missing")
        return self.store.get_bytes(ContentRef(blob_key=row["blob_key"], profile=row["profile"]))

    def open_chapter(self, chapter_id):
        row = self.backend.get_chapter(int(chapter_id))
        if not row:
            raise MissingContent("chapter missing")
        return self.store.open(ContentRef(blob_key=row["blob_key"], profile=row["profile"]))

    def meta_set(self, entity_type, entity_id, key, value):
        self.backend.meta_set(entity_type, entity_id, key, value)

    def meta_get(self, entity_type, entity_id, key):
        return self.backend.meta_get(entity_type, entity_id, key)

    def meta_list(self, entity_type, entity_id):
        return self.backend.meta_list(entity_type, entity_id)

    def iter_live_content(self, scope=None):
        return self.backend.iter_chapter_refs(scope=scope)
