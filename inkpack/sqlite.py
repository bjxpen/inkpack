from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from .types import Busy, ContentRef, InkpackError, MissingContent

LATEST_USER_VERSION = 1


SCHEMA_INDEX = """
CREATE TABLE IF NOT EXISTS repo_config(
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS novels(
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  slug TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS chapters(
  id INTEGER PRIMARY KEY,
  novel_id INTEGER NOT NULL,
  order_key TEXT,
  blob_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  media_type TEXT,
  charset TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS meta(
  entity_type TEXT,
  entity_id TEXT,
  key TEXT,
  value_json TEXT,
  updated_at TEXT,
  PRIMARY KEY(entity_type, entity_id, key)
);
CREATE TABLE IF NOT EXISTS blobs(
  blob_key TEXT PRIMARY KEY,
  raw_len INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS encodings(
  blob_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  codec TEXT NOT NULL,
  codec_params_json TEXT NOT NULL,
  zstd_dict_id TEXT,
  stored_len INTEGER NOT NULL,
  checksum TEXT,
  shard_id INTEGER,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(blob_key, profile)
);
CREATE TABLE IF NOT EXISTS dicts(
  dict_id TEXT PRIMARY KEY,
  codec TEXT NOT NULL,
  dict_bytes BLOB NOT NULL,
  params_json TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chapters_blob_profile ON chapters(blob_key, profile);
CREATE INDEX IF NOT EXISTS idx_encodings_zstd_dict_id ON encodings(zstd_dict_id);
"""

SCHEMA_PAYLOAD = """
CREATE TABLE IF NOT EXISTS payload(
  blob_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(blob_key, profile)
);
"""


class SystemClock:
    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=0.2)
    conn.row_factory = sqlite3.Row
    return conn


def migrate_index(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_INDEX)
    conn.execute(f"PRAGMA user_version={LATEST_USER_VERSION}")


def migrate_payload(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PAYLOAD)


@dataclass
class SqliteBackend:
    root: Path
    mode: str
    index_path: Path
    payload_dir: Path | None
    shard_cap_bytes: int
    shard_min_bytes: int
    clock: Any

    @classmethod
    def open(
        cls,
        path: str | os.PathLike,
        mode: str,
        pragmas: dict | None = None,
        shard_cap_bytes: int = 2 << 30,
        shard_min_bytes: int = 256 << 20,
        clock: Any = None,
    ) -> "SqliteBackend":
        pragmas = pragmas or {}
        clock = clock or SystemClock()
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)

        if mode == "sqlite_single":
            index_path = root / "repo.sqlite"
            payload_dir = None
            conn = _connect(index_path)
            try:
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(f"PRAGMA synchronous={pragmas.get('synchronous', 'NORMAL')}")
                migrate_index(conn)
                migrate_payload(conn)
                conn.commit()
            finally:
                conn.close()
        elif mode == "sqlite_sharded":
            index_path = root / "index.sqlite"
            payload_dir = root / "payload"
            payload_dir.mkdir(parents=True, exist_ok=True)
            conn = _connect(index_path)
            try:
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute(f"PRAGMA synchronous={pragmas.get('synchronous', 'NORMAL')}")
                migrate_index(conn)
                conn.commit()
            finally:
                conn.close()
            cls._ensure_shard_file(payload_dir, 1)
        else:
            raise ValueError("mode must be sqlite_single or sqlite_sharded")

        backend = cls(
            root=root,
            mode=mode,
            index_path=index_path,
            payload_dir=payload_dir,
            shard_cap_bytes=shard_cap_bytes,
            shard_min_bytes=shard_min_bytes,
            clock=clock,
        )
        backend._validate_journal_modes()
        return backend

    @staticmethod
    def _ensure_shard_file(payload_dir: Path, shard_id: int) -> Path:
        path = payload_dir / f"shard-{shard_id:04d}.sqlite"
        if not path.exists():
            conn = _connect(path)
            try:
                conn.execute("PRAGMA journal_mode=DELETE")
                migrate_payload(conn)
                conn.commit()
            finally:
                conn.close()
        return path

    def _validate_journal_modes(self) -> None:
        if self.mode != "sqlite_sharded":
            return
        with _connect(self.index_path) as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
            if mode not in {"delete", "truncate"}:
                raise InkpackError("sharded index DB must use rollback journal mode")
        for shard_id in self.list_shards():
            with _connect(self.shard_path(shard_id)) as conn:
                mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
                if mode not in {"delete", "truncate"}:
                    raise InkpackError("sharded payload DB must use rollback journal mode")

    def now(self) -> str:
        return self.clock.now_iso()

    def shard_path(self, shard_id: int) -> Path:
        if self.payload_dir is None:
            raise ValueError("single mode has no shards")
        return self.payload_dir / f"shard-{shard_id:04d}.sqlite"

    def list_shards(self) -> list[int]:
        if self.payload_dir is None:
            return []
        out = []
        for path in sorted(self.payload_dir.glob("shard-*.sqlite")):
            out.append(int(path.stem.split("-")[1]))
        return out

    def ensure_shard_exists(self, shard_id: int) -> None:
        if self.payload_dir is None:
            return
        self._ensure_shard_file(self.payload_dir, shard_id)

    def choose_shard_for_write(self, estimated_payload_bytes: int) -> int:
        if self.mode != "sqlite_sharded":
            return 0
        shard_ids = self.list_shards() or [1]
        current = shard_ids[-1]
        path = self.shard_path(current)
        current_size = path.stat().st_size if path.exists() else 0
        if current_size + estimated_payload_bytes > self.shard_cap_bytes:
            current += 1
            self.ensure_shard_exists(current)
        return current

    @contextmanager
    def txn(self, write: bool = False, attach_shard_id: int | None = None):
        conn = _connect(self.index_path)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            if self.mode == "sqlite_sharded" and attach_shard_id is not None:
                shard_path = self.shard_path(attach_shard_id)
                conn.execute(f"ATTACH DATABASE '{str(shard_path)}' AS p")
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                raise Busy(str(exc)) from exc
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                if self.mode == "sqlite_sharded" and attach_shard_id is not None:
                    conn.execute("DETACH DATABASE p")
            except Exception:
                pass
            conn.close()

    def config_get(self, key: str):
        with self.txn(write=False) as conn:
            row = conn.execute("SELECT value_json FROM repo_config WHERE key=?", (key,)).fetchone()
            if not row:
                return None
            return json.loads(row[0])

    def config_set(self, key: str, obj: Any) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO repo_config(key, value_json) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, _json_dumps(obj)),
            )

    def get_blob(self, blob_key: str):
        with self.txn(write=False) as conn:
            return conn.execute("SELECT * FROM blobs WHERE blob_key=?", (blob_key,)).fetchone()

    def insert_blob_if_missing(self, blob_key: str, raw_len: int, created_at: str) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO blobs(blob_key, raw_len, created_at) VALUES(?, ?, ?)",
                (blob_key, raw_len, created_at),
            )

    def get_encoding(self, blob_key: str, profile: str):
        with self.txn(write=False) as conn:
            return conn.execute(
                "SELECT * FROM encodings WHERE blob_key=? AND profile=?",
                (blob_key, profile),
            ).fetchone()

    def put_encoding_and_payload(
        self,
        *,
        blob_key: str,
        profile: str,
        codec: str,
        codec_params_json: str,
        zstd_dict_id: str | None,
        stored_len: int,
        checksum: str | None,
        shard_id: int | None,
        updated_at: str,
        payload: bytes,
    ) -> None:
        if self.mode == "sqlite_single":
            with self.txn(write=True) as conn:
                conn.execute(
                    "INSERT INTO payload(blob_key, profile, data) VALUES(?, ?, ?) ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                    (blob_key, profile, payload),
                )
                conn.execute(
                    "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, shard_id, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?) "
                    "ON CONFLICT(blob_key, profile) DO UPDATE SET codec=excluded.codec, codec_params_json=excluded.codec_params_json, zstd_dict_id=excluded.zstd_dict_id, stored_len=excluded.stored_len, checksum=excluded.checksum, shard_id=NULL, updated_at=excluded.updated_at",
                    (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, updated_at),
                )
        else:
            sid = shard_id if shard_id is not None else self.choose_shard_for_write(stored_len)
            self.ensure_shard_exists(sid)
            with self.txn(write=True, attach_shard_id=sid) as conn:
                conn.execute(
                    "INSERT INTO p.payload(blob_key, profile, data) VALUES(?, ?, ?) ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                    (blob_key, profile, payload),
                )
                conn.execute(
                    "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, shard_id, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(blob_key, profile) DO UPDATE SET codec=excluded.codec, codec_params_json=excluded.codec_params_json, zstd_dict_id=excluded.zstd_dict_id, stored_len=excluded.stored_len, checksum=excluded.checksum, shard_id=excluded.shard_id, updated_at=excluded.updated_at",
                    (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, sid, updated_at),
                )

    def get_payload(self, blob_key: str, profile: str, shard_id: int | None) -> bytes | None:
        if self.mode == "sqlite_single":
            with self.txn(write=False) as conn:
                row = conn.execute(
                    "SELECT data FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                ).fetchone()
                return None if not row else row[0]
        sid = int(shard_id) if shard_id is not None else None
        if sid is None:
            return None
        with self.txn(write=False, attach_shard_id=sid) as conn:
            row = conn.execute(
                "SELECT data FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            ).fetchone()
            return None if not row else row[0]

    def iter_encodings(self, limit: int | None = None):
        with self.txn(write=False) as conn:
            sql = "SELECT * FROM encodings ORDER BY blob_key, profile"
            if limit is not None:
                sql += " LIMIT ?"
                rows = conn.execute(sql, (limit,)).fetchall()
            else:
                rows = conn.execute(sql).fetchall()
        for row in rows:
            yield row

    def put_dict(self, dict_id: str, codec: str, dict_bytes: bytes, params_json: str | None, created_at: str) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO dicts(dict_id, codec, dict_bytes, params_json, created_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(dict_id) DO UPDATE SET codec=excluded.codec, dict_bytes=excluded.dict_bytes, params_json=excluded.params_json",
                (dict_id, codec, dict_bytes, params_json, created_at),
            )

    def get_dict(self, dict_id: str) -> bytes | None:
        with self.txn(write=False) as conn:
            row = conn.execute("SELECT dict_bytes FROM dicts WHERE dict_id=?", (dict_id,)).fetchone()
            return None if not row else row[0]

    def create_novel(self, title: str, meta: dict | None = None) -> int:
        now = self.now()
        with self.txn(write=True) as conn:
            cur = conn.execute(
                "INSERT INTO novels(title, created_at, updated_at) VALUES(?, ?, ?)",
                (title, now, now),
            )
            novel_id = int(cur.lastrowid)
        if meta:
            for k, v in meta.items():
                self.meta_set("novel", novel_id, str(k), v)
        return novel_id

    def list_novels(self, search: str = "") -> list[dict]:
        with self.txn(write=False) as conn:
            if search:
                rows = conn.execute(
                    "SELECT * FROM novels WHERE title LIKE ? ORDER BY id DESC",
                    (f"%{search}%",),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM novels ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    def upsert_chapter(
        self,
        novel_id: int,
        order_key: str,
        blob_key: str,
        profile: str,
        media_type: str | None,
        charset: str | None,
        meta: dict | None = None,
    ) -> int:
        now = self.now()
        with self.txn(write=True) as conn:
            existing = conn.execute(
                "SELECT id FROM chapters WHERE novel_id=? AND order_key=?",
                (novel_id, order_key),
            ).fetchone()
            if existing:
                chapter_id = int(existing[0])
                conn.execute(
                    "UPDATE chapters SET blob_key=?, profile=?, media_type=?, charset=?, updated_at=? WHERE id=?",
                    (blob_key, profile, media_type, charset, now, chapter_id),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO chapters(novel_id, order_key, blob_key, profile, media_type, charset, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                    (novel_id, order_key, blob_key, profile, media_type, charset, now, now),
                )
                chapter_id = int(cur.lastrowid)
        if meta:
            for k, v in meta.items():
                self.meta_set("chapter", chapter_id, str(k), v)
        return chapter_id

    def get_chapter(self, chapter_id: int):
        with self.txn(write=False) as conn:
            row = conn.execute("SELECT * FROM chapters WHERE id=?", (chapter_id,)).fetchone()
            return row

    def _normalize_entity_id(self, entity_id) -> str:
        if isinstance(entity_id, int):
            return str(entity_id)
        s = str(entity_id)
        if not s.isdigit():
            raise ValueError("entity_id must be decimal integer string")
        return s

    def meta_set(self, entity_type: str, entity_id, key: str, value) -> None:
        eid = self._normalize_entity_id(entity_id)
        now = self.now()
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO meta(entity_type, entity_id, key, value_json, updated_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
                (entity_type, eid, key, _json_dumps(value), now),
            )

    def meta_get(self, entity_type: str, entity_id, key: str):
        eid = self._normalize_entity_id(entity_id)
        with self.txn(write=False) as conn:
            row = conn.execute(
                "SELECT value_json FROM meta WHERE entity_type=? AND entity_id=? AND key=?",
                (entity_type, eid, key),
            ).fetchone()
            return None if not row else json.loads(row[0])

    def meta_list(self, entity_type: str, entity_id) -> dict:
        eid = self._normalize_entity_id(entity_id)
        with self.txn(write=False) as conn:
            rows = conn.execute(
                "SELECT key, value_json FROM meta WHERE entity_type=? AND entity_id=?",
                (entity_type, eid),
            ).fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    def iter_chapter_refs(self, scope=None):
        del scope
        with self.txn(write=False) as conn:
            rows = conn.execute("SELECT DISTINCT blob_key, profile FROM chapters").fetchall()
        for row in rows:
            yield ContentRef(blob_key=row[0], profile=row[1])

    def delete_encoding_and_payload_group(self, rows: list[tuple[str, str, int | None]]) -> tuple[int, int]:
        if not rows:
            return 0, 0
        enc_deleted = 0
        payload_deleted = 0
        by_shard: dict[int | None, list[tuple[str, str]]] = {}
        for blob_key, profile, shard_id in rows:
            by_shard.setdefault(shard_id, []).append((blob_key, profile))

        if self.mode == "sqlite_single":
            pairs = by_shard.get(None, [])
            with self.txn(write=True) as conn:
                for blob_key, profile in pairs:
                    payload_deleted += conn.execute(
                        "DELETE FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
                    enc_deleted += conn.execute(
                        "DELETE FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
            return enc_deleted, payload_deleted

        for sid, pairs in by_shard.items():
            if sid is None:
                continue
            with self.txn(write=True, attach_shard_id=sid) as conn:
                for blob_key, profile in pairs:
                    payload_deleted += conn.execute(
                        "DELETE FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
                    enc_deleted += conn.execute(
                        "DELETE FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
        return enc_deleted, payload_deleted

    def delete_orphan_blobs(self) -> int:
        with self.txn(write=True) as conn:
            return conn.execute(
                "DELETE FROM blobs WHERE blob_key NOT IN (SELECT DISTINCT blob_key FROM encodings)"
            ).rowcount

    def delete_unreferenced_dicts(self) -> int:
        with self.txn(write=True) as conn:
            return conn.execute(
                "DELETE FROM dicts WHERE dict_id NOT IN (SELECT DISTINCT zstd_dict_id FROM encodings WHERE zstd_dict_id IS NOT NULL)"
            ).rowcount

    def list_dead_encodings(self, live_refs: Iterable[ContentRef]) -> list[tuple[str, str, int | None]]:
        live = {(r.blob_key, r.profile) for r in live_refs}
        out: list[tuple[str, str, int | None]] = []
        with self.txn(write=False) as conn:
            rows = conn.execute("SELECT blob_key, profile, shard_id FROM encodings").fetchall()
        for row in rows:
            pair = (row[0], row[1])
            if pair not in live:
                out.append((row[0], row[1], row[2]))
        return out

    def vacuum_index(self) -> str:
        with _connect(self.index_path) as conn:
            conn.execute("VACUUM")
        return str(self.index_path)

    def vacuum_shard(self, shard_id: int) -> str:
        path = self.shard_path(shard_id)
        with _connect(path) as conn:
            conn.execute("VACUUM")
        return str(path)

    def payload_exists(self, blob_key: str, profile: str, shard_id: int | None) -> bool:
        return self.get_payload(blob_key, profile, shard_id) is not None

    def clear_payload(self, blob_key: str, profile: str, shard_id: int | None) -> None:
        if self.mode == "sqlite_single":
            with self.txn(write=True) as conn:
                conn.execute("DELETE FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile))
            return
        if shard_id is None:
            raise MissingContent("missing shard_id for sharded payload")
        with self.txn(write=True, attach_shard_id=shard_id) as conn:
            conn.execute("DELETE FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile))

