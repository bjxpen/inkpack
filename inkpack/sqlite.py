"""SQLite persistence: schema, forward-only migrations, and the unified backend.

This module is the only place that knows SQL. It implements both backends
(``sqlite_single`` and ``sqlite_sharded``) behind one :class:`SqliteBackend`,
enforces the PRAGMA policy from spec 8, keeps every multi-row write atomic
(spec 7.5, 8.2, decision G), and translates sqlite busy/locked errors into
:class:`~inkpack.types.Busy` for writer operations (decision F).
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from .codec import canonical_json
from .types import Busy, Clock, ContentRef, InkpackError, MissingContent, NotFound

LATEST_USER_VERSION = 1

_SCHEMA_INDEX_V1 = """
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
  novel_id INTEGER NOT NULL REFERENCES novels(id),
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_chapters_novel_order ON chapters(novel_id, order_key);
CREATE INDEX IF NOT EXISTS idx_encodings_zstd_dict_id ON encodings(zstd_dict_id);
"""

_SCHEMA_PAYLOAD_V1 = """
CREATE TABLE IF NOT EXISTS payload(
  blob_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(blob_key, profile)
);
"""

# Forward-only, idempotent migrations (spec 7.8).
# Index migrations are tracked in `PRAGMA user_version` (normative). Payload
# migrations are tracked in a private single-row table because index and
# payload schemas share one file in sqlite_single mode, and a single shared
# counter would make the two chains order-dependent.
MIGRATIONS: tuple[tuple[int, str], ...] = ((1, _SCHEMA_INDEX_V1),)
PAYLOAD_MIGRATIONS: tuple[tuple[int, str], ...] = ((1, _SCHEMA_PAYLOAD_V1),)

_PAYLOAD_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS _inkpack_schema(
  key TEXT PRIMARY KEY,
  version INTEGER NOT NULL
);
"""

_UPSERT_ENCODING_SINGLE = """
INSERT INTO encodings(
  blob_key, profile, codec, codec_params_json, zstd_dict_id,
  stored_len, checksum, shard_id, updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
ON CONFLICT(blob_key, profile) DO UPDATE SET
  codec=excluded.codec,
  codec_params_json=excluded.codec_params_json,
  zstd_dict_id=excluded.zstd_dict_id,
  stored_len=excluded.stored_len,
  checksum=excluded.checksum,
  shard_id=NULL,
  updated_at=excluded.updated_at
"""

_UPSERT_ENCODING_SHARDED = """
INSERT INTO encodings(
  blob_key, profile, codec, codec_params_json, zstd_dict_id,
  stored_len, checksum, shard_id, updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(blob_key, profile) DO UPDATE SET
  codec=excluded.codec,
  codec_params_json=excluded.codec_params_json,
  zstd_dict_id=excluded.zstd_dict_id,
  stored_len=excluded.stored_len,
  checksum=excluded.checksum,
  shard_id=excluded.shard_id,
  updated_at=excluded.updated_at
"""

_UPSERT_CHAPTER = """
INSERT INTO chapters(
  novel_id, order_key, blob_key, profile, media_type, charset, created_at, updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(novel_id, order_key) DO UPDATE SET
  blob_key=excluded.blob_key,
  profile=excluded.profile,
  media_type=excluded.media_type,
  charset=excluded.charset,
  updated_at=excluded.updated_at
"""

_VALID_SYNC_MODES = {"OFF", "NORMAL", "FULL"}
_ENTITY_TYPES = ("novel", "chapter")
_BUSY_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


class SystemClock:
    """Default :class:`Clock`: UTC ISO-8601 timestamps."""

    def now_iso(self) -> str:
        return datetime.now(UTC).isoformat()


def _normalize_pragmas(pragmas: dict[str, Any] | None) -> dict[str, Any]:
    """Validate the user-facing pragma knobs and return a normalized dict."""
    pragmas = dict(pragmas or {})
    unknown = set(pragmas) - {"synchronous", "busy_timeout_ms"}
    if unknown:
        raise ValueError(f"unknown pragmas: {sorted(unknown)}")
    synchronous = str(pragmas.get("synchronous", "NORMAL")).upper()
    if synchronous not in _VALID_SYNC_MODES:
        raise ValueError(f"invalid synchronous mode: {synchronous!r} (expected OFF, NORMAL or FULL)")
    busy_timeout_ms = pragmas.get("busy_timeout_ms", 5000)
    if not isinstance(busy_timeout_ms, int) or isinstance(busy_timeout_ms, bool) or busy_timeout_ms < 0:
        raise ValueError("busy_timeout_ms must be a non-negative integer")
    return {"synchronous": synchronous, "busy_timeout_ms": busy_timeout_ms}


def _connect(db_path: Path, busy_timeout_ms: int) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=max(busy_timeout_ms / 1000.0, 0.001))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
    return conn


def _require_positive_int(value: Any, label: str) -> int:
    """Runtime validation of caller-supplied integers (config values)."""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _require_scope(scope: Any) -> int | None:
    """Runtime validation of the ``iter_live_content`` scope argument."""
    if scope is None:
        return None
    if not isinstance(scope, int) or isinstance(scope, bool):
        raise TypeError("scope must be an int novel id or None")
    return scope


def _is_busy(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code in _BUSY_CODES:
        return True
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def migrate_index(conn: sqlite3.Connection) -> None:
    current = int(conn.execute("PRAGMA user_version").fetchone()[0])
    for version, script in MIGRATIONS:
        if version <= current:
            continue
        conn.executescript(script)
        conn.execute(f"PRAGMA user_version={version}")


def _payload_version(conn: sqlite3.Connection) -> int:
    conn.execute(_PAYLOAD_VERSION_TABLE)
    row = conn.execute("SELECT version FROM _inkpack_schema WHERE key='payload'").fetchone()
    return int(row[0]) if row is not None else 0


def migrate_payload(conn: sqlite3.Connection) -> None:
    current = _payload_version(conn)
    for version, script in PAYLOAD_MIGRATIONS:
        if version <= current:
            continue
        conn.executescript(script)
        conn.execute(
            "INSERT INTO _inkpack_schema(key, version) VALUES('payload', ?) "
            "ON CONFLICT(key) DO UPDATE SET version=excluded.version",
            (version,),
        )


def _ensure_shard_file(payload_dir: Path, shard_id: int, busy_timeout_ms: int) -> Path:
    path = payload_dir / f"shard-{shard_id:04d}.sqlite"
    if not path.exists():
        conn = _connect(path, busy_timeout_ms)
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
            migrate_payload(conn)
        finally:
            conn.close()
    return path


@dataclass
class SqliteBackend:
    """Unified backend for the ``sqlite_single`` and ``sqlite_sharded`` layouts."""

    root: Path
    mode: str
    index_path: Path
    payload_dir: Path | None
    shard_cap_bytes: int
    shard_min_bytes: int
    clock: Clock
    busy_timeout_ms: int
    synchronous: str

    # -- construction -------------------------------------------------------

    @classmethod
    def open(
        cls,
        path: str | os.PathLike[str],
        mode: str,
        pragmas: dict[str, Any] | None = None,
        shard_cap_bytes: int = 2 << 30,
        shard_min_bytes: int = 256 << 20,
        clock: Clock | None = None,
    ) -> SqliteBackend:
        if mode not in ("sqlite_single", "sqlite_sharded"):
            raise ValueError(f"mode must be 'sqlite_single' or 'sqlite_sharded', got {mode!r}")
        shard_cap_bytes = _require_positive_int(shard_cap_bytes, "shard_cap_bytes")
        shard_min_bytes = _require_positive_int(shard_min_bytes, "shard_min_bytes")
        if shard_min_bytes > shard_cap_bytes:
            raise ValueError("shard_min_bytes must not exceed shard_cap_bytes")

        pragmas = _normalize_pragmas(pragmas)
        clock = clock or SystemClock()
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)

        # Journal mode is a creation-time policy (WAL for single, rollback for
        # sharded): we only set it on fresh databases, then always validate it
        # below so that an existing repo with a tampered journal mode is
        # refused instead of silently rewritten.
        if mode == "sqlite_single":
            index_path = root / "repo.sqlite"
            payload_dir = None
            fresh = not index_path.exists()
            conn = _connect(index_path, pragmas["busy_timeout_ms"])
            try:
                if fresh:
                    conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(f"PRAGMA synchronous={pragmas['synchronous']}")
                migrate_index(conn)
                migrate_payload(conn)
            finally:
                conn.close()
        else:
            index_path = root / "index.sqlite"
            payload_dir = root / "payload"
            payload_dir.mkdir(parents=True, exist_ok=True)
            fresh = not index_path.exists()
            conn = _connect(index_path, pragmas["busy_timeout_ms"])
            try:
                if fresh:
                    conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute(f"PRAGMA synchronous={pragmas['synchronous']}")
                migrate_index(conn)
            finally:
                conn.close()
            _ensure_shard_file(payload_dir, 1, pragmas["busy_timeout_ms"])

        backend = cls(
            root=root,
            mode=mode,
            index_path=index_path,
            payload_dir=payload_dir,
            shard_cap_bytes=shard_cap_bytes,
            shard_min_bytes=shard_min_bytes,
            clock=clock,
            busy_timeout_ms=pragmas["busy_timeout_ms"],
            synchronous=pragmas["synchronous"],
        )
        backend._validate_journal_modes()
        return backend

    def _validate_journal_modes(self) -> None:
        if self.mode != "sqlite_sharded":
            return
        dbs: list[tuple[str, Path]] = [("index", self.index_path)]
        dbs.extend((f"shard {sid}", self.shard_path(sid)) for sid in self.list_shards())
        for label, path in dbs:
            with _connect(path, self.busy_timeout_ms) as conn:
                journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            if journal_mode not in {"delete", "truncate"}:
                raise InkpackError(f"{label} DB must use rollback journal mode, found {journal_mode!r}")

    # -- connections --------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        return _connect(self.index_path, self.busy_timeout_ms)

    @contextmanager
    def txn(
        self,
        write: bool = False,
        attach_shard_id: int | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Open a transaction on the index DB, optionally attaching one shard.

        Sharded writes that touch index + payload use ``ATTACH`` so the commit
        is atomic across both files (spec 8.2, decision G).
        """
        conn = self._connect()
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            if self.mode == "sqlite_sharded" and attach_shard_id is not None:
                conn.execute("ATTACH DATABASE ? AS p", (str(self.shard_path(attach_shard_id)),))
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise
        except BaseException:
            conn.rollback()
            raise
        finally:
            try:
                if self.mode == "sqlite_sharded" and attach_shard_id is not None:
                    conn.execute("DETACH DATABASE p")
            except sqlite3.Error:
                pass
            conn.close()

    def now(self) -> str:
        return self.clock.now_iso()

    # -- repo config --------------------------------------------------------

    def config_get(self, key: str) -> Any:
        row = self._query_one("SELECT value_json FROM repo_config WHERE key=?", (key,))
        return None if row is None else json.loads(str(row[0]))

    def config_set(self, key: str, obj: Any) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO repo_config(key, value_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, canonical_json(obj)),
            )

    # -- small query helpers (single source for txn handling) ---------------

    def _query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self.txn(write=False) as conn:
            row = conn.execute(sql, params).fetchone()
            return cast("sqlite3.Row | None", row)

    def _query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.txn(write=False) as conn:
            return cast("list[sqlite3.Row]", conn.execute(sql, params).fetchall())

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """Run a write statement and return its rowcount."""
        with self.txn(write=True) as conn:
            return int(conn.execute(sql, params).rowcount)

    @staticmethod
    def _payload_table(attached: bool) -> str:
        return "p.payload" if attached else "payload"

    # -- content tables -----------------------------------------------------

    def get_blob(self, blob_key: str) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM blobs WHERE blob_key=?", (blob_key,))

    def get_encoding(self, blob_key: str, profile: str) -> sqlite3.Row | None:
        return self._query_one(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
        )

    def store_encoding_and_payload(
        self,
        *,
        blob_key: str,
        raw_len: int,
        created_at: str,
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
        """Atomically write the blob row (if missing) plus encoding + payload rows.

        This is the single write path used by ``put`` and ``reencode`` so that
        ``encodings.stored_len == len(payload.data)`` and the blob/encoding/
        payload triple can never be half-committed.
        """
        attached = self.mode == "sqlite_sharded"
        payload_table = self._payload_table(attached)
        if not attached:
            with self.txn(write=True) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO blobs(blob_key, raw_len, created_at) VALUES(?, ?, ?)",
                    (blob_key, raw_len, created_at),
                )
                conn.execute(
                    f"INSERT INTO {payload_table}(blob_key, profile, data) VALUES(?, ?, ?) "
                    "ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                    (blob_key, profile, payload),
                )
                conn.execute(
                    _UPSERT_ENCODING_SINGLE,
                    (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, updated_at),
                )
            return
        sid = shard_id if shard_id is not None else self.choose_shard_for_write(stored_len)
        self.ensure_shard_exists(sid)
        with self.txn(write=True, attach_shard_id=sid) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO blobs(blob_key, raw_len, created_at) VALUES(?, ?, ?)",
                (blob_key, raw_len, created_at),
            )
            conn.execute(
                f"INSERT INTO {payload_table}(blob_key, profile, data) VALUES(?, ?, ?) "
                "ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                (blob_key, profile, payload),
            )
            conn.execute(
                _UPSERT_ENCODING_SHARDED,
                (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, sid, updated_at),
            )

    def get_payload(self, blob_key: str, profile: str, shard_id: int | None) -> bytes | None:
        if self.mode == "sqlite_single":
            row = self._query_one(
                "SELECT data FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return None if row is None else bytes(row[0])
        if shard_id is None:
            return None
        sid = int(shard_id)
        with self.txn(write=False, attach_shard_id=sid) as conn:
            row = conn.execute(
                "SELECT data FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            ).fetchone()
            return None if row is None else bytes(row[0])

    def payload_exists(self, blob_key: str, profile: str, shard_id: int | None) -> bool:
        return self.get_payload(blob_key, profile, shard_id) is not None

    def clear_payload(self, blob_key: str, profile: str, shard_id: int | None) -> None:
        """Delete a payload row (test/troubleshooting helper; leaves the encoding row)."""
        if self.mode == "sqlite_single":
            self._execute(
                "DELETE FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return
        if shard_id is None:
            raise MissingContent(f"missing shard_id for payload of {(blob_key, profile)!r}")
        with self.txn(write=True, attach_shard_id=shard_id) as conn:
            conn.execute(
                "DELETE FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )

    def iter_encodings(self, limit: int | None = None) -> Iterator[sqlite3.Row]:
        sql = "SELECT * FROM encodings ORDER BY blob_key, profile"
        rows = self._query_all(sql + " LIMIT ?", (limit,)) if limit is not None else self._query_all(sql)
        yield from rows

    # -- dictionaries -------------------------------------------------------

    def put_dict(self, dict_id: str, codec: str, dict_bytes: bytes, params_json: str | None, created_at: str) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO dicts(dict_id, codec, dict_bytes, params_json, created_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(dict_id) DO UPDATE SET "
                "codec=excluded.codec, dict_bytes=excluded.dict_bytes, params_json=excluded.params_json",
                (dict_id, codec, dict_bytes, params_json, created_at),
            )

    def get_dict(self, dict_id: str) -> bytes | None:
        row = self._query_one("SELECT dict_bytes FROM dicts WHERE dict_id=?", (dict_id,))
        return None if row is None else bytes(row[0])

    # -- repository tables --------------------------------------------------

    def create_novel(self, title: str, meta: dict[str, Any] | None = None) -> int:
        now = self.now()
        with self.txn(write=True) as conn:
            cur = conn.execute(
                "INSERT INTO novels(title, created_at, updated_at) VALUES(?, ?, ?)", (title, now, now)
            )
            lastrowid = cur.lastrowid
            novel_id = int(lastrowid) if lastrowid else 0
            if not novel_id:
                raise InkpackError("failed to create novel: no row id returned")
        if meta:
            for key, value in meta.items():
                self.meta_set("novel", novel_id, str(key), value)
        return novel_id

    def list_novels(self, search: str = "") -> list[dict[str, Any]]:
        if search:
            rows = self._query_all(
                "SELECT * FROM novels WHERE title LIKE ? ORDER BY id DESC", (f"%{search}%",)
            )
        else:
            rows = self._query_all("SELECT * FROM novels ORDER BY id DESC")
        return [dict(row) for row in rows]

    def upsert_chapter(
        self,
        novel_id: int,
        order_key: str,
        blob_key: str,
        profile: str,
        media_type: str | None,
        charset: str | None,
        meta: dict[str, Any] | None = None,
    ) -> int:
        """Insert or update the chapter keyed by ``(novel_id, order_key)``.

        Atomic upsert via the unique index; returns the chapter id either way.
        """
        now = self.now()
        try:
            with self.txn(write=True) as conn:
                cur = conn.execute(
                    _UPSERT_CHAPTER,
                    (int(novel_id), order_key, blob_key, profile, media_type, charset, now, now),
                )
                lastrowid = cur.lastrowid
                chapter_id = int(lastrowid) if lastrowid else 0
                if not chapter_id:
                    row = conn.execute(
                        "SELECT id FROM chapters WHERE novel_id=? AND order_key=?",
                        (int(novel_id), order_key),
                    ).fetchone()
                    if row is None:
                        raise NotFound(f"novel {novel_id} not found")
                    chapter_id = int(row[0])
        except sqlite3.IntegrityError as exc:
            raise NotFound(f"novel {novel_id} not found") from exc
        if meta:
            for key, value in meta.items():
                self.meta_set("chapter", chapter_id, str(key), value)
        return chapter_id

    def get_chapter(self, chapter_id: int) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM chapters WHERE id=?", (int(chapter_id),))

    # -- KV metadata (spec 7.2) ---------------------------------------------

    @staticmethod
    def _normalize_entity_id(entity_id: int | str) -> str:
        if isinstance(entity_id, int) and not isinstance(entity_id, bool):
            return str(entity_id)
        if isinstance(entity_id, str) and entity_id.isdigit() and entity_id == str(int(entity_id)):
            return entity_id
        raise ValueError(
            f"entity_id must be the decimal string of an integer primary key, got {entity_id!r}"
        )

    @classmethod
    def _normalize_entity(cls, entity_type: str, entity_id: int | str) -> tuple[str, str]:
        if entity_type not in _ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {_ENTITY_TYPES}, got {entity_type!r}")
        return entity_type, cls._normalize_entity_id(entity_id)

    def meta_set(self, entity_type: str, entity_id: int | str, key: str, value: Any) -> None:
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        now = self.now()
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO meta(entity_type, entity_id, key, value_json, updated_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET "
                "value_json=excluded.value_json, updated_at=excluded.updated_at",
                (entity_type, entity_id, str(key), canonical_json(value), now),
            )

    def meta_get(self, entity_type: str, entity_id: int | str, key: str) -> Any:
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        row = self._query_one(
            "SELECT value_json FROM meta WHERE entity_type=? AND entity_id=? AND key=?",
            (entity_type, entity_id, str(key)),
        )
        return None if row is None else json.loads(str(row[0]))

    def meta_list(self, entity_type: str, entity_id: int | str) -> dict[str, Any]:
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        rows = self._query_all(
            "SELECT key, value_json FROM meta WHERE entity_type=? AND entity_id=?",
            (entity_type, entity_id),
        )
        return {str(row[0]): json.loads(str(row[1])) for row in rows}

    def iter_chapter_refs(self, scope: int | None = None) -> Iterator[ContentRef]:
        """Distinct ``(blob_key, profile)`` pairs referenced by chapters.

        ``scope`` is an optional novel id; ``None`` means the whole repository.
        """
        scope = _require_scope(scope)
        if scope is None:
            sql = "SELECT DISTINCT blob_key, profile FROM chapters"
            params: tuple[Any, ...] = ()
        else:
            sql = "SELECT DISTINCT blob_key, profile FROM chapters WHERE novel_id=?"
            params = (scope,)
        rows = self._query_all(sql, params)
        for row in rows:
            yield ContentRef(blob_key=str(row["blob_key"]), profile=str(row["profile"]))

    # -- maintenance (GC / vacuum) -------------------------------------------

    def list_dead_encodings(self, live_refs: Iterable[ContentRef]) -> list[tuple[str, str, int | None]]:
        """Encodings whose ``(blob_key, profile)`` is not in the live set.

        The live set is staged in a temporary table so it never has to be
        materialized in Python memory.
        """
        with self.txn(write=False) as conn:
            conn.execute(
                "CREATE TEMP TABLE temp_live(blob_key TEXT NOT NULL, profile TEXT NOT NULL, "
                "PRIMARY KEY(blob_key, profile))"
            )
            try:
                conn.executemany(
                    "INSERT OR IGNORE INTO temp_live(blob_key, profile) VALUES(?, ?)",
                    ((ref.blob_key, ref.profile) for ref in live_refs),
                )
                rows = conn.execute(
                    "SELECT e.blob_key, e.profile, e.shard_id FROM encodings e "
                    "WHERE NOT EXISTS ("
                    "  SELECT 1 FROM temp_live l WHERE l.blob_key = e.blob_key AND l.profile = e.profile"
                    ") ORDER BY e.blob_key, e.profile"
                ).fetchall()
            finally:
                conn.execute("DROP TABLE temp_live")
        return [(str(row[0]), str(row[1]), row[2]) for row in rows]

    def delete_encoding_and_payload_group(self, rows: list[tuple[str, str, int | None]]) -> tuple[int, int]:
        """Delete encoding + payload rows for dead refs, atomically per shard."""
        if not rows:
            return 0, 0
        encodings_deleted = 0
        payload_rows_deleted = 0
        by_shard: dict[int | None, list[tuple[str, str]]] = {}
        for blob_key, profile, shard_id in rows:
            by_shard.setdefault(shard_id, []).append((blob_key, profile))

        if self.mode == "sqlite_single":
            with self.txn(write=True) as conn:
                for blob_key, profile in by_shard.get(None, []):
                    payload_rows_deleted += conn.execute(
                        "DELETE FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
                    encodings_deleted += conn.execute(
                        "DELETE FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
            return encodings_deleted, payload_rows_deleted

        for shard_id, pairs in by_shard.items():
            if shard_id is None:
                # Defensive: an encoding without a shard locator in sharded mode;
                # delete the index row only (payload cannot be located).
                with self.txn(write=True) as conn:
                    for blob_key, profile in pairs:
                        encodings_deleted += conn.execute(
                            "DELETE FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
                        ).rowcount
                continue
            with self.txn(write=True, attach_shard_id=shard_id) as conn:
                for blob_key, profile in pairs:
                    payload_rows_deleted += conn.execute(
                        "DELETE FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
                    encodings_deleted += conn.execute(
                        "DELETE FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
                    ).rowcount
        return encodings_deleted, payload_rows_deleted

    def delete_orphan_blobs(self) -> int:
        return self._execute(
            "DELETE FROM blobs WHERE NOT EXISTS ("
            "SELECT 1 FROM encodings e WHERE e.blob_key = blobs.blob_key)"
        )

    def delete_unreferenced_dicts(self) -> int:
        return self._execute(
            "DELETE FROM dicts WHERE NOT EXISTS ("
            "SELECT 1 FROM encodings e WHERE e.zstd_dict_id = dicts.dict_id)"
        )

    def vacuum_index(self) -> str:
        return self._vacuum(self.index_path)

    def vacuum_shard(self, shard_id: int) -> str:
        return self._vacuum(self.shard_path(shard_id))

    def _vacuum(self, path: Path) -> str:
        # VACUUM must run on a connection with no ATTACHed databases (decision H).
        try:
            with _connect(path, self.busy_timeout_ms) as conn:
                conn.execute("VACUUM")
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise
        return str(path)

    # -- shard routing -------------------------------------------------------

    def shard_path(self, shard_id: int) -> Path:
        if self.payload_dir is None:
            raise ValueError("sqlite_single mode has no shards")
        return self.payload_dir / f"shard-{shard_id:04d}.sqlite"

    def list_shards(self) -> list[int]:
        if self.payload_dir is None:
            return []
        shard_ids: list[int] = []
        for path in sorted(self.payload_dir.glob("shard-*.sqlite")):
            shard_ids.append(int(path.stem.split("-")[1]))
        return shard_ids

    def ensure_shard_exists(self, shard_id: int) -> None:
        if self.payload_dir is None:
            return
        _ensure_shard_file(self.payload_dir, shard_id, self.busy_timeout_ms)

    def choose_shard_for_write(self, estimated_payload_bytes: int) -> int:
        """Pick the current (last) shard, rolling to a fresh one past the cap.

        The cap is enforced against the on-disk shard file size, which tracks
        the stored payload volume closely enough for the 2 GiB default.
        """
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
