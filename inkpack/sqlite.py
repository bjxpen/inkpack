"""SQLite persistence: schema, forward-only migrations, and the unified backend.

This module is the only place that knows SQL. It implements both backends
(``sqlite_single`` and ``sqlite_sharded``) behind one :class:`SqliteBackend`,
enforces the PRAGMA policy from spec 8 on *every* connection, keeps every
multi-row write atomic (spec 7.5, 8.2, decision G), translates sqlite
busy/locked errors into :class:`~inkpack.types.Busy` (decision F), and scopes
connection reuse to a single public call / operation (review §13 phase 1).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from .codec import Encoded, canonical_json
from .types import (
    Busy,
    CancelToken,
    Clock,
    ContentRef,
    CorruptContent,
    InkpackError,
    MissingContent,
    NotFound,
    check_cancel,
)

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

_UPSERT_META = """
INSERT INTO meta(entity_type, entity_id, key, value_json, updated_at) VALUES(?, ?, ?, ?, ?)
ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
  value_json=excluded.value_json, updated_at=excluded.updated_at
"""

_VALID_SYNC_MODES = {"OFF", "NORMAL", "FULL"}
_ENTITY_TYPES = ("novel", "chapter")
_BUSY_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})
_GC_BATCH = 1000
_SHARD_FILE_RE = re.compile(r"^shard-([0-9]+)\.sqlite$")


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


def sqlite_uri(db_path: Path, mode: str) -> str:
    """URI for a database file with an explicit open mode.

    Built from ``Path.as_uri()`` so spaces, ``#``, ``%``, ``?`` and other
    characters in paths are percent-encoded (review P0-URI-1) instead of
    being interpolated raw into a ``file:`` string.
    """
    return f"{db_path.resolve().as_uri()}?mode={mode}"


def connect_file(
    db_path: Path,
    busy_timeout_ms: int,
    synchronous: str = "NORMAL",
    *,
    create: bool = False,
) -> sqlite3.Connection:
    """Open a connection with the full per-connection PRAGMA policy (review §2).

    ``journal_mode`` is deliberately NOT set here: it is a creation-time
    policy and must not be silently rewritten on connect.

    ``create=False`` (default) opens in ``mode=rw``: a missing database file
    raises ``OperationalError`` instead of being created, so open/read/compact
    paths can never create SQLite files (review P0-2). Only explicit creation
    paths pass ``create=True`` (``mode=rwc``).
    """
    mode = "rwc" if create else "rw"
    conn = sqlite3.connect(
        sqlite_uri(db_path, mode), uri=True, timeout=max(busy_timeout_ms / 1000.0, 0.001)
    )
    conn.row_factory = sqlite3.Row
    # Autocommit mode: reads never leave an implicit open transaction, so a
    # later explicit BEGIN IMMEDIATE on the same connection is always legal.
    conn.isolation_level = None
    try:
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        conn.execute(f"PRAGMA synchronous={synchronous}")
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.Error as exc:
        conn.close()
        if _is_busy(exc):
            raise Busy(str(exc)) from exc
        raise
    return conn


def _is_busy(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code in _BUSY_CODES:
        return True
    message = str(exc).lower()
    return "locked" in message or "busy" in message


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


def _map_busy(exc: sqlite3.OperationalError) -> None:
    """Translate a sqlite busy/locked error into :class:`Busy` (locked
    semantics S8: open/migrate/validation paths must not leak raw errors)."""
    if _is_busy(exc):
        raise Busy(str(exc)) from exc


def migrate_index(conn: sqlite3.Connection) -> None:
    try:
        current = int(conn.execute("PRAGMA user_version").fetchone()[0])
        for version, script in MIGRATIONS:
            if version <= current:
                continue
            conn.executescript(script)
            conn.execute(f"PRAGMA user_version={version}")
    except sqlite3.OperationalError as exc:
        _map_busy(exc)
        raise


def _payload_version(conn: sqlite3.Connection) -> int:
    conn.execute(_PAYLOAD_VERSION_TABLE)
    row = conn.execute("SELECT version FROM _inkpack_schema WHERE key='payload'").fetchone()
    return int(row[0]) if row is not None else 0


def migrate_payload(conn: sqlite3.Connection) -> None:
    try:
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
    except sqlite3.OperationalError as exc:
        _map_busy(exc)
        raise


def _ensure_shard_file(payload_dir: Path, shard_id: int, busy_timeout_ms: int, synchronous: str) -> Path:
    path = payload_dir / f"shard-{shard_id:04d}.sqlite"
    if not path.exists():
        conn = connect_file(path, busy_timeout_ms, synchronous, create=True)
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA auto_vacuum=NONE")
            migrate_payload(conn)
        finally:
            conn.close()
    return path


# ---------------------------------------------------------------------------
# Operation-scoped connection session (review §13 phase 1)
# ---------------------------------------------------------------------------


class Session:
    """One index connection (plus lazily-opened shard connections) for the
    duration of a single public call or Operation. Closed by ``close()``."""

    def __init__(self, backend: SqliteBackend) -> None:
        self.backend = backend
        self.conn = backend.connect_index()
        self._shard_conns: dict[int, sqlite3.Connection] = {}
        self._missing_shards: set[int] = set()
        self._dict_cache: dict[str, bytes | None] = {}
        self._closed = False

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        try:
            row = self.conn.execute(sql, params).fetchone()
            return cast("sqlite3.Row | None", row)
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        try:
            return cast("list[sqlite3.Row]", self.conn.execute(sql, params).fetchall())
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        try:
            return int(self.conn.execute(sql, params).rowcount)
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise

    def executemany(self, sql: str, seq: Iterable[tuple[Any, ...]]) -> None:
        try:
            self.conn.executemany(sql, seq)
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise

    def config(self, key: str) -> Any:
        row = self.query_one("SELECT value_json FROM repo_config WHERE key=?", (key,))
        return None if row is None else json.loads(str(row[0]))

    def _shard_conn(self, shard_id: int) -> sqlite3.Connection | None:
        """Open (and cache) a shard connection; returns None when the shard
        file is missing — a missing shard behaves like missing content and is
        never created by a read path (review P0-2, P1-SHARD-1).

        Error classification: missing file -> None (negative cache); busy ->
        :class:`Busy`; anything else (e.g. a corrupt shard file) ->
        :class:`CorruptContent` — never a raw sqlite error.
        """
        if shard_id in self._missing_shards:
            return None
        conn = self._shard_conns.get(shard_id)
        if conn is not None:
            return conn
        try:
            conn = connect_file(
                self.backend.shard_path(shard_id), self.backend.busy_timeout_ms, self.backend.synchronous
            )
        except sqlite3.Error as exc:
            message = str(exc).lower()
            if "unable to open database file" in message:
                self._missing_shards.add(shard_id)  # negative cache
                return None
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise CorruptContent(f"shard {shard_id} is present but unusable: {exc}") from exc
        self._shard_conns[shard_id] = conn
        return conn

    def _shard_query_one(self, conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
        try:
            return cast("sqlite3.Row | None", conn.execute(sql, params).fetchone())
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            # A present-but-unreadable shard (e.g. "file is not a database").
            raise CorruptContent(f"shard read failed: {exc}") from exc

    def payload_exists(self, blob_key: str, profile: str, shard_id: int | None) -> bool:
        """Cheap existence probe (``SELECT 1``) — never reads the whole BLOB
        (review P2-2)."""
        if self.backend.mode == "sqlite_single":
            row = self.query_one(
                "SELECT 1 FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return row is not None
        if shard_id is None:
            return False
        conn = self._shard_conn(int(shard_id))
        if conn is None:
            return False
        row = self._shard_query_one(
            conn, "SELECT 1 FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
        )
        return row is not None

    def payload(self, blob_key: str, profile: str, shard_id: int | None) -> bytes | None:
        if self.backend.mode == "sqlite_single":
            row = self.query_one(
                "SELECT data FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return None if row is None else bytes(row[0])
        if shard_id is None:
            return None
        conn = self._shard_conn(int(shard_id))
        if conn is None:
            return None
        row = self._shard_query_one(
            conn, "SELECT data FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
        )
        return None if row is None else bytes(row[0])

    def dict_bytes(self, dict_id: str) -> bytes | None:
        if dict_id not in self._dict_cache:
            row = self.query_one("SELECT dict_bytes FROM dicts WHERE dict_id=?", (dict_id,))
            self._dict_cache[dict_id] = None if row is None else bytes(row[0])
        return self._dict_cache[dict_id]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for conn in self._shard_conns.values():
            conn.close()
        self._shard_conns.clear()
        self._missing_shards.clear()
        self.conn.close()


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


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
    _blob_limit: int | None = field(default=None, init=False)
    _write_shard_id: int | None = field(default=None, init=False)

    # -- construction -------------------------------------------------------

    @classmethod
    def create(
        cls,
        path: str | os.PathLike[str],
        mode: str,
        pragmas: dict[str, Any] | None = None,
        shard_cap_bytes: int = 2 << 30,
        shard_min_bytes: int = 256 << 20,
        clock: Clock | None = None,
    ) -> SqliteBackend:
        """Create a brand-new repository database (never opens existing files)."""
        return cls.open(
            path,
            mode,
            pragmas=pragmas,
            shard_cap_bytes=shard_cap_bytes,
            shard_min_bytes=shard_min_bytes,
            clock=clock,
            create=True,
        )

    @classmethod
    def open(
        cls,
        path: str | os.PathLike[str],
        mode: str,
        pragmas: dict[str, Any] | None = None,
        shard_cap_bytes: int = 2 << 30,
        shard_min_bytes: int = 256 << 20,
        clock: Clock | None = None,
        *,
        create: bool = False,
    ) -> SqliteBackend:
        """Open an existing repository, or create one when ``create=True``.

        With ``create=False`` (the default) a missing repository raises
        :class:`NotFound` and **no files or directories are created** (review
        P0-2). With ``create=True`` an existing repository is refused (review
        P0-6); ``create`` never opens/migrates an existing DB.
        """
        if mode not in ("sqlite_single", "sqlite_sharded"):
            raise ValueError(f"mode must be 'sqlite_single' or 'sqlite_sharded', got {mode!r}")
        shard_cap_bytes = _require_positive_int(shard_cap_bytes, "shard_cap_bytes")
        shard_min_bytes = _require_positive_int(shard_min_bytes, "shard_min_bytes")
        if shard_min_bytes > shard_cap_bytes:
            raise ValueError("shard_min_bytes must not exceed shard_cap_bytes")

        pragmas = _normalize_pragmas(pragmas)
        clock = clock or SystemClock()
        root = Path(path)
        index_name = "index.sqlite" if mode == "sqlite_sharded" else "repo.sqlite"
        index_path = root / index_name
        if create and index_path.exists():
            raise InkpackError(f"repository already exists at {root}")
        if not create and not index_path.exists():
            raise NotFound(f"inkpack repository not found at {root} (missing {index_name})")
        if create:
            root.mkdir(parents=True, exist_ok=True)

        # Journal mode and auto_vacuum are creation-time policies: set only on
        # fresh databases, then always validate journal mode below so a
        # tampered repo is refused instead of silently rewritten.
        if mode == "sqlite_single":
            payload_dir = None
            conn = connect_file(
                index_path, pragmas["busy_timeout_ms"], pragmas["synchronous"], create=create
            )
            try:
                if create:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA auto_vacuum=NONE")
                migrate_index(conn)
                migrate_payload(conn)
            finally:
                conn.close()
        else:
            payload_dir = root / "payload"
            if create:
                payload_dir.mkdir(parents=True, exist_ok=True)
            elif not payload_dir.is_dir():
                raise NotFound(f"inkpack repository payload directory missing at {payload_dir}")
            conn = connect_file(
                index_path, pragmas["busy_timeout_ms"], pragmas["synchronous"], create=create
            )
            try:
                if create:
                    conn.execute("PRAGMA journal_mode=DELETE")
                    conn.execute("PRAGMA auto_vacuum=NONE")
                migrate_index(conn)
            finally:
                conn.close()
            if create:
                _ensure_shard_file(payload_dir, 1, pragmas["busy_timeout_ms"], pragmas["synchronous"])

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
            # sqlite3.Connection's context manager commits/rolls back but does
            # NOT close: close explicitly (review P0-CLOSE-1).
            conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
            try:
                journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            except sqlite3.OperationalError as exc:
                _map_busy(exc)
                raise InkpackError(f"{label} DB is not a valid sqlite database: {exc}") from exc
            finally:
                conn.close()
            if journal_mode not in {"delete", "truncate"}:
                raise InkpackError(f"{label} DB must use rollback journal mode, found {journal_mode!r}")

    # -- connections / transactions ------------------------------------------

    def connect_index(self) -> sqlite3.Connection:
        return connect_file(self.index_path, self.busy_timeout_ms, self.synchronous)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """One index connection (plus per-shard read connections) for a call."""
        session = Session(self)
        try:
            yield session
        finally:
            session.close()

    @contextmanager
    def txn_on(
        self,
        conn: sqlite3.Connection,
        write: bool = False,
        attach_shard_id: int | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Run a transaction on an existing connection (used by ``session``).

        Sharded ATTACH is **require-mode** (locked semantics S2): the shard
        file must already exist (checked explicitly) and is attached with
        ``mode=rw`` so SQLite itself refuses to create it. A missing shard is
        :class:`MissingContent`; a present-but-unusable shard is
        :class:`CorruptContent`; busy maps to :class:`Busy`.
        """
        attached = False
        shard_path: Path | None = None
        if self.mode == "sqlite_sharded" and attach_shard_id is not None:
            shard_path = self.shard_path(attach_shard_id)
            if not shard_path.exists():
                raise MissingContent(f"shard {attach_shard_id} missing at {shard_path}")
        try:
            if self.mode == "sqlite_sharded" and attach_shard_id is not None:
                assert shard_path is not None
                try:
                    conn.execute("ATTACH DATABASE ? AS p", (sqlite_uri(shard_path, "rw"),))
                except sqlite3.OperationalError as exc:
                    _map_busy(exc)
                    raise CorruptContent(
                        f"shard {attach_shard_id} is present but unusable: {exc}"
                    ) from exc
                attached = True
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
            if attached:
                with suppress(sqlite3.Error):
                    conn.execute("DETACH DATABASE p")

    @contextmanager
    def txn(
        self,
        write: bool = False,
        attach_shard_id: int | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Open a dedicated connection and run a transaction on it (whitebox/tests)."""
        conn = self.connect_index()
        try:
            with self.txn_on(conn, write=write, attach_shard_id=attach_shard_id) as c:
                yield c
        finally:
            conn.close()

    def now(self) -> str:
        return self.clock.now_iso()

    def blob_length_limit(self, conn: sqlite3.Connection | None = None) -> int:
        """The connection's SQLITE_LIMIT_LENGTH (typical stock default ~1 GiB).

        ``conn`` may be an already-open session connection to avoid a
        dedicated connection for the probe.
        """
        if self._blob_limit is None:
            if conn is not None:
                self._blob_limit = int(conn.getlimit(sqlite3.SQLITE_LIMIT_LENGTH))
            else:
                probe = self.connect_index()
                try:
                    self._blob_limit = int(probe.getlimit(sqlite3.SQLITE_LIMIT_LENGTH))
                except AttributeError:  # pragma: no cover - very old sqlite3
                    self._blob_limit = 1_000_000_000
                finally:
                    probe.close()
        return self._blob_limit

    # -- repo config ----------------------------------------------------------

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

    def config_update(self, key: str, update: Callable[[Any], Any]) -> Any:
        """Read-modify-write of one config key inside a single write
        transaction (review P1-CFG-1): concurrent writers cannot lose an
        update between the read and the write."""
        with self.txn(write=True) as conn:
            row = conn.execute("SELECT value_json FROM repo_config WHERE key=?", (key,)).fetchone()
            current: Any = None if row is None else json.loads(str(row[0]))
            updated = update(current)
            conn.execute(
                "INSERT INTO repo_config(key, value_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, canonical_json(updated)),
            )
        return updated

    # -- small query helpers (single source for txn handling) -----------------

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

    # -- content tables -------------------------------------------------------

    def get_blob(self, blob_key: str) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM blobs WHERE blob_key=?", (blob_key,))

    def get_encoding(self, blob_key: str, profile: str) -> sqlite3.Row | None:
        return self._query_one(
            "SELECT * FROM encodings WHERE blob_key=? AND profile=?", (blob_key, profile)
        )

    @staticmethod
    def _ensure_blob(conn: sqlite3.Connection, blob_key: str, raw_len: int, created_at: str) -> None:
        """Insert the blob catalog row or verify the stored raw_len (decision J).

        The key is the identity: a mismatched stored ``raw_len`` is treated as
        corruption and never silently "repaired".
        """
        row = conn.execute("SELECT raw_len FROM blobs WHERE blob_key=?", (blob_key,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO blobs(blob_key, raw_len, created_at) VALUES(?, ?, ?)",
                (blob_key, raw_len, created_at),
            )
            return
        if int(row[0]) != raw_len:
            raise CorruptContent(
                f"blobs.raw_len mismatch for {blob_key}: stored {row[0]}, expected {raw_len}"
            )

    def store_encoding_and_payload_on(
        self,
        conn: sqlite3.Connection,
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
        """Write blob + encoding + payload rows; caller must hold a write txn.

        This is the single write path used by put/reencode/atomic-upserts so
        ``encodings.stored_len == len(payload.data)`` and the triple can never
        be half-committed.
        """
        self._ensure_blob(conn, blob_key, raw_len, created_at)
        if self.mode == "sqlite_single":
            conn.execute(
                "INSERT INTO payload(blob_key, profile, data) VALUES(?, ?, ?) "
                "ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                (blob_key, profile, payload),
            )
            conn.execute(
                _UPSERT_ENCODING_SINGLE,
                (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, updated_at),
            )
        else:
            if shard_id is None:
                # A payload write needs a shard locator; without one the row
                # cannot be repaired or created (review P0-4). Typed error, not
                # a raw "no such table: p.payload" from a missing ATTACH.
                raise CorruptContent(
                    f"encoding for {(blob_key, profile)} has no shard locator; cannot persist payload"
                )
            conn.execute(
                "INSERT INTO p.payload(blob_key, profile, data) VALUES(?, ?, ?) "
                "ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                (blob_key, profile, payload),
            )
            conn.execute(
                _UPSERT_ENCODING_SHARDED,
                (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, shard_id, updated_at),
            )

    def store_encoding_and_payload(self, **kwargs: Any) -> None:
        """Public wrapper: open a write txn and persist the triple."""
        shard_id = kwargs.get("shard_id")
        with self.txn(
            write=True, attach_shard_id=shard_id if self.mode == "sqlite_sharded" else None
        ) as conn:
            self.store_encoding_and_payload_on(conn, **kwargs)

    def get_payload(self, blob_key: str, profile: str, shard_id: int | None) -> bytes | None:
        if self.mode == "sqlite_single":
            row = self._query_one(
                "SELECT data FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return None if row is None else bytes(row[0])
        if shard_id is None:
            return None
        sid = int(shard_id)
        # Read paths must never create a missing shard file (review P0-2).
        if not self.shard_path(sid).exists():
            return None
        try:
            with self.txn(write=False, attach_shard_id=sid) as conn:
                row = conn.execute(
                    "SELECT data FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
                ).fetchone()
                return None if row is None else bytes(row[0])
        except CorruptContent:
            raise
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            raise CorruptContent(f"shard {sid} is present but unusable: {exc}") from exc

    def payload_exists(self, blob_key: str, profile: str, shard_id: int | None) -> bool:
        """Cheap existence probe (``SELECT 1``) — never fetches the whole BLOB
        (review P2-EXISTS-1)."""
        if self.mode == "sqlite_single":
            row = self._query_one(
                "SELECT 1 FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return row is not None
        if shard_id is None or not self.shard_path(int(shard_id)).exists():
            return False
        with self.txn(write=False, attach_shard_id=int(shard_id)) as conn:
            row = conn.execute(
                "SELECT 1 FROM p.payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            ).fetchone()
        return row is not None

    def iter_encodings(self, limit: int | None = None) -> Iterator[sqlite3.Row]:
        sql = "SELECT * FROM encodings ORDER BY blob_key, profile"
        rows = self._query_all(sql + " LIMIT ?", (limit,)) if limit is not None else self._query_all(sql)
        yield from rows

    # -- dictionaries ---------------------------------------------------------

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

    # -- repository tables ----------------------------------------------------

    def create_novel(self, title: str, meta: dict[str, Any] | None = None) -> int:
        """Insert the novel and its initial metadata in one transaction."""
        now = self.now()
        with self.txn(write=True) as conn:
            cur = conn.execute(
                "INSERT INTO novels(title, created_at, updated_at) VALUES(?, ?, ?)", (title, now, now)
            )
            lastrowid = cur.lastrowid
            novel_id = int(lastrowid) if lastrowid else 0
            if not novel_id:
                raise InkpackError("failed to create novel: no row id returned")
            for key, value in (meta or {}).items():
                conn.execute(
                    _UPSERT_META, ("novel", str(novel_id), str(key), canonical_json(value), now)
                )
        return novel_id

    def list_novels(self, search: str = "") -> list[dict[str, Any]]:
        if search:
            # User search is a substring, not SQL LIKE: escape wildcards (review §22).
            escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            rows = self._query_all(
                "SELECT * FROM novels WHERE title LIKE ? ESCAPE '\\' ORDER BY id DESC",
                (f"%{escaped}%",),
            )
        else:
            rows = self._query_all("SELECT * FROM novels ORDER BY id DESC")
        return [dict(row) for row in rows]

    def get_novel(self, novel_id: int) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM novels WHERE id=?", (int(novel_id),))

    def list_chapters(self, novel_id: int) -> list[sqlite3.Row]:
        # Catalog ordering by order_key (text sort), id as a stable tiebreak
        # (review P1-7).
        return self._query_all(
            "SELECT * FROM chapters WHERE novel_id=? ORDER BY order_key, id", (int(novel_id),)
        )

    def update_novel(self, novel_id: int, title: str | None = None, slug: str | None = None) -> bool:
        """Update the provided fields; returns False when the novel is missing."""
        sets: list[str] = []
        params: list[Any] = []
        if title is not None:
            sets.append("title=?")
            params.append(title)
        if slug is not None:
            sets.append("slug=?")
            params.append(slug)
        if not sets:
            return self.get_novel(novel_id) is not None
        sets.append("updated_at=?")
        params.append(self.now())
        params.append(int(novel_id))
        return self._execute(f"UPDATE novels SET {', '.join(sets)} WHERE id=?", tuple(params)) > 0

    def delete_chapter(self, chapter_id: int) -> bool:
        """Delete a chapter row and its metadata; returns False when missing."""
        with self.txn(write=True) as conn:
            cur = conn.execute("DELETE FROM chapters WHERE id=?", (int(chapter_id),))
            existed = cur.rowcount > 0
            if existed:
                conn.execute(
                    "DELETE FROM meta WHERE entity_type='chapter' AND entity_id=?",
                    (str(int(chapter_id)),),
                )
        return existed

    def delete_novel(self, novel_id: int, *, cascade: bool = True) -> bool:
        """Delete a novel (cascade deletes its chapters + metadata); catalog only.

        Uses subquery deletes (review P1-DEL-1) so deleting a novel with
        thousands of chapters never exceeds SQLite's variable-number limit.
        """
        with self.txn(write=True) as conn:
            novel = conn.execute("SELECT 1 FROM novels WHERE id=?", (int(novel_id),)).fetchone()
            if novel is None:
                return False
            if not cascade:
                chapter_rows = conn.execute(
                    "SELECT 1 FROM chapters WHERE novel_id=? LIMIT 1", (int(novel_id),)
                ).fetchone()
                if chapter_rows:
                    raise InkpackError(f"novel {novel_id} has chapters; delete with cascade=True")
            conn.execute(
                "DELETE FROM meta WHERE entity_type='chapter' AND entity_id IN "
                "(SELECT CAST(id AS TEXT) FROM chapters WHERE novel_id=?)",
                (int(novel_id),),
            )
            conn.execute("DELETE FROM chapters WHERE novel_id=?", (int(novel_id),))
            conn.execute(
                "DELETE FROM meta WHERE entity_type='novel' AND entity_id=?", (str(int(novel_id)),)
            )
            conn.execute("DELETE FROM novels WHERE id=?", (int(novel_id),))
        return True

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

        Probes the parent novel explicitly so a missing novel is a clear
        :class:`NotFound` and any *other* integrity error stays an
        :class:`InkpackError` (review §10).
        """
        now = self.now()
        with self.txn(write=True) as conn:
            novel = conn.execute("SELECT 1 FROM novels WHERE id=?", (int(novel_id),)).fetchone()
            if novel is None:
                raise NotFound(f"novel {novel_id} not found")
            conn.execute(
                _UPSERT_CHAPTER,
                (int(novel_id), order_key, blob_key, profile, media_type, charset, now, now),
            )
            row = conn.execute(
                "SELECT id FROM chapters WHERE novel_id=? AND order_key=?",
                (int(novel_id), order_key),
            ).fetchone()
            if row is None:
                raise InkpackError("chapter upsert failed to produce an id")
            chapter_id = int(row[0])
        if meta:
            for key, value in meta.items():
                self.meta_set("chapter", chapter_id, str(key), value)
        return chapter_id

    def upsert_chapter_with_content(
        self,
        *,
        novel_id: int,
        order_key: str,
        blob_key: str,
        raw_len: int,
        created_at: str,
        profile: str,
        media_type: str | None,
        charset: str | None,
        meta: dict[str, Any] | None,
        enc: Encoded | None,
        shard_id: int | None,
    ) -> int:
        """Persist content + chapter catalog + metadata in ONE transaction.

        ``enc is not None`` means the payload must be written (new content or
        a repair of a missing payload under the stored policy) — the shared
        triple-writer is used whether or not the encoding row exists, so a
        chapter never commits pointing at missing content (review P0-1).

        ``enc is None`` means a healthy dedupe hit: the existing encoding row
        and its payload are left untouched (decision E) and only the chapter
        row is written — the payload is re-probed in this transaction so a
        chapter never commits pointing at missing content (review P1-UP-1).

        Any failure (missing novel, busy, integrity error) rolls back the
        whole transaction, so a failed upsert never leaves an orphan blob.
        """
        now = self.now()
        # Shard resolution (locked semantics S2): happens here in the write
        # path, never during prepare. Repair rehomes when the referenced shard
        # is missing or the locator is NULL — an explicit, ensured shard is
        # attached with mode=rw; a missing shard is never created by ATTACH.
        if self.mode == "sqlite_sharded" and enc is not None and (
            shard_id is None or not self.shard_path(shard_id).exists()
        ):
            # Probe the novel first so a missing novel cannot trigger
            # shard-file creation (review P1-1 side effects).
            if self._query_one("SELECT 1 FROM novels WHERE id=?", (int(novel_id),)) is None:
                raise NotFound(f"novel {novel_id} not found")
            shard_id = self.choose_shard_for_write(enc.stored_len)
            self.ensure_shard_exists(shard_id)
        attach = shard_id if (self.mode == "sqlite_sharded" and (enc is not None or shard_id is not None)) else None
        try:
            with self.txn(write=True, attach_shard_id=attach) as conn:
                if enc is not None:
                    self.store_encoding_and_payload_on(
                        conn,
                        blob_key=blob_key,
                        raw_len=raw_len,
                        created_at=created_at,
                        profile=profile,
                        codec=enc.codec,
                        codec_params_json=enc.codec_params_json,
                        zstd_dict_id=enc.zstd_dict_id,
                        stored_len=enc.stored_len,
                        checksum=None,
                        shard_id=shard_id,
                        updated_at=now,
                        payload=enc.data,
                    )
                else:
                    existing = conn.execute(
                        "SELECT 1 FROM encodings WHERE blob_key=? AND profile=?",
                        (blob_key, profile),
                    ).fetchone()
                    if existing is None:
                        raise InkpackError(
                            f"encoding for {(blob_key, profile)} disappeared between prepare and persist; retry"
                        )
                    # TOCTOU guard: the payload must exist right now, in this
                    # transaction (review P1-UP-1).
                    if self.mode == "sqlite_single":
                        payload_row = conn.execute(
                            "SELECT 1 FROM payload WHERE blob_key=? AND profile=?",
                            (blob_key, profile),
                        ).fetchone()
                    elif shard_id is not None and self.shard_path(shard_id).exists():
                        payload_row = conn.execute(
                            "SELECT 1 FROM p.payload WHERE blob_key=? AND profile=?",
                            (blob_key, profile),
                        ).fetchone()
                    else:
                        payload_row = None
                    if payload_row is None:
                        raise InkpackError(
                            f"payload for {(blob_key, profile)} missing between prepare and persist; retry"
                        )
                novel = conn.execute("SELECT 1 FROM novels WHERE id=?", (int(novel_id),)).fetchone()
                if novel is None:
                    raise NotFound(f"novel {novel_id} not found")
                # lastrowid is connection-scoped in sqlite3 (it reflects earlier
                # INSERTs in this txn), so resolve the id deterministically.
                conn.execute(
                    _UPSERT_CHAPTER,
                    (int(novel_id), order_key, blob_key, profile, media_type, charset, now, now),
                )
                row = conn.execute(
                    "SELECT id FROM chapters WHERE novel_id=? AND order_key=?",
                    (int(novel_id), order_key),
                ).fetchone()
                if row is None:
                    raise InkpackError("chapter upsert failed to produce an id")
                chapter_id = int(row[0])
                for key, value in (meta or {}).items():
                    conn.execute(
                        _UPSERT_META,
                        ("chapter", str(chapter_id), str(key), canonical_json(value), now),
                    )
        except sqlite3.IntegrityError as exc:
            raise InkpackError(f"chapter upsert failed: {exc}") from exc
        return chapter_id

    def get_chapter(self, chapter_id: int) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM chapters WHERE id=?", (int(chapter_id),))

    # -- KV metadata (spec 7.2) ---------------------------------------------

    @staticmethod
    def _normalize_entity_id(entity_id: int | str) -> str:
        """Normalize an entity id to its canonical decimal string (locked
        semantics S6): non-negative integers only; ``bool`` and negative
        values are rejected."""
        if isinstance(entity_id, int) and not isinstance(entity_id, bool) and entity_id >= 0:
            return str(entity_id)
        if isinstance(entity_id, str) and entity_id.isdigit() and entity_id == str(int(entity_id)):
            return entity_id
        raise ValueError(
            f"entity_id must be the non-negative decimal string of an integer primary key, "
            f"got {entity_id!r}"
        )

    @classmethod
    def _normalize_entity(cls, entity_type: str, entity_id: int | str) -> tuple[str, str]:
        if entity_type not in _ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {_ENTITY_TYPES}, got {entity_type!r}")
        return entity_type, cls._normalize_entity_id(entity_id)

    def meta_set(self, entity_type: str, entity_id: int | str, key: str, value: Any) -> None:
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        with self.txn(write=True) as conn:
            conn.execute(
                _UPSERT_META,
                (entity_type, entity_id, str(key), canonical_json(value), self.now()),
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
        """Distinct ``(blob_key, profile)`` pairs referenced by chapters."""
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

    # -- maintenance (GC / vacuum) --------------------------------------------

    def gc(self, live: Iterable[ContentRef], cancel: CancelToken | None = None) -> tuple[int, int]:
        """Delete dead encodings + payloads; returns ``(encodings, payloads)``.

        The live set is staged into a TEMP table on the operation-scoped
        session connection (batched, cancellable). Dead rows are computed in
        SQL once, then grouped **by ``encodings.shard_id``** — not by the
        filesystem shard list (review P0-3):

        - shard file exists: ATTACH and delete payload + encodings atomically
          (decision G);
        - ``shard_id IS NULL`` or the shard file is missing: delete the
          encodings row only, without creating any file.

        Cancellation between groups leaves previously committed groups deleted
        and the current group untouched.
        """
        encodings_deleted = 0
        payload_rows_deleted = 0
        with self.session() as s:
            check_cancel(cancel)
            s.execute(
                "CREATE TEMP TABLE temp_live("
                "blob_key TEXT NOT NULL, profile TEXT NOT NULL, PRIMARY KEY(blob_key, profile))"
            )
            batch: list[tuple[str, str]] = []

            def flush() -> None:
                nonlocal batch
                if batch:
                    s.executemany(
                        "INSERT OR IGNORE INTO temp_live(blob_key, profile) VALUES(?, ?)", batch
                    )
                    batch = []

            try:
                for ref in live:
                    batch.append((ref.blob_key, ref.profile))
                    if len(batch) >= _GC_BATCH:
                        flush()
                        check_cancel(cancel)
                flush()
                s.execute("DROP TABLE IF EXISTS temp_dead")
                s.execute(
                    "CREATE TEMP TABLE temp_dead AS "
                    "SELECT e.blob_key, e.profile, e.shard_id FROM encodings e WHERE NOT EXISTS ("
                    "SELECT 1 FROM temp_live l WHERE l.blob_key = e.blob_key AND l.profile = e.profile)"
                )
                groups = [
                    cast("int | None", r[0])
                    for r in s.query_all("SELECT DISTINCT shard_id FROM temp_dead ORDER BY shard_id")
                ]
                for shard_id in groups:
                    check_cancel(cancel)
                    sid = int(shard_id) if shard_id is not None else None
                    shard_ok = (
                        sid is not None
                        and self.mode == "sqlite_sharded"
                        and self.shard_path(sid).exists()
                    )
                    with self.txn_on(
                        s.conn,
                        write=True,
                        attach_shard_id=sid if shard_ok else None,
                    ) as conn:
                        if shard_ok:
                            payload_rows_deleted += int(
                                conn.execute(
                                    "DELETE FROM p.payload WHERE (blob_key, profile) IN "
                                    "(SELECT blob_key, profile FROM temp_dead WHERE shard_id = ?)",
                                    (sid,),
                                ).rowcount
                            )
                            encodings_deleted += int(
                                conn.execute(
                                    "DELETE FROM encodings WHERE shard_id = ? AND (blob_key, profile) IN "
                                    "(SELECT blob_key, profile FROM temp_dead)",
                                    (sid,),
                                ).rowcount
                            )
                        else:
                            if shard_id is None and self.mode == "sqlite_single":
                                payload_rows_deleted += int(
                                    conn.execute(
                                        "DELETE FROM payload WHERE (blob_key, profile) IN "
                                        "(SELECT blob_key, profile FROM temp_dead)"
                                    ).rowcount
                                )
                            encodings_sql = (
                                "DELETE FROM encodings WHERE (blob_key, profile) IN "
                                "(SELECT blob_key, profile FROM temp_dead)"
                            )
                            if shard_id is None:
                                encodings_sql += " AND shard_id IS NULL" if self.mode == "sqlite_sharded" else ""
                                encodings_deleted += int(conn.execute(encodings_sql).rowcount)
                            elif sid is not None:
                                encodings_deleted += int(
                                    conn.execute(encodings_sql + " AND shard_id = ?", (sid,)).rowcount
                                )
                    if shard_id is None and self.mode == "sqlite_sharded":
                        # The NULL group's payload lives in an unknown shard
                        # (corrupted locator): clean it from every existing
                        # shard so GC genuinely repairs the repo (review P0-3).
                        for cleanup_sid in self.list_shards():
                            with self.txn_on(
                                s.conn, write=True, attach_shard_id=cleanup_sid
                            ) as conn:
                                payload_rows_deleted += int(
                                    conn.execute(
                                        "DELETE FROM p.payload WHERE (blob_key, profile) IN "
                                        "(SELECT blob_key, profile FROM temp_dead WHERE shard_id IS NULL)"
                                    ).rowcount
                                )
                            check_cancel(cancel)
                    check_cancel(cancel)
            finally:
                s.execute("DROP TABLE IF EXISTS temp_live")
                s.execute("DROP TABLE IF EXISTS temp_dead")
        return encodings_deleted, payload_rows_deleted

    def list_dead_encodings(self, live_refs: Iterable[ContentRef]) -> list[tuple[str, str, int | None]]:
        """Test/debug helper: dead encodings as a Python list.

        The production GC path uses :meth:`gc` (SQL-staged, no materialization).
        """
        live = {(r.blob_key, r.profile) for r in live_refs}
        rows = self._query_all("SELECT blob_key, profile, shard_id FROM encodings")
        return [
            (str(row[0]), str(row[1]), row[2])
            for row in rows
            if (str(row[0]), str(row[1])) not in live
        ]

    def delete_orphan_blobs(self) -> int:
        return self._execute(
            "DELETE FROM blobs WHERE NOT EXISTS ("
            "SELECT 1 FROM encodings e WHERE e.blob_key = blobs.blob_key)"
        )

    def _profile_dict_refs(self) -> set[str]:
        """Dictionary ids referenced by the current profile config.

        Dicts referenced by profiles are treated as live by GC (spec §4.1
        Option B): the documented ``train_dict → set_profile → gc → put``
        workflow must not lose the dictionary.
        """
        raw = self.config_get("profiles")
        refs: set[str] = set()
        mapping = cast("dict[str, Any]", raw) if isinstance(raw, dict) else None
        if mapping is not None:
            for cfg in mapping.values():
                cfg_dict = cast("dict[str, Any]", cfg) if isinstance(cfg, dict) else None
                if cfg_dict is not None:
                    dict_id = cfg_dict.get("zstd_dict_id")
                    if isinstance(dict_id, str):
                        refs.add(dict_id)
        return refs

    def delete_unreferenced_dicts(self) -> int:
        """Delete dicts not referenced by any encoding and not referenced by
        any profile in ``repo_config`` (review §4.1 Option B)."""
        profile_refs = self._profile_dict_refs()
        base = (
            "DELETE FROM dicts WHERE NOT EXISTS ("
            "SELECT 1 FROM encodings e WHERE e.zstd_dict_id = dicts.dict_id)"
        )
        if not profile_refs:
            return self._execute(base)
        placeholders = ",".join("?" * len(profile_refs))
        return self._execute(base + f" AND dict_id NOT IN ({placeholders})", tuple(sorted(profile_refs)))

    def vacuum_index(self) -> str:
        return self._vacuum(self.index_path)

    def vacuum_shard(self, shard_id: int) -> str:
        return self._vacuum(self.shard_path(shard_id))

    def _vacuum(self, path: Path) -> str:
        # VACUUM must run on a connection with no ATTACHed databases (decision H).
        conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
        try:
            conn.execute("VACUUM")
        except sqlite3.OperationalError as exc:
            _map_busy(exc)
            raise
        finally:
            conn.close()
        return str(path)

    # -- shard routing ---------------------------------------------------------

    def shard_path(self, shard_id: int) -> Path:
        if self.payload_dir is None:
            raise ValueError("sqlite_single mode has no shards")
        return self.payload_dir / f"shard-{shard_id:04d}.sqlite"

    def list_shards(self) -> list[int]:
        """Shard ids present on disk (locked semantics S9): only files matching
        ``shard-<digits>.sqlite`` count; any other filename is ignored so
        stray files can never crash open/validation/compact."""
        if self.payload_dir is None:
            return []
        shard_ids: list[int] = []
        for path in sorted(self.payload_dir.glob("shard-*.sqlite")):
            match = _SHARD_FILE_RE.match(path.name)
            if match is not None:
                shard_ids.append(int(match.group(1)))
        return shard_ids

    def ensure_shard_exists(self, shard_id: int) -> None:
        if self.payload_dir is None:
            return
        # Creating a higher-numbered shard moves the write pointer (review §8).
        if self._write_shard_id is None or shard_id > self._write_shard_id:
            self._write_shard_id = shard_id
        _ensure_shard_file(self.payload_dir, shard_id, self.busy_timeout_ms, self.synchronous)

    def choose_shard_for_write(self, estimated_payload_bytes: int) -> int:
        """Pick the write shard: the highest existing id, rolling past the cap.

        ``max(ids)`` (not the lexically-last path) keeps shard-10000 from
        sorting before shard-9999 (review §8). The write pointer is cached on
        the instance; the glob remains the source for compact/validation.
        """
        if self.mode != "sqlite_sharded":
            return 0
        if self._write_shard_id is None:
            shard_ids = self.list_shards()
            self._write_shard_id = max(shard_ids) if shard_ids else 1
            self.ensure_shard_exists(self._write_shard_id)
        current = self._write_shard_id
        path = self.shard_path(current)
        current_size = path.stat().st_size if path.exists() else 0
        if current_size + estimated_payload_bytes > self.shard_cap_bytes:
            current += 1
            self.ensure_shard_exists(current)
            self._write_shard_id = current
        return current
