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
from collections import OrderedDict
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, cast

from .codec import canonical_json
from .failpoints import failpoint
from .types import (
    Busy,
    CancelToken,
    Clock,
    ContentRef,
    CorruptContent,
    GcBatchSummary,
    InkpackError,
    MissingContent,
    NotFound,
    check_cancel,
    profiles_from_config,
)
from .types import (
    require_pk as _require_pk,
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
CREATE TABLE IF NOT EXISTS {p}payload(
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
    try:
        conn = sqlite3.connect(
            sqlite_uri(db_path, mode), uri=True, timeout=max(busy_timeout_ms / 1000.0, 0.001)
        )
    except sqlite3.Error as exc:
        if _is_busy(exc):
            raise Busy(str(exc)) from exc
        # Missing files (mode=rw) surface as "unable to open database file";
        # callers classify that (e.g. Session.attach -> MissingContent).
        raise InkpackError(f"cannot open database {db_path}: {exc}") from exc
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
        # A corrupt file (e.g. "file is not a database") surfaces here on the
        # first PRAGMA that reads the DB header; classify at the root so no
        # caller can leak a raw sqlite3.Error (A1, D9/S8).
        _classify_db_error(exc, what=f"cannot configure connection {db_path}")
    return conn


def _is_busy(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code in _BUSY_CODES:
        return True
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _classify_db_error(exc: sqlite3.Error, *, what: str, shard_id: int | None = None) -> NoReturn:
    """Classify any ``sqlite3.Error`` into the typed hierarchy (A1/A2, D9).

    One classifier for every path that executes SQL against a file whose
    validity is not guaranteed: busy/locked -> :class:`Busy`; a present-but-
    unusable shard -> :class:`CorruptContent`; anything else ->
    :class:`InkpackError` with context. Replaces the triplicated
    busy/unusable branches that previously diverged (some caught only
    ``OperationalError`` and let ``DatabaseError`` escape raw).
    """
    if _is_busy(exc):
        raise Busy(str(exc)) from exc
    if shard_id is not None:
        raise CorruptContent(f"shard {shard_id} is present but unusable: {exc}") from exc
    raise InkpackError(f"{what}: {exc}") from exc


def loads_config(text: str, *, key: str) -> Any:
    """Parse a repo_config/meta JSON value; corrupt JSON is InkpackError, never
    a raw json.JSONDecodeError (locked decision D9)."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise InkpackError(f"invalid repo config JSON for {key}: {exc}") from exc


def shard_filename(shard_id: int) -> str:
    """Canonical shard filename (locked decision D2): zero-padded 4 digits;
    ids >= 10000 naturally grow to 5+ digits."""
    return f"shard-{shard_id:04d}.sqlite"


def repo_markers_exist(root: Path) -> bool:
    """Single source for "is this path already a repository" (review H5, E2):
    index/repo markers OR any ``payload/shard-*.sqlite`` file. ``create`` and
    the factories agree by construction."""
    if (root / "index.sqlite").exists() or (root / "repo.sqlite").exists():
        return True
    payload = root / "payload"
    return payload.is_dir() and any(payload.glob("shard-*.sqlite"))


def _require_positive_int(value: Any, label: str) -> int:
    """Runtime validation of caller-supplied integers (config values)."""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _require_dict_row(conn: sqlite3.Connection, dict_id: str | None, *, what: str) -> None:
    """N7 (r4-P1.1): a committed encoding/hit must name a dict that EXISTS.

    Probes the ``dicts`` table ON THE GIVEN CONNECTION (the write-txn
    connection), not the session's dict cache — the cache is filled during
    prepare, before the write txn, and ``delete_unreferenced_dicts`` does not
    invalidate it. Probing inside the same writer lock closes the window: a
    concurrent gc that deletes the dict holds the lock, so either it ran
    before this txn (probe sees it gone -> :class:`MissingContent`) or after
    (the dict is still present at commit).
    """
    if dict_id is None:
        return
    if conn.execute("SELECT 1 FROM dicts WHERE dict_id=?", (dict_id,)).fetchone() is None:
        raise MissingContent(f"dictionary {dict_id!r} missing for {what}")


def _require_scope(scope: Any) -> int | None:
    """Runtime validation of the ``iter_live_content`` scope argument
    (Issue 15: non-negative int, not bool)."""
    if scope is None:
        return None
    if isinstance(scope, bool) or not isinstance(scope, int) or scope < 0:
        raise TypeError(f"scope must be a non-negative int novel id or None, got {type(scope).__name__}")
    return scope


def translate_sqlite(exc: BaseException, *, what: str) -> NoReturn:
    """Translate any leftover SQLite error after typed classification (A.1).

    Busy/locked -> :class:`Busy`; other ``sqlite3.Error`` -> ``InkpackError``
    with context; already-typed errors pass through unchanged.
    """
    if isinstance(exc, sqlite3.Error) and _is_busy(exc):
        raise Busy(str(exc)) from exc
    if isinstance(exc, sqlite3.Error):
        raise InkpackError(f"{what}: {exc}") from exc
    raise exc


def _map_busy(exc: sqlite3.Error) -> None:
    """Translate a sqlite busy/locked error into :class:`Busy` (locked
    semantics S8: open/migrate/validation paths must not leak raw errors)."""
    translate_sqlite(exc, what="sqlite operation")


def _split_statements(script: str) -> list[str]:
    """Split a migration script into complete statements (E4).

    A ``sqlite3.complete_statement`` accumulator instead of a naive
    ``split(";")``: a ``;`` inside a future string literal or trigger body
    cannot cut a statement in half. Semicolons are re-appended as each piece
    is consumed (``complete_statement`` needs the terminator); a trailing
    partial statement is returned as-is (the guard test asserts every
    produced statement is complete).
    """
    statements: list[str] = []
    buf = ""
    for piece in script.split(";"):
        buf = f"{buf};{piece};" if buf else f"{piece};"
        if buf.strip(";").strip() and sqlite3.complete_statement(buf):
            statements.append(buf.strip())
            buf = ""
    tail = buf.strip().rstrip(";").strip()
    if tail:
        statements.append(tail)
    return statements


def _apply_index_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending index migrations; the caller owns the transaction."""
    current = int(conn.execute("PRAGMA user_version").fetchone()[0])
    for version, script in MIGRATIONS:
        if version <= current:
            continue
        for stmt in _split_statements(script):
            conn.execute(stmt)
        conn.execute(f"PRAGMA user_version={version}")


def migrate_index(conn: sqlite3.Connection) -> None:
    """Run index migrations as individual statements inside ONE explicit
    transaction (review L27: ``executescript`` would implicitly COMMIT)."""
    try:
        conn.execute("BEGIN")
        _apply_index_migrations(conn)
        conn.commit()
    except sqlite3.Error as exc:
        with suppress(sqlite3.Error):
            conn.rollback()
        translate_sqlite(exc, what="index migration")
        raise


def _payload_version(conn: sqlite3.Connection) -> int:
    conn.execute(_PAYLOAD_VERSION_TABLE)
    row = conn.execute("SELECT version FROM _inkpack_schema WHERE key='payload'").fetchone()
    return int(row[0]) if row is not None else 0


def _payload_schema_current(conn: sqlite3.Connection) -> bool:
    """P2.4: is the payload schema already at the latest version?

    Read-only (no DDL) — checks ``sqlite_master`` for the version table and,
    if present, reads the payload version. Lets open/validation skip
    ``migrate_payload`` (which would run a no-op ``CREATE TABLE IF NOT
    EXISTS`` DDL) when the schema is already current. Returns False when the
    version table is absent (a fresh shard) so the caller migrates."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_inkpack_schema'"
    ).fetchone()
    if row is None:
        return False
    row = conn.execute("SELECT version FROM _inkpack_schema WHERE key='payload'").fetchone()
    if row is None:
        return False
    return int(row[0]) >= max(version for version, _script in PAYLOAD_MIGRATIONS)


def _apply_payload_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending payload migrations; the caller owns the transaction."""
    conn.execute(_PAYLOAD_VERSION_TABLE)
    current = _payload_version(conn)
    for version, script in PAYLOAD_MIGRATIONS:
        if version <= current:
            continue
        for stmt in _split_statements(script):
            conn.execute(stmt.replace("{p}", ""))  # Issue 43 / P2.3: tokenized schema
        conn.execute(
            "INSERT INTO _inkpack_schema(key, version) VALUES('payload', ?) "
            "ON CONFLICT(key) DO UPDATE SET version=excluded.version",
            (version,),
        )


def migrate_payload(conn: sqlite3.Connection) -> None:
    """Run payload migrations inside ONE explicit transaction (idempotent)."""
    try:
        conn.execute("BEGIN")
        _apply_payload_migrations(conn)
        conn.commit()
    except sqlite3.Error as exc:
        with suppress(sqlite3.Error):
            conn.rollback()
        translate_sqlite(exc, what="payload migration")
        raise


def migrate_payload_attached(conn: sqlite3.Connection, alias: str = "p") -> None:
    """Migrate the payload schema of an ATTACHed shard (review M10).

    Runs inside the caller's transaction; the version table lives in the
    shard (``<alias>._inkpack_schema``), so each shard tracks its own payload
    schema version independently of the index. ``alias`` is the ATTACH alias:
    ``p`` for whitebox per-txn attaches, ``p<id>`` for session-managed
    attaches (C1).

    P2.3: statements use the ``{p}`` token, substituted with a literal
    ``str.replace`` (NOT ``str.format``) so a literal ``{`` in a future
    script (a JSON default, a trigger body) cannot raise ``KeyError``.
    ATTACHED migrations run in autocommit BEFORE the writer's ``BEGIN`` and
    MUST be idempotent (``IF NOT EXISTS`` / ``ON CONFLICT``); do not move
    them inside the write txn unless a non-idempotent migration arrives.
    """
    a = f"{alias}."
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {a}_inkpack_schema(key TEXT PRIMARY KEY, version INTEGER NOT NULL)"
    )
    row = conn.execute(f"SELECT version FROM {a}_inkpack_schema WHERE key='payload'").fetchone()
    current = int(row[0]) if row is not None else 0
    for version, script in PAYLOAD_MIGRATIONS:
        if version <= current:
            continue
        for stmt in _split_statements(script):
            conn.execute(stmt.replace("{p}", a))
        conn.execute(
            f"INSERT INTO {a}_inkpack_schema(key, version) VALUES('payload', ?) "
            "ON CONFLICT(key) DO UPDATE SET version=excluded.version",
            (version,),
        )


def _write_initial_config(conn: sqlite3.Connection, config: dict[str, Any]) -> None:
    """Write the initial repo_config rows in the caller's transaction (H3)."""
    for key, obj in config.items():
        conn.execute(
            "INSERT INTO repo_config(key, value_json) VALUES(?, ?)",
            (key, canonical_json(obj)),
        )


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

    # Attach capacity: one slot below the connection's SQLITE_LIMIT_ATTACHED
    # (the doc's batch arithmetic uses the same conservative bound).
    _FALLBACK_ATTACH_LIMIT = 10
    # Class-level override for tests (C1 eviction scenario).
    _force_attach_limit: int | None = None

    def __init__(self, backend: SqliteBackend) -> None:
        self.backend = backend
        self.conn = backend.connect_index()
        self._missing_shards: set[int] = set()
        self._dict_cache: dict[str, bytes | None] = {}
        # A3: structural payload-schema probe cache (None = main DB). A dropped
        # table does not return mid-session barring external DDL, so the
        # negative cache is sound; recovery costs one new session.
        self._schema_ok: set[int | None] = set()
        self._schema_bad: set[int | None] = set()
        # C1: session-managed shard attaches. shard_id -> alias, in LRU order.
        # Attaches persist across the operation's transactions (an idle ATTACH
        # holds no locks); capacity is bounded by the attach limit with LRU
        # eviction, so a bulk read of N shards costs O(shards) attaches, not
        # O(rows).
        self._attached: OrderedDict[int, str] = OrderedDict()
        self._migrated: set[int] = set()  # shards migrated this session (write side)
        self._attach_limit_cache: int | None = None
        self._closed = False

    def attach_limit(self) -> int:
        """How many shards may be ATTACHed on this session at once."""
        if self._force_attach_limit is not None:
            return max(1, self._force_attach_limit)
        if self._attach_limit_cache is None:
            try:
                limit = int(self.conn.getlimit(sqlite3.SQLITE_LIMIT_ATTACHED))
            except (AttributeError, sqlite3.Error):
                limit = self._FALLBACK_ATTACH_LIMIT
            self._attach_limit_cache = max(1, limit - 1)
        return self._attach_limit_cache

    def attach(self, shard_id: int) -> str:
        """Idempotent session-scoped ATTACH; evicts the LRU shard at capacity.

        A missing shard file is :class:`MissingContent` (reads of such content
        are missing; the file is never created); a present-but-unusable file
        is :class:`CorruptContent`; a locked file is :class:`Busy`.
        """
        alias = self._attached.get(shard_id)
        if alias is not None:
            self._attached.move_to_end(shard_id)
            return alias
        if self.backend.mode != "sqlite_sharded":
            raise ValueError("sqlite_single mode has no shards")
        path = self.backend.shard_path(shard_id)
        if not path.exists():
            self._missing_shards.add(shard_id)
            raise MissingContent(f"shard {shard_id} missing at {path}")
        while len(self._attached) >= self.attach_limit():
            victim, victim_alias = self._attached.popitem(last=False)
            self._migrated.discard(victim)
            with suppress(sqlite3.Error):
                self.conn.execute(f"DETACH DATABASE {victim_alias}")
        alias = self.alias_for(shard_id)
        try:
            self.conn.execute(f"ATTACH DATABASE ? AS {alias}", (sqlite_uri(path, "rw"),))
        except sqlite3.Error as exc:
            if "unable to open database" in str(exc).lower():
                # The file vanished between the exists() check and the ATTACH:
                # missing content, never create (review M14, locked D9).
                self._missing_shards.add(shard_id)
                raise MissingContent(f"shard {shard_id} missing at {path}") from exc
            _classify_db_error(exc, what="attach shard", shard_id=shard_id)
        self._attached[shard_id] = alias
        return alias

    def detach(self, shard_id: int) -> None:
        alias = self._attached.pop(shard_id, None)
        if alias is not None:
            self._migrated.discard(shard_id)
            with suppress(sqlite3.Error):
                self.conn.execute(f"DETACH DATABASE {alias}")

    def ensure_migrated(self, shard_id: int) -> None:
        """Run the shard's payload migration once per (session, shard).

        N2: the FIRST WRITE touch migrates; read attaches never DDL.
        """
        if shard_id in self._migrated:
            return
        migrate_payload_attached(self.conn, alias=self.alias_for(shard_id))
        self._migrated.add(shard_id)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        try:
            row = self.conn.execute(sql, params).fetchone()
            return cast("sqlite3.Row | None", row)
        except sqlite3.Error as exc:
            translate_sqlite(exc, what="query")

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        try:
            return cast("list[sqlite3.Row]", self.conn.execute(sql, params).fetchall())
        except sqlite3.Error as exc:
            translate_sqlite(exc, what="query")

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        try:
            return int(self.conn.execute(sql, params).rowcount)
        except sqlite3.Error as exc:
            translate_sqlite(exc, what="execute")

    def executemany(self, sql: str, seq: Iterable[tuple[Any, ...]]) -> None:
        try:
            self.conn.executemany(sql, seq)
        except sqlite3.Error as exc:
            translate_sqlite(exc, what="execute")

    def config(self, key: str) -> Any:
        row = self.query_one("SELECT value_json FROM repo_config WHERE key=?", (key,))
        return None if row is None else loads_config(str(row[0]), key=key)

    @staticmethod
    def alias_for(shard_id: int) -> str:
        """SQL alias under which a shard is ATTACHed on a session connection.

        Per-shard aliases (C1) let one session hold several shards at once
        (bounded by the attach limit with LRU eviction); whitebox
        ``txn()`` connections keep the single per-txn alias ``p``.
        """
        return f"p{int(shard_id)}"

    def payload_schema_ok(self, conn: sqlite3.Connection, shard_id: int | None, alias: str | None = None) -> bool:
        """Deterministic structural probe: does the payload table exist (A3)?

        Broken payload *storage* is a per-row content finding (the caller
        raises :class:`CorruptContent`), in contrast to broken *catalog*
        tables, which are repo structure (``InkpackError``). The probe is a
        single ``sqlite_master`` lookup — no message-text matching — and is
        positively and negatively cached per session. Read-only (N2-safe).

        ``alias`` is the ATTACH alias the shard currently carries on ``conn``
        (``p`` for whitebox per-txn attaches; defaults to the session
        alias ``p<id>``). The cache is keyed by shard id only: table
        existence does not depend on the alias.
        """
        if shard_id in self._schema_ok:
            return True
        if shard_id in self._schema_bad:
            return False
        if shard_id is None:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='payload'"
            ).fetchone()
        else:
            a = alias if alias is not None else self.alias_for(int(shard_id))
            row = conn.execute(
                f"SELECT 1 FROM {a}.sqlite_master WHERE type='table' AND name='payload'"
            ).fetchone()
        (self._schema_ok if row is not None else self._schema_bad).add(shard_id)
        return row is not None

    def payload_exists(self, blob_key: str, profile: str, shard_id: int | None) -> bool:
        """Cheap existence probe (``SELECT 1``) — never reads the whole BLOB
        (review P2-2).

        WRITE-side semantics (P1.2/P2.1): a MISSING or present-but-UNUSABLE
        shard is a MISS (``False``), so a put REHOMES / repairs off it instead
        of failing — the write path tolerates junk. P2.1 unifies the probe
        onto the session ATTACH (the same path reads use), so the miss/unusable
        decision is defined once. READS (via ``_read_snapshot``/attach) still
        classify an unusable shard as ``CorruptContent`` (A2/C4). ``Busy``
        propagates either way (locked-but-healthy is not a miss — the
        P1.2 except-ordering trap).
        """
        if self.backend.mode == "sqlite_single":
            row = self.query_one(
                "SELECT 1 FROM payload WHERE blob_key=? AND profile=?", (blob_key, profile)
            )
            return row is not None
        if shard_id is None:
            return False
        try:
            alias = self.attach(int(shard_id))
        except (MissingContent, CorruptContent):
            return False  # missing or present-but-unusable: write-side miss
        # Busy propagates from attach (locked-but-healthy is not a miss).
        try:
            row = self.conn.execute(
                f"SELECT 1 FROM {alias}.payload WHERE blob_key=? AND profile=?",
                (blob_key, profile),
            ).fetchone()
        except sqlite3.Error as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            return False  # file became unusable mid-session: write-side miss
        return row is not None

    def dict_bytes(self, dict_id: str) -> bytes | None:
        if dict_id not in self._dict_cache:
            row = self.query_one("SELECT dict_bytes FROM dicts WHERE dict_id=?", (dict_id,))
            self._dict_cache[dict_id] = None if row is None else bytes(row[0])
        return self._dict_cache[dict_id]

    def forget_missing_shard(self, shard_id: int) -> None:
        """Drop a negative-cache entry after the shard file was (re)created."""
        self._missing_shards.discard(shard_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._missing_shards.clear()
        for shard_id in list(self._attached):
            with suppress(sqlite3.Error):
                self.conn.execute(f"DETACH DATABASE {self._attached[shard_id]}")
        self._attached.clear()
        self._migrated.clear()
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
    # P1.2: shards verified usable (this process's lifetime). Healthy writes
    # pay zero extra probe connections — a bulk reencode over N targets must
    # not open N probe connections. External rot after a cache hit fails at
    # attach, as today.
    _usable_shards: set[int] = field(default_factory=set[int], init=False)

    # -- construction -------------------------------------------------------

    @classmethod
    def create(
        cls,
        path: str | os.PathLike[str],
        mode: str,
        pragmas: dict[str, Any] | None = None,
        shard_cap_bytes: int = 2 << 30,
        min_shard_cap_bytes: int = 256 << 20,
        clock: Clock | None = None,
        initial_config: dict[str, Any] | None = None,
    ) -> SqliteBackend:
        """Create a brand-new repository database.

        Refuses an existing repository (review H5): index/repo markers OR any
        ``payload/shard-*.sqlite`` file (canonical or not) means the path is
        already a repository. ``initial_config`` rows are written in the same
        transaction as the migration (review H3) so creation is all-or-nothing.
        """
        if repo_markers_exist(Path(path)):
            raise InkpackError(f"repository already exists at {path}")
        backend = cls.open(
            path,
            mode,
            pragmas=pragmas,
            shard_cap_bytes=shard_cap_bytes,
            min_shard_cap_bytes=min_shard_cap_bytes,
            clock=clock,
            create=True,
            initial_config=initial_config,
        )
        return backend

    @classmethod
    def open(
        cls,
        path: str | os.PathLike[str],
        mode: str,
        pragmas: dict[str, Any] | None = None,
        shard_cap_bytes: int = 2 << 30,
        min_shard_cap_bytes: int = 256 << 20,
        clock: Clock | None = None,
        *,
        create: bool = False,
        initial_config: dict[str, Any] | None = None,
    ) -> SqliteBackend:
        """Open an existing repository, or create one when ``create=True``.

        With ``create=False`` (the default) a missing repository raises
        :class:`NotFound` and **no files or directories are created** (review
        P0-2). With ``create=True`` an existing repository is refused (review
        P0-6, H5); ``create`` never opens/migrates an existing DB, and
        ``initial_config`` rows are committed in the same transaction as the
        migration (review H3).
        """
        if mode not in ("sqlite_single", "sqlite_sharded"):
            raise ValueError(f"mode must be 'sqlite_single' or 'sqlite_sharded', got {mode!r}")
        if mode == "sqlite_sharded":
            # Issue 16: shard caps are sharded-only knobs.
            shard_cap_bytes = _require_positive_int(shard_cap_bytes, "shard_cap_bytes")
            min_shard_cap_bytes = _require_positive_int(min_shard_cap_bytes, "min_shard_cap_bytes")
            if min_shard_cap_bytes > shard_cap_bytes:
                raise ValueError("min_shard_cap_bytes must not exceed shard_cap_bytes")

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
                    try:
                        conn.execute("PRAGMA journal_mode=WAL")
                        conn.execute("PRAGMA auto_vacuum=NONE")
                    except sqlite3.OperationalError as exc:
                        _map_busy(exc)
                        raise
                    # Issue 10: schema + initial config in ONE transaction.
                    conn.execute("BEGIN")
                    try:
                        _apply_index_migrations(conn)
                        _apply_payload_migrations(conn)
                        if initial_config:
                            _write_initial_config(conn, initial_config)
                        conn.commit()
                    except BaseException:
                        with suppress(sqlite3.Error):
                            conn.rollback()
                        raise
                else:
                    migrate_index(conn)
                    if not _payload_schema_current(conn):  # P2.4: skip DDL if current
                        migrate_payload(conn)
            finally:
                conn.close()
        else:
            payload_dir = root / "payload"
            if create:
                payload_dir.mkdir(parents=True, exist_ok=True)
            elif not payload_dir.is_dir():
                raise InkpackError(f"broken repository layout at {root}: payload directory missing")
            conn = connect_file(
                index_path, pragmas["busy_timeout_ms"], pragmas["synchronous"], create=create
            )
            try:
                if create:
                    try:
                        conn.execute("PRAGMA journal_mode=DELETE")
                        conn.execute("PRAGMA auto_vacuum=NONE")
                    except sqlite3.OperationalError as exc:
                        _map_busy(exc)
                        raise
                    # Issue 10: schema + initial config in ONE transaction.
                    conn.execute("BEGIN")
                    try:
                        _apply_index_migrations(conn)
                        if initial_config:
                            _write_initial_config(conn, initial_config)
                        conn.commit()
                    except BaseException:
                        with suppress(sqlite3.Error):
                            conn.rollback()
                        raise
                else:
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
            shard_min_bytes=min_shard_cap_bytes,  # dataclass field keeps the stable name
            clock=clock,
            busy_timeout_ms=pragmas["busy_timeout_ms"],
            synchronous=pragmas["synchronous"],
        )
        backend._validate_journal_modes()
        return backend

    def _validate_journal_modes(self) -> None:
        """Validate journal modes on open and migrate existing shards (M10).

        Single mode allows ``wal``/``delete``/``truncate`` (WAL is the
        recommended default; tampering is visible, review L27). Sharded mode
        requires rollback journal on the index and every shard (spec 8.2).
        Non-canonical shard filenames are refused (locked decision D2/H4).
        """
        offenders = self._shard_name_offenders()
        if offenders:
            raise InkpackError(
                "non-canonical shard filenames: "
                + ", ".join(sorted(offenders))
                + "; rename e.g. 'shard-1.sqlite' to 'shard-0001.sqlite'"
            )
        if self.mode == "sqlite_single":
            self._check_journal(self.index_path, "index", {"wal", "delete", "truncate"})
            return
        self._check_journal(self.index_path, "index", {"delete", "truncate"})
        self._validate_shard_journals(self.list_shards())

    def _validate_shard_journals(self, shard_ids: list[int]) -> None:
        """C4: per-shard open validation (connect + migrate + journal check).

        Each shard file is independent, so the checks run in a small thread
        pool (one connection per worker, per shard). Determinism: when more
        than one shard fails, the error of the LOWEST shard id is raised.
        """
        if not shard_ids:
            return
        if len(shard_ids) == 1:
            self._validate_shard_journal(shard_ids[0])
            return
        errors: dict[int, BaseException] = {}
        with ThreadPoolExecutor(max_workers=min(8, len(shard_ids))) as pool:
            futures = {pool.submit(self._validate_shard_journal, sid): sid for sid in shard_ids}
            for future in as_completed(futures):
                sid = futures[future]
                try:
                    future.result()
                except BaseException as exc:  # re-raised below, lowest sid first
                    errors[sid] = exc
        if errors:
            raise errors[min(errors)]

    def _validate_shard_journal(self, sid: int) -> None:
        path = self.shard_path(sid)
        conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
        try:
            if not _payload_schema_current(conn):  # P2.4: skip DDL if current (M10)
                migrate_payload(conn)
            journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        except sqlite3.Error as exc:
            _classify_db_error(exc, what=f"shard {sid} DB is not a valid sqlite database")
        finally:
            conn.close()
        if journal_mode not in {"delete", "truncate"}:
            raise InkpackError(f"shard {sid} DB must use rollback journal mode, found {journal_mode!r}")

    def _check_journal(self, path: Path, label: str, allowed: set[str]) -> None:
        conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
        try:
            journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        except sqlite3.Error as exc:
            _classify_db_error(exc, what=f"{label} DB is not a valid sqlite database")
        finally:
            conn.close()
        if journal_mode not in allowed:
            raise InkpackError(f"{label} DB has unsupported journal mode {journal_mode!r}")

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
        *,
        session: Session | None = None,
    ) -> Generator[sqlite3.Connection, None, None]:
        """Run a transaction on an existing connection.

        Sharded ATTACH is **require-mode** (locked semantics S2): the shard
        file must already exist (checked explicitly) and is attached with
        ``mode=rw`` so SQLite itself refuses to create it. A missing shard is
        :class:`MissingContent`; a present-but-unusable shard is
        :class:`CorruptContent`; busy maps to :class:`Busy`.

        With ``session`` (operation-scoped connections, C1) the shard is
        attached through the session's LRU attach manager and persists
        across the operation's transactions (an idle ATTACH holds no locks);
        write transactions migrate the shard once per session (N2). Without
        it (whitebox ``txn()`` connections) each transaction ATTACHes ``p``
        and DETACHes it on exit.
        """
        if session is not None and self.mode == "sqlite_sharded" and attach_shard_id is not None:
            # Session-managed attach (C1): persists across transactions;
            # Session.close() detaches everything.
            session.attach(attach_shard_id)  # MissingContent / CorruptContent / Busy
            if write:
                session.ensure_migrated(attach_shard_id)
        attached = False
        shard_path: Path | None = None
        if (
            session is None
            and self.mode == "sqlite_sharded"
            and attach_shard_id is not None
        ):
            shard_path = self.shard_path(attach_shard_id)
            if not shard_path.exists():
                raise MissingContent(f"shard {attach_shard_id} missing at {shard_path}")
        try:
            if (
                session is None
                and self.mode == "sqlite_sharded"
                and attach_shard_id is not None
            ):
                assert shard_path is not None
                try:
                    conn.execute("ATTACH DATABASE ? AS p", (sqlite_uri(shard_path, "rw"),))
                except sqlite3.Error as exc:
                    if _is_busy(exc):
                        raise Busy(str(exc)) from exc
                    if "unable to open database" in str(exc).lower():
                        # The file vanished between the exists() check and the
                        # ATTACH: missing content, never create (review M14).
                        raise MissingContent(
                            f"shard {attach_shard_id} missing at {shard_path}"
                        ) from exc
                    raise CorruptContent(
                        f"shard {attach_shard_id} is present but unusable: {exc}"
                    ) from exc
                attached = True  # set BEFORE migrate so a failed migrate still DETACHes (Issue 4)
                if write:
                    # Migrations are writer-only (Issue 32): read attaches never DDL.
                    migrate_payload_attached(conn)
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            with suppress(sqlite3.Error):
                conn.rollback()
            translate_sqlite(exc, what="transaction")
        except BaseException:
            # Rollback errors must not mask the original failure (D9).
            with suppress(sqlite3.Error):
                conn.rollback()
            raise
        finally:
            if attached:
                with suppress(sqlite3.Error):
                    conn.execute("DETACH DATABASE p")
            # Session-managed attaches persist (close() detaches all); only
            # whitebox per-txn attaches are detached here.

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
        return None if row is None else loads_config(str(row[0]), key=key)

    def config_set(self, key: str, obj: Any) -> None:
        with self.txn(write=True) as conn:
            conn.execute(
                "INSERT INTO repo_config(key, value_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, canonical_json(obj)),
            )

    def config_update_in_txn(
        self, key: str, update: Callable[[Any, sqlite3.Connection], Any]
    ) -> Any:
        """Read-modify-write of one config key inside a single write
        transaction (review P1-CFG-1, B2): concurrent writers cannot lose an
        update between the read and the write.

        The update callback receives the current value AND the transaction's
        connection, so any checks that must be atomic with the write (e.g.
        B2's dictionary-existence check, which must serialize against gc's
        single-txn dict reclamation) run INSIDE the transaction instead of in
        a preceding read transaction.
        """
        with self.txn(write=True) as conn:
            row = conn.execute("SELECT value_json FROM repo_config WHERE key=?", (key,)).fetchone()
            current: Any = None if row is None else loads_config(str(row[0]), key=key)
            updated = update(current, conn)
            conn.execute(
                "INSERT INTO repo_config(key, value_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, canonical_json(updated)),
            )
        return updated

    def config_update(self, key: str, update: Callable[[Any], Any]) -> Any:
        """Back-compat shim over :meth:`config_update_in_txn` for callers
        whose update does not need the transaction connection."""
        return self.config_update_in_txn(key, lambda current, _conn: update(current))

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

    @staticmethod
    def _ensure_blob(conn: sqlite3.Connection, blob_key: str, raw_len: int, created_at: str) -> None:
        """Insert the blob catalog row or verify the stored raw_len (decision J).

        The key is the identity: a mismatched stored ``raw_len`` is treated as
        corruption and never silently "repaired".
        """
        conn.execute(
            "INSERT OR IGNORE INTO blobs(blob_key, raw_len, created_at) VALUES(?, ?, ?)",
            (blob_key, raw_len, created_at),
        )
        row = conn.execute("SELECT raw_len FROM blobs WHERE blob_key=?", (blob_key,)).fetchone()
        if row is None or int(row[0]) != raw_len:
            raise CorruptContent(
                f"blobs.raw_len mismatch for {blob_key}: stored {row[0] if row else None}, expected {raw_len}"
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
        alias: str = "p",
    ) -> None:
        """Write blob + encoding + payload rows; caller must hold a write txn.

        This is the single write path used by put/reencode/atomic-upserts so
        ``encodings.stored_len == len(payload.data)`` and the triple can never
        be half-committed. ``alias`` is the shard's ATTACH alias: ``p`` for
        whitebox per-txn attaches, ``p<id>`` for session-managed ones (C1).

        N7 (r4-P1.1): the encoding must not name a dictionary that no longer
        exists — the dicts row is re-probed ON THIS CONNECTION (not the
        session's dict cache) inside the same writer lock, so a concurrent
        gc that deleted the dict cannot slip between check and commit.
        """
        _require_dict_row(
            conn, zstd_dict_id, what=f"the new encoding for {(blob_key, profile)}"
        )
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
                f"INSERT INTO {alias}.payload(blob_key, profile, data) VALUES(?, ?, ?) "
                "ON CONFLICT(blob_key, profile) DO UPDATE SET data=excluded.data",
                (blob_key, profile, payload),
            )
            conn.execute(
                _UPSERT_ENCODING_SHARDED,
                (blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, shard_id, updated_at),
            )

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
        return self._query_one("SELECT * FROM novels WHERE id=?", (_require_pk(novel_id, "novel_id"),))

    def list_chapters(self, novel_id: int) -> list[sqlite3.Row]:
        # Catalog ordering by order_key (text sort), id as a stable tiebreak
        # (review P1-7).
        return self._query_all(
            "SELECT * FROM chapters WHERE novel_id=? ORDER BY order_key, id",
            (_require_pk(novel_id, "novel_id"),),
        )

    def update_novel(self, novel_id: int, title: str | None = None, slug: str | None = None) -> bool:
        """Update the provided fields; returns False when the novel is missing.

        ``None`` means "leave unchanged"; a field is only ever overwritten,
        never cleared — to blank a slug pass ``""`` (not ``None``).
        """
        novel_id = _require_pk(novel_id, "novel_id")
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
        chapter_id = _require_pk(chapter_id, "chapter_id")
        with self.txn(write=True) as conn:
            cur = conn.execute("DELETE FROM chapters WHERE id=?", (chapter_id,))
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
        novel_id = _require_pk(novel_id, "novel_id")
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

    def upsert_chapter_catalog_on(
        self,
        conn: sqlite3.Connection,
        *,
        novel_id: int,
        order_key: str,
        blob_key: str,
        profile: str,
        hints: dict[str, Any] | None,
        meta: dict[str, Any] | None,
        now: str,
    ) -> int:
        """Upsert the chapter catalog row + metadata inside an OPEN write
        transaction (novel probe included). Shared by Repository's atomic
        content+catalog flow.

        Hints semantics (r4-P1.8): ``hints is None`` -> the stored
        ``media_type``/``charset`` are PRESERVED (a re-upsert with default
        hints does not NULL them out). ``hints`` provided -> the hints group
        is REPLACED: present keys are written, an omitted key means NULL
        (``{}`` clears both). ``meta`` always merges per key (independent
        upserts). New rows get NULLs for omitted/absent hints either way.
        """
        novel = conn.execute("SELECT 1 FROM novels WHERE id=?", (_require_pk(novel_id, "novel_id"),)).fetchone()
        if novel is None:
            raise NotFound(f"novel {novel_id} not found")
        if hints is None:
            # Preserve the stored hints group on conflict (no magic NULLs):
            # media_type/charset are NULL literals for NEW rows and simply
            # absent from the DO UPDATE SET for EXISTING rows.
            sql = (
                "INSERT INTO chapters("
                " novel_id, order_key, blob_key, profile, media_type, charset, created_at, updated_at"
                ") VALUES(?, ?, ?, ?, NULL, NULL, ?, ?) "
                "ON CONFLICT(novel_id, order_key) DO UPDATE SET "
                "blob_key=excluded.blob_key, "
                "profile=excluded.profile, "
                "updated_at=excluded.updated_at"
            )
            params: tuple[Any, ...] = (int(novel_id), order_key, blob_key, profile, now, now)
        else:
            # Replace the hints group: present keys written, omitted -> NULL.
            sql = (
                "INSERT INTO chapters("
                " novel_id, order_key, blob_key, profile, media_type, charset, created_at, updated_at"
                ") VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(novel_id, order_key) DO UPDATE SET "
                "blob_key=excluded.blob_key, "
                "profile=excluded.profile, "
                "media_type=excluded.media_type, "
                "charset=excluded.charset, "
                "updated_at=excluded.updated_at"
            )
            params = (
                int(novel_id),
                order_key,
                blob_key,
                profile,
                hints.get("media_type"),
                hints.get("charset"),
                now,
                now,
            )
        conn.execute(sql, params)
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
        return chapter_id

    def get_chapter(self, chapter_id: int) -> sqlite3.Row | None:
        return self._query_one("SELECT * FROM chapters WHERE id=?", (_require_pk(chapter_id, "chapter_id"),))

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
        """Return the stored value for ``key``.

        A missing key and a stored JSON ``null`` both return ``None`` — the
        two are not distinguishable (use ``meta_list`` to tell them apart).
        """
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        row = self._query_one(
            "SELECT value_json FROM meta WHERE entity_type=? AND entity_id=? AND key=?",
            (entity_type, entity_id, str(key)),
        )
        return None if row is None else loads_config(str(row[0]), key=f"meta:{entity_type}:{entity_id}:{key}")

    def meta_list(self, entity_type: str, entity_id: int | str) -> dict[str, Any]:
        entity_type, entity_id = self._normalize_entity(entity_type, entity_id)
        rows = self._query_all(
            "SELECT key, value_json FROM meta WHERE entity_type=? AND entity_id=?",
            (entity_type, entity_id),
        )
        return {str(row[0]): loads_config(str(row[1]), key=f"meta:{entity_type}:{entity_id}") for row in rows}

    def iter_chapter_refs(self, scope: int | None = None) -> Iterator[ContentRef]:
        """Distinct ``(blob_key, profile)`` pairs referenced by chapters.

        Paged keyset iteration (C3): each page of 1000 rows is its own short
        read transaction (the ``idx_chapters_blob_profile`` index serves the
        ORDER BY), so a bulk drain never holds a read txn for its whole
        duration.

        Visibility: **at least that of a single-snapshot fetch taken at
        drain start** — a row present at the first page's snapshot sits at a
        fixed key, and the ascending keyset sweep cannot skip it, even under
        concurrent writers; late additions whose key sorts above the cursor
        appear in a later page; mid-drain deletions cause over-retention
        (safe for GC, which deletes what is *not* in the live set). Refs
        added mid-drain below the cursor are missed by that drain exactly as
        a start-of-drain snapshot would miss them.

        Live-set freshness obligation (normative for callers): the exposure
        window is **commits during or after the drain** — such chapters are
        not represented in the result, and ``gc`` will reclaim their content
        (recovery: re-run the upsert). See GUARANTEES.md.
        """
        scope = _require_scope(scope)
        last: tuple[str, str] | None = None
        while True:
            if scope is None:
                if last is None:
                    sql = (
                        "SELECT DISTINCT blob_key, profile FROM chapters "
                        "ORDER BY blob_key, profile LIMIT 1000"
                    )
                    params: tuple[Any, ...] = ()
                else:
                    sql = (
                        "SELECT DISTINCT blob_key, profile FROM chapters "
                        "WHERE (blob_key, profile) > (?, ?) "
                        "ORDER BY blob_key, profile LIMIT 1000"
                    )
                    params = (last[0], last[1])
            elif last is None:
                sql = (
                    "SELECT DISTINCT blob_key, profile FROM chapters "
                    "WHERE novel_id=? ORDER BY blob_key, profile LIMIT 1000"
                )
                params = (scope,)
            else:
                sql = (
                    "SELECT DISTINCT blob_key, profile FROM chapters "
                    "WHERE novel_id=? AND (blob_key, profile) > (?, ?) "
                    "ORDER BY blob_key, profile LIMIT 1000"
                )
                params = (scope, last[0], last[1])
            with self.txn(write=False) as conn:
                rows = conn.execute(sql, params).fetchall()
            if not rows:
                return
            for row in rows:
                yield ContentRef(blob_key=str(row[0]), profile=str(row[1]))
            last = (str(rows[-1][0]), str(rows[-1][1]))

    # -- maintenance (GC / vacuum) --------------------------------------------

    def _gc_batch_size(self, s: Session) -> int:
        """Shards per exclusive GC batch (B1): the session's attach capacity
        (monkeypatchable for the per-batch-commit test)."""
        return max(1, s.attach_limit())

    def _stage_temp_live(
        self, s: Session, live: Iterable[ContentRef], cancel: CancelToken | None
    ) -> None:
        """Stage the STATIC live-authorization list (autocommit TEMP writes,
        batched, cancellable). It outlives every batch: deadness is always
        measured against this list."""
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

        for ref in live:
            batch.append((ref.blob_key, ref.profile))
            if len(batch) >= _GC_BATCH:
                flush()
                check_cancel(cancel)
        flush()

    def gc_iter(
        self, live: Iterable[ContentRef], cancel: CancelToken | None = None
    ) -> Iterator[GcBatchSummary]:
        """Generator: run the batched gc, yielding a :class:`GcBatchSummary`
        after EACH batch's commit (``index`` 1..N, cumulative counts), then a
        FINAL summary (``index == 0``) carrying the full-run totals (including
        the final convergence sweep). The final summary is NOT a batch item —
        it carries the totals only. Does NOT delete orphan blobs /
        unreferenced dicts (the caller does those after the iterator
        exhausts, so it can build the ``GcResult``). Never yields from inside
        an exclusive window (consumer code never runs under the lock).

        **Batched exclusive windows** (Decision G amendment, B1). The live
        set is staged once (autocommit), then each batch of up to
        ``_gc_batch_size`` shards runs:

        1. ATTACH the batch's existing shards on the session connection;
        2. ``BEGIN IMMEDIATE`` — the reserved lock spans the index AND the
           batch's shards, so the snapshot and the deletions share one
           exclusive writer window;
        3. recompute ``temp_dead`` fresh INSIDE the window;
        4. delete the batch's dead payloads + encodings (NULL-locator
           payloads are cleaned from every batch shard; a vanished shard
           file yields an encodings-only delete — no file is ever created);
        5. sweep payloads parked at the wrong shard (Issue 18);
        6. commit — one atomic commit per batch; DETACH;
        7. (multi-batch runs only) one final idempotent whole-universe
           orphan sweep so the run converges (r3-A).

        Two invariants, stated separately:
        *Encoding-deletion:* gc deletes exactly the encodings dead as of a
        batch snapshot taken after any concurrent put completed — a put
        completing before the snapshot is authorizable there or in a later
        batch; a put racing the window blocks, lands after commit, and
        self-heals on its in-txn re-probe.
        *Orphan-payload cleanup:* each batch sweeps its own shards; a FINAL
        IDEMPOTENT SWEEP over the whole shard universe (multi-batch runs
        only) then removes any payload whose encoding died in another batch,
        so one GC run always converges to "no payload rows without matching
        encodings" (r3-A — the batch-restricted sweep alone left cross-batch
        residue for copies parked in EARLIER batches than their locator).
        The final sweep needs no exclusive window: a committing put writes
        encoding+payload atomically, so the sweep's statement snapshot sees
        both or neither. A shard file vanishing between the exists()
        pre-check and attach degrades to an encodings-only pass (r3-D),
        never aborting the run.

        Cancellation keeps committed batches; the in-flight batch rolls back
        whole.
        """
        encodings_deleted = 0
        payload_rows_deleted = 0
        with self.session() as s:
            check_cancel(cancel)
            try:
                # r3-F: staging is INSIDE the guarded region, so a cancel
                # (or any failure) mid-stage still drops the TEMP tables
                # instead of leaking them until session close.
                self._stage_temp_live(s, live, cancel)
                if self.mode == "sqlite_single":
                    pending: Sequence[int | None] = [None]
                else:
                    # The filesystem list PLUS every shard id encodings still
                    # reference: a vanished shard file must still get its
                    # encodings-only pass (never recreated).
                    referenced = {
                        int(r[0])
                        for r in s.query_all(
                            "SELECT DISTINCT shard_id FROM encodings WHERE shard_id IS NOT NULL"
                        )
                    }
                    pending = sorted(set(self.list_shards()) | referenced)
                batch_size = self._gc_batch_size(s)
                total_batches = (len(pending) + batch_size - 1) // batch_size
                batch_num = 0
                for i0 in range(0, len(pending), batch_size):
                    batch = pending[i0 : i0 + batch_size]
                    batch_num += 1
                    check_cancel(cancel)
                    aliases: list[tuple[str, int]] = []
                    try:
                        for sid in batch:
                            if sid is None:
                                continue  # single mode: the main DB holds payloads
                            if not self.shard_path(sid).exists():
                                continue  # vanished shard -> encodings-only
                            try:
                                aliases.append((s.attach(sid), sid))
                            except MissingContent:
                                # r3-D: the file vanished in the window between
                                # the exists() pre-check and attach()'s
                                # re-check — degrade to the encodings-only pass
                                # (the enc_sids re-check below sees it gone),
                                # never abort the whole run.
                                continue
                        try:
                            s.conn.execute("BEGIN IMMEDIATE")
                        except sqlite3.Error as exc:
                            _classify_db_error(exc, what="gc batch")
                        s.execute("DROP TABLE IF EXISTS temp_dead")
                        s.execute(
                            "CREATE TEMP TABLE temp_dead AS "
                            "SELECT e.blob_key, e.profile, e.shard_id FROM encodings e "
                            "WHERE NOT EXISTS (SELECT 1 FROM temp_live l WHERE "
                            "l.blob_key = e.blob_key AND l.profile = e.profile)"
                        )
                        failpoint("gc.post_snapshot")  # H1: inside the exclusive window
                        if self.mode == "sqlite_single":
                            payload_rows_deleted += s.execute(
                                "DELETE FROM payload WHERE (blob_key, profile) IN "
                                "(SELECT blob_key, profile FROM temp_dead)"
                            )
                            encodings_deleted += s.execute(
                                "DELETE FROM encodings WHERE (blob_key, profile) IN "
                                "(SELECT blob_key, profile FROM temp_dead)"
                            )
                        else:
                            for alias, sid in aliases:
                                payload_rows_deleted += s.execute(
                                    f"DELETE FROM {alias}.payload WHERE (blob_key, profile) IN "
                                    "(SELECT blob_key, profile FROM temp_dead WHERE shard_id = ?)",
                                    (sid,),
                                )
                            for alias, _ in aliases:
                                payload_rows_deleted += s.execute(
                                    f"DELETE FROM {alias}.payload WHERE (blob_key, profile) IN "
                                    "(SELECT blob_key, profile FROM temp_dead WHERE shard_id IS NULL)"
                                )
                            enc_sids = [sid for _, sid in aliases]
                            for sid in batch:
                                if sid is not None and not self.shard_path(sid).exists():
                                    enc_sids.append(sid)  # vanished: encodings-only
                            if enc_sids:
                                marks = ",".join("?" * len(enc_sids))
                                encodings_deleted += s.execute(
                                    f"DELETE FROM encodings WHERE shard_id IN ({marks}) AND "
                                    "(blob_key, profile) IN "
                                    "(SELECT blob_key, profile FROM temp_dead)",
                                    tuple(enc_sids),
                                )
                            encodings_deleted += s.execute(
                                "DELETE FROM encodings WHERE shard_id IS NULL AND "
                                "(blob_key, profile) IN (SELECT blob_key, profile FROM temp_dead)"
                            )
                            for alias, _ in aliases:  # wrong-shard sweep, batch-restricted
                                payload_rows_deleted += s.execute(
                                    f"DELETE FROM {alias}.payload WHERE NOT EXISTS ("
                                    f"SELECT 1 FROM encodings e WHERE e.blob_key = "
                                    f"{alias}.payload.blob_key "
                                    f"AND e.profile = {alias}.payload.profile)"
                                )
                        s.conn.commit()
                    except BaseException:
                        with suppress(sqlite3.Error):
                            s.conn.rollback()
                        raise
                    finally:
                        for _alias, sid in aliases:
                            s.detach(sid)
                    check_cancel(cancel)
                    # r4-P3.1: yield the per-batch summary (cumulative counts,
                    # 1-based index, total batches, this batch's shards) AFTER
                    # the batch's commit — never from inside the exclusive
                    # window.
                    yield GcBatchSummary(
                        index=batch_num,
                        batches=total_batches,
                        shards=tuple(int(sid) for sid in batch if sid is not None),
                        encodings_deleted=encodings_deleted,
                        payload_rows_deleted=payload_rows_deleted,
                    )
                # r3-A: one-run convergence. A shard swept in an EARLIER batch
                # may hold an orphan copy whose encoding died in a LATER batch
                # — that batch's restricted sweep saw the encoding still alive,
                # and the earlier shard is never revisited. One final idempotent
                # sweep over the whole shard universe removes any payload whose
                # encoding died in another batch. Safe WITHOUT an exclusive
                # window: a concurrently-committing put writes encoding+payload
                # atomically, so the sweep's statement snapshot sees
                # both-or-neither and never sweeps a live put's row. Skipped for
                # single-batch runs — the in-batch sweep already covered the
                # whole universe there (the common small-repo case pays zero).
                # (r4-P3.1: the final sweep is NOT a batch item — its
                # deletions accumulate into the final totals yield below.)
                if self.mode == "sqlite_sharded" and batch_size < len(pending):
                    for sid in self.list_shards():
                        check_cancel(cancel)
                        try:
                            alias = s.attach(sid)
                        except MissingContent:
                            continue  # r3-D: vanished mid-run — encodings-only
                        try:
                            s.conn.execute("BEGIN IMMEDIATE")
                            payload_rows_deleted += s.execute(
                                f"DELETE FROM {alias}.payload WHERE NOT EXISTS ("
                                f"SELECT 1 FROM encodings e WHERE e.blob_key = "
                                f"{alias}.payload.blob_key "
                                f"AND e.profile = {alias}.payload.profile)"
                            )
                            s.conn.commit()
                        except BaseException:
                            with suppress(sqlite3.Error):
                                s.conn.rollback()
                            raise
                        finally:
                            s.detach(sid)
                # r4-P3.1: final totals summary (index=0 sentinel). Carries the
                # full-run encodings/payloads totals (including the final
                # sweep). The caller reclaims orphan blobs + unreferenced dicts
                # AFTER this (so the final sweep's orphans are seen).
                yield GcBatchSummary(
                    index=0,
                    batches=total_batches,
                    shards=(),
                    encodings_deleted=encodings_deleted,
                    payload_rows_deleted=payload_rows_deleted,
                )
            finally:
                s.execute("DROP TABLE IF EXISTS temp_live")
                s.execute("DROP TABLE IF EXISTS temp_dead")

    def gc(
        self, live: Iterable[ContentRef], cancel: CancelToken | None = None
    ) -> tuple[int, int, int, int]:
        """Delete dead encodings + payloads, then orphan blobs + dicts;
        returns ``(encodings, payloads, blobs, dicts)``.

        Thin wrapper over :meth:`gc_iter` (which yields per-batch summaries
        and reclaims the dead encodings/payloads): this wrapper consumes the
        iterator, then reclaims orphan blobs + unreferenced dicts (each its
        own atomic txn, safe against the puts a committed batch authorized),
        and returns the full-run totals. See :meth:`gc_iter` for the batched
        exclusive-window invariants.
        """
        encodings_deleted = 0
        payload_rows_deleted = 0
        for summary in self.gc_iter(live, cancel):
            if summary.index == 0:  # final totals sentinel
                encodings_deleted = summary.encodings_deleted
                payload_rows_deleted = summary.payload_rows_deleted
        # Orphan blobs and unreferenced dicts are reclaimed only AFTER every
        # batch has committed (and the final sweep): each delete is its own
        # atomic txn and is safe against the puts a committed batch authorized.
        blobs_deleted = self.delete_orphan_blobs()
        dicts_deleted = self.delete_unreferenced_dicts()
        return encodings_deleted, payload_rows_deleted, blobs_deleted, dicts_deleted

    def delete_orphan_blobs(self) -> int:
        return self._execute(
            "DELETE FROM blobs WHERE NOT EXISTS ("
            "SELECT 1 FROM encodings e WHERE e.blob_key = blobs.blob_key)"
        )

    def delete_unreferenced_dicts(self) -> int:
        """Delete dicts not referenced by any encoding and not referenced by
        any profile in ``repo_config`` (spec §4.1 Option B).

        The profiles config is parsed STRICTLY inside the same transaction
        (review M11): a malformed config aborts GC with ``InkpackError``
        instead of best-effort treating all dicts as unreferenced.
        """
        with self.txn(write=True) as conn:
            row = conn.execute(
                "SELECT value_json FROM repo_config WHERE key='profiles'"
            ).fetchone()
            profile_refs: set[str] = set()
            if row is not None:
                raw = loads_config(str(row[0]), key="profiles")
                profiles = profiles_from_config(raw)  # strict; InkpackError on junk
                profile_refs = {
                    p.zstd_dict_id for p in profiles.values() if p.zstd_dict_id is not None
                }
            base = (
                "DELETE FROM dicts WHERE NOT EXISTS ("
                "SELECT 1 FROM encodings e WHERE e.zstd_dict_id = dicts.dict_id)"
            )
            if profile_refs:
                placeholders = ",".join("?" * len(profile_refs))
                base += f" AND dict_id NOT IN ({placeholders})"
            return int(conn.execute(base, tuple(sorted(profile_refs))).rowcount)

    def vacuum_index(self) -> str:
        return self._vacuum(self.index_path)

    def vacuum_shard(self, shard_id: int) -> str:
        return self._vacuum(self.shard_path(shard_id))

    def _vacuum(self, path: Path) -> str:
        # VACUUM must run on a connection with no ATTACHed databases (decision H).
        if not path.exists():
            raise MissingContent(f"cannot vacuum missing database {path}")
        conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
        try:
            conn.execute("VACUUM")
        except sqlite3.Error as exc:
            # A corrupt DB surfaces here as DatabaseError ("file is not a
            # database" / "database disk image is malformed") — broader than
            # the old OperationalError-only catch, routed through the shared
            # classifier so compact() never leaks raw sqlite (A1).
            _classify_db_error(exc, what=f"vacuum failed for {path}")
        finally:
            conn.close()
        return str(path)

    # -- shard routing ---------------------------------------------------------

    def shard_path(self, shard_id: int) -> Path:
        if self.payload_dir is None:
            raise ValueError("sqlite_single mode has no shards")
        return self.payload_dir / f"shard-{shard_id:04d}.sqlite"

    def list_shards(self) -> list[int]:
        """Shard ids present on disk (locked decisions S9 + D2).

        Only canonical ``shard-{id:04d}.sqlite`` files count; junk filenames
        are ignored (S9) and non-canonical ``shard-<digits>.sqlite`` names or
        duplicate numeric ids are recorded as offenders (D2) so open refuses
        them with a rename hint.
        """
        if self.payload_dir is None:
            return []
        shard_ids: list[int] = []
        seen: dict[int, str] = {}
        for path in sorted(self.payload_dir.glob("shard-*.sqlite")):
            match = _SHARD_FILE_RE.match(path.name)
            if match is None:
                continue
            sid = int(match.group(1))
            if path.name != shard_filename(sid) or sid in seen:
                continue  # offenders are reported by _shard_name_offenders()
            seen[sid] = path.name
            shard_ids.append(sid)
        return shard_ids

    def _shard_name_offenders(self) -> list[str]:
        """Re-scan the payload dir and return non-canonical shard names."""
        if self.payload_dir is None:
            return []
        offenders: list[str] = []
        seen: dict[int, str] = {}
        for path in sorted(self.payload_dir.glob("shard-*.sqlite")):
            match = _SHARD_FILE_RE.match(path.name)
            if match is None:
                continue
            sid = int(match.group(1))
            if path.name != shard_filename(sid) or sid in seen:
                offenders.append(path.name)
            seen[sid] = path.name
        return offenders

    def _shard_usable(self, shard_id: int) -> bool:
        """P1.2: is this shard file a usable SQLite DB (for WRITE routing)?

        Missing -> False (never create, S2). Present-but-unusable (junk /
        corrupt header or corrupt payload page) -> False, so a junk *current*
        shard does not brick new writes — the write rolls to ``max+1``.
        ``Busy`` is re-raised, NOT treated as unusable: a locked-but-healthy
        shard must not be rolled off. ``Busy`` subclasses ``InkpackError``,
        so the ``Busy`` catch MUST precede the broad one (else "Busy
        propagates" silently dies). A process-lifetime ``_usable_shards``
        cache means healthy writes pay zero extra connections.

        The probe queries the ``payload`` table (not just ``sqlite_master``)
        so it catches page-level corruption of the payload btree, not just a
        corrupt header. A valid DB that simply lacks the payload table yet
        (unmigrated) is USABLE — it is migrated on write, not corrupt.
        """
        if shard_id in self._usable_shards:
            return True
        path = self.shard_path(shard_id)
        if not path.exists():
            return False
        try:
            conn = connect_file(path, self.busy_timeout_ms, self.synchronous)
        except Busy:
            raise  # locked-but-healthy: do NOT roll off
        except (InkpackError, OSError):
            return False  # present-but-unusable (corrupt header / unreadable)
        try:
            conn.execute("SELECT 1 FROM payload LIMIT 1")
        except sqlite3.Error as exc:
            if _is_busy(exc):
                raise Busy(str(exc)) from exc
            if "no such table" in str(exc).lower():
                # Valid DB, payload table not migrated yet -> usable (migrated
                # on write), not corrupt.
                self._usable_shards.add(shard_id)
                return True
            return False  # present-but-unusable (corrupt payload page)
        finally:
            with suppress(sqlite3.Error):
                conn.close()
        self._usable_shards.add(shard_id)
        return True

    def resolve_write_shard(self, stored_len: int, current: int | None) -> int:
        """Pick the shard a payload write lands in (locked semantics S2/H2).

        Returns ``current`` when it is a usable existing shard; otherwise
        explicitly chooses AND ensures a writable shard (rehoming) — never
        leaves creation to ATTACH. P1.2: "usable" includes present-but-not-
        junk — a corrupt current shard is rehomed off, not bricked.
        """
        if self.mode != "sqlite_sharded":
            return 0
        if current is None or not self._shard_usable(current):
            shard_id = self.choose_shard_for_write(stored_len)
            self.ensure_shard_exists(shard_id)
            return shard_id
        return current

    def ensure_shard_exists(self, shard_id: int) -> None:
        if self.payload_dir is None:
            return
        # Creating a higher-numbered shard moves the write pointer (review §8).
        if self._write_shard_id is None or shard_id > self._write_shard_id:
            self._write_shard_id = shard_id
        _ensure_shard_file(self.payload_dir, shard_id, self.busy_timeout_ms, self.synchronous)
        # NOTE: a freshly ensured shard is trusted only WITHIN the same
        # choose/resolve call (via the caller's ``just_ensured``), NOT added
        # to the process-lifetime ``_usable_shards`` cache — a shard file can
        # be externally corrupted after creation, and a stale "usable" cache
        # entry would then brick writes instead of rolling off (P1.2).

    def choose_shard_for_write(self, estimated_payload_bytes: int) -> int:
        """Pick the write shard: the highest existing id, rolling past the cap.

        ``max(ids)`` (not the lexically-last path) keeps shard-10000 from
        sorting before shard-9999 (review §8). The write pointer is cached on
        the instance; the glob remains the source for compact/validation.

        P1.2: a present-but-unusable (junk) current shard rolls to the next
        shard — N5/N6: the pointer only moves forward, and junk behind it is
        never revisited (runaway bound: each put rolls at most one shard). A
        shard ensured earlier in THIS call is trusted (not re-probed); a
        pre-existing pointer is probed via ``_shard_usable`` (cached if
        healthy). P1.5: the ``stat()`` is TOCTOU-safe — a vanish between
        discovery and ``stat()`` recreates the (harmless, N5) empty shard
        instead of leaking ``FileNotFoundError`` out of put (D9).
        """
        if self.mode != "sqlite_sharded":
            return 0
        just_ensured = False
        if self._write_shard_id is None:
            shard_ids = self.list_shards()
            self._write_shard_id = max(shard_ids) if shard_ids else 1
            self.ensure_shard_exists(self._write_shard_id)
            just_ensured = True
        current = self._write_shard_id
        if not just_ensured and not self._shard_usable(current):
            current += 1
            self.ensure_shard_exists(current)
            self._write_shard_id = current
            return current
        try:
            current_size = self.shard_path(current).stat().st_size
        except FileNotFoundError:
            self.ensure_shard_exists(current)
            current_size = 0
        if current_size + estimated_payload_bytes > self.shard_cap_bytes:
            current += 1
            self.ensure_shard_exists(current)
            self._write_shard_id = current
        return current
