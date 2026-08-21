# lmarena_chat_2026-08-21T09-13-09

[User]
```
# Inkpack — Final Specification (Text-only, SQLite Single/Sharded, fast + practical)

This document defines Inkpack’s **public API contract** and **persistent storage contract**. It intentionally avoids prescribing internal architecture beyond what is required for correctness, interoperability, and durability.

---

## 1) Purpose

Inkpack is a local storage library for **text novel libraries**. It stores chapter bodies as **byte-preserving canonical content** (no forced UTF‑8, no forced HTML), supports **random updates** (user edits), **dictionary-based recompression** (zstd dictionaries), **content dedupe**, and **custom KV metadata** on novels and chapters.

Supported backends (only):
- **sqlite_single**: one SQLite database file.
- **sqlite_sharded**: one index DB plus multiple payload shard DBs (2 GiB default cap).

Not supported:
- pack/VMDK segment formats,
- images/video/transforms,
- group codecs.

---

## 2) Philosophy / design priorities

Inkpack optimizes for:
- **Correctness and robustness** over cleverness.
- **Small public API** (no bloat).
- **Practical performance** for large personal libraries (multi-GB to tens of GB).
- **Simple maintenance** (GC/verify/vacuum; no persistent job framework).

---

## 3) Core Concepts

### 3.1 Canonical bytes
Canonical content is the exact bytes provided by the user/application for a chapter body. Inkpack MUST NOT transcode or normalize canonical bytes.

### 3.2 Identity and dedupe key (`blob_key`)
Inkpack uses a repo-level identity policy. Default policy `ikb1` computes in one streaming pass:
- `raw_len`
- `sha256(raw_bytes)` (hex)
- `blake2b(raw_bytes, digest_size=16)` (hex)

`blob_key` is an opaque, versioned string:
```
ikb1:<raw_len>:<sha256_hex>:<blake2b128_hex>
```

**Dedupe rule (MUST):** identical canonical bytes MUST map to identical `blob_key`. Repos MUST enforce uniqueness of `blob_key`.

### 3.3 ContentRef
Inkpack references stored content as:
```
ContentRef = (blob_key, profile)
```
Where:
- `blob_key` identifies canonical bytes
- `profile` selects an encoding policy (codec/params/dict) for storage

### 3.4 Profiles
A profile is a named encoding policy. Profiles are persisted in repo config and are used as defaults for new writes and for deciding what `reencode()` should target.

**Important (MUST):** Profiles are *not* the source of truth for decoding already-stored content. (See §6.2.)

### 3.5 Encoding model (simplified)
There is exactly **one stored encoding per `(blob_key, profile)`**:
- no encoding_id
- no “active pointer” table
- no multi-encoding retention by default

Recompression/reencode updates that one encoding in place.

---

## 4) Public API Contract (minimal)

### 4.1 Progress events
Long operations yield `OpEvent` objects:

- `kind`: `start|phase|progress|item|log|error|done`
- `op`: `put|reencode|train_dict|verify|gc|compact|...`
- `phase`: optional string
- `message`: optional string
- `metrics`: dict (optional keys: `bytes_in`, `bytes_out`, `items_done`, `items_total`, `elapsed_s`, etc.)

Percent is a presentation concern and is not required.

### 4.2 Operation object
Long calls return an `Operation[T]`:
- iterable yielding `OpEvent`
- exposes `.result` on completion (or raises)

### 4.3 BlobStore API
Core:
- `put_bytes(data: bytes, profile: str, cancel=None) -> Operation[PutResult]`
- `put_stream(fp, profile: str, size_hint=None, cancel=None) -> Operation[PutResult]`
- `get_bytes(ref: ContentRef) -> bytes`
- `open(ref: ContentRef) -> BinaryIO`
- `has_blob(blob_key: str) -> bool`

Maintenance (exclusive writer operations):
- `verify(limit=None, cancel=None) -> Operation[VerifyResult]`
- `gc(live: Iterable[ContentRef], cancel=None) -> Operation[GcResult]`
- `compact(options=None, cancel=None) -> Operation[CompactResult]`

Compression tools:
- `train_dict(samples: Iterable[bytes], options=None, cancel=None) -> Operation[TrainDictResult]`
- `reencode(targets: Iterable[ContentRef], options=None, cancel=None) -> Operation[ReencodeResult]`

### 4.4 Repository API (text-novel model)
- `create_novel(title: str, meta: dict|None=None) -> novel_id`
- `list_novels(search: str="") -> list`
- `upsert_chapter(novel_id, chapter_key/order, body_bytes, profile, hints/meta...) -> chapter_id`
- `get_chapter_bytes(chapter_id) -> bytes`
- `open_chapter(chapter_id) -> BinaryIO`
- KV metadata:
  - `meta_set(entity_type, entity_id, key, value)`
  - `meta_get(entity_type, entity_id, key)`
  - `meta_list(entity_type, entity_id)`
- `iter_live_content(scope=...) -> Iterable[ContentRef]`

---

## 5) Result type minimum fields (contract)

These are minimum required fields; implementations may add more.

- `PutResult`:
  - `ref: ContentRef`
  - `raw_len: int`
  - `stored_len: int`
  - `codec: str`
  - `zstd_dict_id: str|None`

- `TrainDictResult`:
  - `dict_id: str`
  - `dict_size: int`
  - `samples_used: int`
  - `sample_bytes: int`

- `VerifyResult`:
  - `checked: int`
  - `ok: int`
  - `missing: int`
  - `corrupt: int`

- `GcResult`:
  - `encodings_deleted: int`
  - `payload_rows_deleted: int`
  - `blobs_deleted: int`
  - `dicts_deleted: int`

- `CompactResult`:
  - `mode: str` (e.g. `"vacuum"`)
  - `targets: list[str]` (which DB(s) were vacuumed)

---

## 6) Read Semantics (locked)

### 6.1 Stable reads
Inkpack provides a single read mode:

- `open(ref)` MUST remain valid until closed.

### 6.2 Decoding source of truth (MUST)
Decoding MUST use the **stored encoding metadata** from the persistence layer:
- `encodings.codec`
- `encodings.codec_params_json`
- `encodings.zstd_dict_id` (if applicable)

Decoding MUST NOT depend on current profile definitions, except to locate the profile name string (the `profile` in `ContentRef`).

Rationale: profile definitions may change over time; stored content must remain decodable.

### 6.3 `open()` implementation guarantee (MUST)
To guarantee stability without long-lived read transactions:

- `open(ref)` MUST be implemented as `io.BytesIO(get_bytes(ref))` (or equivalent fully materialized decoded bytes).

### 6.4 Integrity checks on read
- `get_bytes/open` MUST decompress successfully or raise `CorruptContent`.
- `get_bytes/open` SHOULD NOT recompute identity hashes by default (performance).
- Optional repo config: `verify_on_read` (default false).

Full identity verification is performed by `verify()`.

---

## 7) Persistence Contract (SQLite schema + semantics)

### 7.1 Repo config
Index DB MUST contain:
- `repo_config(key TEXT PRIMARY KEY, value_json TEXT NOT NULL)`

Required keys (see §4). Profiles must be stored here (or via a `profiles` table referenced by config; but config is normative).

### 7.2 Entity ID normalization for KV metadata (MUST)
In `meta`, `entity_id` MUST be the decimal string of the integer primary key, and `entity_type` must disambiguate (`"novel"`, `"chapter"`).

### 7.3 Tables (index DB, normative)

Repository:
- `novels(id INTEGER PRIMARY KEY, title TEXT NOT NULL, slug TEXT, created_at TEXT, updated_at TEXT)`
- `chapters(id INTEGER PRIMARY KEY,
            novel_id INTEGER NOT NULL,
            order_key TEXT,
            blob_key TEXT NOT NULL,
            profile TEXT NOT NULL,
            media_type TEXT,
            charset TEXT,
            created_at TEXT, updated_at TEXT)`
- `meta(entity_type TEXT, entity_id TEXT, key TEXT, value_json TEXT, updated_at TEXT,
       PRIMARY KEY(entity_type, entity_id, key))`

Identity + encoding metadata:
- `blobs(blob_key TEXT PRIMARY KEY, raw_len INTEGER NOT NULL, created_at TEXT NOT NULL)`
- `encodings(blob_key TEXT NOT NULL,
            profile TEXT NOT NULL,
            codec TEXT NOT NULL,
            codec_params_json TEXT NOT NULL,   -- MUST be canonical JSON
            zstd_dict_id TEXT,                  -- MUST be NULL unless codec=zstd and dict used
            stored_len INTEGER NOT NULL,         -- MUST equal length of payload.data bytes
            checksum TEXT,                       -- OPTIONAL; see §7.6
            shard_id INTEGER,                    -- NULL for sqlite_single
            updated_at TEXT NOT NULL,
            PRIMARY KEY(blob_key, profile))`

Dictionaries (index DB only):
- `dicts(dict_id TEXT PRIMARY KEY,
         codec TEXT NOT NULL,
         dict_bytes BLOB NOT NULL,
         params_json TEXT,
         created_at TEXT NOT NULL)`

### 7.4 Payload table (sqlite_single and shard DBs, normative)
Payload bytes are stored in a dedicated table:

- `payload(blob_key TEXT NOT NULL,
          profile TEXT NOT NULL,
          data BLOB NOT NULL,
          PRIMARY KEY(blob_key, profile))`

#### Payload semantic meaning (MUST)
`payload.data` stores the **encoded/compressed bytes** of the canonical content identified by `blob_key`, encoded according to the corresponding row in `encodings` for `(blob_key, profile)`.

`get_bytes/open` MUST decode `payload.data` using the stored encoding metadata.

### 7.5 In-place update rule (MUST)
When `reencode()` updates an existing `(blob_key, profile)`:
- It MUST update `payload.data` in place for that same key.
- In the **same transaction**, it MUST update the corresponding `encodings` row fields:
  - `codec`, `codec_params_json`, `zstd_dict_id`, `stored_len`, `updated_at` (and optionally checksum)

This preserves consistency and avoids needing encoding IDs.

### 7.6 Checksum semantics (OPTIONAL, locked if present)
`encodings.checksum` is OPTIONAL. If implemented:
- it SHOULD be a hash of the encoded payload bytes (`payload.data`) (e.g. sha256 hex),
- and MAY be used by `verify()` as an additional check.
If not implemented, it MUST be NULL and ignored.

### 7.7 Required indexes
- `chapters(blob_key, profile)` (required for live ref scanning)
- `encodings(zstd_dict_id)` (recommended for dict GC)
- Additional indexes for listing/searching novels/chapters are allowed.

### 7.8 Migrations
- Schema versioning uses `PRAGMA user_version`.
- Migrations are forward-only and idempotent.

---

## 8) Backends and operational PRAGMA policy

### 8.1 sqlite_single
Recommended:
- `journal_mode=WAL`
- `synchronous=NORMAL` (configurable)
- `foreign_keys=ON`
- Default `auto_vacuum=NONE` (see §10.3)

### 8.2 sqlite_sharded
Layout:
```
repo/
  index.sqlite
  payload/shard-0001.sqlite ...
```

Shard sizing:
- default cap: 2 GiB
- configurable; min 256 MiB

**Atomicity (MUST):**
- Writes that modify index + shard MUST be performed using one connection to `index.sqlite` with `ATTACH shard`.
- To guarantee atomic cross-DB commits, index and all shards MUST use rollback journal mode permanently:
  - `journal_mode=DELETE` (or `TRUNCATE`)
- Inkpack MUST validate/enforce this at creation/open.

**Shard uniqueness (MUST):**
- A given `(blob_key, profile)` payload row exists in at most one shard.
- `encodings.shard_id` is authoritative for locating it.

Recommended:
- `foreign_keys=ON` in index DB
- `synchronous=NORMAL` default (configurable)
- `busy_timeout` recommended nonzero (implementation choice; may be documented)

---

## 9) Dictionaries

- Dicts are stored only in index DB.
- `zstd_dict_id` MUST reflect actual dict usage.
- `gc()` MUST remove unreferenced dicts by default.

---

## 10) Maintenance semantics (locked)

### 10.1 verify()
`verify()` MUST:
- decode/decompress canonical bytes for each target ContentRef
- recompute identity under repo identity policy `ikb1`
- compare `raw_len`, `sha256`, `blake2b-128`
- report ok/missing/corrupt counts

### 10.2 gc(live_refs)
`gc(live)` MUST:
- delete encodings not in live set
- delete corresponding payload rows
- delete blobs with no remaining encodings (default)
- delete dicts not referenced by any encoding (default)

GC is a writer operation; if DB is locked/timeout, raise `Busy`.

### 10.3 compact()
Default repo policy: `auto_vacuum=NONE` (optimal for fast writes).

- `compact()` performs `VACUUM` on:
  - sqlite_single: the single DB
  - sqlite_sharded: index and/or selected shards (implementation may vacuum all by default; options may restrict)

If repo was created with `auto_vacuum=INCREMENTAL`, compact MAY use incremental vacuum steps instead (optional creation-time override).

---

## 11) Error model (typed)
Inkpack MUST define:
- `NotFound` / `MissingContent`
- `CorruptContent`
- `Busy`
- `Cancelled`
```


════════════════════════════════════════════════════════════════

[gpt-5.2-high]
## Optimal design (best blend of Dev 1/2/3): **6-module flat package**, injected persistence + codec/identity, single+sharded unified backend

### File layout (flat, no subpackages, no file spam)

```
inkpack/
  __init__.py      # factories + re-exports (public surface)
  types.py         # public contract: types, results, OpEvent/Operation, errors, Profile
  codec.py         # ikb1 identity + canonical_json + encode/decode + zstd dict training
  sqlite.py        # schema+migrations + SqliteBackend (single|sharded; payload router)
  blobstore.py     # BlobStore implementation (put/get/open/verify/gc/compact/reencode/train_dict)
  repo.py          # Repository (novels/chapters/meta/live refs)
```

This keeps Dev 2’s compactness, Dev 3’s clarity about config/profile semantics, and Dev 1’s emphasis on “contract first” + stable reads.

---

## Responsibilities (what lives where)

### `types.py` — **public API contract objects** (no I/O, no sqlite, no zstd)
Owns:
- `ContentRef(blob_key: str, profile: str)`
- `OpEvent(kind, op, phase=None, message=None, metrics=None)`
- `Operation[T]`: iterable of `OpEvent`, `.result` on completion/raises
- Result dataclasses (minimum fields per spec): `PutResult`, `TrainDictResult`, `VerifyResult`, `GcResult`, `CompactResult`
- Errors: `NotFound` / `MissingContent`, `CorruptContent`, `Busy`, `Cancelled`
- `Profile` (write policy only): name, codec, params (as obj), optional `zstd_dict_id`
- Small DI helper protocols (optional but useful):
  - `CancelToken` (callable or `.is_cancelled()`)
  - `Clock` (`now_iso()`)

### `codec.py` — **identity + codec engine**
Owns:
- `ikb1` streaming identity:
  - `blob_key_ikb1_stream(fp, size_hint=None) -> (blob_key, raw_len)`
  - `blob_key_ikb1_bytes(data: bytes) -> (blob_key, raw_len)`
- `canonical_json(obj) -> str` (stable canonical JSON for `codec_params_json`)
- `CodecEngine` + default implementation:
  - `encode(raw: bytes, profile: Profile, dict_bytes: bytes|None) -> (codec, codec_params_json, zstd_dict_id, encoded_bytes)`
  - `decode(encoded: bytes, *, codec, codec_params_json, dict_bytes) -> raw_bytes`  **(never takes Profile)**
  - `train_dict(samples, options) -> (dict_bytes, params_json, TrainDictResult fields)`

### `sqlite.py` — **persistence + schema + migrations**, single/sharded in one backend
Owns:
- Schema DDL + `PRAGMA user_version` forward-only idempotent migrations
- PRAGMA policy enforcement:
  - single: recommend WAL/NORMAL
  - sharded: MUST enforce rollback journal (`DELETE` or `TRUNCATE`) for atomic cross-DB `ATTACH` commits
- `class SqliteBackend` (single|sharded):
  - Opens/validates repo layout
  - Provides transaction context manager(s)
  - Repo config I/O: `repo_config` get/set (profiles, verify_on_read, shard cap, identity_policy)
  - Content tables I/O (atomic invariants):
    - `blobs`, `encodings`, `payload` (and shards)
    - dictionaries: `dicts` in index only
  - Shard routing:
    - chooses shard on write under cap
    - ensures `(blob_key, profile)` exists in at most one shard
    - `encodings.shard_id` authoritative locator

> Key rule: `sqlite.py` is the only place that “knows SQL”.

### `blobstore.py` — **BlobStore API + maintenance ops** (spec §4.3 + §10)
Owns:
- `class BlobStore` that depends on:
  - `backend: SqliteBackend` (injected)
  - `codec: CodecEngine` (injected; default provided)
  - `identity` function/policy (injected; default `ikb1`)
  - `clock`, `cancel` handling (optional)
- Implements:
  - `put_bytes`, `put_stream` (hash -> blob_key -> encode via *current* Profile -> upsert blob+encoding+payload)
  - `get_bytes(ref)` / `open(ref)`:
    - load `encodings.*` + payload bytes (+ dict bytes if needed)
    - decode using stored encoding metadata (never current profile defs)
    - `open(ref) MUST be BytesIO(get_bytes(ref))` for stable reads
  - `verify()`: decode + recompute ikb1 + compare (ok/missing/corrupt)
  - `reencode()`: decode using stored metadata; encode to target; **in-place update** payload+encodings in same txn
  - `gc(live_refs)`: delete unreferenced encodings/payload/blobs/dicts
  - `compact()`: VACUUM target DBs

All long ops return `Operation[T]` and emit `OpEvent`.

### `repo.py` — **text-novel model** (spec §4.4)
Owns:
- `class Repository` depends on:
  - `backend: SqliteBackend` (for novels/chapters/meta tables)
  - `store: BlobStore` (for content bytes)
- Implements:
  - `create_novel`, `list_novels`
  - `upsert_chapter(...)`: store bytes -> persist chapter row pointing to `(blob_key, profile)`
  - `get_chapter_bytes/open_chapter` via `ContentRef`
  - `meta_set/get/list` with required entity_id normalization (decimal string PK)
  - `iter_live_content(scope=...) -> Iterable[ContentRef]` (used for GC)

### `__init__.py` — **composition root + re-exports**
Owns:
- `create_repo(...)` / `open_repo(...)`:
  - builds `SqliteBackend.open(...)`
  - loads/validates `repo_config`:
    - profiles exist
    - identity_policy matches (default enforce `"ikb1"`; allow override only if explicitly supported)
  - constructs default `CodecEngine`, identity policy, BlobStore, Repository
- Re-export public classes/types/errors for ergonomic imports

---

## Dependency injection (minimal, practical)

Construction flow:

```
backend = SqliteBackend.open(path, mode="sqlite_single"|"sqlite_sharded", pragmas=..., clock=...)
codec   = DefaultCodecEngine()          # swappable
ident   = blob_key_ikb1_stream          # swappable, but repo_config must match
store   = BlobStore(backend, codec=codec, identity=ident)
repo    = Repository(backend, store)
```

- Tests can swap `codec` (e.g., “none” only), swap identity for deterministic fixtures, and point backend to temp dirs.
- No global state, no DI framework, no extra `di.py`.

---

## Import graph (kept acyclic)
- `types.py` imports nothing internal
- `codec.py` imports `types.Profile` (and stdlib/zstd)
- `sqlite.py` imports `types` (and stdlib sqlite/json)
- `blobstore.py` imports `types`, `codec`, `sqlite`
- `repo.py` imports `types`, `sqlite`, `blobstore`
- `__init__.py` imports `repo`, `blobstore`, `sqlite`, `types`

---

## Key spec “locks” explicitly preserved
- Canonical bytes never transcoded
- `blob_key` uniqueness and dedupe enforced in DB
- Decode uses stored `encodings.{codec, codec_params_json, zstd_dict_id}` (not profile definitions)
- `open(ref) == BytesIO(get_bytes(ref))` to avoid long-lived read txns
- `reencode()` updates `(blob_key, profile)` payload + encodings **in-place and in one transaction**
- Sharded mode uses `ATTACH` and enforces rollback journal for atomic multi-DB commits

This is about as small as you can make it while still being clean, testable, and faithful to the durability + decoding rules.

════════════════════════════════════════════════════════════════

[gpt-5.2-high]
# Inkpack — Finalized Implementation Doc (6 files, flat, spec-compliant)

This document is the concrete implementation blueprint for the agreed 6-module flat package. It addresses prior gaps (notably **`ReencodeResult`**, **sharded GC atomicity**, and **dedupe-hit PutResult metadata**), and explicitly states the rules required to meet the spec.

Package layout:

```
inkpack/
  __init__.py
  types.py
  codec.py
  sqlite.py
  blobstore.py
  repo.py
```

---

## Cross-cutting compliance rules (apply everywhere)

1. **Canonical bytes are opaque bytes**: never decode/normalize/transcode chapter bodies (§3.1).
2. **Identity policy is repo-level and MUST be enforced**:
   - Repo must store `repo_config["identity_policy"] == "ikb1"` and validate on open (§3.2).
   - Implementations may allow other policies only if explicitly supported; otherwise mismatch MUST raise (e.g. `InkpackError`).
3. **Decode source of truth is stored encoding metadata**:
   - All reads decode strictly from `encodings.codec`, `encodings.codec_params_json`, `encodings.zstd_dict_id` (§6.2).
   - Profile definitions are never used to decode.
4. **Stable reads**:
   - `open(ref)` MUST return `io.BytesIO(get_bytes(ref))` (fully materialized) (§6.3).
5. **Writer operations should translate SQLite busy/locked to `Busy`**:
   - `gc`, `compact`, `reencode`, `train_dict` (writes dicts), and any operation that holds write locks should raise `Busy` on lock timeout (§10.2 mentions GC explicitly; apply consistently).
6. **Cancellation**:
   - Long loops check `cancel` and raise `Cancelled` (contract requirement via API cancel params).

---

# 1) `types.py` — Public contract objects (spec §4, §5, §11)

### Contents (no sqlite/zstd imports)
**Core types**
- `@dataclass(frozen=True) class ContentRef:`
  - `blob_key: str`
  - `profile: str`

**Progress events**
- `@dataclass class OpEvent:`
  - `kind: Literal["start","phase","progress","item","log","error","done"]`
  - `op: str` (e.g. `"put"`, `"gc"`)
  - `phase: str|None = None`
  - `message: str|None = None`
  - `metrics: dict|None = None`

**Operation**
- `class Operation(Generic[T])`:
  - wraps an iterator/generator yielding `OpEvent`
  - stores final result or exception
  - `__iter__()` yields events
  - `.result` returns `T` after completion (or raises)

**Result dataclasses (spec §5 minimum fields)**
- `PutResult`:
  - `ref: ContentRef`
  - `raw_len: int`
  - `stored_len: int`
  - `codec: str`
  - `zstd_dict_id: str|None`
- `TrainDictResult`:
  - `dict_id: str`
  - `dict_size: int`
  - `samples_used: int`
  - `sample_bytes: int`
- `VerifyResult`:
  - `checked: int`
  - `ok: int`
  - `missing: int`
  - `corrupt: int`
- `GcResult`:
  - `encodings_deleted: int`
  - `payload_rows_deleted: int`
  - `blobs_deleted: int`
  - `dicts_deleted: int`
- `CompactResult`:
  - `mode: str`
  - `targets: list[str]`

**Additional operation result (required by API even if spec §5 doesn’t list fields)**
- `ReencodeResult` (minimal but useful):
  - `targets: int`
  - `reencoded: int`
  - `skipped: int`
  - `bytes_in: int`
  - `bytes_out: int`

**Errors (spec §11)**
- `InkpackError`, `NotFound`, `MissingContent`, `CorruptContent`, `Busy`, `Cancelled`

**Profile (write policy only; not decoding truth)**
- `@dataclass(frozen=True) class Profile:`
  - `name: str`
  - `codec: str` (`"none"` or `"zstd"`)
  - `params: dict`
  - `zstd_dict_id: str|None = None`

**Optional tiny DI protocols**
- `CancelToken`: callable returning bool (True => cancel requested)
- `Clock`: `now_iso() -> str`

### Spec compliance
- §4.1–§4.2: `OpEvent`, `Operation`.
- §5: required result fields present.
- §11: typed errors.
- §3.4: Profile exists as write policy only.

---

# 2) `codec.py` — `ikb1` identity + canonical JSON + codecs (spec §3.2, §6.2, §7.3, §9)

### Contents
**Canonical JSON**
- `def canonical_json(obj) -> str`
  - canonicalizes only what must be canonical:
    - `encodings.codec_params_json` MUST be canonical (§7.3)
  - use stable `sort_keys=True` and `separators=(",", ":")`

**Identity (`ikb1`)**
- `def blob_key_ikb1_stream(fp, size_hint=None, cancel=None) -> tuple[str,int,str,str]`
  - single pass over input stream:
    - compute raw_len
    - compute sha256 hex
    - compute blake2b digest_size=16 hex
  - returns `(blob_key, raw_len, sha256_hex, blake2b128_hex)`
- `def blob_key_ikb1_bytes(data: bytes) -> tuple[str,int,str,str]`
- `def parse_blob_key_ikb1(blob_key: str) -> tuple[int,str,str]`
  - extracts `(raw_len, sha256_hex, blake2b128_hex)`; raises on format mismatch

**CodecEngine**
- `@dataclass(frozen=True) class Encoded:`
  - `codec: str`
  - `codec_params_json: str`
  - `zstd_dict_id: str|None`
  - `data: bytes`
  - `stored_len: int`

- `class CodecEngine:`
  - `encode(raw: bytes, profile: Profile, dict_bytes: bytes|None) -> Encoded`
    - if profile.codec == `"none"`: `data = raw`, params `{}` canonical
    - if `"zstd"`: compress using params + optional dict
  - `decode(encoded: bytes, *, codec: str, codec_params_json: str, dict_bytes: bytes|None) -> bytes`
    - **never accepts Profile** (enforces §6.2)
  - `train_dict(samples: Iterable[bytes], options=None, cancel=None) -> tuple[bytes, str|None, int, int]`
    - returns `(dict_bytes, params_json, samples_used, sample_bytes)`

**Dict ID**
- `def dict_id_for_bytes(dict_bytes: bytes) -> str`
  - stable: e.g. `"ikd1:" + sha256(dict_bytes).hexdigest()` (string format is internal; only requires opaque id in schema)

### Spec compliance
- §3.2: `ikb1` blob_key format and streaming computation.
- §7.3: canonical JSON for codec params.
- §6.2: decode uses stored metadata, not profiles.
- §9: zstd dict support.

---

# 3) `sqlite.py` — Schema+migrations + unified backend (single/sharded) (spec §7, §8, §7.8)

This module owns **all SQL** and ensures all durability/atomicity rules.

## 3.1 Migrations and DDL
### Must provide
- `LATEST_USER_VERSION: int`
- `def migrate_index(conn) -> None`
- `def migrate_payload(conn) -> None` (for sqlite_single DB and for each shard DB)
- forward-only, idempotent migrations using `PRAGMA user_version`

### Index DB tables (normative, exactly as spec)
- `repo_config(key TEXT PRIMARY KEY, value_json TEXT NOT NULL)`
- `novels(...)`
- `chapters(...)`
- `meta(...)`
- `blobs(...)`
- `encodings(...)`
- `dicts(...)`

### Payload DB table (normative)
- `payload(blob_key TEXT NOT NULL, profile TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(blob_key, profile))`

### Required indexes
- `chapters(blob_key, profile)` (MUST)
- `encodings(zstd_dict_id)` (recommended)

## 3.2 PRAGMA policy enforcement
### sqlite_single (recommended)
- set/recommend on open:
  - `journal_mode=WAL`
  - `synchronous=NORMAL` (configurable)
  - `foreign_keys=ON`
- creation-time policy:
  - `auto_vacuum=NONE` by default (explicitly set or at least not changed)

### sqlite_sharded (MUST)
- index and shard DBs MUST be rollback-journal mode permanently:
  - `journal_mode=DELETE` (or `TRUNCATE`)
- on open/create validate:
  - reject WAL
- writes modifying index + shard MUST be done via:
  - one connection to `index.sqlite`
  - `ATTACH` shard
  - single transaction commit (atomic cross-db commit)

## 3.3 `class SqliteBackend`
A single backend class supports both layouts by routing payload operations.

### Constructor/open
- `@classmethod open(cls, path, mode, pragmas=None, shard_cap_bytes=2<<30, shard_min_bytes=256<<20, clock=None) -> SqliteBackend`
- For sharded, `path` is repo dir containing `index.sqlite` and `payload/`.

### Core responsibilities / internal API
**Connections / transactions**
- `connect_index(readonly=False) -> sqlite3.Connection`
- `txn(write=True, attach_shard_id: int|None=None) -> contextmanager[(conn, cursor, attached_schema_name|None)]`
  - sharded: attaches one shard as e.g. schema name `"p"` so SQL can reference `p.payload`
  - single: no attach; uses main schema

**Config**
- `config_get(key) -> Any|None`
- `config_set(key, obj) -> None`

**Blob/encoding/payload primitives**
- `get_blob(blob_key) -> row|None`
- `insert_blob_if_missing(blob_key, raw_len, created_at) -> None`
- `get_encoding(blob_key, profile) -> row|None`
- `upsert_encoding(blob_key, profile, codec, codec_params_json, zstd_dict_id, stored_len, checksum, shard_id, updated_at) -> None`
- `get_payload(blob_key, profile, shard_id) -> bytes|None`
- `upsert_payload(blob_key, profile, data, shard_id) -> None`
- `delete_payload(blob_key, profile, shard_id) -> int` (rows deleted)
- `delete_encoding(blob_key, profile) -> int`
- `iter_encodings(limit=None) -> iterator[row]` (include shard_id and encoding fields)

**Shard routing**
- `choose_shard_for_write(estimated_payload_bytes: int) -> int`
- `list_shards() -> list[int]`
- `ensure_shard_exists(shard_id)`

**Dicts**
- `put_dict(dict_id, codec, dict_bytes, params_json, created_at)`
- `get_dict(dict_id) -> bytes|None`
- `delete_dict(dict_id) -> int`
- `count_dict_refs(dict_id) -> int` (via `encodings`)

**Repository tables**
- `create_novel(...)`, `list_novels(search)`, `upsert_chapter(...)`, `get_chapter(chapter_id)`
- `meta_set/get/list`
- `iter_chapter_refs(scope) -> iterator[(blob_key, profile)]`

**Vacuum**
- `vacuum_index()`
- `vacuum_shard(shard_id)`
- For sharded: operate per DB file.

### Spec compliance notes
- Schema matches §7.
- Migrations use `PRAGMA user_version` (§7.8).
- Sharded atomicity enforced as per §8.2.

---

# 4) `blobstore.py` — BlobStore + maintenance ops (spec §4.3, §6, §9, §10)

`BlobStore` is the content-addressable store implementation. It uses:
- `backend: SqliteBackend`
- `codec: CodecEngine`
- `identity: ikb1` functions
- repo_config for profiles + verify_on_read

## 4.1 Profile resolution (write-only)
- `def _load_profiles_from_config() -> dict[str, Profile]`
- `def _get_profile(name: str) -> Profile` (raises if missing)

Profiles influence:
- how new writes are encoded
- what `reencode()` targets by default

Profiles do **not** influence decoding (§6.2).

## 4.2 Core API (spec §4.3)

### `put_bytes(data: bytes, profile: str, cancel=None) -> Operation[PutResult]`
Rules:
- compute `(blob_key, raw_len, sha256, blake2b)` via ikb1
- in one write transaction:
  1. `insert_blob_if_missing(blob_key, raw_len, created_at)`
  2. check if encoding exists for `(blob_key, profile)`
     - if exists: **do not re-encode** by default; return PutResult derived from the **stored encoding row** (codec, stored_len, dict_id)
     - if not exists:
       - resolve Profile definition for encoding
       - load dict bytes if needed
       - encode
       - write payload + encodings row consistently:
         - `encodings.stored_len == len(payload.data)` (MUST)
         - `encodings.zstd_dict_id` NULL unless codec is zstd and dict used (MUST)
- return `PutResult(ref=ContentRef(blob_key, profile), raw_len, stored_len, codec, zstd_dict_id)`

### `put_stream(fp, profile, size_hint=None, cancel=None) -> Operation[PutResult]`
MUST compute `ikb1` in a single pass over the input stream. Implementation:
- stream into a temporary spool (file) while hashing (`blob_key_ikb1_stream`)
- after hash completes, read spool bytes to encode/store
- ensures canonical bytes preserved and avoids re-reading original stream

### `get_bytes(ref: ContentRef) -> bytes`
Algorithm (spec §6.2, §6.4):
1. load encoding row; if missing → `MissingContent`
2. locate payload via shard_id (NULL => single DB)
3. fetch payload bytes; if missing → `MissingContent`
4. if `zstd_dict_id` not NULL: load dict bytes from index; if missing dict → `CorruptContent` (cannot decode reliably)
5. decode using `codec.decode(encoded, codec=..., codec_params_json=..., dict_bytes=...)`
   - decode failure => `CorruptContent`
6. if `verify_on_read` true: recompute ikb1 and compare to blob_key → mismatch => `CorruptContent`
7. return raw bytes

### `open(ref: ContentRef) -> BinaryIO` (spec §6.3 MUST)
- `return io.BytesIO(self.get_bytes(ref))`

### `has_blob(blob_key: str) -> bool`
- checks `blobs` existence (or optionally any encodings); either is acceptable, but be consistent and document it.

## 4.3 Tools / maintenance ops

All long operations:
- return `Operation[T]`
- emit `OpEvent`
- check cancel token and raise `Cancelled`
- translate sqlite busy/locked to `Busy` (writer ops)

### `train_dict(samples, options=None, cancel=None) -> Operation[TrainDictResult]`
- codec trains zstd dict bytes
- compute `dict_id`
- store into `dicts` table (index DB only)
- return `TrainDictResult`

### `reencode(targets: Iterable[ContentRef], options=None, cancel=None) -> Operation[ReencodeResult]`
For each target ref:
1. read encoding+payload; decode using stored metadata (§6.2)
2. determine target encoding policy (typically the current profile definition, unless options override)
3. encode new payload bytes
4. **in one transaction** (spec §7.5 MUST):
   - update payload row *in place* for same `(blob_key, profile)`
   - update encodings row fields in same txn:
     - `codec`, `codec_params_json`, `zstd_dict_id`, `stored_len`, `updated_at` (+ optional checksum)
Return `ReencodeResult` counts and bytes.

### `verify(limit=None, cancel=None) -> Operation[VerifyResult]` (spec §10.1 MUST)
Targets: by default iterate all `encodings` rows (optionally limited).
For each:
- decode
- recompute ikb1
- compare to parsed blob_key parts (raw_len + sha256 + blake2b128)
- count ok/missing/corrupt

### `gc(live: Iterable[ContentRef], cancel=None) -> Operation[GcResult]` (spec §10.2 MUST)
**Critical sharded-mode atomicity rule (added):**
- In sharded mode, deletions that affect both:
  - index `encodings` rows
  - shard `payload` rows
  MUST be performed per-shard with `ATTACH`, so index+payload deletions commit atomically.

Implementation outline:
1. Build live set (recommended scalable approach):
   - create temp table `temp_live(blob_key TEXT, profile TEXT, PRIMARY KEY(blob_key, profile))`
   - insert live refs
2. Compute dead encodings in index DB:
   - `SELECT blob_key, profile, shard_id FROM encodings WHERE (blob_key,profile) NOT IN temp_live`
3. Group dead refs by `shard_id` (NULL for single)
4. For each group:
   - **single mode** (shard_id NULL):
     - one txn in main DB: delete payload rows + delete encodings rows
   - **sharded mode**:
     - open index conn, `ATTACH` that shard as schema `"p"`
     - one txn:
       - delete `p.payload` rows for those keys
       - delete `encodings` rows for those keys
     - commit (atomic across index+attached shard)
5. Delete orphan blobs:
   - `DELETE FROM blobs WHERE blob_key NOT IN (SELECT DISTINCT blob_key FROM encodings)`
6. Delete unreferenced dicts (default):
   - `DELETE FROM dicts WHERE dict_id NOT IN (SELECT DISTINCT zstd_dict_id FROM encodings WHERE zstd_dict_id IS NOT NULL)`

Return `GcResult`.

### `compact(options=None, cancel=None) -> Operation[CompactResult]` (spec §10.3)
- `auto_vacuum` default is NONE (creation-time policy)
- `compact()` uses:
  - single: `VACUUM` DB
  - sharded: `VACUUM` index and (by default) all shards; options may restrict targets

---

# 5) `repo.py` — Text-novel Repository (spec §4.4, §7.2)

`Repository` models novels/chapters/meta and uses BlobStore for content.

### Constructor
- `Repository(backend: SqliteBackend, store: BlobStore)`

### Must implement (spec §4.4)
- `create_novel(title: str, meta: dict|None=None) -> int`
- `list_novels(search: str="") -> list`
- `upsert_chapter(novel_id, chapter_key/order, body_bytes, profile, hints/meta...) -> int`
  - calls `store.put_bytes` (or `put_stream`) and then writes chapter row pointing to `(blob_key, profile)`
- `get_chapter_bytes(chapter_id) -> bytes`
- `open_chapter(chapter_id) -> BinaryIO`

### KV metadata (spec §7.2 MUST)
- `meta_set(entity_type, entity_id, key, value)`
- `meta_get(...)`
- `meta_list(...)`

Normalization rule enforced here:
- `entity_id` MUST be decimal string form of integer PK.
  - if user passes int: convert to `str(int)`
  - if string: validate `.isdigit()` (or stricter) and reject otherwise

### Live refs (for GC)
- `iter_live_content(scope=...) -> Iterable[ContentRef]`
  - yields all `(blob_key, profile)` referenced by chapters in scope
  - used by `BlobStore.gc(live=...)`

---

# 6) `__init__.py` — Factories + public exports

### Factories
- `create_repo(path, backend_mode="sqlite_single"|"sqlite_sharded", profiles: dict[str, Profile], pragmas=None, shard_cap_bytes=..., shard_min_bytes=..., verify_on_read=False) -> Repository`
  - creates DB(s)
  - runs migrations
  - writes `repo_config` keys (see below)
  - returns `Repository` with an attached `.store` (or expose store separately)

- `open_repo(path, pragmas=None) -> Repository`
  - opens existing DB(s), runs migrations (forward-only)
  - loads `repo_config`, validates:
    - `identity_policy == "ikb1"` (MUST)
    - profiles present
    - backend mode matches layout
  - constructs `SqliteBackend`, `CodecEngine`, `BlobStore`, `Repository`

### Re-exports
- `Repository`, `BlobStore`
- all public types/results/errors from `types.py`

---

## `repo_config` keys (index DB) (spec §7.1)

Store as JSON in `repo_config.value_json`.

Required/strongly recommended keys:
- `identity_policy`: `"ikb1"` (**MUST validate on open**)
- `backend_mode`: `"sqlite_single"` or `"sqlite_sharded"`
- `profiles`: mapping name → `{codec: str, params: object, zstd_dict_id: str|null}`
- `verify_on_read`: boolean (default false)
- sharded-only:
  - `shard_cap_bytes` (default 2 GiB)
  - `shard_min_bytes` (default 256 MiB)
- optional:
  - `default_profile`: profile name string

Profiles here are for **write defaults** and `reencode()` targeting only; decoding remains tied to stored `encodings.*`.

---

## Final spec-compliance checklist (what this doc now guarantees)

- §6.3: `open(ref)` is `BytesIO(get_bytes(ref))` (MUST).
- §6.2: decode uses stored encoding metadata only (MUST).
- §7 schema tables/columns and payload semantics match (MUST).
- §7.5: reencode updates payload+encodings in-place in same txn (MUST).
- §8.2: sharded mode uses `ATTACH` + rollback journal enforcement for atomic commits (MUST).
- §10.1: verify decodes and recomputes ikb1 and compares all parts (MUST).
- §10.2: gc deletes encodings/payload/blobs/dicts; **sharded deletions are atomic per shard** (MUST in practice to preserve consistency).
- §11: typed errors defined.

This finalized doc is complete enough to implement directly without inventing extra modules, while meeting the spec’s locked semantics.

════════════════════════════════════════════════════════════════

[gpt-5.2-high]
# Inkpack — Final Test Document (Spec-Compliance Test Plan)

This test plan validates Inkpack against the **public API contract** and **SQLite persistence contract** defined in the spec. It is written to be implementable with `pytest` (recommended) and optionally `hypothesis`.

Backends under test:
- **sqlite_single**
- **sqlite_sharded** (index DB + payload shard DBs)

All tests should be parameterized to run on both backends unless marked sharded-only/single-only.

---

## 0) Acceptance criteria

Inkpack is considered compliant when:
1. All tests pass for both backends.
2. All “MUST/locked” behaviors are asserted:  
   decoding source of truth (§6.2), stable reads (§6.3), in-place reencode (§7.5), sharded journaling atomicity constraints (§8.2), verify semantics (§10.1), gc semantics (§10.2), schema contract (§7).

---

## 1) Test tooling and fixtures

### 1.1 Tooling
- `pytest`
- Optional: `hypothesis` for randomized byte payloads and operation sequences.

### 1.2 Test profiles
Define at least these profiles in repo config:
- `raw`: codec `"none"`, params `{}`, `zstd_dict_id=None`
- `zstd_nodict`: codec `"zstd"`, params e.g. `{"level": 3}`, `zstd_dict_id=None`
- `zstd_dict`: codec `"zstd"`, params e.g. `{"level": 6}`, `zstd_dict_id` set after `train_dict`

### 1.3 Canonical byte samples
Use bytes that prove “no transcoding”:
- `b""`
- `b"\x00\xff\xfe\x00"` (non-UTF8)
- `b"hello\nworld"`
- `os.urandom(1_000_000)` (1 MiB random)
- repetitive text typical of chapters (e.g. `b"word " * 200_000`)

### 1.4 Repo fixtures
- `repo_single(tmp_path)`:
  - `create_repo(..., backend_mode="sqlite_single", profiles=..., verify_on_read=False, pragmas={"busy_timeout_ms": 50})`
- `repo_sharded(tmp_path)`:
  - `create_repo(..., backend_mode="sqlite_sharded", shard_cap_bytes=2<<20, shard_min_bytes=1<<20, profiles=..., verify_on_read=False, pragmas={"busy_timeout_ms": 50})`

Expose:
- `repo` (Repository)
- `store = repo.store` (BlobStore)
- `backend = repo.backend` (or provide helper to open raw sqlite connections to index/shards for whitebox assertions)

> Note: tests may use direct SQLite reads/writes to validate persistence contract.

---

## 2) Operation + progress event contract tests (spec §4.1–§4.2)

### 2.1 `test_operation_iterable_yields_opevent_and_result_put`
1. `op = store.put_bytes(b"abc", profile="raw")`
2. Iterate events: `events = list(op)`
3. Assert:
   - `hasattr(op, "result")`
   - all yielded objects are `OpEvent`
   - `op.result` is a `PutResult`
   - `op.result.ref.blob_key` starts with `"ikb1:"`
4. Do **not** require a specific event sequence; only type/availability.

### 2.2 `test_operation_iterable_for_long_ops`
For each long op (`verify`, `gc`, `compact`, `reencode`, `train_dict`):
- call it with small input
- ensure it returns an `Operation`
- iterating yields `OpEvent`
- `.result` returns the correct result type (or raises on failure)

---

## 3) Schema and migrations tests (spec §7, §7.7, §7.8)

### 3.1 `test_schema_tables_exist_index_db`
Create repo; open index DB; assert tables exist:
- `repo_config, novels, chapters, meta, blobs, encodings, dicts`

Also assert required columns exist (names must match spec):
- `encodings` includes `codec`, `codec_params_json`, `zstd_dict_id`, `stored_len`, `shard_id`, `updated_at`

### 3.2 `test_schema_payload_table_exists`
- single: payload table exists in the main DB
- sharded: payload table exists in each shard DB that exists

### 3.3 `test_required_indexes_exist`
Assert an index exists for `chapters(blob_key, profile)`.

### 3.4 `test_migrations_idempotent`
Run migrations twice (or call `open_repo` twice); ensure no errors and `PRAGMA user_version == LATEST_USER_VERSION`.

---

## 4) Identity (`ikb1`) and dedupe tests (spec §3.2)

### 4.1 `test_ikb1_blob_key_format_and_determinism`
Compute blob_key for a given `data` twice (using put or identity helper):
- must match exactly
- must match regex:
  - `^ikb1:\d+:[0-9a-f]{64}:[0-9a-f]{32}$`

### 4.2 `test_put_dedupe_same_bytes_same_blob_key`
1. Put same `data` twice with same profile.
2. Assert same `blob_key`.
3. Assert:
   - exactly one row in `blobs` for that blob_key
   - exactly one row in `encodings` for `(blob_key, profile)` (PRIMARY KEY)

### 4.3 `test_dedupe_different_bytes_different_blob_key`
Put two different byte strings; assert blob_keys differ.

---

## 5) Read semantics (locked) tests (spec §6)

### 5.1 `test_open_materializes_and_survives_gc` (spec §6.3 intent)
Goal: validate `open()` is materialized (BytesIO) and does not keep read txns open.

1. Put data; `ref = put.result.ref`.
2. `fh = store.open(ref)` but do not read yet (or read partially).
3. Run a writer op that would fail if a read txn were held:
   - Example: `store.gc(live=[])` (or `reencode([ref])`) depending on your policy.
4. Assert writer op completes without raising `Busy`.
5. Now read from `fh`:
   - `fh.read()` must equal the original bytes (even if GC deleted stored rows after open)

### 5.2 `test_decode_uses_stored_encoding_metadata_not_profile_defs` (spec §6.2 MUST)
1. Put `data` under profile `zstd_nodict` (or dict-based).
2. Directly mutate `repo_config["profiles"]` for that profile to an incompatible definition:
   - e.g. change codec `"zstd"` → `"none"` or change dict_id
3. `store.get_bytes(ref)` must still succeed and match original bytes.

### 5.3 `test_get_bytes_raises_corrupt_on_bad_payload`
1. Put under `zstd_nodict`.
2. Corrupt `payload.data` in DB (flip some bytes).
3. `get_bytes(ref)` must raise `CorruptContent`.

### 5.4 `test_missing_payload_is_missingcontent`
1. Put data.
2. Delete payload row (leave encoding row).
3. `get_bytes(ref)` raises `MissingContent` / `NotFound` subtype.

### 5.5 (Optional) `test_verify_on_read_detects_identity_mismatch_when_enabled`
1. Enable `verify_on_read=true` in repo_config (reopen if needed).
2. Put under `raw` (codec none).
3. Replace payload bytes with different bytes.
4. `get_bytes(ref)` must raise `CorruptContent` (identity mismatch under verify-on-read).

---

## 6) Put/stream tests (spec §4.3)

### 6.1 `test_put_bytes_roundtrip_preserves_exact_bytes`
For each canonical byte sample and each profile:
- put
- get
- must match exactly

### 6.2 `test_put_stream_roundtrip_preserves_bytes`
Use a non-UTF8 stream; `put_stream` then `get_bytes` equals original bytes.

### 6.3 `test_put_stream_works_with_non_seekable_stream`
Use a custom stream object that raises on `.seek()` and `.tell()`.
- `put_stream` must still succeed (spool strategy allowed/expected).

---

## 7) Canonical JSON + stored_len invariants (spec §7.3 MUST)

### 7.1 `test_codec_params_json_is_canonical`
1. Define a profile with params in non-sorted order: `{"b":1,"a":2}`.
2. Put content.
3. Read `encodings.codec_params_json` from DB.
4. Assert it equals canonical JSON string (sorted keys, compact separators).
5. Assert `json.loads(codec_params_json)` matches the original object.

### 7.2 `test_stored_len_matches_payload_len`
After `put` and after `reencode`:
- fetch `encodings.stored_len` and `payload.data`
- assert `stored_len == len(payload.data)`.

---

## 8) Dictionary tests (spec §9)

### 8.1 `test_train_dict_persists_dict_row`
1. `op = store.train_dict(samples=[...])`
2. Assert dict row exists in `dicts`:
   - dict_id matches
   - bytes length matches `TrainDictResult.dict_size`

### 8.2 `test_put_with_dict_sets_zstd_dict_id_and_decodes`
1. Train dict, update profile `zstd_dict` in repo_config to reference it.
2. Put data with `zstd_dict`.
3. Assert `PutResult.zstd_dict_id == dict_id`.
4. Assert roundtrip.

### 8.3 `test_gc_deletes_unreferenced_dicts_by_default`
1. Train dict and put content using it.
2. Run `gc(live=[])` so the encoding is removed.
3. Assert dict is removed from `dicts` table.

---

## 9) Reencode tests (spec §7.5 MUST)

### 9.1 `test_reencode_in_place_updates_same_pk_and_roundtrips`
1. Put content under `zstd_nodict`.
2. Record current encoding row and payload bytes.
3. Change profile definition (e.g. zstd level) or provide options so reencode changes encoding.
4. Run `reencode([ref])`.
5. Assert:
   - `(blob_key, profile)` still exists in encodings and payload (same PK)
   - encoding fields changed as expected (`codec_params_json`, `stored_len`, `updated_at`)
   - payload bytes changed (if expected)
   - `get_bytes(ref)` equals original canonical bytes.

### 9.2 `test_reencode_does_not_change_blob_key`
After reencode, blob_key unchanged and `verify()` reports ok.

---

## 10) Verify tests (spec §10.1 MUST)

### 10.1 `test_verify_ok_counts`
Put N items; run `verify()`:
- checked == N
- ok == N
- missing == 0
- corrupt == 0

### 10.2 `test_verify_detects_missing_payload`
Delete one payload row; `verify()` reports missing == 1.

### 10.3 `test_verify_detects_identity_mismatch_decodable_wrong_bytes` (reliable)
Use codec `"none"`:
1. Put under `raw`.
2. Replace payload bytes with different bytes.
3. `verify()` must count corrupt (hash mismatch), not ok.

---

## 11) GC tests (spec §10.2 MUST)

### 11.1 `test_gc_keeps_live_refs_deletes_dead`
1. Put 3 items.
2. Provide `live=[ref_of_one_item]`.
3. Run GC.
4. Assert:
   - live ref remains decodable
   - dead refs raise MissingContent
   - blobs for dead refs are removed if no encodings remain
   - dict GC occurs as specified (if dicts were involved)

### 11.2 `test_gc_end_state_invariants_no_orphans`
After GC:
- every encoding has a payload row in exactly one place (single DB or shard indicated by shard_id)
- no payload rows exist without corresponding encoding row

(Implement via helper described in §15.)

### 11.3 `test_gc_sharded_atomicity_with_failpoint` (sharded-only, recommended)
If the implementation supports a test failpoint/hook (recommended), verify rollback across attached DB.

Procedure:
1. Create enough content to ensure ≥2 shards.
2. Configure failpoint to raise after deleting payload but before deleting encodings (or before commit).
3. Run GC expecting an exception.
4. Assert:
   - encoding row still exists
   - payload row still exists
5. Disable failpoint, run GC again, assert both removed.

If failpoints are not supported, omit this test and rely on 11.2.

---

## 12) Compact tests (spec §10.3)

### 12.1 `test_compact_returns_nonempty_targets`
Run `compact()`:
- `CompactResult.targets` is non-empty list of strings
- `CompactResult.mode` is a non-empty string

Do not assert exact mode string.

---

## 13) Busy + cancellation tests

### 13.1 `test_writer_ops_raise_busy_when_locked`
For each writer op (`gc`, `reencode`, `compact`, and `train_dict` if it writes):
1. Open a raw sqlite conn to index DB.
2. `BEGIN EXCLUSIVE` (or `BEGIN IMMEDIATE` depending on backend pragmas) to hold write lock.
3. Call the operation with short timeout/busy_timeout.
4. Assert it raises `Busy`.

### 13.2 `test_cancelled_long_op_verify`
1. Put many items (e.g. 200).
2. Provide cancel token that flips true after a few iterations.
3. `verify()` must raise `Cancelled`.

### 13.3 `test_cancelled_does_not_corrupt_db_reencode_or_gc`
For `reencode` and/or `gc`:
- cancel mid-run
- then run `verify()` and/or run consistency invariant check
- DB must remain internally consistent (partial progress allowed, but no broken references).

---

## 14) Repository model tests (spec §4.4, §7.2)

### 14.1 `test_create_and_list_novel`
Create a novel and list novels; basic search behavior should work (implementation-defined).

### 14.2 `test_upsert_chapter_roundtrip`
1. Create novel.
2. Upsert chapter bytes.
3. `get_chapter_bytes(chapter_id)` equals original bytes.

### 14.3 `test_meta_entity_id_normalization` (spec §7.2 MUST)
1. Create novel (int id).
2. `meta_set("novel", novel_id, "k", {"v":1})` succeeds.
3. Verify DB row has `entity_id == str(novel_id)`.
4. Passing non-decimal entity_id string must raise.

### 14.4 `test_iter_live_content_matches_chapters`
Create chapters; `iter_live_content()` yields exactly their `(blob_key, profile)` pairs.

---

## 15) Sharded backend specific tests (spec §8.2 MUST)

### 15.1 `test_sharded_enforces_rollback_journal_mode` (sharded-only)
1. Query `PRAGMA journal_mode` on index and shard DBs; assert it is `delete` or `truncate` (case-insensitive).
2. Attempt to switch to WAL manually (best-effort; may vary by SQLite build).
3. Ensure `open_repo()` refuses repo if it detects WAL.

### 15.2 `test_sharded_payload_locator_is_shard_id` (sharded-only)
1. Put content.
2. Read `encodings.shard_id`.
3. Assert payload row exists in that shard DB, and does not exist in other shards.

---

## 16) Consistency invariant helper (recommended)

Implement helper `assert_repo_consistent(repo)` used after GC/reencode/cancel scenarios:

For each `encodings` row:
- payload exists in correct DB (single or shard indicated by shard_id)
- `stored_len == len(payload.data)`
- if `codec != "zstd"` then `zstd_dict_id is NULL`
- if `zstd_dict_id is not NULL` then dict row exists

Also ensure:
- no payload rows exist without a matching encodings row (scan payload tables)

---

## 17) Optional property/fuzz tests (recommended)
With `hypothesis`:
- random bytes roundtrip for each profile
- randomized sequence: put → reencode → gc(live computed from repo) → verify
- assert invariants and verify ok for live refs

---

## Suggested test module organization

```
tests/
  test_ops_contract.py
  test_schema.py
  test_identity_dedupe.py
  test_put_get_open.py
  test_json_len_invariants.py
  test_dicts.py
  test_reencode_verify_gc.py
  test_compact_busy_cancel.py
  test_repo_model.py
  test_sharded_backend.py
```

This final plan avoids fragile assertions, explicitly covers the locked requirements, and includes a clear strategy for the hardest property (sharded atomic deletion) via optional failpoints plus mandatory end-state invariants.

════════════════════════════════════════════════════════════════

[gpt-5.2-high]
# Inkpack Decisions Appendix (Revised, fully locking remaining gaps)

This appendix defines **binding decisions** to remove remaining ambiguity and ensure implementation + tests remain aligned. All decisions are compatible with the spec.

---

## A) `has_blob(blob_key)` semantics (LOCKED)
**Decision:** `has_blob(blob_key)` returns **True iff** a row exists in table:

- `blobs(blob_key TEXT PRIMARY KEY, ...)`

It does **not** depend on presence of any encoding row.

**Rationale:** `blobs` is the repo’s identity catalog; encodings are per-profile materializations that may be GC’d.

**Test implication:** After GC deletes the blob row, `has_blob(blob_key)` must return False.

---

## B) Canonical JSON definition for `encodings.codec_params_json` (LOCKED)
**Decision:** `encodings.codec_params_json` MUST be produced by:

```python
json.dumps(
  obj,
  sort_keys=True,
  separators=(",", ":"),
  ensure_ascii=False,
  allow_nan=False
)
```

- Sorted keys
- Compact separators (no whitespace)
- Always valid JSON (no NaN/Inf)

**Scope:**
- MUST apply to `encodings.codec_params_json`
- MAY apply (optional) to `repo_config.value_json` and `meta.value_json`

---

## C) Missing dictionary handling (LOCKED)
If `encodings.zstd_dict_id` is not NULL:

1. If the corresponding row in `dicts` is missing → treat as **MissingContent**.
2. `get_bytes/open` MUST raise `MissingContent`.
3. `verify()` MUST count this as **missing** (not corrupt), because decoding cannot be performed.

**Rationale:** The payload may not be damaged; a required dependency is absent.

---

## D) `verify()` target set and classification (LOCKED)
**Decision:**
- Default `verify()` iterates **all rows in `encodings`** (optionally limited by `limit`).
- `VerifyResult.checked` counts the number of encodings attempted (after `limit`).

Classification rules per encoding row:
- **missing**:
  - payload row missing, OR
  - required dict row missing
- **corrupt**:
  - payload exists but decode/decompress fails, OR
  - decoded bytes do not match `ikb1` identity (raw_len/sha256/blake2b128)
- **ok**:
  - decode succeeded and identity matches

---

## E) Dedupe-hit behavior for `put_bytes/put_stream` (LOCKED)
If `(blob_key, profile)` already exists in `encodings`:

1. `put_*` MUST NOT rewrite payload or re-encode by default.
2. Returned `PutResult` MUST reflect the **stored encoding row**:
   - `codec`, `stored_len`, `zstd_dict_id` from `encodings`
   - `raw_len` from `blobs.raw_len` (or derived from blob_key)

**Rationale:** Profiles can change; stored encoding metadata is authoritative.

---

## F) Busy mapping and writer ops (LOCKED)
The following operations MUST translate SQLite busy/locked (after configured timeout) into `Busy`:
- `put_bytes`, `put_stream`
- `gc`, `reencode`, `compact`, `train_dict`

(Verify may be implemented read-only; Busy behavior is not required for verify.)

---

## G) Sharded GC atomicity (LOCKED)
In `sqlite_sharded`, GC MUST delete payload and encodings **atomically per shard**:

- Use one connection to `index.sqlite`
- `ATTACH` exactly the shard being processed
- In one transaction commit:
  - delete shard `payload` rows for dead refs in that shard
  - delete corresponding `encodings` rows in index

**Rationale:** Prevent half-deletes and preserve consistency under crashes/cancellation/errors.

---

## H) VACUUM / `compact()` execution constraint (LOCKED)
**Decision:** `compact()` MUST vacuum each SQLite database **without any ATTACHed databases** on the same connection.

- sqlite_single: open connection to the single DB; run `VACUUM`.
- sqlite_sharded:
  - open connection to `index.sqlite` alone; run `VACUUM`.
  - for each shard: open connection to that shard alone; run `VACUUM`.

**Rationale:** SQLite disallows `VACUUM` when other DBs are attached; this prevents runtime errors and ensures deterministic behavior.

**Test implication:** Compact should succeed even when shards exist; targets list must include the DBs vacuumed.

---

## I) Identity policy enforcement on open (LOCKED)
**Decision:** Repository MUST store and validate:

- `repo_config["identity_policy"] == "ikb1"`

If the stored value differs (or is missing), `open_repo()` MUST fail (raise `InkpackError` or a specific config error).

**Rationale:** Prevents silent creation of incorrect blob_keys and breaks in dedupe/verify semantics.

---

## J) `blobs.raw_len` consistency (LOCKED, minimal)
**Decision:** On insertion of a new blob:
- `blobs.raw_len` MUST equal the `raw_len` embedded in `blob_key` (ikb1 format).

If a blob row already exists:
- implementation MUST NOT change `raw_len`
- optional: if mismatch is detected, treat repository as corrupt and fail writes.

**Rationale:** Provides an internal invariant and simplifies diagnostics.

---

## K) `CompactResult.mode` string (test rule)
**Decision:** `CompactResult.mode` is informational; it MUST be a non-empty string. Tests MUST NOT require exact equality to `"vacuum"`.

---

### Minimal additional tests implied by this appendix
- Missing dict row ⇒ `get_bytes` raises `MissingContent`; `verify().missing` increments.
- `identity_policy` mismatch in `repo_config` ⇒ `open_repo` fails.
- `has_blob` follows `blobs` row existence (including after GC deletion).
- `compact()` succeeds in sharded mode (no ATTACH during VACUUM).
---

