# Inkpack — Specification (normative) + Locked Decisions

This document defines Inkpack's **public API contract** and **persistent
storage contract**. It intentionally avoids prescribing internal architecture
beyond what is required for correctness, interoperability, and durability.

Contents: the normative spec (§1–§11), then the **binding decisions
appendix** (all locked decisions, including the r2 amendments — G batched
GC, D verify classification, N7/N8, §11 `Retryable`). The original
chat-transcript draft (design discussion, implementation planning) is
archived at `docs/history/chat_spec_transcript.md`; it is non-normative.

Operational guarantees (durability, threading/fork, GC concurrency,
round-trips) are documented in `GUARANTEES.md`.

---

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

Phase events (§4.1, locked — D6): stream puts (`put_stream`,
`upsert_chapter_stream`) MUST emit exactly one `phase` event
(`phase="persist"`) at the hash→persist boundary. No other operation emits a
`phase` event today; the `kind` stays part of the contract.

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

Reads MUST also enforce the stored-length MUST (§7.3): a payload whose length
disagrees with `encodings.stored_len` is `CorruptContent` (S3 bound now covers
metadata consistency on read, not only the decode output bound).

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

Parameter naming (D1): the public factory kwarg is `min_shard_cap_bytes`;
the persisted `repo_config` key intentionally stays `shard_min_bytes`
(storage stability). The legacy `shard_min_bytes` factory kwarg is a
deprecated alias.

**Atomicity (MUST):**
- Writes that modify index + shard MUST be performed using one connection to `index.sqlite` with `ATTACH shard`.
- To guarantee atomic cross-DB commits, index and all shards MUST use rollback journal mode permanently:
  - `journal_mode=DELETE` (or `TRUNCATE`)
- Inkpack MUST validate/enforce this at creation/open.

**Shard uniqueness (MUST):**
- A given `(blob_key, profile)` payload row exists in at most one shard.
- `encodings.shard_id` is authoritative for locating it.

**No-create on open/read/compact (MUST, normative):** opening, reading,
validating and compacting an existing repository MUST NOT create SQLite
files or directories. Only explicit creation paths (`create_repo`,
`SqliteBackend.create`, shard rollover during a write) may create files. A
missing index DB is `NotFound`; a missing shard file behaves like missing
content (`MissingContent`); `compact()` rejects unknown shard ids instead of
creating empty shard files.

Recommended:
- `foreign_keys=ON` in index DB
- `synchronous=NORMAL` default (configurable)
- `busy_timeout` recommended nonzero (implementation choice; may be documented)

---

## 9) Dictionaries

- Dicts are stored only in index DB.
- `zstd_dict_id` MUST reflect actual dict usage.
- `gc()` MUST remove unreferenced dicts by default. "Unreferenced" means: not
  referenced by any `encodings.zstd_dict_id` **and** not referenced by any
  profile in `repo_config["profiles"]` (amendment: profile-referenced dicts
  are live, so the documented `train_dict → set_profile → gc → put` workflow
  never loses the dictionary).

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
- delete corresponding payload rows (grouped by `encodings.shard_id`, not by
  the filesystem shard list: rows with `shard_id IS NULL` or a missing shard
  file have their encodings row deleted without creating any file; the
  NULL-locator group's payload rows are cleaned from every existing shard)
- delete blobs with no remaining encodings (default)
- delete dicts not referenced by any encoding AND not referenced by any
  profile in `repo_config["profiles"]` (default)

GC is a writer operation; if DB is locked/timeout, raise `Busy`.

Atomicity (Decision G amendment, B1): GC MUST delete payload + encodings in
**batched exclusive windows** — per batch of up to the attach-limit shards,
one `BEGIN IMMEDIATE … COMMIT` that recomputes the dead set fresh INSIDE the
window. Normative: *GC's deadness snapshot and deletions share one exclusive
writer window per batch; concurrent writers block (`Busy`) for the batch
duration; a put completing before a batch's snapshot is authorizable for
collection in that or a later batch.* Cancellation keeps committed batches
and rolls the in-flight batch back whole. The live-set freshness rule is
normative for callers (the exposure window is commits during or after the
drain — see GUARANTEES.md), so the exclusive window alone is not the whole
safety story.

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
- `Retryable` (A6: transient contention/race — safe to retry the call;
  §11 defines a minimum set and additions are sanctioned)

---

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
  - decoded bytes do not match `ikb1` identity (raw_len/sha256/blake2b128), OR
  - **metadata inconsistency (A5 amendment)**: encoding without a `blobs`
    row, `blobs.raw_len` ≠ key `raw_len`, or `encodings.stored_len` ≠
    `len(payload)` — the §7.3 / decision J MUSTs are enforced by verify,
    the designated enforcement op (§10.1)
- **ok**:
  - decode succeeded and identity matches

**Classification boundary (A3 decision-log line):** broken *catalog* tables
(e.g. `encodings`, `repo_config`) are repo STRUCTURE → `InkpackError`
(abort-worthy; `open`/reads raise). Broken *payload storage* (e.g. a dropped
`payload` table) is a per-row content FINDING → `CorruptContent` from reads
and counted `corrupt` by `verify()`. This encodes the boundary the locked
tests (`test_broken_schema_get_bytes_is_inkpack_error`,
`test_verify_counts_damaged_payload_schema_as_corrupt`) imply but never
state.

---

## E) Dedupe-hit behavior for `put_bytes/put_stream` (LOCKED)
If `(blob_key, profile)` already exists in `encodings`:

1. `put_*` MUST NOT rewrite payload or re-encode by default.
2. Returned `PutResult` MUST reflect the **stored encoding row**:
   - `codec`, `stored_len`, `zstd_dict_id` from `encodings`
   - `raw_len` from `blobs.raw_len` (or derived from blob_key)
3. **Readability precondition (amendment):** a dedupe hit MUST NOT report
   success when the content is unreadable — if the stored row's
   `zstd_dict_id` is set and the `dicts` row is missing, `put_*` MUST raise
   `MissingContent` (without rewriting anything). "put succeeded" implies
   "content is readable".
4. **Repair (amendment):** if the encoding row exists but the payload row is
   missing, `put_*` MUST repair the payload by re-encoding `raw` with the
   **stored** codec/params/dict (never the current profile), keeping the same
   `(blob_key, profile)` and `shard_id`. If the stored dict is missing, raise
   `MissingContent`. The same rule applies to `upsert_chapter`-style atomic
   persists: a chapter must never commit pointing at missing content.

**Rationale:** Profiles can change; stored encoding metadata is authoritative.

---

## F) Busy mapping and writer ops (LOCKED)
The following operations MUST translate SQLite busy/locked (after configured timeout) into `Busy`:
- `put_bytes`, `put_stream`
- `gc`, `reencode`, `compact`, `train_dict`

(Verify may be implemented read-only; Busy behavior is not required for verify.)

---

## G) Sharded GC atomicity (LOCKED — AMENDED: per-shard → per-batch, B1)
In `sqlite_sharded`, GC MUST delete payload and encodings **atomically per
batch** (per-batch exclusive window; per-shard atomicity does not deliver the
property callers rely on — "gc never destroys content a completed put just
wrote"):

- Use one connection to `index.sqlite`
- Process shards in batches of at most the session's attach capacity
  (`SQLITE_LIMIT_ATTACHED − 1`, conservatively)
- Per batch, in ONE `BEGIN IMMEDIATE … COMMIT` transaction:
  - `ATTACH` the batch's existing shards
  - recompute the dead set (`temp_dead`) fresh INSIDE the window
  - delete the batch's shard `payload` rows for dead refs
  - delete the corresponding `encodings` rows in index
  - sweep payloads parked at the wrong shard (batch-restricted)
- Per-batch sweeps cover only that batch's shards, so a FINAL IDEMPOTENT
  SWEEP over the whole shard universe runs after the batch loop (multi-batch
  runs only), removing any payload whose encoding died in another batch —
  one GC run always converges to "no payload rows without matching
  encodings" (r3-A; the sweep needs no exclusive window: a committing put
  writes encoding+payload atomically)
- Cancellation keeps committed batches; the in-flight batch rolls back whole

**Concurrency contract (normative):** GC's deadness snapshot and deletions
share one exclusive writer window per batch; concurrent writers block
(`Busy`) for the batch duration; a put completing before a batch's snapshot
is authorizable for collection in that or a later batch. A put racing the
window blocks until commit, is absent from the snapshot, and self-heals on
its in-txn re-probe.

**Rationale:** Prevent half-deletes and preserve consistency under
crashes/cancellation/errors — and close the lost-update window in which a
concurrent put could resurrect content that a stale snapshot still marks
dead.

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


## L) Locked semantics (S1–S9)

**S1 — Operation abandonment (spec §4.2).** `Operation` is single-use.
`close()` prevents work from starting (if never started) and stops it (if
started), releasing resources immediately. Abandonment (close, `with` exit,
or iterator discarded) causes `.result` to raise `Cancelled`. `Operation`
supports `with op:`; `__exit__` calls `close()`. `.result` must never return
`None` unless the operation's actual result type is `None`.

**S2 — Sharded ATTACH cannot create files; repair is rehoming (spec §8.2).**
`ATTACH DATABASE` MUST be performed in a way that cannot create a missing
file: an explicit existence check plus URI `mode=rw` attach. Shard
creation/migration MUST be explicit (`_ensure_shard_file`), never implicit
via ATTACH. Repair policy: when an encoding exists but its referenced shard
file is missing/unusable and the caller is providing the canonical raw bytes
(put/upsert paths), Inkpack MUST (1) select a writable shard,
(2) explicitly ensure/migrate it, (3) write the payload there, and
(4) atomically update `encodings.shard_id` in the same transaction.
Reencode/verify/get do not have raw bytes available for rehoming; they must
not create shards and surface typed errors instead (reencode keeps its
per-target skip behavior).

**S3 — Decode output bound (spec §6.4).** For every read, `raw_len` is parsed
from the `blob_key` and enforced as a hard bound. `zstd`: frames without a
declared content size are `CorruptContent` when bounded; declared size larger
than the bound is `CorruptContent`; decoding passes
`max_output_size=raw_len`. `none`: `len(payload) == raw_len` else
`CorruptContent`. Full identity recompute remains optional
(`verify_on_read` / `verify()`).

**S4 — `ikb1` parse is strict ASCII canonical (spec §3.2).** Only
`^ikb1:[0-9]+:[0-9a-f]{64}:[0-9a-f]{32}$` is accepted; Unicode digits,
uppercase hex and other prefixes are rejected.

**S5 — `compact(options["shard_ids"])` type (spec §10.3).** `shard_ids` is
`list[int] | tuple[int, ...]` with elements `int` and not `bool`; any other
shape raises `TypeError` at call time.

**S6 — KV entity id normalization (spec §7.2).** Entity ids are non-negative
integer primary keys; negative values and `bool` are rejected.

**S7 — `chapter_key` (spec §4.4).** `chapter_key` is `str | int`; `None` and
`bool` are rejected (never stored as `"None"`/`"True"`).

**S8 — Busy mapping (spec §11).** Public calls must not leak raw sqlite
busy/locked errors, including open-time validation and migrations: those
paths map busy/locked to `Busy` too.

**S9 — `list_shards()` (spec §8 operational).** Only files matching
`shard-<digits>.sqlite` count; all other filenames are ignored so stray files
cannot crash open/validation/compact.

## M) Locked decisions (D1–D12)

**D1 — Snapshot reads.** Readers may overlap a writer, but a read of
encodings + payload MUST be one snapshot: `get_bytes`/`verify`/`reencode`
decode inside a transaction that reads both (single: `BEGIN` on the index
connection; sharded: `ATTACH` the shard to the SAME index connection, never a
second shard connection). Maintenance writers (`put*`, `upsert*`, `reencode`,
`gc`, `compact`, `train_dict`) are exclusive with each other (`Busy`).

**D2 — Canonical shard names.** The canonical name is `shard-{id:04d}.sqlite`
(ids ≥ 10000 naturally become 5+ digits). `list_shards()` returns an id ONLY
when the observed filename equals that name; non-canonical `shard-<digits>`
names or two files with the same numeric id → `InkpackError` on open with a
rename hint. Names not matching `shard-<digits>.sqlite` are ignored (S9).

**D3 — Put commit point.** `put_*` is a writer. A dedupe hit is committed
inside a write transaction that re-probes payload + required dict; success
means readable at commit. Later `gc` may still delete it.

**D4 — `shard_min_bytes`.** Stays in the public signature and is persisted,
but is ONLY a validation floor (positive non-bool int, `min ≤ cap`). Routing
uses only `shard_cap_bytes`.

**D5 — All-or-nothing create.** `create_repo` builds in a sibling temp
directory (initial `repo_config` committed in the same transaction as the
migration) and atomically renames into place; any failure leaves no
`repo.sqlite` / `index.sqlite` / `payload/` and a retry behaves as on a fresh
path. Create-phase failures propagate raw; rename-phase failures are
`InkpackError`.

**D6 — Stream identity.** `key_tuple` is not public. Stream puts look up by
digest first (no spool read on hit); a miss/repair reads the spool and
re-binds with `identity.key_bytes(raw)` (forged digests refused). Correctness
over skipping a hash.

**D7 — Stored metadata is authoritative.** Precise split (r4-P0.4): a
**hit/read** enforces exactly what decode enforces — the codec/dict-id
pairing, the dict row's presence, `stored_len`, and the S3 bound — as a typed
error, never a silent success; `codec_params_json` is **not** in that set
(decode ignores it, L27 — the frame is the source of truth), so a hit with
non-object params is readable and succeeds. A **repair / new encode from a
stored row** enforces the full write policy: unparsable/non-object
`codec_params_json` or engine-rejected params is `CorruptContent`, not
`ValueError` and not a silent default (repair must encode). Caller-supplied
policy (new profile, ad-hoc reencode options) stays `ValueError` at the call
boundary.

**D8 — Strict open-time config.** `open_repo` validates `identity_policy`,
`backend_mode`, profiles (missing → `InkpackError("…profiles missing…")`),
`verify_on_read` (`bool` or missing → `False`), and shard ints (`int` not
`bool`, `min ≤ cap`). A broken layout (`index.sqlite` without `payload/`) is
`InkpackError`, never `NotFound`.

**D9 — Typed errors.** No raw `json.JSONDecodeError` / `sqlite3.Error` out of
public or factory paths. Busy/locked → `Busy` (including open/migrate/
validation). Missing file at ATTACH → `MissingContent`. Present-but-unreadable
→ `CorruptContent`. Everything else → `InkpackError`. `txn_on` rollback
errors are suppressed so they cannot mask the original.

**D10 — Operation abandonment.** A consumer exception closes the generator
and `.result` re-raises that exception when observable (on CPython, the
for-loop delivers `GeneratorExit` into a generator-method `__iter__` during
the unwind, so `.result` surfaces the deterministic abandonment state —
`Cancelled`). `close()` / `with` / never-started abandon → `Cancelled`.
Never-started writer `close()` emits `warnings.warn`. No eager-run on
construction.

**D11 — Paths.** `~/…` is expanded (`Path.expanduser().resolve()`) in both
factories. The exists-check (markers or `payload/shard-*.sqlite`) runs BEFORE
`validate_profiles`.

**D12 — Delete is catalog-only.** No `delete(..., reclaim=True)`; the
two-step `delete_chapter` + `gc(iter_live_content())` is spec-locked and
documented in the README.

## N) Locked decisions (fifth review)

**N1 — S1 abandonment is deterministic on `close()` / `with op:` / `.result`;
discarded half-consumed iterators are best-effort.** `Operation` is driven by
a real iterator object (never a generator wrapping a generator), so a stale
outer iterator being closed can never clobber a completed operation's result.
A consumer exception propagates to the caller and the operation stays usable.

**N2 — Read operations MUST NOT perform schema migrations or DDL.** Migrations
occur at open or during writer operations. `txn_on(write=False)` never
migrates; `Session._shard_conn` (a read helper) never migrates; write attaches
migrate (cached per session).

**N3 — `size_hint` stays advisory** (no cap; not a routing input).

**N4 — `ikb1` parse aligns with S4 exactly: leading zeros allowed.**

**N5 — A failed write after shard rollover may leave an empty
`payload/shard-NNNN.sqlite`.** Harmless: later writes may reuse it and it is
never treated as repository contents on its own. No retract.

**N6 — The write pointer is the highest shard id; `compact()` does not move
writes backward.** Empty low shards stay until an admin deletes them.

**N7 — Put success ⇒ payload row + required dict exist at commit** (AMENDED,
A4: "readable" also includes stored-metadata consistency — the hit probe
rejects undecodable stored metadata, e.g. `codec` ≠ `zstd` carrying a
`zstd_dict_id`, exactly as decode does; the hit probe is symmetric with
decode). A present-but-corrupt payload is NOT decoded on the dedupe-hit path.
(r4-P1.1: the dict-existence check runs ON THE COMMIT CONNECTION, inside the
write txn's writer lock — `store_encoding_and_payload_on` and the
`persist_prepared` hit branch probe `dicts` directly, not the session's
dict cache, so a concurrent `gc` cannot delete the dict between check and
commit.)

**N8 — GC computes `temp_dead` freshly PER BATCH inside that batch's
exclusive writer window (see the G amendment); a racing writer blocks on the
window. GC additionally sweeps payloads parked at a shard different from
their `encodings.shard_id` locator (wrong-shard orphans), per batch AND via
the final whole-universe idempotent sweep on multi-batch runs, so one GC run
converges to no orphan payload rows (r3-A).**
