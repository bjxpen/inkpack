# Inkpack Guarantees

Normative guarantees of the current implementation. The normative spec and
the locked decisions live in [`spec.md`](spec.md); this document records the
operational contracts that callers rely on but that are too behavioral for a
schema.

---

## GC concurrency contract (post-B1)

`gc(live)` deletes encodings + payloads not in `live` in **batched atomic
exclusive windows**:

- The live set is staged once (autocommit TEMP table).
- Shards are processed in batches of at most the session's attach capacity
  (`SQLITE_LIMIT_ATTACHED − 1`, conservatively).
- Each batch runs: `ATTACH` its existing shards → **`BEGIN IMMEDIATE`** →
  recompute the dead set **fresh inside the window** → delete that batch's
  dead payloads + encodings (NULL-locator payloads are cleaned from every
  batch shard; a vanished shard file yields an encodings-only pass and is
  never recreated) → sweep wrong-shard payloads → **one `COMMIT`** →
  `DETACH`.

**Invariant:** *GC deletes exactly the encodings dead as of a snapshot taken
after any concurrent put completed.*

- A put that **completed before** a batch's snapshot is visible to it —
  authorizable for collection in that or a later batch.
- A put that **races** the window blocks on the reserved lock until the
  batch commits; it is absent from the snapshot, and its in-txn re-probes see
  the post-commit state (a put whose row was deleted re-prepares as a new
  write and succeeds — it self-heals).
- Cross-batch residue is impossible: anything batch *k* cannot see is caught
  by batch *k+1*'s `NOT EXISTS` sweep (its encoding is gone by then, so the
  payload is an orphan by definition).

**Cancellation** keeps committed batches and rolls the in-flight batch back
whole. **Duration note:** a batch's writer lock is held for the whole
snapshot+delete+commit; a huge dead set holds it longer than any single
per-shard txn of the previous design — comparable to the *sum* of those
txns. That is acceptable for a maintenance operation (see Residual risks in
the r2 issue register).

---

## Durability

- `synchronous` defaults to **`NORMAL`** and is applied to every connection
  (it is per-connection; there is no persistent substitute). `NORMAL` is a
  performance/durability trade-off: it can lose the very last committed
  transaction on a power failure. Use `synchronous=FULL` for full durability.
- `sqlite_single` uses **WAL** on the single file: readers never block
  writers and vice versa; WAL is the recommended default (tampering is
  visible on open).
- `sqlite_sharded` uses **rollback journal** (`delete`/`truncate`) on the
  index and every shard, permanently enforced: cross-DB atomicity of
  `ATTACH`ed writes requires it (spec §8.2). `open_repo()` refuses WAL shards.
- Inkpack never silently rewrites journal mode or `auto_vacuum`; both are
  creation-time policies.

---

## Threading and forking

- **One `Operation` per thread.** Drive each `Operation` from a single
  thread; `Operation` is single-use and not thread-safe (review L27).
- **A `Repository`/`SqliteBackend` is not fork-safe mid-operation.** Fork
  before or after operations, not inside one; child processes should reopen
  (`open_repo`) rather than inherit open SQLite connections.
- Independent operations (each with its own session connection) may run on
  different threads; writers serialize on SQLite locks and surface `Busy`
  after `busy_timeout_ms`.
- **Abandonment timing:** `op.close()`, `with op:`, and `.result` are
  deterministic on every interpreter. Abandonment of a *discarded
  half-consumed iterator* is best-effort: CPython delivers `GeneratorExit`
  during for-loop unwind, but non-refcounting GCs (PyPy) may delay or never
  run the iterator's finalizer (locked semantics S1/N1).

---

## Content and metadata round-trips

- **Bytes are canonical.** `put_*`/`upsert_chapter*` store and `get_bytes` /
  `get_chapter_bytes` / `open*` return the exact bytes given; no transcoding
  or normalization, ever.
- **KV metadata values round-trip through JSON** (`canonical_json`). Only
  JSON-encodable values survive a `meta_set` → `meta_get` cycle; Python
  **tuples come back as lists**, `set`/`bytes`/custom objects do not
  round-trip. `NaN`/`Inf` are rejected at write time.
- **Decoding uses stored metadata only** (spec §6.2): profile changes never
  affect reading; `reencode()` migrates stored content.

---

## Error semantics

- **No raw `sqlite3.Error` or `zstandard` exceptions** escape public calls:
  busy/locked → `Busy`; missing file at ATTACH → `MissingContent`;
  present-but-unusable file → `CorruptContent`; transient races →
  `Retryable`; everything else → `InkpackError` (D9).
- **`MemoryError` is never a content finding** (A7): it propagates from
  encode/decode and aborts `verify()` instead of counting rows corrupt.
- **Broken catalog tables** (e.g. `encodings`, `repo_config`) are repo
  structure: `InkpackError` (abort-worthy). **Broken payload storage** (e.g.
  a dropped `payload` table) is a per-row content finding: `CorruptContent`
  from reads, counted `corrupt` by `verify()` (A3; decision-log line in
  spec.md).
- **`verify()` MUSTs enforced per row:** `stored_len == len(payload)` (A5),
  `blobs.raw_len` present and equal to the key's `raw_len` (A5/J), decode
  success, identity match.
- **`Retryable` means safe-to-retry** (A6): the repository changed between
  two reads of the same call; retrying the call is the recovery path.

---

## Connection policy (C5, scoped)

- **BlobStore operations** run in one operation-scoped session: `get_bytes`
  opens 1 connection; `verify`/`reencode`/`gc` open O(1) connections with
  O(shards) session-managed `ATTACH`es (C1 — an idle ATTACH holds no locks);
  GC's TEMP tables live for the whole operation.
- **`compact()`** VACUUMs each database on a virgin connection with no
  attached databases (decision H).
- **Repository catalog calls** (novels/chapters/meta, `set_profile(s)`,
  `set_verify_on_read`) each open their own short connection; a multi-step UI
  flow paying per-op connection cost can be revisited only on profiling
  evidence (C5 is advisory).

---

## Streaming reads (C3)

`iter_live_content()` pages 1000 rows per short read transaction (keyset on
`(blob_key, profile)`). It is **weakly consistent but monotone-safe for GC
staging**: a page boundary can capture late additions in a later page and can
retain refs deleted mid-drain (over-retention — safe for GC, which deletes
what is *not* in the live set), but it can never miss a live ref.

---

## zstd context caching (C2)

`CodecEngine` caches zstd compressor/decompressor contexts in an LRU
(max 32) keyed by `(level, dict id)` / `(dict id)` — a bulk reencode builds
one context instead of one per target. The one-shot `compress`/`decompress`
APIs are share-safe in the pinned `zstandard` dependency (≥0.22,<1); if a
future version changes that, the fallback is a thread-local cache. Contexts
are keyed by the dictionary **id string**, never by `id(bytes)`.
