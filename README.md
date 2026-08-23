# Inkpack

**Local storage library for text novel libraries.**

Inkpack stores chapter bodies as **byte-preserving canonical content** (no
forced UTF-8, no forced HTML), with content-addressed **dedupe**, real
**zstd dictionary compression**, in-place **recompression**, **GC / verify /
compact** maintenance, and custom **KV metadata** on novels and chapters — all
on top of SQLite, in either a single-file or sharded layout.

The public API is deliberately small, and long operations report progress via
`OpEvent` objects (see [Operations](#operations-and-progress)).

---

## Features

- **Canonical bytes** — content is stored and returned exactly as given; never
  transcoded or normalized.
- **Content-addressed dedupe** — the `ikb1` identity policy computes
  `raw_len + sha256 + blake2b-128` in one streaming pass; identical bytes map
  to one `blob_key` regardless of profile or caller.
- **Codecs** — `none` (identity) and `zstd` (with optional trained
  dictionaries, backed by the real [`zstandard`](https://github.com/facebook/zstd/tree/main/contrib/python-zstandard)
  library).
- **Profiles** — named write policies (`codec`, `params`, optional
  `zstd_dict_id`) stored in the repo config, managed through validated
  wrappers on `Repository` (`get_profiles`, `set_profile`, …). Profiles
  decide how *new* writes are encoded; decoding always uses the **stored
  encoding metadata**, so profile changes never break existing content.
- **Two backends**
  - `sqlite_single` — one `repo.sqlite` file (WAL).
  - `sqlite_sharded` — `index.sqlite` + `payload/shard-0001.sqlite …` with a
    configurable per-shard cap (default 2 GiB, min recommended 256 MiB).
    Cross-DB commits use `ATTACH` + rollback journal mode for atomicity.
- **Maintenance** — `verify()` (decode + recompute identity), `gc(live)`
  (delete unreferenced encodings/payloads/blobs/dicts, in batched atomic
  exclusive windows), `compact()` (`VACUUM` of index and/or shards).
- **KV metadata** — `meta_set/get/list` for novels and chapters, with strict
  entity-id normalization (decimal string of the integer primary key).
- **Typed errors** — `NotFound`, `MissingContent`, `CorruptContent`,
  `Retryable`, `Busy`, `Cancelled`.
- **Cancellation** — every long operation accepts a cancel token.

---

## Installation

```bash
pip install -r requirements.txt        # runtime: zstandard
pip install -r requirements-dev.txt    # development: pytest, hypothesis, mypy, pyright, ruff
```

Requires Python ≥ 3.11.

---

## Quickstart

The fences marked `<!-- runnable -->` are **executable as written** (pinned by
`tests/test_r2_docs.py` — including a guard that every ref the quickstart
still holds a name for must be readable after its own GC). The demo fence
exercises the full write → read → maintain surface; the cleanup fence shows
the catalog-only delete + GC reclamation pattern.

<!-- runnable -->
```python
from pathlib import Path
from inkpack import create_repo, Profile

repo = create_repo(
    path=Path.home() / "novels",   # or "~/novels" — expanded in the factory
    backend_mode="sqlite_single",            # or "sqlite_sharded"
    profiles={
        "raw":        Profile("raw", "none", {}),
        "zstd_nodict": Profile("zstd_nodict", "zstd", {"level": 3}),
    },
)
store = repo.store

# --- content -------------------------------------------------------------
# The demo writes its bytes as CHAPTERS: raw BlobStore puts are invisible to
# repository GC unless you pass them in live, so a standalone put_bytes()
# would be reclaimed by the GC below. (Dedupe makes writing the same bytes
# as a chapter free.)
novel_id = repo.create_novel("My Novel", meta={"genre": "fantasy"})
chapter_id = repo.upsert_chapter(
    novel_id, "ch-001", b"chapter body \x00\xff (opaque bytes)", "raw",
    hints={"media_type": "text/plain", "charset": "utf-8"},
    meta={"words": 4200},
)
ref = next(repo.iter_live_content())          # ContentRef(blob_key, profile)
assert store.get_bytes(ref) == b"chapter body \x00\xff (opaque bytes)"
handle = store.open(ref)                      # io.BytesIO, safe to hold
data = handle.read()
assert store.has_blob(ref.blob_key) is True   # True iff a blobs row exists (decision A)

# streaming chapter upsert (progress events, atomic content+catalog commit);
# streams are consumed, so use a FRESH handle
chapter_path = Path("chapter.txt")
chapter_path.write_bytes(b"streamed chapter body")
with open(chapter_path, "rb") as fh:
    op = repo.upsert_chapter_stream(fh, novel_id, "ch-002", "zstd_nodict")
    chapter2_id = op.result                   # int; OpEvents while iterating

# --- repository model -----------------------------------------------------
assert repo.get_chapter_bytes(chapter_id) == b"chapter body \x00\xff (opaque bytes)"
assert repo.open_chapter(chapter_id).read() == b"chapter body \x00\xff (opaque bytes)"
repo.meta_set("novel", novel_id, "rating", 5)
assert repo.meta_get("novel", novel_id, "rating") == 5
assert repo.meta_list("chapter", chapter_id)["words"] == 4200

# --- catalog (no bodies) ---------------------------------------------------
novel = repo.get_novel(novel_id)              # dict row; NotFound if missing
repo.update_novel(novel_id, title="My Novel (retitled)", slug="my-novel")
chapters = repo.list_chapters(novel_id)       # list[ChapterInfo], no bodies, ordered by order_key
                                           # (order_key is a TEXT sort: zero-pad, e.g. "001")
info = repo.get_chapter(chapter_id)           # ChapterInfo

# --- dictionaries ---------------------------------------------------------
trained = store.train_dict([b"sample prose " * 100] * 5).result
repo.set_profile(Profile("zstd_dict", "zstd", {"level": 6}, trained.dict_id))
# (Until a profile or an encoding references a trained dict, the NEXT gc()
# reclaims it — the profile assignment above pins it.)
repo.upsert_chapter(novel_id, "ch-003", b"sample prose " * 2000, "zstd_dict")
dict_ref = next(
    r for r in repo.iter_live_content(scope=novel_id) if r.profile == "zstd_dict"
)
assert store.get_bytes(dict_ref) == b"sample prose " * 2000

# --- maintenance -----------------------------------------------------------
verify = store.verify().result                # VerifyResult(checked, ok, missing, corrupt)
assert verify.missing == 0 and verify.corrupt == 0
# NOTE: raw BlobStore puts are invisible to repository GC unless you pass
# them in live — gc(live) reclaims only what is NOT referenced by chapters.
live = list(repo.iter_live_content())         # ContentRefs referenced by chapters
gc = store.gc(live=live).result               # GcResult(...)
compacted = store.compact().result            # CompactResult(mode, targets)
reencoded = store.reencode(live).result       # ReencodeResult(...)
```

`store.has_blob(key)` is True iff a `blobs` catalog row exists (decision A)
— it does not mean that `ContentRef` is currently readable.

<!-- runnable -->
```python
# --- deletes are catalog-only (D12), then reclaim and reopen ---------------
from pathlib import Path
from inkpack import open_repo

repo = open_repo(Path.home() / "novels")      # the demo's repository
novel_id = repo.list_novels()[0]["id"]
for cid in [c.id for c in repo.list_chapters(novel_id)]:
    repo.delete_chapter(cid)                  # catalog only
repo.delete_novel(novel_id, cascade=True)     # catalog only; content reclaimed by gc
repo.store.gc(live=list(repo.iter_live_content())).result
verify = repo.store.verify().result
assert verify.ok == 0 and verify.missing == 0 and verify.corrupt == 0
repo2 = open_repo("~/novels")
assert repo2.get_profiles().keys() == {"raw", "zstd_nodict", "zstd_dict"}
```

---

## Operations and progress

Every long call returns an `Operation[T]`:

- iterating it drives the work and yields `OpEvent` objects
  (`kind` ∈ `start | phase | progress | item | log | error | done`, plus
  `op`, `phase`, `message`, `metrics`);
- `.result` blocks until completion and returns `T` (or re-raises the error);
- `cancel` is an optional `Callable[[], bool]`; when it returns `True` the
  operation raises `Cancelled`.

```python
op = store.verify()
for event in op:
    print(event.kind, event.op, event.metrics)   # live progress
result = op.result                               # after completion
```

Operations are single-use: iterate once (or just read `.result`).

Abandonment is **deterministic** for `op.close()`, the `with op:` context
manager, and `.result` (locked semantics S1): a closed operation prevents an
unstarted run, releases its resources immediately, and makes `.result` raise
`Cancelled`. A completed operation keeps its result; `.result` never returns
`None` as a stand-in. Discarding a half-consumed iterator is best-effort (the
iterator object's close runs deterministically; CPython additionally delivers
`GeneratorExit` during for-loop unwind). Closing a never-started operation
warns.
Stream puts (`put_stream`, `upsert_chapter_stream`) emit `progress` events
**live** while the source stream is being hashed — and the identity is
computed exactly once (the spooled bytes are never re-hashed). Each stream
put also emits one `phase` event (`phase="persist"`) at the hash→persist
boundary (D6) — the only place a `phase` event is emitted today; other
operations use `start`/`progress`/`item`/`done`.

### Operation reference

| Call | Result |
| --- | --- |
| `store.put_bytes(data, profile, cancel=None)` | `PutResult` |
| `store.put_stream(fp, profile, size_hint=None, cancel=None)` | `PutResult` (`size_hint` is advisory) |
| `store.get_bytes(ref)` / `store.open(ref)` | `bytes` / `io.BytesIO` (not operations) |
| `store.has_blob(blob_key)` | `bool` (not an operation) |
| `store.train_dict(samples, options=None, cancel=None)` | `TrainDictResult` |
| `store.reencode(targets, options=None, cancel=None)` | `ReencodeResult` |
| `store.verify(limit=None, cancel=None)` | `VerifyResult` |
| `store.gc(live, cancel=None)` | `GcResult` |
| `store.compact(options=None, cancel=None)` | `CompactResult` |

`repo` exposes `.store` (the `BlobStore`) and `.backend` (the
`SqliteBackend`) for advanced use and testing.

---

## Profiles

A `Profile` is a **write policy only** (spec §3.4):

```python
Profile(name="zstd_dict", codec="zstd", params={"level": 6}, zstd_dict_id="ikd1:…")
```

- `codec`: `"none"` or `"zstd"`.
- `params`: codec parameters. `zstd` honors `{"level": 1..22}` (default 6).
- `zstd_dict_id`: id of a trained dictionary stored in the repo (index DB
  only). Must be `None` unless `codec == "zstd"`.

**Decoding never consults profiles** — it uses the stored
`encodings.{codec, codec_params_json, zstd_dict_id}` row (spec §6.2). You can
redefine or delete profiles without breaking stored content; `reencode()`
is what migrates stored content to a new policy.

### Profile management on `Repository`

Normal usage never needs `repo.backend.config_get/set("profiles", ...)` —
`Repository` exposes small validated wrappers:

```python
repo.get_profiles()                              # dict[str, Profile]
repo.get_profile("zstd_dict")                    # Profile; UnknownProfile if unknown
repo.set_profile(Profile("zstd_dict", "zstd", {"level": 6}, trained.dict_id))  # add/replace one
repo.set_profiles({"raw": Profile("raw", "none", {})})  # replace the whole set
```

`Profile.params` is immutable (`MappingProxyType`): mutating the dict used to
construct a profile cannot silently change a cached policy, and the profile
set is parsed strictly — a malformed stored config fails at open/set time,
never at the first `put`.

- `set_profile` / `set_profiles` validate exactly like `create_repo` (codec
  must be `"none"`/`"zstd"`, `zstd_dict_id` only with `zstd`, profile key
  must match `Profile.name`), so invalid definitions fail fast at set time
  instead of surfacing later at write time.
- `set_profiles` requires a non-empty set.
- Replacing or removing profiles never breaks stored content — decoding uses
  stored encoding metadata, not the profile set (spec §6.2). Use
  `reencode()` to migrate existing content to a changed policy.

### `train_dict` options

- `{"dict_size": 4096}` — target dictionary size in bytes
  (valid range 256 .. 67,108,864; the actual trained dictionary may be
  smaller). Requires at least **5 non-empty samples** totaling **≥ 8 bytes**
  (constraints of zstd's dictionary trainer).

**Reclamation rule:** a trained dictionary is reclaimable by the next
`gc()` until a profile or an encoding references it — `train_dict` alone
pins nothing, so `train_dict → gc` (without `set_profile` or a put under a
dict profile) silently loses the dict.

### `reencode` options

`reencode(targets)` re-encodes each target in place (same
`(blob_key, profile)` key; payload + encoding metadata updated in one
transaction, spec §7.5) using the target's *current* profile definition.
Per-target data problems (missing payload/dict, corrupt payload, deleted
profile) **skip that target and continue** — a `log` event names the reason —
so one bad chapter never aborts a bulk recompression. To override the policy
without touching profile definitions:

- `{"profile": "other"}` — use `other`'s policy for all targets;
- `{"codec": "zstd", "params": {"level": 9}, "zstd_dict_id": None}` —
  ad-hoc policy.

Unknown option keys, or `profile` combined with `codec`/`params`/
`zstd_dict_id`, raise `ValueError`.

### `compact` options

- `{"shard_ids": [1, 2]}` — vacuum only those shards (plus the index);
  must be a `list[int] | tuple[int, ...]` of non-bool ints (S5 — any other
  shape raises `TypeError` at call time); an **empty** list vacuums the index
  only, unknown ids raise `ValueError` (compact never creates a shard file),
  and `shard_ids` in `sqlite_single` mode raises `ValueError`. All option
  validation happens at call time, before an `Operation` exists.
  Default: index + all shards. `VACUUM` runs on a connection with no attached
  databases.

---

## Error model

| Error | Raised when |
| --- | --- |
| `InkpackError` | base class for all Inkpack errors; also config/layout problems on `open_repo` |
| `NotFound` | a referenced entity does not exist — novels, chapters (catalog rows), or a missing repository on `open_repo` |
| `MissingContent` | stored content is missing — encoding row, payload row, or required dictionary |
| `CorruptContent` | payload fails to decode, identity mismatch, stored-metadata inconsistency (e.g. `codec` ≠ `zstd` carrying a `zstd_dict_id`, lying `stored_len`), or a damaged payload schema |
| `Retryable` | transient contention/race (e.g. an encoding vanished between prepare and persist, or a shard locator kept changing during a read) — safe to retry the call |
| `Busy` | a writer operation times out on a locked database (SQLite busy/locked) |
| `Cancelled` | a cancel token requested abort |
| `UnknownProfile` | unknown profile name (subclasses both `NotFound` and `KeyError`, so either `except` style works) |
| `ValueError` / `TypeError` | invalid arguments, option values, or codec constraints |

**zstd errors are always surfaced as typed API errors** — raw
`zstandard.ZstdError`/`ValueError` exceptions never leak out of Inkpack:

- encode-side problems (bad `level`, unusable dictionary, unsupported codec)
  raise `ValueError` with an actionable message (e.g. which profile/level
  failed);
- decode-side problems raise `CorruptContent` (and are counted as `corrupt`
  by `verify()`);
- dictionary-training constraints (below) raise `ValueError` that tells you
  what is missing, so you know *when* you can build a dictionary:
  `train_dict` needs **at least 5 non-empty samples totaling ≥ 8 bytes** and
  a `dict_size` in `256 .. 67_108_864`.

## Typical workflows

**Fill a novel, then build a dictionary from its chapters and recompress:**

```python
chapter_ids = [repo.upsert_chapter(novel_id, f"{i:04d}", body, "raw") for i, body in enumerate(bodies)]
samples = [repo.get_chapter_bytes(cid) for cid in chapter_ids]
train = repo.store.train_dict(samples).result                     # dict from the novel's own prose

repo.set_profile(Profile("zstd_dict", "zstd", {"level": 6}, train.dict_id))

refs = list(repo.iter_live_content(scope=novel_id))
repo.store.reencode(refs, options={"profile": "zstd_dict"}).result  # shrink in place
```

**One dictionary per novel:** train separately from each novel's chapters,
point each novel's profile at its own dict id, and reencode per novel:

```python
train_a = store.train_dict([repo.get_chapter_bytes(cid) for cid in chapters_a]).result
train_b = store.train_dict([repo.get_chapter_bytes(cid) for cid in chapters_b]).result
repo.set_profile(Profile("zstd_dict_A", "zstd", {"level": 6}, train_a.dict_id))
repo.set_profile(Profile("zstd_dict_B", "zstd", {"level": 6}, train_b.dict_id))
store.reencode(list(repo.iter_live_content(scope=novel_a)), options={"profile": "zstd_dict_A"}).result
store.reencode(list(repo.iter_live_content(scope=novel_b)), options={"profile": "zstd_dict_B"}).result
```

(see `tests/test_workflows.py::test_per_novel_dictionaries` — a novel's own
dictionary compresses its prose measurably better than another novel's
dictionary, and decoding always uses the dictionary recorded in the stored
encoding row).

**Random writes (replace/remove chapters):** `upsert_chapter` with an
existing `chapter_key` (must be `str | int` — S7; `None`/`bool` raise
`TypeError`) replaces the chapter body in place (same chapter id;
the old blob becomes garbage) and commits content + catalog **atomically** —
a failed upsert (missing novel, busy) leaves no orphan blob. Removals are
catalog-only (D12): `repo.delete_chapter(id)` / `repo.delete_novel(id, cascade=True)`
never auto-GC. The two-step pattern is:

```python
repo.delete_chapter(chapter_id)
repo.store.gc(live=repo.iter_live_content()).result   # reclaim the old body
repo.store.verify().result                             # confirm integrity
```

---

## Persistence contract (summary)

Both backends use the same index schema (spec §7): `repo_config`, `novels`,
`chapters`, `meta`, `blobs`, `encodings`, `dicts`; payload bytes live in a
deduplicated `payload(blob_key, profile, data)` table (single DB or shard
DBs). Key invariants:

- **Payload size ceiling.** Each `(blob_key, profile)` payload is **one SQLite
  BLOB**. Stock SQLite builds enforce `SQLITE_MAX_LENGTH ≈ 1,000,000,000`
  bytes (~1 GiB), so a single chapter's **encoded** size is capped at ~1 GiB
  on typical builds. Inkpack probes the live connection's limit and raises a
  typed `ValueError` (naming the attempted size and the limit) **before**
  writing anything — no partial encodings row, no opaque "string or blob too
  big" errors. Tens of GB applies to library size (many chapters / shards),
  not to one chapter. For codec `none`, the raw length IS the stored length,
  so the cap applies to canonical bytes too. A stream put that misses (or
  repairs) still peaks at ~raw + encoded + zstd scratch — the spool avoids
  re-reading the source, not the materialization of a miss. A failed write
  after shard rollover may leave an empty `payload/shard-NNNN.sqlite`; that
  file is harmless — later writes may reuse it and it is never treated as
  repository contents on its own.
- `blob_key` is `ikb1:<raw_len>:<sha256_hex>:<blake2b-128_hex>` and is unique
  (`blobs` PK; `encodings` PK is `(blob_key, profile)`). Parsing is strict
  ASCII canonical (S4): Unicode digits, uppercase hex and other prefixes are
  rejected.
- **Decode bound (S3).** Every read parses `raw_len` from the `blob_key` and
  enforces it as a hard output bound: `zstd` frames whose declared content
  size exceeds it — or that carry no declared size — are `CorruptContent`;
  codec `none` requires `len(payload) == raw_len` exactly. A same-length
  tamper is only caught by `verify_on_read`/`verify()` (identity recompute).
- `blobs.raw_len` is verified against the key on every write (decision J): a
  mismatched stored `raw_len` raises `CorruptContent` and is never silently
  "repaired". `verify()` enforces it per row too (A5: an encoding without a
  `blobs` row, or a `raw_len` that disagrees with the key, counts as
  `corrupt`).
- Dedupe hits (decision E) never rewrite payload/encodings/`updated_at`; if
  the payload row is missing, `put_*` **repairs** it by re-encoding with the
  *stored* policy (never the current profile) — and the same repair applies
  to `upsert_chapter`, so a chapter never commits pointing at missing
  content. When the encoding's shard file is missing or its locator is NULL,
  repair **rehomes** (S2): the payload is written to an explicitly-ensured
  writable shard and `encodings.shard_id` is updated atomically in the same
  transaction. A hit with a missing required dictionary raises `MissingContent`
  ("put succeeded" implies "content is readable"); a stored key whose
  `blob_key` does not match the content is rejected (`CorruptContent`); a hit
  whose stored metadata is undecodable (e.g. `codec` ≠ `zstd` carrying a
  `zstd_dict_id`) is rejected the same way the read path rejects it
  (`CorruptContent`) — the hit probe is symmetric with decode (A4/N7).
- GC keeps dictionaries referenced by **profiles** in `repo_config` alive
  (spec §9 amendment), so `train_dict → set_profile → gc → put` never loses
  the dictionary; drop the profile reference and GC reclaims it.
- `encodings.stored_len` always equals `len(payload.data)`, and **every read
  enforces it** (A5): a payload whose length disagrees with `stored_len` is
  `CorruptContent` from `get_bytes`/`open`/`verify`/`reencode` targets, so a
  lying row can never be silently re-encoded over.
- `encodings.zstd_dict_id` is `NULL` unless `codec == "zstd"` and a dict was
  used; dictionaries live only in the index DB.
- `encodings.codec_params_json` is canonical JSON (sorted keys, compact
  separators).
- `reencode()` updates payload + encoding metadata **in place, in one
  transaction**.
- Sharded mode: `encodings.shard_id` is the authoritative payload locator;
  a `(blob_key, profile)` payload exists in exactly one shard; index + shard
  writes commit atomically via `ATTACH`; index and shard DBs must stay in
  rollback journal mode (`delete`/`truncate`) — `open_repo()` refuses WAL.
- `open(ref)` is `io.BytesIO(get_bytes(ref))` — fully materialized, stable
  even across GC or an in-place reencode (spec §6.3).
- `verify()` classifies each encoding as `ok` / `missing` (payload or dict
  row absent) / `corrupt`. `corrupt` covers decode failure, identity
  mismatch, a damaged payload schema, a missing/lying `blobs.raw_len`, and a
  lying `stored_len` (A3/A5). `MemoryError` is never counted as corrupt — it
  aborts the run (A7). The checked set is **frozen at run start** (the
  iteration order is materialized once, then paged by keyset seek — one sort
  total, never a per-page full-table sort): encodings added mid-verify are
  picked up by the next run; encodings deleted mid-verify classify as
  `missing`.
- `gc(live)` deletes encodings + payloads not in the live set in **batched
  atomic exclusive windows** (Decision G amendment, B1): each batch of up to
  the attach-limit shards runs one `BEGIN IMMEDIATE … COMMIT` that
  recomputes the dead set fresh inside the window, so a put that completed
  before a batch's snapshot is authorizable, and a put racing the window
  blocks, is absent from the snapshot, and self-heals. On multi-batch runs a
  **final idempotent sweep** over the whole shard universe then guarantees
  single-run convergence to "no payload rows without matching encodings"
  (r3-A). Cancellation keeps committed batches; the in-flight batch rolls
  back whole. Then orphan `blobs`, then unreferenced `dicts`. See
  [GUARANTEES.md](GUARANTEES.md).
- `iter_live_content()` pages 1000 rows per short read transaction (C3).
  Visibility is **at least that of a single-snapshot fetch taken at drain
  start** — every chapter existing when the drain starts is captured even
  under concurrent writers; late additions above the cursor appear in a
  later page; mid-drain deletions cause safe over-retention. The exposure
  window is **commits during or after the drain**: such chapters are not
  represented in `live`, and GC will reclaim their content (recovery:
  re-run the upsert) — run `gc(live=iter_live_content())` with no
  concurrent chapter writers, or re-derive `live` after writers quiesce
  (see [GUARANTEES.md](GUARANTEES.md)).
- Schema versioning: `PRAGMA user_version` tracks the index schema;
  forward-only, idempotent migrations (`inkpack/sqlite.py::MIGRATIONS`).

### Repo config keys

Stored in `repo_config` as canonical JSON: `identity_policy` (must be
`"ikb1"`), `backend_mode`, `profiles`, `verify_on_read` (default `false`),
and (sharded) `shard_cap_bytes`, `shard_min_bytes`. `open_repo()` validates
them (identity policy, profiles present, backend-mode match) and restores the
stored shard caps.

### Recovery runbook

Operator steps for the failure modes the guarantees specify — a junk/malformed
shard that makes `open_repo` refuse, a corrupt current-write shard on an
already-open repo, maintenance vs a junk shard, and `synchronous=NORMAL` —
live in [GUARANTEES.md → Recovery runbook](GUARANTEES.md#recovery-runbook).
Short version: move the bad `payload/shard-NNNN.sqlite` aside (never delete
the only copy), reopen, and `gc(live=iter_live_content())` does the
encodings-only pass; restore the shard from backup for content already on it.

---

## Factories

```python
create_repo(path, backend_mode="sqlite_single", profiles=None,
            pragmas=None, shard_cap_bytes=2 << 30, min_shard_cap_bytes=None,
            verify_on_read=False, *, identity=None, codec=None, clock=None,
            shard_min_bytes=None) -> Repository   # shard_min_bytes: DEPRECATED alias

open_repo(path, pragmas=None, *, identity=None, codec=None, clock=None) -> Repository
```

- **create vs open are distinct operations.** `create_repo` refuses an
  existing repository (`InkpackError`; markers OR any `payload/shard-*.sqlite`
  count as "existing") and requires a non-empty validated profile set
  (`profiles=None` or `{}` raises `ValueError`). Creation is all-or-nothing
  (D5): built in a sibling temp dir and atomically renamed into place, so a
  failed create leaves nothing behind and a retry works. `~/...` is expanded
  in both factories (D11). `open_repo` **never creates files**: it detects
  the layout from the filesystem (`index.sqlite` + `payload/` →
  `sqlite_sharded`, `repo.sqlite` → `sqlite_single`), raises `InkpackError`
  for an ambiguous or broken layout (both markers, or `index.sqlite` without
  `payload/`), refuses non-canonical shard filenames (D2), and raises
  `NotFound` only when no repository exists.
- `pragmas`: `{"busy_timeout_ms": 5000, "synchronous": "NORMAL"}` (the only
  knobs; unknown keys raise `ValueError`). `synchronous` is applied to
  **every** connection (including VACUUM and shard reads), not just at
  creation — it is per-connection, so there is no persistent substitute.
- `verify_on_read`: when `True`, `get_bytes`/`open` recompute the identity
  and raise `CorruptContent` on mismatch (slower; `verify()` is the default
  full check). Read once per call. Toggle at runtime with
  `repo.set_verify_on_read(flag)` (D2; strict `bool`, one txn RMW).
- `identity` / `codec` / `clock`: dependency-injection hooks (see below). A
  custom `Identity` must implement `hasher()` — `put_stream` /
  `upsert_chapter_stream` hash through the incremental hasher.
- `shard_cap_bytes`/`min_shard_cap_bytes` apply to `sqlite_sharded` only.
  `min_shard_cap_bytes` (persisted under the stable key `shard_min_bytes`) is
  the allowed floor for `shard_cap_bytes` (positive non-bool ints,
  `min ≤ cap`); **routing uses only `shard_cap_bytes`** (D4) — writes roll to
  a fresh shard once the current shard file exceeds the cap (the write
  pointer is the highest shard id, never the lexically-last path). The
  legacy `shard_min_bytes` keyword is accepted with a `DeprecationWarning`
  for one minor cycle (D1).
- Connection policy (scoped to **BlobStore operations**, C5): one
  operation-scoped session per `BlobStore` call — `get_bytes` opens 1
  connection, `verify`/`reencode`/`gc` are O(1) connections with O(shards)
  session-managed ATTACHes (C1) instead of one per row, and GC's TEMP tables
  live for the whole operation. `compact()` still VACUUMs on a virgin
  connection with no attached databases. `Repository` catalog calls
  (novels/chapters/meta) each open their own short-lived connection.
- **Write pointer (N6).** Writes go to the highest shard id; `compact()` does
  not move writes backward, and empty low shards stay until an admin deletes
  them.
- **Snapshot reads (D1).** Every read of encodings + payload is ONE snapshot:
  sharded reads `ATTACH` the shard to the index connection inside the read
  transaction — never a second connection.
- **Exclusive writers.** Maintenance writers are exclusive with each other
  (`Busy` on lock); `synchronous=NORMAL` is the default and is not full
  durability (use `FULL` if you need it).

### Dependency injection

<!-- runnable -->
```python
from pathlib import Path
from inkpack import Profile, create_repo
from inkpack.codec import CodecEngine, IKB1
from inkpack.sqlite import SqliteBackend
from inkpack.blobstore import BlobStore
from inkpack.repo import Repository

# create_repo does this composition for you; the parts are public for
# advanced wiring (custom codec engines / identities / clocks).
root = Path.home() / "inkpack-di-demo"
create_repo(root, profiles={"raw": Profile("raw", "none", {})})
backend = SqliteBackend.open(root, mode="sqlite_single")
store = BlobStore(backend=backend, codec=CodecEngine(), identity=IKB1)
repo = Repository(backend=backend, store=store)
assert repo.get_profile("raw").codec == "none"
```

The default identity is `ikb1`; a custom identity must declare `name` and
match the stored `identity_policy` when the repo is reopened.

---

## Changelog

- **`upsert_chapter`/`upsert_chapter_stream` hints (behavior change).**
  `hints=None` (the default) now **preserves** the chapter's stored
  `media_type`/`charset` on a re-upsert instead of NULLing them; a provided
  `hints` dict **replaces the hints group** (present keys written, an omitted
  key means NULL — `{}` clears both). `meta` was already merged per key and
  is unchanged. If you relied on re-upserting with default hints to clear
  the hints, pass `hints={}` explicitly.
- **`create_repo` sharded caps (behavior change).** Omitting
  `min_shard_cap_bytes` now derives `min(DEFAULT_SHARD_MIN_BYTES,
  shard_cap_bytes)` instead of defaulting to 256 MiB — so a small
  `shard_cap_bytes` is creatable without also passing a min. An explicit min
  is validated as before (`min > cap` still raises).
- **Recovery runbook** added (GUARANTEES.md): junk-shard open refusal,
  corrupt current-write shard, maintenance vs a junk shard,
  `synchronous=NORMAL`.

## Development

```bash
pip install -r requirements-dev.txt
python -m pytest                     # full suite (both backends + fuzz + hypothesis)
python -m pytest -m slow             # bench-gated timing tests (C4), excluded by default
ruff check inkpack tests             # lint
mypy inkpack --strict                # type check
pyright inkpack                      # type check (strict)
```

The test-suite is organized by contract area:

```
tests/
  conftest.py                   # fixtures, whitebox helpers, consistency invariant
  test_ops_contract.py          # Operation/OpEvent contract
  test_schema.py                # schema, migrations, repo-config validation
  test_identity_dedupe.py       # ikb1 identity + dedupe semantics
  test_put_get_open.py          # read semantics (stable reads, stored-metadata decoding)
  test_json_len_invariants.py   # canonical JSON + stored_len invariants
  test_dicts.py                 # zstd dictionary training/usage/GC
  test_reencode_verify_gc.py    # maintenance semantics
  test_compact_busy_cancel.py   # VACUUM, Busy mapping, cancellation
  test_repo_model.py            # novels/chapters/KV metadata/live refs
  test_sharded_backend.py       # shard routing, journal-mode enforcement
  test_fuzz.py                  # seeded fuzz + hypothesis property tests
  test_r2_error_model.py        # A: typed-error model (no raw sqlite leaks, etc.)
  test_r2_concurrency.py        # B: GC exclusive window, txn placement (H1 failpoints)
  test_r2_performance.py        # C: session attaches, zstd context reuse, paging
  test_r2_api_docs.py           # D/E: API surface + maintainability guards
  test_r2_docs.py               # F: README runnable blocks execute as written
```

## Scope

Inkpack stores **text content only**. It does not implement pack/VMDK segment
formats, images/video/transforms, or group codecs.
