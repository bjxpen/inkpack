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
  (delete unreferenced encodings/payloads/blobs/dicts, atomically per shard),
  `compact()` (`VACUUM` of index and/or shards).
- **KV metadata** — `meta_set/get/list` for novels and chapters, with strict
  entity-id normalization (decimal string of the integer primary key).
- **Typed errors** — `NotFound`, `MissingContent`, `CorruptContent`, `Busy`,
  `Cancelled`.
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

```python
from inkpack import create_repo, Profile

repo = create_repo(
    path="~/novels",
    backend_mode="sqlite_single",            # or "sqlite_sharded"
    profiles={
        "raw":        Profile("raw", "none", {}),
        "zstd_nodict": Profile("zstd_nodict", "zstd", {"level": 3}),
    },
)
store = repo.store

# --- content -------------------------------------------------------------
put = store.put_bytes(b"chapter body \x00\xff (opaque bytes)", profile="raw")
ref = put.result.ref                        # ContentRef(blob_key, profile)
assert store.get_bytes(ref) == b"chapter body \x00\xff (opaque bytes)"
handle = store.open(ref)                    # io.BytesIO, safe to hold
data = handle.read()

# streaming put (non-seekable sources OK; hashed in one pass)
with open("chapter.txt", "rb") as fh:
    put = store.put_stream(fh, profile="zstd_nodict").result

# --- repository model -----------------------------------------------------
novel_id = repo.create_novel("My Novel", meta={"genre": "fantasy"})
chapter_id = repo.upsert_chapter(
    novel_id, "ch-001", b"once upon a time...", "zstd_nodict",
    hints={"media_type": "text/plain", "charset": "utf-8"},
    meta={"words": 4200},
)
assert repo.get_chapter_bytes(chapter_id) == b"once upon a time..."
assert repo.open_chapter(chapter_id).read() == b"once upon a time..."

repo.meta_set("novel", novel_id, "rating", 5)
repo.meta_get("novel", novel_id, "rating")   # 5
repo.meta_list("chapter", chapter_id)        # {"words": 4200, ...}

# --- catalog (no bodies) ---------------------------------------------------
novel = repo.get_novel(novel_id)             # dict row; NotFound if missing
repo.update_novel(novel_id, title="…", slug="…")
chapters = repo.list_chapters(novel_id)      # list[ChapterInfo], no bodies
info = repo.get_chapter(chapter_id)          # ChapterInfo
repo.delete_chapter(chapter_id)              # catalog only; then gc(iter_live_content())
repo.delete_novel(novel_id, cascade=True)    # catalog only; content reclaimed by gc

# streaming chapter upsert (progress events, atomic content+catalog commit)
op = repo.upsert_chapter_stream(fh, novel_id, "ch-002", "zstd_nodict")
chapter_id = op.result                       # int; OpEvents while iterating

# --- dictionaries ---------------------------------------------------------
trained = store.train_dict([b"sample prose " * 100] * 5).result
repo.set_profile(Profile("zstd_dict", "zstd", {"level": 6}, trained.dict_id))
put = store.put_bytes(b"sample prose " * 2000, profile="zstd_dict").result
assert put.zstd_dict_id == trained.dict_id

# --- maintenance -----------------------------------------------------------
verify = store.verify().result               # VerifyResult(checked, ok, missing, corrupt)
live = list(repo.iter_live_content())        # ContentRefs referenced by chapters
gc = store.gc(live=live).result              # GcResult(...)
compacted = store.compact().result           # CompactResult(mode, targets)
reencoded = store.reencode(live).result      # ReencodeResult(...)

# --- reopen ----------------------------------------------------------------
from inkpack import open_repo
repo2 = open_repo("~/novels")
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

### Operation reference

| Call | Result |
| --- | --- |
| `store.put_bytes(data, profile, cancel=None)` | `PutResult` |
| `store.put_stream(fp, profile, size_hint=None, cancel=None)` | `PutResult` |
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

### `reencode` options

`reencode(targets)` re-encodes each target in place (same
`(blob_key, profile)` key; payload + encoding metadata updated in one
transaction, spec §7.5) using the target's *current* profile definition. To
override the policy without touching profile definitions:

- `{"profile": "other"}` — use `other`'s policy for all targets;
- `{"codec": "zstd", "params": {"level": 9}, "zstd_dict_id": None}` —
  ad-hoc policy.

Unknown option keys, or `profile` combined with `codec`/`params`/
`zstd_dict_id`, raise `ValueError`.

### `compact` options

- `{"shard_ids": [1, 2]}` — vacuum only those shards (plus the index).
  Default: index + all shards. `VACUUM` runs on a connection with no attached
  databases.

---

## Error model

| Error | Raised when |
| --- | --- |
| `InkpackError` | base class for all Inkpack errors; also config/layout problems on `open_repo` |
| `NotFound` | a referenced entity (e.g. novel) does not exist |
| `MissingContent` | encoding row, payload row, or required dictionary is missing |
| `CorruptContent` | payload fails to decode, or identity mismatch under `verify_on_read` |
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
existing `chapter_key` replaces the chapter body in place (same chapter id;
the old blob becomes garbage) and commits content + catalog **atomically** —
a failed upsert (missing novel, busy) leaves no orphan blob. Removals are
catalog-only: `repo.delete_chapter(id)` / `repo.delete_novel(id, cascade=True)`
never auto-GC. The two-step pattern is:
`delete_chapter(id)` → `store.gc(live=repo.iter_live_content())` → `verify()`.

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
  so the cap applies to canonical bytes too.
- `blob_key` is `ikb1:<raw_len>:<sha256_hex>:<blake2b-128_hex>` and is unique
  (`blobs` PK; `encodings` PK is `(blob_key, profile)`).
- `blobs.raw_len` is verified against the key on every write (decision J): a
  mismatched stored `raw_len` raises `CorruptContent` and is never silently
  "repaired".
- Dedupe hits (decision E) never rewrite payload/encodings/`updated_at`; if
  the payload row is missing, `put_*` **repairs** it by re-encoding with the
  *stored* policy (never the current profile), and raises `MissingContent`
  when the stored dictionary is gone.
- `encodings.stored_len` always equals `len(payload.data)`.
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
  even across GC (spec §6.3).
- `verify()` classifies each encoding as `ok` / `missing` (payload or dict
  row absent) / `corrupt` (decode failure or identity mismatch).
- `gc(live)` deletes encodings + payloads not in the live set (atomically
  per shard), then orphan `blobs`, then unreferenced `dicts`.
- Schema versioning: `PRAGMA user_version` tracks the index schema;
  forward-only, idempotent migrations (`inkpack/sqlite.py::MIGRATIONS`).

### Repo config keys

Stored in `repo_config` as canonical JSON: `identity_policy` (must be
`"ikb1"`), `backend_mode`, `profiles`, `verify_on_read` (default `false`),
and (sharded) `shard_cap_bytes`, `shard_min_bytes`. `open_repo()` validates
them (identity policy, profiles present, backend-mode match) and restores the
stored shard caps.

---

## Factories

```python
create_repo(path, backend_mode="sqlite_single", profiles=None,
            pragmas=None, shard_cap_bytes=2 << 30, shard_min_bytes=256 << 20,
            verify_on_read=False, *, identity=None, codec=None, clock=None) -> Repository

open_repo(path, pragmas=None, *, identity=None, codec=None, clock=None) -> Repository
```

- **create vs open are distinct operations.** `create_repo` refuses an
  existing repository (`InkpackError`) and requires a non-empty validated
  profile set (`profiles=None` or `{}` raises `ValueError`). `open_repo`
  **never creates files**: it detects the layout from the filesystem
  (`index.sqlite` + `payload/` → `sqlite_sharded`, `repo.sqlite` →
  `sqlite_single`), raises `InkpackError` for an ambiguous layout (both
  present), and raises `NotFound` when no repository exists.
- `pragmas`: `{"busy_timeout_ms": 5000, "synchronous": "NORMAL"}` (the only
  knobs; unknown keys raise `ValueError`). `synchronous` is applied to
  **every** connection (including VACUUM and shard reads), not just at
  creation — it is per-connection, so there is no persistent substitute.
- `verify_on_read`: when `True`, `get_bytes`/`open` recompute the identity
  and raise `CorruptContent` on mismatch (slower; `verify()` is the default
  full check). Read once per call.
- `identity` / `codec` / `clock`: dependency-injection hooks (see below).
- `shard_cap_bytes`/`shard_min_bytes` apply to `sqlite_sharded` only; the
  cap rolls writes to a fresh shard once the current shard file exceeds it
  (the write pointer is the highest shard id, never the lexically-last path).
- Connection policy: one operation-scoped session per public call — `get_bytes`
  opens 1 connection (2 sharded), `verify`/`reencode`/`gc` are O(1)-O(shards)
  connections instead of one per row, and GC's TEMP tables live for the whole
  operation. `compact()` still VACUUMs on a virgin connection with no attached
  databases.

### Dependency injection

```python
from inkpack.codec import CodecEngine, IKB1
from inkpack.sqlite import SqliteBackend
from inkpack.blobstore import BlobStore
from inkpack.repo import Repository

backend = SqliteBackend.open(path, mode="sqlite_single")
store = BlobStore(backend=backend, codec=CodecEngine(), identity=IKB1)
repo = Repository(backend=backend, store=store)
```

The default identity is `ikb1`; a custom identity must declare `name` and
match the stored `identity_policy` when the repo is reopened.

---

## Development

```bash
pip install -r requirements-dev.txt
python -m pytest                     # full suite (both backends + fuzz + hypothesis)
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
```

## Scope

Inkpack stores **text content only**. It does not implement pack/VMDK segment
formats, images/video/transforms, or group codecs.
