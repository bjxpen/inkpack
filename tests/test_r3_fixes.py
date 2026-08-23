"""Fail-first tests for the r3 follow-up register (A-F).

Every test was designed to fail on the pre-fix code and locks the fixed
behavior; doc-guard tests pin contract phrases (rewording the guarantee
requires updating the guard, which forces the change through review).

- A  GC wrong-shard payload residue across batches -> final idempotent sweep
     (one-run convergence; the "residue impossible" claim was false)
- B  live-set freshness obligation stated at the real exposure window;
     C3 "never miss a live ref" overclaim removed (doc-guard)
- C  verify pagination must be seek-shaped, never sorter-shaped (EQP gate,
     deterministic, fix-agnostic)
- D  GC degrades to an encodings-only pass when attach races a shard vanish
- E  GUARANTEES.md completions: batch-universe composition, open-validation
     determinism (doc-guard)
- F  zstandard pin guard (C2's share-safety guarantee rests on it)
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from inkpack import MissingContent
from inkpack.sqlite import Session, SqliteBackend

from .conftest import assert_repo_consistent, enc_row, payload_bytes

ROOT = Path(__file__).resolve().parents[1]


def _norm(text: str) -> str:
    """Collapse whitespace so the guards pin WORDING, not markdown line-wraps."""
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------
# A — GC cross-batch wrong-shard residue: one-run convergence
# ---------------------------------------------------------------------------


def test_gc_sweeps_wrong_shard_payload_across_batches(repo_sharded, monkeypatch):
    """Cross-batch residue: a stale payload copy parked in an EARLIER batch
    than its encoding's locator shard survives today's batch-restricted sweep
    (batch A sweeps while the encoding is still alive; batch B then deletes
    the encoding; shard A is never revisited). ONE gc run must converge to
    "no payload rows without matching encodings".

    Directional asymmetry — do not simplify this test into a case that never
    failed: copies in a LATER batch than the locator are already reclaimed
    (the encoding is gone when that batch's sweep runs), and NULL-locator
    copies always converge within one run.
    """
    monkeypatch.setattr(SqliteBackend, "_gc_batch_size", lambda self, s: 1)
    repo_sharded.store.put_bytes(b"a" * 1_500_000, "raw").result  # fills shard 1
    ref = repo_sharded.store.put_bytes(b"b" * 700_000, "raw").result.ref  # rolls to shard 2
    old = int(enc_row(repo_sharded, ref)["shard_id"])
    assert old >= 2
    earlier = old - 1
    payload = payload_bytes(repo_sharded, ref)
    with repo_sharded.backend.txn(write=True, attach_shard_id=earlier) as conn:
        conn.execute(
            "INSERT INTO p.payload(blob_key, profile, data) VALUES(?,?,?)",
            (ref.blob_key, ref.profile, payload),
        )

    repo_sharded.store.gc(live=[]).result  # one run must converge

    with repo_sharded.backend.session() as s:
        assert not s.payload_exists(ref.blob_key, ref.profile, earlier)  # today: True
        assert not s.payload_exists(ref.blob_key, ref.profile, old)
    assert_repo_consistent(repo_sharded)


def test_gc_single_batch_run_converges_without_final_sweep(repo_sharded):
    """Two shards under the DEFAULT batch size = one batch: the in-batch
    sweep covers the whole universe, so a wrong-shard copy is reclaimed in
    one run even though the multi-batch final sweep is skipped (batch size
    >= shard count). Guard: the skip must not regress single-batch
    convergence — it comes from the in-batch sweep."""
    repo_sharded.store.put_bytes(b"a" * 1_500_000, "raw").result  # shard 1
    ref = repo_sharded.store.put_bytes(b"b" * 700_000, "raw").result.ref  # shard 2
    old = int(enc_row(repo_sharded, ref)["shard_id"])
    assert old >= 2
    earlier = old - 1
    # Default batch size (attach limit - 1, >= 9 on stock builds) >= 2 shards
    # => this whole run is a single batch; the final sweep must be skipped.
    assert len(repo_sharded.backend.list_shards()) == 2
    payload = payload_bytes(repo_sharded, ref)
    with repo_sharded.backend.txn(write=True, attach_shard_id=earlier) as conn:
        conn.execute(
            "INSERT INTO p.payload(blob_key, profile, data) VALUES(?,?,?)",
            (ref.blob_key, ref.profile, payload),
        )
    repo_sharded.store.gc(live=[]).result
    with repo_sharded.backend.session() as s:
        assert not s.payload_exists(ref.blob_key, ref.profile, earlier)
        assert not s.payload_exists(ref.blob_key, ref.profile, old)
    assert_repo_consistent(repo_sharded)


# ---------------------------------------------------------------------------
# B — live-set freshness + honest C3 visibility (doc-guard)
# ---------------------------------------------------------------------------


def test_guarantees_state_live_set_freshness_and_honest_visibility():
    """The docs are contract: the exposure window must be stated where it
    actually is, and the "never miss a live ref" overclaim must be gone
    everywhere."""
    guarantees = _norm((ROOT / "GUARANTEES.md").read_text())
    readme = _norm((ROOT / "README.md").read_text())
    # The exposure window must be stated where it actually is…
    assert "commits during or after the drain" in guarantees
    # …and the overclaim must be gone everywhere.
    assert "never miss a live ref" not in guarantees
    assert "never miss a live ref" not in readme


# ---------------------------------------------------------------------------
# C — verify pagination must be seek-shaped (EQP plan-shape gate)
# ---------------------------------------------------------------------------


def test_verify_pagination_avoids_full_sort(repo, monkeypatch):
    """Pagination must be seek-shaped, never sorter-shaped. Spies EVERY
    ordered query in the run (not just encodings-page shapes) so the gate
    stays valid whichever pagination design lands. EXPLAIN runs on the
    session's own connection because a temp-table design's page query only
    exists on that connection (a dedicated connection cannot see it)."""
    for i in range(5):
        repo.store.put_bytes(f"qp-{i}".encode(), "raw").result
    plans: list[str] = []
    original = Session.query_all

    def spy(self, sql, params=()):
        if "ORDER BY" in sql:
            plan_rows = self.conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
            plans.append("\n".join(str(r[3]) for r in plan_rows))
        return original(self, sql, params)

    monkeypatch.setattr(Session, "query_all", spy)
    repo.store.verify(limit=3).result
    joined = "\n".join(plans)
    assert plans, "no ordered pagination query observed"
    assert "TEMP B-TREE FOR ORDER BY" not in joined  # today: present -> red


@pytest.mark.slow
def test_verify_100k_rows_bounded_time(repo):
    """Opt-in scaling probe (r3-C, slow lane): the pagination cliff
    (O(pages · N log N) full sorts per page) must not dominate a large
    verify. Rows are bulk-inserted directly (codec 'none', valid key shape)
    so the probe measures the pagination, not 100k put transactions.
    Generous threshold — the deterministic plan-shape gate
    (test_verify_pagination_avoids_full_sort) is the load-bearing check;
    this keeps the cliff visible at scale. Run with: python -m pytest -m slow
    """
    import time

    n = 100_000
    shard_id = 1 if repo.backend.mode == "sqlite_sharded" else None
    keys = [f"ikb1:10:{i:064x}:" + "cd" * 16 for i in range(n)]
    with repo.backend.txn(write=True, attach_shard_id=shard_id) as conn:
        conn.executemany(
            "INSERT INTO encodings(blob_key, profile, codec, codec_params_json, "
            "zstd_dict_id, stored_len, checksum, shard_id, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            [(k, "raw", "none", "{}", None, 10, None, shard_id, "now") for k in keys],
        )
        conn.executemany(
            "INSERT INTO blobs(blob_key, raw_len, created_at) VALUES (?,?,?)",
            [(k, 10, "now") for k in keys],
        )
        table = "p.payload" if repo.backend.mode == "sqlite_sharded" else "payload"
        conn.executemany(
            f"INSERT INTO {table}(blob_key, profile, data) VALUES (?,?,?)",
            [(k, "raw", b"0123456789") for k in keys],
        )
    t0 = time.perf_counter()
    result = repo.store.verify().result
    elapsed = time.perf_counter() - t0
    assert result.checked == n
    # ~5-10s measured locally; 120s is a ~15-25x allowance for slow CI.
    assert elapsed < 120.0, f"verify over {n} rows took {elapsed:.1f}s (pagination cliff?)"


# ---------------------------------------------------------------------------
# D — GC degrades (encodings-only) when attach races a shard vanish
# ---------------------------------------------------------------------------


def test_gc_degrades_to_encodings_only_when_attach_races_a_vanish(repo_sharded, monkeypatch):
    """gc pre-checks exists(), but a file vanishing in the window between
    that check and attach()'s re-check must NOT abort the whole run — the
    documented "vanished shard => encodings-only pass" must hold under the
    race (deterministic simulation: the flaky attach unlinks the file and
    raises MissingContent, exactly what the real race produces)."""
    ref = repo_sharded.store.put_bytes(b"race-vanish " * 100, "raw").result.ref
    sid = int(enc_row(repo_sharded, ref)["shard_id"])
    real_attach = Session.attach
    armed = {"once": True}

    def flaky(self, shard):
        if shard == sid and armed.pop("once", False):
            # gc's exists() passed; the file vanishes before attach()'s check.
            repo_sharded.backend.shard_path(shard).unlink(missing_ok=True)
            raise MissingContent(f"shard {shard} missing")
        return real_attach(self, shard)

    monkeypatch.setattr(Session, "attach", flaky)
    result = repo_sharded.store.gc(live=[]).result  # today: MissingContent aborts
    assert result.encodings_deleted == 1  # encodings-only pass happened
    assert enc_row(repo_sharded, ref) is None
    assert_repo_consistent(repo_sharded)


# ---------------------------------------------------------------------------
# E — GUARANTEES.md completions (doc-guard)
# ---------------------------------------------------------------------------


def test_guarantees_documents_batch_universe_and_open_determinism():
    text = _norm((ROOT / "GUARANTEES.md").read_text())
    assert "union of on-disk canonical shards" in text  # batch universe
    assert "lowest failing shard id" in text  # C4 determinism


# ---------------------------------------------------------------------------
# F — zstandard pin guard (C2 share-safety rests on the pinned range)
# ---------------------------------------------------------------------------


def test_zstandard_dependency_is_pinned():
    req = (ROOT / "requirements.txt").read_text()
    assert re.search(r"(?im)^zstandard[=<>~!]", req), (
        "zstandard must carry a version constraint (context cache share-safety)"
    )
