"""r4.1 locks: fail-first tests (red on the pre-fix tree) and PINs (green,
lock a rule so it cannot flip silently) for the r4.1 change document.

Every test is labeled [FAIL-FIRST] or [PIN] in its docstring. Uses the
PROFILES / repo fixture pattern from test_review5_locks.py.
"""

from __future__ import annotations

import pytest

from inkpack import Profile

PROFILES = {
    "raw": Profile("raw", "none", {}),
    "zstd_nodict": Profile("zstd_nodict", "zstd", {"level": 3}),
}


@pytest.fixture(params=["sqlite_single", "sqlite_sharded"])
def repo(tmp_path, request):
    from .conftest import make_repo

    return make_repo(tmp_path, request.param)


# ---------------------------------------------------------------------------
# P0.4 [PIN] — hit/read ignores codec_params_json (decode does; L27)
# ---------------------------------------------------------------------------


def test_hit_ignores_non_object_codec_params_json(repo):
    """[PIN] N7/L27: params are not a read/hit concern. A dedupe hit with
    non-object ``codec_params_json`` is genuinely readable and succeeds;
    verify agrees (counts ok). Repair still rejects unusable params (M8,
    locked in test_known_issues). Passes today; locks the rule."""
    data = b"params-hit " * 20
    ref = repo.store.put_bytes(data, "raw").result.ref
    with repo.backend.txn(write=True) as conn:
        conn.execute(
            "UPDATE encodings SET codec_params_json=? WHERE blob_key=?",
            ("[]", ref.blob_key),
        )
    again = repo.store.put_bytes(data, "raw").result
    assert again.ref == ref
    assert repo.store.get_bytes(ref) == data
    assert repo.store.verify().result.ok == 1  # verify agrees: not a read finding


# ---------------------------------------------------------------------------
# P0.3 [FAIL-FIRST] — "never miss(ed)" is gone from repo.py too
# ---------------------------------------------------------------------------
# (The guard lives in tests/test_r3_fixes.py::test_no_never_miss_live_ref_claim_anywhere,
# extending the r3-B guard to the module docstring surface.)
