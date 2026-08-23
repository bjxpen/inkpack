"""F1 / r4-P0.1: README runnable fences must be executable as written, and
the quickstart must not delete content it still holds a name for.

Blocks marked ``<!-- runnable -->`` are compiled and executed in a sandboxed
temp home, so the README can never drift into non-runnable prose again. The
P0.1 guard additionally execs the quickstart and asserts every ``ContentRef``
/ ``PutResult`` the block still binds a name for is readable AFTER the block's
own GC — the defect was a standalone ``put_bytes`` whose ref the quickstart's
``gc(live=chapter refs)`` silently reclaimed.
"""

from __future__ import annotations

import re
from pathlib import Path

RUNNABLE = re.compile(r"<!--\s*runnable\s*-->\s*```python\n(.*?)```", re.S)


def _sandbox(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))


def test_readme_runnable_blocks_execute(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    blocks = RUNNABLE.findall((root / "README.md").read_text())
    # P0.1: the quickstart is now TWO fences (demo + cleanup) plus the DI
    # block.
    assert len(blocks) >= 3, "runnable markers lost (demo/cleanup/DI expected)"
    _sandbox(tmp_path, monkeypatch)
    for i, block in enumerate(blocks):
        # Each block must be self-contained: fresh namespace, temp home.
        exec(compile(block, f"<README runnable {i}>", "exec"), {})


def test_readme_quickstart_refs_survive_its_own_gc(tmp_path, monkeypatch):
    """Every ref the quickstart still holds a name for must be readable after
    its own GC (P0.1). TODAY red: the first standalone ``put``'s ``ref``/
    ``put`` are bound, and the quickstart's ``gc(live=chapter refs)``
    reclaims that non-chapter content -> MissingContent."""
    from inkpack import ContentRef, PutResult, open_repo

    root = Path(__file__).resolve().parents[1]
    text = (root / "README.md").read_text()
    blocks = RUNNABLE.findall(text)
    assert len(blocks) >= 3, "runnable markers lost (demo/cleanup/DI expected)"
    _sandbox(tmp_path, monkeypatch)
    ns: dict = {}
    exec(compile(blocks[0], "<README demo>", "exec"), ns)
    repo = open_repo(tmp_path / "novels")

    def refs_of(v):
        if isinstance(v, ContentRef):
            yield v
        elif isinstance(v, PutResult):
            yield v.ref

    kept = [r for v in ns.values() for r in refs_of(v)]
    assert kept, "demo must retain at least one named ref"
    for r in kept:
        repo.store.get_bytes(r)  # TODAY: MissingContent for a standalone put's ref
    assert repo.store.verify().result.missing == 0
    # The lesson must be stated where the GC line is:
    assert "invisible to repository GC" in text

    exec(compile(blocks[1], "<README cleanup>", "exec"), ns)  # cleanup still runs clean


def test_readme_has_no_prose_inside_quickstart_fence(tmp_path):
    """The has_blob sentence used to sit inside the python fence (F1)."""
    root = Path(__file__).resolve().parents[1]
    text = (root / "README.md").read_text()
    # The prose line must appear OUTSIDE any code fence.
    fences = text.split("```")
    outside = "\n".join(fences[0::2])  # even segments are outside fences
    assert "is True iff a" in outside and "catalog row exists" in outside
