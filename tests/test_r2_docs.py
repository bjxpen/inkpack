"""F1: README quickstart and DI blocks must be executable as written.

Blocks marked ``<!-- runnable -->`` are compiled and executed in a sandboxed
temp home, so the README can never drift into non-runnable prose again.
"""

from __future__ import annotations

import re
from pathlib import Path

RUNNABLE = re.compile(r"<!--\s*runnable\s*-->\s*```python\n(.*?)```", re.S)


def test_readme_runnable_blocks_execute(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    blocks = RUNNABLE.findall((root / "README.md").read_text())
    assert len(blocks) >= 2, "runnable markers lost from README"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    for i, block in enumerate(blocks):
        # Each block must be self-contained: fresh namespace, temp home.
        exec(compile(block, f"<README runnable {i}>", "exec"), {})


def test_readme_has_no_prose_inside_quickstart_fence(tmp_path):
    """The has_blob sentence used to sit inside the python fence (F1)."""
    root = Path(__file__).resolve().parents[1]
    text = (root / "README.md").read_text()
    # The prose line must appear OUTSIDE any code fence.
    fences = text.split("```")
    outside = "\n".join(fences[0::2])  # even segments are outside fences
    assert "is True iff a" in outside and "catalog row exists" in outside
