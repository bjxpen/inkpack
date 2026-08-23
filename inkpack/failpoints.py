"""Test-only interception points (H1).

Production behavior: the registry is empty, so every ``failpoint`` check is a
single dict lookup — no thread-timing dependence, no overhead worth
mentioning. Names are internal API; adding or moving a site requires a
changelog entry.

Registration (tests)::

    from inkpack import failpoints
    monkeypatch.setitem(failpoints._REGISTRY, "<name>", fn)

Rules for hooks:
- a hook must never mutate the repository synchronously from the
  intercepted thread while a write transaction is held (deadlock /
  self-contention) — spawn threads to simulate concurrent writers;
- hooks fire only when registered; an unregistered name is a no-op.

Initial sites:
- ``gc.post_snapshot`` — inside the batch transaction, after ``temp_dead``
  is computed (B1: the exclusive window is open).
- ``prepare.post_hit_probe`` — healthy dedupe-hit branch, after the payload
  existence probe (B3: the window between probe and commit).
- ``upsert.pre_commit`` — inside the unified upsert transaction, after the
  catalog row is written, before commit (B3).
"""

from __future__ import annotations

from collections.abc import Callable

_REGISTRY: dict[str, Callable[[], None]] = {}


def failpoint(name: str) -> None:
    """Fire the hook registered under ``name`` (no-op when unregistered)."""
    cb = _REGISTRY.get(name)
    if cb is not None:
        cb()
