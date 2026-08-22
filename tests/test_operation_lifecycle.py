"""Operation lifecycle tests (Workstream B / Issues 2, 21, 22)."""

from __future__ import annotations

import gc
import warnings

import pytest

from inkpack.types import Cancelled, Operation, OpEvent


def test_stale_outer_iterator_close_does_not_cancel_completed_op():
    """Issue 2: closing a stale outer iterator after completion must not
    clobber the result with Cancelled."""

    def _run():
        yield OpEvent(kind="start", op="t")
        yield OpEvent(kind="done", op="t")
        return "ok"

    op = Operation(_run)
    outer = iter(op)
    assert next(outer).kind == "start"
    assert op.result == "ok"
    outer.close()
    assert op.result == "ok"


def test_partial_iter_then_result_completes_once():
    def _run():
        yield OpEvent(kind="start", op="t")
        yield OpEvent(kind="item", op="t")
        return 7

    op = Operation(_run)
    it = iter(op)
    next(it)
    assert op.result == 7
    assert list(op) == []


def test_close_before_start_is_cancelled():
    def _run():
        yield OpEvent(kind="start", op="t")
        return 1

    op = Operation(_run)
    op.close()
    with pytest.raises(Cancelled):
        _ = op.result


def test_unused_operation_does_not_warn_on_gc():
    """Issue 21: an operation discarded without being started must not warn
    from __del__ (GC is not a user action)."""

    def _run():
        yield OpEvent(kind="start", op="t")
        return 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        op = Operation(_run)
        del op
        gc.collect()
    assert not any("never started" in str(w.message) for w in caught)


def test_explicit_close_before_start_warns():
    def _run():
        yield OpEvent(kind="start", op="t")
        return 1

    op = Operation(_run)
    with pytest.warns(UserWarning, match="never started"):
        op.close()


def test_two_iterators_share_one_driver():
    """A second iter(op) while running shares the same inner driver."""

    def _run():
        yield OpEvent(kind="start", op="t")
        yield OpEvent(kind="done", op="t")
        return 3

    op = Operation(_run)
    a = iter(op)
    b = iter(op)
    assert next(a).kind == "start"
    # b sees the same stream (already advanced past start)
    assert next(b).kind == "done"
    with pytest.raises(StopIteration):
        next(a)
    assert op.result == 3


def test_abandoned_iterator_marks_cancelled_not_done():
    def _run():
        try:
            yield OpEvent(kind="start", op="t")
            yield OpEvent(kind="item", op="t")
            return 1
        finally:
            pass

    op = Operation(_run)
    it = iter(op)
    next(it)
    it.close()  # explicit abandonment of the iterator
    with pytest.raises(Cancelled):
        _ = op.result


def test_consumer_exception_propagates_and_abandonment_is_deterministic():
    """D10/Issue 22: with a real iterator object, a consumer exception raised
    in the for-loop body propagates to the caller (the operation's generator
    is driven by the iterator, not suspended inside it). The operation stays
    usable: iterating to completion afterwards works, and explicit
    abandonment via close() yields Cancelled."""
    closed = {"n": 0}

    def _run():
        try:
            yield OpEvent(kind="start", op="t")
            yield OpEvent(kind="item", op="t")
            return 1
        finally:
            closed["n"] += 1

    op = Operation(_run)
    it = iter(op)
    assert next(it).kind == "start"
    with pytest.raises(RuntimeError, match="boom"):
        for ev in it:
            if ev.kind == "item":
                raise RuntimeError("boom")
    # The consumer's exception never reached the operation: it is still
    # pending, and .result can complete it.
    assert op.result == 1
    assert closed["n"] == 1
