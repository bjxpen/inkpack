"""Operation / OpEvent contract tests (spec 4.1-4.2, 5)."""

from __future__ import annotations

import pytest

from inkpack import (
    Cancelled,
    CompactResult,
    GcResult,
    Operation,
    OpEvent,
    PutResult,
    ReencodeResult,
    TrainDictResult,
    VerifyResult,
)

VALID_KINDS = {"start", "phase", "progress", "item", "log", "error", "done"}


def test_operation_iterable_yields_opevents_and_put_result(repo):
    op = repo.store.put_bytes(b"abc\x00\xff", profile="raw")
    assert isinstance(op, Operation)
    events = list(op)
    assert events, "operation must yield events"
    assert all(isinstance(e, OpEvent) for e in events)
    assert events[0].kind == "start"
    assert events[-1].kind == "done"
    assert all(e.op == "put" for e in events)
    assert all(e.kind in VALID_KINDS for e in events)
    assert isinstance(op.result, PutResult)
    assert op.result.ref.blob_key.startswith("ikb1:")


def test_long_ops_return_operations_with_results(repo):
    put = repo.store.put_bytes(b"data-for-long-ops", profile="raw").result
    cases = [
        (repo.store.verify(), VerifyResult),
        (repo.store.gc(live=[put.ref]), GcResult),
        (repo.store.compact(), CompactResult),
        (repo.store.reencode([put.ref]), ReencodeResult),
        (repo.store.train_dict([b"sample text " * 20] * 5), TrainDictResult),
    ]
    for op, result_type in cases:
        events = list(op)
        assert events and events[0].kind == "start" and events[-1].kind == "done"
        assert isinstance(op.result, result_type)


def test_result_after_partial_iteration(repo):
    op = repo.store.put_bytes(b"partial", profile="raw")
    iterator = iter(op)
    first = next(iterator)
    assert isinstance(first, OpEvent)
    assert isinstance(op.result, PutResult)
    assert op.result.raw_len == 7


def test_result_raises_operation_error(repo):
    op = repo.store.train_dict([])  # empty samples -> ValueError
    with pytest.raises(ValueError):
        op.result
    # The same error surfaces when iterating.
    with pytest.raises(ValueError):
        list(repo.store.train_dict([]))


def test_result_raises_cancelled(repo):
    def always_cancel() -> bool:
        return True

    op = repo.store.put_bytes(b"never", profile="raw", cancel=always_cancel)
    with pytest.raises(Cancelled):
        op.result


def test_iterating_twice_yields_events_once(repo):
    op = repo.store.put_bytes(b"once", profile="raw")
    first = list(op)
    second = list(op)
    assert len(first) > 0
    assert second == []
    assert isinstance(op.result, PutResult)


def test_done_event_carries_metrics(repo):
    op = repo.store.put_bytes(b"metrics", profile="raw")
    done = [e for e in op if e.kind == "done"]
    assert done
    metrics = done[-1].metrics or {}
    assert metrics["bytes_in"] == 7
    assert metrics["bytes_out"] >= 0


def test_events_are_opevent_instances_for_stream_put(repo):
    import io

    events = list(repo.store.put_stream(io.BytesIO(b"stream-events"), profile="raw"))
    assert all(isinstance(e, OpEvent) for e in events)
    assert events[0].kind == "start"
    assert events[-1].kind == "done"


def test_verify_item_events_reported(repo):
    for i in range(5):
        repo.store.put_bytes(f"item-{i}".encode(), profile="raw").result
    items = [e for e in repo.store.verify() if e.kind == "item"]
    # Item events are throttled (M22): 5 rows -> the final exact-count item.
    assert len(items) == 1
    assert items[-1].metrics == {"checked": 5, "ok": 5, "missing": 0, "corrupt": 0}


def test_operation_result_type_is_stable(repo):
    """A completed operation keeps returning the same result object."""
    op = repo.store.put_bytes(b"stable-result", profile="raw")
    _ = list(op)
    assert op.result is op.result


def test_generator_return_value_is_captured(repo):
    """Operations whose generator does not return a value yield None result."""
    op = repo.store.put_bytes(b"x", profile="raw")
    events = list(op)
    assert events[-1].kind == "done"
    assert op.result is not None


def test_put_unknown_profile_raises_keyerror(repo):
    with pytest.raises(KeyError):
        repo.store.put_bytes(b"x", profile="no-such-profile").result


def test_reencode_accepts_empty_targets(repo):
    result = repo.store.reencode(iter(())).result
    assert result.targets == 0 and result.reencoded == 0 and result.skipped == 0
