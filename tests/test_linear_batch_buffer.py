"""Integration tests for LinearBatchBuffer — requires the compiled Rust extension."""

from types import SimpleNamespace

import pytest

from agora_rs import LinearBatchBuffer


def make_metrics(**kwargs):
    defaults = {"records_consumed": 0, "records_written": 0, "by_source": {}}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_starts_empty():
    buf = LinearBatchBuffer(4, 10)
    assert buf.len() == 0


def test_push_returns_false_before_batch_full():
    buf = LinearBatchBuffer(4, 10)
    assert buf.push("p", "r", "c", None) is False
    assert buf.push("p", "r", "c", None) is False
    assert buf.push("p", "r", "c", None) is False


def test_push_returns_true_at_batch_size():
    buf = LinearBatchBuffer(3, 10)
    buf.push("p", "r", "c", None)
    buf.push("p", "r", "c", None)
    assert buf.push("p", "r", "c", None) is True


def test_take_batch_returns_all_records():
    buf = LinearBatchBuffer(4, 10)
    buf.push("p1", "r1", "c1", None)
    buf.push("p2", "r2", "c2", "cb")
    batch = buf.take_batch()
    assert len(batch) == 2
    assert batch[0] == ("p1", "r1", "c1", None)
    assert batch[1] == ("p2", "r2", "c2", "cb")
    assert buf.len() == 0


def test_take_batch_clears_buffer():
    buf = LinearBatchBuffer(4, 10)
    buf.push("p", "r", "c", None)
    buf.take_batch()
    assert buf.len() == 0


def test_take_flush_batch_returns_three_lists():
    buf = LinearBatchBuffer(4, 10)
    buf.push("p1", "r1", "c1", None)
    buf.push("p2", "r2", "c2", None)
    processed, raw, checkpoints = buf.take_flush_batch()
    assert list(processed) == ["p1", "p2"]
    assert list(raw) == ["r1", "r2"]
    assert list(checkpoints) == ["c1", "c2"]
    assert buf.len() == 0


def test_take_flush_batch_raises_on_non_none_on_success():
    buf = LinearBatchBuffer(4, 10)
    buf.push("p", "r", "c", None)
    buf.push("p", "r", "c", "callback")
    with pytest.raises(RuntimeError, match="on_success callback"):
        buf.take_flush_batch()
    # buffer must not be corrupted after the error
    assert buf.len() == 2


def test_take_flush_batch_empty():
    buf = LinearBatchBuffer(4, 10)
    processed, raw, checkpoints = buf.take_flush_batch()
    assert list(processed) == []
    assert list(raw) == []
    assert list(checkpoints) == []


def test_batch_size_min_one():
    buf = LinearBatchBuffer(0, 10)
    # batch_size clamped to 1 — first push should return True
    assert buf.push("p", "r", "c", None) is True


def test_inc_consumed_delegates_to_accumulator():
    buf = LinearBatchBuffer(4, 2)
    assert buf.inc_consumed("src") is False
    assert buf.inc_consumed("src") is True  # flush_interval=2


def test_flush_metrics_updates_python_object():
    buf = LinearBatchBuffer(4, 1)
    buf.inc_consumed("src")  # triggers flush signal
    m = make_metrics()
    buf.flush_metrics(m)
    assert m.records_consumed == 1
    assert m.by_source == {"src": 1}


def test_flush_metrics_final_flushes_all():
    buf = LinearBatchBuffer(4, 100)
    buf.inc_consumed("src")
    m = make_metrics()
    buf.flush_metrics_final(m)
    assert m.records_consumed == 1


def test_snapshot_reflects_state():
    buf = LinearBatchBuffer(8, 10)
    pending, batch_size, consumed, written = buf.snapshot()
    assert pending == 0
    assert batch_size == 8
    assert consumed == 0
    assert written == 0

    buf.push("p", "r", "c", None)
    buf.inc_consumed("src")
    pending, _, consumed, _ = buf.snapshot()
    assert pending == 1
    assert consumed == 1


def test_full_cycle():
    """Push batch_size records, take_flush_batch, verify metrics."""
    buf = LinearBatchBuffer(3, 10)
    for i in range(3):
        buf.inc_consumed("src")
        buf.push(f"p{i}", f"r{i}", f"c{i}", None)

    processed, raw, checkpoints = buf.take_flush_batch()
    assert list(processed) == ["p0", "p1", "p2"]
    assert list(raw) == ["r0", "r1", "r2"]
    assert list(checkpoints) == ["c0", "c1", "c2"]

    m = make_metrics()
    buf.flush_metrics_final(m)
    assert m.records_consumed == 3
    assert m.by_source == {"src": 3}
