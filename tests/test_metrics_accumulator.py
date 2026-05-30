"""Integration tests for MetricsAccumulator — requires the compiled Rust extension."""

from types import SimpleNamespace

import pytest

from agora_rs import MetricsAccumulator


def make_metrics(**kwargs):
    defaults = {"records_consumed": 0, "records_written": 0, "by_source": {}}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_inc_consumed_returns_false_before_interval():
    acc = MetricsAccumulator(flush_interval=3)
    assert acc.inc_consumed("src") is False
    assert acc.inc_consumed("src") is False


def test_inc_consumed_returns_true_at_interval():
    acc = MetricsAccumulator(flush_interval=3)
    acc.inc_consumed("src")
    acc.inc_consumed("src")
    assert acc.inc_consumed("src") is True


def test_flush_updates_python_metrics():
    acc = MetricsAccumulator(flush_interval=2)
    acc.inc_consumed("a")
    acc.inc_consumed("a")  # triggers flush signal
    m = make_metrics()
    acc.flush(m)
    assert m.records_consumed == 2
    assert m.by_source == {"a": 2}


def test_flush_accumulates_on_existing_values():
    acc = MetricsAccumulator(flush_interval=1)
    acc.inc_consumed("src")
    m = make_metrics(records_consumed=10, by_source={"src": 5})
    acc.flush(m)
    assert m.records_consumed == 11
    assert m.by_source == {"src": 6}


def test_flush_resets_internal_counters():
    acc = MetricsAccumulator(flush_interval=2)
    acc.inc_consumed("src")
    acc.inc_consumed("src")
    m = make_metrics()
    acc.flush(m)
    consumed, written, since = acc.snapshot()
    assert consumed == 0
    assert since == 0


def test_flush_final_flushes_regardless_of_interval():
    acc = MetricsAccumulator(flush_interval=100)
    acc.inc_consumed("src")  # only 1, interval is 100
    m = make_metrics()
    acc.flush_final(m)
    assert m.records_consumed == 1
    assert m.by_source == {"src": 1}


def test_flush_final_does_not_mutate_flush_interval():
    acc = MetricsAccumulator(flush_interval=100)
    acc.inc_consumed("src")
    m = make_metrics()
    acc.flush_final(m)
    consumed, _, _ = acc.snapshot()
    assert consumed == 0  # flushed
    # flush_interval must not be changed — verify by checking another flush cycle
    for _ in range(99):
        acc.inc_consumed("src")
    assert acc.inc_consumed("src") is True  # 100th call triggers


def test_inc_written_tracked_separately():
    acc = MetricsAccumulator(flush_interval=1)
    acc.inc_written()
    acc.inc_consumed("src")
    m = make_metrics()
    acc.flush(m)
    assert m.records_written == 1
    assert m.records_consumed == 1


def test_multiple_sources():
    acc = MetricsAccumulator(flush_interval=10)
    for _ in range(3):
        acc.inc_consumed("a")
    for _ in range(5):
        acc.inc_consumed("b")
    m = make_metrics()
    acc.flush_final(m)
    assert m.by_source == {"a": 3, "b": 5}
    assert m.records_consumed == 8


def test_snapshot_reflects_pending():
    acc = MetricsAccumulator(flush_interval=100)
    acc.inc_consumed("x")
    acc.inc_consumed("x")
    acc.inc_written()
    consumed, written, since = acc.snapshot()
    assert consumed == 2
    assert written == 1
    assert since == 2


def test_flush_interval_min_one():
    acc = MetricsAccumulator(flush_interval=0)
    # flush_interval clamped to 1 — first inc_consumed should trigger
    assert acc.inc_consumed("src") is True


def test_no_flush_when_nothing_accumulated():
    acc = MetricsAccumulator(flush_interval=10)
    m = make_metrics(records_consumed=5)
    acc.flush(m)
    # nothing accumulated — Python object must not be touched
    assert m.records_consumed == 5
