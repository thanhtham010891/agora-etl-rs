"""Integration tests: agora-etl pipeline running with agora-rs acceleration."""

from __future__ import annotations

from typing import Any, AsyncGenerator

import pytest

from agora import IterableSource, Pipeline
from agora.core.sink import BaseSink
from agora_rs import RUST_AVAILABLE, LinearBatchBuffer, MetricsAccumulator, RecordBuffer


class CollectSink(BaseSink[Any]):
    def __init__(self) -> None:
        self.records: list[Any] = []

    async def write(self, record: Any) -> None:
        self.records.append(record)


# ---------------------------------------------------------------------------
# 1. Sanity: extension is loaded
# ---------------------------------------------------------------------------

def test_rust_available():
    assert RUST_AVAILABLE is True
    from agora_rs import is_available
    assert is_available() is True


# ---------------------------------------------------------------------------
# 2. RecordBuffer used as agora-etl uses it: threaded producer + async consumer
# ---------------------------------------------------------------------------

def test_record_buffer_producer_consumer_matches_input():
    import threading

    items = list(range(50))
    buf = RecordBuffer(16)
    received: list[int] = []

    def producer():
        for item in items:
            buf.push(item)
        buf.close()

    t = threading.Thread(target=producer)
    t.start()

    while not buf.is_done():
        item = buf.try_pop()
        if item is not None:
            received.append(item)

    t.join()
    assert received == items


# ---------------------------------------------------------------------------
# 3. MetricsAccumulator flush cycle matches manual counting
# ---------------------------------------------------------------------------

def test_metrics_accumulator_flush_cycle():
    class FakeMetrics:
        records_consumed = 0
        records_written = 0
        by_source: dict[str, int] = {}

        def __init__(self):
            self.by_source = {}

    acc = MetricsAccumulator(flush_interval=10)
    m = FakeMetrics()

    for i in range(25):
        if acc.inc_consumed("src_a"):
            acc.flush(m)

    acc.flush_final(m)

    assert m.records_consumed == 25
    assert m.by_source.get("src_a", 0) == 25


# ---------------------------------------------------------------------------
# 4. LinearBatchBuffer batch cycle
# ---------------------------------------------------------------------------

def test_linear_batch_buffer_full_cycle():
    buf = LinearBatchBuffer(batch_size=5, metrics_flush_interval=100)

    for i in range(5):
        full = buf.push(f"proc_{i}", f"raw_{i}", f"ckpt_{i}", None)

    assert full is True

    class FakeMetrics:
        records_consumed = 0
        records_written = 0
        by_source: dict[str, int] = {}

        def __init__(self):
            self.by_source = {}

    m = FakeMetrics()
    processed, raw, checkpoints = buf.take_flush_batch()
    buf.flush_metrics_final(m)

    assert list(processed) == [f"proc_{i}" for i in range(5)]
    assert list(raw) == [f"raw_{i}" for i in range(5)]
    assert list(checkpoints) == [f"ckpt_{i}" for i in range(5)]
    assert buf.len() == 0


# ---------------------------------------------------------------------------
# 5. End-to-end: agora-etl Pipeline with IterableSource + CollectSink
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_end_to_end_with_rust():
    records = [{"id": i, "value": i * 2} for i in range(20)]
    source = IterableSource(records)
    sink = CollectSink()

    bound = Pipeline(source).build(sink=sink)
    await bound.run()

    assert len(sink.records) == 20
    assert sink.records == records


# ---------------------------------------------------------------------------
# 6. Pipeline with map middleware — rust acceleration still active
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_with_map_middleware():
    from agora import MapMiddleware

    records = list(range(10))
    source = IterableSource(records)
    sink = CollectSink()

    bound = Pipeline(source).pipe(MapMiddleware(lambda x: x * 3)).build(sink=sink)
    await bound.run()

    assert sink.records == [x * 3 for x in range(10)]


# ---------------------------------------------------------------------------
# 7. repr smoke test — all three classes
# ---------------------------------------------------------------------------

def test_repr_smoke():
    buf = RecordBuffer(8)
    buf.push("x")
    assert "RecordBuffer" in repr(buf)
    assert "size=1" in repr(buf)

    acc = MetricsAccumulator(flush_interval=50)
    acc.inc_consumed("s")
    assert "MetricsAccumulator" in repr(acc)
    assert "consumed=1" in repr(acc)

    lbb = LinearBatchBuffer(batch_size=4, metrics_flush_interval=10)
    lbb.push("p", "r", "c", None)
    assert "LinearBatchBuffer" in repr(lbb)
    assert "pending=1" in repr(lbb)
