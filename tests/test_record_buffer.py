"""Integration tests for RecordBuffer — requires the compiled Rust extension."""

import asyncio
import threading
import time

import pytest

from agora_rs import RecordBuffer


def test_push_and_try_pop():
    buf = RecordBuffer(4)
    buf.push("a")
    buf.push("b")
    assert buf.try_pop() == "a"
    assert buf.try_pop() == "b"
    assert buf.try_pop() is None


def test_size_and_len():
    buf = RecordBuffer(4)
    assert buf.size() == 0
    assert len(buf) == 0
    buf.push("x")
    assert buf.size() == 1
    assert len(buf) == 1


def test_close_marks_done_when_empty():
    buf = RecordBuffer(4)
    assert not buf.is_done()
    buf.close()
    assert buf.is_done()


def test_close_not_done_while_items_remain():
    buf = RecordBuffer(4)
    buf.push("item")
    buf.close()
    assert not buf.is_done()
    buf.try_pop()
    assert buf.is_done()


def test_push_returns_false_after_close():
    buf = RecordBuffer(4)
    buf.close()
    result = buf.push("item")
    assert result is False


def test_cancel_raises_on_push():
    buf = RecordBuffer(4)
    buf.cancel()
    with pytest.raises(RuntimeError, match="cancelled"):
        buf.push("item")


def test_cancel_is_idempotent():
    buf = RecordBuffer(4)
    buf.cancel()
    buf.cancel()
    size, _, _, cancelled = buf.snapshot()
    assert cancelled
    assert size == 0


def test_snapshot_reflects_state():
    buf = RecordBuffer(8)
    size, cap, closed, cancelled = buf.snapshot()
    assert size == 0
    assert cap == 8
    assert not closed
    assert not cancelled

    buf.push("x")
    size, cap, closed, cancelled = buf.snapshot()
    assert size == 1

    buf.close()
    _, _, closed, _ = buf.snapshot()
    assert closed


def test_push_batch():
    buf = RecordBuffer(10)
    n = buf.push_batch(["a", "b", "c"])
    assert n == 3
    assert buf.size() == 3
    assert buf.try_pop() == "a"
    assert buf.try_pop() == "b"
    assert buf.try_pop() == "c"


def test_push_batch_stops_on_close():
    buf = RecordBuffer(2)
    buf.push("x")
    buf.push("y")
    # Buffer is full; close it so push_batch returns early instead of blocking.
    buf.close()
    n = buf.push_batch(["a", "b"])
    assert n == 0  # closed before any item could be pushed


def test_pop_batch():
    buf = RecordBuffer(10)
    buf.push_batch(["a", "b", "c", "d"])
    items = buf.pop_batch(3)
    assert items == ["a", "b", "c"]
    assert buf.size() == 1


def test_pop_batch_empty():
    buf = RecordBuffer(4)
    assert buf.pop_batch(10) == []


def test_wait_for_item_returns_true_when_item_available():
    buf = RecordBuffer(4)
    buf.push("ready")
    assert buf.wait_for_item(100) is True


def test_wait_for_item_times_out_when_empty():
    buf = RecordBuffer(4)
    start = time.monotonic()
    result = buf.wait_for_item(100)
    elapsed_ms = (time.monotonic() - start) * 1000
    assert result is False
    assert elapsed_ms >= 90, f"timed out too fast: {elapsed_ms:.1f}ms"


def test_wait_for_item_unblocked_by_producer_thread():
    buf = RecordBuffer(4)

    def producer():
        time.sleep(0.05)
        buf.push("from_thread")

    t = threading.Thread(target=producer)
    t.start()
    result = buf.wait_for_item(500)
    t.join()
    assert result is True
    assert buf.try_pop() == "from_thread"


def test_wait_for_item_raises_on_cancel():
    buf = RecordBuffer(4)

    def canceller():
        time.sleep(0.05)
        buf.cancel()

    t = threading.Thread(target=canceller)
    t.start()
    with pytest.raises(RuntimeError, match="cancelled"):
        buf.wait_for_item(500)
    t.join()


def test_producer_consumer_threaded():
    """Producer thread pushes N items; main thread pops them all."""
    N = 200
    buf = RecordBuffer(16)
    results = []

    def producer():
        for i in range(N):
            buf.push(i)
        buf.close()

    t = threading.Thread(target=producer)
    t.start()

    while not buf.is_done():
        item = buf.try_pop()
        if item is not None:
            results.append(item)

    t.join()
    assert results == list(range(N))


def test_backpressure_blocks_producer():
    """Producer blocks when buffer is full; consumer unblocks it."""
    buf = RecordBuffer(2)
    pushed_at = {}

    def producer():
        buf.push("a")
        buf.push("b")
        pushed_at["start"] = time.monotonic()
        buf.push("c")  # blocks until consumer pops
        pushed_at["end"] = time.monotonic()

    t = threading.Thread(target=producer)
    t.start()
    time.sleep(0.05)  # let producer fill and block
    buf.try_pop()     # unblock producer
    t.join(timeout=2)
    assert not t.is_alive(), "producer thread should have unblocked"
    assert "end" in pushed_at


@pytest.mark.asyncio
async def test_async_producer_consumer():
    """asyncio.to_thread producer + event loop consumer via wait_for_item."""
    N = 50
    buf = RecordBuffer(8)
    results = []

    async def producer():
        for i in range(N):
            await asyncio.to_thread(buf.push, i)
        buf.close()

    asyncio.create_task(producer())

    while not buf.is_done() or buf.size() > 0:
        ready = await asyncio.to_thread(buf.wait_for_item, 50)
        if ready:
            item = buf.try_pop()
            if item is not None:
                results.append(item)
        await asyncio.sleep(0)

    assert sorted(results) == list(range(N))
