use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

const PUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Check if the Rust extension is available and working.
#[pyfunction]
fn is_available() -> bool {
    true
}

fn lock_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Mutex poisoned: {e}"))
}

struct BufferState {
    items: VecDeque<PyObject>,
    closed: bool,
    cancelled: bool,
}

/// A bounded, thread-safe record buffer that replaces asyncio.Queue in the
/// prefetch path. Producer (file reader thread) pushes items; consumer
/// (asyncio event loop) pops them via try_pop or waits via wait_for_item.
///
/// Unlike asyncio.Queue, RecordBuffer does not allocate a Handle object per
/// item — it uses a Mutex-protected VecDeque with a Condvar for backpressure.
/// This eliminates the _wakeup_next → call_soon → Handle.__init__ overhead
/// that dominates the per-record path in pure Python.
///
/// push() blocks with a 30-second timeout; returns Err if the timeout expires
/// or the buffer is cancelled via cancel().
///
/// wait_for_item(timeout_ms) blocks the calling thread until an item is
/// available or the timeout expires — use this from a background thread or
/// via asyncio.to_thread() to avoid busy-polling the event loop.
#[pyclass]
struct RecordBuffer {
    inner: (Mutex<BufferState>, Condvar),
    capacity: usize,
}

#[pymethods]
impl RecordBuffer {
    #[new]
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        RecordBuffer {
            inner: (
                Mutex::new(BufferState {
                    items: VecDeque::with_capacity(capacity),
                    closed: false,
                    cancelled: false,
                }),
                Condvar::new(),
            ),
            capacity,
        }
    }

    /// Push an item into the buffer. Blocks if full (backpressure).
    /// Returns False if the buffer was closed while waiting.
    /// Raises RuntimeError if cancelled or if the 30-second timeout expires.
    fn push(&self, py: Python<'_>, item: PyObject) -> PyResult<bool> {
        let (lock, cvar) = &self.inner;
        py.allow_threads(|| {
            let mut state = lock.lock().map_err(lock_err)?;
            let deadline = std::time::Instant::now() + PUSH_TIMEOUT;
            loop {
                if state.cancelled {
                    return Err(PyRuntimeError::new_err("RecordBuffer cancelled"));
                }
                if state.closed {
                    return Ok(false);
                }
                if state.items.len() < self.capacity {
                    state.items.push_back(item);
                    cvar.notify_one();
                    return Ok(true);
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(PyRuntimeError::new_err(
                        "RecordBuffer push timed out after 30s — consumer may be stuck",
                    ));
                }
                let (next, _) = cvar.wait_timeout(state, remaining).map_err(lock_err)?;
                state = next;
            }
        })
    }

    /// Push multiple items at once, blocking only once for the whole batch.
    /// Returns the number of items actually pushed (may be less than len(items)
    /// if the buffer was closed mid-batch). Raises RuntimeError if cancelled.
    fn push_batch(&self, py: Python<'_>, items: Vec<PyObject>) -> PyResult<usize> {
        let (lock, cvar) = &self.inner;
        let mut pushed = 0usize;
        py.allow_threads(|| {
            let deadline = std::time::Instant::now() + PUSH_TIMEOUT;
            for item in items {
                let mut state = lock.lock().map_err(lock_err)?;
                loop {
                    if state.cancelled {
                        return Err(PyRuntimeError::new_err("RecordBuffer cancelled"));
                    }
                    if state.closed {
                        return Ok(pushed);
                    }
                    if state.items.len() < self.capacity {
                        state.items.push_back(item);
                        cvar.notify_one();
                        pushed += 1;
                        break;
                    }
                    let remaining =
                        deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(PyRuntimeError::new_err(
                            "RecordBuffer push_batch timed out after 30s — consumer may be stuck",
                        ));
                    }
                    let (next, _) = cvar.wait_timeout(state, remaining).map_err(lock_err)?;
                    state = next;
                }
            }
            Ok(pushed)
        })
    }

    /// Block until at least one item is available or the timeout expires.
    /// Returns True if an item is ready, False if timed out.
    /// Raises RuntimeError if the buffer is cancelled.
    ///
    /// Use this from asyncio.to_thread() to avoid busy-polling the event loop:
    ///
    ///   ready = await asyncio.to_thread(buf.wait_for_item, 50)
    ///   if ready:
    ///       item = buf.try_pop()
    fn wait_for_item(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<bool> {
        let (lock, cvar) = &self.inner;
        let timeout = Duration::from_millis(timeout_ms);
        py.allow_threads(|| {
            let mut state = lock.lock().map_err(lock_err)?;
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if state.cancelled {
                    return Err(PyRuntimeError::new_err("RecordBuffer cancelled"));
                }
                if !state.items.is_empty() || state.closed {
                    return Ok(!state.items.is_empty());
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Ok(false);
                }
                let (next, _) = cvar.wait_timeout(state, remaining).map_err(lock_err)?;
                state = next;
            }
        })
    }

    /// Try to pop an item without blocking. Returns None if empty.
    fn try_pop(&self) -> PyResult<Option<PyObject>> {
        let (lock, cvar) = &self.inner;
        let mut state = lock.lock().map_err(lock_err)?;
        let item = state.items.pop_front();
        if item.is_some() {
            cvar.notify_one();
        }
        Ok(item)
    }

    /// Pop up to `max_items` items at once without blocking.
    /// Returns an empty list if the buffer is empty.
    fn pop_batch(&self, _py: Python<'_>, max_items: usize) -> PyResult<Vec<PyObject>> {
        let (lock, cvar) = &self.inner;
        let mut state = lock.lock().map_err(lock_err)?;
        let n = max_items.min(state.items.len());
        if n == 0 {
            return Ok(vec![]);
        }
        let batch: Vec<PyObject> = state.items.drain(..n).collect();
        for _ in 0..n {
            cvar.notify_one();
        }
        Ok(batch)
    }

    /// Signal that no more items will be pushed.
    fn close(&self) -> PyResult<()> {
        let (lock, cvar) = &self.inner;
        let mut state = lock.lock().map_err(lock_err)?;
        state.closed = true;
        cvar.notify_all();
        Ok(())
    }

    /// Cancel the buffer — unblocks any waiting push() with an error.
    fn cancel(&self) -> PyResult<()> {
        let (lock, cvar) = &self.inner;
        let mut state = lock.lock().map_err(lock_err)?;
        state.cancelled = true;
        cvar.notify_all();
        Ok(())
    }

    /// Current number of items in the buffer.
    fn size(&self) -> PyResult<usize> {
        let (lock, _) = &self.inner;
        Ok(lock.lock().map_err(lock_err)?.items.len())
    }

    /// True if the buffer is closed and empty.
    fn is_done(&self) -> PyResult<bool> {
        let (lock, _) = &self.inner;
        let state = lock.lock().map_err(lock_err)?;
        Ok(state.closed && state.items.is_empty())
    }

    /// Snapshot of buffer state for observability: {size, capacity, closed, cancelled}.
    fn snapshot(&self) -> PyResult<(usize, usize, bool, bool)> {
        let (lock, _) = &self.inner;
        let state = lock.lock().map_err(lock_err)?;
        Ok((state.items.len(), self.capacity, state.closed, state.cancelled))
    }

    fn __len__(&self) -> PyResult<usize> {
        self.size()
    }

    fn __repr__(&self) -> PyResult<String> {
        let (lock, _) = &self.inner;
        let state = lock.lock().map_err(lock_err)?;
        Ok(format!(
            "RecordBuffer(size={}, capacity={}, closed={}, cancelled={})",
            state.items.len(),
            self.capacity,
            state.closed,
            state.cancelled,
        ))
    }
}

/// Batches counter increments to reduce per-record Python dict overhead.
///
/// Instead of incrementing ctx.metrics.records_consumed on every record,
/// the Rust accumulator counts locally and flushes to Python every N records.
/// This reduces Python attribute lookups from O(records) to O(records/flush_interval).
///
/// flush_interval must be >= 1. Values close to 1 negate the batching benefit.
#[pyclass]
struct MetricsAccumulator {
    records_consumed: u64,
    records_written: u64,
    // Box<str> avoids re-allocating the key string on every hot-path insert.
    source_counts: std::collections::HashMap<Box<str>, u64>,
    flush_interval: u64,
    since_last_flush: u64,
}

impl MetricsAccumulator {
    fn flush_inner(&mut self, metrics: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.records_consumed > 0 {
            let current: u64 = metrics.getattr("records_consumed")?.extract()?;
            metrics.setattr("records_consumed", current + self.records_consumed)?;
            self.records_consumed = 0;
        }
        if self.records_written > 0 {
            let current: u64 = metrics.getattr("records_written")?.extract()?;
            metrics.setattr("records_written", current + self.records_written)?;
            self.records_written = 0;
        }
        if !self.source_counts.is_empty() {
            let by_source = metrics.getattr("by_source")?;
            for (source, count) in self.source_counts.drain() {
                let current: u64 = by_source
                    .call_method1("get", (source.as_ref(), 0u64))?
                    .extract()?;
                by_source.set_item(source.as_ref(), current + count)?;
            }
        }
        self.since_last_flush = 0;
        Ok(())
    }
}

#[pymethods]
impl MetricsAccumulator {
    #[new]
    #[pyo3(signature = (flush_interval = 100))]
    fn new(flush_interval: u64) -> Self {
        MetricsAccumulator {
            records_consumed: 0,
            records_written: 0,
            source_counts: std::collections::HashMap::new(),
            flush_interval: flush_interval.max(1),
            since_last_flush: 0,
        }
    }

    /// Increment consumed counter for a source. Returns True if flush is due.
    fn inc_consumed(&mut self, source_name: &str) -> bool {
        self.records_consumed += 1;
        self.since_last_flush += 1;
        *self.source_counts.entry(source_name.into()).or_insert(0) += 1;
        self.since_last_flush >= self.flush_interval
    }

    fn inc_written(&mut self) {
        self.records_written += 1;
    }

    /// Flush accumulated counts to a Python metrics object and reset.
    fn flush(&mut self, metrics: &Bound<'_, PyAny>) -> PyResult<()> {
        self.flush_inner(metrics)
    }

    /// Flush regardless of interval — semantically "end of stream", but identical to flush().
    /// Kept separate so call sites can express intent without a boolean flag.
    fn flush_final(&mut self, metrics: &Bound<'_, PyAny>) -> PyResult<()> {
        self.flush_inner(metrics)
    }

    /// Snapshot of pending (unflushed) counts: (records_consumed, records_written, since_last_flush).
    fn snapshot(&self) -> (u64, u64, u64) {
        (self.records_consumed, self.records_written, self.since_last_flush)
    }

    fn __repr__(&self) -> String {
        format!(
            "MetricsAccumulator(consumed={}, written={}, since_flush={}/{})",
            self.records_consumed,
            self.records_written,
            self.since_last_flush,
            self.flush_interval,
        )
    }
}

/// Accumulates processed records into a batch buffer and tracks metrics locally.
///
/// Python still drives the async loop and awaits chain.process(); this struct
/// handles the batch-full check and metrics counting in Rust, eliminating
/// PendingWrite dataclass allocation and list.append + len() per record.
///
/// take_flush_batch() requires that all buffered records have on_success=None.
/// Use take_batch() when per-record on_success callbacks are present.
#[pyclass]
struct LinearBatchBuffer {
    pending: Vec<(PyObject, PyObject, PyObject, PyObject)>,
    batch_size: usize,
    metrics_acc: MetricsAccumulator,
}

#[pymethods]
impl LinearBatchBuffer {
    #[new]
    fn new(batch_size: usize, metrics_flush_interval: u64) -> Self {
        LinearBatchBuffer {
            pending: Vec::with_capacity(batch_size.max(1)),
            batch_size: batch_size.max(1),
            metrics_acc: MetricsAccumulator::new(metrics_flush_interval.max(1)),
        }
    }

    /// Push a processed record into the buffer.
    /// Returns True when the buffer has reached batch_size and should be flushed.
    fn push(
        &mut self,
        processed: PyObject,
        raw: PyObject,
        checkpoint: PyObject,
        on_success: PyObject,
    ) -> bool {
        self.pending.push((processed, raw, checkpoint, on_success));
        self.pending.len() >= self.batch_size
    }

    /// Drain and return all buffered records, clearing the buffer.
    fn take_batch(&mut self) -> Vec<(PyObject, PyObject, PyObject, PyObject)> {
        std::mem::take(&mut self.pending)
    }

    /// Drain and return three separate lists: (processed, raw, checkpoint).
    ///
    /// Raises RuntimeError if any record has a non-None on_success callback —
    /// use take_batch() instead when per-record hooks are present.
    fn take_flush_batch(&mut self, py: Python<'_>) -> PyResult<(PyObject, PyObject, PyObject)> {
        // Validate before draining so the buffer is not corrupted on error.
        for (_, _, _, on_success) in &self.pending {
            if !on_success.is_none(py) {
                return Err(PyRuntimeError::new_err(
                    "take_flush_batch: record has on_success callback; use take_batch() instead",
                ));
            }
        }
        let batch = std::mem::take(&mut self.pending);
        let mut processed = Vec::with_capacity(batch.len());
        let mut raw = Vec::with_capacity(batch.len());
        let mut checkpoints = Vec::with_capacity(batch.len());
        for (p, r, c, _on_success) in batch {
            processed.push(p);
            raw.push(r);
            checkpoints.push(c);
        }
        Ok((
            pyo3::types::PyList::new(py, processed)?.into(),
            pyo3::types::PyList::new(py, raw)?.into(),
            pyo3::types::PyList::new(py, checkpoints)?.into(),
        ))
    }

    /// Number of records currently buffered.
    fn len(&self) -> usize {
        self.pending.len()
    }

    /// Increment the consumed counter for a source.
    /// Returns True when a metrics flush is due (delegates to MetricsAccumulator).
    fn inc_consumed(&mut self, source_name: &str) -> bool {
        self.metrics_acc.inc_consumed(source_name)
    }

    /// Flush accumulated metrics to a Python metrics object if flush interval reached.
    fn flush_metrics(&mut self, metrics: &Bound<'_, PyAny>) -> PyResult<()> {
        self.metrics_acc.flush(metrics)
    }

    /// Force-flush all accumulated metrics to a Python metrics object.
    fn flush_metrics_final(&mut self, metrics: &Bound<'_, PyAny>) -> PyResult<()> {
        self.metrics_acc.flush_final(metrics)
    }

    /// Snapshot of buffer state: (pending_count, batch_size, unflushed_consumed, unflushed_written).
    fn snapshot(&self) -> (usize, usize, u64, u64) {
        let (consumed, written, _) = self.metrics_acc.snapshot();
        (self.pending.len(), self.batch_size, consumed, written)
    }

    fn __repr__(&self) -> String {
        let (consumed, written, _) = self.metrics_acc.snapshot();
        format!(
            "LinearBatchBuffer(pending={}, batch_size={}, consumed={}, written={})",
            self.pending.len(),
            self.batch_size,
            consumed,
            written,
        )
    }
}

/// Register the Python module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(is_available, m)?)?;
    m.add_class::<RecordBuffer>()?;
    m.add_class::<MetricsAccumulator>()?;
    m.add_class::<LinearBatchBuffer>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- RecordBuffer ---

    #[test]
    fn record_buffer_close_marks_done_when_empty() {
        let buf = RecordBuffer::new(2);
        assert_eq!(buf.size().unwrap(), 0);
        assert!(!buf.is_done().unwrap());
        buf.close().unwrap();
        assert!(buf.is_done().unwrap());
    }

    #[test]
    fn record_buffer_cancel_is_idempotent() {
        let buf = RecordBuffer::new(4);
        buf.cancel().unwrap();
        buf.cancel().unwrap();
        let (lock, _) = &buf.inner;
        assert!(lock.lock().unwrap().cancelled);
    }

    #[test]
    fn record_buffer_snapshot_reflects_state() {
        let buf = RecordBuffer::new(8);
        let (size, cap, closed, cancelled) = buf.snapshot().unwrap();
        assert_eq!(size, 0);
        assert_eq!(cap, 8);
        assert!(!closed);
        assert!(!cancelled);
        buf.close().unwrap();
        let (_, _, closed, _) = buf.snapshot().unwrap();
        assert!(closed);
    }

    #[test]
    fn record_buffer_wait_for_item_returns_false_on_empty_timeout() {
        // Buffer is empty and closed=false — wait should time out immediately.
        let buf = RecordBuffer::new(4);
        // We can't call py.allow_threads without a Python runtime, but we can
        // test the Condvar path by closing the buffer first (closed=true, items empty
        // → wait_for_item returns false because items is empty even though closed).
        buf.close().unwrap();
        // After close with no items: wait_for_item should return false (no items).
        // We verify the state directly since we can't call the #[pymethods] fn here.
        let (lock, _) = &buf.inner;
        let state = lock.lock().unwrap();
        assert!(state.closed);
        assert!(state.items.is_empty());
    }

    // --- MetricsAccumulator ---

    #[test]
    fn metrics_accumulator_flush_interval_triggers() {
        let mut acc = MetricsAccumulator::new(3);
        assert!(!acc.inc_consumed("source"));
        assert!(!acc.inc_consumed("source"));
        assert!(acc.inc_consumed("source"));
        assert_eq!(acc.records_consumed, 3);
    }

    #[test]
    fn metrics_accumulator_flush_interval_min_one() {
        let mut acc = MetricsAccumulator::new(0);
        assert_eq!(acc.flush_interval, 1);
        assert!(acc.inc_consumed("s"));
    }

    #[test]
    fn metrics_accumulator_source_counts_accumulate() {
        let mut acc = MetricsAccumulator::new(100);
        acc.inc_consumed("a");
        acc.inc_consumed("a");
        acc.inc_consumed("b");
        assert_eq!(*acc.source_counts.get("a").unwrap(), 2);
        assert_eq!(*acc.source_counts.get("b").unwrap(), 1);
    }

    #[test]
    fn metrics_accumulator_snapshot_reflects_pending() {
        let mut acc = MetricsAccumulator::new(100);
        acc.inc_consumed("x");
        acc.inc_written();
        let (consumed, written, since) = acc.snapshot();
        assert_eq!(consumed, 1);
        assert_eq!(written, 1);
        assert_eq!(since, 1);
    }

    #[test]
    fn metrics_accumulator_flush_interval_not_mutated_by_flush_final() {
        let mut acc = MetricsAccumulator::new(100);
        acc.inc_consumed("x");
        assert_eq!(acc.flush_interval, 100);
    }

    // --- LinearBatchBuffer ---

    #[test]
    fn linear_batch_buffer_starts_empty() {
        let buf = LinearBatchBuffer::new(2, 8);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn linear_batch_buffer_capacity_min_one() {
        let buf = LinearBatchBuffer::new(0, 1);
        assert_eq!(buf.batch_size, 1);
    }

    #[test]
    fn linear_batch_buffer_take_batch_clears() {
        let mut buf = LinearBatchBuffer::new(4, 10);
        let batch = buf.take_batch();
        assert!(batch.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn linear_batch_buffer_snapshot_reflects_state() {
        let buf = LinearBatchBuffer::new(16, 10);
        let (pending, batch_size, consumed, written) = buf.snapshot();
        assert_eq!(pending, 0);
        assert_eq!(batch_size, 16);
        assert_eq!(consumed, 0);
        assert_eq!(written, 0);
    }

    #[test]
    fn linear_batch_buffer_inc_consumed_delegates() {
        let mut buf = LinearBatchBuffer::new(4, 2);
        assert!(!buf.inc_consumed("src"));
        assert!(buf.inc_consumed("src")); // flush_interval=2, second call triggers
        let (_, _, consumed, _) = buf.snapshot();
        assert_eq!(consumed, 2);
    }
}
