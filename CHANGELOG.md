# Changelog

## 0.1.0 (May 30, 2026)

- Initial release — Rust acceleration layer for agora-etl.
- Added `RecordBuffer`: bounded, thread-safe VecDeque with Condvar backpressure, replacing `asyncio.Queue` in the prefetch path. Releases the GIL on push; includes 30-second push timeout and `cancel()` for clean shutdown.
- Added `MetricsAccumulator`: batches per-record counter increments locally and flushes to Python every N records, reducing Python attribute lookups from O(records) to O(records/flush_interval).
- Added `LinearBatchBuffer`: accumulates processed records into a Rust `Vec`, eliminating `PendingWrite` dataclass allocation and `list.append` + `len()` per record on the Python side.
- Graceful fallback stubs in `agora_rs` Python package when the Rust extension is not installed.
