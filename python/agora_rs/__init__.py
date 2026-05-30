"""agora_rs — Rust acceleration layer for agora-etl."""

from __future__ import annotations

try:
    from agora_rs._core import LinearBatchBuffer, MetricsAccumulator, RecordBuffer, is_available

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

    def is_available() -> bool:  # type: ignore[misc]
        return False

    class RecordBuffer:  # type: ignore[no-redef]
        """Pure-Python stub — Rust extension not available."""

        def __init__(self, capacity: int) -> None:
            raise ImportError(
                "agora-etl-rs is not installed. "
                "Install it with: pip install agora-etl-rs"
            )

    class MetricsAccumulator:  # type: ignore[no-redef]
        """Pure-Python stub — Rust extension not available."""

        def __init__(self, flush_interval: int = 100) -> None:
            raise ImportError(
                "agora-etl-rs is not installed. "
                "Install it with: pip install agora-etl-rs"
            )

    class LinearBatchBuffer:  # type: ignore[no-redef]
        """Pure-Python stub — Rust extension not available."""

        def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
            raise ImportError(
                "agora-etl-rs is not installed. "
                "Install it with: pip install agora-etl-rs"
            )


__all__ = ["LinearBatchBuffer", "MetricsAccumulator", "RecordBuffer", "RUST_AVAILABLE", "is_available"]
