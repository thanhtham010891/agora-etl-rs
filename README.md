# Agora ETL Rust

[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl-rs)](https://pypi.org/project/agora-etl-rs/)

Rust acceleration layer for [agora-etl](https://pypi.org/project/agora-etl/).

Replaces the three hot inner-loop primitives in the agora-etl runtime with GIL-releasing Rust implementations — zero-copy record buffering, batched metrics accumulation, and allocation-free batch buffering. Install it alongside `agora-etl` and the runtime picks it up automatically. No code changes required.

---

## Installation

```bash
pip install agora-etl agora-etl-rs
```

Or use the `[rs]` extra to install both in one step:

```bash
pip install "agora-etl[rs]"
```

Verify the extension loaded correctly:

```python
from agora_rs import is_available, RUST_AVAILABLE

print(RUST_AVAILABLE)   # True
print(is_available())   # True
```

---

## What it accelerates

| Component | Pure Python baseline | Rust implementation |
|---|---|---|
| `RecordBuffer` | `asyncio.Queue` — one `Handle` alloc per item | `Mutex<VecDeque>` + `Condvar`, GIL released on every push/pop |
| `MetricsAccumulator` | Python attribute lookup on every record | Rust counters, flushed to Python every N records |
| `LinearBatchBuffer` | `list.append` + `PendingWrite` dataclass per record | `Vec` accumulation, no Python allocation per record |

The runtime falls back to the pure-Python path transparently if `agora-etl-rs` is not installed.

---

## Requirements

- Python ≥ 3.11
- agora-etl ≥ 0.2.0

---

## Building from source

You need a Rust toolchain and [maturin](https://github.com/PyO3/maturin).

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone the repo
git clone https://github.com/thanhtham010891/agora-etl-rs.git
cd agora-etl-rs

# Create a virtual environment and install build tools
python3.11 -m venv .venv
source .venv/bin/activate
pip install "maturin>=1.0,<2"

# Build and install the extension into the active venv
maturin develop --release
```

### Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.
