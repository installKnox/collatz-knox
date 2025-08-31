# Collatz 3x+1 Parallel Explorer

This project is an experimental high‑performance implementation of the **Collatz 3x+1 problem**, designed to test extremely large seeds safely. It supports automatic fallback between **multiprocessing → threads → single mode** depending on your environment.

## Key Features

* **3x+1 core** (easily extendable to 5x+1 or other variants)
* **Cycle detection**: If a non‑1 cycle is ever encountered, the system immediately raises an alert and stops
* **Big‑integer safety**: Stores all numbers as TEXT in SQLite (avoids 64‑bit integer overflow)
* **Single‑writer design** with *W* workers: disk‑oriented, low RAM usage
* **Automatic runtime selection**
* **Brent’s algorithm** for cycle detection, optimized with fast division by 2^k
* **Seed counter**: helper command to compute the number of seeds processed so far

## Environment Variables

* `COLL_RUN_MODE` — (auto|mp|thread|single) default: auto
* `COLL_NUM_WORKERS` — number of workers (default: os.cpu\_count())
* `COLL_START` — starting seed (default: 3000000000000000000000). Special formats supported: `10**200`, `2^4096`, hex (`0xFFFF...`), numbers with underscores (`1_000_000`).
* `COLL_DB_PATH` — SQLite database file (default: `collatz_knox_cycle.db`)
* `COLL_BATCH_SIZE` — default: 20000
* `COLL_STORE_EVERY` — (default: 0 → off). Example: `10000` stores every 10k seeds.
* `COLL_PROGRESS_EVERY` — default: 1000000
* `COLL_LOG_EVERY_SEC` — default: 60
* `COLL_MAX_STEPS` — per‑seed step limit (0 = unlimited)
* `COLL_MAX_SECS` — per‑seed time limit (0 = unlimited)
* SQLite fine‑tuning options: `COLL_DB_SYNC`, `COLL_DB_CACHE_PAGES`, `COLL_DB_MMAP_MB`

## Usage

```bash
# Run with defaults
python3 collatz_knox.py

# Start from a very large seed
COLL_START="10**200" python3 collatz_knox.py

# Count how many seeds have been processed so far
python3 collatz_knox.py --count

# Run self‑tests
python3 collatz_knox.py --test
```

## Setup

```bash
# Clone the repo
git clone https://github.com/installKnox/collatz-knox.git
cd collatz-knox

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install requirements (if any)
pip install -r requirements.txt
```

## Background

The **Collatz conjecture** (also known as the *3x+1 problem*) is an unsolved problem in mathematics. It states:

* Start with any positive integer `n`.
* If `n` is even, divide it by 2.
* If `n` is odd, compute `3n+1`.
* Repeat indefinitely.

The conjecture asks whether this sequence always eventually reaches 1. Despite its simple rules, it has remained unsolved since it was first introduced by **Lothar Collatz** in 1937. This project provides a tool to experimentally explore very large seeds and monitor for potential non‑1 cycles.
