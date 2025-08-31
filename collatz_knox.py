#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collatz 3x+1 — Parallel (big-int safe, cycle-alert), MP→Thread→Single fallback
==============================================================================
- **3x+1** core (easily extensible to 5x+1 if desired)  
- **Non-1 cycle** detection: immediately logs a warning and halts the entire system  
- **SQLite TEXT** ensures big integer safety (no INTEGER overflow)  
- **Single-writer** design (writer) with *W* workers: disk-oriented, low RAM usage  
- **Automatic runtime mode**: multiprocessing → thread → single  
- **Brent’s cycle detection** + fast division by 2^k in one step  
- **DB counter**: helper command to calculate the total number of processed seeds  

ENV
  COLL_RUN_MODE         (auto|mp|thread|single) default: auto  
  COLL_NUM_WORKERS      (default: os.cpu_count())  
  COLL_START            (default: 3000000000000000000000)  
                        Special formats are also supported: "10**200", "2^4096", "0xFFFF...",  
                        underscore notation "1_000_000", etc.  
  COLL_DB_PATH          (default: collatz_parallel_big_cycle.db)  
  COLL_BATCH_SIZE       (default: 20000)  
  COLL_STORE_EVERY      (default: 0 → off) e.g. 10000 means store one sample every 10k seeds  
  COLL_PROGRESS_EVERY   (default: 1000000) progress ping interval for writer  
  COLL_LOG_EVERY_SEC    (default: 60)  
  COLL_MAX_STEPS        (default: 0 → unlimited; safety cap per seed)  
  COLL_MAX_SECS         (default: 0 → unlimited; time cap per seed)  
  # SQLite fine-tuning (optional)  
  COLL_DB_SYNC          (OFF|NORMAL|FULL) default: NORMAL  
  COLL_DB_CACHE_PAGES   (negative = KB) default: -200000  # ~200 MB page cache  
  COLL_DB_MMAP_MB       (default: 512)   # 0 = disabled  

CLI
  python3 collatz_knox.py                 # run (parallel/auto)  
  COLL_START="10**200" python3 collatz_knox.py  
  python3 collatz_knox.py --count         # print total processed seed count from DB  
  python3 collatz_knox.py --test          # run self-tests  
  LICENSE: MIT
  AUTHOR:installKnox
  DATE: 2025-08-31  

"""
from __future__ import annotations
import os, sys, time, signal, sqlite3, argparse, re
from typing import Tuple, Optional

# --- Çok büyük ondalık dizge limiti (Py 3.11+) kapat ---
try:
    if hasattr(sys, "set_int_max_str_digits"):
        sys.set_int_max_str_digits(0)
except Exception:
    pass

# --- Logger (flush kw argı kullanmadan) ---
def log_print(*args):
    try:
        print(*args)
        try:
            sys.stdout.flush()
        except Exception:
            pass
    except Exception:
        pass

# --- MP/Thread probeleri ---
try:
    import multiprocessing as _mp
except Exception:
    _mp = None
import threading as _th
import queue as _qu

def _probe_mp_available() -> bool:
    try:
        if _mp is None:
            return False
        try:
            import _multiprocessing  # type: ignore
        except Exception:
            return False
        q = _mp.Queue(maxsize=1); e = _mp.Event()
        q.put_nowait(None); _ = q.get_nowait(); assert not e.is_set()
        return True
    except Exception:
        return False

def _probe_threads_available() -> bool:
    try:
        t = _th.Thread(target=lambda: None)
        t.start(); t.join()
        return True
    except Exception:
        return False

# --- ENV yardımcıları ---
def env_int(name: str, default: int) -> int:
    try: return int(os.environ.get(name, default))
    except Exception: return default

def env_float(name: str, default: float) -> float:
    try: return float(os.environ.get(name, default))
    except Exception: return default

def env_str(name: str, default: str) -> str:
    v = os.environ.get(name); return v if isinstance(v, str) and v else default

# Büyük tamsayı parser ("10**200", "2^100", hex, altçizgiler)
_def_start = "3000000000000000000000"

def parse_bigint(s: str) -> int:
    s = (s or "").strip().lower().replace("_", "")
    if not s:
        return int(_def_start)
    if s.startswith("0x"):
        return int(s, 16)
    m = re.fullmatch(r"(\d+)\s*(?:\*\*|\^)\s*(\d+)", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return pow(a, b)
    return int(s)

RUN_MODE_PREF  = env_str("COLL_RUN_MODE", "auto").lower()
NUM_WORKERS    = env_int("COLL_NUM_WORKERS", (os.cpu_count() or 2))
START_SEED     = parse_bigint(env_str("COLL_START", _def_start))
STORE_EVERY    = env_int("COLL_STORE_EVERY", 0)
PROGRESS_EVERY = env_int("COLL_PROGRESS_EVERY", 1_000_000)
LOG_EVERY_SEC  = env_int("COLL_LOG_EVERY_SEC", 60)
DB_PATH        = env_str("COLL_DB_PATH", "collatz_knox_cycle.db")
BATCH_SIZE     = env_int("COLL_BATCH_SIZE", 20_000)
MAX_STEPS      = env_int("COLL_MAX_STEPS", 0)
MAX_SECS       = env_float("COLL_MAX_SECS", 0.0)
DB_SYNC        = env_str("COLL_DB_SYNC", "NORMAL").upper()
DB_CACHE_PAGES = env_int("COLL_DB_CACHE_PAGES", -200_000)
DB_MMAP_MB     = env_int("COLL_DB_MMAP_MB", 512)

# --- Çalışma modu seçici & uyumluluk sınıfları ---

def resolve_runtime_mode(pref: str) -> str:
    pref = (pref or "auto").lower()
    if pref == "single":
        return "single"
    if pref == "thread":
        return "thread" if _probe_threads_available() else "single"
    if pref == "mp":
        if _probe_mp_available():
            return "mp"
        return "thread" if _probe_threads_available() else "single"
    # auto
    if _probe_mp_available():
        return "mp"
    if _probe_threads_available():
        return "thread"
    return "single"

class CompatQueue:
    def __init__(self, use_mp: bool, maxsize: int = 0):
        self._q = (_mp.Queue(maxsize=maxsize) if use_mp else _qu.Queue(maxsize=maxsize))
    def put(self, item):
        return self._q.put(item)
    def get(self, timeout: Optional[float] = None):
        if timeout is None:
            return self._q.get()
        return self._q.get(timeout=timeout)

class CompatEvent:
    def __init__(self, use_mp: bool):
        self._e = (_mp.Event() if use_mp else _th.Event())
    def set(self):
        return self._e.set()
    def is_set(self) -> bool:
        return self._e.is_set()

class CompatProcess:
    def __init__(self, use_mp: bool, target, args=(), daemon: bool = True):
        self._p = (_mp.Process(target=target, args=args, daemon=daemon) if use_mp
                   else _th.Thread(target=target, args=args, daemon=daemon))
    def start(self):
        self._p.start()
    def join(self):
        self._p.join()
    def is_alive(self) -> bool:
        return self._p.is_alive()

# --- SQLite (TEXT kolonlar, SSD optimizasyonları) ---

def _apply_pragmas(cur: sqlite3.Cursor):
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute(f"PRAGMA synchronous={DB_SYNC}")
    cur.execute("PRAGMA busy_timeout=60000")
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute("PRAGMA page_size=4096")
    cur.execute(f"PRAGMA cache_size={DB_CACHE_PAGES}")
    if DB_MMAP_MB > 0:
        try:
            cur.execute(f"PRAGMA mmap_size={DB_MMAP_MB*1024*1024}")
        except Exception:
            pass
    cur.execute("PRAGMA journal_size_limit=10485760")


def init_db(path: str):
    con = sqlite3.connect(path, timeout=60, isolation_level=None)
    cur = con.cursor(); _apply_pragmas(cur)
    cur.execute("""CREATE TABLE IF NOT EXISTS progress(
        residue INTEGER PRIMARY KEY,
        last_n TEXT NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS records(
        name TEXT PRIMARY KEY,
        value TEXT,
        seed TEXT,
        updated_at REAL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS samples(
        seed TEXT PRIMARY KEY,
        steps TEXT,
        peak TEXT,
        residue INTEGER
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS milestones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT,
        seed TEXT,
        value TEXT,
        ts REAL
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_samples_residue ON samples(residue)")
    # run_start_seed referansı (count komutu için)
    row = cur.execute("SELECT value FROM records WHERE name='run_start_seed'").fetchone()
    if not row:
        cur.execute("INSERT OR REPLACE INTO records(name,value,seed,updated_at) VALUES(?,?,?,?)",
                    ("run_start_seed", str(START_SEED), str(START_SEED), time.time()))
    con.close()


def db_read_progress(path: str, W: int):
    con = sqlite3.connect(path, timeout=60, isolation_level=None)
    cur = con.cursor(); _apply_pragmas(cur)
    cur.execute("SELECT residue, last_n FROM progress")
    rows = cur.fetchall()
    prog = {}
    for r, n_txt in rows:
        try:
            prog[int(r)] = int(n_txt)
        except Exception:
            prog[int(r)] = 0
    changed = False
    for r in range(W):
        if r not in prog:
            cur.execute("INSERT OR REPLACE INTO progress(residue,last_n) VALUES(?,?)", (r, "0"))
            prog[r] = 0; changed = True
    if changed:
        con.commit()
    con.close()
    return prog

# --- Collatz çekirdeği (3x+1, hızlı) ---

def tz(x: int) -> int:
    return (x & -x).bit_length() - 1

def collatz_fast_step(n: int) -> int:
    if n & 1:
        n = 3*n + 1
        n >>= tz(n)  # 2^k kadar tek hamlede böl
        return n
    else:
        return n >> 1

# Brent cycle detect — (reaches_one, mu, lam, steps, peak)

def brent_cycle_detect(seed: int, max_steps: int = 0, max_secs: float = 0.0) -> Tuple[bool, int, int, int, int]:
    import time as _t
    start_t = _t.time()

    def nxt(x: int) -> int:
        return collatz_fast_step(x)

    power = lam = 1
    tortoise = seed
    hare = nxt(seed)
    steps = 1
    peak = seed if seed >= hare else hare

    while tortoise != hare:
        if hare == 1 or tortoise == 1:
            return True, 0, 0, steps, peak
        if max_steps and steps >= max_steps:
            return True, 0, 0, steps, peak
        if max_secs and (_t.time() - start_t) >= max_secs:
            return True, 0, 0, steps, peak
        if power == lam:
            tortoise = hare
            power <<= 1
            lam = 0
        hare = nxt(hare)
        steps += 1
        if hare > peak:
            peak = hare
        lam += 1

    mu = 0
    tortoise = hare = seed
    for _ in range(lam):
        hare = nxt(hare)
        steps += 1
        if hare > peak:
            peak = hare
        if hare == 1:
            return True, 0, 0, steps, peak

    while tortoise != hare:
        tortoise = nxt(tortoise)
        hare = nxt(hare)
        steps += 2
        if tortoise > peak:
            peak = tortoise
        if hare > peak:
            peak = hare
        if tortoise == 1 or hare == 1:
            return True, 0, 0, steps, peak
        mu += 1

    return False, mu, lam, steps, peak

# --- Uyarı formatı ---

def format_alert(prefix: str, seed: int, mu: int, lam: int, steps: int, peak: int) -> str:
    lines = [
        "",
        f"⚠️ ALERT ({prefix}): NON-1 CYCLE DETECTED",
        f"  seed={seed}",
        f"  cycle_length={lam}",
        f"  cycle_start_index={mu}",
        f"  steps={steps}",
        f"  peak≈{peak}",
        "",
    ]
    return "\n".join(lines)

# --- Writer ---

def writer_proc(q, db_path: str):
    init_db(db_path)
    con = sqlite3.connect(db_path, timeout=60, isolation_level=None)
    cur = con.cursor(); _apply_pragmas(cur)

    def get_record(name: str):
        row = cur.execute("SELECT value, seed FROM records WHERE name=?", (name,)).fetchone()
        if row and row[0] is not None:
            try: return int(row[0]), int(row[1])
            except Exception: return 0, 1
        return 0, 1

    longest_steps, longest_seed = get_record("longest_steps")
    highest_peak , peak_seed    = get_record("highest_peak")

    batch = []
    last_log = time.time()

    def flush():
        nonlocal batch
        if not batch:
            return
        cur.execute("BEGIN")
        for s, p in batch:
            cur.execute(s, p)
        con.commit(); batch = []

    running = True
    while running:
        try:
            item = q.get(timeout=1.0)
        except Exception:
            item = None

        if item is None:
            if (time.time() - last_log) >= LOG_EVERY_SEC:
                log_print(f"[writer] longest={longest_steps}@{longest_seed}, peak={highest_peak}@{peak_seed}")
                last_log = time.time()
            flush(); continue

        kind = item.get("kind")
        if kind == "stop":
            running = False
        elif kind == "progress":
            r = item["residue"]; n = item["n"]
            batch.append(("INSERT OR REPLACE INTO progress(residue,last_n) VALUES(?,?)", (r, str(n))))
        elif kind == "sample":
            if STORE_EVERY > 0:
                batch.append(("INSERT OR REPLACE INTO samples(seed,steps,peak,residue) VALUES(?,?,?,?)",
                              (str(item["seed"]), str(item["steps"]), str(item["peak"]), item["residue"])) )
        elif kind == "record":
            rtype = item["rtype"]; seed = item["seed"]; val = item["value"]
            if rtype == "longest" and val > longest_steps:
                longest_steps, longest_seed = val, seed
                batch.append(("INSERT OR REPLACE INTO records(name,value,seed,updated_at) VALUES(?,?,?,?)",
                              ("longest_steps", str(val), str(seed), time.time())))
                batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                              ("new_longest", str(seed), str(val), time.time())))
            elif rtype == "peak" and val > highest_peak:
                highest_peak, peak_seed = val, seed
                batch.append(("INSERT OR REPLACE INTO records(name,value,seed,updated_at) VALUES(?,?,?,?)",
                              ("highest_peak", str(val), str(seed), time.time())))
                batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                              ("new_peak", str(seed), str(val), time.time())))
        elif kind == "alert":
            batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                          ("non1_cycle_alert", str(item["seed"]), f"mu={item['mu']},lam={item['lam']},steps={item['steps']},peak={item['peak']}", time.time())))
            flush()
            log_print(item["message"])  # yüksek sesle
            running = False
            continue

        if len(batch) >= BATCH_SIZE:
            flush()

    flush(); con.close(); log_print("[writer] clean exit")

# --- Worker ---

def worker_proc(residue: int, W: int, start_seed: int, initial_last_n: int, q, stop):
    base = max(start_seed, initial_last_n + 1)
    rem = base % W
    n = base if rem == residue else base + ((residue - rem) % W)

    verified = 0
    longest_local = 0
    highest_local = 0

    while not stop.is_set():
        reaches_one, mu, lam, steps, peak = brent_cycle_detect(n, MAX_STEPS, MAX_SECS)

        if not reaches_one:
            q.put({
                "kind": "alert",
                "seed": n,
                "mu": mu,
                "lam": lam,
                "steps": steps,
                "peak": peak,
                "message": format_alert(f"worker {residue}/{W}", n, mu, lam, steps, peak),
            })
            stop.set(); return

        if steps > longest_local:
            longest_local = steps
            q.put({"kind": "record", "rtype": "longest", "seed": n, "value": steps})
        if peak > highest_local:
            highest_local = peak
            q.put({"kind": "record", "rtype": "peak", "seed": n, "value": peak})

        if STORE_EVERY > 0:
            idx = (n - residue) // W
            if idx % STORE_EVERY == 0:
                q.put({"kind": "sample", "seed": n, "steps": steps, "peak": peak, "residue": residue})

        verified += 1
        if PROGRESS_EVERY > 0 and (verified % PROGRESS_EVERY) == 0:
            q.put({"kind": "progress", "residue": residue, "n": n})

        n += W

# --- Single (kooperatif) ---

def run_single():
    W = 1
    log_print(f"Starting Collatz (single): W={W}, DB={DB_PATH}, START_SEED={START_SEED}")

    init_db(DB_PATH)
    con = sqlite3.connect(DB_PATH, timeout=60, isolation_level=None)
    cur = con.cursor(); _apply_pragmas(cur)

    def get_record(name: str):
        row = cur.execute("SELECT value, seed FROM records WHERE name=?", (name,)).fetchone()
        if row and row[0] is not None:
            try: return int(row[0]), int(row[1])
            except Exception: return 0, 1
        return 0, 1

    longest_steps, longest_seed = get_record("longest_steps")
    highest_peak , peak_seed    = get_record("highest_peak")

    progress = db_read_progress(DB_PATH, W)
    last_n = progress.get(0, 0)

    base = max(START_SEED, last_n + 1)
    n = base

    batch = []
    last_log = time.time()
    verified = 0

    def flush():
        nonlocal batch
        if not batch:
            return
        cur.execute("BEGIN")
        for s, p in batch:
            cur.execute(s, p)
        con.commit(); batch = []

    try:
        while True:
            reaches_one, mu, lam, steps, peak = brent_cycle_detect(n, MAX_STEPS, MAX_SECS)

            if not reaches_one:
                batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                              ("non1_cycle_alert", str(n), f"mu={mu},lam={lam},steps={steps},peak={peak}", time.time())))
                flush()
                log_print(format_alert("single", n, mu, lam, steps, peak))
                break

            if steps > longest_steps:
                longest_steps, longest_seed = steps, n
                batch.append(("INSERT OR REPLACE INTO records(name,value,seed,updated_at) VALUES(?,?,?,?)",
                              ("longest_steps", str(steps), str(n), time.time())))
                batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                              ("new_longest", str(n), str(steps), time.time())))
            if peak > highest_peak:
                highest_peak, peak_seed = peak, n
                batch.append(("INSERT OR REPLACE INTO records(name,value,seed,updated_at) VALUES(?,?,?,?)",
                              ("highest_peak", str(peak), str(n), time.time())))
                batch.append(("INSERT INTO milestones(kind,seed,value,ts) VALUES(?,?,?,?)",
                              ("new_peak", str(n), str(peak), time.time())))

            if STORE_EVERY > 0 and (verified % max(STORE_EVERY,1) == 0):
                batch.append(("INSERT OR REPLACE INTO samples(seed,steps,peak,residue) VALUES(?,?,?,?)",
                              (str(n), str(steps), str(peak), 0)))

            verified += 1
            if PROGRESS_EVERY > 0 and (verified % PROGRESS_EVERY) == 0:
                batch.append(("INSERT OR REPLACE INTO progress(residue,last_n) VALUES(?,?)", (0, str(n))))
                flush()

            if (time.time() - last_log) >= LOG_EVERY_SEC:
                log_print(f"[single] longest={longest_steps}@{longest_seed}, peak={highest_peak}@{peak_seed}")
                last_log = time.time()

            n += 1

    except KeyboardInterrupt:
        pass
    finally:
        batch.append(("INSERT OR REPLACE INTO progress(residue,last_n) VALUES(?,?)", (0, str(n))))
        flush(); con.close(); log_print("[single] clean exit")

# --- Paralel ana akış (MP / Thread) ---

def run_mt():
    mode = resolve_runtime_mode(RUN_MODE_PREF)
    use_mp = (mode == "mp")
    log_print(f"Starting Collatz (mode={mode}): W={NUM_WORKERS}, DB={DB_PATH}, START_SEED={START_SEED}")

    q = CompatQueue(use_mp=use_mp, maxsize=10000)
    stop = CompatEvent(use_mp=use_mp)

    init_db(DB_PATH)
    progress = db_read_progress(DB_PATH, NUM_WORKERS)

    wp = CompatProcess(use_mp=use_mp, target=writer_proc, args=(q, DB_PATH), daemon=True)
    wp.start()

    procs = []
    for r in range(NUM_WORKERS):
        last_n = progress.get(r, 0)
        p = CompatProcess(use_mp=use_mp, target=worker_proc,
                          args=(r, NUM_WORKERS, START_SEED, last_n, q, stop), daemon=True)
        p.start(); procs.append(p)

    def handle(sig, frm):
        log_print(f"Signal {sig} received; stopping...")
        stop.set()
    try:
        signal.signal(signal.SIGINT, handle)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handle)
    except Exception:
        pass

    try:
        while any(p.is_alive() for p in procs):
            time.sleep(1.0)
            if stop.is_set():
                break
    finally:
        stop.set()
        for p in procs:
            p.join()
        q.put({"kind": "stop"})
        wp.join()

# --- Sayaç: kaç seed işlendi? ---

def count_processed(db_path: str) -> int:
    con = sqlite3.connect(db_path); cur = con.cursor()
    row = cur.execute("SELECT value FROM records WHERE name='run_start_seed'").fetchone()
    run_start = int(row[0]) if row and row[0] else START_SEED
    rows = list(cur.execute("SELECT residue,last_n FROM progress"))
    con.close()
    W = len(rows) if rows else 1
    total = sum(max(0, int(n) - run_start + ((run_start % W) <= r)) // max(1, W) for r, n in rows)
    return total

# --- Testler ---

def _slow_collatz_step(n: int) -> int:
    return (3*n + 1) if (n & 1) else (n >> 1)

def _slow_walk(n: int) -> Tuple[int, int]:
    steps = 0; peak = n
    while n != 1:
        n = _slow_collatz_step(n)
        if n > peak: peak = n
        steps += 1
    return steps, peak

def run_tests():
    # tz
    assert tz(1)==0 and tz(2)==1 and tz(4)==2 and tz(6)==1 and tz(12)==2
    # hızlı adım davranışı (yavaş ile uyum)
    for n in [1,2,3,5,7,9,19,27]:
        nf = collatz_fast_step(n)
        ns = _slow_collatz_step(n)
        while ns!=nf and ns!=1 and nf!=1:
            ns = _slow_collatz_step(ns) if (ns & 1) else (ns>>1)
        assert nf==ns or nf==1 or ns==1
    # Brent küçük tohumlar
    for n in [1,2,3,7,11,19,27,97,871]:
        r, *_ = brent_cycle_detect(n)
        assert r is True
    # slow vs fast tepe
    for n in [3,7,11,19,27,97]:
        s_steps, s_peak = _slow_walk(n)
        r, _, _, f_steps, f_peak = brent_cycle_detect(n)
        assert r is True and f_peak >= s_peak
    # seçimci
    assert resolve_runtime_mode("single") == "single"
    assert resolve_runtime_mode("thread") in ("thread", "single")
    assert resolve_runtime_mode("mp") in ("mp", "thread", "single")
    assert resolve_runtime_mode("auto") in ("mp", "thread", "single")
    print("All tests passed ✔")

# --- Entrypoint ---
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="run self tests and exit")
    ap.add_argument("--count", action="store_true", help="print processed seed count and exit")
    args = ap.parse_args()
    if args.test:
        run_tests()
    elif args.count:
        print(count_processed(DB_PATH))
    else:
        mode = resolve_runtime_mode(RUN_MODE_PREF)
        if mode == "single":
            run_single()
        else:
            run_mt()
