#!/usr/bin/env python3
"""
Redis Stress Test — auto-detect Cluster / Standalone
Password-based authentication (AUTH)
Copyright (c) 2026 Yurii Onuk
Licensed under MIT License
"""

import time
import threading
import statistics
import argparse
import sys
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

try:
    import redis
    from redis.cluster import RedisCluster, ClusterNode
    from redis.exceptions import (
        ConnectionError as RedisConnectionError,
        AuthenticationError,
        ClusterError,
        ResponseError,
        RedisClusterException,
    )
    REDIS_VERSION = tuple(int(x) for x in redis.__version__.split(".")[:2])
except ImportError:
    print("ERROR: Redis library not found. Install it with: pip install redis")
    sys.exit(1)

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

# ─────────────────────────────────────────────────────────────
# Colors
# ─────────────────────────────────────────────────────────────
class C:
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

def ok(msg):   print(f"{C.GREEN}✔ {msg}{C.RESET}")
def err(msg):  print(f"{C.RED}✘ {msg}{C.RESET}")
def info(msg): print(f"{C.CYAN}ℹ {msg}{C.RESET}")
def warn(msg): print(f"{C.YELLOW}⚠ {msg}{C.RESET}")
def bold(msg): print(f"{C.BOLD}{msg}{C.RESET}")


# ─────────────────────────────────────────────────────────────
# Auto-detect: cluster or standalone?
# ─────────────────────────────────────────────────────────────
def detect_mode(host: str, port: int, password: Optional[str],
                use_ssl: bool) -> str:
    """
    Connects with a plain redis.Redis client and queries INFO server.
    Returns 'cluster' or 'standalone'.
    """
    try:
        c = redis.Redis(
            host=host, port=port, password=password,
            ssl=use_ssl, socket_timeout=5, decode_responses=True,
        )
        mode = c.info("server").get("redis_mode", "standalone")
        c.close()
        return mode  # 'cluster' or 'standalone'
    except Exception:
        return "standalone"


# ─────────────────────────────────────────────────────────────
# Client factory
# ─────────────────────────────────────────────────────────────
def create_client(host: str, port: int, password: Optional[str],
                  use_ssl: bool, is_cluster: bool, timeout: float = 5.0):
    if is_cluster:
        kwargs = dict(
            startup_nodes=[ClusterNode(host=host, port=port)],
            password=password or None,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            ssl=use_ssl,
            decode_responses=True,
            skip_full_coverage_check=True,
        )
        # read_from_replicas deprecated in redis-py >= 5.3
        if REDIS_VERSION < (5, 3):
            kwargs["read_from_replicas"] = False
        return RedisCluster(**kwargs)

    return redis.Redis(
        host=host,
        port=port,
        password=password or None,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        ssl=use_ssl,
        decode_responses=True,
    )


# ─────────────────────────────────────────────────────────────
# Test results
# ─────────────────────────────────────────────────────────────
@dataclass
class TestResult:
    test_name: str
    total_ops: int = 0
    success: int = 0
    failed: int = 0
    latencies_ms: list = field(default_factory=list)
    duration_sec: float = 0.0
    max_connections: int = 0
    errors: dict = field(default_factory=dict)

    @property
    def ops_per_sec(self):
        return self.total_ops / self.duration_sec if self.duration_sec > 0 else 0

    @property
    def success_rate(self):
        return (self.success / self.total_ops * 100) if self.total_ops > 0 else 0

    @property
    def p50(self):
        return statistics.median(self.latencies_ms) if self.latencies_ms else 0

    @property
    def p95(self):
        if not self.latencies_ms:
            return 0
        return sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.95)]

    @property
    def p99(self):
        if not self.latencies_ms:
            return 0
        return sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.99)]

    @property
    def avg_latency(self):
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0

    @property
    def max_latency(self):
        return max(self.latencies_ms) if self.latencies_ms else 0


# ─────────────────────────────────────────────────────────────
# STEP 1: Connectivity check
# ─────────────────────────────────────────────────────────────
def test_connectivity(host, port, password, use_ssl, is_cluster) -> tuple:
    """Returns (success: bool, is_replica: bool)."""
    bold("\n══════════════════════════════════════════")
    bold(" STEP 1: Connectivity check")
    bold("══════════════════════════════════════════")
    try:
        client = create_client(host, port, password, use_ssl, is_cluster)
        pong = client.ping()
        ok(f"PING → {pong}")

        srv = client.info("server")
        ok(f"Redis version : {srv.get('redis_version', '?')}")
        ok(f"Mode          : {srv.get('redis_mode', 'standalone')}")
        ok(f"OS            : {srv.get('os', '?')}")
        ok(f"Uptime (days) : {srv.get('uptime_in_days', '?')}")

        mem = client.info("memory")
        ok(f"Memory used   : {int(mem.get('used_memory', 0)) // 1024 // 1024} MB")

        # Replication role check
        is_replica = False
        try:
            rep = client.info("replication")
            role = rep.get("role", "master")
            if role == "slave":
                is_replica = True
                warn(f"Role          : REPLICA (read-only!)")
                warn(f"Master        : {rep.get('master_host','?')}:{rep.get('master_port','?')}")
                warn("Write ops (SET/DEL) will use GET-only mode to avoid ReadOnlyError")
            else:
                ok(f"Role          : master (read-write)")
        except Exception:
            ok("Role          : master (assumed)")

        if is_cluster:
            try:
                ci = client.cluster_info()
                ok(f"Cluster state : {ci.get('cluster_state', '?')}")
                ok(f"Known nodes   : {ci.get('cluster_known_nodes', '?')}")
                ok(f"Slots assigned: {ci.get('cluster_slots_assigned', '?')}")
            except Exception:
                pass

        client.close()
        return True, is_replica
    except AuthenticationError as e:
        err(f"Authentication error: {e}")
        warn("Check your --password")
    except (RedisConnectionError, RedisClusterException) as e:
        err(f"Connection error: {e}")
    except Exception as e:
        err(f"{type(e).__name__}: {e}")
    return False, False


# ─────────────────────────────────────────────────────────────
# STEP 2: Max connections test
# ─────────────────────────────────────────────────────────────
def test_max_connections(host, port, password, use_ssl, is_cluster,
                         max_conn: int, step: int) -> TestResult:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 2: Max connections test")
    bold("══════════════════════════════════════════")

    result = TestResult("max_connections")
    clients = []
    lock = threading.Lock()

    def open_conn(_):
        try:
            c = create_client(host, port, password, use_ssl, is_cluster, timeout=3.0)
            c.ping()
            with lock:
                clients.append(c)
            return True
        except Exception as e:
            n = type(e).__name__
            with lock:
                result.errors[n] = result.errors.get(n, 0) + 1
            return False

    info(f"Opening connections (step={step}, max={max_conn})…")
    consecutive_failures = 0

    while len(clients) < max_conn:
        needed = min(step, max_conn - len(clients))
        with ThreadPoolExecutor(max_workers=needed) as ex:
            batch = [f.result() for f in as_completed(
                [ex.submit(open_conn, i) for i in range(needed)]
            )]

        failed = batch.count(False)
        print(f"  Open: {C.GREEN}{len(clients)}{C.RESET}  "
              f"Failed: {C.RED}{failed}{C.RESET}        ", end="\r")

        if failed > 0:
            consecutive_failures += 1
            if consecutive_failures >= 3 or failed == needed:
                warn(f"\nConnection limit reached after {consecutive_failures} failed batches!")
                break
        else:
            consecutive_failures = 0

    result.max_connections = len(clients)
    ok(f"\nMax open connections: {C.BOLD}{result.max_connections}{C.RESET}")
    # NOTE: connections are NOT closed here — they are passed to the
    # load test so the OS does not hit TIME_WAIT on re-open.
    result._pool = clients
    return result


# ─────────────────────────────────────────────────────────────
# STEP 3: Load test (SET / GET / DEL)
# Each worker reuses a pre-opened persistent connection from the
# shared pool, so min_conns sockets stay alive for the full test.
# ─────────────────────────────────────────────────────────────
def _open_one_conn(args):
    """Thread-pool target: open + ping one connection."""
    host, port, password, use_ssl, is_cluster = args
    try:
        c = create_client(host, port, password, use_ssl, is_cluster, timeout=5.0)
        c.ping()
        return c
    except Exception as e:
        return e


def build_connection_pool(host, port, password, use_ssl, is_cluster,
                          target: int, step: int = 100):
    """
    Pre-open `target` persistent connections in parallel batches.
    Returns the list of live clients (may be less than target if OS
    limit is hit) and a dict of errors.
    """
    clients = []
    errors = {}
    consecutive_failures = 0

    info(f"Pre-opening {target} persistent connections (batch={step})…")
    while len(clients) < target:
        needed = min(step, target - len(clients))
        task_args = [(host, port, password, use_ssl, is_cluster)] * needed
        with ThreadPoolExecutor(max_workers=needed) as ex:
            results = list(ex.map(_open_one_conn, task_args))

        ok_batch = [r for r in results if not isinstance(r, Exception)]
        err_batch = [r for r in results if isinstance(r, Exception)]
        clients.extend(ok_batch)

        for e in err_batch:
            n = type(e).__name__
            errors[n] = errors.get(n, 0) + 1

        print(f"  Connections ready: {C.GREEN}{len(clients)}{C.RESET}  "
              f"Failed: {C.RED}{len(err_batch)}{C.RESET}        ", end="\r")

        if err_batch:
            consecutive_failures += 1
            if consecutive_failures >= 3 or len(err_batch) == needed:
                warn(f"\nOS connection limit hit at {len(clients)} — continuing with that many")
                break
        else:
            consecutive_failures = 0

    print()
    return clients, errors


def run_worker(worker_id, client, ops_per_worker, key_prefix, rq,
               is_replica: bool = False):
    """
    Worker that reuses an already-open connection from the pool.
    If is_replica=True, only GET is used (replica nodes are read-only).
    """
    lat, s, f, errs = [], 0, 0, {}
    # Master: SET/GET/DEL cycle.  Replica: GET only.
    cycle = ["GET"] if is_replica else ["SET", "GET", "DEL"]
    for i in range(ops_per_worker):
        key = f"{key_prefix}:{worker_id}:{i % 100}"   # reuse 100 keys
        op = cycle[i % len(cycle)]
        try:
            t0 = time.perf_counter()
            if op == "SET":
                client.set(key, f"v{i}", ex=300)
            elif op == "GET":
                client.get(key)
            else:
                client.delete(key)
            lat.append((time.perf_counter() - t0) * 1000)
            s += 1
        except Exception as e:
            f += 1
            n = type(e).__name__
            errs[n] = errs.get(n, 0) + 1
    rq.put((s, f, lat, errs))


def test_load(host, port, password, use_ssl, is_cluster,
              workers, ops_per_worker, min_conns: int = 1000,
              reuse_pool: list = None, is_replica: bool = False) -> TestResult:
    bold("\n══════════════════════════════════════════")
    op_desc = "GET-only (replica)" if is_replica else "SET/GET/DEL"
    bold(f" STEP 3: Load test ({op_desc})")
    bold("══════════════════════════════════════════")

    pool_target = max(workers, min_conns)
    info(f"Workers: {workers}  |  Ops/worker: {ops_per_worker}  "
         f"|  Total ops: {workers * ops_per_worker}")
    info(f"Min persistent connections: {min_conns}  (pool target: {pool_target})")

    result = TestResult("load_test")
    pool_errors = {}
    owned_pool = False  # whether we opened the pool ourselves

    # ── Phase 1: get connection pool ──────────────────────────
    if reuse_pool and len(reuse_pool) >= min_conns:
        # Reuse connections left open by step 2 — avoids TIME_WAIT
        pool = reuse_pool[:pool_target]
        ok(f"Reusing {len(pool)} connections from Step 2 (no TIME_WAIT!)")
    else:
        # Open fresh pool (step 2 was skipped or pool is too small)
        owned_pool = True
        pool, pool_errors = build_connection_pool(
            host, port, password, use_ssl, is_cluster,
            target=pool_target, step=100,
        )

    actual_conns = len(pool)
    if actual_conns == 0:
        err("Could not open any connections — aborting load test")
        return result
    if actual_conns < workers:
        warn(f"Only {actual_conns} connections available — reducing workers to match")
        workers = actual_conns

    ok(f"Pool ready: {C.BOLD}{actual_conns}{C.RESET} live connections")
    result.max_connections = actual_conns
    for k, v in pool_errors.items():
        result.errors[k] = result.errors.get(k, 0) + v

    # ── Phase 2: run workers ──────────────────────────────────
    rq: queue.Queue = queue.Queue()
    key_prefix = f"lt:{int(time.time())}"

    # Pre-populate keys on master so replica GET has data to read
    if is_replica:
        info("Replica mode: skipping SET pre-population (data must exist on master)")

    threads = []
    t_start = time.perf_counter()

    for wid in range(workers):
        client = pool[wid % actual_conns]
        t = threading.Thread(
            target=run_worker,
            args=(wid, client, ops_per_worker, key_prefix, rq, is_replica),
            daemon=True,
        )
        threads.append(t)
        t.start()

    done_evt = threading.Event()
    def progress():
        sp = "⣾⣽⣻⢿⡿⣟⣯⣷"
        i = 0
        while not done_evt.is_set():
            elapsed = time.perf_counter() - t_start
            alive = sum(1 for t in threads if t.is_alive())
            print(f"\r  {sp[i%len(sp)]} Threads: {alive}/{workers}  "
                  f"Live conns: {C.GREEN}{actual_conns}{C.RESET}  "
                  f"Elapsed: {elapsed:.1f}s   ", end="", flush=True)
            i += 1
            time.sleep(0.1)

    prog = threading.Thread(target=progress, daemon=True)
    prog.start()
    for t in threads:
        t.join()
    done_evt.set()
    prog.join()
    result.duration_sec = time.perf_counter() - t_start
    print()

    # ── Phase 3: close pool only if we opened it ─────────────
    if owned_pool:
        info(f"Closing {actual_conns} pooled connections…")
        for c in pool:
            try:
                c.close()
            except Exception:
                pass
        ok("Pool closed")
    else:
        info(f"Leaving {actual_conns} connections open (owned by Step 2)")

    while not rq.empty():
        s, f, lats, errs = rq.get()
        result.success += s
        result.failed += f
        result.latencies_ms.extend(lats)
        for k, v in errs.items():
            result.errors[k] = result.errors.get(k, 0) + v
    result.total_ops = result.success + result.failed
    return result


# ─────────────────────────────────────────────────────────────
# STEP 4: Pipeline test (standalone only)
# ─────────────────────────────────────────────────────────────
def test_pipeline(host, port, password, use_ssl, is_cluster,
                  pipeline_size, iterations, is_replica: bool = False) -> TestResult:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 4: Pipeline test")
    bold("══════════════════════════════════════════")
    result = TestResult("pipeline")

    if is_cluster:
        warn("Pipeline in cluster mode is limited to a single shard — skipping")
        return result

    if is_replica:
        info(f"Batch size: {pipeline_size}  |  Iterations: {iterations}  |  GET-only (replica)")
    else:
        info(f"Batch size: {pipeline_size}  |  Iterations: {iterations}  |  SET")

    try:
        client = create_client(host, port, password, use_ssl, is_cluster)

        # Pre-populate keys so GET pipeline has data (replica or not)
        if not is_replica:
            prep = client.pipeline(transaction=False)
            for j in range(pipeline_size):
                prep.set(f"pipe:prep:{j}", f"v{j}", ex=600)
            prep.execute()

        t_start = time.perf_counter()
        for i in range(iterations):
            pipe = client.pipeline(transaction=False)
            for j in range(pipeline_size):
                if is_replica:
                    pipe.get(f"pipe:prep:{j}")
                else:
                    pipe.set(f"pipe:{i}:{j}", f"v{j}", ex=60)
            t0 = time.perf_counter()
            try:
                pipe.execute()
                result.latencies_ms.append((time.perf_counter() - t0) * 1000)
                result.success += pipeline_size
            except Exception as e:
                result.failed += pipeline_size
                n = type(e).__name__
                result.errors[n] = result.errors.get(n, 0) + 1
        result.duration_sec = time.perf_counter() - t_start
        result.total_ops = result.success + result.failed
        client.close()
        ok(f"Done: {result.total_ops} ops in {result.duration_sec:.2f}s")
    except Exception as e:
        err(f"{type(e).__name__}: {e}")
    return result


# ─────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────
def print_report(results: list):
    bold("\n╔══════════════════════════════════════════╗")
    bold("║              SUMMARY REPORT              ║")
    bold("╚══════════════════════════════════════════╝\n")

    for r in results:
        bold(f"── {r.test_name.upper()} ──")
        if r.test_name == "max_connections":
            ok(f"Max simultaneous connections: {C.BOLD}{r.max_connections}{C.RESET}")
            if r.errors:
                warn("Failure reasons:")
                for n, cnt in r.errors.items():
                    print(f"    {C.RED}{n}{C.RESET}: {cnt}")
        elif r.total_ops == 0:
            warn("Test skipped or 0 operations")
        else:
            rows = [
                ["Total ops",          r.total_ops],
                ["Successful",         r.success],
                ["Failed",             r.failed],
                ["Success rate",       f"{r.success_rate:.2f}%"],
                ["Duration",           f"{r.duration_sec:.2f} s"],
                ["Ops/sec",            f"{r.ops_per_sec:.0f}"],
                ["Active connections", r.max_connections],
                ["Avg latency",        f"{r.avg_latency:.2f} ms"],
                ["P50",                f"{r.p50:.2f} ms"],
                ["P95",                f"{r.p95:.2f} ms"],
                ["P99",                f"{r.p99:.2f} ms"],
                ["Max latency",        f"{r.max_latency:.2f} ms"],
            ]
            if HAS_TABULATE:
                print(tabulate(rows, headers=["Metric", "Value"],
                               tablefmt="rounded_outline"))
            else:
                for name, val in rows:
                    print(f"  {name:<22} {val}")
            if r.errors:
                warn("Errors:")
                for n, cnt in r.errors.items():
                    print(f"    {C.RED}{n}{C.RESET}: {cnt}")
        print()


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Redis Stress Test (auto-detect cluster/standalone)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host",       default="127.0.0.1", help="Redis host")
    p.add_argument("--port",       type=int, default=6379, help="Redis port")
    p.add_argument("--password",   default="", help="AUTH password")
    p.add_argument("--ssl",        action="store_true", help="Enable TLS/SSL")
    p.add_argument("--cluster",    action="store_true",
                   help="Force cluster mode (default: auto-detect)")
    p.add_argument("--no-cluster", action="store_true",
                   help="Force standalone mode (overrides auto-detect)")

    p.add_argument("--max-conn",      type=int, default=10000, help="Max connections to test")
    p.add_argument("--conn-step",     type=int, default=50,    help="Connection open batch size")
    p.add_argument("--workers",       type=int, default=100,   help="Parallel worker threads")
    p.add_argument("--ops",           type=int, default=500,   help="Ops per worker")
    p.add_argument("--min-conns",     type=int, default=1000,  help="Min persistent connections during load test")
    p.add_argument("--pipeline-size", type=int, default=100,   help="Pipeline batch size")
    p.add_argument("--pipeline-iter", type=int, default=100,   help="Pipeline iterations")

    p.add_argument("--skip-conn",     action="store_true", help="Skip connection test")
    p.add_argument("--skip-load",     action="store_true", help="Skip load test")
    p.add_argument("--skip-pipeline", action="store_true", help="Skip pipeline test")
    return p.parse_args()


def main():
    args = parse_args()
    password = args.password or None

    bold("\n╔══════════════════════════════════════════╗")
    bold("║       Redis Cluster Stress Tester        ║")
    bold("╚══════════════════════════════════════════╝")
    info(f"redis-py version : {redis.__version__}")
    info(f"Host             : {args.host}:{args.port}")
    info(f"SSL              : {'yes' if args.ssl else 'no'}")
    info(f"Password         : {'*** (set)' if password else 'not set'}")

    # Mode detection
    if args.no_cluster:
        is_cluster = False
        info("Mode : Standalone (forced)")
    elif args.cluster:
        is_cluster = True
        info("Mode : Cluster (forced)")
    else:
        info("Mode : detecting automatically…")
        detected = detect_mode(args.host, args.port, password, args.ssl)
        is_cluster = (detected == "cluster")
        ok(f"Mode : {detected.capitalize()} (auto-detected)")

    connected, is_replica = test_connectivity(
        args.host, args.port, password, args.ssl, is_cluster)
    if not connected:
        err("Check your connection parameters and try again.")
        sys.exit(1)

    results = []
    conn_pool = None  # connections kept alive from step 2 → reused in step 3

    if not args.skip_conn:
        r = test_max_connections(
            args.host, args.port, password, args.ssl, is_cluster,
            args.max_conn, args.conn_step,
        )
        results.append(r)
        conn_pool = getattr(r, "_pool", None)  # grab the open pool

    if not args.skip_load:
        r = test_load(
            args.host, args.port, password, args.ssl, is_cluster,
            args.workers, args.ops, args.min_conns,
            reuse_pool=conn_pool,
            is_replica=is_replica,
        )
        results.append(r)
        # Now safe to close step-2 pool
        if conn_pool:
            info(f"Closing Step 2 pool ({len(conn_pool)} connections)…")
            for c in conn_pool:
                try:
                    c.close()
                except Exception:
                    pass
            ok("Step 2 pool closed")
            conn_pool = None

    # If step 2 was skipped, close pool if still open
    if conn_pool:
        for c in conn_pool:
            try:
                c.close()
            except Exception:
                pass

    if not args.skip_pipeline:
        results.append(test_pipeline(
            args.host, args.port, password, args.ssl, is_cluster,
            args.pipeline_size, args.pipeline_iter,
            is_replica=is_replica,
        ))

    print_report(results)


if __name__ == "__main__":
    main()
