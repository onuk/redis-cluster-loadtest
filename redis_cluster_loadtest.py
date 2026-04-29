#!/usr/bin/env python3
"""
Redis Stress Tester — standalone-replication aware
Supports: single node, cluster, or multi-node replication (1 master + N replicas).
Authentication via password (AUTH).
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
from typing import Optional, List

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
    MAGENTA= "\033[95m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

def ok(msg):    print(f"{C.GREEN}✔ {msg}{C.RESET}")
def err(msg):   print(f"{C.RED}✘ {msg}{C.RESET}")
def info(msg):  print(f"{C.CYAN}ℹ {msg}{C.RESET}")
def warn(msg):  print(f"{C.YELLOW}⚠ {msg}{C.RESET}")
def bold(msg):  print(f"{C.BOLD}{msg}{C.RESET}")
def head(msg):  print(f"{C.MAGENTA}{C.BOLD}{msg}{C.RESET}")


# ─────────────────────────────────────────────────────────────
# Node descriptor
# ─────────────────────────────────────────────────────────────
@dataclass
class NodeInfo:
    host: str
    port: int
    role: str = "unknown"        # 'master' | 'slave' | 'unknown'
    redis_version: str = "?"
    os: str = "?"
    uptime_days: int = 0
    memory_mb: int = 0
    master_host: str = ""
    master_port: str = ""
    reachable: bool = False

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def is_replica(self) -> bool:
        return self.role == "slave"

    @property
    def label(self) -> str:
        role_str = {
            "master": f"{C.GREEN}MASTER{C.RESET}",
            "slave":  f"{C.YELLOW}REPLICA{C.RESET}",
        }.get(self.role, f"{C.RED}UNKNOWN{C.RESET}")
        return f"{self.addr} [{role_str}]"


# ─────────────────────────────────────────────────────────────
# Test results
# ─────────────────────────────────────────────────────────────
@dataclass
class TestResult:
    test_name: str
    node: str = ""
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
# Parse host list  "host:port,host:port,..."
# ─────────────────────────────────────────────────────────────
def parse_hosts(raw: str) -> List[tuple]:
    """
    Accepts:
      - "host:port"
      - "host:port,host:port,..."
      - "host:port host:port ..." (space-separated)
    Returns list of (host, port) tuples.
    """
    nodes = []
    for token in raw.replace(",", " ").split():
        token = token.strip()
        if not token:
            continue
        if ":" in token:
            h, p = token.rsplit(":", 1)
            nodes.append((h.strip(), int(p.strip())))
        else:
            nodes.append((token, 6379))
    return nodes


# ─────────────────────────────────────────────────────────────
# Client factory
# ─────────────────────────────────────────────────────────────
def make_client(host: str, port: int, password: Optional[str],
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
        if REDIS_VERSION < (5, 3):
            kwargs["read_from_replicas"] = False
        return RedisCluster(**kwargs)

    return redis.Redis(
        host=host, port=port,
        password=password or None,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        ssl=use_ssl,
        decode_responses=True,
    )


# ─────────────────────────────────────────────────────────────
# STEP 1: Probe all nodes, detect topology
# ─────────────────────────────────────────────────────────────
def probe_node(host: str, port: int, password: Optional[str],
               use_ssl: bool, is_cluster: bool) -> NodeInfo:
    node = NodeInfo(host=host, port=port)
    try:
        c = make_client(host, port, password, use_ssl, is_cluster, timeout=5.0)
        c.ping()

        srv = c.info("server")
        node.redis_version = srv.get("redis_version", "?")
        node.os            = srv.get("os", "?")
        node.uptime_days   = int(srv.get("uptime_in_days", 0))

        mem = c.info("memory")
        node.memory_mb = int(mem.get("used_memory", 0)) // 1024 // 1024

        rep = c.info("replication")
        node.role        = rep.get("role", "master")
        node.master_host = rep.get("master_host", "")
        node.master_port = str(rep.get("master_port", ""))

        node.reachable = True
        c.close()
    except AuthenticationError as e:
        err(f"  {host}:{port} — Authentication error: {e}")
    except Exception as e:
        err(f"  {host}:{port} — {type(e).__name__}: {e}")
    return node


def step_connectivity(nodes_raw: List[tuple], password, use_ssl, is_cluster) -> List[NodeInfo]:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 1: Topology discovery")
    bold("══════════════════════════════════════════")

    nodes: List[NodeInfo] = []
    for (host, port) in nodes_raw:
        info(f"Probing {host}:{port} …")
        n = probe_node(host, port, password, use_ssl, is_cluster)
        nodes.append(n)
        if n.reachable:
            role_str = (f"{C.GREEN}MASTER{C.RESET}"  if n.role == "master"
                   else f"{C.YELLOW}REPLICA{C.RESET}" if n.role == "slave"
                   else n.role)
            ok(f"  {host}:{port}  role={role_str}  "
               f"redis={n.redis_version}  mem={n.memory_mb}MB  "
               f"uptime={n.uptime_days}d")
            if n.is_replica:
                info(f"    └─ replicates from {n.master_host}:{n.master_port}")
        else:
            err(f"  {host}:{port}  UNREACHABLE")

    masters  = [n for n in nodes if n.reachable and n.role == "master"]
    replicas = [n for n in nodes if n.reachable and n.is_replica]
    failed   = [n for n in nodes if not n.reachable]

    bold("\n  Topology summary:")
    ok(f"  Masters  : {len(masters)}  → " +
       ", ".join(n.addr for n in masters) if masters else "  Masters  : 0")
    ok(f"  Replicas : {len(replicas)}  → " +
       ", ".join(n.addr for n in replicas)) if replicas else warn("  Replicas : 0")
    if failed:
        warn(f"  Failed   : {len(failed)}  → " + ", ".join(n.addr for n in failed))

    reachable = [n for n in nodes if n.reachable]
    if not reachable:
        err("No reachable nodes — aborting.")
        sys.exit(1)

    return nodes


# ─────────────────────────────────────────────────────────────
# STEP 2: Max connections test  (per node)
# ─────────────────────────────────────────────────────────────
def _open_one(args):
    host, port, password, use_ssl, is_cluster = args
    try:
        c = make_client(host, port, password, use_ssl, is_cluster, timeout=3.0)
        c.ping()
        return c
    except Exception as e:
        return e


def max_conn_for_node(node: NodeInfo, password, use_ssl, is_cluster,
                      max_conn: int, step: int) -> TestResult:
    result = TestResult("max_connections", node=node.addr)
    clients = []
    errors  = {}
    consecutive_failures = 0

    while len(clients) < max_conn:
        needed   = min(step, max_conn - len(clients))
        task_args = [(node.host, node.port, password, use_ssl, is_cluster)] * needed
        with ThreadPoolExecutor(max_workers=needed) as ex:
            batch = list(ex.map(_open_one, task_args))

        ok_batch  = [r for r in batch if not isinstance(r, Exception)]
        err_batch = [r for r in batch if isinstance(r, Exception)]
        clients.extend(ok_batch)

        for e in err_batch:
            n = type(e).__name__
            errors[n] = errors.get(n, 0) + 1

        print(f"  [{node.addr}] Open: {C.GREEN}{len(clients)}{C.RESET}  "
              f"Failed: {C.RED}{len(err_batch)}{C.RESET}        ", end="\r")

        if err_batch:
            consecutive_failures += 1
            if consecutive_failures >= 3 or len(err_batch) == needed:
                warn(f"\n  OS limit hit at {len(clients)} connections")
                break
        else:
            consecutive_failures = 0

    print()
    result.max_connections = len(clients)
    result.errors          = errors
    result._pool           = clients   # kept open for load test reuse
    return result


def step_max_connections(nodes: List[NodeInfo], password, use_ssl, is_cluster,
                         max_conn: int, step: int) -> dict:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 2: Max connections test  (per node)")
    bold("══════════════════════════════════════════")

    results = {}
    for node in nodes:
        if not node.reachable:
            continue
        info(f"\nTesting {node.label}  (target: {max_conn})…")
        r = max_conn_for_node(node, password, use_ssl, is_cluster, max_conn, step)
        ok(f"Max open connections: {C.BOLD}{r.max_connections}{C.RESET}")
        results[node.addr] = r

    # Close all pools — will be re-opened in load test
    # (or pass them through if you want to avoid TIME_WAIT on the same run)
    return results


# ─────────────────────────────────────────────────────────────
# STEP 3: Load test  (per node)
# ─────────────────────────────────────────────────────────────
def _build_pool(node: NodeInfo, password, use_ssl, is_cluster,
                target: int, step: int = 100) -> tuple:
    clients = []
    errors  = {}
    consecutive_failures = 0

    info(f"  Pre-opening {target} connections to {node.addr}…")
    while len(clients) < target:
        needed = min(step, target - len(clients))
        task_args = [(node.host, node.port, password, use_ssl, is_cluster)] * needed
        with ThreadPoolExecutor(max_workers=needed) as ex:
            batch = list(ex.map(_open_one, task_args))
        ok_b  = [r for r in batch if not isinstance(r, Exception)]
        err_b = [r for r in batch if isinstance(r, Exception)]
        clients.extend(ok_b)
        for e in err_b:
            n = type(e).__name__
            errors[n] = errors.get(n, 0) + 1
        print(f"    Connections: {C.GREEN}{len(clients)}{C.RESET}  "
              f"Failed: {C.RED}{len(err_b)}{C.RESET}        ", end="\r")
        if err_b:
            consecutive_failures += 1
            if consecutive_failures >= 3 or len(err_b) == needed:
                warn(f"\n    OS limit hit at {len(clients)}")
                break
        else:
            consecutive_failures = 0
    print()
    return clients, errors


def _worker(worker_id, client, ops, key_prefix, rq, is_replica: bool):
    lat, s, f, errs = [], 0, 0, {}
    cycle = ["GET"] if is_replica else ["SET", "GET", "DEL"]
    for i in range(ops):
        key = f"{key_prefix}:{worker_id}:{i % 100}"
        op  = cycle[i % len(cycle)]
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


def load_test_node(node: NodeInfo, password, use_ssl, is_cluster,
                   workers: int, ops_per_worker: int,
                   min_conns: int, reuse_pool=None) -> TestResult:

    op_label = "GET-only (replica)" if node.is_replica else "SET/GET/DEL"
    info(f"\n  Node: {node.label}  [{op_label}]")
    info(f"  Workers: {workers}  Ops/worker: {ops_per_worker}  "
         f"Total: {workers * ops_per_worker}")

    result  = TestResult("load_test", node=node.addr)
    pool_target = max(workers, min_conns)
    owned_pool  = False

    if reuse_pool and len(reuse_pool) >= min_conns:
        pool = reuse_pool[:pool_target]
        ok(f"  Reusing {len(pool)} connections from Step 2")
    else:
        owned_pool = True
        pool, pool_errs = _build_pool(node, password, use_ssl, is_cluster,
                                      pool_target, step=100)
        for k, v in pool_errs.items():
            result.errors[k] = result.errors.get(k, 0) + v

    actual = len(pool)
    if actual == 0:
        err("  No connections available — skipping this node")
        return result
    if actual < workers:
        warn(f"  Only {actual} connections — reducing workers")
        workers = actual

    ok(f"  Pool ready: {C.BOLD}{actual}{C.RESET} live connections")
    result.max_connections = actual

    rq         = queue.Queue()
    key_prefix = f"lt:{node.port}:{int(time.time())}"
    threads    = []
    t_start    = time.perf_counter()

    for wid in range(workers):
        client = pool[wid % actual]
        t = threading.Thread(
            target=_worker,
            args=(wid, client, ops_per_worker, key_prefix, rq, node.is_replica),
            daemon=True,
        )
        threads.append(t)
        t.start()

    done_evt = threading.Event()
    def progress():
        sp = "⣾⣽⣻⢿⡿⣟⣯⣷"
        i  = 0
        while not done_evt.is_set():
            elapsed = time.perf_counter() - t_start
            alive   = sum(1 for t in threads if t.is_alive())
            print(f"\r  {sp[i%len(sp)]} Threads: {alive}/{workers}  "
                  f"Conns: {C.GREEN}{actual}{C.RESET}  "
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

    if owned_pool:
        for c in pool:
            try: c.close()
            except Exception: pass

    while not rq.empty():
        s, f, lats, errs = rq.get()
        result.success         += s
        result.failed          += f
        result.latencies_ms.extend(lats)
        for k, v in errs.items():
            result.errors[k] = result.errors.get(k, 0) + v
    result.total_ops = result.success + result.failed
    return result


def step_load(nodes: List[NodeInfo], password, use_ssl, is_cluster,
              workers: int, ops_per_worker: int, min_conns: int,
              conn_results: dict) -> List[TestResult]:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 3: Load test  (per node)")
    bold("══════════════════════════════════════════")

    results = []
    for node in nodes:
        if not node.reachable:
            continue
        reuse = getattr(conn_results.get(node.addr), "_pool", None)
        r = load_test_node(node, password, use_ssl, is_cluster,
                           workers, ops_per_worker, min_conns, reuse)
        results.append(r)

        # Close the step-2 pool after load test is done for this node
        if reuse:
            for c in reuse:
                try: c.close()
                except Exception: pass
            info(f"  Step 2 pool closed for {node.addr}")

    return results


# ─────────────────────────────────────────────────────────────
# STEP 4: Pipeline test  (master only — replicas are read-only)
# ─────────────────────────────────────────────────────────────
def step_pipeline(nodes: List[NodeInfo], password, use_ssl, is_cluster,
                  pipeline_size: int, iterations: int) -> List[TestResult]:
    bold("\n══════════════════════════════════════════")
    bold(" STEP 4: Pipeline test")
    bold("══════════════════════════════════════════")

    if is_cluster:
        warn("Pipeline skipped in cluster mode (single-shard limitation)")
        return []

    results = []
    for node in nodes:
        if not node.reachable:
            continue

        info(f"\n  Node: {node.label}  batch={pipeline_size}  iters={iterations}")
        result = TestResult("pipeline", node=node.addr)

        if node.is_replica:
            # GET pipeline on replica
            op_label = "GET-only (replica)"
        else:
            op_label = "SET"
        info(f"  Operations: {op_label}")

        try:
            client = make_client(node.host, node.port, password, use_ssl, is_cluster)

            # Pre-populate keys for GET pipeline on replica
            if not node.is_replica:
                prep = client.pipeline(transaction=False)
                for j in range(pipeline_size):
                    prep.set(f"pipe:prep:{j}", f"v{j}", ex=600)
                prep.execute()
            else:
                info("  Replica: using existing keys from master for GET pipeline")

            t_start = time.perf_counter()
            for i in range(iterations):
                pipe = client.pipeline(transaction=False)
                for j in range(pipeline_size):
                    if node.is_replica:
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
            result.total_ops    = result.success + result.failed
            client.close()
            ok(f"  Done: {result.total_ops} ops in {result.duration_sec:.2f}s  "
               f"({result.ops_per_sec:.0f} ops/sec)")
        except Exception as e:
            err(f"  {type(e).__name__}: {e}")

        results.append(result)

    return results


# ─────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────
def _print_result(r: TestResult):
    if r.test_name == "max_connections":
        ok(f"Max simultaneous connections: {C.BOLD}{r.max_connections}{C.RESET}")
        if r.errors:
            warn("  Failure reasons:")
            for n, cnt in r.errors.items():
                print(f"    {C.RED}{n}{C.RESET}: {cnt}")
    elif r.total_ops == 0:
        warn("  Test skipped or 0 operations")
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
            warn("  Errors:")
            for n, cnt in r.errors.items():
                print(f"    {C.RED}{n}{C.RESET}: {cnt}")


def print_report(nodes: List[NodeInfo],
                 conn_res: dict,
                 load_res: List[TestResult],
                 pipe_res: List[TestResult]):
    bold("\n╔══════════════════════════════════════════╗")
    bold("║              SUMMARY REPORT              ║")
    bold("╚══════════════════════════════════════════╝")

    # Index results by node addr
    load_by_node = {r.node: r for r in load_res}
    pipe_by_node = {r.node: r for r in pipe_res}

    for node in nodes:
        if not node.reachable:
            continue
        role_str = ("MASTER"  if node.role == "master"
               else "REPLICA" if node.role == "slave"
               else "UNKNOWN")
        head(f"\n┌─ {node.addr}  [{role_str}]  redis={node.redis_version}"
             f"  mem={node.memory_mb}MB")

        if node.addr in conn_res:
            bold("│  ── MAX CONNECTIONS ──")
            _print_result(conn_res[node.addr])

        if node.addr in load_by_node:
            bold("│  ── LOAD TEST ──")
            _print_result(load_by_node[node.addr])

        if node.addr in pipe_by_node:
            bold("│  ── PIPELINE ──")
            _print_result(pipe_by_node[node.addr])

        print()


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Redis Stress Tester — standalone, replication, or cluster",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="""
Examples:
  # Single node
  python redis_cluster_test.py --hosts 127.0.0.1:6379 --password secret

  # Replication set (1 master + 2 replicas)
  python redis_cluster_test.py \\
    --hosts "redis.gm3.local:32570,redis.gm3.local:32571,redis.gm3.local:32572" \\
    --password secret

  # Redis Cluster
  python redis_cluster_test.py --hosts redis.example.com:6379 --password secret --cluster
""",
    )
    # Connection
    p.add_argument("--hosts",      default="127.0.0.1:6379",
                   help="Comma-separated list of host:port  (e.g. h1:6379,h2:6380,h3:6381)")
    p.add_argument("--password",   default="", help="AUTH password")
    p.add_argument("--ssl",        action="store_true", help="Enable TLS/SSL")
    p.add_argument("--cluster",    action="store_true", help="Force Redis Cluster mode")
    p.add_argument("--no-cluster", action="store_true", help="Force standalone mode")

    # Step 2
    p.add_argument("--max-conn",      type=int, default=10000, help="Max connections per node")
    p.add_argument("--conn-step",     type=int, default=50,    help="Connection open batch size")

    # Step 3
    p.add_argument("--workers",       type=int, default=100,   help="Parallel workers per node")
    p.add_argument("--ops",           type=int, default=500,   help="Ops per worker")
    p.add_argument("--min-conns",     type=int, default=1000,  help="Min live connections during load test")

    # Step 4
    p.add_argument("--pipeline-size", type=int, default=100,   help="Pipeline batch size")
    p.add_argument("--pipeline-iter", type=int, default=100,   help="Pipeline iterations")

    # Skip
    p.add_argument("--skip-conn",     action="store_true", help="Skip Step 2")
    p.add_argument("--skip-load",     action="store_true", help="Skip Step 3")
    p.add_argument("--skip-pipeline", action="store_true", help="Skip Step 4")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────
def main():
    args     = parse_args()
    password = args.password or None

    bold("\n╔══════════════════════════════════════════╗")
    bold("║       Redis Cluster Stress Tester        ║")
    bold("╚══════════════════════════════════════════╝")
    info(f"redis-py version : {redis.__version__}")
    info(f"Hosts            : {args.hosts}")
    info(f"SSL              : {'yes' if args.ssl else 'no'}")
    info(f"Password         : {'*** (set)' if password else 'not set'}")

    nodes_raw  = parse_hosts(args.hosts)
    is_cluster = args.cluster and not args.no_cluster

    if args.no_cluster:
        info("Mode : Standalone (forced)")
    elif args.cluster:
        info("Mode : Cluster (forced)")
    else:
        info("Mode : Standalone / Replication (auto)")

    # ── STEP 1 ─────────────────────────────────────────────────
    nodes = step_connectivity(nodes_raw, password, args.ssl, is_cluster)

    reachable = [n for n in nodes if n.reachable]
    masters   = [n for n in reachable if n.role == "master"]
    replicas  = [n for n in reachable if n.is_replica]

    if not reachable:
        err("No reachable nodes. Exiting.")
        sys.exit(1)

    if masters:
        ok(f"Master node(s)  : {', '.join(n.addr for n in masters)}")
    if replicas:
        info(f"Replica node(s) : {', '.join(n.addr for n in replicas)}")
        info("Replicas will use GET-only operations (read-only nodes)")

    # ── STEP 2 ─────────────────────────────────────────────────
    conn_results = {}
    if not args.skip_conn:
        conn_results = step_max_connections(
            reachable, password, args.ssl, is_cluster,
            args.max_conn, args.conn_step,
        )

    # ── STEP 3 ─────────────────────────────────────────────────
    load_results = []
    if not args.skip_load:
        load_results = step_load(
            reachable, password, args.ssl, is_cluster,
            args.workers, args.ops, args.min_conns,
            conn_results,
        )

    # ── STEP 4 ─────────────────────────────────────────────────
    pipe_results = []
    if not args.skip_pipeline:
        pipe_results = step_pipeline(
            reachable, password, args.ssl, is_cluster,
            args.pipeline_size, args.pipeline_iter,
        )

    # ── Report ─────────────────────────────────────────────────
    print_report(nodes, conn_results, load_results, pipe_results)


if __name__ == "__main__":
    main()
