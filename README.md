# Redis Stress Tester

A Python stress-testing tool for Redis — standalone or cluster — with password authentication.  
Tests maximum simultaneous connections, load throughput (SET / GET / DEL), and pipeline performance.  
Automatically detects whether the target node is a cluster master, standalone master, or a **read-only replica** and adjusts all tests accordingly.

---

## Features

- **Auto-detect** cluster vs. standalone mode (no need to pass a flag)
- **Replica-aware** — detects read-only replicas and switches to GET-only mode automatically
- **Connection pool reuse** — connections opened in Step 2 are handed directly to Step 3, avoiding OS `TIME_WAIT` socket exhaustion
- **Persistent connection floor** — load test holds a configurable minimum number of live connections throughout the run
- **Four test steps**, each independently skippable
- Colored terminal output with a live progress spinner
- Pretty summary table (via `tabulate` if installed, plain text fallback)

---

## Requirements

| Package | Version | Notes |
|---------|---------|-------|
| Python | ≥ 3.9 | Uses `dataclasses`, `threading`, `queue` |
| `redis` | ≥ 4.0 | Tested with 7.x (`ClusterNode` API) |
| `tabulate` | any | Optional — prettier report table |

```bash
pip install redis
pip install tabulate   # optional
```

---

## Quick Start

```bash
# Standalone Redis with password (auto-detect mode)
python redis_cluster_loadtest.py \
  --host 127.0.0.1 \
  --port 6379 \
  --password "secret"

# Cluster node
python redis_cluster_loadtest.py \
  --host redis.example.com \
  --port 6379 \
  --password "secret" \
  --cluster

# Force standalone (skip auto-detect)
python redis_cluster_loadtest.py \
  --host redis.example.com \
  --port 32570 \
  --password "secret" \
  --no-cluster
```

---

## CLI Reference

### Connection

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Redis host |
| `--port` | `6379` | Redis port |
| `--password` | _(empty)_ | AUTH password |
| `--ssl` | off | Enable TLS/SSL |
| `--cluster` | off | Force cluster mode |
| `--no-cluster` | off | Force standalone mode |

### Step 2 — Max connections

| Flag | Default | Description |
|------|---------|-------------|
| `--max-conn` | `10000` | Target number of simultaneous connections |
| `--conn-step` | `50` | How many connections to open per batch |

### Step 3 — Load test

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `100` | Number of parallel worker threads |
| `--ops` | `500` | Operations per worker |
| `--min-conns` | `1000` | Minimum live connections to hold during the test |

### Step 4 — Pipeline test

| Flag | Default | Description |
|------|---------|-------------|
| `--pipeline-size` | `100` | Commands per pipeline batch |
| `--pipeline-iter` | `100` | Number of pipeline batches |

### Skip flags

| Flag | Skips |
|------|-------|
| `--skip-conn` | Step 2 (max connections) |
| `--skip-load` | Step 3 (load test) |
| `--skip-pipeline` | Step 4 (pipeline) |

---

## What Each Step Does

### Step 1 — Connectivity check
Connects with a single client and prints:
- Redis version, OS, uptime, memory usage
- Replication role (`master` or `replica`)
- Cluster state and slot assignment (cluster mode only)

If the node is a **replica**, all subsequent write operations are automatically replaced with GETs.

### Step 2 — Max connections test
Opens connections in parallel batches until either `--max-conn` is reached or three consecutive batches fail.  
The connections are **kept open** and passed to Step 3 to avoid `TIME_WAIT` socket exhaustion.

### Step 3 — Load test
Maintains at least `--min-conns` live sockets throughout the test.  
Worker threads share the connection pool (round-robin) and execute:

| Mode | Operations |
|------|-----------|
| Master | SET → GET → DEL (cycling) |
| Replica | GET only |

Reports ops/sec and latency percentiles (avg, P50, P95, P99, max).

### Step 4 — Pipeline test
Sends commands in batches without waiting for individual responses (pipelining).  
Automatically skipped in cluster mode (pipelines are shard-local in Redis Cluster).

---

## OS Tuning

Redis can handle thousands of connections, but the OS imposes limits on open file descriptors.  
The default on macOS is **256**, which is why the test may stop at ~246.

```bash
# Check current limit
ulimit -n

# Raise for the current session
ulimit -n 65535

# macOS — raise system-wide
sudo launchctl limit maxfiles 65536 200000
sudo sysctl -w kern.maxfiles=200000
sudo sysctl -w kern.maxfilesperproc=65536

# Linux — raise for current session
ulimit -n 65535

# Linux — persist in /etc/security/limits.conf
echo "* soft nofile 65535" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.conf
```

Also check the Redis server's own connection limit:

```bash
redis-cli -h HOST -p PORT -a PASSWORD config get maxclients
# Default is 10000
```

---

## Sample Output

```
╔══════════════════════════════════════════╗
║       Redis Cluster Stress Tester        ║
╚══════════════════════════════════════════╝
ℹ redis-py version : 7.0.1
ℹ Host             : redis.example.com:6379
ℹ SSL              : no
ℹ Password         : *** (set)
ℹ Mode : detecting automatically…
✔ Mode : Standalone (auto-detected)

══════════════════════════════════════════
 STEP 1: Connectivity check
══════════════════════════════════════════
✔ PING → True
✔ Redis version : 7.2.7
✔ Mode          : standalone
✔ Role          : master (read-write)
✔ Memory used   : 2 MB

══════════════════════════════════════════
 STEP 2: Max connections test
══════════════════════════════════════════
  Open: 9989  Failed: 11
✔ Max open connections: 9989

══════════════════════════════════════════
 STEP 3: Load test (SET/GET/DEL)
══════════════════════════════════════════
✔ Reusing 9989 connections from Step 2 (no TIME_WAIT!)
✔ Pool ready: 1000 live connections
  ⣾ Threads: 45/100  Live conns: 1000  Elapsed: 4.2s

╔══════════════════════════════════════════╗
║              SUMMARY REPORT              ║
╚══════════════════════════════════════════╝

── LOAD_TEST ──
╭────────────────────────┬───────────────╮
│ Metric                 │ Value         │
├────────────────────────┼───────────────┤
│ Total ops              │ 50000         │
│ Successful             │ 50000         │
│ Failed                 │ 0             │
│ Success rate           │ 100.00%       │
│ Duration               │ 6.83 s        │
│ Ops/sec                │ 7320          │
│ Active connections     │ 1000          │
│ Avg latency            │ 13.42 ms      │
│ P50                    │ 12.88 ms      │
│ P95                    │ 21.34 ms      │
│ P99                    │ 28.11 ms      │
│ Max latency            │ 54.20 ms      │
╰────────────────────────┴───────────────╯
```

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `AuthenticationError` | Wrong or missing password | Check `--password` |
| `ReadOnlyError` | Connected to a replica | Script auto-switches to GET-only; to test writes, point to the master |
| `ConnectionError` at ~246 | macOS fd limit (256) | Run `ulimit -n 65535` before the test |
| `RedisClusterException: Cluster mode is not enabled` | Node is standalone but `--cluster` was passed | Remove `--cluster` or use auto-detect |
| Pool stuck at 195 after step 2 | Sockets in `TIME_WAIT` | Fixed — step 3 reuses step 2 pool |

---

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).  
Copyright (c) 2026 [Yurii Onuk](https://onuk.org.ua/)