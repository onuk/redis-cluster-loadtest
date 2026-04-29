# Redis Stress Tester

A Python stress-testing tool for Redis with support for **single node**, **standalone replication** (1 master + N replicas), and **cluster** mode.
Tests maximum simultaneous connections, load throughput (SET / GET / DEL), and pipeline performance.  
Automatically discovers topology, detects read-only replicas, and runs all tests per-node with appropriate read/write operations.

---

## Features

- **Multi-node support** — pass a comma-separated list of `host:port` entries; the tool probes each one and builds the full topology map
- **Auto role detection** — identifies masters and replicas via `INFO replication`; replicas automatically run GET-only operations (no `ReadOnlyError`)
- **Connection pool reuse** — connections opened in Step 2 are handed directly to Step 3, avoiding `TIME_WAIT` socket exhaustion
- **Persistent connection floor** — load test holds a configurable minimum number of live connections throughout the entire run
- **Per-node results** — every step runs independently against each node; the summary report is grouped by node with its role
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

### Single node
```bash
python redis_cluster_test.py \
  --hosts redis0.example.com:6379 \
  --password "secret"
```

### Standalone replication (1 master + 2 replicas)
```bash
python redis_cluster_test.py \
  --hosts "redis0.example.com:6379,redis1.example.com:6379,redis2.example.com:6379" \
  --password "secret"
```

### Replication with full load options
```bash
ulimit -n 65535

python redis_cluster_test.py \
  --hosts "redis0.example.com:6379,redis1.example.com:6379,redis2.example.com:6379" \
  --password "secret" \
  --min-conns 1000 \
  --workers 200 \
  --ops 1000
```
This opens **1 000 live connections and runs 200 workers × 1 000 ops = 200 000 operations** against each node (600 000 total across 3 nodes).

### Redis Cluster
```bash
python redis_cluster_test.py \
  --hosts redis.example.com:6379 \
  --password "secret" \
  --cluster
```

---

## CLI Reference

### Connection

| Flag | Default | Description |
|------|---------|-------------|
| `--hosts` | `127.0.0.1:6379` | Comma-separated `host:port` list. Single or multiple nodes |
| `--password` | _(empty)_ | AUTH password |
| `--ssl` | off | Enable TLS/SSL |
| `--cluster` | off | Force Redis Cluster mode |
| `--no-cluster` | off | Force standalone mode (skip auto-detect) |

### Step 2 — Max connections

| Flag | Default | Description |
|------|---------|-------------|
| `--max-conn` | `10000` | Target simultaneous connections **per node** |
| `--conn-step` | `50` | How many connections to open per batch |

### Step 3 — Load test

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `100` | Parallel worker threads **per node** |
| `--ops` | `500` | Operations per worker |
| `--min-conns` | `1000` | Minimum live connections to hold during the test **per node** |

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

### Step 1 — Topology discovery
Probes every node in `--hosts`, queries `INFO replication`, and prints a full topology map:

```
✔ redis0.example.com:6379  role=MASTER   redis=7.2.7  mem=2MB  uptime=1d
✔ redis1.example.com:6379  role=REPLICA  redis=7.2.7  mem=2MB  uptime=1d
     └─ replicates from redis0.example.com:6379
✔ redis2.example.com:6379  role=REPLICA  redis=7.2.7  mem=2MB  uptime=1d
     └─ replicates from redis0.example.com:6379
```

Unreachable or auth-failed nodes are flagged and excluded from subsequent steps.

### Step 2 — Max connections test _(per node)_
Opens connections in parallel batches until either `--max-conn` is reached or three consecutive batches fail (OS file-descriptor limit).  
Connections are **kept open** and passed directly to Step 3 to avoid `TIME_WAIT` socket exhaustion.

### Step 3 — Load test _(per node)_
Maintains at least `--min-conns` live sockets for the full duration of the test.  
Worker threads share the connection pool (round-robin) and execute:

| Node role | Operations |
|-----------|-----------|
| Master | SET → GET → DEL (cycling) |
| Replica | GET only (read-only nodes) |

Reports ops/sec and latency percentiles (avg, P50, P95, P99, max) for each node.

### Step 4 — Pipeline test _(per node)_
Sends commands in batches without waiting for individual responses.  
Masters use SET pipelines; replicas use GET pipelines against keys pre-populated by the master.  
Automatically skipped in cluster mode (pipelines are shard-local in Redis Cluster).

---

## Per-Node Scaling

All load parameters apply **independently to each node**:

| Parameter | Per node | 3-node total |
|-----------|----------|-------------|
| `--min-conns 1000` | 1 000 conns | 3 000 conns |
| `--workers 200` | 200 threads | 600 threads |
| `--ops 1000` | 200 000 ops | 600 000 ops |

Nodes are tested **sequentially** by default. Each node gets the full allocation of connections and workers.

---

## OS Tuning

Redis can handle thousands of connections but the OS limits open file descriptors.  
The default on macOS is **256**, which stops the test at ~246 connections.

```bash
# Check current limit
ulimit -n

# Raise for current session (recommended before running)
ulimit -n 65535

# macOS — raise system-wide
sudo launchctl limit maxfiles 65536 200000
sudo sysctl -w kern.maxfiles=200000
sudo sysctl -w kern.maxfilesperproc=65536

# Linux — raise for current session
ulimit -n 65535

# Linux — persist across reboots
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
ℹ Hosts            : redis0.example.com:6379,redis1.example.com:6379,redis2.example.com:6379
ℹ SSL              : no
ℹ Password         : *** (set)
ℹ Mode : Standalone / Replication (auto)

══════════════════════════════════════════
 STEP 1: Topology discovery
══════════════════════════════════════════
ℹ Probing redis0.example.com:6379 …
✔   redis0.example.com:6379  role=MASTER   redis=7.2.7  mem=2MB  uptime=0d
ℹ Probing redis1.example.com:6379 …
✔   redis1.example.com:6379  role=REPLICA  redis=7.2.7  mem=2MB  uptime=0d
ℹ     └─ replicates from redis0.example.com:6379
ℹ Probing redis2.example.com:6379 …
✔   redis2.example.com:6379  role=REPLICA  redis=7.2.7  mem=2MB  uptime=1d
ℹ     └─ replicates from redis0.example.com:6379

  Topology summary:
✔   Masters  : 1  → redis0.example.com:6379
✔   Replicas : 2  → redis1.example.com:6379, redis2.example.com:6379
ℹ Replicas will use GET-only operations (read-only nodes)

══════════════════════════════════════════
 STEP 2: Max connections test  (per node)
══════════════════════════════════════════

Testing redis0.example.com:6379 [MASTER]  (target: 10000)…
  [redis0.example.com:6379] Open: 9992  Failed: 8
✔ Max open connections: 9992

Testing redis1.example.com:6379 [REPLICA]  (target: 10000)…
  [redis1.example.com:6379] Open: 6231  Failed: 50
⚠ OS limit hit at 6231 connections
✔ Max open connections: 6231

Testing redis2.example.com:6379 [REPLICA]  (target: 10000)…
  [redis2.example.com:6379] Open: 0  Failed: 50
⚠ OS limit hit at 0 connections
✔ Max open connections: 0

══════════════════════════════════════════
 STEP 3: Load test  (per node)
══════════════════════════════════════════

  Node: redis0.example.com:6379 [MASTER]  [SET/GET/DEL]
  Workers: 200  Ops/worker: 1000  Total: 200000
✔ Reusing 500 connections from Step 2
✔ Pool ready: 500 live connections
  ⣟ Threads: 1/200  Conns: 500  Elapsed: 120.5s

  Node: redis1.example.com:6379 [REPLICA]  [GET-only (replica)]
  Workers: 200  Ops/worker: 1000  Total: 200000
✔ Reusing 500 connections from Step 2
✔ Pool ready: 500 live connections
  ⣽ Threads: 1/200  Conns: 500  Elapsed: 87.0s

  Node: redis2.example.com:6379 [REPLICA]  [GET-only (replica)]
  Workers: 200  Ops/worker: 1000  Total: 200000
  Pre-opening 500 connections to redis2.example.com:6379…
✔ Pool ready: 500 live connections
  ⡿ Threads: 1/200  Conns: 500  Elapsed: 54.1s

══════════════════════════════════════════
 STEP 4: Pipeline test
══════════════════════════════════════════

  Node: redis0.example.com:6379 [MASTER]  batch=100  iters=100
  Operations: SET
✘ ReadOnlyError: node is a replica — pipeline skipped

  Node: redis1.example.com:6379 [REPLICA]  batch=100  iters=100
  Operations: GET-only (replica)
✔ Done: 10000 ops in 5.55s  (1802 ops/sec)

  Node: redis2.example.com:6379 [REPLICA]  batch=100  iters=100
  Operations: GET-only (replica)
✔ Done: 10000 ops in 5.86s  (1706 ops/sec)

╔══════════════════════════════════════════╗
║              SUMMARY REPORT              ║
╚══════════════════════════════════════════╝

┌─ redis0.example.com:6379  [MASTER]  redis=7.2.7  mem=2MB
│  ── MAX CONNECTIONS ──
✔ Max simultaneous connections: 9992
⚠   Failure reasons:
    ConnectionError: 16
│  ── LOAD TEST ──
  Total ops              200000
  Successful             65519
  Failed                 134481
  Success rate           32.76%
  Duration               120.63 s
  Ops/sec                1658
  Active connections     500
  Avg latency            58.56 ms
  P50                    53.06 ms
  P95                    67.10 ms
  P99                    88.77 ms
  Max latency            5291.45 ms
⚠   Errors:
    ReadOnlyError: 131247
    ConnectionError: 3234

┌─ redis1.example.com:6379  [REPLICA]  redis=7.2.7  mem=2MB
│  ── MAX CONNECTIONS ──
✔ Max simultaneous connections: 6231
⚠   Failure reasons:
    ConnectionError: 69
│  ── LOAD TEST ──
  Total ops              200000
  Successful             197772
  Failed                 2228
  Success rate           98.89%
  Duration               87.10 s
  Ops/sec                2296
  Active connections     500
  Avg latency            56.22 ms
  P50                    52.51 ms
  P95                    63.36 ms
  P99                    73.49 ms
  Max latency            5256.54 ms
⚠   Errors:
    ConnectionError: 2228
│  ── PIPELINE ──
  Total ops              10000
  Successful             10000
  Success rate           100.00%
  Duration               5.55 s
  Ops/sec                1802
  Avg latency            55.26 ms
  P50                    50.69 ms
  P95                    57.52 ms
  P99                    391.39 ms
  Max latency            391.39 ms

┌─ redis2.example.com:6379  [REPLICA]  redis=7.2.7  mem=2MB
│  ── MAX CONNECTIONS ──
✔ Max simultaneous connections: 0
⚠   Failure reasons:
    ConnectionError: 50
│  ── LOAD TEST ──
  Total ops              200000
  Successful             200000
  Failed                 0
  Success rate           100.00%
  Duration               54.24 s
  Ops/sec                3688
  Active connections     500
  Avg latency            53.66 ms
  P50                    52.44 ms
  P95                    62.72 ms
  P99                    70.90 ms
  Max latency            152.80 ms
│  ── PIPELINE ──
  Total ops              10000
  Successful             10000
  Success rate           100.00%
  Duration               5.86 s
  Ops/sec                1706
  Avg latency            58.39 ms
  P50                    50.58 ms
  P95                    55.23 ms
  P99                    781.28 ms
  Max latency            781.28 ms
```

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `AuthenticationError` | Wrong or missing password | Check `--password` |
| `ReadOnlyError` in load test | Node reported as master but is actually a replica internally (e.g. failover in progress) | Point `--hosts` directly to the true master IP |
| `ReadOnlyError` in pipeline | Script detected wrong role during Step 1 | Re-run; if persistent — use `--no-cluster` and single master host |
| `ConnectionError` at ~246 | macOS fd limit (256) | Run `ulimit -n 65535` before the test |
| `ConnectionError` mid-load | Server closed idle connections or hit `maxclients` | Increase Redis `maxclients` or reduce `--workers` |
| Max connections = 0 on a node | OS already exhausted all fds from previous nodes in same run | Run `--skip-conn` and test load only, or increase `ulimit` |
| `RedisClusterException` | Node is standalone but `--cluster` was passed | Remove `--cluster` or use auto-detect |
| Pool stuck at ~195 after Step 2 | Sockets in `TIME_WAIT` | Fixed — Step 3 reuses the Step 2 pool directly |

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).  
Copyright (c) 2026 [Yurii Onuk](https://onuk.org.ua/)