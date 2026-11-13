# Raft fault-injection study

A reproducible study of how DistKV's Raft implementation behaves under injected faults:
**availability, tail latency and recovery time** across 12 fault scenarios plus a no-fault baseline,
each repeated three times on a fresh 5-node cluster of real `distkv` processes.

📄 **Report:** [raft-fault-injection-study.pdf](raft-fault-injection-study.pdf) ·
📊 **Data:** [results/](results/) (`summary.csv`, `runs.json`, one `bins.csv` timeline per run) ·
🖼 **Figures:** [results/figures/](results/figures/)

## One command

```bash
make study          # 13 scenarios x 3 runs (~40 min), then plots and the PDF
make study-quick    # one run per scenario (~13 min)
make study-report   # re-plot and rebuild the PDF from existing results
```

`make study` builds the binaries, runs `bin/distkv-study`, then `study/plot.py` and `study/report.py`
in a local virtualenv (`matplotlib`, `reportlab`). Run a subset with
`bin/distkv-study --scenarios crash-leader,partition-minority --runs 1` and list them with `--list`.

## What each run does

1. Start 5 `distkv` nodes (24 shards, replication factor 3, fsync on, fault injection enabled) in a
   fresh temporary directory, wait until every shard has a leader and the cluster has been healthy
   for 3 s, preload 20,000 keys, and let leadership balancing settle.
2. Drive an **open-loop** workload: 3,000 ops/s, 80% linearizable reads, smart client with retries and
   a 5 s deadline per operation. Latency is measured **from each operation's scheduled start**, so
   time queued behind a stall counts (no coordinated omission).
3. After 5 s of warm-up and 10 s of steady state, pick the victim(s) (the node leading the most
   shards), inject the fault, hold it for 10 s, heal it, and observe for 20 s more.
4. Record every operation, bin them every 100 ms, and summarise the windows before, during and
   after the fault.

| Scenario | Fault |
|---|---|
| `baseline` | none |
| `crash-leader` | `SIGKILL` the busiest leader; restart it from disk 10 s later |
| `crash-leader-et250` / `-et1000` | the same with a 250 ms / 1 s election timeout |
| `stop-leader` | `SIGTERM` (leadership transfer before exit); restart 10 s later |
| `pause-leader` | `SIGSTOP` for 10 s, then `SIGCONT` (a GC pause or a frozen VM) |
| `partition-leader` | isolate the leader from all peers for 10 s |
| `partition-minority` | isolate the two busiest leaders: shards with 2 of 3 replicas there lose quorum |
| `partition-flapping` | isolate/reconnect the leader every second, five times |
| `slow-link-50ms` / `-200ms` | delay every Raft message to/from the leader |
| `lossy-link-30` | drop 30% of the leader's Raft messages |
| `rolling-restart` | graceful stop + start of every node in turn |

## Metrics

* **Availability**: successful operations / attempted operations in a window. An operation fails if it
  doesn't succeed within 5 s, retries included.
* **Tail latency**: p99 / p99.9 / max of successful operations, from scheduled start.
* **Recovery time**: from injection until the client-observed service is back to baseline (no
  errors and every 100 ms bin's p99 under max(50 ms, 5x baseline p99)) for 1 s straight. Faults the
  system cannot mask (lost quorum) recover only after the heal; those are marked with `*`.
* **Disrupted time**: total length of degraded 100 ms bins from injection to the end of the run.
* **Rejoin time**: from the heal until every node is ready and every shard has a leader again.

## Caveats

All nodes and the load generator share one machine, so absolute latencies include CPU and disk
contention; differences between scenarios are the signal. The network faults act on Raft traffic
between nodes (client requests still reach every node), which models a partition of the peer network.
