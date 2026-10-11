#!/usr/bin/env bash
# Availability under failure: run the 10k ops/s workload against a local
# 3-node cluster while a nemesis SIGKILLs a random node every ~12s and
# restarts it 4s later (one node down at a time, so every shard keeps
# quorum). Reports the fraction of operations that succeeded.
#
#   scripts/chaos-bench.sh [rate] [duration-seconds]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RATE=${1:-10000}
DURATION=${2:-60}

"$ROOT/scripts/cluster.sh" clean >/dev/null 2>&1 || true
"$ROOT/scripts/cluster.sh" start 3 16
trap '"$ROOT/scripts/cluster.sh" stop >/dev/null' EXIT
sleep 6

"$ROOT/bin/distkv-bench" --endpoints http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003 \
  --rate "$RATE" --duration "${DURATION}s" --keys 100000 --read-ratio 0.8 &
BENCH=$!

sleep 8 # preload + warmup
end=$((SECONDS + DURATION - 10))
while (( SECONDS < end )); do
  victim="n$((RANDOM % 3 + 1))"
  echo "[nemesis] SIGKILL $victim" >&2
  "$ROOT/scripts/cluster.sh" kill "$victim" >/dev/null
  sleep 4
  echo "[nemesis] restart $victim" >&2
  "$ROOT/scripts/cluster.sh" restart "$victim" >/dev/null
  sleep 8
done
wait "$BENCH"
