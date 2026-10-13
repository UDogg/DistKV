#!/usr/bin/env bash
# Reproduce the headline benchmark: a fresh local 3-node cluster with 16
# shards, durable (fsync) storage, driven at 10,000 ops/s for 60s with an
# 80/20 read/write mix over 100k keys.
#
#   scripts/bench.sh [rate] [duration] [extra distkv-bench flags...]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RATE=${1:-10000}
DURATION=${2:-60s}
shift $(( $# > 2 ? 2 : $# )) || true

"$ROOT/scripts/cluster.sh" clean >/dev/null 2>&1 || true
"$ROOT/scripts/cluster.sh" start 3 16
trap '"$ROOT/scripts/cluster.sh" stop >/dev/null' EXIT
sleep 6 # let the leader balancer settle

echo "== environment: $(nproc) CPUs, $(uname -sr), $(go version | awk '{print $3}')"
"$ROOT/bin/distkv-bench" --endpoints http://127.0.0.1:7001 --rate "$RATE" --duration "$DURATION" \
  --keys 100000 --value-size 128 --read-ratio 0.8 --quiet "$@"
