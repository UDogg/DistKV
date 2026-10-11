#!/usr/bin/env bash
# Run a local multi-process DistKV cluster.
#
#   scripts/cluster.sh start [nodes=3] [shards=16]   # build + start
#   scripts/cluster.sh stop                          # graceful stop (SIGTERM)
#   scripts/cluster.sh kill <id>                     # crash one node (SIGKILL)
#   scripts/cluster.sh restart <id>                  # restart a stopped node
#   scripts/cluster.sh status                        # shard/leader table
#   scripts/cluster.sh clean                         # stop and wipe data
#
# Environment: BASE_PORT (default 7001), CLUSTER_DIR (default ./.cluster),
# EXTRA_FLAGS (passed to every node, e.g. "--no-sync"), TLS=1 (generate a dev
# CA and run with mutual TLS between nodes).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${CLUSTER_DIR:-$ROOT/.cluster}"
BASE_PORT="${BASE_PORT:-7001}"
BIN="$ROOT/bin"

SCHEME=http
[[ "${TLS:-}" == 1 ]] && SCHEME=https

members() {
  local n=$1 out=""
  for i in $(seq 1 "$n"); do
    out+="n$i=$SCHEME://127.0.0.1:$((BASE_PORT + i - 1)),"
  done
  echo "${out%,}"
}

tls_flags() {
  [[ -f "$DIR/certs/ca.pem" ]] || return 0
  echo "--tls-cert $DIR/certs/node.pem --tls-key $DIR/certs/node-key.pem --tls-ca $DIR/certs/ca.pem"
}

ctl() {
  local scheme=http extra=()
  if [[ -f "$DIR/certs/ca.pem" ]]; then
    scheme=https
    extra=(--ca "$DIR/certs/ca.pem")
  fi
  "$BIN/distkvctl" --endpoints "$scheme://127.0.0.1:$BASE_PORT" ${extra[@]+"${extra[@]}"} "$@"
}

start_node() {
  local id=$1
  local cfg="$DIR/cluster.env"
  # shellcheck disable=SC1090
  source "$cfg"
  mkdir -p "$DIR/$id"
  nohup "$BIN/distkv" --id "$id" --members "$MEMBERS" --shards "$SHARDS" \
    --data-dir "$DIR/$id/data" $(tls_flags) ${EXTRA_FLAGS:-} >"$DIR/$id/node.log" 2>&1 &
  echo $! >"$DIR/$id/pid"
  echo "started $id (pid $!, log $DIR/$id/node.log)"
}

case "${1:-}" in
start)
  NODES=${2:-3}
  SHARDS=${3:-16}
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
  mkdir -p "$DIR"
  if [[ "${TLS:-}" == 1 ]]; then
    "$BIN/distkvctl" dev-certs "$DIR/certs" >/dev/null
  fi
  printf 'MEMBERS=%q\nSHARDS=%q\nNODES=%q\n' "$(members "$NODES")" "$SHARDS" "$NODES" >"$DIR/cluster.env"
  for i in $(seq 1 "$NODES"); do start_node "n$i"; done
  echo -n "waiting for cluster to become ready"
  for _ in $(seq 1 100); do
    ok=1
    for i in $(seq 1 "$NODES"); do
      BASE_PORT=$((BASE_PORT + i - 1)) ctl health >/dev/null 2>&1 || ok=0
    done
    [[ $ok == 1 ]] && { echo " ok"; exit 0; }
    echo -n "."
    sleep 0.2
  done
  echo " timed out"; exit 1
  ;;
stop)
  for p in "$DIR"/*/pid; do
    [[ -f $p ]] || continue
    kill -TERM "$(cat "$p")" 2>/dev/null || true
    rm -f "$p"
  done
  wait 2>/dev/null || true
  echo "stopped"
  ;;
kill)
  kill -KILL "$(cat "$DIR/$2/pid")" && rm -f "$DIR/$2/pid" && echo "killed $2"
  ;;
restart)
  start_node "$2"
  ;;
status)
  ctl cluster
  ;;
clean)
  "$0" stop || true
  rm -rf "$DIR"
  echo "wiped $DIR"
  ;;
*)
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
  ;;
esac
