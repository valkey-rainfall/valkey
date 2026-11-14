#!/usr/bin/env bash
# Start / stop / inspect one D3 server (owner threads) for a bench cell, as the server user.
#
#   d3-launch.sh start  --bin <tree>/src/valkey-server --w W [--cpu0 0] [--bg-cpu 63]
#                       [--bind "IP 127.0.0.1"] [--announce-ip IP] [--port 6379] [--owner-port 6400]
#                       [--dir /tmp/vkevo-d3]
#   d3-launch.sh status [--port 6379]     # INFO owners (native commands, handoffs, world stops, per owner)
#   d3-launch.sh stop   [--port 6379]
#
# Equal-W layout: W owner threads, owner i pinned to CPU cpu0+i and listening on owner-port+i.
# Main (the fallback path, crons, port 6379) shares CPU cpu0 with owner 0, so the server uses exactly the
# W worker CPUs cpu0..cpu0+W-1. io-threads 1 (owner threads require it). Persistence off and
# protected-mode off as in round 1; the bind list is the caller's (private IP + loopback).
# Background work runs on --bg-cpu, outside the owners: the bio thread (AOF fsync, closing and unlinking old
# AOF files, lazy free) and the fork children (AOF rewrite, RDB save). By default 63, the bench host's OS core,
# where D0's bench server also runs its bio thread and both children. On cpu0 a rewrite child took owner 0's CPU
# for the whole rewrite, and every world stop waited on owner 0.
set -euo pipefail
cmd=${1:-}; shift || true
BIN=""; W=0; CPU0=0; BG=63; BIND="127.0.0.1"; ANNOUNCE=""; PORT=6379; OPORT=6400; DIR=/tmp/vkevo-d3
while [ $# -gt 0 ]; do
  case $1 in
    --bin) BIN=$2; shift 2 ;;
    --w) W=$2; shift 2 ;;
    --cpu0) CPU0=$2; shift 2 ;;
    --bg-cpu) BG=$2; shift 2 ;;
    --bind) BIND=$2; shift 2 ;;
    --announce-ip) ANNOUNCE=$2; shift 2 ;;
    --port) PORT=$2; shift 2 ;;
    --owner-port) OPORT=$2; shift 2 ;;
    --dir) DIR=$2; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done
cli() { "$(dirname "${BIN:-$(command -v valkey-server)}")/valkey-cli" -p "$PORT" "$@"; }
case $cmd in
start)
  [ -x "$BIN" ] && [ "$W" -gt 0 ] || { echo "--bin and --w required" >&2; exit 2; }
  mkdir -p "$DIR"
  last=$((CPU0 + W - 1))
  if [ "$BG" -ge "$CPU0" ] && [ "$BG" -le "$last" ]; then
    echo "--bg-cpu $BG is an owner CPU ($CPU0-$last)" >&2; exit 2
  fi
  # shellcheck disable=SC2086
  "$BIN" --port "$PORT" --bind $BIND --protected-mode no --save '' --appendonly no \
    --io-threads 1 --owner-threads "$W" --owner-port "$OPORT" --owner-cpulist "$CPU0-$last" \
    --server-cpulist "$CPU0" --bio-cpulist "$BG" --aof-rewrite-cpulist "$BG" --bgsave-cpulist "$BG" \
    ${ANNOUNCE:+--owner-announce-ip "$ANNOUNCE"} --maxclients 100000 \
    --daemonize yes --dir "$DIR" --logfile "$DIR/valkey.log" --pidfile "$DIR/valkey.pid"
  for _ in $(seq 50); do cli ping > /dev/null 2>&1 && break; sleep 0.1; done
  echo "D3 up: port $PORT, owners $W on CPUs $CPU0-$last, owner ports $OPORT-$((OPORT + W - 1)), bio + fork children on CPU $BG, pid $(cat "$DIR/valkey.pid")"
  ;;
status)
  cli info owners | tr -d '\r'
  ;;
stop)
  cli shutdown nosave > /dev/null 2>&1 || true
  echo "D3 stopped (port $PORT)"
  ;;
*) sed -n '2,17p' "$0"; exit 2 ;;
esac
