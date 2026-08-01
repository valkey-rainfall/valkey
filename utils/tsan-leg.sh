#!/bin/bash
# TSan leg for the D+/door-2 concurrent-read design (V3 of the verification
# track). Runs the GET+SET+expiry churn workload in both ownership modes
# under ThreadSanitizer with the scoped suppressions in src/tsan.sup.
# With suppressions active the expected report count is ZERO — any report
# this leg produces is actionable signal.
#
# Prereqs: make SANITIZER=thread valkey-server valkey-cli valkey-benchmark
# On hosts missing the libtsan soname symlink (e.g. AL2023):
#   mkdir -p /tmp/tsan-lib && ln -sf /usr/lib/gcc/*/*/libtsan.so.0.0.0 /tmp/tsan-lib/libtsan.so.0
#   export LD_LIBRARY_PATH=/tmp/tsan-lib
set -u
cd "$(dirname "$0")/.."
PORT=${PORT:-7399}
OUT=${OUT:-/tmp/tsan-leg-reports}
mkdir -p "$OUT"
rc=0
for mode in yes no; do
  tag=$([ "$mode" = yes ] && echo on || echo off)
  export TSAN_OPTIONS="suppressions=$PWD/src/tsan.sup log_path=$OUT/srv-$tag history_size=7"
  ./src/valkey-server --port "$PORT" --save "" --protected-mode no --daemonize no \
    --logfile "$OUT/server-$tag.log" --io-threads 4 --io-threads-ownership "$mode" &
  SRVPID=$!
  sleep 8
  ./src/valkey-cli -p "$PORT" ping > /dev/null || { echo "BOOT FAIL $tag"; kill "$SRVPID"; rc=1; continue; }
  ./src/valkey-benchmark -p "$PORT" -t set -n 200000 -r 100000 -P 10 -c 8 -q > /dev/null 2>&1 &
  ./src/valkey-benchmark -p "$PORT" -t get -n 400000 -r 100000 -P 10 -c 16 -q > /dev/null 2>&1 &
  for _ in $(seq 1 30); do
    ./src/valkey-cli -p "$PORT" eval \
      "for i=1,200 do redis.call('set','exp:'..math.random(10000),'v','PX',math.random(5,15)) end return 1" 0 > /dev/null
    sleep 1
  done
  wait %2 %3 2>/dev/null
  ./src/valkey-cli -p "$PORT" shutdown nosave 2>/dev/null
  wait "$SRVPID" 2>/dev/null
  n=$(cat "$OUT"/srv-$tag.* 2>/dev/null | grep -c "WARNING: ThreadSanitizer" || true)
  echo "mode=$tag tsan_reports=${n:-0}"
  [ "${n:-0}" -eq 0 ] || rc=1
done
exit $rc
