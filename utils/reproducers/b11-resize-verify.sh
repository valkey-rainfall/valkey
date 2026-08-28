#!/bin/bash
# B11 runtime io-threads resize verifier (shell form).
#
# Exercises repeated runtime resizes (5->3->5->2->6) under owned pipelined GET
# traffic. Pre-fix (before 9dce806e): SIGSEGV during the first shrink (worker
# cancelled while owning client fds) and a lost-reply wedge for the CONFIG SET
# issuer. The in-suite regression is ownership.tcl "OWNERSHIP B11".
#
# Usage: b11-resize-verify.sh <port> <valkey-server-binary> [extra server args...]
set -u
PORT=$1; BIN=$2; shift 2
CLI="$(dirname "$BIN")/valkey-cli"
LOG=/tmp/b11-verify-server.log
rm -f $LOG
$BIN --port $PORT --io-threads 5 --io-threads-ownership yes \
    --save '' --appendonly no --logfile $LOG "$@" &
SRV=$!
sleep 1
R="$CLI -p $PORT"
$R set k v > /dev/null || { echo "FAIL: server not up"; exit 1; }
PIDS=()
for i in 1 2 3 4; do
  setsid bash -c "while true; do printf 'get k\r\n%.0s' {1..64} | $CLI -p $PORT --pipe > /dev/null 2>&1; done" &
  PIDS+=($!)
done
sleep 2
$R config set io-threads 3 > /dev/null; sleep 2; A1=$($R ping)
$R config set io-threads 5 > /dev/null; sleep 2; A2=$($R ping)
$R config set io-threads 2 > /dev/null; sleep 2; A3=$($R ping)
$R config set io-threads 6 > /dev/null; sleep 2; A4=$($R ping)
V=$($R get k)
for p in "${PIDS[@]}"; do kill -- -"$p" 2>/dev/null; done
$R shutdown nosave 2>/dev/null
sleep 1
echo "shrink53=$A1 grow35=$A2 shrink52=$A3 grow26=$A4 value=$V"
echo "=== crash check ==="
grep -ciE "sigsegv|crashed|assertion" $LOG
if [ "$A1" = PONG ] && [ "$A2" = PONG ] && [ "$A3" = PONG ] && [ "$A4" = PONG ] && [ "$V" = v ]; then
  echo B11-PASS
else
  echo B11-FAIL
fi
