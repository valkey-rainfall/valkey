#!/bin/bash
# B12 parser-desync soak (authoritative hammer).
#
# Reproduces the ownership read-path parser desync (lost chunk): a ~1MB
# multibulk (100K-arg SADD + UNLINK) streamed repeatedly via valkey-cli --pipe
# while 8 pipelined noise loops keep workers and main contended.
#
# History: pre-fix rate ~0.5%/iteration (fix 1: 3028b455 trim-before-release;
# fix 2: 90a7ec21 no-bottom-trim-for-owned). Post-fix: 0/12,000 iterations.
# The in-suite tripwire is ownership.tcl "OWNERSHIP B12"; this script is the
# statistical hammer for revalidation after read-path changes.
#
# Usage: b12-crlf-soak.sh <port> <label> <valkey-server-binary> <iterations> [extra server args...]
# Example: b12-crlf-soak.sh 22010 soak ./src/valkey-server 4000 --io-threads 5 --io-threads-ownership yes
set -u
PORT=$1; LABEL=$2; BIN=$3; N=$4; shift 4
CLI="$(dirname "$BIN")/valkey-cli"
LOG=/tmp/b12-$LABEL-server.log
OUT=/tmp/b12-$LABEL
PAYLOAD=/tmp/b12-payload.resp
rm -rf $OUT; mkdir -p $OUT; rm -f $LOG

python3 - << 'EOF'
n = 100000
parts = [f"*{n+2}\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n"]
for i in range(n):
    s = str(i)
    parts.append(f"${len(s)}\r\n{s}\r\n")
parts.append("*2\r\n$6\r\nUNLINK\r\n$5\r\nmyset\r\n")
open('/tmp/b12-payload.resp','w').write(''.join(parts))
EOF

$BIN --port $PORT --save '' --appendonly no --logfile $LOG "$@" &
SRV=$!
sleep 1
$CLI -p $PORT ping > /dev/null || { echo "$LABEL FAIL: not up"; exit 1; }
$CLI -p $PORT set noise:key noise:val > /dev/null
NOISE=()
for i in 1 2 3 4 5 6 7 8; do
  setsid bash -c "while true; do { printf 'set noise:$i %d\r\n' \$RANDOM; printf 'get noise:key\r\n%.0s' {1..32}; } | $CLI -p $PORT --pipe > /dev/null 2>&1; done" &
  NOISE+=($!)
done
ERRS=0
for it in $(seq 1 $N); do
  $CLI -p $PORT --pipe < $PAYLOAD > $OUT/it$it.log 2>&1
  if grep -qE 'Protocol error' $OUT/it$it.log; then
    ERRS=$((ERRS+1))
    grep -E 'Protocol error' $OUT/it$it.log | head -1 | sed "s/^/[$LABEL it$it] /"
  fi
done
for p in "${NOISE[@]}"; do kill -- -"$p" 2>/dev/null; done
kill -9 $SRV 2>/dev/null
echo "$LABEL: iterations=$N errors=$ERRS"
echo "server log: $LOG (build with -DIO_LOOKUP_OFFLOAD_STATS for D+ proto-desync forensics)"
