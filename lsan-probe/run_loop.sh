#!/usr/bin/env bash
# run_loop.sh <label> <iterations> <binary> [binary args...]
# Runs the binary N times under a timeout and classifies each run:
#   ok      exit 0, no sanitizer output
#   warn    stderr mentions the LSan ptrace warning (run still exited)
#   hang    killed by timeout (exit 124)
#   other   anything else
# Keeps the first 3 non-ok stderr captures under $OUT/<label>/.
set -u
label=$1; iters=$2; shift 2
OUT=${OUT:-probe-out}
mkdir -p "$OUT/$label"
ok=0; warn=0; hang=0; other=0; kept=0
for i in $(seq 1 "$iters"); do
    err="$OUT/$label/err.$i"
    timeout -s KILL 20 "$@" 2>"$err" >/dev/null
    rc=$?
    if [ $rc -eq 0 ] && [ ! -s "$err" ]; then
        ok=$((ok+1)); rm -f "$err"; continue
    fi
    if grep -q "ptrace appears to be blocked" "$err"; then
        if [ $rc -eq 137 ] || [ $rc -eq 124 ]; then hang=$((hang+1)); else warn=$((warn+1)); fi
    elif [ $rc -eq 137 ] || [ $rc -eq 124 ]; then
        hang=$((hang+1))
    else
        other=$((other+1))
    fi
    echo "rc=$rc" >>"$err"
    kept=$((kept+1))
    if [ $kept -gt 3 ]; then rm -f "$err"; fi
done
echo "RESULT label=$label iters=$iters ok=$ok warn=$warn hang=$hang other=$other"
for f in "$OUT/$label"/err.*; do
    [ -f "$f" ] || continue
    echo "----- $f"; head -c 4000 "$f"; echo
done
