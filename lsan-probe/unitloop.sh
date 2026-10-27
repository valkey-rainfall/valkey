#!/usr/bin/env bash
# unitloop.sh <unit> <iters> <label> <tmp>
# Runs a single test unit under the ASan build <iters> times and classifies
# each run. A sanitizer hang aborts the unit and prints the LSan ptrace
# warning, so it is detectable per run:
#   pass            : runtest returned 0
#   sanitizer_hang  : runtest failed AND the log carries the LSan ptrace/hang signature
#   other_fail      : runtest failed for some other reason (unrelated flake)
set -u
unit=$1; iters=$2; label=$3; tmp=$4
mkdir -p "$tmp/$label"
pass=0; san=0; other=0; kept=0
for i in $(seq 1 "$iters"); do
    log="$tmp/$label/rt.$i.log"
    if ./runtest --single "$unit" --dump-logs >"$log" 2>&1; then
        pass=$((pass+1)); rm -f "$log"
    elif grep -q "ptrace appears to be blocked\|Sanitizer error\|LeakSanitizer may hang" "$log"; then
        san=$((san+1))
        if [ $kept -lt 3 ]; then kept=$((kept+1)); else rm -f "$log"; fi
    else
        other=$((other+1)); rm -f "$log"
    fi
done
echo "RESULT unit=$unit label=$label iters=$iters pass=$pass sanitizer_hang=$san other_fail=$other"
for f in "$tmp/$label"/rt.*.log; do
    [ -f "$f" ] || continue
    echo "----- $f (tail)"; tail -n 35 "$f"; echo
done
