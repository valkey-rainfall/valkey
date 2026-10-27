#!/usr/bin/env bash
# reproloop.sh <label> <iters> <server-bin> <cli-bin> <workbase>
#
# Reproduces the field failure directly: start the ASan-built valkey-server,
# issue SHUTDOWN NOSAVE (which runs exit() -> LeakSanitizer leak scan ->
# stop-the-world ptrace), and see whether the process actually exits.
# Classifies each iteration:
#   ok    : exited cleanly within the deadline, no sanitizer stderr
#   warn  : stderr has "ptrace appears to be blocked" but the process still left
#   hang  : did not exit within the deadline (the field failure: stuck in exit())
#   other : non-zero exit or sanitizer stderr without the ptrace warning
set -u
label=$1; iters=$2; bin=$3; cli=$4; base=$5
port=7700
deadline_ticks=150   # 150 * 0.1s = 15s, matches the field harness kill window
ok=0; warn=0; hang=0; other=0; kept=0
mkdir -p "$base/$label"
for i in $(seq 1 "$iters"); do
    err="$base/$label/err.$i"
    dir="$base/$label/d.$i"; mkdir -p "$dir"
    ASAN_OPTIONS=detect_leaks=1 "$bin" --port $port --dir "$dir" \
        --save '' --appendonly no --logfile '' >"$dir/out" 2>"$err" &
    pid=$!
    up=0
    for t in $(seq 1 50); do "$cli" -p $port ping >/dev/null 2>&1 && { up=1; break; }; sleep 0.1; done
    if [ $up -eq 0 ]; then
        kill -KILL $pid 2>/dev/null; wait $pid 2>/dev/null
        other=$((other+1)); cls=other
    else
        "$cli" -p $port shutdown nosave >/dev/null 2>&1
        gone=0
        for t in $(seq 1 $deadline_ticks); do kill -0 $pid 2>/dev/null || { gone=1; break; }; sleep 0.1; done
        if [ $gone -eq 0 ]; then
            kill -KILL $pid 2>/dev/null; wait $pid 2>/dev/null
            hang=$((hang+1)); cls=hang
        else
            wait $pid 2>/dev/null; rc=$?
            if grep -q "ptrace appears to be blocked" "$err" 2>/dev/null; then warn=$((warn+1)); cls=warn
            elif [ -s "$err" ]; then other=$((other+1)); cls=other
            elif [ $rc -ne 0 ]; then other=$((other+1)); cls=other
            else ok=$((ok+1)); cls=ok; fi
        fi
    fi
    rm -rf "$dir"
    if [ "$cls" = ok ]; then rm -f "$err"
    elif [ $kept -lt 4 ]; then kept=$((kept+1))
    else rm -f "$err"; fi
done
echo "RESULT label=$label iters=$iters ok=$ok warn=$warn hang=$hang other=$other"
for f in "$base/$label"/err.*; do
    [ -f "$f" ] || continue
    echo "----- $f"; head -c 3000 "$f"; echo
done
