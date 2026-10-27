#!/usr/bin/env bash
# suite_watchdog3.sh <tmproot> <outdir>
#
# Captures the hung server AND proves it is a true hang. Polls the suite tmp
# tree for the LSan ptrace warning, parses the hung server's own pid out of
# "==<pid>==WARNING", and on that exact pid: (1) dumps per-thread kernel stacks
# of the process and its forked LSan tracer child plus a gdb backtrace, then
# (2) records a liveness timeline (is it still alive at +2/+5/+10/+20s, with a
# re-dump of the main thread at +8s) so a capture distinguishes a genuine
# multi-second exit hang from a warn-then-exit false positive.
set -u
tmproot=$1; outdir=$2
mkdir -p "$outdir"
seen="$outdir/.seen"; : > "$seen"
caps=0
stacks() {
    local p=$1
    for tt in /proc/$p/task/*; do
        [ -d "$tt" ] || continue
        echo "-- tid $(basename "$tt") comm=$(cat "$tt/comm" 2>/dev/null) state=$(awk '{print $3}' "$tt/stat" 2>/dev/null) --"
        timeout 5 sudo cat "$tt/stack" 2>/dev/null || echo "   (stack unavailable)"
    done
}
while true; do
    hits=$(grep -rlE "==[0-9]+==WARNING: ptrace appears to be blocked" "$tmproot" 2>/dev/null || true)
    for f in $hits; do
        grep -qxF "$f" "$seen" 2>/dev/null && continue
        echo "$f" >> "$seen"
        pid=$(grep -ohE "==[0-9]+==WARNING: ptrace appears" "$f" 2>/dev/null | grep -oE "[0-9]+" | head -1)
        [ -z "$pid" ] && continue
        [ -d "/proc/$pid" ] || { echo "WATCHDOG pid=$pid already gone"; continue; }
        caps=$((caps+1)); out="$outdir/CAPTURE.$caps.txt"
        {
            echo "== HUNG pid=$pid (from $f) $(date -u +%H:%M:%S) concurrent_valkey=$(pgrep -c -f valkey-server 2>/dev/null) load=$(cat /proc/loadavg) =="
            echo "### hung process threads (t0) ###"; echo "Threads: $(grep -i '^Threads' /proc/$pid/status 2>/dev/null)"; stacks "$pid"
            for ch in $(pgrep -P "$pid" 2>/dev/null); do echo "### child $ch of hung (candidate LSan stop-the-world tracer) ###"; stacks "$ch"; done
            echo "### gdb -p $pid (t0) ###"; timeout 30 sudo gdb -p "$pid" -batch -ex 'set pagination off' -ex 'info threads' -ex 'thread apply all bt' 2>&1 || echo "   (gdb attach failed)"
            echo "### liveness timeline ###"
            a2=na; a5=na; a10=na; a20=na
            sleep 2;  kill -0 "$pid" 2>/dev/null && a2=alive  || a2=gone
            echo "### main-thread re-dump at +~2s (same frame => stuck) ###"; [ -d /proc/$pid/task/$pid ] && timeout 5 sudo cat /proc/$pid/task/$pid/stack 2>/dev/null
            sleep 3;  kill -0 "$pid" 2>/dev/null && a5=alive  || a5=gone
            sleep 5;  kill -0 "$pid" 2>/dev/null && a10=alive || a10=gone
            sleep 10; kill -0 "$pid" 2>/dev/null && a20=alive || a20=gone
            echo "alive_at: +2s=$a2 +5s=$a5 +10s=$a10 +20s=$a20  (alive at +20s => genuine exit hang, not a warn-then-exit)"
            echo "### warning lines ###"; grep -h "ptrace appears to be blocked\|Child exited with signal" "$f" 2>/dev/null | head -4
        } > "$out" 2>&1
        echo "WATCHDOG_CAPTURED pid=$pid -> $out"
    done
    [ $caps -ge 4 ] && break
    sleep 0.5
done
