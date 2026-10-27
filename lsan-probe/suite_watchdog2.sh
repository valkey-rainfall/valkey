#!/usr/bin/env bash
# suite_watchdog2.sh <tmproot> <outdir>
#
# Sharpened capture: poll the suite tmp tree for the LeakSanitizer ptrace
# warning, parse the hung server's OWN pid out of the "==<pid>==WARNING" line,
# and immediately snapshot THAT process first -- its per-thread kernel stacks
# (/proc/<pid>/task/*/stack, no ptrace needed, root), its forked LSan tracer
# child, and a gdb backtrace -- before the harness reaps it ~18s later. The
# previous watchdog swept ~50 servers and reached the hung one too late.
set -u
tmproot=$1; outdir=$2
mkdir -p "$outdir"
seen="$outdir/.seen"; : > "$seen"
caps=0
dumpproc() {
    local p=$1
    echo "== pid $p comm=$(cat /proc/$p/comm 2>/dev/null) $(grep -iE '^(Threads|State)' /proc/$p/status 2>/dev/null | tr '\n' ' ') =="
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
        caps=$((caps+1)); out="$outdir/CAPTURE.$caps.txt"
        {
            echo "== HUNG SERVER pid=$pid (from $f) $(date -u +%H:%M:%S) =="
            echo "== concurrent valkey-server count = $(pgrep -c -f valkey-server 2>/dev/null) =="
            if [ -d "/proc/$pid" ]; then
                echo "### hung process threads ###"; dumpproc "$pid"
                for c in $(pgrep -P "$pid" 2>/dev/null); do
                    echo "### child $c of hung process (candidate LSan tracer) ###"; dumpproc "$c"
                done
                echo "### gdb -p $pid (best effort; root bypasses yama) ###"
                timeout 30 sudo gdb -p "$pid" -batch -ex 'set pagination off' -ex 'info threads' -ex 'thread apply all bt' 2>&1 || echo "   (gdb attach failed)"
            else
                echo "   (pid $pid already gone before dump)"
            fi
            echo "== dmesg tail =="; timeout 5 sudo dmesg 2>/dev/null | tail -n 25
            echo "== warning file tail =="; tail -n 15 "$f" 2>/dev/null
        } > "$out" 2>&1
        echo "WATCHDOG_CAPTURED pid=$pid -> $out"
    done
    [ $caps -ge 4 ] && break
    sleep 0.5
done
