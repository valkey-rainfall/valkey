#!/usr/bin/env bash
# suite_watchdog.sh <tmproot> <outdir>
#
# Runs alongside the full test suite. Polls the suite's tmp tree for the
# LeakSanitizer ptrace warning ("ptrace appears to be blocked"), which the
# hung server prints to its stderr as the stop-the-world stall begins. The
# server then stays alive (hung in exit) until the harness SIGKILLs it ~18s
# later, so there is a wide window to snapshot it. On each new occurrence,
# capture kernel stacks + a gdb backtrace of every live valkey-server and its
# children, plus dmesg. Caps at 4 captures, then exits.
set -u
tmproot=$1; outdir=$2
mkdir -p "$outdir"
seen="$outdir/.seen"; : > "$seen"
caps=0
while true; do
    hits=$(grep -rl "ptrace appears to be blocked" "$tmproot" 2>/dev/null || true)
    if [ -n "$hits" ]; then
        for f in $hits; do
            grep -qxF "$f" "$seen" 2>/dev/null && continue
            echo "$f" >> "$seen"
            caps=$((caps+1)); out="$outdir/CAPTURE.$caps.txt"
            {
                echo "== warning file: $f =="; tail -n 25 "$f" 2>/dev/null
                echo "== live valkey-server processes =="; pgrep -a -f valkey-server 2>/dev/null
                for pid in $(pgrep -f valkey-server 2>/dev/null); do
                    echo "== pid $pid comm=$(cat /proc/$pid/comm 2>/dev/null) $(grep -i '^Threads' /proc/$pid/status 2>/dev/null) =="
                    for tt in /proc/$pid/task/*; do
                        [ -d "$tt" ] || continue
                        echo "-- tid $(basename "$tt") comm=$(cat "$tt/comm" 2>/dev/null) --"
                        timeout 5 sudo cat "$tt/stack" 2>/dev/null || echo "   (stack unavailable)"
                    done
                    for c in $(pgrep -P "$pid" 2>/dev/null); do
                        echo "== child $c comm=$(cat /proc/$c/comm 2>/dev/null) =="
                        for tt in /proc/$c/task/*; do
                            [ -d "$tt" ] || continue
                            echo "-- tid $(basename "$tt") --"; timeout 5 sudo cat "$tt/stack" 2>/dev/null
                        done
                    done
                    echo "== gdb pid $pid (best effort) =="
                    timeout 25 sudo gdb -p "$pid" -batch -ex 'set pagination off' -ex 'info threads' -ex 'thread apply all bt' 2>&1 || echo "   (gdb attach failed)"
                done
                echo "== dmesg tail =="; timeout 5 sudo dmesg 2>/dev/null | tail -n 25
            } > "$out" 2>&1
            echo "WATCHDOG_CAPTURED $out"
        done
    fi
    [ $caps -ge 4 ] && break
    sleep 1
done
