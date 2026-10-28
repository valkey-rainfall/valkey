#!/usr/bin/env bash
# suite_watchdog5.sh <tmproot> <outdir> <runtest.out>
#
# Adds a second detector: every pid the harness reports as "Waiting for
# process N to exit" (printed after 1s of waiting) is dumped once, warning or
# not, with its ps state and per-thread kernel stacks (SLOW.<n>.txt). This
# catches an exit stall that has no LSan warning, and a stopped process that
# was not resumed (state T).
#
# Capture + fork-deadlock probe. Like suite_watchdog3.sh it finds the hung
# server by parsing its pid out of the LSan "==<pid>==WARNING: ptrace appears
# to be blocked" line and dumps its per-thread kernel stacks, its forked LSan
# tracer child and /proc/<pid>/maps (needed to symbolize raw PCs of a PIE).
#
# New: once the hang is confirmed (still alive at +5s) the watchdog sends the
# SIGSEGV itself, the way the test harness's "Forcing process to crash" does,
# and then watches what the crash handler manages to do:
#   - a NEW child of the hung pid + the handling thread in do_wait/nanosleep
#       => fork() returned; the symbolizer child is what is stuck (PR 4306
#          then prints raw frames after its 500ms timeout, PR 4993 may let
#          the child finish)
#   - no new child, the handling thread moved from sched_yield to futex_wait
#       => fork() itself deadlocked in ASan's ForkBefore() on the LSan locks
#          the hung thread already holds; neither PR can print anything
# It records thread stacks at +1/+3/+6s after the signal and tails the server
# log (dirname of the warning file), where the crash report lands.
set -u
tmproot=$1; outdir=$2; rout=${3:-/dev/null}
mkdir -p "$outdir"
seen="$outdir/.seen"; : > "$seen"
seenslow="$outdir/.seenslow"; : > "$seenslow"
caps=0; slows=0
stacks() {
    local p=$1
    for tt in /proc/$p/task/*; do
        [ -d "$tt" ] || continue
        echo "-- tid $(basename "$tt") comm=$(cat "$tt/comm" 2>/dev/null) state=$(awk '{print $3}' "$tt/stat" 2>/dev/null) wchan=$(cat "$tt/wchan" 2>/dev/null) syscall=$(timeout 2 sudo cat "$tt/syscall" 2>/dev/null | cut -d' ' -f1) --"
        timeout 5 sudo cat "$tt/stack" 2>/dev/null || echo "   (stack unavailable)"
    done
}
children() {
    local p=$1 label=$2
    for ch in $(pgrep -P "$p" 2>/dev/null); do
        echo "### child $ch of hung ($label) starttime=$(awk '{print $22}' /proc/$ch/stat 2>/dev/null) ###"
        stacks "$ch"
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
        sdir=$(dirname "$f")
        {
            echo "== HUNG pid=$pid (from $f) $(date -u +%H:%M:%S) concurrent_valkey=$(pgrep -c -f valkey-server 2>/dev/null) load=$(cat /proc/loadavg) =="
            echo "### hung process threads (t0) ###"; echo "Threads: $(grep -i '^Threads' /proc/$pid/status 2>/dev/null)"; stacks "$pid"
            children "$pid" "t0, candidate LSan stop-the-world tracer"
            echo "### /proc/$pid/maps (for offline symbolization of raw PCs) ###"
            sudo cat /proc/$pid/maps 2>/dev/null > "$outdir/maps.$caps.txt" && echo "saved $(wc -l < "$outdir/maps.$caps.txt") lines -> maps.$caps.txt"
            grep -E "valkey-server|libasan|libc\.so|libbacktrace" "$outdir/maps.$caps.txt" | grep " r-xp " | head -8
            pre_children=$(pgrep -P "$pid" 2>/dev/null | tr '\n' ' ')
            echo "### liveness before probe ###"
            sleep 2; kill -0 "$pid" 2>/dev/null && a2=alive || a2=gone
            sleep 3; kill -0 "$pid" 2>/dev/null && a5=alive || a5=gone
            echo "alive_at: +2s=$a2 +5s=$a5"
            if [ "$a5" != alive ]; then
                echo "PROBE=skipped (process exited before +5s: warn-then-exit, not a hang)"
            else
                echo "### main-thread re-dump at +5s (same frame as t0 => stuck) ###"
                [ -d /proc/$pid/task/$pid ] && timeout 5 sudo cat /proc/$pid/task/$pid/stack 2>/dev/null
                echo "### PROBE: sending SIGSEGV to $pid at $(date -u +%H:%M:%S.%N | cut -c1-12) (pre-existing children: ${pre_children:-none}) ###"
                kill -SEGV "$pid" 2>/dev/null && echo "PROBE=sent" || echo "PROBE=kill-failed"
                for step in 1:1 3:2 6:3; do
                    dt=${step%%:*}; sleep "${step##*:}"
                    if kill -0 "$pid" 2>/dev/null; then
                        echo "### +${dt}s after SIGSEGV: pid alive; threads ###"; stacks "$pid"
                        echo "### +${dt}s after SIGSEGV: children (NEW pid not in [${pre_children:-none}] = crash-handler symbolizer fork) ###"
                        children "$pid" "+${dt}s"
                    else
                        echo "### +${dt}s after SIGSEGV: pid GONE (crash handler completed and re-raised, or was killed) ###"
                    fi
                done
                sleep 4; kill -0 "$pid" 2>/dev/null && echo "alive_at: +10s after SIGSEGV=alive" || echo "alive_at: +10s after SIGSEGV=gone"
                echo "### server log tail after probe ($sdir/stdout) ###"
                tail -n 60 "$sdir/stdout" 2>/dev/null | grep -v "^$" | tail -n 45
                echo "### server stderr tail ($sdir/stderr) ###"
                tail -n 8 "$sdir/stderr" 2>/dev/null
            fi
            echo "$sdir" > "$outdir/serverdir.$caps.txt"
            echo "### warning lines ###"; grep -h "ptrace appears to be blocked\|Child exited with signal" "$f" 2>/dev/null | head -4
        } > "$out" 2>&1
        echo "WATCHDOG_CAPTURED pid=$pid -> $out"
    done
    for spid in $(grep -oE "Waiting for process [0-9]+ to exit" "$rout" 2>/dev/null | grep -oE "[0-9]+" | sort -u); do
        grep -qxF "$spid" "$seenslow" 2>/dev/null && continue
        echo "$spid" >> "$seenslow"
        [ -d "/proc/$spid" ] || { echo "WATCHDOG slow pid=$spid already gone"; continue; }
        slows=$((slows+1)); sout="$outdir/SLOW.$slows.txt"
        {
            echo "== SLOW EXIT pid=$spid $(date -u +%H:%M:%S) ps_state=$(ps -o stat= -p "$spid" 2>/dev/null) concurrent_valkey=$(pgrep -c -f valkey-server 2>/dev/null) load=$(cat /proc/loadavg) =="
            echo "cmdline: $(tr '\0' ' ' < /proc/$spid/cmdline 2>/dev/null)"
            echo "SigPnd/ShdPnd/SigBlk: $(grep -E '^(SigPnd|ShdPnd|SigBlk)' /proc/$spid/status 2>/dev/null | tr '\n' ' ')"
            echo "### threads ###"; stacks "$spid"
            children "$spid" "slow-exit"
            sleep 3; kill -0 "$spid" 2>/dev/null && echo "alive_at +3s: alive ps_state=$(ps -o stat= -p "$spid" 2>/dev/null)" || echo "alive_at +3s: gone"
            echo "### server log tail ###"
            for d in "$tmproot"/server*; do [ -f "$d/stdout" ] && grep -q "^$spid:" "$d/stdout" 2>/dev/null && { tail -n 12 "$d/stdout"; echo "stderr: $(tail -n 3 "$d/stderr" 2>/dev/null)"; break; }; done
        } > "$sout" 2>&1
        echo "WATCHDOG_SLOW pid=$spid -> $sout"
    done
    [ $caps -ge 4 ] && break
    sleep 0.5
done
