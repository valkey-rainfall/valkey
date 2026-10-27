#!/usr/bin/env bash
# exotic_repro.sh <label> <iters> <server> <cli> <base>
#
# Targeted reproduction of the LeakSanitizer exit-time hang. Each iteration
# puts the primary into the "exotic exit state" the field failures shared:
#   - many io-threads (more threads for LSan stop-the-world to attach)
#   - a replica attached mid full-sync (so a bgsave fork child is live at exit)
#   - several blocked clients
#   - a loaded dataset so the sync has heft
# then SHUTDOWN NOSAVE. If the primary logs "ready to exit" and is still alive
# after a grace window, capture the stuck state (per-thread kernel stacks of
# the process and its descendants, a best-effort gdb backtrace, and dmesg)
# BEFORE killing it. Classify: ok / warn (ptrace warning printed) / hang / other.
set -u
label=$1; iters=$2; bin=$3; cli=$4; base=$5
pport=7800; rport=7801
ok=0; warn=0; hang=0; other=0; caps=0
mkdir -p "$base/$label"

capture() {
    local pid=$1 iter=$2 out=$3
    {
        echo "== label=$label iter=$iter primary_pid=$pid $(date -u +%H:%M:%S) =="
        echo "== /proc/$pid/status (threads) =="; grep -iE 'Threads|State' "/proc/$pid/status" 2>/dev/null
        echo "== process tree around primary =="; ps -eLf 2>/dev/null | grep -E "valkey|lsan|$pid" | grep -v grep
        local pids="$pid"
        for c in $(pgrep -P "$pid" 2>/dev/null); do pids="$pids $c"; for g in $(pgrep -P "$c" 2>/dev/null); do pids="$pids $g"; done; done
        for p in $pids; do
            echo "== pid $p ($(cat /proc/$p/comm 2>/dev/null)) per-thread kernel stacks =="
            for tt in /proc/$p/task/*; do
                [ -d "$tt" ] || continue
                echo "-- tid $(basename "$tt") comm=$(cat "$tt/comm" 2>/dev/null) =="
                timeout 5 sudo cat "$tt/stack" 2>/dev/null || echo "   (stack unavailable)"
            done
        done
        echo "== gdb all-thread backtrace (best effort) =="
        timeout 30 sudo gdb -p "$pid" -batch -ex 'set pagination off' -ex 'info threads' -ex 'thread apply all bt' 2>&1 || echo "   (gdb attach failed)"
        echo "== dmesg tail =="; timeout 5 sudo dmesg 2>/dev/null | tail -n 30
        echo "== server log tail =="; tail -n 25 "$base/$label/d.$iter/p.log" 2>/dev/null
        echo "== server stderr tail =="; tail -n 25 "$base/$label/d.$iter/p.err" 2>/dev/null
    } > "$out" 2>&1
}

for i in $(seq 1 "$iters"); do
    d="$base/$label/d.$i"; mkdir -p "$d/p" "$d/r"
    plog="$d/p.log"; perr="$d/p.err"
    ASAN_OPTIONS=detect_leaks=1 "$bin" --port $pport --dir "$d/p" --io-threads 9 \
        --save '' --appendonly no --logfile "$plog" --daemonize no 2>"$perr" &
    ppid=$!
    up=0; for t in $(seq 1 60); do "$cli" -p $pport ping >/dev/null 2>&1 && { up=1; break; }; sleep 0.1; done
    if [ $up -eq 0 ]; then sudo kill -KILL $ppid 2>/dev/null; wait $ppid 2>/dev/null; other=$((other+1)); rm -rf "$d"; continue; fi
    "$cli" -p $pport eval "for j=1,100000 do redis.call('set','k'..j,j) end" 0 >/dev/null 2>&1
    ASAN_OPTIONS=detect_leaks=1 "$bin" --port $rport --dir "$d/r" --io-threads 3 \
        --save '' --appendonly no --logfile "$d/r.log" --daemonize no \
        --replicaof 127.0.0.1 $pport 2>"$d/r.err" &
    rpid=$!
    for c in 1 2 3 4 5; do ( "$cli" -p $pport blpop nokey:$c 30 >/dev/null 2>&1 & ) ; done
    sleep 0.3
    "$cli" -p $pport shutdown nosave >/dev/null 2>&1
    gone=0; for t in $(seq 1 40); do kill -0 $ppid 2>/dev/null || { gone=1; break; }; sleep 0.1; done
    if [ $gone -eq 0 ] && grep -q "ready to exit" "$plog" 2>/dev/null && [ $caps -lt 3 ]; then
        caps=$((caps+1)); capture "$ppid" "$i" "$base/$label/CAPTURE.$i.txt"
    fi
    for t in $(seq 1 110); do kill -0 $ppid 2>/dev/null || { gone=1; break; }; sleep 0.1; done
    if [ $gone -eq 0 ]; then
        sudo kill -KILL $ppid 2>/dev/null; wait $ppid 2>/dev/null; hang=$((hang+1))
    else
        wait $ppid 2>/dev/null; rc=$?
        if grep -q "ptrace appears to be blocked" "$perr" 2>/dev/null; then warn=$((warn+1))
        elif [ $rc -ne 0 ]; then other=$((other+1))
        else ok=$((ok+1)); fi
    fi
    kill -KILL $rpid 2>/dev/null; wait $rpid 2>/dev/null
    pkill -f "blpop noke[y]" 2>/dev/null
    rm -rf "$d"
done
echo "RESULT label=$label iters=$iters ok=$ok warn=$warn hang=$hang other=$other captures=$caps"
for f in "$base/$label"/CAPTURE.*.txt; do
    [ -f "$f" ] || continue
    echo "########## $f ##########"; cat "$f"; echo
done
