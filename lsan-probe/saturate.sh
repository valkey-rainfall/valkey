#!/usr/bin/env bash
# saturate.sh <label> <cycles> <nservers> <server> <cli> <base>
#
# Forces the suspected trigger directly: start N ASan valkey-servers at once,
# load a little data in each, pin both cores with stress-ng, then SHUTDOWN
# NOSAVE all N near-simultaneously so many LSan exit-time leak scans run under
# heavy CPU + concurrent-fork pressure. Capture is gated on the real hang
# signature (the "ptrace appears to be blocked" warning in a server's stderr):
# when it appears and that server is still alive, snapshot its own threads, its
# forked tracer child, and gdb, keyed on the exact pid.
set -u
label=$1; cycles=$2; N=$3; bin=$4; cli=$5; base=$6
hang=0; caps=0
mkdir -p "$base/$label"
dumpproc() {
    local p=$1
    echo "== pid $p comm=$(cat /proc/$p/comm 2>/dev/null) $(grep -iE '^(Threads|State)' /proc/$p/status 2>/dev/null | tr '\n' ' ') =="
    for tt in /proc/$p/task/*; do
        [ -d "$tt" ] || continue
        echo "-- tid $(basename "$tt") comm=$(cat "$tt/comm" 2>/dev/null) state=$(awk '{print $3}' "$tt/stat" 2>/dev/null) --"
        timeout 5 sudo cat "$tt/stack" 2>/dev/null || echo "   (stack unavailable)"
    done
}
for c in $(seq 1 "$cycles"); do
    d="$base/$label/c.$c"; mkdir -p "$d"
    declare -A PID; bp=20000
    for i in $(seq 1 "$N"); do
        port=$((bp+i)); dir="$d/s$i"; mkdir -p "$dir"
        ASAN_OPTIONS=detect_leaks=1 "$bin" --port $port --dir "$dir" --io-threads 3 \
            --save '' --appendonly no --logfile "$dir/log" 2>"$dir/err" &
        PID[$i]=$!
    done
    for i in $(seq 1 "$N"); do
        port=$((bp+i))
        for t in $(seq 1 60); do "$cli" -p $port ping >/dev/null 2>&1 && break; sleep 0.05; done
        "$cli" -p $port eval "for j=1,20000 do redis.call('set','k'..j,j) end" 0 >/dev/null 2>&1
    done
    stress-ng --cpu 4 --cpu-load 90 --timeout 30s >/dev/null 2>&1 &
    stpid=$!
    sleep 1
    for i in $(seq 1 "$N"); do port=$((bp+i)); ( "$cli" -p $port shutdown nosave >/dev/null 2>&1 & ); done
    declare -A CAP
    for w in $(seq 1 30); do
        for i in $(seq 1 "$N"); do
            [ -n "${CAP[$i]:-}" ] && continue
            p=${PID[$i]}
            if grep -q "ptrace appears to be blocked" "$d/s$i/err" 2>/dev/null && kill -0 "$p" 2>/dev/null; then
                CAP[$i]=1; hang=$((hang+1))
                if [ $caps -lt 4 ]; then
                    caps=$((caps+1)); out="$base/$label/CAPTURE.$c.$i.txt"
                    {
                        echo "== HUNG pid=$p cycle=$c idx=$i concurrent_valkey=$(pgrep -c -f valkey-server 2>/dev/null) load=$(cat /proc/loadavg) =="
                        echo "### hung process threads ###"; dumpproc "$p"
                        for ch in $(pgrep -P "$p" 2>/dev/null); do echo "### child $ch of hung (candidate LSan tracer) ###"; dumpproc "$ch"; done
                        echo "### gdb -p $p ###"; timeout 30 sudo gdb -p "$p" -batch -ex 'set pagination off' -ex 'info threads' -ex 'thread apply all bt' 2>&1 || echo "   (gdb attach failed)"
                        echo "### warning line ###"; grep -h "ptrace appears to be blocked\|Child exited with signal" "$d/s$i/err" 2>/dev/null | head -4
                    } > "$out" 2>&1
                    echo "WATCHDOG_CAPTURED $out"
                fi
            fi
        done
        sleep 0.5
    done
    kill "$stpid" 2>/dev/null
    for i in $(seq 1 "$N"); do p=${PID[$i]}; for t in $(seq 1 60); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done; sudo kill -KILL "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
    unset CAP PID
    rm -rf "$d"
done
echo "RESULT label=$label cycles=$cycles nservers=$N hang=$hang captures=$caps"
for f in "$base/$label"/CAPTURE.*.txt; do [ -f "$f" ] || continue; echo "########## $f ##########"; cat "$f"; echo; done
