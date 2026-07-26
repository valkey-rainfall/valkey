#!/usr/bin/env python3
"""Contention witness: 1Hz sampler of system CPU activity during test runs.

Per sample line (JSONL, epoch-ms timestamps):
- cpu_busy_pct: whole-box CPU busy% over the window
- psi_cpu: /proc/pressure/cpu 'some' avg10 and total-stall delta (µs)
- runnable: count of R-state processes
- top: top-N CPU consumers over the window as [comm, pid, cpu_pct]
- vk_run_delay_ms: per valkey-server pid, runqueue wait delta (schedstat field 2)

Zero dependencies. Overhead ~1-2% of one core. Stop with SIGTERM.
"""
import json
import os
import signal
import sys
import time

TOP_N = 8
INTERVAL = 1.0
running = True
signal.signal(signal.SIGTERM, lambda *a: globals().__setitem__("running", False))
signal.signal(signal.SIGINT, lambda *a: globals().__setitem__("running", False))


def read_cpu_total():
    with open("/proc/stat") as f:
        parts = f.readline().split()
    vals = [int(x) for x in parts[1:]]
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    return sum(vals), idle


def read_psi():
    try:
        with open("/proc/pressure/cpu") as f:
            line = f.readline()  # some avg10=X avg60=Y avg300=Z total=N
        fields = dict(kv.split("=") for kv in line.split()[1:])
        return float(fields["avg10"]), int(fields["total"])
    except Exception:
        return None, None


def scan_procs():
    """Return {pid: (comm, utime+stime, state)} and valkey run_delay {pid: ns}."""
    procs, vk = {}, {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                raw = f.read()
            # comm may contain spaces/parens: split around last ')'
            lp, rp = raw.index("("), raw.rindex(")")
            comm = raw[lp + 1:rp]
            rest = raw[rp + 2:].split()
            state = rest[0]
            ticks = int(rest[11]) + int(rest[12])  # utime + stime
            procs[int(pid)] = (comm, ticks, state)
            if comm == "valkey-server":
                with open(f"/proc/{pid}/schedstat") as f:
                    vk[int(pid)] = int(f.read().split()[1])  # run_delay ns
        except Exception:
            continue
    return procs, vk


def main():
    out = open(sys.argv[1], "a", buffering=1) if len(sys.argv) > 1 else sys.stdout
    hz = os.sysconf("SC_CLK_TCK")
    ncpu = os.cpu_count() or 1
    p_total, p_idle = read_cpu_total()
    _, p_psi_total = read_psi()
    p_procs, p_vk = scan_procs()
    while running:
        time.sleep(INTERVAL)
        t_ms = int(time.time() * 1000)
        total, idle = read_cpu_total()
        psi_avg10, psi_total = read_psi()
        procs, vk = scan_procs()

        dt_total = max(1, total - p_total)
        busy_pct = round(100 * (1 - (idle - p_idle) / dt_total), 1)
        window_s = dt_total / hz / ncpu

        deltas = []
        runnable = 0
        for pid, (comm, ticks, state) in procs.items():
            if state == "R":
                runnable += 1
            prev = p_procs.get(pid)
            d = ticks - prev[1] if prev and prev[0] == comm else ticks
            if d > 0:
                deltas.append((d, comm, pid))
        deltas.sort(reverse=True)
        top = [[c, p, round(100 * d / hz / window_s, 1)] for d, c, p in deltas[:TOP_N]]

        vk_delay = {str(p): round((ns - p_vk.get(p, ns)) / 1e6, 2) for p, ns in vk.items()}
        psi_d = (psi_total - p_psi_total) if (psi_total is not None and p_psi_total is not None) else None

        out.write(json.dumps({
            "ts": t_ms, "cpu_busy_pct": busy_pct, "runnable": runnable,
            "psi_avg10": psi_avg10, "psi_stall_us": psi_d,
            "vk_run_delay_ms": vk_delay, "top": top,
        }) + "\n")
        p_total, p_idle, p_psi_total, p_procs, p_vk = total, idle, psi_total, procs, vk


if __name__ == "__main__":
    main()
