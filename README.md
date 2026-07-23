# runner-jitter-probe

Standalone probe that characterizes the scheduling and clock behavior of a CI
runner host, independent of any application workload. Built to compare hosted
CI runner VMs (GitHub Actions) against dedicated cloud instances, to determine
whether latency-assertion test flakiness is caused by the runner environment
rather than the software under test.

## What it measures

| Metric | Method | What it detects |
|---|---|---|
| `sleep_overshoot_us` | `nanosleep(1ms)` x N, overshoot distribution | timer/wakeup latency, scheduler delay on wakeup |
| `busy_gap_us` / `busy_loop` | tight `clock_gettime` loop, gaps > 2µs | involuntary preemption, vCPU descheduling, hypervisor steal |
| `fork_call_us` / `fork_total_us` | `fork()` + child `_exit` + `waitpid`, with a touched heap (`--fork-heap-mb`) | page-table copy cost and fork stalls (bgsave-style fork path) |
| `clock` | REALTIME and MONOTONIC_RAW drift vs MONOTONIC, clock resolution, min observable step | NTP slewing, clock quality |
| `steal` | `/proc/stat` steal-tick delta across the run | hypervisor CPU steal |

`--load N` spawns N busy-spinning workers during measurement to simulate a
parallel test suite sharing the box (CI-like contention).

## Build & run

```
gcc -O2 -Wall -Wextra -Werror -o jitter_probe jitter_probe.c
./jitter_probe --label my-host > result.json
./jitter_probe --load 4 --label my-host-loaded > result-loaded.json
```

Zero dependencies (libc + POSIX only). Output is a single JSON object;
all latency stats in microseconds. Key comparison fields: each distribution's
`p99`/`max`/`over_5ms`/`over_10ms`, `busy_loop.lost_pct`, `steal.steal_pct`.

## Options

```
--sleep-iters N     nanosleep samples (default 2000)
--sleep-us US       requested sleep per sample (default 1000)
--busy-secs S       busy-loop duration (default 5)
--fork-iters N      fork samples (default 200)
--fork-heap-mb MB   heap touched before forking (default 64)
--load N            parallel busy workers during measurement (default 0)
--label STR         free-form label embedded in output
```
