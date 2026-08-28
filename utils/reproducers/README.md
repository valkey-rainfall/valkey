# Door-2 dev tools (NOT the canonical reproducers)

The canonical regression + soak coverage for these bugs lives in
tests/unit/ownership.tcl, with environment knobs to turn the suite tripwires
into statistical hammers:

- B11 (runtime resize):   B11_CYCLES=50   ./runtest --single unit/ownership --only '/B11' ...
- B12 (parser desync):    B12_ITERATIONS=4000 ./runtest --single unit/ownership --only '/B12' ...

Pre-fix B12 fired at ~0.5%/iteration; >= 4000 iterations gives a meaningful
closure bound. Build with SERVER_CFLAGS=-DIO_LOOKUP_OFFLOAD_STATS to get the
"D+ proto-desync" forensic log line on any hit.

The shell scripts below are convenience DEV TOOLS kept for two workflows the
harness handles poorly: running against an arbitrary standalone binary with no
repo/harness checkout (copy one file to any host), and parallel multi-config
cells on distinct ports (the harness fights parallel self-invocation).

- b11-resize-verify.sh -- resize-under-traffic liveness/crash check.
- b12-crlf-soak.sh -- --pipe-based soak, iteration-indexed output dirs.

TSan differential convention: --only '/EPOCH|B12'.
