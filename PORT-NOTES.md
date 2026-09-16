# Port notes: read-command offload to I/O threads (from valkey-io/valkey#2976)

Faithful port of Uri Yagelnik's "Commands offload to IO threads in cluster mode enable"
(PR #2976, commit `2152899097`, design by Dan Touitou and Uri Yagelnik in issue #2022)
onto upstream `unstable` at `6225ee7c3` (2026-09-15).

Ported: the offload-only delta (`a4b9aacdd..2152899097`, +2,110/-242). The PR's first
commit (`a4b9aacdd`, "Redesign IO threading communication model", PR #2909) is NOT ported:
upstream landed an independent implementation of the same design as #3324 (April 2026),
and this port targets that.

Rule for this phase: **mechanism identical, substitutions only where upstream forced them.**
Improvements (command allowlist, standalone mode, heuristic removal) are separate commits.

## Deviations from the original PR, with reasons

| # | Area | Original | Port | Why |
|---|---|---|---|---|
| 1 | Queue transport | `src/io_queues.h` (409 lines, SPSC/SPMC/MPSC) | Dropped; uses upstream `src/queues.h` | Upstream #3324 provides the same three queues, per-thread SPSC private inboxes, thread-local MPSC tickets. |
| 2 | Job tags | `JOB_REQ_COMMAND` in the shared SPMC request enum | `JOB_SPSC_COMMAND` in the SPSC private-inbox enum | Upstream's SPMC enum is full (8 tags, 3-bit field). Command jobs are only ever sent to a specific thread's private inbox (slot-to-thread affinity), so the SPSC enum is the correct home anyway. `JOB_RES_COMMAND` / `JOB_RES_JOBLIST` added to the result enum (7 of 8 used). |
| 3 | Result lanes | Single MPSC outbox | Command results and deferred-job lists go on `JOB_PRIORITY_NORMAL` via upstream `sendToMainThread` | Upstream has HIGH/NORMAL lanes. Offloaded commands are `CLIENT_TYPE_NORMAL` only. Both the job list and the result use the same lane so FIFO order (job list before result) is preserved. |
| 4 | `processOutboxBatch` | `while(1)` drain loop | Upstream's single-batch structure, with command/joblist handling added | Upstream restructured to a per-lane batch function with QoS preemptive polling between lanes. |
| 5 | Poll batch cap | `aeApiPoll(..., eventLoop->poll_batch_size)` in `ae_epoll.c` | `MIN(setsize, poll_batch_size)` at the two main-loop call sites in `ae.c` (`aePoll`, `aeProcessEvents`) | Upstream refactored `aeApiPoll` to be event-loop-agnostic for the QoS secondary poll (#4076). The QoS priority poll is deliberately NOT capped. |
| 6 | `processClientIOWriteDone` | `connSetPostponeUpdateState(0); connUpdateState(); if (nwritten>0 \|\| err) postWriteToClient()` | Upstream's postpone-mask ordering (#3611) kept; the PR's `nwritten > 0 \|\| error` guard around `postWriteToClient` added | The guard is needed because an I/O thread may have already flushed the reply after an offloaded command, leaving a write job with nothing written. |
| 7 | Read-job prefetch | Also prefetched `migrating_slots_to[slot]` and `importing_slots_from[slot]` | Only `server.cluster->slots[slot]` and the slot-queue entry | Upstream turned those two arrays into hashtables; no per-slot cache line exists. |
| 8 | Module gate | `moduleCount() > 0` disables offload | `moduleCountDynamic() > 0` (new helper: non-static modules only) | Upstream now registers the built-in Lua scripting engine as a static module named `lua` at startup, so `moduleCount()` is never 0 and the original gate disabled offloading unconditionally. The Lua engine registers only a scripting engine: no keyspace notifications, filters, data types, or server-event hooks. |
| 9 | Thread-local conversion | `server.current_client`, `server.executing_client`, `server.cmd_time_snapshot` -> `_Thread_local` globals | Same, redone across upstream's ~120 call sites | Mechanical. One hazard found and fixed: upstream's `moduleNotifyKeyspaceEvent` had a local `client *executing_client = server.executing_client;` which the rename turned into a self-initialising shadow (`= executing_client`), crashing module keymiss tests. Renamed the local to `notify_client`. `-Wshadow` probe over the touched files finds no other case. |
| 10 | Unit tests | `src/unit/test_cmd_offload.c` (21 cases) + `test_io_queues.c` (16 cases), old C harness | Not carried in this commit | Upstream moved unit tests to GoogleTest (`src/unit/*.cpp`). The queue tests are redundant with upstream `test_queues.cpp`. The 21 `cmd_offload` tests need a GTest port: **follow-up commit**. |
| 11 | Cluster-only prefetch in `handleReadJobs` | unguarded | unguarded (c->slot is -1 outside cluster mode today) | Must be guarded by `server.cluster_enabled` when standalone mode lands (Phase B). |

Not changed (kept for fidelity, candidates for later removal): AIMD throttle, saturation
estimator, per-thread I/O-skip heuristic, server-cron deferral, `AE_SERVER_POLL_BATCH_SIZE`
= 200, `io-threads-do-command-offloading-with-modules` config.

## Known interactions to watch

- **QoS scheduler (#4076)**: `processIOThreadsResponses` now preemptively polls QoS events
  between the HIGH and NORMAL outbox lanes. Offloaded command results ride the NORMAL lane,
  so a burst of cluster-bus traffic can delay result processing (and therefore slot release).
  Not measured yet.
- **Poll batch cap vs QoS**: the cap applies to the main poll only. Whether 200 is still the
  right number with QoS's secondary polling is an open question for measurement.
- **Upstream test assumptions at `io-threads 4`**: three upstream tests fail on the untouched
  base at this thread count in cluster mode (`unit/networking` "prefetch works as expected when
  killing a client from the middle...", `unit/info` "client input and output buffer limit
  disconnections", `unit/maxmemory` "eviction due to output buffers of many MGET clients").
  They fail identically with and without the port. The default harness uses `io-threads 2`,
  which is below the offload gate (`active_io_threads_num > 2`), so a plain `--io-threads`
  run does not exercise offloading at all.

## Verification performed (2026-09-15/16)

- Build: `make -j16` clean, zero warnings.
- Engagement smoke (cluster mode, 4 I/O threads, non-pipelined): 249,991 / 250,000 GET+HGETALL
  executed on I/O threads; no asserts, no crashes.
- Tcl regression with offload live (`--cluster-mode`, harness patched to `io-threads 4`):
  3,342 passed across type/*, expire, networking, introspection, keyspace, multi, tracking,
  scan, scripting, functions, pubsub*, info*, latency-monitor, maxmemory, bitops, geo,
  hyperloglog, sort, dump, lazyfree, querybuf, protocol, auth, acl, client-eviction, and
  moduleapi/{keyspace_events,misc,datatype,hooks,commandfilter,blockedclient,scan}.
  3 failures, all reproduced on the untouched base (see above).
- Tcl regression standalone (`--io-threads`, offload gated off): 1,422 passed; same
  pre-existing networking failure; two "can't start" were unbuilt test modules (since built).

## Unit tests: C harness -> GTest (2026-09-16)

Upstream replaced the C unit-test harness with GoogleTest (`src/unit/*.cpp`, C++17, server
headers pulled in through `wrappers.h`). The PR's 20 `test_cmd_offload.c` cases are ported
1:1 by name to `src/unit/test_cmd_offload.cpp` (`CmdOffloadTest.*`); the earlier "21" count
in this file was a miscount. Deviations forced by the C++ shim, not by design:

- `wrappers.h` maps `_Atomic(T)` to `alignas(sizeof(T)) T`, and `extern alignas(...)` on an
  array declaration is ill-formed C++. The PR exposed `io_threads_stat_{cmd,io}_cpu[]` and
  `io_threads_io_skipped[]` as `extern atomic_int` arrays; they are now `static` in
  `io_threads.c` behind `getIOThreadCmdCpuPct()`, `getIOThreadIoCpuPct()`,
  `getAverageIOThreadIoCpuPct()`, `getAverageIOThreadCmdCpuPct()`, plus
  `testOnlySetIOThreadCpuPct()` for the saturation-estimator tests. The generic
  `getAverageThreadStat(_Atomic int *, int)` became static (a pointer-to-atomic parameter
  cannot be spelled portably through the shim). INFO uses the accessors.
- `slotQueue` internals stay private to `cmd_offload.c`; `testOnly*` accessors in
  `cmd_offload.h` expose only what the tests need (refcounts, deferred-job lists, slot
  context). Behaviour of the tested code is unchanged.
- `_Thread_local` added to the shim's C11->C++ keyword map (the PR's thread-local state is
  declared in headers the tests include).
- `stat_keyspace_hits/misses` spelled `_Atomic(long long)` so the shim macro matches.
- The unit-test link line does not carry `-lsystemd` even when the server objects were built
  with libsystemd auto-detected; on this host pass it via `GTEST_LIBS`. Upstream gap, not
  port-specific; CI hosts lack libsystemd and never hit it.

Result: `valkey-unit-gtests --gtest_filter='CmdOffloadTest.*'` 20/20 pass; the full filter
`*Offload*` (58 tests incl. upstream `ClusterIOOffloadTest`) passes. Server rebuild after the
refactor: zero warnings.

## Sanitizer passes (2026-09-16, tip 233a281c3, cluster mode, `io-threads 4`, 44 units)

Harness: `run-sanitizer.sh <tsan|asan> [<sha> <label>]` at the worktree root; builds like
upstream CI (`SANITIZER=address` without jemalloc, `SANITIZER=thread` with it), runs the same
44 units as the Tcl regression above. This host lacks the libasan/libtsan runtime RPMs, so
`~/.local/lib/sanitizer-rt` carries the `libtsan.so.0` / `libasan.so.6` soname links.

- **ASan: clean.** 3,270 passed, 0 failed, 0 reports, runtest exit 0.
- **TSan: no port-attributable race.** Upstream has no TSan CI job and its I/O-thread design
  hands clients between threads through relaxed-atomic queues TSan cannot see as
  happens-before edges, so a full run is a firehose on the *untouched base* too:

  | run | passed | failed | TSan reports | distinct racing functions |
  |---|---|---|---|---|
  | base `6225ee7c3` | 3,273 | 43 | 1,926 | 103 |
  | port `233a281c3` | 3,273 | 45 | 1,822 | 101 |

  Top frames in both: `siphash` (~360), `ustime` (136), `consumeCommandQueue`, `sdsType`,
  `sdslen`, `addCommandToBatchAndProcessIfFull`, `tryOffloadFreeArgvToIOThreads` -- all upstream
  I/O-thread paths. Function-level diff of the racing frame: only two functions report in the
  port and not the base, and both are the same handoff class with shifted attribution:
  - `processClientIOWriteDone` (14): the PR's "nothing written" guard reads `c->nwritten` one
    frame earlier than upstream's `postWriteToClient`, which carries 84 reports on the base.
  - `zsetKeyCompare` (6): main compares an sds the I/O thread wrote while parsing; the base
    reports the identical pattern via `dictSdsKeyCompare` / `objectGetVal`.
  No `cmd_offload.c` frame is the racing access in any report. The zmalloc per-thread counter
  race (`zmalloc_used_memory` vs `update_zmalloc_stat_*`, upstream's intentional design) is
  suppressed via `tsan.supp`; the first halt-on-error run stopped on it within a minute.
  Histograms: `read-offload-port-tsan/sanitizer-logs{,-base}/hist-*.txt`.

  Caveat for the RFC: "no new TSan reports" is the strongest statement this tooling allows. A
  TSan-clean baseline would need upstream to annotate its queue handoffs (or a suppression
  file for the ~100 known classes), which is out of scope for Phase A but is exactly the kind
  of "most solid guarantee" work the complexity objection asks for.

Not yet done: A/B throughput vs base on the benchmark fleet.
