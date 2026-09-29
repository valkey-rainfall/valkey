# exp/staged-loop-poc — build/gate report

Base: `valkey-rainfall/valkey` `exp/shared-base-proposal` @ `46a6675e7`.
Branch: `exp/staged-loop-poc` (cut from `46a6675e7`).
Build: `make SERVER_CFLAGS=-DIO_LOOKUP_OFFLOAD_STATS` (instrumented, matches SBP).
Gate battery (reused from SBP-REPORT / sbp-logs, baseports 25700+):
- gate1: `unit/io-threads` + `unit/lazyfree` `--io-threads --clients 4`
- gatespec: `unit/speculative-reads --clients 1`
- gate2a/2b: `unit/dplus-correctness --clients 1` (x2)
- gate3: `unit/dplus-replica-only --clients 1`

Scripts (worktree, literal paths, untracked): `slp-build.sh`, `slp-gate.sh`.
Logs under `slp-logs/`.

---

## Step 0 — stage vocabulary (stage.h/stage_thread.h/stage.c, identity + asserts)

**Commit:** `54ed99453` stage: name thread identity, per-iteration policy, and crossing asserts.

**What landed**
- `src/stage_thread.h` (new): `stageThreadId {domain, tid}`, `__thread stageSelf`,
  `stageSetSelf`. Dependency-free so `fastpath.h` can give `cmdBatch` a `holder`
  without a circular include. Domain values equal `CC_OWNER_MAIN/IO`.
- `src/stage.h` (new): `stagePolicy {fanout, read_exec, speculate_replica_only}`,
  fanout enum (inline/single-IO/workers), read_exec enum (punt-all/lockstep/
  speculate), `stagePolicyRead()`; `stageAssertOwner`/`stageAssertHolds` debug
  macros (via `debugServerAssert`).
- `src/stage.c` (new): `stageSelf` definition, static_asserts that
  `STAGE_DOMAIN_MAIN==CC_OWNER_MAIN` and `STAGE_DOMAIN_IO==CC_OWNER_IO`,
  `stagePolicyRead()` deriving policy from `active_io_threads_num`,
  `io_threads_fast_path`, `io_threads_speculation_replica_only`.
- `src/fastpath.h`: `cmdBatch` gains `stageThreadId holder`; includes `stage_thread.h`.
- `src/fastpath.c`: identity via holder set at `fpAllocBatch` (→IO owner),
  `fpSubmit` (publish: assert holds + →MAIN), `fastpathDrain` (take: assert holds;
  return: assert holds + →IO owner); `stageAssertOwner` at `fpSend` (write send)
  and at the reply-append in `fpSpeculate`.
- `src/server.c`: `stageSetSelf(STAGE_DOMAIN_MAIN, 0)` at `initServer`.
- `src/io_threads.c`: `stageSetSelf(STAGE_DOMAIN_IO, id)` at `IOThreadMain` start.
- `src/Makefile`: `stage.o` registered.

No renames, no moves. SINGLE_IO and LOCKSTEP are named enum values only.

**Diff stat:** 8 files, +161 / -0.

**Gate — all green (instrumented build, BUILD_RC=0):**
| gate | test | baseport | result |
|---|---|---|---|
| 1 | io-threads + lazyfree (`--io-threads --clients 4`) | 25700 | 12 ok / 0 err |
| spec | speculative-reads (`--clients 1`) | 25710 | 12 ok / 0 err |
| 2a | dplus-correctness (`--clients 1`) | 25720 | 16 ok / 0 err |
| 2b | dplus-correctness (`--clients 1`) | 25730 | 16 ok / 0 err |
| 3 | dplus-replica-only (`--clients 1`) | 25740 | 4 ok / 0 err |

Logs: `slp-logs/build.log`, `gate1.log`, `gatespec.log`, `gate2a.log`, `gate2b.log`, `gate3.log`.

**Pushed:** `origin exp/staged-loop-poc` @ `54ed99453`.

---

## Step 1 — serverStageIteration: name the loop, move epoch reclaim to iteration end

**Commit:** `f8aed4186` stage: name the loop as take/execute/publish; move epoch reclaim to iteration end.

**What landed**
- `src/fastpath.c`: `fastpathDrain` split into named helpers — `handoffTake`
  (dequeue submit ring + the dependent prefetch), `stageExecuteBatch` (fpExecute
  loop + charge + reset executor), `stageWritePublish` (holder→IO owner +
  `spscEnqueue(&t->ret)`). Thin `fastpathDrain` composes them per worker; the
  `io_batch_drain_us` deadline still wraps take+execute+publish together via the
  `goto again` over the whole worker sweep. No per-command indirect call.
- `src/server.c` `beforeSleep`: `stagePolicyRead()` read once per iteration into
  a local at the top of the non-blocking section; the primary drive
  (`processIOThreadsResponses`) named as take/execute/publish ahead of the legacy
  ring + outbox lanes; `dplusAggregateStats()`/`dplusReclaimRetired()` moved from
  the head of the section to the end of the iteration (before
  `updateCopyAvoidPressure`).

**Diff stat:** 2 files, +72 / -33.

**Gate — battery all green (instrumented build, BUILD_RC=0):**
| gate | test | baseport | result |
|---|---|---|---|
| 1 | io-threads + lazyfree | 25700 | 12 ok / 0 err |
| spec | speculative-reads | 25710 | 12 ok / 0 err |
| 2a | dplus-correctness | 25720 | 16 ok / 0 err |
| 2b | dplus-correctness | 25730 | 16 ok / 0 err |
| 3 | dplus-replica-only | 25740 | 4 ok / 0 err |

The reclaim move did NOT change any gate result (stop condition not triggered).

**TSan differential (46a6675e7 vs tip, unit/io-threads `--io-threads --clients 4`):**
Both built `SANITIZER=thread MALLOC=libc SERVER_CFLAGS=-DIO_LOOKUP_OFFLOAD_STATS`,
run with `LD_LIBRARY_PATH=$HOME/.local/lib-sanitizers`, TSAN halt_on_error=0.
- base: 47 race warnings, 14 unique function-pair signatures.
- tip: 48 race warnings, 13 unique function-pair signatures.
- **NEW races in tip (function-pair): NONE.** Every tip signature is a subset of
  base's; base had one extra (`ioThreadWriteSlab <=> reconcileLazyWrite`) that did
  not recur (run-to-run variance — a reduction, not an addition). The apparent
  line-number "new" entries in the raw pair diff are pure line drift from this
  step's +1/+2 line edits (clientsCron server.c:1294→1295,
  trySendReadToIOThreads io_threads.c:1579→1581, etc.) — same functions, same
  pairs. No stage.* code, the moved dplusAggregateStats, or any drain-split
  helper appears in any race.
- Logs: tip `slp-logs/tsan-race-tip.*`, `tsan-build-tip.log`, `tsan-run-tip.log`;
  base frames archived at `slp-logs/tsan-frames-base.txt` (base worktree removed
  after the run). Scripts: `slp-tsan-tip.sh`.

**Flat-numbers A/B:** the parent runs this on the fleet (not this session's job).

**Pushed:** `origin exp/staged-loop-poc` @ `f8aed4186`.

---

## Step 1 scope note — cross-transport unified ready queue NOT built

The design §2.1 asks for `processCommandRing`/outbox drains to "become
`stageReadMain` completions feeding **the same ready queue** as the fast-path
batches, so `stageExecute` has **one input**." That single physical queue was
**not** built, per the brief's stop condition ("if step 1 cannot preserve
ordering without a per-command indirect call, STOP"). The achievable part
(fast-path lane as one ready queue via the take/execute/publish split, epoch
move, policy-once) landed in `f8aed4186`; the cross-transport unification is
deferred.

**Exact conflict — the two lanes have different execution mechanisms:**
- Fast-path entry: a parsed `cmdEntry` in a batch arena, executed by
  `fpExecute(ec, b, e)` (`src/fastpath.c:1102`) on a **shared executor client**,
  reply written into the batch arena and returned via the ret ring.
- Legacy completion: a real `client *` tagged in `io_cmd_ring`, executed by
  `ringExecuteOne(c)` then `processClientsCommandsBatch()` inside
  `processCommandRingOne` (`src/io_threads.c:739-742`) **on the client itself**,
  reply on the client's own buffers, written by `handleClientsWithPendingWrites`.

A queue holding both would require, per dequeued item, discriminating "arena
cmdEntry → fpExecute" from "legacy ring client → ringExecuteOne" at execute
time — a tagged/indirect dispatch on the per-command hot path, which the design
forbids ("no new indirect call on the per-command path", sketch §1). This does
NOT block steps 2-3 (worker-side renames/moves and the transport double).

---

## Step 2 — worker renames onto stage names (+ file-move scope)

**Commit:** `7c4b86f23` stage: rename worker functions onto the read/execute/handoff/write stage names.

**What landed (rename half, step 2 per §2.2 table):** renamed in place, all call
sites within fastpath.c (file-static), bodies byte-for-byte unchanged:
- `fpRead` → `stageReadCollect`
- `fpSpeculate` → `stageExecuteEligible`
- `fpHarvest` → `handoffPublish`
- `fpDeliverBatch` → `handoffReturn`
- `fpSend` → `stageWriteSend`

`fpReadmit` is a distinct function, untouched. **Diff stat:** 1 file, +13 / -13.

**Gate — battery all green** (instrumented, BUILD_RC=0): gate1 12/0, spec 12/0,
dplus-correctness 16/0 ×2, dplus-replica-only 4/0.

**File-move half (stage_read.c / stage_exec.c / stage_write.c / handoff.c):
DEFERRED.** The worker functions are deeply coupled through fastpath.c
file-statics — `fp_threads[]`, `fp_slots`, `fp_exec_client[]`, `fp_detaching`,
and ~15 static helpers (`fpExecutor`, `fpResolve`, `fpControlDetaching`,
`fpReplyChargeBatch`, `fpBeginLeave`, the `stageWriteSend`↔`fpFlushOut` mutual
recursion, …). A clean 4-TU split first requires exposing `fpThread` and that
helper set through an internal header (`fastpath_internal.h`), then relocating
each cluster as its own build-and-gate move commit. That is pure mechanical
churn with no behavior change; the design itself places it "after the flat gate
and before any struct change" as the churny rebase step (§4). It was deprioritised
in favour of step 3 (the transport double + the four bug-incident gtests), which
is the design's actual "PoC proves the interfaces" gate and is self-contained.
The stage names are already in place, so the later move is a pure `git mv`-style
relocation.

**Pushed:** `origin exp/staged-loop-poc` @ `7c4b86f23`.

---

## Step 3 — transport double + four thread-crossing gtests

**Commit:** `469aec8c2` stage: in-memory transport double and four thread-crossing guard tests.

**What landed**
- `src/transport_mem.c` / `src/transport_mem.h` (new): an in-memory stand-in for
  the thread-to-thread transport — two SPSC rings per IO thread (`submit`,
  `ret`), a tiny `memControl` owner registry, and an explicit `memWake()` in
  place of an epoll readiness edge. No sockets, no epoll, no threads. `memControl`
  and `memBatch` name their `owner_domain`/`owner_tid` and `holder` fields
  identically to `ClientControl`/`cmdBatch`, so `stageAssertOwner` /
  `stageAssertHolds` apply to the double unchanged. Crossing ops:
  `memHandoffPublish`/`Take`/`Return`, `memStageWriteSend`,
  `memReplyAppendFromMain`, `memFlagsOwnerSet`, `memRegisterFd`,
  `memTransferOwnership`, `memWorkerShrinkReset`.
- `src/Makefile`: `transport_mem.o` registered (after `stage.o`) so the double
  links into `libvalkey.a` for the gtest binary.
- `src/unit/test_stage_transport.cpp` (new): four incidents (§3 of the sketch),
  one per incident, each with a guard-stubbed corruption `TEST_F` and a guard-in
  `EXPECT_DEATH`. Each sets `stageSelf` directly (no threads) and drives the
  mem-transport ops. The RED/GREEN switch is `server.enable_debug_assert`, flipped
  by the `SLP_GUARD` env var (`0` → guard stubbed, `1` → guard in):
  - (a) flags-word RMW from a non-owner thread erases the owner's bit;
  - (b) reply append into a worker-owned buffer from main;
  - (c) fd registration while the owner has inflight work;
  - (d) shrink IDLE reset of an owned, non-drained connection.

**Diff stat:** 4 files, +472 / -0.

**gtest RED / GREEN (instrumented build):**
- RED (`SLP_GUARD=0`, guard stubbed): the 4 corruption tests RUN and PASS —
  each observes the incident's silent corruption; the 4 death tests SKIP. All
  passed. Log: `slp-logs/gtest-red.log`.
- GREEN (`SLP_GUARD=1`, guard in): the 4 `EXPECT_DEATH` tests RUN and PASS —
  each assert aborts before the corrupting write (fork/abort real, ~3.1s); the
  4 corruption tests SKIP. All passed. Log: `slp-logs/gtest-green.log`.

Every incident is RED-without-guard and GREEN-with-guard.

**Gate — battery all green** (instrumented, BUILD_RC=0), unchanged from step 2:
| gate | test | baseport | result |
|---|---|---|---|
| 1 | io-threads + lazyfree (`--io-threads --clients 4`) | 25700 | 12 ok / 0 err |
| spec | speculative-reads (`--clients 1`) | 25710 | 12 ok / 0 err |
| 2a | dplus-correctness (`--clients 1`) | 25720 | 16 ok / 0 err |
| 2b | dplus-correctness (`--clients 1`) | 25730 | 16 ok / 0 err |
| 3 | dplus-replica-only (`--clients 1`) | 25740 | 4 ok / 0 err |

Logs: `slp-logs/build.log`, `gate1.log`, `gatespec.log`, `gate2a.log`,
`gate2b.log`, `gate3.log`, `gtest-build.log`, `gtest-red.log`, `gtest-green.log`.

**Deviations**
- The unit-test binary links only after passing `LDFLAGS=-lsystemd` (this host's
  server is built `-DHAVE_LIBSYSTEMD`, but `src/unit/Makefile`'s `LD_LIBS` never
  adds `-lsystemd`, so `sd_notify` is undefined at gtest link). This is a
  gtest-Makefile/host gap, not a code change: it is passed on the command line
  and NOT persisted — `src/.make-settings` `LDFLAGS` is left empty so a plain
  fleet `make` links the server normally. The server itself needs no override.
- The unit-test binary needs googletest ≥ 1.12 (`src/unit/main.cpp` uses
  `GTEST_FLAG_SET`); the distro package here is 1.11.0. Built against a 1.14.0
  install under the user prefix instead: `PKG_CONFIG_PATH` pointed at it, and its
  include dir passed as `-isystem` rather than the pkg-config `-I`, because the
  1.14 headers trip `-Werror=double-promotion` when treated as project headers.
  No tracked file changed for this. Script: `slp-gtest.sh` (untracked).

**Pushed:** `origin exp/staged-loop-poc` @ `469aec8c2`.

---

## Deferred

- **Cross-transport unified ready queue** (design §2.1): NOT built. The fast-path
  lane and the legacy ring/outbox lane have different execution mechanisms —
  fast-path executes a parsed `cmdEntry` in a batch arena via `fpExecute` on a
  shared executor client, legacy executes a real `client *` via `ringExecuteOne`
  on the client itself. A single queue holding both would require a tagged/
  indirect dispatch on the per-command hot path to discriminate them at execute
  time, which the design forbids ("no new indirect call on the per-command
  path", sketch §1). The achievable part (fast-path lane as one ready queue via
  the take/execute/publish split, epoch move, policy-once) landed in step 1
  (`f8aed4186`). Full detail in the step-1 scope note above.
- **`fastpath.c` four-TU split** (`stage_read.c` / `stage_exec.c` /
  `stage_write.c` / `handoff.c` via `fastpath_internal.h`): NOT started. The
  worker functions are deeply coupled through `fastpath.c` file-statics
  (`fp_threads[]`, `fp_slots`, `fp_exec_client[]`, `fp_detaching`, ~15 static
  helpers, the `stageWriteSend`↔`fpFlushOut` mutual recursion). A clean split
  first needs `fpThread` and that helper set exposed through an internal header,
  then each cluster relocated as its own build-and-gate move commit — pure
  mechanical churn with no behavior change, which the design itself places
  "after the flat gate and before any struct change" (§4). The stage names are
  already in place (step 2), so the later move is a pure `git mv`-style
  relocation. Deprioritised in favour of step 3, the design's actual "PoC proves
  the interfaces" gate, which is self-contained.

