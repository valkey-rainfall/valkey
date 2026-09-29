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
