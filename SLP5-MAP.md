# SLP5-MAP: feasibility of main as IO owner at io-threads 1

Scope: can the IO-owner stage path (read -> handoffPublish -> handoffTake ->
stageExecute -> write.publish -> fastpathDrain) run on the main thread as domain
MAIN / tid 0, selected by policy, without a structural change to main's event
loop or to how main reads sockets? This map lists every site where the IO-owner
path assumes the owner is a worker thread (tid >= 1) rather than main, the
minimal relaxation each needs, and whether that relaxation sits behind the new
policy.

## The one architectural fact that decides it

A worker owns its clients' sockets through a **per-tid epoll fd** (`io_epfd[tid]`,
`io_threads.c:44`), created in `startIOThread` (`io_threads.c:1237`) and scanned
by `ioThreadPollPartition(id)` (`io_threads.c:963`+, the `epoll_wait(io_epfd[id],
...)` at `io_threads.c:552`) **from inside that worker's own loop**
(`IOThreadMain`, `io_threads.c:1177`: `if (io_epfd[id] > 0) processed +=
ioThreadPollPartition(id)`). Main (tid 0) has no `io_epfd[0]` and no such scan:
main reads its own clients through `server.el` with the connection read handler
`readQueryFromClient` (`networking.c:357`), and toggles write interest through the
ae loop, not through `epoll_ctl`.

So "main is an IO owner" needs main to drive the owner read path for tid 0. There
are two ways, and the lighter one avoids a second poll on main entirely:

- **(A) give main its own `io_epfd[0]` and scan it from `beforeSleep`.** Pulls the
  worker's poll structure onto main as a second poll loop. This would be the
  structural change the stop condition warns against.
- **(B) install the IO-owner read path as the `server.el` read handler for a
  tid-0-owned client**, i.e. `connSetReadHandler(c->conn, <wrapper calling
  fastpathClientReadable(0, c)>)` instead of `readQueryFromClient`, and let main's
  existing ae loop deliver readability. Writes and write-interest for tid 0 go
  through `connSetWriteHandler` / the ae loop, not `epoll_ctl`.

**(B) is chosen.** It reuses main's existing event loop as the socket poller for
owned clients, which is exactly what the sketch's "main is domain MAIN with its
own slot table, same code" row intends: the *stage code* is shared; the *poll* is
whatever that domain already has. The owner read body itself
(`stageReadCollect` -> `readToQueryBuf` -> parse -> `stageExecuteEligible` ->
`handoffPublish`, `fastpath.c:750`) touches no epoll fd; it reads the connection
directly. The epoll fd is used only at four edges (register on take, deregister on
leave, write-interest MOD, writable callback), each of which has an ae-loop
equivalent for a main-owned connection. None of this restructures main's loop.

**Verdict: feasible under the stop condition.** It is tid-range relaxations, a few
early-return relaxations, an attach path to tid 0, ring init for tid 0, a
`beforeSleep` drive for tid 0, and an epoll-vs-ae split at the four socket edges
for tid 0. Estimated engine change well under ~500 lines. The stop condition does
NOT trigger.

## Site map

Legend for "policy-gated": **yes** = the relaxation only takes effect when
`io_threads_main_owner` is on and `active_io_threads_num <= 1` (SINGLE_IO);
**n/a** = a shared helper that already keys on `c->io_tid`/tid and simply needs
tid 0 to be a legal value, so no behavior changes at io-threads N or when the
config is off.

### Owner-tid selection (the tid-range assumption)

| site | assumption | minimal relaxation | policy-gated |
|---|---|---|---|
| `fastpath.c:457` `fastpathAttach` | owner tid picked from `1 .. ioThreadsReadyNum()-1` via `fp_rr`; main (0) never chosen | under SINGLE_IO, route attach to tid 0 (main owns its own clients); otherwise unchanged | yes |
| `networking.c:1943` accept | attaches only when `fastpathEligible(c)`; eligibility requires `strictOffloadActive() && io_threads_num >= 2` (`fpSessionEligible`, `fastpath.c:290`), so no client is ever eligible at io-threads 1 | add a SINGLE_IO eligibility predicate that admits at io-threads 1 under the config, reusing the same per-client session checks minus the `io_threads_num >= 2` / `strictOffloadActive` gate; on admit, install the owner read handler (route B) instead of `connSetReadHandler(NULL)` | yes |
| `fastpath.c:290` `fpSessionEligible` | `!strictOffloadActive() \|\| io_threads_num < 2` returns 0 | split the thread-count gate from the per-client session gate; SINGLE_IO uses the session gate alone | yes |

### Socket read / write / register edges (epoll vs ae)

| site | assumption | minimal relaxation | policy-gated |
|---|---|---|---|
| `fastpath.c:1086` `fpTakeClient` EPOLL_CTL_ADD on `ioThreadEpollFd(c->io_tid)` | owner has an epoll fd; registers the socket there | for tid 0, no epoll register: the client's `server.el` read handler is set to the owner read path at admission (route B); keep epoll register for tid >= 1 | yes (via `c->io_tid == 0` branch, reached only under SINGLE_IO) |
| `fastpath.c:823` `fpEnableWriteInterest` EPOLL_CTL_MOD | write interest toggled via epoll | for tid 0, `connSetWriteHandler(c->conn, <writable wrapper>)` / clear; for tid >= 1, epoll MOD as today | yes |
| `fastpath.c` `fpBeginLeave` EPOLL_CTL_DEL (`ioThreadEpollFd(c->io_tid)`) | owner deregisters from its epoll fd | for tid 0, `connSetReadHandler(c->conn, NULL)` (ae) and drop write handler; for tid >= 1, epoll DEL | yes |
| `fastpath.c:704`/`fastpath.c` `fastpathClientReadable` / `fastpathClientWritable` | invoked by `ioThreadPollPartition` on the worker | for tid 0, invoked from the `server.el` read/write handler installed at admission; same function, tid 0 | yes |
| `io_threads.c:552` `epoll_wait(io_epfd[id], ...)` in `ioThreadPollPartition` | per-worker poll scans owned sockets | **not used for tid 0** under route B; main's ae loop is the poller. No change to this function; it is simply never driven for tid 0 | n/a |

### Ring init / drive (the self-loop)

| site | assumption | minimal relaxation | policy-gated |
|---|---|---|---|
| `fastpath.c:143` `fastpathInitThread` | called per worker from `startIOThread`; main's `fp_threads[0]` (submit/ret rings, slots, registry) is never initialized | initialize `fp_threads[0]` (same `fastpathInitThread(0)`) when SINGLE_IO is active, so main has its own submit/ret rings, owner-slot table, registry; free it when the config/thread shape changes back | yes |
| `fastpath.c:1189` `fastpathDrain` loops `tid = 1 .. fp_slots` | main drains workers only; never its own ring | under SINGLE_IO, include tid 0 in the drain (start at 0): `handoffTake(fp_threads[0]) -> stageExecute -> stageWritePublish`, a synchronous self-loop (publish and take are the same thread, so the ring is drained the same iteration). For tid >= 1 unchanged | yes |
| `server.c:2029` `beforeSleep` `processIOThreadsResponses()` | the per-iteration drive; already the place the step-1 note names for take/execute/publish | no new call site: `fastpathDrain`'s tid-0 inclusion rides the existing `processIOThreadsResponses()` -> `fastpathDrain()` call | yes (behavior only under SINGLE_IO) |
| `io_threads.c:1177` `IOThreadMain` drives `ioThreadPollPartition` + `fastpathProcessReturns` + `fastpathSubmitPending` per worker | the owner's returns/submits are driven on the worker thread | for tid 0 these are driven on main: returns via `fastpathDrain`'s self-publish (no cross-thread ret ring hop needed; still goes through `stageWritePublish` -> `fastpathProcessReturns(0)` or a direct write-out on the same thread), submit via `fastpathSubmitPending(0)` called from `beforeSleep`. No worker loop for tid 0 | yes |

### Thread identity and asserts

| site | assumption | minimal relaxation | policy-gated |
|---|---|---|---|
| `stage.c:14` `stageSelf = {MAIN, 0}` | main is already domain MAIN tid 0 | **no change** — this is exactly the identity a main owner needs; `stageSelf` stays `{MAIN, 0}` | n/a |
| `fastpath.h` `cmdBatch.holder`, `fpAllocBatch` sets `{IO, tid}` | a batch opened by a worker is held by `{IO, tid}`; `fpSubmit` publishes to `{MAIN,0}`, `stageWritePublish` returns to `{IO, io_tid}` | for a tid-0 batch the open holder is `{MAIN,0}` not `{IO,0}`; `stageWritePublish` returns to `{MAIN,0}`. `stageAssertHolds` must accept the publishing domain, i.e. `{MAIN,0}` holding its own batch end to end. Widen to "holder is the publishing domain," never to "anyone" | yes |
| `stage.h` `stageAssertOwner(cc)` | `owner_domain == stageSelf.domain && (domain != IO \|\| owner_tid == self.tid)` | a main-owned client has `owner_domain == CC_OWNER_MAIN`, `stageSelf.domain == MAIN`: the assert already passes for a main owner with no change. It encodes "owner is the current domain," not "owner is IO" | n/a (already correct) |
| `fastpath.c:481` attach sets `owner_domain = CC_OWNER_IO` | admitted client is owned by an IO thread | under SINGLE_IO set `owner_domain = CC_OWNER_MAIN`, `owner_tid = 0`; `stageAssertOwner` then holds on main | yes |

### io-threads==1 early returns (the gates that keep the owner path off at 1 thread)

These return early precisely because today nothing owns a socket at io-threads 1.
Each must let the SINGLE_IO owner path run; none needs a structural change, each is
a conjunction with the new config.

| site | current guard | relaxation | policy-gated |
|---|---|---|---|
| `io_threads.c:238` `clientIsPartitionable`/slab route `active_io_threads_num <= 1 return 0` | no partition target at 1 thread | unchanged for the partition/slab path (SINGLE_IO uses the fast-path owner route, not the partition route) | n/a |
| `io_threads.c:450` `armPartitionedClientRead` `<=1 return` | mid-scale guard | unchanged (partition path) | n/a |
| `io_threads.c:832,867` `ioThreadUpcall`/scale `io_threads_num == 1 return` | no workers to activate | unchanged — SINGLE_IO adds no worker; main is the owner | n/a |
| `io_threads.c:848,893` `active_io_threads_num == 1` branches | single-thread fast exits | SINGLE_IO must still take the main-owner drive in `beforeSleep`; the drive is in `fastpathDrain`, not these scale branches, so they stay | n/a |
| `io_threads.c:1506` `io_threads_num == 1 return` (`initThreadedIO` / teardown region) | no threaded IO to set up | SINGLE_IO needs `fastpathInitThread(0)` to run; add that init under the config without starting a worker thread | yes |
| `io_threads.c:1577,1625,1743,1806,1857,1908,1958,1987` `active_io_threads_num <= 1` command/offload paths | offload disabled at 1 thread | unchanged — these gate worker offload of specific commands; SINGLE_IO does not offload across threads, it runs the stage code inline on main | n/a |
| `networking.c:341` `io_threads_num == 1` copy-avoidance | reply copy policy at 1 thread | unchanged — reply copy avoidance is orthogonal to ownership | n/a |

### Speculation on the owner (the seqlock side)

| site | assumption | relaxation | policy-gated |
|---|---|---|---|
| `fastpath.c:738` `stageExecuteEligible` -> `dplusSpeculateBatch(c, tid)` | the speculating reader is a worker thread distinct from main (which holds the seqlock writer side during execute) | On a single thread, read-stage speculation and the execute writer are the SAME thread, so the seqlock's reader-vs-writer concurrency the design relies on does not exist: a speculated read on main would validate against a version the same thread is about to bump, with no other writer to race. Two safe options: **(i)** under SINGLE_IO, disable speculation on tid 0 (read_exec effectively PUNT within the owner loop) so the A/B measures the staging cost cleanly without a degenerate same-thread seqlock; **(ii)** allow it and rely on it being trivially consistent. **Choose (i)** for M-stage: it keeps the A/B a clean "staging overhead" measurement and avoids reasoning about a same-thread reader/writer. Record the gating line (`fastpath_speculated` will be 0 on the main owner by design). | yes |

## Summary

Total distinct sites: **22**. Needing a relaxation (code change): **16**
(tid-range 3, socket edges 4, ring init/drive 4, identity/asserts 2
[`stageAssertHolds` widen + attach owner_domain], io-threads==1 early-return
init 1, speculation gate 1, plus the two already-correct identity sites counted
as no-change). Sites that are already correct or unaffected (`stageSelf`,
`stageAssertOwner`, the copy-avoidance and worker-offload early returns, the
epoll scan which is simply not driven for tid 0): **6**.

No site forces a structural change to main's event loop or to how main reads
sockets: route B reuses `server.el` as the poller and splits only the four
socket edges by `c->io_tid == 0`. The one genuine design decision is speculation
on a single thread (gated off on the main owner, option (i)), which is a policy
choice, not a structural obstacle. Proceeding to M1.
