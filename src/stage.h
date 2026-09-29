/* Staged event loop: per-iteration policy and the crossing asserts.
 *
 * One loop iteration reads as read -> handoff.take -> execute -> write.publish -> reclaim, with
 * the same stage names on the worker side and the policy chosen once per iteration into a local.
 * This header carries no per-command indirect call: the policy is a value read once and switched
 * on, and every assert is a thread-local load compiled out of release builds.
 *
 * Scope of this first step: the vocabulary only. It defines the policy the loop reads and the
 * assertions placed at each thread crossing. It renames nothing and moves nothing; the loop split
 * and the worker-side renames come in later steps. Thread identity lives in stage_thread.h so a
 * transport header can name a batch holder without depending on this file.
 */

#ifndef STAGE_H
#define STAGE_H

#include <stdint.h>
#include "stage_thread.h" /* stageSelf, stageThreadId, stageDomain, stageSetSelf */
#include "fastpath.h"     /* CC_OWNER_MAIN / CC_OWNER_IO, ClientControl, cmdBatch */

/* Per-iteration policy -------------------------------------------------------------------------- */

/* How the three stages are spread across threads. Chosen at ownership assignment and loop
 * construction, not per command. The PoC implements INLINE (one io-thread today: main walks all
 * three stages) and WORKERS (io-threads N: workers own read and write, main executes); SINGLE_IO
 * is a named value that asserts "not yet" until it is built. */
typedef enum {
    STAGE_FANOUT_INLINE,    /* main walks read, execute, and write itself */
    STAGE_FANOUT_SINGLE_IO, /* one IO thread owns read+write even on a single core (not yet) */
    STAGE_FANOUT_WORKERS,   /* IO workers own read+write; main executes */
} stageFanout;

/* What the read stage does with an eligible read. Consulted once per batch. PUNT_ALL and SPECULATE
 * are live; LOCKSTEP is a named value that asserts "not yet" until it is built. LOCKSTEP and
 * SPECULATE share every line of the read prefix and differ only in whether main is parked while the
 * prefix runs, which is a barrier decision in the handoff take, not a second reader. */
typedef enum {
    STAGE_READ_EXEC_PUNT_ALL,  /* every command crosses to main to execute */
    STAGE_READ_EXEC_LOCKSTEP,  /* workers run read prefixes while main parks, then a barrier (not yet) */
    STAGE_READ_EXEC_SPECULATE, /* eligible reads execute in the read stage, version-validated */
} stageReadExec;

typedef struct stagePolicy {
    stageFanout fanout;
    stageReadExec read_exec;
    uint8_t speculate_replica_only; /* speculate only while a replica: a predicate folded into the read gate */
} stagePolicy;

/* Derive the iteration's policy from the current configuration and role. Read once per iteration
 * (main) or per wake (IO thread) into a local; per-command code then switches on the local with no
 * pointer chase. A CONFIG SET that changes thread count takes effect at the next iteration through
 * the existing quiesce/reopen path, so reading this per iteration is sufficient.
 *
 * Defined in stage.c so the server-configuration read stays out of this header. */
stagePolicy stagePolicyRead(void);

/* Crossing asserts (debug builds only) --------------------------------------------------------- */

/* Only the connection's current owner may touch its owner-scoped state: append to its reply
 * stream, publish or take its batch, return it, or write it out. stageAssertOwner compares the
 * control's owner (single-writer, published by the owner) against this thread's identity.
 *
 * Compiled out of release builds via debugServerAssert (server.enable_debug_assert). The TSan CI
 * job is the release-build net under these. */
#define stageAssertOwner(cc)                                                                        \
    do {                                                                                            \
        debugServerAssert((cc)->owner_domain == stageSelf.domain &&                                 \
                          ((cc)->owner_domain != CC_OWNER_IO || (cc)->owner_tid == stageSelf.tid)); \
    } while (0)

/* A batch may be touched only by the thread that currently holds it. handoff publish/take set the
 * holder to main; handoff return sets it to the IO owner. Producer-after-publish and
 * consumer-after-return are the two mistakes this catches. */
#define stageAssertHolds(batch)                                                                     \
    do {                                                                                            \
        debugServerAssert((batch)->holder.domain == stageSelf.domain &&                             \
                          ((batch)->holder.domain != STAGE_DOMAIN_IO ||                             \
                           (batch)->holder.tid == stageSelf.tid));                                  \
    } while (0)

#endif /* STAGE_H */
