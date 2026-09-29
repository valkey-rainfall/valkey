/* Staged event loop: identity storage and per-iteration policy derivation. */

#include "server.h"
#include "stage.h"

/* The socket-owning stage domains are laid out to equal the control plane's owner tags, so a
 * stageDomain compares directly against ClientControl.owner_domain with no translation. Assert that
 * correspondence here rather than coupling stage_thread.h to fastpath.h. */
static_assert((int)STAGE_DOMAIN_MAIN == CC_OWNER_MAIN, "STAGE_DOMAIN_MAIN must equal CC_OWNER_MAIN");
static_assert((int)STAGE_DOMAIN_IO == CC_OWNER_IO, "STAGE_DOMAIN_IO must equal CC_OWNER_IO");

/* Every thread starts as main until it announces otherwise; an IO worker calls stageSetSelf at its
 * start, so a thread that never does is main by construction. */
__thread stageThreadId stageSelf = {STAGE_DOMAIN_MAIN, 0};

stagePolicy stagePolicyRead(void) {
    stagePolicy p;

    /* One active IO thread means main walks all three stages inline; more than one means the
     * workers own read and write and main executes. active_io_threads_num counts main, so the
     * boundary is at 1. */
    p.fanout = (server.active_io_threads_num <= 1) ? STAGE_FANOUT_INLINE : STAGE_FANOUT_WORKERS;

    /* The read stage speculates eligible reads when the fast path is on; otherwise every command
     * crosses to main. The replica-only knob is carried as a predicate, not a third read_exec
     * value, so the read gate applies it without changing the stage the loop is in. */
    p.read_exec = server.io_threads_fast_path ? STAGE_READ_EXEC_SPECULATE : STAGE_READ_EXEC_PUNT_ALL;
    p.speculate_replica_only = server.io_threads_speculation_replica_only ? 1 : 0;

    return p;
}
