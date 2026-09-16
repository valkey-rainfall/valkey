/* pwtrace.h -- DEBUG-ONLY instrumentation for the feature-1
 * (io-threads-ownership no + speculative reads) clients_pending_write
 * corruption crash (adlist.c:195/203, networking.c handleClientsWithPendingWrites
 * serverAssert(c->flag.pending_write)).
 *
 * Records every mutation of a client's clients_pending_write list membership
 * into a small per-client ring and a global ring, so a crash dump can show
 * the exact sequence of link/unlink calls (site, thread, prior flag/state,
 * client id) leading up to the corruption. Zero runtime cost when
 * PWTRACE_ENABLED is not defined at compile time (it is, by default, in this
 * branch -- see the Makefile note below). Not intended to ship upstream;
 * this whole mechanism should be reverted once the race is confirmed or
 * ruled out.
 *
 * To disable: comment out PWTRACE_ENABLED below and rebuild.
 */
#ifndef PWTRACE_H
#define PWTRACE_H

#include <stdint.h>

#define PWTRACE_ENABLED 1

#define PWTRACE_RING_SIZE 8
#define PWTRACE_GLOBAL_RING_SIZE 64

/* One entry per clients_pending_write mutation (link or unlink). */
typedef enum {
    PWTRACE_NONE = 0,
    PWTRACE_PUT_QUEUE,        /* putClientInPendingWriteQueue: link */
    PWTRACE_TRYSEND_UNLINK,   /* trySendWriteToIOThreads (io_threads.c): unlink */
    PWTRACE_HANDLE_PW_UNLINK, /* handleClientsWithPendingWrites: unlink */
    PWTRACE_HANDLE_PW_HEAL_UNLINK, /* handleClientsWithPendingWrites clientWriteIsWaiting heal: unlink */
    PWTRACE_UNLINK_CLIENT,    /* unlinkClient: unlink (client teardown) */
    PWTRACE_BEFORENEXT_F8_UNLINK, /* beforeNextClient F8 owned-staging path: unlink */
    PWTRACE_B13_ARM_UNLINK,   /* b13ArmOwnerWriteHandler: unlink */
    PWTRACE_MODULE_LINK,      /* module.c RM_* write scheduling: link */
} pwTraceSite;

typedef struct pwTraceEntry {
    uint64_t seq;         /* global monotonic sequence number */
    uint64_t client_id;   /* c->id (0 for entries that somehow lack a client) */
    pwTraceSite site;
    uint8_t is_main;       /* inMainThread() at the time of the call */
    uint8_t tid;           /* getCurTid() at the time of the call */
    uint8_t pending_write_before; /* c->flag.pending_write BEFORE this mutation */
    uint8_t io_write_state;       /* c->io_write_state at the time of the call */
    uint8_t io_read_state;        /* c->io_read_state at the time of the call */
    uint8_t is_link;              /* 1 = link, 0 = unlink */
} pwTraceEntry;

struct client;

/* Record one mutation: updates the client's own ring (if c != NULL) and the
 * global ring unconditionally. Call BEFORE performing the actual
 * list link/unlink so pending_write_before/io_*_state reflect pre-mutation
 * state. */
void pwTraceRecord(struct client *c, pwTraceSite site, int is_link);

/* Print both rings (global, and the given client's if non-NULL) to the
 * server log. Safe to call from a signal-adjacent assertion-failure path:
 * only touches already-populated plain memory, no locks, no allocation. */
void pwTraceDump(struct client *c);

/* Set by pwTraceRecord the first time it runs; pwTraceDump / the assert
 * hook use this to skip printing empty rings (e.g. ownership-on runs that
 * never touch this path, or a crash unrelated to clients_pending_write). */
extern int pwtrace_populated;

/* Set by handleClientsWithPendingWrites right before the serverAssert on
 * c->flag.pending_write (that macro carries no context of its own), so the
 * assert-failure hook can dump this client's per-client ring. May be stale
 * or point at a freed client by the time it's read in other assert paths --
 * only trust it immediately after a serverAssert(c->flag.pending_write)
 * failure at that specific call site. NULL otherwise. */
extern struct client *pwtrace_last_client;

#endif
