/* In-memory transport double for the staged event loop.
 *
 * The socket-level connection already has a fake path through ConnectionType; this swaps the layer
 * above it -- the thread-to-thread carrier -- for an in-memory one: SPSC rings, a small owner
 * registry, and an explicit wake() a test calls instead of an epoll readiness edge. No sockets, no
 * epoll, no threads. Stage bodies see neither swap.
 *
 * The point is to drive the stage crossing operations (handoff publish/take/return, the owner-only
 * reply append, ownership transfer, fd registration, worker shrink) with a thread identity a test
 * sets by hand (stageSetSelf), so the four cross-thread incidents from the interface sketch can be
 * reproduced without spawning a single thread. Each operation carries the same owner/holder guard
 * the real transport asserts, so a test is RED (silent corruption) with the guard stubbed out
 * (server.enable_debug_assert == 0) and GREEN (the guard fires) with it in.
 */

#ifndef TRANSPORT_MEM_H
#define TRANSPORT_MEM_H

#include <stdint.h>
#include <stddef.h>
#include "stage_thread.h"

/* Lifecycle values, monotonic in transfer, mirroring the fast-path FP_* order. */
typedef enum {
    MEM_ACTIVE = 0,
    MEM_LEAVING = 1,
    MEM_CLOSING = 2,
    MEM_DETACHED = 3,
} memLifecycle;

/* A connection's shared control in the double: owner identity, lifecycle, an in-flight count, and
 * a single flags word two domains could race (the incident-(a) hazard). Deliberately tiny. */
typedef struct memControl {
    uint8_t owner_domain; /* CC_OWNER_MAIN / CC_OWNER_IO, compared against stageSelf */
    uint16_t owner_tid;
    uint8_t lifecycle;   /* memLifecycle, asserted monotonic across a transfer */
    uint32_t inflight;   /* commands published to main and not yet returned; a transfer needs this 0 */
    uint64_t flags;      /* one word; main and an IO owner each want a bit in it (incident a) */
    uint8_t fd_registered; /* whether this connection's fd is in an event loop (incident c) */
    /* An owner-private reply staging buffer; only the current owner may append (incident b). */
    char reply[256];
    size_t reply_len;
} memControl;

/* Flags-word bits: one written by main, one by the IO owner. In one word so a stubbed guard lets a
 * cross-thread RMW erase the other (incident a). */
#define MEM_FLAG_MAIN_PENDING_WRITE (1ull << 0) /* main-written */
#define MEM_FLAG_IO_DEFERRED (1ull << 1)        /* IO-owner-written */

/* A minimal batch that crosses the rings, carrying the holder tag the stage asserts check. */
typedef struct memBatch {
    stageThreadId holder;
    int count;
    memControl *cc; /* the connection this batch's commands belong to */
} memBatch;

/* One IO thread's two SPSC rings plus a wake flag the test raises explicitly. */
typedef struct memTransport {
    void *submit_buf[16];
    void *ret_buf[16];
    struct spscQueue *submit; /* IO owner -> main */
    struct spscQueue *ret;    /* main -> IO owner */
    int woken;                /* raised by wake(), cleared by the consumer; no epoll edge */
} memTransport;

/* Lifecycle of the double itself. */
memTransport *memTransportNew(uint16_t io_tid);
void memTransportFree(memTransport *tp);
memControl *memControlNew(uint8_t owner_domain, uint16_t owner_tid);
void memControlFree(memControl *cc);

/* Explicit readiness: a test calls this instead of the kernel signalling the fd. */
void memWake(memTransport *tp);
int memWoken(const memTransport *tp);

/* Stage crossing operations, each guarded. Caller sets stageSelf first to impersonate a thread. */
memBatch *memHandoffPublish(memTransport *tp, memControl *cc, int count); /* IO owner -> main */
memBatch *memHandoffTake(memTransport *tp);                               /* main takes one */
void memHandoffReturn(memTransport *tp, memBatch *b);                     /* main -> IO owner */
void memStageWriteSend(memControl *cc, const char *bytes, size_t n);      /* owner-only reply append */
void memReplyAppendFromMain(memControl *cc, const char *bytes, size_t n); /* incident (b): a main push */
void memFlagsOwnerSet(memControl *cc, uint64_t bit);                      /* incident (a): owner-only flag set */
void memRegisterFd(memControl *cc);                                       /* incident (c): owner-only, inflight 0 */
void memTransferOwnership(memControl *cc, uint8_t to_domain, uint16_t to_tid, uint8_t to_lifecycle);
void memWorkerShrinkReset(memControl *cc);                                /* incident (d): retire an owned client */

#endif /* TRANSPORT_MEM_H */
