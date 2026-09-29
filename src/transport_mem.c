/* In-memory transport double for the staged event loop. See transport_mem.h. */

#include "server.h"
#include "queues.h"
#include "stage.h"
#include "transport_mem.h"

/* The stage asserts read cc->owner_domain/owner_tid and batch->holder. memControl and memBatch
 * name those fields identically to ClientControl and cmdBatch, so stageAssertOwner/stageAssertHolds
 * apply to the double unchanged, and their debugServerAssert gate (server.enable_debug_assert) is
 * the RED/GREEN switch the tests flip. */

memTransport *memTransportNew(uint16_t io_tid) {
    memTransport *tp = zcalloc(sizeof(*tp));
    tp->submit = zcalloc(sizeof(spscQueue));
    tp->ret = zcalloc(sizeof(spscQueue));
    spscInit(tp->submit, 16);
    spscInit(tp->ret, 16);
    (void)io_tid;
    return tp;
}

void memTransportFree(memTransport *tp) {
    if (!tp) return;
    spscFree(tp->submit);
    spscFree(tp->ret);
    zfree(tp->submit);
    zfree(tp->ret);
    zfree(tp);
}

memControl *memControlNew(uint8_t owner_domain, uint16_t owner_tid) {
    memControl *cc = zcalloc(sizeof(*cc));
    cc->owner_domain = owner_domain;
    cc->owner_tid = owner_tid;
    cc->lifecycle = MEM_ACTIVE;
    return cc;
}

void memControlFree(memControl *cc) {
    zfree(cc);
}

void memWake(memTransport *tp) {
    tp->woken = 1; /* explicit; no epoll readiness edge exists in the double */
}

int memWoken(const memTransport *tp) {
    return tp->woken;
}

/* handoff.publish: the IO owner assembles a batch for its own client and publishes it to main. The
 * batch is created held by the current thread (the IO owner) and its holder flips to main here. */
memBatch *memHandoffPublish(memTransport *tp, memControl *cc, int count) {
    memBatch *b = zcalloc(sizeof(*b));
    b->holder = (stageThreadId){STAGE_DOMAIN_IO, (uint16_t)stageSelf.tid};
    b->count = count;
    b->cc = cc;
    stageAssertOwner(cc);   /* only the current owner may publish its client's commands */
    stageAssertHolds(b);    /* and it must hold the batch it is publishing */
    cc->inflight += (uint32_t)count;
    b->holder = (stageThreadId){STAGE_DOMAIN_MAIN, 0}; /* published to main */
    spscEnqueue(tp->submit, b, true);
    memWake(tp);
    return b;
}

/* handoff.take: main dequeues one published batch and asserts it now holds it. */
memBatch *memHandoffTake(memTransport *tp) {
    void *item = NULL;
    if (spscDequeueBatch(tp->submit, &item, 1) != 1) return NULL;
    tp->woken = 0;
    memBatch *b = item;
    stageAssertHolds(b); /* main holds the published batch */
    return b;
}

/* handoff.return: main returns the executed batch to its IO owner; holder flips back and the
 * in-flight count drops. */
void memHandoffReturn(memTransport *tp, memBatch *b) {
    stageAssertHolds(b); /* main held it through execute */
    b->holder = (stageThreadId){STAGE_DOMAIN_IO, b->cc->owner_tid};
    if (b->cc->inflight >= (uint32_t)b->count) b->cc->inflight -= (uint32_t)b->count;
    spscEnqueue(tp->ret, b, true);
}

/* write.send: append reply bytes to the connection's owner-private staging buffer. Owner only. */
void memStageWriteSend(memControl *cc, const char *bytes, size_t n) {
    stageAssertOwner(cc); /* only the current owner may write this connection's output */
    if (cc->reply_len + n > sizeof(cc->reply)) n = sizeof(cc->reply) - cc->reply_len;
    memcpy(cc->reply + cc->reply_len, bytes, n);
    cc->reply_len += n;
}

/* Incident (b): a push from main (pubsub / tracking) into a client's reply buffer. Uses the same
 * owner-only guard as the write send; if main is not the owner the guard fires. */
void memReplyAppendFromMain(memControl *cc, const char *bytes, size_t n) {
    stageAssertOwner(cc); /* main may append only to a client main owns */
    if (cc->reply_len + n > sizeof(cc->reply)) n = sizeof(cc->reply) - cc->reply_len;
    memcpy(cc->reply + cc->reply_len, bytes, n);
    cc->reply_len += n;
}

/* Incident (a): set one bit in the shared flags word. Owner only, so the word is never RMW'd from
 * two domains at once. With the guard stubbed a cross-domain caller proceeds and its read-modify-
 * write erases the other domain's bit. */
void memFlagsOwnerSet(memControl *cc, uint64_t bit) {
    stageAssertOwner(cc);
    cc->flags |= bit;
}

/* Incident (c): register the connection's fd into an event loop. Only the owner may, and only when
 * nothing is in flight (a registration mid-sweep races the worker reading its events[]). */
void memRegisterFd(memControl *cc) {
    stageAssertOwner(cc);
    debugServerAssert(cc->inflight == 0); /* no fd (re)registration while commands are in flight */
    cc->fd_registered = 1;
}

/* The single path that changes ownership. Lifecycle is asserted monotonic and the old owner must
 * have drained (nothing in flight) before the handoff. */
void memTransferOwnership(memControl *cc, uint8_t to_domain, uint16_t to_tid, uint8_t to_lifecycle) {
    stageAssertOwner(cc);                       /* only the current owner hands off */
    debugServerAssert(cc->inflight == 0);       /* drained before transfer */
    debugServerAssert(to_lifecycle >= cc->lifecycle); /* monotonic: ACTIVE -> LEAVING/CLOSING -> DETACHED */
    cc->owner_domain = to_domain;
    cc->owner_tid = to_tid;
    cc->lifecycle = to_lifecycle;
}

/* Incident (d): a config-shrink retiring a worker must not stomp an owned, non-drained client's
 * state. Retiring goes through the transfer path, so the monotonic-lifecycle + drained asserts are
 * the guard; a raw reset of an ACTIVE owned client trips them. */
void memWorkerShrinkReset(memControl *cc) {
    /* Model the shrink as retiring the client back to main via the guarded transfer. An ACTIVE
     * client with commands in flight cannot be retired: the asserts fire. */
    memTransferOwnership(cc, CC_OWNER_MAIN, 0, MEM_DETACHED);
}
