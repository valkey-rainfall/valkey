/*
 * io_uring_batch.c -- batch client read(2)/write(2) into io_uring.
 * See io_uring_batch.h for the scope statement.
 *
 * DESIGN NOTE (PoC)
 *
 * Valkey's event loop costs, for N ready clients per iteration:
 *      1 epoll_wait + N read() + N write()  =  2N+1 syscalls
 * With this module it costs:
 *      1 epoll_wait + 1 io_uring_enter(reads) + 1 io_uring_enter(writes) = 3
 * The kernel completes MSG_DONTWAIT recv/send on a ready TCP socket inline
 * during io_uring_enter, so the batch is fully reaped when submit returns
 * (io_uring_submit_and_wait(nqueued) covers the rare deferred case).
 *
 * Two hosts of the same mechanism:
 *   - Main thread (io-threads 1, or clients main keeps for itself): reads are
 *     queued from readQueryFromClient() and flushed from the ae after-events
 *     hook; writes are queued from handleClientsWithPendingWrites() and
 *     flushed at its end.
 *   - I/O threads (io-threads > 1): the worker loop dequeues a share of the
 *     pending read/write jobs instead of one, queues each into the thread's
 *     own ring, and flushes once per direction. The per-job tails
 *     (ioThreadReadQueryFromClientTail, the write completion + hand-back)
 *     then run exactly as they would have after a synchronous syscall.
 *   Every thread has its own ring, slots, buffer pool and counters; nothing
 *   here is shared between threads.
 *
 * What this module does NOT do:
 *   - It does not replace epoll. Readiness still comes from aeApiPoll; this
 *     only removes the per-client data-movement syscalls. A multishot-recv +
 *     provided-buffer-ring design would also remove epoll_wait and the
 *     per-fd poll bookkeeping, but requires changing query-buffer ownership
 *     and is out of scope for a measurement PoC.
 *   - It does not batch replica or primary links, TLS or RDMA connections,
 *     or big-arg (bulk >= 32KB) reads. All of those take the unchanged
 *     synchronous path.
 *   - Reply-list / encoded writes are batched as a single sendmsg of up to
 *     IOU_WIOV iovecs; if that does not drain the client the ordinary write
 *     handler continues on the next iteration (same as a short writev).
 *
 * Correctness invariants, in the order they bit during design:
 *   1. All CQEs of a batch are reaped and their results applied to client
 *      state (nread/sdsIncrLen, nwritten) BEFORE any client handler runs.
 *      Otherwise a handler that re-enters the event loop (processEvents
 *      WhileBlocked) could read()/write() a client whose batched result is
 *      not yet accounted and corrupt its buffers.
 *   2. A handler for client A may free client B synchronously (CLIENT KILL,
 *      maxclients...). freeClient() calls ioUringBatchClientFreed(B), which
 *      detaches B's slot so its deferred handler is skipped and, if B was
 *      reading into a pooled buffer, hands the buffer back before
 *      freeClient's sdsfree() would double-free it. (Main thread only: a
 *      client inside an I/O-thread batch has io_*_state == PENDING_IO and
 *      freeClient() defers it with freeClientAsync().)
 *   3. thread_shared_qb is a single per-thread scratch buffer, so batched
 *      reads cannot use it: N recvs need N distinct destinations. Batched
 *      clients with a NULL querybuf borrow from a per-thread pool with the
 *      same "give it back if fully consumed, else the client takes
 *      ownership" semantics as resetSharedQueryBuf().
 *   4. Nothing is batched while a nested event loop is running
 *      (ProcessingEventsWhileBlocked) or while a flush is in progress.
 *   5. Main-thread reads are flushed from the ae after-events hook, i.e.
 *      after the fired fd loop and BEFORE time events (serverCron ->
 *      clientsCronResizeQueryBuffer may realloc a query buffer, which must
 *      not happen with an SQE pointing into it).
 *   6. Inside an I/O thread the job is counted finished (io_jobs_finished)
 *      only after the flush that completes it, so drainIOThreadsQueue()
 *      keeps its meaning.
 */
#include "server.h"
#include "io_uring_batch.h"
#include "reply_iov.h"
#include "io_threads.h"

/* Per-thread counters; index 0 is the main thread. Summed for INFO. */
static ioUringBatchStats stats_per_thread[IO_THREADS_MAX_NUM];

void ioUringBatchStatsTotal(ioUringBatchStats *out) {
    memset(out, 0, sizeof(*out));
    for (int t = 0; t < IO_THREADS_MAX_NUM; t++) {
        ioUringBatchStats *s = &stats_per_thread[t];
        out->read_batches += s->read_batches;
        out->read_sqes += s->read_sqes;
        out->write_batches += s->write_batches;
        out->write_sqes += s->write_sqes;
        out->writev_sqes += s->writev_sqes;
        out->fallback_reads += s->fallback_reads;
        out->fallback_writes += s->fallback_writes;
        out->cancelled += s->cancelled;
        if (s->max_read_batch > out->max_read_batch) out->max_read_batch = s->max_read_batch;
        if (s->max_write_batch > out->max_write_batch) out->max_write_batch = s->max_write_batch;
    }
}

#ifdef HAVE_LIBURING
#include <liburing.h>
#include <sys/socket.h>
#include <limits.h>

/* networking.c internals reused by the deferred completion paths. */
extern int ProcessingEventsWhileBlocked;
extern _Thread_local sds thread_shared_qb;
int handleReadResult(client *c);
void trimCommandQueue(client *c);
int postWriteToClient(client *c);
void installClientWriteHandler(client *c);
void ioThreadReadQueryFromClientTail(client *c);

/* Ring depth. If more clients are ready in one iteration than fit, the
 * overflow simply takes the synchronous path (counted as fallback). */
#define IOU_DEPTH 4096
/* iovecs per batched reply-list write. 16KB c->buf + reply blocks of 16KB
 * each: 32 covers ~500KB of pending reply, more than NET_MAX_WRITES_PER_EVENT.
 * Encoded buffers need 3 iovecs per bulk string. */
#define IOU_WIOV 32
/* Reply-list write contexts allocated lazily per thread; beyond this the
 * client falls back to writev(2). */
#define IOU_WCTX_MAX 1024

typedef struct iouWriteCtx {
    struct iovec iov[IOU_WIOV];
    char prefixes[IOU_WIOV / NUM_OF_IOV_PER_BULK_STR + 1][BULK_STR_LEN_PREFIX_MAX_SIZE];
    char crlf[2];
    bufWriteMetadata meta[IOU_WIOV + 1];
    replyIOV reply;
    int bufcnt;
    struct msghdr msg;
} iouWriteCtx;

typedef struct iouSlot {
    client *c;       /* NULL once cancelled */
    sds buf;         /* read: pooled query buffer lent to the client, else NULL */
    iouWriteCtx *wc; /* write: reply-list context, NULL for a c->buf send */
    int res;         /* CQE result (bytes or -errno) */
} iouSlot;

typedef struct iouThread {
    struct io_uring ring;
    int ok;
    int in_flush;
    int tid; /* 0 = main */
    iouSlot *rslots;
    int nreads, rcap;
    iouSlot *wslots;
    int nwrites, wcap;
    int last_pool_demand; /* pooled buffers handed out in the last batch */
    int pool_keep;        /* decaying high-water mark of that demand */
    /* Query-buffer pool for batched reads (see invariant 3). */
    sds *qb_pool;
    int qb_pool_len, qb_pool_cap;
    /* Reply-list write contexts. */
    iouWriteCtx **wctx_pool;
    int wctx_pool_len, wctx_pool_cap, wctx_total;
} iouThread;

static _Thread_local iouThread *T = NULL;

static void flushAll(void);
static void finishReadIOThread(iouSlot *s);
static void finishWriteIOThread(client *c);

#define STATS (stats_per_thread[T->tid])

static sds qbPoolTake(void) {
    if (T->qb_pool_len > 0) return T->qb_pool[--T->qb_pool_len];
    sds s = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(s);
    return s;
}

static void qbPoolReturn(sds s) {
    sdsclear(s);
    if (T->qb_pool_len == T->qb_pool_cap) {
        T->qb_pool_cap = T->qb_pool_cap ? T->qb_pool_cap * 2 : 64;
        T->qb_pool = zrealloc(T->qb_pool, sizeof(sds) * T->qb_pool_cap);
    }
    T->qb_pool[T->qb_pool_len++] = s;
}

static iouWriteCtx *wctxTake(void) {
    if (T->wctx_pool_len > 0) return T->wctx_pool[--T->wctx_pool_len];
    if (T->wctx_total >= IOU_WCTX_MAX) return NULL;
    T->wctx_total++;
    iouWriteCtx *w = zmalloc(sizeof(*w));
    w->crlf[0] = '\r';
    w->crlf[1] = '\n';
    return w;
}

static void wctxReturn(iouWriteCtx *w) {
    if (T->wctx_pool_len == T->wctx_pool_cap) {
        T->wctx_pool_cap = T->wctx_pool_cap ? T->wctx_pool_cap * 2 : 64;
        T->wctx_pool = zrealloc(T->wctx_pool, sizeof(*T->wctx_pool) * T->wctx_pool_cap);
    }
    T->wctx_pool[T->wctx_pool_len++] = w;
}

static int growSlots(iouSlot **slots, int *cap, int need) {
    if (need < *cap) return 1;
    if (*cap >= IOU_DEPTH) return 0;
    int ncap = *cap * 2;
    if (ncap > IOU_DEPTH) ncap = IOU_DEPTH;
    *slots = zrealloc(*slots, sizeof(iouSlot) * ncap);
    memset(*slots + *cap, 0, sizeof(iouSlot) * (ncap - *cap));
    *cap = ncap;
    return 1;
}

/* Bound the pool by a high-water mark of per-batch demand that decays by
 * 1/16 of the gap each batch: a steady load never reallocates even though
 * batch sizes jitter, and a burst's buffers are released within a few dozen
 * batches instead of being pinned for the life of the thread. */
static void qbPoolTrim(void) {
    int d = T->last_pool_demand;
    if (d > T->pool_keep)
        T->pool_keep = d;
    else
        T->pool_keep -= (T->pool_keep - d) >> 4;
    int keep = T->pool_keep > 8 ? T->pool_keep : 8;
    while (T->qb_pool_len > keep) sdsfree(T->qb_pool[--T->qb_pool_len]);
    T->last_pool_demand = 0;
}

static int threadInit(int tid) {
    if (!server.io_uring_enabled) return 0;
    if (T) return T->ok;
    iouThread *t = zcalloc(sizeof(*t));
    t->tid = tid;
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    /* Each ring has exactly one submitter and reaper (its thread); these
     * flags let the kernel skip the cross-task completion machinery. Fall
     * back to a plain ring on kernels that predate them (< 6.1). */
    p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN;
    int rc = io_uring_queue_init_params(IOU_DEPTH, &t->ring, &p);
    if (rc < 0) {
        memset(&p, 0, sizeof(p));
        rc = io_uring_queue_init_params(IOU_DEPTH, &t->ring, &p);
    }
    if (rc < 0) {
        serverLog(LL_WARNING, "io-uring requested but io_uring_queue_init failed (thread %d): %s; falling back to read/write",
                  tid, strerror(-rc));
        zfree(t);
        return 0;
    }
    /* Slot arrays start small and grow with demand so an idle or lightly
     * loaded server does not carry IOU_DEPTH slots per thread. */
    t->rcap = t->wcap = 64;
    t->rslots = zcalloc(sizeof(iouSlot) * t->rcap);
    t->wslots = zcalloc(sizeof(iouSlot) * t->wcap);
    t->ok = 1;
    T = t;
    if (tid == 0)
        serverLog(LL_NOTICE, "io_uring batching enabled (depth %d, sq %u, cq %u, flags 0x%x)", IOU_DEPTH, p.sq_entries,
                  p.cq_entries, p.flags);
    else
        serverLog(LL_VERBOSE, "io_uring batching enabled on io thread %d", tid);
    return 1;
}

static void threadFree(void) {
    if (!T) return;
    if (T->ok) io_uring_queue_exit(&T->ring);
    for (int i = 0; i < T->qb_pool_len; i++) sdsfree(T->qb_pool[i]);
    zfree(T->qb_pool);
    for (int i = 0; i < T->wctx_pool_len; i++) zfree(T->wctx_pool[i]);
    zfree(T->wctx_pool);
    zfree(T->rslots);
    zfree(T->wslots);
    zfree(T);
    T = NULL;
}

int ioUringBatchInit(void) {
    return threadInit(0);
}

void ioUringBatchFree(void) {
    threadFree();
}

int ioUringBatchInitThread(int tid) {
    return threadInit(tid);
}

void ioUringBatchFreeThread(void) {
    threadFree();
}

int ioUringBatchActive(void) {
    return T != NULL && T->ok;
}

/* Common eligibility for both hosts: plain socket, no flush in progress. */
static inline int batchableConn(client *c) {
    if (!T || !T->ok || T->in_flush) return 0;
    if (!c->conn) return 0;
    int t = connGetType(c->conn);
    if (t != CONN_TYPE_SOCKET && t != CONN_TYPE_UNIX) return 0;
    if (c->conn->fd < 0 || connGetState(c->conn) != CONN_STATE_CONNECTED) return 0;
    if (c->io_uring_slot != -1) return 0; /* already in a batch */
    return 1;
}

/* Main-thread eligibility: additionally no nested loop and the client is not
 * owned by an I/O thread right now. */
static inline int batchableMain(client *c) {
    if (ProcessingEventsWhileBlocked) return 0;
    if (!batchableConn(c)) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    return 1;
}

/* CQE user_data: slot index with the direction in the top bit. */
#define UD_WRITE ((uintptr_t)1 << 63)
#define UD_READ(i) ((void *)(uintptr_t)(i))
#define UD_WRITE_OF(i) ((void *)(((uintptr_t)(i)) | UD_WRITE))

/* Submit everything queued in BOTH directions and reap all of it. Both
 * arrays are then fully accounted; callers apply results and run tails in
 * whatever order they need. */
static void submitAndReap(void) {
    int want = T->nreads + T->nwrites;
    if (want == 0) return;
    int got = 0;
    int rc = io_uring_submit_and_wait(&T->ring, want);
    if (rc < 0 && rc != -EINTR && rc != -EBUSY) {
        serverLog(LL_WARNING, "io_uring_submit_and_wait: %s", strerror(-rc));
    }
    while (got < want) {
        struct io_uring_cqe *cqe;
        unsigned head, n = 0;
        io_uring_for_each_cqe(&T->ring, head, cqe) {
            uintptr_t ud = (uintptr_t)io_uring_cqe_get_data(cqe);
            iouSlot *slots = (ud & UD_WRITE) ? T->wslots : T->rslots;
            slots[ud & ~UD_WRITE].res = cqe->res;
            n++;
        }
        io_uring_cq_advance(&T->ring, n);
        got += n;
        if (got < want) {
            rc = io_uring_wait_cqe(&T->ring, &cqe);
            if (rc < 0 && rc != -EINTR) {
                serverLog(LL_WARNING, "io_uring_wait_cqe: %s", strerror(-rc));
                /* Mark the stragglers as EAGAIN so callers retry via epoll. */
                for (int i = 0; i < T->nreads; i++)
                    if (T->rslots[i].res == INT_MIN) T->rslots[i].res = -EAGAIN;
                for (int i = 0; i < T->nwrites; i++)
                    if (T->wslots[i].res == INT_MIN) T->wslots[i].res = -EAGAIN;
                break;
            }
        }
    }
}

/* ---------------------------------------------------------------- reads */

/* Prepare the client's query buffer and queue a recv SQE. Shared by both
 * hosts; the caller has already decided the client is eligible. */
static int queueRecv(client *c) {
    if (c->flag.close_asap || c->flag.primary || isReplicatedClient(c)) return 0;
    /* Big-arg exact-size reads keep the synchronous path (they size the
     * read to the bulk boundary so the parser can steal the buffer). */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1 && c->bulklen >= PROTO_MBULK_BIG_ARG)
        return 0;
    if (T->nreads + T->nwrites >= IOU_DEPTH) return 0;
    if (!growSlots(&T->rslots, &T->rcap, T->nreads)) return 0;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&T->ring);
    if (!sqe) return 0;

    iouSlot *s = &T->rslots[T->nreads];
    s->c = c;
    s->buf = NULL;
    s->wc = NULL;
    s->res = INT_MIN;

    /* Mirror readToQueryBuf()'s non-big-arg sizing. */
    size_t readlen = PROTO_IOBUF_LEN;
    size_t qblen = c->querybuf ? sdslen(c->querybuf) : 0;
    if (c->querybuf == NULL) {
        c->querybuf = qbPoolTake();
        s->buf = c->querybuf;
        qblen = 0;
        T->last_pool_demand++;
    }
    if (sdsalloc(c->querybuf) < PROTO_IOBUF_LEN) {
        c->querybuf = sdsMakeRoomForNonGreedy(c->querybuf, readlen);
        if (c->querybuf_peak < qblen + readlen) c->querybuf_peak = qblen + readlen;
    } else {
        c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
        readlen = sdsavail(c->querybuf);
    }
    /* If the pooled buffer was reallocated it is gone (freed by sds); the
     * client now owns the new allocation. */
    if (s->buf && c->querybuf != s->buf) s->buf = NULL;

    io_uring_prep_recv(sqe, c->conn->fd, c->querybuf + qblen, readlen, MSG_DONTWAIT);
    io_uring_sqe_set_data(sqe, UD_READ(T->nreads));
    c->io_uring_slot = T->nreads;
    T->nreads++;
    STATS.read_sqes++;
    return 1;
}

int ioUringBatchQueueRead(client *c) {
    if (batchableMain(c) && queueRecv(c)) return 1;
    if (T) STATS.fallback_reads++;
    return 0;
}

int ioUringBatchQueueIOThreadRead(client *c) {
    if (batchableConn(c) && queueRecv(c)) return 1;
    if (T) STATS.fallback_reads++;
    return 0;
}

/* Apply one recv CQE to the client, replicating connSocketRead() +
 * readToQueryBuf()'s post-read accounting. */
static void applyReadResult(iouSlot *s) {
    client *c = s->c;
    connection *conn = c->conn;
    int res = s->res;
    if (res > 0) {
        c->nread = res;
        sdsIncrLen(c->querybuf, res);
        size_t qblen = sdslen(c->querybuf);
        if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
        size_t qb_memory = qblen + (c->mstate ? c->mstate->argv_len_sums : 0);
        if (qb_memory > server.client_max_querybuf_len ||
            (qb_memory > 1024 * 1024 && (c->read_flags & READ_FLAGS_AUTH_REQUIRED))) {
            c->read_flags |= READ_FLAGS_QB_LIMIT_REACHED;
        }
    } else if (res == 0) {
        c->nread = 0;
        conn->state = CONN_STATE_CLOSED;
    } else {
        c->nread = -1;
        if (res != -EAGAIN) {
            conn->last_errno = -res;
            if (res != -EINTR && conn->state == CONN_STATE_CONNECTED) conn->state = CONN_STATE_ERROR;
        }
    }
}

/* The tail of readQueryFromClient() for a non-replicated client (main). */
static void finishReadMain(iouSlot *s) {
    client *c = s->c;
    if (handleReadResult(c) == C_OK) {
        if (processInputBuffer(c) == C_ERR) return; /* client freed */
        trimCommandQueue(c);
    }
    /* handleReadResult may have freeClientAsync'd; the client object is
     * still valid until beforeSleep, and beforeNextClient() is safe. */
    beforeNextClient(c);
}

/* Run the per-client read tail with the pooled buffer playing the role of
 * thread_shared_qb so every existing hand-back rule applies:
 * resetSharedQueryBuf() before command execution, trimClientQueryBuffer()
 * in beforeNextClient()/the io-thread tail, sdsclear-not-free in
 * freeClient(). If the client keeps unconsumed bytes it takes ownership and
 * initSharedQueryBuf() allocates a fresh buffer, which we keep for the pool. */
static void runReadTail(iouSlot *s, void (*tail)(iouSlot *)) {
    sds saved = thread_shared_qb;
    if (s->buf) thread_shared_qb = s->buf;
    tail(s);
    if (s->buf) {
        if (thread_shared_qb != s->buf) {
            qbPoolReturn(thread_shared_qb); /* fresh one from initSharedQueryBuf */
            s->buf = NULL;                  /* client owns the old one now */
        } else if (s->c && s->c->querybuf == s->buf) {
            s->buf = NULL; /* defensive: client still holds it -> ownership */
        }
        thread_shared_qb = saved;
    }
    if (s->c) s->c->io_uring_slot = -1;
}

/* Apply results + run tails for the reads that were part of the last
 * submitAndReap(). T->in_flush must be held by the caller. */
static void completeReads(void (*tail)(iouSlot *)) {
    int n = T->nreads;
    if (n == 0) return;
    STATS.read_batches++;
    if (n > STATS.max_read_batch) STATS.max_read_batch = n;

    for (int i = 0; i < n; i++) {
        iouSlot *s = &T->rslots[i];
        if (s->c) runReadTail(s, tail);
        /* Not processed (cancelled) or processed and handed back. */
        if (s->buf) qbPoolReturn(s->buf);
        s->c = NULL;
        s->buf = NULL;
    }
    T->nreads = 0;
    qbPoolTrim();
}

void ioUringBatchFlushReads(struct aeEventLoop *el) {
    UNUSED(el);
    flushAll();
}

static void finishReadIOThread(iouSlot *s) {
    ioThreadReadQueryFromClientTail(s->c);
}

/* --------------------------------------------------------------- writes */

/* Queue a send (static buffer) or sendmsg (reply list / encoded) SQE. */
static int queueSend(client *c) {
    if (getClientType(c) == CLIENT_TYPE_REPLICA || c->flag.primary) return 0;
    if (T->nreads + T->nwrites >= IOU_DEPTH) return 0;
    if (!growSlots(&T->wslots, &T->wcap, T->nwrites)) return 0;

    listNode *lastblock;
    size_t bufpos;
    if (T->tid == 0) {
        lastblock = listLast(c->reply);
        bufpos = (size_t)c->bufpos;
    } else {
        bufpos = c->io_last_reply_block ? (size_t)c->bufpos : c->io_last_bufpos;
        lastblock = c->io_last_reply_block;
    }

    iouSlot *s = &T->wslots[T->nwrites];
    s->c = c;
    s->buf = NULL;
    s->wc = NULL;
    s->res = INT_MIN;

    if (lastblock || c->flag.buf_encoded) {
        /* Reply list / encoded: one sendmsg over the same iovec writev(2)
         * would have used. */
        iouWriteCtx *w = wctxTake();
        if (!w) return 0;
        w->bufcnt = buildReplyIOV(c, IOU_WIOV, w->iov, w->prefixes, w->crlf, &w->reply, w->meta);
        if (w->reply.iovcnt == 0) {
            wctxReturn(w);
            return 0;
        }
        struct io_uring_sqe *sqe = io_uring_get_sqe(&T->ring);
        if (!sqe) {
            wctxReturn(w);
            return 0;
        }
        memset(&w->msg, 0, sizeof(w->msg));
        w->msg.msg_iov = w->reply.iov;
        w->msg.msg_iovlen = w->reply.iovcnt;
        io_uring_prep_sendmsg(sqe, c->conn->fd, &w->msg, MSG_DONTWAIT | MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, UD_WRITE_OF(T->nwrites));
        s->wc = w;
        STATS.writev_sqes++;
    } else {
        if (bufpos == 0) return 0;
        if (c->io_last_written.data_len != 0 && c->io_last_written.buf != c->buf) return 0;
        struct io_uring_sqe *sqe = io_uring_get_sqe(&T->ring);
        if (!sqe) return 0;
        size_t off = c->io_last_written.data_len;
        io_uring_prep_send(sqe, c->conn->fd, c->buf + off, bufpos - off, MSG_DONTWAIT | MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, UD_WRITE_OF(T->nwrites));
    }
    c->io_uring_slot = T->nwrites;
    T->nwrites++;
    STATS.write_sqes++;
    return 1;
}

int ioUringBatchQueueWrite(client *c) {
    if (batchableMain(c) && queueSend(c)) return 1;
    if (T) STATS.fallback_writes++;
    return 0;
}

int ioUringBatchQueueIOThreadWrite(client *c) {
    if (batchableConn(c) && !(c->write_flags & WRITE_FLAGS_IS_REPLICA) && queueSend(c)) return 1;
    if (T) STATS.fallback_writes++;
    return 0;
}

/* Replicates connSocketWrite()/connSocketWritev() error mapping plus
 * _writeToClient()'s or writevToClient()'s bookkeeping for one CQE. */
static void applyWriteResult(iouSlot *s) {
    client *c = s->c;
    connection *conn = c->conn;
    int res = s->res;
    c->nwritten = 0;
    /* writeToClient() zeroes write_flags before writing; the io-thread path
     * keeps the flags main set (IS_REPLICA, which we never batch). */
    if (T->tid == 0)
        c->write_flags = 0;
    else
        c->write_flags &= ~WRITE_FLAGS_WRITE_ERROR;
    if (res < 0 && res != -EAGAIN) {
        conn->last_errno = -res;
        if (res != -EINTR && conn->state == CONN_STATE_CONNECTED) conn->state = CONN_STATE_ERROR;
    }
    if (s->wc) {
        iouWriteCtx *w = s->wc;
        applyReplyIOVWritten(c, &w->reply, w->meta, w->bufcnt, res == 0 ? 0 : (res < 0 ? -1 : res));
        wctxReturn(w);
        s->wc = NULL;
        return;
    }
    if (res <= 0) {
        c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
        c->nwritten = res == 0 ? 0 : -1;
        return;
    }
    size_t bufpos = (T->tid == 0) ? (size_t)c->bufpos : c->io_last_bufpos;
    size_t bytes_to_write = bufpos - c->io_last_written.data_len;
    c->nwritten = res;
    c->io_last_written.buf = c->buf;
    c->io_last_written.bufpos = ((size_t)res == bytes_to_write ? bufpos : 0);
    c->io_last_written.data_len += res;
}

static void completeWrites(void (*tail)(client *)) {
    int n = T->nwrites;
    if (n == 0) return;
    STATS.write_batches++;
    if (n > STATS.max_write_batch) STATS.max_write_batch = n;

    for (int i = 0; i < n; i++) {
        iouSlot *s = &T->wslots[i];
        client *c = s->c;
        if (s->wc) { /* cancelled before apply */
            wctxReturn(s->wc);
            s->wc = NULL;
        }
        if (!c) continue;
        c->io_uring_slot = -1;
        s->c = NULL;
        tail(c);
    }
    T->nwrites = 0;
}

static void finishWriteMain(client *c) {
    if (postWriteToClient(c) == C_ERR) return;
    if (clientHasPendingReplies(c)) installClientWriteHandler(c);
}

/* The tail of ioThreadWriteToClient(). */
static void finishWriteIOThread(client *c) {
    c->io_write_state = CLIENT_COMPLETED_IO;
    sendToMainThread(c, JOB_RES_WRITE_CLIENT);
}

/* One io_uring_enter for everything queued in both directions. Invariant 1:
 * every result in both directions is applied to client state before any
 * tail runs; then read tails (which may parse and, on main, execute and add
 * replies), then write tails. On the main thread the two directions are
 * never queued together (reads flush from after-events, writes from the end
 * of handleClientsWithPendingWrites), so this degenerates to one direction. */
static void flushAll(void) {
    if (!T || T->in_flush || T->nreads + T->nwrites == 0) return;
    T->in_flush = 1;
    submitAndReap();
    for (int i = 0; i < T->nreads; i++)
        if (T->rslots[i].c) applyReadResult(&T->rslots[i]);
    for (int i = 0; i < T->nwrites; i++)
        if (T->wslots[i].c) applyWriteResult(&T->wslots[i]);
    if (T->tid == 0) {
        completeReads(finishReadMain);
        completeWrites(finishWriteMain);
    } else {
        completeReads(finishReadIOThread);
        completeWrites(finishWriteIOThread);
    }
    T->in_flush = 0;
}

void ioUringBatchFlushWrites(void) {
    flushAll();
}

void ioUringBatchFlushIOThread(void) {
    flushAll();
}

int ioUringBatchIOThreadPending(void) {
    return T ? T->nreads + T->nwrites : 0;
}

/* -------------------------------------------------------------- cancel */

void ioUringBatchClientFreed(client *c) {
    int idx = c->io_uring_slot;
    if (idx < 0) return;
    if (!T) return; /* not this thread's batch (cannot happen, see invariant 2) */
    c->io_uring_slot = -1;
    STATS.cancelled++;
    /* The slot is in whichever batch is currently open/flushing. */
    if (idx < T->nreads && T->rslots[idx].c == c) {
        iouSlot *s = &T->rslots[idx];
        /* Keep freeClient's sdsfree() off the pooled buffer; the flush loop
         * returns it to the pool (s->buf stays set). */
        if (s->buf && c->querybuf == s->buf) c->querybuf = NULL;
        s->c = NULL;
        return;
    }
    if (idx < T->nwrites && T->wslots[idx].c == c) {
        T->wslots[idx].c = NULL; /* wc is returned by the flush loop */
        return;
    }
}

#else /* !HAVE_LIBURING */

int ioUringBatchInit(void) {
    if (server.io_uring_enabled) serverLog(LL_WARNING, "io-uring requested but this build has no liburing support");
    return 0;
}
void ioUringBatchFree(void) {
}
int ioUringBatchInitThread(int tid) {
    UNUSED(tid);
    return 0;
}
void ioUringBatchFreeThread(void) {
}
int ioUringBatchActive(void) {
    return 0;
}
int ioUringBatchQueueRead(client *c) {
    UNUSED(c);
    return 0;
}
void ioUringBatchFlushReads(struct aeEventLoop *el) {
    UNUSED(el);
}
int ioUringBatchQueueWrite(client *c) {
    UNUSED(c);
    return 0;
}
void ioUringBatchFlushWrites(void) {
}
int ioUringBatchQueueIOThreadRead(client *c) {
    UNUSED(c);
    return 0;
}
int ioUringBatchQueueIOThreadWrite(client *c) {
    UNUSED(c);
    return 0;
}
void ioUringBatchFlushIOThread(void) {
}
int ioUringBatchIOThreadPending(void) {
    return 0;
}
void ioUringBatchClientFreed(client *c) {
    UNUSED(c);
}

#endif
