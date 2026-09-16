/*
 * io_uring_batch.c -- batch main-thread client read(2)/write(2) into io_uring.
 * See io_uring_batch.h for the scope statement.
 *
 * DESIGN NOTE (PoC)
 *
 * Valkey's main-thread loop today costs, for N ready clients per iteration:
 *      1 epoll_wait + N read() + N write()  =  2N+1 syscalls
 * With this module it costs:
 *      1 epoll_wait + 1 io_uring_enter(reads) + 1 io_uring_enter(writes) = 3
 * The kernel completes MSG_DONTWAIT recv/send on a ready TCP socket inline
 * during io_uring_enter, so the batch is fully reaped when submit returns
 * (io_uring_submit_and_wait(nqueued) covers the rare deferred case).
 *
 * What this module does NOT do:
 *   - It does not replace epoll. Readiness still comes from aeApiPoll; this
 *     only removes the per-client data-movement syscalls. A multishot-recv +
 *     provided-buffer-ring design would also remove epoll_wait and the
 *     per-fd poll bookkeeping, but requires changing query-buffer ownership
 *     and is out of scope for a measurement PoC.
 *   - It does not touch I/O threads. With io-threads > 1 every client is
 *     offloaded and the module is idle.
 *   - It does not batch large replies (reply list / encoded buffers), replica
 *     or primary links, TLS or RDMA connections, or big-arg (bulk >= 32KB)
 *     reads. All of those take the unchanged synchronous path.
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
 *      freeClient's sdsfree() would double-free it.
 *   3. thread_shared_qb is a single per-thread scratch buffer, so batched
 *      reads cannot use it: N recvs need N distinct destinations. Batched
 *      clients with a NULL querybuf borrow from a pool with the same
 *      "give it back if fully consumed, else the client takes ownership"
 *      semantics as resetSharedQueryBuf().
 *   4. Nothing is batched while a nested event loop is running
 *      (ProcessingEventsWhileBlocked) or while a flush is in progress.
 *   5. Reads are flushed from the ae after-events hook, i.e. after the fired
 *      fd loop and BEFORE time events (serverCron -> clientsCronResizeQuery
 *      Buffer may realloc a query buffer, which must not happen with an SQE
 *      pointing into it).
 */
#include "server.h"
#include "io_uring_batch.h"

ioUringBatchStats io_uring_batch_stats;

#ifdef HAVE_LIBURING
#include <liburing.h>
#include <sys/socket.h>
#include <limits.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

/* Ring depth. If more clients are ready in one iteration than fit, the
 * overflow simply takes the synchronous path (counted as fallback). */
#define IOU_DEPTH 4096

typedef struct iouSlot {
    client *c;  /* NULL once cancelled */
    sds buf;    /* read: pooled query buffer lent to the client, else NULL */
    int res;    /* CQE result (bytes or -errno) */
} iouSlot;

static struct io_uring ring;
static int ring_ok = 0;
static int in_flush = 0;

static iouSlot rslots[IOU_DEPTH];
static int nreads = 0;
static iouSlot wslots[IOU_DEPTH];
static int nwrites = 0;

/* Query-buffer pool for batched reads (see invariant 3). */
static sds *qb_pool = NULL;
static int qb_pool_len = 0, qb_pool_cap = 0;

static sds qbPoolTake(void) {
    if (qb_pool_len > 0) return qb_pool[--qb_pool_len];
    sds s = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(s);
    return s;
}

static void qbPoolReturn(sds s) {
    sdsclear(s);
    if (qb_pool_len == qb_pool_cap) {
        qb_pool_cap = qb_pool_cap ? qb_pool_cap * 2 : 64;
        qb_pool = zrealloc(qb_pool, sizeof(sds) * qb_pool_cap);
    }
    qb_pool[qb_pool_len++] = s;
}

int ioUringBatchInit(void) {
    if (!server.io_uring_enabled) return 0;
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    /* Main thread is the only submitter and reaper; these flags let the
     * kernel skip the cross-task completion machinery. Fall back to a plain
     * ring on kernels that predate them (< 6.1). */
    p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN;
    int rc = io_uring_queue_init_params(IOU_DEPTH, &ring, &p);
    if (rc < 0) {
        memset(&p, 0, sizeof(p));
        rc = io_uring_queue_init_params(IOU_DEPTH, &ring, &p);
    }
    if (rc < 0) {
        serverLog(LL_WARNING, "io-uring requested but io_uring_queue_init failed: %s; falling back to read/write",
                  strerror(-rc));
        return 0;
    }
    ring_ok = 1;
    serverLog(LL_NOTICE, "io_uring batching enabled (depth %d, sq %u, cq %u, flags 0x%x)", IOU_DEPTH, p.sq_entries,
              p.cq_entries, p.flags);
    return 1;
}

void ioUringBatchFree(void) {
    if (!ring_ok) return;
    io_uring_queue_exit(&ring);
    ring_ok = 0;
    for (int i = 0; i < qb_pool_len; i++) sdsfree(qb_pool[i]);
    zfree(qb_pool);
    qb_pool = NULL;
    qb_pool_len = qb_pool_cap = 0;
}

int ioUringBatchActive(void) {
    return ring_ok;
}

/* Common eligibility: plain socket, main thread, no nested loop, no IO
 * thread ownership. */
static inline int batchable(client *c) {
    if (!ring_ok || in_flush || ProcessingEventsWhileBlocked) return 0;
    if (server.io_threads_num > 1) return 0;
    if (!c->conn) return 0;
    int t = connGetType(c->conn);
    if (t != CONN_TYPE_SOCKET && t != CONN_TYPE_UNIX) return 0;
    if (c->conn->fd < 0 || connGetState(c->conn) != CONN_STATE_CONNECTED) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    if (c->io_uring_slot != -1) return 0; /* already in a batch */
    return 1;
}

/* Submit everything queued and reap exactly `want` completions into slots. */
static void submitAndReap(iouSlot *slots, int want) {
    int got = 0;
    int rc = io_uring_submit_and_wait(&ring, want);
    if (rc < 0 && rc != -EINTR && rc != -EBUSY) {
        serverLog(LL_WARNING, "io_uring_submit_and_wait: %s", strerror(-rc));
    }
    while (got < want) {
        struct io_uring_cqe *cqe;
        unsigned head, n = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            uintptr_t idx = (uintptr_t)io_uring_cqe_get_data(cqe);
            slots[idx].res = cqe->res;
            n++;
        }
        io_uring_cq_advance(&ring, n);
        got += n;
        if (got < want) {
            rc = io_uring_wait_cqe(&ring, &cqe);
            if (rc < 0 && rc != -EINTR) {
                serverLog(LL_WARNING, "io_uring_wait_cqe: %s", strerror(-rc));
                /* Mark the stragglers as EAGAIN so callers retry via epoll. */
                for (int i = 0; i < want; i++)
                    if (slots[i].res == INT_MIN) slots[i].res = -EAGAIN;
                break;
            }
        }
    }
}

/* ---------------------------------------------------------------- reads */

int ioUringBatchQueueRead(client *c) {
    if (!batchable(c)) goto fallback;
    if (c->flag.close_asap || c->flag.primary || isReplicatedClient(c)) goto fallback;
    /* Big-arg exact-size reads keep the synchronous path (they size the
     * read to the bulk boundary so the parser can steal the buffer). */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1 && c->bulklen >= PROTO_MBULK_BIG_ARG)
        goto fallback;
    if (nreads == IOU_DEPTH || nwrites != 0) goto fallback;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) goto fallback;

    iouSlot *s = &rslots[nreads];
    s->c = c;
    s->buf = NULL;
    s->res = INT_MIN;

    /* Mirror readToQueryBuf()'s non-big-arg sizing. */
    size_t readlen = PROTO_IOBUF_LEN;
    size_t qblen = c->querybuf ? sdslen(c->querybuf) : 0;
    if (c->querybuf == NULL) {
        c->querybuf = qbPoolTake();
        s->buf = c->querybuf;
        qblen = 0;
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
    io_uring_sqe_set_data(sqe, (void *)(uintptr_t)nreads);
    c->io_uring_slot = nreads;
    nreads++;
    io_uring_batch_stats.read_sqes++;
    return 1;

fallback:
    io_uring_batch_stats.fallback_reads++;
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

/* The tail of readQueryFromClient() for a non-replicated client. */
static void finishRead(iouSlot *s) {
    client *c = s->c;
    if (handleReadResult(c) == C_OK) {
        if (processInputBuffer(c) == C_ERR) return; /* client freed */
        trimCommandQueue(c);
    }
    /* handleReadResult may have freeClientAsync'd; the client object is
     * still valid until beforeSleep, and beforeNextClient() is safe. */
    beforeNextClient(c);
}

void ioUringBatchFlushReads(struct aeEventLoop *el) {
    UNUSED(el);
    if (in_flush || nreads == 0) return;
    in_flush = 1;
    int n = nreads;
    io_uring_batch_stats.read_batches++;
    if (n > io_uring_batch_stats.max_read_batch) io_uring_batch_stats.max_read_batch = n;

    submitAndReap(rslots, n);

    /* Invariant 1: account every result before any handler runs. */
    for (int i = 0; i < n; i++)
        if (rslots[i].c) applyReadResult(&rslots[i]);

    for (int i = 0; i < n; i++) {
        iouSlot *s = &rslots[i];
        client *c = s->c;
        if (!c) continue; /* cancelled by a peer's handler */
        finishRead(s);
        /* finishRead may have freed c synchronously -> slot cancelled. */
        c = s->c;
        if (!c) continue;
        c->io_uring_slot = -1;
        if (s->buf && c->querybuf == s->buf) {
            if (sdslen(c->querybuf) - c->qb_pos == 0) {
                c->querybuf = NULL;
                c->qb_pos = 0;
                c->qb_applied = 0;
                qbPoolReturn(s->buf);
            }
            /* else: client keeps ownership, like resetSharedQueryBuf(). */
        }
        s->c = NULL;
        s->buf = NULL;
    }
    nreads = 0;
    in_flush = 0;
}

/* --------------------------------------------------------------- writes */

int ioUringBatchQueueWrite(client *c) {
    if (!batchable(c)) goto fallback;
    /* Same restriction as valkey-io/valkey#112: static buffer only. */
    if (getClientType(c) == CLIENT_TYPE_REPLICA || c->flag.primary) goto fallback;
    if (listLength(c->reply) != 0 || c->flag.buf_encoded || c->bufpos <= 0) goto fallback;
    if (c->io_last_written.data_len != 0 && c->io_last_written.buf != c->buf) goto fallback;
    if (nwrites == IOU_DEPTH || nreads != 0) goto fallback;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) goto fallback;

    iouSlot *s = &wslots[nwrites];
    s->c = c;
    s->buf = NULL;
    s->res = INT_MIN;
    size_t off = c->io_last_written.data_len;
    io_uring_prep_send(sqe, c->conn->fd, c->buf + off, c->bufpos - off, MSG_DONTWAIT | MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, (void *)(uintptr_t)nwrites);
    c->io_uring_slot = nwrites;
    nwrites++;
    io_uring_batch_stats.write_sqes++;
    return 1;

fallback:
    io_uring_batch_stats.fallback_writes++;
    return 0;
}

/* Replicates connSocketWrite() error mapping + _writeToClient()'s
 * static-buffer bookkeeping for one send CQE. */
static void applyWriteResult(iouSlot *s) {
    client *c = s->c;
    connection *conn = c->conn;
    int res = s->res;
    c->nwritten = 0;
    c->write_flags = 0;
    if (res <= 0) {
        c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
        if (res < 0 && res != -EAGAIN) {
            conn->last_errno = -res;
            if (res != -EINTR && conn->state == CONN_STATE_CONNECTED) conn->state = CONN_STATE_ERROR;
        }
        c->nwritten = res == 0 ? 0 : -1;
        return;
    }
    size_t bytes_to_write = c->bufpos - c->io_last_written.data_len;
    c->nwritten = res;
    c->io_last_written.buf = c->buf;
    c->io_last_written.bufpos = ((size_t)res == bytes_to_write ? (size_t)c->bufpos : 0);
    c->io_last_written.data_len += res;
}

void ioUringBatchFlushWrites(void) {
    if (in_flush || nwrites == 0) return;
    in_flush = 1;
    int n = nwrites;
    io_uring_batch_stats.write_batches++;
    if (n > io_uring_batch_stats.max_write_batch) io_uring_batch_stats.max_write_batch = n;

    submitAndReap(wslots, n);

    for (int i = 0; i < n; i++)
        if (wslots[i].c) applyWriteResult(&wslots[i]);

    for (int i = 0; i < n; i++) {
        iouSlot *s = &wslots[i];
        client *c = s->c;
        if (!c) continue;
        c->io_uring_slot = -1;
        s->c = NULL;
        if (postWriteToClient(c) == C_ERR) continue;
        if (clientHasPendingReplies(c)) installClientWriteHandler(c);
    }
    nwrites = 0;
    in_flush = 0;
}

/* -------------------------------------------------------------- cancel */

void ioUringBatchClientFreed(client *c) {
    int idx = c->io_uring_slot;
    if (idx < 0) return;
    c->io_uring_slot = -1;
    io_uring_batch_stats.cancelled++;
    /* The slot is in whichever batch is currently open/flushing. */
    if (idx < nreads && rslots[idx].c == c) {
        iouSlot *s = &rslots[idx];
        if (s->buf && c->querybuf == s->buf) {
            c->querybuf = NULL; /* keep freeClient's sdsfree off our pool buffer */
            qbPoolReturn(s->buf);
        }
        s->c = NULL;
        s->buf = NULL;
        return;
    }
    if (idx < nwrites && wslots[idx].c == c) {
        wslots[idx].c = NULL;
        return;
    }
}

#else /* !HAVE_LIBURING */

int ioUringBatchInit(void) {
    if (server.io_uring_enabled) serverLog(LL_WARNING, "io-uring requested but this build has no liburing support");
    return 0;
}
void ioUringBatchFree(void) {}
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
void ioUringBatchFlushWrites(void) {}
void ioUringBatchClientFreed(client *c) {
    UNUSED(c);
}

#endif
