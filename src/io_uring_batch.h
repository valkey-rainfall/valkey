/*
 * io_uring_batch: batch the per-client read(2)/write(2) system calls of the
 * main thread's event loop into one io_uring_enter(2) per direction per loop
 * iteration.
 *
 * Readiness detection stays with epoll; only the data movement is batched,
 * so every recv/send is issued MSG_DONTWAIT against an
 * fd that epoll (reads) or the reply path (writes) just told us about and
 * completes inline inside the single submit call. Nothing here is
 * asynchronous from the event loop's point of view: a batch is opened,
 * filled, flushed and fully reaped before any client handler runs again.
 *
 * Scope (deliberately narrow, see the design note in io_uring_batch.c):
 *   - main thread only (io-threads == 1); TLS/RDMA and replica/primary links
 *     take the ordinary synchronous path.
 *   - writes: only the static reply buffer (c->buf) with no reply list, the
 *     same restriction as valkey-io/valkey#112. Anything else falls back.
 *   - reads: plain client connections whose query buffer is not currently
 *     mid-bulk (big-arg) and are not replicated links.
 */
#ifndef IO_URING_BATCH_H
#define IO_URING_BATCH_H

#include <stddef.h>

struct client;
struct aeEventLoop;

/* Returns 1 if the build has liburing and the kernel accepted the ring. */
int ioUringBatchInit(void);
void ioUringBatchFree(void);
int ioUringBatchActive(void);

/* ---- read side --------------------------------------------------------
 * Called from readQueryFromClient() for an epoll-readable client. If the
 * client qualifies, a recv SQE is queued and 1 is returned: the caller must
 * return immediately, the rest of readQueryFromClient runs later from
 * ioUringBatchFlushReads(). Returns 0 to take the ordinary path. */
int ioUringBatchQueueRead(struct client *c);

/* Submit all queued reads (one syscall), reap, then run the deferred
 * read-completion path for each client. Installed as the ae after-events
 * hook so it runs once per event-loop iteration, after the fired-fd loop and
 * before beforeSleep()/serverCron can touch query buffers. */
void ioUringBatchFlushReads(struct aeEventLoop *el);

/* ---- write side -------------------------------------------------------
 * Called from handleClientsWithPendingWrites(). If the client qualifies a
 * send SQE is queued and 1 is returned; the caller skips writeToClient().
 * Returns 0 to take the ordinary path. */
int ioUringBatchQueueWrite(struct client *c);

/* Submit + reap all queued writes, then run postWriteToClient() and the
 * write-handler installation for each. Called at the end of
 * handleClientsWithPendingWrites(). */
void ioUringBatchFlushWrites(void);

/* A client is being freed synchronously while it may still have a queued
 * SQE (e.g. CLIENT KILL from another client's handler in the same batch).
 * Detach it so the completion is dropped on the floor. */
void ioUringBatchClientFreed(struct client *c);

/* INFO stats. */
typedef struct ioUringBatchStats {
    long long read_batches;   /* flushes with >=1 read */
    long long read_sqes;      /* recv SQEs submitted */
    long long write_batches;  /* flushes with >=1 write */
    long long write_sqes;     /* send SQEs submitted */
    long long fallback_reads; /* clients that took the read(2) path */
    long long fallback_writes;
    long long cancelled;      /* SQEs whose client was freed mid-batch */
    long long max_read_batch;
    long long max_write_batch;
} ioUringBatchStats;
extern ioUringBatchStats io_uring_batch_stats;

#endif /* IO_URING_BATCH_H */
