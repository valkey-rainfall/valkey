/*
 * io_uring_batch: batch the per-client read(2)/write(2) system calls of an
 * event loop pass (main thread or I/O thread) into one io_uring_enter(2) per
 * direction.
 *
 * Readiness detection stays with epoll; only the data movement is batched,
 * so every recv/send is issued MSG_DONTWAIT against an
 * fd that epoll (reads) or the reply path (writes) just told us about and
 * completes inline inside the single submit call. Nothing here is
 * asynchronous from the event loop's point of view: a batch is opened,
 * filled, flushed and fully reaped before any client handler runs again.
 *
 * Scope (deliberately narrow, see the design note in io_uring_batch.c):
 *   - main thread and I/O threads each run their own ring; TLS/RDMA and
 *     replica/primary links take the ordinary synchronous path.
 *   - writes: the static reply buffer as one send, or the reply list /
 *     encoded buffer as one sendmsg over up to IOU_WIOV iovecs.
 *   - reads: plain client connections whose query buffer is not currently
 *     mid-bulk (big-arg) and are not replicated links.
 */
#ifndef IO_URING_BATCH_H
#define IO_URING_BATCH_H

#include <stddef.h>
#ifndef __cplusplus
#include <stdatomic.h>
#endif

struct client;
struct aeEventLoop;

/* Cap on SPMC jobs one io_uring-batching worker absorbs per loop pass
 * (upper bound of the io-uring-io-thread-share config). */
#define IO_URING_JOB_SHARE_MAX 512
/* Default share. A queued read waits for the end of its worker's batch, so
 * the share bounds the added tail latency; 32 keeps the syscall reduction of
 * an unbounded share while clipping the long passes that drive p99. */
#define IO_URING_JOB_SHARE_DEFAULT 32

/* Returns 1 if the build has liburing and the kernel accepted the ring. */
int ioUringBatchInit(void);
void ioUringBatchFree(void);
/* Per-I/O-thread ring; call from the thread itself (tid >= 1). */
int ioUringBatchInitThread(int tid);
void ioUringBatchFreeThread(void);
/* 1 if the CALLING thread has a working ring. */
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

/* ---- I/O-thread side --------------------------------------------------
 * Called from IOThreadMain() for a dequeued JOB_REQ_READ_CLIENT /
 * JOB_REQ_WRITE_CLIENT. Returns 1 if the job was absorbed into the thread's
 * open batch (the caller must not process it), 0 to process it inline. */
int ioUringBatchQueueIOThreadRead(struct client *c);
int ioUringBatchQueueIOThreadWrite(struct client *c);
/* Submit + reap the thread's open batches and run each job's tail. */
void ioUringBatchFlushIOThread(void);
/* Number of jobs absorbed and not yet flushed on this thread. */
int ioUringBatchIOThreadPending(void);

/* ---- adaptive share ---------------------------------------------------
 * Batching worker reads pays off when each read carries one command (two
 * syscalls per command to save) and costs tail latency when each read
 * carries many (nothing left to save, and a batch of N reads holds N x
 * depth commands back from the main thread). Each I/O thread keeps an EWMA
 * of commands parsed per read and stops batching once the EWMA passes
 * IO_URING_ADAPTIVE_HIGH_CMDS, resuming below IO_URING_ADAPTIVE_LOW_CMDS
 * (hysteresis).
 *
 * The decision is a pure function of an ioUringAdaptiveState so it can be
 * unit-tested; the server binds one state to each I/O thread. */
#define IO_URING_ADAPTIVE_HIGH_CMDS 4   /* commands/read above which batching stops */
#define IO_URING_ADAPTIVE_LOW_CMDS 2    /* ... and below which it resumes */
#define IO_URING_ADAPTIVE_Q 16          /* EWMA fixed-point fraction bits */
#define IO_URING_ADAPTIVE_ALPHA_SHIFT 6 /* EWMA weight of a new sample: 1/64 */

typedef struct ioUringAdaptiveState {
    /* EWMA of commands per read in Q16 fixed point; 0 until the first read.
     * Written by the owning thread, read by INFO from the main thread. */
    _Atomic(long long) cmds_per_read_q16;
    int suppressed; /* 1 while the share is being forced to 1 (owner only) */
} ioUringAdaptiveState;

/* Fold one read that parsed ``ncmds`` commands into the EWMA. */
void ioUringAdaptiveNoteRead(ioUringAdaptiveState *st, int ncmds);
/* The share cap a pass should use: ``configured`` normally, 1 while the
 * EWMA says reads carry many commands. Updates the hysteresis state. A
 * configured cap of 1 is returned untouched without consulting the state. */
int ioUringAdaptiveShareCap(ioUringAdaptiveState *st, int configured);
/* The EWMA as a double (0 if no read has been folded in yet). */
double ioUringAdaptiveCmdsPerRead(const ioUringAdaptiveState *st);

/* Server bindings: the calling I/O thread's own state.
 * NoteIOThreadRead is called from the I/O-thread read tail once the read
 * has been parsed; IOThreadShareCap from the worker's SPMC pass. */
void ioUringBatchNoteIOThreadRead(struct client *c);
int ioUringBatchIOThreadShareCap(int configured);
/* Mean commands-per-read over worker threads that have parsed reads
 * (0 if none), for INFO. */
double ioUringBatchCmdsPerRead(void);

/* A client is being freed synchronously while it may still have a queued
 * SQE (e.g. CLIENT KILL from another client's handler in the same batch).
 * Detach it so the completion is dropped on the floor. */
void ioUringBatchClientFreed(struct client *c);

/* INFO stats. */
typedef struct ioUringBatchStats {
    long long read_batches;   /* flushes with >=1 read */
    long long read_sqes;      /* recv SQEs submitted */
    long long write_batches;  /* flushes with >=1 write */
    long long write_sqes;     /* send + sendmsg SQEs submitted */
    long long writev_sqes;    /* of which sendmsg (reply list / encoded) */
    long long fallback_reads; /* clients that took the read(2) path */
    long long fallback_writes;
    long long cancelled; /* SQEs whose client was freed mid-batch */
    long long max_read_batch;
    long long max_write_batch;
    long long adaptive_suppressed; /* worker passes whose share was forced to 1 */
} ioUringBatchStats;
/* Sum of every thread's counters (for INFO). */
void ioUringBatchStatsTotal(ioUringBatchStats *out);

#endif /* IO_URING_BATCH_H */
