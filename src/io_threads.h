#ifndef IO_THREADS_H
#define IO_THREADS_H

#include "server.h"

/* Tag values for tagged pointers on work/response queues.
 * Tags must fit in 3 bits (0-7) due to 8-byte alignment from jemalloc
 * with --with-lg-quantum=3. SPSC and SPMC tags are separate enums
 * because they occupy independent queues and may reuse values. */

/* Tags for the SPSC private inbox (main thread → specific I/O thread). */
typedef enum {
    JOB_SPSC_FREE_ARGV = 0,
    JOB_SPSC_POLL = 1,
    /* DIAGNOSTIC ONLY (diag/handoff-roundtrip): tags 2-7 are free in this
     * enum as of da91ccd12f. JOB_SPSC_PROBE picks the next free slot; no
     * reuse/sentinel trick was needed. Remove this tag along with the rest
     * of the probe when the diagnostic is retired. */
    JOB_SPSC_PROBE = 2,
} JobRequestSPSC;

/* DIAGNOSTIC ONLY (diag/handoff-roundtrip): a single-shot cross-core
 * round-trip probe. Main enqueues one of these to a specific I/O thread's
 * private inbox, spins/yields on `done`, and times the interval. See
 * handoffProbeMaybeFire() in io_threads.c for the trigger site and
 * ioThreadHandoffProbe() for the I/O-thread-side handler.
 * Tagged pointers require 8-byte-aligned pointers (3 free low bits) --
 * _Alignas(8) forces that on a struct whose natural alignment would
 * otherwise be 4 (a lone _Atomic int), since a stack local only guarantees
 * its own natural alignment, not the queue's tagging requirement. Without
 * this, an unlucky stack address silently corrupts the tag (observed as
 * "Invalid SPSC job type: 6" during smoke testing -- the low bits of an
 * unaligned &probe bled into the JOB_SPSC_PROBE=2 tag). */
typedef struct {
    _Alignas(8) _Atomic int done;
} handoffProbe;

/* DIAGNOSTIC ONLY (diag/handoff-roundtrip): main-thread trigger, called
 * from processClientsCommandsBatch() with the number of commands drained. */
void handoffProbeMaybeFire(int commands_drained);

/* Tags for the SPMC shared inbox (main thread → any I/O thread). */
typedef enum {
    JOB_REQ_READ_CLIENT = 0,
    JOB_REQ_WRITE_CLIENT,
    JOB_REQ_FREE_OBJ,
    JOB_REQ_POLL,
    JOB_REQ_ACCEPT,
    JOB_REQ_CLUSTER_READ,
    JOB_REQ_CLUSTER_WRITE,
    JOB_REQ_CLUSTER_ACCEPT,
    JOB_REQ_COUNT
} JobRequestSPMC;
static_assert(JOB_REQ_COUNT <= 8, "JOB_REQ_COUNT must not exceed 8 for pointer arithmetic");

/* Tags for the MPSC response queue (I/O threads → main thread). */
typedef enum {
    JOB_RES_READ_CLIENT = 0,
    JOB_RES_WRITE_CLIENT,
    JOB_RES_CLUSTER_READ,
    JOB_RES_CLUSTER_WRITE,
    JOB_RES_CLUSTER_ACCEPT,
    JOB_RES_COUNT
} JobResult;
static_assert(JOB_RES_COUNT <= 8, "JOB_RES_COUNT must not exceed 8 for pointer arithmetic");

typedef void (*job_handler)(void *);

void initIOThreads(int prev_threads_num);
void killIOThreads(void);
int inMainThread(void);
int trySendReadToIOThreads(client *c);
int trySendWriteToIOThreads(client *c);
int tryOffloadFreeObjToIOThreads(robj *o);
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv);
void IOThreadsAfterSleep(int numevents);
void IOThreadsBeforeSleep(long long current_time);
void drainIOThreadsQueue(void);
void testOnlyInitIOThreadQueues(void);
void testOnlyFreeIOThreadQueues(void);
void testOnlyFillIOThreadInbox(void);
size_t testOnlyGetClusterIOPendingResponses(void);
void trySendPollJobToIOThreads(void);
int trySendAcceptToIOThreads(connection *conn);
struct clusterLink;
int trySendClusterReadToIOThreads(struct clusterLink *link);
int trySendClusterWriteToIOThreads(struct clusterLink *link);
int trySendClusterAcceptToIOThreads(connection *conn);
int updateIOThreads(const char **err);
long long getIOThreadActiveTimeMicroseconds(int id);
int clientHasPendingIO(struct client *c);
int processIOThreadsResponses(void);
int getCurTid(void);
void sendToMainThread(void *data, int type);

#endif /* IO_THREADS_H */
