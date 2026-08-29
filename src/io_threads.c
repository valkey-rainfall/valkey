/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "io_threads.h"
#include "cluster_migrateslots.h"
#include "queues.h"
#include "dplus.h"
#include <sys/resource.h>

#define IO_MPSC_QUEUE_SIZE 16384
#define IO_SPMC_QUEUE_SIZE 4096
#define IO_SPSC_QUEUE_SIZE 4096

static _Thread_local int thread_id = 0;
static _Thread_local mpscTicket io_thread_ticket = {0};
/* Door-2: count of event-loop fires that did no useful work this pump
 * iteration (owned client still waiting on main). Pump reads+resets to
 * decide backoff. Thread-local: each worker counts only its own fires. */
static _Thread_local int io_worker_useless_fires = 0;
/* Backlog of responses when io_shared_outbox is full. Should be rare. */
static _Thread_local list *pending_io_responses = NULL;
static pthread_t io_threads[IO_THREADS_MAX_NUM] = {0};
static pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];
static int cur_epoll_thread = 0;
// Main -> IO: Shared Queue (Single Producer Multi Consumer) where all IO threads pull jobs from
static spmcQueue io_shared_inbox = {0};
// IO -> Main: Response Channel (Multi Producer Single Consumer) used by IO threads to send results back to main-thread
static mpscQueue io_shared_outbox = {0};
// Main -> IO (Thread-Specific) for tasks that must run on specific IO thread where IO threads check their private inbox before the shared queue
static spscQueue io_private_inbox[IO_THREADS_MAX_NUM] = {0};
/* F13a: per-worker wake pipe. Main writes wake_pipe[id][1] after a committed
 * SPSC enqueue when the owner worker is parked in aePollDirect (see
 * consumer_parked in queues.h); the read end is registered in worker_el[id]
 * so the write lands as an fd event and ends the park immediately.
 * Contrast with ringMainDoorbell: no seq_cst Dekker fences here — a lost
 * ring is backstopped by the park's 2ms timeout (latency, never liveness),
 * so the producer-side check stays a single acquire load per enqueue.
 * fd = -1 when creation failed: everything degrades to the 2ms park. */
static int io_wake_pipe[IO_THREADS_MAX_NUM][2];
/* F13a-v3: load signal for ring gating — size of main's last nonempty punt
 * drain (decayed by half on empty visits). Main-thread-only, plain. The two
 * load regimes want OPPOSITE wake policies: at low load, latency is
 * everything and there is nothing to batch (ring, µs delivery); at
 * saturation, batching amortization is everything and a 2ms park is
 * invisible inside a ~2ms RTT (don't ring — jobcost pair measured the
 * always-ring policy costing −7% s20 with FLAT per-job exec cost, i.e. pure
 * amortization loss, Aug 23). */
static long long dplus_last_drain_jobs = 0;
#define DPLUS_F13_RING_MAX_DRAIN 32
static size_t io_jobs_submitted;
static _Atomic(size_t) io_jobs_finished;
static int io_threads_initialized = 0;
/* Door-2 wakeup coalescing: doorbell for the main-thread wakeup pipe.
 * 0 = disarmed (main may be asleep; next response must ring), 1 = armed
 * (a pipe byte is already in flight since main's last disarm; skip the
 * syscall). Workers ring on the queue's empty->non-empty transition only;
 * main disarms at the top of every processIOThreadsResponses() drain.
 * Replaces one write(2) PER RESPONSE with one per main wakeup — the
 * fix2flame profile showed modulePipeReadable at 23.3% of main plus
 * ~5% worker-side anon_pipe_write at the P10 ceiling.
 * Sharing note (drain-free lesson): main writes this line a few times per
 * event-loop tick (per drain call), NOT per batch — workers' per-response
 * relaxed loads mostly hit a shared cached copy, and each load replaces a
 * ~microsecond syscall, so the coherence trade is the right direction. */
static _Alignas(CACHE_LINE_SIZE) _Atomic(int) io_main_doorbell = 0;
_Atomic long long used_active_time_io_thread[IO_THREADS_MAX_NUM] = {0};

/* Per-worker event loops for door-2 ownership model. Each IO worker gets
 * its own aeEventLoop with a private epoll fd. When conn->el is set to
 * worker_el[tid], that connection's fd lives on the worker's epoll instead
 * of server.el. Currently allocated but empty (no fds registered). */
static aeEventLoop *worker_el[IO_THREADS_MAX_NUM] = {0};

/* Job Types for Tagged Pointers
 * We use the lower 3 bits of the pointer to store the job type.
 * Requires data pointers to be 8-byte aligned (standard for zmalloc/ptrs). */
#define JOB_TAG_MASK 0x7
#define JOB_PTR_MASK (~(uintptr_t)JOB_TAG_MASK)

static inline void *tagJob(void *ptr, int type) {
    return (void *)((uintptr_t)ptr | type);
}

static inline void untagJob(void *tagged_ptr, void **ptr, int *type) {
    *type = (int)((uintptr_t)tagged_ptr & JOB_TAG_MASK);
    *ptr = (void *)((uintptr_t)tagged_ptr & JOB_PTR_MASK);
}

/* Handler prototypes */
void ioThreadReadQueryFromClient(client *c);
void ioThreadWriteToClient(client *c);
void ioThreadFreeArgv(robj **argv);
void ioThreadPoll(aeEventLoop *el);
static void ioThreadAccept(client *c);

int inMainThread(void) {
    return thread_id == 0;
}

int getCurTid(void) {
    return thread_id;
}

void commitIOJobs(void) {
    for (int i = 1; i < server.active_io_threads_num; i++) {
        spscCommit(&io_private_inbox[i]);
    }
}

/* Jobs sent but not yet processed by IO threads. */
static size_t getPendingIOThreadsJobs(void) {
    return io_jobs_submitted - atomic_load_explicit(&io_jobs_finished, memory_order_acquire);
}

/* Read/write jobs awaiting response from IO threads. */
static int getPendingIOResponsesCount(void) {
    return server.stat_io_writes_pending + server.stat_io_reads_pending;
}

/* Drains the I/O threads queue by waiting for all jobs to be processed.
 * This function must be called from the main thread. */
void drainIOThreadsQueue(void) {
    serverAssert(inMainThread());
    commitIOJobs();
    /* Under io-threads-always-active, IOThreadsBeforeSleep may have deactivated
     * threads (locked their mutexes) in this same event-loop iteration. If there
     * are pending jobs, threads must be reactivated to process them. */
    if (getPendingIOThreadsJobs() > 0 && server.io_threads_always_active && server.active_io_threads_num == 1) {
        for (int i = 1; i < server.io_threads_num; i++) {
            pthread_mutex_unlock(&io_threads_mutex[i]);
        }
        server.active_io_threads_num = server.io_threads_num;
    }
    /* Spin until all submitted jobs are completed by IO threads.
     * Safety: panic after ~5 seconds (calibrated at ~2.5GHz = ~12.5B cycles)
     * to convert silent hangs into diagnosable crashes. Under normal operation
     * the drain completes in microseconds. */
    monotime drain_start = getMonotonicUs();
    while (getPendingIOThreadsJobs()) {
        atomic_thread_fence(memory_order_acquire);
        if ((getMonotonicUs() - drain_start) > 5000000) { /* 5 seconds */
            serverPanic("drainIOThreadsQueue: timeout after 5s. "
                "submitted=%zu finished=%zu pending=%zu active_threads=%d",
                io_jobs_submitted,
                (size_t)atomic_load_explicit(&io_jobs_finished, memory_order_acquire),
                getPendingIOThreadsJobs(),
                server.active_io_threads_num);
        }
    }
}

/* Returns if a worker operation is active or awaiting main completion.
 * WAITING_WRITABLE is intentionally excluded: an event is armed (or suspended
 * across a main handoff), but no worker is touching mutable client state. */
int clientHasPendingIO(client *c) {
    return c->io_read_state != CLIENT_IDLE ||
           (c->io_write_state != CLIENT_IDLE && c->io_write_state != CLIENT_WAITING_WRITABLE);
}

static void ringWorkerWakePipe(int tid); /* fwd (defined with the F13a machinery) */

/* Wait until active IO-thread work is done. WAITING_WRITABLE never waits on
 * remote socket progress; callers that inspect/migrate the client synchronize
 * with its owner event-loop lock instead. */
void waitForClientIO(client *c) {
    if (c->io_read_state == CLIENT_IDLE &&
        (c->io_write_state == CLIENT_IDLE || c->io_write_state == CLIENT_WAITING_WRITABLE)) return;

    /* Door-2 ownership (B13): a staged owner-write may still be UNCOMMITTED
     * in the owner's SPSC (F12-B batches commits until drain end), and at
     * high load the F13a-v3 policy deliberately skips the wake ring -- while
     * the classic obuf scenario means the client's fd produces no activity
     * (it stopped reading). A waiter that just spins can therefore never be
     * satisfied: parked worker + invisible job = 5s panic. Make the wait
     * self-sufficient: publish any staged jobs for this owner and ring its
     * wake pipe unconditionally (rare inspection/teardown path; the ring's
     * amortization cost is irrelevant here). */
    if (c->owner_tid != 0) {
        spscCommit(&io_private_inbox[c->owner_tid]);
        ringWorkerWakePipe(c->owner_tid);
    }

    /* Wait for read operation to complete if pending. */
    {
        monotime _w = getMonotonicUs();
        while (c->io_read_state == CLIENT_PENDING_IO) {
            atomic_thread_fence(memory_order_acquire);
            if (getMonotonicUs() - _w > 5000000) serverPanic("waitForClientIO READ spin timeout");
        }
    }

    /* Wait for write operation to complete if pending. */
    {
        monotime _w2 = getMonotonicUs();
        while (c->io_write_state == CLIENT_PENDING_IO) {
            atomic_thread_fence(memory_order_acquire);
            if (getMonotonicUs() - _w2 > 5000000) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
                serverLog(LL_WARNING,
                          "D+ B13 forensics: id=%llu owner_tid=%d write_flags=%d bufpos=%d reply_len=%llu "
                          "io_last_bufpos=%llu buf_encoded=%d close_asap=%d parked=%d pending_write_flag=%d "
                          "spsc_head=%llu spsc_tail=%llu spsc_tail_local=%llu",
                          (unsigned long long)c->id, (int)c->owner_tid, (int)c->write_flags, (int)c->bufpos,
                          (unsigned long long)listLength(c->reply), (unsigned long long)c->io_last_bufpos,
                          (int)c->flag.buf_encoded, (int)c->flag.close_asap,
                          c->owner_tid ? (int)atomic_load_explicit(&io_private_inbox[c->owner_tid].consumer_parked,
                                                                   memory_order_acquire) : -1,
                          (int)c->flag.pending_write,
                          (unsigned long long)(c->owner_tid ? atomic_load_explicit(&io_private_inbox[c->owner_tid].head, memory_order_acquire) : 0),
                          (unsigned long long)(c->owner_tid ? atomic_load_explicit(&io_private_inbox[c->owner_tid].tail, memory_order_acquire) : 0),
                          (unsigned long long)(c->owner_tid ? io_private_inbox[c->owner_tid].tail_local : 0));
#endif
                serverPanic("waitForClientIO WRITE spin timeout");
            }
        }
    }

    /* Final memory barrier to ensure all changes are visible */
    atomic_thread_fence(memory_order_acquire);
}

void IOThreadsBeforeSleep(long long current_time) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());

    commitIOJobs();

    if (server.io_threads_always_active) {
        /* active_all_io_threads state is for debug purposes: deactivate all threads before sleep if no pending jobs,
         * and reactivate all after sleep. We can't leave it active all the time as it will consume much CPU that will interfere with tests */
        /* BUGFIX: also check getPendingIOResponsesCount(). On ARM (weak memory
         * model), the MPSC outbox consumer may see an advanced tail but a stale
         * NULL buffer slot because the producer's relaxed fetch_add on tail can
         * propagate before the release store to the buffer. If we deactivate
         * threads in this window and no new events arrive (numevents==0 keeps
         * threads locked), the response is stranded until the next timer cycle
         * re-dequeues — but by then afterSleep won't unlock (numevents==0).
         * Checking getPendingIOResponsesCount()>0 prevents deactivation while
         * main still expects responses, making the stall self-healing. */
        if (server.active_io_threads_num > 1 && getPendingIOThreadsJobs() == 0 &&
            getPendingIOResponsesCount() == 0 && !server.io_threads_ownership) {
            for (int i = 1; i < server.active_io_threads_num; i++) {
                /* Door-2: don't lock (deactivate) workers that have owned fds */
                if (worker_el[i] && worker_el[i]->maxfd != -1) continue;
                pthread_mutex_lock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = 1;
        }
    }

    /* If threads are not active, track main-thread active time for ignition decision */
    if (server.active_io_threads_num == 1) {
        static long long last_measurement_time = 0;
        if (current_time - last_measurement_time < 50000) return; /* Sample once in 50ms */
        last_measurement_time = current_time;
        trackInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME, server.stat_active_time, current_time, 1000000);
    }
}

#define IO_COOLDOWN_MS 1000
#define IO_SAMPLE_RATE_MS 10
#define IO_IGNITION_EVENTS 4
/* Start using I/O threads when the main thread is active for more than the below
 * defined percentage of the time. This number is picked somewhat arbitrarily but
 * needed to be low enough to make sure we start the next thread quickly while not
 * starting too many threads unnecessarily to avoid contention. */
#define IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT 30
#define BATCH_SIZE 32

void IOThreadsAfterSleep(int numevents) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());
    /* Door-2 ownership: all workers are permanently active (set at init);
     * ignition/scaling would only ever try to park a worker, which loses
     * wakeups for owned fds assigned after the park check. */
    if (server.io_threads_ownership) return;
    /* Always Active Policy */
    if (server.io_threads_always_active) {
        if (numevents > 0 && server.active_io_threads_num < server.io_threads_num) {
            for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
                pthread_mutex_unlock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = server.io_threads_num;
        }
        return;
    }

    mstime_t now = server.mstime;
    static long long last_scale_time = 0;

    /* Ignition Policy */
    if (server.active_io_threads_num == 1) {
        int should_ignite = 0;
        float main_thread_active_time = (float)getInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME) / 10000.0;
        /* Ignite IO threads when main-thread active time exceeds the threshold (30%) */
        should_ignite = (main_thread_active_time > (float)IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT);
        if (should_ignite) {
            pthread_mutex_unlock(&io_threads_mutex[1]);
            server.active_io_threads_num++;
            last_scale_time = now;
            serverLog(LL_DEBUG, "IO threads ignition: increased to %d", server.active_io_threads_num);
        }
        return;
    }

    static mstime_t last_sample_time = 0;
    static size_t spmc_size_sum = 0;
    static size_t sample_count = 0;

    /* Scaling Up/Down Policy */
    if (now - last_sample_time < IO_SAMPLE_RATE_MS) return;
    last_sample_time = now;

    size_t q_size = spmcSize(&io_shared_inbox);
    spmc_size_sum += q_size;
    sample_count++;

    trackInstantaneousMetric(STATS_METRIC_IO_WAIT, spmc_size_sum, sample_count, 1);

    /* Decision (Every STATS_METRIC_SAMPLES Samples) */
    if (sample_count % STATS_METRIC_SAMPLES != 0) return;

    size_t avg_q_size = getInstantaneousMetric(STATS_METRIC_IO_WAIT);
    size_t active = server.active_io_threads_num;
    size_t target = active;

    /* Calculate Target */
    if (avg_q_size > 1 && active < (size_t)server.io_threads_num) {
        target++;
    } else if (avg_q_size == 0 && (now - last_scale_time > IO_COOLDOWN_MS)) {
        if (target > 1) target--;
    }

    /* Scale Up */
    if (target > active) {
        for (size_t i = active; i < target; i++) {
            pthread_mutex_unlock(&io_threads_mutex[i]);
        }
        last_scale_time = now;
        server.active_io_threads_num = target;
        serverLog(LL_DEBUG, "IO threads increased from %zu to %zu", active, target);
    }
    /* Scale Down*/
    else if (target < active) {
        int tid = active - 1;

        /* Don't suspend if work remains in the specific thread's queue... */
        if (!spscIsEmpty(&io_private_inbox[tid])) return;
        /* ...or if we are dropping to 1 thread but the global queue still has work */
        if (target == 1 && !spmcIsEmpty(&io_shared_inbox)) return;
        /* Door-2: don't deactivate workers with owned fds. */
        if (worker_el[tid] && worker_el[tid]->maxfd != -1) return;

        pthread_mutex_lock(&io_threads_mutex[tid]);
        server.active_io_threads_num--;
        serverLog(LL_DEBUG, "IO threads decreased from %zu to %d", active, server.active_io_threads_num);
    }
}

/* This function performs polling on the given event loop and updates the server's
 * IO fired events count and poll state. */
void ioThreadPoll(aeEventLoop *el) {
    struct timeval tvp = {0, 0};
    int num_events = aePoll(el, &tvp);
    server.io_ae_fired_events = num_events;
    atomic_store_explicit(&server.io_poll_state, AE_IO_STATE_DONE, memory_order_release);
}

static void ringMainDoorbell(void);

static void flushPendingIOResponses(int blocking) {
    if (!pending_io_responses) return;
    listIter li;
    listNode *ln;
    listRewind(pending_io_responses, &li);

    int flushed = 0;
    while ((ln = listNext(&li))) {
        void *job = listNodeValue(ln);
        int pushed = 0;

        /* Try to enqueue. If blocking is set, retry until success. */
        do {
            pushed = mpscEnqueue(&io_shared_outbox, job, &io_thread_ticket);
            if (pushed || !blocking || server.crashed) break; /* On server crash we kill the IO threads, no point in sending back jobs to the main-thread. */
            atomic_thread_fence(memory_order_acquire);
        } while (true);

        if (pushed) {
            flushed++;
            listDelNode(pending_io_responses, ln);
        } else {
            goto out;
        }
    }

    /* List is fully drained */
    listRelease(pending_io_responses);
    pending_io_responses = NULL;
out:
    /* Coalesced wakeup: backlog jobs were pushed outside sendToMainThread's
     * ring, so ring here. (Pre-coalescing this site had no pipe write and
     * relied on the next response's unconditional write to wake main.) */
    if (flushed > 0 && server.io_threads_ownership) {
        ringMainDoorbell();
    }
}

/* Define a cleanup function that will clean all thread resources */
void cleanupThreadResources(void *dummy) {
    UNUSED(dummy);

    /* Cancellation cannot leave an ACTIVE epoch pin behind. Main still waits
     * for pthread_join before publishing OFFLINE. */
    dplusReaderWorkerQuiescent(thread_id);

    /* Blocking flush: ensure all pending jobs are sent before thread dies */
    flushPendingIOResponses(1);

    /* Free the shared query buffer */
    freeSharedQueryBuf();
}

static void *IOThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.io_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    valkey_set_thread_title(thdname);
    serverSetCpuAffinity(server.server_cpulist);
    initSharedQueryBuf();
    pthread_cleanup_push(cleanupThreadResources, NULL);

    thread_id = (int)id;
    void *batch_jobs[BATCH_SIZE];
    int processed = 0;
    /* F13a-v2 (spin-then-park): consecutive all-empty sweeps. The parked-bit
     * protocol only engages after DPLUS_F13_SPIN_SWEEPS empty passes — at
     * load the park path cycles hot (measured 2.1M parks/s at bench s20) and
     * per-cycle parked-bit stores ping-pong main's SPSC producer line, taxing
     * every enqueue. Spinning through brief gaps keeps the bit (and main's
     * line) untouched; a genuinely idle worker still parks after ~50 sweeps
     * and gets the µs wake-fd latency path. */
    int idle_streak = 0;
/* v3b: 150 sweeps (~150µs). The f13v3 bench gauge showed 50 sweeps (~50µs)
 * sitting right AT the per-worker arrival gap (~40µs at s20): the window
 * expired between arrivals, re-engaging the park path 170K times/s — that
 * is both parked-bit producer-line traffic and, with the ring gate closed
 * at load, park-remainder delay on staged replies (d2b 600µs). 150µs
 * clears the gap with margin; true idle still parks after ~150µs of
 * bounded spin. */
#define DPLUS_F13_SPIN_SWEEPS 150
    monotime work_start_time = 0;
    while (1) {
        /* Cancellation point so that pthread_cancel() from main thread is honored. */
        pthread_testcancel();
        size_t batch_count = 0;
        monotime prev_work_start_time = work_start_time;
        work_start_time = getMonotonicUs();
        if (processed != 0) {
            atomic_fetch_add_explicit(&used_active_time_io_thread[id],
                                      work_start_time - prev_work_start_time,
                                      memory_order_relaxed);
        }
        processed = 0;
        /* PRIORITY 1: Drain Private SPSC Queue (Batch Processing) */
        while ((batch_count = spscDequeueBatch(&io_private_inbox[id], batch_jobs, BATCH_SIZE)) > 0) {
            for (size_t i = 0; i < batch_count; i++) {
                void *data;
                int type;
                untagJob(batch_jobs[i], &data, &type);

                switch (type) {
                case JOB_REQ_FREE_ARGV:
                    ioThreadFreeArgv((robj **)data);
                    break;
                case JOB_REQ_POLL:
                    ioThreadPoll((aeEventLoop *)data);
                    break;
                case JOB_REQ_OWNER_WRITE:
                    /* F8b: staged by main at punt handback (PENDING_IO already
                     * published). Same write path as the F7 handler. Counter
                     * lives HERE (worker's own stats slot — single-writer) now
                     * that the F7 event-handler is bypassed. */
                    dplus_thread_stats[id].punted_replies_written++;
                    ioThreadWriteToClient((client *)data);
                    break;
                default:
                    serverPanic("Invalid SPSC job type: %d", type);
                }
            }
            processed += batch_count;
        }

        /* PRIORITY 2: Shared Global Queue (SPMC)
         * Only checked after SPSC is drained. */
        void *tagged_job = spmcDequeue(&io_shared_inbox);
        if (tagged_job) {
            void *data;
            int type;
            untagJob(tagged_job, &data, &type);

            switch (type) {
            case JOB_REQ_READ_CLIENT:
                ioThreadReadQueryFromClient((client *)data);
                break;
            case JOB_REQ_WRITE_CLIENT:
                ioThreadWriteToClient((client *)data);
                break;
            case JOB_REQ_FREE_OBJ:
                decrRefCount(data);
                break;
            case JOB_REQ_ACCEPT:
                ioThreadAccept((client *)data);
                break;
            case JOB_REQ_POLL:
                ioThreadPoll((aeEventLoop *)data);
                break;
            default:
                serverPanic("Invalid SPMC job type: %d", type);
            }
            processed++;
        }

        if (processed) {
            atomic_fetch_add_explicit(&io_jobs_finished, processed, memory_order_release);
        }

        /* Door-2 infrastructure: pump the per-worker event loop if it has
         * any registered fds. With no fds, the maxfd == -1 check
         * short-circuits before any syscall. Teardown safety comes from
         * PER-FD dispatch locking inside aeProcessEvents (AE_PROTECT_POLL):
         * main's aeAcquireLock waits at most one callback, and events
         * deleted by a teardown can't fire afterwards (fe->mask recheck
         * runs under the lock).
         *
         * BACKOFF: a fire whose client is still waiting on main (handoff in
         * flight) does no work — level-triggered epoll re-fires it every
         * sweep until main finishes. Counting those as "processed" makes
         * this loop spin hot. Only count fires that did real work; an
         * all-useless sweep falls through to the idle usleep below. */
        if (worker_el[id]->maxfd != -1) {
            io_worker_useless_fires = 0;
            int ev_processed = aeProcessEvents(worker_el[id], AE_FILE_EVENTS | AE_DONT_WAIT);
            if (ev_processed > io_worker_useless_fires) processed += ev_processed - io_worker_useless_fires;
        }

        /* If both queues were empty (no processing done), wait for signal. */
        if (processed != 0) idle_streak = 0;
        if (processed == 0) {
            if (unlikely(pending_io_responses)) {
                flushPendingIOResponses(0);
            } else if (server.io_threads_ownership || worker_el[id]->maxfd != -1) {
                /* Door-2 ownership: worker may hold client fds — or be assigned
                 * one by main at ANY moment (accept-time assignment). Blocking on
                 * the mutex here loses that wakeup. And usleep-polling burns CPU
                 * at density: 30 idle ownership servers measured 84% aggregate
                 * CPU (vs 5% parked legacy) — enough to starve replies machine-
                 * wide under the test harness (the "reconnect race"). Instead,
                 * sleep IN the worker's epoll: instant wake on owned-fd activity,
                 * ~zero idle cost. Bounded at 2ms so cross-thread queue jobs and
                 * newly-migrated fds (registered by main via epoll_ctl — visible
                 * to an in-progress epoll_wait) are picked up promptly. */
                if (++idle_streak < DPLUS_F13_SPIN_SWEEPS) {
                    /* Spin window: non-blocking poll only. No parked-bit
                     * traffic, no ring eligibility — an SPSC job landing now
                     * is seen by the next sweep within ~1µs. */
                    struct timeval tv0 = {0, 0};
                    aePollDirect(worker_el[id], &tv0);
                } else {
                    struct timeval tv = {0, 2000};
                    /* F13a park protocol: publish parked, then RE-CHECK the SPSC
                     * inbox before blocking — a producer that read the bit as 0
                     * just before our store won't ring, but its enqueue then
                     * precedes this check. Without seq_cst fences on both sides
                     * (cf. ringMainDoorbell) a lost ring remains theoretically
                     * possible; the 2ms timeout bounds it — the ring is a latency
                     * accelerator, never a liveness requirement. */
                    atomic_store_explicit(&io_private_inbox[id].consumer_parked, 1, memory_order_release);
                    if (spscIsEmpty(&io_private_inbox[id])) {
                        aePollDirect(worker_el[id], &tv);
                    }
                    atomic_store_explicit(&io_private_inbox[id].consumer_parked, 0, memory_order_relaxed);
                }
            } else {
                /* If it is locked. We should block until main thread unlocks it. */
                pthread_mutex_lock(&io_threads_mutex[id]);
                pthread_mutex_unlock(&io_threads_mutex[id]);
            }
        }
    }
    pthread_cleanup_pop(0);
    return NULL;
}

long long getIOThreadActiveTimeMicroseconds(int id) {
    return atomic_load_explicit(&used_active_time_io_thread[id], memory_order_relaxed);
}

/* F13a: drain the worker's wake pipe. The byte's only job was ending the
 * park; the sweep at the top of IOThreadMain's loop does the actual SPSC
 * work. Runs on the worker thread via worker_el[id]. */
static void wakePipeReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);
    char buf[64];
    while (read(fd, buf, sizeof(buf)) == sizeof(buf))
        ;
}

/* F13a: ring a parked owner worker after a committed SPSC enqueue.
 * EAGAIN = a wakeup byte is already pending, free to skip. EINTR = retry:
 * failing to write would defer the reply to the 2ms park timeout. */
static void ringWorkerWakePipe(int tid) {
    if (io_wake_pipe[tid][1] == -1) return;
    while (write(io_wake_pipe[tid][1], "W", 1) != 1) {
        if (errno != EINTR) break;
    }
}

static void createIOThread(int id) {
    serverAssert(server.io_threads_num > 0);
    serverAssert(id > 0 && id < server.io_threads_num);

    /* Initialize the private SPSC queue for this thread */
    spscInit(&io_private_inbox[id], IO_SPSC_QUEUE_SIZE);

    /* Create per-worker event loop (same setsize as server.el).
     * Currently no fds are registered — this is infrastructure for
     * the door-2 ownership model where client fds will migrate here. */
    int setsize = server.maxclients + CONFIG_FDSET_INCR;
    worker_el[id] = aeCreateEventLoop(setsize);
    if (!worker_el[id]) {
        serverLog(LL_WARNING, "Fatal: Can't create event loop for IO thread %d", id);
        exit(1);
    }
    /* DIAG (pollbatch-cap): in ownership mode client fds live on the worker
     * loops, so the anti-lockstep cap must apply here too. */
    aeSetPollBatchSize(worker_el[id], AE_SERVER_POLL_BATCH_SIZE);
    /* Enable poll protection so aeCreateFileEvent/aeDeleteFileEvent from main
     * thread serialize against the worker's aeProcessEvents via poll_mutex.
     * This is belt-and-suspenders: for NEW fd registration from main, the fd
     * can't fire until after epoll_ctl completes (kernel barrier), so there's
     * no actual race for accept-time registration. But we set it to be safe
     * for future fd modifications (handler changes, deletions). */
    aeSetPollProtect(worker_el[id], 1);
    serverLog(LL_NOTICE, "IO thread %d: created per-worker event loop (setsize=%d)", id, setsize);

    /* F13a: create + register the wake pipe before the thread starts (no
     * concurrent registration race). Failure is non-fatal: fds stay -1 and
     * punt-reply pickup falls back to the 2ms park timeout. */
    io_wake_pipe[id][0] = io_wake_pipe[id][1] = -1;
    /* OFF-mode fix (battery finding #2, Aug 24): only create/register the
     * wake pipe under ownership — its sole ring site (ioSubmitOwnerWrite) is
     * ownership-only, and registering it unconditionally made
     * worker_el->maxfd != -1 in ALL modes, which permanently tripped the
     * scale-down guard ("don't deactivate workers with owned fds") so
     * io_threads_active could never return to 0. Restores the invariant
     * that OFF-mode worker_el is empty (OFF == stock behavior). */
    if (server.io_threads_ownership) {
        int wakefds[2];
        if (anetPipe(wakefds, O_CLOEXEC | O_NONBLOCK, O_CLOEXEC | O_NONBLOCK) == 0) {
            if (aeCreateFileEvent(worker_el[id], wakefds[0], AE_READABLE, wakePipeReadHandler, NULL) == AE_OK) {
                io_wake_pipe[id][0] = wakefds[0];
                io_wake_pipe[id][1] = wakefds[1];
            } else {
                close(wakefds[0]);
                close(wakefds[1]);
                serverLog(LL_WARNING, "IO thread %d: wake pipe fd registration failed; parked-reply pickup degrades to poll timeout", id);
            }
        } else {
            serverLog(LL_WARNING, "IO thread %d: wake pipe creation failed (%s); parked-reply pickup degrades to poll timeout", id, strerror(errno));
        }
    }

    pthread_t tid;
    pthread_mutex_init(&io_threads_mutex[id], NULL);
    pthread_mutex_lock(&io_threads_mutex[id]); /* Thread will be stopped. */
    /* Publish a safe lifecycle state before the new thread can run. */
    dplusReaderWorkerOnline(id);
    int err = pthread_create(&tid, NULL, IOThreadMain, (void *)(long)id);
    if (err) {
        serverLog(LL_WARNING, "Fatal: Can't initialize IO thread, pthread_create failed with: %s", strerror(err));
        exit(1);
    }
    io_threads[id] = tid;
}

/* Terminates the IO thread specified by id. */
static void shutdownIOThread(int id) {
    int err;
    pthread_t tid = io_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    /* Only unlock mutex for inactive threads. Active threads are already unlocked. */
    if (id >= server.active_io_threads_num) {
        pthread_mutex_unlock(&io_threads_mutex[id]);
    }
    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "IO thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        /* Join is the proof that no stale worker can publish into this slot. */
        dplusReaderWorkerOffline(id);
        serverLog(LL_NOTICE, "IO thread(tid:%lu) terminated", (unsigned long)tid);
    }
    pthread_mutex_destroy(&io_threads_mutex[id]);
    spscFree(&io_private_inbox[id]);

    /* Free the per-worker event loop. */
    if (worker_el[id]) {
        aeDeleteEventLoop(worker_el[id]);
        worker_el[id] = NULL;
    }

    /* F13a: close the wake pipe (event already gone with the loop). */
    if (io_wake_pipe[id][0] != -1) {
        close(io_wake_pipe[id][0]);
        close(io_wake_pipe[id][1]);
        io_wake_pipe[id][0] = io_wake_pipe[id][1] = -1;
    }
}

void killIOThreads(void) {
    for (int j = 1; j < server.io_threads_num; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownIOThread(j);
    }
}

void ioWorkerCountUselessFire(void) {
    io_worker_useless_fires++;
}

/* Return the per-worker event loop for the given thread ID, or NULL if the
 * slot has no live loop. Bounded by slot liveness (worker_el), NOT by
 * server.io_threads_num: during a runtime shrink the config is updated
 * before the removed workers' clients are disowned, so their loops must
 * remain reachable until shutdownIOThread frees them (B11). */
aeEventLoop *ioGetWorkerEventLoop(int tid) {
    if (tid < 1 || tid >= IO_THREADS_MAX_NUM) return NULL;
    return worker_el[tid];
}

int updateIOThreads(const char **err) {
    serverAssert(inMainThread());

    int prev_threads_num = 1;
    for (int i = IO_THREADS_MAX_NUM - 1; i > 0; i--) {
        if (io_threads[i]) {
            prev_threads_num = i + 1;
            break;
        }
    }
    if (prev_threads_num == server.io_threads_num) return 1;

    /* DEADLOCK PREVENTION:
     * Check if the pending workload fits in the return queue.
     * If the number of pending jobs is greater than the capacity of the Global MPSC queue,
     * the worker threads might fill the queue and block. If we enter drainIOThreadsQueue
     * in that state, we will deadlock (Main thread waits for worker, Worker waits for queue space). */
    size_t pending = getPendingIOResponsesCount();

    if (pending > io_shared_outbox.queue_size) {
        if (err) *err = "Can't update IO threads under load, try again later";
        return 0;
    }

    serverLog(LL_NOTICE, "Changing number of IO threads from %d to %d.", prev_threads_num, server.io_threads_num);
    drainIOThreadsQueue();

    /* Door-2 ownership (B11/D1): resizing while workers own client fds crashed
     * in ioSubmitOwnerWrite -- shrink freed a removed worker's event loop and
     * SPSC inbox while clients still carried its owner_tid. Follow the epoch
     * lifecycle resize sequence: exclusive mode drains speculation, then every
     * client owned by a removed slot completes its pending IO and is disowned
     * back to main BEFORE the worker is cancelled and its structures freed. */
    if (server.io_threads_ownership) {
        dplusExclusiveEnter();
        if (server.io_threads_num < prev_threads_num) {
            listIter li;
            listNode *ln;
            listRewind(server.clients, &li);
            while ((ln = listNext(&li))) {
                client *c = listNodeValue(ln);
                if (c->owner_tid >= server.io_threads_num) {
                    waitForClientIO(c);
                    disownClient(c);
                    /* A client disowned mid-execution (e.g. the very client
                     * issuing this CONFIG SET) sits at CLIENT_COMPLETED_IO;
                     * the owned release in beforeNextClient() is gated on
                     * owner_tid != 0 and will never run for it again, and
                     * writeToClient() no-ops while either IO state is
                     * non-idle -- the reply wedges forever (B11 lost-reply).
                     * Mirror that release here. Workers for these slots are
                     * quiesced under exclusive mode, so a plain store is
                     * sufficient. */
                    if (c->io_read_state == CLIENT_COMPLETED_IO) c->io_read_state = CLIENT_IDLE;
                }
            }
        }
    }

    /* Set active threads to 1, will be adjusted based on workload later. */
    for (int i = 1; i < server.active_io_threads_num; i++) {
        pthread_mutex_lock(&io_threads_mutex[i]);
    }
    server.active_io_threads_num = 1;

    if (server.io_threads_num > prev_threads_num) {
        initIOThreads(prev_threads_num);
    } else {
        for (int i = prev_threads_num - 1; i >= server.io_threads_num; i--) {
            /* Unblock inactive thread. */
            pthread_mutex_unlock(&io_threads_mutex[i]);
            shutdownIOThread(i);
            io_threads[i] = 0;
        }
    }

    /* Door-2 ownership: restore the permanently-active invariant for the
     * surviving workers after a shrink (grow restores it in initIOThreads).
     * All deactivation paths are skipped under ownership, so a worker left
     * parked here would never reactivate and its owned clients would hang. */
    if (server.io_threads_ownership) {
        if (server.io_threads_num < prev_threads_num) {
            for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
                pthread_mutex_unlock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = server.io_threads_num;
        }
        dplusExclusiveLeave();
    }
    return 1;
}

/* Initialize the data structures needed for I/O threads. */
void initIOThreads(int prev_threads_num) {
    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    serverAssert(server.io_threads_num <= IO_THREADS_MAX_NUM);

    if (!io_threads_initialized) {
        server.active_io_threads_num = 1; /* We start with threads not active. */
        server.io_poll_state = AE_IO_STATE_NONE;
        server.io_ae_fired_events = 0;
        spmcInit(&io_shared_inbox, IO_SPMC_QUEUE_SIZE);
        mpscInit(&io_shared_outbox, IO_MPSC_QUEUE_SIZE);
        io_jobs_submitted = 0;
        atomic_init(&io_jobs_finished, 0);
        prefetchCommandsBatchInit();
        io_threads_initialized = 1;
    }

    /* Spawn and initialize the I/O threads. */
    for (int i = prev_threads_num; i < server.io_threads_num; i++) {
        createIOThread(i);
    }

    /* Door-2 ownership: workers must be permanently active. A parked worker
     * checked maxfd == -1 BEFORE main assigned it a client fd and would never
     * re-check (lost wakeup: accepted clients register on the worker's epoll
     * but their reads never fire). Unlock all parked workers now; the
     * deactivation paths are all skipped under ownership mode. */
    if (server.io_threads_ownership) {
        for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
            pthread_mutex_unlock(&io_threads_mutex[i]);
        }
        server.active_io_threads_num = server.io_threads_num;
        serverLog(LL_NOTICE, "Door-2 ownership: all %d IO threads permanently active", server.io_threads_num - 1);
    }
}

int trySendReadToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* Door-2: owned clients handle reads on their worker's event loop.
     * Return C_OK so main won't try to handle them either. */
    if (c->owner_tid != 0) return C_OK;
    /* If IO thread is already reading, return C_OK to make sure the main thread will not handle it. */
    if (c->io_read_state != CLIENT_IDLE) return C_OK;
    if (c->io_write_state == CLIENT_PENDING_IO) return C_OK;
    /* For simplicity, don't offload replica clients reads as read traffic from replica is negligible */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) return C_ERR;
    /* With Lua debug client we may call connWrite directly in the main thread */
    if (c->flag.lua_debug) return C_ERR;
    /* For simplicity let the main-thread handle the blocked clients */
    if (c->flag.blocked || c->flag.unblocked) return C_ERR;
    if (c->flag.close_asap) return C_ERR;

    c->read_flags = canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE;
    c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    c->read_flags |= isReplicatedClient(c) ? READ_FLAGS_REPLICATED : 0;

    c->io_read_state = CLIENT_PENDING_IO;
    connSetPostponeUpdateState(c->conn, 1);

    if (unlikely(spmcEnqueue(&io_shared_inbox, tagJob(c, JOB_REQ_READ_CLIENT)) == false)) {
        c->read_flags = 0;
        c->io_read_state = CLIENT_IDLE;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    io_jobs_submitted++;
    server.stat_io_reads_pending++;
    c->flag.pending_read = 1;
    return C_OK;
}

/* This function attempts to offload the client's write to an I/O thread.
 * Returns C_OK if the client's writes were successfully offloaded to an I/O thread,
 * or C_ERR if the client is not eligible for offloading. */
int trySendWriteToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* Door-2 ownership: owned clients must NEVER enter the SPSC write-offload
     * path. Their writes happen either worker-locally (WRITE_FLAGS_OWNED_LOCAL
     * in the owned read path) or synchronously on main under the owner
     * worker's loop lock (handleClientsWithPendingWrites). Routing them here
     * would create a third concurrent writer on an arbitrary IO thread that
     * neither protocol synchronizes with: double JOB_RES_WRITE_CLIENT events
     * (networking.c:3392 assert) and racing socket writes. */
    if (c->owner_tid != 0) return C_ERR;
    /* The I/O thread is already writing for this client. */
    if (c->io_write_state != CLIENT_IDLE) return C_OK;
    if (c->io_read_state == CLIENT_PENDING_IO) return C_ERR;
    /* Nothing to write */
    if (!clientHasPendingReplies(c)) return C_ERR;
    /* For simplicity, avoid offloading non-online replicas */
    if (getClientType(c) == CLIENT_TYPE_REPLICA && c->repl_data->repl_state != REPLICA_STATE_ONLINE) return C_ERR;
    /* We can't offload debugged clients as the main-thread may read at the same time  */
    if (c->flag.lua_debug) return C_ERR;
    /* Avoid offloading writes to IO thread for the slot migration export job while snapshotting.
     * During this phase, replies accumulate in the output buffer but must not be flushed
     * as concurrent IO thread writes would race with the main thread processing incoming
     * ACKs on the same client's query buffer. */
    if (c->slot_migration_job && !clusterSlotMigrationShouldInstallWriteHandler(c)) return C_ERR;

    int is_replica = getClientType(c) == CLIENT_TYPE_REPLICA;
    clientReplyBlock *block = NULL;
    if (is_replica) {
        c->io_last_reply_block = listLast(server.repl_buffer_blocks);
        replBufBlock *o = listNodeValue(c->io_last_reply_block);
        c->io_last_bufpos = o->used;
    } else {
        /* Save the last block of the reply list to io_last_reply_block and the used
         * position to io_last_bufpos. The I/O thread will write only up to
         * io_last_bufpos, regardless of the c->bufpos value. This is to prevent I/O
         * threads from reading data that might be invalid in their local CPU cache. */
        c->io_last_reply_block = listLast(c->reply);
        if (c->io_last_reply_block) {
            block = (clientReplyBlock *)listNodeValue(c->io_last_reply_block);
            c->io_last_bufpos = block->used;
        } else {
            c->io_last_bufpos = (size_t)c->bufpos;
        }
    }

    serverAssert(c->bufpos > 0 || c->io_last_bufpos > 0 || is_replica);

    /* The main-thread will update the client state after the I/O thread completes the write. */
    connSetPostponeUpdateState(c->conn, 1);
    c->write_flags = is_replica ? WRITE_FLAGS_IS_REPLICA : 0;
    c->io_write_state = CLIENT_PENDING_IO;
    void *job = tagJob(c, JOB_REQ_WRITE_CLIENT);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
        c->io_write_state = CLIENT_IDLE;
        connSetPostponeUpdateState(c->conn, 0);
        c->write_flags = 0;
        c->io_last_reply_block = NULL;
        c->io_last_bufpos = 0;
        return C_ERR;
    }
    /* Force new header after successful enqueue so the main thread doesn't
     * extend a header the I/O thread is currently reading. */
    if (!is_replica) {
        if (block) {
            if (block->flag.buf_encoded) block->last_header = NULL;
        } else {
            if (c->flag.buf_encoded) c->last_header = NULL;
        }
    }
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }

    io_jobs_submitted++;
    server.stat_io_writes_pending++;
    return C_OK;
}

/* Internal function to free the client's argv in an IO thread. */
void ioThreadFreeArgv(robj **argv) {
    int last_arg = 0;
    for (int i = 0;; i++) {
        robj *o = argv[i];
        if (o == NULL) {
            continue;
        }

        /* The main-thread set the refcount to 0 to indicate that this is the last argument to free */
        if (objectGetRefcount(o) == 0) {
            last_arg = 1;
            o->refcount = 1;
        }

        decrRefCount(o);

        if (last_arg) {
            break;
        }
    }

    zfree(argv);
}

/* This function attempts to offload the client's argv to an IO thread.
 * Returns C_OK if the client's argv were successfully offloaded to an IO thread,
 * C_ERR otherwise. */
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv) {
    if (server.active_io_threads_num <= 1 || argc == 0) {
        return C_ERR;
    }

    int target_id = c->cur_tid;
    if (target_id < 1 || target_id >= server.active_io_threads_num) {
        target_id = (c->id % (server.active_io_threads_num - 1)) + 1;
    }

    if (spscIsFull(&io_private_inbox[target_id])) {
        return C_ERR;
    }

    int last_arg_to_free = -1;

    /* Prepare the argv */
    for (int j = 0; j < argc; j++) {
        if (argv[j]->refcount > 1) {
            decrRefCount(argv[j]);
            /* Set argv[j] to NULL to avoid double free */
            argv[j] = NULL;
        } else {
            last_arg_to_free = j;
        }
    }

    /* If no argv to free, free the argv array at the main thread */
    if (last_arg_to_free == -1) {
        zfree(argv);
        return C_OK;
    }

    /* We set the refcount of the last arg to free to 0 to indicate that
     * this is the last argument to free. With this approach, we don't need to
     * send the argc to the IO thread and we can send just the argv ptr. */
    argv[last_arg_to_free]->refcount = 0;
    void *job = tagJob(argv, JOB_REQ_FREE_ARGV);
    /* We pass false to enqueue the job without committing the queue index immediately.
     * This allows us to batch multiple free jobs together and
     * commit them in a single operation later in the event loop. This reduces the overhead
     * of memory barriers and cache line bouncing associated
     * with updating the queue's write pointer per job. */
    spscEnqueue(&io_private_inbox[target_id], job, false);
    io_jobs_submitted++;

    return C_OK;
}

/* F8b: submit a staged owned-punted-reply write to the OWNER worker's private
 * SPSC. Caller (main, punt handback) has already snapshotted io_last_*, set
 * write_flags, bumped stat_io_writes_pending for the offload tier, and set
 * io_write_state = CLIENT_PENDING_IO. Enqueue commits immediately: the punt
 * handback is itself batched per drain, and the worker's sweep polls the
 * SPSC every iteration. */
void ioSubmitOwnerWrite(client *c) {
    serverAssert(c->owner_tid != 0 && c->io_write_state == CLIENT_PENDING_IO);
    void *job = tagJob(c, JOB_REQ_OWNER_WRITE);
    spscEnqueue(&io_private_inbox[c->owner_tid], job, true);
    io_jobs_submitted++;
    /* F13a-v3: ring the parked owner ONLY at low load (small last drain).
     * At saturation the ring's instant wake destroys worker/main batching
     * amortization (−7% s20 measured) while buying nothing — the park is
     * shorter than the RTT. At low load the ring is the 2000x latency win.
     * One plain load + compare; both sides main-thread. */
    if (dplus_last_drain_jobs <= DPLUS_F13_RING_MAX_DRAIN &&
        atomic_load_explicit(&io_private_inbox[c->owner_tid].consumer_parked, memory_order_acquire)) {
        ringWorkerWakePipe(c->owner_tid);
    }
}

/* This function attempts to offload the free of an object to an IO thread.
 * Returns C_OK if the object was successfully offloaded to an IO thread,
 * C_ERR otherwise.*/
int tryOffloadFreeObjToIOThreads(robj *obj) {
    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    if (obj->refcount > 1) return C_ERR;

    if (obj->encoding != OBJ_ENCODING_RAW || obj->type != OBJ_STRING) return C_ERR;

    void *job = tagJob(obj, JOB_REQ_FREE_OBJ);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) return C_ERR;
    io_jobs_submitted++;
    server.stat_io_freed_objects++;
    return C_OK;
}

/* This function retrieves the results of the IO Thread poll.
 * returns the number of fired events if the IO thread has finished processing poll events, 0 otherwise. */
static int getIOThreadPollResults(aeEventLoop *eventLoop) {
    int io_state;
    io_state = atomic_load_explicit(&server.io_poll_state, memory_order_acquire);
    if (io_state == AE_IO_STATE_POLL) {
        /* IO thread is still processing poll events. */
        return 0;
    }

    /* IO thread is done processing poll events. */
    serverAssert(io_state == AE_IO_STATE_DONE);
    server.stat_poll_processed_by_io_threads++;
    server.io_poll_state = AE_IO_STATE_NONE;

    /* Remove the custom poll proc. */
    aeSetCustomPollProc(eventLoop, NULL);
    aeSetPollProtect(eventLoop, 0);
    return server.io_ae_fired_events;
}

void trySendPollJobToIOThreads(void) {
    if (server.active_io_threads_num <= 1) {
        return;
    }

    /* If there are no pending jobs, let the main thread do the poll-wait by itself. */
    if (getPendingIOResponsesCount() == 0) {
        return;
    }

    /* If the IO thread is already processing poll events, don't send another job. */
    if (server.io_poll_state != AE_IO_STATE_NONE) {
        return;
    }

    void *job = tagJob(server.el, JOB_REQ_POLL);

    server.io_poll_state = AE_IO_STATE_POLL;
    aeSetPollProtect(server.el, 1);

    /* Use SPMC to minimize polling overhead. At high thread counts, use private SPSC queues for lower latency. */
    if (server.active_io_threads_num <= 9) {
        if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
    } else {
        cur_epoll_thread = ((cur_epoll_thread) % (server.active_io_threads_num - 1)) + 1;
        if (unlikely(spscIsFull(&io_private_inbox[cur_epoll_thread]))) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
        spscEnqueue(&io_private_inbox[cur_epoll_thread], job, true);
    }

    aeSetCustomPollProc(server.el, getIOThreadPollResults);
    io_jobs_submitted++;
}

/* Door-2 wakeup coalescing: ring main's wakeup pipe only if no ring is
 * already in flight (doorbell disarmed). Called by workers after publishing
 * a response into io_shared_outbox (or its backlog flush).
 *
 * Lost-wakeup proof (Dekker pairing with the disarm at the top of
 * processIOThreadsResponses): our enqueue store and main's disarm store are
 * both ordered before the respective subsequent loads via seq_cst fences.
 * In the single total order, either main's queue peek observes our job, or
 * our doorbell load observes main's disarm (and we ring). Either way the
 * response is drained without waiting for the timer. */
static void ringMainDoorbell(void) {
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&io_main_doorbell, memory_order_relaxed)) {
        dplus_thread_stats[thread_id].doorbell_coalesced++;
        return;
    }
    if (atomic_exchange_explicit(&io_main_doorbell, 1, memory_order_seq_cst)) {
        dplus_thread_stats[thread_id].doorbell_coalesced++;
        return;
    }
    dplus_thread_stats[thread_id].doorbell_rings++;
    /* We won the arm race: exactly one byte per main sleep/drain cycle.
     * EINTR: retry (the doorbell is armed; failing to write would defer the
     * response to the timer). EAGAIN (pipe full): a wakeup is already
     * pending in the pipe — free to skip. */
    while (write(server.module_pipe[1], "A", 1) != 1) {
        if (errno != EINTR) break;
    }
}

void sendToMainThread(void *data, int type) {
    if (unlikely(pending_io_responses)) {
        flushPendingIOResponses(0);
    }
    void *job = tagJob(data, type);
    if (unlikely(pending_io_responses || !mpscEnqueue(&io_shared_outbox, job, &io_thread_ticket))) {
        /* Failed to push new job: initialize list if needed and save job */
        if (pending_io_responses == NULL) {
            pending_io_responses = listCreate();
        }
        listAddNodeTail(pending_io_responses, job);
    }
    /* Door-2 ownership: main owns NO client fds, so its event loop sleeps in
     * epoll for up to a full server-hz tick (~100ms) with our response queued.
     * Wake it the way module threads do — one byte down the module pipe —
     * but COALESCED: only the response that transitions main's view from
     * empty to non-empty pays the syscall (fix2flame measured the
     * per-response scheme at 23.3% of main in modulePipeReadable + ~5%
     * worker-side pipe writes at the P10 ceiling). */
    if (server.io_threads_ownership) {
        ringMainDoorbell();
    }
}

static void ioThreadAccept(client *c) {
    connAccept(c->conn, NULL);
    atomic_thread_fence(memory_order_release);
    c->io_read_state = CLIENT_COMPLETED_IO;
    sendToMainThread(c, JOB_RES_READ_CLIENT);
}

/*
 * Attempts to offload an Accept operation (currently used for TLS accept) for a client
 * connection to I/O threads.
 *
 * Returns:
 *   C_OK  - If the accept operation was successfully queued for processing
 *   C_ERR - If the connection is not eligible for offloading
 *
 * Parameters:
 *   conn - The connection object to perform the accept operation on
 */
int trySendAcceptToIOThreads(connection *conn) {
    if (server.io_threads_num <= 1) {
        return C_ERR;
    }

    if (!(conn->flags & CONN_FLAG_ALLOW_ACCEPT_OFFLOAD)) {
        return C_ERR;
    }

    client *c = connGetPrivateData(conn);
    if (c->io_read_state != CLIENT_IDLE) {
        return C_OK;
    }

    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    c->io_read_state = CLIENT_PENDING_IO;
    c->flag.pending_read = 1;
    connSetPostponeUpdateState(c->conn, 1);

    void *job = tagJob(c, JOB_REQ_ACCEPT);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
        c->io_read_state = CLIENT_IDLE;
        c->flag.pending_read = 0;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    server.stat_io_reads_pending++;
    server.stat_io_accept_offloaded++;
    io_jobs_submitted++;
    return C_OK;
}

/* Function to handle read jobs */
static void handleReadJobs(client **read_jobs, int read_count) {
    int legacy_count = 0;
    /* process each client */
    for (int i = 0; i < read_count; i++) {
        client *c = read_jobs[i];
        /* Discriminate by the STABLE per-read flag set at the submit/entry
         * site, NOT by owner_tid: freeClient's detach and disownClient zero
         * owner_tid while an owned handoff can still be queued here, which
         * made this decrement fire for reads that never incremented the
         * counter (underflow assert under accept/kill churn). */
        if (!(c->read_flags & READ_FLAGS_OWNED_HANDOFF)) legacy_count++;
        processClientIOReadsDone(c);
    }
    /* Only decrement for legacy (offloaded) reads — owned-path handoffs
     * bypass stat_io_reads_pending on the submit side. */
    server.stat_io_reads_pending -= legacy_count;
    serverAssert(server.stat_io_reads_pending >= 0);

    /* Process commands in batch if we processed any reads */
    if (read_count) {
        server.stat_io_reads_processed += read_count;
        processClientsCommandsBatch();
    }
}

/* Function to handle write jobs */
static void handleWriteJobs(client **write_jobs, int write_count) {
    /* Door-2: worker-local completions (owning worker wrote its own client's
     * reply and sent JOB_RES_WRITE_CLIENT for cleanup only) never passed
     * through trySendWriteToIOThreads, so they never incremented
     * stat_io_writes_pending — don't decrement for them. */
    int offloaded = 0;
    for (int i = 0; i < write_count; i++) {
        if (!(write_jobs[i]->write_flags & (WRITE_FLAGS_OWNED_LOCAL | WRITE_FLAGS_OWNED_HANDLER))) offloaded++;
    }
    server.stat_io_writes_pending -= offloaded;
    serverAssert(server.stat_io_writes_pending >= 0);

    for (int i = 0; i < write_count; i++) {
        client *c = write_jobs[i];
        server.stat_io_writes_processed++;
        processClientIOWriteDone(c);
    }
}

#define JOB_BATCH_SIZE (16)
int processIOThreadsResponses(void) {
    /* We don't check for threads number  since some threads may return jobs then deactivate/shut-down */

    /* Door-2 wakeup coalescing: disarm the doorbell BEFORE peeking the
     * queue. Dekker pairing with ringMainDoorbell(): after this
     * store+fence, any response enqueued before a worker's doorbell check
     * is visible to the peek below, and any enqueued after it will find
     * the doorbell disarmed and ring. Gated on ownership so OFF mode stays
     * byte-identical (gate-off cost-free is a standing verdict). */
    if (server.io_threads_ownership) {
        atomic_store_explicit(&io_main_doorbell, 0, memory_order_seq_cst);
        atomic_thread_fence(memory_order_seq_cst);
    }

    /* Quick check if any pending operations exist.
     * Note: owned-client reads bypass io_jobs_submitted/finished accounting,
     * so also peek at the MPSC outbox directly for their responses. */
    if (getPendingIOResponsesCount() == 0) {
        /* Door-2: owned clients push to MPSC without incrementing io_jobs_submitted.
         * Check if the MPSC has data by comparing head vs tail. */
        size_t head = atomic_load_explicit(&io_shared_outbox.head, memory_order_relaxed);
        size_t tail = atomic_load_explicit(&io_shared_outbox.tail, memory_order_acquire);
        if (head == tail) {
            /* F13a-v3: empty visit — decay the load signal so a system going
             * idle re-opens the ring gate within a few main-loop iterations. */
            dplus_last_drain_jobs >>= 1;
            return 0;
        }
    }

    int total_processed = 0;
    void *jobs[JOB_BATCH_SIZE];
    client *read_jobs[JOB_BATCH_SIZE];
    client *write_jobs[JOB_BATCH_SIZE];

    /* Loop until we consume all pending jobs */
    while (1) {
        int received_responses = 0;
        int dequeued_count = 0;
        int read_count = 0;
        int write_count = 0;

        /* Try to dequeue JOB_BATCH_SIZE */
        while (received_responses < JOB_BATCH_SIZE) {
            dequeued_count = mpscDequeueBatch(&io_shared_outbox, jobs, JOB_BATCH_SIZE - received_responses);

            /* Stop if we can't get more jobs from the queue. */
            if (dequeued_count == 0) break;

            received_responses += dequeued_count;
            total_processed += dequeued_count;

            for (int i = 0; i < dequeued_count; i++) {
                void *data;
                int job_type;
                untagJob(jobs[i], &data, &job_type);
                client *c = (client *)data;
                if (job_type == JOB_RES_READ_CLIENT) {
                    if (c->io_read_state != CLIENT_COMPLETED_IO) {
                        serverLog(LL_WARNING,
                            "Door-2 DEQUEUE BUG: client id=%llu owner_tid=%d "
                            "io_read_state=%d io_write_state=%d flags=0x%llx",
                            (unsigned long long)c->id, c->owner_tid,
                            c->io_read_state, c->io_write_state,
                            (unsigned long long)c->raw_flag1);
                    }
                    serverAssert(c->io_read_state == CLIENT_COMPLETED_IO);
                    read_jobs[read_count++] = c;
                } else if (job_type == JOB_RES_WRITE_CLIENT) {
                    serverAssert(c->io_write_state == CLIENT_COMPLETED_IO);
                    write_jobs[write_count++] = c;
                } else {
                    serverPanic("Unknown job type %d", job_type);
                }
            }
        }

        if (read_count) handleReadJobs(read_jobs, read_count);
        if (write_count) handleWriteJobs(write_jobs, write_count);

        /* If the queue was empty at the last try - don't try again */
        if (dequeued_count == 0) {
            /* F13a-v3: record this drain's size as the load signal for ring
             * gating (see ioSubmitOwnerWrite). Decay on empty so idle
             * transitions re-open the gate. Main-thread-only, plain. */
            if (total_processed > 0)
                dplus_last_drain_jobs = total_processed;
            else
                dplus_last_drain_jobs >>= 1;
            return total_processed;
        }
    }
}
