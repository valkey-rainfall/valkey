/* D+ Read-Side Prototype — Speculative GET execution on IO threads.
 *
 * GET-shaped commands execute entirely on the IO thread that parsed them:
 * optimistic lookup validated by a sharded seqlock, reply written into the
 * client's output buffer by the same thread. The main thread never sees a
 * successful speculative GET. Mutations, validation misses, and everything
 * non-GET-shaped punt to the main thread via today's parsed-command queue.
 *
 * REPLY-BUFFER ORDERING GUARANTEE:
 * The "contiguous prefix" rule is the key invariant. The IO thread writes
 * speculative replies for a contiguous prefix of GET commands in the batch.
 * As soon as a non-speculative command is encountered (write, non-GET,
 * validation fail, exclusive mode), ALL remaining commands in that client's
 * batch are punted to main-thread processing.
 * Since:
 *   (a) speculative replies are written first (IO thread, before main-thread
 *       processClientIOReadsDone dispatches), and
 *   (b) punted commands are processed strictly after the IO-thread read
 *       completes (main thread calls processPendingCommandAndInputBuffer),
 * the reply buffer naturally accumulates in command order:
 *   [spec_reply_0][spec_reply_1]...[spec_reply_k][main_reply_k+1]...
 *
 * We NEVER speculate a command that has an un-speculated predecessor in the
 * same batch.
 */

#ifndef DPLUS_H
#define DPLUS_H

#include <stdatomic.h>
#include <stdint.h>
#include <stdbool.h>

/* --- Component 1: Sharded version array --- */

/* Number of version shards. 256 = 32 cache lines × 8 counters per line = 2KB. */
#define DPLUS_VERSION_SHARDS 256

/* Cache line size for alignment. */
#define DPLUS_CACHELINE 64

/* Shard index from a pre-computed hash (free — hash already computed for lookup). */
#define DPLUS_SHARD_INDEX(hash) ((hash) & (DPLUS_VERSION_SHARDS - 1))

/* Version array: 256 counters, grouped 8 per cache line (32 lines total = 2KB).
 * Stored at the tail of struct hashtable via tail allocation. */
typedef struct dplusVersionLine {
    _Atomic(uint64_t) v[8] __attribute__((aligned(64))); /* 8 counters per line */
} dplusVersionLine;

/* Full sharded version array: 32 lines × 8 counters = 256 shards. */
#define DPLUS_VERSION_LINES (DPLUS_VERSION_SHARDS / 8)

struct dplusVersionArray {
    dplusVersionLine lines[DPLUS_VERSION_LINES];
};

/* --- Component 4: Exclusive mode --- */

/* Global exclusive_mode flag. Set by commands that require full atomicity
 * (EVAL, MULTI-EXEC, KEYS, DEBUG, FLUSHALL/FLUSHDB, active defrag start).
 * Readers check this before speculating. */
extern _Atomic(int) dplus_exclusive_mode;

/* Per-IO-thread in_speculative_read flag. Reader sets before speculation,
 * clears after. Writer spins until all clear before running exclusive ops. */
#define DPLUS_MAX_IO_THREADS 256
extern _Atomic(int) dplus_in_speculative_read[DPLUS_MAX_IO_THREADS];

/* --- Per-IO-thread command counters (no-enqueue Phase-1) ---
 *
 * When IO threads consume speculated commands locally, they must NOT write
 * to shared stat_numcommands (cache-line bouncing per command kills throughput).
 * Instead, each IO thread accumulates into its own cache-line-aligned counter.
 * The main thread aggregates (add-and-zero) once per event-loop iteration in
 * beforeSleep — a per-LOOP touch, not per-command. */
typedef struct dplusThreadStats {
    long long commands_processed; /* speculated commands consumed on this thread */
    long long usec;               /* wall time spent executing them (for commandstats) */
    long long owned_writes;       /* clean owned-local writes completed worker-side (fix #2) */
    long long owned_net_bytes;    /* bytes written by those completions */
    long long doorbell_rings;     /* wakeup-pipe bytes actually written (coalescing prototype) */
    long long doorbell_coalesced; /* responses that skipped the pipe write (doorbell armed) */
    long long punted_replies_written; /* F7: punted-command replies staged by main, written by owner */
    /* Q7 punt round-trip stage instrumentation (per-thread, race-free). */
    long long punt_rt_count;      /* number of complete punt round-trips measured */
    long long sum_d01;            /* cumulative T1-T0 (queue wait: worker->main) in µs */
    long long sum_d12;            /* cumulative T2-T1 (main execution time) in µs */
    long long sum_d23;            /* cumulative T3-T2 (return wait: main->worker) in µs */
    /* Q7b cadence counters. */
    long long sweeps;             /* outer loop iterations (per-worker sweep rate) */
    long long handoffs_started;   /* Q7c: T0 count (per owner thread) */
    long long handoffs_done;      /* Q7c: T3 count */
    long long sum_d2b;            /* F9 probe: T2→owner-dequeue (SPSC wait) in µs */
    char _pad[2 * DPLUS_CACHELINE - 15 * sizeof(long long)]; /* pad to 2 cache lines */
} __attribute__((aligned(DPLUS_CACHELINE))) dplusThreadStats;

extern dplusThreadStats dplus_thread_stats[DPLUS_MAX_IO_THREADS];

/* --- Write-tax gate (r100 heuristic) ---
 *
 * Per-IO-thread rolling window tracking the ratio of "punted to main because
 * non-GET/write" batches vs "speculated" batches. When recent traffic is
 * write-heavy, speculation is pure overhead (version misses / wasted prefetches).
 * Skip dplusSpeculateBatch entirely when writes dominate.
 *
 * Implementation: per-thread shift register (64-bit) — each bit represents one
 * batch: 1 = had eligible speculation, 0 = punted (first cmd was non-GET/write).
 * If popcount < threshold, skip. Updated per-batch in dplusSpeculateBatch.
 * No shared-line reads (each thread owns its own counter). */
#define DPLUS_WRITE_TAX_WINDOW 64
#define DPLUS_WRITE_TAX_THRESHOLD 8  /* Skip if < 8/64 recent batches speculated */

typedef struct dplusWriteTaxGate {
    uint64_t history;  /* Shift register: bit=1 means batch was speculated */
} __attribute__((aligned(DPLUS_CACHELINE))) dplusWriteTaxGate;

extern dplusWriteTaxGate dplus_write_tax[DPLUS_MAX_IO_THREADS];

/* --- Component 6: Stats (behind -DIO_LOOKUP_OFFLOAD_STATS) --- */

#ifdef IO_LOOKUP_OFFLOAD_STATS
typedef struct {
    _Atomic(uint64_t) speculative_attempts;
    _Atomic(uint64_t) speculative_hits;
    _Atomic(uint64_t) validation_misses;
    _Atomic(uint64_t) exclusive_punts;
    _Atomic(uint64_t) large_value_punts;
    _Atomic(uint64_t) expired_replies;
    _Atomic(uint64_t) intra_batch_write_punts;
} dplusStats;

extern dplusStats dplus_stats;
#endif

/* --- API declarations --- */

/* Component 1: Version manipulation (implemented in hashtable.c) */
void dplusVersionArrayInit(dplusVersionArray *va);
static inline uint64_t dplusVersionRead(dplusVersionArray *va, unsigned shard) {
    return atomic_load_explicit(&va->lines[shard / 8].v[shard % 8], memory_order_acquire);
}
static inline void dplusVersionBumpShard(dplusVersionArray *va, unsigned shard) {
    /* Relaxed store — single writer (main thread). The release ordering
     * rides existing sync points (see spec §1, b4v4 lesson). */
    _Atomic(uint64_t) *slot = &va->lines[shard / 8].v[shard % 8];
    atomic_store_explicit(slot, atomic_load_explicit(slot, memory_order_relaxed) + 1, memory_order_relaxed);
}
static inline void dplusVersionBumpAll(dplusVersionArray *va) {
    /* Structural mutation (rehash/resize): bump all shards with relaxed stores,
     * then one release fence to publish. Rare operation. */
    for (unsigned i = 0; i < DPLUS_VERSION_SHARDS; i++) {
        _Atomic(uint64_t) *slot = &va->lines[i / 8].v[i % 8];
        atomic_store_explicit(slot, atomic_load_explicit(slot, memory_order_relaxed) + 1, memory_order_relaxed);
    }
    atomic_thread_fence(memory_order_release);
}

/* Component 2: Read-only find (implemented in hashtable.c) */
bool hashtableFindReadOnly(hashtable *ht, const void *key, void **found);

/* Component 2: Speculative find with pre-computed hash (implemented in hashtable.c) */
bool hashtableFindSpeculative(void *ht, const void *key, void **found, uint64_t hash,
                              unsigned shard, uint64_t *ver_out);

/* Component 2: Hash key accessor (implemented in hashtable.c) */
uint64_t hashtableHashKey(hashtable *ht, const void *key);

/* Component 2: Version array accessor (implemented in hashtable.c) */
dplusVersionArray *hashtableGetVersionArray(hashtable *ht);

/* Component 4: Exclusive mode helpers (implemented in dplus.c) */
void dplusExclusiveEnter(void);  /* Main thread: set exclusive + spin-wait */
void dplusExclusiveLeave(void);  /* Main thread: clear exclusive */

/* Component 2: Speculative GET execution on IO thread (implemented in dplus.c) */
struct client;
int dplusSpeculativeGet(struct client *c, void *key_sds, int resp);

/* Component 2/5: IO-thread batch speculation (implemented in dplus.c).
 * Called from ioThreadReadQueryFromClient. Returns count of commands
 * speculatively completed. tid = IO thread index (1..N-1). */
/* Quiescence-deferred reclamation (entry-lifetime-design.md). */
#define DPLUS_LIMBO_SYNC 0
#define DPLUS_LIMBO_ASYNC 1
#define DPLUS_LIMBO_OFFLOAD_PREF 2
#define DPLUS_LIMBO_RAW 3 /* zfree() at flush — bucket arrays etc. */
int dplusDeferFree(struct serverObject *o, int route);
int dplusDeferFreeRaw(void *ptr);
void dplusFlushLimbo(void);
size_t dplusLimboPeak(void);

int dplusSpeculateBatch(struct client *c, int tid);

/* Component 5: IO-thread consumption (implemented in dplus.c).
 * Called from ioThreadReadQueryFromClient AFTER dplusSpeculateBatch succeeds.
 * Consumes the N speculated commands at the IO thread: frees argv, advances
 * cmd_queue, increments per-thread stats. The main thread never sees these
 * commands. Returns count of commands consumed. */
void dplusConsumeSpeculated(struct client *c, int count, int tid);

/* Aggregate per-IO-thread command counters into server.stat_numcommands.
 * Called once per event-loop iteration from beforeSleep. O(num_io_threads). */
void dplusAggregateStats(void);
/* Door-2 wakeup-coalescing counters (prototype instrumentation). */
long long dplusDoorbellRings(void);
long long dplusDoorbellCoalesced(void);

/* Maximum embedded value size for speculative copy. Larger values punt. */
#define DPLUS_MAX_SPECULATIVE_VALUE_LEN 64

/* Component 6: INFO section (dplus.c, only if -DIO_LOOKUP_OFFLOAD_STATS) */
#ifdef IO_LOOKUP_OFFLOAD_STATS
sds dplusInfoString(sds info);
/* Q7b cadence counters: main-side drain stats (defined in io_threads.c) */
/* DIAG BRANCH ONLY: stats compiled in unconditionally so conductress cells
 * (which do not pass the flag) still instrument. */
#ifndef IO_LOOKUP_OFFLOAD_STATS
#define IO_LOOKUP_OFFLOAD_STATS 1
#endif

void dplusScaleGaugeCron(void);
void dplusGetDrainCounters(long long *calls, long long *nonempty, long long *jobs);
#endif

#endif /* DPLUS_H */
