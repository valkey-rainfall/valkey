/* specread Read-Side Prototype — Speculative GET execution on IO threads.
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

#include <assert.h> /* S2.2 bracket parity checks */

#ifndef SPECREAD_H
#define SPECREAD_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

struct client;
struct serverObject;

/* --- Component 1: Sharded version array --- */

/* Number of version shards. 256 = 32 cache lines × 8 counters per line = 2KB. */
#define SPECREAD_VERSION_SHARDS 256

/* Cache line size for alignment. */
#define SPECREAD_CACHELINE 64

/* Shard index from a pre-computed hash (free — hash already computed for lookup). */
#define SPECREAD_SHARD_INDEX(hash) ((hash) & (SPECREAD_VERSION_SHARDS - 1))

/* Version array: 256 counters, grouped 8 per cache line (32 lines total = 2KB).
 * Stored at the tail of struct hashtable via tail allocation. */
typedef struct specreadVersionLine {
    _Atomic(uint64_t) v[8] __attribute__((aligned(64))); /* 8 counters per line */
} specreadVersionLine;

/* Full sharded version array: 32 lines × 8 counters = 256 shards. */
#define SPECREAD_VERSION_LINES (SPECREAD_VERSION_SHARDS / 8)

struct specreadVersionArray {
    specreadVersionLine lines[SPECREAD_VERSION_LINES];
};

/* --- Component 4: Exclusive mode --- */

/* Global exclusive_mode flag. Set by commands that require full atomicity
 * (EVAL, MULTI-EXEC, KEYS, DEBUG, FLUSHALL/FLUSHDB, active defrag start).
 * Readers check this before speculating. */
extern _Atomic(int) specread_exclusive_mode;

/* Epoch/QSBR reader states. OFFLINE and QUIESCENT never hold speculative
 * pointers; every other value is the epoch announced by an active reader. */
#define SPECREAD_MAX_IO_THREADS 256
#define SPECREAD_READER_OFFLINE 0
#define SPECREAD_READER_QUIESCENT UINT64_MAX

/* IO-thread lifecycle hooks. A slot is initialized QUIESCENT before thread
 * creation and becomes OFFLINE only after pthread_join completes. */
void specreadReaderWorkerOnline(int tid);
void specreadReaderWorkerQuiescent(int tid);
void specreadReaderWorkerOffline(int tid);

/* --- Per-IO-thread command counters (no-enqueue Phase-1) ---
 *
 * When IO threads consume speculated commands locally, they must NOT write
 * to shared stat_numcommands (cache-line bouncing per command kills throughput).
 * Instead, each IO thread accumulates into its own cache-line-aligned counter.
 * The main thread aggregates (add-and-zero) once per event-loop iteration in
 * beforeSleep — a per-LOOP touch, not per-command. */
typedef struct specreadThreadStats {
    long long commands_processed; /* speculated commands consumed on this thread */
    long long usec;               /* wall time spent executing them (for commandstats) */
    long long owned_writes;       /* clean owned-local writes completed worker-side (fix #2) */
    long long owned_net_bytes;    /* bytes written by those completions */
    long long doorbell_rings;     /* wakeup-pipe bytes actually written (coalescing prototype) */
    long long doorbell_coalesced; /* responses that skipped the pipe write (doorbell armed) */
    long long punted_replies_written; /* F7: punted-command replies staged by main, written by owner */
    long long keyspace_hits;      /* E3: speculative GET hits (bypass main's stat_keyspace_hits) */
    long long retire_segs_freed;  /* specread deferred-free segments freed whole on this IO thread */
} __attribute__((aligned(SPECREAD_CACHELINE))) specreadThreadStats;

extern specreadThreadStats specread_thread_stats[SPECREAD_MAX_IO_THREADS];

/* --- Write-tax gate (r100 heuristic) ---
 *
 * Per-IO-thread rolling window tracking the ratio of "punted to main because
 * non-GET/write" batches vs "speculated" batches. When recent traffic is
 * write-heavy, speculation is pure overhead (version misses / wasted prefetches).
 * Skip specreadSpeculateBatch entirely when writes dominate.
 *
 * Implementation: per-thread shift register (64-bit) — each bit represents one
 * batch: 1 = had eligible speculation, 0 = punted (first cmd was non-GET/write).
 * If popcount < threshold, skip. Updated per-batch in specreadSpeculateBatch.
 * No shared-line reads (each thread owns its own counter). */
#define SPECREAD_WRITE_TAX_WINDOW 64
#define SPECREAD_WRITE_TAX_THRESHOLD 8  /* Skip if < 8/64 recent batches speculated */

typedef struct specreadWriteTaxGate {
    uint64_t history;  /* Shift register: bit=1 means batch was speculated */
} __attribute__((aligned(SPECREAD_CACHELINE))) specreadWriteTaxGate;

extern specreadWriteTaxGate specread_write_tax[SPECREAD_MAX_IO_THREADS];

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
    _Atomic(uint64_t) bracket_entry_punts; /* S2.3: odd-version refusals at batch entry --
                                            * the rehash-memo Option-2 discriminator (high
                                            * sustained rate during rehash windows would be
                                            * the evidence RCU chains need) */
    _Atomic(uint64_t) miss_punts;
    _Atomic(uint64_t) b13_waiting_transitions;
    _Atomic(uint64_t) b13_handler_fires;
    _Atomic(uint64_t) b13_read_suspends;
    _Atomic(uint64_t) b13_rearms;
    _Atomic(uint64_t) b13_info_lock_calls;
    _Atomic(uint64_t) b13_info_lock_wait_us;
    _Atomic(uint64_t) b13_info_lock_hold_us;
} __attribute__((aligned(SPECREAD_CACHELINE))) specreadStats;

/* One slot per thread, each on its own cache lines. A counter is written only
 * by the thread that owns the slot, so an increment is an uncontended RMW on a
 * line no other core writes; INFO sums the slots. A single shared struct would
 * put every thread's increments on one line and turn the stats build into a
 * coherence benchmark instead of a measurement of the code under test. */
extern specreadStats specread_stats[SPECREAD_MAX_IO_THREADS];

#define SPECREAD_STAT_ADD(field, n) \
    atomic_fetch_add_explicit(&specread_stats[getCurTid()].field, (n), memory_order_relaxed)

/* Sum of one counter across all thread slots (relaxed; INFO-time read). */
uint64_t specreadStatsSum(size_t field_offset);
#define SPECREAD_STAT_SUM(field) specreadStatsSum(offsetof(specreadStats, field))
#endif

/* D5: owned-client over-limit signal (correctness machinery — present in
 * flagless builds, NOT gated on IO_LOOKUP_OFFLOAD_STATS). */
extern _Atomic int specread_client_mem_pressure;

/* --- API declarations --- */

/* Component 1: Version manipulation (implemented in hashtable.c) */
void specreadVersionArrayInit(specreadVersionArray *va);
static inline uint64_t specreadVersionRead(specreadVersionArray *va, unsigned shard) {
    return atomic_load_explicit(&va->lines[shard / 8].v[shard % 8], memory_order_acquire);
}
/* Seqlock reader exit: validate that the shard version is unchanged since
 * v_before. The acquire FENCE is load-bearing: the caller's value copy uses
 * plain loads, and without the fence those reads may sink below the version
 * re-read on weakly-ordered targets (ARM64), making validation vacuous. An
 * acquire load on the re-read alone does NOT provide this ordering (it only
 * constrains later accesses). Fence + re-read is the standard seqlock
 * read_seqretry shape (smp_rmb before the second sequence read). ALWAYS use
 * this helper for the post-copy check — never call specreadVersionRead directly
 * to validate. */
static inline bool specreadVersionValidate(specreadVersionArray *va, unsigned shard, uint64_t v_before) {
    atomic_thread_fence(memory_order_acquire);
    /* S2.2a: odd v_before means the reader raced a bracket -- never valid.
     * (Entry-side refusal should have punted already; this is the backstop.) */
    return ((v_before & 1) == 0) && specreadVersionRead(va, shard) == v_before;
}
/* --- S2.2: odd/even bracket primitives (§5.2 paired protocol) ---
 * Every main-thread mutation of reader-reachable state is bracketed:
 * Begin makes the version ODD (new readers refuse at entry; in-flight
 * readers will fail validation), End makes it EVEN again. Both are release
 * stores (S1.1). Parity is a global invariant: versions are even at rest --
 * ALL bump sites must use brackets; a lone bump leaves odd residue that
 * permanently punts the shard. assert() (compiled out with NDEBUG) checks
 * parity at each transition. */
static inline void specreadVersionBracketBegin(specreadVersionArray *va, unsigned shard) {
    _Atomic(uint64_t) *slot = &va->lines[shard / 8].v[shard % 8];
    uint64_t v = atomic_load_explicit(slot, memory_order_relaxed);
    assert((v & 1) == 0); /* even at rest */
    atomic_store_explicit(slot, v + 1, memory_order_release);
}
static inline void specreadVersionBracketEnd(specreadVersionArray *va, unsigned shard) {
    _Atomic(uint64_t) *slot = &va->lines[shard / 8].v[shard % 8];
    uint64_t v = atomic_load_explicit(slot, memory_order_relaxed);
    assert((v & 1) == 1); /* odd inside a bracket */
    atomic_store_explicit(slot, v + 1, memory_order_release);
}
static inline void specreadVersionBracketAllBegin(specreadVersionArray *va) {
    atomic_thread_fence(memory_order_release);
    for (unsigned i = 0; i < SPECREAD_VERSION_SHARDS; i++) {
        _Atomic(uint64_t) *slot = &va->lines[i / 8].v[i % 8];
        uint64_t v = atomic_load_explicit(slot, memory_order_relaxed);
        assert((v & 1) == 0);
        atomic_store_explicit(slot, v + 1, memory_order_relaxed);
    }
}
static inline void specreadVersionBracketAllEnd(specreadVersionArray *va) {
    atomic_thread_fence(memory_order_release);
    for (unsigned i = 0; i < SPECREAD_VERSION_SHARDS; i++) {
        _Atomic(uint64_t) *slot = &va->lines[i / 8].v[i % 8];
        uint64_t v = atomic_load_explicit(slot, memory_order_relaxed);
        assert((v & 1) == 1);
        atomic_store_explicit(slot, v + 1, memory_order_relaxed);
    }
}
static inline void specreadVersionBumpShard(specreadVersionArray *va, unsigned shard) {
    /* Release store — single writer (main thread), so non-RMW load+store is
     * safe. Release is REQUIRED here, not optional: the reader validates by
     * acquire-reloading this slot, and without release the mutation's prior
     * stores may become visible AFTER the bump on weakly-ordered targets
     * (ARM64/Graviton), letting a reader observe the old version alongside
     * torn data and validate successfully. The earlier claim that ordering
     * rides other sync points does not hold for the validation window (see
     * sharded-version-safety-audit-sep4.md). Free on x86-TSO; str→stlr on
     * ARM64. */
    _Atomic(uint64_t) *slot = &va->lines[shard / 8].v[shard % 8];
    atomic_store_explicit(slot, atomic_load_explicit(slot, memory_order_relaxed) + 1, memory_order_release);
}
static inline void specreadVersionBumpAll(specreadVersionArray *va) {
    /* Structural mutation (rehash/resize): publish prior writes, then bump
     * all shards. The release fence must be SEQUENCED BEFORE the relaxed
     * stores (C11 fence–store pairing: an acquire load that observes one of
     * these stores synchronizes with the fence). The previous placement —
     * fence AFTER the stores — provided no synchronizes-with edge for the
     * bumps at all. One fence + relaxed stores is cheaper than 256 release
     * stores on ARM; rare operation either way. */
    atomic_thread_fence(memory_order_release);
    for (unsigned i = 0; i < SPECREAD_VERSION_SHARDS; i++) {
        _Atomic(uint64_t) *slot = &va->lines[i / 8].v[i % 8];
        atomic_store_explicit(slot, atomic_load_explicit(slot, memory_order_relaxed) + 1, memory_order_relaxed);
    }
}

/* Component 2: Read-only find (implemented in hashtable.c) */
bool hashtableFindReadOnly(hashtable *ht, const void *key, void **found);

/* Component 2: Speculative find with pre-computed hash (implemented in hashtable.c) */
bool hashtableFindSpeculative(void *ht, const void *key, void **found, uint64_t hash,
                              unsigned shard, uint64_t *ver_out);

/* Component 2: Hash key accessor (implemented in hashtable.c) */
uint64_t hashtableHashKey(hashtable *ht, const void *key);

/* Component 2: Version array accessor (implemented in hashtable.c) */
specreadVersionArray *hashtableGetVersionArray(hashtable *ht);

/* Component 4: Exclusive mode helpers (implemented in specread.c) */
void specreadExclusiveEnter(void);  /* Main thread: set exclusive + spin-wait */
void specreadExclusiveLeave(void);  /* Main thread: clear exclusive */

/* ACL/AUTH gate for speculation (main-thread writers; workers read the
 * per-client spec_acl_ok byte inside specreadSpeculateBatch). */
void specreadRecomputeSpecAclOk(struct client *c);
void specreadOnAclRulesChanged(void);
/* F6: called when module command-result SUCCESS listener count transitions. */
void specreadOnCommandResultListenersChanged(int success_listeners);
void specreadOnMonitorsChanged(void);

/* Component 2: Speculative GET execution on IO thread (implemented in specread.c) */
struct client;

/* specread (S1.5): OOM-pinning invariant. A worker's reader slot must be QUIESCENT
 * or OFFLINE whenever the worker parks/sleeps -- an ACTIVE(epoch) slot held
 * across a park pins retirement segments (memory grows unboundedly) and
 * stalls every exclusive-gate entrant up to the 5s panic bound. All
 * speculation exit paths publish quiescence (specreadSpeculateBatch 'out:',
 * ReaderEnter punt paths, thread cancellation cleanup); this assert catches
 * any future path that forgets. Debug builds only -- reads a seq_cst slot
 * the worker itself owns, so it is race-free by the single-writer rule. */
void specreadReaderAssertParkSafe(int tid);

/* Component 2/5: IO-thread batch speculation (implemented in specread.c).
 * Called from ioThreadReadQueryFromClient. Returns count of commands
 * speculatively completed. tid = IO thread index (1..N-1). */
/* Quiescence-deferred reclamation (entry-lifetime-design.md). */
#define SPECREAD_LIMBO_SYNC 0
#define SPECREAD_LIMBO_ASYNC 1
#define SPECREAD_LIMBO_OFFLOAD_PREF 2
#define SPECREAD_LIMBO_RAW 3 /* zfree() at flush — bucket arrays etc. */
int specreadDeferFree(struct serverObject *o, int route);
int specreadDeferFreeRaw(void *ptr);
void specreadReclaimRetired(void);
void specreadForceReclaimAll(void);
size_t specreadLimboPeak(void);
int specreadDebugHoldNextReader(long long usec);
int specreadDebugHoldPrevalidate(long long usec);
void specreadDebugPrevalidateState(uint64_t *holding, uint64_t *consumed);
void specreadDebugPrevalidateBump(void);
int specreadDebugPinReader(uint64_t *epoch);
int specreadDebugUnpinReader(void);
void specreadDebugEpochStats(uint64_t stats[8]);

int specreadSpeculateBatch(struct client *c, int tid);

/* Component 5: IO-thread consumption (implemented in specread.c).
 * Called from ioThreadReadQueryFromClient AFTER specreadSpeculateBatch succeeds.
 * Consumes the N speculated commands at the IO thread: frees argv, advances
 * cmd_queue, increments per-thread stats. The main thread never sees these
 * commands. Returns count of commands consumed. */
void specreadConsumeSpeculated(struct client *c, int count, int tid);

/* Aggregate per-IO-thread command counters into server.stat_numcommands.
 * Called once per event-loop iteration from beforeSleep. O(num_io_threads). */
void specreadAggregateStats(void);
/* Door-2 wakeup-coalescing counters (prototype instrumentation). */
long long specreadDoorbellRings(void);
long long specreadDoorbellCoalesced(void);

/* Maximum embedded value size for speculative copy. Larger values punt.
 * W14 (Rain-blessed Aug 25 2026): 1024 is the Track-1 shipping default. The
 * value-size sweep showed a smooth gradient with NO cliff (96B parity, 512B
 * 3.3x above OFF, 1KB gradient) and wider copies did not inflate validation
 * misses, so no per-size knob is warranted (simplicity bar). specread-only constant,
 * not a standalone upstream PR. */
#define SPECREAD_MAX_SPECULATIVE_VALUE_LEN 1024

/* Component 6: INFO section. Epoch engagement/lifecycle gauges are always
 * available; detailed speculative counters remain build-flag dependent. */
sds specreadInfoString(sds info);

/* Free one whole specread deferred-free segment on an IO thread. The segment was
 * detached from the retire list and handed off by the reclaim walk; this frees
 * every entry by its recorded route and then frees the segment metadata. `tid`
 * is the owning IO thread (single writer of its per-thread stats slot). */
void specreadFreeRetireSegmentOnWorker(void *segment, int tid);

#endif /* SPECREAD_H */
