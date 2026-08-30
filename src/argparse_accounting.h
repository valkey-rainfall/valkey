/* argparse_accounting.h -- Diagnostic counters for argument parsing overhead.
 *
 * Compiled ONLY when -DARGPARSE_ACCOUNTING is passed. When the flag is absent,
 * every macro expands to nothing -- zero code, zero data, zero branches.
 *
 * THREAD SAFETY
 * Parsing runs on both the main thread (thread_id 0) and IO worker threads
 * (thread_id 1..N). Each thread owns a private, cache-line-separated counter
 * slot indexed by getCurTid(). Every counter is _Atomic uint64_t with relaxed
 * memory ordering so that the INFO aggregator can safely read all slots from
 * the main thread while IO workers parse concurrently, without requiring an
 * IO barrier or quiescence. Relaxed ordering is sufficient because:
 *   - Each counter is a monotonic sum; no ordering between counters matters.
 *   - The INFO reader tolerates slightly stale values (diagnostic counters).
 *   - No control-flow decision depends on cross-thread counter reads.
 *
 * SIZE/INDEX BUCKETS
 * We classify arguments by byte-length into four buckets rather than by
 * command role (key vs value vs command-name). Role classification would
 * require cmd-table lookup inside the parser, adding latency to the path
 * we are trying to measure.
 *
 * ARGUMENT ORDINAL ACCOUNTING
 * We track per-ordinal (arg index 0, 1, 2, 3+) counts and bytes to
 * distinguish command-name (0), key (1), value/first-extra (2), and
 * remaining (3+) without requiring command-table lookup. This is sufficient
 * to characterize SET-family workloads (command/key/value distribution). */

#ifndef ARGPARSE_ACCOUNTING_H
#define ARGPARSE_ACCOUNTING_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ── Size-bucket definitions ──────────────────────────────────────────────── */
/* Argument size buckets:
 *   [0]  0-63 B        tiny (keys, short values, commands)
 *   [1]  64-511 B      small
 *   [2]  512-32767 B   medium (< PROTO_MBULK_BIG_ARG)
 *   [3]  32768+ B      big (>= PROTO_MBULK_BIG_ARG, adoption-eligible)
 */
#define APA_SIZE_BUCKETS     4
#define APA_BUCKET_TINY      0
#define APA_BUCKET_SMALL     1
#define APA_BUCKET_MEDIUM    2
#define APA_BUCKET_BIG       3

/* ── Ordinal bucket definitions ───────────────────────────────────────────── */
/* Argument ordinal buckets:
 *   [0]  arg index 0   command name (SET, GET, etc.)
 *   [1]  arg index 1   key
 *   [2]  arg index 2   value / first extra arg
 *   [3]  arg index 3+  remaining args
 */
#define APA_ORD_BUCKETS      4
#define APA_ORD_CMD          0  /* arg index 0: command name */
#define APA_ORD_KEY          1  /* arg index 1: key */
#define APA_ORD_VAL          2  /* arg index 2: value */
#define APA_ORD_REST         3  /* arg index 3+: remaining */

static inline int apaOrdBucket(int argidx) {
    return argidx < 3 ? argidx : 3;
}

static inline int apaSizeBucket(size_t len) {
    if (len < 64)    return APA_BUCKET_TINY;
    if (len < 512)   return APA_BUCKET_SMALL;
    if (len < 32768) return APA_BUCKET_MEDIUM;
    return APA_BUCKET_BIG;
}

#ifdef ARGPARSE_ACCOUNTING

/* ── Counter structure (one per thread, all counters atomic) ──────────────── */
/* Every counter is _Atomic(uint64_t) so the main-thread INFO aggregator can
 * safely read all slots without IO barrier. Writers use relaxed stores/adds;
 * the reader uses relaxed loads.
 *
 * We use the parenthesized _Atomic(T) form (not bare _Atomic T) so that
 * C++ unit tests can compile the header: wrappers.h provides a shim
 * #define _Atomic(type) alignas(sizeof(type)) type for C++ builds,
 * matching the convention used throughout server.h. */
#ifndef __cplusplus
#include <stdatomic.h>
#endif

typedef struct argparseAccounting {
    /* Per-command aggregates */
    _Atomic(uint64_t) commands_total;           /* Commands fully parsed */
    _Atomic(uint64_t) args_total;               /* Total argument objects created */

    /* Argument allocation by size bucket */
    _Atomic(uint64_t) args_by_bucket[APA_SIZE_BUCKETS];

    /* Bytes copied during parsing */
    _Atomic(uint64_t) multibulk_copy_bytes;     /* Bytes copied via createStringObject in multibulk path */
    _Atomic(uint64_t) inline_copy_bytes;        /* Bytes copied via createObject(sdssplit) in inline path */

    /* Big-argument adoption (zero-copy querybuf handoff) */
    _Atomic(uint64_t) big_arg_adopt_count;      /* Times querybuf was adopted as argv object */
    _Atomic(uint64_t) big_arg_adopt_bytes;      /* Bytes adopted (avoided copy) */

    /* argv array allocations */
    _Atomic(uint64_t) argv_initial_allocs;      /* Initial zmalloc of argv array */
    _Atomic(uint64_t) argv_growth_reallocs;     /* zrealloc growth events during a command */
    _Atomic(uint64_t) argv_initial_slots;       /* Sum of initial slot counts allocated */
    _Atomic(uint64_t) argv_growth_slots;        /* Sum of new slot counts after reallocs */

    /* Pipeline / cmd_queue */
    _Atomic(uint64_t) cmd_queue_allocs;         /* cmd_queue array (re)allocations */
    _Atomic(uint64_t) cmd_queue_slots;          /* Sum of cmd_queue.cap after each alloc */

    /* Per-ordinal accounting (arg index 0/1/2/3+) */
    _Atomic(uint64_t) ord_count[APA_ORD_BUCKETS];      /* Args parsed per ordinal */
    _Atomic(uint64_t) ord_copied_bytes[APA_ORD_BUCKETS]; /* Bytes copied per ordinal */
    _Atomic(uint64_t) ord_adopted_bytes[APA_ORD_BUCKETS]; /* Bytes adopted per ordinal */
} argparseAccounting;

/* ── Per-thread storage ──────────────────────────────────────────────────── */
/* IO_THREADS_MAX_NUM is 256 (config.h). Thread 0 = main. We size the array
 * to IO_THREADS_MAX_NUM so every possible thread_id is covered.
 * Each element is cache-line-padded to prevent false sharing between
 * adjacent slots even though atomics make concurrent reads safe. */
#include "config.h" /* IO_THREADS_MAX_NUM, CACHE_LINE_SIZE */

typedef struct apaThreadSlot {
    argparseAccounting counters;
    /* Pad to a multiple of cache-line size to avoid false sharing between
     * adjacent slots. The alignment attribute ensures slot[0] starts on a
     * cache-line boundary. */
} __attribute__((aligned(CACHE_LINE_SIZE))) apaThreadSlot;

extern apaThreadSlot g_apa_slots[IO_THREADS_MAX_NUM];

/* getCurTid() returns 0 for main, 1..N for IO workers.
 * Declared in io_threads.h but we forward-declare to avoid pulling in
 * the full server header chain from every translation unit. */
int getCurTid(void);

/* ── Hot-path macros (relaxed atomic increments) ──────────────────────────── */
/* Each thread writes only to its own slot. We use relaxed ordering because:
 * (1) no control flow depends on these counters, (2) the INFO reader tolerates
 * slightly stale values, and (3) relaxed is the cheapest memory ordering
 * on both x86 (free) and aarch64 (no barrier).
 *
 * In C++ test builds, _Atomic(uint64_t) is shimmed to plain uint64_t,
 * so we use plain += instead of atomic_fetch_add_explicit. */

#define APA_SLOT()                (&g_apa_slots[getCurTid()].counters)

#ifdef __cplusplus
/* C++ test shim: _Atomic is stripped by wrappers.h, so use plain arithmetic. */
#define APA_COUNTER_INC(ptr)        ((*(ptr))++)
#define APA_COUNTER_ADD(ptr, n)     ((*(ptr)) += (uint64_t)(n))
#else
/* Server C build: explicitly relaxed diagnostic updates. */
#define APA_COUNTER_INC(ptr) \
    atomic_fetch_add_explicit((ptr), 1, memory_order_relaxed)
#define APA_COUNTER_ADD(ptr, n) \
    atomic_fetch_add_explicit((ptr), (uint64_t)(n), memory_order_relaxed)
#endif

#define APA_ATOMIC_INC(field)       APA_COUNTER_INC(&APA_SLOT()->field)
#define APA_ATOMIC_ADD(field, n)    APA_COUNTER_ADD(&APA_SLOT()->field, (n))

/* Legacy compat shims (for any generic INC/ADD usage) */
#define APA_INC(field)            APA_ATOMIC_INC(field)
#define APA_ADD(field, n)         APA_ATOMIC_ADD(field, n)
#define APA_INC_BUCKET(len)       APA_ATOMIC_ADD(args_by_bucket[apaSizeBucket(len)], 1)

/* Called after each command is fully parsed. */
#define APA_COMMAND_DONE()        APA_ATOMIC_INC(commands_total)

/* Argument created via copy (createStringObject) in multibulk path.
 * argidx: 0-based position in the argument vector for this command. */
#define APA_ARG_COPIED(len, argidx) do {                                       \
    argparseAccounting *_s = APA_SLOT();                                       \
    int _ord = apaOrdBucket(argidx);                                           \
    APA_COUNTER_INC(&_s->args_total);                                          \
    APA_COUNTER_INC(&_s->args_by_bucket[apaSizeBucket(len)]);                  \
    APA_COUNTER_ADD(&_s->multibulk_copy_bytes, (len));                          \
    APA_COUNTER_INC(&_s->ord_count[_ord]);                                     \
    APA_COUNTER_ADD(&_s->ord_copied_bytes[_ord], (len));                        \
} while (0)

/* Argument created in inline path.
 * argidx: 0-based position in the argument vector. */
#define APA_ARG_INLINE(len, argidx) do {                                       \
    argparseAccounting *_s = APA_SLOT();                                       \
    int _ord = apaOrdBucket(argidx);                                           \
    APA_COUNTER_INC(&_s->args_total);                                          \
    APA_COUNTER_INC(&_s->args_by_bucket[apaSizeBucket(len)]);                  \
    APA_COUNTER_ADD(&_s->inline_copy_bytes, (len));                             \
    APA_COUNTER_INC(&_s->ord_count[_ord]);                                     \
    APA_COUNTER_ADD(&_s->ord_copied_bytes[_ord], (len));                        \
} while (0)

/* Big-argument adoption (querybuf handed off, no copy).
 * argidx: 0-based position in the argument vector. */
#define APA_ARG_ADOPTED(len, argidx) do {                                      \
    argparseAccounting *_s = APA_SLOT();                                       \
    int _ord = apaOrdBucket(argidx);                                           \
    APA_COUNTER_INC(&_s->args_total);                                          \
    APA_COUNTER_INC(&_s->args_by_bucket[apaSizeBucket(len)]);                  \
    APA_COUNTER_INC(&_s->big_arg_adopt_count);                                 \
    APA_COUNTER_ADD(&_s->big_arg_adopt_bytes, (len));                           \
    APA_COUNTER_INC(&_s->ord_count[_ord]);                                     \
    APA_COUNTER_ADD(&_s->ord_adopted_bytes[_ord], (len));                       \
} while (0)

/* Initial argv allocation. */
#define APA_ARGV_ALLOC(slots) do {                                             \
    argparseAccounting *_s = APA_SLOT();                                       \
    APA_COUNTER_INC(&_s->argv_initial_allocs);                                 \
    APA_COUNTER_ADD(&_s->argv_initial_slots, (slots));                          \
} while (0)

/* argv growth reallocation. */
#define APA_ARGV_GROW(new_slots) do {                                          \
    argparseAccounting *_s = APA_SLOT();                                       \
    APA_COUNTER_INC(&_s->argv_growth_reallocs);                                \
    APA_COUNTER_ADD(&_s->argv_growth_slots, (new_slots));                       \
} while (0)

/* cmd_queue (re)allocation. */
#define APA_CMD_QUEUE_ALLOC(cap) do {                                          \
    argparseAccounting *_s = APA_SLOT();                                       \
    APA_COUNTER_INC(&_s->cmd_queue_allocs);                                    \
    APA_COUNTER_ADD(&_s->cmd_queue_slots, (cap));                               \
} while (0)

/* Reset all per-thread slots (call from main thread only, with IO quiesced). */
void argparseAccountingReset(void);

/* Aggregate across all thread slots and append INFO section.
 * Caller passes &info where info is an sds. */
void argparseAccountingGenInfo(void *info_sds_ptr);

/* Return the aggregated snapshot (sum of all thread slots). */
argparseAccounting argparseAccountingAggregate(void);

#else /* !ARGPARSE_ACCOUNTING -- everything compiles away */

/* Dummy struct so sizeof works in non-enabled test stubs. */
typedef struct argparseAccounting {
    uint64_t dummy;
} argparseAccounting;

#define APA_INC(field)                  ((void)0)
#define APA_ADD(field, n)               ((void)0)
#define APA_INC_BUCKET(len)             ((void)0)
#define APA_COMMAND_DONE()              ((void)0)
#define APA_ARG_COPIED(len, argidx)     ((void)0)
#define APA_ARG_INLINE(len, argidx)     ((void)0)
#define APA_ARG_ADOPTED(len, argidx)    ((void)0)
#define APA_ARGV_ALLOC(slots)           ((void)0)
#define APA_ARGV_GROW(new_slots)        ((void)0)
#define APA_CMD_QUEUE_ALLOC(cap)        ((void)0)

static inline void argparseAccountingReset(void) {}
static inline void argparseAccountingGenInfo(void *info_sds_ptr) { (void)info_sds_ptr; }

#endif /* ARGPARSE_ACCOUNTING */

#endif /* ARGPARSE_ACCOUNTING_H */
