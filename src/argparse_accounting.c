/* argparse_accounting.c -- Per-thread diagnostic counters for argument parsing.
 *
 * This file is ALWAYS compiled (it must appear in the link for the stub inline
 * functions in the header when the flag is off). The actual counter storage
 * and INFO generator are gated behind #ifdef ARGPARSE_ACCOUNTING. */

#include "argparse_accounting.h"

#ifdef ARGPARSE_ACCOUNTING

#include "sds.h"
#include <stdio.h>
#include <stdatomic.h>

/* Per-thread slot array. Index 0 = main thread, 1..N = IO workers.
 * Each slot is cache-line-aligned (via the struct attribute) to prevent
 * false sharing between concurrent parser threads. */
apaThreadSlot g_apa_slots[IO_THREADS_MAX_NUM];

/* Helper: relaxed-load a single _Atomic uint64_t. */
static inline uint64_t apa_load(const _Atomic uint64_t *p) {
    return atomic_load_explicit(p, memory_order_relaxed);
}

/* Aggregate all per-thread slots into a single snapshot.
 * Uses relaxed loads -- safe to call from main thread while IO workers parse.
 * The snapshot may be slightly stale but never torn (each load is atomic). */
argparseAccounting argparseAccountingAggregate(void) {
    argparseAccounting agg;
    /* Zero-init all _Atomic fields via memset (valid for zero-initializable
     * atomic types, which uint64_t is on all platforms we target). */
    memset(&agg, 0, sizeof(agg));

    for (int t = 0; t < IO_THREADS_MAX_NUM; t++) {
        const argparseAccounting *s = &g_apa_slots[t].counters;
        /* Skip untouched slots (common case: most slots are zero).
         * Check enough fields to cover all macro entry points. */
        uint64_t cmds = apa_load(&s->commands_total);
        uint64_t args = apa_load(&s->args_total);
        uint64_t allocs = apa_load(&s->argv_initial_allocs);
        uint64_t qallocs = apa_load(&s->cmd_queue_allocs);
        if (cmds == 0 && args == 0 && allocs == 0 && qallocs == 0) continue;

        /* We store into agg using non-atomic += since agg is local. Cast
         * through a non-atomic pointer to avoid _Atomic on the LHS. */
        uint64_t *a = (uint64_t *)&agg.commands_total;
        const _Atomic uint64_t *src = &s->commands_total;
        /* Walk all scalar fields in declaration order. The struct layout is:
         * commands_total, args_total, args_by_bucket[4], multibulk_copy_bytes,
         * inline_copy_bytes, big_arg_adopt_count, big_arg_adopt_bytes,
         * argv_initial_allocs, argv_growth_reallocs, argv_initial_slots,
         * argv_growth_slots, cmd_queue_allocs, cmd_queue_slots,
         * ord_count[4], ord_copied_bytes[4], ord_adopted_bytes[4]
         * = 2 + 4 + 2 + 2 + 4 + 2 + 4 + 4 + 4 = 28 uint64_t fields total */
        int nfields = (int)(sizeof(argparseAccounting) / sizeof(_Atomic uint64_t));
        for (int f = 0; f < nfields; f++) {
            a[f] += apa_load(&src[f]);
        }
    }
    return agg;
}

/* Reset all per-thread slots. Must be called with IO threads quiesced
 * (e.g. from DEBUG ARGPARSE-ACCOUNTING RESET or during server init).
 * Uses relaxed stores since no other thread should be writing during reset. */
void argparseAccountingReset(void) {
    for (int t = 0; t < IO_THREADS_MAX_NUM; t++) {
        _Atomic uint64_t *p = &g_apa_slots[t].counters.commands_total;
        int nfields = (int)(sizeof(argparseAccounting) / sizeof(_Atomic uint64_t));
        for (int f = 0; f < nfields; f++) {
            atomic_store_explicit(&p[f], 0, memory_order_relaxed);
        }
    }
}

/* Append the "# ArgparseAccounting" INFO section to *info_sds_ptr.
 * Caller passes &info where info is an sds. */
void argparseAccountingGenInfo(void *info_sds_ptr) {
    sds *pinfo = (sds *)info_sds_ptr;
    argparseAccounting a = argparseAccountingAggregate();

    /* Read aggregated values through non-atomic pointer (agg is local). */
    const uint64_t *v = (const uint64_t *)&a;
    (void)v; /* suppress unused warning if we use named fields below */

    *pinfo = sdscatprintf(*pinfo,
        "# ArgparseAccounting\r\n"
        "apa_commands_total:%llu\r\n"
        "apa_args_total:%llu\r\n"
        "apa_args_tiny_0_63:%llu\r\n"
        "apa_args_small_64_511:%llu\r\n"
        "apa_args_medium_512_32767:%llu\r\n"
        "apa_args_big_32768_plus:%llu\r\n"
        "apa_multibulk_copy_bytes:%llu\r\n"
        "apa_inline_copy_bytes:%llu\r\n"
        "apa_big_arg_adopt_count:%llu\r\n"
        "apa_big_arg_adopt_bytes:%llu\r\n"
        "apa_argv_initial_allocs:%llu\r\n"
        "apa_argv_growth_reallocs:%llu\r\n"
        "apa_argv_initial_slots:%llu\r\n"
        "apa_argv_growth_slots:%llu\r\n"
        "apa_cmd_queue_allocs:%llu\r\n"
        "apa_cmd_queue_slots:%llu\r\n"
        "apa_ord0_cmd_count:%llu\r\n"
        "apa_ord0_cmd_copied_bytes:%llu\r\n"
        "apa_ord0_cmd_adopted_bytes:%llu\r\n"
        "apa_ord1_key_count:%llu\r\n"
        "apa_ord1_key_copied_bytes:%llu\r\n"
        "apa_ord1_key_adopted_bytes:%llu\r\n"
        "apa_ord2_val_count:%llu\r\n"
        "apa_ord2_val_copied_bytes:%llu\r\n"
        "apa_ord2_val_adopted_bytes:%llu\r\n"
        "apa_ord3_rest_count:%llu\r\n"
        "apa_ord3_rest_copied_bytes:%llu\r\n"
        "apa_ord3_rest_adopted_bytes:%llu\r\n",
        (unsigned long long)apa_load(&a.commands_total),
        (unsigned long long)apa_load(&a.args_total),
        (unsigned long long)apa_load(&a.args_by_bucket[APA_BUCKET_TINY]),
        (unsigned long long)apa_load(&a.args_by_bucket[APA_BUCKET_SMALL]),
        (unsigned long long)apa_load(&a.args_by_bucket[APA_BUCKET_MEDIUM]),
        (unsigned long long)apa_load(&a.args_by_bucket[APA_BUCKET_BIG]),
        (unsigned long long)apa_load(&a.multibulk_copy_bytes),
        (unsigned long long)apa_load(&a.inline_copy_bytes),
        (unsigned long long)apa_load(&a.big_arg_adopt_count),
        (unsigned long long)apa_load(&a.big_arg_adopt_bytes),
        (unsigned long long)apa_load(&a.argv_initial_allocs),
        (unsigned long long)apa_load(&a.argv_growth_reallocs),
        (unsigned long long)apa_load(&a.argv_initial_slots),
        (unsigned long long)apa_load(&a.argv_growth_slots),
        (unsigned long long)apa_load(&a.cmd_queue_allocs),
        (unsigned long long)apa_load(&a.cmd_queue_slots),
        (unsigned long long)apa_load(&a.ord_count[APA_ORD_CMD]),
        (unsigned long long)apa_load(&a.ord_copied_bytes[APA_ORD_CMD]),
        (unsigned long long)apa_load(&a.ord_adopted_bytes[APA_ORD_CMD]),
        (unsigned long long)apa_load(&a.ord_count[APA_ORD_KEY]),
        (unsigned long long)apa_load(&a.ord_copied_bytes[APA_ORD_KEY]),
        (unsigned long long)apa_load(&a.ord_adopted_bytes[APA_ORD_KEY]),
        (unsigned long long)apa_load(&a.ord_count[APA_ORD_VAL]),
        (unsigned long long)apa_load(&a.ord_copied_bytes[APA_ORD_VAL]),
        (unsigned long long)apa_load(&a.ord_adopted_bytes[APA_ORD_VAL]),
        (unsigned long long)apa_load(&a.ord_count[APA_ORD_REST]),
        (unsigned long long)apa_load(&a.ord_copied_bytes[APA_ORD_REST]),
        (unsigned long long)apa_load(&a.ord_adopted_bytes[APA_ORD_REST]));
}

#endif /* ARGPARSE_ACCOUNTING */
