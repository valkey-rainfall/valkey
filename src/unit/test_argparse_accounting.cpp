/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Unit tests for argparse_accounting per-thread diagnostic counters.
 *
 * In the C++ test harness, _Atomic(uint64_t) is shimmed to plain uint64_t
 * via wrappers.h (#define _Atomic(type) alignas(sizeof(type)) type). The
 * struct fields are therefore plain integers in the test binary; relaxed
 * atomic semantics only matter in the real C server binary. Tests access
 * fields directly and use the APA_* macros (which also degrade to plain
 * increments in C++ mode).
 */

#include "generated_wrappers.hpp"
#include <gtest/gtest.h>
#include <cstring>
#include <cstdint>

extern "C" {
#include "argparse_accounting.h"
}

#ifdef ARGPARSE_ACCOUNTING

/* In the unit-test harness the main thread has getCurTid() == 0.
 * We access slots directly for verification.
 * In C++ builds, _Atomic(uint64_t) == uint64_t (via shim), so direct
 * field access is safe and there are no atomic operations to worry about. */

class ArgparseAccountingTest : public ::testing::Test {
protected:
    void SetUp() override {
        argparseAccountingReset();
    }
};

/* ── Reset ────────────────────────────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, ResetZeroesAllSlots) {
    /* Dirty slot 0 (main thread). */
    memset(&g_apa_slots[0].counters, 0xFF, sizeof(argparseAccounting));
    /* Dirty a non-main slot to verify reset covers all. */
    memset(&g_apa_slots[1].counters, 0xAA, sizeof(argparseAccounting));

    argparseAccountingReset();

    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 0u);
    EXPECT_EQ(agg.args_total, 0u);
    for (int i = 0; i < APA_SIZE_BUCKETS; i++) {
        EXPECT_EQ(agg.args_by_bucket[i], 0u);
    }
    EXPECT_EQ(agg.multibulk_copy_bytes, 0u);
    EXPECT_EQ(agg.inline_copy_bytes, 0u);
    EXPECT_EQ(agg.big_arg_adopt_count, 0u);
    EXPECT_EQ(agg.big_arg_adopt_bytes, 0u);
    EXPECT_EQ(agg.argv_initial_allocs, 0u);
    EXPECT_EQ(agg.argv_growth_reallocs, 0u);
    EXPECT_EQ(agg.argv_initial_slots, 0u);
    EXPECT_EQ(agg.argv_growth_slots, 0u);
    EXPECT_EQ(agg.cmd_queue_allocs, 0u);
    EXPECT_EQ(agg.cmd_queue_slots, 0u);
    for (int i = 0; i < APA_ORD_BUCKETS; i++) {
        EXPECT_EQ(agg.ord_count[i], 0u);
        EXPECT_EQ(agg.ord_copied_bytes[i], 0u);
        EXPECT_EQ(agg.ord_adopted_bytes[i], 0u);
    }
}

/* ── Size bucket classification ───────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, SizeBucketClassification) {
    EXPECT_EQ(apaSizeBucket(0), APA_BUCKET_TINY);
    EXPECT_EQ(apaSizeBucket(3), APA_BUCKET_TINY);
    EXPECT_EQ(apaSizeBucket(63), APA_BUCKET_TINY);
    EXPECT_EQ(apaSizeBucket(64), APA_BUCKET_SMALL);
    EXPECT_EQ(apaSizeBucket(511), APA_BUCKET_SMALL);
    EXPECT_EQ(apaSizeBucket(512), APA_BUCKET_MEDIUM);
    EXPECT_EQ(apaSizeBucket(32767), APA_BUCKET_MEDIUM);
    EXPECT_EQ(apaSizeBucket(32768), APA_BUCKET_BIG);
    EXPECT_EQ(apaSizeBucket(1000000), APA_BUCKET_BIG);
}

/* ── Ordinal bucket classification ────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, OrdinalBucketClassification) {
    EXPECT_EQ(apaOrdBucket(0), APA_ORD_CMD);
    EXPECT_EQ(apaOrdBucket(1), APA_ORD_KEY);
    EXPECT_EQ(apaOrdBucket(2), APA_ORD_VAL);
    EXPECT_EQ(apaOrdBucket(3), APA_ORD_REST);
    EXPECT_EQ(apaOrdBucket(4), APA_ORD_REST);
    EXPECT_EQ(apaOrdBucket(100), APA_ORD_REST);
}

/* ── Macro correctness (all go into slot 0 in the test harness) ───────── */

TEST_F(ArgparseAccountingTest, ArgCopiedMacroWithOrdinal) {
    APA_ARG_COPIED(100, 0);  /* command name, small bucket */
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.args_total, 1u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_SMALL], 1u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 100u);
    EXPECT_EQ(agg.inline_copy_bytes, 0u);
    EXPECT_EQ(agg.big_arg_adopt_count, 0u);
    /* Ordinal: command (index 0) */
    EXPECT_EQ(agg.ord_count[APA_ORD_CMD], 1u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_CMD], 100u);
    EXPECT_EQ(agg.ord_adopted_bytes[APA_ORD_CMD], 0u);
    EXPECT_EQ(agg.ord_count[APA_ORD_KEY], 0u);
}

TEST_F(ArgparseAccountingTest, ArgInlineMacroWithOrdinal) {
    APA_ARG_INLINE(10, 1);  /* key, inline */
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.args_total, 1u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_TINY], 1u);
    EXPECT_EQ(agg.inline_copy_bytes, 10u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 0u);
    /* Ordinal: key (index 1) */
    EXPECT_EQ(agg.ord_count[APA_ORD_KEY], 1u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_KEY], 10u);
}

TEST_F(ArgparseAccountingTest, ArgAdoptedMacroWithOrdinal) {
    APA_ARG_ADOPTED(50000, 2);  /* value, adopted */
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.args_total, 1u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_BIG], 1u);
    EXPECT_EQ(agg.big_arg_adopt_count, 1u);
    EXPECT_EQ(agg.big_arg_adopt_bytes, 50000u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 0u);
    /* Ordinal: value (index 2) */
    EXPECT_EQ(agg.ord_count[APA_ORD_VAL], 1u);
    EXPECT_EQ(agg.ord_adopted_bytes[APA_ORD_VAL], 50000u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_VAL], 0u);
}

TEST_F(ArgparseAccountingTest, ArgvAllocAndGrow) {
    APA_ARGV_ALLOC(3);
    APA_ARGV_GROW(6);
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.argv_initial_allocs, 1u);
    EXPECT_EQ(agg.argv_initial_slots, 3u);
    EXPECT_EQ(agg.argv_growth_reallocs, 1u);
    EXPECT_EQ(agg.argv_growth_slots, 6u);
}

TEST_F(ArgparseAccountingTest, CmdQueueAllocMacro) {
    APA_CMD_QUEUE_ALLOC(8);
    APA_CMD_QUEUE_ALLOC(16);
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.cmd_queue_allocs, 2u);
    EXPECT_EQ(agg.cmd_queue_slots, 24u);
}

TEST_F(ArgparseAccountingTest, CommandDoneMacro) {
    APA_COMMAND_DONE();
    APA_COMMAND_DONE();
    APA_COMMAND_DONE();
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 3u);
}

/* ── Aggregation across multiple slots ────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, AggregationAcrossSlots) {
    /* Simulate two threads having independently counted. */
    g_apa_slots[0].counters.commands_total = 100;
    g_apa_slots[0].counters.args_total = 300;
    g_apa_slots[0].counters.multibulk_copy_bytes = 5000;
    g_apa_slots[0].counters.args_by_bucket[APA_BUCKET_TINY] = 200;
    g_apa_slots[0].counters.args_by_bucket[APA_BUCKET_BIG] = 10;
    g_apa_slots[0].counters.ord_count[APA_ORD_CMD] = 100;
    g_apa_slots[0].counters.ord_copied_bytes[APA_ORD_CMD] = 300;

    g_apa_slots[3].counters.commands_total = 50;
    g_apa_slots[3].counters.args_total = 150;
    g_apa_slots[3].counters.multibulk_copy_bytes = 2500;
    g_apa_slots[3].counters.args_by_bucket[APA_BUCKET_TINY] = 100;
    g_apa_slots[3].counters.args_by_bucket[APA_BUCKET_BIG] = 5;
    g_apa_slots[3].counters.ord_count[APA_ORD_CMD] = 50;
    g_apa_slots[3].counters.ord_copied_bytes[APA_ORD_CMD] = 150;

    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 150u);
    EXPECT_EQ(agg.args_total, 450u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 7500u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_TINY], 300u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_BIG], 15u);
    EXPECT_EQ(agg.ord_count[APA_ORD_CMD], 150u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_CMD], 450u);
}

/* ── No double-count on partial/reparsed commands ────────────────────────── */

TEST_F(ArgparseAccountingTest, NoDoubleCountOnSingleParse) {
    /* Simulate parsing a 3-arg command: SET key val */
    APA_ARGV_ALLOC(3);
    APA_ARG_COPIED(3, 0);  /* SET */
    APA_ARG_COPIED(3, 1);  /* key */
    APA_ARG_COPIED(5, 2);  /* val */
    APA_COMMAND_DONE();

    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 1u);
    EXPECT_EQ(agg.args_total, 3u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 11u);
    EXPECT_EQ(agg.argv_initial_allocs, 1u);
}

/* ── Mixed workload accumulation with ordinals ────────────────────────────── */

TEST_F(ArgparseAccountingTest, MixedWorkloadAccumulation) {
    /* SET key val (3 tiny args, all copied) */
    APA_ARGV_ALLOC(3);
    APA_ARG_COPIED(3, 0);   /* SET */
    APA_ARG_COPIED(3, 1);   /* key */
    APA_ARG_COPIED(3, 2);   /* val */
    APA_COMMAND_DONE();

    /* SET key <40KB_value> (big-arg adopted at index 2) */
    APA_ARGV_ALLOC(3);
    APA_ARG_COPIED(3, 0);      /* SET */
    APA_ARG_COPIED(3, 1);      /* key */
    APA_ARG_ADOPTED(40960, 2); /* 40KB value */
    APA_COMMAND_DONE();

    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 2u);
    EXPECT_EQ(agg.args_total, 6u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_TINY], 5u);
    EXPECT_EQ(agg.args_by_bucket[APA_BUCKET_BIG], 1u);
    EXPECT_EQ(agg.multibulk_copy_bytes, 15u);
    EXPECT_EQ(agg.big_arg_adopt_count, 1u);
    EXPECT_EQ(agg.big_arg_adopt_bytes, 40960u);
    EXPECT_EQ(agg.argv_initial_allocs, 2u);
    EXPECT_EQ(agg.argv_initial_slots, 6u);

    /* Ordinal checks: cmd(0)=2, key(1)=2, val(2)=2, rest(3+)=0 */
    EXPECT_EQ(agg.ord_count[APA_ORD_CMD], 2u);
    EXPECT_EQ(agg.ord_count[APA_ORD_KEY], 2u);
    EXPECT_EQ(agg.ord_count[APA_ORD_VAL], 2u);
    EXPECT_EQ(agg.ord_count[APA_ORD_REST], 0u);

    /* Ordinal copied bytes: cmd=6, key=6, val=3 */
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_CMD], 6u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_KEY], 6u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_VAL], 3u);

    /* Ordinal adopted bytes: only val ordinal */
    EXPECT_EQ(agg.ord_adopted_bytes[APA_ORD_CMD], 0u);
    EXPECT_EQ(agg.ord_adopted_bytes[APA_ORD_KEY], 0u);
    EXPECT_EQ(agg.ord_adopted_bytes[APA_ORD_VAL], 40960u);
}

/* ── Multi-arg command with rest bucket ───────────────────────────────────── */

TEST_F(ArgparseAccountingTest, RestBucketAccumulation) {
    /* MSET key1 val1 key2 val2 (5 args: indices 0,1,2,3,4) */
    APA_ARGV_ALLOC(5);
    APA_ARG_COPIED(4, 0);   /* MSET */
    APA_ARG_COPIED(4, 1);   /* key1 */
    APA_ARG_COPIED(4, 2);   /* val1 */
    APA_ARG_COPIED(4, 3);   /* key2 -> rest bucket */
    APA_ARG_COPIED(4, 4);   /* val2 -> rest bucket */
    APA_COMMAND_DONE();

    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.args_total, 5u);
    EXPECT_EQ(agg.ord_count[APA_ORD_CMD], 1u);
    EXPECT_EQ(agg.ord_count[APA_ORD_KEY], 1u);
    EXPECT_EQ(agg.ord_count[APA_ORD_VAL], 1u);
    EXPECT_EQ(agg.ord_count[APA_ORD_REST], 2u);
    EXPECT_EQ(agg.ord_copied_bytes[APA_ORD_REST], 8u);
}

/* ── Slot isolation ───────────────────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, SlotIsolation) {
    /* Write directly into two separate slots and verify they don't interfere. */
    g_apa_slots[0].counters.commands_total = 42;
    g_apa_slots[5].counters.commands_total = 7;

    /* Slot 0 unchanged by slot 5's write. */
    EXPECT_EQ(g_apa_slots[0].counters.commands_total, 42u);
    EXPECT_EQ(g_apa_slots[5].counters.commands_total, 7u);

    /* Aggregate sees both. */
    argparseAccounting agg = argparseAccountingAggregate();
    EXPECT_EQ(agg.commands_total, 49u);
}

/* ── Cache-line alignment ─────────────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, CacheLineAlignment) {
    /* Verify that the slot array base and consecutive slots are aligned. */
    EXPECT_EQ(reinterpret_cast<uintptr_t>(&g_apa_slots[0]) % CACHE_LINE_SIZE, 0u);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(&g_apa_slots[1]) % CACHE_LINE_SIZE, 0u);
    /* Slots must be at least CACHE_LINE_SIZE apart. */
    uintptr_t delta = reinterpret_cast<uintptr_t>(&g_apa_slots[1]) -
                      reinterpret_cast<uintptr_t>(&g_apa_slots[0]);
    EXPECT_GE(delta, static_cast<uintptr_t>(CACHE_LINE_SIZE));
}

/* ── Struct field count ───────────────────────────────────────────────────── */

TEST_F(ArgparseAccountingTest, StructFieldCountMatchesExpected) {
    /* 2 + 4 + 2 + 2 + 4 + 2 + 4 + 4 + 4 = 28 uint64_t fields */
    int expected = 2 + APA_SIZE_BUCKETS + 2 + 2 + 4 + 2 +
                   APA_ORD_BUCKETS + APA_ORD_BUCKETS + APA_ORD_BUCKETS;
    EXPECT_EQ(static_cast<int>(sizeof(argparseAccounting) / sizeof(uint64_t)), expected);
}

#else /* !ARGPARSE_ACCOUNTING */

/* When the flag is off, verify stubs compile and do nothing. */
TEST(ArgparseAccountingDisabled, MacrosAreNoOps) {
    APA_COMMAND_DONE();
    APA_ARG_COPIED(100, 0);
    APA_ARG_INLINE(10, 1);
    APA_ARG_ADOPTED(50000, 2);
    APA_ARGV_ALLOC(3);
    APA_ARGV_GROW(6);
    APA_CMD_QUEUE_ALLOC(8);
    SUCCEED();
}

TEST(ArgparseAccountingDisabled, ResetAndInfoAreNoOps) {
    argparseAccountingReset();
    argparseAccountingGenInfo(NULL);
    SUCCEED();
}

#endif /* ARGPARSE_ACCOUNTING */
