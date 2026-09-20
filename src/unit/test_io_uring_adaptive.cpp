/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "io_uring_batch.h"
}

/* The adaptive share decision is a pure function of an ioUringAdaptiveState:
 * an EWMA of commands per read and a hysteresis bit. These tests drive it
 * with synthetic reads; no server, ring or thread is involved. */
class IoUringAdaptiveTest : public ::testing::Test {
  protected:
    ioUringAdaptiveState st;
    static constexpr int cap = 512;

    void SetUp() override {
        memset(&st, 0, sizeof(st));
    }

    /* Feed ``n`` reads of ``ncmds`` commands each. */
    void reads(int n, int ncmds) {
        for (int i = 0; i < n; i++) ioUringAdaptiveNoteRead(&st, ncmds);
    }

    /* Feed reads of ``ncmds`` until the EWMA crosses ``target`` in the
     * direction of ``ncmds``; fails the test if it never gets there. */
    void readsUntil(int ncmds, double target) {
        for (int i = 0; i < 100000; i++) {
            ioUringAdaptiveNoteRead(&st, ncmds);
            double e = ioUringAdaptiveCmdsPerRead(&st);
            if (ncmds > target ? e > target : e < target) return;
        }
        FAIL() << "EWMA never reached " << target << " feeding " << ncmds << " commands per read";
    }
};

TEST_F(IoUringAdaptiveTest, EwmaStartsAtFirstSampleAndConverges) {
    EXPECT_DOUBLE_EQ(ioUringAdaptiveCmdsPerRead(&st), 0.0);
    reads(1, 16);
    EXPECT_DOUBLE_EQ(ioUringAdaptiveCmdsPerRead(&st), 16.0);
    /* Alpha 1/64: after 64 reads of 1 the EWMA has moved ~63% of the way. */
    reads(64, 1);
    double e = ioUringAdaptiveCmdsPerRead(&st);
    EXPECT_GT(e, 5.0);
    EXPECT_LT(e, 7.0);
    /* And settles on the new level. */
    reads(2000, 1);
    EXPECT_LT(ioUringAdaptiveCmdsPerRead(&st), 1.01);
    EXPECT_GE(ioUringAdaptiveCmdsPerRead(&st), 1.0);
}

TEST_F(IoUringAdaptiveTest, NonPositiveReadsAreIgnored) {
    reads(1, 4);
    reads(10, 0);
    reads(10, -3);
    EXPECT_DOUBLE_EQ(ioUringAdaptiveCmdsPerRead(&st), 4.0);
}

TEST_F(IoUringAdaptiveTest, ConfiguredCapPassesThroughAtDepthOne) {
    reads(200, 1);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), cap);
    EXPECT_EQ(st.suppressed, 0);
}

TEST_F(IoUringAdaptiveTest, SuppressesAboveHighAndReleasesBelowLowWithHysteresis) {
    /* Below the high threshold from below: not suppressed. */
    readsUntil(IO_URING_ADAPTIVE_HIGH_CMDS - 1, IO_URING_ADAPTIVE_HIGH_CMDS - 1.1);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), cap);
    /* Cross the high threshold: suppressed, cap forced to 1. */
    readsUntil(16, IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), 1);
    EXPECT_EQ(st.suppressed, 1);
    /* Fall back into the band between low and high: still suppressed. */
    readsUntil(IO_URING_ADAPTIVE_LOW_CMDS + 1, IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_GT(ioUringAdaptiveCmdsPerRead(&st), (double)IO_URING_ADAPTIVE_LOW_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), 1);
    /* Below the low threshold: released. */
    readsUntil(1, IO_URING_ADAPTIVE_LOW_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), cap);
    EXPECT_EQ(st.suppressed, 0);
    /* Climb back into the band from below: still released. */
    readsUntil(IO_URING_ADAPTIVE_HIGH_CMDS - 1, IO_URING_ADAPTIVE_LOW_CMDS);
    EXPECT_LT(ioUringAdaptiveCmdsPerRead(&st), (double)IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), cap);
}

TEST_F(IoUringAdaptiveTest, DecisionIsTakenOnlyWhenCapIsConsulted) {
    /* The EWMA alone never flips the bit; the cap call does. */
    readsUntil(16, IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_EQ(st.suppressed, 0);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, cap), 1);
    EXPECT_EQ(st.suppressed, 1);
}

TEST_F(IoUringAdaptiveTest, CapOfOneShortCircuitsWithoutTouchingState) {
    readsUntil(16, IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, 1), 1);
    EXPECT_EQ(st.suppressed, 0);
}

TEST_F(IoUringAdaptiveTest, SuppressedReturnsOneForAnyConfiguredCap) {
    readsUntil(16, IO_URING_ADAPTIVE_HIGH_CMDS);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, 2), 1);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, 16), 1);
    EXPECT_EQ(ioUringAdaptiveShareCap(&st, IO_URING_JOB_SHARE_MAX), 1);
}
