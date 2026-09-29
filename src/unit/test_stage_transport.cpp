/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Stage-crossing contract tests over the in-memory transport double (transport_mem.c).
 *
 * Each of the four thread-crossing bug incidents from the interface sketch is reproduced with no
 * threads: the test wears a thread's identity by setting stageSelf directly, then drives a mem-
 * transport crossing operation from the wrong thread. Every crossing carries the same owner/holder
 * guard the real transport asserts, gated by server.enable_debug_assert:
 *
 *   guard OFF (enable_debug_assert == 0)  -> the assert compiles to a no-op, the wrong-thread write
 *                                            proceeds, and the test observes the resulting silent
 *                                            corruption. This is the RED reading: without the guard
 *                                            the incident happens.
 *   guard ON  (enable_debug_assert == 1)  -> the assert fires and the process aborts before the
 *                                            corrupting write. This is the GREEN reading, checked
 *                                            with EXPECT_DEATH.
 *
 * The whole binary is run twice, selected by the SLP_GUARD environment variable (0 -> RED, 1 ->
 * GREEN), so slp-logs/gtest-red.log is the guard-stubbed run and slp-logs/gtest-green.log the
 * guard-in run. Both runs pass: RED asserts the corruption is real, GREEN asserts the guard stops
 * it. The four incidents map to the sketch's §3 table:
 *
 *   (a) an IO-thread read-modify-write on the shared flags word erases main's bit.
 *   (b) a pubsub/tracking push from main appends into a buffer an IO owner is writing.
 *   (c) main registers an fd into a worker's loop while the worker has inflight work.
 *   (d) an io-threads shrink resets an owned, non-drained client to IDLE.
 */

#include "generated_wrappers.hpp"

#include <cstdlib>
#include <cstring>

extern "C" {
#include "server.h"
#include "stage.h"
#include "stage_thread.h"
#include "transport_mem.h"
}

/* Whether this run exercises the guard-in (GREEN) or guard-stubbed (RED) path. Read once from the
 * environment so the same test bodies serve both runs. */
static int guardOn(void) {
    const char *v = getenv("SLP_GUARD");
    return v != NULL && v[0] == '1';
}

class StageTransportTest : public ::testing::Test {
  protected:
    void SetUp() override {
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        /* The one switch under test: it is the RED/GREEN selector for every crossing guard. */
        server.enable_debug_assert = guardOn();
    }

    /* Impersonate main (domain MAIN, tid 0). */
    static void beMain(void) { stageSetSelf(STAGE_DOMAIN_MAIN, 0); }
    /* Impersonate IO thread `tid`. */
    static void beIO(uint16_t tid) { stageSetSelf(STAGE_DOMAIN_IO, tid); }
};

using StageTransportDeathTest = StageTransportTest;

/* ------------------------------------------------------------------------------------------------
 * Incident (a): flags-word write from a non-owner thread.
 *
 * The connection is owned by main, which has set its own bit (MAIN_PENDING_WRITE). An IO thread
 * then sets its own bit (IO_DEFERRED) in the SAME word via memFlagsOwnerSet. With the guard in,
 * only the owner may touch the word, so the IO-thread set aborts. With the guard stubbed the set
 * proceeds; because the real hazard is a read-modify-write of a word the two threads share, the
 * test models the RMW losing main's store and checks main's bit is gone -- the corruption.
 * ---------------------------------------------------------------------------------------------- */

TEST_F(StageTransportTest, IncidentA_FlagsWordCrossThreadRmwStubbed) {
    if (guardOn()) GTEST_SKIP() << "guard-in run covers this in the death test";
    memControl *cc = memControlNew(CC_OWNER_MAIN, 0);

    /* Main owns the client and publishes its pending-write bit. */
    beMain();
    memFlagsOwnerSet(cc, MEM_FLAG_MAIN_PENDING_WRITE);
    EXPECT_TRUE(cc->flags & MEM_FLAG_MAIN_PENDING_WRITE);

    /* An IO thread performs a racing read-modify-write on the same word. Model the lost update the
     * real race produces: the IO thread read the word before main's store landed, ORs in its own
     * bit, and writes back -- erasing main's bit. memFlagsOwnerSet with the guard stubbed lets the
     * wrong-thread write through; we then reproduce the RMW's lost store. */
    beIO(2);
    uint64_t stale = cc->flags & ~MEM_FLAG_MAIN_PENDING_WRITE; /* IO's stale pre-image */
    memFlagsOwnerSet(cc, MEM_FLAG_IO_DEFERRED);                /* wrong-thread write admitted */
    cc->flags = stale | MEM_FLAG_IO_DEFERRED;                  /* the RMW writes its stale image back */

    /* Corruption: main's pending-write bit was erased by the cross-thread RMW. */
    EXPECT_FALSE(cc->flags & MEM_FLAG_MAIN_PENDING_WRITE);
    EXPECT_TRUE(cc->flags & MEM_FLAG_IO_DEFERRED);
    memControlFree(cc);
}

TEST_F(StageTransportDeathTest, IncidentA_FlagsWordCrossThreadWriteGuarded) {
    if (!guardOn()) GTEST_SKIP() << "guard-stubbed run covers this in the corruption test";
    beMain();
    EXPECT_DEATH(
        {
            memControl *cc = memControlNew(CC_OWNER_MAIN, 0);
            memFlagsOwnerSet(cc, MEM_FLAG_MAIN_PENDING_WRITE);
            beIO(2);                                       /* now impersonating a non-owner */
            memFlagsOwnerSet(cc, MEM_FLAG_IO_DEFERRED);    /* owner-only guard must fire here */
        },
        "");
}

/* ------------------------------------------------------------------------------------------------
 * Incident (b): reply append into a worker-owned buffer from main.
 *
 * A client is owned by IO thread 2, which is mid-write into its reply buffer. Main, running a
 * pubsub/tracking push, appends into the same buffer via memReplyAppendFromMain. Guard in: the
 * owner-only assert aborts main's append. Guard stubbed: main's bytes interleave with the owner's,
 * and the test observes the corrupted buffer.
 * ---------------------------------------------------------------------------------------------- */

TEST_F(StageTransportTest, IncidentB_MainReplyAppendIntoOwnedBufferStubbed) {
    if (guardOn()) GTEST_SKIP() << "guard-in run covers this in the death test";
    memControl *cc = memControlNew(CC_OWNER_IO, 2);

    /* The IO owner writes the first half of a reply. */
    beIO(2);
    memStageWriteSend(cc, "+OK", 3);
    EXPECT_EQ(cc->reply_len, (size_t)3);

    /* Main pushes a tracking invalidation into the SAME buffer. Wrong owner; guard stubbed admits
     * it, so the push corrupts the in-flight reply by appending after the owner's partial write. */
    beMain();
    memReplyAppendFromMain(cc, ">3\r\n", 4);

    /* Corruption: the buffer now holds the owner's partial reply spliced with main's push. */
    EXPECT_EQ(cc->reply_len, (size_t)7);
    EXPECT_EQ(memcmp(cc->reply, "+OK>3\r\n", 7), 0);
    memControlFree(cc);
}

TEST_F(StageTransportDeathTest, IncidentB_MainReplyAppendIntoOwnedBufferGuarded) {
    if (!guardOn()) GTEST_SKIP() << "guard-stubbed run covers this in the corruption test";
    EXPECT_DEATH(
        {
            memControl *cc = memControlNew(CC_OWNER_IO, 2);
            beIO(2);
            memStageWriteSend(cc, "+OK", 3);
            beMain();                            /* main is not the owner */
            memReplyAppendFromMain(cc, ">3\r\n", 4); /* owner-only guard must fire here */
        },
        "");
}

/* ------------------------------------------------------------------------------------------------
 * Incident (c): fd registration while the owner has inflight work.
 *
 * A worker owns a client and has published a batch to main (inflight != 0). Registering the
 * connection's fd into an event loop mid-sweep races the worker reading its events[]. memRegisterFd
 * requires the owner AND inflight == 0. Guard in: the inflight assert aborts. Guard stubbed: the
 * registration proceeds while a batch is outstanding, the corruption being an fd registered under
 * an owner that is still draining.
 * ---------------------------------------------------------------------------------------------- */

TEST_F(StageTransportTest, IncidentC_FdRegisterWithInflightStubbed) {
    if (guardOn()) GTEST_SKIP() << "guard-in run covers this in the death test";
    memTransport *tp = memTransportNew(2);
    memControl *cc = memControlNew(CC_OWNER_IO, 2);

    /* The owner publishes a batch: inflight is now non-zero. */
    beIO(2);
    memHandoffPublish(tp, cc, 3);
    EXPECT_EQ(cc->inflight, (uint32_t)3);

    /* Register the fd while the batch is still outstanding. Guard stubbed admits it. */
    memRegisterFd(cc);

    /* Corruption: the fd is registered while the owner still has inflight commands draining. */
    EXPECT_EQ(cc->fd_registered, (uint8_t)1);
    EXPECT_NE(cc->inflight, (uint32_t)0);
    memControlFree(cc);
    memTransportFree(tp);
}

TEST_F(StageTransportDeathTest, IncidentC_FdRegisterWithInflightGuarded) {
    if (!guardOn()) GTEST_SKIP() << "guard-stubbed run covers this in the corruption test";
    EXPECT_DEATH(
        {
            memTransport *tp = memTransportNew(2);
            memControl *cc = memControlNew(CC_OWNER_IO, 2);
            beIO(2);
            memHandoffPublish(tp, cc, 3); /* inflight = 3 */
            memRegisterFd(cc);            /* inflight-zero guard must fire here */
        },
        "");
}

/* ------------------------------------------------------------------------------------------------
 * Incident (d): shrink IDLE reset of a client the thread does not own / has not drained.
 *
 * An io-threads shrink retires a worker. The retire goes through the guarded ownership transfer,
 * which asserts the client is drained (inflight == 0) and lifecycle is monotonic. Resetting an
 * ACTIVE client that still has inflight work must abort. Guard in: the transfer's inflight assert
 * fires. Guard stubbed: the reset proceeds, stomping an owned, non-drained client -- the retired
 * client keeps its inflight commands while its ownership flips to main.
 * ---------------------------------------------------------------------------------------------- */

TEST_F(StageTransportTest, IncidentD_ShrinkResetNonDrainedStubbed) {
    if (guardOn()) GTEST_SKIP() << "guard-in run covers this in the death test";
    memTransport *tp = memTransportNew(2);
    memControl *cc = memControlNew(CC_OWNER_IO, 2);

    /* The worker owns an ACTIVE client with inflight work. */
    beIO(2);
    memHandoffPublish(tp, cc, 2);
    EXPECT_EQ(cc->inflight, (uint32_t)2);
    EXPECT_EQ(cc->lifecycle, (uint8_t)MEM_ACTIVE);

    /* The shrink retires it via the transfer path. Guard stubbed admits the reset of a non-drained
     * client. */
    memWorkerShrinkReset(cc);

    /* Corruption: ownership flipped to main and lifecycle jumped to DETACHED while commands are
     * still inflight -- the completed-client reset the incident describes. */
    EXPECT_EQ(cc->owner_domain, (uint8_t)CC_OWNER_MAIN);
    EXPECT_EQ(cc->lifecycle, (uint8_t)MEM_DETACHED);
    EXPECT_NE(cc->inflight, (uint32_t)0);
    memControlFree(cc);
    memTransportFree(tp);
}

TEST_F(StageTransportDeathTest, IncidentD_ShrinkResetNonDrainedGuarded) {
    if (!guardOn()) GTEST_SKIP() << "guard-stubbed run covers this in the corruption test";
    EXPECT_DEATH(
        {
            memTransport *tp = memTransportNew(2);
            memControl *cc = memControlNew(CC_OWNER_IO, 2);
            beIO(2);
            memHandoffPublish(tp, cc, 2); /* ACTIVE, inflight = 2 */
            memWorkerShrinkReset(cc);     /* drained-before-transfer guard must fire here */
        },
        "");
}
