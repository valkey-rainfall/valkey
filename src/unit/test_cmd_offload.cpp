/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Ported from PR #2976's src/unit/test_cmd_offload.c (author: Uri Yagelnik),
 * written against the pre-GoogleTest C unit-test harness. Same scenarios and
 * assertions, with TEST_ASSERT converted to the EXPECT and ASSERT macro
 * families. A handful of testOnly accessors were added to cmd_offload.c and
 * cmd_offload.h because slotQueue is intentionally opaque outside that file
 * (the old harness got free access by textually including cmd_offload.c into
 * the test binary).
 */
#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "cluster.h"
#include "cmd_offload.h"
#include "io_threads.h"
#include "module.h"
#include "server.h"
}

/* C++17 has no designated initializers, so build the flag-only mock commands with a helper. */
static struct serverCommand mockCmdWithFlags(uint64_t flags) {
    struct serverCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.flags = flags;
    return cmd;
}

/* Mock command flag combinations used throughout, mirroring the original
 * static serverCommand fixtures. */
static struct serverCommand mockReadCmd = mockCmdWithFlags(CMD_READONLY);
static struct serverCommand mockWriteCmd = mockCmdWithFlags(CMD_WRITE);
static struct serverCommand mockAdminCmd = mockCmdWithFlags(CMD_ADMIN);
static struct serverCommand mockNoMandatoryKeysCmd = mockCmdWithFlags(CMD_NO_MANDATORY_KEYS);
static struct serverCommand mockTouchesArbitraryKeysCmd = mockCmdWithFlags(CMD_TOUCHES_ARBITRARY_KEYS);
static struct serverCommand mockExecCmd;
static struct serverCommand mockBlockingCmd = mockCmdWithFlags(CMD_READONLY | CMD_BLOCKING);
static struct serverCommand mockMayReplicateCmd = mockCmdWithFlags(CMD_READONLY | CMD_MAY_REPLICATE);
static struct serverCommand mockSlotExclusiveCmd = mockCmdWithFlags(CMD_WRITE);

class CmdOffloadTest : public ::testing::Test {
  protected:
    valkeyServer saved_state;
    client *tc1 = NULL; /* Test client 1 */
    client *tc2 = NULL; /* Test client 2 */

    void SetUp() override {
        /* canCommandBeOffloaded consults the module registry; initialise it once
         * per process (the list is process-global and re-initialising would leak). */
        static bool modules_initialised = false;
        if (!modules_initialised) {
            moduleInitModulesSystem();
            modules_initialised = true;
        }

        testOnlyResetSlotQueues();

        /* Save the small slice of server state these tests mutate. */
        saved_state.cluster_enabled = server.cluster_enabled;
        saved_state.io_threads_num = server.io_threads_num;
        saved_state.active_io_threads_num = server.active_io_threads_num;
        saved_state.io_threads_do_commands_offloading_with_modules = server.io_threads_do_commands_offloading_with_modules;
        saved_state.notify_keyspace_events = server.notify_keyspace_events;
        saved_state.io_threads_saturated = server.io_threads_saturated;
        saved_state.offload_throttle_pct = server.offload_throttle_pct;
        saved_state.mstime = server.mstime;

        tc1 = (client *)zcalloc(sizeof(client));
        tc1->bstate = (blockingState *)zcalloc(sizeof(blockingState));
        tc2 = (client *)zcalloc(sizeof(client));
        tc2->bstate = (blockingState *)zcalloc(sizeof(blockingState));
    }

    void TearDown() override {
        server.cluster_enabled = saved_state.cluster_enabled;
        server.io_threads_num = saved_state.io_threads_num;
        server.active_io_threads_num = saved_state.active_io_threads_num;
        server.io_threads_do_commands_offloading_with_modules = saved_state.io_threads_do_commands_offloading_with_modules;
        server.notify_keyspace_events = saved_state.notify_keyspace_events;
        server.io_threads_saturated = saved_state.io_threads_saturated;
        server.offload_throttle_pct = saved_state.offload_throttle_pct;
        server.mstime = saved_state.mstime;

        if (tc1) {
            if (tc1->bstate) zfree(tc1->bstate);
            zfree(tc1);
            tc1 = NULL;
        }
        if (tc2) {
            if (tc2->bstate) zfree(tc2->bstate);
            zfree(tc2);
            tc2 = NULL;
        }

        testOnlyResetSlotQueues();
    }
};

TEST_F(CmdOffloadTest, TestGetSlotQueue) {
    /* slot -1 refcount reflects the global exclusive queue. */
    EXPECT_EQ(testOnlyGetSlotRefCount(-1), 0);

    EXPECT_EQ(getSlotThreadId(0), -1); /* Available initially */
    slotQueueIncRef(0);
    setSlotThreadId(0, 5);
    EXPECT_EQ(getSlotThreadId(0), 5);
    slotQueueDecRef(0);
    EXPECT_EQ(getSlotThreadId(0), -1); /* Available again */

    /* Test slot at boundary */
    slotQueueIncRef(CLUSTER_SLOTS - 1);
    setSlotThreadId(CLUSTER_SLOTS - 1, 3);
    EXPECT_EQ(getSlotThreadId(CLUSTER_SLOTS - 1), 3);
    slotQueueDecRef(CLUSTER_SLOTS - 1);

    /* Test global queue */
    exclusiveQueueIncRef();
    setSlotThreadId(-1, 7);
    EXPECT_EQ(getSlotThreadId(-1), 7);
    /* Manual decrement (not exclusiveQueueDecRef) to avoid processing jobs. */
    testOnlySetSlotRefCount(-1, testOnlyGetSlotRefCount(-1) - 1);
    EXPECT_EQ(getSlotThreadId(-1), -1);
}

TEST_F(CmdOffloadTest, TestRequiresServerExclusivity) {
    EXPECT_EQ(requiresServerExclusivity(&mockWriteCmd, -1), 1); /* Write with no slot */
    EXPECT_EQ(requiresServerExclusivity(&mockWriteCmd, 0), 0);  /* Write with slot */
    EXPECT_EQ(requiresServerExclusivity(&mockReadCmd, -1), 0);  /* Read with no slot */
    EXPECT_EQ(requiresServerExclusivity(&mockAdminCmd, 0), 1);  /* Admin always */
    EXPECT_EQ(requiresServerExclusivity(&mockAdminCmd, -1), 1);
    EXPECT_EQ(requiresServerExclusivity(&mockNoMandatoryKeysCmd, 0), 1);
    EXPECT_EQ(requiresServerExclusivity(&mockTouchesArbitraryKeysCmd, 0), 1);

    mockExecCmd.proc = execCommand;
    EXPECT_EQ(requiresServerExclusivity(&mockExecCmd, 0), 1); /* EXEC always */
}

TEST_F(CmdOffloadTest, TestCanCommandBeOffloaded) {
    server.cluster_enabled = 1;
    server.active_io_threads_num = 4;
    server.io_threads_do_commands_offloading_with_modules = 1;
    server.notify_keyspace_events = 0;

    EXPECT_EQ(canCommandBeOffloaded(-1, &mockReadCmd), 0); /* slot -1 cannot offload */

    server.cluster_enabled = 0;
    EXPECT_EQ(canCommandBeOffloaded(0, &mockReadCmd), 0); /* non-cluster mode */

    server.cluster_enabled = 1;
    server.active_io_threads_num = 2;
    EXPECT_EQ(canCommandBeOffloaded(0, &mockReadCmd), 0); /* not enough threads */

    server.active_io_threads_num = 4;
    server.notify_keyspace_events = NOTIFY_KEY_MISS;
    EXPECT_EQ(canCommandBeOffloaded(0, &mockReadCmd), 0); /* NOTIFY_KEY_MISS */

    server.notify_keyspace_events = 0;
    EXPECT_EQ(canCommandBeOffloaded(0, &mockBlockingCmd), 0);     /* blocking cmd */
    EXPECT_EQ(canCommandBeOffloaded(0, &mockMayReplicateCmd), 0); /* MAY_REPLICATE */
    EXPECT_EQ(canCommandBeOffloaded(0, &mockWriteCmd), 0);        /* write cmd */
    EXPECT_EQ(canCommandBeOffloaded(0, &mockNoMandatoryKeysCmd), 0);
    EXPECT_EQ(canCommandBeOffloaded(0, &mockReadCmd), 1); /* valid read cmd */
}

TEST_F(CmdOffloadTest, TestSlotQueueRefCounting) {
    EXPECT_EQ(getSlotThreadId(100), -1); /* Available */
    slotQueueIncRef(100);
    setSlotThreadId(100, 2);
    EXPECT_EQ(getSlotThreadId(100), 2);
    slotQueueIncRef(100);
    slotQueueDecRef(100);
    EXPECT_EQ(getSlotThreadId(100), 2); /* Still busy */
    slotQueueDecRef(100);
    EXPECT_EQ(getSlotThreadId(100), -1); /* Available again */
}

static int job_executed = 0;
static void testJobHandler(void *data) {
    int *val = (int *)data;
    job_executed = *val;
}

TEST_F(CmdOffloadTest, TestThreadDeferredJobs) {
    initThreadDeferredJobs();

    int test_val = 42;
    threadAddDeferredJob(5, testJobHandler, sizeof(int), &test_val);

    EXPECT_NE(testOnlyGetThreadDeferredJobCount(), -1);
    EXPECT_EQ(testOnlyGetThreadDeferredJobCount(), 1);

    freeThreadDeferredJobs();
    EXPECT_EQ(testOnlyGetThreadDeferredJobCount(), -1); /* NULL after free */
}

TEST_F(CmdOffloadTest, TestSlotQueueAddJobAndDispatch) {
    job_executed = 0;
    int test_val = 99;

    /* Global job (slot -1) executes immediately */
    list *jobs = listCreate();
    slotQueueAddJob(jobs, -1, testJobHandler, sizeof(int), &test_val);
    EXPECT_EQ(listLength(jobs), 1u);
    dispatchSlotQueueJobs(jobs);
    EXPECT_EQ(job_executed, 99);

    /* Slot job when slot available executes immediately */
    job_executed = 0;
    jobs = listCreate();
    slotQueueAddJob(jobs, 50, testJobHandler, sizeof(int), &test_val);
    dispatchSlotQueueJobs(jobs);
    EXPECT_EQ(job_executed, 99);

    /* Slot job when slot busy gets queued */
    job_executed = 0;
    slotQueueIncRef(50);
    jobs = listCreate();
    slotQueueAddJob(jobs, 50, testJobHandler, sizeof(int), &test_val);
    dispatchSlotQueueJobs(jobs);
    EXPECT_EQ(job_executed, 0);

    /* Release slot - job executes */
    slotQueueDecRef(50);
    EXPECT_EQ(job_executed, 99);
}

TEST_F(CmdOffloadTest, TestCanExecuteCommand) {
    tc1->cmd = &mockReadCmd;
    tc1->slot = 0;

    /* Non-cluster mode always allows execution */
    server.cluster_enabled = 0;
    server.io_threads_num = 4;
    EXPECT_EQ(canExecuteCommand(tc1), 1);

    /* Single IO thread always allows execution */
    server.cluster_enabled = 1;
    server.io_threads_num = 1;
    EXPECT_EQ(canExecuteCommand(tc1), 1);
}

TEST_F(CmdOffloadTest, TestYieldForBusySlot) {
    tc1->slot = 200;

    /* Slot available - no yield */
    EXPECT_EQ(yieldForBusySlot(tc1), 0);

    /* Slot busy - should yield */
    slotQueueIncRef(200);
    server.stat_offload_blocked = 0;
    server.stat_io_threaded_clients_blocked_on_slot = 0;
    server.stat_io_threaded_clients_blocked_total = 0;
    EXPECT_EQ(yieldForBusySlot(tc1), 1);
    EXPECT_EQ(tc1->flag.blocked, 1u);
    EXPECT_EQ(server.stat_io_threaded_clients_blocked_on_slot, 1);

    slotQueueRemoveClient(tc1);
    slotQueueDecRef(200);
}

TEST_F(CmdOffloadTest, TestSlotQueueRemoveClient) {
    /* Remove client not in any queue - early return */
    tc1->bstate->slot_pending_list = NULL;
    slotQueueRemoveClient(tc1);

    /* Add client to busy slot queue */
    tc1->slot = 300;
    slotQueueIncRef(300);
    server.stat_io_threaded_clients_blocked_on_slot = 0;
    yieldForBusySlot(tc1);
    EXPECT_EQ(tc1->flag.blocked, 1u);
    EXPECT_NE(tc1->bstate->slot_pending_list, (void *)NULL);

    /* Remove client from queue */
    slotQueueRemoveClient(tc1);
    EXPECT_EQ(tc1->flag.blocked, 0u);
    EXPECT_EQ(tc1->bstate->slot_pending_list, (void *)NULL);
    EXPECT_EQ(server.stat_io_threaded_clients_blocked_on_slot, 0);

    slotQueueDecRef(300);
}

TEST_F(CmdOffloadTest, TestIoThreadsOnUnlinkClient) {
    tc1->slot = 400;

    /* Client not in any queue */
    ioThreadsOnUnlinkClient(tc1);
    EXPECT_EQ(tc1->bstate->slot_pending_list, (void *)NULL);

    /* Add client to queue then unlink */
    slotQueueIncRef(400);
    yieldForBusySlot(tc1);
    EXPECT_NE(tc1->bstate->slot_pending_list, (void *)NULL);

    ioThreadsOnUnlinkClient(tc1);
    EXPECT_EQ(tc1->bstate->slot_pending_list, (void *)NULL);
    EXPECT_EQ(tc1->flag.blocked, 0u);

    slotQueueDecRef(400);

    /* Test with NULL bstate */
    zfree(tc1->bstate);
    tc1->bstate = NULL;
    ioThreadsOnUnlinkClient(tc1); /* Must not crash. */
}

TEST_F(CmdOffloadTest, TestIsServerCronDeferred) {
    /* Non-cluster mode - not deferred */
    server.cluster_enabled = 0;
    server.io_threads_num = 4;
    EXPECT_EQ(isServerCronDeferred(), 0);

    /* Single IO thread - not deferred */
    server.cluster_enabled = 1;
    server.io_threads_num = 1;
    EXPECT_EQ(isServerCronDeferred(), 0);

    /* Multiple threads, queue available - not deferred */
    server.io_threads_num = 4;
    EXPECT_EQ(isServerCronDeferred(), 0);

    /* Queue busy - should be deferred */
    exclusiveQueueIncRef();
    EXPECT_EQ(isServerCronDeferred(), 1);
    EXPECT_EQ(testOnlyGetDeferredJobCount(-1), 1);
    /* Manual decrement (not exclusiveQueueDecRef) to avoid processing jobs;
     * drop the deferred serverCron job it queued the same way the original
     * test removed the list node by hand. */
    testOnlySetSlotRefCount(-1, testOnlyGetSlotRefCount(-1) - 1);
    testOnlyClearDeferredJobsForSlot(-1);
}

TEST_F(CmdOffloadTest, TestUpdateOffloadingThrottle) {
    /* High congestion - should decrease throttle */
    server.offload_throttle_pct = 100;
    server.stat_offload_attempts = 100;
    server.stat_offload_blocked = 20; /* 20% > 15% threshold */
    server.mstime = 1000;
    updateOffloadingThrottle();
    server.mstime = 1200;
    updateOffloadingThrottle();
    EXPECT_LT(server.offload_throttle_pct, 100);

    /* Low congestion - should increase throttle */
    server.offload_throttle_pct = 50;
    server.stat_offload_attempts = 100;
    server.stat_offload_blocked = 2; /* 2% < 5% threshold */
    server.mstime = 1400;
    updateOffloadingThrottle();
    EXPECT_GT(server.offload_throttle_pct, 50);
}

TEST_F(CmdOffloadTest, TestPrefetchSlotQueueInfo) {
    /* Must not crash across the valid slot range. */
    prefetchSlotQueueInfo(0);
    prefetchSlotQueueInfo(CLUSTER_SLOTS - 1);
    prefetchSlotQueueInfo(8000);
}

TEST_F(CmdOffloadTest, TestUpdateOffloadingSaturation) {
    /* Reset state - use large mstime to avoid static last_update_time issues */
    server.io_threads_saturated = 0;
    server.offload_throttle_pct = 50;
    server.io_threads_num = 4;
    server.mstime = 1000000;

    /* Test: Skip when not enough threads */
    server.active_io_threads_num = 2;
    updateOffloadingSaturation();
    EXPECT_EQ(server.offload_throttle_pct, 50); /* Unchanged - skipped */

    /* Test: Throttle increases when not saturated (first valid call sets last_update_time) */
    server.active_io_threads_num = 4;
    for (int i = 1; i < 4; i++) {
        testOnlySetIOThreadCpuPct(i, 10, 20); /* Low IO load */
    }
    updateOffloadingSaturation();
    EXPECT_EQ(server.io_threads_saturated, 0);
    EXPECT_GT(server.offload_throttle_pct, 50); /* Increased */

    /* Test: Skip due to time interval (called too soon) */
    int prev_throttle = server.offload_throttle_pct;
    server.mstime += 50; /* Only 50ms later, need 100ms */
    updateOffloadingSaturation();
    EXPECT_EQ(server.offload_throttle_pct, prev_throttle); /* Unchanged - skipped */

    /* Test: Becomes saturated with high adjusted IO */
    server.offload_throttle_pct = 50;
    server.io_threads_saturated = 0;
    server.mstime += 200; /* Enough time passed */
    for (int i = 1; i < 4; i++) {
        testOnlySetIOThreadCpuPct(i, 10, 52); /* High IO - adjusted will be 52*100/90=57 > 55 */
    }
    updateOffloadingSaturation();
    EXPECT_EQ(server.io_threads_saturated, 1);
    EXPECT_LT(server.offload_throttle_pct, 50); /* Decreased */

    /* Test: Exits saturation when IO drops */
    server.mstime += 200;
    for (int i = 1; i < 4; i++) {
        testOnlySetIOThreadCpuPct(i, 10, 30); /* Below unsaturation limit */
    }
    updateOffloadingSaturation();
    EXPECT_EQ(server.io_threads_saturated, 0);
}

TEST_F(CmdOffloadTest, TestCanExecuteCommandAdvanced) {
    server.cluster_enabled = 1;
    server.io_threads_num = 4;
    server.stat_offload_blocked = 0;
    server.stat_io_threaded_clients_blocked_on_slot = 0;
    server.stat_io_threaded_clients_blocked_total = 0;

    /* Server-exclusive command when exclusivity available */
    tc1->cmd = &mockAdminCmd;
    tc1->slot = 0;
    EXPECT_EQ(canExecuteCommand(tc1), 1);

    /* Server-exclusive command when exclusivity NOT available */
    exclusiveQueueIncRef();
    tc1->cmd = &mockAdminCmd;
    tc1->slot = 0;
    server.stat_offload_blocked = 0;
    EXPECT_EQ(canExecuteCommand(tc1), 0);
    EXPECT_EQ(tc1->flag.blocked, 1u);
    slotQueueRemoveClient(tc1);
    exclusiveQueueDecRef();

    /* Non-exclusive command with no slot */
    tc1->cmd = &mockReadCmd;
    tc1->slot = -1;
    EXPECT_EQ(canExecuteCommand(tc1), 1);

    /* Command blocked by global exclusive queue */
    exclusiveQueueIncRef();
    tc2->cmd = &mockAdminCmd;
    tc2->slot = 0;
    canExecuteCommand(tc2); /* Adds tc2 to global queue */

    tc1->cmd = &mockReadCmd;
    tc1->slot = 100;
    EXPECT_EQ(canExecuteCommand(tc1), 0);
    slotQueueRemoveClient(tc1);
    slotQueueRemoveClient(tc2);
    exclusiveQueueDecRef();

    /* Slot-exclusive command when slot is busy */
    slotQueueIncRef(500);
    tc1->cmd = &mockSlotExclusiveCmd;
    tc1->slot = 500;
    EXPECT_EQ(canExecuteCommand(tc1), 0);
    slotQueueRemoveClient(tc1);
    slotQueueDecRef(500);

    /* Command in same slot context */
    testOnlySetSlotContextToSlot(123);
    tc1->cmd = &mockReadCmd;
    tc1->slot = 123;
    EXPECT_EQ(canExecuteCommand(tc1), 1);
    testOnlyClearSlotContext();

    /* Server-exclusive command in CTX_EXCLUSIVE context */
    testOnlySetSlotContextExclusive();
    tc1->cmd = &mockAdminCmd;
    tc1->slot = 0;
    EXPECT_EQ(canExecuteCommand(tc1), 1);
    testOnlyClearSlotContext();

    /* Command queued when slot queue is not empty (FIFO) */
    slotQueueIncRef(600);
    tc2->slot = 600;
    yieldForBusySlot(tc2);

    tc1->cmd = &mockReadCmd;
    tc1->slot = 600;
    EXPECT_EQ(canExecuteCommand(tc1), 0);
    slotQueueRemoveClient(tc1);
    slotQueueRemoveClient(tc2);
    slotQueueDecRef(600);
}

TEST_F(CmdOffloadTest, TestIsSlotExclusiveCmd) {
    EXPECT_EQ(testOnlyIsSlotExclusiveCmd(&mockReadCmd, 0), 0);
    EXPECT_EQ(testOnlyIsSlotExclusiveCmd(&mockAdminCmd, 0), 0);
    EXPECT_EQ(testOnlyIsSlotExclusiveCmd(&mockSlotExclusiveCmd, 0), 1);
    EXPECT_EQ(testOnlyIsSlotExclusiveCmd(&mockBlockingCmd, 0), 1);
}

static int job_counter = 0;
static int job_data_received = 0;
static void countingJobHandler(void *data) {
    (void)data;
    job_counter++;
}

static void dataJobHandler(void *data) {
    int *val = (int *)data;
    job_data_received = *val;
}

TEST_F(CmdOffloadTest, TestCreateJobNodeAndProcessOrAddJob) {
    job_counter = 0;
    job_data_received = 0;

    /* Job with data executes immediately when slot available */
    int test_data = 12345;
    testOnlyCreateAndProcessJob(600, dataJobHandler, sizeof(int), &test_data);
    EXPECT_EQ(job_data_received, 12345);

    /* Job executes immediately when slot available */
    testOnlyCreateAndProcessJob(600, countingJobHandler, 0, NULL);
    EXPECT_EQ(job_counter, 1);

    /* Job queued when slot busy */
    slotQueueIncRef(600);
    testOnlyCreateAndProcessJob(600, countingJobHandler, 0, NULL);
    EXPECT_EQ(job_counter, 1);
    EXPECT_EQ(testOnlyGetDeferredJobCount(600), 1);

    /* Release slot - job executes */
    slotQueueDecRef(600);
    EXPECT_EQ(job_counter, 2);
}

TEST_F(CmdOffloadTest, TestProcessDeferredJobsList) {
    job_counter = 0;

    /* NULL deferred_jobs - early return */
    EXPECT_TRUE(testOnlyDeferredJobsListIsNull(650));
    testOnlyProcessDeferredJobsForSlot(650);
    EXPECT_EQ(job_counter, 0);

    /* Global queue - list not released after processing */
    testOnlyQueueJobDirectly(-1, countingJobHandler, 0, NULL);
    testOnlyProcessDeferredJobsForSlot(-1);
    EXPECT_EQ(job_counter, 1);
    EXPECT_FALSE(testOnlyDeferredJobsListIsNull(-1));

    /* Slot queue - list released after processing */
    testOnlyQueueJobDirectly(700, countingJobHandler, 0, NULL);
    testOnlyProcessDeferredJobsForSlot(700);
    EXPECT_EQ(job_counter, 2);
    EXPECT_TRUE(testOnlyDeferredJobsListIsNull(700));
}

TEST_F(CmdOffloadTest, TestSlotQueueRemoveClientFromGlobalQueue) {
    server.cluster_enabled = 1;
    server.io_threads_num = 4;

    testOnlySetSlotRefCount(-1, 1);
    tc1->cmd = &mockAdminCmd;
    tc1->slot = 0;
    server.stat_offload_blocked = 0;
    server.stat_io_threaded_clients_blocked_on_slot = 0;
    server.stat_io_threaded_clients_blocked_total = 0;
    EXPECT_EQ(canExecuteCommand(tc1), 0);
    EXPECT_EQ(tc1->flag.blocked, 1u);

    slotQueueRemoveClient(tc1);
    EXPECT_EQ(tc1->flag.blocked, 0u);
    EXPECT_FALSE(testOnlyPendingClientsIsNull(-1));

    /* Direct reset (not exclusiveQueueDecRef) to avoid draining the queue. */
    testOnlySetSlotRefCount(-1, 0);
}

TEST_F(CmdOffloadTest, TestUpdateOffloadingThrottleBoundaries) {
    /* Throttle doesn't go below minimum */
    server.offload_throttle_pct = 6;
    server.stat_offload_attempts = 100;
    server.stat_offload_blocked = 50;
    server.mstime = 2000;
    updateOffloadingThrottle();
    server.mstime = 2200;
    updateOffloadingThrottle();
    EXPECT_GE(server.offload_throttle_pct, 5);

    /* Throttle doesn't go above 100 */
    server.offload_throttle_pct = 98;
    server.stat_offload_attempts = 100;
    server.stat_offload_blocked = 1;
    server.mstime = 2400;
    updateOffloadingThrottle();
    server.mstime = 2600;
    updateOffloadingThrottle();
    EXPECT_LE(server.offload_throttle_pct, 100);

    /* No update when attempts is 0 */
    server.offload_throttle_pct = 50;
    server.stat_offload_attempts = 0;
    server.stat_offload_blocked = 0;
    server.mstime = 2800;
    updateOffloadingThrottle();
    EXPECT_EQ(server.offload_throttle_pct, 50);
}
