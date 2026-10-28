/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Per-slot network-bytes-in for a command that was parsed but never dispatched.
 * On a cluster replica with parallel apply, processPlannedCommand defers a plain SET
 * to an apply lane (replApplyDefer) and calls commandProcessed without running
 * processCommand, so c->cmd is NULL while the lane parse already set c->slot. The
 * slot-stats hook then read c->cmd->proc and the replica crashed (issue #23, audit
 * items CLU-14 / REPL-11). Stock Valkey counts the primary stream's bytes per slot
 * on a replica, so the hook must count them without a command, not skip them. */

#include "generated_wrappers.hpp"

#include <cstdlib>

extern "C" {
#include "cluster.h"
#include "cluster_legacy.h"
#include "server.h"
void clusterSlotStatsAddNetworkBytesInForUserClient(client *c);
}

class ClusterSlotStatsDeferredTest : public ::testing::Test {};

static void addBytesForDeferredCommand(void) {
    server.cluster_enabled = 1;
    server.cluster_slot_stats_enabled = 1;
    server.in_exec = 0;
    server.cluster = (clusterState *)zcalloc(sizeof(clusterState));
    client *c = (client *)zcalloc(sizeof(client));
    c->slot = 42;    /* set by the lane parse */
    c->cmd = NULL;   /* processCommand never ran */
    c->net_input_bytes_curr_cmd = 37;
    clusterSlotStatsAddNetworkBytesInForUserClient(c);
    int ok = server.cluster->slot_stats[42].network_bytes_in == 37;
    zfree(c);
    zfree(server.cluster);
    server.cluster = NULL;
    server.cluster_enabled = 0;
    server.cluster_slot_stats_enabled = 0;
    exit(ok ? 0 : 3);
}

/* Run in a child: on the unfixed code the hook dereferences NULL and the child dies
 * by SIGSEGV, which fails the test instead of killing the runner. Exit code 3 means
 * no crash but the bytes were not counted (the accounting stock Valkey does). */
TEST_F(ClusterSlotStatsDeferredTest, BytesInCountedForDeferredCommandWithoutCmd) {
    EXPECT_EXIT(addBytesForDeferredCommand(), ::testing::ExitedWithCode(0), "");
}
