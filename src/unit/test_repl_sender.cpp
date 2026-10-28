/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* repl-stream-sender: disabling the sender must hand every owned replica back to
 * the main path. Before the fix, replStreamSenderStop freed the member list but left
 * stream_via_sender set on the replicas, so putClientInPendingWriteQueue skipped them
 * forever and the replica stopped receiving the stream (audit item REPL-01; the same
 * state makes the final flush in finishShutdown skip them, REPL-02). */

#include "generated_wrappers.hpp"

#include "fake_connection.hpp"

#include <cstring>

extern "C" {
#include "server.h"
void replStreamSenderInit(void);
void replStreamSenderStop(void);
void replStreamSenderAdd(client *replica);
void replStreamSenderTick(void);
int replStreamSenderActive(void);
unsigned long replStreamSenderOwnedReplicas(void);
}

class ReplSenderTest : public ::testing::Test {
  protected:
    void SetUp() override {
        server.logfile = const_cast<char *>("/dev/null");
        server.verbosity = LL_WARNING;
        server.commandlog[COMMANDLOG_TYPE_LARGE_REPLY].threshold = -1;
        if (!server.replicas) server.replicas = listCreate();
        if (!server.clients_pending_write) server.clients_pending_write = listCreate();
    }
};

static client *makeOnlineReplica(fakeConnection *conn) {
    client *c = (client *)zcalloc(sizeof(client));
    initClientReplicationData(c);
    c->reply = listCreate();
    c->conn = (connection *)conn;
    c->flag.replica = 1;
    c->repl_data->repl_state = REPLICA_STATE_ONLINE;
    return c;
}

static void freeReplica(client *c) {
    listRelease(c->reply);
    zfree(c->repl_data);
    zfree(c);
}

TEST_F(ReplSenderTest, DisableHandsOwnedReplicaBackToMain) {
    fakeConnection *conn = connCreateFake(1024);
    client *c = makeOnlineReplica(conn);

    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    ASSERT_TRUE(replStreamSenderActive());
    replStreamSenderAdd(c);
    ASSERT_EQ(c->repl_data->stream_via_sender, 1);
    ASSERT_EQ(replStreamSenderOwnedReplicas(), 1u);

    /* CONFIG SET repl-stream-sender no: the next tick stops the thread. */
    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderTick();
    ASSERT_FALSE(replStreamSenderActive());

    /* The replica must belong to the main path again. */
    EXPECT_EQ(c->repl_data->stream_via_sender, 0);
    EXPECT_TRUE(c->repl_data->send_node == NULL);

    /* And main must be able to queue a write for it. */
    putClientInPendingWriteQueue(c);
    EXPECT_TRUE(c->flag.pending_write);
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }

    connFreeFake(conn);
    freeReplica(c);
}

TEST_F(ReplSenderTest, ReenableAfterDisableOwnsReplicaAgain) {
    fakeConnection *conn = connCreateFake(1024);
    client *c = makeOnlineReplica(conn);

    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    replStreamSenderAdd(c);
    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderTick();

    /* Re-enable: a replica that is ONLINE and eligible can be handed over again
     * (replicaPutOnline is not re-run, so Add must not be refused by stale state). */
    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    replStreamSenderAdd(c);
    EXPECT_EQ(replStreamSenderOwnedReplicas(), 1u);

    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderStop();
    connFreeFake(conn);
    freeReplica(c);
}
