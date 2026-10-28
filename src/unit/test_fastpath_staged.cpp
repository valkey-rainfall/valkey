/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Staged fast path driven inline on one thread: thread 1's read phase, main's write phase and
 * thread 1's reply phase run back to back, then main finishes the iteration. Covers admission,
 * per-client reply order, spilled replies, leaving for the main path, and closes applied after
 * the reply barrier. */

#include "generated_wrappers.hpp"

#include <cstring>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "poller.h"
#include "server.h"
#include "staged_loop.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
void fastGetRecomputeSpecAclOk(client *c); /* from fastget.h, declared here to avoid <stdatomic.h> in C++ */
}

static const char *st_logfile = "/dev/null";

class FastpathStagedTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(st_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.client_max_querybuf_len = 1024ll * 1024 * 1024;
        server.proto_max_bulk_len = 512ll * 1024 * 1024;
        server.maxclients = 1000;
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();

        server.dbnum = 1;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *)));
        server.db[0] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[0]->id = 0;
        server.db[0]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);
        server.db[0]->expires = kvstoreCreate(&kvstoreExpiresHashtableType, 0, 0);
        server.db[0]->watched_keys = dictCreate(&keylistDictType);

        server.clients = listCreate();
        server.clients_to_close = listCreate();
        server.clients_pending_write = listCreate();
        server.clients_pending_read = listCreate();
        server.unblocked_clients = listCreate();
        server.ready_keys = listCreate();
        server.tracking_pending_keys = listCreate();
        server.monitors = listCreate();
        server.replicas = listCreate();
        server.pending_push_messages = listCreate();
        server.postponed_clients = listCreate();
        server.clients_waiting_acks = listCreate();
        server.clients_index = raxNew();
        server.clients_timeout_table = raxNew();
        server.errors = raxNew();
        for (int i = 0; i < COMMANDLOG_TYPE_NUM; i++) server.commandlog[i].threshold = -1;

        server.io_threads_num = 2;
        server.io_threads_fast_path = 1;
        server.io_threads_strict_offload = 1;
        server.io_batch_commands = 16;
        connTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyFreeIOThreadQueues();
    }

    void SetUp() override {
        testOnlyInitIOThreadQueues();
        ASSERT_EQ(testOnlyStagedStart(), 0);
    }

    void TearDown() override {
        testOnlyStagedStop();
        freeClientsInAsyncFreeQueue();
    }

    /* A connected client admitted to the staged fast path, with the peer end of its socket in *peer. */
    static client *newStagedClient(int *peer) {
        int sv[2];
        EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
        conn->state = CONN_STATE_CONNECTED;
        client *c = createClient(conn);
        struct sockaddr_in sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin_family = AF_INET;
        sa.sin_port = htons(40000);
        sa.sin_addr.s_addr = htonl(0x7f000001);
        EXPECT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
        c->fp_local = c->fp_peer;
        EXPECT_EQ(fastpathAttach(c), C_OK);
        EXPECT_EQ(c->flag.fastpath, 1u);
        *peer = sv[1];
        return c;
    }

    static void send(int peer, const std::string &req) {
        EXPECT_EQ(write(peer, req.data(), req.size()), (ssize_t)req.size());
    }

    /* Whatever the socket holds right now; never blocks. */
    static std::string recv(int peer) {
        std::string out;
        char buf[65536];
        ssize_t n;
        while ((n = ::recv(peer, buf, sizeof(buf), MSG_DONTWAIT)) > 0) out.append(buf, n);
        return out;
    }

    static void readPhase() {
        pollerGetNextBatch(stagedPoller(), 0);
        fastpathStagedReadPhase(1);
    }

    static void writeAndReplyPhases() {
        fastpathStagedExecute();
        if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
        fastpathStagedFinish();
    }

    static void iteration() {
        readPhase();
        writeAndReplyPhases();
    }

    static std::string mainBuf(client *c) {
        return std::string(c->buf, c->bufpos);
    }

    static void freeStaged(client *c, int peer) {
        close(peer);
        freeClient(c);
        iteration();
        freeClientsInAsyncFreeQueue();
    }
};

/* Admission gives the connection one control; batched commands run on main and their replies are
 * written by the IO thread in the reply phase, in order. */
TEST_F(FastpathStagedTest, PipelinedCommandsRepliedInOrderAfterReplyPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    ASSERT_NE(c->control, nullptr);
    ClientControl *cc = c->control;
    EXPECT_EQ(cc->client_id, c->id);
    EXPECT_EQ(cc->lifecycle, FP_ACTIVE);
    EXPECT_EQ(fastpathControlEnsure(c), C_OK);
    EXPECT_EQ(c->control, cc);

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nst:a\r\n*2\r\n$4\r\nINCR\r\n$4\r\nst:a\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 2u);
    EXPECT_TRUE(fastpathStagedRepliesPending());
    EXPECT_EQ(recv(peer), ""); /* nothing is written before the reply phase */
    writeAndReplyPhases();
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_FALSE(fastpathStagedRepliesPending());
    EXPECT_EQ(recv(peer), ":1\r\n:2\r\n");
    EXPECT_EQ(c->flag.fastpath, 1u);

    freeStaged(c, peer);
}

/* Two clients in one batch each get exactly their own replies. */
TEST_F(FastpathStagedTest, ClientsSharingABatchGetTheirOwnReplies) {
    int pa, pb;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    send(pa, "*2\r\n$4\r\nINCR\r\n$4\r\nst:b\r\n");
    send(pb, "*2\r\n$4\r\nINCR\r\n$4\r\nst:b\r\n*2\r\n$4\r\nINCR\r\n$4\r\nst:c\r\n");
    iteration();
    std::string ra = recv(pa), rb = recv(pb);
    /* Execution order across clients follows fd readiness; each client's own order is kept. */
    EXPECT_TRUE((ra == ":1\r\n" && rb == ":2\r\n:1\r\n") || (ra == ":2\r\n" && rb == ":1\r\n:1\r\n"));
    freeStaged(a, pa);
    freeStaged(b, pb);
}

/* A reply larger than the batch arena spills to its own block and is delivered whole. */
TEST_F(FastpathStagedTest, SpilledReplyDeliveredWhole) {
    int peer;
    client *c = newStagedClient(&peer);
    std::string big(32 * 1024, 'x');
    send(peer, "*3\r\n$3\r\nSET\r\n$5\r\nbigky\r\n$" + std::to_string(big.size()) + "\r\n" + big + "\r\n");
    std::string out;
    for (int i = 0; i < 16 && out.empty(); i++) {
        iteration();
        out = recv(peer);
    }
    EXPECT_EQ(out, "+OK\r\n");

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nbigky\r\n");
    std::string expect = "$" + std::to_string(big.size()) + "\r\n" + big + "\r\n";
    out.clear();
    for (int i = 0; i < 16 && out.size() < expect.size(); i++) {
        iteration();
        out += recv(peer);
    }
    EXPECT_EQ(out, expect);
    freeStaged(c, peer);
}

/* A command the fast path does not run makes the client leave: what it batched before is replied by
 * the IO thread, then main takes the client back after the reply phase and runs the rest itself. */
TEST_F(FastpathStagedTest, UnsupportedCommandHandsClientBackAfterReplyPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nst:d\r\n*2\r\n$4\r\nPING\r\n$1\r\nx\r\n*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$1\r\nn\r\n");
    readPhase();
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);
    EXPECT_EQ(c->flag.fastpath, 1u); /* main takes it back only after the reply phase */
    writeAndReplyPhases();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(c->control->lifecycle, FP_DETACHED);
    /* The batched replies come first; CLIENT SETNAME ran on main, which may already have written it. */
    std::string out = recv(peer) + mainBuf(c);
    EXPECT_EQ(out, ":1\r\n$1\r\nx\r\n+OK\r\n");
    ASSERT_NE(c->name, nullptr);
    EXPECT_STREQ((const char *)objectGetVal(c->name), "n");
    close(peer);
    freeClient(c);
}

/* freeClient on a staged client only records the close: its batched entries are skipped, and the
 * client stays allocated until main unmaps it after the reply barrier. */
TEST_F(FastpathStagedTest, CloseSkipsEntriesAndFreesAfterReplyBarrier) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nst:e\r\n$1\r\n1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u);

    EXPECT_EQ(freeClient(c), 0); /* main, write phase: recorded, not freed */
    EXPECT_EQ(c->flag.fastpath, 1u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    fastpathStagedExecute();
    robj *key = createStringObject("st:e", 4);
    EXPECT_EQ(lookupKeyRead(server.db[0], key), nullptr); /* the closing client's entry did not run */
    decrRefCount(key);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
    EXPECT_EQ(recv(peer), "");
    fastpathStagedFinish();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
}

/* freeClientAsync on a staged client records the same close as freeClient. */
TEST_F(FastpathStagedTest, AsyncFreeRecordsCloseAndFreesAfterReplyBarrier) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nst:f\r\n$1\r\n1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u);

    freeClientAsync(c);
    EXPECT_EQ(c->flag.fp_detach_sent, 1u);
    EXPECT_EQ(c->control->closing, 1u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    fastpathStagedExecute();
    robj *key = createStringObject("st:f", 4);
    EXPECT_EQ(lookupKeyRead(server.db[0], key), nullptr);
    decrRefCount(key);
    if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
}

/* A client idle on the fast path is released by freeClientAsync at the next iteration. */
TEST_F(FastpathStagedTest, AsyncFreeOfIdleClientFreesNextIteration) {
    int peer;
    client *c = newStagedClient(&peer);
    freeClientAsync(c);
    freeClientAsync(c); /* repeated requests record one close */
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    iteration();
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    EXPECT_EQ(fastpathClientCount(), 0u);
    close(peer);
}

/* Unwritten replies past the normal output buffer limit hand the client back, and main closes it. */
TEST_F(FastpathStagedTest, OutputBufferLimitClosesClient) {
    clientBufferLimitsConfig saved = server.client_obuf_limits[CLIENT_TYPE_NORMAL];
    server.client_obuf_limits[CLIENT_TYPE_NORMAL].hard_limit_bytes = 64 * 1024;
    long long closed = server.stat_client_outbuf_limit_disconnections;
    int peer;
    client *c = newStagedClient(&peer);
    int sz = 4096;
    setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    setsockopt(c->conn->fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    fcntl(c->conn->fd, F_SETFL, fcntl(c->conn->fd, F_GETFL) | O_NONBLOCK); /* as accepted sockets are */
    std::string big(32 * 1024, 'y');
    send(peer, "*3\r\n$3\r\nSET\r\n$5\r\nobufk\r\n$" + std::to_string(big.size()) + "\r\n" + big + "\r\n");
    for (int i = 0; i < 16 && recv(peer).empty(); i++) iteration();

    std::string get = "*2\r\n$3\r\nGET\r\n$5\r\nobufk\r\n";
    for (int i = 0; i < 64 && c->flag.fastpath; i++) {
        send(peer, get);
        iteration();
    }
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(c->flag.close_asap, 1u);
    EXPECT_EQ(server.stat_client_outbuf_limit_disconnections, closed + 1);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
    server.client_obuf_limits[CLIENT_TYPE_NORMAL] = saved;
}

/* Turning staged mode off hands every staged client back to main. */
TEST_F(FastpathStagedTest, ReleaseAllHandsEveryClientBack) {
    int pa, pb;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    EXPECT_EQ(fastpathClientCount(), 2u);
    testOnlyStagedStop();
    EXPECT_EQ(fastpathClientCount(), 0u);
    EXPECT_EQ(a->flag.fastpath, 0u);
    EXPECT_EQ(b->flag.fastpath, 0u);
    EXPECT_EQ(a->control->lifecycle, FP_DETACHED);
    close(pa);
    close(pb);
    freeClient(a);
    freeClient(b);
    ASSERT_EQ(testOnlyStagedStart(), 0);
}

/* A client that never entered the fast path has no control, and reclaiming it is a no-op. */
TEST_F(FastpathStagedTest, ReclaimWithoutControlIsSafe) {
    client *fake = createClient(NULL);
    EXPECT_EQ(fake->control, nullptr);
    fastpathControlReclaim(fake);
    EXPECT_EQ(fake->control, nullptr);
    freeClient(fake);
}

/* Fast GET answers a pure-GET read on the IO thread during the read phase, before the write and
 * reply phases run. This proves the speculate path is exercised, so the pause gate below is a real
 * gate and not a test that passes because fast GET never engaged. */
TEST_F(FastpathStagedTest, FastGetServesPureGetInReadPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    fastGetRecomputeSpecAclOk(c); /* default (no) user is unrestricted: gate opens */

    send(peer, "*3\r\n$3\r\nSET\r\n$3\r\nfgk\r\n$1\r\nv\r\n");
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n");

    /* A pure-GET read: the reply is written straight to the socket in the read phase. */
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\nfgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");
    writeAndReplyPhases();
    freeStaged(c, peer);
}

/* CLIENT PAUSE ALL (and FAILOVER, which pauses ALL) must hold a fast-path client's reads too. A
 * client admitted before the pause keeps its fd on the staged poller, so without a pause gate in
 * fastGetSpeculateBatch its GET is answered on the IO thread during the pause. Stock holds every
 * read until unpause. This test fails on the superset binary before the fix (the read phase writes
 * the reply) and passes after it. A read is not held by PAUSE WRITE, which the last block checks. */
TEST_F(FastpathStagedTest, ClientPauseAllHoldsFastGet) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    fastGetRecomputeSpecAclOk(c);

    send(peer, "*3\r\n$3\r\nSET\r\n$3\r\npgk\r\n$1\r\nv\r\n");
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n");

    /* Pause all client actions (isPausedActions reads server.paused_actions directly). */
    server.paused_actions = PAUSE_ACTION_CLIENT_ALL;
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\npgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), ""); /* held: nothing served in the read phase while paused ALL */

    /* Unpause and finish the iteration: the held GET was harvested for main and now replies. This
     * also drains the client so the next block starts with nothing in flight (a precondition for
     * fast GET to speculate again). */
    server.paused_actions = 0;
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");

    /* A read is not held by PAUSE WRITE: fast GET serves it in the read phase. */
    server.paused_actions = PAUSE_ACTION_CLIENT_WRITE;
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\npgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");

    server.paused_actions = 0;
    writeAndReplyPhases();
    freeStaged(c, peer);
}
/* ---- Regression tests added by the superset audit (p4-unit-tests-confirmed) ---- */

extern "C" {
#include "bio.h"
}
#include <pthread.h>

/* SPT-14 / SPT-15: main wrote into c->buf (a tracking push to a REDIRECT target) and owns that
 * write. A fast GET must not answer this read: sending c->buf from the IO thread too would send
 * main's bytes twice (97 pushes seen for 50 invalidations). The GET goes to main instead. */
TEST_F(FastpathStagedTest, FastGetPuntsWhenMainWroteIntoClientBuffer) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$7\r\nst:spt1\r\n$1\r\nv\r\n");
    iteration();
    ASSERT_EQ(recv(peer), "+OK\r\n");

    const std::string push = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n";
    ASSERT_LT(push.size(), (size_t)c->buf_usable_size);
    memcpy(c->buf, push.data(), push.size());
    c->bufpos = (int)push.size();
    c->flag.pending_write = 1; /* main queued its own write of these bytes */

    send(peer, "*2\r\n$3\r\nGET\r\n$7\r\nst:spt1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u) << "the GET must be harvested for main, not answered by fast GET";
    EXPECT_EQ(recv(peer), "") << "the IO thread sent bytes that main owns";
    EXPECT_EQ(mainBuf(c), push) << "main's bytes were consumed or moved by the IO thread";

    /* Hand main's bytes back so freeStaged sees a clean client. */
    c->bufpos = 0;
    c->flag.pending_write = 0;
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");
    freeStaged(c, peer);
}

/* REPL-15: on a replica whose link is down, with replica-serve-stale-data no, a GET must be
 * refused with -MASTERDOWN, as processCommand does on main. Fast GET must not answer it. */
TEST_F(FastpathStagedTest, FastGetRefusesStaleDataWhenReplicaLinkIsDown) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$7\r\nst:rpl1\r\n$1\r\nv\r\n");
    iteration();
    ASSERT_EQ(recv(peer), "+OK\r\n");

    sds saved_host = server.primary_host;
    int saved_state = server.repl_state, saved_stale = server.repl_serve_stale_data;
    server.primary_host = sdsnew("primary.example");
    server.primary_port = 1;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_serve_stale_data = 0;

    send(peer, "*2\r\n$3\r\nGET\r\n$7\r\nst:rpl1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u) << "the GET must reach main while the link is down";
    EXPECT_EQ(recv(peer), "") << "fast GET served stale data";
    writeAndReplyPhases();
    std::string got = recv(peer);
    EXPECT_EQ(got.compare(0, 11, "-MASTERDOWN"), 0) << "reply was: " << got;

    sdsfree(server.primary_host);
    server.primary_host = saved_host;
    server.repl_state = saved_state;
    server.repl_serve_stale_data = saved_stale;
    freeStaged(c, peer);
}

