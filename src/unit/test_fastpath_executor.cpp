/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Runs a staged write phase while the originating client's memory is
 * unmapped: main executes every entry from its borrowed argv and
 * CommandOrigin and never dereferences the client, so the unmapped client
 * cannot fault, and origin data must still be intact. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <sys/epoll.h>
#include <sys/mman.h>
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
void ACLFreeUser(user *u);
int modulePopulateClientInfoStructure(void *ci, client *client, int structver);
}

static const char *fp_logfile = "/dev/null";

class FastpathExecutorTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(fp_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.client_max_querybuf_len = 1024ll * 1024 * 1024;
        server.proto_max_bulk_len = 512ll * 1024 * 1024;
        server.maxclients = 1000;
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        unitTestInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        unitTestPopulateCommandTable();
        unitTestACLInit();

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
        unitTestConnTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyClearIOThreadReady(1);
        testOnlyFreeIOThreadQueues();
        unitTestDeleteEventLoop();
    }

    void SetUp() override {
        testOnlyInitIOThreadQueues();
        ASSERT_EQ(testOnlyStagedStart(), 0);
    }

    void TearDown() override {
        testOnlyStagedStop();
    }

    /* Read whatever the executor produced for the client from the other end of the pair. */
    static std::string readReply(int fd) {
        char buf[256];
        ssize_t n = read(fd, buf, sizeof(buf));
        return n > 0 ? std::string(buf, n) : std::string();
    }
};

TEST_F(FastpathExecutorTest, MainExecutesWithOriginOnlyWhileClientIsUnmapped) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;

    /* The client lives alone in its own pages so it can be unmapped while its batch runs. */
    size_t pagesz = sysconf(_SC_PAGESIZE);
    size_t maplen = (sizeof(client) + pagesz - 1) / pagesz * pagesz;
    client *c = static_cast<client *>(mmap(NULL, maplen, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    ASSERT_NE(c, MAP_FAILED);
    client *tmp = createClient(NULL);
    memcpy(c, tmp, sizeof(client));
    zfree(tmp);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 424242;
    c->fp_out = NULL;
    struct sockaddr_in6 sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin6_family = AF_INET6;
    sa.sin6_port = htons(31337);
    ASSERT_EQ(inet_pton(AF_INET6, "2001:db8::7", &sa.sin6_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;

    /* Admit through the real flow so the connection gets a ClientControl; the control is a separate
     * allocation, reachable while the client's own pages are unmapped. */
    ASSERT_EQ(fastpathAttach(c), C_OK);

    /* Two pipelined commands: GET on a missing key and SET, both plain data commands. */
    const char *req = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    ASSERT_EQ(c->fp_inflight, 2u);

    /* The write phase runs while the client is unreachable: it must use the entries and origin only. */
    ASSERT_EQ(mprotect(c, maplen, PROT_NONE), 0);
    EXPECT_EQ(fastpathStagedExecute(), 2);
    ASSERT_EQ(mprotect(c, maplen, PROT_READ | PROT_WRITE), 0);

    /* The reply phase writes both replies to the client. */
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(readReply(sv[1]), "$-1\r\n+OK\r\n");
    robj *key = createStringObject("foo", 3);
    EXPECT_NE(lookupKeyRead(server.db[0], key), nullptr);
    decrRefCount(key);

    /* Hand the client back (no freeClient on mmap'd pages), then reclaim its separately allocated control. */
    testOnlyStagedStop();
    EXPECT_EQ(c->flag.fastpath, 0u);
    connSetReadHandler(conn, NULL);
    fastpathControlReclaim(c);
    ASSERT_EQ(testOnlyStagedStart(), 0);

    close(sv[1]);
    close(sv[0]);
    conn->fd = -1;
    munmap(c, maplen);
}

/* A retired user struct is kept until the last fast-path client naming it hands off,
 * and every hop of a retire chain drops one reference per returning client. */
TEST_F(FastpathExecutorTest, RetiredPrincipalResolvesAndFreesAtHandoff) {
    user *u1 = ACLCreateUnlinkedUser();
    user *u2 = ACLCreateUnlinkedUser();
    user *u3 = ACLCreateUnlinkedUser();
    EXPECT_EQ(ACLResolveUser(u1), u1);

    u1->flags |= USER_FLAG_RETIRED;
    u1->successor = u2;
    u1->fp_refs = 1;
    EXPECT_EQ(ACLResolveUser(u1), u2);
    u2->flags |= USER_FLAG_RETIRED;
    u2->successor = u3;
    u2->fp_refs = 2; /* one client behind u1, one pointing at u2 directly */
    EXPECT_EQ(ACLResolveUser(u1), u3);
    EXPECT_EQ(ACLResolveUser(u2), u3);

    client a;
    memset(&a, 0, sizeof(a));
    a.user = u1;
    a.flag.authenticated = 1;
    ACLFastpathClientReturned(&a);
    EXPECT_EQ(a.user, u3);
    EXPECT_EQ(a.flag.authenticated, 1u); /* u1 is gone now; u2 still has one client */
    EXPECT_EQ(u2->fp_refs, 1u);

    /* The successor was deleted meanwhile: the returning client is demoted, as unstable does before closing it. */
    u2->successor = NULL;
    client b;
    memset(&b, 0, sizeof(b));
    b.user = u2;
    b.flag.authenticated = 1;
    ACLFastpathClientReturned(&b);
    EXPECT_EQ(b.user, DefaultUser);
    EXPECT_EQ(b.flag.authenticated, 0u);

    /* A live principal is left alone. */
    client d;
    memset(&d, 0, sizeof(d));
    d.user = u3;
    ACLFastpathClientReturned(&d);
    EXPECT_EQ(d.user, u3);
    ACLFreeUser(u3);
}

/* A module asking about a fast-path client by id gets the admission-time peer, not a
 * getpeername() on the IO-owned socket. */
TEST_F(FastpathExecutorTest, ClientInfoOfFastpathClientComesFromAdmissionPeer) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 515151;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40001);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.77", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);

    ValkeyModuleClientInfoV1 ci;
    memset(&ci, 0, sizeof(ci));
    ci.version = 1;
    c->flag.fastpath = 1;
    ASSERT_EQ(modulePopulateClientInfoStructure(&ci, c, 1), VALKEYMODULE_OK);
    EXPECT_STREQ(ci.addr, "192.0.2.77");
    EXPECT_EQ(ci.port, 40001);
    EXPECT_EQ(ci.id, 515151u);
    EXPECT_EQ(ci.db, 0);

    /* Off the fast path the socket is main's again and is asked directly. */
    c->flag.fastpath = 0;
    ASSERT_EQ(modulePopulateClientInfoStructure(&ci, c, 1), VALKEYMODULE_OK);
    EXPECT_STRNE(ci.addr, "192.0.2.77");

    close(sv[1]);
    close(sv[0]);
    conn->fd = -1;
}

/* ---- Regression tests added by the superset audit (repro-group-c) ---- */

extern "C" {
long long clientsTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
void updateClientMemoryUsage(client *c);
void getExpensiveClientsInfo(size_t *in_usage, size_t *out_usage);
}

/* An admitted fast-path client on one end of a socketpair; the test writes requests to *peer_fd. */
static client *fpTestAdmitClient(int *peer_fd, connection **conn_out) {
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return NULL;
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->fp_out = NULL;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40002);
    inet_pton(AF_INET, "192.0.2.78", &sa.sin_addr);
    if (peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)) != C_OK) return NULL;
    c->fp_local = c->fp_peer;
    if (fastpathAttach(c) != C_OK) return NULL;
    *peer_fd = sv[1];
    *conn_out = conn;
    return c;
}

/* Hand the client back to main and release the socket. The client struct is leaked on purpose:
 * freeClient needs the full server state that this fixture does not build. */
static void fpTestReleaseClient(client *c, connection *conn, int peer_fd) {
    testOnlyStagedStop();
    connSetReadHandler(conn, NULL);
    fastpathControlReclaim(c);
    testOnlyStagedStart();
    close(peer_fd);
    close(conn->fd);
    conn->fd = -1;
}

/* Bytes the peer can read right now, without blocking. */
static ssize_t fpTestReadNow(int fd, char *buf, size_t cap) {
    ssize_t n = recv(fd, buf, cap, MSG_DONTWAIT);
    return n < 0 ? 0 : n;
}

/* EV-02 / STR-05: a GET answered on the IO thread (fast GET) must update the LRU clock and the
 * LFU counter of the value, as lookupKey does on main. Otherwise *-lru / *-lfu eviction evicts
 * the hottest keys first and OBJECT IDLETIME / FREQ are wrong. */
TEST_F(FastpathExecutorTest, FastGetTouchesLruAndLfu) {
    server.child_pid = -1; /* no fork in progress: lookupKey would touch as well */
    server.maxmemory_policy = MAXMEMORY_ALLKEYS_LRU;
    lrulfu_updateClockAndPolicy(mstime(), false);

    robj *key = createStringObject("hot", 3);
    robj *val = createStringObject("x", 1);
    dbAdd(server.db[0], key, &val);
    robj *stored = lookupKeyReadWithFlags(server.db[0], key, LOOKUP_NOTOUCH | LOOKUP_NOSTATS | LOOKUP_NONOTIFY);
    ASSERT_NE(stored, nullptr);
    stored->lru = lru_import(100); /* the key looks idle for 100 s */
    uint32_t idle = 0;
    lrulfu_getIdleness(stored->lru, &idle);
    ASSERT_GE(idle, 99u);

    int peer;
    connection *conn;
    client *c = fpTestAdmitClient(&peer, &conn);
    ASSERT_NE(c, nullptr);

    /* One read whose only complete command is a 2-argument GET: the fast GET path answers it on
     * the IO thread during the read phase, before main executes anything. */
    const char *req = "*2\r\n$3\r\nGET\r\n$3\r\nhot\r\n";
    ASSERT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    char buf[64];
    ssize_t n = fpTestReadNow(peer, buf, sizeof(buf));
    ASSERT_EQ(n, 7) << "the GET was not answered by fast GET (engagement check)";
    EXPECT_EQ(memcmp(buf, "$1\r\nx\r\n", 7), 0) << "unexpected fast GET reply";
    EXPECT_EQ(c->fp_inflight, 0u);
    fastpathStagedExecute();
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();

    lrulfu_getIdleness(stored->lru, &idle);
    EXPECT_EQ(idle, 0u) << "fast GET did not reset the LRU idle time";

    /* LFU: with lfu-log-factor 0 every touch increments the counter. */
    server.maxmemory_policy = MAXMEMORY_ALLKEYS_LFU;
    lfu_config_log_factor = 0;
    lrulfu_updateClockAndPolicy(mstime(), true);
    stored->lru = lfu_import(5);
    uint8_t freq = 0;
    lfu_getFrequency(stored->lru, &freq);
    ASSERT_EQ(freq, 5);
    char many[8 * 24 + 1]; /* 8 GETs of 24 bytes, 8 replies of 7 bytes */
    many[0] = '\0';
    for (int i = 0; i < 8; i++) strcat(many, req);
    ASSERT_EQ(write(peer, many, strlen(many)), (ssize_t)strlen(many));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    n = fpTestReadNow(peer, buf, sizeof(buf));
    ASSERT_EQ(n, 8 * 7) << "the GET batch was not answered by fast GET (engagement check)";
    fastpathStagedExecute();
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    lfu_getFrequency(stored->lru, &freq);
    EXPECT_GT(freq, 5) << "fast GET did not increment the LFU counter";

    fpTestReleaseClient(c, conn, peer);
    dbDelete(server.db[0], key);
    decrRefCount(key);
    server.maxmemory_policy = 0;
    lrulfu_updateClockAndPolicy(mstime(), false);
}

/* The LRU/LFU touch of a value answered by fast GET is recorded by the IO thread and applied by main
 * once the read phase has closed (fastGetApplyTouches, called from fastpathStagedExecute and by the
 * staged loop when the READ barrier closes): the IO thread never writes the object's first word,
 * which it shares with type, encoding, the flags and the refcount that other fast GETs read at the
 * same time. So right after the read phase the value is still idle; after main's write phase entry
 * it is touched, and with lfu-log-factor 0 every one of the 8 GETs counts (5 + 8 = 13), as 8
 * lookupKey calls on main would count. A touch applied on the IO thread fails the first check. */
TEST_F(FastpathExecutorTest, FastGetTouchIsAppliedByMainAfterTheReadPhase) {
    server.child_pid = -1;
    server.maxmemory_policy = MAXMEMORY_ALLKEYS_LRU;
    lrulfu_updateClockAndPolicy(mstime(), false);
    robj *key = createStringObject("hot2", 4);
    robj *val = createStringObject("y", 1);
    dbAdd(server.db[0], key, &val);
    robj *stored = lookupKeyReadWithFlags(server.db[0], key, LOOKUP_NOTOUCH | LOOKUP_NOSTATS | LOOKUP_NONOTIFY);
    ASSERT_NE(stored, nullptr);
    stored->lru = lru_import(100);
    uint32_t idle = 0;
    lrulfu_getIdleness(stored->lru, &idle);
    ASSERT_GE(idle, 99u);

    int peer;
    connection *conn;
    client *c = fpTestAdmitClient(&peer, &conn);
    ASSERT_NE(c, nullptr);
    const char *req = "*2\r\n$3\r\nGET\r\n$4\r\nhot2\r\n";
    ASSERT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    char buf[128];
    ASSERT_EQ(fpTestReadNow(peer, buf, sizeof(buf)), 7) << "the GET was not answered by fast GET (engagement check)";
    lrulfu_getIdleness(stored->lru, &idle);
    EXPECT_GE(idle, 99u) << "the IO thread wrote the object's lru during the read phase";
    fastpathStagedExecute(); /* main: the deferred touch lands here */
    lrulfu_getIdleness(stored->lru, &idle);
    EXPECT_EQ(idle, 0u) << "main did not apply the deferred touch";
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();

    /* LFU: every one of 8 GETs counts once. */
    server.maxmemory_policy = MAXMEMORY_ALLKEYS_LFU;
    lfu_config_log_factor = 0;
    lrulfu_updateClockAndPolicy(mstime(), true);
    stored->lru = lfu_import(5);
    uint8_t freq = 0;
    char many[8 * 25 + 1];
    many[0] = '\0';
    for (int i = 0; i < 8; i++) strcat(many, req);
    ASSERT_EQ(write(peer, many, strlen(many)), (ssize_t)strlen(many));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    ASSERT_EQ(fpTestReadNow(peer, buf, sizeof(buf)), 8 * 7) << "the GET batch was not answered by fast GET (engagement check)";
    lfu_getFrequency(stored->lru, &freq);
    EXPECT_EQ(freq, 5) << "the IO thread changed the LFU counter during the read phase";
    fastpathStagedExecute();
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    lfu_getFrequency(stored->lru, &freq);
    EXPECT_EQ(freq, 13) << "8 fast GETs must count as 8 touches";

    /* A child process in progress skips the touch, as lookupKey does, and drops the records. */
    stored->lru = lfu_import(5);
    ASSERT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    ASSERT_EQ(fpTestReadNow(peer, buf, sizeof(buf)), 7);
    server.child_pid = 1; /* hasActiveChildProcess() */
    fastpathStagedExecute();
    server.child_pid = -1;
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    lfu_getFrequency(stored->lru, &freq);
    EXPECT_EQ(freq, 5) << "a touch recorded while a child runs must not be applied";

    fpTestReleaseClient(c, conn, peer);
    dbDelete(server.db[0], key);
    decrRefCount(key);
    server.maxmemory_policy = 0;
    lrulfu_updateClockAndPolicy(mstime(), false);
}

/* OBS-08: bytes read and written by fast-path IO threads must be counted in the stock INFO stats
 * (total_net_input_bytes, total_net_output_bytes, total_reads_processed, total_writes_processed),
 * as the stock IO threads count them. */
TEST_F(FastpathExecutorTest, FastpathTrafficIsCountedInServerStats) {
    server.child_pid = -1;
    server.stat_net_input_bytes = 0;
    server.stat_net_output_bytes = 0;
    server.stat_total_reads_processed = 0;
    server.stat_total_writes_processed = 0;

    int peer;
    connection *conn;
    client *c = fpTestAdmitClient(&peer, &conn);
    ASSERT_NE(c, nullptr);

    /* A SET runs on main through the executor; its reply goes out in the reply phase. */
    const char *req = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    ASSERT_EQ(c->fp_inflight, 1u) << "the SET was not harvested by the fast path (engagement check)";
    EXPECT_EQ(fastpathStagedExecute(), 1);
    fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    char buf[64];
    ASSERT_EQ(fpTestReadNow(peer, buf, sizeof(buf)), 5);
    EXPECT_EQ(memcmp(buf, "+OK\r\n", 5), 0);

    EXPECT_EQ(server.stat_net_input_bytes, (long long)strlen(req));
    EXPECT_EQ(server.stat_net_output_bytes, 5);
    EXPECT_EQ(server.stat_total_reads_processed, 1);
    EXPECT_EQ(server.stat_total_writes_processed, 1);

    fpTestReleaseClient(c, conn, peer);
    robj *key = createStringObject("foo", 3);
    dbDelete(server.db[0], key);
    decrRefCount(key);
}

/* OBS-15: with client eviction off (maxmemory-clients 0, the default) clientsCron must still
 * refresh the memory of a fast-path client into stat_clients_type_memory (INFO mem_clients_normal,
 * MEMORY STATS clients.normal) and the recent-max output buffer sample
 * (INFO client_recent_max_output_buffer). */
TEST_F(FastpathExecutorTest, FastpathClientMemoryIsRefreshedByClientsCron) {
    server.maxmemory_clients = 0;
    server.hz = CONFIG_DEFAULT_HZ;
    server.unixtime = time(NULL);
    server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] = 0;

    int peer;
    connection *conn;
    client *c = fpTestAdmitClient(&peer, &conn);
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(c->flag.fastpath, 1u);

    /* 1 MiB of replies the IO thread could not write yet. */
    const size_t pending = 1024 * 1024;
    c->fp_out = sdsnewlen(NULL, pending);
    listAddNodeTail(server.clients, c);
    c->client_list_node = listLast(server.clients);

    for (int i = 0; i < 3; i++) clientsTimeProc(server.el, 0, NULL);

    EXPECT_GE(server.stat_clients_type_memory[CLIENT_TYPE_NORMAL], pending)
        << "mem_clients_normal does not include the fast-path client's pending output";
    size_t in_usage = 0, out_usage = 0;
    getExpensiveClientsInfo(&in_usage, &out_usage);
    EXPECT_GE(out_usage, pending) << "client_recent_max_output_buffer ignores the fast-path client";

    /* Settle the accounting before leaving. */
    sdsfree(c->fp_out);
    c->fp_out = NULL;
    updateClientMemoryUsage(c);
    listDelNode(server.clients, c->client_list_node);
    c->client_list_node = NULL;
    fpTestReleaseClient(c, conn, peer);
}

/* tryPartitionClient: EPOLL_CTL_ADD publishes the client to the IO thread, which may claim and
 * read it at once. io_tid, flag.partitioned and flag.pending_read must already be stored when the
 * ADD is made, and a failed ADD must leave the client as it was. */
static int fp_publish_calls = 0;
static int fp_publish_tid = -1, fp_publish_partitioned = -1, fp_publish_pending = -1;
static void fpRecordPublish(client *c) {
    fp_publish_calls++;
    fp_publish_tid = c->io_tid;
    fp_publish_partitioned = c->flag.partitioned;
    fp_publish_pending = c->flag.pending_read;
}

TEST_F(FastpathExecutorTest, PartitionedClientStateIsStoredBeforeEpollAdd) {
    testOnlyStagedStop(); /* partitioning is off while the staged loop runs */
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);

    /* The socket is already in the IO thread's epoll set, so the ADD fails with EEXIST after the
     * hook has seen what the IO thread would see. Close the suite's epoll fd for slot 1 before
     * replacing it, or the fd stays open for the rest of the process. */
    testOnlyClearIOThreadReady(1);
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    ASSERT_GE(epfd, 0);
    testOnlySetIOThreadReady(1, epfd);
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, sv[0], &ev), 0);

    fp_publish_calls = 0;
    testOnlySetPartitionPublishHook(fpRecordPublish);
    EXPECT_EQ(tryPartitionClient(c), C_ERR);
    testOnlySetPartitionPublishHook(NULL);

    ASSERT_EQ(fp_publish_calls, 1) << "tryPartitionClient did not reach the EPOLL_CTL_ADD";
    EXPECT_EQ(fp_publish_tid, 1) << "io_tid is stored after the client is published";
    EXPECT_EQ(fp_publish_partitioned, 1) << "flag.partitioned is stored after the client is published";
    EXPECT_EQ(fp_publish_pending, 1) << "flag.pending_read is stored after the client is published";
    EXPECT_EQ(c->io_tid, 0) << "a failed ADD left io_tid set";
    EXPECT_EQ(c->flag.partitioned, 0u) << "a failed ADD left the client partitioned";
    EXPECT_EQ(c->flag.pending_read, 0u) << "a failed ADD left a read pending";

    epoll_ctl(epfd, EPOLL_CTL_DEL, sv[0], NULL);
    close(epfd);
    testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    c->conn = NULL;
    close(sv[1]);
    close(sv[0]);
    testOnlyStagedStart();
}
