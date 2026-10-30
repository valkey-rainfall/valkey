/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Write lanes in cluster mode, driven inline on one thread as in test_fastpath_lanes.cpp. The read
 * phase accepts a lane entry only while this node serves its slot stably (clusterServesSlotStably).
 * The ring (main-path) clients' commands and the cluster bus run on main between the read phase and
 * the lane phase, and can change slot ownership, the migrating / importing tables or the cluster
 * state in that window. A lane runs cmd->proc with no redirect check, so an entry classified before
 * the change must not run on a lane after it: it has to go to main, where processCommand gives the
 * MOVED, ASK or CLUSTERDOWN reply stock execution gives. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstdarg>
#include <cstring>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "cluster.h"
#include "cluster_legacy.h"
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "poller.h"
#include "server.h"
#include "staged_loop.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
extern dictType objToHashtableDictType;
extern dictType objectKeyPointerValueDictType;
extern dictType clusterNodesDictType;
extern clusterNode *myself;
clusterNode *createClusterNode(char *nodename, int flags);
void createSharedObjects(void);
}

static const char *cl_logfile = "/dev/null";

/* Slot number keys encoded as pointers, as the cluster's migrating / importing tables use. */
static dictType clSlotDictType;

static dict *clSlotDictCreate(void) {
    memset(&clSlotDictType, 0, sizeof(clSlotDictType));
    clSlotDictType.entryGetKey = dictEntryGetKey;
    clSlotDictType.entryDestructor = zfree;
    return dictCreate(&clSlotDictType);
}

class FastpathClusterLanesTest : public ::testing::Test {
  protected:
    static clusterNode *peer; /* the other primary, target of the moves */

    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(cl_logfile);
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

        /* Cluster mode: one keyspace hashtable per slot, as createDatabase does. */
        server.cluster_enabled = 1;
        server.keyspace_shard_bits = 0;
        server.cluster_require_full_coverage = 1;
        server.cluster_preferred_endpoint_type = CLUSTER_ENDPOINT_TYPE_IP;
        int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES;
        server.dbnum = 1;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *)));
        server.db[0] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[0]->id = 0;
        server.db[0]->keys = kvstoreCreate(&kvstoreKeysHashtableType, CLUSTER_SLOT_MASK_BITS, flags);
        server.db[0]->expires = kvstoreCreate(&kvstoreExpiresHashtableType, CLUSTER_SLOT_MASK_BITS, flags);
        server.db[0]->keys_with_volatile_items = kvstoreCreate(&kvstoreExpiresHashtableType, CLUSTER_SLOT_MASK_BITS, flags);
        server.db[0]->watched_keys = dictCreate(&keylistDictType);
        server.db[0]->blocking_keys = dictCreate(&keylistDictType);
        server.db[0]->blocking_keys_unblock_on_nokey = dictCreate(&objectKeyPointerValueDictType);
        server.db[0]->ready_keys = dictCreate(&objectKeyPointerValueDictType);
        server.pubsub_channels = kvstoreCreate(&kvstoreChannelHashtableType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.pubsub_patterns = dictCreate(&objToHashtableDictType);
        server.pubsubshard_channels =
            kvstoreCreate(&kvstoreChannelHashtableType, CLUSTER_SLOT_MASK_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

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

        /* The parts of the cluster state the fast path, the slot functions and the redirect read:
         * this node a primary serving every slot, the cluster OK, nothing migrating or importing. */
        server.cluster = static_cast<clusterState *>(zcalloc(sizeof(clusterState)));
        server.cluster->nodes = dictCreate(&clusterNodesDictType);
        server.cluster->migrating_slots_to = clSlotDictCreate();
        server.cluster->importing_slots_from = clSlotDictCreate();
        server.cluster->slot_migration_jobs = listCreate();
        myself = server.cluster->myself = createClusterNode(NULL, CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        peer = createClusterNode(NULL, CLUSTER_NODE_PRIMARY);
        memcpy(peer->ip, "10.0.0.2", 9);
        peer->tcp_port = 45001;
        for (int s = 0; s < CLUSTER_SLOTS; s++) ASSERT_EQ(clusterAddSlot(myself, s), C_OK);
        server.cluster->state = CLUSTER_OK;

        server.io_threads_num = 2;
        server.io_threads_fast_path = 1;
        server.io_threads_strict_offload = 1;
        server.io_threads_parallel_writes = 1;
        server.io_threads_parallel_writes_min = 1; /* every segment runs on the lanes */
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
        testOnlyStagedSetWorkers(1, 1); /* main lane 0, IO thread 1 lane 1 */
        server.io_threads_parallel_writes_min = 1;
        emptyDbStructure(server.db, -1, 0, NULL);
    }

    void TearDown() override {
        testOnlyStagedStop();
        freeClientsInAsyncFreeQueue();
        /* Every slot back to this node, nothing migrating or importing, cluster OK. */
        for (int s = 0; s < CLUSTER_SLOTS; s++) {
            if (server.cluster->slots[s] != myself) {
                clusterDelSlot(s);
                clusterAddSlot(myself, s);
            }
            setMigratingSlotDest(s, NULL);
            setImportingSlotSource(s, NULL);
        }
        server.cluster->state = CLUSTER_OK;
    }

    static int slotOf(const char *key) {
        return (int)keyHashSlot(key, (int)strlen(key));
    }

    static client *newStagedClient(int *peerfd) {
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
        *peerfd = sv[1];
        return c;
    }

    static void send(int fd, const std::string &req) {
        EXPECT_EQ(write(fd, req.data(), req.size()), (ssize_t)req.size());
    }

    static std::string recv(int fd) {
        std::string out;
        char buf[65536];
        ssize_t n;
        while ((n = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT)) > 0) out.append(buf, n);
        return out;
    }

    /* One staged iteration: main plans the lanes, thread 1 reads and classifies, main executes (lane
     * phases inline), thread 1 writes the replies. */
    static void iteration() {
        fastpathStagedPrepareLanes();
        pollerGetNextBatch(stagedPoller(), 0);
        fastpathStagedReadPhase(1);
        fastpathStagedExecute();
        if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
        fastpathStagedFinish();
    }

    /* The read phase of an iteration: the pipeline on fd is parsed and classified onto the lanes with
     * the cluster state as it is now. What runs between this and runWindowed() models the ring
     * clients' commands and the cluster bus of the same iteration. */
    static void readPhase(int fd, const std::string &req) {
        fastpathStagedPrepareLanes();
        send(fd, req);
        pollerGetNextBatch(stagedPoller(), 0);
        fastpathStagedReadPhase(1);
    }

    static void writeAndReplyPhases() {
        fastpathStagedExecute();
        if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
        fastpathStagedFinish();
    }

    static long long infoStat(const char *name) {
        sds info = sdsempty();
        fastpathInfo(&info);
        const char *p = strstr(info, name);
        long long v = -1;
        if (p) v = atoll(p + strlen(name) + 1);
        sdsfree(info);
        return v;
    }

    static std::string bulk(const std::string &s) {
        return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
    }

    static std::string cmd(int argc, ...) {
        std::string out = "*" + std::to_string(argc) + "\r\n";
        va_list ap;
        va_start(ap, argc);
        for (int i = 0; i < argc; i++) out += bulk(va_arg(ap, const char *));
        va_end(ap);
        return out;
    }

    static void keyAbsent(const char *name) {
        robj *k = createStringObject(name, strlen(name));
        EXPECT_EQ(lookupKeyRead(server.db[0], k), nullptr) << name << " was written by a stale lane entry";
        decrRefCount(k);
    }

    static void keyPresent(const char *name) {
        robj *k = createStringObject(name, strlen(name));
        EXPECT_NE(lookupKeyRead(server.db[0], k), nullptr) << name << " must have been written";
        decrRefCount(k);
    }

    static void freeStaged(client *c, int fd) {
        close(fd);
        freeClient(c);
        iteration();
        freeClientsInAsyncFreeQueue();
    }
};

clusterNode *FastpathClusterLanesTest::peer = nullptr;

/* Baseline: a lane write to a slot this node serves stably runs on the lanes and is applied. */
TEST_F(FastpathClusterLanesTest, LaneWriteToAStableOwnedSlotIsApplied) {
    int fd;
    client *c = newStagedClient(&fd);
    long long phases = infoStat("fastpath_write_lane_phases");
    long long entries = infoStat("fastpath_write_lane_entries");
    readPhase(fd, cmd(3, "SET", "b1stable", "v"));
    writeAndReplyPhases();
    EXPECT_EQ(recv(fd), "+OK\r\n");
    keyPresent("b1stable");
    EXPECT_GT(infoStat("fastpath_write_lane_phases"), phases) << "precondition: the write ran in a lane phase";
    EXPECT_GT(infoStat("fastpath_write_lane_entries"), entries);
    freeStaged(c, fd);
}

/* The slot moves to another node in the window, through the functions CLUSTER SETSLOT <slot> NODE
 * calls (clusterDelSlot then clusterAddSlot). The entry was classified for a lane while the slot was
 * ours; it must not run there. On main, processCommand answers MOVED and nothing is written. */
TEST_F(FastpathClusterLanesTest, SlotMovedInTheWindowGetsMovedNotALaneWrite) {
    const char *key = "b1moved";
    int slot = slotOf(key);
    int fd;
    client *c = newStagedClient(&fd);
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    /* The window: a ring client's CLUSTER SETSLOT <slot> NODE <peer>. */
    ASSERT_EQ(clusterDelSlot(slot), C_OK);
    ASSERT_EQ(clusterAddSlot(peer, slot), C_OK);
    writeAndReplyPhases();

    std::string expect = "-MOVED " + std::to_string(slot) + " 10.0.0.2:45001\r\n";
    EXPECT_EQ(recv(fd), expect);
    keyAbsent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* The slot is unassigned in the window (CLUSTER DELSLOTS, clusterDelSlot alone). Stock execution
 * answers that the slot is not served; a lane would write it. */
TEST_F(FastpathClusterLanesTest, SlotDeletedInTheWindowGetsClusterDownNotALaneWrite) {
    const char *key = "b1deleted";
    int slot = slotOf(key);
    int fd;
    client *c = newStagedClient(&fd);
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    ASSERT_EQ(clusterDelSlot(slot), C_OK); /* the window */
    writeAndReplyPhases();

    EXPECT_EQ(recv(fd), "-CLUSTERDOWN Hash slot not served\r\n");
    keyAbsent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* An unassigned slot is assigned to this node in the window (CLUSTER ADDSLOTS, clusterAddSlot
 * alone). The write to our own stable slot is still applied, on main, with the stock checks. */
TEST_F(FastpathClusterLanesTest, SlotAddedInTheWindowRunsTheIterationOnMain) {
    const char *key = "b1added";
    int slot = slotOf(key);
    int other = (slot + 2) % CLUSTER_SLOTS;
    int fd;
    client *c = newStagedClient(&fd);
    ASSERT_EQ(clusterDelSlot(other), C_OK); /* before the sample: the read phase sees it unassigned */
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    ASSERT_EQ(clusterAddSlot(myself, other), C_OK); /* the window */
    writeAndReplyPhases();

    EXPECT_EQ(recv(fd), "+OK\r\n");
    keyPresent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* The slot is set MIGRATING in the window (CLUSTER SETSLOT <slot> MIGRATING <peer>). Stock execution
 * answers ASK for a key that is not here; a lane would create it. */
TEST_F(FastpathClusterLanesTest, SlotSetMigratingInTheWindowGetsAskNotALaneWrite) {
    const char *key = "b1migrating";
    int slot = slotOf(key);
    int fd;
    client *c = newStagedClient(&fd);
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    setMigratingSlotDest(slot, peer); /* the window */
    writeAndReplyPhases();

    std::string expect = "-ASK " + std::to_string(slot) + " 10.0.0.2:45001\r\n";
    EXPECT_EQ(recv(fd), expect);
    keyAbsent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* An IMPORTING entry appears in the window for another slot. The classification required the tables
 * to be empty (clusterServesSlotStably), so the plan is stale and must run on main; the write to our
 * own slot is still applied there, with the stock checks. */
TEST_F(FastpathClusterLanesTest, SlotSetImportingInTheWindowRunsTheIterationOnMain) {
    const char *key = "b1importing";
    int slot = slotOf(key);
    int other = (slot + 1) % CLUSTER_SLOTS;
    int fd;
    client *c = newStagedClient(&fd);
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    setImportingSlotSource(other, peer); /* the window */
    writeAndReplyPhases();

    EXPECT_EQ(recv(fd), "+OK\r\n");
    keyPresent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* The cluster state changes in the window, as the cluster bus does when a node is marked failing
 * (clusterUpdateState from the ring's event processing). Stock execution answers CLUSTERDOWN. */
TEST_F(FastpathClusterLanesTest, ClusterStateChangeInTheWindowGetsClusterDownNotALaneWrite) {
    const char *key = "b1down";
    int fd;
    client *c = newStagedClient(&fd);
    long long serial = infoStat("fastpath_write_serial_segments");

    readPhase(fd, cmd(3, "SET", key, "v"));
    /* The window: with no other reachable primary known, the state evaluation turns FAIL. */
    clusterUpdateState();
    ASSERT_EQ(server.cluster->state, CLUSTER_FAIL) << "precondition: the state must have changed";
    writeAndReplyPhases();

    EXPECT_EQ(recv(fd), "-CLUSTERDOWN The cluster is down\r\n");
    keyAbsent(key);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial) << "the iteration must have gone serial";
    freeStaged(c, fd);
}

/* A second entry of the pipeline, in a slot that did not move, still gets its write: the demotion to
 * main keeps the plan's order and the stock checks answer each entry on its own. */
TEST_F(FastpathClusterLanesTest, OnlyTheMovedSlotIsRedirectedTheRestOfThePipelineIsApplied) {
    const char *moved = "b1pipe-moved";
    const char *kept = "b1pipe-kept";
    int slot = slotOf(moved);
    ASSERT_NE(slot, slotOf(kept));
    int fd;
    client *c = newStagedClient(&fd);

    readPhase(fd, cmd(3, "SET", kept, "1") + cmd(3, "SET", moved, "2") + cmd(3, "SET", kept, "3") + cmd(2, "GET", kept));
    ASSERT_EQ(clusterDelSlot(slot), C_OK);
    ASSERT_EQ(clusterAddSlot(peer, slot), C_OK);
    writeAndReplyPhases();

    std::string expect = "+OK\r\n-MOVED " + std::to_string(slot) + " 10.0.0.2:45001\r\n+OK\r\n$1\r\n3\r\n";
    EXPECT_EQ(recv(fd), expect);
    keyAbsent(moved);
    keyPresent(kept);
    freeStaged(c, fd);
}
