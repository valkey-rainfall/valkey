/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* D+ speculative readers never wait for main, and main frees memory a reader can reach
 * at once only inside an exclusive window main has drained itself. */

#include "generated_wrappers.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "module.h"
#include "server.h"
#include "dplus.h"
extern hashtableType commandSetType;
extern int dplus_exclusive_mode; /* _Atomic(int) in C */
void createSharedObjects(void);
}

static const char *dplus_logfile = "/dev/null";

class DplusTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(dplus_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();
        connTypeInitialize();
        initSharedQueryBuf();

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

        server.io_threads_num = 2;
        dplusEnableSpeculativeReads();
    }

    static void TearDownTestSuite() {
        aeDeleteEventLoop(server.el);
        server.el = NULL;
    }

    static robj *str(const std::string &s) { return createStringObject(s.data(), s.size()); }

    static hashtable *keys() { return kvstoreGetHashtable(server.db[0]->keys, 0); }

    static unsigned shardOf(const std::string &key) {
        sds k = sdsnewlen(key.data(), key.size());
        unsigned shard = DPLUS_SHARD_INDEX(hashtableHashKey(keys(), k));
        sdsfree(k);
        return shard;
    }

    static void set(const std::string &key, const std::string &val) {
        robj *k = str(key);
        robj *v = str(val);
        setKey(NULL, server.db[0], k, &v, 0);
        decrRefCount(k);
    }

    static robj **getArgv(const std::string &key) {
        robj **argv = static_cast<robj **>(zmalloc(sizeof(robj *) * 2));
        argv[0] = str("GET");
        argv[1] = str(key);
        return argv;
    }

    /* A client whose read parsed GET first, then the queued GETs. */
    static client *readerClient(const std::string &first, const std::vector<std::string> &queued) {
        client *c = createClient(NULL);
        struct serverCommand *get = lookupCommandByCString("get");
        c->argc = 2;
        c->argv_len = 2;
        c->argv = getArgv(first);
        c->parsed_cmd = get;
        c->cmd_queue.cmds = static_cast<parsedCommand *>(zcalloc(sizeof(parsedCommand) * queued.size()));
        c->cmd_queue.cap = queued.size();
        c->cmd_queue.len = queued.size();
        for (size_t i = 0; i < queued.size(); i++) {
            parsedCommand *p = &c->cmd_queue.cmds[i];
            p->argc = 2;
            p->argv_len = 2;
            p->argv = getArgv(queued[i]);
            p->cmd = get;
        }
        __atomic_store_n(&c->spec_acl_ok, 1, __ATOMIC_RELEASE);
        return c;
    }

    /* Main's side of a shard bracket: odd while a write is in progress. */
    static void bumpShard(dplusVersionArray *va, unsigned shard) {
        __atomic_fetch_add(&va->lines[shard / 8].v[shard % 8], 1, __ATOMIC_SEQ_CST);
    }

    static int exclusiveDepth() { return __atomic_load_n(&dplus_exclusive_mode, __ATOMIC_SEQ_CST); }

    static bool waitFor(const std::function<bool()> &done, std::chrono::milliseconds limit) {
        auto deadline = std::chrono::steady_clock::now() + limit;
        while (!done()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }
};

/* Main holds the later GET's shard odd. The reader must end the prefix there instead of waiting:
 * main can be draining readers while it holds shards odd, as rehash completion does. */
TEST_F(DplusTest, LaterGetOnOddShardEndsThePrefix) {
    set("a", "va");
    std::string b = "b";
    for (int i = 0; shardOf(b) == shardOf("a"); i++) b = "b" + std::to_string(i);
    set(b, "vb");

    client *c = readerClient("a", {b});
    dplus_write_tax[1].history = ~(uint64_t)0; /* recent batches speculated */
    dplusVersionArray *va = hashtableGetVersionArray(keys());
    ASSERT_NE(va, nullptr);
    bumpShard(va, shardOf(b)); /* odd */

    std::atomic<int> speculated{-1};
    std::thread reader([&] { speculated = dplusSpeculateBatch(c, 1); });
    bool returned_while_odd = waitFor([&] { return speculated.load() >= 0; }, std::chrono::milliseconds(2000));
    bumpShard(va, shardOf(b)); /* even again: lets a waiting reader finish */
    reader.join();

    EXPECT_TRUE(returned_while_odd);
    EXPECT_EQ(speculated.load(), 1);
    EXPECT_TRUE(c->read_flags & READ_FLAGS_DPLUS_SPECULATED);
    EXPECT_FALSE(c->cmd_queue.cmds[0].read_flags & READ_FLAGS_DPLUS_SPECULATED);
    EXPECT_EQ(std::string(c->buf, c->bufpos), "$2\r\nva\r\n");
    freeClient(c);
}

/* A thread other than main (BIO freeing a table) must not open an exclusive window: main would
 * take it as proof that readers drained and free retired memory an active reader can still reach. */
TEST_F(DplusTest, DrainOffMainLeavesMainRetirementsDeferred) {
    uint64_t epoch;
    ASSERT_EQ(dplusDebugPinReader(&epoch), C_OK); /* an active reader */

    std::atomic<bool> left{false};
    std::thread bio([&] {
        dplusExclusiveEnter();
        dplusExclusiveLeave();
        left = true;
    });
    waitFor([&] { return left.load() || exclusiveDepth() > 0; }, std::chrono::milliseconds(1000));
    bool bio_waited_for_reader = !left.load();

    void *p = zmalloc(64);
    int deferred = dplusDeferFreeRaw(p);
    ASSERT_EQ(dplusDebugUnpinReader(), C_OK);
    bio.join();
    if (!deferred) zfree(p);

    EXPECT_FALSE(bio_waited_for_reader);
    EXPECT_EQ(deferred, 1);
    dplusReclaimRetired();
    uint64_t stats[8];
    dplusDebugEpochStats(stats);
    EXPECT_EQ(stats[1], 0u); /* nothing left retired */
}

/* Inside main's own window every reader has drained, so a retirement frees at once. */
TEST_F(DplusTest, MainExclusiveWindowFreesAtOnce) {
    dplusExclusiveEnter();
    EXPECT_EQ(exclusiveDepth(), 1);
    void *p = zmalloc(64);
    int deferred = dplusDeferFreeRaw(p);
    if (!deferred) zfree(p);
    dplusExclusiveLeave();
    EXPECT_EQ(deferred, 0);
    EXPECT_EQ(exclusiveDepth(), 0);
}

/* A module thread holding the GIL writes the keyspace like main, so its exclusive window must
 * drain readers like main's. */
TEST_F(DplusTest, ModuleThreadHoldingTheGilDrainsReaders) {
    uint64_t epoch;
    ASSERT_EQ(dplusDebugPinReader(&epoch), C_OK); /* an active reader */

    std::atomic<bool> entered{false};
    std::thread module_thread([&] {
        dplusGilWriterBegin();
        dplusExclusiveEnter();
        entered = true;
        dplusExclusiveLeave();
        dplusGilWriterEnd();
    });
    bool entered_before_drain = waitFor([&] { return entered.load(); }, std::chrono::milliseconds(200));
    ASSERT_EQ(dplusDebugUnpinReader(), C_OK);
    module_thread.join();

    EXPECT_FALSE(entered_before_drain);
    EXPECT_TRUE(entered.load());
    EXPECT_EQ(exclusiveDepth(), 0);
}

/* A unit gives each held shard one bracket however many writes land in it, and leaves the
 * other shards to their own brackets. */
TEST_F(DplusTest, UnitBracketsAHeldShardOnceAcrossWrites) {
    set("u", "0");
    std::string other = "o";
    for (int i = 0; shardOf(other) == shardOf("u"); i++) other = "o" + std::to_string(i);
    dplusVersionArray *va = hashtableGetVersionArray(keys());
    auto version = [&](const std::string &k) {
        unsigned s = shardOf(k);
        return __atomic_load_n(&va->lines[s / 8].v[s % 8], __ATOMIC_SEQ_CST);
    };
    uint64_t u0 = version("u"), o0 = version(other);

    ASSERT_EQ(dplusUnitHoldShard(va, shardOf("u")), 1);
    EXPECT_EQ(version("u"), u0 + 1); /* odd while held */
    set("u", "1");
    set("u", "2");
    set(other, "x");
    EXPECT_EQ(version("u"), u0 + 1); /* the writes inside add nothing */
    dplusUnitRelease();

    EXPECT_EQ(version("u"), u0 + 2);
    EXPECT_EQ(version(other), o0 + 2); /* bracketed by its own write */
}
