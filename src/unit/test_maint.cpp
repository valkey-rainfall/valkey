/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* MAINT jobs run inline on main: no staged workers, so maintRun takes the single-lane path. */

#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "maint.h"
#include "server.h"
serverDb *createDatabase(int id);
}

class MaintTest : public ::testing::Test {
  protected:
    void SetUp() override {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.child_pid = -1;
        server.logfile = (char *)"/dev/null";
        server.verbosity = LL_WARNING;
        server.replicas = listCreate();
        server.keyspace_shard_bits = 2;
        server.dbnum = 1;
        server.db = (serverDb **)zcalloc(sizeof(serverDb *));
        server.db[0] = createDatabase(0);
        maintReset();
    }

    void TearDown() override {
        maintReset();
        serverDb *db = server.db[0];
        kvstoreRelease(db->keys);
        kvstoreRelease(db->expires);
        kvstoreRelease(db->keys_with_volatile_items);
        dictRelease(db->blocking_keys);
        dictRelease(db->blocking_keys_unblock_on_nokey);
        dictRelease(db->ready_keys);
        dictRelease(db->watched_keys);
        zfree(db);
        zfree(server.db);
        server.db = NULL;
        listRelease(server.replicas);
        server.replicas = NULL;
    }

    /* Add keys key:<from>..key:<to-1> to table didx of db 0. */
    static void addKeys(int didx, int from, int to) {
        for (int i = from; i < to; i++) {
            sds key = sdscatprintf(sdsempty(), "key:%d", i);
            robj *o = objectSetKeyAndExpire(createStringObject("v", 1), key, -1);
            ASSERT_TRUE(kvstoreHashtableAdd(server.db[0]->keys, didx, o));
            sdsfree(key);
        }
    }

    static void delKeys(int didx, int from, int to) {
        for (int i = from; i < to; i++) {
            sds key = sdscatprintf(sdsempty(), "key:%d", i);
            ASSERT_TRUE(kvstoreHashtableDelete(server.db[0]->keys, didx, key));
            sdsfree(key);
        }
    }
};

/* serverCron can fork a save after databasesCron posted RESIZE and REHASH; the table work must not run
 * while the child exists, as the inline cron path skips it then. */
TEST_F(MaintTest, NoTableWorkWhileForkChildExists) {
    kvstore *keys = server.db[0]->keys;
    addKeys(0, 0, 4096);
    while (kvstoreHashtableRehashingAt(keys, 0)) kvstoreRehashHashtable(keys, 0, 1000);
    unsigned long full = kvstoreHashtableBuckets(keys, 0);
    /* Deletes shrink the table themselves unless resizing is avoided, as it is while a child exists;
     * leave it under the normal minimum fill but above the one for a child. */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    delKeys(0, 300, 4096);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
    unsigned long sparse = kvstoreHashtableBuckets(keys, 0);
    ASSERT_EQ(sparse, full);
    ASSERT_FALSE(kvstoreHashtableRehashingAt(keys, 0));

    server.child_pid = 12345; /* a fork child exists */
    maintRequest(MAINT_JOB_RESIZE, 0);
    maintRequest(MAINT_JOB_REHASH, 1000);
    maintRun();
    ASSERT_EQ(kvstoreHashtableBuckets(keys, 0), sparse);
    ASSERT_FALSE(kvstoreHashtableRehashingAt(keys, 0));

    /* With no child the same request rightsizes the table. */
    server.child_pid = -1;
    maintRequest(MAINT_JOB_RESIZE, 0);
    maintRun();
    ASSERT_TRUE(kvstoreHashtableRehashingAt(keys, 0) || kvstoreHashtableBuckets(keys, 0) != sparse);

    /* A rehash under way is not advanced while a child exists. */
    if (kvstoreHashtableRehashingAt(keys, 0)) {
        server.child_pid = 12345;
        maintRequest(MAINT_JOB_REHASH, 100000);
        maintRun();
        ASSERT_TRUE(kvstoreHashtableRehashingAt(keys, 0));
        server.child_pid = -1;
    }
    delKeys(0, 0, 300);
}

/* Tables the mocked lane expiry visited, in order, and what each visit costs. */
static int expire_visits[64];
static int expire_nvisits = 0;
static uint64_t expire_table_cost_us = 0;

static uint64_t fakeExpireTableInLane(int tid, serverDb *db, int didx, mstime_t now, uint64_t budget_us, int *hit_limit) {
    UNUSED(tid);
    UNUSED(db);
    UNUSED(now);
    UNUSED(budget_us);
    UNUSED(hit_limit); /* each table finishes: nothing left expired in it */
    if (expire_nvisits < 64) expire_visits[expire_nvisits++] = didx;
    return expire_table_cost_us;
}

class MaintExpireTest : public MaintTest {
  protected:
    MockValkey mock;

    void SetUp() override {
        MaintTest::SetUp();
        using ::testing::_;
        using ::testing::Invoke;
        using ::testing::Return;
        EXPECT_CALL(mock, activeExpireInLanes()).WillRepeatedly(Return(1));
        EXPECT_CALL(mock, maintExpireReplayNeeded()).WillRepeatedly(Return(0));
        EXPECT_CALL(mock, activeExpireTableInLane(_, _, _, _, _, _)).WillRepeatedly(Invoke(fakeExpireTableInLane));
        EXPECT_CALL(mock, activeExpireFieldsTableInLane(_, _, _, _, _, _)).WillRepeatedly(Return(0));
        /* One key with a TTL in every table, so every table has expire work. */
        for (int didx = 0; didx < 4; didx++) {
            sds key = sdscatprintf(sdsempty(), "ttl:%d", didx);
            robj *o = objectSetKeyAndExpire(createStringObject("v", 1), key, 1);
            kvstoreHashtableAdd(server.db[0]->keys, didx, o);
            kvstoreHashtableAdd(server.db[0]->expires, didx, o);
            sdsfree(key);
        }
        expire_nvisits = 0;
        expire_table_cost_us = 0;
    }

    void TearDown() override {
        for (int didx = 0; didx < 4; didx++) {
            sds key = sdscatprintf(sdsempty(), "ttl:%d", didx);
            kvstoreHashtableDelete(server.db[0]->expires, didx, key);
            kvstoreHashtableDelete(server.db[0]->keys, didx, key);
            sdsfree(key);
        }
        MaintTest::TearDown();
    }

    /* Run one phase with the given jobs; returns how many tables it expired. */
    static int runPhase(int with_resize, uint64_t expire_budget_us) {
        int before = expire_nvisits;
        if (with_resize) maintRequest(MAINT_JOB_RESIZE, 0);
        maintRequest(MAINT_JOB_EXPIRE, expire_budget_us);
        maintRun();
        return expire_nvisits - before;
    }
};

/* The lane's budget runs out on a table that finished, so no table was cut off; the next phase must
 * start at the first table this one did not expire rather than at the same place again. */
TEST_F(MaintExpireTest, NextPhaseStartsAtFirstTableNotExpired) {
    expire_table_cost_us = 60;
    ASSERT_EQ(runPhase(0, 100), 2);
    int first = expire_visits[0];
    ASSERT_EQ(expire_visits[1], (first + 1) % 4);
    ASSERT_EQ(runPhase(0, 100), 2);
    ASSERT_EQ(expire_visits[2], (first + 2) % 4);
    ASSERT_EQ(expire_visits[3], (first + 3) % 4);
}

/* RESIZE keeps the lane claiming tables after its expire budget is spent; those tables are claimed
 * without expiry and the next phase must start expiring there. */
TEST_F(MaintExpireTest, TablesClaimedWithoutExpiryComeFirstNextPhase) {
    expire_table_cost_us = 60;
    ASSERT_EQ(runPhase(1, 100), 2);
    int first = expire_visits[0];
    ASSERT_EQ(runPhase(1, 100), 2);
    ASSERT_EQ(expire_visits[2], (first + 2) % 4);
    ASSERT_EQ(expire_visits[3], (first + 3) % 4);
}

/* An expire cycle posted while lanes were on must still run when they are off by the time MAINT runs
 * (cron-lanes turned off, a forkless save started): it runs on main instead of being dropped. */
TEST_F(MaintTest, PostedExpireRunsOnMainWhenLanesAreOff) {
    server.active_expire_enabled = 1;
    server.active_expire_effort = 1;
    server.mstime = mstime();
    serverDb *db = server.db[0];
    for (int i = 0; i < 16; i++) {
        sds key = sdscatprintf(sdsempty(), "ttl:%d", i);
        robj *o = objectSetKeyAndExpire(createStringObject("v", 1), key, server.mstime + 1000000);
        int didx = i % 4;
        kvstoreHashtableAdd(db->keys, didx, o);
        kvstoreHashtableAdd(db->expires, didx, o);
        sdsfree(key);
    }
    db->expiry[KEYS].avg_ttl = 0;

    /* Lanes are off here (no staged workers): the posted cycle samples the keys on main, which sets the
     * db's average TTL. */
    maintRequest(MAINT_JOB_EXPIRE, 25000);
    maintRun();
    ASSERT_GT(db->expiry[KEYS].avg_ttl, 0);

    for (int i = 0; i < 16; i++) {
        sds key = sdscatprintf(sdsempty(), "ttl:%d", i);
        kvstoreHashtableDelete(db->expires, i % 4, key);
        kvstoreHashtableDelete(db->keys, i % 4, key);
        sdsfree(key);
    }
}
