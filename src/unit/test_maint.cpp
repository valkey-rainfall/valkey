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
