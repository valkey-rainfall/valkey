/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Standalone keyspace shards (keyspace-shard-bits): every bit of a key must reach its table. */

#include "generated_wrappers.hpp"

#include <cstdlib>
#include <cstring>

extern "C" {
#include "server.h"
}

class KeyspaceShardTest : public ::testing::Test {
  protected:
    int saved_cluster_enabled;
    int saved_shard_bits;

    void SetUp() override {
        saved_cluster_enabled = server.cluster_enabled;
        saved_shard_bits = server.keyspace_shard_bits;
        server.cluster_enabled = 0;
        server.keyspace_shard_bits = 8;
    }

    void TearDown() override {
        server.cluster_enabled = saved_cluster_enabled;
        server.keyspace_shard_bits = saved_shard_bits;
    }
};

static int shardOf(const char *buf, size_t len) {
    sds key = sdsnewlen(buf, len);
    int shard = getKVStoreIndexForKey(key);
    sdsfree(key);
    return shard;
}

TEST_F(KeyspaceShardTest, ShortKeysDifferingInOneCharacterSpread) {
    int a = shardOf("key:A", 5);
    int b = shardOf("key:a", 5);
    int c = shardOf("key:!", 5);
    ASSERT_FALSE(a == b && b == c);
}

/* For every key length up to three words and every bit of the key, flipping that bit moves some keys
 * to another table. */
TEST_F(KeyspaceShardTest, EveryKeyBitReachesTheTable) {
    srand(12345);
    char buf[24];
    for (size_t len = 1; len <= sizeof(buf); len++) {
        for (size_t bit = 0; bit < len * 8; bit++) {
            int moved = 0;
            for (int k = 0; k < 64 && !moved; k++) {
                for (size_t i = 0; i < len; i++) buf[i] = (char)(rand() & 0xff);
                int before = shardOf(buf, len);
                buf[bit / 8] ^= (char)(1 << (bit % 8));
                if (shardOf(buf, len) != before) moved = 1;
            }
            ASSERT_TRUE(moved) << "key length " << len << ", bit " << bit << " never changes the table";
        }
    }
}
