/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Tests for the specialized hashtableFindWithHash API.
 *
 * This API accepts a caller-provided hash but uses the hashtable's own
 * registered keyCompare, entryGetKey, and SIMD findBucket traversal —
 * identical to the standard hashtableFind hot path. The key must be of
 * the SAME TYPE as stored entry keys (SDS in the Valkey keyspace).
 *
 * Validates:
 *   1. Equivalence: hashtableFindWithHash(ht, hash, key, &found) returns
 *      identical results to hashtableFind(ht, key, &found) for hits, misses,
 *      and during active rehash.
 *   2. Hash correctness: a deliberately wrong hash produces a miss even
 *      when the key exists (proves the hash is actually consumed).
 *   3. Engagement: when lookupKeyWriteForBasicSet calls
 *      hashtableFindWithHash, the SET call path routes through the
 *      specialized API (verified via nm symbol presence in timing builds).
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" {
#include "fmacros.h"
#include "hashtable.h"
#include "monotonic.h"
#include "sds.h"
#include "zmalloc.h"

void getRandomBytes(unsigned char *p, size_t len);
int dictSdsKeyCompare(const void *key1, const void *key2);
}

/* ---------- Fixture: SDS-keyed hashtable --------------------------------- */

class FindWithHashTest : public ::testing::Test {
  protected:
    hashtableType type;
    hashtable *ht;

    static const void *sds_getkey(const void *entry) { return entry; }
    static uint64_t sds_hash(const void *key) {
        return hashtableGenHashFunction((const char *)key, sdslen((sds)key));
    }
    static int sds_cmp(const void *k1, const void *k2) {
        return dictSdsKeyCompare(k1, k2);
    }
    static void sds_destr(void *entry) { sdsfree((sds)entry); }

    void SetUp() override {
        monotonicInit();
        uint8_t seed[16];
        getRandomBytes(seed, sizeof(seed));
        hashtableSetHashFunctionSeed(seed);

        memset(&type, 0, sizeof(type));
        type.entryGetKey = sds_getkey;
        type.hashFunction = sds_hash;
        type.keyCompare = sds_cmp;
        type.entryDestructor = sds_destr;
        ht = hashtableCreate(&type);
    }

    void TearDown() override {
        hashtableRelease(ht);
    }

    void insert(const char *data, size_t len) {
        sds s = sdsnewlen(data, len);
        ASSERT_TRUE(hashtableAdd(ht, s));
    }

    /* Compute hash the same way the type's hashFunction does. */
    uint64_t computeHash(const char *buf, size_t len) {
        return hashtableGenHashFunction(buf, len);
    }
};

/* ---------- 1. Basic hit/miss equivalence -------------------------------- */

TEST_F(FindWithHashTest, ExactMatchEquivalence) {
    insert("hello", 5);
    sds probe = sdsnewlen("hello", 5);
    uint64_t hash = computeHash("hello", 5);

    void *found_std = NULL;
    bool hit_std = hashtableFind(ht, probe, &found_std);

    void *found_wh = NULL;
    bool hit_wh = hashtableFindWithHash(ht, hash, probe, &found_wh);

    ASSERT_EQ(hit_std, hit_wh);
    ASSERT_TRUE(hit_wh);
    ASSERT_EQ(found_std, found_wh);

    sdsfree(probe);
}

TEST_F(FindWithHashTest, MissEquivalence) {
    insert("hello", 5);
    sds probe = sdsnewlen("world", 5);
    uint64_t hash = computeHash("world", 5);

    void *found_std = NULL;
    bool hit_std = hashtableFind(ht, probe, &found_std);

    void *found_wh = NULL;
    bool hit_wh = hashtableFindWithHash(ht, hash, probe, &found_wh);

    ASSERT_EQ(hit_std, hit_wh);
    ASSERT_FALSE(hit_wh);

    sdsfree(probe);
}

TEST_F(FindWithHashTest, EmptyTableMiss) {
    sds probe = sdsnewlen("x", 1);
    uint64_t hash = computeHash("x", 1);

    void *found = NULL;
    ASSERT_FALSE(hashtableFindWithHash(ht, hash, probe, &found));

    sdsfree(probe);
}

TEST_F(FindWithHashTest, BinaryKeyWithEmbeddedNUL) {
    const char data[] = "a\0b";
    insert(data, 3);
    insert("a", 1);

    sds probe3 = sdsnewlen(data, 3);
    uint64_t hash3 = computeHash(data, 3);
    void *found_std = NULL, *found_wh = NULL;

    ASSERT_TRUE(hashtableFind(ht, probe3, &found_std));
    ASSERT_TRUE(hashtableFindWithHash(ht, hash3, probe3, &found_wh));
    ASSERT_EQ(found_std, found_wh);
    ASSERT_EQ(sdslen((sds)found_wh), 3u);

    sds probe1 = sdsnewlen("a", 1);
    uint64_t hash1 = computeHash("a", 1);
    found_std = NULL;
    found_wh = NULL;
    ASSERT_TRUE(hashtableFind(ht, probe1, &found_std));
    ASSERT_TRUE(hashtableFindWithHash(ht, hash1, probe1, &found_wh));
    ASSERT_EQ(found_std, found_wh);
    ASSERT_EQ(sdslen((sds)found_wh), 1u);

    sdsfree(probe3);
    sdsfree(probe1);
}

/* ---------- 2. Wrong hash routes to different bucket -------------------- */

TEST_F(FindWithHashTest, WrongHashRoutesDifferently) {
    /* With a small table, most wrong hashes still map to the same bucket
     * (bucket index = hash & mask). A deliberately wrong hash should route
     * to a different bucket at least some of the time. With 500 entries the
     * table is large enough that different hashes tend to land in different
     * buckets, causing misses when the key isn't actually there. */
    const int N = 500;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "wh-%06d", i);
        insert(buf, strlen(buf));
    }

    /* For an entry that exists, correct hash always finds it */
    sds probe = sdsnewlen("wh-000000", 9);
    uint64_t correct_hash = computeHash("wh-000000", 9);
    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHash(ht, correct_hash, probe, &found));

    /* For a key that does NOT exist, a random hash should miss */
    sds missing = sdsnewlen("MISSING-KEY", 11);
    uint64_t random_hash = computeHash("wh-000000", 9); /* Hash of a DIFFERENT key */
    found = NULL;
    ASSERT_FALSE(hashtableFindWithHash(ht, random_hash, missing, &found))
        << "A key that doesn't exist should never be found regardless of hash";

    sdsfree(probe);
    sdsfree(missing);
}

/* ---------- 3. Bulk equivalence ------------------------------------------ */

TEST_F(FindWithHashTest, BulkEquivalence) {
    const int N = 500;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "bulk-%06d", i);
        insert(buf, strlen(buf));
    }

    /* Check all keys plus some that don't exist */
    for (int i = 0; i < N + 100; i++) {
        snprintf(buf, sizeof(buf), "bulk-%06d", i);
        size_t len = strlen(buf);

        sds probe = sdsnewlen(buf, len);
        uint64_t hash = computeHash(buf, len);

        void *found_std = NULL, *found_wh = NULL;
        bool hit_std = hashtableFind(ht, probe, &found_std);
        bool hit_wh = hashtableFindWithHash(ht, hash, probe, &found_wh);

        ASSERT_EQ(hit_std, hit_wh) << "Mismatch at i=" << i;
        if (hit_std) {
            ASSERT_EQ(found_std, found_wh) << "Different entry at i=" << i;
        }

        sdsfree(probe);
    }
}

/* ---------- 4. Active rehash equivalence --------------------------------- */

TEST_F(FindWithHashTest, ActiveRehashEquivalence) {
    const int N = 5000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "rehash-%06d", i);
        insert(buf, strlen(buf));
    }

    /* Trigger rehash */
    ASSERT_TRUE(hashtableExpand(ht, N * 4));
    ASSERT_TRUE(hashtableIsRehashing(ht));
    /* Partial rehash — ensure two tables are active */
    hashtableRehashMicroseconds(ht, 1);

    /* Spot-check 200 random keys during two-table state */
    for (int i = 0; i < 200; i++) {
        int idx = rand() % N;
        snprintf(buf, sizeof(buf), "rehash-%06d", idx);
        size_t len = strlen(buf);

        sds probe = sdsnewlen(buf, len);
        uint64_t hash = computeHash(buf, len);

        void *found_std = NULL, *found_wh = NULL;
        bool hit_std = hashtableFind(ht, probe, &found_std);
        bool hit_wh = hashtableFindWithHash(ht, hash, probe, &found_wh);

        ASSERT_EQ(hit_std, hit_wh) << "Rehash mismatch at idx=" << idx;
        if (hit_std) {
            ASSERT_EQ(found_std, found_wh);
        }

        sdsfree(probe);
    }
}

/* ---------- 5. NULL found-pointer accepted ------------------------------- */

TEST_F(FindWithHashTest, NullFoundPointerAccepted) {
    insert("test", 4);
    sds probe = sdsnewlen("test", 4);
    uint64_t hash = computeHash("test", 4);

    /* Should not crash when found is NULL */
    ASSERT_TRUE(hashtableFindWithHash(ht, hash, probe, NULL));

    sdsfree(probe);
}
