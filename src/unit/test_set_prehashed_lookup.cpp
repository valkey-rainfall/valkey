/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Tests for the SET-specific prehashed lookup path (value-gate experiment).
 *
 * Validates:
 *   1. Hash equivalence: sdsHashConfigurableSeed(sds) ==
 *      genHashFunctionConfigurableSeed(buf, len) for the same bytes.
 *   2. setLookupCompare equivalence: stringRef-vs-SDS comparator matches
 *      dictSdsKeyCompare for matching/non-matching pairs including prefix,
 *      length-only mismatches, binary keys with embedded NUL.
 *   3. Prehashed find equivalence: hashtableFindWithHashAndCompare returns
 *      the same result as hashtableFind for hits and misses, including
 *      during active rehash.
 *
 * These tests run in the serverless unit test environment — no valkey-server
 * or Tcl harness is needed.
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
#include "server.h"
#include "sds.h"
#include "zmalloc.h"

/* Functions declared/defined in server.c */
uint64_t genHashFunctionConfigurableSeed(const char *buf, size_t len);
uint64_t sdsHashConfigurableSeed(const void *key);
int dictSdsKeyCompare(const void *key1, const void *key2);
void getRandomBytes(unsigned char *p, size_t len);
}

/* ---------- 1. Hash equivalence ----------------------------------------- */

/* This test validates that the two ways of computing the configurable-seed
 * hash produce identical results for the same byte content:
 *   sdsHashConfigurableSeed(sds_key) == genHashFunctionConfigurableSeed(buf, len)
 * This is the hash provenance guarantee for the SET prehashed lookup. */

class HashEquivalenceTest : public ::testing::Test {
  protected:
    void SetUp() override {
        /* Both functions read the same static configurable_hash_seed in
         * server.c.  In the unit-test environment it's zero-initialized
         * by default — that's fine for equivalence testing. */
    }
};

TEST_F(HashEquivalenceTest, EmptyKey) {
    sds key = sdsempty();
    uint64_t h1 = sdsHashConfigurableSeed(key);
    uint64_t h2 = genHashFunctionConfigurableSeed(key, sdslen(key));
    ASSERT_EQ(h1, h2);
    sdsfree(key);
}

TEST_F(HashEquivalenceTest, SingleByte) {
    sds key = sdsnewlen("x", 1);
    uint64_t h1 = sdsHashConfigurableSeed(key);
    uint64_t h2 = genHashFunctionConfigurableSeed(key, sdslen(key));
    ASSERT_EQ(h1, h2);
    sdsfree(key);
}

TEST_F(HashEquivalenceTest, EmbeddedNUL) {
    const char data[] = "a\0b\0c";
    sds key = sdsnewlen(data, 5);
    uint64_t h1 = sdsHashConfigurableSeed(key);
    uint64_t h2 = genHashFunctionConfigurableSeed(key, sdslen(key));
    ASSERT_EQ(h1, h2);
    sdsfree(key);
}

TEST_F(HashEquivalenceTest, LongKey) {
    /* 1000-byte key */
    char buf[1000];
    for (int i = 0; i < 1000; i++) buf[i] = (char)(i & 0xFF);
    sds key = sdsnewlen(buf, 1000);
    uint64_t h1 = sdsHashConfigurableSeed(key);
    uint64_t h2 = genHashFunctionConfigurableSeed(key, sdslen(key));
    ASSERT_EQ(h1, h2);
    sdsfree(key);
}

TEST_F(HashEquivalenceTest, RandomKeys) {
    /* Property test: 1000 random keys */
    for (int i = 0; i < 1000; i++) {
        size_t len = (rand() % 256) + 1;
        unsigned char *data = (unsigned char *)zmalloc(len);
        getRandomBytes(data, len);
        sds key = sdsnewlen(data, len);
        uint64_t h1 = sdsHashConfigurableSeed(key);
        uint64_t h2 = genHashFunctionConfigurableSeed(key, sdslen(key));
        ASSERT_EQ(h1, h2) << "Mismatch at key index " << i << " len=" << len;
        sdsfree(key);
        zfree(data);
    }
}

/* ---------- 2. Compare equivalence via prehashed API -------------------- */

/* We test the stringRef-vs-SDS comparator (identical to setLookupCompare in
 * db.c) through the hashtable API: insert SDS entries and look them up with
 * stringRef views, verifying that find/miss results match the homogeneous
 * hashtableFind exactly. */

class CompareEquivalenceTest : public ::testing::Test {
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

    /* Matches setLookupCompare in db.c exactly: (entry_key=SDS, search_key=stringRef*).
     * Returns non-zero on match. */
    static int stringref_sds_compare(const void *entry_key, const void *search_key) {
        const sds stored = (sds)entry_key;
        const stringRef *ref = (const stringRef *)search_key;
        size_t stored_len = sdslen(stored);
        if (stored_len != ref->len) return 0;
        return memcmp(stored, ref->buf, stored_len) == 0;
    }

    /* Hash function matching the type's hashFunction but taking a stringRef. */
    static uint64_t hash_stringref(const stringRef *ref) {
        return hashtableGenHashFunction(ref->buf, ref->len);
    }

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
};

TEST_F(CompareEquivalenceTest, ExactMatch) {
    insert("hello", 5);
    stringRef ref = {"hello", 5};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, stringref_sds_compare, &found));
    ASSERT_EQ(memcmp((sds)found, "hello", 5), 0);
}

TEST_F(CompareEquivalenceTest, PrefixMismatch) {
    insert("hello", 5);
    /* "hell" is a prefix — must NOT match */
    stringRef ref = {"hell", 4};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;
    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, stringref_sds_compare, &found));
}

TEST_F(CompareEquivalenceTest, LengthOnlyMismatch) {
    insert("abc", 3);
    /* Same prefix bytes but different length — must NOT match */
    stringRef ref = {"abcd", 4};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;
    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, stringref_sds_compare, &found));
}

TEST_F(CompareEquivalenceTest, BinaryKeyWithEmbeddedNUL) {
    const char data[] = "a\0b";
    insert(data, 3);
    insert("a", 1);

    /* Must find the 3-byte binary key, not the 1-byte "a" */
    stringRef ref3 = {data, 3};
    uint64_t hash3 = hash_stringref(&ref3);
    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash3, &ref3, stringref_sds_compare, &found));
    ASSERT_EQ(sdslen((sds)found), 3u);

    /* Must find the 1-byte "a" */
    stringRef ref1 = {"a", 1};
    uint64_t hash1 = hash_stringref(&ref1);
    found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash1, &ref1, stringref_sds_compare, &found));
    ASSERT_EQ(sdslen((sds)found), 1u);
}

TEST_F(CompareEquivalenceTest, CrossCheckWithDictSdsKeyCompare) {
    /* Insert 200 keys and verify that stringref_sds_compare and
     * dictSdsKeyCompare agree on every match/miss pair. */
    const int N = 200;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "cross-%06d", i);
        insert(buf, strlen(buf));
    }

    for (int i = 0; i < N + 50; i++) {
        snprintf(buf, sizeof(buf), "cross-%06d", i);
        size_t len = strlen(buf);

        /* dictSdsKeyCompare uses SDS */
        sds probe = sdsnewlen(buf, len);
        void *homo_found = NULL;
        bool homo_hit = hashtableFind(ht, probe, &homo_found);

        /* stringref compare uses stringRef */
        stringRef ref = {buf, len};
        uint64_t hash = hash_stringref(&ref);
        void *hetero_found = NULL;
        bool hetero_hit = hashtableFindWithHashAndCompare(ht, hash, &ref,
                                                          stringref_sds_compare, &hetero_found);

        ASSERT_EQ(homo_hit, hetero_hit) << "Mismatch at i=" << i;
        if (homo_hit) {
            ASSERT_EQ(homo_found, hetero_found) << "Different entry at i=" << i;
        }

        sdsfree(probe);
    }
}

/* ---------- 3. Active rehash equivalence -------------------------------- */

TEST_F(CompareEquivalenceTest, ActiveRehashEquivalence) {
    const int N = 5000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "rehash-eq-%06d", i);
        insert(buf, strlen(buf));
    }

    /* Trigger rehash */
    ASSERT_TRUE(hashtableExpand(ht, N * 4));
    ASSERT_TRUE(hashtableIsRehashing(ht));
    /* Partial rehash — ensure two tables are active */
    hashtableRehashMicroseconds(ht, 1);

    /* Spot-check 100 random keys during two-table state */
    for (int i = 0; i < 100; i++) {
        int idx = rand() % N;
        snprintf(buf, sizeof(buf), "rehash-eq-%06d", idx);
        size_t len = strlen(buf);

        sds probe = sdsnewlen(buf, len);
        void *homo_found = NULL;
        bool homo_hit = hashtableFind(ht, probe, &homo_found);

        stringRef ref = {buf, len};
        uint64_t hash = hash_stringref(&ref);
        void *hetero_found = NULL;
        bool hetero_hit = hashtableFindWithHashAndCompare(ht, hash, &ref,
                                                          stringref_sds_compare, &hetero_found);

        ASSERT_EQ(homo_hit, hetero_hit);
        if (homo_hit) {
            ASSERT_EQ(homo_found, hetero_found);
        }

        sdsfree(probe);
    }
}
