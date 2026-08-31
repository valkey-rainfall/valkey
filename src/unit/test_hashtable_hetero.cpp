/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Heterogeneous lookup API tests.
 *
 * Validates:
 *   - hashtableFindWithHashAndCompare: hit, miss, binary keys, embedded NUL
 *   - hashtableFindRefWithHashAndCompare: pointer-to-slot return, in-place replacement
 *   - hashtableHeteroFindInit + Step + GetResult: full lifecycle via separate state
 *   - Active rehash / two-table lookup for all three APIs
 *   - Comparator engagement: custom cmp actually invoked (counter proof)
 *   - No SDS materialization: zmalloc_used_memory unchanged across lookups
 *   - Deletion via existing hashtablePop after heterogeneous FindRef
 *   - Existing upstream APIs unchanged (regression)
 *   - Batch incremental find (parallel prefetch pattern)
 *
 * The synthetic type stores SDS entries but lookups use stringRef-like views
 * (a raw {buf,len} pair) with a custom comparator — exactly the pattern
 * needed by borrowed SET and IO-thread prefetch.
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
#include "server.h"  /* stringRef */
#include "zmalloc.h"

void getRandomBytes(unsigned char *p, size_t len);
}

/* ---------- Synthetic type: entries are SDS, lookups are stringRef -------- */

static const void *sds_getkey(const void *entry) {
    return entry;
}

static uint64_t sds_hash(const void *key) {
    sds s = (sds)key;
    return hashtableGenHashFunction(s, sdslen(s));
}

static int sds_keycmp(const void *k1, const void *k2) {
    sds a = (sds)k1;
    sds b = (sds)k2;
    size_t la = sdslen(a), lb = sdslen(b);
    if (la != lb) return 0;
    return memcmp(a, b, la) == 0;
}

static void sds_destructor(void *entry) {
    sdsfree((sds)entry);
}

/* Heterogeneous compare: entry_key is SDS, search_key is stringRef*.
 * Returns non-zero on match — same convention as keyCompare. */
static int hetero_cmp_counter = 0;

static int hetero_compare(const void *entry_key, const void *search_key) {
    hetero_cmp_counter++;
    sds s = (sds)entry_key;
    const stringRef *ref = (const stringRef *)search_key;
    size_t slen = sdslen(s);
    if (slen != ref->len) return 0;
    return memcmp(s, ref->buf, slen) == 0;
}

static uint64_t hash_stringref(const stringRef *ref) {
    return hashtableGenHashFunction(ref->buf, ref->len);
}

class HeteroLookupTest : public ::testing::Test {
  protected:
    hashtableType type;
    hashtable *ht;

    void SetUp() override {
        monotonicInit();

        uint8_t seed[16];
        getRandomBytes(seed, sizeof(seed));
        hashtableSetHashFunctionSeed(seed);

        memset(&type, 0, sizeof(type));
        type.entryGetKey = sds_getkey;
        type.hashFunction = sds_hash;
        type.keyCompare = sds_keycmp;
        type.entryDestructor = sds_destructor;

        ht = hashtableCreate(&type);
        hetero_cmp_counter = 0;
    }

    void TearDown() override {
        hashtableRelease(ht);
    }

    void insert(const char *data, size_t len) {
        sds s = sdsnewlen(data, len);
        ASSERT_TRUE(hashtableAdd(ht, s));
    }

    void insert_str(const char *str) {
        insert(str, strlen(str));
    }
};

/* ---------- FindWithHashAndCompare --------------------------------------- */

TEST_F(HeteroLookupTest, Find_Hit) {
    insert_str("hello");
    insert_str("world");

    stringRef ref = {"hello", 5};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;
    hetero_cmp_counter = 0;

    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    ASSERT_NE(found, (void *)NULL);
    sds result = (sds)found;
    ASSERT_EQ(sdslen(result), 5u);
    ASSERT_EQ(memcmp(result, "hello", 5), 0);
    ASSERT_GT(hetero_cmp_counter, 0);
}

TEST_F(HeteroLookupTest, Find_Miss) {
    insert_str("hello");

    stringRef ref = {"missing", 7};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;

    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    ASSERT_EQ(found, (void *)NULL);
}

TEST_F(HeteroLookupTest, Find_EmptyTable) {
    stringRef ref = {"anything", 8};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;

    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
}

TEST_F(HeteroLookupTest, Find_BinaryKey) {
    /* Key with embedded NUL: "a\0b" (3 bytes) */
    insert("a\0b", 3);
    insert("a", 1);

    const char data[] = "a\0b";
    stringRef ref = {data, 3};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;

    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    sds result = (sds)found;
    ASSERT_EQ(sdslen(result), 3u);
    ASSERT_EQ(memcmp(result, "a\0b", 3), 0);

    /* 1-byte key must not confuse with the 3-byte one. */
    stringRef ref1 = {"a", 1};
    uint64_t hash1 = hash_stringref(&ref1);
    found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash1, &ref1, hetero_compare, &found));
    result = (sds)found;
    ASSERT_EQ(sdslen(result), 1u);
}

/* ---------- FindRefWithHashAndCompare ------------------------------------ */

TEST_F(HeteroLookupTest, FindRef_Hit) {
    insert_str("reftest");

    stringRef ref = {"reftest", 7};
    uint64_t hash = hash_stringref(&ref);

    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    ASSERT_NE(slot, (void **)NULL);
    sds result = (sds)*slot;
    ASSERT_EQ(sdslen(result), 7u);
    ASSERT_EQ(memcmp(result, "reftest", 7), 0);
}

TEST_F(HeteroLookupTest, FindRef_Miss) {
    insert_str("reftest");

    stringRef ref = {"nope", 4};
    uint64_t hash = hash_stringref(&ref);

    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    ASSERT_EQ(slot, (void **)NULL);
}

TEST_F(HeteroLookupTest, FindRef_ReplaceInPlace) {
    insert_str("replace-me");

    stringRef ref = {"replace-me", 10};
    uint64_t hash = hash_stringref(&ref);

    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    ASSERT_NE(slot, (void **)NULL);

    sds old = (sds)*slot;
    sds replacement = sdsnew("replace-me");
    *slot = replacement;
    sdsfree(old);

    /* Verify the replacement is in the table. */
    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    ASSERT_EQ(found, replacement);
}

/* ---------- Hetero incremental find -------------------------------------- */

TEST_F(HeteroLookupTest, HeteroIncr_Hit) {
    insert_str("incr-find");

    stringRef ref = {"incr-find", 9};
    uint64_t hash = hash_stringref(&ref);
    hetero_cmp_counter = 0;

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);

    while (hashtableHeteroFindStep(&state)) {}

    void *found = NULL;
    ASSERT_TRUE(hashtableHeteroFindGetResult(&state, &found));
    sds result = (sds)found;
    ASSERT_EQ(sdslen(result), 9u);
    ASSERT_EQ(memcmp(result, "incr-find", 9), 0);
    ASSERT_GT(hetero_cmp_counter, 0);
}

TEST_F(HeteroLookupTest, HeteroIncr_Miss) {
    insert_str("exists");

    stringRef ref = {"ghost", 5};
    uint64_t hash = hash_stringref(&ref);

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);

    while (hashtableHeteroFindStep(&state)) {}

    void *found = NULL;
    ASSERT_FALSE(hashtableHeteroFindGetResult(&state, &found));
}

TEST_F(HeteroLookupTest, HeteroIncr_EmptyTable) {
    stringRef ref = {"anything", 8};
    uint64_t hash = hash_stringref(&ref);

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);

    ASSERT_FALSE(hashtableHeteroFindStep(&state));

    void *found = NULL;
    ASSERT_FALSE(hashtableHeteroFindGetResult(&state, &found));
}

TEST_F(HeteroLookupTest, HeteroIncr_BinaryKey) {
    const char data[] = "x\0y\0z";
    insert(data, 5);

    stringRef ref = {data, 5};
    uint64_t hash = hash_stringref(&ref);

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);

    while (hashtableHeteroFindStep(&state)) {}

    void *found = NULL;
    ASSERT_TRUE(hashtableHeteroFindGetResult(&state, &found));
    sds result = (sds)found;
    ASSERT_EQ(sdslen(result), 5u);
    ASSERT_EQ(memcmp(result, data, 5), 0);
}

/* ---------- Active rehash / two-table lookup ----------------------------- */

TEST_F(HeteroLookupTest, Find_DuringRehash) {
    const int N = 10000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "key-%06d", i);
        insert_str(buf);
    }

    ASSERT_TRUE(hashtableExpand(ht, N * 4));
    ASSERT_TRUE(hashtableIsRehashing(ht));
    hashtableRehashMicroseconds(ht, 1);

    snprintf(buf, sizeof(buf), "key-%06d", N / 2);
    stringRef ref = {buf, strlen(buf)};
    uint64_t hash = hash_stringref(&ref);
    void *found = NULL;

    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    ASSERT_EQ(strcmp((const char *)(sds)found, buf), 0);

    /* Miss must check both tables. */
    stringRef miss = {"nonexistent-rehash", 18};
    uint64_t miss_hash = hash_stringref(&miss);
    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, miss_hash, &miss, hetero_compare, NULL));
}

TEST_F(HeteroLookupTest, FindRef_DuringRehash) {
    const int N = 10000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "rehash-ref-%06d", i);
        insert_str(buf);
    }

    ASSERT_TRUE(hashtableExpand(ht, N * 4));
    ASSERT_TRUE(hashtableIsRehashing(ht));
    hashtableRehashMicroseconds(ht, 1);

    snprintf(buf, sizeof(buf), "rehash-ref-%06d", N / 3);
    stringRef ref = {buf, strlen(buf)};
    uint64_t hash = hash_stringref(&ref);

    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    ASSERT_NE(slot, (void **)NULL);
    ASSERT_EQ(strcmp((const char *)(sds)*slot, buf), 0);
}

TEST_F(HeteroLookupTest, HeteroIncr_DuringRehash) {
    const int N = 10000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "incr-rehash-%06d", i);
        insert_str(buf);
    }

    ASSERT_TRUE(hashtableExpand(ht, N * 4));
    ASSERT_TRUE(hashtableIsRehashing(ht));
    hashtableRehashMicroseconds(ht, 1);

    snprintf(buf, sizeof(buf), "incr-rehash-%06d", N - 1);
    stringRef ref = {buf, strlen(buf)};
    uint64_t hash = hash_stringref(&ref);

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);
    while (hashtableHeteroFindStep(&state)) {}

    void *found = NULL;
    ASSERT_TRUE(hashtableHeteroFindGetResult(&state, &found));
    ASSERT_EQ(strcmp((const char *)(sds)found, buf), 0);
}

/* ---------- Comparator engagement ---------------------------------------- */

TEST_F(HeteroLookupTest, ComparatorEngagement_Find) {
    insert_str("engage-a");

    stringRef ref = {"engage-a", 8};
    uint64_t hash = hash_stringref(&ref);
    hetero_cmp_counter = 0;

    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found));
    /* The custom comparator must have been called — this is the actual
     * engagement proof.  A counter of 0 would mean the API short-circuited
     * or used the type's keyCompare instead. */
    ASSERT_GT(hetero_cmp_counter, 0);
    ASSERT_NE(found, (void *)NULL);
}

TEST_F(HeteroLookupTest, ComparatorEngagement_HeteroIncr) {
    insert_str("incr-engage");

    stringRef ref = {"incr-engage", 11};
    uint64_t hash = hash_stringref(&ref);
    hetero_cmp_counter = 0;

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);
    while (hashtableHeteroFindStep(&state)) {}

    void *found = NULL;
    ASSERT_TRUE(hashtableHeteroFindGetResult(&state, &found));
    ASSERT_GT(hetero_cmp_counter, 0);
    ASSERT_NE(found, (void *)NULL);
}

/* ---------- No SDS materialization proof --------------------------------- */

TEST_F(HeteroLookupTest, NoMaterialization_Find) {
    insert_str("no-alloc-test");

    const char raw[] = "no-alloc-test";
    stringRef ref = {raw, sizeof(raw) - 1};
    uint64_t hash = hash_stringref(&ref);

    size_t mem_before = zmalloc_used_memory();
    void *found = NULL;
    hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found);
    size_t mem_after = zmalloc_used_memory();

    ASSERT_TRUE(found != NULL);
    ASSERT_LE(mem_after, mem_before);
}

TEST_F(HeteroLookupTest, NoMaterialization_FindRef) {
    insert_str("no-alloc-ref");

    const char raw[] = "no-alloc-ref";
    stringRef ref = {raw, sizeof(raw) - 1};
    uint64_t hash = hash_stringref(&ref);

    size_t mem_before = zmalloc_used_memory();
    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    size_t mem_after = zmalloc_used_memory();

    ASSERT_NE(slot, (void **)NULL);
    ASSERT_LE(mem_after, mem_before);
}

TEST_F(HeteroLookupTest, NoMaterialization_HeteroIncr) {
    insert_str("no-alloc-incr");

    const char raw[] = "no-alloc-incr";
    stringRef ref = {raw, sizeof(raw) - 1};
    uint64_t hash = hash_stringref(&ref);

    size_t mem_before = zmalloc_used_memory();

    hashtableHeteroFindState state;
    hashtableHeteroFindInit(&state, ht, hash, &ref, hetero_compare);
    while (hashtableHeteroFindStep(&state)) {}
    void *found = NULL;
    hashtableHeteroFindGetResult(&state, &found);

    size_t mem_after = zmalloc_used_memory();

    ASSERT_TRUE(found != NULL);
    ASSERT_LE(mem_after, mem_before);
}

/* ---------- Deletion via FindRef + existing hashtablePop ------------------ */

TEST_F(HeteroLookupTest, DeletionAfterFindRef) {
    insert_str("del-target");
    insert_str("keep-this");

    ASSERT_EQ(hashtableSize(ht), 2u);

    stringRef ref = {"del-target", 10};
    uint64_t hash = hash_stringref(&ref);
    void **slot = hashtableFindRefWithHashAndCompare(ht, hash, &ref, hetero_compare);
    ASSERT_NE(slot, (void **)NULL);

    sds found_key = (sds)*slot;
    ASSERT_TRUE(hashtableDelete(ht, found_key));
    ASSERT_EQ(hashtableSize(ht), 1u);

    ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, NULL));

    stringRef keep = {"keep-this", 9};
    uint64_t keep_hash = hash_stringref(&keep);
    void *found = NULL;
    ASSERT_TRUE(hashtableFindWithHashAndCompare(ht, keep_hash, &keep, hetero_compare, &found));
    ASSERT_EQ(sdslen((sds)found), 9u);
}

/* ---------- Existing upstream APIs unchanged (regression) ---------------- */

TEST_F(HeteroLookupTest, ExistingAPIs_Unchanged) {
    insert_str("homo-test");

    sds key = sdsnew("homo-test");
    void *found = NULL;
    ASSERT_TRUE(hashtableFind(ht, key, &found));
    ASSERT_EQ(sdslen((sds)found), 9u);

    void **slot = hashtableFindRef(ht, key);
    ASSERT_NE(slot, (void **)NULL);
    ASSERT_EQ(found, *slot);

    /* Existing 5-word incremental find unchanged. */
    hashtableIncrementalFindState state;
    hashtableIncrementalFindInit(&state, ht, key);
    while (hashtableIncrementalFindStep(&state)) {}
    void *incr_found = NULL;
    ASSERT_TRUE(hashtableIncrementalFindGetResult(&state, &incr_found));
    ASSERT_EQ(incr_found, found);

    sdsfree(key);
}

/* ---------- Opaque state sizes ------------------------------------------- */

TEST_F(HeteroLookupTest, StateSizes) {
    /* Existing state is exactly 5 words (unchanged from upstream). */
    ASSERT_EQ(sizeof(hashtableIncrementalFindState), 5 * sizeof(uint64_t));
    /* Hetero state is 6 words — one word for the custom_cmp pointer. */
    ASSERT_EQ(sizeof(hashtableHeteroFindState), 6 * sizeof(uint64_t));
}

/* ---------- Batch hetero incremental find (parallel prefetch pattern) ---- */

TEST_F(HeteroLookupTest, BatchHeteroIncrFind) {
    const int N = 200;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "batch-%06d", i);
        insert_str(buf);
    }

    const int BATCH = 8;
    hashtableHeteroFindState states[8];
    stringRef refs[8];
    char keys[8][64];
    bool done[8] = {};

    for (int i = 0; i < BATCH; i++) {
        snprintf(keys[i], sizeof(keys[i]), "batch-%06d", i * 25);
        refs[i] = {keys[i], strlen(keys[i])};
        uint64_t hash = hash_stringref(&refs[i]);
        hashtableHeteroFindInit(&states[i], ht, hash, &refs[i], hetero_compare);
    }

    int remaining = BATCH;
    while (remaining > 0) {
        for (int i = 0; i < BATCH; i++) {
            if (done[i]) continue;
            if (!hashtableHeteroFindStep(&states[i])) {
                done[i] = true;
                remaining--;
            }
        }
    }

    for (int i = 0; i < BATCH; i++) {
        void *found = NULL;
        ASSERT_TRUE(hashtableHeteroFindGetResult(&states[i], &found));
        ASSERT_EQ(strcmp((const char *)(sds)found, keys[i]), 0);
    }
}

/* ---------- Stress (moderate size — sufficient for hash collision paths) -- */

TEST_F(HeteroLookupTest, StressLookup) {
    const int N = 2000;
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "stress-%06d", i);
        insert_str(buf);
    }

    int found_count = 0;
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "stress-%06d", i);
        stringRef ref = {buf, strlen(buf)};
        uint64_t hash = hash_stringref(&ref);
        void *found = NULL;
        if (hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, &found)) {
            found_count++;
            ASSERT_EQ(strcmp((const char *)(sds)found, buf), 0);
        }
    }
    ASSERT_EQ(found_count, N);

    /* Verify misses. */
    for (int i = N; i < N + 50; i++) {
        snprintf(buf, sizeof(buf), "stress-%06d", i);
        stringRef ref = {buf, strlen(buf)};
        uint64_t hash = hash_stringref(&ref);
        ASSERT_FALSE(hashtableFindWithHashAndCompare(ht, hash, &ref, hetero_compare, NULL));
    }
}
