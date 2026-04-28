/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "vstr.h"
#include "sds.h"
}

class VstrTest : public ::testing::Test {};

/* --- Borrowed vstr --- */

TEST_F(VstrTest, BorrowedBasic) {
    const char *raw = "hello";
    vstr v;
    vstrInitBorrowed(&v, raw, 5);

    ASSERT_EQ(vstrType(&v), VSTR_BORROWED);
    ASSERT_TRUE(vstrIsBorrowed(&v));
    ASSERT_EQ(vstrLen(&v), 5u);
    ASSERT_EQ(memcmp(vstrData(&v), "hello", 5), 0);
    /* Borrowed data points directly at the original buffer. */
    ASSERT_EQ(vstrData(&v), raw);
}

TEST_F(VstrTest, BorrowedEmpty) {
    vstr v;
    vstrInitBorrowed(&v, "", 0);

    ASSERT_EQ(vstrType(&v), VSTR_BORROWED);
    ASSERT_EQ(vstrLen(&v), 0u);
    ASSERT_NE(vstrData(&v), nullptr);
}

TEST_F(VstrTest, BorrowedBinaryData) {
    /* vstr must handle binary data with embedded nulls. */
    const char data[] = "ab\0cd";
    vstr v;
    vstrInitBorrowed(&v, data, 5);

    ASSERT_EQ(vstrLen(&v), 5u);
    ASSERT_EQ(memcmp(vstrData(&v), "ab\0cd", 5), 0);
}

TEST_F(VstrTest, BorrowedFreeIsNoop) {
    const char *raw = "test";
    vstr v;
    vstrInitBorrowed(&v, raw, 4);
    /* vstrFree on a borrowed vstr should not crash or free the underlying buffer. */
    vstrFree(&v);
    /* The original buffer is still valid (not freed). */
    ASSERT_EQ(memcmp(raw, "test", 4), 0);
}

/* --- SDS vstr --- */

TEST_F(VstrTest, SdsBasic) {
    sds s = sdsnewlen("world", 5);
    vstr v;
    vstrInitSds(&v, s);

    ASSERT_EQ(vstrType(&v), VSTR_SDS);
    ASSERT_FALSE(vstrIsBorrowed(&v));
    ASSERT_EQ(vstrLen(&v), 5u);
    ASSERT_EQ(memcmp(vstrData(&v), "world", 5), 0);

    vstrFree(&v);
}

TEST_F(VstrTest, SdsEmpty) {
    sds s = sdsempty();
    vstr v;
    vstrInitSds(&v, s);

    ASSERT_EQ(vstrLen(&v), 0u);
    ASSERT_NE(vstrData(&v), nullptr);

    vstrFree(&v);
}

/* --- Equality --- */

TEST_F(VstrTest, EqualSameType) {
    vstr a, b;
    vstrInitBorrowed(&a, "abc", 3);
    vstrInitBorrowed(&b, "abc", 3);
    ASSERT_TRUE(vstrEqual(&a, &b));
}

TEST_F(VstrTest, EqualDifferentTypes) {
    sds s = sdsnewlen("abc", 3);
    vstr a, b;
    vstrInitBorrowed(&a, "abc", 3);
    vstrInitSds(&b, s);
    ASSERT_TRUE(vstrEqual(&a, &b));

    vstrFree(&b);
}

TEST_F(VstrTest, NotEqualDifferentContent) {
    vstr a, b;
    vstrInitBorrowed(&a, "abc", 3);
    vstrInitBorrowed(&b, "xyz", 3);
    ASSERT_FALSE(vstrEqual(&a, &b));
}

TEST_F(VstrTest, NotEqualDifferentLength) {
    vstr a, b;
    vstrInitBorrowed(&a, "abc", 3);
    vstrInitBorrowed(&b, "ab", 2);
    ASSERT_FALSE(vstrEqual(&a, &b));
}

TEST_F(VstrTest, EqualRaw) {
    vstr v;
    vstrInitBorrowed(&v, "hello", 5);
    ASSERT_TRUE(vstrEqualRaw(&v, "hello", 5));
    ASSERT_FALSE(vstrEqualRaw(&v, "hell", 4));
    ASSERT_FALSE(vstrEqualRaw(&v, "hellx", 5));
}

TEST_F(VstrTest, EqualSds) {
    sds s = sdsnewlen("hello", 5);
    vstr v;
    vstrInitBorrowed(&v, "hello", 5);
    ASSERT_TRUE(vstrEqualSds(&v, s));
    sdsfree(s);

    s = sdsnewlen("other", 5);
    ASSERT_FALSE(vstrEqualSds(&v, s));
    sdsfree(s);
}

/* --- Hashing --- */

TEST_F(VstrTest, HashConsistentAcrossTypes) {
    /* A borrowed vstr and an sds vstr with the same bytes must produce
     * the same hash. This is critical for hashtable lookups. */
    sds s = sdsnewlen("testkey", 7);
    vstr a, b;
    vstrInitBorrowed(&a, "testkey", 7);
    vstrInitSds(&b, s);

    ASSERT_EQ(vstrHash(&a), vstrHash(&b));

    vstrFree(&b);
}

TEST_F(VstrTest, HashDifferentForDifferentContent) {
    vstr a, b;
    vstrInitBorrowed(&a, "key1", 4);
    vstrInitBorrowed(&b, "key2", 4);
    /* Not guaranteed by hash function contract, but extremely likely. */
    EXPECT_NE(vstrHash(&a), vstrHash(&b));
}

/* --- Materialization --- */

TEST_F(VstrTest, MaterializeBorrowed) {
    const char *raw = "materialize_me";
    vstr v;
    vstrInitBorrowed(&v, raw, 14);

    ASSERT_TRUE(vstrIsBorrowed(&v));
    vstrMaterialize(&v);
    ASSERT_FALSE(vstrIsBorrowed(&v));
    ASSERT_EQ(vstrType(&v), VSTR_SDS);
    ASSERT_EQ(vstrLen(&v), 14u);
    ASSERT_EQ(memcmp(vstrData(&v), "materialize_me", 14), 0);
    /* After materialization, data should be a separate copy. */
    ASSERT_NE(vstrData(&v), raw);

    vstrFree(&v);
}

TEST_F(VstrTest, MaterializeSdsIsNoop) {
    sds s = sdsnewlen("already_owned", 13);
    const char *original_ptr = s;
    vstr v;
    vstrInitSds(&v, s);

    vstrMaterialize(&v);
    /* Should still be the same sds, no reallocation. */
    ASSERT_EQ(vstrType(&v), VSTR_SDS);
    ASSERT_EQ(vstrData(&v), original_ptr);

    vstrFree(&v);
}

/* --- TakeSds --- */

TEST_F(VstrTest, TakeSdsFromOwned) {
    sds s = sdsnewlen("take_me", 7);
    vstr v;
    vstrInitSds(&v, s);

    sds taken = vstrTakeSds(&v);
    ASSERT_EQ(taken, s);
    ASSERT_EQ(sdslen(taken), 7u);
    ASSERT_EQ(memcmp(taken, "take_me", 7), 0);

    sdsfree(taken);
    /* vstr is now in undefined state — don't use it. */
}

TEST_F(VstrTest, TakeSdsFromBorrowed) {
    const char *raw = "copy_me";
    vstr v;
    vstrInitBorrowed(&v, raw, 7);

    sds taken = vstrTakeSds(&v);
    /* Should be a new sds copy, not the original pointer. */
    ASSERT_NE((const char *)taken, raw);
    ASSERT_EQ(sdslen(taken), 7u);
    ASSERT_EQ(memcmp(taken, "copy_me", 7), 0);

    sdsfree(taken);
}

/* --- Reinit after free --- */

TEST_F(VstrTest, ReinitAfterFree) {
    sds s = sdsnewlen("first", 5);
    vstr v;
    vstrInitSds(&v, s);
    vstrFree(&v);

    /* Reinitialize as borrowed — should work fine. */
    vstrInitBorrowed(&v, "second", 6);
    ASSERT_EQ(vstrType(&v), VSTR_BORROWED);
    ASSERT_EQ(vstrLen(&v), 6u);
    ASSERT_EQ(memcmp(vstrData(&v), "second", 6), 0);
}
