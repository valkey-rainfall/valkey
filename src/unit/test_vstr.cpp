/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>

extern "C" {
#include "hashtable.h"
#include "sds.h"
#include "util.h"
#include "vstr.h"

/* Hash callbacks defined in server.c — not exposed via a header. */
uint64_t vstrHashCallback(const void *key);
uint64_t sdsHashConfigurableSeed(const void *key);
void setConfigurableHashSeed(uint8_t *seed);

/* Hashtable type for sets — uses vstr-aware callbacks. */
extern hashtableType setHashtableType;
}

/* Helper: tag a vstr pointer for use with vstrHashCallback /
 * vstrCompareCallback (which expect either a tagged vstr * or a plain sds). */
static inline void *tagVstr(const vstr *v) {
    return vstrTagPtr(v);
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

/* --- Property-Based Tests --- */

// Feature: vstr-zero-copy-poc, Property 1: Hash equivalence between vstr representations and sds

/*
 * **Validates: Requirements 1.3, 17.1, 17.2**
 *
 * For any byte sequence (including empty, binary data with embedded nulls,
 * and arbitrary length), the vstr-aware hash callback applied to a vstr
 * containing those bytes SHALL produce the same hash value as
 * sdsHashConfigurableSeed applied to an sds containing the same bytes,
 * regardless of whether the vstr is a borrowed reference or an owned sds.
 */

class VstrProperty1Test : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        /* Seed the configurable hash function so that
         * genHashFunctionConfigurableSeed produces consistent results. */
        uint8_t seed[16];
        getRandomBytes(seed, sizeof(seed));
        setConfigurableHashSeed(seed);
    }
};

TEST_F(VstrProperty1Test, HashEquivalenceBetweenVstrAndSds) {
    const int iterations = 200;
    const size_t max_len = 4096;

    for (int i = 0; i < iterations; i++) {
        /* Generate a random length in [0, max_len]. */
        unsigned char len_bytes[2];
        getRandomBytes(len_bytes, sizeof(len_bytes));
        size_t len = ((size_t)len_bytes[0] | ((size_t)len_bytes[1] << 8)) % (max_len + 1);

        /* Generate random bytes (may include embedded nulls). */
        char *buf = (char *)malloc(len);
        ASSERT_NE(buf, nullptr);
        if (len > 0) {
            getRandomBytes((unsigned char *)buf, len);
        }

        /* Create a plain sds from the random bytes. */
        sds s = sdsnewlen(buf, len);

        /* Create a borrowed vstr from the same bytes. */
        vstr v_borrowed;
        vstrInitBorrowed(&v_borrowed, buf, len);

        /* Create an sds vstr from a copy of the same bytes. */
        sds s_copy = sdsnewlen(buf, len);
        vstr v_sds;
        vstrInitSds(&v_sds, s_copy);

        /* Hash via the sds callback (the baseline). */
        uint64_t hash_sds = sdsHashConfigurableSeed(s);

        /* Hash via the vstr callback on a borrowed vstr. */
        uint64_t hash_borrowed = vstrHashCallback(tagVstr(&v_borrowed));

        /* Hash via the vstr callback on an sds vstr. */
        uint64_t hash_sds_vstr = vstrHashCallback(tagVstr(&v_sds));

        ASSERT_EQ(hash_sds, hash_borrowed)
            << "Hash mismatch between sds and borrowed vstr at iteration " << i
            << " (len=" << len << ")";
        ASSERT_EQ(hash_sds, hash_sds_vstr)
            << "Hash mismatch between sds and sds vstr at iteration " << i
            << " (len=" << len << ")";

        /* Cleanup. */
        sdsfree(s);
        vstrFree(&v_sds);
        free(buf);
    }
}

// Feature: vstr-zero-copy-poc, Property 2: Compare equivalence across all representation combinations

/*
 * **Validates: Requirements 2.1, 2.2, 2.3**
 *
 * For any two byte sequences and any combination of vstr representations
 * (borrowed×borrowed, borrowed×sds, sds×borrowed, sds×sds), the vstr-aware
 * compare callback SHALL return 1 if and only if the byte sequences are
 * identical, matching the result that dictSdsKeyCompare would produce for
 * sds values containing the same respective bytes.
 */

extern "C" {
int vstrCompareCallback(const void *key1, const void *key2);
int dictSdsKeyCompare(const void *key1, const void *key2);
}

class VstrProperty2Test : public ::testing::Test {};

TEST_F(VstrProperty2Test, CompareEquivalenceAcrossAllRepresentationCombinations) {
    const int iterations = 200;
    const size_t max_len = 512;

    for (int i = 0; i < iterations; i++) {
        /* Generate a random length for the first byte sequence. */
        unsigned char len_bytes_a[2];
        getRandomBytes(len_bytes_a, sizeof(len_bytes_a));
        size_t len_a = ((size_t)len_bytes_a[0] | ((size_t)len_bytes_a[1] << 8)) % (max_len + 1);

        /* Generate random bytes for the first sequence. */
        char *buf_a = (char *)malloc(len_a > 0 ? len_a : 1);
        ASSERT_NE(buf_a, nullptr);
        if (len_a > 0) {
            getRandomBytes((unsigned char *)buf_a, len_a);
        }

        /* Decide whether the second sequence should be identical or different.
         * ~50% chance of identical to ensure we test both equal and unequal. */
        unsigned char coin;
        getRandomBytes(&coin, 1);
        bool make_identical = (coin & 1) == 0;

        char *buf_b;
        size_t len_b;

        if (make_identical) {
            /* Identical bytes. */
            len_b = len_a;
            buf_b = (char *)malloc(len_b > 0 ? len_b : 1);
            ASSERT_NE(buf_b, nullptr);
            if (len_b > 0) memcpy(buf_b, buf_a, len_b);
        } else {
            /* Generate a different second sequence. Use a random length that
             * sometimes matches len_a (to test content-only differences). */
            unsigned char len_bytes_b[2];
            getRandomBytes(len_bytes_b, sizeof(len_bytes_b));
            len_b = ((size_t)len_bytes_b[0] | ((size_t)len_bytes_b[1] << 8)) % (max_len + 1);

            buf_b = (char *)malloc(len_b > 0 ? len_b : 1);
            ASSERT_NE(buf_b, nullptr);
            if (len_b > 0) {
                getRandomBytes((unsigned char *)buf_b, len_b);
            }
            /* If by chance the random bytes are identical, flip one byte. */
            if (len_a == len_b && len_a > 0 && memcmp(buf_a, buf_b, len_a) == 0) {
                buf_b[len_b - 1] ^= 0x01;
            }
        }

        /* Determine expected comparison result. */
        int expected = (len_a == len_b && (len_a == 0 || memcmp(buf_a, buf_b, len_a) == 0)) ? 1 : 0;

        /* Verify dictSdsKeyCompare agrees with our expected result. */
        sds sds_a = sdsnewlen(buf_a, len_a);
        sds sds_b = sdsnewlen(buf_b, len_b);
        int sds_result = dictSdsKeyCompare(sds_a, sds_b);
        ASSERT_EQ(sds_result, expected)
            << "dictSdsKeyCompare baseline mismatch at iteration " << i;

        /* Create all 4 vstr representations. */
        vstr v_borrowed_a, v_borrowed_b;
        vstrInitBorrowed(&v_borrowed_a, buf_a, len_a);
        vstrInitBorrowed(&v_borrowed_b, buf_b, len_b);

        sds sds_copy_a = sdsnewlen(buf_a, len_a);
        sds sds_copy_b = sdsnewlen(buf_b, len_b);
        vstr v_sds_a, v_sds_b;
        vstrInitSds(&v_sds_a, sds_copy_a);
        vstrInitSds(&v_sds_b, sds_copy_b);

        /* Test all 4 representation combinations via tagged pointers. */
        const void *t_ba = tagVstr(&v_borrowed_a), *t_bb = tagVstr(&v_borrowed_b);
        const void *t_sa = tagVstr(&v_sds_a), *t_sb = tagVstr(&v_sds_b);

        /* borrowed × borrowed */
        int result_bb = vstrCompareCallback(t_ba, t_bb);
        ASSERT_EQ(result_bb, expected)
            << "borrowed×borrowed mismatch at iteration " << i
            << " (len_a=" << len_a << ", len_b=" << len_b << ")";

        /* borrowed × sds */
        int result_bs = vstrCompareCallback(t_ba, t_sb);
        ASSERT_EQ(result_bs, expected)
            << "borrowed×sds mismatch at iteration " << i
            << " (len_a=" << len_a << ", len_b=" << len_b << ")";

        /* sds × borrowed */
        int result_sb = vstrCompareCallback(t_sa, t_bb);
        ASSERT_EQ(result_sb, expected)
            << "sds×borrowed mismatch at iteration " << i
            << " (len_a=" << len_a << ", len_b=" << len_b << ")";

        /* sds × sds */
        int result_ss = vstrCompareCallback(t_sa, t_sb);
        ASSERT_EQ(result_ss, expected)
            << "sds×sds mismatch at iteration " << i
            << " (len_a=" << len_a << ", len_b=" << len_b << ")";

        /* Cleanup. */
        sdsfree(sds_a);
        sdsfree(sds_b);
        vstrFree(&v_sds_a);
        vstrFree(&v_sds_b);
        free(buf_a);
        free(buf_b);
    }
}

TEST_F(VstrProperty2Test, EdgeCaseEmptyStrings) {
    /* Both empty — should be equal across all combos. */
    vstr v_borrowed_a, v_borrowed_b;
    vstrInitBorrowed(&v_borrowed_a, "", 0);
    vstrInitBorrowed(&v_borrowed_b, "", 0);

    sds s_a = sdsempty();
    sds s_b = sdsempty();
    vstr v_sds_a, v_sds_b;
    vstrInitSds(&v_sds_a, s_a);
    vstrInitSds(&v_sds_b, s_b);

    const void *t_ba = tagVstr(&v_borrowed_a), *t_bb = tagVstr(&v_borrowed_b);
    const void *t_sa = tagVstr(&v_sds_a), *t_sb = tagVstr(&v_sds_b);

    ASSERT_EQ(vstrCompareCallback(t_ba, t_bb), 1);
    ASSERT_EQ(vstrCompareCallback(t_ba, t_sb), 1);
    ASSERT_EQ(vstrCompareCallback(t_sa, t_bb), 1);
    ASSERT_EQ(vstrCompareCallback(t_sa, t_sb), 1);

    vstrFree(&v_sds_a);
    vstrFree(&v_sds_b);
}

TEST_F(VstrProperty2Test, EdgeCaseDifferOnlyInLastByte) {
    /* Two strings that differ only in the last byte. */
    const char data_a[] = "abcdefg";
    const char data_b[] = "abcdefh";

    vstr v_borrowed_a, v_borrowed_b;
    vstrInitBorrowed(&v_borrowed_a, data_a, 7);
    vstrInitBorrowed(&v_borrowed_b, data_b, 7);

    sds s_a = sdsnewlen(data_a, 7);
    sds s_b = sdsnewlen(data_b, 7);
    vstr v_sds_a, v_sds_b;
    vstrInitSds(&v_sds_a, s_a);
    vstrInitSds(&v_sds_b, s_b);

    const void *t_ba2 = tagVstr(&v_borrowed_a), *t_bb2 = tagVstr(&v_borrowed_b);
    const void *t_sa2 = tagVstr(&v_sds_a), *t_sb2 = tagVstr(&v_sds_b);

    ASSERT_EQ(vstrCompareCallback(t_ba2, t_bb2), 0);
    ASSERT_EQ(vstrCompareCallback(t_ba2, t_sb2), 0);
    ASSERT_EQ(vstrCompareCallback(t_sa2, t_bb2), 0);
    ASSERT_EQ(vstrCompareCallback(t_sa2, t_sb2), 0);

    vstrFree(&v_sds_a);
    vstrFree(&v_sds_b);
}

TEST_F(VstrProperty2Test, EdgeCaseDifferentLengths) {
    /* Strings where one is a prefix of the other. */
    const char *short_str = "abc";
    const char *long_str = "abcdef";

    vstr v_borrowed_short, v_borrowed_long;
    vstrInitBorrowed(&v_borrowed_short, short_str, 3);
    vstrInitBorrowed(&v_borrowed_long, long_str, 6);

    sds s_short = sdsnewlen(short_str, 3);
    sds s_long = sdsnewlen(long_str, 6);
    vstr v_sds_short, v_sds_long;
    vstrInitSds(&v_sds_short, s_short);
    vstrInitSds(&v_sds_long, s_long);

    const void *t_bs = tagVstr(&v_borrowed_short), *t_bl = tagVstr(&v_borrowed_long);
    const void *t_ss = tagVstr(&v_sds_short), *t_sl = tagVstr(&v_sds_long);

    ASSERT_EQ(vstrCompareCallback(t_bs, t_bl), 0);
    ASSERT_EQ(vstrCompareCallback(t_bs, t_sl), 0);
    ASSERT_EQ(vstrCompareCallback(t_ss, t_bl), 0);
    ASSERT_EQ(vstrCompareCallback(t_ss, t_sl), 0);

    vstrFree(&v_sds_short);
    vstrFree(&v_sds_long);
}

// Feature: vstr-zero-copy-poc, Property 3: Materialization round-trip preserves content

/*
 * **Validates: Requirements 9.2, 10.2, 11.2, 13.2**
 *
 * For any borrowed vstr initialized with arbitrary bytes (including binary
 * data with embedded nulls), materializing it via vstrMaterialize() and then
 * reading back via vstrData()/vstrLen() SHALL produce the original byte
 * sequence and length unchanged.
 */

class VstrProperty3Test : public ::testing::Test {};

TEST_F(VstrProperty3Test, MaterializationRoundTripPreservesContent) {
    const int iterations = 200;
    const size_t max_len = 4096;

    for (int i = 0; i < iterations; i++) {
        /* Generate a random length in [0, max_len]. */
        unsigned char len_bytes[2];
        getRandomBytes(len_bytes, sizeof(len_bytes));
        size_t len = ((size_t)len_bytes[0] | ((size_t)len_bytes[1] << 8)) % (max_len + 1);

        /* Generate random bytes (may include embedded nulls). */
        char *buf = (char *)malloc(len > 0 ? len : 1);
        ASSERT_NE(buf, nullptr);
        if (len > 0) {
            getRandomBytes((unsigned char *)buf, len);
        }

        /* Create a borrowed vstr from the random bytes. */
        vstr v;
        vstrInitBorrowed(&v, buf, len);

        /* Verify it starts as borrowed. */
        ASSERT_TRUE(vstrIsBorrowed(&v))
            << "vstr should be borrowed before materialization at iteration " << i;
        ASSERT_EQ(vstrLen(&v), len)
            << "Length mismatch before materialization at iteration " << i;
        ASSERT_EQ(vstrData(&v), buf)
            << "Borrowed data should point at original buffer at iteration " << i;

        /* Materialize: copy borrowed bytes into an owned sds. */
        vstrMaterialize(&v);

        /* Verify it is now owned (not borrowed). */
        ASSERT_FALSE(vstrIsBorrowed(&v))
            << "vstr should be owned after materialization at iteration " << i;
        ASSERT_EQ(vstrType(&v), VSTR_SDS)
            << "vstr type should be VSTR_SDS after materialization at iteration " << i;

        /* Verify length is preserved. */
        ASSERT_EQ(vstrLen(&v), len)
            << "Length mismatch after materialization at iteration " << i
            << " (expected " << len << ", got " << vstrLen(&v) << ")";

        /* Verify content is preserved (byte-for-byte, including embedded nulls). */
        if (len > 0) {
            ASSERT_EQ(memcmp(vstrData(&v), buf, len), 0)
                << "Content mismatch after materialization at iteration " << i
                << " (len=" << len << ")";
        }

        /* Verify the data pointer is different from the original buffer
         * (materialization creates a copy, not an alias). */
        ASSERT_NE(vstrData(&v), buf)
            << "Materialized data should be a separate copy at iteration " << i;

        /* Cleanup. */
        vstrFree(&v);
        free(buf);
    }
}

// Feature: vstr-zero-copy-poc, Property 4: vstrTakeSds round-trip preserves content

/*
 * **Validates: Requirements 14.1, 14.2**
 *
 * For any vstr (borrowed or owned), calling vstrTakeSds() SHALL produce an
 * sds whose bytes and length are identical to the original vstr's
 * vstrData()/vstrLen() as observed before the call.
 */

class VstrProperty4Test : public ::testing::Test {};

TEST_F(VstrProperty4Test, TakeSdsRoundTripPreservesContent) {
    const int iterations = 200;
    const size_t max_len = 4096;

    for (int i = 0; i < iterations; i++) {
        /* Generate a random length in [0, max_len]. */
        unsigned char len_bytes[2];
        getRandomBytes(len_bytes, sizeof(len_bytes));
        size_t len = ((size_t)len_bytes[0] | ((size_t)len_bytes[1] << 8)) % (max_len + 1);

        /* Generate random bytes (may include embedded nulls). */
        char *buf = (char *)malloc(len > 0 ? len : 1);
        ASSERT_NE(buf, nullptr);
        if (len > 0) {
            getRandomBytes((unsigned char *)buf, len);
        }

        /* --- Test 1: vstrTakeSds on a borrowed vstr --- */
        {
            vstr v;
            vstrInitBorrowed(&v, buf, len);

            /* Record data and length before the call. */
            const char *data_before = vstrData(&v);
            size_t len_before = vstrLen(&v);
            ASSERT_EQ(data_before, buf);
            ASSERT_EQ(len_before, len);

            /* Take the sds out. For borrowed, this creates a new sds copy. */
            sds taken = vstrTakeSds(&v);
            ASSERT_NE(taken, nullptr);

            /* Verify the resulting sds has the same length. */
            ASSERT_EQ(sdslen(taken), len_before)
                << "Borrowed vstrTakeSds length mismatch at iteration " << i
                << " (expected " << len_before << ", got " << sdslen(taken) << ")";

            /* Verify the resulting sds has the same bytes. */
            if (len > 0) {
                ASSERT_EQ(memcmp(taken, buf, len), 0)
                    << "Borrowed vstrTakeSds content mismatch at iteration " << i
                    << " (len=" << len << ")";
            }

            sdsfree(taken);
        }

        /* --- Test 2: vstrTakeSds on an sds vstr --- */
        {
            /* Create an sds from a copy of the same bytes. */
            sds s = sdsnewlen(buf, len);
            vstr v;
            vstrInitSds(&v, s);

            /* Record data and length before the call. */
            const char *data_before = vstrData(&v);
            size_t len_before = vstrLen(&v);
            ASSERT_EQ(data_before, s);
            ASSERT_EQ(len_before, len);

            /* Take the sds out. For owned, this returns the sds directly. */
            sds taken = vstrTakeSds(&v);
            ASSERT_NE(taken, nullptr);

            /* Verify the resulting sds has the same length. */
            ASSERT_EQ(sdslen(taken), len_before)
                << "Sds vstrTakeSds length mismatch at iteration " << i
                << " (expected " << len_before << ", got " << sdslen(taken) << ")";

            /* Verify the resulting sds has the same bytes. */
            if (len > 0) {
                ASSERT_EQ(memcmp(taken, buf, len), 0)
                    << "Sds vstrTakeSds content mismatch at iteration " << i
                    << " (len=" << len << ")";
            }

            sdsfree(taken);
        }

        /* Cleanup. */
        free(buf);
    }
}

// Feature: vstr-zero-copy-poc, Property 5: Lookup equivalence under vstr callbacks

/*
 * **Validates: Requirements 3.1, 4.1–4.6, 5.1, 5.4, 6.1, 6.2, 7.1, 7.2, 17.3**
 *
 * For any set of entries stored in a hashtable using vstr-aware callbacks,
 * and any lookup key, finding via a borrowed vstr SHALL locate the same entry
 * (or correctly report not-found) as finding via an sds-initialized vstr
 * containing the same bytes.
 */

class VstrProperty5Test : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        /* Seed the configurable hash function so that
         * genHashFunctionConfigurableSeed produces consistent results. */
        uint8_t seed[16];
        getRandomBytes(seed, sizeof(seed));
        setConfigurableHashSeed(seed);
    }
};

/* Helper: generate a random sds of length in [1, max_len]. */
static sds generateRandomSds(size_t max_len) {
    unsigned char len_bytes[2];
    getRandomBytes(len_bytes, sizeof(len_bytes));
    size_t len = 1 + (((size_t)len_bytes[0] | ((size_t)len_bytes[1] << 8)) % max_len);

    char *buf = (char *)malloc(len);
    getRandomBytes((unsigned char *)buf, len);
    sds s = sdsnewlen(buf, len);
    free(buf);
    return s;
}

TEST_F(VstrProperty5Test, LookupEquivalenceUnderVstrCallbacks) {
    const int iterations = 100;
    const size_t max_key_len = 256;

    for (int i = 0; i < iterations; i++) {
        /* Determine number of keys to insert: 5–50. */
        unsigned char count_byte;
        getRandomBytes(&count_byte, 1);
        size_t num_keys = 5 + (count_byte % 46); /* [5, 50] */

        /* Create a hashtable using setHashtableType (vstr-aware callbacks).
         * setHashtableType stores plain sds entries directly. */
        hashtable *ht = hashtableCreate(&setHashtableType);

        /* Generate and insert random sds keys, keeping a copy for verification. */
        sds *keys = (sds *)malloc(num_keys * sizeof(sds));
        size_t inserted = 0;

        for (size_t j = 0; j < num_keys; j++) {
            sds key = generateRandomSds(max_key_len);

            /* Check for duplicates — skip if already present. */
            vstr vk;
            vstrInitBorrowed(&vk, key, sdslen(key));
            void *existing = NULL;
            if (hashtableFind(ht, vstrTagPtr(&vk), &existing)) {
                sdsfree(key);
                continue;
            }

            /* Insert the sds into the hashtable. The hashtable takes ownership
             * of this sds (entryDestructor = dictSdsDestructor). */
            sds entry = sdsdup(key);
            ASSERT_TRUE(hashtableAdd(ht, entry))
                << "Failed to add key at iteration " << i << ", key index " << j;

            keys[inserted] = key; /* Keep our own copy for verification. */
            inserted++;
        }

        ASSERT_GE(inserted, 1u)
            << "Should have inserted at least 1 key at iteration " << i;

        /* --- Verify present keys are found --- */
        for (size_t j = 0; j < inserted; j++) {
            vstr vk;
            vstrInitBorrowed(&vk, keys[j], sdslen(keys[j]));

            void *found = NULL;
            int result = hashtableFind(ht, vstrTagPtr(&vk), &found);
            ASSERT_TRUE(result)
                << "Present key not found at iteration " << i
                << ", key index " << j
                << " (key len=" << sdslen(keys[j]) << ")";
            ASSERT_NE(found, nullptr)
                << "Found entry is NULL for present key at iteration " << i;

            /* Verify the found entry matches the key bytes. */
            sds found_sds = (sds)found;
            ASSERT_EQ(sdslen(found_sds), sdslen(keys[j]))
                << "Found entry length mismatch at iteration " << i;
            ASSERT_EQ(memcmp(found_sds, keys[j], sdslen(keys[j])), 0)
                << "Found entry content mismatch at iteration " << i;
        }

        /* --- Verify absent keys are not found --- */
        size_t num_absent = 10 + (count_byte % 11); /* 10–20 absent keys */
        for (size_t j = 0; j < num_absent; j++) {
            sds absent_key = generateRandomSds(max_key_len);

            /* Make sure this key is actually absent by checking against
             * our inserted keys. If it collides, skip. */
            bool is_present = false;
            for (size_t k = 0; k < inserted; k++) {
                if (sdslen(absent_key) == sdslen(keys[k]) &&
                    memcmp(absent_key, keys[k], sdslen(absent_key)) == 0) {
                    is_present = true;
                    break;
                }
            }

            if (!is_present) {
                vstr vk;
                vstrInitBorrowed(&vk, absent_key, sdslen(absent_key));

                void *found = NULL;
                int result = hashtableFind(ht, vstrTagPtr(&vk), &found);
                ASSERT_FALSE(result)
                    << "Absent key incorrectly found at iteration " << i
                    << ", absent key index " << j;
            }

            sdsfree(absent_key);
        }

        /* Cleanup: free our verification copies and the hashtable. */
        for (size_t j = 0; j < inserted; j++) {
            sdsfree(keys[j]);
        }
        free(keys);
        hashtableRelease(ht); /* Frees the sds entries via entryDestructor. */
    }
}

// Feature: vstr-zero-copy-poc, Property 6: RESP parser produces correct borrowed vstr content

/*
 * **Validates: Requirements 8.1, 8.3**
 *
 * For any valid RESP bulk string argument (arbitrary bytes, arbitrary length),
 * the mechanism the parser uses — vstrInitBorrowed into a buffer containing
 * the RESP framing, or vstrInitSds for big arguments — SHALL produce a vstr
 * whose vstrData()/vstrLen() yield the exact argument bytes and length from
 * the original payload.
 */

class VstrProperty6Test : public ::testing::Test {};

TEST_F(VstrProperty6Test, RespParserBorrowedVstrContent) {
    const int iterations = 200;
    const size_t max_len = 4096;

    for (int i = 0; i < iterations; i++) {
        /* Generate a random argument length in [0, max_len]. */
        unsigned char len_bytes[2];
        getRandomBytes(len_bytes, sizeof(len_bytes));
        size_t arg_len = ((size_t)len_bytes[0] | ((size_t)len_bytes[1] << 8)) % (max_len + 1);

        /* Generate random argument content (may include embedded nulls, \r, \n). */
        char *arg_content = (char *)malloc(arg_len > 0 ? arg_len : 1);
        ASSERT_NE(arg_content, nullptr);
        if (arg_len > 0) {
            getRandomBytes((unsigned char *)arg_content, arg_len);
        }

        /* --- Normal-argument path: borrowed vstr into a RESP buffer --- */
        {
            /* Build a mock RESP bulk string: $<len>\r\n<content>\r\n
             * The parser would point a borrowed vstr at the <content> portion. */
            char len_str[32];
            int len_str_len = snprintf(len_str, sizeof(len_str), "%zu", arg_len);

            /* Total buffer: '$' + len_str + '\r\n' + content + '\r\n' */
            size_t resp_buf_len = 1 + (size_t)len_str_len + 2 + arg_len + 2;
            char *resp_buf = (char *)malloc(resp_buf_len);
            ASSERT_NE(resp_buf, nullptr);

            /* Assemble the RESP frame. */
            size_t pos = 0;
            resp_buf[pos++] = '$';
            memcpy(resp_buf + pos, len_str, (size_t)len_str_len);
            pos += (size_t)len_str_len;
            resp_buf[pos++] = '\r';
            resp_buf[pos++] = '\n';
            size_t content_offset = pos; /* This is where the parser would point. */
            if (arg_len > 0) {
                memcpy(resp_buf + pos, arg_content, arg_len);
            }
            pos += arg_len;
            resp_buf[pos++] = '\r';
            resp_buf[pos++] = '\n';
            ASSERT_EQ(pos, resp_buf_len);

            /* Simulate what parseMultibulk does for a normal-sized argument:
             * vstrInitBorrowed(&c->argv[c->argc], c->querybuf + qb_pos, bulklen) */
            vstr v;
            vstrInitBorrowed(&v, resp_buf + content_offset, arg_len);

            /* Verify the vstr is borrowed. */
            ASSERT_TRUE(vstrIsBorrowed(&v))
                << "Normal-path vstr should be borrowed at iteration " << i;

            /* Verify length matches the argument length. */
            ASSERT_EQ(vstrLen(&v), arg_len)
                << "Normal-path length mismatch at iteration " << i
                << " (expected " << arg_len << ", got " << vstrLen(&v) << ")";

            /* Verify data pointer points into the RESP buffer at the content offset. */
            ASSERT_EQ(vstrData(&v), resp_buf + content_offset)
                << "Normal-path data should point into RESP buffer at iteration " << i;

            /* Verify content matches the original argument bytes. */
            if (arg_len > 0) {
                ASSERT_EQ(memcmp(vstrData(&v), arg_content, arg_len), 0)
                    << "Normal-path content mismatch at iteration " << i
                    << " (arg_len=" << arg_len << ")";
            }

            free(resp_buf);
        }

        /* --- Big-argument path: sds vstr taking ownership of the buffer --- */
        {
            /* Simulate what parseMultibulk does for a big argument:
             * vstrInitSds(&c->argv[c->argc], c->querybuf)
             * where querybuf is an sds containing the argument content. */
            sds querybuf = sdsnewlen(arg_content, arg_len);
            vstr v;
            vstrInitSds(&v, querybuf);

            /* Verify the vstr is owned (not borrowed). */
            ASSERT_FALSE(vstrIsBorrowed(&v))
                << "Big-arg-path vstr should be owned at iteration " << i;
            ASSERT_EQ(vstrType(&v), VSTR_SDS)
                << "Big-arg-path vstr type should be VSTR_SDS at iteration " << i;

            /* Verify length matches the argument length. */
            ASSERT_EQ(vstrLen(&v), arg_len)
                << "Big-arg-path length mismatch at iteration " << i
                << " (expected " << arg_len << ", got " << vstrLen(&v) << ")";

            /* Verify content matches the original argument bytes. */
            if (arg_len > 0) {
                ASSERT_EQ(memcmp(vstrData(&v), arg_content, arg_len), 0)
                    << "Big-arg-path content mismatch at iteration " << i
                    << " (arg_len=" << arg_len << ")";
            }

            vstrFree(&v);
        }

        /* Cleanup. */
        free(arg_content);
    }
}

TEST_F(VstrProperty6Test, EdgeCaseEmptyArgument) {
    /* RESP bulk string for empty argument: $0\r\n\r\n */
    const char resp[] = "$0\r\n\r\n";
    size_t content_offset = 4; /* After "$0\r\n" */

    /* Normal path: borrowed vstr with zero length. */
    vstr v;
    vstrInitBorrowed(&v, resp + content_offset, 0);
    ASSERT_TRUE(vstrIsBorrowed(&v));
    ASSERT_EQ(vstrLen(&v), 0u);
    ASSERT_NE(vstrData(&v), nullptr);

    /* Big-arg path: sds with zero length. */
    sds s = sdsempty();
    vstr v2;
    vstrInitSds(&v2, s);
    ASSERT_EQ(vstrLen(&v2), 0u);
    vstrFree(&v2);
}

TEST_F(VstrProperty6Test, EdgeCaseBinaryContentWithCRLF) {
    /* Argument content that contains \r\n sequences — the parser must use
     * the length prefix, not scan for delimiters. */
    const char content[] = "hello\r\nworld\r\n";
    size_t content_len = 14; /* includes the embedded \r\n bytes */

    /* Build RESP: $14\r\nhello\r\nworld\r\n\r\n */
    const char resp[] = "$14\r\nhello\r\nworld\r\n\r\n";
    size_t content_offset = 5; /* After "$14\r\n" */

    vstr v;
    vstrInitBorrowed(&v, resp + content_offset, content_len);
    ASSERT_EQ(vstrLen(&v), content_len);
    ASSERT_EQ(memcmp(vstrData(&v), content, content_len), 0);
}

TEST_F(VstrProperty6Test, EdgeCaseNullBytesInContent) {
    /* Argument content with embedded null bytes. */
    const char content[] = "ab\0cd\0ef";
    size_t content_len = 8;

    /* Build RESP buffer manually. */
    char resp[32];
    size_t pos = 0;
    resp[pos++] = '$';
    resp[pos++] = '8';
    resp[pos++] = '\r';
    resp[pos++] = '\n';
    size_t content_offset = pos;
    memcpy(resp + pos, content, content_len);
    pos += content_len;
    resp[pos++] = '\r';
    resp[pos++] = '\n';

    vstr v;
    vstrInitBorrowed(&v, resp + content_offset, content_len);
    ASSERT_EQ(vstrLen(&v), content_len);
    ASSERT_EQ(memcmp(vstrData(&v), content, content_len), 0);
}
