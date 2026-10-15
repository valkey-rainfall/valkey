/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

extern "C" {
#include "server.h"
}

/* Tests for the hashtable encoding of sets, which stores integer members
 * inline as tagged words and everything else as sds. See the setEntry*
 * helpers in server.h for the representation. */

#define INLINE_MAX ((1LL << 62) - 1)
#define INLINE_MIN (-(1LL << 62))

/* Largest number of members any test below puts in one set. */
#define MAX_MEMBERS 16

static int compareSds(const void *a, const void *b) {
    return sdscmp(*(const sds *)a, *(const sds *)b);
}

static int compareCStr(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* setType helpers taking C strings, so tests read like client commands. */
static int setAdd(robj *set, const char *member) {
    sds s = sdsnew(member);
    int ret = setTypeAdd(set, s);
    sdsfree(s);
    return ret;
}

static int setIsMember(robj *set, const char *member) {
    sds s = sdsnew(member);
    int ret = setTypeIsMember(set, s);
    sdsfree(s);
    return ret;
}

static int setRemove(robj *set, const char *member) {
    sds s = sdsnew(member);
    int ret = setTypeRemove(set, s);
    sdsfree(s);
    return ret;
}

/* Looks a C string up directly in the set's hashtable as an sds key. This
 * bypasses the integer classification, so it tells us which form a member is
 * actually stored in. */
static int findAsSds(robj *set, const char *member) {
    sds s = sdsnew(member);
    int ret = hashtableFind((hashtable *)objectGetVal(set), s, NULL);
    sdsfree(s);
    return ret;
}

static int findAsInt(robj *set, long long value) {
    return hashtableFind((hashtable *)objectGetVal(set), setEntryFromInt(value), NULL);
}

/* Iterates the set, storing every member as a sorted array of sds in 'out'
 * (caller frees with freeMembers) and counting how many the iterator yielded
 * as integers rather than strings. Returns the member count. */
static int collectMembers(robj *set, sds *out, int *int_count) {
    int count = 0;
    *int_count = 0;
    setTypeIterator *si = setTypeInitIterator(set);
    char *str;
    size_t len;
    int64_t ll;
    while (setTypeNext(si, &str, &len, &ll) != -1) {
        if (str == NULL) {
            (*int_count)++;
            out[count++] = sdsfromlonglong(ll);
        } else {
            out[count++] = sdsnewlen(str, len);
        }
    }
    setTypeReleaseIterator(si);
    qsort(out, count, sizeof(sds), compareSds);
    return count;
}

static void freeMembers(sds *members, int count) {
    for (int i = 0; i < count; i++) sdsfree(members[i]);
}

/* Asserts that the set holds exactly the given members and that
 * 'expected_ints' of them come out of the iterator as integers. */
static void expectMembers(robj *set, const char **expected, int expected_count, int expected_ints) {
    sds members[MAX_MEMBERS];
    int int_count;
    int count = collectMembers(set, members, &int_count);
    EXPECT_EQ(count, expected_count);
    EXPECT_EQ(int_count, expected_ints);

    const char *sorted[MAX_MEMBERS];
    memcpy(sorted, expected, expected_count * sizeof(const char *));
    qsort(sorted, expected_count, sizeof(const char *), compareCStr);
    for (int i = 0; i < count && i < expected_count; i++) EXPECT_STREQ(members[i], sorted[i]);
    freeMembers(members, count);
}

/*-----------------------------------------------------------------------------
 * Entry encoding
 *----------------------------------------------------------------------------*/

TEST(SetEntryTest, IntegersInRangeRoundTrip) {
    const long long values[] = {0, 1, -1, 42, -42,
                                INT32_MAX, INT32_MIN, (long long)INT32_MAX + 1, (long long)INT32_MIN - 1,
                                1700000000000000000LL, INLINE_MAX, INLINE_MIN};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        long long v = values[i];
        ASSERT_TRUE(setEntryIntFits(v)) << v;
        void *entry = setEntryFromInt(v);
        EXPECT_TRUE(setEntryIsInt(entry)) << v;
        EXPECT_EQ(setEntryToInt(entry), v);
    }
}

TEST(SetEntryTest, IntegersOutOfRangeDoNotFit) {
    EXPECT_FALSE(setEntryIntFits(INLINE_MAX + 1));
    EXPECT_FALSE(setEntryIntFits(INLINE_MIN - 1));
    EXPECT_FALSE(setEntryIntFits(INT64_MAX));
    EXPECT_FALSE(setEntryIntFits(INT64_MIN));
}

TEST(SetEntryTest, SdsIsNeverTaggedAsInt) {
    sds s = sdsnew("4611686018427387903");
    EXPECT_FALSE(setEntryIsInt(s));
    sdsfree(s);
    sds empty = sdsempty();
    EXPECT_FALSE(setEntryIsInt(empty));
    sdsfree(empty);
}

TEST(SetEntryTest, OnlyCanonicalDecimalsInRangeAreInline) {
    const char *accepted_str[] = {"0", "1", "-7", "4611686018427387903", "-4611686018427387904"};
    const long long accepted_val[] = {0, 1, -7, INLINE_MAX, INLINE_MIN};
    for (size_t i = 0; i < sizeof(accepted_str) / sizeof(accepted_str[0]); i++) {
        void *entry = setEntryIntFromString(accepted_str[i], strlen(accepted_str[i]));
        ASSERT_TRUE(entry != NULL) << accepted_str[i];
        EXPECT_TRUE(setEntryIsInt(entry));
        EXPECT_EQ(setEntryToInt(entry), accepted_val[i]) << accepted_str[i];
    }

    /* Not canonical decimal, or an integer outside the inline range. These
     * must all be stored as sds so they stay distinct from the inline forms. */
    const char *rejected[] = {
        "", "foo", "007", "+5", "-0", " 1",
        "1 ", "1.0", "1e3", "0x10",
        "4611686018427387904",  /* INLINE_MAX + 1 */
        "-4611686018427387905", /* INLINE_MIN - 1 */
        "9223372036854775807",  /* INT64_MAX */
        "-9223372036854775808", /* INT64_MIN */
        "99999999999999999999", /* overflows long long */
    };
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        EXPECT_TRUE(setEntryIntFromString(rejected[i], strlen(rejected[i])) == NULL) << "'" << rejected[i] << "'";
    }
}

TEST(SetEntryTest, HashtableTypeCallbacks) {
    void *five = setEntryFromInt(5);
    void *five_again = setEntryFromInt(5);
    void *six = setEntryFromInt(6);
    sds five_str = sdsnew("5");
    sds five_str_again = sdsnew("5");
    sds foo = sdsnew("foo");

    /* Inline integers compare by value. */
    EXPECT_TRUE(setHashtableType.keyCompare(five, five_again));
    EXPECT_FALSE(setHashtableType.keyCompare(five, six));
    EXPECT_EQ(setHashtableType.hashFunction(five), setHashtableType.hashFunction(five_again));

    /* Strings compare by content. */
    EXPECT_TRUE(setHashtableType.keyCompare(five_str, five_str_again));
    EXPECT_FALSE(setHashtableType.keyCompare(five_str, foo));
    EXPECT_EQ(setHashtableType.hashFunction(five_str), setHashtableType.hashFunction(five_str_again));

    /* An inline integer never equals an sds, even one spelling the same
     * digits. The classification rule guarantees such an sds is never stored,
     * so the compare does not need to parse. */
    EXPECT_FALSE(setHashtableType.keyCompare(five, five_str));
    EXPECT_FALSE(setHashtableType.keyCompare(five_str, five));

    /* The destructor frees sds and ignores inline integers. */
    setHashtableType.entryDestructor(five);
    setHashtableType.entryDestructor(five_str);
    setHashtableType.entryDestructor(five_str_again);
    setHashtableType.entryDestructor(foo);
}

TEST(SetEntryTest, EntryToStringFormatsInts) {
    char buf[LONG_STR_SIZE];
    size_t len;
    const char *s = setTypeEntryToString(setEntryFromInt(-1234567890123LL), buf, &len);
    EXPECT_EQ(s, buf);
    EXPECT_EQ(len, strlen("-1234567890123"));
    EXPECT_EQ(memcmp(s, "-1234567890123", len), 0);

    sds foo = sdsnew("foo");
    s = setTypeEntryToString(foo, buf, &len);
    EXPECT_EQ(s, (const char *)foo); /* returned in place, not copied */
    EXPECT_EQ(len, 3u);
    sdsfree(foo);
}

/*-----------------------------------------------------------------------------
 * setType API on hashtable-encoded sets
 *----------------------------------------------------------------------------*/

/* Fixture only to pin the conversion thresholds low for the duration of each
 * test, so sets become hashtable-encoded after two members. */
class SetTypeTest : public ::testing::Test {
  protected:
    size_t saved_intset;
    size_t saved_listpack;

    void SetUp() override {
        saved_intset = server.set_max_intset_entries;
        saved_listpack = server.set_max_listpack_entries;
        server.set_max_intset_entries = 2;
        server.set_max_listpack_entries = 2;
    }
    void TearDown() override {
        server.set_max_intset_entries = saved_intset;
        server.set_max_listpack_entries = saved_listpack;
    }
};

TEST_F(SetTypeTest, HashtableSetStoresIntegersInlineAndStringsAsSds) {
    robj *set = createSetObject();
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);

    EXPECT_EQ(setAdd(set, "5"), 1);
    EXPECT_EQ(setAdd(set, "-7"), 1);
    EXPECT_EQ(setAdd(set, "foo"), 1);
    EXPECT_EQ(setAdd(set, "007"), 1);                 /* not canonical: sds */
    EXPECT_EQ(setAdd(set, "4611686018427387904"), 1); /* out of inline range: sds */
    EXPECT_EQ(setTypeSize(set), 5u);

    /* Integers are stored inline only, strings as sds only. */
    EXPECT_TRUE(findAsInt(set, 5));
    EXPECT_TRUE(findAsInt(set, -7));
    EXPECT_FALSE(findAsSds(set, "5"));
    EXPECT_TRUE(findAsSds(set, "foo"));
    EXPECT_TRUE(findAsSds(set, "007"));
    EXPECT_TRUE(findAsSds(set, "4611686018427387904"));
    EXPECT_FALSE(findAsInt(set, 7)); /* "007" is not the integer 7 */

    /* The public API hides the distinction. */
    EXPECT_EQ(setIsMember(set, "5"), 1);
    EXPECT_EQ(setIsMember(set, "-7"), 1);
    EXPECT_EQ(setIsMember(set, "foo"), 1);
    EXPECT_EQ(setIsMember(set, "007"), 1);
    EXPECT_EQ(setIsMember(set, "4611686018427387904"), 1);
    EXPECT_EQ(setIsMember(set, "7"), 0);
    EXPECT_EQ(setIsMember(set, "+5"), 0);
    EXPECT_EQ(setIsMember(set, "05"), 0);

    /* Integer entry point of the Aux functions (str == NULL, value in llval). */
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 5, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, -7, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 6, 0), 0);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 7, 0), 0);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, INLINE_MAX + 1, 0), 1);

    /* Duplicates in either form are rejected. */
    EXPECT_EQ(setAdd(set, "5"), 0);
    EXPECT_EQ(setAdd(set, "foo"), 0);
    EXPECT_EQ(setAdd(set, "4611686018427387904"), 0);
    EXPECT_EQ(setTypeAddAux(set, NULL, 0, 5, 0), 0);
    EXPECT_EQ(setTypeAddAux(set, NULL, 0, INLINE_MAX + 1, 0), 0);
    EXPECT_EQ(setTypeSize(set), 5u);

    /* Iteration yields inline integers as integers and everything else as
     * strings. */
    const char *expected[] = {"5", "-7", "foo", "007", "4611686018427387904"};
    expectMembers(set, expected, 5, 2);

    /* Random access returns members in either form; every result must be a
     * real member. */
    for (int i = 0; i < 64; i++) {
        char *str;
        size_t len;
        int64_t ll;
        ASSERT_EQ(setTypeRandomElement(set, &str, &len, &ll), OBJ_ENCODING_HASHTABLE);
        if (str == NULL) {
            EXPECT_TRUE(ll == 5 || ll == -7) << ll;
        } else {
            sds m = sdsnewlen(str, len);
            EXPECT_TRUE(setTypeIsMember(set, m)) << m;
            sdsfree(m);
        }
    }

    /* Removal through both entry points. */
    EXPECT_EQ(setRemove(set, "5"), 1);
    EXPECT_EQ(setRemove(set, "5"), 0);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, -7, 0), 1);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, -7, 0), 0);
    EXPECT_EQ(setRemove(set, "007"), 1);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, INLINE_MAX + 1, 0), 1);
    EXPECT_EQ(setTypeSize(set), 1u);
    EXPECT_EQ(setIsMember(set, "foo"), 1);

    decrRefCount(set);
}

TEST_F(SetTypeTest, IntsetConversionLandsIntegersInline) {
    robj *set = createIntsetObject();
    EXPECT_EQ(setAdd(set, "1"), 1);
    EXPECT_EQ(setAdd(set, "2"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_INTSET);

    /* Crossing set-max-intset-entries converts to hashtable with every member
     * inline; no sds is allocated for them. */
    EXPECT_EQ(setAdd(set, "3"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);
    const char *ints[] = {"1", "2", "3"};
    expectMembers(set, ints, 3, 3);
    EXPECT_TRUE(findAsInt(set, 1));
    EXPECT_FALSE(findAsSds(set, "1"));

    /* Adding a string to a hashtable of integers is a plain insert: the
     * encoding does not change and the integers stay inline. */
    EXPECT_EQ(setAdd(set, "x"), 1);
    EXPECT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);
    const char *mixed[] = {"1", "2", "3", "x"};
    expectMembers(set, mixed, 4, 3);

    decrRefCount(set);
}

TEST_F(SetTypeTest, OutOfRangeIntegersConvertToSdsNotInline) {
    robj *set = createIntsetObject();
    EXPECT_EQ(setAdd(set, "9223372036854775807"), 1);
    EXPECT_EQ(setAdd(set, "-9223372036854775808"), 1);
    EXPECT_EQ(setAdd(set, "4611686018427387903"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);

    EXPECT_TRUE(findAsInt(set, INLINE_MAX));
    EXPECT_TRUE(findAsSds(set, "9223372036854775807"));
    EXPECT_TRUE(findAsSds(set, "-9223372036854775808"));

    /* The iterator reports the out-of-range ones as strings, and the public
     * API finds them through both entry points. */
    const char *expected[] = {"9223372036854775807", "-9223372036854775808", "4611686018427387903"};
    expectMembers(set, expected, 3, 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, INT64_MAX, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, INT64_MIN, 0), 1);
    EXPECT_EQ(setIsMember(set, "9223372036854775807"), 1);

    decrRefCount(set);
}

TEST_F(SetTypeTest, DupPreservesInlineIntegers) {
    robj *set = createSetObject();
    setAdd(set, "10");
    setAdd(set, "20");
    setAdd(set, "foo");

    robj *copy = setTypeDup(set);
    ASSERT_EQ(copy->encoding, OBJ_ENCODING_HASHTABLE);
    const char *expected[] = {"10", "20", "foo"};
    expectMembers(copy, expected, 3, 2);
    EXPECT_TRUE(findAsInt(copy, 10));
    EXPECT_FALSE(findAsSds(copy, "10"));

    decrRefCount(copy);
    decrRefCount(set);
}
