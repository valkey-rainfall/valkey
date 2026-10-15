/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>
#include <set>
#include <string>

extern "C" {
#include "server.h"
}

/* Tests for the hashtable encoding of sets, which stores integer members
 * inline as tagged words and everything else as sds. See the setEntry*
 * helpers in server.h for the representation. */

static const long long kInlineMax = (1LL << 62) - 1;
static const long long kInlineMin = -(1LL << 62);

class SetEntryTest : public ::testing::Test {};

TEST_F(SetEntryTest, IntegersInRangeRoundTrip) {
    const long long values[] = {0, 1, -1, 42, -42, INT32_MAX, INT32_MIN, (long long)INT32_MAX + 1,
                                (long long)INT32_MIN - 1, 1700000000000000000LL, kInlineMax, kInlineMin};
    for (long long v : values) {
        ASSERT_TRUE(setEntryIntFits(v)) << v;
        void *entry = setEntryFromInt(v);
        EXPECT_TRUE(setEntryIsInt(entry)) << v;
        EXPECT_EQ(setEntryToInt(entry), v);
    }
}

TEST_F(SetEntryTest, IntegersOutOfRangeDoNotFit) {
    const long long values[] = {kInlineMax + 1, kInlineMin - 1, INT64_MAX, INT64_MIN};
    for (long long v : values) EXPECT_FALSE(setEntryIntFits(v)) << v;
}

TEST_F(SetEntryTest, SdsIsNeverTaggedAsInt) {
    sds s = sdsnew("4611686018427387903");
    EXPECT_FALSE(setEntryIsInt(s));
    sdsfree(s);
    sds empty = sdsempty();
    EXPECT_FALSE(setEntryIsInt(empty));
    sdsfree(empty);
}

TEST_F(SetEntryTest, OnlyCanonicalDecimalsInRangeAreInline) {
    struct {
        const char *str;
        long long value;
    } accepted[] = {
        {"0", 0},
        {"1", 1},
        {"-7", -7},
        {"4611686018427387903", kInlineMax},
        {"-4611686018427387904", kInlineMin},
    };
    for (auto &a : accepted) {
        void *entry = setEntryIntFromString(a.str, strlen(a.str));
        ASSERT_NE(entry, nullptr) << a.str;
        EXPECT_TRUE(setEntryIsInt(entry));
        EXPECT_EQ(setEntryToInt(entry), a.value) << a.str;
    }

    /* Not canonical decimal, or an integer outside the inline range. These
     * must all be stored as sds so they stay distinct from the inline forms. */
    const char *rejected[] = {
        "", "foo", "007", "+5", "-0", " 1", "1 ", "1.0", "1e3", "0x10",
        "4611686018427387904",  /* kInlineMax + 1 */
        "-4611686018427387905", /* kInlineMin - 1 */
        "9223372036854775807",  /* INT64_MAX */
        "-9223372036854775808", /* INT64_MIN */
        "99999999999999999999", /* overflows long long */
    };
    for (const char *r : rejected) EXPECT_EQ(setEntryIntFromString(r, strlen(r)), nullptr) << "'" << r << "'";
}

TEST_F(SetEntryTest, HashtableTypeCallbacks) {
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

TEST_F(SetEntryTest, EntryToStringFormatsInts) {
    char buf[LONG_STR_SIZE];
    size_t len;
    const char *s = setTypeEntryToString(setEntryFromInt(-1234567890123LL), buf, &len);
    EXPECT_EQ(s, buf);
    EXPECT_EQ(std::string(s, len), "-1234567890123");

    sds foo = sdsnew("foo");
    s = setTypeEntryToString(foo, buf, &len);
    EXPECT_EQ(s, (const char *)foo); /* returned in place, not copied */
    EXPECT_EQ(std::string(s, len), "foo");
    sdsfree(foo);
}

class SetTypeTest : public ::testing::Test {
  protected:
    size_t saved_intset, saved_listpack;

    void SetUp() override {
        saved_intset = server.set_max_intset_entries;
        saved_listpack = server.set_max_listpack_entries;
        /* Keep the thresholds tiny so sets convert to hashtable quickly. */
        server.set_max_intset_entries = 2;
        server.set_max_listpack_entries = 2;
    }
    void TearDown() override {
        server.set_max_intset_entries = saved_intset;
        server.set_max_listpack_entries = saved_listpack;
    }

    static int add(robj *set, const char *member) {
        sds s = sdsnew(member);
        int ret = setTypeAdd(set, s);
        sdsfree(s);
        return ret;
    }
    static int isMember(robj *set, const char *member) {
        sds s = sdsnew(member);
        int ret = setTypeIsMember(set, s);
        sdsfree(s);
        return ret;
    }
    static int remove(robj *set, const char *member) {
        sds s = sdsnew(member);
        int ret = setTypeRemove(set, s);
        sdsfree(s);
        return ret;
    }
    /* Returns every member as a string, and counts how many were yielded as
     * integers by the iterator (str == NULL). */
    static std::set<std::string> members(robj *set, int *int_count) {
        std::set<std::string> out;
        *int_count = 0;
        setTypeIterator *si = setTypeInitIterator(set);
        char *str;
        size_t len;
        int64_t ll;
        while (setTypeNext(si, &str, &len, &ll) != -1) {
            if (str == NULL) {
                (*int_count)++;
                out.insert(std::to_string(ll));
            } else {
                out.insert(std::string(str, len));
            }
        }
        setTypeReleaseIterator(si);
        return out;
    }
};

TEST_F(SetTypeTest, HashtableSetStoresIntegersInlineAndStringsAsSds) {
    robj *set = createSetObject();
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);
    hashtable *ht = (hashtable *)objectGetVal(set);

    EXPECT_EQ(add(set, "5"), 1);
    EXPECT_EQ(add(set, "-7"), 1);
    EXPECT_EQ(add(set, "foo"), 1);
    EXPECT_EQ(add(set, "007"), 1);                 /* not canonical: sds */
    EXPECT_EQ(add(set, "4611686018427387904"), 1); /* out of inline range: sds */
    EXPECT_EQ(setTypeSize(set), 5u);

    /* Integers are found by their inline form only, strings by sds only. */
    EXPECT_TRUE(hashtableFind(ht, setEntryFromInt(5), NULL));
    EXPECT_TRUE(hashtableFind(ht, setEntryFromInt(-7), NULL));
    sds five = sdsnew("5");
    EXPECT_FALSE(hashtableFind(ht, five, NULL));
    sdsfree(five);
    sds foo = sdsnew("foo");
    EXPECT_TRUE(hashtableFind(ht, foo, NULL));
    sdsfree(foo);
    sds big = sdsnew("4611686018427387904");
    EXPECT_TRUE(hashtableFind(ht, big, NULL));
    sdsfree(big);

    /* The public API hides the distinction. */
    EXPECT_EQ(isMember(set, "5"), 1);
    EXPECT_EQ(isMember(set, "-7"), 1);
    EXPECT_EQ(isMember(set, "foo"), 1);
    EXPECT_EQ(isMember(set, "007"), 1);
    EXPECT_EQ(isMember(set, "4611686018427387904"), 1);
    EXPECT_EQ(isMember(set, "7"), 0);
    EXPECT_EQ(isMember(set, "+5"), 0);
    EXPECT_EQ(isMember(set, "05"), 0);

    /* Integer entry point of the Aux functions. */
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 5, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, -7, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 6, 0), 0);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, 7, 0), 0); /* "007" is not 7 */
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, kInlineMax + 1, 0), 1);

    /* Duplicates in either form are rejected. */
    EXPECT_EQ(add(set, "5"), 0);
    EXPECT_EQ(add(set, "foo"), 0);
    EXPECT_EQ(add(set, "4611686018427387904"), 0);
    EXPECT_EQ(setTypeAddAux(set, NULL, 0, 5, 0), 0);
    EXPECT_EQ(setTypeAddAux(set, NULL, 0, kInlineMax + 1, 0), 0);
    EXPECT_EQ(setTypeSize(set), 5u);

    /* Iteration yields inline integers as integers and everything else as
     * strings. */
    int int_count;
    std::set<std::string> all = members(set, &int_count);
    EXPECT_EQ(int_count, 2);
    EXPECT_EQ(all, (std::set<std::string>{"5", "-7", "foo", "007", "4611686018427387904"}));

    /* Random access returns both forms. */
    char *str;
    size_t len;
    int64_t ll;
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(setTypeRandomElement(set, &str, &len, &ll), OBJ_ENCODING_HASHTABLE);
        std::string m = str ? std::string(str, len) : std::to_string(ll);
        EXPECT_TRUE(all.count(m)) << m;
    }

    /* Removal through both entry points. */
    EXPECT_EQ(remove(set, "5"), 1);
    EXPECT_EQ(remove(set, "5"), 0);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, -7, 0), 1);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, -7, 0), 0);
    EXPECT_EQ(remove(set, "007"), 1);
    EXPECT_EQ(setTypeRemoveAux(set, NULL, 0, kInlineMax + 1, 0), 1);
    EXPECT_EQ(setTypeSize(set), 1u);
    EXPECT_EQ(isMember(set, "foo"), 1);

    decrRefCount(set);
}

TEST_F(SetTypeTest, IntsetConversionLandsIntegersInline) {
    robj *set = createIntsetObject();
    EXPECT_EQ(add(set, "1"), 1);
    EXPECT_EQ(add(set, "2"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_INTSET);

    /* Crossing set-max-intset-entries converts to hashtable with every member
     * inline; no sds is allocated for them. */
    EXPECT_EQ(add(set, "3"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);
    int int_count;
    EXPECT_EQ(members(set, &int_count), (std::set<std::string>{"1", "2", "3"}));
    EXPECT_EQ(int_count, 3);

    /* Adding a string to a hashtable of integers is a plain insert: the
     * encoding does not change and the integers stay inline. */
    EXPECT_EQ(add(set, "x"), 1);
    EXPECT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);
    EXPECT_EQ(members(set, &int_count), (std::set<std::string>{"1", "2", "3", "x"}));
    EXPECT_EQ(int_count, 3);

    decrRefCount(set);
}

TEST_F(SetTypeTest, OutOfRangeIntegersConvertToSdsNotInline) {
    robj *set = createIntsetObject();
    EXPECT_EQ(add(set, "9223372036854775807"), 1);
    EXPECT_EQ(add(set, "-9223372036854775808"), 1);
    EXPECT_EQ(add(set, "4611686018427387903"), 1);
    ASSERT_EQ(set->encoding, OBJ_ENCODING_HASHTABLE);

    hashtable *ht = (hashtable *)objectGetVal(set);
    EXPECT_TRUE(hashtableFind(ht, setEntryFromInt(kInlineMax), NULL));
    sds max = sdsnew("9223372036854775807");
    sds min = sdsnew("-9223372036854775808");
    EXPECT_TRUE(hashtableFind(ht, max, NULL));
    EXPECT_TRUE(hashtableFind(ht, min, NULL));
    sdsfree(max);
    sdsfree(min);

    /* The iterator still reports them as strings, and the public API finds
     * them through both entry points. */
    int int_count;
    EXPECT_EQ(members(set, &int_count),
              (std::set<std::string>{"9223372036854775807", "-9223372036854775808", "4611686018427387903"}));
    EXPECT_EQ(int_count, 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, INT64_MAX, 0), 1);
    EXPECT_EQ(setTypeIsMemberAux(set, NULL, 0, INT64_MIN, 0), 1);
    EXPECT_EQ(isMember(set, "9223372036854775807"), 1);

    decrRefCount(set);
}

TEST_F(SetTypeTest, DupPreservesInlineIntegers) {
    robj *set = createSetObject();
    add(set, "10");
    add(set, "20");
    add(set, "foo");

    robj *copy = setTypeDup(set);
    ASSERT_EQ(copy->encoding, OBJ_ENCODING_HASHTABLE);
    int int_count;
    EXPECT_EQ(members(copy, &int_count), (std::set<std::string>{"10", "20", "foo"}));
    EXPECT_EQ(int_count, 2);
    EXPECT_TRUE(hashtableFind((hashtable *)objectGetVal(copy), setEntryFromInt(10), NULL));

    decrRefCount(copy);
    decrRefCount(set);
}
