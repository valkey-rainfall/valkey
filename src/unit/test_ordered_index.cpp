/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "server.h"
#include "ordered_index.h"
}

/* Undefine min/max macros from server.h to avoid conflicts */
#undef min
#undef max

#include <cmath>
#include <cstring>

/* Clean up shared lex sentinels allocated by OrderedIndexTest::SetUp(). */
static void cleanupSharedSentinels(void) __attribute__((destructor));
static void cleanupSharedSentinels(void) {
    if (shared.minstring) {
        sdsfree(shared.minstring);
        shared.minstring = NULL;
    }
    if (shared.maxstring) {
        sdsfree(shared.maxstring);
        shared.maxstring = NULL;
    }
}

/* ---- C-style test helpers ---- */

#define TEST_MIN(a, b) ((a) < (b) ? (a) : (b))
#define TEST_MAX(a, b) ((a) > (b) ? (a) : (b))

/* Collect all elements from an ordered index into a pre-allocated sds array. */
static sds *collectIndexToSds(OrderedIndex *oi, size_t *out_n) {
    size_t n = orderedIndexLength(oi);
    *out_n = n;
    if (n == 0) return NULL;
    sds *arr = (sds *)zmalloc(sizeof(sds) * n);
    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    for (size_t i = 0; i < n; i++) {
        OrderedIndexItem *pos = orderedIndexNext(&iter);
        const char *ptr;
        size_t len;
        orderedIndexGetElementRaw(pos, &ptr, &len);
        arr[i] = sdsnewlen(ptr, len);
    }
    orderedIndexResetIterator(&iter);
    return arr;
}

static void freeSdsArray(sds *arr, size_t n) {
    for (size_t i = 0; i < n; i++) sdsfree(arr[i]);
    zfree(arr);
}

#define ASSERT_SDS_ARRAY_EQ(arr, n, ...)                \
    do {                                                \
        const char *_exp[] = {__VA_ARGS__};             \
        size_t _exp_n = sizeof(_exp) / sizeof(_exp[0]); \
        ASSERT_EQ((size_t)(n), _exp_n);                 \
        for (size_t _i = 0; _i < _exp_n; _i++) {        \
            ASSERT_STREQ(arr[_i], _exp[_i]);            \
        }                                               \
    } while (0)

static int sdsArrayCmp(const void *a, const void *b) {
    return sdscmp(*(sds *)a, *(sds *)b);
}

static void sortSdsArray(sds *arr, size_t n) {
    qsort(arr, n, sizeof(sds), sdsArrayCmp);
}

static void reverseDoubleArray(double *arr, size_t n) {
    for (size_t i = 0; i < n / 2; i++) {
        double tmp = arr[i];
        arr[i] = arr[n - 1 - i];
        arr[n - 1 - i] = tmp;
    }
}

static ::testing::AssertionResult verifyIntegrity(OrderedIndex *oi) {
    char errmsg[256];
    if (orderedIndexVerifyIntegrity(oi, errmsg, sizeof(errmsg)))
        return ::testing::AssertionResult(true);
    return ::testing::AssertionFailure() << errmsg;
}

#define VERIFY_INTEGRITY(idx_ptr) ASSERT_TRUE(verifyIntegrity(idx_ptr))

static const double POS_INF = (double)INFINITY;
static const double NEG_INF = (double)-INFINITY;

/* ========== Shared test data ========== */

static const char *FRUITS[] = {"apple", "banana", "cherry", "date", "elderberry"};
static const int FRUITS_COUNT = 5;
static const char *NATO[] = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"};
static const int NATO_COUNT = 6;

/* ========== Test fixture ========== */

class OrderedIndexTest : public ::testing::Test {
  protected:
    OrderedIndex *oi = nullptr;

    void SetUp() override {
        if (!shared.minstring) shared.minstring = sdsnew("minstring");
        if (!shared.maxstring) shared.maxstring = sdsnew("maxstring");
        oi = orderedIndexCreate();
    }
    void TearDown() override {
        if (oi) orderedIndexFree(oi);
    }

    OrderedIndexItem *insert(double score, const char *ele) {
        sds s = sdsnew(ele);
        OrderedIndexItem *node = orderedIndexInsert(oi, score, s, sdslen(s));
        sdsfree(s);
        return node;
    }

    void populateSequential(int n) {
        for (int i = 0; i < n; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "key%d", i);
            insert((double)i, buf);
        }
    }

    OrderedIndexItem *assertNextScore(OrderedIndexIterator *iter, double expected) {
        OrderedIndexItem *pos = orderedIndexNext(iter);
        EXPECT_NE(pos, nullptr);
        if (pos) {
            EXPECT_DOUBLE_EQ(orderedIndexGetScore(pos), expected);
        }
        return pos;
    }

    OrderedIndexItem *assertPrevScore(OrderedIndexIterator *iter, double expected) {
        OrderedIndexItem *pos = orderedIndexPrev(iter);
        EXPECT_NE(pos, nullptr);
        if (pos) {
            EXPECT_DOUBLE_EQ(orderedIndexGetScore(pos), expected);
        }
        return pos;
    }

    void assertElement(OrderedIndexItem *node, const char *expected) {
        const char *ptr;
        size_t len;
        orderedIndexGetElementRaw(node, &ptr, &len);
        ASSERT_EQ(len, strlen(expected));
        ASSERT_EQ(memcmp(ptr, expected, len), 0);
    }

    void assertScore(OrderedIndexItem *node, double expected) {
        ASSERT_DOUBLE_EQ(orderedIndexGetScore(node), expected);
    }

    unsigned long deleteLexRange(const char *min_str, const char *max_str, int min_ex, int max_ex, OrderedIndexOnDelete cb, void *ctx) {
        sds min = sdsnew(min_str);
        sds max = sdsnew(max_str);
        unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, min_ex, max_ex, cb, ctx);
        sdsfree(min);
        sdsfree(max);
        return deleted;
    }

    unsigned long countLexRange(const char *min_str, const char *max_str, int min_ex, int max_ex) {
        sds min = sdsnew(min_str);
        sds max = sdsnew(max_str);
        unsigned long count = orderedIndexCountLexRange(oi, min, max, min_ex, max_ex);
        sdsfree(min);
        sdsfree(max);
        return count;
    }

    void seekToLexRange(OrderedIndexIterator *it, const char *min_str, const char *max_str, int min_ex, int max_ex, long offset) {
        sds min = sdsnew(min_str);
        sds max = sdsnew(max_str);
        orderedIndexSeekToLexRange(it, min, max, min_ex, max_ex, offset);
        sdsfree(min);
        sdsfree(max);
    }

    void assertAllElements(const char *expected[], size_t count) {
        OrderedIndexIterator it;
        orderedIndexInitIterator(&it, oi);
        OrderedIndexItem *pos;
        size_t i = 0;
        while ((pos = orderedIndexNext(&it)) != NULL) {
            ASSERT_LT(i, count) << "More elements than expected";
            assertElement(pos, expected[i]);
            i++;
        }
        orderedIndexResetIterator(&it);
        ASSERT_EQ(i, count) << "Fewer elements than expected";
    }

    void verifyOI() {
        ASSERT_TRUE(verifyIntegrity(oi));
    }
};

#define ASSERT_ALL_ELEMENTS(...)                                     \
    do {                                                             \
        const char *_elems[] = {__VA_ARGS__};                        \
        assertAllElements(_elems, sizeof(_elems) / sizeof(*_elems)); \
    } while (0)

/* ========== Basic tests ========== */

TEST_F(OrderedIndexTest, CreateFree) {
    ASSERT_NE(oi, nullptr);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    verifyOI();

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, InsertSingle) {
    OrderedIndexItem *node = insert(1.0, "test");
    verifyOI();

    ASSERT_NE(node, nullptr);
    ASSERT_EQ(orderedIndexLength(oi), 1UL);
    assertScore(node, 1.0);
    assertElement(node, "test");

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexNext(&iter), node);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, InsertMultipleOrdered) {
    populateSequential(10);

    ASSERT_EQ(orderedIndexLength(oi), 10UL);
    verifyOI();

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    for (int i = 0; i < 10; i++) {
        ASSERT_NE((pos = orderedIndexNext(&iter)), nullptr);
        ASSERT_DOUBLE_EQ(orderedIndexGetScore(pos), (double)i);
    }
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    for (int i = 9; i >= 0; i--) {
        ASSERT_NE((pos = orderedIndexPrev(&iter)), nullptr);
        ASSERT_DOUBLE_EQ(orderedIndexGetScore(pos), (double)i);
    }
    ASSERT_EQ(orderedIndexPrev(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, DuplicateScores) {
    for (int i = 0; i < 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert(1.0, buf);
    }

    ASSERT_EQ(orderedIndexLength(oi), 5UL);
    verifyOI();

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    for (int i = 0; i < 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        pos = assertNextScore(&iter, 1.0);
        assertElement(pos, buf);
    }
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, IndexOperations) {
    OrderedIndexItem *nodes[10];

    for (int i = 0; i < 10; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        nodes[i] = insert((double)i, buf);
    }
    verifyOI();

    for (int i = 0; i < 10; i++) {
        unsigned long idx = orderedIndexGetIndex(oi, nodes[i]);
        ASSERT_EQ(idx, (unsigned long)i);
    }

    for (int i = 0; i < 10; i++) {
        OrderedIndexItem *node = orderedIndexGetByIndex(oi, i);
        ASSERT_EQ(node, nodes[i]);
    }
}

TEST_F(OrderedIndexTest, Delete) {
    OrderedIndexItem *nodes[5];
    for (int i = 0; i < 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        nodes[i] = insert((double)i, buf);
    }
    ASSERT_EQ(orderedIndexLength(oi), 5UL);

    orderedIndexDelete(oi, nodes[2]);
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    verifyOI();

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 0.0);
    assertNextScore(&iter, 1.0);
    assertNextScore(&iter, 3.0);
    assertNextScore(&iter, 4.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, PopFirst) {
    ASSERT_EQ(orderedIndexPopFirst(oi), nullptr);

    populateSequential(5);
    ASSERT_EQ(orderedIndexLength(oi), 5UL);

    OrderedIndexItem *item = orderedIndexPopFirst(oi);
    ASSERT_NE(item, nullptr);
    assertScore(item, 0.0);
    assertElement(item, "key0");
    orderedIndexFreeItem(item);
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    verifyOI();

    item = orderedIndexPopFirst(oi);
    assertScore(item, 1.0);
    orderedIndexFreeItem(item);
    ASSERT_EQ(orderedIndexLength(oi), 3UL);
    verifyOI();
}

TEST_F(OrderedIndexTest, PopLast) {
    ASSERT_EQ(orderedIndexPopLast(oi), nullptr);

    populateSequential(5);
    ASSERT_EQ(orderedIndexLength(oi), 5UL);

    OrderedIndexItem *item = orderedIndexPopLast(oi);
    ASSERT_NE(item, nullptr);
    assertScore(item, 4.0);
    assertElement(item, "key4");
    orderedIndexFreeItem(item);
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    verifyOI();

    item = orderedIndexPopLast(oi);
    assertScore(item, 3.0);
    orderedIndexFreeItem(item);
    ASSERT_EQ(orderedIndexLength(oi), 3UL);
    verifyOI();
}

TEST_F(OrderedIndexTest, GetFirstAndLast) {
    ASSERT_EQ(orderedIndexGetFirst(oi), nullptr);
    ASSERT_EQ(orderedIndexGetLast(oi), nullptr);

    insert(2.0, "bravo");
    insert(1.0, "alpha");
    insert(3.0, "charlie");

    assertElement(orderedIndexGetFirst(oi), "alpha");
    assertScore(orderedIndexGetFirst(oi), 1.0);
    assertElement(orderedIndexGetLast(oi), "charlie");
    assertScore(orderedIndexGetLast(oi), 3.0);

    orderedIndexDelete(oi, orderedIndexGetFirst(oi));
    orderedIndexDelete(oi, orderedIndexGetLast(oi));
    ASSERT_EQ(orderedIndexLength(oi), 1UL);
    assertElement(orderedIndexGetFirst(oi), "bravo");
    assertElement(orderedIndexGetLast(oi), "bravo");
}

TEST_F(OrderedIndexTest, UpdateScore) {
    OrderedIndexItem *node1 = insert(1.0, "key1");
    OrderedIndexItem *node2 = insert(2.0, "key2");
    insert(3.0, "key3");

    OrderedIndexItem *updated = orderedIndexUpdateScore(oi, node2, 4.0);
    ASSERT_NE(updated, nullptr);
    assertScore(updated, 4.0);
    verifyOI();
    assertElement(updated, "key2");

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 1.0);
    assertNextScore(&iter, 3.0);
    pos = assertNextScore(&iter, 4.0);
    assertElement(pos, "key2");
    orderedIndexResetIterator(&iter);

    updated = orderedIndexUpdateScore(oi, node1, 1.0);
    assertScore(updated, 1.0);
    verifyOI();
}

TEST_F(OrderedIndexTest, DeleteRangeByScore) {
    populateSequential(10);

    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 3.0, 6.0, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 4UL);
    ASSERT_EQ(orderedIndexLength(oi), 6UL);
    verifyOI();

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    for (int i = 0; i < 3; i++) {
        assertNextScore(&iter, (double)i);
    }
    for (int i = 7; i < 10; i++) {
        assertNextScore(&iter, (double)i);
    }
    orderedIndexResetIterator(&iter);

    deleted = orderedIndexDeleteRangeByScore(oi, 2.0, 8.0, 1, 1, NULL, NULL);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(orderedIndexLength(oi), 5UL);
    verifyOI();
}

TEST_F(OrderedIndexTest, DeleteRangeByIndex) {
    populateSequential(10);

    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 2, 4, NULL, NULL);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(orderedIndexLength(oi), 7UL);
    verifyOI();

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 0.0);
    orderedIndexResetIterator(&iter);

    OrderedIndexItem *node = orderedIndexGetByIndex(oi, 2);
    assertScore(node, 5.0);
}

TEST_F(OrderedIndexTest, MixedOperationsIndexIntegrity) {
    OrderedIndexItem *nodes[100];

    for (int i = 0; i < 100; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        nodes[i] = insert((double)i, buf);
    }

    for (int i = 2; i < 100; i += 3) {
        orderedIndexDelete(oi, nodes[i]);
        nodes[i] = NULL;
    }
    verifyOI();

    nodes[10] = orderedIndexUpdateScore(oi, nodes[10], 150.0);
    nodes[19] = orderedIndexUpdateScore(oi, nodes[19], 160.0);
    verifyOI();

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    unsigned long expectedIdx = 0;
    while (((pos = orderedIndexNext(&iter)) != NULL)) {
        unsigned long actualIdx = orderedIndexGetIndex(oi, pos);
        ASSERT_EQ(actualIdx, expectedIdx);
        expectedIdx++;
    }
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, BackwardTraversalAfterDeletions) {
    OrderedIndexItem *nodes[20];

    for (int i = 0; i < 20; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        nodes[i] = insert((double)i, buf);
    }

    orderedIndexDelete(oi, nodes[5]);
    orderedIndexDelete(oi, nodes[10]);
    orderedIndexDelete(oi, nodes[15]);
    verifyOI();

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    int expected_scores[] = {19, 18, 17, 16, 14, 13, 12, 11, 9, 8, 7, 6, 4, 3, 2, 1, 0};

    for (int i = 0; i < 17; i++) {
        assertPrevScore(&iter, (double)expected_scores[i]);
    }
    ASSERT_EQ(orderedIndexPrev(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, LexicographicEdgeCases) {
    insert(1.0, "z");
    insert(1.0, "");
    insert(1.0, "a");

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    pos = assertNextScore(&iter, 1.0);
    assertElement(pos, "");
    pos = assertNextScore(&iter, 1.0);
    assertElement(pos, "a");
    pos = assertNextScore(&iter, 1.0);
    assertElement(pos, "z");
    orderedIndexResetIterator(&iter);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    char long_buf[1024];
    memset(long_buf, 'x', 1023);
    long_buf[1023] = '\0';
    insert(1.0, long_buf);
    insert(1.0, "short");

    orderedIndexInitIterator(&iter, oi);
    pos = assertNextScore(&iter, 1.0);
    assertElement(pos, "short");
    pos = assertNextScore(&iter, 1.0);
    assertElement(pos, long_buf);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, RangeBoundaryPrecision) {
    double base = 1.0;
    double epsilon = 1e-10;

    insert(base, "at_base");
    insert(base + epsilon, "at_base_plus_epsilon");
    insert(base + 2 * epsilon, "at_base_plus_2epsilon");

    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, base, base + 2 * epsilon, 1, 1, NULL, NULL);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, base);
    assertNextScore(&iter, base + 2 * epsilon);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SpecialDoubleValues) {
    insert(NEG_INF, "neg_inf");
    insert(POS_INF, "pos_inf");
    insert(0.0, "zero");
    insert(1.0, "one");

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, NEG_INF);
    assertNextScore(&iter, 0.0);
    assertNextScore(&iter, 1.0);
    assertNextScore(&iter, POS_INF);
    orderedIndexResetIterator(&iter);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    insert(0.0, "pos_zero");
    insert(-0.0, "neg_zero");

    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    orderedIndexInitIterator(&iter, oi);
    pos = assertNextScore(&iter, 0.0);
    assertElement(pos, "neg_zero");
    pos = assertNextScore(&iter, 0.0);
    assertElement(pos, "pos_zero");
    orderedIndexResetIterator(&iter);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    double denorm = 1e-320;
    insert(denorm, "denorm");
    insert(1.0, "normal");

    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    orderedIndexInitIterator(&iter, oi);
    pos = assertNextScore(&iter, denorm);
    ASSERT_TRUE(orderedIndexGetScore(pos) < 1.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, EmptyIndexOperations) {
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    ASSERT_EQ(orderedIndexPrev(&iter), nullptr);
    orderedIndexResetIterator(&iter);
    ASSERT_EQ(orderedIndexGetByIndex(oi, 0), nullptr);
}

TEST_F(OrderedIndexTest, DeleteEdgeCases) {
    OrderedIndexItem *node = insert(1.0, "only");
    orderedIndexDelete(oi, node);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    verifyOI();
    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    OrderedIndexItem *nodes[3];
    for (int i = 0; i < 3; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        nodes[i] = insert((double)i, buf);
    }
    orderedIndexDelete(oi, nodes[0]);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    verifyOI();
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 1.0);
    orderedIndexResetIterator(&iter);

    orderedIndexDelete(oi, nodes[2]);
    ASSERT_EQ(orderedIndexLength(oi), 1UL);
    verifyOI();
    orderedIndexInitIterator(&iter, oi);
    assertPrevScore(&iter, 1.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, IndexEdgeCases) {
    populateSequential(5);

    ASSERT_EQ(orderedIndexGetByIndex(oi, 5), nullptr);
    ASSERT_EQ(orderedIndexGetByIndex(oi, 99), nullptr);
    ASSERT_NE(orderedIndexGetByIndex(oi, 0), nullptr);
    ASSERT_NE(orderedIndexGetByIndex(oi, 4), nullptr);
}

TEST_F(OrderedIndexTest, DuplicateInsert) {
    OrderedIndexItem *node1 = insert(1.0, "duplicate");
    OrderedIndexItem *node2 = insert(1.0, "duplicate");

    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    ASSERT_NE(node1, node2);
}

TEST_F(OrderedIndexTest, UpdateScoreEdgeCases) {
    populateSequential(5);

    OrderedIndexItem *first = orderedIndexGetByIndex(oi, 0);
    OrderedIndexItem *updated = orderedIndexUpdateScore(oi, first, 10.0);
    assertScore(updated, 10.0);
    verifyOI();
    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexPrev(&iter), updated);
    orderedIndexResetIterator(&iter);

    OrderedIndexItem *last = orderedIndexGetByIndex(oi, orderedIndexLength(oi) - 1);
    updated = orderedIndexUpdateScore(oi, last, -1.0);
    assertScore(updated, -1.0);
    verifyOI();
    orderedIndexInitIterator(&iter, oi);
    ASSERT_EQ(orderedIndexNext(&iter), updated);
    orderedIndexResetIterator(&iter);

    OrderedIndexItem *middle = orderedIndexGetByIndex(oi, 2);
    updated = orderedIndexUpdateScore(oi, middle, 0.5);
    assertScore(updated, 0.5);
    verifyOI();
    ASSERT_EQ(orderedIndexGetIndex(oi, updated), 1UL);

    OrderedIndexItem *n = orderedIndexGetByIndex(oi, 3);
    unsigned long idx_before = orderedIndexGetIndex(oi, n);
    updated = orderedIndexUpdateScore(oi, n, orderedIndexGetScore(n) + 0.1);
    ASSERT_EQ(orderedIndexGetIndex(oi, updated), idx_before);
    verifyOI();
}

TEST_F(OrderedIndexTest, RangeDeleteEdgeCases) {
    populateSequential(10);

    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 5.0, 4.0, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(orderedIndexLength(oi), 10UL);

    deleted = orderedIndexDeleteRangeByScore(oi, 10.5, 11.5, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(orderedIndexLength(oi), 10UL);

    deleted = orderedIndexDeleteRangeByIndex(oi, 0, 1, NULL, NULL);
    ASSERT_EQ(deleted, 2UL);
    verifyOI();
    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 2.0);
    orderedIndexResetIterator(&iter);

    unsigned long len = orderedIndexLength(oi);
    deleted = orderedIndexDeleteRangeByIndex(oi, len - 2, len - 1, NULL, NULL);
    ASSERT_EQ(deleted, 2UL);
    verifyOI();
    orderedIndexInitIterator(&iter, oi);
    assertPrevScore(&iter, 7.0);
    orderedIndexResetIterator(&iter);

    deleted = orderedIndexDeleteRangeByScore(oi, -100.0, 100.0, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 6UL);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    verifyOI();
}

TEST_F(OrderedIndexTest, TraversalEdgeCases) {
    insert(1.0, "single");

    OrderedIndexIterator iter;
    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 1.0);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    assertPrevScore(&iter, 1.0);
    ASSERT_EQ(orderedIndexPrev(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekToIndex) {
    for (int i = 1; i <= 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert((double)i, buf);
    }

    OrderedIndexIterator iter;

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 0);
    assertNextScore(&iter, 2.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 0);
    assertPrevScore(&iter, 1.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 1);
    assertNextScore(&iter, 3.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 1);
    assertPrevScore(&iter, 2.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 2);
    assertNextScore(&iter, 4.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 2);
    assertPrevScore(&iter, 3.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 4);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToIndex(&iter, 4);
    assertPrevScore(&iter, 5.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, ReverseIteration) {
    for (int i = 1; i <= 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert((double)i, buf);
    }

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;

    orderedIndexInitIterator(&iter, oi);
    int count = 0;
    double expected = 5.0;
    while (((pos = orderedIndexPrev(&iter)) != NULL)) {
        assertScore(pos, expected);
        expected -= 1.0;
        count++;
    }
    ASSERT_EQ(count, 5);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    assertPrevScore(&iter, 5.0);
    assertNextScore(&iter, 5.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    assertNextScore(&iter, 1.0);
    assertPrevScore(&iter, 1.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekToScoreRange) {
    for (int i = 0; i < 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert((double)(i * 2), buf);
    }

    OrderedIndexIterator iter;

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, 0);
    assertNextScore(&iter, 2.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, 1);
    assertNextScore(&iter, 4.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, -1);
    assertPrevScore(&iter, 6.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 1, 1, 0);
    assertNextScore(&iter, 4.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 10.0, 20.0, 0, 0, 0);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, -20.0, -10.0, 0, 0, 0);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, 10);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, -10);
    ASSERT_EQ(orderedIndexPrev(&iter), nullptr);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 6.0, 0, 0, -2);
    assertPrevScore(&iter, 4.0);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 6.0, 2.0, 0, 0, 0);
    ASSERT_EQ(orderedIndexNext(&iter), nullptr);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekToScoreRangeIteration) {
    populateSequential(10);

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 3.0, 7.0, 0, 0, 0);
    int count = 0;
    double expected = 3.0;
    while (((pos = orderedIndexNext(&iter)) != NULL) && orderedIndexGetScore(pos) <= 7.0) {
        assertScore(pos, expected);
        expected += 1.0;
        count++;
    }
    ASSERT_EQ(count, 5);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 3.0, 7.0, 0, 0, -1);
    count = 0;
    expected = 7.0;
    while (((pos = orderedIndexPrev(&iter)) != NULL) && orderedIndexGetScore(pos) >= 3.0) {
        assertScore(pos, expected);
        expected -= 1.0;
        count++;
    }
    ASSERT_EQ(count, 5);
    orderedIndexResetIterator(&iter);

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, 2.0, 8.0, 0, 0, 2);
    assertNextScore(&iter, 4.0);
    assertNextScore(&iter, 5.0);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekInfReverseIteration) {
    for (int i = 1; i <= 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert((double)i, buf);
    }

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, NEG_INF, POS_INF, 0, 0, -1);
    int count = 0;
    double expected = 5.0;
    while (((pos = orderedIndexPrev(&iter)) != NULL)) {
        assertScore(pos, expected);
        expected -= 1.0;
        count++;
    }
    ASSERT_EQ(count, 5);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekInfForwardIteration) {
    for (int i = 1; i <= 5; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "key%d", i);
        insert((double)i, buf);
    }

    OrderedIndexIterator iter;
    OrderedIndexItem *pos;

    orderedIndexInitIterator(&iter, oi);
    orderedIndexSeekToScoreRange(&iter, NEG_INF, POS_INF, 0, 0, 0);
    int count = 0;
    double expected = 1.0;
    while (((pos = orderedIndexNext(&iter)) != NULL)) {
        assertScore(pos, expected);
        expected += 1.0;
        count++;
    }
    ASSERT_EQ(count, 5);
    orderedIndexResetIterator(&iter);
}

TEST_F(OrderedIndexTest, SeekToLexRange) {
    for (int i = 0; i < FRUITS_COUNT; i++) insert(1.0, FRUITS[i]);

    OrderedIndexIterator it;
    OrderedIndexItem *pos;

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "banana", "date", 0, 0, 0);
    ASSERT_NE((pos = orderedIndexNext(&it)), nullptr);
    assertElement(pos, "banana");
    orderedIndexResetIterator(&it);

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "banana", "date", 0, 0, 1);
    ASSERT_NE((pos = orderedIndexNext(&it)), nullptr);
    assertElement(pos, "cherry");
    orderedIndexResetIterator(&it);

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "banana", "date", 0, 0, -1);
    ASSERT_NE((pos = orderedIndexPrev(&it)), nullptr);
    assertElement(pos, "date");
    orderedIndexResetIterator(&it);

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "banana", "date", 1, 1, 0);
    ASSERT_NE((pos = orderedIndexNext(&it)), nullptr);
    assertElement(pos, "cherry");
    orderedIndexResetIterator(&it);

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "zzz", "zzzz", 0, 0, 0);
    ASSERT_EQ(orderedIndexNext(&it), nullptr);
    orderedIndexResetIterator(&it);

    orderedIndexInitIterator(&it, oi);
    seekToLexRange(&it, "banana", "date", 0, 0, 10);
    ASSERT_EQ(orderedIndexNext(&it), nullptr);
    orderedIndexResetIterator(&it);
}

TEST_F(OrderedIndexTest, DeleteRangeByLexInclusive) {
    for (int i = 0; i < FRUITS_COUNT; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(deleteLexRange("banana", "date", 0, 0, NULL, NULL), 3UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    verifyOI();
    ASSERT_ALL_ELEMENTS("apple", "elderberry");
}

TEST_F(OrderedIndexTest, DeleteRangeByLexExclusive) {
    for (int i = 0; i < FRUITS_COUNT; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(deleteLexRange("banana", "date", 1, 1, NULL, NULL), 1UL);
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    ASSERT_ALL_ELEMENTS("apple", "banana", "date", "elderberry");
}

TEST_F(OrderedIndexTest, DeleteRangeByLex_EmptyRange) {
    for (int i = 0; i < 3; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(deleteLexRange("zzz", "aaa", 0, 0, NULL, NULL), 0UL);
    ASSERT_EQ(orderedIndexLength(oi), 3UL);
}

TEST_F(OrderedIndexTest, DeleteRangeByLex_All) {
    for (int i = 0; i < 3; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(deleteLexRange("a", "z", 0, 0, NULL, NULL), 3UL);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
}

TEST_F(OrderedIndexTest, DeleteRangeByLex_SingleElement) {
    for (int i = 0; i < 3; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(deleteLexRange("banana", "banana", 0, 0, NULL, NULL), 1UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);
    ASSERT_ALL_ELEMENTS("apple", "cherry");
}

TEST_F(OrderedIndexTest, DeleteRangeByLexPreservesOutside) {
    for (int i = 0; i < NATO_COUNT; i++) insert(1.0, NATO[i]);

    ASSERT_EQ(deleteLexRange("charlie", "delta", 0, 0, NULL, NULL), 2UL);
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    ASSERT_ALL_ELEMENTS("alpha", "bravo", "echo", "foxtrot");

    OrderedIndexIterator it;
    orderedIndexInitIterator(&it, oi);
    OrderedIndexItem *pos;
    while ((pos = orderedIndexNext(&it)) != NULL) {
        assertScore(pos, 1.0);
    }
    orderedIndexResetIterator(&it);

    for (unsigned long r = 0; r < 4; r++) {
        OrderedIndexItem *node = orderedIndexGetByIndex(oi, r);
        ASSERT_NE(node, nullptr);
        ASSERT_EQ(orderedIndexGetIndex(oi, node), r);
    }
}

TEST_F(OrderedIndexTest, LexRangeSentinels) {
    insert(0.0, "alpha");
    insert(0.0, "bravo");
    insert(0.0, "charlie");
    insert(0.0, "delta");
    insert(0.0, "echo");

    sds charlie = sdsnew("charlie");

    ASSERT_EQ(countLexRange("minstring", "maxstring", 0, 0), 0UL);
    ASSERT_EQ(orderedIndexCountLexRange(oi, shared.minstring, shared.maxstring, 0, 0), 5UL);
    ASSERT_EQ(orderedIndexCountLexRange(oi, shared.minstring, charlie, 0, 0), 3UL);
    ASSERT_EQ(orderedIndexCountLexRange(oi, charlie, shared.maxstring, 0, 0), 3UL);

    ASSERT_EQ(orderedIndexCountLexRange(oi, shared.maxstring, shared.minstring, 0, 0), 0UL);
    ASSERT_EQ(orderedIndexCountLexRange(oi, charlie, shared.minstring, 0, 0), 0UL);

    OrderedIndexIterator it;
    orderedIndexInitIterator(&it, oi);
    orderedIndexSeekToLexRange(&it, shared.minstring, shared.maxstring, 0, 0, 0);
    assertNextScore(&it, 0.0);
    assertNextScore(&it, 0.0);
    assertNextScore(&it, 0.0);
    assertNextScore(&it, 0.0);
    assertNextScore(&it, 0.0);
    ASSERT_EQ(orderedIndexNext(&it), nullptr);
    orderedIndexResetIterator(&it);

    ASSERT_EQ(orderedIndexDeleteRangeByLex(oi, shared.minstring, shared.maxstring, 0, 0, NULL, NULL), 5UL);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);

    sdsfree(charlie);
}

/* ========== Randomized property tests ========== */

extern char *seed;
static uint32_t test_fuzz_seed(void) {
    uint32_t s = seed ? (uint32_t)atoi(seed) : 42;
    printf("  [fuzz seed: %u]\n", s);
    return s;
}

static uint32_t test_rand_next(uint32_t *state) {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    return *state;
}

static int test_rand_range(uint32_t *state, int min, int max) {
    return min + (int)(test_rand_next(state) % (uint32_t)(max - min + 1));
}

static double test_rand_double(uint32_t *state, double lo, double hi) {
    return lo + (hi - lo) * ((double)test_rand_next(state) / (double)UINT32_MAX);
}

struct RandomIndexEntry {
    OrderedIndexItem *node;
    double score;
    sds element;
};

static sds test_random_element(uint32_t *state, int maxLen) {
    int len = test_rand_range(state, 1, maxLen);
    sds s = sdsnewlen(NULL, len);
    for (int i = 0; i < len; i++) s[i] = (char)test_rand_range(state, 'a', 'z');
    return s;
}

static double test_random_score(uint32_t *state) {
    return test_rand_double(state, -1e6, 1e6);
}

static RandomIndexEntry *test_build_random_index(OrderedIndex *oi, uint32_t *state, int count) {
    RandomIndexEntry *entries = (RandomIndexEntry *)zmalloc(sizeof(RandomIndexEntry) * count);
    for (int i = 0; i < count; i++) {
        double score = test_random_score(state);
        sds elem = test_random_element(state, 16);
        elem = sdscatfmt(elem, "%i", i);
        OrderedIndexItem *node = orderedIndexInsert(oi, score, elem, sdslen(elem));
        entries[i] = {node, score, elem};
    }
    return entries;
}

static void freeRandomEntries(RandomIndexEntry *entries, int count) {
    for (int i = 0; i < count; i++) sdsfree(entries[i].element);
    zfree(entries);
}

TEST_F(OrderedIndexTest, RandomizedInsertAndTraversal) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 1, 50);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)n);
        verifyOI();

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        int count = 0;
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            double s = orderedIndexGetScore(pos);
            ASSERT_GE(s, prevScore);
            prevScore = s;
            count++;
        }
        ASSERT_EQ(count, n);
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedBackwardTraversal) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 1, 50);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        int count = 0;
        double prevScore = POS_INF;
        while (((pos = orderedIndexPrev(&iter)) != NULL)) {
            double s = orderedIndexGetScore(pos);
            ASSERT_LE(s, prevScore);
            prevScore = s;
            count++;
        }
        ASSERT_EQ(count, n);
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedScoreRetrieval) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 1, 50);

        RandomIndexEntry *entries = test_build_random_index(oi, &rng, n);

        for (int i = 0; i < n; i++) {
            assertScore(entries[i].node, entries[i].score);
        }
        freeRandomEntries(entries, n);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedIndexConsistency) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 1, 50);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        unsigned long expectedIdx = 0;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            unsigned long idx = orderedIndexGetIndex(oi, pos);
            ASSERT_EQ(idx, expectedIdx);
            OrderedIndexItem *byIdx = orderedIndexGetByIndex(oi, expectedIdx);
            ASSERT_EQ(byIdx, pos);
            expectedIdx++;
        }
        ASSERT_EQ(expectedIdx, (unsigned long)n);
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedDelete) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 2, 30);

        RandomIndexEntry *entries = test_build_random_index(oi, &rng, n);

        int delIdx = test_rand_range(&rng, 0, n - 1);
        orderedIndexDelete(oi, entries[delIdx].node);

        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)(n - 1));
        verifyOI();

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        int count = 0;
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            ASSERT_GE(orderedIndexGetScore(pos), prevScore);
            prevScore = orderedIndexGetScore(pos);
            count++;
        }
        ASSERT_EQ(count, n - 1);
        orderedIndexResetIterator(&iter);
        freeRandomEntries(entries, n);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedUpdateScore) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 2, 30);

        RandomIndexEntry *entries = test_build_random_index(oi, &rng, n);

        int updIdx = test_rand_range(&rng, 0, n - 1);
        double newScore = test_random_score(&rng);

        OrderedIndexItem *updated = orderedIndexUpdateScore(oi, entries[updIdx].node, newScore);
        ASSERT_NE(updated, nullptr);
        assertScore(updated, newScore);
        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)n);
        verifyOI();

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            ASSERT_GE(orderedIndexGetScore(pos), prevScore);
            prevScore = orderedIndexGetScore(pos);
        }
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        freeRandomEntries(entries, n);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedPop) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 10; trial++) {
        int n = test_rand_range(&rng, 3, 30);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        ASSERT_NE((pos = orderedIndexNext(&iter)), nullptr);
        double minScore = orderedIndexGetScore(pos);
        orderedIndexResetIterator(&iter);

        orderedIndexInitIterator(&iter, oi);
        ASSERT_NE((pos = orderedIndexPrev(&iter)), nullptr);
        double maxScore = orderedIndexGetScore(pos);
        orderedIndexResetIterator(&iter);

        OrderedIndexItem *first = orderedIndexPopFirst(oi);
        ASSERT_NE(first, nullptr);
        assertScore(first, minScore);
        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)(n - 1));
        orderedIndexFreeItem(first);
        verifyOI();

        OrderedIndexItem *last = orderedIndexPopLast(oi);
        ASSERT_NE(last, nullptr);
        assertScore(last, maxScore);
        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)(n - 2));
        orderedIndexFreeItem(last);
        verifyOI();

        orderedIndexInitIterator(&iter, oi);
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            ASSERT_GE(orderedIndexGetScore(pos), prevScore);
            prevScore = orderedIndexGetScore(pos);
        }
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedDeleteRangeByScore) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 5, 40);

        RandomIndexEntry *entries = test_build_random_index(oi, &rng, n);

        double s1 = test_random_score(&rng), s2 = test_random_score(&rng);
        double lo = TEST_MIN(s1, s2), hi = TEST_MAX(s1, s2);

        int expectedDeleted = 0;
        for (int i = 0; i < n; i++) {
            if (entries[i].score >= lo && entries[i].score <= hi) expectedDeleted++;
        }

        unsigned long deleted = orderedIndexDeleteRangeByScore(oi, lo, hi, 0, 0, NULL, NULL);
        ASSERT_EQ(deleted, (unsigned long)expectedDeleted);
        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)(n - expectedDeleted));
        verifyOI();

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            double s = orderedIndexGetScore(pos);
            ASSERT_TRUE(s < lo || s > hi);
            ASSERT_GE(s, prevScore);
            prevScore = s;
        }
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        freeRandomEntries(entries, n);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedDeleteRangeByIndex) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 5, 40);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        int r1 = test_rand_range(&rng, 0, n - 1), r2 = test_rand_range(&rng, 0, n - 1);
        unsigned long start = (unsigned long)TEST_MIN(r1, r2);
        unsigned long end = (unsigned long)TEST_MAX(r1, r2);
        unsigned long expectedDeleted = end - start + 1;

        unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, start, end, NULL, NULL);
        ASSERT_EQ(deleted, expectedDeleted);
        ASSERT_EQ(orderedIndexLength(oi), (unsigned long)(n)-expectedDeleted);
        verifyOI();

        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        orderedIndexInitIterator(&iter, oi);
        int remaining = 0;
        double prevScore = NEG_INF;
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            ASSERT_GE(orderedIndexGetScore(pos), prevScore);
            prevScore = orderedIndexGetScore(pos);
            remaining++;
        }
        ASSERT_EQ(remaining, n - (int)expectedDeleted);
        orderedIndexResetIterator(&iter);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

TEST_F(OrderedIndexTest, RandomizedForwardBackwardMirror) {
    uint32_t rng = test_fuzz_seed();
    for (int trial = 0; trial < 20; trial++) {
        int n = test_rand_range(&rng, 1, 50);

        {
            RandomIndexEntry *_e = test_build_random_index(oi, &rng, n);
            freeRandomEntries(_e, n);
        }

        double *forwardScores = (double *)zmalloc(sizeof(double) * n);
        OrderedIndexIterator iter;
        OrderedIndexItem *pos;
        int fi = 0;
        orderedIndexInitIterator(&iter, oi);
        while (((pos = orderedIndexNext(&iter)) != NULL)) {
            forwardScores[fi++] = orderedIndexGetScore(pos);
        }
        orderedIndexResetIterator(&iter);

        double *backwardScores = (double *)zmalloc(sizeof(double) * n);
        int bi = 0;
        orderedIndexInitIterator(&iter, oi);
        while (((pos = orderedIndexPrev(&iter)) != NULL)) {
            backwardScores[bi++] = orderedIndexGetScore(pos);
        }
        orderedIndexResetIterator(&iter);

        ASSERT_EQ(fi, bi);
        reverseDoubleArray(backwardScores, bi);
        for (int i = 0; i < fi; i++) {
            ASSERT_DOUBLE_EQ(forwardScores[i], backwardScores[i]);
        }
        zfree(forwardScores);
        zfree(backwardScores);
        orderedIndexFree(oi);
        oi = orderedIndexCreate();
    }
}

/* ========== Count range tests ========== */

TEST_F(OrderedIndexTest, CountScoreRange) {
    populateSequential(10);

    ASSERT_EQ(orderedIndexCountScoreRange(oi, NEG_INF, POS_INF, 0, 0), 10UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 3.0, 6.0, 0, 0), 4UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 3.0, 6.0, 1, 1), 2UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 5.0, 5.0, 0, 0), 1UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 5.0, 5.0, 1, 0), 0UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 10.0, 20.0, 0, 0), 0UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, -20.0, -10.0, 0, 0), 0UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 6.0, 3.0, 0, 0), 0UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 0.0, 0.0, 0, 0), 1UL);
    ASSERT_EQ(orderedIndexCountScoreRange(oi, 9.0, 9.0, 0, 0), 1UL);
}

TEST_F(OrderedIndexTest, CountScoreRangeEmpty) {
    ASSERT_EQ(orderedIndexCountScoreRange(oi, NEG_INF, POS_INF, 0, 0), 0UL);
}

TEST_F(OrderedIndexTest, CountLexRange) {
    for (int i = 0; i < FRUITS_COUNT; i++) insert(1.0, FRUITS[i]);

    ASSERT_EQ(countLexRange("banana", "date", 0, 0), 3UL);
    ASSERT_EQ(countLexRange("banana", "date", 1, 1), 1UL);
    ASSERT_EQ(countLexRange("cherry", "cherry", 0, 0), 1UL);
    ASSERT_EQ(countLexRange("fig", "grape", 0, 0), 0UL);
    ASSERT_EQ(countLexRange("a", "z", 0, 0), 5UL);
}

TEST_F(OrderedIndexTest, CountLexRangeEmpty) {
    ASSERT_EQ(countLexRange("a", "z", 0, 0), 0UL);
}

/* ========== On-Delete Callback Tests ========== */

struct OnDeleteRecord {
    int count;
    int capacity;
    sds *elements;
};

static void initOnDeleteRecord(OnDeleteRecord *rec, int capacity) {
    rec->count = 0;
    rec->capacity = capacity;
    rec->elements = (sds *)zmalloc(sizeof(sds) * capacity);
}

static void freeOnDeleteRecord(OnDeleteRecord *rec) {
    for (int i = 0; i < rec->count; i++) sdsfree(rec->elements[i]);
    zfree(rec->elements);
}

static void testOnDeleteCallback(OrderedIndexItem *item, void *ctx) {
    OnDeleteRecord *rec = (OnDeleteRecord *)ctx;
    const char *ptr;
    size_t len;
    orderedIndexGetElementRaw(item, &ptr, &len);
    rec->elements[rec->count] = sdsnewlen(ptr, len);
    rec->count++;
}

class OnDeleteCallbackTest : public ::testing::Test {
  protected:
    OrderedIndex *oi = nullptr;

    void SetUp() override {
        oi = orderedIndexCreate();
    }
    void TearDown() override {
        if (oi) orderedIndexFree(oi);
    }

    void verifyOI() {
        char errmsg[256];
        ASSERT_TRUE(orderedIndexVerifyIntegrity(oi, errmsg, sizeof(errmsg))) << errmsg;
    }

    void insert(double score, const char *ele) {
        orderedIndexInsert(oi, score, ele, strlen(ele));
    }

    void insertN(int n) {
        for (int i = 0; i < n; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "key%d", i);
            insert((double)i, buf);
        }
    }

    void insertLex(const char *elems[], int count, double score = 1.0) {
        for (int i = 0; i < count; i++) {
            insert(score, elems[i]);
        }
    }

    sds *collectElements(OrderedIndex *idx, size_t *out_n) {
        return collectIndexToSds(idx, out_n);
    }
};

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_EmptyAndNoMatch) {
    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);

    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 0.0, 10.0, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    insertN(5);
    rec.count = 0;
    deleted = orderedIndexDeleteRangeByScore(oi, 10.0, 20.0, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    ASSERT_EQ(orderedIndexLength(oi), 5UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_Subset) {
    insertN(10);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 3.0, 6.0, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 4UL);
    ASSERT_EQ(rec.count, 4);
    ASSERT_EQ(orderedIndexLength(oi), 6UL);
    verifyOI();

    sortSdsArray(rec.elements, rec.count);
    ASSERT_SDS_ARRAY_EQ(rec.elements, rec.count, "key3", "key4", "key5", "key6");

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "key0", "key1", "key2", "key7", "key8", "key9");
        freeSdsArray(_r, _rn);
    }
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_All) {
    insertN(5);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, NEG_INF, POS_INF, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 5UL);
    ASSERT_EQ(rec.count, 5);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    verifyOI();
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_NullCallback) {
    insertN(5);

    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 1.0, 3.0, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_ExclusiveBounds) {
    insertN(10);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 3.0, 7.0, 1, 1, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(rec.count, 3);
    sortSdsArray(rec.elements, rec.count);
    ASSERT_SDS_ARRAY_EQ(rec.elements, rec.count, "key4", "key5", "key6");
    ASSERT_EQ(orderedIndexLength(oi), 7UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByScore_SingleElement) {
    insertN(5);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByScore(oi, 2.0, 2.0, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(rec.count, 1);
    ASSERT_STREQ(rec.elements[0], "key2");
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_EmptyAndNoMatch) {
    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);

    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 0, 4, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    insertN(3);
    rec.count = 0;
    deleted = orderedIndexDeleteRangeByIndex(oi, 10, 20, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    ASSERT_EQ(orderedIndexLength(oi), 3UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_Subset) {
    insertN(10);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 2, 4, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(rec.count, 3);
    ASSERT_EQ(orderedIndexLength(oi), 7UL);

    sortSdsArray(rec.elements, rec.count);
    ASSERT_SDS_ARRAY_EQ(rec.elements, rec.count, "key2", "key3", "key4");

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "key0", "key1", "key5", "key6", "key7", "key8", "key9");
        freeSdsArray(_r, _rn);
    }
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_All) {
    insertN(5);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 0, 4, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 5UL);
    ASSERT_EQ(rec.count, 5);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_NullCallback) {
    insertN(5);

    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 2, 4, NULL, NULL);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_ExclusiveBounds) {
    insertN(5);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 2, 2, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(rec.count, 1);
    ASSERT_STREQ(rec.elements[0], "key2");

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "key0", "key1", "key3", "key4");
        freeSdsArray(_r, _rn);
    }
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByIndex_SingleElement) {
    insertN(5);

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    unsigned long deleted = orderedIndexDeleteRangeByIndex(oi, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(rec.count, 1);
    ASSERT_STREQ(rec.elements[0], "key0");
    ASSERT_EQ(orderedIndexLength(oi), 4UL);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_EmptyAndNoMatch) {
    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);

    sds min = sdsnew("a");
    sds max = sdsnew("z");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    sdsfree(min);
    sdsfree(max);
    orderedIndexFree(oi);

    oi = orderedIndexCreate();
    {
        const char *_l[] = {"apple", "banana", "cherry"};
        insertLex(_l, 3);
    }
    rec.count = 0;
    min = sdsnew("x");
    max = sdsnew("z");
    deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 0UL);
    ASSERT_EQ(rec.count, 0);
    ASSERT_EQ(orderedIndexLength(oi), 3UL);
    sdsfree(min);
    sdsfree(max);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_Subset) {
    {
        const char *_l[] = {"apple", "banana", "cherry", "date", "elderberry"};
        insertLex(_l, 5);
    }

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    sds min = sdsnew("banana");
    sds max = sdsnew("date");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(rec.count, 3);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);

    sortSdsArray(rec.elements, rec.count);
    ASSERT_SDS_ARRAY_EQ(rec.elements, rec.count, "banana", "cherry", "date");

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "apple", "elderberry");
        freeSdsArray(_r, _rn);
    }

    sdsfree(min);
    sdsfree(max);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_All) {
    {
        const char *_l[] = {"apple", "banana", "cherry"};
        insertLex(_l, 3);
    }

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    sds min = sdsnew("a");
    sds max = sdsnew("z");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 3UL);
    ASSERT_EQ(rec.count, 3);
    ASSERT_EQ(orderedIndexLength(oi), 0UL);

    sdsfree(min);
    sdsfree(max);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_NullCallback) {
    {
        const char *_l[] = {"apple", "banana", "cherry", "date"};
        insertLex(_l, 4);
    }

    sds min = sdsnew("banana");
    sds max = sdsnew("cherry");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, NULL, NULL);
    ASSERT_EQ(deleted, 2UL);
    ASSERT_EQ(orderedIndexLength(oi), 2UL);

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "apple", "date");
        freeSdsArray(_r, _rn);
    }

    sdsfree(min);
    sdsfree(max);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_ExclusiveBounds) {
    {
        const char *_l[] = {"apple", "banana", "cherry", "date", "elderberry"};
        insertLex(_l, 5);
    }

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    sds min = sdsnew("banana");
    sds max = sdsnew("date");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 1, 1, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(rec.count, 1);
    ASSERT_STREQ(rec.elements[0], "cherry");
    ASSERT_EQ(orderedIndexLength(oi), 4UL);

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "apple", "banana", "date", "elderberry");
        freeSdsArray(_r, _rn);
    }

    sdsfree(min);
    sdsfree(max);
    freeOnDeleteRecord(&rec);
}

TEST_F(OnDeleteCallbackTest, DeleteRangeByLex_SingleElement) {
    {
        const char *_l[] = {"apple", "banana", "cherry"};
        insertLex(_l, 3);
    }

    OnDeleteRecord rec;
    initOnDeleteRecord(&rec, 10);
    sds min = sdsnew("banana");
    sds max = sdsnew("banana");
    unsigned long deleted = orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, testOnDeleteCallback, &rec);
    ASSERT_EQ(deleted, 1UL);
    ASSERT_EQ(rec.count, 1);
    ASSERT_STREQ(rec.elements[0], "banana");
    ASSERT_EQ(orderedIndexLength(oi), 2UL);

    {
        size_t _rn;
        sds *_r = collectElements(oi, &_rn);
        ASSERT_SDS_ARRAY_EQ(_r, _rn, "apple", "cherry");
        freeSdsArray(_r, _rn);
    }

    sdsfree(min);
    sdsfree(max);
    freeOnDeleteRecord(&rec);
}

/* ========== Range-Delete Hashtable Consistency Tests ========== */

struct SimHt {
    sds *elems;
    int count;
    int capacity;
};

static void simHtInit(SimHt *ht, int cap) {
    ht->elems = (sds *)zmalloc(sizeof(sds) * cap);
    ht->count = 0;
    ht->capacity = cap;
}

static void simHtFree(SimHt *ht) {
    for (int i = 0; i < ht->count; i++) sdsfree(ht->elems[i]);
    zfree(ht->elems);
}

static void simHtAdd(SimHt *ht, const char *s, size_t len) {
    ht->elems[ht->count++] = sdsnewlen(s, len);
}

static void simHtRemove(SimHt *ht, const char *s, size_t len) {
    for (int i = 0; i < ht->count; i++) {
        if (sdslen(ht->elems[i]) == len && memcmp(ht->elems[i], s, len) == 0) {
            sdsfree(ht->elems[i]);
            ht->elems[i] = ht->elems[--ht->count];
            return;
        }
    }
}

static void simHtSort(SimHt *ht) {
    sortSdsArray(ht->elems, ht->count);
}

static void hashtableConsistencyOnDelete(OrderedIndexItem *item, void *ctx) {
    void **args = (void **)ctx;
    SimHt *ht = (SimHt *)args[0];
    const char *ptr;
    size_t len;
    orderedIndexGetElementRaw(item, &ptr, &len);
    simHtRemove(ht, ptr, len);
}

class RangeDeleteHashtableConsistencyTest : public ::testing::Test {
  protected:
    OrderedIndex *oi = nullptr;

    void SetUp() override {
        oi = orderedIndexCreate();
    }
    void TearDown() override {
        if (oi) orderedIndexFree(oi);
    }

    void insert(double score, const char *ele) {
        orderedIndexInsert(oi, score, ele, strlen(ele));
    }

    void insertN(SimHt &ht, int n) {
        for (int i = 0; i < n; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "key%d", i);
            insert((double)i, buf);
            simHtAdd(&ht, buf, strlen(buf));
        }
    }

    void insertLex(SimHt &ht, const char *elems[], int count, double score = 1.0) {
        for (int i = 0; i < count; i++) {
            insert(score, elems[i]);
            simHtAdd(&ht, elems[i], strlen(elems[i]));
        }
    }

    void assertHtMatchesIndex(SimHt &ht) {
        size_t idx_n;
        sds *idx_elems = collectIndexToSds(oi, &idx_n);
        sortSdsArray(idx_elems, idx_n);
        simHtSort(&ht);
        ASSERT_EQ(idx_n, (size_t)ht.count);
        for (size_t i = 0; i < idx_n; i++) {
            ASSERT_STREQ(idx_elems[i], ht.elems[i]);
        }
        freeSdsArray(idx_elems, idx_n);
    }
};

TEST_F(RangeDeleteHashtableConsistencyTest, ByScore_PartialDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByScore(oi, 3.0, 6.0, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByScore_FullDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByScore(oi, NEG_INF, POS_INF, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByScore_EmptyRange) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByScore(oi, 20.0, 30.0, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByIndex_PartialDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByIndex(oi, 2, 4, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByIndex_FullDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByIndex(oi, 0, 9, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByIndex_EmptyRange) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    insertN(simulatedHt, 10);

    orderedIndexDeleteRangeByIndex(oi, 20, 30, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByLex_PartialDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    {
        const char *_l[] = {"apple", "banana", "cherry", "date", "elderberry"};
        insertLex(simulatedHt, _l, 5);
    }

    sds min = sdsnew("banana");
    sds max = sdsnew("date");
    orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);

    sdsfree(min);
    sdsfree(max);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByLex_FullDelete) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    {
        const char *_l[] = {"apple", "banana", "cherry", "date", "elderberry"};
        insertLex(simulatedHt, _l, 5);
    }

    sds min = sdsnew("a");
    sds max = sdsnew("z");
    orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);

    sdsfree(min);
    sdsfree(max);
    simHtFree(&simulatedHt);
}

TEST_F(RangeDeleteHashtableConsistencyTest, ByLex_EmptyRange) {
    SimHt simulatedHt;
    simHtInit(&simulatedHt, 20);
    void *cbCtx[] = {&simulatedHt, NULL};
    {
        const char *_l[] = {"apple", "banana", "cherry", "date", "elderberry"};
        insertLex(simulatedHt, _l, 5);
    }

    sds min = sdsnew("zzz");
    sds max = sdsnew("zzzz");
    orderedIndexDeleteRangeByLex(oi, min, max, 0, 0, hashtableConsistencyOnDelete, cbCtx);

    assertHtMatchesIndex(simulatedHt);

    sdsfree(min);
    sdsfree(max);
    simHtFree(&simulatedHt);
}
