/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* stringRef unit tests — upstream type only, no wrapper/production code.
 *
 * Validates:
 *   1. Initialization from sds, raw buffer, and literals.
 *   2. Empty and zero-length edge cases.
 *   3. Binary safety: embedded NUL, CRLF, all 256 byte values.
 *   4. Shallow non-owning lifetime: backing buffer outlives view.
 *   5. Randomized byte preservation round-trip.
 *
 * Deferred until P0 APIs exist:
 *   - Hash/equality callback integration with hashtable.
 *   - Heterogeneous lookup (requires an explicit portable hashtable API).
 *   - RESP parser borrowed-view plumbing.
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
#include "sds.h"
#include "server.h"
}

class StringRefTest : public ::testing::Test {};

/* ---------- 1. Initialization ------------------------------------------ */

TEST_F(StringRefTest, InitFromRawBuffer) {
    const char raw[] = "hello";
    stringRef ref = {raw, 5};
    ASSERT_EQ(ref.buf, raw);
    ASSERT_EQ(ref.len, 5u);
    ASSERT_EQ(memcmp(ref.buf, "hello", 5), 0);
}

TEST_F(StringRefTest, InitFromSds) {
    sds s = sdsnew("valkey");
    stringRef ref = {s, sdslen(s)};
    ASSERT_EQ(ref.buf, s);
    ASSERT_EQ(ref.len, 6u);
    ASSERT_EQ(memcmp(ref.buf, "valkey", 6), 0);
    sdsfree(s);
}

TEST_F(StringRefTest, InitFromLiteral) {
    /* String literal persists for the program's lifetime. */
    stringRef ref = {"literal", 7};
    ASSERT_EQ(ref.len, 7u);
    ASSERT_EQ(memcmp(ref.buf, "literal", 7), 0);
}

/* ---------- 2. Empty / zero-length ------------------------------------- */

TEST_F(StringRefTest, EmptyWithNullBuf) {
    stringRef ref = {NULL, 0};
    ASSERT_EQ(ref.buf, (const char *)NULL);
    ASSERT_EQ(ref.len, 0u);
}

TEST_F(StringRefTest, EmptyWithNonNullBuf) {
    const char empty[] = "";
    stringRef ref = {empty, 0};
    ASSERT_EQ(ref.buf, empty);
    ASSERT_EQ(ref.len, 0u);
}

TEST_F(StringRefTest, EmptySds) {
    sds s = sdsempty();
    stringRef ref = {s, sdslen(s)};
    ASSERT_EQ(ref.len, 0u);
    sdsfree(s);
}

/* ---------- 3. Binary safety ------------------------------------------- */

TEST_F(StringRefTest, EmbeddedNul) {
    /* "a\0b" — three bytes, NUL in the middle. */
    sds s = sdsnewlen("a\0b", 3);
    stringRef ref = {s, sdslen(s)};
    ASSERT_EQ(ref.len, 3u);
    ASSERT_EQ(ref.buf[0], 'a');
    ASSERT_EQ(ref.buf[1], '\0');
    ASSERT_EQ(ref.buf[2], 'b');
    sdsfree(s);
}

TEST_F(StringRefTest, CrlfContent) {
    const char data[] = "line1\r\nline2\r\n";
    size_t datalen = sizeof(data) - 1; /* exclude trailing NUL */
    sds s = sdsnewlen(data, datalen);
    stringRef ref = {s, sdslen(s)};
    ASSERT_EQ(ref.len, datalen);
    ASSERT_EQ(memcmp(ref.buf, data, datalen), 0);
    sdsfree(s);
}

TEST_F(StringRefTest, AllByteValues) {
    /* Build a 256-byte sds containing every byte value 0x00..0xFF. */
    char raw[256];
    for (int i = 0; i < 256; i++) raw[i] = (char)(unsigned char)i;
    sds s = sdsnewlen(raw, 256);
    stringRef ref = {s, sdslen(s)};
    ASSERT_EQ(ref.len, 256u);
    ASSERT_EQ(memcmp(ref.buf, raw, 256), 0);
    sdsfree(s);
}

/* ---------- 4. Shallow non-owning lifetime ----------------------------- */

TEST_F(StringRefTest, BackingBufferOutlivesView) {
    sds s = sdsnew("persistent");
    const char *saved_ptr;
    size_t saved_len;

    {
        /* View in an inner scope — goes out of scope without freeing s. */
        stringRef ref = {s, sdslen(s)};
        saved_ptr = ref.buf;
        saved_len = ref.len;
    }

    /* Backing sds is still valid after the view goes away. */
    ASSERT_EQ(saved_ptr, s);
    ASSERT_EQ(saved_len, sdslen(s));
    ASSERT_EQ(memcmp(s, "persistent", 10), 0);
    sdsfree(s);
}

TEST_F(StringRefTest, ViewReflectsBackingMutation) {
    /* stringRef is a raw pointer+len; mutation of the backing buffer
     * is visible through the view (no copy). */
    char buf[] = "ABCDE";
    stringRef ref = {buf, 5};
    ASSERT_EQ(ref.buf[2], 'C');

    /* Mutate through the backing buffer (not through ref, which is const). */
    buf[2] = 'X';
    ASSERT_EQ(ref.buf[2], 'X');
}

TEST_F(StringRefTest, MultipleViewsSameBuffer) {
    sds s = sdsnew("shared-backing");
    stringRef v1 = {s, sdslen(s)};
    stringRef v2 = {s, sdslen(s)};

    ASSERT_EQ(v1.buf, v2.buf);
    ASSERT_EQ(v1.len, v2.len);
    ASSERT_EQ(memcmp(v1.buf, v2.buf, v1.len), 0);
    sdsfree(s);
}

/* ---------- 5. Randomized byte preservation ---------------------------- */

TEST_F(StringRefTest, RandomizedBytePreservation) {
    /* 100 rounds of random-length (1..1024) random-byte buffers:
     * create an sds, wrap in stringRef, verify byte-for-byte. */
    unsigned int seed = 42;
    for (int round = 0; round < 100; round++) {
        size_t len = 1 + (rand_r(&seed) % 1024);
        char *raw = (char *)malloc(len);
        ASSERT_NE(raw, (char *)NULL);
        for (size_t j = 0; j < len; j++) raw[j] = (char)(rand_r(&seed) & 0xFF);

        sds s = sdsnewlen(raw, len);
        stringRef ref = {s, sdslen(s)};

        ASSERT_EQ(ref.len, len);
        ASSERT_EQ(memcmp(ref.buf, raw, len), 0);

        sdsfree(s);
        free(raw);
    }
}
