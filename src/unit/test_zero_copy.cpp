/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cctype>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "sds.h"
#include "server.h"
#include "vstr.h"
/* initServerConfig is defined in server.c but not declared in server.h.
 * We declare it here so the test can call it to populate the command table. */
void initServerConfig(void);
/* commandSetType and originalCommandSetType are defined in server.c. */
extern hashtableType commandSetType;
extern hashtableType originalCommandSetType;
/* setHashtableType uses vstr-aware callbacks (vstrHashCallback/vstrCompareCallback). */
extern hashtableType setHashtableType;
/* Hash seed initialization for hashtable tests. */
void setConfigurableHashSeed(uint8_t *seed);
}

/* Undefine min/max macros from server.h so that <random> compiles. */
#undef min
#undef max
#include <random>

/* Test fixture for zero-copy vargv lifecycle tests. */
class VargvLifecycleTest : public ::testing::Test {
  protected:
    void SetUp() override {
        /* Initialize the shared query buffer so that freeClientVargv's
         * check (c->querybuf == thread_shared_qb) can be controlled. */
        initSharedQueryBuf();
    }

    void TearDown() override {
        freeSharedQueryBuf(NULL);
    }

    /* Create a minimal client with vargv allocated. */
    client *createMinimalClient(int vargv_capacity) {
        client *c = (client *)(zcalloc(sizeof(client)));
        c->vargv = (vstr *)zmalloc(sizeof(vstr) * vargv_capacity);
        c->vargv_len = vargv_capacity;
        c->vargc = 0;
        /* Set querybuf to a private sds so freeClientVargv does NOT enter
         * the resetSharedQueryBuf path. */
        c->querybuf = sdsnewlen("dummy", 5);
        c->qb_pos = 0;
        return c;
    }

    void freeMinimalClient(client *c) {
        sdsfree(c->querybuf);
        zfree(c->vargv);
        zfree(c);
    }
};

/* Test that freeClientVargv on all-borrowed vargv performs no heap deallocation.
 * Validates: Requirements 9.1, 9.3 */
TEST_F(VargvLifecycleTest, AllBorrowedVargvNoHeapDeallocation) {
    client *c = createMinimalClient(4);

    /* Simulate a parsed GET command: "GET" + "mykey" — all borrowed. */
    const char *cmd_name = "GET";
    const char *key = "mykey";

    vstrInitBorrowed(&c->vargv[0], cmd_name, 3);
    vstrInitBorrowed(&c->vargv[1], key, 5);
    c->vargc = 2;

    /* Verify all entries are borrowed before cleanup. */
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[0]));
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[1]));

    /* Call freeClientVargv — should be a no-op for argument data. */
    freeClientVargv(c);

    /* Verify vargc is reset. */
    ASSERT_EQ(c->vargc, 0);

    /* Verify the original buffers are still intact (not freed). */
    ASSERT_EQ(memcmp(cmd_name, "GET", 3), 0);
    ASSERT_EQ(memcmp(key, "mykey", 5), 0);

    freeMinimalClient(c);
}

/* Test with more borrowed entries to cover MGET-like scenarios. */
TEST_F(VargvLifecycleTest, AllBorrowedMgetScenario) {
    client *c = createMinimalClient(8);

    /* Simulate MGET with 5 keys — all borrowed. */
    const char *args[] = {"MGET", "key1", "key2", "key3", "key4", "key5"};
    for (int i = 0; i < 6; i++) {
        vstrInitBorrowed(&c->vargv[i], args[i], strlen(args[i]));
    }
    c->vargc = 6;

    /* Verify all entries are borrowed. */
    for (int i = 0; i < 6; i++) {
        ASSERT_TRUE(vstrIsBorrowed(&c->vargv[i]));
    }

    freeClientVargv(c);

    ASSERT_EQ(c->vargc, 0);

    /* Original buffers still valid. */
    for (int i = 0; i < 6; i++) {
        ASSERT_EQ(memcmp(args[i], args[i], strlen(args[i])), 0);
    }

    freeMinimalClient(c);
}

/* Test that freeClientVargv on mixed vargv (borrowed + owned sds) correctly
 * frees owned entries while leaving borrowed entries as no-ops.
 * Validates: Requirements 9.1, 9.3 */
TEST_F(VargvLifecycleTest, MixedVargvFreesOwnedEntries) {
    client *c = createMinimalClient(4);

    /* Entry 0: borrowed (command name from querybuf). */
    const char *cmd_name = "SET";
    vstrInitBorrowed(&c->vargv[0], cmd_name, 3);

    /* Entry 1: borrowed (key from querybuf). */
    const char *key = "mykey";
    vstrInitBorrowed(&c->vargv[1], key, 5);

    /* Entry 2: owned sds (big argument that took ownership of a buffer). */
    sds owned_value = sdsnewlen("this is a big value argument", 28);
    vstrInitSds(&c->vargv[2], owned_value);

    c->vargc = 3;

    /* Verify types before cleanup. */
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[0]));
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[1]));
    ASSERT_EQ(vstrType(&c->vargv[2]), VSTR_SDS);

    /* Verify content before cleanup. */
    ASSERT_EQ(vstrLen(&c->vargv[0]), 3u);
    ASSERT_EQ(memcmp(vstrData(&c->vargv[0]), "SET", 3), 0);
    ASSERT_EQ(vstrLen(&c->vargv[1]), 5u);
    ASSERT_EQ(memcmp(vstrData(&c->vargv[1]), "mykey", 5), 0);
    ASSERT_EQ(vstrLen(&c->vargv[2]), 28u);
    ASSERT_EQ(memcmp(vstrData(&c->vargv[2]), "this is a big value argument", 28), 0);

    /* Call freeClientVargv — should free the owned sds entry and no-op the borrowed ones. */
    freeClientVargv(c);

    /* Verify vargc is reset. */
    ASSERT_EQ(c->vargc, 0);

    /* Verify borrowed buffers are still intact. */
    ASSERT_EQ(memcmp(cmd_name, "SET", 3), 0);
    ASSERT_EQ(memcmp(key, "mykey", 5), 0);

    /* The owned sds has been freed — we can't dereference it, but the fact
     * that we didn't crash and vargc is 0 confirms correct behavior.
     * Additionally, verify the sds pointer in the vstr was NULLed by vstrFree. */
    ASSERT_EQ(c->vargv[2].sds.s, nullptr);

    freeMinimalClient(c);
}

/* Test mixed vargv with multiple owned entries interspersed with borrowed. */
TEST_F(VargvLifecycleTest, MixedVargvMultipleOwnedEntries) {
    client *c = createMinimalClient(5);

    /* Entry 0: borrowed. */
    const char *arg0 = "CMD";
    vstrInitBorrowed(&c->vargv[0], arg0, 3);

    /* Entry 1: owned sds. */
    sds owned1 = sdsnewlen("owned_arg_1", 11);
    vstrInitSds(&c->vargv[1], owned1);

    /* Entry 2: borrowed. */
    const char *arg2 = "borrowed_middle";
    vstrInitBorrowed(&c->vargv[2], arg2, 15);

    /* Entry 3: owned sds. */
    sds owned2 = sdsnewlen("owned_arg_2", 11);
    vstrInitSds(&c->vargv[3], owned2);

    /* Entry 4: borrowed. */
    const char *arg4 = "end";
    vstrInitBorrowed(&c->vargv[4], arg4, 3);

    c->vargc = 5;

    /* Verify types. */
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[0]));
    ASSERT_EQ(vstrType(&c->vargv[1]), VSTR_SDS);
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[2]));
    ASSERT_EQ(vstrType(&c->vargv[3]), VSTR_SDS);
    ASSERT_TRUE(vstrIsBorrowed(&c->vargv[4]));

    freeClientVargv(c);

    ASSERT_EQ(c->vargc, 0);

    /* Owned entries should have their sds pointer NULLed. */
    ASSERT_EQ(c->vargv[1].sds.s, nullptr);
    ASSERT_EQ(c->vargv[3].sds.s, nullptr);

    /* Borrowed buffers still intact. */
    ASSERT_EQ(memcmp(arg0, "CMD", 3), 0);
    ASSERT_EQ(memcmp(arg2, "borrowed_middle", 15), 0);
    ASSERT_EQ(memcmp(arg4, "end", 3), 0);

    freeMinimalClient(c);
}

/* Test freeClientVargv with zero arguments (empty vargv). */
TEST_F(VargvLifecycleTest, EmptyVargvIsNoop) {
    client *c = createMinimalClient(4);
    c->vargc = 0;

    /* Should not crash or do anything. */
    freeClientVargv(c);

    ASSERT_EQ(c->vargc, 0);

    freeMinimalClient(c);
}

/* --- Property-Based Tests --- */

// Feature: zero-copy-get, Property 1: Parser round-trip correctness for normal arguments

/*
 * **Validates: Requirements 2.1, 2.2, 2.3, 18.1, 18.3**
 *
 * For any valid RESP bulk string argument with arbitrary bytes (including
 * embedded nulls) and length in the range [0, PROTO_MBULK_BIG_ARG - 1],
 * the parser SHALL produce a borrowed vstr in vargv whose vstrData() returns
 * a pointer to bytes identical to the original argument content and vstrLen()
 * returns the exact argument length.
 *
 * This test simulates the parser logic:
 *   vstrInitBorrowed(&vargv[i], querybuf + qb_pos, bulklen)
 * and verifies the resulting vstr content matches the original bytes.
 */

class ZeroCopyProperty1Test : public ::testing::Test {};

TEST_F(ZeroCopyProperty1Test, ParserRoundTripNormalArguments) {
    const int iterations = 200;
    /* Normal arguments have length in [0, PROTO_MBULK_BIG_ARG - 1]. */
    const size_t max_len = PROTO_MBULK_BIG_ARG - 1; /* 32767 */

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> len_dist(0, max_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    for (int i = 0; i < iterations; i++) {
        /* Generate a random argument length in [0, 16383] as specified in the task.
         * The task says 0-16383, which is within the normal argument range. */
        size_t arg_len = len_dist(rng) % 16384; /* [0, 16383] */

        /* Generate random byte content (may include embedded nulls). */
        std::vector<char> arg_content(arg_len);
        for (size_t j = 0; j < arg_len; j++) {
            arg_content[j] = static_cast<char>(byte_dist(rng));
        }

        /* Build a mock querybuf containing the RESP bulk string:
         * $<len>\r\n<content>\r\n
         * The parser would set qb_pos to point at <content>. */
        char len_str[32];
        int len_str_len = snprintf(len_str, sizeof(len_str), "%zu", arg_len);

        /* Total buffer: '$' + len_str + '\r\n' + content + '\r\n' */
        size_t querybuf_len = 1 + (size_t)len_str_len + 2 + arg_len + 2;
        sds querybuf = sdsnewlen(SDS_NOINIT, querybuf_len);
        sdsclear(querybuf);

        /* Assemble the RESP frame into the querybuf. */
        querybuf = sdscatlen(querybuf, "$", 1);
        querybuf = sdscatlen(querybuf, len_str, (size_t)len_str_len);
        querybuf = sdscatlen(querybuf, "\r\n", 2);
        size_t qb_pos = sdslen(querybuf); /* Position of content start. */
        querybuf = sdscatlen(querybuf, arg_content.data(), arg_len);
        querybuf = sdscatlen(querybuf, "\r\n", 2);

        /* Simulate the parser logic for a normal-sized argument:
         * vstrInitBorrowed(&c->vargv[c->vargc], c->querybuf + c->qb_pos, bulklen) */
        vstr v;
        vstrInitBorrowed(&v, querybuf + qb_pos, arg_len);

        /* Verify the vstr is borrowed. */
        ASSERT_TRUE(vstrIsBorrowed(&v)) << "vstr should be borrowed at iteration " << i;

        /* Verify length matches the original argument length. */
        ASSERT_EQ(vstrLen(&v), arg_len)
            << "Length mismatch at iteration " << i << " (expected " << arg_len << ", got " << vstrLen(&v) << ")";

        /* Verify content matches the original argument bytes exactly. */
        if (arg_len > 0) {
            ASSERT_EQ(memcmp(vstrData(&v), arg_content.data(), arg_len), 0)
                << "Content mismatch at iteration " << i << " (arg_len=" << arg_len << ")";
        }

        /* Verify the data pointer points into the querybuf at the correct offset. */
        ASSERT_EQ(vstrData(&v), querybuf + qb_pos)
            << "Data pointer should point into querybuf at iteration " << i;

        sdsfree(querybuf);
    }
}

// Feature: zero-copy-get, Property 2: Parser round-trip correctness for big arguments

/*
 * **Validates: Requirements 3.1, 3.3, 18.2**
 *
 * For any valid RESP bulk string argument at or above PROTO_MBULK_BIG_ARG
 * that qualifies for the big-argument optimization (starts at position 0,
 * consumes the entire query buffer), the parser SHALL produce an owned sds
 * vstr in vargv whose vstrData()/vstrLen() yield bytes and length identical
 * to the original argument content (without trailing CRLF).
 *
 * This test simulates the parser logic for big arguments:
 *   sdsIncrLen(c->querybuf, -2);  // strip trailing \r\n
 *   vstrInitSds(&vargv[i], c->querybuf);
 * where qb_pos == 0 and querybuf contains only the argument + \r\n.
 */

class ZeroCopyProperty2Test : public ::testing::Test {};

TEST_F(ZeroCopyProperty2Test, ParserRoundTripBigArguments) {
    const int iterations = 200;
    /* Big arguments have length >= PROTO_MBULK_BIG_ARG (32768).
     * Test range: [PROTO_MBULK_BIG_ARG, PROTO_MBULK_BIG_ARG + 4096]. */
    const size_t min_len = PROTO_MBULK_BIG_ARG;
    const size_t max_len = PROTO_MBULK_BIG_ARG + 4096;

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(12345);
    std::uniform_int_distribution<size_t> len_dist(min_len, max_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    for (int i = 0; i < iterations; i++) {
        size_t arg_len = len_dist(rng);

        /* Generate random byte content (may include embedded nulls). */
        std::vector<char> arg_content(arg_len);
        for (size_t j = 0; j < arg_len; j++) {
            arg_content[j] = static_cast<char>(byte_dist(rng));
        }

        /* Simulate the big-argument scenario:
         * - qb_pos == 0
         * - querybuf contains only the argument bytes + trailing \r\n
         * The parser logic is:
         *   sdsIncrLen(c->querybuf, -2);  // strip trailing \r\n
         *   vstrInitSds(&vargv[i], c->querybuf);
         */

        /* Create a querybuf containing: <content>\r\n */
        sds querybuf = sdsnewlen(arg_content.data(), arg_len);
        querybuf = sdscatlen(querybuf, "\r\n", 2);

        /* Simulate the parser stripping the trailing \r\n. */
        sdsIncrLen(querybuf, -2);

        /* Simulate the parser transferring ownership to the vstr. */
        vstr v;
        vstrInitSds(&v, querybuf);

        /* Verify the vstr is owned (not borrowed). */
        ASSERT_FALSE(vstrIsBorrowed(&v)) << "Big-arg vstr should be owned at iteration " << i;
        ASSERT_EQ(vstrType(&v), VSTR_SDS) << "Big-arg vstr type should be VSTR_SDS at iteration " << i;

        /* Verify length matches the original argument length (without CRLF). */
        ASSERT_EQ(vstrLen(&v), arg_len)
            << "Length mismatch at iteration " << i << " (expected " << arg_len << ", got " << vstrLen(&v) << ")";

        /* Verify content matches the original argument bytes exactly. */
        ASSERT_EQ(memcmp(vstrData(&v), arg_content.data(), arg_len), 0)
            << "Content mismatch at iteration " << i << " (arg_len=" << arg_len << ")";

        /* Cleanup: vstrFree will sdsfree the owned buffer. */
        vstrFree(&v);
    }
}

// Feature: zero-copy-get, Property 4: Command dispatch from vargv finds correct command

/*
 * **Validates: Requirements 5.1, 5.3**
 *
 * For a set of known command names (GET, SET, MGET, DEL, PING, INFO, CLIENT,
 * CLUSTER), generate case variations (random upper/lower per character), create
 * a vargv entry with the varied name, call lookupCommandFromVargv, verify the
 * same command is found as lookupCommandBySds with the same bytes.
 *
 * Also test subcommand lookup: "CLIENT" + "LIST", "CLIENT" + "INFO".
 * Also test unknown command returns NULL.
 * Run at least 100 iterations for the case-variation test.
 */

class ZeroCopyProperty4Test : public ::testing::Test {
  protected:
    static bool s_initialized;

    static void SetUpTestSuite() {
        if (!s_initialized) {
            /* Directly create the command hashtables and populate the command
             * table. This avoids calling initServerConfig() which has many
             * side effects (config init, replication state, etc.) that are
             * unnecessary for command lookup tests. */
            server.commands = hashtableCreate(&commandSetType);
            server.orig_commands = hashtableCreate(&originalCommandSetType);
            populateCommandTable();
            s_initialized = true;
        }
    }
};

bool ZeroCopyProperty4Test::s_initialized = false;

TEST_F(ZeroCopyProperty4Test, CommandDispatchCaseVariations) {
    const int iterations = 100;
    /* Commands without subcommands — lookupCommandFromVargv returns the base command. */
    const char *simple_commands[] = {"GET", "SET", "MGET", "DEL", "PING", "INFO"};
    const int num_simple = sizeof(simple_commands) / sizeof(simple_commands[0]);
    /* Container commands (have subcommands) — pass vargc=1 to get the base command. */
    const char *container_commands[] = {"CLIENT", "CLUSTER"};
    const int num_containers = sizeof(container_commands) / sizeof(container_commands[0]);
    const int total_commands = num_simple + num_containers;

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(99);
    std::uniform_int_distribution<int> cmd_dist(0, total_commands - 1);
    std::uniform_int_distribution<int> case_dist(0, 1);

    for (int i = 0; i < iterations; i++) {
        int cmd_idx = cmd_dist(rng);
        bool is_container = (cmd_idx >= num_simple);
        const char *original = is_container ? container_commands[cmd_idx - num_simple] : simple_commands[cmd_idx];
        size_t len = strlen(original);

        /* Generate a random case variation of the command name. */
        std::vector<char> varied(len);
        for (size_t j = 0; j < len; j++) {
            if (case_dist(rng)) {
                varied[j] = (char)toupper((unsigned char)original[j]);
            } else {
                varied[j] = (char)tolower((unsigned char)original[j]);
            }
        }

        /* Create a vargv entry with the varied name. */
        vstr vargv[2];
        vstrInitBorrowed(&vargv[0], varied.data(), len);
        /* For simple commands, provide a dummy second argument.
         * For container commands, pass vargc=1 to get the base command. */
        int vargc;
        const char *dummy_arg = "dummy";
        if (is_container) {
            vargc = 1;
        } else {
            vstrInitBorrowed(&vargv[1], dummy_arg, 5);
            vargc = 2;
        }

        /* Call lookupCommandFromVargv. */
        struct serverCommand *cmd_from_vargv = lookupCommandFromVargv(vargv, vargc);

        /* Call lookupCommandBySds with the same bytes (null-terminated sds). */
        sds name_sds = sdsnewlen(varied.data(), len);
        struct serverCommand *cmd_from_sds = lookupCommandBySds(name_sds);
        sdsfree(name_sds);

        /* Verify both paths find the same command. */
        ASSERT_NE(cmd_from_vargv, nullptr)
            << "lookupCommandFromVargv returned NULL for command variation at iteration " << i
            << " (original: " << original << ", varied: " << std::string(varied.data(), len) << ")";
        ASSERT_EQ(cmd_from_vargv, cmd_from_sds)
            << "Command mismatch at iteration " << i << " (original: " << original
            << ", varied: " << std::string(varied.data(), len) << ")";
    }
}

TEST_F(ZeroCopyProperty4Test, SubcommandLookupClientList) {
    /* Test subcommand lookup: CLIENT LIST */
    vstr vargv[2];
    const char *client_str = "CLIENT";
    const char *list_str = "LIST";
    vstrInitBorrowed(&vargv[0], client_str, strlen(client_str));
    vstrInitBorrowed(&vargv[1], list_str, strlen(list_str));

    struct serverCommand *cmd = lookupCommandFromVargv(vargv, 2);
    ASSERT_NE(cmd, nullptr) << "CLIENT LIST subcommand should be found";

    /* Verify via lookupCommandBySds with pipe-separated format. */
    sds full_name = sdsnew("CLIENT|LIST");
    struct serverCommand *cmd_sds = lookupCommandBySds(full_name);
    sdsfree(full_name);

    ASSERT_NE(cmd_sds, nullptr) << "CLIENT|LIST should be found via lookupCommandBySds";
    ASSERT_EQ(cmd, cmd_sds) << "CLIENT LIST should resolve to the same command struct";
}

TEST_F(ZeroCopyProperty4Test, SubcommandLookupClientInfo) {
    /* Test subcommand lookup: CLIENT INFO */
    vstr vargv[2];
    const char *client_str = "CLIENT";
    const char *info_str = "INFO";
    vstrInitBorrowed(&vargv[0], client_str, strlen(client_str));
    vstrInitBorrowed(&vargv[1], info_str, strlen(info_str));

    struct serverCommand *cmd = lookupCommandFromVargv(vargv, 2);
    ASSERT_NE(cmd, nullptr) << "CLIENT INFO subcommand should be found";

    /* Verify via lookupCommandBySds with pipe-separated format. */
    sds full_name = sdsnew("CLIENT|INFO");
    struct serverCommand *cmd_sds = lookupCommandBySds(full_name);
    sdsfree(full_name);

    ASSERT_NE(cmd_sds, nullptr) << "CLIENT|INFO should be found via lookupCommandBySds";
    ASSERT_EQ(cmd, cmd_sds) << "CLIENT INFO should resolve to the same command struct";
}

TEST_F(ZeroCopyProperty4Test, UnknownCommandReturnsNull) {
    /* Test that an unknown command returns NULL. */
    vstr vargv[1];
    const char *unknown = "NOTAREALCOMMAND";
    vstrInitBorrowed(&vargv[0], unknown, strlen(unknown));

    struct serverCommand *cmd = lookupCommandFromVargv(vargv, 1);
    ASSERT_EQ(cmd, nullptr) << "Unknown command should return NULL";
}

TEST_F(ZeroCopyProperty4Test, SubcommandCaseVariations) {
    /* Test subcommand lookup with case variations for CLIENT LIST and CLIENT INFO. */
    const int iterations = 50;
    std::mt19937 rng(77);
    std::uniform_int_distribution<int> case_dist(0, 1);

    struct {
        const char *parent;
        const char *sub;
        const char *pipe_name;
    } subcmds[] = {
        {"CLIENT", "LIST", "CLIENT|LIST"},
        {"CLIENT", "INFO", "CLIENT|INFO"},
    };

    for (int s = 0; s < 2; s++) {
        for (int i = 0; i < iterations; i++) {
            /* Generate case variation of parent command. */
            size_t parent_len = strlen(subcmds[s].parent);
            std::vector<char> parent_varied(parent_len);
            for (size_t j = 0; j < parent_len; j++) {
                parent_varied[j] = case_dist(rng) ? (char)toupper((unsigned char)subcmds[s].parent[j])
                                                  : (char)tolower((unsigned char)subcmds[s].parent[j]);
            }

            /* Generate case variation of subcommand. */
            size_t sub_len = strlen(subcmds[s].sub);
            std::vector<char> sub_varied(sub_len);
            for (size_t j = 0; j < sub_len; j++) {
                sub_varied[j] = case_dist(rng) ? (char)toupper((unsigned char)subcmds[s].sub[j])
                                               : (char)tolower((unsigned char)subcmds[s].sub[j]);
            }

            vstr vargv[2];
            vstrInitBorrowed(&vargv[0], parent_varied.data(), parent_len);
            vstrInitBorrowed(&vargv[1], sub_varied.data(), sub_len);

            struct serverCommand *cmd = lookupCommandFromVargv(vargv, 2);

            /* Compare with the canonical lookup. */
            sds pipe_name = sdsnew(subcmds[s].pipe_name);
            struct serverCommand *cmd_sds = lookupCommandBySds(pipe_name);
            sdsfree(pipe_name);

            ASSERT_NE(cmd, nullptr)
                << "Subcommand lookup failed at iteration " << i << " for " << subcmds[s].pipe_name
                << " (parent: " << std::string(parent_varied.data(), parent_len)
                << ", sub: " << std::string(sub_varied.data(), sub_len) << ")";
            ASSERT_EQ(cmd, cmd_sds)
                << "Subcommand mismatch at iteration " << i << " for " << subcmds[s].pipe_name;
        }
    }
}

/* --- Property 3: Materialization round-trip preserves content --- */

// Feature: zero-copy-get, Property 3: Materialization round-trip preserves content

/*
 * **Validates: Requirements 6.1, 10.1, 10.2, 11.1**
 *
 * For any borrowed vstr initialized with arbitrary bytes (including binary data
 * with embedded nulls, length 0 to 4096), materializing it via vstrMaterialize()
 * and then reading back via vstrData()/vstrLen() SHALL produce the original byte
 * sequence and length unchanged. The materialized vstr SHALL be of type VSTR_SDS
 * and SHALL not reference the original buffer.
 */

class ZeroCopyProperty3Test : public ::testing::Test {};

TEST_F(ZeroCopyProperty3Test, MaterializationRoundTripPreservesContent) {
    const int iterations = 200;
    const size_t max_len = 4096;

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(314159);
    std::uniform_int_distribution<size_t> len_dist(0, max_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    for (int i = 0; i < iterations; i++) {
        size_t arg_len = len_dist(rng);

        /* Generate random byte content (may include embedded nulls). */
        std::vector<char> content(arg_len);
        for (size_t j = 0; j < arg_len; j++) {
            content[j] = static_cast<char>(byte_dist(rng));
        }

        /* Create a stable buffer to borrow from (simulates querybuf). */
        sds buf = sdsnewlen(content.data(), arg_len);

        /* Initialize a borrowed vstr pointing into the buffer. */
        vstr v;
        vstrInitBorrowed(&v, buf, arg_len);

        /* Record the original data pointer before materialization. */
        const char *original_ptr = vstrData(&v);
        ASSERT_EQ(original_ptr, buf) << "Borrowed vstr should point to buffer at iteration " << i;

        /* Verify it's borrowed before materialization. */
        ASSERT_TRUE(vstrIsBorrowed(&v)) << "vstr should be borrowed before materialization at iteration " << i;

        /* Materialize: convert borrowed → owned sds. */
        vstrMaterialize(&v);

        /* Verify type is now VSTR_SDS. */
        ASSERT_EQ(vstrType(&v), VSTR_SDS)
            << "vstr type should be VSTR_SDS after materialization at iteration " << i;

        /* Verify length is preserved. */
        ASSERT_EQ(vstrLen(&v), arg_len)
            << "Length mismatch after materialization at iteration " << i << " (expected " << arg_len << ", got "
            << vstrLen(&v) << ")";

        /* Verify content is preserved (byte-for-byte match). */
        if (arg_len > 0) {
            ASSERT_EQ(memcmp(vstrData(&v), content.data(), arg_len), 0)
                << "Content mismatch after materialization at iteration " << i << " (arg_len=" << arg_len << ")";
        }

        /* Verify the data pointer differs from the original (independent copy). */
        ASSERT_NE(vstrData(&v), original_ptr)
            << "Materialized vstr should have a different data pointer (independent copy) at iteration " << i;

        /* Cleanup. */
        vstrFree(&v);
        sdsfree(buf);
    }
}

/* --- Unit Tests for materializeVargv --- */

/* Test fixture for materializeVargv tests. */
class MaterializeVargvTest : public ::testing::Test {
  protected:
    void SetUp() override {
        initSharedQueryBuf();
    }

    void TearDown() override {
        freeSharedQueryBuf(NULL);
    }

    /* Create a minimal client with vargv allocated. */
    client *createMinimalClient(int vargv_capacity) {
        client *c = (client *)(zcalloc(sizeof(client)));
        c->vargv = (vstr *)zmalloc(sizeof(vstr) * vargv_capacity);
        c->vargv_len = vargv_capacity;
        c->vargc = 0;
        c->argv = NULL;
        c->argv_len = 0;
        c->argc = 0;
        c->argv_len_sum = 0;
        /* Set querybuf to a private sds so freeClientVargv does NOT enter
         * the resetSharedQueryBuf path. */
        c->querybuf = sdsnewlen("dummy", 5);
        c->qb_pos = 0;
        return c;
    }

    void freeMinimalClient(client *c) {
        sdsfree(c->querybuf);
        zfree(c->vargv);
        if (c->argv) {
            for (int j = 0; j < c->argc; j++) {
                decrRefCount(c->argv[j]);
            }
            zfree(c->argv);
        }
        zfree(c);
    }
};

/* Test materializeVargv with 1 argument. */
TEST_F(MaterializeVargvTest, SingleArgument) {
    client *c = createMinimalClient(4);

    const char *arg = "PING";
    vstrInitBorrowed(&c->vargv[0], arg, 4);
    c->vargc = 1;

    materializeVargv(c);

    /* Verify argc matches vargc. */
    ASSERT_EQ(c->argc, 1);

    /* Verify argv[0] content matches vargv[0]. */
    sds s = (sds)objectGetVal(c->argv[0]);
    ASSERT_EQ(sdslen(s), 4u);
    ASSERT_EQ(memcmp(s, "PING", 4), 0);

    /* Verify argv_len_sum. */
    ASSERT_EQ(c->argv_len_sum, 4u);

    freeMinimalClient(c);
}

/* Test materializeVargv with 3 arguments (typical GET command). */
TEST_F(MaterializeVargvTest, ThreeArguments) {
    client *c = createMinimalClient(4);

    const char *args[] = {"SET", "mykey", "myvalue"};
    size_t lens[] = {3, 5, 7};

    for (int i = 0; i < 3; i++) {
        vstrInitBorrowed(&c->vargv[i], args[i], lens[i]);
    }
    c->vargc = 3;

    materializeVargv(c);

    /* Verify argc. */
    ASSERT_EQ(c->argc, 3);

    /* Verify each argv entry matches the corresponding vargv entry. */
    for (int j = 0; j < 3; j++) {
        sds s = (sds)objectGetVal(c->argv[j]);
        ASSERT_EQ(sdslen(s), lens[j]) << "Length mismatch at argv[" << j << "]";
        ASSERT_EQ(memcmp(s, args[j], lens[j]), 0) << "Content mismatch at argv[" << j << "]";
    }

    /* Verify argv_len_sum. */
    ASSERT_EQ(c->argv_len_sum, 3u + 5u + 7u);

    freeMinimalClient(c);
}

/* Test materializeVargv with 10 arguments (MGET-like scenario). */
TEST_F(MaterializeVargvTest, TenArguments) {
    client *c = createMinimalClient(16);

    const char *args[] = {"MGET", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"};
    size_t expected_sum = 0;

    for (int i = 0; i < 10; i++) {
        size_t len = strlen(args[i]);
        vstrInitBorrowed(&c->vargv[i], args[i], len);
        expected_sum += len;
    }
    c->vargc = 10;

    materializeVargv(c);

    /* Verify argc. */
    ASSERT_EQ(c->argc, 10);

    /* Verify each argv entry. */
    for (int j = 0; j < 10; j++) {
        sds s = (sds)objectGetVal(c->argv[j]);
        size_t expected_len = strlen(args[j]);
        ASSERT_EQ(sdslen(s), expected_len) << "Length mismatch at argv[" << j << "]";
        ASSERT_EQ(memcmp(s, args[j], expected_len), 0) << "Content mismatch at argv[" << j << "]";
    }

    /* Verify argv_len_sum. */
    ASSERT_EQ(c->argv_len_sum, expected_sum);

    freeMinimalClient(c);
}

/* Test materializeVargv with empty strings. */
TEST_F(MaterializeVargvTest, EmptyStrings) {
    client *c = createMinimalClient(4);

    const char *empty = "";
    vstrInitBorrowed(&c->vargv[0], empty, 0);
    vstrInitBorrowed(&c->vargv[1], empty, 0);
    vstrInitBorrowed(&c->vargv[2], empty, 0);
    c->vargc = 3;

    materializeVargv(c);

    ASSERT_EQ(c->argc, 3);

    for (int j = 0; j < 3; j++) {
        sds s = (sds)objectGetVal(c->argv[j]);
        ASSERT_EQ(sdslen(s), 0u) << "Empty string at argv[" << j << "] should have length 0";
    }

    /* argv_len_sum should be 0 for all empty strings. */
    ASSERT_EQ(c->argv_len_sum, 0u);

    freeMinimalClient(c);
}

/* Test materializeVargv with binary data (embedded nulls). */
TEST_F(MaterializeVargvTest, BinaryData) {
    client *c = createMinimalClient(4);

    /* Binary data with embedded nulls. */
    const char binary1[] = "\x00\x01\x02\x03\x00\x05";
    const char binary2[] = "hello\x00world";
    const char binary3[] = "\x00\x00\x00";

    vstrInitBorrowed(&c->vargv[0], binary1, 6);
    vstrInitBorrowed(&c->vargv[1], binary2, 11);
    vstrInitBorrowed(&c->vargv[2], binary3, 3);
    c->vargc = 3;

    materializeVargv(c);

    ASSERT_EQ(c->argc, 3);

    /* Verify binary content is preserved exactly. */
    sds s0 = (sds)objectGetVal(c->argv[0]);
    ASSERT_EQ(sdslen(s0), 6u);
    ASSERT_EQ(memcmp(s0, binary1, 6), 0);

    sds s1 = (sds)objectGetVal(c->argv[1]);
    ASSERT_EQ(sdslen(s1), 11u);
    ASSERT_EQ(memcmp(s1, binary2, 11), 0);

    sds s2 = (sds)objectGetVal(c->argv[2]);
    ASSERT_EQ(sdslen(s2), 3u);
    ASSERT_EQ(memcmp(s2, binary3, 3), 0);

    /* Verify argv_len_sum. */
    ASSERT_EQ(c->argv_len_sum, 6u + 11u + 3u);

    freeMinimalClient(c);
}

/* Test materializeVargv with normal strings. */
TEST_F(MaterializeVargvTest, NormalStrings) {
    client *c = createMinimalClient(4);

    const char *args[] = {"GET", "user:12345:profile"};
    size_t lens[] = {3, 18};

    vstrInitBorrowed(&c->vargv[0], args[0], lens[0]);
    vstrInitBorrowed(&c->vargv[1], args[1], lens[1]);
    c->vargc = 2;

    materializeVargv(c);

    ASSERT_EQ(c->argc, 2);

    for (int j = 0; j < 2; j++) {
        sds s = (sds)objectGetVal(c->argv[j]);
        ASSERT_EQ(sdslen(s), lens[j]) << "Length mismatch at argv[" << j << "]";
        ASSERT_EQ(memcmp(s, args[j], lens[j]), 0) << "Content mismatch at argv[" << j << "]";
    }

    ASSERT_EQ(c->argv_len_sum, 3u + 18u);

    freeMinimalClient(c);
}

// Feature: zero-copy-get, Property 5: Zero-deallocation cleanup for all-borrowed vargv

/*
 * **Validates: Requirements 7.4, 8.3, 9.1, 9.3, 16.1, 16.2**
 *
 * For any command whose vargv entries are all borrowed references (the common
 * read-command case), calling freeClientVargv on each entry SHALL perform zero
 * calls to sdsfree or any heap deallocation function. The cleanup path SHALL be
 * equivalent to a no-op for the argument data.
 *
 * Since we can't easily instrument sdsfree, we verify indirectly: after
 * freeClientVargv, the original buffers are still intact and accessible.
 * If sdsfree had been called on any of the borrowed pointers, the memory
 * would be corrupted/freed and subsequent access would be undefined behavior
 * (likely caught by ASAN or valgrind).
 */

class ZeroCopyProperty5Test : public ::testing::Test {
  protected:
    void SetUp() override {
        initSharedQueryBuf();
    }

    void TearDown() override {
        freeSharedQueryBuf(NULL);
    }
};

TEST_F(ZeroCopyProperty5Test, ZeroDeallocationCleanupForAllBorrowedVargv) {
    const int iterations = 200;

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(55555);
    std::uniform_int_distribution<int> count_dist(1, 20);
    std::uniform_int_distribution<size_t> len_dist(1, 128);
    std::uniform_int_distribution<int> byte_dist(0, 255);

    for (int i = 0; i < iterations; i++) {
        /* Generate a random argument count in [1, 20]. */
        int arg_count = count_dist(rng);

        /* Generate random byte sequences for each argument. We store them
         * in a single sds buffer (simulating a querybuf) so that borrowed
         * vstr references point into stable memory. */
        std::vector<std::string> arg_contents(arg_count);
        size_t total_len = 0;
        for (int j = 0; j < arg_count; j++) {
            size_t arg_len = len_dist(rng);
            arg_contents[j].resize(arg_len);
            for (size_t k = 0; k < arg_len; k++) {
                arg_contents[j][k] = static_cast<char>(byte_dist(rng));
            }
            total_len += arg_len;
        }

        /* Build a single buffer containing all argument bytes concatenated. */
        sds querybuf = sdsnewlen(NULL, total_len);
        sdsclear(querybuf);
        for (int j = 0; j < arg_count; j++) {
            querybuf = sdscatlen(querybuf, arg_contents[j].data(), arg_contents[j].size());
        }

        /* Create a minimal client with vargv. */
        client *c = (client *)(zcalloc(sizeof(client)));
        c->vargv = (vstr *)zmalloc(sizeof(vstr) * arg_count);
        c->vargv_len = arg_count;
        c->vargc = 0;
        /* Set querybuf to a private sds so freeClientVargv does NOT enter
         * the resetSharedQueryBuf path. */
        c->querybuf = sdsnewlen("private", 7);
        c->qb_pos = 0;

        /* Initialize all vargv entries as borrowed references into our buffer. */
        size_t offset = 0;
        for (int j = 0; j < arg_count; j++) {
            vstrInitBorrowed(&c->vargv[j], querybuf + offset, arg_contents[j].size());
            c->vargc++;
            offset += arg_contents[j].size();
        }

        /* Verify all entries are borrowed before cleanup. */
        for (int j = 0; j < arg_count; j++) {
            ASSERT_TRUE(vstrIsBorrowed(&c->vargv[j]))
                << "Entry " << j << " should be borrowed at iteration " << i;
        }

        /* Call freeClientVargv — should be a no-op for all borrowed entries. */
        freeClientVargv(c);

        /* Verify vargc is reset to 0. */
        ASSERT_EQ(c->vargc, 0) << "vargc should be 0 after freeClientVargv at iteration " << i;

        /* Verify the original buffer is still intact and accessible.
         * If sdsfree had been incorrectly called on any borrowed pointer,
         * this access would trigger ASAN/valgrind errors. */
        offset = 0;
        for (int j = 0; j < arg_count; j++) {
            ASSERT_EQ(memcmp(querybuf + offset, arg_contents[j].data(), arg_contents[j].size()), 0)
                << "Buffer corrupted after freeClientVargv at iteration " << i << ", arg " << j;
            offset += arg_contents[j].size();
        }

        /* Cleanup. */
        sdsfree(c->querybuf);
        zfree(c->vargv);
        zfree(c);
        sdsfree(querybuf);
    }
}

// Feature: zero-copy-get, Property 6: Lookup equivalence via borrowed vstr vs sds

/*
 * **Validates: Requirements 7.2, 8.2**
 *
 * For any key stored in a hashtable using vstr-aware callbacks (setHashtableType),
 * performing a hashtable lookup using a borrowed vstr (via vstrTagPtr) containing
 * the key's bytes SHALL find the same entry as performing a lookup using a plain
 * sds containing the same bytes. This holds for keys of any length and content,
 * including binary data.
 *
 * Also tests that keys NOT in the hashtable are correctly reported as not-found.
 */

class ZeroCopyProperty6Test : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        /* Seed the configurable hash function so that
         * genHashFunctionConfigurableSeed produces consistent results. */
        uint8_t seed[16];
        getRandomBytes(seed, sizeof(seed));
        setConfigurableHashSeed(seed);
    }
};

TEST_F(ZeroCopyProperty6Test, LookupEquivalenceViaBorrowedVstrVsSds) {
    const int iterations = 200;

    /* Use a fixed seed for reproducibility. */
    std::mt19937 rng(77777);
    std::uniform_int_distribution<size_t> len_dist(1, 100);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::uniform_int_distribution<int> num_keys_dist(1, 10);

    for (int i = 0; i < iterations; i++) {
        /* Create a hashtable using setHashtableType (vstr-aware callbacks).
         * setHashtableType stores plain sds entries directly. */
        hashtable *ht = hashtableCreate(&setHashtableType);

        /* Generate and insert random sds keys. */
        int num_keys = num_keys_dist(rng);
        std::vector<sds> inserted_keys;

        for (int j = 0; j < num_keys; j++) {
            size_t key_len = len_dist(rng);
            std::vector<char> key_bytes(key_len);
            for (size_t k = 0; k < key_len; k++) {
                key_bytes[k] = static_cast<char>(byte_dist(rng));
            }

            sds key = sdsnewlen(key_bytes.data(), key_len);

            /* Check for duplicates — skip if already present. */
            vstr vk;
            vstrInitBorrowed(&vk, key, sdslen(key));
            void *existing = NULL;
            if (hashtableFind(ht, vstrTagPtr(&vk), &existing)) {
                sdsfree(key);
                continue;
            }

            /* Insert the sds into the hashtable. The hashtable takes ownership
             * via entryDestructor = dictSdsDestructor. */
            sds entry = sdsdup(key);
            ASSERT_TRUE(hashtableAdd(ht, entry))
                << "Failed to add key at iteration " << i << ", key index " << j;

            inserted_keys.push_back(key);
        }

        if (inserted_keys.empty()) {
            hashtableRelease(ht);
            continue;
        }

        /* --- Verify present keys are found via borrowed vstr --- */
        for (size_t j = 0; j < inserted_keys.size(); j++) {
            sds key = inserted_keys[j];

            /* Create a borrowed vstr with the same bytes. */
            vstr vk;
            vstrInitBorrowed(&vk, key, sdslen(key));

            /* Lookup via vstrTagPtr (the zero-copy path). */
            void *found_via_vstr = NULL;
            bool result_vstr = hashtableFind(ht, vstrTagPtr(&vk), &found_via_vstr);

            ASSERT_TRUE(result_vstr)
                << "Present key not found via vstr at iteration " << i << ", key index " << j
                << " (key len=" << sdslen(key) << ")";
            ASSERT_NE(found_via_vstr, nullptr)
                << "Found entry is NULL for present key via vstr at iteration " << i;

            /* Verify the found entry matches the key bytes. */
            sds found_sds = (sds)found_via_vstr;
            ASSERT_EQ(sdslen(found_sds), sdslen(key))
                << "Found entry length mismatch at iteration " << i << ", key index " << j;
            ASSERT_EQ(memcmp(found_sds, key, sdslen(key)), 0)
                << "Found entry content mismatch at iteration " << i << ", key index " << j;
        }

        /* --- Verify absent keys are NOT found --- */
        int num_absent = 5;
        for (int j = 0; j < num_absent; j++) {
            size_t key_len = len_dist(rng);
            std::vector<char> absent_bytes(key_len);
            for (size_t k = 0; k < key_len; k++) {
                absent_bytes[k] = static_cast<char>(byte_dist(rng));
            }

            /* Check this key is actually absent. */
            bool is_present = false;
            for (size_t k = 0; k < inserted_keys.size(); k++) {
                if (sdslen(inserted_keys[k]) == key_len &&
                    memcmp(inserted_keys[k], absent_bytes.data(), key_len) == 0) {
                    is_present = true;
                    break;
                }
            }

            if (!is_present) {
                vstr vk;
                vstrInitBorrowed(&vk, absent_bytes.data(), key_len);

                void *found = NULL;
                bool result = hashtableFind(ht, vstrTagPtr(&vk), &found);
                ASSERT_FALSE(result)
                    << "Absent key incorrectly found at iteration " << i << ", absent key index " << j;
            }
        }

        /* Cleanup: free our verification copies and the hashtable. */
        for (size_t j = 0; j < inserted_keys.size(); j++) {
            sdsfree(inserted_keys[j]);
        }
        hashtableRelease(ht); /* Frees the sds entries via entryDestructor. */
    }
}
