/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* The value prefetch stage of the fast GET batch (fastGetBatchResolve in fastget.c) prefetches the
 * value's sds only when the value is stored outside the object's allocation, which it decides from
 * the encoding alone: RAW means val_ptr is a separate sds, EMBSTR means the value follows the embedded
 * key inside the object, INT has no pointer. These tests pin the object layout facts that decision
 * relies on, for values built the way dbAdd builds them (createStringObjectWithKeyAndExpire), on both
 * sides of the embedding limit and at the lengths that change the sds header size. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "server.h"
/* fastget.h uses <stdatomic.h>, which C++ cannot include; declare what the test calls. */
char *fastGetValueToPrefetch(const robj *o);
}

class FastGetValuePrefetchTest : public ::testing::Test {
  protected:
    /* Objects are built with the metadata size the server runs with by default (0); the lengths below
     * assume it. The size is process-wide and another suite may have set it: as ObjectTest does, hide
     * that suite's databases, start from 0 and leave 0 behind. */
    void SetUp() override {
        saved_db = server.db;
        saved_dbnum = server.dbnum;
        server.db = NULL;
        server.dbnum = 0;
        testOnlyResetObjectMetadataSize();
    }
    void TearDown() override {
        server.db = saved_db;
        server.dbnum = saved_dbnum;
    }
    static void TearDownTestSuite() { testOnlyResetObjectMetadataSize(); }
    serverDb **saved_db = nullptr;
    int saved_dbnum = 0;

    /* Value lengths around the embedding limit of shouldEmbedStringObject: with a 16-byte key (an
     * SDS_TYPE_5 sds of 1 + 16 + 1 bytes, plus the header-size byte), no expire and no metadata the
     * object is 8 + 19 + (len + 4) bytes and embeds while that is at most 128, so len 97 embeds and len
     * 98 does not; with an expire field 89 embeds and 90 does not. 255 and 256 are the SDS_TYPE_8
     * limit, 1024 and 1025 the fast GET size limit. */
    static std::vector<size_t> lengths() {
        return {0, 1, 15, 16, 17, 31, 32, 44, 45, 88, 89, 90, 96, 97, 98, 100, 128, 255, 256, 512, 1024, 1025, 4000};
    }

    static bool inside(const robj *o, const void *p) {
        const char *base = reinterpret_cast<const char *>(o);
        const char *q = static_cast<const char *>(p);
        return q >= base && q < base + zmalloc_size(const_cast<robj *>(o));
    }
};

/* For every length, with and without an expire: the encoding tells where the value is. RAW has the
 * sds in its own allocation (outside the object) and val_ptr is that sds; EMBSTR has it inside the
 * object, right after the embedded key; the prefetch target is the sds for RAW and NULL otherwise. */
TEST_F(FastGetValuePrefetchTest, PrefetchesOnlyValuesStoredOutsideTheObject) {
    sds key = sdsnew("key:000000000001"); /* 16 bytes, the benchmark key format */
    for (long long expire : {(long long)EXPIRY_NONE, 1700000000000LL}) {
        for (size_t len : lengths()) {
            std::string v(len, 'x');
            robj *o = createStringObjectWithKeyAndExpire(v.data(), len, key, expire);
            ASSERT_NE(o, nullptr);
            EXPECT_EQ(objectGetType(o), OBJ_STRING);
            int enc = objectGetEncoding(o);
            char *val = static_cast<char *>(objectGetVal(o));
            char *target = fastGetValueToPrefetch(o);
            SCOPED_TRACE("len " + std::to_string(len) + (expire == EXPIRY_NONE ? " no expire" : " with expire"));

            /* The flag and the encoding move together (object.c sets them in the same constructors). */
            EXPECT_EQ(o->hasembval != 0, enc == OBJ_ENCODING_EMBSTR);
            ASSERT_TRUE(enc == OBJ_ENCODING_EMBSTR || enc == OBJ_ENCODING_RAW);
            EXPECT_EQ(sdslen(val), len);

            if (enc == OBJ_ENCODING_RAW) {
                /* A separate allocation: neither the header nor the bytes are in the object. */
                EXPECT_FALSE(inside(o, val - 1));
                EXPECT_FALSE(inside(o, val));
                EXPECT_EQ(o->val_ptr, val);
                EXPECT_EQ(target, val) << "a RAW value is prefetched through its sds pointer";
            } else {
                /* Inside the object, 4 bytes after the last byte of the embedded key (NUL, then the
                 * 3-byte SDS_TYPE_8 header), within the allocation the key compare already loaded. */
                sds ekey = objectGetKey(o);
                ASSERT_NE(ekey, nullptr);
                EXPECT_EQ(val - 3, ekey + sdslen(ekey) + 1);
                EXPECT_EQ(sdsType(val), SDS_TYPE_8);
                EXPECT_TRUE(inside(o, val - 3));
                EXPECT_TRUE(inside(o, val + len));
                EXPECT_LE(zmalloc_size(o), (size_t)128);
                EXPECT_EQ(target, nullptr) << "an embedded value is not prefetched";
            }
            decrRefCount(o);
        }
    }
    sdsfree(key);
}

/* The embedding limit itself, as the comment on lengths() states it. */
TEST_F(FastGetValuePrefetchTest, EmbeddingLimitWithASixteenByteKey) {
    sds key = sdsnew("key:000000000001");
    std::string v(98, 'x');
    robj *emb = createStringObjectWithKeyAndExpire(v.data(), 97, key, EXPIRY_NONE);
    robj *raw = createStringObjectWithKeyAndExpire(v.data(), 98, key, EXPIRY_NONE);
    robj *emb_x = createStringObjectWithKeyAndExpire(v.data(), 89, key, 1700000000000LL);
    robj *raw_x = createStringObjectWithKeyAndExpire(v.data(), 90, key, 1700000000000LL);
    EXPECT_EQ(objectGetEncoding(emb), OBJ_ENCODING_EMBSTR);
    EXPECT_EQ(objectGetEncoding(raw), OBJ_ENCODING_RAW);
    EXPECT_EQ(objectGetEncoding(emb_x), OBJ_ENCODING_EMBSTR);
    EXPECT_EQ(objectGetEncoding(raw_x), OBJ_ENCODING_RAW);
    EXPECT_EQ(fastGetValueToPrefetch(emb), nullptr);
    EXPECT_EQ(fastGetValueToPrefetch(raw), objectGetVal(raw));
    EXPECT_EQ(fastGetValueToPrefetch(emb_x), nullptr);
    EXPECT_EQ(fastGetValueToPrefetch(raw_x), objectGetVal(raw_x));
    decrRefCount(emb);
    decrRefCount(raw);
    decrRefCount(emb_x);
    decrRefCount(raw_x);
    sdsfree(key);
}

/* An INT value has no pointer; a value that is RAW but not a string (a module value) is left alone. */
TEST_F(FastGetValuePrefetchTest, IntAndNonStringValuesAreNotPrefetched) {
    sds key = sdsnew("key:000000000001");
    robj *num = createStringObject("12345", 5);
    robj *entry = tryPrebuildStringEntry(num, key);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(objectGetEncoding(entry), OBJ_ENCODING_INT);
    EXPECT_EQ(entry->hasembval, 0u);
    EXPECT_EQ(fastGetValueToPrefetch(entry), nullptr);
    decrRefCount(entry);
    decrRefCount(num);

    robj m;
    memset(&m, 0, sizeof(m));
    m.type = OBJ_MODULE;
    m.encoding = OBJ_ENCODING_RAW;
    m.refcount = OBJ_STATIC_REFCOUNT;
    m.val_ptr = &m;
    EXPECT_EQ(fastGetValueToPrefetch(&m), nullptr);

    robj *h = createHashObject();
    EXPECT_EQ(fastGetValueToPrefetch(h), nullptr);
    decrRefCount(h);
    sdsfree(key);
}

/* A value that was embedded and then unembedded (objectUnembedVal, used when a value grows in
 * place) becomes RAW with hasembval clear and val_ptr the new sds: the rule holds for it too. */
TEST_F(FastGetValuePrefetchTest, UnembeddedValueIsPrefetchedThroughValPtr) {
    sds key = sdsnew("key:000000000001");
    robj *o = createStringObjectWithKeyAndExpire("sixteen-byte-val", 16, key, EXPIRY_NONE);
    ASSERT_EQ(objectGetEncoding(o), OBJ_ENCODING_EMBSTR);
    objectUnembedVal(o);
    EXPECT_EQ(objectGetEncoding(o), OBJ_ENCODING_RAW);
    EXPECT_EQ(o->hasembval, 0u);
    char *val = static_cast<char *>(objectGetVal(o));
    EXPECT_EQ(o->val_ptr, val);
    EXPECT_EQ(fastGetValueToPrefetch(o), val);
    EXPECT_EQ(memcmp(val, "sixteen-byte-val", 16), 0);
    EXPECT_EQ(sdslen(objectGetKey(o)), (size_t)16); /* the key moved with the data */
    decrRefCount(o);
    sdsfree(key);
}
