/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "fmacros.h"

#include "vstr.h"
#include "sds.h"
#include "serverassert.h"

#include <string.h>

/* Verify the struct fits in 24 bytes (3 words on 64-bit). */
static_assert(sizeof(vstr) <= 24, "vstr should fit in 24 bytes");

/* --- Access --- */

const char *vstrData(const vstr *v) {
    switch (v->type) {
    case VSTR_SDS: return v->sds.s;
    case VSTR_BORROWED: return v->borrowed.ptr;
    }
    return NULL;
}

size_t vstrLen(const vstr *v) {
    switch (v->type) {
    case VSTR_SDS: return sdslen(v->sds.s);
    case VSTR_BORROWED: return v->borrowed.len;
    }
    return 0;
}

/* --- Comparison and hashing --- */

bool vstrEqual(const vstr *a, const vstr *b) {
    size_t la = vstrLen(a);
    size_t lb = vstrLen(b);
    if (la != lb) return false;
    return memcmp(vstrData(a), vstrData(b), la) == 0;
}

bool vstrEqualRaw(const vstr *v, const char *ptr, size_t len) {
    size_t vlen = vstrLen(v);
    if (vlen != len) return false;
    return memcmp(vstrData(v), ptr, len) == 0;
}

bool vstrEqualSds(const vstr *v, const char *s) {
    return vstrEqualRaw(v, s, sdslen(s));
}

/* Hash using the same function as sds keys, for compatibility with existing
 * hashtable entries. */
extern uint64_t genHashFunctionConfigurableSeed(const char *buf, size_t len);

uint64_t vstrHash(const vstr *v) {
    return genHashFunctionConfigurableSeed(vstrData(v), vstrLen(v));
}

/* --- Ownership and materialization --- */

void vstrMaterialize(vstr *v) {
    if (v->type != VSTR_BORROWED) return;
    sds s = sdsnewlen(v->borrowed.ptr, v->borrowed.len);
    v->type = VSTR_SDS;
    v->sds.s = s;
}

char *vstrTakeSds(vstr *v) {
    switch (v->type) {
    case VSTR_SDS: {
        char *s = v->sds.s;
        v->sds.s = NULL;
        return s;
    }
    case VSTR_BORROWED:
        return sdsnewlen(v->borrowed.ptr, v->borrowed.len);
    }
    return NULL;
}

void vstrFree(vstr *v) {
    if (v->type == VSTR_SDS && v->sds.s != NULL) {
        sdsfree(v->sds.s);
        v->sds.s = NULL;
    }
    /* BORROWED: nothing to free. */
}
