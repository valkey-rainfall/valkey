/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* vstr — Valkey string type.
 *
 * A lightweight, polymorphic string representation for use as command arguments,
 * lookup keys, and transient values. Unlike robj, vstr carries no LRU/LFU
 * metadata, no type/encoding beyond string variants, and no dataset semantics.
 *
 * Representations:
 *
 *   VSTR_SDS       — Owned sds string. The vstr owns the allocation and frees
 *                    it on vstrFree().
 *
 *   VSTR_BORROWED  — A (ptr, len) reference into memory owned by someone else
 *                    (e.g., a client query buffer). No allocation, no free.
 *                    Valid only for the lifetime of the underlying buffer.
 *
 * The primary use case is command argument parsing: the RESP parser produces
 * borrowed vstr references into the query buffer instead of allocating sds
 * strings. Hashtable lookups, membership checks, and other read operations
 * use the borrowed bytes directly. Allocation only happens when the string
 * must be stored (materialization).
 *
 * Materialization:
 *
 *   A borrowed vstr does not own its memory. If code needs to retain the string
 *   beyond the lifetime of the underlying buffer, it must call vstrMaterialize()
 *   to copy the bytes into an owned sds representation.
 *
 * Stack allocation:
 *
 *   vstr is designed for stack allocation. The common pattern is:
 *
 *       vstr v;
 *       vstrInitBorrowed(&v, ptr, len);
 *       hashtableFind(ht, &v, &found);
 *       // no cleanup needed — borrowed vstr doesn't own memory
 */

#ifndef VSTR_H
#define VSTR_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* Representation types. */
#define VSTR_SDS 0
#define VSTR_BORROWED 1

typedef struct vstr {
    uint8_t type;
    union {
        /* VSTR_SDS: owned sds string. */
        struct {
            char *s; /* sds pointer */
        } sds;

        /* VSTR_BORROWED: non-owned reference. */
        struct {
            const char *ptr;
            size_t len;
        } borrowed;
    };
} vstr;

/* --- Initialization (no allocation) --- */

/* Initialize a vstr as a borrowed reference. No allocation, no ownership. */
static inline void vstrInitBorrowed(vstr *v, const char *ptr, size_t len) {
    v->type = VSTR_BORROWED;
    v->borrowed.ptr = ptr;
    v->borrowed.len = len;
}

/* Initialize a vstr from an owned sds. The vstr takes ownership — the caller
 * must not free the sds after this call. */
static inline void vstrInitSds(vstr *v, char *s) {
    v->type = VSTR_SDS;
    v->sds.s = s;
}

/* --- Access --- */

/* Get a pointer to the string bytes. Always valid for both types. */
const char *vstrData(const vstr *v);

/* Get the string length. Always valid for both types. */
size_t vstrLen(const vstr *v);

/* Get the representation type. */
static inline uint8_t vstrType(const vstr *v) {
    return v->type;
}

/* Returns true if the vstr is a borrowed reference (does not own its memory). */
static inline bool vstrIsBorrowed(const vstr *v) {
    return v->type == VSTR_BORROWED;
}

/* --- Comparison and hashing --- */

/* Compare two vstrs for equality. Works across both representation types. */
bool vstrEqual(const vstr *a, const vstr *b);

/* Compare a vstr against raw bytes. */
bool vstrEqualRaw(const vstr *v, const char *ptr, size_t len);

/* Compare a vstr against an sds. */
bool vstrEqualSds(const vstr *v, const char *s);

/* Hash the vstr bytes using the configurable hash seed. */
uint64_t vstrHash(const vstr *v);

/* --- Ownership and materialization --- */

/* Ensure the vstr owns its memory. If borrowed, copies the bytes into an
 * owned sds. If already owned (SDS), this is a no-op. */
void vstrMaterialize(vstr *v);

/* Extract the owned sds from a vstr, invalidating the vstr. For SDS, returns
 * the sds directly. For BORROWED, allocates a new sds copy. After this call,
 * the vstr is in an undefined state and must not be used without
 * re-initialization. */
char *vstrTakeSds(vstr *v);

/* Free any owned memory. Safe to call on any vstr type — no-op for BORROWED.
 * After this call, the vstr is in an undefined state. */
void vstrFree(vstr *v);

#endif /* VSTR_H */
