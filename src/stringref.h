#ifndef STRINGREF_H
#define STRINGREF_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Forward declare sds type to avoid circular includes. */
typedef char *sds;

/* A non-owning view of a string buffer. Does not manage memory.
 *
 * Can be used as:
 * - A transient stack-local view for hashtable lookups (avoids temp sds alloc)
 * - A long-lived reference to externally owned memory (e.g. module buffers)
 *
 * The caller must ensure the referenced memory outlives the stringRef. */
typedef struct stringRef {
    const char *buf; /* Pointer to the buffer */
    size_t len;      /* Length of the buffer */
} stringRef;

/* Construction */
static inline stringRef stringRefCreate(const char *buf, size_t len) {
    return (stringRef){.buf = buf, .len = len};
}

static inline stringRef stringRefFromSds(sds s) {
    /* sds stores length before the pointer; we call sdslen indirectly.
     * To avoid including sds.h here, caller should use stringRefCreate
     * with the known length, or use the macro below after including sds.h. */
    return (stringRef){.buf = s, .len = 0}; /* placeholder — see STRINGREF_FROM_SDS */
}

/* Use this macro when sds.h is included (provides sdslen). */
#define STRINGREF_FROM_SDS(s) ((stringRef){.buf = (s), .len = sdslen(s)})

/* Access */
static inline const char *stringRefBuf(const stringRef *ref) {
    return ref->buf;
}

static inline size_t stringRefLen(const stringRef *ref) {
    return ref->len;
}

/* Comparison — memcmp semantics (0 = equal) */
static inline int stringRefCmp(const stringRef *a, const stringRef *b) {
    size_t minlen = a->len < b->len ? a->len : b->len;
    int cmp = memcmp(a->buf, b->buf, minlen);
    if (cmp != 0) return cmp;
    if (a->len < b->len) return -1;
    if (a->len > b->len) return 1;
    return 0;
}

/* Hash — uses the same siphash as hashtableGenHashFunction.
 * Declared here, implemented in stringref.c to avoid exposing siphash. */
uint64_t stringRefHash(const stringRef *ref);

/* Materialization — allocates a new sds copy. Caller owns the result.
 * Declared here, implemented in stringref.c to avoid including sds.h. */
sds stringRefToSds(const stringRef *ref);

#endif /* STRINGREF_H */
