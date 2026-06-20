#ifndef STRINGREF_H
#define STRINGREF_H

#include <stddef.h>
#include <stdint.h>
#include "sds.h"

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
stringRef stringRefCreate(const char *buf, size_t len);
stringRef stringRefFromSds(sds s);

/* Access */
const char *stringRefBuf(const stringRef *ref);
size_t stringRefLen(const stringRef *ref);

/* Comparison — memcmp semantics (negative/0/positive) */
int stringRefCmp(const stringRef *a, const stringRef *b);

/* Equality check. Returns 1 if equal, 0 otherwise.
 * Short-circuits on length mismatch before touching memory. */
int stringRefEqual(const stringRef *a, const stringRef *b);

/* Case-insensitive equality check against a null-terminated C string.
 * Returns 1 if equal, 0 otherwise. */
int stringRefCaseEqualCStr(const stringRef *ref, const char *cstr);

/* Hash */
uint64_t stringRefHash(const stringRef *ref);
uint64_t stringRefCaseHash(const stringRef *ref);

/* Materialization — allocates a new sds copy. Caller owns the result. */
sds stringRefToSds(const stringRef *ref);

#endif /* STRINGREF_H */
