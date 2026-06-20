#include "stringref.h"
#include "hashtable.h"
#include <string.h>
#include <strings.h>

/* Construction */
stringRef stringRefCreate(const char *buf, size_t len) {
    return (stringRef){.buf = buf, .len = len};
}

stringRef stringRefFromSds(sds s) {
    return (stringRef){.buf = s, .len = sdslen(s)};
}

/* Access */
const char *stringRefBuf(const stringRef *ref) {
    return ref->buf;
}

size_t stringRefLen(const stringRef *ref) {
    return ref->len;
}

/* Comparison — memcmp semantics */
int stringRefCmp(const stringRef *a, const stringRef *b) {
    size_t minlen = a->len < b->len ? a->len : b->len;
    int cmp = memcmp(a->buf, b->buf, minlen);
    if (cmp != 0) return cmp;
    if (a->len < b->len) return -1;
    if (a->len > b->len) return 1;
    return 0;
}

/* Equality check */
int stringRefEqual(const stringRef *a, const stringRef *b) {
    if (a->len != b->len) return 0;
    return memcmp(a->buf, b->buf, a->len) == 0;
}

/* Case-insensitive equality against C string */
int stringRefCaseEqualCStr(const stringRef *ref, const char *cstr) {
    size_t clen = strlen(cstr);
    if (ref->len != clen) return 0;
    return strncasecmp(ref->buf, cstr, clen) == 0;
}

/* Hash */
uint64_t stringRefHash(const stringRef *ref) {
    return hashtableGenHashFunction(ref->buf, ref->len);
}

uint64_t stringRefCaseHash(const stringRef *ref) {
    return hashtableGenCaseHashFunction(ref->buf, ref->len);
}

/* Materialization */
sds stringRefToSds(const stringRef *ref) {
    return sdsnewlen(ref->buf, ref->len);
}
