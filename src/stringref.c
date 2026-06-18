#include "stringref.h"
#include "hashtable.h"
#include "sds.h"

uint64_t stringRefHash(const stringRef *ref) {
    return hashtableGenHashFunction(ref->buf, ref->len);
}

uint64_t stringRefCaseHash(const stringRef *ref) {
    return hashtableGenCaseHashFunction(ref->buf, ref->len);
}

sds stringRefToSds(const stringRef *ref) {
    return sdsnewlen(ref->buf, ref->len);
}
