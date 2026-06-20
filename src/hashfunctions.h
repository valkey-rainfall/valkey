#ifndef HASHFUNCTIONS_H
#define HASHFUNCTIONS_H

#include <stdint.h>
#include <stddef.h>

/* Generic hash/compare functions for hashtableType definitions.
 * These operate on raw keys (sds, C strings, robj, pointers) and are used
 * both directly in hashtableType.hashKey/keyCompare fields and as building
 * blocks for dictEntry wrapper callbacks. Implementations are in server.c. */

/* Configurable-seed hash (used by data structures: sets, zsets, hashes) */
uint64_t genHashFunctionConfigurableSeed(const char *buf, size_t len);
uint64_t sdsHashConfigurableSeed(const void *key);
uint8_t *getConfigurableHashSeed(void);
void setConfigurableHashSeed(uint8_t *seed);

/* SDS hash/compare (case-sensitive) */
uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(const void *key1, const void *key2);

/* SDS hash/compare (case-insensitive) */
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCaseCompare(const void *key1, const void *key2);

/* C-string hash/compare (case-sensitive) */
uint64_t dictCStrHash(const void *key);
int dictCStrKeyCompare(const void *key1, const void *key2);

/* C-string hash/compare (case-insensitive) */
uint64_t dictCStrCaseHash(const void *key);
int dictCStrKeyCaseCompare(const void *key1, const void *key2);

/* Encoded robj hash/compare (handles OBJ_ENCODING_INT) */
uint64_t dictEncObjHash(const void *key);
int dictEncObjKeyCompare(const void *key1, const void *key2);

/* Pointer hash/compare (identity by pointer value) */
uint64_t dictPtrHash(const void *key);
int dictPtrKeyCompare(const void *key1, const void *key2);

#endif /* HASHFUNCTIONS_H */
