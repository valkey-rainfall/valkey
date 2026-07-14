#include "hashfunctions.h"
#include "sds.h"
#include "hashtable.h"
#include <string.h>
#include <strings.h>

/* Configurable hash seed for data hashtables (keys, sets, zsets, hashes).
 * Allows deterministic iteration order across cluster nodes. */
static uint8_t configurable_hash_seed[16];

extern uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);

void setConfigurableHashSeed(uint8_t *seed) {
    memcpy(configurable_hash_seed, seed, sizeof(configurable_hash_seed));
}

uint8_t *getConfigurableHashSeed(void) {
    return configurable_hash_seed;
}

uint64_t genHashFunctionConfigurableSeed(const char *buf, size_t len) {
    return siphash((const uint8_t *)buf, len, configurable_hash_seed);
}

uint64_t sdsHashConfigurableSeed(const void *key) {
    return genHashFunctionConfigurableSeed(key, sdslen(key));
}

/* SDS hash/compare (case-sensitive) */
uint64_t dictSdsHash(const void *key) {
    return hashtableGenHashFunction(key, sdslen(key));
}

bool dictSdsKeyCompare(const void *key1, const void *key2) {
    int l1 = sdslen((sds)key1);
    int l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* SDS hash/compare (case-insensitive) */
uint64_t dictSdsCaseHash(const void *key) {
    return hashtableGenCaseHashFunction(key, sdslen(key));
}

bool dictSdsKeyCaseCompare(const void *key1, const void *key2) {
    return strcasecmp(key1, key2) == 0;
}

/* C-string hash/compare (case-sensitive) */
uint64_t dictCStrHash(const void *key) {
    return hashtableGenHashFunction(key, strlen(key));
}

bool dictCStrKeyCompare(const void *key1, const void *key2) {
    return strcmp(key1, key2) == 0;
}

/* C-string hash/compare (case-insensitive) */
uint64_t dictCStrCaseHash(const void *key) {
    return hashtableGenCaseHashFunction(key, strlen(key));
}

bool dictCStrKeyCaseCompare(const void *key1, const void *key2) {
    return strcasecmp(key1, key2) == 0;
}

/* Pointer hash/compare (identity by pointer value) */
uint64_t dictPtrHash(const void *key) {
    return hashtableGenHashFunction((const char *)&key, sizeof(key));
}

bool dictPtrKeyCompare(const void *key1, const void *key2) {
    return key1 == key2;
}
