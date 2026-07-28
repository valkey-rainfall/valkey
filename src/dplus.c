/* D+ Read-Side Prototype — Speculative GET execution on IO threads.
 * See dplus.h for architecture overview and reply-ordering guarantee. */

#include "dplus.h"
#include "server.h"

#include <string.h>
#include <stdatomic.h>

/* --- Component 4: Exclusive mode globals --- */
_Atomic(int) dplus_exclusive_mode = 0;
_Atomic(int) dplus_in_speculative_read[DPLUS_MAX_IO_THREADS] = {0};

/* --- Component 6: Stats --- */
#ifdef IO_LOOKUP_OFFLOAD_STATS
dplusStats dplus_stats = {0};
#endif

/* --- Component 4: Exclusive mode implementation --- */

void dplusExclusiveEnter(void) {
    /* Set exclusive mode with seq_cst to ensure visibility to all IO threads. */
    atomic_store_explicit(&dplus_exclusive_mode, 1, memory_order_seq_cst);
    /* Spin until no IO thread is in a speculative read. Bounded by one GET
     * execution time (sub-microsecond). */
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) {
        while (atomic_load_explicit(&dplus_in_speculative_read[i], memory_order_seq_cst)) {
            /* Spin — bounded by single GET latency. */
        }
    }
}

void dplusExclusiveLeave(void) {
    atomic_store_explicit(&dplus_exclusive_mode, 0, memory_order_seq_cst);
}

/* --- Component 2: Speculative GET on IO thread --- */

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int dplusWriteBulkReply(client *c, const char *val, size_t vallen) {
    /* RESP2 bulk: $<len>\r\n<data>\r\n
     * RESP3 bulk: same format for simple strings. */
    char hdr[32];
    int hdrlen = snprintf(hdr, sizeof(hdr), "$%zu\r\n", vallen);
    size_t total = hdrlen + vallen + 2; /* +2 for trailing \r\n */
    size_t available = c->buf_usable_size - c->bufpos;
    if (total > available) return 0; /* Won't fit — punt */

    memcpy(c->buf + c->bufpos, hdr, hdrlen);
    c->bufpos += hdrlen;
    memcpy(c->buf + c->bufpos, val, vallen);
    c->bufpos += vallen;
    memcpy(c->buf + c->bufpos, "\r\n", 2);
    c->bufpos += 2;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return (int)total;
}

/* Format a RESP nil reply directly into the client's output buffer. */
static int dplusWriteNilReply(client *c, int resp) {
    const char *nil_resp;
    size_t nil_len;
    if (resp >= 3) {
        nil_resp = "_\r\n";
        nil_len = 3;
    } else {
        nil_resp = "$-1\r\n";
        nil_len = 5;
    }
    size_t available = c->buf_usable_size - c->bufpos;
    if (nil_len > available) return 0;
    memcpy(c->buf + c->bufpos, nil_resp, nil_len);
    c->bufpos += nil_len;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return (int)nil_len;
}

/* Format a RESP integer reply for OBJ_ENCODING_INT robj. */
static int dplusWriteIntAsBulk(client *c, long long intval) {
    /* Convert int to string, then emit as bulk string. */
    char buf[21]; /* max int64 string length */
    int len = ll2string(buf, sizeof(buf), intval);
    return dplusWriteBulkReply(c, buf, len);
}

/* Execute a speculative GET for the given key on the IO thread.
 *
 * Returns 1 on success (reply written to c->buf), 0 on punt (main thread
 * must execute). The caller must have already set dplus_in_speculative_read
 * for the thread and verified !exclusive_mode.
 *
 * key_sds: the SDS key string (objectGetVal(argv[1]))
 * resp: client RESP version (2 or 3)
 */
int dplusSpeculativeGet(client *c, void *key_sds, int resp) {
    serverDb *db = c->db;
    int dict_index = 0; /* Non-cluster: always 0. */

    /* Get the hashtable for the keys kvstore. */
    hashtable *ht = kvstoreGetHashtable(db->keys, dict_index);
    if (!ht) {
        /* Empty database — reply nil. */
        return dplusWriteNilReply(c, resp) > 0 ? 1 : 0;
    }

    /* Compute hash, determine shard. */
    uint64_t hash = kvstoreHashForKey(db->keys, dict_index, key_sds);
    unsigned shard = DPLUS_SHARD_INDEX(hash);

    /* Get the version array from the hashtable. */
    dplusVersionArray *va = hashtableGetVersionArray(ht);
    if (!va) return 0; /* No version array — punt */

    /* Read shard version (acquire). */
    uint64_t v_before = dplusVersionRead(va, shard);

    /* Speculative lookup — read-only, no rehash step. */
    void *entry = NULL;
    bool found = hashtableFindReadOnly(ht, key_sds, &entry);

    if (!found) {
        /* Key not found. Validate version. */
        uint64_t v_after = dplusVersionRead(va, shard);
        if (v_before != v_after) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.validation_misses, 1, memory_order_relaxed);
#endif
            return 0; /* Version mismatch — punt */
        }
        /* Valid nil — write nil reply. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.speculative_hits, 1, memory_order_relaxed);
#endif
        return dplusWriteNilReply(c, resp) > 0 ? 1 : 0;
    }

    /* Found an entry. It's an robj*. */
    robj *o = (robj *)entry;

    /* Type check: must be OBJ_STRING. Non-string → punt (error reply too complex). */
    if (objectGetType(o) != OBJ_STRING) return 0;

    /* Expiry check: if the key has an expire, check logically.
     * We do NOT call expireIfNeeded (that mutates) — just check timestamp. */
    if (o->hasexpire) {
        /* Look up expiry from the expires kvstore — read-only. */
        void *expire_entry = NULL;
        hashtable *expire_ht = kvstoreGetHashtable(db->expires, dict_index);
        if (expire_ht && hashtableFindReadOnly(expire_ht, key_sds, &expire_entry)) {
            long long when = objectGetExpire((robj *)expire_entry);
            mstime_t now = mstime();
            if (when >= 0 && now >= when) {
                /* Logically expired — reply nil, enqueue lazy-delete to main.
                 * For the prototype, just punt to main (it handles TTL cleanly). */
#ifdef IO_LOOKUP_OFFLOAD_STATS
                atomic_fetch_add_explicit(&dplus_stats.expired_replies, 1, memory_order_relaxed);
#endif
                return 0; /* Punt — main handles expiry + notifications */
            }
        }
    }

    /* Value extraction: copy value bytes BEFORE validation re-read.
     * Only handle embstr and int encodings (small values).
     * RAW encoding (heap-allocated SDS) with len > threshold → punt. */
    char valbuf[DPLUS_MAX_SPECULATIVE_VALUE_LEN + 21]; /* extra for int formatting */
    size_t vallen;
    int encoding = objectGetEncoding(o);

    if (encoding == OBJ_ENCODING_INT) {
        long long intval = (long long)(long)objectGetVal(o);
        vallen = ll2string(valbuf, sizeof(valbuf), intval);
    } else if (encoding == OBJ_ENCODING_EMBSTR || encoding == OBJ_ENCODING_RAW) {
        sds s = objectGetVal(o);
        vallen = sdslen(s);
        if (vallen > DPLUS_MAX_SPECULATIVE_VALUE_LEN) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.large_value_punts, 1, memory_order_relaxed);
#endif
            return 0; /* Large value — punt */
        }
        memcpy(valbuf, s, vallen);
    } else {
        return 0; /* Unknown encoding — punt */
    }

    /* VALIDATION RE-READ: check shard version after value copy. */
    uint64_t v_after = dplusVersionRead(va, shard);
    if (v_before != v_after) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.validation_misses, 1, memory_order_relaxed);
#endif
        return 0; /* Torn read — punt, no reply written */
    }

    /* Version valid — write reply. */
    int written;
    if (encoding == OBJ_ENCODING_INT) {
        written = dplusWriteBulkReply(c, valbuf, vallen);
    } else {
        written = dplusWriteBulkReply(c, valbuf, vallen);
    }

    if (written <= 0) return 0; /* Buffer full — punt */

#ifdef IO_LOOKUP_OFFLOAD_STATS
    atomic_fetch_add_explicit(&dplus_stats.speculative_hits, 1, memory_order_relaxed);
#endif
    return 1; /* Success — reply in buffer, skip main-thread execution */
}
