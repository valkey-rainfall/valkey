/* D+ Read-Side Prototype — Speculative GET execution on IO threads.
 * See dplus.h for architecture overview and reply-ordering guarantee. */

#include "server.h"
#include "dplus.h"

#include <string.h>
#include <stdatomic.h>

/* --- Component 4: Exclusive mode globals --- */
_Atomic(int) dplus_exclusive_mode = 0;
_Atomic(int) dplus_in_speculative_read[DPLUS_MAX_IO_THREADS] = {0};

/* --- Per-IO-thread command counters (no-enqueue Phase-1) --- */
dplusThreadStats dplus_thread_stats[DPLUS_MAX_IO_THREADS] = {{0}};

/* --- Write-tax gate (per-IO-thread) --- */
dplusWriteTaxGate dplus_write_tax[DPLUS_MAX_IO_THREADS] = {{.history = ~(uint64_t)0}}; /* Start optimistic (all-ones = all speculated) */

/* --- Component 6: Stats --- */
#ifdef IO_LOOKUP_OFFLOAD_STATS
dplusStats dplus_stats = {0};
#endif

/* --- Component 4: Exclusive mode implementation --- */

/* Exclusive mode is a COUNTER, not a flag (changed for the expiry-race fix):
 * hashtable teardown paths (rehashingCompleted / hashtableEmpty) now enter
 * exclusive mode, and they can run NESTED inside an existing exclusive
 * window (FLUSHALL's proc is already wrapped) or from a BIO lazyfree thread
 * CONCURRENTLY with main. fetch_add/fetch_sub keeps every window intact;
 * workers punt while the counter is nonzero (their existing nonzero-true
 * load needs no change). Every enterer spin-drains — trivially fast when a
 * nested/concurrent enterer already drained (flags stay 0 while count>0). */
void dplusExclusiveEnter(void) {
    atomic_fetch_add_explicit(&dplus_exclusive_mode, 1, memory_order_seq_cst);
    /* No IO threads => no speculation to drain. Also makes this BOOT-SAFE:
     * hashtable teardown paths call here during earliest init (command-table
     * dict rehash), before the monotonic clock is initialized — the fast
     * paths below must not touch getMonotonicUs() unless a walk flag is
     * actually set (impossible before workers exist). */
    if (server.io_threads_num <= 1) return;
    /* Spin until no IO thread is in a speculative read. Bounded by one GET
     * execution time (sub-microsecond). */
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) {
        if (!atomic_load_explicit(&dplus_in_speculative_read[i], memory_order_seq_cst)) continue;
        monotime _w = getMonotonicUs();
        while (atomic_load_explicit(&dplus_in_speculative_read[i], memory_order_seq_cst)) {
            /* Spin — bounded by single GET latency. */
            if (getMonotonicUs() - _w > 5000000) serverPanic("dplusExclusiveEnter spin timeout on slot %d", i);
        }
    }
}

void dplusExclusiveLeave(void) {
    atomic_fetch_sub_explicit(&dplus_exclusive_mode, 1, memory_order_seq_cst);
}

/* --- ACL/AUTH gate for speculation --- */

/* Recompute the per-client speculation ACL gate. MAIN THREAD ONLY.
 * Speculation bypasses processCommand(), so the NOAUTH check and
 * ACLCheckAllPerm never run on the speculative path — this byte is the
 * substitute, computed at every auth-state change (clientSetUser covers
 * AUTH/HELLO/RESET/module-auth/deluser-kick), on SELECT (per-db ACLs), and
 * on ACL admin ops (dplusOnAclRulesChanged).
 *
 * Condition (graceful give-up, per design): authenticated AND some selector
 * permits GET with UNRESTRICTED key read access (~* / %R~* / allkeys).
 * Key-pattern-restricted users PUNT to the stock main-thread path — per-key
 * pattern matching on workers would race main-mutated user structs.
 * flag.authenticated (sticky, only changed via clientSetUser) is used
 * instead of authRequired(): the latter reads DefaultUser->flags which can
 * change with no per-client event (CONFIG SET requirepass) — conservative
 * punts only, never a bypass. */
void dplusRecomputeSpecAclOk(client *c) {
    uint8_t ok = 0;
    if (c->flag.authenticated) {
        if (c->user == NULL) {
            /* No associated user = unrestricted context (stock semantics). */
            ok = 1;
        } else {
            static struct serverCommand *get_cmd = NULL;
            if (!get_cmd) get_cmd = lookupCommandByCString("get");
            /* Synthetic argv: the helper only needs cmd identity + dbid for
             * the command/db checks; the ALLKEYS-or-%R~* requirement is what
             * actually gates key access, so a placeholder key can only cause
             * a (safe) conservative punt, never a false allow. */
            robj *argv[2];
            argv[0] = createStringObject("get", 3);
            argv[1] = createStringObject("k", 1);
            int dbid = c->db ? c->db->id : 0;
            if (ACLUserCheckCmdWithUnrestrictedKeyAccess(c->user, get_cmd, argv, 2, dbid, CMD_KEY_ACCESS)) ok = 1;
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
    atomic_store_explicit(&c->spec_acl_ok, ok, memory_order_release);
}

/* ACL rules changed (ACL SETUSER/DELUSER/LOAD, module SetUserACL).
 * MAIN THREAD ONLY. Enter exclusive FIRST: blocks new speculation and drains
 * in-flight batches that passed the OLD gate (they serialize before the
 * change — stock-equivalent semantics, F12-C temporal-window class), then
 * recompute every client's gate under the new rules. Rare admin op — the
 * blanket client walk is deliberate simplicity. */
void dplusOnAclRulesChanged(void) {
    dplusExclusiveEnter();
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) dplusRecomputeSpecAclOk((client *)listNodeValue(ln));
    dplusExclusiveLeave();
}

/* --- Component 2: Speculative GET — reply helpers --- */

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int dplusWriteBulkReply(client *c, const char *val, size_t vallen) {
    /* Fast bulk-header formatting: "$<len>\r\n" using ll2string instead of
     * snprintf (eliminates 2.6% __vfprintf_internal from worker profile). */
    char hdr[32];
    hdr[0] = '$';
    int numlen = ll2string(hdr + 1, sizeof(hdr) - 3, (long long)vallen);
    hdr[numlen + 1] = '\r';
    hdr[numlen + 2] = '\n';
    int hdrlen = numlen + 3;
    size_t total = hdrlen + vallen + 2; /* trailing \r\n */
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

/* --- Component 2: Core speculative GET execution --- */

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
    int dict_index = 0; /* Non-cluster: always slot 0. */

    /* Get the hashtable for the keys kvstore. */
    hashtable *ht = kvstoreGetHashtable(db->keys, dict_index);
    if (!ht) {
        /* Empty database — reply nil. */
        return dplusWriteNilReply(c, resp) > 0 ? 1 : 0;
    }

    /* Compute hash, determine shard. */
    uint64_t hash = hashtableHashKey(ht, key_sds);
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

    /* Type check: must be OBJ_STRING. Non-string → punt. */
    if (objectGetType(o) != OBJ_STRING) return 0;

    /* Expiry check: read embedded expiry from the robj (no separate lookup).
     * We do NOT call expireIfNeeded (that mutates) — just check timestamp. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
            /* Logically expired — punt to main for lazy-delete + notifications. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.expired_replies, 1, memory_order_relaxed);
#endif
            return 0;
        }
    }

    /* Value extraction: copy value bytes BEFORE validation re-read.
     * Only handle embstr and int encodings (small values).
     * RAW encoding with len > threshold → punt. */
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
    int written = dplusWriteBulkReply(c, valbuf, vallen);
    if (written <= 0) return 0; /* Buffer full — punt */

#ifdef IO_LOOKUP_OFFLOAD_STATS
    atomic_fetch_add_explicit(&dplus_stats.speculative_hits, 1, memory_order_relaxed);
#endif
    return 1; /* Success — reply in buffer, skip main-thread execution */
}

/* --- Component 2/5: IO-thread batch speculation with batched prefetch --- */

/* Maximum batch depth for prefetch overlap. Matches server.prefetch_batch_max_size default. */
#define DPLUS_BATCH_DEPTH 16

/* Per-key state for the batched prefetch + validate pipeline. */
typedef struct dplusBatchEntry {
    void *key_sds;                           /* SDS key string */
    uint64_t hash;                           /* Pre-computed hash */
    unsigned shard;                          /* Version shard index */
    uint64_t v_before;                       /* Version read before find */
    hashtableIncrementalFindState find_state; /* Incremental find state */
    int is_first_cmd;                        /* 1 if this is c->argv, 0 if queue */
    int queue_idx;                           /* Index in cmd_queue (if !is_first_cmd) */
} dplusBatchEntry;

/* Execute the validate+copy+reply phase for a single prefetched entry.
 * Returns 1 on success (reply written), 0 on punt. */
static int dplusValidateAndReply(client *c, dplusBatchEntry *e, hashtable *ht, int resp) {
    dplusVersionArray *va = hashtableGetVersionArray(ht);

    /* Get the find result — entry is now in cache from prefetch. */
    void *entry = NULL;
    bool found = hashtableIncrementalFindGetResult(&e->find_state, &entry);

    if (!found) {
        /* Key not found. Validate version. */
        uint64_t v_after = dplusVersionRead(va, e->shard);
        if (e->v_before != v_after) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.validation_misses, 1, memory_order_relaxed);
#endif
            return 0;
        }
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.speculative_hits, 1, memory_order_relaxed);
#endif
        return dplusWriteNilReply(c, resp) > 0 ? 1 : 0;
    }

    /* Found an entry. It's an robj*. */
    robj *o = (robj *)entry;

    /* Type check: must be OBJ_STRING. */
    if (objectGetType(o) != OBJ_STRING) return 0;

    /* Expiry check. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.expired_replies, 1, memory_order_relaxed);
#endif
            return 0;
        }
    }

    /* Value extraction: copy BEFORE validation re-read. */
    char valbuf[DPLUS_MAX_SPECULATIVE_VALUE_LEN + 21];
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
            return 0;
        }
        memcpy(valbuf, s, vallen);
    } else {
        return 0;
    }

    /* VALIDATION RE-READ: check shard version after value copy. */
    uint64_t v_after = dplusVersionRead(va, e->shard);
    if (e->v_before != v_after) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.validation_misses, 1, memory_order_relaxed);
#endif
        return 0;
    }

    /* Version valid — write reply. */
    int written = dplusWriteBulkReply(c, valbuf, vallen);
    if (written <= 0) return 0;

#ifdef IO_LOOKUP_OFFLOAD_STATS
    atomic_fetch_add_explicit(&dplus_stats.speculative_hits, 1, memory_order_relaxed);
#endif
    return 1;
}

/* Called from ioThreadReadQueryFromClient after parsing is complete.
 * Iterates the parsed command queue attempting speculative GETs on a
 * contiguous prefix. Uses batched incremental-find to overlap memory
 * prefetches across multiple keys before executing the serial
 * validate+copy+reply phase with warm caches.
 *
 * REPLY ORDERING: The contiguous-prefix invariant guarantees that all
 * speculative replies are written into c->buf BEFORE the main thread
 * processes any punted commands (which append their replies after).
 * This preserves pipeline reply ordering without any reordering logic.
 *
 * Returns the number of commands speculatively completed.
 */
int dplusSpeculateBatch(client *c, int tid) {
    int speculated = 0;

    /* Early exit: speculation disabled (single-threaded or cluster mode). */
    if (server.io_threads_num <= 1 || server.cluster_enabled) return 0;

    /* CLIENT-STATE GUARD: inside MULTI every command must reply +QUEUED and
     * execute only at EXEC — speculating a GET here would execute it early
     * and strand it from the transaction (real bug: pipelined MULTI batches
     * were safe only because MULTI heads the batch; a GET arriving in a
     * LATER read while flag.multi is set would be speculated). Blocked /
     * just-unblocked clients must have replies ordered after the unblock
     * reply. Cheap plain-byte tests, ordered before any atomics.
     *
     * BUFFER-STATE GUARD: dplusWriteBulkReply appends RAW RESP to c->buf.
     * If the buffer is in copy-avoidance ENCODED mode (header-framed
     * payloads), raw bytes corrupt the framing — trackBufReferences walks
     * off the payload chain ('ptr == buf + bufpos' assert; found by the
     * fork's dataset tests mixing KEYS/HSET-encoded replies with speculated
     * GETs). A non-empty reply list is equally disqualifying: new reply
     * data belongs at the list tail, not in buf, or replies reorder. */
    if (c->flag.multi || c->flag.blocked || c->flag.unblocked ||
        c->flag.buf_encoded || listLength(c->reply) != 0)
        return 0;

    /* DEFRAG GUARD: active defrag MOVES allocations (entries/sds) and frees
     * the originals outside the limbo hooks — reads racing a move are the
     * same UAF class. Punt speculation while defrag is running (rare,
     * default-off feature; costs nothing otherwise). */
    if (server.active_defrag_cpu_percent > 0) return 0;

    /* ZERO-COST WRITE-BATCH EXIT: if the first command isn't even a GET,
     * return before touching any atomics or gate state. Eliminates the r100
     * write-tax (2 seq_cst ops + gate history shift cost on pure-write batches).
     * Uses a lazily-cached command pointer (static, idempotent init). */
    {
        static struct serverCommand *dplus_get_cmd_fast = NULL;
        if (!dplus_get_cmd_fast) dplus_get_cmd_fast = lookupCommandByCString("get");
        if (c->argc != 2 || c->parsed_cmd != dplus_get_cmd_fast) return 0;
    }

    /* ACL/AUTH GUARD: speculation bypasses processCommand, so neither the
     * NOAUTH check nor ACLCheckAllPerm ever runs on this path. Only clients
     * pre-cleared on the main thread may speculate; everyone else punts to
     * the stock path which enforces auth/ACL as usual. The byte is written
     * only at rare auth-state changes, so this read does not ping-pong the
     * line. Placed after the GET fast-exit to keep pure-write batches at
     * zero added cost. ACL admin ops drain in-flight speculation before
     * gates update (dplusOnAclRulesChanged), closing the temporal window. */
    if (!atomic_load_explicit(&c->spec_acl_ok, memory_order_acquire)) return 0;

    /* Write-tax gate pointer — declared early so the out: label can update it.
     * The actual gate CHECK happens below, after first-command eligibility is
     * determined (to avoid a dead-state where the gate blocks all speculation
     * including the GET traffic that would recover it). */
    dplusWriteTaxGate *gate = &dplus_write_tax[tid];

    /* Lazily cache the GET command pointer — eliminates repeated case-insensitive
     * siphash lookups (7% of worker profile per the per-TID decomposition). */
    static struct serverCommand *dplus_get_cmd = NULL;
    if (!dplus_get_cmd) dplus_get_cmd = lookupCommandByCString("get");

    /* Set per-thread flag (seq_cst handshake with exclusive mode). */
    atomic_store_explicit(&dplus_in_speculative_read[tid], 1, memory_order_seq_cst);

    /* Time the batch for commandstats parity: speculated GETs bypass call(),
     * so their duration must be accumulated per-thread and folded into the
     * GET command's microseconds by dplusAggregateStats (as with calls). */
    monotime spec_start = getMonotonicUs();

    /* Check exclusive mode AFTER setting flag (barrier handshake). */
    if (atomic_load_explicit(&dplus_exclusive_mode, memory_order_seq_cst)) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.exclusive_punts, 1, memory_order_relaxed);
#endif
        goto out;
    }

    /* --- Phase 1: Collect eligible prefix keys into batch --- */
    serverDb *db = c->db;
    int dict_index = 0;
    hashtable *ht = kvstoreGetHashtable(db->keys, dict_index);
    if (!ht) goto out;

    dplusVersionArray *va = hashtableGetVersionArray(ht);
    if (!va) goto out;

    dplusBatchEntry batch[DPLUS_BATCH_DEPTH];
    int batch_count = 0;

    /* First command (c->argv / c->parsed_cmd). */
    if (c->argc == 2 && c->parsed_cmd != NULL &&
        c->parsed_cmd == dplus_get_cmd &&
        (c->parsed_cmd->flags & (CMD_READONLY | CMD_FAST)) == (CMD_READONLY | CMD_FAST)) {

        /* Write-tax gate: first command IS eligible (GET), so we know the
         * workload can speculate. Check the gate to decide whether recent
         * traffic justifies the prefetch investment. If traffic has been
         * write-heavy, speculation attempts mostly hit validation misses,
         * wasting prefetch cycles. Skip the batch but shift in a 1 so the
         * gate recovers once GETs resume. */
        if (__builtin_popcountll(gate->history) < DPLUS_WRITE_TAX_THRESHOLD) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.intra_batch_write_punts, 1, memory_order_relaxed);
#endif
            /* Shift in 1 (eligible batch seen) to allow recovery. */
            gate->history = (gate->history << 1) | 1;
            goto out;
        }

        dplusBatchEntry *e = &batch[batch_count];
        e->key_sds = objectGetVal(c->argv[1]);
        e->hash = hashtableHashKey(ht, e->key_sds);
        e->shard = DPLUS_SHARD_INDEX(e->hash);
        e->v_before = dplusVersionRead(va, e->shard);
        e->is_first_cmd = 1;
        e->queue_idx = -1;
        batch_count++;
    } else {
        /* First command not eligible — nothing to speculate. */
        goto out;
    }

    /* Command queue (pipelined commands): STREAMING SLOT-REFILL (fix #1b).
     * The chunked design processed the prefix in phase-bounded windows of
     * DPLUS_BATCH_DEPTH: fill 16, step all finds to completion, validate all
     * 16, repeat. Every chunk boundary was a pipeline bubble — the next
     * chunk's finds started cold with zero overlap.
     *
     * Streaming replaces the phases with a ring of DPLUS_BATCH_DEPTH slots
     * holding IN-FLIGHT finds. Each sweep steps every pending find once
     * (one prefetch issued per slot per sweep — the same interleave cadence
     * that made the chunked design cache-friendly). Replies must be emitted
     * in command order, so validation+reply happens only at the HEAD slot:
     * the moment the head's find completes it is validated and replied
     * (cache-warm), the slot is freed, and the fill loop tops the window
     * back up from the queue. Prefetch depth stays constant across the
     * entire eligible prefix — no bubbles. Out-of-order completions simply
     * wait as completed find states until they become the head; the seqlock
     * validation at reply time covers the (unchanged) racy window.
     * The contiguous-prefix rule holds globally: a validation failure or
     * ineligible command ends speculation for the whole read. */
    cmdQueue *queue = &c->cmd_queue;
    int queue_pos = queue->off;
    int prefix_open = 1;

    /* Ring state: batch[] reused as the slot array. head..tail are monotonic;
     * slot index = seq % DPLUS_BATCH_DEPTH. done[] marks completed finds so
     * they are not re-stepped. batch_count==1 from the first-command block
     * above: seed the ring with it. */
    unsigned head = 0, tail = 0;
    uint8_t done[DPLUS_BATCH_DEPTH];
    hashtableIncrementalFindInit(&batch[0].find_state, ht, batch[0].key_sds);
    done[0] = 0;
    tail = 1;
    (void)batch_count;

    while (head < tail || prefix_open) {
        /* Fill: top the window up from the queue. */
        while (prefix_open && tail - head < DPLUS_BATCH_DEPTH && queue_pos < queue->len) {
            parsedCommand *p = &queue->cmds[queue_pos];
            if (p->cmd == NULL || !(p->cmd->flags & CMD_READONLY) || p->argc != 2 ||
                !(p->cmd->flags & CMD_FAST) || p->cmd != dplus_get_cmd) {
                prefix_open = 0; /* ineligible command ends the global prefix */
                break;
            }
            dplusBatchEntry *e = &batch[tail % DPLUS_BATCH_DEPTH];
            e->key_sds = objectGetVal(p->argv[1]);
            e->hash = hashtableHashKey(ht, e->key_sds);
            e->shard = DPLUS_SHARD_INDEX(e->hash);
            e->v_before = dplusVersionRead(va, e->shard);
            e->is_first_cmd = 0;
            e->queue_idx = queue_pos;
            hashtableIncrementalFindInit(&e->find_state, ht, e->key_sds);
            done[tail % DPLUS_BATCH_DEPTH] = 0;
            tail++;
            queue_pos++;
        }
        if (queue_pos >= queue->len) prefix_open = 0; /* queue exhausted — drain */
        if (head == tail) break; /* nothing in flight and nothing to fill */

        /* Sweep: one find step per pending slot (one prefetch each). */
        for (unsigned s = head; s < tail; s++) {
            unsigned i = s % DPLUS_BATCH_DEPTH;
            if (!done[i]) done[i] = !hashtableIncrementalFindStep(&batch[i].find_state);
        }

        /* Drain: validate + reply completed HEADS in command order. */
        while (head < tail && done[head % DPLUS_BATCH_DEPTH]) {
            dplusBatchEntry *e = &batch[head % DPLUS_BATCH_DEPTH];
#ifdef IO_LOOKUP_OFFLOAD_STATS
            atomic_fetch_add_explicit(&dplus_stats.speculative_attempts, 1, memory_order_relaxed);
#endif
            if (dplusValidateAndReply(c, e, ht, c->resp)) {
                if (e->is_first_cmd) {
                    c->read_flags |= READ_FLAGS_DPLUS_SPECULATED;
                } else {
                    queue->cmds[e->queue_idx].read_flags |= READ_FLAGS_DPLUS_SPECULATED;
                }
                speculated++;
                head++;
            } else {
                /* Contiguous-prefix rule: first failure ends speculation
                 * globally. In-flight slots behind it are abandoned (their
                 * commands fall through to main execution). */
                prefix_open = 0;
                head = tail; /* discard remaining in-flight slots */
                break;
            }
        }
    }

out:
    /* Update write-tax gate: shift in 1 if any speculation succeeded, 0 if punted. */
    gate->history = (gate->history << 1) | (speculated > 0 ? 1 : 0);

    /* Commandstats parity: fold execution time into the per-thread counter
     * (calls are accumulated in dplusConsumeSpeculated). */
    if (speculated > 0) dplus_thread_stats[tid].usec += (long long)(getMonotonicUs() - spec_start);

    /* Clear per-thread flag. */
    atomic_store_explicit(&dplus_in_speculative_read[tid], 0, memory_order_seq_cst);
    return speculated;
}

/* --- Phase-1 no-enqueue: IO-thread command consumption --- */

/* Consume speculated commands at the IO thread so they never reach main.
 *
 * After dplusSpeculateBatch writes replies into c->buf and marks commands
 * with READ_FLAGS_DPLUS_SPECULATED, this function:
 * 1. Frees the first command's argv (c->argv) and clears pending_command state.
 * 2. Advances cmd_queue past all DPLUS_SPECULATED entries, freeing their argv.
 * 3. Increments per-IO-thread command counter (NOT shared stat_numcommands).
 * 4. Increments c->commands_processed directly — safe because the IO thread
 *    owns the client exclusively during the read phase (io_read_state ==
 *    CLIENT_PENDING_IO; main thread will not touch the client until we
 *    set CLIENT_COMPLETED_IO and send to main).
 *
 * After this, c->argc == 0, pending_command is clear, and cmd_queue has no
 * speculated entries. Main-thread processClientIOReadsDone sees nothing to
 * execute. */
void dplusConsumeSpeculated(client *c, int count, int tid) {
    int consumed = 0;

    /* --- First command (c->argv / c->parsed_cmd case) --- */
    if (count > 0 && (c->read_flags & READ_FLAGS_DPLUS_SPECULATED)) {
        c->read_flags &= ~READ_FLAGS_DPLUS_SPECULATED;
        /* CRITICAL: clear pending_command — the parse path set it, and without
         * clearing it here the main thread's processPendingCommandAndInputBuffer
         * would call processCommandAndResetClient() on argc==0 + a STALE c->cmd
         * (the removed main-side skip path used to clear this flag). */
        c->flag.pending_command = 0;
        /* Free argv INLINE — do NOT call freeClientArgv() which routes through
         * tryOffloadFreeArgvToIOThreads(). That function increments the non-atomic
         * io_jobs_submitted from the IO thread (data race with main) and enqueues
         * to a SPSC queue potentially owned by this same thread (invariant
         * violation: SPSC is single-producer = main thread only). The race causes
         * lost increments -> submitted < finished -> infinite spin in
         * drainIOThreadsQueue on shutdown. */
        for (int j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
        zfree(c->argv);
        c->argv = NULL;
        c->argc = 0;
        c->cmd = NULL;
        c->parsed_cmd = NULL;
        c->argv_len_sum = 0;
        c->argv_len = 0;
        c->slot = -1;
        c->commands_processed++;
        consumed++;
    }

    /* --- Command queue entries --- */
    cmdQueue *queue = &c->cmd_queue;
    while (consumed < count && queue->off < queue->len) {
        parsedCommand *p = &queue->cmds[queue->off];
        if (!(p->read_flags & READ_FLAGS_DPLUS_SPECULATED)) break;
        p->read_flags &= ~READ_FLAGS_DPLUS_SPECULATED;
        queue->off++;
        /* Free argv for the consumed command. */
        for (int j = 0; j < p->argc; j++) {
            decrRefCount(p->argv[j]);
        }
        zfree(p->argv);
        p->argv = NULL;
        p->argc = 0;
        c->commands_processed++;
        consumed++;
    }

    /* Compact queue if fully consumed. */
    if (queue->off == queue->len) {
        queue->off = queue->len = 0;
    }

    /* Accumulate into per-IO-thread counter (no shared-line write). */
    dplus_thread_stats[tid].commands_processed += consumed;
}

/* Aggregate per-IO-thread command counters into server.stat_numcommands.
 * Called from beforeSleep once per event-loop iteration. The main thread
 * is the sole writer of stat_numcommands, so this is a plain add-and-zero
 * with no atomics needed on the server side. The IO threads only increment
 * their own counter (single-writer per slot), so a plain read + zero is
 * safe: worst case we miss a few commands this iteration and catch them
 * next time (bounded lag of one event-loop cycle, ~1ms).
 *
 * BOUNDARY CALLERS (beyond beforeSleep): CONFIG RESETSTAT and INFO
 * commandstats must call this too — otherwise pending per-thread counts
 * survive a reset (stale +N folds in later) or are missing from a read
 * (found by valkey-benchmark.tcl: calls=101 vs 100, calls=10000 vs 10010).
 * For RESETSTAT the settle happens BEFORE the zeroing, absorbing pending
 * counts into the stats about to be cleared. */
void dplusAggregateStats(void) {
    /* Commandstats parity: speculated GETs bypass call(), which is where
     * cmd->calls / cmd->microseconds are normally incremented. Fold the
     * per-thread counters into the GET command here so INFO COMMANDSTATS
     * (and everything downstream) sees them. Only GET is ever speculated,
     * so a single target command suffices.
     *
     * MONOTONIC-DELTA scheme (NOT add-and-zero): workers increment their
     * slots concurrently with this fold; zeroing a slot from main races the
     * worker's read-modify-write and PERMANENTLY loses counts (observed as
     * calls=99-vs-100 once INFO/RESETSTAT-time folds made folding frequent).
     * Workers' counters only ever grow; main folds the delta since its own
     * last_seen snapshot. A racy read can only UNDER-read (missing the very
     * latest increment), which the next fold picks up — never loses. */
    static long long seen_calls[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_usec[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_writes[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_net_bytes[DPLUS_MAX_IO_THREADS] = {0};
    static struct serverCommand *get_cmd = NULL;
    if (!get_cmd) get_cmd = lookupCommandByCString("get");
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = dplus_thread_stats[i].commands_processed;
        if (n > seen_calls[i]) {
            long long delta = n - seen_calls[i];
            seen_calls[i] = n;
            server.stat_numcommands += delta;
            if (get_cmd) get_cmd->calls += delta;
        }
        long long us = dplus_thread_stats[i].usec;
        if (us > seen_usec[i]) {
            long long delta = us - seen_usec[i];
            seen_usec[i] = us;
            if (get_cmd) get_cmd->microseconds += delta;
        }
        /* Fix #2 (main-free completion): fold worker-side write completions
         * into the global write stats. Same monotonic-delta scheme. */
        long long w = dplus_thread_stats[i].owned_writes;
        if (w > seen_writes[i]) {
            server.stat_total_writes_processed += w - seen_writes[i];
            seen_writes[i] = w;
        }
        long long b = dplus_thread_stats[i].owned_net_bytes;
        if (b > seen_net_bytes[i]) {
            server.stat_net_output_bytes += b - seen_net_bytes[i];
            seen_net_bytes[i] = b;
        }
    }
}

/* --- Component 7: Quiescence-deferred reclamation (entry lifetime) ---
 *
 * See entry-lifetime-design.md. Worker speculation reads hashtable entry
 * memory (key compare in the walk, value copy, robj field reads) that main
 * frees on delete/expiry/overwrite. Copy-then-validate discards stale
 * RESULTS but still LOADS from freed memory — UB that was benign only while
 * jemalloc kept pages mapped (mass expiry unmapped one: SIGSEGV in
 * dictSdsKeyCompare, both ownership modes).
 *
 * Scheme: db->keys mutation paths defer robj frees onto this main-thread
 * limbo list instead of freeing. beforeSleep flushes: one bounded
 * exclusive-mode drain (workers' walks are sub-µs and announced via
 * dplus_in_speculative_read), then the swapped-out list is freed OUTSIDE
 * the window — preserving the original sync/lazyfree routing per object.
 * A pointer reachable by any walk is thus never freed until every walk
 * that could have observed it has completed. */

typedef struct dplusLimboEntry {
    robj *o;
    uint8_t route; /* DPLUS_LIMBO_SYNC / _ASYNC / _OFFLOAD_PREF */
} dplusLimboEntry;

static dplusLimboEntry *dplus_limbo = NULL;
static size_t dplus_limbo_len = 0;
static size_t dplus_limbo_cap = 0;
static size_t dplus_limbo_peak = 0; /* high-water for INFO/tests */

/* Defer an robj free until the next quiescence flush. Returns 1 if deferred,
 * 0 if the caller should free immediately (no walkers can exist). Callers
 * pass the object AFTER unlinking it from the table, so no NEW walk can
 * reach it; only in-flight walks might hold it.
 *
 * route preserves the caller's free-path choice, DECIDED AT DEFER TIME
 * (lazyfree's effort heuristic needs the key, which is gone by flush time):
 *   DPLUS_LIMBO_SYNC          decrRefCount at flush
 *   DPLUS_LIMBO_ASYNC         hand to BIO lazyfree at flush (pre-judged)
 *   DPLUS_LIMBO_OFFLOAD_PREF  prefer IO-thread free offload, else sync */
int dplusDeferFree(robj *o, int route) {
    /* No IO threads => no speculative walkers => immediate free is safe.
     * (Also keeps single-threaded perf and boot paths untouched.) */
    if (server.io_threads_num <= 1) return 0;
    if (dplus_limbo_len == dplus_limbo_cap) {
        dplus_limbo_cap = dplus_limbo_cap ? dplus_limbo_cap * 2 : 128;
        dplus_limbo = zrealloc(dplus_limbo, dplus_limbo_cap * sizeof(dplusLimboEntry));
    }
    dplus_limbo[dplus_limbo_len].o = o;
    dplus_limbo[dplus_limbo_len].route = (uint8_t)route;
    dplus_limbo_len++;
    if (dplus_limbo_len > dplus_limbo_peak) dplus_limbo_peak = dplus_limbo_len;
    return 1;
}

/* Raw-pointer variant: zfree at flush. For non-robj memory a walk can hold —
 * child BUCKETS freed by delete-path chain compaction (pruneLastBucket) and
 * per-step rehash cleanup (rehashStepFinalize). Part 1 only drained
 * whole-table frees; these incremental frees were the residual escape
 * (OFF-mode expiry-leg crash: walker held a chain bucket while main's
 * expiry delete compacted it away). */
int dplusDeferFreeRaw(void *ptr) {
    if (server.io_threads_num <= 1) return 0;
    if (dplus_limbo_len == dplus_limbo_cap) {
        dplus_limbo_cap = dplus_limbo_cap ? dplus_limbo_cap * 2 : 128;
        dplus_limbo = zrealloc(dplus_limbo, dplus_limbo_cap * sizeof(dplusLimboEntry));
    }
    dplus_limbo[dplus_limbo_len].o = (robj *)ptr;
    dplus_limbo[dplus_limbo_len].route = DPLUS_LIMBO_RAW;
    dplus_limbo_len++;
    if (dplus_limbo_len > dplus_limbo_peak) dplus_limbo_peak = dplus_limbo_len;
    return 1;
}

/* Flush the limbo list. Called from beforeSleep (main). The drain is bounded
 * by one in-flight speculation batch (sub-µs); the frees happen outside the
 * exclusive window and follow each object's recorded route, so lazyfree/BIO
 * and IO-thread offload behave exactly as an immediate free would have. */
void dplusFlushLimbo(void) {
    if (dplus_limbo_len == 0) return;
    dplusLimboEntry *batch = dplus_limbo;
    size_t n = dplus_limbo_len;
    /* Detach so any re-entrant defer starts a fresh list rather than
     * mutating the one being flushed. */
    dplus_limbo = NULL;
    dplus_limbo_len = 0;
    dplus_limbo_cap = 0;

    dplusExclusiveEnter();
    /* All in-flight walks have completed; new speculation punts until Leave.
     * Nothing to do inside the window — the objects are already unlinked;
     * the drain itself is the synchronization. */
    dplusExclusiveLeave();

    for (size_t i = 0; i < n; i++) {
        robj *o = batch[i].o;
        switch (batch[i].route) {
        case DPLUS_LIMBO_OFFLOAD_PREF:
            if (tryOffloadFreeObjToIOThreads(o) == C_OK) break;
            /* fall through to sync */
        case DPLUS_LIMBO_SYNC: decrRefCount(o); break;
        case DPLUS_LIMBO_ASYNC: lazyfreeObjPrejudged(o); break;
        case DPLUS_LIMBO_RAW: zfree(o); break;
        }
    }
    zfree(batch);
}

size_t dplusLimboPeak(void) {
    return dplus_limbo_peak;
}

/* --- Component 6: INFO section --- */
/* Door-2 wakeup-coalescing counters (prototype): sums of per-worker plain
 * fields written only by their owning worker — racy-by-design stats reads.
 * Always compiled (not gated on IO_LOOKUP_OFFLOAD_STATS) so the coalescing
 * A/B can read them from a default build's INFO stats. */
long long dplusDoorbellRings(void) {
    long long sum = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) sum += dplus_thread_stats[i].doorbell_rings;
    return sum;
}

long long dplusDoorbellCoalesced(void) {
    long long sum = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) sum += dplus_thread_stats[i].doorbell_coalesced;
    return sum;
}

#ifdef IO_LOOKUP_OFFLOAD_STATS
sds dplusInfoString(sds info) {
    /* Doorbell counters are per-thread plain fields written only by their
     * owning worker; summing here is a racy-by-design stats read. */
    long long doorbell_rings = 0, doorbell_coalesced = 0, punted_replies = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) {
        doorbell_rings += dplus_thread_stats[i].doorbell_rings;
        doorbell_coalesced += dplus_thread_stats[i].doorbell_coalesced;
        punted_replies += dplus_thread_stats[i].punted_replies_written;
    }
    info = sdscatprintf(info,
        "# Dplus\r\n"
        "dplus_speculative_attempts:%llu\r\n"
        "dplus_speculative_hits:%llu\r\n"
        "dplus_validation_misses:%llu\r\n"
        "dplus_exclusive_punts:%llu\r\n"
        "dplus_large_value_punts:%llu\r\n"
        "dplus_expired_replies:%llu\r\n"
        "dplus_intra_batch_write_punts:%llu\r\n"
        "dplus_doorbell_rings:%llu\r\n"
        "dplus_doorbell_coalesced:%llu\r\n"
        "dplus_punted_replies_written:%llu\r\n",
        (unsigned long long)atomic_load_explicit(&dplus_stats.speculative_attempts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.speculative_hits, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.validation_misses, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.exclusive_punts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.large_value_punts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.expired_replies, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.intra_batch_write_punts, memory_order_relaxed),
        (unsigned long long)doorbell_rings,
        (unsigned long long)doorbell_coalesced,
        (unsigned long long)punted_replies);
    return info;
}
#endif
