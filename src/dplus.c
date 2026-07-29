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

/* --- Component 2: Speculative GET — reply helpers --- */

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int dplusWriteBulkReply(client *c, const char *val, size_t vallen) {
    char hdr[32];
    int hdrlen = snprintf(hdr, sizeof(hdr), "$%zu\r\n", vallen);
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

/* --- Component 2/5: IO-thread batch speculation --- */

/* Called from ioThreadReadQueryFromClient after parsing is complete.
 * Iterates the parsed command queue attempting speculative GETs on a
 * contiguous prefix. As soon as a command can't be speculated (non-GET,
 * write, validation fail, exclusive mode), we stop — remaining commands
 * punt to main-thread execution via the normal path.
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

    /* Set per-thread flag (seq_cst handshake with exclusive mode). */
    atomic_store_explicit(&dplus_in_speculative_read[tid], 1, memory_order_seq_cst);

    /* Check exclusive mode AFTER setting flag (barrier handshake). */
    if (atomic_load_explicit(&dplus_exclusive_mode, memory_order_seq_cst)) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.exclusive_punts, 1, memory_order_relaxed);
#endif
        goto out;
    }

    /* --- First command (c->argv / c->parsed_cmd) --- */
    if (c->argc == 2 && c->parsed_cmd != NULL &&
        c->parsed_cmd == lookupCommandByCString("get") &&
        (c->parsed_cmd->flags & (CMD_READONLY | CMD_FAST)) == (CMD_READONLY | CMD_FAST)) {

        void *key_sds = objectGetVal(c->argv[1]);
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.speculative_attempts, 1, memory_order_relaxed);
#endif
        if (dplusSpeculativeGet(c, key_sds, c->resp)) {
            c->read_flags |= READ_FLAGS_DPLUS_SPECULATED;
            speculated++;
        } else {
            /* First command failed — stop (contiguous-prefix rule). */
            goto out;
        }
    } else {
        /* First command not eligible — nothing to speculate. */
        goto out;
    }

    /* --- Command queue (pipelined commands) --- */
    cmdQueue *queue = &c->cmd_queue;
    for (int i = queue->off; i < queue->len; i++) {
        parsedCommand *p = &queue->cmds[i];

        /* Intra-batch write guard: if any prior command was a write,
         * stop speculating (read-your-writes correctness). */
        if (p->cmd == NULL) break;
        if (!(p->cmd->flags & CMD_READONLY)) break;

        /* Must be GET (argc==2, CMD_FAST, explicit cmd check). */
        if (p->argc != 2 || !(p->cmd->flags & CMD_FAST)) break;
        if (p->cmd != lookupCommandByCString("get")) break;

        void *key_sds = objectGetVal(p->argv[1]);
#ifdef IO_LOOKUP_OFFLOAD_STATS
        atomic_fetch_add_explicit(&dplus_stats.speculative_attempts, 1, memory_order_relaxed);
#endif
        if (dplusSpeculativeGet(c, key_sds, c->resp)) {
            p->read_flags |= READ_FLAGS_DPLUS_SPECULATED;
            speculated++;
        } else {
            /* Validation fail or punt — stop. */
            break;
        }
    }

out:
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
 * next time (bounded lag of one event-loop cycle, ~1ms). */
void dplusAggregateStats(void) {
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = dplus_thread_stats[i].commands_processed;
        if (n > 0) {
            server.stat_numcommands += n;
            dplus_thread_stats[i].commands_processed = 0;
        }
    }
}

/* --- Component 6: INFO section --- */
#ifdef IO_LOOKUP_OFFLOAD_STATS
sds dplusInfoString(sds info) {
    info = sdscatprintf(info,
        "# Dplus\r\n"
        "dplus_speculative_attempts:%llu\r\n"
        "dplus_speculative_hits:%llu\r\n"
        "dplus_validation_misses:%llu\r\n"
        "dplus_exclusive_punts:%llu\r\n"
        "dplus_large_value_punts:%llu\r\n"
        "dplus_expired_replies:%llu\r\n"
        "dplus_intra_batch_write_punts:%llu\r\n",
        (unsigned long long)atomic_load_explicit(&dplus_stats.speculative_attempts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.speculative_hits, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.validation_misses, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.exclusive_punts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.large_value_punts, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.expired_replies, memory_order_relaxed),
        (unsigned long long)atomic_load_explicit(&dplus_stats.intra_batch_write_punts, memory_order_relaxed));
    return info;
}
#endif
