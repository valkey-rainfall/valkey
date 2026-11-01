/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Fast GET: GET execution on IO threads. See fastget.h. */

#include "server.h"
#include "fastget.h"
#include "io_threads.h"
#include "module.h"
#include "cluster.h"

#include <string.h>
#include <stdatomic.h>

fastGetThreadStats fastget_thread_stats[FASTGET_MAX_IO_THREADS] = {{0}};

/* Stats (with -DIO_LOOKUP_OFFLOAD_STATS) */
#ifdef IO_LOOKUP_OFFLOAD_STATS
fastGetStats fastget_stats[FASTGET_MAX_IO_THREADS] = {{0}};

uint64_t fastGetStatsSum(size_t field_offset) {
    uint64_t sum = 0;
    for (int i = 0; i < FASTGET_MAX_IO_THREADS; i++) {
        _Atomic(uint64_t) *p = (_Atomic(uint64_t) *)((char *)&fastget_stats[i] + field_offset);
        sum += atomic_load_explicit(p, memory_order_relaxed);
    }
    return sum;
}
#endif

/* Main only. GETs answered on IO threads skip processCommand, so they skip the NOAUTH and ACL checks;
 * this byte stands in for both. It is recomputed at every auth change (clientSetUser), on SELECT
 * (per-db ACLs) and when ACL rules change (fastGetOnAclRulesChanged).
 *
 * A client qualifies when it is authenticated and some selector allows GET on every key (~*, %R~* or
 * allkeys). Users with key patterns go to main, since matching patterns on IO threads would race main
 * changing the user. flag.authenticated is used rather than authRequired(), which reads DefaultUser's
 * flags; those can change with no per-client event, and the flag can only make a client go to main,
 * never let it skip a check. */
void fastGetRecomputeSpecAclOk(client *c) {
    uint8_t ok = 0;
    /* ACL LOAD retires the user a fast-path client still points at; its successor holds the rules. */
    user *u = c->user && (c->user->flags & USER_FLAG_RETIRED) ? ACLResolveUser(c->user) : c->user;
    if (c->flag.authenticated && (u || !c->user)) {
        if (u == NULL) {
            /* No associated user = unrestricted context (stock semantics). */
            ok = 1;
        } else {
            static struct serverCommand *get_cmd = NULL;
            if (!get_cmd) get_cmd = lookupCommandByCString("get");
            if (!get_cmd) goto done; /* sentinel has no GET */
            /* The check needs only the command and db; key access is decided by the unrestricted-key
             * requirement, so the placeholder key can only make the result stricter. */
            robj *argv[2];
            argv[0] = createStringObject("get", 3);
            argv[1] = createStringObject("k", 1);
            int dbid = c->db ? c->db->id : 0;
            if (ACLUserCheckCmdWithUnrestrictedKeyAccess(u, get_cmd, argv, 2, dbid, CMD_KEY_ACCESS)) ok = 1;
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
done:
    atomic_store_explicit(&c->spec_acl_ok, ok, memory_order_release);
}

/* Main only, after ACL SETUSER, DELUSER, LOAD or a module ACL change: recompute every client's gate. */
void fastGetOnAclRulesChanged(void) {
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) fastGetRecomputeSpecAclOk((client *)listNodeValue(ln));
}

/* A module subscribed to command results gets one event per command from call(), which GETs answered on
 * IO threads skip, so those stop while any success subscriber exists. They can only succeed (a failure
 * goes to main), so only success subscribers count. */
_Atomic int fastget_module_cmdresult_gate = 0;

void fastGetOnCommandResultListenersChanged(int success_listeners) {
    atomic_store_explicit(&fastget_module_cmdresult_gate, success_listeners > 0, memory_order_release);
}

/* MONITOR is fed from call(), which GETs answered on IO threads skip, so those stop while any monitor is
 * attached. */
_Atomic int fastget_monitor_gate = 0;

void fastGetOnMonitorsChanged(void) {
    atomic_store_explicit(&fastget_monitor_gate, listLength(server.monitors) > 0, memory_order_release);
}

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int fastGetWriteBulkReply(client *c, const char *val, size_t vallen) {
    /* "$<len>\r\n" with ll2string: snprintf is measurably slower here. */
    char hdr[32];
    hdr[0] = '$';
    int numlen = ll2string(hdr + 1, sizeof(hdr) - 3, (long long)vallen);
    hdr[numlen + 1] = '\r';
    hdr[numlen + 2] = '\n';
    int hdrlen = numlen + 3;
    size_t total = hdrlen + vallen + 2; /* trailing \r\n */
    size_t available = c->buf_usable_size - c->bufpos;
    if (total > available) return 0; /* does not fit: main replies */

    memcpy(c->buf + c->bufpos, hdr, hdrlen);
    c->bufpos += hdrlen;
    memcpy(c->buf + c->bufpos, val, vallen);
    c->bufpos += vallen;
    memcpy(c->buf + c->bufpos, "\r\n", 2);
    c->bufpos += 2;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return (int)total;
}

/* Maximum batch depth for prefetch overlap. Matches server.prefetch_batch_max_size default. */
#define FASTGET_BATCH_DEPTH 16

/* How many slots ahead of the one being replied to may have their value line in flight: the value
 * prefetch window (see fastGetBatchPrefetchValue). A slot's value is prefetched when its find
 * completes if it is within this many slots of the head, otherwise when it moves into the window as
 * the head advances. FASTGET_BATCH_DEPTH means the whole ring: every value is prefetched as soon as
 * its find completes, before any slot of that sweep is replied to. */
#ifndef FASTGET_VALUE_PREFETCH_DISTANCE
#define FASTGET_VALUE_PREFETCH_DISTANCE FASTGET_BATCH_DEPTH
#endif

/* Per-key state for the batched prefetch + reply pipeline. */
typedef struct fastGetBatchEntry {
    hashtableIncrementalFindState find_state; /* Incremental find state */
    int is_first_cmd;                         /* 1 if this is c->argv, 0 if queue */
    int queue_idx;                            /* Index in cmd_queue (if !is_first_cmd) */
    robj *obj;                                /* The found value, NULL on a miss (set when the find completes) */
    char *val;                                /* Its value pointer (the sds of a RAW or EMBSTR string), NULL for INT */
} fastGetBatchEntry;

/* The find of slot e has completed: record its result. The value pointer (the sds of a RAW or
 * EMBSTR string) is read now, while the robj line the key compare just loaded is in L1, so that
 * the reply need not call objectGetVal again and the value can be prefetched. The encoding does
 * not change in the read phase, so reading the header word here and again in fastGetReplyFromEntry
 * (which also reads type and lru from it, for the touch) gives the same values; the word is read
 * through the atomic load as everywhere on this path. */
static inline void fastGetBatchResolve(fastGetBatchEntry *e) {
    void *entry = NULL;
    e->val = NULL;
    e->obj = hashtableIncrementalFindGetResult(&e->find_state, &entry) ? (robj *)entry : NULL;
    if (e->obj == NULL) return;
    robj h;
    h.hdr[0] = objectHeaderWord0(e->obj);
    /* Not INT: the value has a pointer. A value of another type (which the reply punts) gets its
     * pointer read and prefetched too; that is harmless and saves the type test here. */
    if (h.encoding != OBJ_ENCODING_INT) e->val = objectGetVal(e->obj);
}

/* Prefetch the line that holds the value's sds header. The lookup prefetches the bucket and the
 * robj (with its embedded key), never the value: for a RAW string the sds is its own allocation,
 * so the first read of its header in the reply (sdsType, then sdslen) is a dependent miss that
 * nothing hides, about 600 cycles on a large keyspace. The reply reads the header at val - 1
 * (the flags byte) and the length just before it; the allocator aligns the sds to 16 bytes, and the
 * header of a value fast GET answers (at most FASTGET_MAX_SPECULATIVE_VALUE_LEN bytes) is 3, 5 or
 * 9 bytes, so val - 1 and the length are in one cache line. A prefetch
 * has no effect on behavior: it neither faults nor changes memory. An EMBSTR value sits in the
 * robj allocation, where the prefetch costs one instruction and may fetch the next line. */
static inline void fastGetBatchPrefetchValue(fastGetBatchEntry *e) {
    if (e->val == NULL) return;
    valkey_prefetch(e->val - 1);
#ifdef FASTGET_VALUE_PREFETCH_FIRST_LINE
    /* Also the line where the value bytes continue past the header's line, which the reply's memcpy
     * reads next: with a 16-byte aligned header this is the line of val + 63 whatever the offset of
     * the header within its line. */
    valkey_prefetch(e->val + 63);
#endif
}

/* IO thread, read phase: this node serves a GET on the slot for client c with no redirect or cluster
 * error. The common states are decided from the slot owner directly: this node owns the slot, or it is a
 * replica of the owner and the client sent READONLY, with the cluster healthy and no slot migrating or
 * importing anywhere. Any other state goes through getNodeByQuery, which reads the command and slot from
 * the client, so they are set for the call. */
static int fastGetClusterServes(client *c, struct serverCommand *cmd, int slot, int read_flags) {
    if (slot < 0 || (read_flags & (READ_FLAGS_CROSSSLOT | READ_FLAGS_NO_KEYS))) return 0;
    if (!isAnySlotInManualMigratingState() && !isAnySlotInManualImportingState() && isClusterHealthy() &&
        !(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)) {
        clusterNode *owner = getNodeBySlot(slot);
        if (owner == NULL) return 0;
        clusterNode *myself = getMyClusterNode();
        if (owner == myself) return 1;
        return c->flag.readonly && clusterNodeIsReplica(myself) && clusterNodeGetPrimary(myself) == owner;
    }
    if (getMigratingSlotDest(slot) != NULL || getImportingSlotSource(slot) != NULL) return 0;
    struct serverCommand *saved_cmd = c->cmd;
    int saved_slot = c->slot, saved_flags = c->read_flags;
    c->cmd = cmd;
    c->slot = slot;
    c->read_flags = read_flags;
    int err = CLUSTER_REDIR_NONE;
    clusterNode *n = getNodeByQuery(c, &err);
    c->cmd = saved_cmd;
    c->slot = saved_slot;
    c->read_flags = saved_flags;
    return n != NULL && clusterNodeIsMyself(n) && err == CLUSTER_REDIR_NONE;
}

/* The keyspace table of a GET's key. In cluster mode that is the slot the parser already computed. */
static inline int fastGetTableIndex(sds key, int slot) {
    if (server.cluster_enabled && slot >= 0) return slot;
    return getKVStoreIndexForKey(key);
}

/* The commandlog threshold of a type, or -1 while that log records nothing. */
static long long fastGetCommandlogThreshold(int type) {
    if (server.commandlog[type].threshold < 0 || server.commandlog[type].max_len == 0) return -1;
    return server.commandlog[type].threshold;
}

/* A value read by fast GET counts as an access for LRU and LFU, as lookupKey does on main (EV-02 /
 * STR-05). The touch is applied here, on the IO thread, into the first header word of the value.
 *
 * Who else accesses that word in the read phase: other fast GETs of the same value (IO threads and
 * main's read share), which read type, encoding and lru and may store lru. Nothing else: main runs
 * no command in the read phase, lanes and the stream run in the write phase, bio frees only unlinked
 * values, IO threads free only sole-reference argv objects, and module threads wait for the GIL,
 * which main holds from afterSleep to beforeSleep. The second header word (the embedded-data flags
 * and the refcount) is a separate memory location (the zero-width bit-field in struct serverObject),
 * which nobody stores in the read phase, so its plain reads race with nothing.
 *
 * So the store is a C11 data race only with the reads of the first word, and every one of those goes
 * through an atomic load: objectGetType, objectGetEncoding and objectGetLRU read the word with
 * objectHeaderWord0, and fastGetReplyFromEntry loads it once for type, encoding and this touch. The
 * store is a relaxed atomic store of the whole word with only the lru bits changed. Two threads
 * touching the same value at once both store the same type and encoding bits (nothing changes them
 * in the read phase); for LRU both store the same clock, for LFU one random increment of the
 * approximate counter can be lost, which eviction tolerates. Outside the read phase main and the
 * lanes read and write the word with plain accesses, ordered before and after these atomics by the
 * phase barrier (principles P6).
 *
 * The store is skipped when the touch would not change the word: in LRU mode a value read earlier in
 * the same clock second already holds the clock (at 10M GET/s over 3M keys that is about 71 percent of
 * the touches), in LFU mode an increment that the random draw did not grant. OBJECT IDLETIME, OBJECT
 * FREQ and eviction order see the same values either way; what the skip saves is a dirtied cache line
 * per skipped touch. The touch is skipped while a child process runs (no copy-on-write storms), as
 * lookupKey does; server.child_pid changes only on main, outside the read phase. LFU needs a random
 * number and rand() is not thread safe, so each thread keeps its own xorshift generator. */
static __attribute__((noinline)) uint32_t fastGetLfuTouch(uint32_t lfu) {
    static __thread uint64_t rng;
    if (rng == 0) rng = ((uint64_t)(uintptr_t)&rng) ^ (uint64_t)ustime() ^ 0x9E3779B97F4A7C15ULL;
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    double r = (double)(rng >> 11) / (double)(1ULL << 53);
    return lrulfu_touchWithRandom(lfu, r);
}

static inline void fastGetTouch(robj *o, uint32_t hdr0) {
    if (hasActiveChildProcess()) return;
    robj h;
    h.hdr[0] = hdr0; /* the word fastGetReplyFromEntry loaded for type and encoding */
    /* LRU: the clock of this iteration; main moves it outside the read phase. The LFU path is kept out
     * of the lookup loop's code (noinline): the random draw and the decay are floating point. */
    uint32_t touched = lrulfu_isUsingLFU() ? fastGetLfuTouch(h.lru) : lru_import(0);
    if (touched == h.lru) return;
    h.lru = touched;
    __atomic_store_n(&o->hdr[0], h.hdr[0], __ATOMIC_RELAXED);
}

/* Reply to one prefetched GET from the keyspace. Returns 1 on success (reply
 * written), 0 on punt. The keyspace is quiescent while IO threads read: main
 * mutates it only outside the read phase. */
static int fastGetReplyFromEntry(client *c, fastGetBatchEntry *e) {
    /* A GET the large-request commandlog would record goes to main, which records it. */
    long long request_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    if (request_threshold >= 0) {
        size_t in = e->is_first_cmd ? c->net_input_bytes_curr_cmd : c->cmd_queue.cmds[e->queue_idx].input_bytes;
        if ((long long)in >= request_threshold) return 0;
    }
    if (e->obj == NULL) {
        /* A miss goes to main, which fires the keymiss notification and counts the miss. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
        FASTGET_STAT_ADD(miss_punts, 1);
#endif
        return 0;
    }

    robj *o = e->obj;
    robj h; /* the first header word, read once (see fastGetTouch) */
    h.hdr[0] = objectHeaderWord0(o);
    if (h.type != OBJ_STRING) return 0;

    /* Expired keys punt: only main may lazily delete them. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(expired_replies, 1);
#endif
            return 0;
        }
    }

    char intbuf[LONG_STR_SIZE];
    const char *val;
    size_t vallen;
    int encoding = h.encoding;
    if (encoding == OBJ_ENCODING_INT) {
        vallen = ll2string(intbuf, sizeof(intbuf), (long long)(long)objectGetVal(o));
        val = intbuf;
    } else if (encoding == OBJ_ENCODING_EMBSTR || encoding == OBJ_ENCODING_RAW) {
        val = e->val; /* objectGetVal(o), read by fastGetBatchResolve; its line was prefetched */
        vallen = sdslen((sds)val);
        if (vallen > FASTGET_MAX_SPECULATIVE_VALUE_LEN) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(large_value_punts, 1);
#endif
            return 0;
        }
    } else {
        return 0;
    }

    /* A reply the large-reply commandlog would record goes to main, which records it. */
    long long reply_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && (long long)(1 + digits10(vallen) + 2 + vallen + 2) >= reply_threshold) return 0;

    if (fastGetWriteBulkReply(c, val, vallen) <= 0) return 0;
    fastGetTouch(o, h.hdr[0]);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    FASTGET_STAT_ADD(speculative_hits, 1);
#endif
    return 1;
}

/* The command GET, looked up once. */
static struct serverCommand *fastget_get_cmd = NULL;

/* IO thread, read phase: is this read kept from fast GET? Returns 1 when the GETs at its head must go
 * to main, 0 when they may be answered here. Every check runs once per read. Main changes the state
 * the checks read only outside the read phase (CLIENT PAUSE, FAILOVER, the replication state machine,
 * module and monitor registration, and the client's own flags and buffer all move on main), so the
 * plain reads are ordered by the phase barrier like every other gate (P6).
 *
 * The checks are compiled apart from the lookup loop of fastGetSpeculateBatch on purpose (noinline).
 * The two client fields the F11 gate reads, c->flag.pending_write and c->bufpos, are on the 64-byte
 * line of the client (bytes 320-383) that the MULTI check loads a few instructions earlier and that
 * fastGetWriteBulkReply writes for every reply, so they add no cache line and no coherence transfer.
 * What a check can still cost is its effect on the code the compiler generates for the loop when both
 * share one function: adding the F11 gate moved the loop's register allocation and its hot/cold
 * split (gcc 11, x86: prefix_open spilled to the stack, the split point moved). With the checks in
 * their own function the loop's code is the same whatever the checks do, now and after the next
 * gate is added. */
static __attribute__((noinline)) int fastGetReadGated(client *c) {
    if (server.io_threads_num <= 1) return 1;

    /* io-threads-speculation-replica-only: only a replica answers GETs on IO threads. */
    if (server.io_threads_speculation_replica_only && server.primary_host == NULL) return 1;

    /* Gates processCommand applies to every command, which a GET answered here skips.
     * CLIENT PAUSE ALL holds reads too: the client is postponed on main until unpause. */
    if (server.paused_actions & PAUSE_ACTION_CLIENT_ALL) return 1;
    /* A replica with a broken primary link and replica-serve-stale-data no answers -MASTERDOWN. */
    if (server.primary_host && server.repl_state != REPL_STATE_CONNECTED && server.repl_serve_stale_data == 0) return 1;

    /* Inside MULTI a GET must be queued, not run. A blocked or just-unblocked client's replies must
     * follow its unblock reply. The reply is written as plain RESP into c->buf, which would corrupt a
     * buffer in copy-avoidance encoded mode, and would reorder replies if the reply list is not
     * empty.
     *
     * pending_write (F11): main queued a write of this client's buffers (a tracking push to a REDIRECT
     * target) and owns that write; sending c->buf from here too would send those bytes twice. The bit
     * is in the flag word the four tests above load, so it costs one more bit in the same mask. */
    if (c->flag.multi || c->flag.blocked || c->flag.unblocked || c->flag.buf_encoded || c->flag.pending_write ||
        listLength(c->reply) != 0)
        return 1;

    /* F11, second half: main's bytes are in c->buf although no write is queued any more (main wrote
     * part of them and installed a write handler for the rest). Same cache line as the flag word. */
    if (c->bufpos != 0) return 1;

    /* A tracking client must have its keys remembered, which this path skips, or a later write would
     * not invalidate its cache. BCAST tracking invalidates by prefix and is unaffected. Main does not
     * run this client's commands while its read is parsed, so the plain flags are stable here. */
    if (c->flag.tracking && !c->flag.tracking_bcast) return 1;

    /* A module subscribed to command results (see fastGetOnCommandResultListenersChanged). */
    if (atomic_load_explicit(&fastget_module_cmdresult_gate, memory_order_acquire)) return 1;

    /* A module command filter may rewrite or redirect the GET. Filters change only on main, which
     * waits at the barrier while IO threads run this. */
    if (moduleHasCommandFilters()) return 1;

    /* An attached monitor (see fastGetOnMonitorsChanged). */
    if (atomic_load_explicit(&fastget_monitor_gate, memory_order_acquire)) return 1;

    /* Only a read that starts with a GET is answered here. */
    if (!fastget_get_cmd) fastget_get_cmd = lookupCommandByCString("get");
    if (c->argc != 2 || c->parsed_cmd != fastget_get_cmd) return 1;

    /* Answer the read here only when every complete command in it is a GET. A prefix of GETs answered
     * here while the rest waits for main splits one pipeline's replies into two writes; the client
     * then refills its pipeline in pieces, the next reads are smaller, and under load the server
     * settles into serving fragments of a couple of commands per read. */
    for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
        parsedCommand *p = &c->cmd_queue.cmds[i];
        if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED)) break; /* the trailing partial */
        if (p->cmd != fastget_get_cmd || p->argc != 2) return 1;
    }

    /* In cluster mode only a GET this node answers without a redirect: the same check main runs, on
     * cluster state main changes only outside the read phase. A slot being migrated or imported needs
     * key-by-key checks and a possible ASK or TRYAGAIN, so its GETs go to main. With per-slot stats on,
     * main runs every GET so the stats count it. */
    if (server.cluster_enabled && server.cluster_slot_stats_enabled) return 1;
    if (server.cluster_enabled && !fastGetClusterServes(c, c->parsed_cmd, c->slot, c->read_flags)) return 1;

    /* The client's ACL and auth gate (see fastGetRecomputeSpecAclOk). */
    if (!atomic_load_explicit(&c->spec_acl_ok, memory_order_acquire)) return 1;

    /* With a slow threshold of 0 the commandlog records every command, so every GET goes to main, which
     * records it. Large requests and replies are checked per GET. */
    if (fastGetCommandlogThreshold(COMMANDLOG_TYPE_SLOW) == 0) return 1;

    return 0;
}

/* Called from the IO thread after parsing is complete. Executes a contiguous
 * prefix of GET commands, using batched incremental finds to overlap memory
 * prefetches across keys before replying in command order.
 *
 * REPLY ORDERING: all speculative replies are written into c->buf BEFORE the
 * main thread processes any punted command, whose replies append after.
 *
 * Returns the number of commands speculatively completed. */
int fastGetSpeculateBatch(client *c, int tid) {
    int speculated = 0;

    if (fastGetReadGated(c)) return 0;

    /* Time the batch for commandstats parity: speculated GETs bypass call(),
     * so their duration must be accumulated per-thread and folded into the
     * GET command's microseconds by fastGetAggregateStats (as with calls). */
    monotime spec_start = getMonotonicUs();

    hashtable *ht = kvstoreGetHashtable(c->db->keys, fastGetTableIndex(objectGetVal(c->argv[1]), c->slot));
    if (!ht) return 0;

    /* STREAMING SLOT-REFILL: a ring of FASTGET_BATCH_DEPTH slots holds in-flight
     * finds. Each sweep steps every pending find once (one prefetch per slot);
     * the head slot replies as soon as its find completes, and the fill loop
     * tops the window back up from the queue, so prefetch depth stays constant
     * across the eligible prefix. The first ineligible command or punt ends
     * speculation for the whole read. head..tail are monotonic; slot index =
     * seq % FASTGET_BATCH_DEPTH; done[] marks completed finds: 0 in flight,
     * 1 complete, 2 complete with the value line prefetched.
     *
     * The find pipeline hides the bucket and robj misses; the value's sds (a
     * separate allocation for a RAW string) is the one miss left, and it was
     * taken in the drain, one slot at a time, serialized by the reply code
     * that depends on it. The value prefetch stage issues that line for a slot
     * as soon as its find completes (within the window, see
     * FASTGET_VALUE_PREFETCH_DISTANCE), so the drain finds it in cache. */
    fastGetBatchEntry batch[FASTGET_BATCH_DEPTH];
    uint8_t done[FASTGET_BATCH_DEPTH];
    cmdQueue *queue = &c->cmd_queue;
    int queue_pos = queue->off;
    int prefix_open = 1;
    unsigned head = 0, tail = 1;

    batch[0].is_first_cmd = 1;
    batch[0].queue_idx = -1;
    hashtableIncrementalFindInit(&batch[0].find_state, ht, objectGetVal(c->argv[1]));
    done[0] = 0;

    while (head < tail || prefix_open) {
        /* Fill: top the window up from the queue. */
        while (prefix_open && tail - head < FASTGET_BATCH_DEPTH && queue_pos < queue->len) {
            parsedCommand *p = &queue->cmds[queue_pos];
            if (p->cmd != fastget_get_cmd || p->argc != 2 ||
                (server.cluster_enabled && !fastGetClusterServes(c, p->cmd, p->slot, p->read_flags))) {
                prefix_open = 0; /* ineligible command ends the global prefix */
                break;
            }
            hashtable *pht = kvstoreGetHashtable(c->db->keys, fastGetTableIndex(objectGetVal(p->argv[1]), p->slot));
            if (!pht) {
                prefix_open = 0; /* an unallocated shard ends the prefix; main answers it */
                break;
            }
            fastGetBatchEntry *e = &batch[tail % FASTGET_BATCH_DEPTH];
            e->is_first_cmd = 0;
            e->queue_idx = queue_pos;
            hashtableIncrementalFindInit(&e->find_state, pht, objectGetVal(p->argv[1]));
            done[tail % FASTGET_BATCH_DEPTH] = 0;
            tail++;
            queue_pos++;
        }
        if (queue_pos >= queue->len) prefix_open = 0; /* queue exhausted -- drain */
        if (head == tail) break;                      /* nothing in flight and nothing to fill */

        /* Sweep: one find step per pending slot (one prefetch each). A find
         * that completes resolves its entry and, inside the window, prefetches
         * its value line. */
        for (unsigned s = head; s < tail; s++) {
            unsigned i = s % FASTGET_BATCH_DEPTH;
            if (done[i]) continue;
            if (hashtableIncrementalFindStep(&batch[i].find_state)) continue;
            fastGetBatchResolve(&batch[i]);
            /* With the window as large as the ring every slot is inside it (tail - head is at most
             * FASTGET_BATCH_DEPTH), so the test folds away. */
            if (FASTGET_VALUE_PREFETCH_DISTANCE >= FASTGET_BATCH_DEPTH || s - head < FASTGET_VALUE_PREFETCH_DISTANCE) {
                fastGetBatchPrefetchValue(&batch[i]);
                done[i] = 2;
            } else {
                done[i] = 1;
            }
        }

        /* Drain: reply completed HEADS in command order. */
        while (head < tail && done[head % FASTGET_BATCH_DEPTH]) {
            fastGetBatchEntry *e = &batch[head % FASTGET_BATCH_DEPTH];
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(speculative_attempts, 1);
#endif
            if (!fastGetReplyFromEntry(c, e)) {
                /* Contiguous-prefix rule: in-flight slots behind a punt are
                 * abandoned and their commands fall through to main. */
                prefix_open = 0;
                head = tail;
                break;
            }
            if (e->is_first_cmd)
                c->read_flags |= READ_FLAGS_FASTGET_SPECULATED;
            else
                queue->cmds[e->queue_idx].read_flags |= READ_FLAGS_FASTGET_SPECULATED;
            speculated++;
            head++;
            if (FASTGET_VALUE_PREFETCH_DISTANCE < FASTGET_BATCH_DEPTH) {
                /* The slot that just moved into the window: prefetch its value if its find is
                 * already complete (otherwise the sweep does it when the find completes). */
                unsigned n = head + FASTGET_VALUE_PREFETCH_DISTANCE - 1;
                if (n < tail && done[n % FASTGET_BATCH_DEPTH] == 1) {
                    fastGetBatchPrefetchValue(&batch[n % FASTGET_BATCH_DEPTH]);
                    done[n % FASTGET_BATCH_DEPTH] = 2;
                }
            }
        }
    }

    /* Commandstats parity: fold execution time into the per-thread counter
     * (calls are accumulated in fastGetConsumeSpeculated). */
    if (speculated > 0) fastget_thread_stats[tid].usec += (long long)(getMonotonicUs() - spec_start);
    return speculated;
}

/* --- Phase-1 no-enqueue: IO-thread command consumption --- */

/* IO thread: drop the commands fastGetSpeculateBatch answered, so main never runs them. Frees their argv,
 * clears pending_command, advances the command queue past them and counts them per thread. The IO thread
 * owns the client for the whole read phase, so it updates commands_processed itself. Afterwards
 * c->argc is 0 and the queue holds only commands main still has to run. */
void fastGetConsumeSpeculated(client *c, int count, int tid) {
    int consumed = 0;

    /* --- First command (c->argv / c->parsed_cmd case) --- */
    if (count > 0 && (c->read_flags & READ_FLAGS_FASTGET_SPECULATED)) {
        c->read_flags &= ~READ_FLAGS_FASTGET_SPECULATED;
        /* The parser set pending_command; left set, main would run a stale c->cmd with argc 0. */
        c->flag.pending_command = 0;
        /* Freed here, not with freeClientArgv(): that may offload the free to an IO thread, which only
         * main may do (it updates main's job counters and a main-produced queue). */
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
        if (!(p->read_flags & READ_FLAGS_FASTGET_SPECULATED)) break;
        p->read_flags &= ~READ_FLAGS_FASTGET_SPECULATED;
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
    fastget_thread_stats[tid].commands_processed += consumed;
    /* Every answered GET is a keyspace hit: misses, expired keys and large values go to main. Main folds
     * this into stat_keyspace_hits, which lookupKey would have counted. */
    fastget_thread_stats[tid].keyspace_hits += consumed;
}

/* Main: fold the per-thread counts of GETs answered on IO threads into stat_numcommands,
 * stat_keyspace_hits and GET's commandstats. Runs in beforeSleep, and also before CONFIG RESETSTAT
 * zeroes the stats and before INFO renders them, so a reset clears pending counts and a read includes
 * them. */
void fastGetAggregateStats(void) {
    /* IO threads only ever increase their counters; main folds the growth since its last snapshot.
     * Zeroing a counter from main would race an IO thread's increment and lose counts, while a stale
     * read here only defers counts to the next fold. */
    static long long seen_calls[FASTGET_MAX_IO_THREADS] = {0};
    static long long seen_usec[FASTGET_MAX_IO_THREADS] = {0};
    static long long seen_hits[FASTGET_MAX_IO_THREADS] = {0};
    static struct serverCommand *get_cmd = NULL;
    if (!get_cmd) get_cmd = lookupCommandByCString("get");
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = fastget_thread_stats[i].commands_processed;
        long long calls_delta = 0;
        if (n > seen_calls[i]) {
            calls_delta = n - seen_calls[i];
            seen_calls[i] = n;
            ectx->stat_numcommands += calls_delta;
            if (get_cmd) get_cmd->calls += calls_delta;
        }
        /* Keyspace hits, folded the same way. */
        long long h = fastget_thread_stats[i].keyspace_hits;
        if (h > seen_hits[i]) {
            ectx->stat_keyspace_hits += h - seen_hits[i];
            seen_hits[i] = h;
        }
        long long us = fastget_thread_stats[i].usec;
        long long us_delta = 0;
        if (us > seen_usec[i]) {
            us_delta = us - seen_usec[i];
            seen_usec[i] = us;
            if (get_cmd) get_cmd->microseconds += us_delta;
        }
        /* Speculated GETs count in LATENCY HISTOGRAM and latencystats at their average duration. */
        if (get_cmd && calls_delta > 0 && server.latency_tracking_enabled) {
            int64_t avg_ns = (int64_t)(us_delta * 1000 / calls_delta);
            if (avg_ns < 1) avg_ns = 1;
            updateCommandLatencyHistogramCount(&get_cmd->latency_histogram, avg_ns, calls_delta);
        }
    }
}

sds fastGetInfoString(sds info) {
    long long speculated = 0;
    for (int i = 0; i < FASTGET_MAX_IO_THREADS; i++) speculated += fastget_thread_stats[i].commands_processed;
    info = sdscatprintf(info, "# Fastget\r\nfastget_speculated_commands:%lld\r\n", speculated);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    info = sdscatprintf(info,
                        "fastget_speculative_attempts:%llu\r\n"
                        "fastget_speculative_hits:%llu\r\n"
                        "fastget_large_value_punts:%llu\r\n"
                        "fastget_expired_replies:%llu\r\n"
                        "fastget_miss_punts:%llu\r\n",
                        (unsigned long long)FASTGET_STAT_SUM(speculative_attempts),
                        (unsigned long long)FASTGET_STAT_SUM(speculative_hits),
                        (unsigned long long)FASTGET_STAT_SUM(large_value_punts),
                        (unsigned long long)FASTGET_STAT_SUM(expired_replies),
                        (unsigned long long)FASTGET_STAT_SUM(miss_punts));
#endif
    return info;
}
