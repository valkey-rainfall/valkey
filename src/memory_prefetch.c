/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
/*
 * This file utilizes prefetching keys and data for multiple commands in a batch,
 * to improve performance by amortizing memory access costs across multiple operations.
 */

#include "memory_prefetch.h"
#include "server.h"
#include "io_threads.h"

extern int ProcessingEventsWhileBlocked;

typedef enum {
    PREFETCH_ENTRY,        /* Initial state, prefetch entries associated with the given key's hash */
    PREFETCH_VALUE,        /* prefetch the value object of the entry found in the previous step */
    PREFETCH_VALUE_NESTED, /* nested prefetch of inner hashtable for hash/zset types */
    PREFETCH_DONE          /* Indicates that prefetching for this key is complete */
} PrefetchState;

typedef enum {
    NESTED_PREFETCH_INIT,  /* Init incremental find on inner hashtable */
    NESTED_PREFETCH_STEP,  /* Step through incremental find */
    NESTED_PREFETCH_VALUE, /* Prefetch the found entry's value (non-embedded only) */
} NestedPrefetchPhase;

typedef struct KeyPrefetchInfo {
    PrefetchState state; /* Current state of the prefetch operation */
    hashtableIncrementalFindState hashtab_state;
    /* Fields for nested prefetching of inner hashtables (hash/zset) */
    robj *member;      /* field/member to look up in the inner table, NULL if none */
    int inner_is_zset; /* inner table is a zset index: lookup keys need marking */
    NestedPrefetchPhase nested_phase;
    hashtableIncrementalFindState inner_hashtab_state;
} KeyPrefetchInfo;

/* PrefetchCommandsBatch structure holds the state of the current batch of client commands being processed. */
typedef struct PrefetchCommandsBatch {
    size_t cur_idx;                 /* Index of the current key being processed */
    size_t keys_done;               /* Number of keys that have been prefetched */
    size_t key_count;               /* Number of keys in the current batch */
    size_t client_count;            /* Number of clients in the current batch */
    size_t max_prefetch_size;       /* Maximum number of keys to prefetch in a batch */
    size_t executed_commands;       /* Number of commands executed in the current batch */
    int *slots;                     /* Array of slots for each key */
    void **keys;                    /* Array of keys to prefetch in the current batch */
    client **clients;               /* Array of clients in the current batch */
    robj **key_members;             /* Member to prefetch for each key (NULL = no nested prefetch) */
    hashtable **keys_tables;        /* Main table for each key */
    KeyPrefetchInfo *prefetch_info; /* Prefetch info for each key */
} PrefetchCommandsBatch;

static PrefetchCommandsBatch *batch = NULL;

void freePrefetchCommandsBatch(void) {
    if (batch == NULL) {
        return;
    }

    zfree(batch->clients);
    zfree(batch->key_members);
    zfree(batch->keys);
    zfree(batch->keys_tables);
    zfree(batch->slots);
    zfree(batch->prefetch_info);
    zfree(batch);
    batch = NULL;
}

void prefetchCommandsBatchInit(void) {
    if (batch) return;
    size_t max_prefetch_size = server.prefetch_batch_max_size;

    if (max_prefetch_size == 0) {
        return;
    }

    batch = zcalloc(sizeof(PrefetchCommandsBatch));
    batch->max_prefetch_size = max_prefetch_size;
    batch->clients = zcalloc(max_prefetch_size * sizeof(client *));
    batch->key_members = zcalloc(max_prefetch_size * sizeof(robj *));
    batch->keys = zcalloc(max_prefetch_size * sizeof(void *));
    batch->keys_tables = zcalloc(max_prefetch_size * sizeof(hashtable *));
    batch->slots = zcalloc(max_prefetch_size * sizeof(int));
    batch->prefetch_info = zcalloc(max_prefetch_size * sizeof(KeyPrefetchInfo));
}

int onMaxBatchSizeChange(const char **err) {
    UNUSED(err);
    if (batch && batch->client_count > 0) {
        /* We need to process the current batch before updating the size */
        return 1;
    }

    freePrefetchCommandsBatch();
    prefetchCommandsBatchInit();
    return 1;
}

/* Move to the next key in the batch. */
static void moveToNextKey(void) {
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

static void markKeyAsdone(KeyPrefetchInfo *info) {
    info->state = PREFETCH_DONE;
    server.stat_total_prefetch_entries++;
    batch->keys_done++;
}

/* Returns the next KeyPrefetchInfo structure that needs to be processed. */
static KeyPrefetchInfo *getNextPrefetchInfo(void) {
    size_t start_idx = batch->cur_idx;
    do {
        KeyPrefetchInfo *info = &batch->prefetch_info[batch->cur_idx];
        if (info->state != PREFETCH_DONE) return info;
        batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
    } while (batch->cur_idx != start_idx);
    return NULL;
}

/* Initialize per-key state and start the main-hashtable find for each key. */
static void initBatchInfo(hashtable **tables) {
    /* Initialize the prefetch info */
    for (size_t i = 0; i < batch->key_count; i++) {
        KeyPrefetchInfo *info = &batch->prefetch_info[i];
        if (!tables[i] || hashtableSize(tables[i]) == 0) {
            info->state = PREFETCH_DONE;
            batch->keys_done++;
            continue;
        }
        info->state = PREFETCH_ENTRY;
        info->member = batch->key_members[i];
        info->inner_is_zset = 0;
        info->nested_phase = NESTED_PREFETCH_INIT;
        hashtableIncrementalFindInit(&info->hashtab_state, tables[i], batch->keys[i]);
    }
}

/* A key is eligible for nested prefetch when its command supplied a member and the
 * value is backed by a hashtable we can look the member up in. */
static inline int canNestedPrefetch(KeyPrefetchInfo *info, robj *val) {
    return info->member != NULL && (val->encoding == OBJ_ENCODING_HASHTABLE ||
                                    (val->type == OBJ_ZSET && val->encoding == OBJ_ENCODING_BTREE));
}

/* Advance the main-hashtable find and pick the next state once the entry is found. */
static void prefetchEntry(KeyPrefetchInfo *info) {
    if (hashtableIncrementalFindStep(&info->hashtab_state)) {
        /* Not done yet */
        moveToNextKey();
    } else {
        info->state = PREFETCH_VALUE;
    }
}

/* Prefetch the entry's value object, then hand hash and zset keys to the nested path. */
static void prefetchValue(KeyPrefetchInfo *info) {
    void *entry;
    if (hashtableIncrementalFindGetResult(&info->hashtab_state, &entry)) {
        robj *val = entry;
        if (canNestedPrefetch(info, val)) {
            valkey_prefetch(objectGetVal(val));
            info->state = PREFETCH_VALUE_NESTED;
            info->nested_phase = NESTED_PREFETCH_INIT;
            moveToNextKey();
            return;
        }
        if (server.io_threads_num < server.min_io_threads_copy_avoid && val->encoding == OBJ_ENCODING_RAW && val->type == OBJ_STRING) {
            valkey_prefetch(objectGetVal(val));
        }
    }

    markKeyAsdone(info);
}

/* Nested prefetch: walk the inner hashtable for hash/zset types using a phased
 * approach (INIT -> STEP [-> VALUE]) to amortize cache misses across commands
 * in the batch. Prefetches the single member supplied by the command. The VALUE
 * phase runs only for non-embedded hash values. */
static void prefetchValueNested(KeyPrefetchInfo *info) {
    void *entry;
    if (!hashtableIncrementalFindGetResult(&info->hashtab_state, &entry)) {
        markKeyAsdone(info);
        return;
    }
    robj *val = entry;

    switch (info->nested_phase) {
    case NESTED_PREFETCH_INIT: {
        /* The header is warm now, so the inner hashtable pointer can be read. */
        hashtable *inner_ht = NULL;
        if (val->encoding == OBJ_ENCODING_HASHTABLE) {
            inner_ht = objectGetVal(val);
        } else if (val->type == OBJ_ZSET && val->encoding == OBJ_ENCODING_BTREE) {
            zset *zs = objectGetVal(val);
            inner_ht = zs->ht;
            info->inner_is_zset = 1;
        }
        if (!inner_ht || hashtableSize(inner_ht) == 0) {
            markKeyAsdone(info);
            return;
        }
        /* The zset hashtable stores packed [score][element] items, so a plain sds
         * lookup key must be marked for the callbacks to read it as an element. */
        sds member = objectGetVal(info->member);
        if (info->inner_is_zset) zsetMarkLookupKey(member);
        hashtableIncrementalFindInit(&info->inner_hashtab_state, inner_ht, member);
        if (info->inner_is_zset) zsetUnmarkLookupKey(member);
        info->nested_phase = NESTED_PREFETCH_STEP;
        moveToNextKey();
        return;
    }

    case NESTED_PREFETCH_STEP: {
        /* A step may invoke the compare callback, so mark the lookup key here too. */
        sds step_member = objectGetVal(info->member);
        if (info->inner_is_zset) zsetMarkLookupKey(step_member);
        int more = hashtableIncrementalFindStep(&info->inner_hashtab_state);
        if (info->inner_is_zset) zsetUnmarkLookupKey(step_member);
        if (more) {
            moveToNextKey();
            return;
        }
        /* Only non-embedded hash values have a separate value pointer worth
         * prefetching; embedded values and zset/set skip the VALUE phase. */
        if (val->type == OBJ_HASH) {
            void *inner_entry;
            if (hashtableIncrementalFindGetResult(&info->inner_hashtab_state, &inner_entry) && inner_entry &&
                !entryHasEmbeddedValue(inner_entry)) {
                info->nested_phase = NESTED_PREFETCH_VALUE;
                moveToNextKey();
                return;
            }
        }
        markKeyAsdone(info);
        return;
    }

    case NESTED_PREFETCH_VALUE: {
        void *inner_entry;
        if (hashtableIncrementalFindGetResult(&info->inner_hashtab_state, &inner_entry) && inner_entry) {
            char *value = entryGetValue(inner_entry, NULL);
            if (value) valkey_prefetch(value);
        }
        markKeyAsdone(info);
        return;
    }
    default: serverPanic("Unknown nested prefetch phase %d", info->nested_phase);
    }
}

/* Prefetch hashtable data for an array of keys.
 *
 * This function takes an array of tables and keys, attempting to bring
 * data closer to the L1 cache that might be needed for hashtable operations
 * on those keys.
 *
 * tables - An array of hashtables to prefetch data from.
 * prefetch_value - If true, we prefetch the value data for each key.
 * to bring the key's value data closer to the L1 cache as well.
 */
static void hashtablePrefetch(hashtable **tables) {
    initBatchInfo(tables);
    KeyPrefetchInfo *info;
    while ((info = getNextPrefetchInfo())) {
        switch (info->state) {
        case PREFETCH_ENTRY: prefetchEntry(info); break;
        case PREFETCH_VALUE: prefetchValue(info); break;
        case PREFETCH_VALUE_NESTED: prefetchValueNested(info); break;
        default: serverPanic("Unknown prefetch state %d", info->state);
        }
    }
}

static void resetCommandsBatch(void) {
    batch->cur_idx = 0;
    batch->keys_done = 0;
    batch->key_count = 0;
    batch->client_count = 0;
    batch->executed_commands = 0;
}

/* Prefetch command-related data:
 * 1. Prefetch the command arguments allocated by the I/O thread to bring them closer to the L1 cache.
 * 2. Prefetch the keys and values for all commands in the current batch from the main hashtable. */
static void prefetchArgvs(void) {
    /* Prefetch argv's for all clients */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (!c || c->argc <= 1) continue;
        /* Skip prefetching first argv (cmd name) it was already looked up by the I/O thread. */
        for (int j = 1; j < c->argc; j++) {
            valkey_prefetch(c->argv[j]);
        }
    }

    /* Prefetch the argv->ptr if required */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (!c || c->argc <= 1) continue;
        for (int j = 1; j < c->argc; j++) {
            if (c->argv[j]->encoding == OBJ_ENCODING_RAW) {
                valkey_prefetch(objectGetVal(c->argv[j]));
            }
        }
    }
}

static void prefetchCommands(void) {
    prefetchArgvs();

    /* Get the keys ptrs - we do it here after the key obj was prefetched. */
    for (size_t i = 0; i < batch->key_count; i++) {
        batch->keys[i] = objectGetVal((robj *)batch->keys[i]);
    }

    /* Prefetch hashtable keys for all commands. Prefetching is beneficial only if there are more than one key. */
    if (batch->key_count > 1) {
        server.stat_total_prefetch_batches++;
        /* Prefetch keys from the main hashtable */
        hashtablePrefetch(batch->keys_tables);
    }
}

/* ---- Streaming prefetch ring (main thread) --------------------------------
 *
 * Alternative to the fill-then-drain batch above, enabled by the hidden config
 * `prefetch-ring`. Instead of prefetching all keys of a batch and then executing
 * all its commands, the ring keeps up to `prefetch-batch-max-size` key lookups
 * in flight *across* command execution: each command executed on main advances
 * every in-flight lookup by one stage and admits the next command from the
 * execution order (the batch's clients in order, each client's pending command
 * then its command queue). Memory-level parallelism therefore never drops to
 * zero between batches, and the prefetch distance is bounded by the ring depth
 * regardless of how deep a client's pipeline is.
 *
 * Safety: a slot holds only (db, kvstore index, hash, stage). It never holds a
 * pointer into the hashtable (the restart-safe stages in hashtable.c re-derive
 * the bucket from the live table on every call) and never dereferences client
 * memory after admission (`c` and `argv` are used purely as identity tokens to
 * match the executing command). Execution of arbitrary commands between stages
 * is therefore safe by construction; it can only make a prefetch useless. */

typedef struct ringSlot {
    client *c;     /* identity only, never dereferenced */
    robj **argv;   /* identity only, never dereferenced */
    serverDb *db;
    int kvslot;
    uint64_t hash;
    uint8_t stage; /* 0 = bucket, 1 = candidates, 2 = values, 3 = done */
} ringSlot;

static struct {
    ringSlot *slots;
    int cap;
    int head;
    int count;
    int first_live; /* offset from head of the first slot that may still need a step */
    /* Admission cursor: the next command in execution order. */
    client *cur_c;
    int cur_queue_idx; /* -1 = cur_c's pending (already parsed) command; else index into cmd_queue */
    int cur_batch_idx; /* index into batch->clients, or -1 when not walking a batch */
    int cur_waiting;   /* cur_c's queue is exhausted but its querybuf still holds unparsed
                        * commands (e.g. inline protocol): hold position, do not skip ahead */
} ring;

/* Index of the batch client currently being executed by processClientsCommandsBatch,
 * so the ring can resynchronise its cursor when it loses track. */
static int batch_exec_idx = -1;

static inline ringSlot *ringSlotAt(int i) {
    int idx = ring.head + i;
    if (idx >= ring.cap) idx -= ring.cap;
    return &ring.slots[idx];
}
#define RING_SLOT(i) (*ringSlotAt(i))

static void ringReset(void) {
    ring.head = 0;
    ring.count = 0;
    ring.first_live = 0;
    ring.cur_c = NULL;
    ring.cur_queue_idx = 0;
    ring.cur_batch_idx = -1;
    ring.cur_waiting = 0;
}

static void ringEnsureInit(void) {
    int cap = server.prefetch_batch_max_size;
    if (cap < 2) cap = 2;
    if (ring.slots != NULL && ring.cap == cap) return;
    if (ring.slots != NULL && ring.count > 0) return; /* resize once drained */
    zfree(ring.slots);
    ring.slots = zcalloc(sizeof(ringSlot) * cap);
    ring.cap = cap;
    ringReset();
}

/* Advance one slot by one stage. */
static inline void ringStepSlot(ringSlot *s) {
    if (s->stage >= 3) return;
    hashtable *ht = kvstoreGetHashtable(s->db->keys, s->kvslot);
    if (ht == NULL || hashtableSize(ht) == 0) {
        s->stage = 3;
        return;
    }
    switch (s->stage) {
    case 0:
        hashtablePrefetchBucketForHash(ht, s->hash);
        s->stage = 1;
        break;
    case 1:
        if (hashtablePrefetchCandidatesForHash(ht, s->hash) == 0) {
            s->stage = 3;
        } else if (server.io_threads_num >= server.min_io_threads_copy_avoid) {
            /* Copy avoidance: main never reads the value bytes, so the entry
             * prefetch is the last useful stage (same policy as the batch). */
            server.stat_total_prefetch_entries++;
            s->stage = 3;
        } else {
            s->stage = 2;
        }
        break;
    case 2: {
        /* Value prefetch, same policy as the batch: only when main will read
         * the value bytes itself (no copy avoidance). */
        if (server.io_threads_num < server.min_io_threads_copy_avoid) {
            void *cands[4];
            int n = hashtableGetCandidatesForHash(ht, s->hash, cands, 4);
            for (int i = 0; i < n; i++) {
                robj *val = cands[i];
                if (val->encoding == OBJ_ENCODING_RAW && val->type == OBJ_STRING) valkey_prefetch(objectGetVal(val));
            }
        }
        server.stat_total_prefetch_entries++;
        s->stage = 3;
        break;
    }
    default: break;
    }
}

static inline void ringStepAll(void) {
    for (int i = ring.first_live; i < ring.count; i++) ringStepSlot(ringSlotAt(i));
    while (ring.first_live < ring.count && ringSlotAt(ring.first_live)->stage >= 3) ring.first_live++;
}

/* Fetch the command the cursor points at. Returns 0 if the cursor is exhausted. */
static inline int ringCursorCommand(struct serverCommand **cmd, robj ***argv, int *argc, int *bad, int *slot) {
    client *c = ring.cur_c;
    if (c == NULL) return 0;
    if (ring.cur_queue_idx < 0) {
        *cmd = c->parsed_cmd;
        *argv = c->argv;
        *argc = c->argc;
        *bad = (c->read_flags & READ_FLAGS_BAD_ARITY) != 0;
        *slot = c->slot;
        return 1;
    }
    cmdQueue *q = &c->cmd_queue;
    if (ring.cur_queue_idx < q->off || ring.cur_queue_idx >= q->len) return 0;
    parsedCommand *p = &q->cmds[ring.cur_queue_idx];
    *cmd = p->cmd;
    *argv = p->argv;
    *argc = p->argc;
    *bad = (p->read_flags & READ_FLAGS_BAD_ARITY) != 0;
    *slot = p->slot;
    return 1;
}

/* Move the cursor to the next client of the batch, or park it (cur_c = NULL). */
static void ringCursorNextClient(void) {
    ring.cur_c = NULL;
    ring.cur_waiting = 0;
    if (ring.cur_batch_idx < 0 || batch == NULL) return;
    for (size_t i = ring.cur_batch_idx + 1; i < batch->client_count; i++) {
        client *n = batch->clients[i];
        if (n == NULL) continue;
        ring.cur_batch_idx = i;
        ring.cur_c = n;
        ring.cur_queue_idx = n->flag.pending_command ? -1 : n->cmd_queue.off;
        return;
    }
    ring.cur_batch_idx = -1;
}

/* Move the cursor to the next command in execution order. */
static void ringCursorAdvance(void) {
    client *c = ring.cur_c;
    if (c == NULL) return;
    if (ring.cur_queue_idx < 0) {
        ring.cur_queue_idx = c->cmd_queue.off;
    } else {
        ring.cur_queue_idx++;
    }
    if (ring.cur_queue_idx < c->cmd_queue.len) return;

    /* Queue exhausted but more commands will be parsed from querybuf later:
     * hold here rather than admitting a later client out of order. */
    if (c->querybuf != NULL && c->qb_pos < sdslen(c->querybuf)) {
        ring.cur_waiting = 1;
        return;
    }

    ringCursorNextClient();
}

/* Admit commands from the cursor until the ring is full or the cursor is exhausted. */
static void ringAdmit(void) {
    while (ring.count < ring.cap) {
        struct serverCommand *cmd;
        robj **argv;
        int argc, bad, slot;
        if (!ringCursorCommand(&cmd, &argv, &argc, &bad, &slot)) {
            if (ring.cur_waiting) return;
            ringCursorAdvance();
            if (ring.cur_c == NULL || ring.cur_waiting) return;
            continue;
        }
        client *c = ring.cur_c;
        int kvslot = (server.cluster_enabled && slot >= 0) ? slot : 0;
        int free_slots = ring.cap - ring.count;
        int admitted = 0;

        if (cmd != NULL && !bad) {
            getKeysResult result;
            initGetKeysResult(&result);
            int num_keys = getKeysFromCommand(cmd, argv, argc, &result);
            if (num_keys > free_slots && ring.count > 0) {
                /* Wait for room so a multi-key command is admitted whole. */
                getKeysFreeResult(&result);
                return;
            }
            hashtable *ht = kvstoreGetHashtable(c->db->keys, kvslot);
            for (int i = 0; i < num_keys && ring.count < ring.cap; i++) {
                ringSlot *s = &RING_SLOT(ring.count);
                s->c = c;
                s->argv = argv;
                s->db = c->db;
                s->kvslot = kvslot;
                if (ht != NULL && hashtableSize(ht) > 0) {
                    s->hash = hashtableHashKey(ht, objectGetVal(argv[result.keys[i].pos]));
                    s->stage = 0;
                } else {
                    s->hash = 0;
                    s->stage = 3;
                }
                ring.count++;
                admitted++;
            }
            getKeysFreeResult(&result);
        }
        if (admitted == 0) {
            /* Keyless (or unparseable) command: one done slot keeps the FIFO aligned. */
            ringSlot *s = &RING_SLOT(ring.count);
            s->c = c;
            s->argv = argv;
            s->db = c->db;
            s->kvslot = 0;
            s->hash = 0;
            s->stage = 3;
            ring.count++;
        }
        ringCursorAdvance();
    }
}

/* A client joined the batch: start its lookups now, so they are in flight while
 * the remaining read completions are processed and before execution begins. */
static void ringOnClientAdded(client *c, int batch_idx) {
    ringEnsureInit();
    if (ring.cur_c == NULL) {
        ring.cur_batch_idx = batch_idx;
        ring.cur_c = c;
        ring.cur_queue_idx = c->flag.pending_command ? -1 : c->cmd_queue.off;
    }
    ringAdmit();
    ringStepAll();
}

/* Batch execution is starting. Every client was already admitted when it joined
 * (ringOnClientAdded), so only advance the in-flight lookups one more stage. */
static void ringStartBatch(void) {
    ringEnsureInit();
    ringAdmit();
    ringStepAll();
}

/* Called on the main thread immediately before a command in c->argv executes. */
void prefetchRingBeforeExecute(client *c) {
    if (!server.prefetch_ring) return;
    if (ProcessingEventsWhileBlocked) {
        /* Nested processing breaks execution order; drop everything and let
         * the outer drain resynchronise. */
        if (ring.slots) ringReset();
        return;
    }
    ringEnsureInit();

    /* Anything ahead of this command in the ring belongs to commands that did
     * not execute in the expected order (blocked client, skipped client...). */
    while (ring.count > 0 && !(RING_SLOT(0).c == c && RING_SLOT(0).argv == c->argv)) {
        ring.head = (ring.head + 1) % ring.cap;
        ring.count--;
        if (ring.first_live > 0) ring.first_live--;
    }

    if (ring.count == 0) {
        /* This command was never admitted: resynchronise the cursor to what
         * executes after it. From the pending position (-1), advance() lands on
         * cmd_queue.off, which is the next command whether the current one was
         * the pending command or was just popped from the queue. */
        ring.cur_c = c;
        ring.cur_queue_idx = -1;
        ring.cur_batch_idx = (batch != NULL && batch->executed_commands > 0) ? batch_exec_idx : -1;
        ringCursorAdvance();
    } else {
        /* Head-ready policy: make sure this command's own lookups are done,
         * advancing every in-flight lookup while we wait. Bounded: each slot
         * needs at most 3 steps. */
        for (int round = 0; round < 3; round++) {
            int pending = 0;
            for (int i = ring.first_live; i < ring.count; i++) {
                ringSlot *s = ringSlotAt(i);
                if (s->c != c || s->argv != c->argv) break;
                if (s->stage < 3) pending = 1;
            }
            if (!pending) break;
            ringStepAll();
        }
        /* Retire this command's slots. */
        while (ring.count > 0 && RING_SLOT(0).c == c && RING_SLOT(0).argv == c->argv) {
            ring.head = (ring.head + 1) % ring.cap;
            ring.count--;
            if (ring.first_live > 0) ring.first_live--;
        }
    }

    if (ring.cur_waiting && ring.cur_c == c) {
        /* The command that just executed came from the querybuf; the queue may
         * have been refilled behind it. Re-seed from the current position. */
        ring.cur_waiting = 0;
        ring.cur_queue_idx = -1;
        ringCursorAdvance();
    }
    ringAdmit();
    ringStepAll();
}

/* The client is going away (or leaving the batch): make sure the cursor never
 * dereferences it again. Slots are identity-only and need no cleanup. */
void prefetchRingClientGone(client *c) {
    if (ring.slots == NULL) return;
    if (ring.cur_c == c) ringCursorNextClient();
}

/* Processes all the prefetched commands in the current batch. */
void processClientsCommandsBatch(void) {
    if (!batch || batch->client_count == 0) return;

    /* If executed_commands is not 0,
     * it means that we are in the middle of processing a batch and this is a recursive call */
    if (batch->executed_commands == 0) {
        if (server.prefetch_ring) {
            prefetchArgvs();
            /* Same accounting as batch mode: a batch counts when it carries more
             * than one key to look up. */
            int multi = batch->client_count > 1;
            if (!multi) {
                client *c0 = batch->clients[0];
                multi = c0 != NULL && c0->cmd_queue.len - c0->cmd_queue.off >= 1;
            }
            if (multi) server.stat_total_prefetch_batches++;
            ringStartBatch();
        } else {
            prefetchCommands();
        }
    }

    /* Process the commands */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (c == NULL) continue;

        /* Set the client to null immediately to avoid accessing it again recursively when ProcessingEventsWhileBlocked */
        batch->clients[i] = NULL;
        batch->executed_commands++;
        batch_exec_idx = i;
        if (processPendingCommandAndInputBuffer(c) != C_ERR) beforeNextClient(c);
    }
    batch_exec_idx = -1;

    resetCommandsBatch();

    /* Handle the case where the max prefetch size has been changed. */
    if (batch->max_prefetch_size != (size_t)server.prefetch_batch_max_size) {
        onMaxBatchSizeChange(NULL);
    }
}

/* Get a command's keys and add them to the current prefetching batch. */
static void addCommandToBatch(struct serverCommand *cmd, robj **argv, int argc, serverDb *db, int slot) {
    getKeysResult result;
    initGetKeysResult(&result);
    int num_keys = getKeysFromCommand(cmd, argv, argc, &result);
    int member_idx = cmd->member_arg_index;
    robj *member = (member_idx > 0 && member_idx < argc) ? argv[member_idx] : NULL;
    for (int i = 0; i < num_keys && batch->key_count < batch->max_prefetch_size; i++) {
        batch->keys[batch->key_count] = argv[result.keys[i].pos];
        batch->slots[batch->key_count] = slot >= 0 ? slot : 0;
        batch->keys_tables[batch->key_count] = kvstoreGetHashtable(db->keys, batch->slots[batch->key_count]);
        batch->key_members[batch->key_count] =
            (result.keys[i].flags & CMD_KEY_OW) && !(result.keys[i].flags & CMD_KEY_ACCESS) ? NULL : member;
        batch->key_count++;
    }
    getKeysFreeResult(&result);
}

/* Adds the client's command to the current batch and processes the batch
 * if it becomes full.
 *
 * Returns C_OK if the command was added successfully, C_ERR otherwise. */
int addCommandToBatchAndProcessIfFull(client *c) {
    if (!batch) return C_ERR;

    batch->clients[batch->client_count++] = c;

    if (server.prefetch_ring) {
        /* The ring does its own key extraction in execution order. */
        ringOnClientAdded(c, batch->client_count - 1);
        if (batch->client_count == batch->max_prefetch_size) processClientsCommandsBatch();
        return C_OK;
    }

    /* Client's next command */
    if (c->parsed_cmd && !(c->read_flags & READ_FLAGS_BAD_ARITY)) {
        c->read_flags |= READ_FLAGS_PREFETCHED;
        addCommandToBatch(c->parsed_cmd, c->argv, c->argc, c->db, c->slot);
    }

    /* Commands in the queue. */
    for (int j = c->cmd_queue.off; j < c->cmd_queue.len && batch->key_count < batch->max_prefetch_size; j++) {
        parsedCommand *p = &c->cmd_queue.cmds[j];
        /* Error, incomplete command, or a command whose argc violates its arity. The latter must be
         * skipped because getKeysFromCommand() assumes the arity check has already passed. */
        if (!p->cmd || p->read_flags & READ_FLAGS_BAD_ARITY) continue;
        p->read_flags |= READ_FLAGS_PREFETCHED;
        addCommandToBatch(p->cmd, p->argv, p->argc, c->db, p->slot);
    }

    /* If the batch is full, process it.
     * We also check the client count to handle cases where
     * no keys exist for the clients' commands. */
    if (batch->client_count == batch->max_prefetch_size || batch->key_count == batch->max_prefetch_size) {
        processClientsCommandsBatch();
    }

    return C_OK;
}

/* Removes the given client from the pending prefetch batch, if present. */
void removeClientFromPendingCommandsBatch(client *c) {
    prefetchRingClientGone(c);
    if (!batch) return;

    for (size_t i = 0; i < batch->client_count; i++) {
        if (batch->clients[i] == c) {
            batch->clients[i] = NULL;
            return;
        }
    }
}
