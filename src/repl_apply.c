/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Parallel apply of the replication stream on a replica. See repl_apply.h. */

#include "server.h"
#include "repl_apply.h"
#include "staged_loop.h"
#include "fastpath.h"
#include "memory_prefetch.h"
#include "hotkeys.h"
#include "cluster_slot_stats.h"
#include "hdr_histogram.h"


_Thread_local int repl_apply_in_lane = 0;
_Thread_local int repl_apply_plan_lane = 0;
_Thread_local const char *repl_apply_key = NULL;
_Thread_local int repl_apply_shard = 0;

typedef struct {
    serverDb *db;
    robj **argv;
    int argc;
    int shard;
    long long pxat; /* -1: no expire */
    unsigned long long in_bytes;
    long long duration; /* usec, set only for an op kept for the commandlog */
} replApplyOp;

typedef struct {
    replApplyOp *ops;
    int len;
    int cap;
    monotime usec;
    struct hdr_histogram *hist; /* this lane's SET latencies, merged into the command's by main */
    long long max_us;
    int *logs; /* ops the commandlog may take; they keep their arguments until main logs them */
    int nlogs;
    int logcap;
} replApplyLane;

static replApplyLane ra_lanes[IO_THREADS_MAX_NUM];
static int ra_nlanes = 0; /* lanes of the held batch; fixed until it is applied */
static int ra_pending = 0;
static int ra_main_lane = 1; /* lane 0 runs on main; otherwise lane i runs on IO thread i + 1 */
static struct serverCommand *ra_set_cmd = NULL;

/* The keyspace is split into tables: by slot in cluster mode, by keyspace-shard-bits otherwise. Lanes
 * apply writes to disjoint tables. */
static int replApplySharded(void) {
    return server.cluster_enabled || server.keyspace_shard_bits;
}

/* ---------------------------------------------------------------------------------------------
 * The primary's stream on the write lanes.
 *
 * A planned read of the primary (READ_FLAGS_STREAM_LANES) is parsed in two steps. In the read phase the IO
 * thread that reads it parses the first command as usual and scans the rest for command boundaries, cutting
 * the complete commands into up to one chunk per staged thread and noting, at each cut, the database and
 * transaction state the commands before it leave. In the write phase main opens a PARSE phase in which
 * every staged thread parses its chunk into its slice of the client's command queue and classifies each
 * command, in stream order (T): a write lane by the shard of its keys, with the lane commands and key checks
 * of a primary's write lanes, or a barrier. Main then runs T segment by segment: a segment's lane commands
 * in an APPLY phase, one lane per staged thread, and the barrier that ends it on main, as the primary client
 * runs it. After a segment main moves the replication offset past it and proxies its bytes to the backlog
 * and the sub-replicas, so nothing is acknowledged or forwarded before it is applied. Whatever main does not
 * run this way is parsed and run the usual way.
 * --------------------------------------------------------------------------------------------- */

#define RP_STOP 2                /* rpRunOnMain: the client cannot take its next command now */
#define RP_MAX_CMDS 65000        /* commands of one planned read: the command queue holds 65535 */
#define RP_MIN_CHUNK (16 * 1024) /* bytes; a read with fewer is one chunk, parsed by its IO thread */
#define RP_IDLE 0
#define RP_PARSE 1 /* the phase running is the plan's PARSE phase */
#define RP_APPLY 2 /* ... or one of its APPLY phases */

#define RP_GROW(arr, n, cap, init)                                   \
    do {                                                             \
        if ((n) == (cap)) {                                          \
            (cap) = (cap) ? (cap) * 2 : (init);                      \
            (arr) = zrealloc((arr), sizeof(*(arr)) * (size_t)(cap)); \
        }                                                            \
    } while (0)

typedef struct rpEntry {
    size_t qb_end; /* querybuf offset just past the command */
    serverDb *db;
    int shard;    /* lane entries: the keyspace hashtable of all its keys */
    int lane;     /* -1: a barrier */
    uint32_t seg; /* barriers before it in its chunk */
    int marker;   /* RL-g2l: the MULTI or EXEC of a transaction its lane applies whole; the lane drops it */
} rpEntry;

/* What a command leaves for the next ones' classification. */
typedef struct rpState {
    serverDb *db;
    int in_multi;
    int stop; /* the rest of the read are barriers */
} rpState;

/* Plan positions [first, first + n): written by the one thread that parses the chunk. */
typedef struct __attribute__((aligned(64))) rpChunk {
    size_t start; /* querybuf offset of its first command */
    int first, n;
    rpState st;                  /* before its first command; while parsing, the current state */
    int *lane_idx[FP_MAX_LANES]; /* per lane: plan positions of its lane commands, in T order */
    int lane_n[FP_MAX_LANES], lane_cap[FP_MAX_LANES];
    int *bpos; /* plan positions of its barriers */
    int nb, bcap;
    uint32_t base; /* barriers of the chunks before it */
    /* RL-g2l: a MULTI this chunk opened, its plan positions so far; placed at its EXEC (rpTxnEnd) */
    int *txn;
    int txn_n, txn_cap, txn_open, txn_ok, txn_shard;
} rpChunk;

/* Lane l's place in the plan: a chunk and a position in that chunk's vector for l. */
typedef struct __attribute__((aligned(64))) rpCursor {
    int chunk, pos;
} rpCursor;

static struct {
    client *c;             /* the primary client of the plan */
    int ready;             /* built by the IO thread, not yet run by main */
    int parsed;            /* the chunks are parsed and classified */
    int mode;              /* RP_PARSE or RP_APPLY while a phase runs the plan */
    int nlanes, main_lane; /* main sets them before the read */
    rpChunk ch[FP_MAX_LANES];
    int nchunks;
    rpEntry *e; /* plan positions: 0 is c->argv, k >= 1 the command queue's slot k - 1 */
    int n, cap;
    int *bpos; /* plan positions of all barriers, in T order */
    int nb, bcap;
    size_t scan_end;                /* querybuf offset past the last command of the plan */
    uint32_t seg;                   /* the segment an APPLY phase runs */
    long long read_us, classify_us; /* IO thread: the read job, and the scan and planning in it */
} rp;

static rpCursor rp_cur[FP_MAX_LANES];

int replApplyPlanBegin(void) {
    if (!server.replica_parallel_apply || !stagedApplyReady() || !fastpathStreamLanesAllowed()) return 0;
    int main_lane = stagedMainTakesLane();
    int n = stagedWorkerCount() + main_lane;
    if (n > FP_MAX_LANES) n = FP_MAX_LANES;
    if (n < 2) return 0;
    rp.nlanes = n;
    rp.main_lane = main_lane;
    return 1;
}

void replApplyPlanReset(void) {
    rp.ready = 0;
    rp.read_us = rp.classify_us = 0;
}

void replApplyPlanReadTime(long long us) {
    rp.read_us = us;
}

static int rpNameIs(const char *s, size_t len, const char *name) {
    return len == strlen(name) && !strncasecmp(s, name, len);
}

/* After a barrier named name (first argument arg): MULTI, EXEC and DISCARD open and close a transaction, and
 * SELECT moves to a database. A database not created yet is created when main runs the SELECT, so the rest of
 * the read waits for it. */
static void rpTrack(rpState *st, const char *name, size_t nlen, const char *arg, size_t alen, int argc) {
    if (nlen < 4 || nlen > 7) return;
    /* One length and first letter each: most commands are rejected without a compare. */
    char c0 = (char)(name[0] | 0x20);
    if (nlen == 5 && c0 == 'm' && rpNameIs(name, nlen, "multi")) {
        st->in_multi = 1;
    } else if ((nlen == 4 && c0 == 'e' && rpNameIs(name, nlen, "exec")) ||
               (nlen == 7 && c0 == 'd' && rpNameIs(name, nlen, "discard"))) {
        st->in_multi = 0;
    } else if (nlen == 6 && c0 == 's' && rpNameIs(name, nlen, "select")) {
        long long id;
        serverDb *db = NULL;
        if (!st->in_multi && argc == 2 && string2ll(arg, alen, &id) && id >= 0 && id < server.dbnum)
            db = server.db[id];
        if (db)
            st->db = db;
        else
            st->stop = 1;
    }
}

/* Place the command at plan position idx in its chunk: on a lane (lane >= 0), or as a barrier. */
static void rpPlace(rpChunk *k, int idx, int lane, int marker) {
    rpEntry *e = &rp.e[idx];
    e->seg = (uint32_t)k->nb;
    e->lane = lane;
    e->marker = marker;
    if (lane >= 0) {
        RP_GROW(k->lane_idx[lane], k->lane_n[lane], k->lane_cap[lane], 256);
        k->lane_idx[lane][k->lane_n[lane]++] = idx;
        return;
    }
    e->shard = -1;
    RP_GROW(k->bpos, k->nb, k->bcap, 16);
    k->bpos[k->nb++] = idx;
}

static void rpTrackArgv(rpChunk *k, robj **argv, int argc) {
    if (argc > 0 && sdsEncodedObject(argv[0])) {
        int a = argc > 1 && sdsEncodedObject(argv[1]);
        rpTrack(&k->st, objectGetVal(argv[0]), sdslen(objectGetVal(argv[0])), a ? objectGetVal(argv[1]) : NULL,
                a ? sdslen(objectGetVal(argv[1])) : 0, argc);
    }
}

/* RL-g2l-repl-speed: the transaction the chunk opened ends (at its EXEC: whole = 1; at a DISCARD, or the
 * chunk's end: 0). When every command in it is a lane command of one shard, all of it goes to that shard's
 * lane, which runs the commands in order inside one APPLY phase and drops the MULTI and the EXEC: nothing
 * else touches that shard during the phase and nothing reads the keyspace until it ends, so the transaction
 * is applied atomically, and the segment around it stays whole. Otherwise every command of it is a barrier,
 * as before: main runs the transaction as the primary client. */
static void rpTxnEnd(rpChunk *k, int whole) {
    int lane = whole && k->txn_ok && k->txn_shard >= 0 ? k->txn_shard % rp.nlanes : -1;
    for (int i = 0; i < k->txn_n; i++) {
        int idx = k->txn[i];
        if (lane >= 0) rp.e[idx].shard = k->txn_shard;
        rpPlace(k, idx, lane, lane >= 0 && (i == 0 || i == k->txn_n - 1));
    }
    k->txn_n = 0;
    k->txn_open = 0;
}

static void rpTxnAdd(rpChunk *k, int idx) {
    RP_GROW(k->txn, k->txn_n, k->txn_cap, 16);
    k->txn[k->txn_n++] = idx;
}

/* The lane of the command at plan position idx, or a barrier. */
static void rpClassify(rpChunk *k, int idx, struct serverCommand *cmd, robj **argv, int argc, int read_flags, int slot, size_t qb_end) {
    rpEntry *e = &rp.e[idx];
    e->qb_end = qb_end;
    e->db = k->st.db;
    e->shard = -1;
    e->lane = -1;
    e->marker = 0;
    e->seg = (uint32_t)k->nb;
    int shard = -1;
    if (!k->st.stop && cmd && cmd->write_lane && !(read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY)))
        shard = fastpathKeysShard(k->st.db, cmd, argv, argc, slot);
    sds name = argc > 0 && sdsEncodedObject(argv[0]) ? objectGetVal(argv[0]) : NULL;
    size_t nlen = name ? sdslen(name) : 0;
    if (k->txn_open) {
        /* In a transaction this chunk opened: decided at its EXEC. Classified as the barriers they were. */
        rpTxnAdd(k, idx);
        int exec = name && rpNameIs(name, nlen, "exec");
        if (exec) {
            if (argc != 1) k->txn_ok = 0;
        } else if (shard < 0 || (k->txn_shard >= 0 && shard != k->txn_shard)) {
            k->txn_ok = 0;
        } else {
            k->txn_shard = shard;
        }
        rpTrackArgv(k, argv, argc);
        if (exec || !k->st.in_multi) rpTxnEnd(k, exec);
        return;
    }
    if (!k->st.in_multi && shard >= 0) {
        e->shard = shard;
        rpPlace(k, idx, shard % rp.nlanes, 0);
        return;
    }
    if (server.replica_parallel_apply_multi && idx > 0 && !k->st.stop && !k->st.in_multi && argc == 1 && name &&
        rpNameIs(name, nlen, "multi")) {
        k->txn_open = 1;
        k->txn_ok = 1;
        k->txn_shard = -1;
        k->txn_n = 0;
        rpTxnAdd(k, idx);
        rpTrackArgv(k, argv, argc);
        return;
    }
    rpPlace(k, idx, -1, 0);
    rpTrackArgv(k, argv, argc);
}

/* A RESP integer ending at the next \r\n of [p, end). Returns the position after the \n, or 0 when the
 * line is not complete or not a number. */
static inline size_t rpScanInt(const char *qb, size_t p, size_t end, long long *v) {
    /* The stream's lengths are plain decimals: digits only, no sign, no leading zero, at most 18 of them
     * (string2ll's rules on that range). Anything else returns 0 and goes the usual way. */
    size_t q = p;
    unsigned long long x = 0;
    *v = 0;
    while (q < end && q - p < 18) {
        unsigned d = (unsigned char)qb[q] - '0';
        if (d > 9) break;
        x = x * 10 + d;
        q++;
    }
    if (q == p || (qb[p] == '0' && q - p > 1)) return 0;
    if (q + 1 >= end || qb[q] != '\r' || qb[q + 1] != '\n') return 0;
    *v = (long long)x;
    return q + 2;
}

/* IO thread: cut the complete multibulk commands from qb_pos on into chunks of about equal bytes, carrying
 * the classification state across each cut. Stops at a partial command, or at anything the parser should
 * judge. Returns the number of commands. */
static int rpScan(client *c, rpState st) {
    const char *qb = c->querybuf;
    size_t end = sdslen(c->querybuf), p = c->qb_pos;
    size_t target = (end - p) / (size_t)rp.nlanes + 1;
    if (target < RP_MIN_CHUNK) target = RP_MIN_CHUNK;
    int total = 0, t = 0;
    rpChunk *k = &rp.ch[0];
    k->start = p;
    k->n = 0;
    while (p < end && total < RP_MAX_CMDS) {
        long long n, len;
        if (qb[p] != '*') break;
        size_t q = rpScanInt(qb, p + 1, end, &n);
        if (!q || n <= 0 || n > 1024 * 1024) break;
        const char *name = NULL, *arg = NULL;
        size_t nlen = 0, alen = 0;
        long long j;
        for (j = 0; j < n; j++) {
            if (q >= end || qb[q] != '$') break;
            size_t d = rpScanInt(qb, q + 1, end, &len);
            if (!d || len < 0 || d + (size_t)len + 2 > end || qb[d + len] != '\r' || qb[d + len + 1] != '\n') break;
            if (j == 0) name = qb + d, nlen = (size_t)len;
            if (j == 1) arg = qb + d, alen = (size_t)len;
            q = d + (size_t)len + 2;
        }
        if (j < n) break;
        if (k->n > 0 && p - k->start >= target && t + 1 < rp.nlanes) {
            t++;
            k = &rp.ch[t];
            k->start = p;
            k->first = 1 + total;
            k->n = 0;
            k->st = st;
        }
        k->n++;
        total++;
        rpTrack(&st, name, nlen, arg, alen, (int)n);
        p = q;
    }
    rp.nchunks = t + 1;
    rp.scan_end = p;
    return total;
}

/* Parse the command the scan found at offset p into pc. Returns its length. */
static size_t rpParseOne(const char *qb, size_t p, parsedCommand *pc) {
    long long n, len;
    size_t q = rpScanInt(qb, p + 1, SIZE_MAX, &n);
    pc->argc = (int)n;
    pc->argv_len = (int)n;
    pc->argv = zmalloc(sizeof(robj *) * (size_t)n);
    pc->argv_len_sum = 0;
    for (int j = 0; j < n; j++) {
        size_t d = rpScanInt(qb, q + 1, SIZE_MAX, &len);
        pc->argv[j] = createStringObject(qb + d, (size_t)len);
        pc->argv_len_sum += (size_t)len;
        q = d + (size_t)len + 2;
    }
    pc->input_bytes = q - p;
    pc->read_flags = READ_FLAGS_PARSING_COMPLETED;
    pc->cmd = NULL;
    pc->slot = -1;
    prepareParsedCommand(pc);
    return q - p;
}

/* Thread of lane t: parse and classify chunk t into its slice of the command queue. */
static void rpParseChunk(int t) {
    rpChunk *k = &rp.ch[t];
    client *c = rp.c;
    size_t p = k->start;
    for (int i = 0; i < k->n; i++) {
        int idx = k->first + i;
        parsedCommand *pc = &c->cmd_queue.cmds[idx - 1];
        p += rpParseOne(c->querybuf, p, pc);
        rpClassify(k, idx, pc->cmd, pc->argv, pc->argc, pc->read_flags, pc->slot, p);
    }
    if (k->txn_open) rpTxnEnd(k, 0); /* its EXEC is in a later chunk or read: main runs it */
}

static void rpResetChunk(rpChunk *k) {
    for (int l = 0; l < rp.nlanes; l++) k->lane_n[l] = 0;
    k->nb = 0;
    k->txn_n = 0;
    k->txn_open = 0;
}

void replApplyPlanRead(client *c) {
    rp.ready = 0;
    if (!(c->read_flags & READ_FLAGS_PARSING_COMPLETED) || c->argc == 0 || (c->read_flags & READ_FLAGS_ERROR_MASK))
        return;
    monotime start = getMonotonicUs();
    rp.c = c;
    rp.parsed = 0;
    for (int t = 0; t < rp.nlanes; t++) rpResetChunk(&rp.ch[t]);
    rpChunk *k0 = &rp.ch[0];
    k0->first = 1;
    k0->st = (rpState){.db = c->db, .in_multi = c->flag.multi, .stop = 0};
    if (!rp.cap) {
        rp.cap = 1024;
        rp.e = zmalloc(sizeof(rpEntry) * (size_t)rp.cap);
    }
    rpClassify(k0, 0, c->parsed_cmd, c->argv, c->argc, c->read_flags, c->slot, c->qb_applied);
    int total = rpScan(c, k0->st);
    rp.n = 1 + total;
    if (rp.cap < rp.n) {
        rp.cap = rp.n;
        rp.e = zrealloc(rp.e, sizeof(rpEntry) * (size_t)rp.cap);
    }
    cmdQueue *q = &c->cmd_queue;
    serverAssert(q->len == 0);
    if (q->cap < total) {
        q->cmds = zrealloc(q->cmds, sizeof(parsedCommand) * (size_t)total);
        q->cap = (uint16_t)total;
    }
    /* A small read is parsed here; chunks wait for the PARSE phase. */
    if (rp.nchunks == 1) {
        rpParseChunk(0);
        q->len = (uint16_t)total;
        q->off = 0;
        c->qb_pos = rp.scan_end;
        rp.parsed = 1;
    }
    rp.classify_us = getMonotonicUs() - start;
    rp.ready = 1;
}

/* Main, after the chunks are parsed: the barriers of all chunks in T order. */
static void rpMerge(void) {
    rp.nb = 0;
    uint32_t base = 0;
    for (int t = 0; t < rp.nchunks; t++) {
        rpChunk *k = &rp.ch[t];
        k->base = base;
        for (int i = 0; i < k->nb; i++) {
            RP_GROW(rp.bpos, rp.nb, rp.bcap, 64);
            rp.bpos[rp.nb++] = k->bpos[i];
        }
        base += (uint32_t)k->nb;
    }
    for (int l = 0; l < rp.nlanes; l++) rp_cur[l] = (rpCursor){0, 0};
}

/* Lane thread, APPLY phase: apply this lane's commands of the current segment, chunk after chunk. The commands are
 * taken in windows of the prefetch batch size: the keys of a window are looked up ahead (bucket, entry, value, as
 * main's and the client lanes' batches do) and its arguments, which the parsing thread left in its own cache,
 * brought in, before the window runs. */
#define RP_PF_WINDOW 128
static void *rp_pf[FP_MAX_LANES];

static void rpRunLane(int lane) {
    client *c = rp.c;
    rpCursor *cur = &rp_cur[lane];
    int entered = 0;
    int want = server.prefetch_batch_max_size;
    int use_pf = server.io_threads_lane_prefetch && want > 1;
    if (want < 1) want = 1;
    if (want > RP_PF_WINDOW) want = RP_PF_WINDOW;
    if (use_pf && (!rp_pf[lane] || prefetchLaneBatchSize(rp_pf[lane]) != server.prefetch_batch_max_size)) {
        if (rp_pf[lane]) prefetchLaneBatchFree(rp_pf[lane]);
        rp_pf[lane] = prefetchLaneBatchCreate();
    }
    if (!rp_pf[lane]) use_pf = 0;
    int win[RP_PF_WINDOW];
    for (;;) {
        int nwin = 0;
        while (nwin < want && cur->chunk < rp.nchunks) {
            rpChunk *k = &rp.ch[cur->chunk];
            if (cur->pos >= k->lane_n[lane]) {
                cur->chunk++;
                cur->pos = 0;
                continue;
            }
            int idx = k->lane_idx[lane][cur->pos];
            uint32_t seg = k->base + rp.e[idx].seg;
            if (seg > rp.seg) break; /* the next segment's: stays for its phase */
            cur->pos++;
            if (seg < rp.seg) continue; /* its segment ran on main */
            win[nwin++] = idx;
        }
        if (!nwin) break;
        if (!entered) {
            fastpathStreamLaneEnter(lane);
            repl_apply_plan_lane = 1;
            entered = 1;
        }
        if (use_pf && nwin > 1) {
            prefetchLaneBatchUse(rp_pf[lane]);
            getKeysResult result;
            initGetKeysResult(&result);
            int room = 1;
            for (int j = 0; j < nwin && room; j++) {
                int idx = win[j];
                if (rp.e[idx].marker || idx == 0) continue;
                parsedCommand *p = &c->cmd_queue.cmds[idx - 1];
                if (p->cmd) room = prefetchBatchAddCommand(p->cmd, p->argv, p->argc, rp.e[idx].db, rp.e[idx].shard, &result);
            }
            getKeysFreeResult(&result);
            for (int j = 0; j < nwin; j++) {
                if (win[j] == 0) continue;
                parsedCommand *p = &c->cmd_queue.cmds[win[j] - 1];
                for (int a = 1; a < p->argc; a++) valkey_prefetch(p->argv[a]);
            }
            prefetchBatchRun();
            prefetchBatchReset();
            prefetchLaneBatchUse(NULL);
        }
        for (int j = 0; j < nwin; j++) {
            int idx = win[j];
            rpEntry *e = &rp.e[idx];
            if (e->marker) { /* RL-g2l: a whole transaction's MULTI or EXEC (never position 0) */
                parsedCommand *p = &c->cmd_queue.cmds[idx - 1];
                fastpathStreamLaneDrop(lane, p->argv, p->argc, p->argv_len, p->argv_len_sum);
                continue;
            }
            if (idx == 0) {
                fastpathStreamLaneCall(lane, e->db, c->parsed_cmd, c->argv, c->argc, c->argv_len, c->argv_len_sum,
                                       c->net_input_bytes_curr_cmd, e->shard, (uint64_t)idx);
            } else {
                parsedCommand *p = &c->cmd_queue.cmds[idx - 1];
                fastpathStreamLaneCall(lane, e->db, p->cmd, p->argv, p->argc, p->argv_len, p->argv_len_sum,
                                       p->input_bytes, e->shard, (uint64_t)idx);
            }
        }
    }
    if (entered) {
        repl_apply_plan_lane = 0;
        fastpathStreamLaneLeave();
    }
}

/* Staged thread tid (0 = main) in a phase of the plan. */
static void rpPhase(int tid) {
    int lane = rp.main_lane ? tid : tid - 1;
    if (lane < 0 || lane >= rp.nlanes) return;
    if (rp.mode == RP_PARSE) {
        if (lane < rp.nchunks) rpParseChunk(lane);
    } else {
        rpRunLane(lane);
    }
}

/* Main: the stream is applied up to qb_applied. Advance the replication offset and proxy the bytes to the
 * backlog and the sub-replicas, as commandProcessed does after a command. */
static void rpAdvance(client *c) {
    long long prev = c->repl_data->reploff;
    c->repl_data->reploff = c->repl_data->read_reploff - sdslen(c->querybuf) + c->qb_applied;
    serverAssert(c->repl_data->reploff > prev);
    long long applied = c->repl_data->reploff - prev;
    replicationFeedStreamFromPrimaryStream(c->querybuf + c->repl_data->repl_applied, applied);
    c->repl_data->repl_applied += applied;
}

/* Main: the lanes applied the commands at positions [from, to); take them off the client. */
static void rpConsume(client *c, int from, int to) {
    if (from == 0) {
        /* The lane freed the arguments of the client's current command. */
        c->argv = NULL;
        c->argc = 0;
        c->argv_len = 0;
        c->argv_len_sum = 0;
        c->parsed_cmd = NULL;
        c->slot = -1;
        c->net_input_bytes_curr_cmd = 0;
    }
    if (to > 1) {
        cmdQueue *q = &c->cmd_queue;
        q->off = (uint16_t)(to - 1);
        if (q->off == q->len) q->off = q->len = 0;
    }
    c->qb_applied = rp.e[to - 1].qb_end;
    c->commands_processed += to - from;
    int markers = 0;
    for (int idx = from; idx < to; idx++) markers += rp.e[idx].marker;
    server.stat_repl_lane_txns += markers / 2;
    rpAdvance(c);
}

/* Main: the client is where the plan expects it before position idx. A barrier that discarded the queue or
 * replaced the link leaves nothing of the plan to run. */
static int rpQueueAt(client *c, int idx, int qlen) {
    if (server.primary != c || c->cmd_queue.len != qlen) return 0;
    if (idx == 0) return c->argc > 0 && c->cmd_queue.off == 0;
    return c->argc == 0 && c->cmd_queue.off == idx - 1;
}

/* Main: run the command at position idx as the primary client, the usual way. C_ERR: the client is gone. */
static int rpRunOnMain(client *c, int idx, int qlen) {
    if (!rpQueueAt(c, idx, qlen)) return RP_STOP;
    int r = processPlannedCommand(c, idx > 0);
    return r == C_OK || r == C_ERR ? r : RP_STOP;
}

/* Main: a phase of the plan on all staged threads. */
static void rpRunPhase(int mode) {
    rp.mode = mode;
    if (mode == RP_APPLY) dbLanePhaseBegin();
    stagedRunApplyPhase(1);
    if (mode == RP_APPLY) dbLanePhaseEnd();
    rp.mode = RP_IDLE;
}

/* Main: a lane command of [from, to) has a key a client watches or is blocked on. The read was classified
 * against the observers that existed when it was read; commands that ran on main since (a WATCH, a
 * blocking command) can have added more. */
static int rpObservedInRange(client *c, int from, int to) {
    int blocked = server.blocked_clients_by_type[BLOCKED_LIST] || server.blocked_clients_by_type[BLOCKED_ZSET] ||
                  server.blocked_clients_by_type[BLOCKED_STREAM];
    if (!blocked && !server.watching_clients) return 0;
    for (int idx = from; idx < to; idx++) {
        rpEntry *e = &rp.e[idx];
        struct serverCommand *cmd;
        robj **argv;
        int argc;
        if (idx == 0) {
            cmd = c->parsed_cmd, argv = c->argv, argc = c->argc;
        } else {
            parsedCommand *p = &c->cmd_queue.cmds[idx - 1];
            cmd = p->cmd, argv = p->argv, argc = p->argc;
        }
        getKeysResult result;
        initGetKeysResult(&result);
        int n = getKeysFromCommand(cmd, argv, argc, &result);
        int observed = 0;
        for (int i = 0; i < n && !observed; i++) {
            robj *k = argv[result.keys[i].pos];
            observed = (server.watching_clients && dictFind(e->db->watched_keys, k)) ||
                       (blocked && dictFind(e->db->blocking_keys, k));
        }
        getKeysFreeResult(&result);
        if (observed) return 1;
    }
    return 0;
}

static void rpApplySegment(uint32_t s, int count) {
    monotime start = getMonotonicUs();
    fastpathStreamLanesPrepare(rp.nlanes, rp.main_lane);
    rp.seg = s;
    rpRunPhase(RP_APPLY);
    fastpathStreamLanesMerge(rp.nlanes);
    server.stat_repl_parallel_applied += count;
    server.stat_repl_apply_phases++;
    server.stat_repl_lane_apply_usec += getMonotonicUs() - start;
}

/* Main: the plan, segment by segment. Commands of a segment too small for a lane phase run the usual way,
 * where plain SETs are held for one parallel apply phase. C_ERR: the client is gone. */
static int rpRunSegments(client *c) {
    int qlen = c->cmd_queue.len;
    int pos = 0;
    for (int s = 0;; s++) {
        int end = s < rp.nb ? rp.bpos[s] : rp.n;
        if (end - pos >= server.io_threads_parallel_writes_min && !rpObservedInRange(c, pos, end)) {
            if (!rpQueueAt(c, pos, qlen)) return C_OK;
            replApplyFlush(c); /* writes held from earlier segments land first */
            rpApplySegment((uint32_t)s, end - pos);
            rpConsume(c, pos, end);
        } else {
            /* Too few to pay for a lane phase, or one is observed: on main, in order; the lanes skip them. */
            for (int i = pos; i < end; i++) {
                int r = rpRunOnMain(c, i, qlen);
                if (r != C_OK) return r == C_ERR ? C_ERR : C_OK;
            }
        }
        if (s == rp.nb) break;
        server.stat_repl_lane_barriers++;
        int r = rpRunOnMain(c, end, qlen);
        if (r != C_OK) return r == C_ERR ? C_ERR : C_OK;
        pos = end + 1;
        /* A barrier may turn on something that observes writes: the rest goes the usual way. */
        if (!fastpathStreamLanesAllowed()) return C_OK;
    }
    return C_OK;
}

static int rpRun(client *c) {
    int rc = rpRunSegments(c);
    if (rc == C_OK)
        replApplyFlush(c);
    else
        serverAssert(!replApplyPending());
    return rc;
}

int replApplyRunPlan(client *c) {
    if (!rp.ready || rp.c != c) return C_OK;
    rp.ready = 0;
    /* Lanes or threads changed since the read, or the client cannot run commands now: the usual way. */
    if (ProcessingEventsWhileBlocked || !stagedApplyReady() || !fastpathStreamLanesAllowed()) return C_OK;
    int main_lane = stagedMainTakesLane();
    int nlanes = stagedWorkerCount() + main_lane;
    if (nlanes > FP_MAX_LANES) nlanes = FP_MAX_LANES;
    if (nlanes != rp.nlanes || main_lane != rp.main_lane) return C_OK;
    if (c->argc == 0 || c->cmd_queue.off != 0 || c->flag.pending_command || !canParseCommand(c)) return C_OK;
    monotime start = getMonotonicUs();
    if (!rp.parsed) {
        monotime parse_start = getMonotonicUs();
        rpRunPhase(RP_PARSE);
        server.stat_repl_lane_parse_usec += getMonotonicUs() - parse_start;
        c->cmd_queue.len = (uint16_t)(rp.n - 1);
        c->cmd_queue.off = 0;
        c->qb_pos = rp.scan_end;
    }
    rpMerge();
    server.stat_repl_lane_reads++;
    server.stat_repl_lane_read_usec += rp.read_us;
    server.stat_repl_lane_classify_usec += rp.classify_us;
    int rc = rpRun(c);
    server.stat_repl_lane_plan_usec += getMonotonicUs() - start;
    return rc;
}

int replApplyMayDefer(void) {
    return server.replica_parallel_apply && replApplySharded() && stagedApplyReady();
}

int replApplyPending(void) {
    return ra_pending;
}

/* Nothing that observes an individual write may be active: those writes must go through call(). The
 * planned lanes' gate covers the server-wide state. */
static int replApplyAllowed(client *c) {
    if (!server.replica_parallel_apply || !replApplySharded()) return 0;
    if (!c->flag.primary || c->flag.multi || !inMainThread()) return 0;
    if (!stagedApplyReady() || !fastpathStreamParallelAllowed()) return 0;
    if (server.tracking_clients) return 0;
    if (dictSize(c->db->watched_keys) || dictSize(c->db->blocking_keys)) return 0;
    return 1;
}

/* SET key value, or SET key value PXAT ms: the forms a primary propagates for a plain SET. */
static int replApplyEligible(client *c, long long *pxat) {
    if (!ra_set_cmd) ra_set_cmd = lookupCommandByCString("set");
    struct serverCommand *cmd = c->parsed_cmd ? c->parsed_cmd : lookupCommand(c->argv, c->argc);
    if (cmd != ra_set_cmd) return 0;
    *pxat = -1;
    if (c->argc == 3) return 1;
    if (c->argc != 5 || strcasecmp(objectGetVal(c->argv[3]), "pxat")) return 0;
    long long ms;
    if (getLongLongFromObject(c->argv[4], &ms) != C_OK || ms <= 0) return 0;
    *pxat = ms;
    return 1;
}

int replApplyDefer(client *c) {
    long long pxat;
    if (!replApplyAllowed(c) || !replApplyEligible(c, &pxat)) return 0;
    if (ra_pending == 0) {
        ra_main_lane = stagedMainTakesLane();
        ra_nlanes = stagedWorkerCount() + ra_main_lane;
    }

    int shard = getKVStoreIndexForKey(objectGetVal(c->argv[1]));
    int lane = shard % ra_nlanes;
    replApplyLane *l = &ra_lanes[lane];
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 256;
        l->ops = zrealloc(l->ops, sizeof(replApplyOp) * l->cap);
    }
    l->ops[l->len++] = (replApplyOp){.db = c->db,
                                     .argv = c->argv,
                                     .argc = c->argc,
                                     .shard = shard,
                                     .pxat = pxat,
                                     .in_bytes = c->net_input_bytes_curr_cmd};
    ra_pending++;
    hotkeysRecordLookup(c->argv[1], c->db->id, LOOKUP_WRITE);

    /* The lane owns the arguments now. */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len = 0;
    c->argv_len_sum = 0;
    c->commands_processed++;

    if (ra_pending >= 16384) replApplyFlush(c);
    return 1;
}

/* setGenericCommand for the eligible forms, without the reply, the dirty count and the events. */
static void replApplyOne(replApplyOp *op) {
    robj **argv = op->argv;
    robj *key = argv[1];
    robj *val = argv[2];
    repl_apply_key = objectGetVal(key);
    repl_apply_shard = op->shard;
    if (!val->hasembkey) val = tryObjectEncoding(val);
    int flags = lookupKeyWriteWithFlags(op->db, key, LOOKUP_NOHOTKEYS) ? SETKEY_ALREADY_EXIST : SETKEY_DOESNT_EXIST;
    flags |= SETKEY_NO_SIGNAL;
    if (op->pxat >= 0) flags |= SETKEY_KEEPTTL;
    setKey(NULL, op->db, key, &val, flags);
    if (op->pxat >= 0) setExpire(NULL, op->db, key, op->pxat);
    repl_apply_key = NULL;
}

/* The value went to the keyspace; the other arguments are the op's. */
static void replApplyFreeArgs(replApplyOp *op) {
    for (int j = 0; j < op->argc; j++)
        if (j != 2) decrRefCount(op->argv[j]);
    zfree(op->argv);
}

static int commandlogTakes(int type, long long value) {
    return server.commandlog[type].threshold >= 0 && server.commandlog[type].max_len != 0 &&
           value >= server.commandlog[type].threshold;
}

/* After the op ran: whether the commandlog takes it, as call() would decide on main. */
static int replApplyLogged(replApplyOp *op, long long duration) {
    return commandlogTakes(COMMANDLOG_TYPE_SLOW, duration) ||
           commandlogTakes(COMMANDLOG_TYPE_LARGE_REQUEST, (long long)op->in_bytes) ||
           commandlogTakes(COMMANDLOG_TYPE_LARGE_REPLY, 0);
}

/* Keep the op's arguments for main's commandlog entry, with a copy of the value the keyspace now owns. */
static void replApplyKeepForLog(replApplyLane *l, int i, long long duration) {
    replApplyOp *op = &l->ops[i];
    robj *v = lookupKeyWriteWithFlags(op->db, op->argv[1], LOOKUP_NOEFFECTS);
    op->argv[2] = v && v->type == OBJ_STRING ? dupStringObject(v) : createStringObject("", 0);
    op->duration = duration;
    if (l->nlogs == l->logcap) {
        l->logcap = l->logcap ? l->logcap * 2 : 16;
        l->logs = zrealloc(l->logs, sizeof(int) * l->logcap);
    }
    l->logs[l->nlogs++] = i;
}

static void replApplyRunHeld(int lane);

void replApplyRunLane(int tid) {
    if (rp.mode != RP_IDLE) {
        rpPhase(tid);
        return;
    }
    int lane = ra_main_lane ? tid : tid - 1;
    if (lane < 0 || lane >= ra_nlanes) return;
    replApplyRunHeld(lane);
}

/* Lane thread (or main, for a tiny flush): apply lane `lane`'s held SETs. */
static void replApplyRunHeld(int lane) {
    replApplyLane *l = &ra_lanes[lane];
    if (!l->len) return;
    repl_apply_in_lane = 1;
    monotime start = getMonotonicUs();
    int slot_stats = server.cluster_enabled && server.cluster_slot_stats_enabled;
    int histogram = server.latency_tracking_enabled;
    int slowlog = server.commandlog[COMMANDLOG_TYPE_SLOW].threshold >= 0 &&
                  server.commandlog[COMMANDLOG_TYPE_SLOW].max_len != 0;
    if (slot_stats || histogram || slowlog || server.latency_monitor_threshold) {
        /* Per-op time, as call() measures it on the main path, for the per-slot CPU, the latency
         * histogram, the latency monitor and the commandlog. In cluster mode op->shard is the slot and
         * one slot maps to one lane, so no two threads touch the same slot counter. A lane op is
         * sub-microsecond, so per-slot CPU counts nanoseconds and credits whole microseconds as they
         * accrue (a sub-microsecond residue may land on the next op's slot); the final residual rounds
         * up once so a lone op is not free. */
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t prev = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec, carry = 0;
        for (int i = 0; i < l->len; i++) {
            replApplyOp *op = &l->ops[i];
            replApplyOne(op);
            clock_gettime(CLOCK_MONOTONIC, &ts);
            uint64_t now = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            uint64_t ns = now - prev;
            prev = now;
            if (slot_stats) {
                carry += ns;
                clusterSlotStatsAddCpuDurationForSlot(op->shard, carry / 1000);
                carry %= 1000;
            }
            if (histogram) updateCommandLatencyHistogram(&l->hist, (int64_t)ns);
            long long us = (long long)(ns / 1000);
            if (us > l->max_us) l->max_us = us;
            if (replApplyLogged(op, us))
                replApplyKeepForLog(l, i, us);
            else
                replApplyFreeArgs(op);
        }
        if (slot_stats && carry && l->len) clusterSlotStatsAddCpuDurationForSlot(l->ops[l->len - 1].shard, 1);
    } else {
        for (int i = 0; i < l->len; i++) {
            replApplyOp *op = &l->ops[i];
            replApplyOne(op);
            if (replApplyLogged(op, 0))
                replApplyKeepForLog(l, i, 0);
            else
                replApplyFreeArgs(op);
        }
    }
    l->usec = getMonotonicUs() - start;
    repl_apply_in_lane = 0;
}

/* Main, outside phases: fold the held SETs' per-lane latencies into SET's histogram (before it is read). */
void replApplyFoldLaneHistograms(void) {
    for (int i = 0; i < IO_THREADS_MAX_NUM; i++) {
        struct hdr_histogram *h = ra_lanes[i].hist;
        if (!h || h->total_count == 0) continue;
        if (!ra_set_cmd->latency_histogram)
            hdr_init(LATENCY_HISTOGRAM_MIN_VALUE, LATENCY_HISTOGRAM_MAX_VALUE, LATENCY_HISTOGRAM_PRECISION,
                     &ra_set_cmd->latency_histogram);
        hdr_add(ra_set_cmd->latency_histogram, h);
        hdr_reset(h);
    }
}

/* Main, outside phases (CONFIG RESETSTAT): drop the latencies not folded yet. */
void replApplyResetLaneStats(void) {
    for (int i = 0; i < IO_THREADS_MAX_NUM; i++)
        if (ra_lanes[i].hist && ra_lanes[i].hist->total_count) hdr_reset(ra_lanes[i].hist);
}

/* Main: the commandlog entry call() would add for the op, attributed to the primary client c. */
static void replApplyLogOp(client *c, replApplyOp *op) {
    robj **argv = c->argv, **original_argv = c->original_argv;
    int argc = c->argc, original_argc = c->original_argc;
    long long duration = c->duration;
    unsigned long long in = c->net_input_bytes_curr_cmd, out = c->net_output_bytes_curr_cmd;
    uint32_t redact = c->redact_arg_bitmap;
    c->argv = op->argv;
    c->argc = op->argc;
    c->original_argv = NULL;
    c->original_argc = 0;
    c->duration = op->duration;
    c->net_input_bytes_curr_cmd = op->in_bytes;
    c->net_output_bytes_curr_cmd = 0;
    c->redact_arg_bitmap = 0;
    commandlogPushCurrentCommand(c, ra_set_cmd);
    c->argv = argv;
    c->argc = argc;
    c->original_argv = original_argv;
    c->original_argc = original_argc;
    c->duration = duration;
    c->net_input_bytes_curr_cmd = in;
    c->net_output_bytes_curr_cmd = out;
    c->redact_arg_bitmap = redact;
    for (int j = 0; j < op->argc; j++) decrRefCount(op->argv[j]);
    zfree(op->argv);
}

void replApplyFlush(client *c) {
    if (!ra_pending) return;
    client *old = ectx->current_client;
    ectx->current_client = c; /* replicated writes never see a key as expired */
    dbLanePhaseBegin();
    if (ra_pending < server.io_threads_parallel_writes_min) {
        /* Too few to pay for waking the threads and waiting at the phase barrier (a primary's transactions
         * end most segments after a SET or two): main applies every lane itself, lane by lane, as the
         * inline lanes of the unit tests do. Lanes own disjoint tables, so the order between them is free. */
        for (int i = 0; i < ra_nlanes; i++) replApplyRunHeld(i);
        server.stat_repl_apply_inline++;
    } else {
        stagedRunApplyPhase(ra_nlanes > 1 || !ra_main_lane);
    }
    /* Fold each thread's per-table changes into the kvstore-wide bookkeeping. */
    dbLanePhaseEnd();
    ectx->current_client = old;

    monotime usec = 0;
    long long max_us = 0;
    for (int i = 0; i < ra_nlanes; i++) {
        replApplyLane *l = &ra_lanes[i];
        usec += l->usec;
        if (l->max_us > max_us) max_us = l->max_us;
        /* The lane's SET latencies stay in its histogram until INFO latencystats or LATENCY HISTOGRAM reads
         * them (replApplyFoldLaneHistograms) or CONFIG RESETSTAT clears them: folding a histogram walks all
         * its buckets, which cost main more than the SETs themselves at one or two SETs per flush. */
        for (int j = 0; j < l->nlogs; j++) replApplyLogOp(c, &l->ops[l->logs[j]]);
        l->usec = 0;
        l->max_us = 0;
        l->len = 0;
        l->nlogs = 0;
    }
    latencyAddSampleIfNeeded((ra_set_cmd->flags & CMD_FAST) ? "fast-command" : "command", max_us);
    ectx->dirty += ra_pending;
    ectx->stat_numcommands += ra_pending;
    server.stat_repl_parallel_applied += ra_pending;
    server.stat_repl_apply_phases++;
    ra_set_cmd->calls += ra_pending;
    ra_set_cmd->microseconds += usec;
    ra_pending = 0;
    /* The held SETs' bytes were not published to the stream sender while they were unapplied. */
    replStreamSenderPublishAppend(server.primary_repl_offset);
}
