/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Replica side of the write lanes (repl_apply.c): the primary's stream through the lane plan versus the
 * plain processInputBuffer path.
 *
 * The fixture is a replica (primary_host set, a primary client that owns a socket) with two lanes: main
 * is lane 0 and IO thread 1 is lane 1, keyspace-shard-bits 4, every staged phase inline on the calling
 * thread (testOnlyStagedSetWorkers, testOnlyStagedSetOn). A read of the primary is driven by hand in two
 * steps, as the staged loop does it: the IO thread's part (readToQueryBuf, parseInputBuffer and, for a
 * planned read, replApplyPlanRead) and main's part (handleReadResult, replApplyRunPlan and then the usual
 * processing of what the plan left). Three executors apply the same bytes:
 *
 *   LANES  READ_FLAGS_STREAM_LANES: rpClassify sorts the commands into lanes and barriers, every segment
 *          runs as an APPLY phase lane by lane, barriers run on main through processPlannedCommand.
 *   HELD   no plan; replica-parallel-apply still holds plain SETs (replApplyDefer) for one APPLY phase.
 *   PLAIN  replica-parallel-apply off: processInputBuffer runs every command on main in stream order.
 *
 * Seeded programs are written as a primary propagates them: writes only, a split MSET as its per-lane
 * parts wrapped in MULTI/EXEC, SELECT between commands and inside MULTI, plain SET with and without
 * PXAT. A local client may WATCH random keys either before the read is classified (the keys are
 * observed when the lane flags are sampled, so rpClassify makes the entries barriers) or between the
 * classification and the plan run (the issue 29 window: rpDemoteObserved demotes them). The final
 * dataset of both databases, the replication offset and the watcher's dirty flag must be identical.
 *
 * Environment: RADIFF_SEEDS (default 1500) seeds; RADIFF_SEED replays one seed and prints the program;
 * RADIFF_FIRST shifts the seed range. */

#include "generated_wrappers.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "poller.h"
#include "repl_apply.h"
#include "server.h"
#include "staged_loop.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
void watchForKey(client *c, robj *key);
int handleReadResult(client *c);
void trimCommandQueue(client *c);
}

static const char *ra_logfile = "/dev/null";

#define LANE_BITS 4

/* Keys: k0 (shard 12), k1 (8), k7 (0) on lane 0; k2 (5), k3 (1), k9 (15) on lane 1. Six distinct shards. */
static const char *ra_keys[] = {"k0", "k1", "k2", "k3", "k7", "k9"};
static const int ra_nkeys = 6;
static const char *ra_members[] = {"m1", "m2", "m3", "m4"};
static const char *ra_fields[] = {"f1", "f2", "f3"};
static const char *ra_values[] = {"1", "2", "10", "a", "bb", "-3", "x7"};
static const char *ra_far_pxat = "4102444800000"; /* 2100-01-01 */
static const char *ra_far_pxat2 = "4102444900000";

typedef std::vector<std::string> Cmd;
typedef std::vector<Cmd> Program;

struct RaRng {
    uint64_t s;
    explicit RaRng(uint64_t seed) : s(seed * 0x9E3779B97F4A7C15ull + 0x7654321ull) {
        if (!s) s = 1;
    }
    uint64_t next() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }
    int below(int n) { return (int)(next() % (uint64_t)n); }
    int keyIdx() { return below(ra_nkeys); }
    const char *key() { return ra_keys[keyIdx()]; }
    const char *member() { return ra_members[below(4)]; }
    const char *field() { return ra_fields[below(3)]; }
    const char *value() { return ra_values[below(7)]; }
};

class ReplApplyTest : public ::testing::Test {
  protected:
    static void newDb(int id) {
        server.db[id] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[id]->id = id;
        server.db[id]->keys = kvstoreCreate(&kvstoreKeysHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[id]->expires = kvstoreCreate(&kvstoreExpiresHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[id]->keys_with_volatile_items =
            kvstoreCreate(&kvstoreExpiresHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[id]->watched_keys = dictCreate(&keylistDictType);
        server.db[id]->blocking_keys = dictCreate(&keylistDictType);
        server.db[id]->blocking_keys_unblock_on_nokey = dictCreate(&keylistDictType);
    }

    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(ra_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.client_max_querybuf_len = 1024ll * 1024 * 1024;
        server.proto_max_bulk_len = 512ll * 1024 * 1024;
        server.maxclients = 1000;
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();

        server.keyspace_shard_bits = LANE_BITS;
        server.dbnum = 2;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *) * 2));
        newDb(0);
        newDb(1);

        server.clients = listCreate();
        server.clients_to_close = listCreate();
        server.clients_pending_write = listCreate();
        server.clients_pending_read = listCreate();
        server.unblocked_clients = listCreate();
        server.ready_keys = listCreate();
        server.tracking_pending_keys = listCreate();
        server.monitors = listCreate();
        server.replicas = listCreate();
        server.pending_push_messages = listCreate();
        server.postponed_clients = listCreate();
        server.clients_waiting_acks = listCreate();
        server.clients_index = raxNew();
        server.clients_timeout_table = raxNew();
        server.errors = raxNew();
        for (int i = 0; i < COMMANDLOG_TYPE_NUM; i++) server.commandlog[i].threshold = -1;

        server.io_threads_num = 2;
        server.io_threads_fast_path = 1;
        server.io_threads_strict_offload = 1;
        server.io_threads_parallel_writes = 1;
        server.io_threads_parallel_writes_min = 1;
        server.replica_parallel_apply = 1;
        server.io_batch_commands = 16;

        /* A replica whose link is up. */
        server.primary_host = sdsnew("primary.example");
        server.primary_port = 1;
        server.repl_state = REPL_STATE_CONNECTED;
        server.repl_serve_stale_data = 1;
        server.repl_replica_ro = 1;

        connTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
        stagedSetLaneRunner(STAGED_PHASE_APPLY, replApplyRunLane); /* as InitServerLast does */
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyFreeIOThreadQueues();
    }

    void SetUp() override {
        testOnlyInitIOThreadQueues();
        ASSERT_EQ(testOnlyStagedStart(), 0);
        testOnlyStagedSetWorkers(1, 1); /* main lane 0, IO thread 1 lane 1 */
        testOnlyStagedSetOn(1);
        server.io_threads_parallel_writes = 1;
        server.io_threads_parallel_writes_min = 1;
        server.replica_parallel_apply = 1;
        ASSERT_EQ(laneOf("k0"), 0);
        ASSERT_EQ(laneOf("k1"), 0);
        ASSERT_EQ(laneOf("k7"), 0);
        ASSERT_EQ(laneOf("k2"), 1);
        ASSERT_EQ(laneOf("k3"), 1);
        ASSERT_EQ(laneOf("k9"), 1);
        emptyDbStructure(server.db, -1, 0, NULL);
        ASSERT_TRUE(fastpathStreamLanesAllowed());
        ASSERT_TRUE(stagedApplyReady());
    }

    void TearDown() override {
        testOnlyStagedSetOn(0);
        testOnlyStagedStop();
        freeClientsInAsyncFreeQueue();
    }

    /* A value as text; a byte outside printable ASCII (SETRANGE pads with zero bytes) is shown as \xNN so
     * the dump has no embedded NUL and prints whole. */
    static std::string sdsStr(sds s) {
        std::string out;
        for (size_t i = 0; i < sdslen(s); i++) {
            unsigned char ch = (unsigned char)s[i];
            if (ch >= 0x20 && ch < 0x7f) {
                out += (char)ch;
            } else {
                char buf[8];
                snprintf(buf, sizeof(buf), "\\x%02x", ch);
                out += buf;
            }
        }
        return out;
    }

    static int laneOf(const char *key) {
        sds k = sdsnew(key);
        int shard = getKVStoreIndexForKey(k);
        sdsfree(k);
        return shard % 2;
    }

    /* The primary client of this replica, owning one end of a socket pair; *peer is the other end. */
    static client *newPrimary(int *peer) {
        int sv[2];
        EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
        conn->state = CONN_STATE_CONNECTED;
        client *c = createClient(conn);
        c->flag.primary = 1;
        c->user = NULL; /* as replicationCreatePrimaryClient: no ACL */
        c->querybuf = sdsempty();
        initClientReplicationData(c);
        c->repl_data->reploff = 0;
        c->repl_data->read_reploff = 0;
        selectDb(c, 0);
        server.primary = c;
        *peer = sv[1];
        return c;
    }

    static void freePrimary(client *c, int peer) {
        close(peer);
        server.primary = NULL;
        c->flag.primary = 0; /* freeClient would cache the primary for a PSYNC otherwise */
        freeClient(c);
    }

    static void send(int peer, const std::string &req) {
        EXPECT_EQ(write(peer, req.data(), req.size()), (ssize_t)req.size());
    }

    enum { MODE_LANES = 0, MODE_HELD = 1, MODE_PLAIN = 2 };

    /* The IO thread's part of one read of the primary, as ioThreadReadQueryFromClient does it for a
     * staged read (READ_FLAGS_DRAIN). Returns 1 when the read was planned for the lanes. */
    static int ioReadStep(client *c, int mode) {
        fastpathStagedPrepareLanes(); /* samples the lane flags and the observer generation */
        int plan = mode == MODE_LANES ? replApplyPlanBegin() : 0;
        int flags = READ_FLAGS_DRAIN | READ_FLAGS_REPLICATED | (plan ? READ_FLAGS_STREAM_LANES : 0);
        c->read_flags = flags | (canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE);
        c->io_read_state = CLIENT_PENDING_IO;
        if (plan) replApplyPlanReset();
        ssize_t total = 0;
        for (int i = 0; i < 25; i++) {
            int full = readToQueryBuf(c);
            if (c->nread <= 0) break;
            total += c->nread;
            if (!full) break;
        }
        if (total > 0) c->nread = total;
        if (c->nread > 0 && !(c->read_flags & READ_FLAGS_DONT_PARSE)) {
            parseInputBuffer(c);
            if (!plan) trimCommandQueue(c);
            prepareCommandQueue(c);
            if (plan) replApplyPlanRead(c);
        }
        c->io_read_state = CLIENT_COMPLETED_IO;
        return plan;
    }

    /* Main's part: the plan first, then whatever it left the usual way. C_ERR: the client is gone. */
    static int mainStep(client *c) {
        c->io_read_state = CLIENT_IDLE;
        if (handleReadResult(c) == C_ERR) return C_ERR;
        if ((c->read_flags & READ_FLAGS_STREAM_LANES) && replApplyRunPlan(c) == C_ERR) return C_ERR;
        EXPECT_FALSE(isParsingError(c));
        if (c->argc > 0 && !c->flag.blocked) c->flag.pending_command = 1;
        return processPendingCommandAndInputBuffer(c);
    }

    static std::string resp(const Cmd &c) {
        std::string out = "*" + std::to_string(c.size()) + "\r\n";
        for (size_t i = 0; i < c.size(); i++) out += "$" + std::to_string(c[i].size()) + "\r\n" + c[i] + "\r\n";
        return out;
    }

    static std::string respProgram(const Program &p) {
        std::string out;
        for (size_t i = 0; i < p.size(); i++) out += resp(p[i]);
        return out;
    }

    static Cmd mk(const char *a, const char *b = NULL, const char *c = NULL, const char *d = NULL, const char *e = NULL,
                  const char *f = NULL, const char *g = NULL) {
        Cmd out;
        const char *w[] = {a, b, c, d, e, f, g};
        for (int i = 0; i < 7; i++)
            if (w[i]) out.push_back(w[i]);
        return out;
    }

    /* MSET of n distinct keys as a primary propagates it: one MSET per lane holding that lane's pairs,
     * wrapped in MULTI/EXEC when more than one lane has a part (fpFeedSplitPropagation). */
    static void splitMset(RaRng &r, Program &p, int n) {
        std::vector<Cmd> parts(2);
        int used[6] = {0, 0, 0, 0, 0, 0};
        for (int i = 0; i < n; i++) {
            int ki = r.keyIdx();
            if (used[ki]) continue;
            used[ki] = 1;
            Cmd &part = parts[laneOf(ra_keys[ki])];
            if (part.empty()) part.push_back("MSET");
            part.push_back(ra_keys[ki]);
            part.push_back(r.value());
        }
        int nparts = (int)!parts[0].empty() + (int)!parts[1].empty();
        if (nparts == 0) return;
        if (nparts > 1) p.push_back(mk("MULTI"));
        for (int l = 0; l < 2; l++)
            if (!parts[l].empty()) p.push_back(parts[l]);
        if (nparts > 1) p.push_back(mk("EXEC"));
    }

    /* One propagated write: the shapes a primary feeds its replicas, no reads, no relative times. */
    static void randomCmd(RaRng &r, Program &p) {
        switch (r.below(32)) {
        case 0:
        case 1:
        case 2: p.push_back(mk("SET", r.key(), r.value())); return;
        case 3: p.push_back(mk("SET", r.key(), r.value(), "PXAT", r.below(2) ? ra_far_pxat : ra_far_pxat2)); return;
        case 4: splitMset(r, p, 2); return;
        case 5: splitMset(r, p, 3); return;
        case 6: p.push_back(mk("MSET", r.key(), r.value(), r.key(), r.value())); return; /* one shard or a barrier */
        case 7: p.push_back(mk("INCR", r.key())); return;
        case 8: p.push_back(mk("INCRBY", r.key(), r.value())); return;
        case 9: p.push_back(mk("APPEND", r.key(), r.value())); return;
        case 10: p.push_back(mk("DEL", r.key(), r.key())); return;
        case 11: p.push_back(mk("UNLINK", r.key())); return;
        case 12: p.push_back(mk("PEXPIREAT", r.key(), ra_far_pxat)); return;
        case 13: p.push_back(mk("PERSIST", r.key())); return;
        case 14: p.push_back(mk("RENAME", r.key(), r.key())); return;
        case 15: p.push_back(mk("SMOVE", r.key(), r.key(), r.member())); return;
        case 16: p.push_back(mk("SADD", r.key(), r.member(), r.member())); return;
        case 17: p.push_back(mk("SREM", r.key(), r.member())); return;
        case 18: p.push_back(mk("SINTERSTORE", r.key(), r.key(), r.key())); return;
        case 19: p.push_back(mk("LPUSH", r.key(), r.value(), r.value())); return;
        case 20: p.push_back(mk("RPUSH", r.key(), r.value())); return;
        case 21: p.push_back(mk("LPOP", r.key())); return;
        case 22: p.push_back(mk("LMOVE", r.key(), r.key(), r.below(2) ? "LEFT" : "RIGHT", r.below(2) ? "LEFT" : "RIGHT")); return;
        case 23: p.push_back(mk("ZADD", r.key(), r.value(), r.member())); return;
        case 24: p.push_back(mk("ZINCRBY", r.key(), "1", r.member())); return;
        case 25: p.push_back(mk("HSET", r.key(), r.field(), r.value())); return;
        case 26: p.push_back(mk("HINCRBY", r.key(), r.field(), "2")); return;
        case 27: p.push_back(mk("SETRANGE", r.key(), r.below(2) ? "0" : "3", r.value())); return;
        case 28: p.push_back(mk("GETDEL", r.key())); return;
        case 29: p.push_back(mk("SELECT", r.below(2) ? "1" : "0")); return;
        case 30: {
            /* A transaction of 1 to 3 writes, sometimes with a SELECT inside. */
            p.push_back(mk("MULTI"));
            int n = 1 + r.below(3);
            for (int i = 0; i < n; i++) {
                if (r.below(4) == 0) p.push_back(mk("SELECT", r.below(2) ? "1" : "0"));
                p.push_back(mk("SET", r.key(), r.value()));
            }
            p.push_back(mk("EXEC"));
            return;
        }
        default: p.push_back(mk("SET", r.key(), r.value(), "PXAT", ra_far_pxat)); return;
        }
    }

    struct Scenario {
        int nrounds;
        std::vector<Program> rounds;
        int watch_mode;              /* 0 none, 1 before the read is classified, 2 after (demotion window) */
        std::vector<int> watch_keys; /* key index + 6 * db */
    };

    static Scenario makeScenario(uint64_t seed) {
        RaRng r(seed);
        Scenario s;
        s.nrounds = 1 + r.below(3);
        for (int ro = 0; ro < s.nrounds; ro++) {
            Program p;
            int n = 1 + r.below(14);
            for (int i = 0; i < n; i++) randomCmd(r, p);
            s.rounds.push_back(p);
        }
        s.watch_mode = r.below(3);
        if (s.watch_mode) {
            int n = 1 + r.below(3);
            for (int i = 0; i < n; i++) s.watch_keys.push_back(r.keyIdx() + 6 * r.below(2));
        }
        return s;
    }

    /* The dataset of both databases, read directly: type, value by type, absolute expire time. */
    static std::string dump() {
        std::string out;
        for (int d = 0; d < 2; d++) {
            serverDb *db = server.db[d];
            out += "db" + std::to_string(d) + " n=" + std::to_string(kvstoreSize(db->keys)) + "\n";
            for (int i = 0; i < ra_nkeys; i++) {
                robj *key = createStringObject(ra_keys[i], strlen(ra_keys[i]));
                robj *o = lookupKeyReadWithFlags(db, key, LOOKUP_NOEFFECTS);
                out += std::string(" ") + ra_keys[i] + ":";
                if (!o) {
                    out += " -\n";
                    decrRefCount(key);
                    continue;
                }
                out += " t" + std::to_string(o->type);
                if (o->type == OBJ_STRING) {
                    robj *dec = getDecodedObject(o);
                    out += " '" + sdsStr((sds)objectGetVal(dec)) + "'";
                    decrRefCount(dec);
                } else if (o->type == OBJ_LIST) {
                    listTypeIterator *li = listTypeInitIterator(o, 0, LIST_TAIL);
                    listTypeEntry entry;
                    out += " [";
                    while (listTypeNext(li, &entry)) {
                        robj *v = listTypeGet(&entry);
                        robj *dec = getDecodedObject(v); /* an integer element is a shared INT object */
                        out += sdsStr((sds)objectGetVal(dec)) + ",";
                        decrRefCount(dec);
                        decrRefCount(v);
                    }
                    listTypeReleaseIterator(li);
                    out += "]";
                } else if (o->type == OBJ_SET) {
                    out += " {";
                    for (int m = 0; m < 4; m++) {
                        sds mem = sdsnew(ra_members[m]);
                        if (setTypeIsMember(o, mem)) out += std::string(ra_members[m]) + ",";
                        sdsfree(mem);
                    }
                    out += "}";
                } else if (o->type == OBJ_ZSET) {
                    out += " z{";
                    for (int m = 0; m < 4; m++) {
                        sds mem = sdsnew(ra_members[m]);
                        double score;
                        if (zsetScore(o, mem, &score) == C_OK) out += std::string(ra_members[m]) + "=" + std::to_string(score) + ",";
                        sdsfree(mem);
                    }
                    out += "}";
                } else if (o->type == OBJ_HASH) {
                    out += " h{";
                    for (int f = 0; f < 3; f++) {
                        sds fld = sdsnew(ra_fields[f]);
                        robj *v = hashTypeGetValueObject(o, fld);
                        if (v) {
                            robj *dec = getDecodedObject(v);
                            out += std::string(ra_fields[f]) + "=" + sdsStr((sds)objectGetVal(dec)) + ",";
                            decrRefCount(dec);
                            decrRefCount(v);
                        }
                        sdsfree(fld);
                    }
                    out += "}";
                }
                out += " x" + std::to_string(getExpire(db, key)) + "\n";
                decrRefCount(key);
            }
        }
        return out;
    }

    struct RunResult {
        std::string dump;
        long long reploff, bytes;
        int dirty;      /* the watcher's dirty_cas, -1 without a watcher */
        int planned;    /* rounds the read was planned for the lanes */
        long long lane_reads, applied, demoted, barriers, rechecks, phases;
    };

    static client *newWatcher(const Scenario &s) {
        if (!s.watch_mode) return NULL;
        client *w = createClient(NULL);
        initClientMultiState(w);
        for (size_t i = 0; i < s.watch_keys.size(); i++) {
            int ki = s.watch_keys[i] % 6, db = s.watch_keys[i] / 6;
            selectDb(w, db);
            robj *k = createStringObject(ra_keys[ki], strlen(ra_keys[ki]));
            watchForKey(w, k);
            decrRefCount(k);
        }
        return w;
    }

    static void freeWatcher(client *w) {
        if (!w) return;
        unwatchAllKeys(w);
        freeClient(w);
    }

    /* Run the scenario on an empty dataset with one executor. */
    static RunResult run(const Scenario &s, int mode) {
        RunResult res;
        emptyDbStructure(server.db, -1, 0, NULL);
        server.replica_parallel_apply = mode == MODE_PLAIN ? 0 : 1;
        long long reads0 = server.stat_repl_lane_reads, applied0 = server.stat_repl_parallel_applied;
        long long demoted0 = server.stat_repl_lane_demoted, barriers0 = server.stat_repl_lane_barriers;
        long long rechecks0 = server.stat_repl_lane_rechecks, phases0 = server.stat_repl_apply_phases;
        int peer;
        client *p = newPrimary(&peer);
        client *w = NULL;
        if (s.watch_mode == 1) w = newWatcher(s);
        res.bytes = 0;
        res.planned = 0;
        for (int ro = 0; ro < s.nrounds; ro++) {
            std::string bytes = respProgram(s.rounds[ro]);
            send(peer, bytes);
            res.bytes += (long long)bytes.size();
            res.planned += ioReadStep(p, mode);
            if (s.watch_mode == 2 && ro == 0) w = newWatcher(s); /* after the sample, before the plan runs */
            EXPECT_EQ(mainStep(p), C_OK) << "mode " << mode << " round " << ro;
            EXPECT_EQ(p->argc, 0) << "mode " << mode << " round " << ro << ": a command is left";
            EXPECT_EQ(p->cmd_queue.off, p->cmd_queue.len);
            EXPECT_EQ(p->flag.multi, 0u) << "mode " << mode << " round " << ro << ": a transaction is open";
        }
        res.dump = dump();
        res.reploff = p->repl_data->reploff;
        res.dirty = w ? (int)w->flag.dirty_cas : -1;
        res.lane_reads = server.stat_repl_lane_reads - reads0;
        res.applied = server.stat_repl_parallel_applied - applied0;
        res.demoted = server.stat_repl_lane_demoted - demoted0;
        res.barriers = server.stat_repl_lane_barriers - barriers0;
        res.rechecks = server.stat_repl_lane_rechecks - rechecks0;
        res.phases = server.stat_repl_apply_phases - phases0;
        freeWatcher(w);
        freePrimary(p, peer);
        freeClientsInAsyncFreeQueue();
        server.replica_parallel_apply = 1;
        return res;
    }

    static std::string show(const Program &p) {
        std::string out;
        for (size_t i = 0; i < p.size(); i++) {
            for (size_t j = 0; j < p[i].size(); j++) out += (j ? " " : "  ") + p[i][j];
            out += "\n";
        }
        return out;
    }

    static std::string showScenario(uint64_t seed, const Scenario &s) {
        std::string out = "seed " + std::to_string(seed) + ": " + std::to_string(s.nrounds) + " rounds, watch mode " +
                          std::to_string(s.watch_mode) + " keys";
        for (size_t i = 0; i < s.watch_keys.size(); i++)
            out += std::string(" db") + std::to_string(s.watch_keys[i] / 6) + ":" + ra_keys[s.watch_keys[i] % 6];
        out += "\n";
        for (int ro = 0; ro < s.nrounds; ro++) out += " round " + std::to_string(ro) + ":\n" + show(s.rounds[ro]);
        return out;
    }

    /* One seed: lanes versus held versus plain. Returns false and prints the program on a difference. */
    static bool checkSeed(uint64_t seed, bool verbose, long long *applied_total, long long *demoted_total) {
        Scenario s = makeScenario(seed);
        RunResult lane = run(s, MODE_LANES);
        RunResult held = run(s, MODE_HELD);
        RunResult plain = run(s, MODE_PLAIN);
        bool ok = lane.dump == plain.dump && held.dump == plain.dump && lane.reploff == plain.reploff &&
                  held.reploff == plain.reploff && lane.dirty == plain.dirty && held.dirty == plain.dirty;
        if (!ok || verbose) {
            std::string msg = showScenario(seed, s);
            msg += " dump lanes:\n" + lane.dump + " dump held:\n" + held.dump + " dump plain:\n" + plain.dump;
            msg += " reploff lanes " + std::to_string(lane.reploff) + " held " + std::to_string(held.reploff) + " plain " +
                   std::to_string(plain.reploff) + " bytes " + std::to_string(plain.bytes) + "\n";
            msg += " dirty lanes " + std::to_string(lane.dirty) + " held " + std::to_string(held.dirty) + " plain " +
                   std::to_string(plain.dirty) + "\n";
            msg += " lanes: planned " + std::to_string(lane.planned) + " lane_reads " + std::to_string(lane.lane_reads) +
                   " applied " + std::to_string(lane.applied) + " demoted " + std::to_string(lane.demoted) + " barriers " +
                   std::to_string(lane.barriers) + " rechecks " + std::to_string(lane.rechecks) + " phases " +
                   std::to_string(lane.phases) + "; held: applied " + std::to_string(held.applied) + " phases " +
                   std::to_string(held.phases) + "\n";
            if (!ok) ADD_FAILURE() << "the lane plan, the held-SET path and the plain path differ\n" << msg;
            else printf("%s", msg.c_str());
        }
        /* Engagement: every read of the lane run was planned and run as a plan; the others never. */
        EXPECT_EQ(lane.planned, s.nrounds) << "seed " << seed << ": a read was not planned";
        EXPECT_EQ(lane.lane_reads, s.nrounds) << "seed " << seed << ": a plan did not run";
        EXPECT_EQ(plain.reploff, plain.bytes) << "seed " << seed << ": not the whole stream was applied";
        EXPECT_EQ(held.lane_reads, 0) << "seed " << seed;
        EXPECT_EQ(plain.lane_reads, 0) << "seed " << seed;
        EXPECT_EQ(plain.applied, 0) << "seed " << seed << ": the plain run applied in a lane phase";
        if (s.watch_mode == 2) {
            EXPECT_EQ(lane.rechecks, 1) << "seed " << seed << ": the first plan must re-check";
        }
        *applied_total += lane.applied;
        *demoted_total += lane.demoted;
        return ok;
    }

    static uint64_t envNum(const char *name, uint64_t dflt) {
        const char *v = getenv(name);
        return v ? strtoull(v, NULL, 10) : dflt;
    }

    /* A fixed stream through the lane plan with an optional WATCH in the demotion window. */
    struct FixedResult {
        std::string dump;
        long long reploff, bytes, applied, demoted, barriers, rechecks;
        int dirty;
    };

    static FixedResult runFixed(const Program &prog, const char *watch_key) {
        FixedResult r;
        emptyDbStructure(server.db, -1, 0, NULL);
        long long applied0 = server.stat_repl_parallel_applied, demoted0 = server.stat_repl_lane_demoted;
        long long barriers0 = server.stat_repl_lane_barriers, rechecks0 = server.stat_repl_lane_rechecks;
        int peer;
        client *p = newPrimary(&peer);
        std::string bytes = respProgram(prog);
        send(peer, bytes);
        r.bytes = (long long)bytes.size();
        EXPECT_EQ(ioReadStep(p, MODE_LANES), 1) << "the read was not planned";
        EXPECT_FALSE(fastpathKeyObserversChanged());
        client *w = NULL;
        if (watch_key) {
            w = createClient(NULL);
            initClientMultiState(w);
            robj *k = createStringObject(watch_key, strlen(watch_key));
            watchForKey(w, k);
            decrRefCount(k);
            EXPECT_TRUE(fastpathKeyObserversChanged()) << "the WATCH must open the issue 29 window";
        }
        EXPECT_EQ(mainStep(p), C_OK);
        EXPECT_EQ(p->argc, 0);
        EXPECT_EQ(p->flag.multi, 0u);
        r.dump = dump();
        r.reploff = p->repl_data->reploff;
        r.applied = server.stat_repl_parallel_applied - applied0;
        r.demoted = server.stat_repl_lane_demoted - demoted0;
        r.barriers = server.stat_repl_lane_barriers - barriers0;
        r.rechecks = server.stat_repl_lane_rechecks - rechecks0;
        r.dirty = w ? (int)w->flag.dirty_cas : -1;
        freeWatcher(w);
        freePrimary(p, peer);
        freeClientsInAsyncFreeQueue();
        return r;
    }

    static std::string valueOf(int db, const char *key) {
        robj *k = createStringObject(key, strlen(key));
        robj *o = lookupKeyReadWithFlags(server.db[db], k, LOOKUP_NOEFFECTS);
        std::string out = "-";
        if (o && o->type == OBJ_STRING) {
            robj *dec = getDecodedObject(o);
            out = sdsStr((sds)objectGetVal(dec));
            decrRefCount(dec);
        }
        decrRefCount(k);
        return out;
    }
};

/* Many seeds: the final dataset, the replication offset and a watcher's dirty flag agree between the lane
 * plan, the held-SET path and the plain path. */
TEST_F(ReplApplyTest, LanePlanMatchesPlainApplyOverRandomStreams) {
    long long applied = 0, demoted = 0;
    uint64_t one = envNum("RADIFF_SEED", 0);
    if (one) {
        EXPECT_TRUE(checkSeed(one, true, &applied, &demoted));
        return;
    }
    uint64_t first = envNum("RADIFF_FIRST", 1);
    uint64_t n = envNum("RADIFF_SEEDS", 1500);
    int failed = 0;
    for (uint64_t seed = first; seed < first + n; seed++) {
        if (!checkSeed(seed, false, &applied, &demoted)) {
            failed++;
            if (failed >= (int)envNum("RADIFF_MAXFAIL", 3)) break;
        }
    }
    EXPECT_EQ(failed, 0) << "replay with RADIFF_SEED=<seed> --gtest_filter=ReplApplyTest.*";
    EXPECT_GT(applied, 0) << "no command of any seed ran in an APPLY phase";
    EXPECT_GT(demoted, 0) << "no seed exercised rpDemoteObserved";
}

/* The plain path is deterministic: the same stream twice gives the same dataset. Guards the oracle. */
TEST_F(ReplApplyTest, PlainPathIsDeterministic) {
    uint64_t n = envNum("RADIFF_SEEDS", 1500) / 10 + 1;
    for (uint64_t seed = 1; seed <= n; seed++) {
        Scenario s = makeScenario(seed);
        RunResult a = run(s, MODE_PLAIN);
        RunResult b = run(s, MODE_PLAIN);
        ASSERT_EQ(a.dump, b.dump) << "seed " << seed;
        ASSERT_EQ(a.reploff, b.reploff) << "seed " << seed;
    }
}

/* Issue 29 on the replica: a key becomes watched after the lane flags were sampled and before the plan
 * runs. Every lane entry that writes it is demoted whole to a barrier and runs on main at its own stream
 * position; the other entries keep their lanes; the offset covers the whole read. */
TEST_F(ReplApplyTest, EntryObservedAfterSampleIsDemotedWholeAtItsPosition) {
    Program prog;
    prog.push_back(mk("SET", "k0", "1"));      /* 0 lane 0, demoted */
    prog.push_back(mk("SET", "k2", "2"));      /* 1 lane 1 */
    prog.push_back(mk("INCR", "k0"));          /* 2 lane 0, demoted */
    prog.push_back(mk("INCR", "k2"));          /* 3 lane 1 */
    prog.push_back(mk("RENAME", "k0", "k3"));  /* 4 barrier: two shards */
    prog.push_back(mk("INCR", "k3"));          /* 5 lane 1, after the barrier */
    prog.push_back(mk("APPEND", "k0", "z"));   /* 6 lane 0, demoted (not a plain SET: once the dirtied
                                                  watcher is unwatched, a plain SET would take the held path) */
    prog.push_back(mk("APPEND", "k0", "y"));   /* 7 lane 0, demoted */

    FixedResult r = runFixed(prog, "k0");
    EXPECT_EQ(r.rechecks, 1) << "the plan must look its keys up again";
    EXPECT_EQ(r.demoted, 4) << "the four k0 entries, each whole";
    EXPECT_EQ(r.barriers, 5) << "the demoted entries and RENAME run on main";
    EXPECT_EQ(r.applied, 3) << "SET k2, INCR k2 and INCR k3 stay on lane 1";
    EXPECT_EQ(r.reploff, r.bytes) << "the offset must cover the whole read";
    EXPECT_EQ(r.dirty, 1) << "a demoted write on main dirties the WATCH";
    EXPECT_EQ(valueOf(0, "k0"), "zy") << "RENAME moved k0 away before the two APPENDs";
    EXPECT_EQ(valueOf(0, "k2"), "3");
    EXPECT_EQ(valueOf(0, "k3"), "3") << "INCR k0 ran before RENAME, INCR k3 after it";
    EXPECT_FALSE(fastpathKeyObserversChanged()) << "the plan re-sampled the observers";

    /* The same stream with no WATCH: nothing is demoted, every lane entry runs in an APPLY phase. */
    FixedResult n = runFixed(prog, NULL);
    EXPECT_EQ(n.rechecks, 0);
    EXPECT_EQ(n.demoted, 0);
    EXPECT_EQ(n.barriers, 1) << "only RENAME";
    EXPECT_EQ(n.applied, 7);
    EXPECT_EQ(n.dump, r.dump) << "demotion changes where entries run, never the result";
}

/* A split MSET arrives as its parts wrapped in MULTI/EXEC. The parts are never separated: inside MULTI
 * every command is a barrier, so EXEC applies all parts on main in one step, and a WATCH that arrives
 * in the demotion window finds nothing of the transaction on a lane. The counter that shows where a
 * command ran is stat_repl_parallel_applied (commands applied in APPLY phases). */
TEST_F(ReplApplyTest, MultiWrappedSplitMsetIsNeverSeparated) {
    Program prog;
    prog.push_back(mk("SET", "k9", "z"));          /* lane 1 */
    prog.push_back(mk("MULTI"));
    prog.push_back(mk("MSET", "k0", "a", "k1", "b")); /* part for lane 0 */
    prog.push_back(mk("MSET", "k2", "c"));            /* part for lane 1 */
    prog.push_back(mk("EXEC"));
    prog.push_back(mk("SET", "k7", "q"));          /* lane 0 */

    FixedResult n = runFixed(prog, NULL);
    EXPECT_EQ(n.applied, 2) << "only SET k9 and SET k7 may run in an APPLY phase";
    EXPECT_EQ(n.barriers, 4) << "MULTI, the two parts and EXEC run on main";
    EXPECT_EQ(n.demoted, 0);
    EXPECT_EQ(n.reploff, n.bytes);
    EXPECT_EQ(valueOf(0, "k0"), "a");
    EXPECT_EQ(valueOf(0, "k1"), "b");
    EXPECT_EQ(valueOf(0, "k2"), "c");
    EXPECT_EQ(valueOf(0, "k7"), "q");
    EXPECT_EQ(valueOf(0, "k9"), "z");

    /* A WATCH on a part's key in the demotion window: there is no lane entry to demote, the parts are
     * still applied together by EXEC on main, and the WATCH is dirtied. */
    FixedResult w = runFixed(prog, "k2");
    EXPECT_EQ(w.rechecks, 1);
    EXPECT_EQ(w.demoted, 0) << "a part inside MULTI was on a lane";
    EXPECT_EQ(w.applied, 2);
    EXPECT_EQ(w.barriers, 4);
    EXPECT_EQ(w.dirty, 1);
    EXPECT_EQ(w.dump, n.dump);
}
