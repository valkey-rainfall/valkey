/* Owner threads: vkevo design D3 ("owner-steer"). One process, one port per worker core.
 *
 * owner-threads N starts N owner threads. Owner i owns a contiguous range of cluster slots (about
 * 16384/N, on 16-slot boundaries) of every database and serves the clients that connect to port
 * owner-port + i. With owner threads the standalone keyspace has one table per 16 slots
 * (OWNER_SHARD_BITS), and every owner owns whole tables, so owners mutate disjoint tables. The
 * kvstores run in concurrent mode (the lane mode of kvstore.c) whenever owners may run, and main
 * folds the per-table deltas back when it stops them.
 *
 * CLUSTER SLOTS / SHARDS / NODES on such a server announce the N owners as cluster nodes, so a
 * cluster-aware client connects to the port of the owner of each key. NIC steering (one ntuple
 * rule per port, utils/vkevo-d3/d3-steer.sh) gives each port its own NIC queue. Port 6379 stays
 * the stock main path for plain clients.
 *
 * Native commands: an owner runs GET, SET, DEL with one key, INCR and PING itself when the key's
 * slot is its own: no barrier and no cross-core hop. Everything else (a key another owner owns,
 * multi-key commands, MULTI/EXEC, scripts, blocking, pub/sub, admin) falls back to the main path:
 * the owner hands the client (socket, parsed input, unsent replies) to main, which runs it as a
 * stock client and hands it back once it is idle again. A client is served by one thread at a
 * time and its replies are appended in command order, so per-connection reply order holds.
 *
 * The world: main holds it from afterSleep to the end of beforeSleep, that is whenever it runs.
 * Owners run "windows" only while main sleeps in epoll_wait. A window is entered with a Dekker
 * handshake (owner: active = 1, full fence, read stop; main: stop = 1, full fence, wait until
 * every active is 0), so main never overlaps a window, and two owners never touch the same key.
 * Every command is atomic with respect to everything that can touch its key, so the history is a
 * sequence of atomic executions in real-time order: per-key linearizable. EXEC runs on main with
 * the world stopped, hence atomic and isolated from owners.
 *
 * Features that need one global order or view (replicas and the backlog, the replica role,
 * maxmemory-clients, MONITOR, keyspace notifications, tracking, WATCH, keys with blocked clients,
 * client pause, loaded modules, the latency monitor, hotkeys, forkless save, throttling,
 * loading) veto the native path while they are active: owners then hand every command to main,
 * which serves it exactly as the stock server does. One process: one replication stream, one
 * AOF, one maxmemory.
 *
 * AOF (appendonly yes, D3veto): a native command's execution unit propagates into the owner's
 * ectx as call() would; the owner keeps the ops in order and main replays them into the AOF buffer
 * (replayLanePropagation) the moment it stops the world, before it runs anything itself. Every
 * write of a key is made by one thread at a time (its owner in a window, or main with the world
 * stopped), so the AOF has each key's writes in execution order. A reply that may show a write the
 * AOF does not have yet (any reply of an owner with unfolded or unflushed ops) is held until main
 * has written the AOF in beforeSleep (fsynced under appendfsync always), the same point where the
 * stock loop sends replies: "a reply never shows a write before the AOF has it" (aofHoldsReplies).
 *
 * maxmemory (D3veto): an owner runs a native step (a read, a command, a table resize) only while
 * main's last reading of used memory, plus what each owner has added since (its own zmalloc counter,
 * measured), plus a bound on what the step can add, stays under maxmemory. Then the stock code
 * would neither evict nor refuse before that command, and native skipping it changes nothing. A
 * step that does not fit goes to main, whose processCommand evicts or refuses as stock does.
 *
 * shortcut: one global stop for every main iteration, and a client that falls back returns to its
 * owner as soon as it is idle (no hysteresis). Fine while main is mostly idle (cluster clients on
 * owner ports); a heavy plain-client load on 6379 or many cross-owner commands stop owners often.
 * Upgrade: per-owner stop for single-owner fallbacks, or a fallback that runs on the owner. */

#include "server.h"
#include "owner.h"
#include "cluster.h"
#include "io_threads.h"
#include "module.h"
#include "bgiteration.h"
#include "throttle.h"
#include "hotkeys.h"
#include "hdr_histogram.h"

#include <pthread.h>
#include <sched.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#endif

#define OWNER_MAX 256
#define OWNER_EVENTS 256
#define OWNER_WAKE_TOKEN UINT64_MAX

enum { OWN_GET, OWN_SET, OWN_DEL, OWN_INCR, OWN_PING, OWN_NCMDS };
static const char *const own_cmd_names[OWN_NCMDS] = {"get", "set", "del", "incr", "ping"};

/* Why an owner handed a client to main (client->owner_why). */
enum { OWN_WHY_RUN = 1, OWN_WHY_CLOSE, OWN_WHY_QBLIMIT, OWN_WHY_IDLE };

/* Native-path vetoes (own_veto). */
#define OWN_VETO_ALL 1
#define OWN_VETO_WRITE 2

/* maxmemory: what a native step may add, as a headroom requirement before it runs (what it really
 * adds is measured afterwards from the owner's own zmalloc counter). */
#define OWN_MM_SLACK (1 << 20)       /* bookkeeping between steps (handoff nodes, fd map), all owners */
#define OWN_MM_READ (256 * 1024)     /* one read and its parse: query buffer growth, argv objects */
#define OWN_MM_CMD (64 * 1024)       /* a command: objects, table entries, a reply block, commandlog entries */
#define OWN_MM_HISTOGRAM (64 * 1024) /* a command's first latency histogram (about 25 KB) */
/* Below this headroom per owner the native path is vetoed: near the limit main serves everything (and
 * evicts or refuses), else a client whose read does not fit would bounce between owner and main. */
#define OWN_MM_TIGHT (2 << 20)

typedef struct ownerPropUnit {
    client *c;  /* whose command it was: main sets its woff (WAITAOF) */
    int first;  /* its ops in pops[first .. first + n) */
    int n;
    int wrap;   /* MULTI/EXEC around the ops, as propagatePendingCommands would have decided */
} ownerPropUnit;

typedef struct ownerCmdStat {
    long long calls, usec, failed;
    struct hdr_histogram *hist;
} ownerCmdStat;

typedef struct ownerError {
    sds msg;
    int flags;
} ownerError;

typedef struct ownerLog {
    void *entry;
    int type;
} ownerLog;

typedef struct owner {
    _Atomic int active; /* inside a window: written by the owner, read by main */
    char pad0[CACHE_LINE_SIZE - sizeof(int)];
    int id;
    int cpu; /* -1: not pinned */
    int slot_lo, slot_hi;
    int port; /* 0: no listener */
    int lfd[CONFIG_BINDADDR_MAX];
    int nlfd;
    pthread_t thread;
    int epfd, wakefd;
    client **fdmap; /* fd -> resident client; written by the owner in a window, by main under the stop */
    int fdmap_cap;
    int nclients;
    pthread_mutex_t inbox_mu;
    list *inbox; /* clients main handed over, not yet registered */
    execContext ectx;
    /* Written by the owner, folded and cleared by main under the stop. */
    ownerCmdStat cs[OWN_NCMDS];
    long long reads, writes, net_in, net_out, outbuf_disconnects;
    ownerError *errors;
    int nerrors, errors_cap;
    ownerLog *logs;
    int nlogs, logs_cap;
    void *pf; /* this owner's key prefetch batch (memory_prefetch.c); NULL below 2 keys */
    /* AOF: the ops native commands propagated since main last folded, in execution order. Written
     * by the owner in a window, replayed and cleared by main under the stop (ownerFoldProp). */
    serverOp *pops;
    int npops, pops_cap;
    ownerPropUnit *units;
    int nunits, units_cap;
    int unflushed;  /* ops since main's last AOF write: this owner's replies wait (ownerReplyOut) */
    int release;    /* main: the AOF has them now, write the held replies (ownerWriteHeld) */
    client **held;  /* clients whose replies wait for the AOF */
    int nheld, held_cap;
    int wake_main;  /* this window left main something to do (ops to fold, memory over) */
    /* maxmemory: this owner's zmalloc counter when the period (one main sleep) began. */
    size_t mm_base;
    unsigned long long mm_period;
    /* Totals for INFO, never cleared. */
    long long native_total, handoffs_total, adopted_total, windows_total;
    long long held_total, mm_refused_total;
} owner;

_Thread_local owner *own_self = NULL;
int owner_accept_home = -1;

static owner *owners = NULL;
static int own_n = 0;
static struct serverCommand *own_cmds[OWN_NCMDS];

static _Atomic int own_stop = 1; /* main holds (or is taking) the world; owners start stopped */
static int own_held = 1;         /* main's view of the same */
static _Atomic int own_veto = OWN_VETO_ALL;
static pthread_mutex_t own_wait_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t own_wait_cv = PTHREAD_COND_INITIALIZER;
static _Atomic int own_waiters = 0;

static pthread_mutex_t own_main_mu = PTHREAD_MUTEX_INITIALIZER;
static list *own_main_inbox = NULL; /* clients owners handed to main */
static int own_main_wakefd = -1;
static list *own_guests = NULL; /* clients with a home owner, served by main now */

static long long own_stops = 0, own_stop_wait_us = 0, own_stop_wait_max_us = 0;
static long long own_takes = 0, own_returns = 0;
static int own_last_veto = OWN_VETO_ALL;
static int own_dormant = 0; /* io-threads > 1: owners sleep, main serves everything */

/* What the owners' next run must do, set by main when it resumes them (ownerResumeWorld). */
static int own_aof = 0;                   /* appendonly: native ops are kept for the AOF */
static int own_mm = 0;                    /* maxmemory: native steps check their headroom */
static long long own_mm_slice = 0;        /* bytes each owner may add this period */
static unsigned long long own_mm_period = 0;
static _Atomic int own_mm_over = 0;       /* an owner went over its slice: every owner sends work to main */
static long long own_aof_ops = 0;         /* main: ops folded into the AOF */

int ownerThreadsEnabled(void) {
    return own_n > 0;
}

/* The client's replies no longer wait on its owner (it leaves the owner, or they are written). */
static void ownerUnhold(owner *o, client *c) {
    int i = c->owner_held - 1;
    if (i < 0) return;
    client *last = o->held[--o->nheld];
    o->held[i] = last;
    last->owner_held = i + 1;
    c->owner_held = 0;
}

static inline void ownerCpuRelax(void) {
#if defined(__aarch64__)
    __asm__ __volatile__("isb" ::: "memory");
#elif defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause" ::: "memory");
#else
    atomic_signal_fence(memory_order_seq_cst);
#endif
}

/* ------------------------------------------------------------------------------------------------
 * Configuration and startup
 * ---------------------------------------------------------------------------------------------- */

/* Startup, before the keyspace is created: owner threads need a standalone, single-threaded main
 * path and a keyspace split by slot. With another layout configured they switch off (and say so),
 * so a config that asks for that layout gets exactly it. */
int ownerValidateConfig(const char **err) {
    if (server.owner_threads == 0) return C_OK;
    const char *off = NULL;
#ifndef __linux__
    off = "owner-threads needs Linux (epoll, eventfd)";
#endif
    if (server.cluster_enabled) {
        /* Cluster nodes already own slots and route by them: nothing for owner threads to add. */
        off = "owner-threads is a standalone mode; cluster-enabled turns it off";
    } else if (server.io_threads_num != 1) {
        off = "owner-threads needs io-threads 1 (the main path is the stock single-threaded loop); io-threads turns it off";
    } else if (server.keyspace_shard_bits_config >= 0 && server.keyspace_shard_bits_config != OWNER_SHARD_BITS) {
        off = "owner-threads splits the keyspace by slot; keyspace-shard-bits turns it off";
    }
    if (off) {
        serverLog(LL_WARNING, "%s", off);
        server.owner_threads = 0;
        return C_OK;
    }
    if (server.owner_port && server.owner_port + server.owner_threads - 1 > 65535) {
        *err = "owner-port + owner-threads - 1 must be a valid port";
        return C_ERR;
    }
    return C_OK;
}

/* The CPU of owner i: the i-th entry (wrapping) of owner-cpulist, "a-b,c,d-e" form. */
static int ownerCpuOf(int i) {
    const char *s = server.owner_cpulist;
    if (!s || !*s) return -1;
    int cpus[1024], n = 0;
    while (*s && n < 1024) {
        char *end;
        long a = strtol(s, &end, 10);
        if (end == s) break;
        long b = a;
        s = end;
        if (*s == '-') {
            b = strtol(s + 1, &end, 10);
            s = end;
        }
        for (long x = a; x <= b && n < 1024; x++) cpus[n++] = (int)x;
        if (*s == ',') s++;
    }
    return n ? cpus[i % n] : -1;
}

static void ownerAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void ownerMainWakeHandler(aeEventLoop *el, int fd, void *privdata, int mask);

void ownerThreadsInit(void) {
    if (server.owner_threads == 0) return;
#ifdef __linux__
    own_n = server.owner_threads;
    owners = zcalloc(sizeof(owner) * own_n);
    own_main_inbox = listCreate();
    own_guests = listCreate();
    for (int k = 0; k < OWN_NCMDS; k++) {
        /* By original name: rename-command moves the same command under another name. */
        own_cmds[k] = lookupCommandByCStringLogic(server.orig_commands, own_cmd_names[k]);
        serverAssert(own_cmds[k] != NULL);
    }
    own_main_wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (own_main_wakefd == -1 ||
        aeCreateFileEvent(server.el, own_main_wakefd, AE_READABLE, ownerMainWakeHandler, NULL) == AE_ERR) {
        serverLog(LL_WARNING, "owner threads: cannot create the main wake event: %s", strerror(errno));
        exit(1);
    }
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        o->id = i;
        o->cpu = ownerCpuOf(i);
        /* Whole tables: owner i owns tables [T*i/N, T*(i+1)/N), each 16384/T slots. */
        const int tables = 1 << OWNER_SHARD_BITS, per = CLUSTER_SLOTS / tables;
        o->slot_lo = (int)((long long)tables * i / own_n) * per;
        o->slot_hi = (int)((long long)tables * (i + 1) / own_n) * per - 1;
        o->inbox = listCreate();
        pthread_mutex_init(&o->inbox_mu, NULL);
        o->fdmap_cap = 1024;
        o->fdmap = zcalloc(sizeof(client *) * o->fdmap_cap);
        o->epfd = epoll_create1(EPOLL_CLOEXEC);
        o->wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        struct epoll_event ev = {.events = EPOLLIN, .data.u64 = OWNER_WAKE_TOKEN};
        if (o->epfd == -1 || o->wakefd == -1 || epoll_ctl(o->epfd, EPOLL_CTL_ADD, o->wakefd, &ev) == -1) {
            serverLog(LL_WARNING, "owner threads: cannot create owner %d's event loop: %s", i, strerror(errno));
            exit(1);
        }
        /* A listener per owner on every bind address, accepted on main (clientAcceptHandler adopts). */
        if (server.owner_port) {
            connListener l = {0};
            l.bindaddr = server.bindaddr;
            l.bindaddr_count = server.bindaddr_count;
            l.port = server.owner_port + i;
            l.ct = connectionTypeTcp();
            if (listenToPort(&l) == C_ERR) {
                serverLog(LL_WARNING, "owner threads: failed listening on port %d for owner %d", l.port, i);
                exit(1);
            }
            o->port = l.port;
            o->nlfd = l.count;
            for (int j = 0; j < l.count; j++) {
                o->lfd[j] = l.fd[j];
                if (aeCreateFileEvent(server.el, l.fd[j], AE_READABLE, ownerAcceptHandler, (void *)(intptr_t)i) ==
                    AE_ERR) {
                    serverLog(LL_WARNING, "owner threads: cannot watch owner %d's listener", i);
                    exit(1);
                }
            }
        }
    }
    serverLog(LL_NOTICE, "owner threads: %d owners, ports %d-%d, slots split %d ways%s", own_n, server.owner_port,
              server.owner_port ? server.owner_port + own_n - 1 : 0, own_n,
              server.owner_adopt_all ? ", every TCP client adopted" : "");
#endif
}

/* ------------------------------------------------------------------------------------------------
 * The world: main stops the owners whenever it runs
 * ---------------------------------------------------------------------------------------------- */

static void ownerWaitWorld(void) {
    for (int i = 0; i < 4096; i++) {
        if (!atomic_load_explicit(&own_stop, memory_order_relaxed)) return;
        ownerCpuRelax();
    }
    pthread_mutex_lock(&own_wait_mu);
    atomic_fetch_add_explicit(&own_waiters, 1, memory_order_seq_cst);
    while (atomic_load_explicit(&own_stop, memory_order_seq_cst)) pthread_cond_wait(&own_wait_cv, &own_wait_mu);
    atomic_fetch_sub_explicit(&own_waiters, 1, memory_order_relaxed);
    pthread_mutex_unlock(&own_wait_mu);
}

static void ownerEnter(owner *o) {
    for (;;) {
        atomic_store_explicit(&o->active, 1, memory_order_relaxed);
        atomic_thread_fence(memory_order_seq_cst);
        if (!atomic_load_explicit(&own_stop, memory_order_relaxed)) break;
        atomic_store_explicit(&o->active, 0, memory_order_release);
        ownerWaitWorld();
    }
    /* Pairs with main's release of own_stop: everything main did before resuming is visible. */
    atomic_thread_fence(memory_order_acquire);
}

static void ownerLeave(owner *o) {
    atomic_store_explicit(&o->active, 0, memory_order_release);
}

/* Main, the world just stopped, before main runs anything: what native commands propagated goes to
 * the AOF (and the WAITAOF offset) in each owner's execution order, as call() would have sent it. */
static void ownerFoldProp(void) {
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        for (int u = 0; u < o->nunits; u++) {
            ownerPropUnit *pu = &o->units[u];
            long long off = server.primary_repl_offset;
            replayLanePropagation(o->pops + pu->first, pu->n, pu->wrap);
            if (server.primary_repl_offset != off) pu->c->woff = server.primary_repl_offset;
            own_aof_ops += pu->n;
        }
        o->nunits = o->npops = 0;
    }
}

/* Main: wait until no owner is inside a window. From here until ownerResumeWorld main has the
 * keyspace, the clients and every owner's state to itself. */
static void ownerStopWorld(void) {
    if (own_held) return;
    atomic_store_explicit(&own_stop, 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    monotime t0 = getMonotonicUs();
    for (int i = 0; i < own_n; i++) {
        unsigned spins = 0;
        while (atomic_load_explicit(&owners[i].active, memory_order_acquire)) {
            ownerCpuRelax();
            /* Yield soon: main may share its CPU with the owner it waits for. */
            if (++spins == 1u << 10) {
                spins = 0;
                sched_yield();
            }
        }
    }
    long long us = (long long)(getMonotonicUs() - t0);
    own_stops++;
    own_stop_wait_us += us;
    if (us > own_stop_wait_max_us) own_stop_wait_max_us = us;
    own_held = 1;
    /* The tables owners touched report their changes to their kvstores now. */
    dbLanePhaseEnd();
    ownerFoldProp();
}

/* maxmemory: the bytes all owners may add before main must look at memory again (0 when an eviction
 * cut short by its time limit goes on: its timer proc on main finishes it first). */
static long long ownerMmHeadroom(void) {
    size_t used = zmalloc_used_memory();
    if (server.stat_last_eviction_exceeded_time || used + OWN_MM_SLACK >= server.maxmemory) return 0;
    return (long long)(server.maxmemory - used - OWN_MM_SLACK);
}

static int ownerComputeVeto(void) {
    /* AOF on is served natively (ownerFoldProp); turning it on (AOF_WAIT_REWRITE) is not. */
    if ((server.aof_state != AOF_OFF && server.aof_state != AOF_ON) || server.repl_backlog ||
        listLength(server.replicas) || server.primary_host)
        return OWN_VETO_ALL;
    if (server.loading || server.async_loading || server.paused_actions || server.import_mode) return OWN_VETO_ALL;
    /* maxmemory is served natively within each owner's headroom (ownerMmCmdFits), down to
     * OWN_MM_TIGHT; client eviction and batched eviction decide on main between commands from state
     * owners change. */
    if (server.maxmemory_clients || (server.maxmemory && server.maxmemory_eviction_batch)) return OWN_VETO_ALL;
    if (server.maxmemory && ownerMmHeadroom() / own_n < OWN_MM_TIGHT) return OWN_VETO_ALL;
    if (listLength(server.monitors) || !trackingIsIdle() || server.notify_keyspace_events) return OWN_VETO_ALL;
    if (server.watching_clients || server.blocked_clients_by_type[BLOCKED_LIST] ||
        server.blocked_clients_by_type[BLOCKED_ZSET] || server.blocked_clients_by_type[BLOCKED_STREAM])
        return OWN_VETO_ALL;
    if (moduleCountLoaded() || moduleObservesWrites() || server.blocked_clients_by_type[BLOCKED_MODULE] ||
        server.busy_module_yield_flags)
        return OWN_VETO_ALL;
    if (server.latency_monitor_threshold || hotkeysEnabled() || bgIteration_iterationActive() ||
        throttle_anyThrottlers())
        return OWN_VETO_ALL;
    if (writeCommandsDeniedByDiskError() != DISK_ERROR_TYPE_NONE || !checkGoodReplicasStatus())
        return OWN_VETO_WRITE;
    return 0;
}

/* Owners look commands up concurrently, so no command table may be mid-rehash while they run: a
 * lookup would take a rehash step (a paused rehash takes none). Tables change only at startup and
 * when a module registers commands. */
static size_t own_settled_modules = (size_t)-1;

static void ownerSettleTable(hashtable *ht) {
    while (ht && hashtableIsRehashing(ht) && !hashtableIsRehashingPaused(ht)) hashtableRehashMicroseconds(ht, 1000);
}

static void ownerSettleCommandTables(void) {
    size_t loaded = moduleCountLoaded();
    if (loaded == own_settled_modules) return;
    own_settled_modules = loaded;
    hashtable *tables[2] = {server.commands, server.orig_commands};
    for (int t = 0; t < 2; t++) {
        ownerSettleTable(tables[t]);
        hashtableIterator iter;
        void *next;
        hashtableInitIterator(&iter, tables[t], 0);
        while (hashtableNext(&iter, &next)) {
            struct serverCommand *cmd = next;
            if (cmd->subcommands_ht) ownerSettleTable(cmd->subcommands_ht);
        }
        hashtableCleanupIterator(&iter);
    }
}

/* Main, resuming: every op folded at this stop is in the AOF now (beforeSleep wrote it, and fsynced
 * it under always), so the replies that waited for it may go: each owner writes its own. If the
 * write did not happen (it cannot under always: a failed write exits), main comes back at once. */
static void ownerReleaseHeld(void) {
    int any = 0;
    for (int i = 0; i < own_n; i++) any |= owners[i].unflushed | owners[i].nheld;
    if (!any) return;
    if (aofHoldsReplies()) flushAppendOnlyFile(0);
    if (aofHoldsReplies()) {
        aeSetDontWait(server.el, 1);
        return;
    }
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        o->unflushed = 0;
        if (!o->nheld) continue;
        o->release = 1;
        uint64_t one = 1;
        if (write(o->wakefd, &one, sizeof(one)) == -1) { /* counter full: a wakeup is pending anyway */
        }
    }
}

/* Main, resuming: each owner's slice of the headroom under maxmemory for the period until main runs
 * again. Used memory is read now, with the owners stopped; each owner measures what it adds. */
static void ownerMmPlan(void) {
    own_mm = server.maxmemory != 0;
    if (!own_mm) return;
    own_mm_slice = ownerMmHeadroom() / own_n;
    own_mm_period++;
    atomic_store_explicit(&own_mm_over, 0, memory_order_relaxed);
}

/* Main, last thing before it sleeps: let the owners run. */
static void ownerResumeWorld(void) {
    if (!own_held) return;
    own_last_veto = ownerComputeVeto();
    atomic_store_explicit(&own_veto, own_last_veto, memory_order_relaxed);
    own_aof = server.aof_state == AOF_ON;
    ownerReleaseHeld();
    ownerMmPlan();
    ownerSettleCommandTables();
    ownerSettleTable(server.commands);
    ownerSettleTable(server.orig_commands);
    dbLanePhaseBegin();
    own_held = 0;
    atomic_store_explicit(&own_stop, 0, memory_order_seq_cst);
    if (atomic_load_explicit(&own_waiters, memory_order_seq_cst)) {
        pthread_mutex_lock(&own_wait_mu);
        pthread_cond_broadcast(&own_wait_cv);
        pthread_mutex_unlock(&own_wait_mu);
    }
}

/* ------------------------------------------------------------------------------------------------
 * Main side: stats, handed-off clients, guests
 * ---------------------------------------------------------------------------------------------- */

static void ownerFoldStats(void) {
    execContext *m = &server.exec_ctx;
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        execContext *e = &o->ectx;
        m->dirty += e->dirty;
        m->stat_numcommands += e->stat_numcommands;
        m->stat_keyspace_hits += e->stat_keyspace_hits;
        m->stat_keyspace_misses += e->stat_keyspace_misses;
        m->stat_expiredkeys += e->stat_expiredkeys;
        m->stat_expiredfields += e->stat_expiredfields;
        m->stat_total_error_replies += e->stat_total_error_replies;
        e->dirty = e->stat_numcommands = e->stat_keyspace_hits = e->stat_keyspace_misses = 0;
        e->stat_expiredkeys = e->stat_expiredfields = e->stat_total_error_replies = 0;
        for (int k = 0; k < OWN_NCMDS; k++) {
            ownerCmdStat *cs = &o->cs[k];
            struct serverCommand *cmd = own_cmds[k];
            if (cs->calls || cs->usec || cs->failed) {
                cmd->calls += cs->calls;
                cmd->microseconds += cs->usec;
                cmd->failed_calls += cs->failed;
                cs->calls = cs->usec = cs->failed = 0;
            }
            if (cs->hist && cs->hist->total_count) {
                if (cmd->latency_histogram == NULL)
                    hdr_init(LATENCY_HISTOGRAM_MIN_VALUE, LATENCY_HISTOGRAM_MAX_VALUE, LATENCY_HISTOGRAM_PRECISION,
                             &cmd->latency_histogram);
                hdr_add(cmd->latency_histogram, cs->hist);
                hdr_reset(cs->hist);
            }
        }
        for (int j = 0; j < o->nerrors; j++) {
            incrementErrorStatsForReply(o->errors[j].msg, sdslen(o->errors[j].msg), o->errors[j].flags);
            sdsfree(o->errors[j].msg);
        }
        o->nerrors = 0;
        for (int j = 0; j < o->nlogs; j++) commandlogPushPrepared(o->logs[j].entry, o->logs[j].type);
        o->nlogs = 0;
        server.stat_net_input_bytes += o->net_in;
        server.stat_net_output_bytes += o->net_out;
        server.stat_total_reads_processed += o->reads;
        server.stat_total_writes_processed += o->writes;
        server.stat_client_outbuf_limit_disconnections += o->outbuf_disconnects;
        o->net_in = o->net_out = o->reads = o->writes = o->outbuf_disconnects = 0;
    }
}

static void ownerGuestAdd(client *c) {
    if (c->owner_guest_node) return;
    listAddNodeTail(own_guests, c);
    c->owner_guest_node = listLast(own_guests);
}

void ownerClientFreed(client *c) {
    if (c->owner_guest_node) {
        listDelNode(own_guests, c->owner_guest_node);
        c->owner_guest_node = NULL;
    }
}

/* Main, world held: a client an owner handed over becomes a stock client on main. */
static void ownerTakeOne(client *c) {
    serverAssert(c->owner_tid == OWNER_IN_TRANSIT);
    c->owner_tid = OWNER_ON_MAIN;
    int why = c->owner_why;
    c->owner_why = 0;
    own_takes++;
    if (c->flag.close_asap) {
        /* The owner asked for the close (output buffer limit); main's queue frees it. */
        c->flag.close_asap = 0;
        freeClientAsync(c);
        return;
    }
    if (why == OWN_WHY_CLOSE) {
        if (server.verbosity <= LL_VERBOSE) {
            sds info = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_VERBOSE, "Client closed connection %s", info);
            sdsfree(info);
        }
        freeClientAsync(c);
        return;
    }
    if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
        freeClientAsync(c);
        return;
    }
    if (c->owner_home >= 0) ownerGuestAdd(c);
    if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
    if (why == OWN_WHY_QBLIMIT) {
        handleQbLimitReached(c);
        return;
    }
    if (why == OWN_WHY_IDLE) return; /* nothing read: main's own read handler takes the input */
    ownerResumeOnMain(c);
}

/* One client at a time: a client's command can run a long script, and main then serves the other
 * clients from inside it (processEventsWhileBlocked), including the ones still queued here. */
static void ownerTakeHandoffs(void) {
    for (;;) {
        pthread_mutex_lock(&own_main_mu);
        listNode *ln = listFirst(own_main_inbox);
        client *c = ln ? listNodeValue(ln) : NULL;
        if (ln) listDelNode(own_main_inbox, ln);
        pthread_mutex_unlock(&own_main_mu);
        if (!c) return;
        ownerTakeOne(c);
    }
}

static void ownerMainWakeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);
    uint64_t v;
    if (read(fd, &v, sizeof(v)) == -1) { /* EAGAIN: already drained */
    }
    /* afterSleep already took the clients; anything newer arrives with the next wakeup. */
    ownerTakeHandoffs();
}

/* Main, world held: give a client to its home owner. */
static void ownerSendToOwner(client *c) {
    serverAssert(own_held && c->owner_tid == OWNER_ON_MAIN && c->owner_home >= 0 && c->owner_home < own_n);
    owner *o = &owners[c->owner_home];
    connSetReadHandler(c->conn, NULL);
    connSetWriteHandler(c->conn, NULL);
    c->owner_tid = o->id;
    pthread_mutex_lock(&o->inbox_mu);
    listAddNodeTail(o->inbox, c);
    pthread_mutex_unlock(&o->inbox_mu);
    uint64_t one = 1;
    if (write(o->wakefd, &one, sizeof(one)) == -1) { /* counter full: a wakeup is pending anyway */
    }
}

/* Returns 1 when an owner took the client, 0 when it stays on main as a guest (it goes home later). */
int ownerAdoptAccepted(client *c) {
    /* Main is inside a long command (busy script, loading): owners stay stopped until it ends, so
     * the new client stays on main, where it can get BUSY or send SCRIPT KILL. Same while the
     * owners sleep under io-threads > 1. */
    if (ProcessingEventsWhileBlocked || own_dormant || (ownerComputeVeto() & OWN_VETO_ALL)) {
        ownerGuestAdd(c);
        return 0;
    }
    ownerSendToOwner(c);
    return 1;
}

/* Main, world held: take a client back from its owner (freeClient), or out of main's own queue of
 * handed-off clients when a nested command (behind a busy script) frees it before its turn. */
void ownerReclaim(client *c) {
    if (c->owner_tid == OWNER_IN_TRANSIT) {
        pthread_mutex_lock(&own_main_mu);
        listNode *ln = listSearchKey(own_main_inbox, c);
        serverAssert(ln != NULL);
        listDelNode(own_main_inbox, ln);
        pthread_mutex_unlock(&own_main_mu);
        c->owner_tid = OWNER_ON_MAIN;
        c->owner_why = 0;
        if (c->flag.close_asap) c->flag.close_asap = 0; /* the owner's request; freeClient follows */
        return;
    }
    serverAssert(own_held && c->owner_tid >= 0 && c->owner_tid < own_n);
    owner *o = &owners[c->owner_tid];
    if (c->owner_onloop) {
        int fd = c->conn->fd;
        epoll_ctl(o->epfd, EPOLL_CTL_DEL, fd, NULL);
        if (fd < o->fdmap_cap && o->fdmap[fd] == c) o->fdmap[fd] = NULL;
        c->owner_onloop = 0;
        c->owner_wantout = 0;
        /* Main writes what it held, after its own AOF write (beforeSleep), as for any client. */
        ownerUnhold(o, c);
        o->nclients--;
    } else {
        pthread_mutex_lock(&o->inbox_mu);
        listNode *ln = listSearchKey(o->inbox, c);
        serverAssert(ln != NULL);
        listDelNode(o->inbox, ln);
        pthread_mutex_unlock(&o->inbox_mu);
    }
    c->owner_tid = OWNER_ON_MAIN;
}

/* A guest may go home when nothing about it needs main: no input or output in flight, no
 * transaction, watch, block, subscription, tracking, monitor or replication role, no pending
 * close, and no reply mode main must honor. */
static int ownerGuestIdle(client *c) {
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command || c->flag.protected) return 0;
    if (c->flag.pending_write || clientHasPendingReplies(c) || connHasWriteHandler(c->conn)) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    if (c->flag.pending_command || c->flag.executing_command || c->argc || c->cmd_queue.off < c->cmd_queue.len)
        return 0;
    if (c->querybuf && sdslen(c->querybuf) > c->qb_pos) return 0;
    if (c->multibulklen || c->bulklen != -1 || c->reqtype) return 0; /* partial request */
    if (c->flag.multi || (c->mstate && listLength(&c->mstate->watched_keys))) return 0;
    if (c->flag.blocked || c->flag.unblocked || c->flag.throttled || c->flag.pubsub || c->flag.tracking) return 0;
    if (c->pubsub_data && clientTotalPubSubSubscriptionCount(c)) return 0;
    if (c->flag.monitor || c->flag.replica || c->flag.primary || c->repl_data || c->slot_migration_job) return 0;
    if (c->flag.lua_debug || c->flag.fastpath || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next)
        return 0;
    if (c->deferred_reply_errors || (c->deferred_reply && listLength(c->deferred_reply))) return 0;
    if (getClientType(c) != CLIENT_TYPE_NORMAL || authRequired(c)) return 0;
    return 1;
}

static void ownerReturnGuests(void) {
    listIter li;
    listNode *ln;
    listRewind(own_guests, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (!ownerGuestIdle(c)) continue;
        listDelNode(own_guests, ln);
        c->owner_guest_node = NULL;
        own_returns++;
        ownerSendToOwner(c);
    }
}

/* Main, world held: client c of owner o becomes a guest on main. */
static void ownerTakeToMain(client *c) {
    ownerReclaim(c);
    own_takes++;
    if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
        freeClientAsync(c);
        return;
    }
    ownerGuestAdd(c);
    if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
}

/* Main inside a long command (processEventsWhileBlocked: busy script, loading, module yield): the
 * owners stay stopped until it ends, so main takes every owner client with input waiting and serves
 * it itself (BUSY replies, SCRIPT KILL, SHUTDOWN). Level-triggered epoll reports the input to main
 * even if the owner thread has the same event in hand; the owner then finds the client gone. */
static void ownerTakeReadable(void) {
    struct epoll_event evs[OWNER_EVENTS];
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        /* Clients main handed over that the owner has not registered yet are in no epoll set at
         * all: take them too, so their input is seen. */
        listNode *ln;
        while ((ln = listFirst(o->inbox))) ownerTakeToMain(listNodeValue(ln));
        int n = epoll_wait(o->epfd, evs, OWNER_EVENTS, 0);
        for (int k = 0; k < n; k++) {
            uint64_t d = evs[k].data.u64;
            if (d == OWNER_WAKE_TOKEN) continue; /* clients waiting to be adopted stay queued */
            int fd = (int)(uint32_t)d;
            client *c = fd < o->fdmap_cap ? o->fdmap[fd] : NULL;
            if (!c || (uint32_t)c->id != (uint32_t)(d >> 32) || !c->owner_onloop) continue;
            ownerTakeToMain(c);
        }
    }
}

/* Main, world held: CONFIG SET io-threads. With more than one IO thread the main path is CF's staged
 * loop, which owner windows must not overlap: the owners sleep (main keeps the world) and main takes
 * all their clients. Back at io-threads 1 they wake, and idle guests go home. */
void ownerIOThreadsChanged(void) {
    if (!own_n) return;
    if (server.io_threads_num > 1 && !own_dormant) {
        own_dormant = 1;
        for (int i = 0; i < own_n; i++) {
            owner *o = &owners[i];
            for (int fd = 0; fd < o->fdmap_cap; fd++)
                if (o->fdmap[fd]) ownerTakeToMain(o->fdmap[fd]);
            listNode *ln;
            while ((ln = listFirst(o->inbox))) ownerTakeToMain(listNodeValue(ln));
        }
        serverLog(LL_NOTICE, "owner threads: asleep while io-threads is %d; main serves their clients",
                  server.io_threads_num);
    } else if (server.io_threads_num == 1 && own_dormant) {
        own_dormant = 0;
        serverLog(LL_NOTICE, "owner threads: awake (io-threads 1)");
    }
}

void ownerAfterSleep(void) {
    if (!own_n) return;
    if (ProcessingEventsWhileBlocked) {
        if (own_held) {
            ownerTakeHandoffs(); /* queued behind the command that is running now */
            ownerTakeReadable();
        }
        return;
    }
    ownerStopWorld();
    ownerFoldStats();
    ownerTakeHandoffs();
}

void ownerBeforeSleepEnd(void) {
    if (!own_n || ProcessingEventsWhileBlocked || own_dormant) return;
    /* While the native path is vetoed, guests stay on main: owners would only hand them back. */
    if (!(ownerComputeVeto() & OWN_VETO_ALL)) ownerReturnGuests();
    ownerResumeWorld();
}

static void ownerAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    int cport, cfd, max = server.max_new_conns_per_cycle;
    char cip[NET_IP_STR_LEN];
    struct ClientFlags flags = {0};
    while (max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (anetRetryAcceptOnError(errno)) continue;
            if (errno != EWOULDBLOCK) serverLog(LL_WARNING, "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE, "Accepted %s:%d (owner %d)", cip, cport, (int)(intptr_t)privdata);
        if (server.tcpkeepalive) anetKeepAlive(NULL, cfd, server.tcpkeepalive);
        owner_accept_home = (int)(intptr_t)privdata;
        acceptCommonHandler(connCreateAccepted(connectionTypeTcp(), cfd, NULL), flags, cip);
        owner_accept_home = -1;
    }
}

/* ------------------------------------------------------------------------------------------------
 * Owner side
 * ---------------------------------------------------------------------------------------------- */

static inline uint64_t ownerTag(client *c) {
    return (uint64_t)(uint32_t)c->conn->fd | ((uint64_t)(uint32_t)c->id << 32);
}

static int ownerWrite(owner *o, client *c);

/* Owner, in a window: leave a client to main. It is off this owner's loop when this returns. */
static int ownerHandOff(owner *o, client *c, int why) {
    int fd = c->conn->fd;
    epoll_ctl(o->epfd, EPOLL_CTL_DEL, fd, NULL);
    if (fd < o->fdmap_cap && o->fdmap[fd] == c) o->fdmap[fd] = NULL;
    c->owner_onloop = 0;
    c->owner_wantout = 0;
    /* Held replies stay in the buffer; main writes them after its AOF write. */
    ownerUnhold(o, c);
    o->nclients--;
    o->handoffs_total++;
    /* A shared query buffer must not leave the thread. */
    if (c->querybuf && c->querybuf == thread_shared_qb) trimClientQueryBuffer(c);
    /* The slots and prefetch marks ownerPrefetch left on the parsed commands are this owner's:
     * main gets them as the parser left them. */
    c->slot = -1;
    c->read_flags &= ~READ_FLAGS_PREFETCHED;
    for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
        c->cmd_queue.cmds[i].slot = -1;
        c->cmd_queue.cmds[i].read_flags &= ~READ_FLAGS_PREFETCHED;
    }
    c->owner_why = why;
    c->owner_tid = OWNER_IN_TRANSIT;
    pthread_mutex_lock(&own_main_mu);
    listAddNodeTail(own_main_inbox, c);
    pthread_mutex_unlock(&own_main_mu);
    return 1;
}

static void ownerRegister(owner *o, client *c) {
    int fd = c->conn->fd;
    if (fd >= o->fdmap_cap) {
        int cap = o->fdmap_cap;
        while (cap <= fd) cap *= 2;
        o->fdmap = zrealloc(o->fdmap, sizeof(client *) * cap);
        memset(o->fdmap + o->fdmap_cap, 0, sizeof(client *) * (cap - o->fdmap_cap));
        o->fdmap_cap = cap;
    }
    o->fdmap[fd] = c;
    o->nclients++;
    o->adopted_total++;
    c->owner_onloop = 1;
    c->owner_wantout = 0;
    struct epoll_event ev = {.events = EPOLLIN, .data.u64 = ownerTag(c)};
    if (epoll_ctl(o->epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        serverLog(LL_WARNING, "owner %d: cannot watch fd %d: %s", o->id, fd, strerror(errno));
        ownerHandOff(o, c, OWN_WHY_CLOSE);
        return;
    }
    if (clientHasPendingReplies(c)) ownerWrite(o, c);
}

static void ownerTakeInbox(owner *o) {
    uint64_t v;
    if (read(o->wakefd, &v, sizeof(v)) == -1) { /* EAGAIN */
    }
    pthread_mutex_lock(&o->inbox_mu);
    list *l = o->inbox;
    o->inbox = listCreate();
    pthread_mutex_unlock(&o->inbox_mu);
    listNode *ln;
    while ((ln = listFirst(l))) {
        client *c = listNodeValue(ln);
        listDelNode(l, ln);
        ownerRegister(o, c);
    }
    listRelease(l);
}

static void ownerSetWantOut(owner *o, client *c, int on) {
    if (c->owner_wantout == on) return;
    struct epoll_event ev = {.events = EPOLLIN | (on ? EPOLLOUT : 0), .data.u64 = ownerTag(c)};
    epoll_ctl(o->epfd, EPOLL_CTL_MOD, c->conn->fd, &ev);
    c->owner_wantout = on;
}

/* Write what the client has, as an IO thread would (the snapshot below stands for main's). */
static void ownerWriteOnce(owner *o, client *c) {
    c->io_last_reply_block = listLast(c->reply);
    if (c->io_last_reply_block)
        c->io_last_bufpos = ((clientReplyBlock *)listNodeValue(c->io_last_reply_block))->used;
    else
        c->io_last_bufpos = (size_t)c->bufpos;
    c->nwritten = 0;
    c->write_flags = 0;
    _writeToClient(c);
    c->io_last_reply_block = NULL;
    c->io_last_bufpos = 0;
    o->writes++;
    ownerPostWriteToClient(c); /* buffer bookkeeping; bytes go to own_self */
    if (c->nwritten > 0) {
        c->net_output_bytes += c->nwritten;
        c->last_interaction = server.unixtime;
    }
}

/* Returns 1 when the client left the owner. */
static int ownerWrite(owner *o, client *c) {
    ownerWriteOnce(o, c);
    if ((c->write_flags & WRITE_FLAGS_WRITE_ERROR) && connGetState(c->conn) != CONN_STATE_CONNECTED)
        return ownerHandOff(o, c, OWN_WHY_CLOSE);
    if (!clientHasPendingReplies(c)) {
        resetLastWrittenBuf(c);
        ownerSetWantOut(o, c, 0);
        if (c->flag.close_after_reply) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    } else {
        ownerSetWantOut(o, c, 1);
    }
    return 0;
}

/* Owner: the client's replies wait until main has written the AOF (ownerReleaseHeld). */
static void ownerHold(owner *o, client *c) {
    if (o->nheld == o->held_cap) {
        o->held_cap = o->held_cap ? o->held_cap * 2 : 64;
        o->held = zrealloc(o->held, sizeof(client *) * o->held_cap);
    }
    o->held[o->nheld++] = c;
    c->owner_held = o->nheld;
    o->held_total++;
    ownerSetWantOut(o, c, 0); /* level-triggered EPOLLOUT would fire on and on */
}

/* Returns 1 when the client left the owner. A reply of an owner with ops main has not written to
 * the AOF yet may show one of their writes (GET after SET of the same key), so it waits; replies
 * already waiting keep their order behind which the new ones queue. */
static int ownerReplyOut(owner *o, client *c) {
    if (c->owner_held) return 0;
    if (o->unflushed) {
        ownerHold(o, c);
        return 0;
    }
    return ownerWrite(o, c);
}

/* Owner, first thing in a window after main released them: write the held replies. */
static void ownerWriteHeld(owner *o) {
    o->release = 0;
    int n = o->nheld;
    o->nheld = 0;
    for (int i = 0; i < n; i++) {
        client *c = o->held[i];
        c->owner_held = 0;
        if (ownerWrite(o, c)) o->wake_main = 1; /* it left: main takes it */
    }
}

/* Owner: keep what the native command's execution unit propagated, for main to replay in order
 * (ownerFoldProp), as fastpath.c's fpLaneTakeProp does for a lane. */
static void ownerTakeProp(owner *o, client *c) {
    serverOpArray *oa = &ectx->also_propagate;
    int n = oa->numops;
    if (!own_aof) {
        serverOpArrayFree(oa);
        return;
    }
    if (o->npops + n > o->pops_cap) {
        while (o->npops + n > o->pops_cap) o->pops_cap = o->pops_cap ? o->pops_cap * 2 : 256;
        o->pops = zrealloc(o->pops, sizeof(serverOp) * o->pops_cap);
    }
    if (o->nunits == o->units_cap) {
        o->units_cap = o->units_cap ? o->units_cap * 2 : 256;
        o->units = zrealloc(o->units, sizeof(ownerPropUnit) * o->units_cap);
    }
    memcpy(o->pops + o->npops, oa->ops, sizeof(serverOp) * n);
    o->units[o->nunits++] = (ownerPropUnit){
        .c = c, .first = o->npops, .n = n, .wrap = n > 1 && !(c->cmd->flags & CMD_TOUCHES_ARBITRARY_KEYS)};
    o->npops += n;
    oa->numops = 0; /* the ops' argv now belong to pops */
    o->unflushed = 1;
    o->wake_main = 1;
}

/* ------------------------------------------------------------------------------------------------
 * Owner side: maxmemory headroom
 * ---------------------------------------------------------------------------------------------- */

/* What this owner added since the period began (negative when it freed more), or LLONG_MAX when
 * the thread has no allocation counter of its own (then nothing runs natively under maxmemory). */
static long long ownerMmGrowth(owner *o) {
    size_t now;
    if (!zmalloc_thread_used_memory(&now)) return LLONG_MAX;
    return (long long)(now - o->mm_base);
}

/* Whether a step that may add `need` bytes fits this owner's slice. */
static int ownerMmFits(owner *o, size_t need) {
    if (atomic_load_explicit(&own_mm_over, memory_order_relaxed)) return 0;
    long long g = ownerMmGrowth(o);
    return g != LLONG_MAX && g + (long long)need <= own_mm_slice;
}

/* Owner: after a read and its parse, which can allocate more than any bound before it (a big
 * argument's buffer). Over the slice: every owner sends its work to main until main has looked. */
static void ownerMmAfterRead(owner *o) {
    long long g = ownerMmGrowth(o);
    if (g == LLONG_MAX || g > own_mm_slice) {
        atomic_store_explicit(&own_mm_over, 1, memory_order_relaxed);
        o->wake_main = 1;
    }
}

/* The bytes each of this owner's growable arrays can take when it doubles. */
static size_t ownerArraysBytes(owner *o) {
    return sizeof(serverOp) * (size_t)o->pops_cap + sizeof(ownerPropUnit) * (size_t)o->units_cap +
           sizeof(client *) * (size_t)o->held_cap + sizeof(ownerError) * (size_t)o->errors_cap +
           sizeof(ownerLog) * (size_t)o->logs_cap;
}

/* The length of the string a GET (or SET ... GET) returns, looked up in this owner's own table. */
static size_t ownerStoredLen(client *c) {
    void *found;
    if (!kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(c->slot), objectGetVal(c->argv[1]), &found)) return 0;
    robj *v = found;
    return objectGetType(v) == OBJ_STRING ? stringObjectLen(v) : 0;
}

/* Whether native command `kind` fits: a bound on what it can add (the value, objects and entries, the
 * reply and a reply block, commandlog entries, a first latency histogram, the ops kept for the AOF and
 * a lazy-expire DEL's, an array doubling) on top of what the owner added so far. */
static int ownerMmCmdFits(owner *o, client *c, int kind) {
    size_t b = OWN_MM_CMD + 2 * c->argv_len_sum + 64 * (size_t)c->argc + 2 * ownerArraysBytes(o);
    if (server.latency_tracking_enabled && !o->cs[kind].hist) b += OWN_MM_HISTOGRAM;
    if (own_aof) b += 4 * (c->argv_len_sum + 32 * (size_t)c->argc + 64);
    int returns_value = kind == OWN_GET;
    if (kind == OWN_SET)
        for (int j = 3; j < c->argc && !returns_value; j++)
            returns_value = sdsEncodedObject(c->argv[j]) && !strcasecmp(objectGetVal(c->argv[j]), "get");
    if (returns_value) b += ownerStoredLen(c);
    return ownerMmFits(o, b);
}

/* hashtableResizeAllowed / hashtableCollectionResizeAllowed on an owner thread. Under maxmemory every
 * resize asks (hashtableSetResizeAskAlways); one that does not fit is skipped, as a forbidden one is. */
int ownerResizeAllowed(size_t more_mem) {
    if (!own_mm) return 1;
    return ownerMmFits(own_self, more_mem);
}

/* The cluster slot of a native candidate's key (GET, SET, DEL with one key, INCR), or -1. */
static int ownerKeySlot(struct serverCommand *cmd, robj **argv, int argc, int read_flags) {
    if (!cmd || (read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) return -1;
    serverCommandProc *p = cmd->proc;
    if (p != getCommand && p != setCommand && p != incrCommand && (p != delCommand || argc != 2)) return -1;
    sds key = objectGetVal(argv[1]);
    return (int)keyHashSlot(key, (int)sdslen(key));
}

/* Owner, before the client's current command: hash each command's key once and keep its slot on
 * the command (c->slot, p->slot) for ownerNativeKind and the keyspace lookups
 * (getKVStoreIndexUsingCachedSlot), and prefetch the keys of the current command and the queue
 * behind it into this owner's batch until the batch is full, as fastpath.c's fpPrefetchRange does.
 * The commands walked are marked READ_FLAGS_PREFETCHED; the first unmarked one starts the next
 * batch. Only keys in this owner's slots are looked up: other owners change their tables meanwhile. */
static void ownerPrefetch(owner *o, client *c) {
    if (c->read_flags & READ_FLAGS_PREFETCHED) return;
    c->read_flags |= READ_FLAGS_PREFETCHED;
    if (c->slot < 0) c->slot = ownerKeySlot(c->parsed_cmd, c->argv, c->argc, c->read_flags);
    cmdQueue *q = &c->cmd_queue;
    if (q->off >= q->len) return; /* one command: nothing to overlap */
    if (prefetchLaneBatchSize(o->pf) != server.prefetch_batch_max_size) {
        prefetchLaneBatchFree(o->pf);
        o->pf = prefetchLaneBatchCreate();
        prefetchLaneBatchUse(o->pf); /* this thread's prefetchBatch* calls use it, never main's */
    }
    if (!o->pf) return;
    getKeysResult result;
    initGetKeysResult(&result);
    int room = 1;
    if (c->slot >= o->slot_lo && c->slot <= o->slot_hi)
        room = prefetchBatchAddCommand(c->parsed_cmd, c->argv, c->argc, c->db, OWNER_TABLE_OF_SLOT(c->slot), &result);
    for (int i = q->off; i < q->len && room; i++) {
        parsedCommand *p = &q->cmds[i];
        p->read_flags |= READ_FLAGS_PREFETCHED;
        if (p->slot < 0) p->slot = ownerKeySlot(p->cmd, p->argv, p->argc, p->read_flags);
        if (p->slot >= o->slot_lo && p->slot <= o->slot_hi)
            room = prefetchBatchAddCommand(p->cmd, p->argv, p->argc, c->db, OWNER_TABLE_OF_SLOT(p->slot), &result);
    }
    getKeysFreeResult(&result);
    prefetchBatchRun();
    prefetchBatchReset();
}

/* Which native command this is, or -1 when main must run it. */
static int ownerNativeKind(owner *o, client *c) {
    struct serverCommand *cmd = c->parsed_cmd;
    if (!cmd || (c->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) return -1;
    int veto = atomic_load_explicit(&own_veto, memory_order_relaxed);
    if (veto & OWN_VETO_ALL) return -1;
    serverCommandProc *p = cmd->proc;
    int kind;
    if (p == getCommand)
        kind = OWN_GET;
    else if (p == setCommand)
        kind = OWN_SET;
    else if (p == delCommand && c->argc == 2)
        kind = OWN_DEL;
    else if (p == incrCommand)
        kind = OWN_INCR;
    else if (p == pingCommand)
        kind = OWN_PING;
    else
        return -1;
    if ((c->user && (c->user->flags & USER_FLAG_RETIRED)) || authRequired(c) || !ACLUserUnrestricted(c->user))
        return -1;
    if (kind != OWN_PING) {
        if (kind != OWN_GET && (veto & OWN_VETO_WRITE)) return -1;
        /* The slot ownerPrefetch computed: the key is hashed once per command. */
        if (c->slot < o->slot_lo || c->slot > o->slot_hi) return -1; /* another owner's key: main */
    }
    /* Under maxmemory the stock server may evict or refuse before any command: main decides that
     * once the command does not fit this owner's headroom. */
    if (own_mm && !ownerMmCmdFits(o, c, kind)) {
        o->mm_refused_total++;
        return -1;
    }
    return kind;
}

/* The part of processCommand() and call() that applies to a native command. */
static void ownerCall(owner *o, client *c, int kind) {
    struct serverCommand *cmd = c->parsed_cmd;
    c->cmd = c->lastcmd = c->realcmd = cmd;
    ectx->current_client = c;
    ectx->executing_client = c;
    ectx->in_call++;
    long long errors = ectx->stat_total_error_replies;
    c->flag.force_aof = 0;
    c->flag.force_repl = 0;
    c->flag.prevent_prop = 0;
    c->redact_arg_bitmap = 0;
    monotime start = getMonotonicUs();
    enterExecutionUnit(0, 0);
    c->flag.executing_command = 1;
    c->flag.buffered_reply = 0;
    c->flag.keyspace_notified = 0;
    long long dirty = ectx->dirty;

    cmd->proc(c);

    exitExecutionUnit();
    c->flag.executing_command = 0;
    ustime_t duration = (ustime_t)(getMonotonicUs() - start);
    /* Propagation as call() decides it; the ops (and a lazy-expire DEL's) are kept for the AOF. The
     * veto keeps every other target (replicas, a backlog, slot export) off. */
    dirty = ectx->dirty - dirty;
    if (!c->flag.prevent_prop) {
        int flags = dirty > 0 ? (PROPAGATE_AOF | PROPAGATE_REPL) : PROPAGATE_NONE;
        if (c->flag.force_repl) flags |= PROPAGATE_REPL;
        if (c->flag.force_aof) flags |= PROPAGATE_AOF;
        if (c->flag.prevent_repl_prop || c->flag.module_prevent_repl_prop) flags &= ~PROPAGATE_REPL;
        if (c->flag.prevent_aof_prop || c->flag.module_prevent_aof_prop) flags &= ~PROPAGATE_AOF;
        if (flags != PROPAGATE_NONE) alsoPropagate(c->db->id, c->argv, c->argc, flags, -1);
    }
    c->flag.force_aof = 0;
    c->flag.force_repl = 0;
    c->flag.prevent_prop = 0;
    if (ectx->also_propagate.numops) ownerTakeProp(o, c);
    ownerCmdStat *cs = &o->cs[kind];
    cs->calls++;
    cs->usec += duration;
    if (ectx->stat_total_error_replies > errors) cs->failed++;
    if (server.latency_tracking_enabled) updateCommandLatencyHistogram(&cs->hist, duration * 1000);
    c->duration = duration;
    void *entries[3];
    int types[3];
    int n = commandlogPrepareCurrentCommand(c, cmd, entries, types);
    for (int i = 0; i < n; i++) {
        if (o->nlogs == o->logs_cap) {
            o->logs_cap = o->logs_cap ? o->logs_cap * 2 : 8;
            o->logs = zrealloc(o->logs, sizeof(ownerLog) * o->logs_cap);
        }
        o->logs[o->nlogs++] = (ownerLog){.entry = entries[i], .type = types[i]};
    }
    c->duration = 0;
    c->commands_processed++;
    ectx->stat_numcommands++;
    o->native_total++;
    ectx->in_call--;
    ectx->executing_client = NULL;
}

/* ownerRunParsedInput() callback: run the client's current command here if it is native. */
static int ownerTryNative(client *c) {
    owner *o = own_self;
    ownerPrefetch(o, c);
    int kind = ownerNativeKind(o, c);
    if (kind < 0) return 0;
    ownerCall(o, c, kind);
    commandProcessed(c);
    ectx->current_client = NULL;
    return 1;
}

/* Error replies of native commands: the counts are kept here, the per-prefix stats fold on main. */
void ownerErrorReply(client *c, const char *s, size_t len, int flags) {
    owner *o = own_self;
    if (flags & ERR_REPLY_FLAG_NO_STATS_UPDATE) {
        for (int k = 0; k < OWN_NCMDS; k++)
            if (own_cmds[k] == c->realcmd) o->cs[k].failed++;
        return;
    }
    ectx->stat_total_error_replies++;
    if (o->nerrors == o->errors_cap) {
        o->errors_cap = o->errors_cap ? o->errors_cap * 2 : 8;
        o->errors = zrealloc(o->errors, sizeof(ownerError) * o->errors_cap);
    }
    o->errors[o->nerrors++] = (ownerError){.msg = sdsnewlen(s, len < 64 ? len : 64), .flags = flags};
}

void ownerCountOutbufDisconnect(void) {
    own_self->outbuf_disconnects++;
}

void ownerCountNetOut(long long n) {
    own_self->net_out += n;
}

/* Returns 1 when the client left the owner. */
static int ownerRead(owner *o, client *c) {
    if (c->flag.close_asap) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    /* Vetoed: main serves the client exactly as the stock server, reads included (query buffer
     * accounting, client eviction, pause), so leave the input unread. */
    if (atomic_load_explicit(&own_veto, memory_order_relaxed) & OWN_VETO_ALL)
        return ownerHandOff(o, c, OWN_WHY_IDLE);
    if (!canParseCommand(c)) return ownerHandOff(o, c, OWN_WHY_RUN);
    /* Under maxmemory a read must fit the headroom too (query buffer, argv objects; a big argument's
     * buffer is allocated when its header is parsed: ownerMmAfterRead). Otherwise main reads it, as
     * the stock server; a slice this small is the OWN_MM_TIGHT veto, which keeps the client there. */
    if (own_mm && !ownerMmFits(o, OWN_MM_READ)) {
        o->mm_refused_total++;
        return ownerHandOff(o, c, OWN_WHY_IDLE);
    }
    c->read_flags = authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    readToQueryBuf(c);
    if (c->nread <= 0) {
        if (c->nread == 0 || connGetState(c->conn) != CONN_STATE_CONNECTED) return ownerHandOff(o, c, OWN_WHY_CLOSE);
        trimClientQueryBuffer(c); /* EAGAIN: give back the shared query buffer */
        return 0;
    }
    o->reads++;
    o->net_in += c->nread;
    c->net_input_bytes += c->nread;
    c->last_interaction = server.unixtime;
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) return ownerHandOff(o, c, OWN_WHY_QBLIMIT);

    ectx->cmd_time_snapshot = mstime();
    parseInputBuffer(c);
    prepareCommandQueue(c);
    if (own_mm) ownerMmAfterRead(o);
    int to_main = ownerRunParsedInput(c, ownerTryNative);
    trimCommandQueue(c);
    trimClientQueryBuffer(c);
    if (c->flag.close_asap) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    if (to_main) {
        /* Send the native replies before main appends its own; main writes what is left. Replies
         * that wait for the AOF stay: main writes them after its AOF write. */
        if (clientHasPendingReplies(c) && !c->owner_held && !o->unflushed) ownerWriteOnce(o, c);
        return ownerHandOff(o, c, OWN_WHY_RUN);
    }
    if (clientHasPendingReplies(c)) return ownerReplyOut(o, c);
    return 0;
}

static void *ownerThreadMain(void *arg) {
    owner *o = arg;
    char name[32];
    snprintf(name, sizeof(name), "owner_%d", o->id);
    valkey_set_thread_title(name);
    if (o->cpu >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(o->cpu, &set);
        if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0)
            serverLog(LL_WARNING, "owner %d: cannot pin to CPU %d", o->id, o->cpu);
    }
    own_self = o;
    ectx = &o->ectx;
    ioThreadsMarkOwnerThread();
    initSharedQueryBuf();

    struct epoll_event evs[OWNER_EVENTS];
    for (;;) {
        int n = epoll_wait(o->epfd, evs, OWNER_EVENTS, -1);
        if (n < 0) {
            if (errno != EINTR) serverLog(LL_WARNING, "owner %d: epoll_wait: %s", o->id, strerror(errno));
            continue;
        }
        ownerEnter(o);
        o->windows_total++;
        o->wake_main = 0;
        if (o->release) ownerWriteHeld(o);
        if (own_mm && o->mm_period != own_mm_period) {
            /* A new period: what this owner adds is measured from here (it allocates only in windows). */
            if (!zmalloc_thread_used_memory(&o->mm_base)) o->mm_base = 0;
            o->mm_period = own_mm_period;
        }
        hashtableSetResizeAskAlways(own_mm); /* under maxmemory every table allocation asks */
        int handed = 0;
        for (int k = 0; k < n; k++) {
            uint64_t d = evs[k].data.u64;
            if (d == OWNER_WAKE_TOKEN) {
                ownerTakeInbox(o);
                continue;
            }
            int fd = (int)(uint32_t)d;
            client *c = fd < o->fdmap_cap ? o->fdmap[fd] : NULL;
            if (!c || (uint32_t)c->id != (uint32_t)(d >> 32)) continue; /* stale: main took it back */
            uint32_t e = evs[k].events;
            if ((e & EPOLLOUT) && !c->owner_held && ownerWrite(o, c)) {
                handed = 1;
                continue;
            }
            if (e & (EPOLLIN | EPOLLERR | EPOLLHUP)) handed |= ownerRead(o, c);
            /* Main is waiting: the rest are level-triggered and come back with the next epoll_wait. */
            if (atomic_load_explicit(&own_stop, memory_order_relaxed)) break;
        }
        ownerLeave(o);
        if (handed || o->wake_main) {
            uint64_t one = 1;
            if (write(own_main_wakefd, &one, sizeof(one)) == -1) { /* a wakeup is pending anyway */
            }
        }
    }
    return NULL;
}

void ownerThreadsStart(void) {
    for (int i = 0; i < own_n; i++) {
        if (pthread_create(&owners[i].thread, NULL, ownerThreadMain, &owners[i]) != 0) {
            serverLog(LL_WARNING, "owner threads: cannot start owner %d", i);
            exit(1);
        }
    }
}

/* ------------------------------------------------------------------------------------------------
 * CLUSTER topology for cluster-aware clients, and INFO
 * ---------------------------------------------------------------------------------------------- */

static void ownerNodeId(int i, char *buf) {
    snprintf(buf, CLUSTER_NAMELEN + 1, "%.32s%08x", server.runid, (unsigned)i);
}

/* The address to announce: owner-announce-ip, else the one the client reached us on. */
static void ownerAnnounceIp(client *c, char *ip, size_t len) {
    if (server.owner_announce_ip && *server.owner_announce_ip) {
        valkey_strlcpy(ip, server.owner_announce_ip, len);
        return;
    }
    int port;
    if (!c->conn || connAddrSockName(c->conn, ip, len, &port) == -1) valkey_strlcpy(ip, "127.0.0.1", len);
}

static int ownerOfPort(int port) {
    if (!server.owner_port || port < server.owner_port || port >= server.owner_port + own_n) return -1;
    return port - server.owner_port;
}

int ownerClusterCommand(client *c) {
    if (!own_n || !server.owner_port || c->argc < 2) return 0;
    const char *sub = objectGetVal(c->argv[1]);
    char ip[NET_IP_STR_LEN], id[CLUSTER_NAMELEN + 1];
    ownerAnnounceIp(c, ip, sizeof(ip));
    int myport = 0;
    char lip[NET_IP_STR_LEN];
    if (c->conn) connAddrSockName(c->conn, lip, sizeof(lip), &myport);
    int me = ownerOfPort(myport);
    if (!strcasecmp(sub, "slots") && c->argc == 2) {
        addReplyArrayLen(c, own_n);
        for (int i = 0; i < own_n; i++) {
            owner *o = &owners[i];
            ownerNodeId(i, id);
            addReplyArrayLen(c, 3);
            addReplyLongLong(c, o->slot_lo);
            addReplyLongLong(c, o->slot_hi);
            addReplyArrayLen(c, 4);
            addReplyBulkCString(c, ip);
            addReplyLongLong(c, o->port);
            addReplyBulkCBuffer(c, id, CLUSTER_NAMELEN);
            addReplyMapLen(c, 0);
        }
        return 1;
    }
    if (!strcasecmp(sub, "shards") && c->argc == 2) {
        addReplyArrayLen(c, own_n);
        for (int i = 0; i < own_n; i++) {
            owner *o = &owners[i];
            ownerNodeId(i, id);
            addReplyMapLen(c, 2);
            addReplyBulkCString(c, "slots");
            addReplyArrayLen(c, 2);
            addReplyLongLong(c, o->slot_lo);
            addReplyLongLong(c, o->slot_hi);
            addReplyBulkCString(c, "nodes");
            addReplyArrayLen(c, 1);
            addReplyMapLen(c, 7);
            addReplyBulkCString(c, "id");
            addReplyBulkCBuffer(c, id, CLUSTER_NAMELEN);
            addReplyBulkCString(c, "port");
            addReplyLongLong(c, o->port);
            addReplyBulkCString(c, "ip");
            addReplyBulkCString(c, ip);
            addReplyBulkCString(c, "endpoint");
            addReplyBulkCString(c, ip);
            addReplyBulkCString(c, "role");
            addReplyBulkCString(c, "master");
            addReplyBulkCString(c, "replication-offset");
            addReplyLongLong(c, 0);
            addReplyBulkCString(c, "health");
            addReplyBulkCString(c, "online");
        }
        return 1;
    }
    if (!strcasecmp(sub, "nodes") && c->argc == 2) {
        sds s = sdsempty();
        for (int i = 0; i < own_n; i++) {
            owner *o = &owners[i];
            ownerNodeId(i, id);
            s = sdscatprintf(s, "%s %s:%d@%d %s - 0 0 %d connected %d-%d\n", id, ip, o->port, o->port + 10000,
                             i == me ? "myself,master" : "master", i + 1, o->slot_lo, o->slot_hi);
        }
        addReplyVerbatim(c, s, sdslen(s), "txt");
        sdsfree(s);
        return 1;
    }
    if (!strcasecmp(sub, "myid") && c->argc == 2) {
        ownerNodeId(me >= 0 ? me : 0, id);
        addReplyBulkCBuffer(c, id, CLUSTER_NAMELEN);
        return 1;
    }
    if (!strcasecmp(sub, "info") && c->argc == 2) {
        sds s = sdscatprintf(sdsempty(),
                             "cluster_state:ok\r\ncluster_slots_assigned:%d\r\ncluster_slots_ok:%d\r\n"
                             "cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:%d\r\n"
                             "cluster_size:%d\r\ncluster_current_epoch:%d\r\ncluster_my_epoch:%d\r\n",
                             CLUSTER_SLOTS, CLUSTER_SLOTS, own_n, own_n, own_n, me >= 0 ? me + 1 : 0);
        addReplyVerbatim(c, s, sdslen(s), "txt");
        sdsfree(s);
        return 1;
    }
    if (!strcasecmp(sub, "keyslot") && c->argc == 3) {
        sds key = objectGetVal(c->argv[2]);
        addReplyLongLong(c, keyHashSlot(key, (int)sdslen(key)));
        return 1;
    }
    return 0;
}

void ownerInfo(sds *info) {
    long long native = 0, handoffs = 0, adopted = 0, windows = 0, held = 0, mm_refused = 0;
    int resident = 0;
    for (int i = 0; i < own_n; i++) {
        native += owners[i].native_total;
        handoffs += owners[i].handoffs_total;
        adopted += owners[i].adopted_total;
        windows += owners[i].windows_total;
        held += owners[i].held_total;
        mm_refused += owners[i].mm_refused_total;
        resident += owners[i].nclients;
    }
    *info = sdscatprintf(*info,
                         "owner_threads:%d\r\nowner_port:%d\r\nowner_veto:%s\r\nowner_native_commands:%lld\r\n"
                         "owner_handoffs_to_main:%lld\r\nowner_returns_from_main:%lld\r\nowner_adoptions:%lld\r\n"
                         "owner_windows:%lld\r\nowner_resident_clients:%d\r\nowner_guest_clients:%lu\r\n"
                         "owner_world_stops:%lld\r\nowner_world_stop_wait_us:%lld\r\nowner_world_stop_wait_max_us:%lld\r\n",
                         own_n, server.owner_port,
                         own_last_veto == OWN_VETO_ALL ? "all" : own_last_veto == OWN_VETO_WRITE ? "writes" : "none",
                         native, own_takes, own_returns, adopted, windows, resident,
                         own_guests ? listLength(own_guests) : 0UL, own_stops, own_stop_wait_us, own_stop_wait_max_us);
    *info = sdscatprintf(*info, "owner_aof_ops:%lld\r\nowner_held_replies:%lld\r\nowner_maxmemory_to_main:%lld\r\n",
                         own_aof_ops, held, mm_refused);
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        *info = sdscatprintf(*info, "owner%d:port=%d,cpu=%d,slots=%d-%d,clients=%d,native=%lld,handoffs=%lld\r\n", i,
                             o->port, o->cpu, o->slot_lo, o->slot_hi, o->nclients, o->native_total, o->handoffs_total);
    }
}
