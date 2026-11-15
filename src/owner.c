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
 * Native commands: an owner runs GET, SET, DEL, INCR, MGET, MSET and PING itself when every key's
 * slot is its own (multi-key commands: one slot, as hash tags give): no barrier and no cross-core
 * hop. MULTI/EXEC too, while every queued command is one of those on the owner's keys: the owner
 * queues them and runs EXEC within one window. WATCH/UNWATCH of its own keys too: an owner keeps its
 * own registry of watched keys (multi.c), which only it, or main with the owners stopped, touches;
 * a watcher that comes to main has its keys moved to the dbs' registries (watchMoveToMain) and is a
 * stock watcher from then on. Everything else (a key another owner owns, keys in two slots, any
 * other command inside a transaction, scripts, blocking, pub/sub, admin) falls back to the main
 * path: the owner hands the client (socket, parsed input, transaction so far, unsent replies) to
 * main, which runs it as a stock client and hands it back once it is idle again. A client is served
 * by one thread at a time and its replies are appended in command order, so per-connection reply
 * order holds.
 *
 * The world: main holds it from afterSleep to the end of beforeSleep, that is whenever it runs.
 * Owners run "windows" only while main sleeps in epoll_wait. A window is entered with a Dekker
 * handshake (owner: active = 1, full fence, read stop; main: stop = 1, full fence, wait until
 * every active is 0), so main never overlaps a window, and two owners never touch the same key.
 * Every command is atomic with respect to everything that can touch its key, so the history is a
 * sequence of atomic executions in real-time order: per-key linearizable. An EXEC runs either on
 * main with the world stopped or on the owner of all its keys inside one window: atomic and
 * isolated either way.
 *
 * Features that need one global order or view (the replica role,
 * maxmemory-clients, MONITOR, keyspace notifications, tracking, WATCH through main, keys with
 * blocked clients, client pause, loaded modules, the latency monitor, hotkeys, forkless save,
 * throttling,
 * loading) veto the native path while they are active: owners then hand every command to main,
 * which serves it exactly as the stock server does. One process: one replication stream, one
 * AOF, one maxmemory.
 *
 * Replicas (D3-all-g11-repl): a primary with replicas or a backlog stays native. Each native
 * write's record (formatted on the owner for the AOF, below) is also the replication stream's frame
 * for it, so the owner appends its window's records to the shared replication buffer at the end of
 * the window, on the AOF drain's lock (or main does at its stop), as replicationFeedReplicas would
 * have one by one: offset, SELECT, backlog, replica references and output buffer limits, and each
 * client's WAIT offset. A key's writes are made by one thread at a time and reach the stream in that
 * thread's order, so every key's writes are in order; a reply waits until its write is in the stream
 * (held as for the AOF), so a client never sees a write the stream lacks and its later writes on
 * other owners come after it; main stops the owners and folds their records before anything it runs
 * (a fork for a full sync, PSYNC from the backlog, a replica coming online, active expiry and
 * eviction DELs, cross-owner and multi-slot commands), so a snapshot and the stream after it agree.
 * The repl-stream-sender thread (started as append by owner-repl-sender) writes the replica sockets
 * from what an append publishes: neither main nor an owner writes them on the hot path.
 *
 * AOF (appendonly yes, D3veto): a native command's execution unit propagates into the owner's
 * ectx as call() would; the owner formats the ops into its own AOF records right away, as
 * feedAppendOnlyFile would (D3-all-aof). Under appendfsync everysec or no the owner writes its
 * records to the AOF itself at the end of the window that made them (ownerDrainAof, D3-all-g4-aof):
 * owners take turns on one lock, append to the AOF buffer and flush it as beforeSleep does, and main
 * is not woken for it. Under always (one fsync for every owner's records, not one per window), and
 * whenever main stops the world, main appends each owner's remaining bytes to the AOF buffer the
 * moment it stops the world, before it runs anything itself. Every
 * write of a key is made by one thread at a time (its owner in a window, or main with the world
 * stopped), so the AOF has each key's writes in execution order. A reply that may show a write the
 * AOF does not have yet (any reply of an owner with unwritten ops) is held until that write: the
 * owner's own flush, or main's in beforeSleep (fsynced under appendfsync always), the same point
 * where the stock loop sends replies: "a reply never shows a write before the AOF has it"
 * (aofHoldsReplies).
 * A native EXEC keeps all its queued commands' ops as one unit, in MULTI/EXEC when more than one,
 * as the stock EXEC's execution unit propagates them (D3-all).
 *
 * maxmemory (D3veto): an owner runs a native step (a read, a command, a table resize) only while
 * main's last reading of used memory, plus what each owner has added since (its own zmalloc counter,
 * measured), plus a bound on what the step can add, stays under maxmemory. Then the stock code
 * would neither evict nor refuse before that command, and native skipping it changes nothing. A
 * step that does not fit goes to main, whose processCommand evicts or refuses as stock does.
 * Each owner's share of the headroom is its memory lease for the period (one main sleep): main
 * splits maxmemory minus used memory (less OWN_MM_SLACK) across the owners when it resumes them, and
 * an owner checks only its own allocation counter against its lease, never the global sum.
 *
 * Memory leases (owner-mm-lease yes, the default; D3-all-g11-maxmem): while an owner's lease has room
 * for a whole read batch (OWN_MM_LEASE_ROOM), it batches its reads through io_uring and runs SET bursts
 * under maxmemory too. Each read queued in a batch reserves OWN_MM_READ from the lease until its turn
 * (o->mm_resv), and every check in between (commands, table resizes, the next reads) counts the
 * reservations, so a batched read finds the room the unbatched path checked for right before it. A
 * SET burst checks each SET against the same bound the parser's path checks it against (ownerMmCmdFits)
 * before the SET allocates, and ends at the first one that does not fit: the parser takes it, and
 * ownerNativeKind sends it to main. Below OWN_MM_LEASE_ROOM, and with owner-mm-lease no, the owner
 * takes g10's exact path (reads one at a time, no SET bursts). Bound: as before, between two of main's
 * readings every owner stays within its lease, so used memory stays at or below maxmemory minus
 * OWN_MM_SLACK, except that one read's parse can overshoot its OWN_MM_READ estimate (a big argument);
 * that read sets own_mm_over and every owner sends its work to main until main looks again. Leases add
 * no overshoot of their own.
 *
 * shortcut: one global stop for every main iteration, and a client that falls back returns to its
 * owner as soon as it is idle (no hysteresis). Fine while main is mostly idle (cluster clients on
 * owner ports); a heavy plain-client load on 6379 or many cross-owner commands stop owners often.
 * Upgrade: per-owner stop for single-owner fallbacks, or a fallback that runs on the owner.
 *
 * io_uring (owner-uring yes, the default; D3-all-g8-iouring): epoll still says which sockets are ready,
 * but each owner reads the ready sockets OWN_URING_BATCH at a time with one io_uring_enter on its own
 * ring (recv, MSG_DONTWAIT), runs them in event order, and sends their replies the same way (send of
 * the plain c->buf bytes; replies with reply blocks or value references take the writev path). Every
 * op completes inside the io_uring_enter that submits it (MSG_DONTWAIT: a socket that is not ready
 * fails at once), so nothing is in flight outside a window, main never touches a ring, and handoffs,
 * world stops and ProcessingEventsWhileBlocked see the sockets as the read/write path leaves them.
 * A recv or send that moved no bytes (end of file, an error, a full socket) is redone by the stock
 * read or write, which sets the connection state as before. A read batch's replies go out after the
 * whole batch ran: each was queued only when ownerReplyOut would have written it right away (no
 * unflushed AOF ops then), and one connection's replies stay in order. Main stopping the world ends
 * the window after the batch in hand. Under maxmemory the reads are batched only inside a memory lease
 * with room for the batch (see above); otherwise one at a time, each read's headroom check following
 * the commands before it. With owner-uring-deep N > 0 a client whose last read ran
 * N commands or more is read, run and answered at once, as without io_uring; the default is 0 (D3-all-
 * g9-fold): every read is batched, deep pipelines included (fleet W=32 K1 +4.8%, K2 +2.7% vs 32).
 *
 * D3-all-g9-fold: under appendonly the owners keep the syscall path (reads, replies and held AOF
 * replies, as D3-all-g7-rwchild), unless owner-uring-aof yes: batching there tied K13 (SET p1 + AOF)
 * throughput at fleet W=32 and raised its SET p99 21%. The GET bursts (ownerGetBurst) drain what either
 * path read into the query buffer, in the read's turn of the batch, before the parser. */

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
#include <sys/mman.h>
#endif
#ifdef HAVE_LIBURING
#include <liburing.h>
#endif

#define OWNER_MAX 256
#define OWNER_EVENTS 256
#define OWNER_WAKE_TOKEN UINT64_MAX
#define OWN_RING_MAX 128 /* GET and MGET ring slots: prefetch-batch-max-size's maximum; a power of two */
/* io_uring (owner-uring, D3-all-g8): reads and writes per io_uring_enter. Each read slot has a
 * PROTO_IOBUF_LEN scratch buffer (mmap: only touched pages count, and not toward used_memory). */
#define OWN_URING_BATCH 64

enum {
    OWN_GET,
    OWN_SET,
    OWN_DEL,
    OWN_INCR,
    OWN_PING,
    OWN_MGET,
    OWN_MSET,
    OWN_MULTI, /* from here on: not queueable in a native transaction (ownerTxnNative) */
    OWN_EXEC,
    OWN_DISCARD,
    OWN_WATCH,
    OWN_UNWATCH,
    OWN_NCMDS
};
static const char *const own_cmd_names[OWN_NCMDS] = {"get",  "set",  "del",  "incr",    "ping",  "mget",
                                                     "mset", "multi", "exec", "discard", "watch", "unwatch"};

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
/* Memory leases (D3-all-g11-maxmem): a window batches its reads and runs SET bursts under maxmemory only
 * while this owner's lease has this much room: a full batch's read reservations, twice over. */
#define OWN_MM_LEASE_ROOM (2 * OWN_URING_BATCH * OWN_MM_READ)
/* A SET burst's slow commandlog entries, made after its last SET: per SET (the entry, its key and value). */
#define OWN_MM_SETBURST_LOG 1024
/* The bound factor for the records a step formats (sds doubling of the owner's record buffer): twice when
 * the owner encodes each record twice, compact for the AOF and RESP for the stream (D3-all-g14-fold). */
#define OWN_MM_REC ((size_t)(own_split ? 8 : 4))

typedef struct ownerPropUnit {
    client *c; /* whose command it was: its woff is set when the records are appended (WAITAOF) */
    int n;     /* its ops (INFO owner_aof_ops) */
    int repl;  /* replication offset steps its records take: one per AOF record with the REPL target */
    size_t end; /* where its records end in the owner's aofbuf (its WAIT offset with a backlog) */
    size_t rend; /* D3-all-g14-fold: the same in replbuf, when the stream keeps its own RESP copy (own_split) */
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

/* One read or write of a batch submitted with one io_uring_enter (owner-uring). */
typedef struct ownerUringOp {
    client *c;
    char *buf;   /* read: where the bytes go (the slot's scratch, or the client's own query buffer) */
    size_t len;
    int scratch; /* read: buf is the slot's scratch; the bytes are copied into the query buffer */
    int res;     /* the CQE's result */
} ownerUringOp;

/* D3-all-g11-bigreply: a GET value an owner sends from where it is stored instead of copying it into the
 * reply (ownerRefAdd). Its bytes go out at offset `at` of c->buf, between the "$<len>\r\n" before it and the
 * "\r\n" after it, which are in c->buf. The reference holds the object (refcount); the owner drops it in
 * the window that took it, once the bytes are written or copied into a reply block (ownerRefsMaterialize). */
typedef struct ownerRef {
    robj *o;
    const char *s; /* the value's bytes, as they were when the reference was taken */
    uint32_t len;
    uint32_t at;
} ownerRef;
#define OWN_REF_MAX 256                 /* references per client and window; more values are copied */
#define OWN_REF_BYTES_MAX (1024 * 1024) /* referenced bytes per client and window; more are copied */

/* The size test the GET paths make before they try a reference (ownerRefAdd, kept out of line so the
 * small-value loops stay as they were). */
static inline int ownerRefWanted(size_t len) {
    int min = server.owner_bigreply_min;
    return min > 0 && len >= (size_t)min;
}

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
    dict **watched;     /* per db: the keys this owner's clients WATCH here (multi.c), like db->watched_keys */
    long long watchers; /* clients with keys in it; written by the owner, or by main under the stop */
    /* AOF: the records of the ops native commands propagated since they last went to the AOF buffer,
     * in execution order, formatted by the owner as feedAppendOnlyFile formats them (ownerTakeProp).
     * Written by the owner in a window; appended to the AOF buffer and cleared by the owner at the end
     * of the window (ownerDrainAof) or by main under the stop (ownerFoldProp). The SELECT before the
     * first keyed record depends on the db the AOF stream has selected then, so it is decided there
     * (ownerAofAppend); the owner writes the later ones. */
    sds aofbuf;
    size_t aof_sel_at; /* where in aofbuf the first keyed record starts */
    int aof_first_db;  /* that record's db; -1: no keyed record yet */
    int aof_db;        /* the db selected at the end of aofbuf (once aof_first_db != -1) */
    /* D3-all-g14-fold: the replication stream's own copy of the records, in RESP, kept only while the
     * AOF's records are compact and the stream is on (own_split); the same dbs and SELECTs as aofbuf. */
    sds replbuf;
    size_t repl_sel_at; /* where in replbuf the first keyed record starts */
    ownerPropUnit *units;
    int nunits, units_cap;
    int unflushed;  /* ops not written to the AOF yet: this owner's replies wait (ownerReplyOut) */
    int release;    /* main: the AOF has them now, write the held replies (ownerWriteHeld) */
    client **held;  /* clients whose replies wait for the AOF */
    int nheld, held_cap;
    int wake_main;  /* this window left main something to do (ops to fold, memory over) */
    /* maxmemory: this owner's zmalloc counter when the period (one main sleep) began. */
    size_t mm_base;
    unsigned long long mm_period;
    /* Memory leases: OWN_MM_READ for each read queued in this window's read batch and not run yet
     * (ownerMmFits counts it as used). 0 outside a batch. */
    size_t mm_resv;
    int mm_roomy; /* this window: no maxmemory, or a lease with OWN_MM_LEASE_ROOM to spare (batch, SET bursts) */
    long long mm_lease_windows, mm_exact_windows; /* windows under maxmemory: batched in the lease, or exact */
    /* Totals for INFO, never cleared. */
    long long native_total, handoffs_total, adopted_total, windows_total;
    long long held_total, mm_refused_total;
    long long burst_total, bursts_total; /* GETs answered by bursts, and the bursts (ownerGetBurst) */
    long long setburst_total, setbursts_total; /* SETs answered by SET bursts, and those bursts (ownerSetBurst) */
    /* io_uring (owner-uring): the owner's own ring, used only by the owner thread, inside its windows.
     * Every op is MSG_DONTWAIT and completes inside the io_uring_enter that submits it, so no op is in
     * flight outside a window and main never sees one: the sockets, query buffers and replies are
     * exactly as the read/write path would leave them. */
    int uring;          /* the ring is up */
    char *uring_scratch; /* OWN_URING_BATCH * PROTO_IOBUF_LEN */
    ownerUringOp uops[OWN_URING_BATCH];
    client **wq;        /* clients whose replies go out at the end of the read batch */
    int nwq, wq_cap;
    long long uring_enters, uring_reads, uring_writes; /* INFO, never cleared */
    long long uring_short, uring_redo; /* sends that moved less than asked; ops redone by the syscall path */
    /* D3-all-g11-bigreply: the clients whose replies hold value references (ownerRefAdd), none outside a
     * window; and INFO totals, never cleared. */
    client **refc;
    int nrefc, refc_cap;
    long long ref_replies, ref_bytes, ref_writes, ref_copied, ref_copied_bytes;
#ifdef HAVE_LIBURING
    struct io_uring ring;
#endif
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
/* appendonly with appendfsync everysec or no: each owner writes its own records to the AOF at the end
 * of its window (ownerDrainAof), one owner at a time on own_aof_mu, and its held replies go then; main
 * stops the world only for its own work. Under always main still writes them for every owner at once
 * (ownerFoldProp, then one fsync in beforeSleep). */
static int own_aof_drain = 0;
/* D3-all-g11-repl: a top-level primary with replicas or a replication backlog. Native ops are kept as
 * records for the replication stream too (the same bytes as the AOF's), appended to the shared
 * replication buffer by the owner at the end of its window (ownerDrainAof, on own_aof_mu, as the
 * AOF drain) or by main at its stop (ownerFoldProp), with the replication offset, SELECT state and
 * WAIT offsets replicationFeedReplicas would give. own_keep: records are kept for either stream;
 * own_aof_drain then also covers the replication-only case. */
static int own_repl = 0;
static int own_keep = 0;
static long long own_repl_ops = 0, own_repl_bytes = 0; /* INFO: ops and bytes owners fed the stream */
/* D3-all-g14-fold (g11-binaof x g11-repl): replicas always get RESP frames. own_rec_fmt is the format of
 * the records in each owner's aofbuf: the open incr file's (server.aof_fd_fmt) with the AOF on, else RESP
 * (the stream's). With the AOF's records compact and the stream on (own_split), each owner also writes
 * every record once more in RESP into its replbuf, which feeds the stream; otherwise the stream and the
 * AOF share aofbuf's bytes. All three change only while the owners are stopped (ownerResumeWorld), as
 * aof_fd_fmt does (a rewrite starting, appendonly on), after main folded every owner's records. */
static int own_rec_fmt = AOF_FMT_RESP;
static int own_split = 0;
static long long own_split_ops = 0; /* INFO: ops the owners encoded twice (compact for the AOF, RESP for the stream) */
static pthread_mutex_t own_aof_mu = PTHREAD_MUTEX_INITIALIZER;
static long long own_aof_drains = 0;      /* owner AOF writes (under own_aof_mu) */
#define OWN_AOF_FSYNC_BYTES (32LL << 20) /* everysec: an owner drain starts an fsync once this much is unsynced */
static int own_mm = 0;                    /* maxmemory: native steps check their headroom */
static long long own_mm_slice = 0;        /* bytes each owner may add this period */
static unsigned long long own_mm_period = 0;
static _Atomic int own_mm_over = 0;       /* an owner went over its slice: every owner sends work to main */
static int own_mm_lease = 0;              /* maxmemory and owner-mm-lease: batched reads and SET bursts in the lease */
static long long own_aof_ops = 0;         /* main: ops folded into the AOF */
/* GET bursts (ownerGetBurst): the command a "GET"/"get" frame resolves to, when that is getCommand under
 * its own name (rename-command can move it); NULL turns bursts off. The shared name objects stand in
 * for the frame's argv[0] in slow commandlog entries. */
static struct serverCommand *own_burst_cmd = NULL;
static robj *own_burst_name[2]; /* "GET", "get" */
/* SET bursts (ownerSetBurst, D3-all-g10-setburst): the same for "SET"/"set" frames and setCommand. */
static struct serverCommand *own_setburst_cmd = NULL;
static robj *own_setburst_name[2]; /* "SET", "set" */

int ownerThreadsEnabled(void) {
    return own_n > 0;
}

/* ------------------------------------------------------------------------------------------------
 * WATCH registries (multi.c)
 * ---------------------------------------------------------------------------------------------- */

int ownerCount(void) {
    return own_n;
}

int ownerSelfId(void) {
    return own_self ? own_self->id : -1;
}

dict *ownerWatchRegistry(int owner, int dbid) {
    return owner >= 0 && owner < own_n ? owners[owner].watched[dbid] : NULL;
}

void ownerWatchersAdd(int owner, int delta) {
    owners[owner].watchers += delta;
}

long long ownerWatchersTotal(void) {
    long long n = 0;
    for (int i = 0; i < own_n; i++) n += owners[i].watchers;
    return n;
}

/* The owner of a key's slot (ranges are contiguous and ascending). */
int ownerOfKey(robj *key) {
    sds k = objectGetVal(key);
    int slot = (int)keyHashSlot(k, (int)sdslen(k)), lo = 0, hi = own_n - 1;
    while (lo < hi) {
        int mid = (lo + hi + 1) / 2;
        if (owners[mid].slot_lo <= slot)
            lo = mid;
        else
            hi = mid - 1;
    }
    return lo;
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

/* The slot hash of the owner hot path: keyHashSlot() (the slot a cluster client routes a key by),
 * with its CRC16 taken 8 bytes per step instead of 1. own_crc16[k][v] is the CRC16 of 8 bytes with
 * byte k = v and the rest 0, built from crc16() itself: CRC16 with init 0 is linear, so a chunk's
 * CRC is the XOR of its bytes' entries, with the running CRC folded into the first two bytes.
 * keyHashSlot's byte loop was 7.5% of an owner's CPU under pipelined GETs (D3-g1 perf). */
static uint16_t own_crc16[8][256];

static inline uint16_t ownerCrc16(const unsigned char *s, size_t len) {
    uint16_t crc = 0;
    for (; len >= 8; s += 8, len -= 8)
        crc = own_crc16[0][s[0] ^ (crc >> 8)] ^ own_crc16[1][s[1] ^ (crc & 0xff)] ^ own_crc16[2][s[2]] ^
              own_crc16[3][s[3]] ^ own_crc16[4][s[4]] ^ own_crc16[5][s[5]] ^ own_crc16[6][s[6]] ^ own_crc16[7][s[7]];
    for (; len; len--) crc = (uint16_t)(crc << 8) ^ own_crc16[7][(crc >> 8) ^ *s++]; /* [7] is crc16's table */
    return crc;
}

/* keyHashSlot(key, len), hash tags included. */
static inline int ownerKeyHashSlot(const char *key, size_t len) {
    const char *s = memchr(key, '{', len);
    if (s) {
        const char *e = memchr(s + 1, '}', len - (size_t)(s + 1 - key));
        if (e && e > s + 1) return ownerCrc16((const unsigned char *)s + 1, (size_t)(e - s - 1)) & 0x3FFF;
    }
    return ownerCrc16((const unsigned char *)key, len) & 0x3FFF;
}

static void ownerSlotHashInit(void) {
    char buf[80];
    for (int k = 0; k < 8; k++) {
        for (int v = 0; v < 256; v++) {
            memset(buf, 0, 8);
            buf[k] = (char)v;
            own_crc16[k][v] = crc16(buf, 8);
        }
    }
    /* Self-check against keyHashSlot: every length to 80, bytes with many '{' and '}'. */
    uint64_t x = 0x9e3779b97f4a7c15ULL;
    for (int i = 0; i < 20000; i++) {
        int len = i % 81;
        for (int j = 0; j < len; j++) {
            x = x * 6364136223846793005ULL + 1442695040888963407ULL;
            unsigned char b = (unsigned char)(x >> 56);
            buf[j] = (b & 7) == 0 ? '{' : (b & 7) == 1 ? '}' : (char)b;
        }
        serverAssert(ownerKeyHashSlot(buf, len) == (int)keyHashSlot(buf, len));
    }
}

void ownerThreadsInit(void) {
    if (server.owner_threads == 0) return;
#ifdef __linux__
    ownerSlotHashInit();
    own_n = server.owner_threads;
    /* D3-all-g11-repl: owners append to the replication buffer while main sleeps, so the replica sockets
     * are written by the repl-stream-sender thread (it streams what an append publishes, off main and
     * off the owners) unless the config says otherwise. The thread starts from main (its CPU set). */
    if (server.owner_repl_sender && server.repl_stream_sender == REPL_STREAM_SENDER_NO)
        server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    owners = zcalloc(sizeof(owner) * own_n);
    own_main_inbox = listCreate();
    own_guests = listCreate();
    for (int k = 0; k < OWN_NCMDS; k++) {
        /* By original name: rename-command moves the same command under another name. */
        own_cmds[k] = lookupCommandByCStringLogic(server.orig_commands, own_cmd_names[k]);
        serverAssert(own_cmds[k] != NULL);
    }
    /* A burst takes "GET"/"get" frames for getCommand: only while that name still means it. */
    struct serverCommand *get = lookupCommandByCString("get");
    if (get && get == own_cmds[OWN_GET] && get->proc == getCommand) {
        own_burst_cmd = get;
        own_burst_name[0] = makeObjectShared(createStringObject("GET", 3));
        own_burst_name[1] = makeObjectShared(createStringObject("get", 3));
    }
    struct serverCommand *set = lookupCommandByCString("set");
    if (set && set == own_cmds[OWN_SET] && set->proc == setCommand) {
        own_setburst_cmd = set;
        own_setburst_name[0] = makeObjectShared(createStringObject("SET", 3));
        own_setburst_name[1] = makeObjectShared(createStringObject("set", 3));
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
        o->watched = zmalloc(sizeof(dict *) * server.dbnum);
        for (int j = 0; j < server.dbnum; j++) o->watched[j] = dictCreate(&keylistDictType);
        o->aofbuf = sdsempty();
        o->replbuf = sdsempty();
        o->aof_first_db = -1;
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

/* What native commands propagated on owner o since its records last went to the AOF buffer goes
 * there now (and to the WAITAOF offset), in the owner's execution order, as call() would have sent
 * it. The owner formatted the records (ownerTakeProp); this appends them as propagateNow and
 * feedAppendOnlyFile would have fed them one by one: the same timestamp annotation, SELECTs and
 * bytes, and one replication offset step per record with the REPL target.
 * D3-all-g11-repl: with replicas or a backlog (a top-level primary), the same records go to the
 * replication stream as replicationFeedReplicas would have fed them one by one: replicas prepared to
 * write first, the SELECT before the first keyed record when the stream has another db selected
 * (server.replicas_eldb; the later SELECTs are in the records), the bytes into the shared replication
 * buffer (feedReplicationBuffer: offset, backlog, replica references, output buffer limits), and each
 * client's WAIT offset at the end of its own records. The records' frames are the ones the stock
 * stream carries (catAppendOnlyGenericCommand and replicationFeedReplicas format a command alike).
 * The caller has the AOF and replication state (server.aof_buf, the selected dbs, the offset, the
 * replication buffer and the replica clients) to itself: main with the world stopped (ownerFoldProp),
 * or owner o inside its window holding own_aof_mu (ownerDrainAof); main never overlaps a window, and
 * the owners take turns on the lock. The repl-stream-sender thread reads only bytes below the stable
 * offset published after the append (feedReplicationBuffer), as with main appending. */
static void ownerAofAppend(owner *o) {
    if (!o->nunits) return;
    /* propagateNow: shouldPropagate (the AOF target) and propagate_to_aof; feedAppendOnlyFile's own
     * state check for the bytes. */
    int allowed = server.replication_allowed && !server.loading;
    int feed = allowed && server.aof_state != AOF_OFF;
    int to_aof = server.aof_state == AOF_ON ||
                 (server.aof_state == AOF_WAIT_REWRITE && server.child_type == CHILD_TYPE_AOF);
    /* shouldPropagate's REPL target. Slot export needs cluster mode, where owner threads are off. */
    int repl = allowed && server.primary_host == NULL && (server.repl_backlog || listLength(server.replicas));
    size_t len = sdslen(o->aofbuf);
    if (feed) {
        if (to_aof && len) {
            if (server.aof_timestamp_enabled) {
                sds ts = genAofTimestampAnnotationIfNeeded(0);
                if (ts != NULL) {
                    server.aof_buf = aofCatAnnotation(server.aof_buf, ts, server.aof_fd_fmt);
                    sdsfree(ts);
                }
            }
            if (o->aof_first_db == -1) {
                server.aof_buf = sdscatlen(server.aof_buf, o->aofbuf, len);
            } else {
                server.aof_buf = sdscatlen(server.aof_buf, o->aofbuf, o->aof_sel_at);
                if (o->aof_first_db != server.aof_selected_db)
                    server.aof_buf = aofCatSelect(server.aof_buf, o->aof_first_db, server.aof_fd_fmt);
                server.aof_buf = sdscatlen(server.aof_buf, o->aofbuf + o->aof_sel_at, len - o->aof_sel_at);
                server.aof_selected_db = o->aof_db;
            }
        }
        for (int u = 0; u < o->nunits; u++) own_aof_ops += o->units[u].n;
    }
    if (repl) {
        /* replicationFeedReplicas: there is a backlog whenever there are replicas. */
        serverAssert(server.repl_backlog != NULL);
        prepareReplicasToWrite();
        long long base = server.primary_repl_offset;
        size_t sel_len = 0;
        /* D3-all-g14-fold: the stream's frames are RESP. With the AOF's records compact (own_split) they are
         * replbuf's (the same records, encoded in RESP by the owner); otherwise aofbuf's, which are RESP. */
        serverAssert(own_split || own_rec_fmt == AOF_FMT_RESP);
        char *rb = own_split ? o->replbuf : o->aofbuf;
        size_t rlen = own_split ? sdslen(o->replbuf) : len;
        size_t rsel = own_split ? o->repl_sel_at : o->aof_sel_at;
        if (rlen) {
            if (o->aof_first_db == -1) {
                feedReplicationBuffer(rb, rlen);
            } else {
                if (rsel) feedReplicationBuffer(rb, rsel);
                if (server.replicas_eldb != o->aof_first_db) {
                    /* generateSelectCommand's bytes (replicationFeedSelectDb) */
                    char sel[96], seldb[32];
                    int dl = snprintf(seldb, sizeof(seldb), "%d", o->aof_first_db);
                    int sl = snprintf(sel, sizeof(sel), "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", dl, seldb);
                    feedReplicationBuffer(sel, (size_t)sl);
                    sel_len = (size_t)sl;
                }
                feedReplicationBuffer(rb + rsel, rlen - rsel);
                server.replicas_eldb = o->aof_db;
            }
        }
        for (int u = 0; u < o->nunits; u++) {
            ownerPropUnit *pu = &o->units[u];
            size_t end = own_split ? pu->rend : pu->end;
            size_t at = end + (o->aof_first_db != -1 && end > rsel ? sel_len : 0);
            pu->c->woff = base + (long long)at;
            own_repl_ops += pu->n;
            if (own_split) own_split_ops += pu->n;
        }
        own_repl_bytes += server.primary_repl_offset - base;
        /* An owner may have closed a replica on its output buffer limit (freeClientAsync on an owner
         * thread only flags it); main queues the close at its stop (ownerReplCloses). */
        if (own_self) {
            listIter li;
            listNode *ln;
            listRewind(server.replicas, &li);
            while ((ln = listNext(&li))) {
                client *r = ln->value;
                /* A replica main writes itself (not the repl-stream-sender's) waits for main. */
                if (r->flag.close_asap || (r->flag.pending_write && !r->repl_data->stream_via_sender))
                    o->wake_main = 1;
            }
        }
    } else if (feed) {
        for (int u = 0; u < o->nunits; u++) {
            ownerPropUnit *pu = &o->units[u];
            if (pu->repl) {
                server.primary_repl_offset += pu->repl; /* replicationFeedReplicas without replicas */
                pu->c->woff = server.primary_repl_offset;
            }
        }
    }
    o->nunits = 0;
    o->aof_first_db = -1;
    /* shortcut: the buffer keeps up to 1 MB between folds (the stock aof_buf keeps 4 KB); a bigger
     * one is freed. Upgrade if owners' write bursts make that churn. */
    if (sdsalloc(o->aofbuf) > (1 << 20)) {
        sdsfree(o->aofbuf);
        o->aofbuf = sdsempty();
    } else {
        sdsclear(o->aofbuf);
    }
    if (sdsalloc(o->replbuf) > (1 << 20)) {
        sdsfree(o->replbuf);
        o->replbuf = sdsempty();
    } else {
        sdsclear(o->replbuf);
    }
}

/* Main, world stopped: a replica an owner closed on its output buffer limit has close_asap set but is
 * not queued yet (freeClientAsync on an owner thread only flags the client): queue it as
 * freeClientAsync on main would. */
static void ownerReplCloses(void) {
    listIter li;
    listNode *ln;
    listRewind(server.replicas, &li);
    while ((ln = listNext(&li))) {
        client *r = ln->value;
        if (r->flag.close_asap && !listSearchKey(server.clients_to_close, r)) listAddNodeTail(server.clients_to_close, r);
    }
}

/* Main, the world just stopped, before main runs anything: the records owners have not written
 * themselves (ownerDrainAof) go to the AOF buffer ahead of anything main feeds. */
static void ownerFoldProp(void) {
    for (int i = 0; i < own_n; i++) ownerAofAppend(&owners[i]);
    if (listLength(server.replicas)) ownerReplCloses();
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
    /* AOF on is served natively (ownerFoldProp); turning it on (AOF_WAIT_REWRITE) is not. Replicas and
     * the backlog of a top-level primary are served natively too (D3-all-g11-repl: ownerAofAppend
     * feeds the stream); being a replica is not. */
    if ((server.aof_state != AOF_OFF && server.aof_state != AOF_ON) || server.primary_host ||
        (!server.owner_repl_native && (server.repl_backlog || listLength(server.replicas))))
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
    own_mm_lease = own_mm && server.owner_mm_lease;
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
    own_repl = !(own_last_veto & OWN_VETO_ALL) && server.primary_host == NULL &&
               (server.repl_backlog != NULL || listLength(server.replicas) != 0);
    own_keep = own_aof || own_repl;
    own_rec_fmt = own_aof ? server.aof_fd_fmt : AOF_FMT_RESP;
    own_split = own_aof && own_repl && own_rec_fmt != AOF_FMT_RESP;
    /* Each owner appends its own records at the end of its window, except under appendfsync always
     * (main writes every owner's records at its stop and fsyncs them once). */
    own_aof_drain = own_keep && !(own_aof && server.aof_fsync == AOF_FSYNC_ALWAYS);
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
    watchMoveToMain(c); /* main serves only stock watchers */
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
    watchMoveToMain(c);
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
        /* Handed-off clients main has not taken yet: their watched keys move now, before the
         * staged loop (which reads only the dbs' registries) runs a command. */
        pthread_mutex_lock(&own_main_mu);
        listIter li;
        listNode *ln;
        listRewind(own_main_inbox, &li);
        while ((ln = listNext(&li))) watchMoveToMain(listNodeValue(ln));
        pthread_mutex_unlock(&own_main_mu);
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
static void ownerRefsMaterialize(owner *o, client *c, size_t skip);

/* Owner, in a window: leave a client to main. It is off this owner's loop when this returns. */
static int ownerHandOff(owner *o, client *c, int why) {
    /* Main gets plain reply bytes: no reference leaves the owner (ownerRefAdd). */
    if (c->owner_nrefs) ownerRefsMaterialize(o, c, 0);
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

/* ------------------------------------------------------------------------------------------------
 * Large GET values sent from where they are stored (D3-all-g11-bigreply)
 *
 * A GET reply copied each value into c->buf, then into reply blocks once the 16 KB buffer was full: a
 * 4 KB value cost a user-space copy (memcpy, from a cold value) plus a block allocation every few
 * replies, before the kernel copied it again into the socket. As D0's fast reads (fastget.c
 * fastGetWriteBulkReply, fastpath.c fpSend), a value of owner-bigreply-min bytes or more (default 2048) is
 * now sent from the object: c->buf gets its "$<len>\r\n" and "\r\n", the client a reference, and the write is
 * one writev of c->buf cut around the values. D0 needs no reference count because main changes no value
 * during its read phase; on an owner, later commands of the same window may overwrite or delete the
 * key, so the reference holds the object (incrRefCount: a shared value is copied before any change, as
 * for any value with a second reference). Every reference count change is the owner's own, inside its
 * window: the owner drops the references once the bytes are written, and copies what the socket did
 * not take into a reply block (ownerRefsMaterialize). That copy also runs before a client's replies
 * wait for the AOF (ownerHold), before it goes to main (ownerHandOff), and for any client still holding
 * references when the window ends (ownerRefsSettle). So main, which only touches owner clients and
 * values with the owners stopped, never sees a reference, and a reply's bytes and order are those of
 * the copying path.
 * Why 2048 and not D0's 1024: on loopback at W=8, GET P10 with every value sent by reference ran -17% at
 * 256 B, -9% at 1 KB, -8% at 1.5 KB, +2% at 2 KB, +12% at 3 KB and +24% at 4 KB against copying: a
 * reference adds iovec segments and the kernel copies the value cold, which costs more than the user
 * copy saves below ~2 KB.
 * ---------------------------------------------------------------------------------------------- */

/* Drop the client's references (its reply no longer needs them). */
static void ownerRefsRelease(owner *o, client *c) {
    for (int i = 0; i < c->owner_nrefs; i++) decrRefCount(c->owner_refs[i].o);
    c->owner_nrefs = 0;
    c->owner_ref_bytes = 0;
    if (c->owner_refc_at) {
        int k = c->owner_refc_at - 1;
        serverAssert(k < o->nrefc && o->refc[k] == c);
        client *last = o->refc[--o->nrefc];
        o->refc[k] = last;
        last->owner_refc_at = k + 1;
        c->owner_refc_at = 0;
    }
}

/* The client's reply with its references is c->buf with each value's bytes at its offset: the
 * part from byte skip on (the bytes before went out) becomes one plain reply block at the head of
 * the reply list, before what spilled there after the references; c->buf is then empty and the
 * references are dropped. The next write starts at that block. */
static __attribute__((noinline)) void ownerRefsMaterialize(owner *o, client *c, size_t skip) {
    size_t total = (size_t)c->bufpos + c->owner_ref_bytes;
    serverAssert(skip <= total && c->io_last_written.data_len == 0);
    size_t rem = total - skip;
    if (rem) {
        size_t usable, size = rem < PROTO_REPLY_CHUNK_BYTES ? PROTO_REPLY_CHUNK_BYTES : rem;
        clientReplyBlock *b = zmalloc_usable(size + sizeof(clientReplyBlock), &usable);
        b->size = usable - sizeof(clientReplyBlock);
        b->used = 0;
        b->flag.buf_encoded = 0;
        b->last_header = NULL;
        size_t pos = 0, prev = 0; /* pos: offset in the reply; prev: in c->buf */
        for (int i = 0; i <= c->owner_nrefs; i++) {
            const char *seg[2];
            size_t segl[2];
            int ns = 0;
            size_t at = i < c->owner_nrefs ? c->owner_refs[i].at : (size_t)c->bufpos;
            seg[ns] = c->buf + prev, segl[ns++] = at - prev;
            if (i < c->owner_nrefs) seg[ns] = c->owner_refs[i].s, segl[ns++] = c->owner_refs[i].len;
            prev = at;
            for (int j = 0; j < ns; j++) {
                size_t l = segl[j], from = 0;
                if (pos + l <= skip) {
                    pos += l;
                    continue;
                }
                if (pos < skip) from = skip - pos;
                memcpy(b->buf + b->used, seg[j] + from, l - from);
                b->used += l - from;
                pos += l;
            }
        }
        serverAssert(b->used == rem);
        listAddNodeHead(c->reply, b);
        c->reply_bytes += b->size;
    }
    ownerRefsRelease(o, c);
    c->bufpos = 0;
    c->flag.buf_encoded = 0;
    c->last_header = NULL;
    resetLastWrittenBuf(c);
    o->ref_copied++;
    o->ref_copied_bytes += rem;
    if (rem) closeClientOnOutputBufferLimitReached(c, 1);
}

/* Reply to a GET with the bulk string of val (s, len: its bytes) by reference: the header and the trailing
 * CRLF into c->buf, the value's bytes left where they are. Returns 0, having done nothing, when the
 * value is copied as before: below owner-bigreply-min, not an sds string, a reply already in the
 * reply list or partly written, a reply mode other than the plain one, or no room. */
static __attribute__((noinline)) int ownerRefAdd(client *c, robj *val, const char *s, size_t len) {
    owner *o = own_self;
    int min = server.owner_bigreply_min;
    if (min <= 0 || len < (size_t)min || len > UINT32_MAX || !sdsEncodedObject(val)) return 0;
    /* D3-all-g13-fold: never under maxmemory, even inside a roomy memory lease (D3-all-g11-maxmem): a value a
     * reply still holds stays allocated after an overwrite in the same window, and the lease bounds count
     * no such bytes. GET bursts and rings are off under maxmemory anyway; this keeps it so. */
    if (own_mm) return 0;
    if (objectGetRefcount(val) >= OBJ_FIRST_SPECIAL_REFCOUNT) return 0;
    if (c->owner_nrefs >= OWN_REF_MAX || c->owner_ref_bytes + len > OWN_REF_BYTES_MAX) return 0;
    if (listLength(c->reply) || server.debug_client_enforce_reply_list || c->flag.close_after_reply ||
        c->flag.pushing || isDeferredReplyEnabled(c) || c->conn == NULL || c->conn->type != connectionTypeTcp())
        return 0;
    /* A write that left part of c->buf: the reply goes on as bytes until it is out. */
    if (c->io_last_written.buf || c->io_last_written.data_len) return 0;
    if (!c->bufpos)
        c->flag.buf_encoded = 0; /* as _addReplyToBuffer starts a buffer for an owner client */
    else if (c->flag.buf_encoded)
        return 0;
    char hdr[LONG_STR_SIZE + 3];
    hdr[0] = '$';
    size_t hl = 1 + (size_t)ll2string(hdr + 1, LONG_STR_SIZE, (long long)len);
    hdr[hl++] = '\r';
    hdr[hl++] = '\n';
    if (c->buf_usable_size - (size_t)c->bufpos < hl + 2) return 0;
    if (c->owner_nrefs == c->owner_refs_cap) {
        c->owner_refs_cap = c->owner_refs_cap ? c->owner_refs_cap * 2 : 16;
        c->owner_refs = zrealloc(c->owner_refs, sizeof(ownerRef) * c->owner_refs_cap);
    }
    if (!c->owner_nrefs) {
        if (o->nrefc == o->refc_cap) {
            o->refc_cap = o->refc_cap ? o->refc_cap * 2 : 64;
            o->refc = zrealloc(o->refc, sizeof(client *) * o->refc_cap);
        }
        o->refc[o->nrefc++] = c;
        c->owner_refc_at = o->nrefc;
    }
    memcpy(c->buf + c->bufpos, hdr, hl);
    c->bufpos += hl;
    incrRefCount(val);
    c->owner_refs[c->owner_nrefs++] = (ownerRef){.o = val, .s = s, .len = (uint32_t)len, .at = (uint32_t)c->bufpos};
    c->owner_ref_bytes += len;
    memcpy(c->buf + c->bufpos, "\r\n", 2);
    c->bufpos += 2;
    if (c->buf_peak < (size_t)c->bufpos) c->buf_peak = (size_t)c->bufpos;
    c->flag.buffered_reply = 1; /* as prepareClientToWrite for an owner client */
    c->net_output_bytes_curr_cmd += hl + len + 2;
    o->ref_replies++;
    o->ref_bytes += len;
    return 1;
}

/* ownerWriteOnce for a client whose reply holds references, all of it in c->buf (nothing in the reply
 * list): writev of c->buf cut around the values, in reply order, until it is out or the socket is
 * full; what is left is copied into a reply block, so no reference outlives this call. */
static __attribute__((noinline)) void ownerRefsWrite(owner *o, client *c) {
    struct iovec iov[2 * OWN_REF_MAX + 1];
    int n = 0;
    size_t prev = 0;
    for (int i = 0; i < c->owner_nrefs; i++) {
        ownerRef *r = &c->owner_refs[i];
        if (r->at > prev) iov[n++] = (struct iovec){.iov_base = c->buf + prev, .iov_len = r->at - prev};
        iov[n++] = (struct iovec){.iov_base = (void *)r->s, .iov_len = r->len};
        prev = r->at;
    }
    if ((size_t)c->bufpos > prev) iov[n++] = (struct iovec){.iov_base = c->buf + prev, .iov_len = (size_t)c->bufpos - prev};
    size_t total = (size_t)c->bufpos + c->owner_ref_bytes, sent = 0;
    c->write_flags = 0;
    for (int k = 0; k < n;) {
        int w = connWritev(c->conn, iov + k, n - k);
        if (w <= 0) {
            c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
            break;
        }
        sent += (size_t)w;
        size_t left = (size_t)w;
        while (k < n && left >= iov[k].iov_len) left -= iov[k++].iov_len;
        if (k < n && left) {
            iov[k].iov_base = (char *)iov[k].iov_base + left;
            iov[k].iov_len -= left;
        }
    }
    c->nwritten = (ssize_t)sent;
    o->writes++;
    o->ref_writes++;
    if (sent == total) {
        ownerRefsRelease(o, c);
        c->bufpos = 0;
        c->last_header = NULL;
        resetLastWrittenBuf(c);
    } else {
        ownerRefsMaterialize(o, c, sent);
    }
    if (sent > 0) {
        ownerCountNetOut((long long)sent);
        c->net_output_bytes += sent;
        c->last_interaction = server.unixtime;
    }
}

/* Owner, at the end of its window: the clients that still hold references (replies waiting for the AOF,
 * or not written in this window) get their replies copied out; those not waiting write them on their
 * next writable event. */
static __attribute__((noinline)) void ownerRefsSettle(owner *o) {
    while (o->nrefc) {
        client *c = o->refc[o->nrefc - 1];
        ownerRefsMaterialize(o, c, 0);
        if (!c->owner_held && c->owner_onloop) ownerSetWantOut(o, c, 1);
    }
}

/* Write what the client has, as an IO thread would (the snapshot below stands for main's). */
static void ownerWriteOnce(owner *o, client *c) {
    if (c->owner_nrefs) {
        if (!listLength(c->reply)) {
            ownerRefsWrite(o, c);
            return;
        }
        ownerRefsMaterialize(o, c, 0); /* replies spilled into the list after the references */
    }
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

/* After a write: the want-out flag and a close after the reply. Returns 1 when the client left the owner. */
static int ownerWriteTail(owner *o, client *c) {
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

/* Returns 1 when the client left the owner. */
static int ownerWrite(owner *o, client *c) {
    ownerWriteOnce(o, c);
    return ownerWriteTail(o, c);
}

/* io_uring: submit the first n ops prepared in o->ring and wait for all n completions, which come
 * inside this io_uring_enter (MSG_DONTWAIT: a socket that is not ready fails with -EAGAIN at once).
 * Each op's result goes to o->uops[user_data].res. */
#ifdef HAVE_LIBURING
static void ownerUringRun(owner *o, int n) {
    int tries = 0, got = 0;
    for (;;) {
        int r = io_uring_submit_and_wait(&o->ring, n);
        if (r >= 0) break;
        if ((r == -EINTR || r == -EAGAIN || r == -EBUSY) && ++tries < 1000) continue;
        serverPanic("owner %d: io_uring_submit_and_wait: %s", o->id, strerror(-r));
    }
    o->uring_enters++;
    while (got < n) {
        struct io_uring_cqe *cqe;
        unsigned head, seen = 0;
        io_uring_for_each_cqe(&o->ring, head, cqe) {
            serverAssert(cqe->user_data < (uint64_t)n);
            o->uops[cqe->user_data].res = cqe->res;
            seen++;
        }
        io_uring_cq_advance(&o->ring, seen);
        got += seen;
        if (got < n) {
            int r = io_uring_wait_cqe(&o->ring, &cqe);
            if (r < 0 && r != -EINTR && r != -EAGAIN) serverPanic("owner %d: io_uring_wait_cqe: %s", o->id, strerror(-r));
        }
    }
}
#endif

/* The reply bytes a write of c would send, when they are all plain bytes in c->buf (no reply blocks,
 * no references to values): what _writeToClient's connWrite branch sends. 0 otherwise. */
static int ownerPlainSpan(client *c, char **buf, size_t *len) {
    if (c->owner_nrefs || listLength(c->reply) || c->flag.buf_encoded || c->bufpos <= 0) return 0;
    if (c->conn->type != connectionTypeTcp()) return 0;
    serverAssert(c->io_last_written.data_len == 0 || c->io_last_written.buf == c->buf);
    if ((size_t)c->bufpos <= c->io_last_written.data_len) return 0;
    *buf = c->buf + c->io_last_written.data_len;
    *len = (size_t)c->bufpos - c->io_last_written.data_len;
    return 1;
}

/* ownerWriteOnce for a write io_uring made: res > 0 of the len bytes ownerPlainSpan gave went out, as
 * if _writeToClient's connWrite loop had written them and then met a full socket (res < len). */
static void ownerWriteDone(owner *o, client *c, ssize_t res, size_t len) {
    c->write_flags = 0;
    c->nwritten = res;
    c->io_last_written.buf = c->buf;
    c->io_last_written.bufpos = ((size_t)res == len ? (size_t)c->bufpos : 0);
    c->io_last_written.data_len += res;
    c->io_last_reply_block = NULL;
    c->io_last_bufpos = 0;
    o->writes++;
    ownerPostWriteToClient(c);
    c->net_output_bytes += c->nwritten;
    c->last_interaction = server.unixtime;
}

/* io_uring in this window (D3-all-g9-fold): the ring is up, owner-uring is on, and appendonly is off
 * unless owner-uring-aof yes. Under appendonly the owners read and write as D3-all-g7-rwchild did (the
 * K13 SET tail); under maxmemory the reads are batched only inside a memory lease with room
 * (ownerThreadMain). own_aof changes only
 * while the owners are stopped, so one window sees one answer. */
static inline int ownerUringOn(owner *o) {
    return o->uring && server.owner_uring && (!own_aof || server.owner_uring_aof);
}

/* Write the replies of n clients of this owner (none held), as ownerWrite would one after the other;
 * with owner-uring, the plain ones go out OWN_URING_BATCH per io_uring_enter. A send that wrote
 * nothing (a full socket, an error, a closed peer) is redone by ownerWrite, so the connection state
 * and the error handling are the read/write path's. Returns 1 when any client left the owner. */
static int ownerWriteMany(owner *o, client **cs, int n) {
    int left = 0;
#ifdef HAVE_LIBURING
    int use = ownerUringOn(o);
#else
    int use = 0;
#endif
    for (int i = 0; i < n;) {
        int m = 0;
        for (; i < n && m < OWN_URING_BATCH; i++) {
            client *c = cs[i];
            char *buf;
            size_t len;
            if (!use || !ownerPlainSpan(c, &buf, &len)) {
                left |= ownerWrite(o, c);
                continue;
            }
#ifdef HAVE_LIBURING
            struct io_uring_sqe *sqe = io_uring_get_sqe(&o->ring);
            serverAssert(sqe != NULL);
            io_uring_prep_send(sqe, c->conn->fd, buf, len, MSG_DONTWAIT);
            io_uring_sqe_set_data64(sqe, (uint64_t)m);
            o->uops[m] = (ownerUringOp){.c = c, .buf = buf, .len = len, .scratch = 0, .res = 0};
            m++;
#endif
        }
        if (!m) continue;
#ifdef HAVE_LIBURING
        ownerUringRun(o, m);
        o->uring_writes += m;
        for (int j = 0; j < m; j++) {
            ownerUringOp *op = &o->uops[j];
            if ((size_t)op->res != op->len) o->uring_short++;
            if (op->res > 0) {
                ownerWriteDone(o, op->c, op->res, op->len);
                left |= ownerWriteTail(o, op->c);
            } else {
                o->uring_redo++;
                left |= ownerWrite(o, op->c);
            }
        }
#endif
    }
    return left;
}

/* A client whose replies go out at the end of the read batch (ownerReadBatch). */
static void ownerQueueWrite(owner *o, client *c) {
    if (o->nwq == o->wq_cap) {
        o->wq_cap = o->wq_cap ? o->wq_cap * 2 : 2 * OWN_URING_BATCH;
        o->wq = zrealloc(o->wq, sizeof(client *) * o->wq_cap);
    }
    o->wq[o->nwq++] = c;
}

static int ownerFlushWrites(owner *o) {
    int n = o->nwq;
    o->nwq = 0;
    return n ? ownerWriteMany(o, o->wq, n) : 0;
}

/* Owner: the client's replies wait until main has written the AOF (ownerReleaseHeld). */
static void ownerHold(owner *o, client *c) {
    if (c->owner_nrefs) ownerRefsMaterialize(o, c, 0); /* the replies outlive the window */
    if (o->nheld == o->held_cap) {
        o->held_cap = o->held_cap ? o->held_cap * 2 : 64;
        o->held = zrealloc(o->held, sizeof(client *) * o->held_cap);
    }
    o->held[o->nheld++] = c;
    c->owner_held = o->nheld;
    o->held_total++;
    ownerSetWantOut(o, c, 0); /* level-triggered EPOLLOUT would fire on and on */
}

/* Returns 1 when the client left the owner. A reply of an owner with ops not written to the AOF
 * yet may show one of their writes (GET after SET of the same key), so it waits; replies
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
    for (int i = 0; i < n; i++) o->held[i]->owner_held = 0;
    /* ownerWriteMany may hold nothing and unhold nobody (owner_held is 0 for all), so o->held stays as is
     * while it walks the list. */
    if (ownerWriteMany(o, o->held, n)) o->wake_main = 1; /* one left: main takes it */
}

/* Owner: the db of the next keyed record; a SELECT before it when the buffer selected another one, as
 * feedAppendOnlyFile writes it. The first keyed record's SELECT depends on the db the AOF stream has
 * selected when the buffer is appended (ownerAofAppend). */
static void ownerAofSelect(owner *o, int dbid) {
    if (dbid == -1) return;
    if (o->aof_first_db == -1) {
        o->aof_first_db = o->aof_db = dbid;
        o->aof_sel_at = sdslen(o->aofbuf);
        o->repl_sel_at = sdslen(o->replbuf);
        return;
    }
    if (dbid == o->aof_db) return;
    o->aofbuf = aofCatSelect(o->aofbuf, dbid, own_rec_fmt);
    if (own_split) o->replbuf = aofCatSelect(o->replbuf, dbid, AOF_FMT_RESP); /* the stream's copy, RESP */
    o->aof_db = dbid;
}

/* Owner: format what the native command's execution unit propagated into this owner's AOF records,
 * as propagatePendingCommands -> propagateNow -> feedAppendOnlyFile would (MULTI/EXEC around more
 * than one op, unless the command touches arbitrary keys), and free the ops here, on the thread that
 * made them. The bytes are appended in order by this owner (ownerDrainAof) or by main at its stop
 * (ownerFoldProp). With the AOF on and no replicas, a
 * backlog or slot export (vetoed), an op propagates only through its AOF target, and each such
 * record with the REPL target steps the replication offset (WAITAOF). */
static void ownerTakeProp(owner *o, client *c) {
    serverOpArray *oa = &ectx->also_propagate;
    int n = oa->numops;
    if (!own_keep) {
        serverOpArrayFree(oa);
        return;
    }
    /* One record buffer serves both streams: with both on, every native op has both targets
     * (ownerNativeKind sends clients with propagation flags to main); with one on, its target. */
    int want = (own_aof ? PROPAGATE_AOF : 0) | (own_repl ? PROPAGATE_REPL : 0);
    if (o->nunits == o->units_cap) {
        o->units_cap = o->units_cap ? o->units_cap * 2 : 256;
        o->units = zrealloc(o->units, sizeof(ownerPropUnit) * o->units_cap);
    }
    /* shared.multi / shared.exec as catAppendOnlyGenericCommand formats them, without touching the
     * shared objects from this thread. vkevo binaof: every record in the open incr file's format
     * (server.aof_fd_fmt changes only with the owners stopped and their records folded); D3-all-g14-fold:
     * own_rec_fmt (RESP with the AOF off), plus a RESP copy for the stream when that format is compact. */
    int fmt = own_rec_fmt;
    int wrap = n > 1 && !(c->cmd->flags & CMD_TOUCHES_ARBITRARY_KEYS), repl = 0;
    if (wrap) {
        o->aofbuf = aofCatMulti(o->aofbuf, fmt);
        if (own_split) o->replbuf = aofCatMulti(o->replbuf, AOF_FMT_RESP);
        repl++;
    }
    for (int j = 0; j < n; j++) {
        serverOp *op = oa->ops + j;
        if (!(op->target & want)) continue;
        serverAssert(!own_aof || !own_repl || (op->target & want) == want);
        ownerAofSelect(o, op->dbid);
        o->aofbuf = aofCatCommand(o->aofbuf, op->argc, op->argv, fmt);
        if (own_split) o->replbuf = catAppendOnlyGenericCommand(o->replbuf, op->argc, op->argv);
        if (op->target & PROPAGATE_REPL) repl++;
    }
    if (wrap) {
        o->aofbuf = aofCatExec(o->aofbuf, fmt);
        if (own_split) o->replbuf = aofCatExec(o->replbuf, AOF_FMT_RESP);
        repl++;
    }
    serverOpArrayFree(oa);
    o->units[o->nunits++] =
        (ownerPropUnit){.c = c, .n = n, .repl = repl, .end = sdslen(o->aofbuf), .rend = sdslen(o->replbuf)};
    o->unflushed = 1;
    if (!own_aof_drain) o->wake_main = 1; /* else this owner writes them at the end of its window */
}

/* Owner, end of a window, appendfsync everysec or no (own_aof_drain): write this window's records to
 * the AOF here instead of waking main to stop the world for them, then let the replies that waited
 * for them go. Owners take turns on own_aof_mu and main never overlaps a window, so the holder has
 * main's AOF state to itself: its records go to the AOF buffer behind everything fed before them
 * (main's own, the records main folded at its last stop, other owners' drains), and
 * flushAppendOnlyFile writes the buffer as beforeSleep does, with the same policy: the write, an
 * fsync job once a second under everysec, a write postponed behind a running fsync, a failed write
 * kept in the buffer. The replies then go exactly when the stock loop would send them: when
 * aofHoldsReplies() is false. Each key is written by one thread at a time and its records reach the
 * buffer in that thread's execution order, so the AOF has each key's writes in order.
 * Under everysec the drain also starts the next background fsync once OWN_AOF_FSYNC_BYTES are written
 * and not fsynced and none runs, besides the stock once a second (D3-all-g6-aoftail). The stock write
 * is postponed while an fsync runs, so the buffer piles up for the whole fsync, and the first drain
 * after it writes all of it under own_aof_mu while every other owner waits on the lock with its
 * replies held. K13 at about 450 MB/s of AOF (W=16 loopback, 150 B values) showed fsyncs of 100-300
 * ms once a second, 50-100 MB writes after them and lock holds up to 47 ms. Bounding the bytes per
 * fsync bounds its time and the pile behind it; below 32 MB a second nothing changes. Never later
 * than stock: data is fsynced sooner; the write policy and the reply release are unchanged.
 * shortcut: one byte threshold for every disk. Upgrade: derive it from the measured fsync time if
 * disks differ a lot. */
static void ownerDrainAof(owner *o) {
    pthread_mutex_lock(&own_aof_mu);
    ownerAofAppend(o);
    int hold = 0, err = 0;
    if (own_aof) {
        flushAppendOnlyFile(0);
        aofBackgroundFsyncIfDue(OWN_AOF_FSYNC_BYTES);
        own_aof_drains++;
        hold = aofHoldsReplies();
        /* writeCommandsDeniedByDiskError's AOF part: after a failed write or background fsync the next
         * write goes to main, which refuses it, as processCommand would, until the AOF works again. */
        err = server.aof_last_write_status == C_ERR ||
              atomic_load_explicit(&server.aof_bio_fsync_status, memory_order_acquire) == C_ERR;
        /* repl-stream-sender with the AOF on: replica bytes go once the AOF has them (as the sender's
         * tick bounds them on main), here the moment this drain wrote them. */
        if (own_repl && !hold) replStreamSenderPublishStable(server.primary_repl_offset);
    }
    pthread_mutex_unlock(&own_aof_mu);
    if (err) {
        atomic_fetch_or_explicit(&own_veto, OWN_VETO_WRITE, memory_order_relaxed);
        o->wake_main = 1;
    }
    if (hold) {
        /* Not under everysec or no (a write that did not happen leaves aofHoldsReplies false), but if
         * it were: main writes the buffer and releases the replies (ownerReleaseHeld). */
        o->wake_main = 1;
        return;
    }
    o->unflushed = 0;
    if (o->nheld) ownerWriteHeld(o);
}

/* ------------------------------------------------------------------------------------------------
 * Owner side: maxmemory headroom
 * ---------------------------------------------------------------------------------------------- */

/* What this owner added since the period began (negative when it freed more), or LLONG_MAX when
 * the thread has no allocation counter of its own (then nothing runs natively under maxmemory). */
static long long ownerMmGrowth(owner *o);

/* D3-all-g14-fold (g11-repl x g11-maxmem): the replication buffer blocks this owner's window will
 * allocate when it appends its records to the stream at the window's end (ownerAofAppend on the owner
 * counts them as the owner's growth, as g11-repl does): the stream's pending bytes (replbuf's, or
 * aofbuf's when the stream shares them) plus the blocks' headers, held in the lease from the moment
 * the records are formatted, so a check never admits a step the append would then push over the lease. */
static inline long long ownerMmStreamDue(owner *o) {
    if (!own_repl) return 0;
    size_t n = own_split ? sdslen(o->replbuf) : sdslen(o->aofbuf);
    return n ? (long long)(n + (n >> 4) + 1024) : 0;
}

static long long ownerMmGrowth(owner *o) {
    size_t now;
    if (!zmalloc_thread_used_memory(&now)) return LLONG_MAX;
    return (long long)(now - o->mm_base);
}

/* Whether a step that may add `need` bytes fits this owner's slice, with the reads its batch still
 * has queued counted as their OWN_MM_READ each (o->mm_resv). */
static int ownerMmFits(owner *o, size_t need) {
    if (atomic_load_explicit(&own_mm_over, memory_order_relaxed)) return 0;
    long long g = ownerMmGrowth(o);
    return g != LLONG_MAX && g + (long long)need + (long long)o->mm_resv + ownerMmStreamDue(o) <= own_mm_slice;
}

/* Owner: after a read and its parse, which can allocate more than any bound before it (a big
 * argument's buffer). Over the slice (the batch's queued reads counted): every owner sends its work
 * to main until main has looked. */
static void ownerMmAfterRead(owner *o) {
    long long g = ownerMmGrowth(o);
    if (g == LLONG_MAX || g + (long long)o->mm_resv + ownerMmStreamDue(o) > own_mm_slice) {
        atomic_store_explicit(&own_mm_over, 1, memory_order_relaxed);
        o->wake_main = 1;
    }
}

/* The bytes each of this owner's growable arrays can take when it doubles. */
static size_t ownerArraysBytes(owner *o) {
    return sdsalloc(o->aofbuf) + sdsalloc(o->replbuf) + sizeof(ownerPropUnit) * (size_t)o->units_cap +
           sizeof(client *) * (size_t)o->held_cap + sizeof(ownerError) * (size_t)o->errors_cap +
           sizeof(ownerLog) * (size_t)o->logs_cap;
}

/* The length of the string at `key` in this owner's table of `slot`, or 0 (no key, not a string). */
static size_t ownerStoredLenOf(client *c, int slot, robj *key) {
    void *found;
    if (slot < 0 || !kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(slot), objectGetVal(key), &found))
        return 0;
    robj *v = found;
    return objectGetType(v) == OBJ_STRING ? stringObjectLen(v) : 0;
}

/* What the reply of a GET, an MGET or a SET ... GET on this owner's keys can hold: the stored strings. */
static size_t ownerReplyValuesLen(client *c, struct serverCommand *cmd, robj **argv, int argc, int slot) {
    serverCommandProc *p = cmd->proc;
    size_t len = 0;
    if (p == getCommand) return ownerStoredLenOf(c, slot, argv[1]);
    if (p == mgetCommand) {
        for (int j = 1; j < argc; j++) len += ownerStoredLenOf(c, slot, argv[j]);
        return len;
    }
    if (p == setCommand)
        for (int j = 3; j < argc; j++)
            if (sdsEncodedObject(argv[j]) && !strcasecmp(objectGetVal(argv[j]), "get"))
                return ownerStoredLenOf(c, slot, argv[1]);
    return 0;
}

/* EXEC (D3-all): what its queued commands can add, each bounded as ownerMmCmdFits bounds one command;
 * 3x the queued arguments covers their encoding and the values a queued SET makes a later GET return. */
static size_t ownerMmExecBound(owner *o, client *c) {
    multiState *ms = c->mstate;
    size_t b = 3 * ms->argv_len_sums;
    for (int j = 0; j < ms->count; j++) {
        multiCmd *mc = ms->commands + j;
        b += OWN_MM_CMD + 64 * (size_t)mc->argc + ownerReplyValuesLen(c, mc->cmd, mc->argv, mc->argc, mc->slot);
        if (own_keep) b += OWN_MM_REC * (32 * (size_t)mc->argc + 64) + 2 * sizeof(serverOp);
    }
    if (own_keep) b += OWN_MM_REC * ms->argv_len_sums;
    if (server.latency_tracking_enabled)
        for (int k = 0; k < OWN_MULTI; k++)
            if (!o->cs[k].hist) b += OWN_MM_HISTOGRAM; /* a queued command's first histogram */
    return b;
}

/* Whether native command `kind` fits: a bound on what it can add (the value, objects and entries, the
 * reply and a reply block, commandlog entries, a first latency histogram, the ops kept for the AOF and
 * a lazy-expire DEL's, an array doubling; for EXEC its queued commands too) on top of what the owner
 * added so far. */
static int ownerMmCmdFits(owner *o, client *c, int kind) {
    size_t b = OWN_MM_CMD + 2 * c->argv_len_sum + 64 * (size_t)c->argc + 2 * ownerArraysBytes(o);
    if (server.latency_tracking_enabled && !o->cs[kind].hist) b += OWN_MM_HISTOGRAM;
    if (own_keep) b += OWN_MM_REC * (c->argv_len_sum + 32 * (size_t)c->argc + 64);
    if (kind == OWN_EXEC)
        b += ownerMmExecBound(o, c);
    else
        b += ownerReplyValuesLen(c, c->parsed_cmd, c->argv, c->argc, c->slot);
    return ownerMmFits(o, b);
}

/* Memory leases: whether the next SET of a SET burst fits, bounded as ownerMmCmdFits bounds that SET on
 * the parser's path (argv_len = its three arguments' bytes; a plain SET's reply holds no stored value),
 * plus the slow commandlog entries of the burst's SETs so far and this one, which ownerSetBurstEnd makes
 * after the last of them. */
static int ownerMmSetBurstFits(owner *o, size_t argv_len, int ran) {
    size_t b = OWN_MM_CMD + 2 * argv_len + 64 * 3 + 2 * ownerArraysBytes(o) + (size_t)(ran + 1) * OWN_MM_SETBURST_LOG;
    if (server.latency_tracking_enabled && !o->cs[OWN_SET].hist) b += OWN_MM_HISTOGRAM;
    if (own_keep) b += OWN_MM_REC * (argv_len + 32 * 3 + 64); /* g14: records for the stream too (g11-repl) */
    return ownerMmFits(o, b);
}

/* hashtableResizeAllowed / hashtableCollectionResizeAllowed on an owner thread. Under maxmemory every
 * resize asks (hashtableSetResizeAskAlways); one that does not fit is skipped, as a forbidden one is. */
int ownerResizeAllowed(size_t more_mem) {
    if (!own_mm) return 1;
    return ownerMmFits(own_self, more_mem);
}

/* The cluster slot of a native candidate's keys, or -1: GET, SET and INCR (one key), and DEL, MGET,
 * MSET and WATCH when all their keys hash to one slot (hash-tagged, as cluster mode requires). Keys in
 * two slots go to main even when one owner owns both: the slot cached on the command must hold for
 * every key (getKVStoreIndexUsingCachedSlot). */
static int ownerKeySlot(struct serverCommand *cmd, robj **argv, int argc, int read_flags) {
    if (!cmd || (read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) return -1;
    serverCommandProc *p = cmd->proc;
    int step; /* distance between keys; 0: one key */
    if (p == getCommand || p == setCommand || p == incrCommand)
        step = 0;
    else if (p == delCommand || p == mgetCommand || p == watchCommand)
        step = 1;
    else if (p == msetCommand)
        step = 2;
    else
        return -1;
    sds key = objectGetVal(argv[1]);
    int slot = ownerKeyHashSlot(key, sdslen(key));
    for (int j = 1 + step; step && j < argc; j += step) {
        key = objectGetVal(argv[j]);
        if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
    }
    return slot;
}

/* Owner, before a native command other than a GET run (ownerGetRun): hash each queued command's key
 * once and keep its slot on the command (p->slot; ownerTryNative did c->slot) for ownerNativeKind
 * and the keyspace lookups (getKVStoreIndexUsingCachedSlot), and prefetch the keys of the current
 * command and the queue behind it into this owner's batch until the batch is full, as fastpath.c's
 * fpPrefetchRange does. The commands walked are marked READ_FLAGS_PREFETCHED; the first unmarked
 * one starts the next batch. Only keys in this owner's slots are looked up: other owners change
 * their tables meanwhile. */
static void ownerPrefetch(owner *o, client *c) {
    if (c->read_flags & READ_FLAGS_PREFETCHED) return;
    c->read_flags |= READ_FLAGS_PREFETCHED;
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

/* The native kind of a command, or -1. */
static int ownerKindOf(serverCommandProc *p) {
    if (p == getCommand) return OWN_GET;
    if (p == setCommand) return OWN_SET;
    if (p == delCommand) return OWN_DEL;
    if (p == incrCommand) return OWN_INCR;
    if (p == pingCommand) return OWN_PING;
    if (p == mgetCommand) return OWN_MGET;
    if (p == msetCommand) return OWN_MSET;
    if (p == multiCommand) return OWN_MULTI;
    if (p == execCommand) return OWN_EXEC;
    if (p == discardCommand) return OWN_DISCARD;
    if (p == watchCommand) return OWN_WATCH;
    if (p == unwatchCommand) return OWN_UNWATCH;
    return -1;
}

/* EXEC can run here: every queued command is a native data command on this owner's keys. That is
 * the only way an owner queues (ownerNativeKind), so this only guards the invariant. */
static int ownerTxnNative(owner *o, client *c) {
    for (int j = 0; j < c->mstate->count; j++) {
        multiCmd *mc = c->mstate->commands + j;
        int kind = ownerKindOf(mc->cmd->proc);
        if (kind < 0 || kind >= OWN_MULTI) return 0;
        if (kind != OWN_PING && (mc->slot < o->slot_lo || mc->slot > o->slot_hi)) return 0;
    }
    return 1;
}

/* Which native command this is, or -1 when main must run it. In a transaction an owner queues the
 * native data commands on its own keys (ownerQueue) and runs EXEC (ownerExec) and DISCARD itself;
 * anything else (another owner's key, any other command, an error to report) hands the client to
 * main with the transaction so far, and main finishes it exactly as the stock server does. */
static int ownerNativeKind(owner *o, client *c) {
    struct serverCommand *cmd = c->parsed_cmd;
    if (!cmd || (c->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) return -1;
    int veto = atomic_load_explicit(&own_veto, memory_order_relaxed);
    if (veto & OWN_VETO_ALL) return -1;
    int kind = ownerKindOf(cmd->proc);
    if (kind < 0) return -1;
    /* With both streams on, one record buffer serves both (ownerTakeProp): a client whose ops could
     * have only one of the targets runs on main. */
    if (own_aof && own_repl &&
        (c->flag.prevent_aof_prop || c->flag.prevent_repl_prop || c->flag.module_prevent_aof_prop ||
         c->flag.module_prevent_repl_prop || c->flag.force_aof || c->flag.force_repl))
        return -1;
    if ((c->user && (c->user->flags & USER_FLAG_RETIRED)) || authRequired(c) || !ACLUserUnrestricted(c->user))
        return -1;
    /* This client's watched keys, if any, are in this owner's registry (a client watching through main
     * stays on main until it unwatches, ownerGuestIdle). */
    int mine = !c->mstate || !listLength(&c->mstate->watched_keys) || c->mstate->watch_owner == o->id + 1;
    int keyed = 0; /* the command's keys must be this owner's */
    switch (kind) {
    case OWN_PING: break;
    case OWN_MULTI:
        if (c->flag.multi) return -1; /* nested MULTI: main replies the error */
        break;
    case OWN_DISCARD:
        if (!c->flag.multi) return -1;
        break;
    case OWN_EXEC:
        /* EXEC conditions (keys), a write veto (main rejects the EXEC): main. */
        if (!c->flag.multi || c->argc != 1 || veto || !mine || !ownerTxnNative(o, c)) return -1;
        break;
    case OWN_UNWATCH:
        if (c->flag.multi || !mine) return -1;
        break;
    case OWN_WATCH:
        if (c->flag.multi || !mine) return -1; /* WATCH inside MULTI: main replies the error */
        keyed = 1;
        break;
    default: keyed = 1; break;
    }
    if (keyed) {
        if (kind != OWN_GET && kind != OWN_MGET && kind != OWN_WATCH && (veto & OWN_VETO_WRITE)) return -1;
        /* The slot ownerTryNative computed: the keys are hashed once per command. */
        if (c->slot < o->slot_lo || c->slot > o->slot_hi) return -1; /* another owner's key: main */
    }
    /* Under maxmemory the stock server may evict or refuse before any command (queued ones and EXEC
     * included): main decides that once the command does not fit this owner's headroom. */
    if (own_mm && !ownerMmCmdFits(o, c, kind)) {
        o->mm_refused_total++;
        return -1;
    }
    return kind;
}

/* Keep a native command's commandlog entries for main, which adds them when it folds the stats. */
static void ownerKeepLogs(owner *o, void **entries, int *types, int n) {
    for (int i = 0; i < n; i++) {
        if (o->nlogs == o->logs_cap) {
            o->logs_cap = o->logs_cap ? o->logs_cap * 2 : 8;
            o->logs = zrealloc(o->logs, sizeof(ownerLog) * o->logs_cap);
        }
        o->logs[o->nlogs++] = (ownerLog){.entry = entries[i], .type = types[i]};
    }
}

static long long ownerExec(owner *o, client *c);

/* The part of processCommand() and call() that applies to a native command. top: the client's own
 * command; else a queued command run by ownerExec (call() from execCommand), which leaves lastcmd at
 * EXEC and the current client as it is. */
static void ownerCall(owner *o, client *c, struct serverCommand *cmd, int kind, int top) {
    if (top) {
        c->cmd = c->lastcmd = c->realcmd = cmd;
        ectx->current_client = c;
    }
    client *prev_client = ectx->executing_client;
    ectx->executing_client = c;
    ectx->in_call++;
    long long errors = ectx->stat_total_error_replies, inner_errors = 0;
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

    if (kind == OWN_EXEC)
        inner_errors = ownerExec(o, c);
    else
        cmd->proc(c);

    exitExecutionUnit();
    c->flag.executing_command = 0;
    ustime_t duration = (ustime_t)(getMonotonicUs() - start);
    /* Propagation as call() decides it; the ops (and a lazy-expire DEL's) are kept for the AOF. The
     * veto keeps every other target (replicas, a backlog, slot export) off. As call(), EXEC itself
     * never propagates: its queued commands did, into the same execution unit, and the top-level
     * EXEC keeps all their ops as one unit (wrapped in MULTI/EXEC when more than one, as
     * propagatePendingCommands does at the end of the stock EXEC). */
    dirty = ectx->dirty - dirty;
    if (!c->flag.prevent_prop && kind != OWN_EXEC) {
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
    if (top && ectx->also_propagate.numops) ownerTakeProp(o, c);
    ownerCmdStat *cs = &o->cs[kind];
    cs->calls++;
    cs->usec += duration;
    /* As call(): a queued command's errors count for that command, not for EXEC. */
    if (ectx->stat_total_error_replies - errors > inner_errors) cs->failed++;
    if (server.latency_tracking_enabled) updateCommandLatencyHistogram(&cs->hist, duration * 1000);
    c->duration = duration;
    void *entries[3];
    int types[3];
    ownerKeepLogs(o, entries, types, commandlogPrepareCurrentCommand(c, cmd, entries, types));
    c->duration = 0;
    c->commands_processed++;
    ectx->stat_numcommands++;
    o->native_total++;
    ectx->in_call--;
    ectx->executing_client = prev_client;
}

/* execCommand() on the owner (ownerNativeKind: no EXEC conditions, no watched keys, no veto, every
 * queued command a native one on this owner's keys). The queued commands all run inside this one
 * window: main never overlaps a window and no other owner touches these keys, so the transaction is
 * atomic and isolated. Returns the error replies of the queued commands (theirs, not EXEC's). */
static long long ownerExec(owner *o, client *c) {
    if (isWatchedKeyExpired(c)) c->flag.dirty_cas = 1;
    if (c->flag.dirty_cas || c->flag.dirty_exec) {
        if (c->flag.dirty_exec)
            addReplyErrorObject(c, shared.execaborterr);
        else
            addReply(c, shared.nullarray[c->resp]);
        discardTransaction(c);
        return 0;
    }
    struct ClientFlags old_flags = c->flag;
    c->flag.deny_blocking = 1;
    unwatchAllKeys(c);
    robj **orig_argv = c->argv;
    int orig_argc = c->argc, orig_argv_len = c->argv_len, orig_slot = c->slot;
    struct serverCommand *orig_cmd = c->cmd;
    long long inner_errors = 0;
    c->mstate->transaction_db_id = c->db->id;
    addReplyArrayLen(c, c->mstate->count);
    for (int j = 0; j < c->mstate->count; j++) {
        multiCmd *mc = c->mstate->commands + j;
        c->argc = mc->argc;
        c->argv = mc->argv;
        c->argv_len = mc->argv_len;
        c->cmd = c->realcmd = mc->cmd;
        c->slot = mc->slot; /* the keyspace lookups use the queued command's slot */
        long long e = ectx->stat_total_error_replies;
        ownerCall(o, c, mc->cmd, ownerKindOf(mc->cmd->proc), 0);
        inner_errors += ectx->stat_total_error_replies - e;
        /* Commands may alter argc/argv (MSET encodes its values): keep them in mstate. */
        mc->argc = c->argc;
        mc->argv = c->argv;
        mc->argv_len = c->argv_len;
        mc->cmd = c->cmd;
        freeClientOriginalArgv(c);
    }
    if (!old_flags.deny_blocking) c->flag.deny_blocking = 0;
    c->argv = orig_argv;
    c->argv_len = orig_argv_len;
    c->argc = orig_argc;
    c->cmd = c->realcmd = orig_cmd;
    c->slot = orig_slot;
    discardTransaction(c);
    return inner_errors;
}

/* processCommand()'s MULTI branch: queue the command and reply QUEUED (no call, no stats). */
static void ownerQueue(client *c) {
    c->cmd = c->lastcmd = c->realcmd = c->parsed_cmd;
    ectx->current_client = c;
    c->flag.buffered_reply = 0;
    queueMultiCommand(c, getCommandFlags(c));
    addReply(c, shared.queued);
}

/* ------------------------------------------------------------------------------------------------
 * GET runs: D0's fastget streaming ring (fastget.c fastGetSpeculateBatch) on the owner
 * ---------------------------------------------------------------------------------------------- */

typedef struct ownerRingEntry {
    hashtableIncrementalFindState find;
    int done;
} ownerRingEntry;

/* A commandlog type's threshold, or -1 while that log records nothing. */
static long long ownerLogThreshold(int type) {
    if (server.commandlog[type].threshold < 0 || server.commandlog[type].max_len == 0) return -1;
    return server.commandlog[type].threshold;
}

/* The value has an expire (D3-all-g8-k1): its expire field holds a time. An object can carry the field
 * with EXPIRY_NONE in it (createEmbeddedStringObjectWithKeyAndExpire adds one when the allocation has
 * room for it; PERSIST leaves it), which is no expire: lookupKey's expireIfNeeded finds the key valid.
 * The GET and MGET runs used to send every value with the field to getCommand / mgetCommand. */
static inline int ownerValHasExpire(const robj *val) {
    return val->hasexpire && objectGetExpire(val) != EXPIRY_NONE;
}

/* Reply to one GET from its completed find, with getCommand's effects (touch, hit or miss count,
 * the bulk or null reply). Returns 0, having done nothing, when getCommand must run it instead: a
 * key with an expire (it may have to be deleted and the delete propagated), a value that is not a
 * string (WRONGTYPE), or a reply the large-reply commandlog records. */
static int ownerGetReply(client *c, ownerRingEntry *e, long long reply_threshold) {
    void *found = NULL;
    if (!hashtableIncrementalFindGetResult(&e->find, &found)) {
        if (reply_threshold >= 0 && (c->resp == 2 ? 5 : 3) >= reply_threshold) return 0;
        ectx->stat_keyspace_misses++; /* the keymiss notification is vetoed while owners run */
        addReplyOrErrorObject(c, shared.null[c->resp]);
        return 1;
    }
    robj *val = found;
    /* shortcut: a key with an expire goes to getCommand even while it is valid; answer the valid
     * ones here (commandTimeSnapshot vs the expire) if TTL-heavy GET loads matter. */
    if (ownerValHasExpire(val) || objectGetType(val) != OBJ_STRING) return 0;
    if (reply_threshold >= 0) {
        size_t len = sdsEncodedObject(val) ? sdslen(objectGetVal(val)) : (size_t)sdigits10((long)objectGetVal(val));
        if ((long long)(1 + digits10(len) + 2 + len + 2) >= reply_threshold) return 0;
    }
    if (!c->flag.no_touch && !server.key_touch_paused) val->lru = lrulfu_touch(val->lru);
    ectx->stat_keyspace_hits++;
    if (sdsEncodedObject(val)) {
        size_t len = sdslen(objectGetVal(val));
        /* A large value by reference (ownerRefAdd), when addReplyBulk would add the reply at all. */
        if (ownerRefWanted(len) && prepareClientToWrite(c) == C_OK && ownerRefAdd(c, val, objectGetVal(val), len))
            return 1;
    }
    addReplyBulk(c, val);
    return 1;
}

/* The arguments of the k-th command of a run: the current one, then the queue from its offset. */
static void ownerRunArgs(client *c, int k, robj ***argv, int *argc, size_t *in) {
    if (k == 0) {
        *argv = c->argv, *argc = c->argc, *in = c->net_input_bytes_curr_cmd;
    } else {
        parsedCommand *p = &c->cmd_queue.cmds[c->cmd_queue.off + k - 1];
        *argv = p->argv, *argc = p->argc, *in = p->input_bytes;
    }
}

/* The start of a run of n commands (ownerGetRun, ownerMgetRun): call()'s state around them. */
static monotime ownerRunBegin(client *c, struct serverCommand *cmd) {
    c->cmd = c->lastcmd = c->realcmd = cmd;
    ectx->current_client = c;
    ectx->executing_client = c;
    ectx->in_call++;
    monotime start = getMonotonicUs();
    enterExecutionUnit(0, 0);
    c->flag.executing_command = 1;
    c->flag.buffered_reply = 0;
    c->flag.keyspace_notified = 0;
    return start;
}

/* Give a consumed command's argv back to the parser: a shared name + one key whole (the GET frame
 * template takes it back in one pop), anything else argument by argument. */
static inline void ownerRecycleArgs(robj **argv, int argc, int argv_len) {
    if (parseRecycleKeyArgv(argv, argc, argv_len)) return;
    for (int j = 0; j < argc; j++) parseRecycleArg(argv[j]);
    parseRecycleArgv(argv, argv_len);
}

/* The end of a run that answered the client's first n commands (all `cmd`, native kind `kind`): as in
 * fastget, they share the run's time in commandstats, the latency histogram and the slow commandlog
 * (one entry per command at the run's average; the large-request and -reply ones did not run here),
 * and their argvs go back to the parser's recycler. Returns n; 0 leaves the current command as it is. */
static int ownerRunEnd(owner *o, client *c, struct serverCommand *cmd, int kind, int n, monotime start) {
    cmdQueue *q = &c->cmd_queue;
    c->flag.executing_command = 0;
    c->net_output_bytes_curr_cmd = 0;
    exitExecutionUnit();
    long long usec = (long long)(getMonotonicUs() - start);
    if (n) {
        ownerCmdStat *cs = &o->cs[kind];
        cs->calls += n;
        cs->usec += usec;
        if (server.latency_tracking_enabled)
            updateCommandLatencyHistogramCount(&cs->hist, usec * 1000 / n > 0 ? usec * 1000 / n : 1, n);
        long long slow = ownerLogThreshold(COMMANDLOG_TYPE_SLOW);
        if (slow >= 0 && usec / n >= slow) {
            robj **saved_argv = c->argv;
            int saved_argc = c->argc;
            for (int k = 0; k < n; k++) {
                void *entries[3];
                int types[3];
                size_t in;
                ownerRunArgs(c, k, &c->argv, &c->argc, &in);
                c->duration = usec / n;
                c->net_input_bytes_curr_cmd = 0;
                ownerKeepLogs(o, entries, types, commandlogPrepareCurrentCommand(c, cmd, entries, types));
            }
            c->argv = saved_argv;
            c->argc = saved_argc;
            c->duration = 0;
        }
        /* Consume, as fastGetConsumeSpeculated: the owner's parser built these argvs. */
        ownerRecycleArgs(c->argv, c->argc, c->argv_len);
        c->argv = NULL;
        c->argc = 0;
        c->argv_len = 0;
        c->argv_len_sum = 0;
        for (int k = 1; k < n; k++) {
            parsedCommand *p = &q->cmds[q->off++];
            c->qb_applied += p->input_bytes;
            ownerRecycleArgs(p->argv, p->argc, p->argv_len);
            p->argv = NULL;
            p->argc = 0;
        }
        if (q->off == q->len) q->off = q->len = 0;
        c->commands_processed += n;
        ectx->stat_numcommands += n;
        o->native_total += n;
    }
    ectx->in_call--;
    ectx->executing_client = NULL;
    if (!n) return 0; /* nothing done: ownerCall runs the current command */
    commandProcessed(c); /* the reset after the run's last command (one reset per command is the same) */
    ectx->current_client = NULL;
    return n;
}

/* Owner: answer the client's current GET and the own-slot GETs queued behind it as one run. Up to
 * prefetch-batch-max-size key finds are in flight in a ring; each sweep steps every pending find
 * once (one prefetch each) and replies to the oldest commands whose finds completed, then tops the
 * ring up from the queue, so the next keys load while the current one is answered. A reply comes
 * from the find (no second lookup). The run ends before the first command that is not an own-slot
 * GET, that ownerGetReply leaves to getCommand, or that the large-request commandlog records; the
 * caller runs that one. As in fastget, the run's commands share its time in commandstats, the
 * latency histogram and the slow commandlog. Returns the number of commands answered and
 * consumed (their argvs go back to the parser's recycler); 0 leaves the current one untouched. */
static int ownerGetRun(owner *o, client *c) {
    long long req_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    long long reply_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (req_threshold >= 0 && (long long)c->net_input_bytes_curr_cmd >= req_threshold) return 0;
    hashtable *ht = kvstoreGetHashtable(c->db->keys, OWNER_TABLE_OF_SLOT(c->slot));
    if (!ht) return 0;
    unsigned depth = server.prefetch_batch_max_size < 1 ? 1 : (unsigned)server.prefetch_batch_max_size;
    if (depth > OWN_RING_MAX) depth = OWN_RING_MAX;
    ownerRingEntry ring[OWN_RING_MAX];
    struct serverCommand *cmd = c->parsed_cmd;
    cmdQueue *q = &c->cmd_queue;
    int next = q->off, open = 1;
    unsigned head = 0, tail = 1;
    ring[0].done = 0;
    hashtableIncrementalFindInit(&ring[0].find, ht, objectGetVal(c->argv[1]));

    monotime start = ownerRunBegin(c, cmd);
    for (;;) {
        /* Fill: top the ring up with the own-slot GETs queued behind. */
        while (open && tail - head < depth && next < q->len) {
            parsedCommand *p = &q->cmds[next];
            if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED) || !p->cmd || p->cmd->proc != getCommand ||
                (p->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) {
                open = 0;
                break;
            }
            if (p->slot < 0) p->slot = ownerKeySlot(p->cmd, p->argv, p->argc, p->read_flags);
            hashtable *pht = NULL;
            if (p->slot >= o->slot_lo && p->slot <= o->slot_hi &&
                (req_threshold < 0 || (long long)p->input_bytes < req_threshold))
                pht = kvstoreGetHashtable(c->db->keys, OWNER_TABLE_OF_SLOT(p->slot));
            if (!pht) {
                open = 0;
                break;
            }
            ownerRingEntry *e = &ring[tail % OWN_RING_MAX];
            e->done = 0;
            hashtableIncrementalFindInit(&e->find, pht, objectGetVal(p->argv[1]));
            tail++;
            next++;
        }
        if (next >= q->len) open = 0;
        if (head == tail) break;
        /* Sweep: one find step per pending entry. */
        for (unsigned s = head; s < tail; s++) {
            ownerRingEntry *e = &ring[s % OWN_RING_MAX];
            if (!e->done) e->done = !hashtableIncrementalFindStep(&e->find);
        }
        /* Drain: reply to the completed oldest entries, in command order. */
        while (head < tail && ring[head % OWN_RING_MAX].done) {
            c->net_output_bytes_curr_cmd = 0;
            if (!ownerGetReply(c, &ring[head % OWN_RING_MAX], reply_threshold)) {
                open = 0;
                tail = head; /* the rest go command by command */
                break;
            }
            head++;
            /* An output buffer limit closes the client: run nothing more, as processInputBuffer. */
            if (c->flag.close_asap) {
                open = 0;
                tail = head;
                break;
            }
        }
    }
    return ownerRunEnd(o, c, cmd, OWN_GET, (int)head, start);
}

/* ------------------------------------------------------------------------------------------------
 * MGET runs: the GET ring over every key of the own-slot MGETs (D3-all-g2)
 * ---------------------------------------------------------------------------------------------- */

#define OWN_MGET_REPLY_MAX 4096 /* an MGET reply gathered in one buffer; a longer one runs mgetCommand */

/* Gather one MGET's reply from its n completed finds (ring slots first..first+n-1) into buf in one
 * pass, with mgetCommand's effects: a touch and a keyspace hit per found string, a miss and a null
 * per missing key. Returns the reply's length, or 0, having changed nothing, when mgetCommand must
 * run it instead: a key with an expire (lazy expire stays in lookupKey), a value that is not a
 * string, a reply longer than buf or one the large-reply commandlog records. */
static size_t ownerMgetReply(client *c, ownerRingEntry *ring, unsigned first, int n, char *buf,
                             long long reply_threshold) {
    robj *vals[OWN_RING_MAX];
    char *w = buf, *end = buf + OWN_MGET_REPLY_MAX;
    *w++ = '*';
    w += ll2string(w, LONG_STR_SIZE, n);
    *w++ = '\r';
    *w++ = '\n';
    for (int i = 0; i < n; i++) {
        void *found = NULL;
        vals[i] = NULL;
        if (!hashtableIncrementalFindGetResult(&ring[(first + i) % OWN_RING_MAX].find, &found)) {
            if (end - w < 5) return 0;
            if (c->resp == 2) {
                memcpy(w, "$-1\r\n", 5);
                w += 5;
            } else {
                memcpy(w, "_\r\n", 3);
                w += 3;
            }
            continue;
        }
        robj *val = found;
        if (ownerValHasExpire(val) || objectGetType(val) != OBJ_STRING) return 0;
        char ibuf[LONG_STR_SIZE];
        const char *s = ibuf;
        size_t len;
        if (sdsEncodedObject(val)) {
            s = objectGetVal(val);
            len = sdslen((sds)s);
        } else {
            len = (size_t)ll2string(ibuf, sizeof(ibuf), (long)objectGetVal(val));
        }
        if ((size_t)(end - w) < 1 + LONG_STR_SIZE + 2 + len + 2) return 0;
        *w++ = '$';
        w += ll2string(w, LONG_STR_SIZE, (long long)len);
        *w++ = '\r';
        *w++ = '\n';
        memcpy(w, s, len);
        w += len;
        *w++ = '\r';
        *w++ = '\n';
        vals[i] = val;
    }
    size_t total = (size_t)(w - buf);
    if (reply_threshold >= 0 && (long long)total >= reply_threshold) return 0;
    int touch = !c->flag.no_touch && !server.key_touch_paused;
    for (int i = 0; i < n; i++) {
        if (!vals[i]) {
            ectx->stat_keyspace_misses++; /* the keymiss notification is vetoed while owners run */
            continue;
        }
        if (touch) vals[i]->lru = lrulfu_touch(vals[i]->lru);
        ectx->stat_keyspace_hits++;
    }
    return total;
}

/* The own-slot table of an MGET the run can take (its keys hash to one slot: ownerKeySlot), or NULL. */
static hashtable *ownerMgetTable(owner *o, client *c, int slot, int argc, size_t in, long long req_threshold) {
    if (argc - 1 > OWN_RING_MAX || slot < o->slot_lo || slot > o->slot_hi) return NULL;
    if (req_threshold >= 0 && (long long)in >= req_threshold) return NULL;
    return kvstoreGetHashtable(c->db->keys, OWNER_TABLE_OF_SLOT(slot));
}

/* Owner: answer the client's current MGET and the own-slot MGETs queued behind it as one run, with
 * ownerGetRun's ring over their keys: up to prefetch-batch-max-size key finds in flight (a whole
 * MGET always fits: one is let in alone when the ring is empty), each sweep steps every pending find
 * once, and the oldest MGET whose finds have all completed is answered in one pass from them
 * (ownerMgetReply, no second lookup), then the ring tops up with the next MGET's keys. The run ends
 * before the first command that is not an own-slot MGET of at most OWN_RING_MAX keys, that
 * ownerMgetReply leaves to mgetCommand, or that the large-request commandlog records; the caller
 * runs that one. Cross-owner and two-slot MGETs never get here (ownerKeySlot: main). Returns the
 * number of commands answered and consumed; 0 leaves the current one untouched. */
static int ownerMgetRun(owner *o, client *c) {
    long long req_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    long long reply_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    hashtable *ht = ownerMgetTable(o, c, c->slot, c->argc, c->net_input_bytes_curr_cmd, req_threshold);
    if (!ht) return 0;
    unsigned depth = server.prefetch_batch_max_size < 1 ? 1 : (unsigned)server.prefetch_batch_max_size;
    if (depth > OWN_RING_MAX) depth = OWN_RING_MAX;
    ownerRingEntry ring[OWN_RING_MAX];
    char buf[OWN_MGET_REPLY_MAX];
    struct serverCommand *cmd = c->parsed_cmd;
    cmdQueue *q = &c->cmd_queue;
    int next = q->off, open = 1;
    int added = 1, answered = 0;    /* commands of the run in the ring / answered */
    unsigned head = 0, tail = 0;    /* key finds: the answered MGETs' keys end at head */
    for (int j = 1; j < c->argc; j++, tail++) {
        ring[tail].done = 0;
        hashtableIncrementalFindInit(&ring[tail].find, ht, objectGetVal(c->argv[j]));
    }

    monotime start = ownerRunBegin(c, cmd);
    for (;;) {
        /* Fill: whole own-slot MGETs from the queue while their keys fit the ring's depth. */
        while (open && next < q->len) {
            parsedCommand *p = &q->cmds[next];
            if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED) || !p->cmd || p->cmd->proc != mgetCommand ||
                (p->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) {
                open = 0;
                break;
            }
            if (p->slot < 0) p->slot = ownerKeySlot(p->cmd, p->argv, p->argc, p->read_flags);
            hashtable *pht = ownerMgetTable(o, c, p->slot, p->argc, p->input_bytes, req_threshold);
            if (!pht) {
                open = 0;
                break;
            }
            if (tail > head && tail - head + (unsigned)(p->argc - 1) > depth) break; /* after the next answer */
            for (int j = 1; j < p->argc; j++, tail++) {
                ownerRingEntry *e = &ring[tail % OWN_RING_MAX];
                e->done = 0;
                hashtableIncrementalFindInit(&e->find, pht, objectGetVal(p->argv[j]));
            }
            added++;
            next++;
        }
        if (answered == added) break;
        /* Sweep: one find step per pending key. */
        for (unsigned s = head; s < tail; s++) {
            ownerRingEntry *e = &ring[s % OWN_RING_MAX];
            if (!e->done) e->done = !hashtableIncrementalFindStep(&e->find);
        }
        /* Drain: answer the oldest MGETs whose keys all completed, in command order. */
        while (answered < added) {
            robj **argv;
            int argc;
            size_t in;
            ownerRunArgs(c, answered, &argv, &argc, &in);
            int n = argc - 1, k = 0;
            while (k < n && ring[(head + k) % OWN_RING_MAX].done) k++;
            if (k < n) break;
            c->net_output_bytes_curr_cmd = 0;
            size_t len = ownerMgetReply(c, ring, head, n, buf, reply_threshold);
            if (!len) {
                open = 0;
                added = answered; /* the rest go command by command */
                break;
            }
            addReplyProto(c, buf, len);
            head += (unsigned)n;
            answered++;
            /* An output buffer limit closes the client: run nothing more, as processInputBuffer. */
            if (c->flag.close_asap) {
                open = 0;
                added = answered;
                break;
            }
        }
    }
    return ownerRunEnd(o, c, cmd, OWN_MGET, answered, start);
}

/* ------------------------------------------------------------------------------------------------
 * GET bursts: the GET ring straight off the query buffer (D3-all-g8-k1)
 * ---------------------------------------------------------------------------------------------- */

/* A read that starts with GET frames answers them before anything is parsed: each frame is matched
 * in the query buffer, its key copied into a ring entry and its find started; the ring is ownerGetRun's
 * (finds in flight, a sweep, replies in command order), and each reply is formatted straight into
 * the client's buffer. No argv, no command lookup, no per-command reset: one run's accounting per
 * burst. A burst takes exactly what ownerTryNative would give ownerGetRun (parseOwnerFrame's GET
 * frame, getCommand under its own name, the key in this owner's slots, no large-request or
 * large-reply log, no expire, a string) and ends before the first frame it cannot answer; the parser
 * then takes that frame and the rest of the buffer as before, so the commands run in order and the
 * replies keep it. The client state a burst needs (ownerBurstReady) is the state ownerNativeKind and
 * resetClient leave untouched over a GET; anything else parses as before.
 * shortcut: a burst starts only where a read starts at a frame (pipelines that fit one read, as K1's);
 * a read that starts inside a frame (a pipeline longer than one read) parses as before. Upgrade: let
 * the parser complete that frame, run it, then burst the rest. */
#define OWN_BURST_MAX 256    /* GETs one burst answers; the next burst picks up after them */
#define OWN_BURST_KEY_MAX 64 /* a longer key ends the burst: the frame parser takes it */

/* A burst frame's key, copied out of the query buffer as an sds8 for its find, NUL padded to whole
 * 8-byte words (ownerBurstSlot reads them). */
typedef struct __attribute__((__packed__)) ownerBurstKey {
    uint8_t len, alloc;
    unsigned char flags;
    char buf[OWN_BURST_KEY_MAX + 8];
} ownerBurstKey;

typedef struct ownerBurstEntry {
    hashtableIncrementalFindState find;
    int done;
    int absent;   /* the key's table does not exist: a miss, no find */
    uint32_t end; /* where its frame ends, from the burst's start in the query buffer */
    ownerBurstKey key;
} ownerBurstEntry;

/* Where an answered GET's name and key are in the query buffer: its slow commandlog entry. */
typedef struct ownerBurstArg {
    uint32_t key_off;
    uint8_t key_len, lower;
} ownerBurstArg;

/* The client can take a burst now: a complete command boundary in a clean multibulk state, and
 * everything ownerNativeKind checks for a GET, plus the reply and reset state a GET leaves as it is
 * (resetClient clears ASKING and CACHING after a command and steps CLIENT REPLY SKIP: a client with
 * any of them parses as before). */
static int ownerBurstReady(client *c) {
#ifdef LOG_REQ_RES
    return 0; /* the request/response log records each reply's offset */
#endif
    if (!own_burst_cmd || (own_mm && !own_self->mm_roomy) ||
        (atomic_load_explicit(&own_veto, memory_order_relaxed) & OWN_VETO_ALL))
        return 0;
    if (c->argc || c->multibulklen || c->cmd_queue.len || c->reqtype == PROTO_REQ_INLINE || c->original_argv)
        return 0;
    if (c->read_flags & (READ_FLAGS_AUTH_REQUIRED | READ_FLAGS_REPLICATED)) return 0;
    if (!c->querybuf || c->qb_pos >= sdslen(c->querybuf) || c->querybuf[c->qb_pos] != '*') return 0;
    if (!canParseCommand(c) || c->flag.multi || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next ||
        c->flag.asking || c->flag.tracking_caching || c->flag.pushing || c->flag.primary || c->flag.fake ||
        c->flag.script || c->flag.module || c->flag.executor || c->deferred_reply_errors)
        return 0;
    if (c->io_write_state == CLIENT_COMPLETED_IO || isDeferredReplyEnabled(c) || getClientType(c) != CLIENT_TYPE_NORMAL)
        return 0;
    if ((c->user && (c->user->flags & USER_FLAG_RETIRED)) || authRequired(c) || !ACLUserUnrestricted(c->user))
        return 0;
    return 1;
}

/* The GET frame at b, as parseOwnerFrame takes it: "*2\r\n$3\r\nGET\r\n$<n>\r\n<key>\r\n" with the name all
 * upper or all lower case and 1 <= n <= key_max (a short length header: no sign, no leading zero),
 * complete in the avail bytes there. Returns its length and sets the key's offset and length and the
 * spelling; 0 for anything else. */
static inline size_t ownerBurstFrame(const char *b, size_t avail, size_t key_max, size_t *key_off, size_t *key_len,
                                     int *lower) {
    if (avail < 20) return 0; /* the shortest: a 1-byte key */
    /* "*2\r\n$3\r\n" then "GET\r" or "get\r" then "\n", as word compares */
    uint64_t head, head_want;
    uint32_t name, name_up, name_low;
    memcpy(&head, b, 8);
    memcpy(&head_want, "*2\r\n$3\r\n", 8);
    memcpy(&name, b + 8, 4);
    memcpy(&name_up, "GET\r", 4);
    memcpy(&name_low, "get\r", 4);
    if (head != head_want || b[12] != '\n') return 0;
    if (name == name_up)
        *lower = 0;
    else if (name == name_low)
        *lower = 1;
    else
        return 0;
    if (b[13] != '$' || b[14] < '1' || b[14] > '9') return 0;
    size_t len = (size_t)(b[14] - '0'), pos = 15;
    if (b[15] >= '0' && b[15] <= '9') {
        len = len * 10 + (size_t)(b[15] - '0');
        pos = 16;
    }
    if (len > key_max || avail < pos + 2 + len + 2 || b[pos] != '\r' || b[pos + 1] != '\n') return 0;
    pos += 2;
    if (b[pos + len] != '\r' || b[pos + len + 1] != '\n') return 0;
    *key_off = pos;
    *key_len = len;
    return pos + len + 2;
}

/* keyHashSlot of a burst key: the 8-byte CRC16 unless a '{' is in it (whole words: the padding is NUL),
 * then ownerKeyHashSlot's hash tag rules. */
static inline int ownerBurstSlot(const ownerBurstKey *k) {
    const uint64_t ones = 0x0101010101010101ULL, highs = 0x8080808080808080ULL, brace = ones * (uint64_t)'{';
    for (size_t i = 0; i < k->len; i += 8) {
        uint64_t w;
        memcpy(&w, k->buf + i, 8);
        w ^= brace;
        if ((w - ones) & ~w & highs) return ownerKeyHashSlot(k->buf, k->len);
    }
    return ownerCrc16((const unsigned char *)k->buf, k->len) & 0x3FFF;
}

/* need bytes of reply fit straight into c->buf, where _addReplyToBufferOrList would put them: nothing in
 * the reply list, and the buffer plain (an owner client's always is: isCopyAvoidPreferred). */
static inline int ownerBurstRoom(client *c, size_t need) {
    if (listLength(c->reply) || server.debug_client_enforce_reply_list) return 0;
    if (!c->bufpos)
        c->flag.buf_encoded = 0; /* as _addReplyToBuffer starts a buffer for an owner client */
    else if (c->flag.buf_encoded)
        return 0;
    return c->buf_usable_size - c->bufpos >= need;
}

static inline void ownerBurstWrote(client *c, size_t n) {
    c->bufpos += n;
    if (c->buf_peak < (size_t)c->bufpos) c->buf_peak = (size_t)c->bufpos;
    c->flag.buffered_reply = 1; /* as prepareClientToWrite for an owner client */
}

/* ownerGetReply for a burst entry: getCommand's effects and its reply bytes (the bulk or the null),
 * formatted into c->buf, or through addReplyBulk / addReplyOrErrorObject when they do not fit there.
 * Returns 0, having done nothing, where ownerGetReply would. */
static int ownerBurstReply(client *c, ownerBurstEntry *e, long long reply_threshold) {
    void *found = NULL;
    if (e->absent || !hashtableIncrementalFindGetResult(&e->find, &found)) {
        size_t n = c->resp == 2 ? 5 : 3;
        if (reply_threshold >= 0 && (long long)n >= reply_threshold) return 0;
        ectx->stat_keyspace_misses++; /* the keymiss notification is vetoed while owners run */
        if (ownerBurstRoom(c, n)) {
            memcpy(c->buf + c->bufpos, c->resp == 2 ? "$-1\r\n" : "_\r\n", n);
            ownerBurstWrote(c, n);
        } else {
            addReplyOrErrorObject(c, shared.null[c->resp]);
        }
        return 1;
    }
    robj *val = found;
    if (ownerValHasExpire(val) || objectGetType(val) != OBJ_STRING) return 0;
    char ibuf[LONG_STR_SIZE];
    const char *s;
    size_t len;
    if (sdsEncodedObject(val)) {
        s = objectGetVal(val);
        len = sdslen((const_sds)s);
    } else {
        len = (size_t)ll2string(ibuf, sizeof(ibuf), (long)objectGetVal(val));
        s = ibuf;
    }
    if (reply_threshold >= 0 && (long long)(1 + digits10(len) + 2 + len + 2) >= reply_threshold) return 0;
    if (!c->flag.no_touch && !server.key_touch_paused) val->lru = lrulfu_touch(val->lru);
    ectx->stat_keyspace_hits++;
    if (ownerRefWanted(len) && ownerRefAdd(c, val, s, len)) return 1; /* a large value: by reference */
    if (!ownerBurstRoom(c, 1 + LONG_STR_SIZE + 2 + len + 2)) {
        addReplyBulk(c, val);
        return 1;
    }
    char *w = c->buf + c->bufpos, *w0 = w;
    *w++ = '$';
    if (len < 10) {
        *w++ = (char)('0' + len);
    } else if (len < 100) {
        *w++ = (char)('0' + len / 10);
        *w++ = (char)('0' + len % 10);
    } else {
        w += ll2string(w, LONG_STR_SIZE, (long long)len);
    }
    *w++ = '\r';
    *w++ = '\n';
    memcpy(w, s, len);
    w += len;
    *w++ = '\r';
    *w++ = '\n';
    ownerBurstWrote(c, (size_t)(w - w0));
    return 1;
}

/* A burst's call() state, as ownerRunBegin without the command: the client keeps no command until the
 * burst answered one (ownerBurstEnd), so a burst that answers none leaves it as it found it. */
static monotime ownerBurstBegin(client *c) {
    ectx->current_client = c;
    ectx->executing_client = c;
    ectx->in_call++;
    monotime start = getMonotonicUs();
    enterExecutionUnit(0, 0);
    c->flag.executing_command = 1;
    c->flag.buffered_reply = 0;
    c->flag.keyspace_notified = 0;
    return start;
}

/* ownerRunEnd for a burst that answered n GETs: they share its time in commandstats, the latency
 * histogram and the slow commandlog (one entry per command at the average, its arguments taken from the
 * frame still in the query buffer at qb), then the one reset after the last of them. */
static void ownerBurstEnd(owner *o, client *c, int n, monotime start, const ownerBurstArg *args, const char *qb) {
    c->flag.executing_command = 0;
    c->net_output_bytes_curr_cmd = 0;
    exitExecutionUnit();
    long long usec = (long long)(getMonotonicUs() - start);
    if (n) {
        ownerCmdStat *cs = &o->cs[OWN_GET];
        cs->calls += n;
        cs->usec += usec;
        if (server.latency_tracking_enabled)
            updateCommandLatencyHistogramCount(&cs->hist, usec * 1000 / n > 0 ? usec * 1000 / n : 1, n);
        long long slow = ownerLogThreshold(COMMANDLOG_TYPE_SLOW);
        if (slow >= 0 && usec / n >= slow) {
            for (int k = 0; k < n; k++) {
                void *entries[3];
                int types[3];
                robj *argv[2] = {own_burst_name[args[k].lower], createStringObject(qb + args[k].key_off, args[k].key_len)};
                c->argv = argv;
                c->argc = 2;
                c->duration = usec / n;
                c->net_input_bytes_curr_cmd = 0;
                ownerKeepLogs(o, entries, types, commandlogPrepareCurrentCommand(c, own_burst_cmd, entries, types));
                decrRefCount(argv[1]);
            }
            c->argv = NULL;
            c->argc = 0;
            c->duration = 0;
        }
        c->commands_processed += n;
        ectx->stat_numcommands += n;
        o->native_total += n;
        o->burst_total += n;
        o->bursts_total++;
    }
    ectx->in_call--;
    ectx->executing_client = NULL;
    if (n) {
        c->cmd = c->lastcmd = c->realcmd = own_burst_cmd;
        commandProcessed(c); /* the reset after the burst's last command */
    }
    ectx->current_client = NULL;
}

/* Owner, after a read (ownerBurstReady): answer the GET frames at the start of the unparsed input, up to
 * OWN_BURST_MAX, and step the parser position past them. Returns how many it answered. */
static int ownerGetBurst(owner *o, client *c) {
    /* shortcut: none under maxmemory (a burst's replies are not bounded against the lease; as the GET
     * runs, ownerTryNative). Upgrade: check each reply's value length against the lease first. */
    if (own_mm) return 0;
    long long req_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    long long reply_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    size_t key_max = OWN_BURST_KEY_MAX;
    if ((long long)key_max > server.proto_max_bulk_len) key_max = (size_t)server.proto_max_bulk_len;
    unsigned depth = server.prefetch_batch_max_size < 1 ? 1 : (unsigned)server.prefetch_batch_max_size;
    if (depth > OWN_RING_MAX) depth = OWN_RING_MAX;
    ownerBurstEntry ring[OWN_RING_MAX];
    ownerBurstArg args[OWN_BURST_MAX];
    const char *qb = c->querybuf + c->qb_pos;
    size_t avail = sdslen(c->querybuf) - c->qb_pos, pos = 0, done = 0;
    kvstore *keys = c->db->keys;
    int open = 1, added = 0, answered = 0;
    unsigned head = 0, tail = 0;
    monotime start = 0;
    for (;;) {
        /* Fill: the next frames, while they are own-slot GETs. */
        while (open && tail - head < depth && added < OWN_BURST_MAX) {
            size_t koff = 0, klen = 0;
            int lower = 0;
            size_t flen = ownerBurstFrame(qb + pos, avail - pos, key_max, &koff, &klen, &lower);
            if (!flen || (req_threshold >= 0 && (long long)flen >= req_threshold)) {
                open = 0;
                break;
            }
            ownerBurstEntry *e = &ring[tail % OWN_RING_MAX];
            const char *k = qb + pos + koff;
            if (pos + koff + klen + 8 <= avail) {
                /* whole words, all in the buffer: the bytes past the key are overwritten next */
                for (size_t i = 0; i < klen; i += 8) memcpy(e->key.buf + i, k + i, 8);
            } else {
                memcpy(e->key.buf, k, klen);
            }
            memset(e->key.buf + klen, 0, 8);
            e->key.len = e->key.alloc = (uint8_t)klen;
            e->key.flags = SDS_TYPE_8;
            int slot = ownerBurstSlot(&e->key);
            if (slot < o->slot_lo || slot > o->slot_hi) {
                open = 0;
                break;
            }
            /* No table for the slot: no key in it, a miss as lookupKey finds it (getCommand's reply). */
            hashtable *ht = kvstoreGetHashtable(keys, OWNER_TABLE_OF_SLOT(slot));
            if (!added) start = ownerBurstBegin(c);
            e->end = (uint32_t)(pos + flen);
            e->absent = ht == NULL;
            e->done = e->absent;
            if (ht) hashtableIncrementalFindInit(&e->find, ht, e->key.buf);
            args[added] = (ownerBurstArg){.key_off = (uint32_t)(pos + koff), .key_len = (uint8_t)klen, .lower = (uint8_t)lower};
            tail++;
            added++;
            pos += flen;
        }
        if (head == tail) break;
        /* Sweep: one find step per pending entry. */
        for (unsigned s = head; s < tail; s++) {
            ownerBurstEntry *e = &ring[s % OWN_RING_MAX];
            if (!e->done) e->done = !hashtableIncrementalFindStep(&e->find);
        }
        /* Drain: reply to the completed oldest entries, in command order. */
        while (head < tail && ring[head % OWN_RING_MAX].done) {
            ownerBurstEntry *e = &ring[head % OWN_RING_MAX];
            if (!ownerBurstReply(c, e, reply_threshold)) {
                open = 0;
                tail = head; /* this frame and the rest go to the parser */
                break;
            }
            done = e->end;
            head++;
            answered++;
            /* An output buffer limit closes the client: run nothing more, as processInputBuffer. */
            if (c->flag.close_asap) {
                open = 0;
                tail = head;
                break;
            }
        }
    }
    if (!added) return 0;
    c->qb_pos += done;
    c->qb_applied = c->qb_pos;
    ownerBurstEnd(o, c, answered, start, args, qb);
    return answered;
}

/* ------------------------------------------------------------------------------------------------
 * SET bursts: plain SET frames straight off the query buffer (D3-all-g10-setburst)
 * ---------------------------------------------------------------------------------------------- */

/* The write side of the GET bursts. A read whose unparsed input starts with plain "SET key value" frames
 * (three arguments, no option) runs them before anything is parsed: each frame is matched in the query
 * buffer, its key copied out as an sds8 and its slot hashed; the keys' finds run as one ring (prefetch
 * only: every find ends before the first write, which may resize a table); then the SETs run in order,
 * each as setCommand would run it (setKeyPlainOrFail: setGenericCommand from lookupKeyWrite to setKey,
 * on the store-ready value the parser would build), each one's AOF record appended to this owner's
 * records as ownerTakeProp appends it (the frame's own bytes: catAppendOnlyGenericCommand writes exactly
 * those for this argv), and each +OK written into c->buf. No argv, no command lookup, no alsoPropagate,
 * no per-command reset: one run's accounting per burst (ownerSetBurstEnd, as ownerBurstEnd).
 * A burst takes only what ownerNativeKind would run as OWN_SET with no veto of any kind (ownerBurstReady
 * and ownerSetBurstReady: no MULTI, no reply mode, an unrestricted user, no notifications, tracking,
 * replicas, modules, disk error; under maxmemory only in a window whose memory lease has room, each SET
 * checked against the lease before it allocates) and ends before the first frame it cannot run: another
 * command or a SET with options, a key over OWN_BURST_KEY_MAX bytes, a value over OWN_SETBURST_VAL_MAX,
 * another owner's key, a value the parser would not prebuild (an integer, too long to embed), a request
 * the large-request commandlog records, an expired key (lookupKeyWrite's lazy delete propagates). The
 * parser takes that frame and the rest as before, so the commands run in order and the replies keep it. */
#define OWN_SETBURST_MAX OWN_RING_MAX /* SETs one burst runs; the next burst picks up after them */
#define OWN_SETBURST_VAL_MAX 99       /* a value length of one or two digits */

typedef struct ownerSetEntry {
    hashtableIncrementalFindState find;
    int done;
    int slot;
    hashtable *ht;                 /* the key's table, NULL when there is none yet (no find) */
    uint32_t frame_off, frame_len; /* from the burst's start in the query buffer */
    uint32_t val_off;
    uint8_t val_len, lower;
    ownerBurstKey key;
} ownerSetEntry;

/* The SET frame at b, "*3\r\n$3\r\nSET\r\n$<k>\r\n<key>\r\n$<v>\r\n<value>\r\n" with the name all upper or all
 * lower case, 1 <= k <= key_max and 1 <= v <= val_max (val_max <= 99) in short length headers (no sign,
 * no leading zero), complete in the avail bytes there. Returns its length and sets the key's and the
 * value's offsets and lengths and the spelling; 0 for anything else. */
static inline size_t ownerSetFrame(const char *b, size_t avail, size_t key_max, size_t val_max, size_t *key_off,
                                   size_t *key_len, size_t *val_off, size_t *val_len, int *lower) {
    if (avail < 27) return 0; /* the shortest: a 1-byte key and a 1-byte value */
    uint64_t head, head_want;
    uint32_t name, name_up, name_low;
    memcpy(&head, b, 8);
    memcpy(&head_want, "*3\r\n$3\r\n", 8);
    memcpy(&name, b + 8, 4);
    memcpy(&name_up, "SET\r", 4);
    memcpy(&name_low, "set\r", 4);
    if (head != head_want || b[12] != '\n') return 0;
    if (name == name_up)
        *lower = 0;
    else if (name == name_low)
        *lower = 1;
    else
        return 0;
    /* the key: b[13..15] are in the 27 bytes */
    if (b[13] != '$' || b[14] < '1' || b[14] > '9') return 0;
    size_t klen = (size_t)(b[14] - '0'), pos = 15;
    if (b[15] >= '0' && b[15] <= '9') {
        klen = klen * 10 + (size_t)(b[15] - '0');
        pos = 16;
    }
    if (klen > key_max || avail < pos + 2 + klen + 2 + 4) return 0;
    if (b[pos] != '\r' || b[pos + 1] != '\n') return 0;
    size_t koff = pos + 2;
    pos = koff + klen;
    if (b[pos] != '\r' || b[pos + 1] != '\n') return 0;
    pos += 2;
    /* the value: pos + 4 <= avail, so its header's first three bytes are there */
    if (b[pos] != '$' || b[pos + 1] < '1' || b[pos + 1] > '9') return 0;
    size_t vlen = (size_t)(b[pos + 1] - '0');
    pos += 2;
    if (b[pos] >= '0' && b[pos] <= '9') {
        vlen = vlen * 10 + (size_t)(b[pos] - '0');
        pos++;
    }
    if (vlen > val_max || avail < pos + 2 + vlen + 2) return 0;
    if (b[pos] != '\r' || b[pos + 1] != '\n') return 0;
    size_t voff = pos + 2;
    pos = voff + vlen;
    if (b[pos] != '\r' || b[pos + 1] != '\n') return 0;
    *key_off = koff;
    *key_len = klen;
    *val_off = voff;
    *val_len = vlen;
    return pos + 2;
}

/* What a SET burst needs beyond ownerBurstReady: the burst on, setCommand under its own name, no veto at
 * all (a write veto sends SETs to main), the parser's value prebuild on (the burst builds that value),
 * and nothing that drops the record's AOF or REPL target (ownerCall's propagation flags). */
static inline int ownerSetBurstReady(client *c) {
    return own_setburst_cmd && server.owner_set_burst && !parse_defer_value_prebuild &&
           !atomic_load_explicit(&own_veto, memory_order_relaxed) && !c->flag.prevent_aof_prop &&
           !c->flag.prevent_repl_prop && !c->flag.module_prevent_aof_prop && !c->flag.module_prevent_repl_prop;
}

/* ownerBurstEnd for a SET burst that ran n SETs: their commandstats, latency histogram and slow
 * commandlog entries (one per command at the average, its arguments from the entry's key and the value
 * still in the query buffer at qb), the burst counts, then the one reset after the last of them. */
static void ownerSetBurstEnd(owner *o, client *c, int n, monotime start, const ownerSetEntry *ents, const char *qb) {
    c->flag.executing_command = 0;
    c->net_output_bytes_curr_cmd = 0;
    exitExecutionUnit();
    long long usec = (long long)(getMonotonicUs() - start);
    if (n) {
        ownerCmdStat *cs = &o->cs[OWN_SET];
        cs->calls += n;
        cs->usec += usec;
        if (server.latency_tracking_enabled)
            updateCommandLatencyHistogramCount(&cs->hist, usec * 1000 / n > 0 ? usec * 1000 / n : 1, n);
        long long slow = ownerLogThreshold(COMMANDLOG_TYPE_SLOW);
        if (slow >= 0 && usec / n >= slow) {
            for (int k = 0; k < n; k++) {
                const ownerSetEntry *e = &ents[k];
                void *entries[3];
                int types[3];
                robj *argv[3] = {own_setburst_name[e->lower], createStringObject(e->key.buf, e->key.len),
                                 createStringObject(qb + e->val_off, e->val_len)};
                c->argv = argv;
                c->argc = 3;
                c->duration = usec / n;
                c->net_input_bytes_curr_cmd = 0;
                ownerKeepLogs(o, entries, types, commandlogPrepareCurrentCommand(c, own_setburst_cmd, entries, types));
                decrRefCount(argv[1]);
                decrRefCount(argv[2]);
            }
            c->argv = NULL;
            c->argc = 0;
            c->duration = 0;
        }
        c->commands_processed += n;
        ectx->stat_numcommands += n;
        o->native_total += n;
        o->setburst_total += n;
        o->setbursts_total++;
    }
    ectx->in_call--;
    ectx->executing_client = NULL;
    if (n) {
        c->cmd = c->lastcmd = c->realcmd = own_setburst_cmd;
        commandProcessed(c); /* the reset after the burst's last command */
    }
    ectx->current_client = NULL;
}

/* Owner, after a read (ownerBurstReady, ownerSetBurstReady): run the plain SET frames at the start of the
 * unparsed input, up to OWN_SETBURST_MAX, and step the parser position past them. Returns how many ran. */
static int ownerSetBurst(owner *o, client *c) {
    long long req_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    long long reply_threshold = ownerLogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && 5 >= reply_threshold) return 0; /* the large-reply log records +OK */
    size_t key_max = OWN_BURST_KEY_MAX, val_max = OWN_SETBURST_VAL_MAX;
    if ((long long)key_max > server.proto_max_bulk_len) key_max = (size_t)server.proto_max_bulk_len;
    if ((long long)val_max > server.proto_max_bulk_len) val_max = (size_t)server.proto_max_bulk_len;
    unsigned depth = server.prefetch_batch_max_size < 1 ? 1 : (unsigned)server.prefetch_batch_max_size;
    if (depth > OWN_RING_MAX) depth = OWN_RING_MAX;
    ownerSetEntry ents[OWN_SETBURST_MAX];
    const char *qb = c->querybuf + c->qb_pos;
    size_t avail = sdslen(c->querybuf) - c->qb_pos, pos = 0;
    kvstore *keys = c->db->keys;
    int n = 0;
    /* Match: the leading frames, while they are plain SETs of this owner's keys. */
    while (n < OWN_SETBURST_MAX) {
        size_t koff = 0, klen = 0, voff = 0, vlen = 0;
        int lower = 0;
        size_t flen = ownerSetFrame(qb + pos, avail - pos, key_max, val_max, &koff, &klen, &voff, &vlen, &lower);
        if (!flen || (req_threshold >= 0 && (long long)flen >= req_threshold)) break;
        ownerSetEntry *e = &ents[n];
        const char *k = qb + pos + koff;
        if (pos + koff + klen + 8 <= avail) {
            /* whole words, all in the buffer: the bytes past the key are overwritten next */
            for (size_t i = 0; i < klen; i += 8) memcpy(e->key.buf + i, k + i, 8);
        } else {
            memcpy(e->key.buf, k, klen);
        }
        memset(e->key.buf + klen, 0, 8);
        e->key.len = e->key.alloc = (uint8_t)klen;
        e->key.flags = SDS_TYPE_8;
        int slot = ownerBurstSlot(&e->key);
        if (slot < o->slot_lo || slot > o->slot_hi) break;
        e->slot = slot;
        e->ht = kvstoreGetHashtable(keys, OWNER_TABLE_OF_SLOT(slot));
        e->frame_off = (uint32_t)pos;
        e->frame_len = (uint32_t)flen;
        e->val_off = (uint32_t)(pos + voff);
        e->val_len = (uint8_t)vlen;
        e->lower = (uint8_t)lower;
        n++;
        pos += flen;
    }
    if (!n) return 0;
    /* Prefetch: the finds as one ring, depth in flight, each run to its end before anything is written
     * (a write may add a key and resize a table under a find). Their results are not used. */
    unsigned head = 0, tail = 0;
    while (head < (unsigned)n) {
        while (tail < (unsigned)n && tail - head < depth) {
            ownerSetEntry *e = &ents[tail++];
            e->done = e->ht == NULL;
            if (e->ht) hashtableIncrementalFindInit(&e->find, e->ht, e->key.buf);
        }
        for (unsigned s = head; s < tail; s++) {
            ownerSetEntry *e = &ents[s];
            if (!e->done) e->done = !hashtableIncrementalFindStep(&e->find);
        }
        while (head < tail && ents[head].done) head++;
    }
    /* Run: each SET in order, as setCommand. */
    int saved_slot = c->slot, ran = 0;
    size_t done = 0;
    monotime start = ownerBurstBegin(c);
    for (int k = 0; k < n; k++) {
        ownerSetEntry *e = &ents[k];
        /* Under maxmemory (a memory lease with room): this SET must fit as on the parser's path, before it
         * allocates; else the burst ends here and the parser's path sends it to main (ownerNativeKind). */
        if (own_mm && !ownerMmSetBurstFits(o, 3 + (size_t)e->key.len + e->val_len, ran)) break;
        /* The parser's value for this frame: built only when it would build it (else setCommand runs it). */
        robj *val = tryCreatePrebuiltStringEntry(qb + e->val_off, e->val_len, e->key.buf);
        if (!val) break;
        robj key;
        initStaticStringObject(key, e->key.buf);
        c->slot = e->slot;
        if (!setKeyPlainOrFail(c, c->db, &key, &val, OWNER_TABLE_OF_SLOT(e->slot))) {
            decrRefCount(val);
            break;
        }
        ectx->dirty++;
        if (own_keep) {
            if (!ran) ownerAofSelect(o, c->db->id);
            if (own_rec_fmt == AOF_FMT_COMPACT) /* vkevo binaof: the same SET as a compact record */
                o->aofbuf = aofCatCompactSet(o->aofbuf, e->lower, e->key.buf, e->key.len, qb + e->val_off, e->val_len);
            else
                o->aofbuf = sdscatlen(o->aofbuf, qb + e->frame_off, e->frame_len);
            /* D3-all-g14-fold: the stream gets the frame's RESP bytes as they came in */
            if (own_split) o->replbuf = sdscatlen(o->replbuf, qb + e->frame_off, e->frame_len);
        }
        if (ownerBurstRoom(c, 5)) {
            memcpy(c->buf + c->bufpos, "+OK\r\n", 5);
            ownerBurstWrote(c, 5);
        } else {
            addReply(c, shared.ok);
        }
        ran++;
        done = e->frame_off + e->frame_len;
        /* An output buffer limit closes the client: run nothing more, as processInputBuffer. */
        if (c->flag.close_asap) break;
    }
    if (ran && own_keep) {
        /* ownerTakeProp's unit for these records: each SET one op with the AOF and REPL targets. */
        if (o->nunits == o->units_cap) {
            o->units_cap = o->units_cap ? o->units_cap * 2 : 256;
            o->units = zrealloc(o->units, sizeof(ownerPropUnit) * o->units_cap);
        }
        o->units[o->nunits++] =
            (ownerPropUnit){.c = c, .n = ran, .repl = ran, .end = sdslen(o->aofbuf), .rend = sdslen(o->replbuf)};
        o->unflushed = 1;
        if (!own_aof_drain) o->wake_main = 1; /* else this owner writes them at the end of its window */
    }
    if (!ran) c->slot = saved_slot;
    c->qb_pos += done;
    c->qb_applied = c->qb_pos;
    ownerSetBurstEnd(o, c, ran, start, ents, qb);
    return ran;
}

/* ownerRunParsedInput() callback: run the client's current command here if it is native. */
static int ownerTryNative(client *c) {
    owner *o = own_self;
    if (c->slot < 0) c->slot = ownerKeySlot(c->parsed_cmd, c->argv, c->argc, c->read_flags);
    int kind = ownerNativeKind(o, c);
    if (kind < 0) return 0;
    if (c->flag.multi && kind != OWN_EXEC && kind != OWN_DISCARD) {
        /* Queued, not run: a GET inside MULTI is no GET run. */
        ownerPrefetch(o, c);
        ownerQueue(c);
    } else {
        /* shortcut: under maxmemory GETs and MGETs run one by one (ownerNativeKind bounds each reply);
         * a ring run answers up to prefetch-batch-max-size keys' commands with no headroom check
         * between them. Upgrade: check each ring reply's value length against the headroom first. */
        if (kind == OWN_GET && !own_mm && ownerGetRun(o, c)) return 1;
        if (kind == OWN_MGET && !own_mm && ownerMgetRun(o, c)) return 1;
        ownerPrefetch(o, c);
        ownerCall(o, c, c->parsed_cmd, kind, 1);
        /* EXEC's bound is a sum over its queued commands: should it still have gone over, every
         * owner sends its work to main until main has looked, as after an oversized read. */
        if (own_mm && kind == OWN_EXEC) ownerMmAfterRead(o);
    }
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

/* ownerRead before its read: returns 1 when the client left the owner, 0 when it stays unread,
 * OWN_READ_GO to read it. */
#define OWN_READ_GO (-1)
static int ownerReadPre(owner *o, client *c) {
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
    return OWN_READ_GO;
}

/* ownerRead after its read (c->nread as readToQueryBuf left it): parse, run, reply. With batch set
 * (ownerReadBatch) a reply that may go out now is queued for the batch's writes instead. Returns 1
 * when the client left the owner. */
static int ownerReadPost(owner *o, client *c, int batch) {
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
    /* GET frames at the start of the input: answered off the buffer (ownerGetBurst); what follows the
     * last of them is parsed and run as before. The burst's GETs count toward owner_deep too. */
    long long ran = o->native_total;
    /* Plain SET frames likewise (ownerSetBurst): GET and SET bursts take turns while either runs one. */
    while (ownerBurstReady(c)) {
        int g = ownerGetBurst(o, c), s = 0;
        if (c->flag.close_asap || c->qb_pos >= sdslen(c->querybuf)) break;
        if (g == OWN_BURST_MAX) continue;
        if (ownerSetBurstReady(c)) s = ownerSetBurst(o, c);
        if (!s || c->flag.close_asap || c->qb_pos >= sdslen(c->querybuf)) break;
    }
    int to_main = 0;
    if (!c->flag.close_asap && c->qb_pos < sdslen(c->querybuf)) {
        parseInputBuffer(c);
        prepareCommandQueue(c);
        if (own_mm) ownerMmAfterRead(o);
        to_main = ownerRunParsedInput(c, ownerTryNative);
    }
    c->owner_deep = server.owner_uring_deep && o->native_total - ran >= server.owner_uring_deep;
    trimCommandQueue(c);
    trimClientQueryBuffer(c);
    if (c->flag.close_asap) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    if (to_main) {
        /* Send the native replies before main appends its own; main writes what is left. Replies
         * that wait for the AOF stay: main writes them after its AOF write. */
        if (clientHasPendingReplies(c) && !c->owner_held && !o->unflushed) ownerWriteOnce(o, c);
        return ownerHandOff(o, c, OWN_WHY_RUN);
    }
    if (clientHasPendingReplies(c)) {
        /* Queued only when ownerReplyOut would write it now: the replies show no write the AOF
         * lacks (no unflushed ops yet), so they may go out after later clients' commands too. */
        if (batch && !c->owner_held && !o->unflushed) {
            ownerQueueWrite(o, c);
            return 0;
        }
        return ownerReplyOut(o, c);
    }
    return 0;
}

/* Returns 1 when the client left the owner. */
static int ownerRead(owner *o, client *c) {
    int r = ownerReadPre(o, c);
    if (r != OWN_READ_GO) return r;
    readToQueryBuf(c);
    return ownerReadPost(o, c, 0);
}

/* owner-uring: a read of c joins the batch as o->uops[m] when it can go through io_uring (a TCP
 * client): returns 1 then. Into the client's own query buffer when it has one, else into the slot's
 * scratch, copied into the thread's shared query buffer when the batch runs (one client at a time
 * holds it, as with one read at a time). */
static int ownerReadQueue(owner *o, client *c, int m) {
    if (c->conn->type != connectionTypeTcp()) return 0;
    ownerUringOp *op = &o->uops[m];
    char *dst;
    size_t len;
    if (readToQueryBufPrepare(c, 0, &dst, &len)) {
        *op = (ownerUringOp){.c = c, .buf = dst, .len = len, .scratch = 0, .res = 0};
    } else {
        *op = (ownerUringOp){.c = c, .buf = o->uring_scratch + (size_t)m * PROTO_IOBUF_LEN, .len = PROTO_IOBUF_LEN,
                             .scratch = 1, .res = 0};
    }
    return 1;
}

/* owner-uring: the m queued reads with one io_uring_enter, then each client as ownerRead goes on
 * after its read, in event order; then the replies they queued, OWN_URING_BATCH per io_uring_enter.
 * A read that got no bytes (end of file, an error, nothing there) is redone by readToQueryBuf, so the
 * connection state and the close and error handling are the read path's. Returns 1 when any client
 * left the owner. */
static int ownerReadBatch(owner *o, int m) {
    int left = 0;
#ifdef HAVE_LIBURING
    for (int j = 0; j < m; j++) {
        ownerUringOp *op = &o->uops[j];
        struct io_uring_sqe *sqe = io_uring_get_sqe(&o->ring);
        serverAssert(sqe != NULL);
        io_uring_prep_recv(sqe, op->c->conn->fd, op->buf, op->len, MSG_DONTWAIT);
        io_uring_sqe_set_data64(sqe, (uint64_t)j);
    }
    ownerUringRun(o, m);
    o->uring_reads += m;
    /* The results leave o->uops before any client runs: the batch's writes reuse the slots. */
    ownerUringOp ops[OWN_URING_BATCH];
    memcpy(ops, o->uops, sizeof(ownerUringOp) * m);
    for (int j = 0; j < m; j++) {
        ownerUringOp *op = &ops[j];
        client *c = op->c;
        /* Memory leases: this read's reservation ends as its bytes go into its query buffer and parse;
         * what they really add is measured from here (ownerMmAfterRead after the parse). */
        if (own_mm) {
            serverAssert(o->mm_resv >= OWN_MM_READ);
            o->mm_resv -= OWN_MM_READ;
        }
        if (op->res > 0) {
            c->nread = op->res;
            size_t readlen = op->len;
            if (op->scratch) {
                char *dst;
                int ok = readToQueryBufPrepare(c, 1, &dst, &readlen);
                serverAssert(ok && readlen >= (size_t)op->res);
                memcpy(dst, op->buf, op->res);
            }
            readToQueryBufCommit(c, readlen);
        } else {
            o->uring_redo++;
            readToQueryBuf(c);
        }
        left |= ownerReadPost(o, c, 1);
    }
    left |= ownerFlushWrites(o);
#else
    UNUSED(o);
    UNUSED(m);
#endif
    return left;
}

#ifdef HAVE_LIBURING
/* Owner thread start: its own ring (one issuer, task work only when it enters) and read scratch. */
static void ownerUringInit(owner *o) {
    struct io_uring_params p = {0};
    p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SUBMIT_ALL;
    int r = io_uring_queue_init_params(OWN_URING_BATCH, &o->ring, &p);
    if (r < 0) {
        serverLog(LL_WARNING, "owner %d: no io_uring (%s): reads and writes are syscalls", o->id, strerror(-r));
        return;
    }
    void *s = mmap(NULL, (size_t)OWN_URING_BATCH * PROTO_IOBUF_LEN, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (s == MAP_FAILED) {
        serverLog(LL_WARNING, "owner %d: no io_uring scratch (%s): reads and writes are syscalls", o->id,
                  strerror(errno));
        io_uring_queue_exit(&o->ring);
        return;
    }
    o->uring_scratch = s;
    o->uring = 1;
}
#endif

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
#ifdef HAVE_LIBURING
    ownerUringInit(o);
#endif

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
        /* owner-uring: the readable clients' reads are batched, OWN_URING_BATCH per io_uring_enter, and
         * run in event order once the batch has read; the replies follow in batches too. Main stopping
         * the world ends the window after the batch in hand (its bytes are read: they run now).
         * Under maxmemory only inside a memory lease with OWN_MM_LEASE_ROOM to spare (owner-mm-lease,
         * D3-all-g11-maxmem): each queued read reserves OWN_MM_READ (o->mm_resv) until its turn, and every
         * headroom check counts the reservations, so a read still finds the room the unbatched path checks
         * for right before it; with less room the reads go one at a time (g10's exact path). Not under
         * appendonly either, unless owner-uring-aof yes (ownerUringOn). */
        o->mm_roomy = !own_mm || (own_mm_lease && ownerMmFits(o, OWN_MM_LEASE_ROOM));
        if (own_mm) {
            if (o->mm_roomy)
                o->mm_lease_windows++;
            else
                o->mm_exact_windows++;
        }
        int batch = ownerUringOn(o) && o->mm_roomy;
        int m = 0, stop = 0;
        for (int k = 0; k < n && !stop; k++) {
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
            if (e & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                if (!batch || c->owner_deep) {
                    /* A deep pipeline (owner-uring-deep commands or more in its last read) already shares
                     * its syscalls among its commands: read, run and reply at once, as without io_uring. */
                    handed |= ownerRead(o, c);
                } else {
                    int r = ownerReadPre(o, c);
                    if (r != OWN_READ_GO) {
                        handed |= r;
                    } else if (ownerReadQueue(o, c, m)) {
                        if (own_mm) o->mm_resv += OWN_MM_READ; /* the lease's room for it, until its turn */
                        if (++m == OWN_URING_BATCH) {
                            handed |= ownerReadBatch(o, m);
                            m = 0;
                        }
                    } else {
                        readToQueryBuf(c);
                        handed |= ownerReadPost(o, c, 0);
                    }
                }
            }
            /* Main is waiting: the rest are level-triggered and come back with the next epoll_wait. */
            if (atomic_load_explicit(&own_stop, memory_order_relaxed)) stop = 1;
        }
        if (m) handed |= ownerReadBatch(o, m);
        serverAssert(o->mm_resv == 0); /* every queued read ran, and gave its reservation back */
        /* This window's AOF records: written here (everysec, no), unless main is stopping the world,
         * which folds them first thing and writes them with its own. */
        if (o->unflushed && own_aof_drain && !atomic_load_explicit(&own_stop, memory_order_relaxed))
            ownerDrainAof(o);
        if (o->nrefc) ownerRefsSettle(o); /* no value reference outlives the window */
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
    long long mm_lease_w = 0, mm_exact_w = 0;
    long long burst = 0, bursts = 0, setburst = 0, setbursts = 0;
    long long u_enters = 0, u_reads = 0, u_writes = 0, u_short = 0, u_redo = 0;
    long long r_replies = 0, r_bytes = 0, r_writes = 0, r_copied = 0, r_copied_bytes = 0;
    int resident = 0, u_on = 0;
    for (int i = 0; i < own_n; i++) {
        native += owners[i].native_total;
        burst += owners[i].burst_total;
        bursts += owners[i].bursts_total;
        setburst += owners[i].setburst_total;
        setbursts += owners[i].setbursts_total;
        handoffs += owners[i].handoffs_total;
        adopted += owners[i].adopted_total;
        windows += owners[i].windows_total;
        held += owners[i].held_total;
        mm_refused += owners[i].mm_refused_total;
        mm_lease_w += owners[i].mm_lease_windows;
        mm_exact_w += owners[i].mm_exact_windows;
        resident += owners[i].nclients;
        u_enters += owners[i].uring_enters;
        u_reads += owners[i].uring_reads;
        u_writes += owners[i].uring_writes;
        u_short += owners[i].uring_short;
        u_redo += owners[i].uring_redo;
        u_on += owners[i].uring;
        r_replies += owners[i].ref_replies;
        r_bytes += owners[i].ref_bytes;
        r_writes += owners[i].ref_writes;
        r_copied += owners[i].ref_copied;
        r_copied_bytes += owners[i].ref_copied_bytes;
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
    *info = sdscatprintf(*info,
                         "owner_aof_ops:%lld\r\nowner_aof_drains:%lld\r\nowner_held_replies:%lld\r\n"
                         "owner_maxmemory_to_main:%lld\r\n",
                         own_aof_ops, own_aof_drains, held, mm_refused);
    /* D3-all-g11-maxmem: memory leases. owner_mm_lease_bytes = each owner's lease this period (0: no
     * maxmemory); windows under maxmemory that batched inside the lease, or took the exact path. */
    *info = sdscatprintf(*info,
                         "owner_mm_lease:%s\r\nowner_mm_lease_bytes:%lld\r\nowner_mm_lease_windows:%lld\r\n"
                         "owner_mm_exact_windows:%lld\r\n",
                         server.owner_mm_lease ? "yes" : "no", own_mm ? own_mm_slice : 0LL, mm_lease_w, mm_exact_w);
    *info = sdscatprintf(*info, "owner_burst_commands:%lld\r\nowner_bursts:%lld\r\n", burst, bursts);
    *info = sdscatprintf(*info, "owner_repl_native:%s\r\nowner_repl:%s\r\nowner_repl_ops:%lld\r\nowner_repl_bytes:%lld\r\n",
                         server.owner_repl_native ? "yes" : "no", own_repl ? "yes" : "no", own_repl_ops, own_repl_bytes);
    /* D3-all-g14-fold: owner_repl_split: the owners encode each record twice (compact AOF, RESP stream). */
    *info = sdscatprintf(*info, "owner_repl_split:%s\r\nowner_repl_split_ops:%lld\r\n", own_split ? "yes" : "no",
                         own_split_ops);
    *info = sdscatprintf(*info, "owner_set_burst:%s\r\nowner_setburst_commands:%lld\r\nowner_setbursts:%lld\r\n",
                         server.owner_set_burst && own_setburst_cmd ? "yes" : "no", setburst, setbursts);
    *info = sdscatprintf(*info,
                         "owner_uring:%s\r\nowner_uring_rings:%d\r\nowner_uring_enters:%lld\r\n"
                         "owner_uring_reads:%lld\r\nowner_uring_writes:%lld\r\nowner_uring_short_writes:%lld\r\n"
                         "owner_uring_redone:%lld\r\n",
                         server.owner_uring ? "yes" : "no", u_on, u_enters, u_reads, u_writes, u_short, u_redo);
    /* D3-all-g9-fold: whether the owners batch now (ownerUringOn): yes, or why not. */
    const char *batching = !server.owner_uring || !u_on                  ? "off"
                           : own_aof && !server.owner_uring_aof         ? "aof"
                           : own_mm && !own_mm_lease                    ? "maxmemory"
                                                                        : "yes";
    *info = sdscatprintf(*info, "owner_uring_batching:%s\r\nowner_uring_deep:%d\r\nowner_uring_aof:%s\r\n", batching,
                         server.owner_uring_deep, server.owner_uring_aof ? "yes" : "no");
    /* D3-all-g11-bigreply: GET values sent from where they are (ownerRefAdd); copied = replies whose
     * referenced rest was copied into a reply block (socket full, AOF wait, handoff, window end). */
    *info = sdscatprintf(*info,
                         "owner_bigreply_min:%d\r\nowner_ref_replies:%lld\r\nowner_ref_bytes:%lld\r\n"
                         "owner_ref_writes:%lld\r\nowner_ref_copied:%lld\r\nowner_ref_copied_bytes:%lld\r\n",
                         server.owner_bigreply_min, r_replies, r_bytes, r_writes, r_copied, r_copied_bytes);
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        *info = sdscatprintf(*info, "owner%d:port=%d,cpu=%d,slots=%d-%d,clients=%d,native=%lld,handoffs=%lld\r\n", i,
                             o->port, o->cpu, o->slot_lo, o->slot_hi, o->nclients, o->native_total, o->handoffs_total);
    }
}
