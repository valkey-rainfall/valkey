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
 * hop. RL-1: also the single-key list, hash, set and sorted-set commands and the key-level commands
 * (EXPIRE ... TYPE, EXISTS), each by its stock proc; own_kinds[] lists every native command and its
 * properties. Hash commands stay main's while any hash in the key's table has fields with their own
 * expiry (keys_with_volatile_items): field expiry, its HSETEX rewrites and its tracking are main's.
 * RL-g2o-long-tail: the real-world long tail of single-key and single-slot data commands too (GETEX ...
 * TOUCH, OWN_K_TAIL rows), each by its stock proc; under maxmemory APPEND, SETRANGE and SMOVE go to main.
 * RL-g6c-native-streams: the non-blocking stream commands too (XADD ... XSETID, OWN_FAM_STREAM rows) when all
 * their keys are in one slot, each by its stock proc; XREAD / XREADGROUP with BLOCK stay main's.
 * MULTI/EXEC too, while every queued command is a queueable one (OWN_K_TXN) on the
 * owner's keys: the owner
 * queues them and runs EXEC within one window. WATCH/UNWATCH of its own keys too: an owner keeps its
 * own registry of watched keys (multi.c), which only it, or main with the owners stopped, touches;
 * a watcher that comes to main has its keys moved to the dbs' registries (watchMoveToMain) and is a
 * stock watcher from then on. Everything else (a key another owner owns, keys in two slots, any
 * other command inside a transaction, scripts (except EVALSHA / EVAL on one owner's keys: ownerScriptRun),
 * blocking, pub/sub, admin) falls back to the main
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
 * Pipelines (owner-pipeline-resume N, default 4; RL-g1g): a command its owner cannot run hands the
 * client to main with the rest of its input, and main runs that command. When the client's next N
 * commands, parsed whole, are ones the owner runs (data commands, MULTI, EXEC on the home owner's keys),
 * or its input ends, main stops there (ownerGuestYield) and at the end of beforeSleep sends the client
 * home with the rest of its input and the replies main made for it (main writes none of them), and the
 * owner runs the rest first thing (ownerResumeInput) until the next command it cannot run, then writes
 * all the replies in order. So each such command goes to main on its own and the commands around it
 * stay native; a run of fewer than N native commands between two others costs main less to run itself
 * than a trip home. One thread at a time serves a connection, and its replies leave in command order.
 * A client that cannot go home then (a socket main is writing, replies the AOF still holds, a veto that
 * came up since) has the rest run by main at its next wakeup, as before (ownerRerunOnMain). With
 * owner-pipeline-resume 0, main runs the rest of the pipeline, as the parent did.
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
#include "intset.h"
#include "listpack.h"
#include "call_reply.h"
#include "mt19937-64.h"

#include <ctype.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/timerfd.h>
#endif
#ifdef HAVE_LIBURING
#include <liburing.h>
#endif

#define OWNER_MAX 256
#define OWNER_EVENTS 256
#define OWNER_WAKE_TOKEN UINT64_MAX
#define OWNER_EXPIRE_TOKEN (UINT64_MAX - 1) /* RL-g2b: the owner's expire timer */
#define OWN_RING_MAX 128 /* GET and MGET ring slots: prefetch-batch-max-size's maximum; a power of two */
/* io_uring (owner-uring, D3-all-g8): reads and writes per io_uring_enter. Each read slot has a
 * PROTO_IOBUF_LEN scratch buffer (mmap: only touched pages count, and not toward used_memory). */
#define OWN_URING_BATCH 64
/* RL-g3h replica flow control: an owner that left readable sockets unread naps this long before it looks again. */
#define OWN_FLOW_NAP_US 50

/* Native command kinds. The order means nothing: every property of a kind is in its own_kinds[] row,
 * looked up by name, so a kind can be added anywhere without changing another kind's meaning. */
enum {
    OWN_GET,
    OWN_SET,
    OWN_DEL,
    OWN_INCR,
    OWN_PING,
    OWN_MGET,
    OWN_MSET,
    OWN_MULTI,
    OWN_EXEC,
    OWN_DISCARD,
    OWN_WATCH,
    OWN_UNWATCH,
    /* RL-g1b-native-list: single-key list commands. A push must wake the clients blocked on its key
     * (dbAdd's signalKeyAsReady). Owners never run while a client is blocked on a list: any blocked
     * client (list, zset, stream or module) vetoes the whole native path (ownerComputeVeto:
     * OWN_VETO_ALL), and main recomputes the veto each time it resumes the owners. Only main blocks or
     * unblocks a client, and only while the owners are stopped, so in an owner's window no client is
     * blocked on a list and signalKeyAsReady returns at its first check, as it does on main. */
    OWN_LPUSH,
    OWN_RPUSH,
    OWN_LPOP,
    OWN_RPOP,
    OWN_LLEN,
    OWN_LRANGE,
    OWN_LINDEX,
    /* RL-g1c-native-keyspace: key-level commands. They run the stock procs, so lazy expiry, the
     * PEXPIREAT / DEL rewrites and their records are the main path's. */
    OWN_EXPIRE,
    OWN_PEXPIRE,
    OWN_EXPIREAT,
    OWN_PEXPIREAT,
    OWN_PERSIST,
    OWN_TTL,
    OWN_PTTL,
    OWN_EXISTS,
    OWN_TYPE,
    /* RL-g1a-native-hash: single-key hash commands. HSET and HMSET share hsetCommand; ownerKindOf tells
     * them apart by command (per-command stats). */
    OWN_HSET,
    OWN_HMSET,
    OWN_HINCRBY,
    OWN_HDEL,
    OWN_HGET,
    OWN_HMGET,
    OWN_HEXISTS,
    OWN_HLEN,
    OWN_HGETALL,
    /* RL-g1e-native-set-zset: single-key set and sorted-set commands. SMEMBERS shares sinterCommand with
     * SINTER (not native): ownerKindOf is by command. The set / zset code has no shared mutable state
     * (t_set.c, t_zset.c write no globals and draw no random level; dirty and stats go through ectx). */
    OWN_SADD,
    OWN_SREM,
    OWN_SISMEMBER,
    OWN_SMEMBERS,
    OWN_SCARD,
    OWN_ZADD,
    OWN_ZREM,
    OWN_ZRANGE,
    OWN_ZSCORE,
    OWN_ZCARD,
    OWN_ZINCRBY,
    /* RL-g1f-owner-evalsha: EVALSHA / EVAL on the owner of their keys (ownerScriptRun), and the commands
     * only a script runs on an owner (OWN_K_INNER; a client's own INCRBY, DECR or DECRBY goes to main).
     * EXPIRE, PEXPIRE, TTL, PTTL and EXISTS, which scripts run there too, are RL-g1c's kinds above. */
    OWN_EVALSHA,
    OWN_EVAL,
    OWN_INCRBY,
    OWN_DECR,
    OWN_DECRBY,
    /* RL-g2o-long-tail: the real-world tail of single-key (or single-slot) data commands, each by its stock
     * proc as above (OWN_K_TAIL counts them). Left on main on purpose: SPOP, SRANDMEMBER, HRANDFIELD and
     * ZRANDMEMBER (hashtable sampling draws from genrand64, one global state that is not thread-safe), the
     * blocking variants, SORT (BY / GET read other keys), commands whose keys are not at a fixed place
     * (SINTERCARD, ZINTERCARD, OBJECT), field-expiry hash commands (main's keys_with_volatile_items), and
     * every keyless, admin, pub/sub, stream, script-only and module command. */
    OWN_GETEX,
    OWN_GETDEL,
    OWN_GETSET,
    OWN_SETNX,
    OWN_SETEX,
    OWN_PSETEX,
    OWN_APPEND,
    OWN_STRLEN,
    OWN_GETRANGE,
    OWN_SUBSTR,
    OWN_SETRANGE,
    OWN_INCRBYFLOAT,
    OWN_MSETNX,
    OWN_HSETNX,
    OWN_HINCRBYFLOAT,
    OWN_HKEYS,
    OWN_HVALS,
    OWN_HSTRLEN,
    OWN_LPUSHX,
    OWN_RPUSHX,
    OWN_LSET,
    OWN_LINSERT,
    OWN_LREM,
    OWN_LTRIM,
    OWN_LPOS,
    OWN_RPOPLPUSH,
    OWN_LMOVE,
    OWN_SMISMEMBER,
    OWN_SMOVE,
    OWN_ZREVRANGE,
    OWN_ZRANGEBYSCORE,
    OWN_ZREVRANGEBYSCORE,
    OWN_ZRANGEBYLEX,
    OWN_ZREVRANGEBYLEX,
    OWN_ZRANK,
    OWN_ZREVRANK,
    OWN_ZCOUNT,
    OWN_ZLEXCOUNT,
    OWN_ZMSCORE,
    OWN_ZPOPMIN,
    OWN_ZPOPMAX,
    OWN_ZREMRANGEBYSCORE,
    OWN_ZREMRANGEBYRANK,
    OWN_ZREMRANGEBYLEX,
    OWN_EXPIRETIME,
    OWN_PEXPIRETIME,
    OWN_UNLINK,
    OWN_TOUCH,
    /* RL-g3g-long-tail-rand: the random reads and SPOP (each owner thread draws from its own random state,
     * mt19937-64.c threadRandom / genrand64: the shared state was not thread-safe), and the commands whose keys are
     * not at argv[1] (a numkeys argument first, or OBJECT's key at argv[2]). */
    OWN_SPOP,
    OWN_SRANDMEMBER,
    OWN_HRANDFIELD,
    OWN_ZRANDMEMBER,
    OWN_SINTERCARD,
    OWN_ZINTERCARD,
    OWN_ZUNION,
    OWN_ZINTER,
    OWN_ZDIFF,
    OWN_LMPOP,
    OWN_ZMPOP,
    OWN_OBJECT_ENCODING,
    OWN_OBJECT_REFCOUNT,
    OWN_OBJECT_IDLETIME,
    OWN_OBJECT_FREQ,
    /* RL-g3i-accept-storm: CLUSTER SLOTS, which a cluster client sends on each new connection: the owners'
     * map is fixed at startup, so its owner answers it (a connection storm was one trip to main per client). */
    OWN_CLUSTER_SLOTS,
    /* RL-g4c-blocking-native: blocking pops on one owner's keys. Data there: they run as their non-blocking
     * forms. None: the client parks on the owner (ownerPark): a write of the owner that makes a key ready
     * serves its parked clients right after that command, first parked first; a timeout on the owner's
     * clock replies the null array. Anything main does to a parked client or its keys first makes every
     * parked client a stock blocked client on main (ownerParkedToMain). */
    OWN_BLPOP,
    OWN_BRPOP,
    OWN_BLMOVE,
    OWN_BRPOPLPUSH,
    OWN_BZPOPMIN,
    OWN_BZPOPMAX,
    OWN_BLMPOP,
    OWN_BZMPOP,
    /* RL-g6c-native-streams: the stream commands that do not block, by their stock procs (t_stream.c keeps
     * nothing shared: dirty and the command time go through ectx, propagation through ectx->also_propagate,
     * the node limits are configs). XREAD / XREADGROUP with BLOCK, XGROUP HELP and XINFO HELP stay main's. */
    OWN_XADD,
    OWN_XLEN,
    OWN_XRANGE,
    OWN_XREVRANGE,
    OWN_XDEL,
    OWN_XDELEX,
    OWN_XTRIM,
    OWN_XREAD,
    OWN_XREADGROUP,
    OWN_XACK,
    OWN_XACKDEL,
    OWN_XPENDING,
    OWN_XCLAIM,
    OWN_XAUTOCLAIM,
    OWN_XGROUP_CREATE,
    OWN_XGROUP_SETID,
    OWN_XGROUP_DESTROY,
    OWN_XGROUP_CREATECONSUMER,
    OWN_XGROUP_DELCONSUMER,
    OWN_XINFO_STREAM,
    OWN_XINFO_GROUPS,
    OWN_XINFO_CONSUMERS,
    OWN_XSETID,
    OWN_NCMDS
};
/* RL-g2o-long-tail: every kind a script may run (OWN_K_SCRIPT) is below OWN_DECRBY + 1, so a script run's
 * per-kind stats (ownerScriptRunState) stay as long as the parent's (ownerThreadsInit checks it). */
#define OWN_SCRIPT_NCMDS (OWN_DECRBY + 1)

/* own_kinds[].flags */
#define OWN_K_READ 1 /* only reads its keys: runs under a write veto (OWN_VETO_WRITE), as GET does */
#define OWN_K_TXN 2  /* queued natively inside MULTI and run by ownerExec; inside MULTI any other kind
                      * goes to main with the transaction */
#define OWN_K_SCRIPT 4 /* RL-g1f: a script on an owner may run it (ownerScriptCommand) */
#define OWN_K_INNER 8  /* RL-g1f: only inside an owner's script; a client's own command goes to main */
#define OWN_K_TAIL 16  /* RL-g2o-long-tail: a long-tail kind (INFO owner_tail_commands) */
#define OWN_K_BLOCK 32 /* RL-g4c: a blocking pop: parks on the owner (INFO owner_block_commands) */
/* own_kinds[].keys: where the keys are (ownerKeySlot). */
enum {
    OWN_KEYS_NONE,   /* no key (PING, MULTI, EXEC, ...): no slot */
    OWN_KEYS_ONE,    /* argv[1] only (the rest are fields, values, options) */
    OWN_KEYS_ALL,    /* argv[1..]: every one in the same slot */
    OWN_KEYS_PAIRS,  /* argv[1], argv[3], ...: key value pairs, every key in the same slot */
    OWN_KEYS_SCRIPT, /* EVALSHA / EVAL: numkeys at argv[2], the keys after it, all in one slot (at least one) */
    OWN_KEYS_TWO,    /* RL-g2o: argv[1] and argv[2] (source, destination), in the same slot */
    OWN_KEYS_NUMKEYS, /* RL-g3g: numkeys at argv[1], then that many keys (SINTERCARD, ZUNION, LMPOP, ...), one slot */
    OWN_KEYS_ARG2,    /* RL-g3g: the key at argv[2] (OBJECT ENCODING key) */
    OWN_KEYS_BUTLAST, /* RL-g4c: argv[1 .. argc-2], the timeout last (BLPOP, BRPOP, BZPOPMIN, BZPOPMAX), one slot */
    OWN_KEYS_NUMKEYS2, /* RL-g4c: timeout at argv[1], numkeys at argv[2], the keys after it (BLMPOP, BZMPOP), one slot */
    OWN_KEYS_STREAMS  /* RL-g6c: XREAD / XREADGROUP: the keys after STREAMS (half of the rest), one slot; no BLOCK */
};
/* own_kinds[].mm: what the command can add under maxmemory beyond its arguments and a reply block,
 * which ownerMmCmdFits counts for every command (ownerMmMore). */
enum {
    OWN_MM_NONE,   /* nothing more */
    OWN_MM_VALUES, /* its reply holds stored strings (GET, MGET, SET ... GET) */
    OWN_MM_EXEC,   /* its queued commands (ownerMmExecBound) */
    OWN_MM_LIST,   /* its reply holds elements of the list: at most the whole list (ownerStoredListReplyLen) */
    OWN_MM_HASH_W, /* a hash write: a listpack may convert to a hashtable (ownerHashBound) */
    OWN_MM_HASH_R, /* a hash read that replies values: at most the whole listpack; no cheap bound on a
                    * hashtable (ownerHashBound) */
    OWN_MM_SET_W,  /* SADD: an intset or listpack may be rebuilt (ownerSetZsetBound) */
    OWN_MM_ZSET_W, /* ZADD, ZINCRBY: a listpack may be rebuilt, a tree node may split (ownerSetZsetBound) */
    OWN_MM_COLL_R, /* SMEMBERS, ZRANGE: at most the whole compact collection; no cheap bound on a hashtable
                    * or a tree (ownerSetZsetBound) */
    OWN_MM_KEYVAL, /* RL-g2o: its reply holds the string stored at argv[1] (GETEX, GETDEL, GETSET, GETRANGE) */
    OWN_MM_RAND,   /* RL-g3g: SPOP, SRANDMEMBER, HRANDFIELD, ZRANDMEMBER: count elements of a compact collection
                    * (ownerRandBound); no cheap bound on a hashtable or a tree */
    OWN_MM_UNBOUND /* RL-g2o: no cheap bound (APPEND and SETRANGE grow a string by sds growth, SMOVE grows its
                    * destination): under maxmemory main runs it */
};
/* own_kinds[].family: the per-family INFO counters (owner_<family>_commands). */
enum { OWN_FAM_STRING, OWN_FAM_TXN, OWN_FAM_LIST, OWN_FAM_KEYSPACE, OWN_FAM_HASH, OWN_FAM_SETZSET, OWN_FAM_SCRIPT,
       OWN_FAM_STREAM, OWN_NFAM };

typedef struct ownKind {
    const char *name; /* the command's original name (own_cmds[]) */
    unsigned char flags, keys, mm, family;
} ownKind;

static const ownKind own_kinds[OWN_NCMDS] = {
    [OWN_GET] = {"get", OWN_K_READ | OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_VALUES, OWN_FAM_STRING},
    [OWN_SET] = {"set", OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_VALUES, OWN_FAM_STRING},
    [OWN_DEL] = {"del", OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_ALL, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_INCR] = {"incr", OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_PING] = {"ping", OWN_K_TXN, OWN_KEYS_NONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_MGET] = {"mget", OWN_K_READ | OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_ALL, OWN_MM_VALUES, OWN_FAM_STRING},
    [OWN_MSET] = {"mset", OWN_K_TXN | OWN_K_SCRIPT, OWN_KEYS_PAIRS, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_MULTI] = {"multi", 0, OWN_KEYS_NONE, OWN_MM_NONE, OWN_FAM_TXN},
    [OWN_EXEC] = {"exec", 0, OWN_KEYS_NONE, OWN_MM_EXEC, OWN_FAM_TXN},
    [OWN_DISCARD] = {"discard", 0, OWN_KEYS_NONE, OWN_MM_NONE, OWN_FAM_TXN},
    [OWN_WATCH] = {"watch", OWN_K_READ, OWN_KEYS_ALL, OWN_MM_NONE, OWN_FAM_TXN},
    [OWN_UNWATCH] = {"unwatch", 0, OWN_KEYS_NONE, OWN_MM_NONE, OWN_FAM_TXN},
    /* RL-g1b-native-list: queueable in a native MULTI, as SET; LLEN, LRANGE and LINDEX only read. */
    [OWN_LPUSH] = {"lpush", OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_RPUSH] = {"rpush", OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LPOP] = {"lpop", OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_RPOP] = {"rpop", OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_LLEN] = {"llen", OWN_K_READ | OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LRANGE] = {"lrange", OWN_K_READ | OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_LINDEX] = {"lindex", OWN_K_READ | OWN_K_TXN, OWN_KEYS_ONE, OWN_MM_LIST, OWN_FAM_LIST},
    /* RL-g1c-native-keyspace: not queued natively (inside MULTI main queues them); TTL, PTTL, EXISTS
     * and TYPE only read; EXISTS with several keys in one slot, as DEL. RL-g1f: scripts on an owner run
     * EXPIRE, PEXPIRE, TTL, PTTL and EXISTS too (one entry each: shared stats). */
    [OWN_EXPIRE] = {"expire", OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_PEXPIRE] = {"pexpire", OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_EXPIREAT] = {"expireat", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_PEXPIREAT] = {"pexpireat", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_PERSIST] = {"persist", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_TTL] = {"ttl", OWN_K_READ | OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_PTTL] = {"pttl", OWN_K_READ | OWN_K_SCRIPT, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_EXISTS] = {"exists", OWN_K_READ | OWN_K_SCRIPT, OWN_KEYS_ALL, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_TYPE] = {"type", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    /* RL-g1a-native-hash: not queued natively; HGET ... HGETALL only read. ownerNativeKind sends a
     * table's hash commands to main while any hash in it has fields with their own expiry. */
    [OWN_HSET] = {"hset", 0, OWN_KEYS_ONE, OWN_MM_HASH_W, OWN_FAM_HASH},
    [OWN_HMSET] = {"hmset", 0, OWN_KEYS_ONE, OWN_MM_HASH_W, OWN_FAM_HASH},
    [OWN_HINCRBY] = {"hincrby", 0, OWN_KEYS_ONE, OWN_MM_HASH_W, OWN_FAM_HASH},
    [OWN_HDEL] = {"hdel", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_HASH},
    [OWN_HGET] = {"hget", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_HASH_R, OWN_FAM_HASH},
    [OWN_HMGET] = {"hmget", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_HASH_R, OWN_FAM_HASH},
    [OWN_HEXISTS] = {"hexists", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_HASH},
    [OWN_HLEN] = {"hlen", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_HASH},
    [OWN_HGETALL] = {"hgetall", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_HASH_R, OWN_FAM_HASH},
    /* RL-g1e-native-set-zset: not queued natively; SISMEMBER, SMEMBERS, SCARD, ZRANGE, ZSCORE and ZCARD
     * only read. */
    [OWN_SADD] = {"sadd", 0, OWN_KEYS_ONE, OWN_MM_SET_W, OWN_FAM_SETZSET},
    [OWN_SREM] = {"srem", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_SISMEMBER] = {"sismember", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_SMEMBERS] = {"smembers", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_SCARD] = {"scard", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZADD] = {"zadd", 0, OWN_KEYS_ONE, OWN_MM_ZSET_W, OWN_FAM_SETZSET},
    [OWN_ZREM] = {"zrem", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZRANGE] = {"zrange", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZSCORE] = {"zscore", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZCARD] = {"zcard", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZINCRBY] = {"zincrby", 0, OWN_KEYS_ONE, OWN_MM_ZSET_W, OWN_FAM_SETZSET},
    /* RL-g1f-owner-evalsha: EVALSHA / EVAL write (any veto sends them to main: ownerNativeKind); INCRBY,
     * DECR and DECRBY only inside an owner's script. */
    [OWN_EVALSHA] = {"evalsha", 0, OWN_KEYS_SCRIPT, OWN_MM_NONE, OWN_FAM_SCRIPT},
    [OWN_EVAL] = {"eval", 0, OWN_KEYS_SCRIPT, OWN_MM_NONE, OWN_FAM_SCRIPT},
    /* RL-g2o-long-tail: a client's own INCRBY, DECR and DECRBY run natively too (incrDecrCommand, as INCR). */
    [OWN_INCRBY] = {"incrby", OWN_K_SCRIPT | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_DECR] = {"decr", OWN_K_SCRIPT | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_DECRBY] = {"decrby", OWN_K_SCRIPT | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    /* RL-g2o-long-tail: not queued natively (inside MULTI main queues them), not run by an owner's script.
     * Reads (OWN_K_READ) only read their keys. Writes propagate as their stock procs rewrite them (GETEX:
     * PEXPIREAT or PERSIST; GETDEL: DEL; SETEX, PSETEX: SET PXAT; INCRBYFLOAT, HINCRBYFLOAT: SET KEEPTTL,
     * HSET; ZPOPMIN, ZPOPMAX as they are); a push wakes nobody (any blocked client vetoes the native path,
     * as for LPUSH). */
    [OWN_GETEX] = {"getex", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_KEYVAL, OWN_FAM_STRING},
    [OWN_GETDEL] = {"getdel", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_KEYVAL, OWN_FAM_STRING},
    [OWN_GETSET] = {"getset", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_KEYVAL, OWN_FAM_STRING},
    [OWN_SETNX] = {"setnx", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_SETEX] = {"setex", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_PSETEX] = {"psetex", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_APPEND] = {"append", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STRING},
    [OWN_STRLEN] = {"strlen", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_GETRANGE] = {"getrange", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_KEYVAL, OWN_FAM_STRING},
    [OWN_SUBSTR] = {"substr", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_KEYVAL, OWN_FAM_STRING},
    [OWN_SETRANGE] = {"setrange", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STRING},
    [OWN_INCRBYFLOAT] = {"incrbyfloat", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_MSETNX] = {"msetnx", OWN_K_TAIL, OWN_KEYS_PAIRS, OWN_MM_NONE, OWN_FAM_STRING},
    [OWN_HSETNX] = {"hsetnx", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_HASH_W, OWN_FAM_HASH},
    [OWN_HINCRBYFLOAT] = {"hincrbyfloat", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_HASH_W, OWN_FAM_HASH},
    [OWN_HKEYS] = {"hkeys", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_HASH_R, OWN_FAM_HASH},
    [OWN_HVALS] = {"hvals", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_HASH_R, OWN_FAM_HASH},
    [OWN_HSTRLEN] = {"hstrlen", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_HASH},
    [OWN_LPUSHX] = {"lpushx", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_RPUSHX] = {"rpushx", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LSET] = {"lset", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LINSERT] = {"linsert", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LREM] = {"lrem", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LTRIM] = {"ltrim", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_LIST},
    [OWN_LPOS] = {"lpos", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_RPOPLPUSH] = {"rpoplpush", OWN_K_TAIL, OWN_KEYS_TWO, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_LMOVE] = {"lmove", OWN_K_TAIL, OWN_KEYS_TWO, OWN_MM_LIST, OWN_FAM_LIST},
    [OWN_SMISMEMBER] = {"smismember", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_SMOVE] = {"smove", OWN_K_TAIL, OWN_KEYS_TWO, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_ZREVRANGE] = {"zrevrange", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZRANGEBYSCORE] = {"zrangebyscore", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZREVRANGEBYSCORE] = {"zrevrangebyscore", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZRANGEBYLEX] = {"zrangebylex", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZREVRANGEBYLEX] = {"zrevrangebylex", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZRANK] = {"zrank", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZREVRANK] = {"zrevrank", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZCOUNT] = {"zcount", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZLEXCOUNT] = {"zlexcount", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZMSCORE] = {"zmscore", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZPOPMIN] = {"zpopmin", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZPOPMAX] = {"zpopmax", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_COLL_R, OWN_FAM_SETZSET},
    [OWN_ZREMRANGEBYSCORE] = {"zremrangebyscore", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZREMRANGEBYRANK] = {"zremrangebyrank", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZREMRANGEBYLEX] = {"zremrangebylex", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_EXPIRETIME] = {"expiretime", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_PEXPIRETIME] = {"pexpiretime", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_UNLINK] = {"unlink", OWN_K_TAIL, OWN_KEYS_ALL, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_TOUCH] = {"touch", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ALL, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    /* RL-g3g-long-tail-rand: as RL-g2o's rows (not queued natively, not run by a script, a veto sends them to main).
     * SPOP propagates as its stock proc makes it: SREM (one element, or batches of them), or DEL / UNLINK when it
     * pops the whole set; the other random commands only read. HRANDFIELD keeps the hash rule (main while the table
     * has field expiry). ZUNION, ZINTER, ZDIFF, LMPOP and ZMPOP reply up to whole collections: main under maxmemory. */
    [OWN_SPOP] = {"spop", OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_RAND, OWN_FAM_SETZSET},
    [OWN_SRANDMEMBER] = {"srandmember", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_RAND, OWN_FAM_SETZSET},
    [OWN_HRANDFIELD] = {"hrandfield", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_RAND, OWN_FAM_HASH},
    [OWN_ZRANDMEMBER] = {"zrandmember", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ONE, OWN_MM_RAND, OWN_FAM_SETZSET},
    [OWN_SINTERCARD] = {"sintercard", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZINTERCARD] = {"zintercard", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_NONE, OWN_FAM_SETZSET},
    [OWN_ZUNION] = {"zunion", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_ZINTER] = {"zinter", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_ZDIFF] = {"zdiff", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_LMPOP] = {"lmpop", OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_ZMPOP] = {"zmpop", OWN_K_TAIL, OWN_KEYS_NUMKEYS, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_OBJECT_ENCODING] = {"object|encoding", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_OBJECT_REFCOUNT] = {"object|refcount", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_OBJECT_IDLETIME] = {"object|idletime", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    [OWN_OBJECT_FREQ] = {"object|freq", OWN_K_READ | OWN_K_TAIL, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_KEYSPACE},
    /* RL-g3i-accept-storm: CLUSTER SLOTS, keyless, answered from the owners' fixed map (ownerClusterCommand). */
    [OWN_CLUSTER_SLOTS] = {"cluster|slots", 0, OWN_KEYS_NONE, OWN_MM_NONE, OWN_FAM_STRING},
    /* RL-g4c-blocking-native: not queued natively in MULTI (main queues them; EXEC runs them as non-blocking), not
     * run by a script, main's under maxmemory (they may reply whole collections), never for TLS clients. Served,
     * they propagate as their stock procs rewrite them: LPOP / RPOP [count], LMOVE, ZPOPMIN / ZPOPMAX [count]. */
    [OWN_BLPOP] = {"blpop", OWN_K_BLOCK, OWN_KEYS_BUTLAST, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_BRPOP] = {"brpop", OWN_K_BLOCK, OWN_KEYS_BUTLAST, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_BLMOVE] = {"blmove", OWN_K_BLOCK, OWN_KEYS_TWO, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_BRPOPLPUSH] = {"brpoplpush", OWN_K_BLOCK, OWN_KEYS_TWO, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_BZPOPMIN] = {"bzpopmin", OWN_K_BLOCK, OWN_KEYS_BUTLAST, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_BZPOPMAX] = {"bzpopmax", OWN_K_BLOCK, OWN_KEYS_BUTLAST, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    [OWN_BLMPOP] = {"blmpop", OWN_K_BLOCK, OWN_KEYS_NUMKEYS2, OWN_MM_UNBOUND, OWN_FAM_LIST},
    [OWN_BZMPOP] = {"bzmpop", OWN_K_BLOCK, OWN_KEYS_NUMKEYS2, OWN_MM_UNBOUND, OWN_FAM_SETZSET},
    /* RL-g6c-native-streams: as RL-g2o's rows (not queued natively in MULTI, not run by an owner's script, any veto
     * sends them to main; a stream blocked client on main vetoes everything, so XADD wakes nobody). Effects are the
     * stock procs': XADD propagates with its generated ID and its trim rewritten exactly (= and the threshold it kept),
     * XREADGROUP as XCLAIM per delivered entry and XGROUP SETID, XCLAIM / XAUTOCLAIM as XCLAIM rewrites, XACKDEL /
     * XDELEX as XACK / XDEL. Under maxmemory every one that can grow memory or reply a range runs on main. */
    [OWN_XADD] = {"xadd", 0, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XLEN] = {"xlen", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XRANGE] = {"xrange", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XREVRANGE] = {"xrevrange", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XDEL] = {"xdel", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XDELEX] = {"xdelex", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XTRIM] = {"xtrim", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XREAD] = {"xread", OWN_K_READ, OWN_KEYS_STREAMS, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XREADGROUP] = {"xreadgroup", 0, OWN_KEYS_STREAMS, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XACK] = {"xack", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XACKDEL] = {"xackdel", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XPENDING] = {"xpending", OWN_K_READ, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XCLAIM] = {"xclaim", 0, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XAUTOCLAIM] = {"xautoclaim", 0, OWN_KEYS_ONE, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XGROUP_CREATE] = {"xgroup|create", 0, OWN_KEYS_ARG2, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XGROUP_SETID] = {"xgroup|setid", 0, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XGROUP_DESTROY] = {"xgroup|destroy", 0, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XGROUP_CREATECONSUMER] = {"xgroup|createconsumer", 0, OWN_KEYS_ARG2, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XGROUP_DELCONSUMER] = {"xgroup|delconsumer", 0, OWN_KEYS_ARG2, OWN_MM_NONE, OWN_FAM_STREAM},
    [OWN_XINFO_STREAM] = {"xinfo|stream", OWN_K_READ, OWN_KEYS_ARG2, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XINFO_GROUPS] = {"xinfo|groups", OWN_K_READ, OWN_KEYS_ARG2, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XINFO_CONSUMERS] = {"xinfo|consumers", OWN_K_READ, OWN_KEYS_ARG2, OWN_MM_UNBOUND, OWN_FAM_STREAM},
    [OWN_XSETID] = {"xsetid", 0, OWN_KEYS_ONE, OWN_MM_NONE, OWN_FAM_STREAM},
};
static const char *const own_fam_names[OWN_NFAM] = {
    [OWN_FAM_STRING] = "string", [OWN_FAM_TXN] = "txn",   [OWN_FAM_LIST] = "list",
    [OWN_FAM_KEYSPACE] = "keyspace", [OWN_FAM_HASH] = "hash", [OWN_FAM_SETZSET] = "setzset",
    [OWN_FAM_SCRIPT] = "script", [OWN_FAM_STREAM] = "stream"};

/* Why an owner handed a client to main (client->owner_why). OWN_WHY_ACCEPTED: its TLS handshake ended
 * on the owner (done or failed); main runs the accept handler (RL-g1d). */
enum { OWN_WHY_RUN = 1, OWN_WHY_CLOSE, OWN_WHY_QBLIMIT, OWN_WHY_IDLE, OWN_WHY_ACCEPTED, OWN_WHY_TIMEOUT };

/* RL-g1d owner TLS ports: a listener's accept-handler data is the owner id, | OWN_LISTEN_TLS for its TLS twin. */
#define OWN_LISTEN_TLS 0x10000
/* A TLS read returns at most one record; reads in a row while OpenSSL may hold more input (ownerReadTls). */
#define OWN_TLS_READS 16
/* RL-g2k: reads a TLS client whose input filled a batch read makes directly before it goes back to the
 * io_uring batches; a direct read of at least OWN_TLS_BIG_READ bytes starts the count again. */
#define OWN_TLS_DIRECT 15
#define OWN_TLS_BIG_READ 4096
/* RL-g2s: ready events a wakeup needs before its TLS clients use the io_uring batches (ownerThreadMain). */
#define OWN_TLS_BATCH_MIN 8

/* Native-path vetoes (own_veto). */
#define OWN_VETO_ALL 1
#define OWN_VETO_WRITE 2
#define OWN_VETO_BLOCKED 4 /* RL-g5c-block-scope: clients blocked on main; only commands on their slots go to main */

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
    int tls;      /* RL-g2k: a TLS client's ciphertext (read: into the scratch; write: buf/len is the record bytes) */
    size_t ptlen; /* RL-g2k write: the reply bytes ownerTlsSpan gave */
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
    unsigned long long rng_seed; /* RL-g3g: this owner's own random state (genrand64, threadRandom), drawn by main */
    int port; /* 0: no listener */
    int lfd[CONFIG_BINDADDR_MAX];
    int nlfd;
    int tls_port; /* RL-g1d: its TLS twin (owner-tls-port + id); 0: none */
    int tlfd[CONFIG_BINDADDR_MAX];
    int ntlfd;
    /* RL-g1d: TLS clients (ownerTag) whose input OpenSSL may hold where no event reports it, read in the
     * next window (ownerTlsPending); stale entries are skipped as stale epoll events are. */
    uint64_t *tlsp;
    int ntlsp, tlsp_cap;
    long long tls_handshakes, tls_reads; /* INFO, never cleared */
    /* RL-g2v: TLS accepts this owner finished itself (no trip to main; INFO, never cleared), and the ones
     * main has not counted in stat_numconnections yet (folded at the next stop). */
    long long tls_local_accepts, accepts_unfolded;
    long long tls_uring_reads, tls_uring_writes; /* RL-g2k: TLS reads and writes on the io_uring batches */
    pthread_t thread;
    int epfd, wakefd;
    client **fdmap; /* fd -> resident client; written by the owner in a window, by main under the stop */
    int fdmap_cap;
    int nclients;
    pthread_mutex_t inbox_mu;
    list *inbox; /* clients main handed over, not yet registered */
    int inbox_wake; /* main: clients went into the inbox while the world is held; wake the owner at resume */
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
    _Atomic int drain_req; /* RL-g7b: 1 = this window's stream records wait for the lock holder, 2 = appended */
    int release;    /* main: the AOF has them now, write the held replies (ownerWriteHeld) */
    int may_leave;    /* RL-g4a: this window may leave work for the next one on a pending stop (ownerStopPending) */
    int left_early;   /* RL-g4a: this window left work for the next one */
    int leave_streak; /* RL-g4a: consecutive windows that left work behind (at most OWN_LEAVE_STREAK) */
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
    long long flow_skipped_total, flow_naps_total; /* RL-g3h: reads left in the socket, naps after such a window */
    long long flow_paced_total, flow_paced_us_total; /* RL-g3h: pacing naps after a window, their planned us */
    long long held_total, mm_refused_total;
    long long burst_total, bursts_total; /* GETs answered by bursts, and the bursts (ownerGetBurst) */
    long long setburst_total, setbursts_total; /* SETs answered by SET bursts, and those bursts (ownerSetBurst) */
    long long fam_total[OWN_NFAM]; /* RL-1: native commands by family (own_kinds[].family; in native_total too) */
    long long tail_total;          /* RL-g2o: native long-tail commands (OWN_K_TAIL; in native_total too) */
    /* RL-g4c-blocking-native: clients parked on this owner. Written by the owner in its windows, by main only with
     * the world stopped (ownerReclaim, ownerParkedToMain). */
    dict **park_keys;   /* per db: key -> list of ownerPark, in the order they parked (keylistDictType) */
    list *parked;       /* every ownerPark, in the order they parked */
    list *ready;        /* ownerReady: keys a write of this owner made ready, served after its command */
    list *unparked;     /* clients served or timed out in this window: replies and the rest of their input */
    rax *park_to;       /* 8-byte big-endian deadline + 8-byte client id -> ownerPark, parked clients with a timeout */
    long long nparked, nparked_keys, block_total, park_wakes, park_timeouts;
    long long rerun_timeout; /* the original timeout of the parked command being run again */
    long long park_next;     /* the earliest parked deadline (0: none), for epoll_wait (set in the window) */
    int serving;        /* in ownerServeReady */
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
    /* RL-g1f-owner-evalsha (ownerScriptRun): this owner's Lua state and its copy of main's script cache
     * (both owner only), the shas it asked main for (written by the owner in a window, taken by main
     * under the stop) and main's answers (written by main under the stop for epoch sans_epoch, taken by
     * the owner in a window), the client a script's commands run on, and INFO totals. */
    void *lua;
    dict *scripts;
    unsigned long long script_epoch;
    struct ownerScript *cur_script;
    sds *swant;
    int nswant, swant_cap;
    struct ownerScriptAnswer *sans;
    int nsans, sans_cap;
    unsigned long long sans_epoch;
    client *sc;
    long long script_runs, script_aborts, script_misses;
    /* RL-g2c-owner-cron: the cron of this owner's clients (ownerClientsCron), run in its windows. */
    long long cron_next_us; /* monotonic: when the next slice is due */
    int cron_period_ms;     /* 1000 / hz, as of the last slice (epoll timeout outside windows) */
    int cron_fd;            /* fdmap cursor: each slice walks fdmap_cap / hz entries, the whole map once a second */
    size_t peak_in[CLIENTS_PEAK_MEM_USAGE_SLOTS], peak_out[CLIENTS_PEAK_MEM_USAGE_SLOTS]; /* clientsCron's peaks */
    long long cmem[CLIENT_TYPE_COUNT]; /* stat_clients_type_memory changes, folded by main under the stop */
    long long cron_clients, cron_slices, cron_timeouts, cron_closes; /* INFO, never cleared */
    /* RL-g2f-owner-spin: adaptive spinning on the owner's own core, always outside a window (active 0),
     * so main's stop never waits for a spinning owner. spin_ok: the owner is pinned to a CPU main is not
     * pinned to (an owner sharing main's core never spins: it would take main's CPU). The budgets adapt:
     * a spin that found work grows its budget by a quarter (up to the config), one that found none
     * halves it; a sleep that ended sooner than the config sets it to twice that sleep. */
    int spin_ok;
    int spin_us;      /* idle budget now: poll the owner's epoll this long before sleeping in it */
    int spin_stop_us; /* stop budget now: spin this long on own_stop before sleeping on own_wait_cv */
    long long spins, spin_hits, spin_time_us, waits;   /* INFO, never cleared */
    long long stop_spins, stop_spin_hits, stop_sleeps; /* INFO, never cleared */
    /* RL-g3b-light-cron: what main reads while the owners run, published by the owner at the end of each window
     * (relaxed stores; the counters are the unfolded ones, which main zeroes when it folds them, owners stopped). */
    _Atomic long long pub_cmds, pub_net_in, pub_net_out, pub_dirty, pub_exp_min;
    long long exp_min;              /* the earliest key / field expire this owner set since the last reset */
    int tbl_lo, tbl_hi, tbl_cursor; /* its keyspace tables, and the rightsize cursor over them */
    long long tbl_checked, tbl_rehash_us; /* INFO, never cleared: tables rightsize-checked, rehash microseconds */
    /* RL-g2p-busy-poll: bp_ok = this owner may busy poll (pinned to a CPU main is not pinned to); bp_on =
     * its epoll set has owner-busy-poll-us now (main: EPIOCSPARAMS took); tid = the owner thread's id
     * (INFO reads its sleeps); epoll_waits = epoll_waits that could block (INFO, never cleared). */
    int bp_ok, bp_on;
    _Atomic int tid;
    long long epoll_waits;
    /* RL-g2b: active expiry of this owner's keys (ownerExpireRun), only by this thread in its windows.
     * Per db and own table a scan cursor, per db the table its walk is at; the cycle in progress (it
     * runs in slices); what main folds (cycle time, time-limit stops, the TTL sample for avg_ttl). */
    unsigned long long *exp_cur;  /* [db * exp_ntables + t]: table exp_t0 + t */
    int *exp_at;                  /* [db]: the walk's table, 0..exp_ntables-1 */
    long long *exp_ttl_sum, *exp_ttl_n; /* [db]: folded and cleared by main */
    int exp_t0, exp_ntables;      /* its tables */
    int exp_db, exp_dbs, exp_j;   /* the cycle in progress: next db, dbs left to do, dbs visited */
    long long exp_left;           /* its time left in us; 0: no cycle in progress */
    unsigned long exp_c_sampled, exp_c_expired; /* its samples so far (the stale estimate at its end) */
    int exp_timelimit;            /* the last cycle stopped on its time limit (stock timelimit_exit) */
    int exp_fold;                 /* something for main to fold */
    int exp_tfd, exp_hz, exp_due; /* the slow cycles' timer (timerfd, period 1/exp_hz; 0: off); it fired */
    int cron_tick; /* RL-g3f: the expire timer fired; while it runs it also ticks RL-g2c / RL-g3b's cron slices */
    monotime exp_last_fast;
    double exp_stale;             /* stock stat_expired_keys_stale_perc, over this owner's keys */
    long long exp_us, exp_cap;    /* folded and cleared by main: cycle time, time-limit stops */
    long long exp_cycles, exp_fast, exp_slices, exp_keys, exp_sampled, exp_us_total; /* INFO, never cleared */
#ifdef HAVE_LIBURING
    struct io_uring ring;
#endif
} owner;

_Thread_local owner *own_self = NULL;
/* RL-g4c-blocking-native: clients parked on every owner, the order they parked in, and how many main made stock
 * blocked clients. */
static _Atomic long long own_parked_total = 0;
static _Atomic long long own_park_seq = 0;
static long long own_parked_moved = 0;
/* RL-g5c-block-scope: the slots a stock LIST/ZSET/STREAM client is blocked on, so the native path sends only the
 * commands that write one of those slots (or block there) to main. Written by main at resume (world stopped),
 * read by the owners in their windows. Published by the own_stop barrier, like own_mm_slice. */
#define OWN_BLOCKED_WORDS (CLUSTER_SLOTS / 64)
static uint64_t own_blocked_slots[OWN_BLOCKED_WORDS];
static int own_blocked_any = 0;        /* main: a bit is set (resume clears the map only when it must) */
static long long own_block_scoped = 0; /* INFO: resumes with a scoped veto */
static inline int ownerSlotBlocked(int slot) {
    if (slot < 0) return 0;
    return (int)((own_blocked_slots[slot >> 6] >> (slot & 63)) & 1);
}
static void ownerParkedToMainSlot(int slot);
static void ownerServeReady(struct owner *o);
static int ownerParkedRead(struct owner *o, client *c);
static void ownerUnpark(struct owner *o, client *c);
static int ownerParkWaitMs(struct owner *o);
static void ownerParkTimeouts(struct owner *o);
static int ownerRunUnparked(struct owner *o);
static long long ownerParkNext(struct owner *o);
int owner_accept_home = -1;

static owner *owners = NULL;
static int own_n = 0;
static struct serverCommand *own_cmds[OWN_NCMDS];

/* The native kind of a command, or -1 (own_cmds[k]->owner_kind = k + 1, set by ownerThreadsInit). By
 * command, not proc: HSET and HMSET share a proc, SMEMBERS shares SINTER's. */
static inline int ownerKindOf(struct serverCommand *cmd) {
    return cmd->owner_kind - 1;
}

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
/* owner-pipeline-resume (RL-g1g), main only: guests main stopped between two commands because their owner
 * runs the next one (ownerGuestYield), sent home at the end of beforeSleep (ownerResumeYielded); and the
 * ones that could not go, whose rest main runs at its next wakeup (ownerRerunOnMain). client->owner_resume
 * says which list a client is in, or that it is on its way home (OWN_RES_HOME: its owner runs the rest). */
enum { OWN_RES_NONE = 0, OWN_RES_YIELDED, OWN_RES_HOME, OWN_RES_RERUN };
static list *own_yielded = NULL, *own_rerun = NULL;
static int own_no_yield = 0; /* main runs a rerun client's whole input itself */
static long long own_resumes = 0, own_reruns = 0;
static void ownerResumeYielded(int home);
static void ownerRerunAdd(client *c);
static void ownerRerunOnMain(void);

static long long own_stops = 0, own_stop_wait_us = 0, own_stop_wait_max_us = 0;
/* RL-g2f: resumes that had to wake owners sleeping on own_wait_cv, and main's time in those wakes. */
static long long own_resume_broadcasts = 0, own_resume_broadcast_us = 0;
static long long own_takes = 0, own_returns = 0;
static long long own_home_now = 0; /* RL-g2a: handed-over clients sent home right after their command */
/* RL-g4a: work an owner left for its next window because main asked for the world (ownerStopPending). */
static _Atomic long long own_early_leaves = 0, own_inbox_left = 0, own_reads_left = 0;
static int own_last_veto = OWN_VETO_ALL;
static int own_dormant = 0; /* io-threads > 1: owners sleep, main serves everything */

/* RL-g1f-owner-evalsha: bumped by main (owners stopped) whenever a script owners may hold leaves main's
 * cache (SCRIPT FLUSH, a delete); an owner drops its copies when its cache is for an older epoch. */
static unsigned long long own_script_epoch = 1;
static void ownerScriptAnswerAll(void);

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
/* RL-g7b: own_aof_mu taken by owners (ownerAofLock): at once, after a short spin, after sleeping on it */
static _Atomic long long own_aof_lock_takes = 0, own_aof_lock_spun = 0, own_aof_lock_sleeps = 0;
/* RL-g7b: replication only, no maxmemory (owner-repl-combine): the lock holder appends the waiting owners' records
 * too (ownerDrainCombined); batches appended for another owner, and holds that appended any */
static int own_combine = 0;
static _Atomic long long own_drain_combined = 0, own_drain_combines = 0;
#define OWN_AOF_FSYNC_BYTES (32LL << 20) /* everysec: an owner drain starts an fsync once this much is unsynced */
static int own_mm = 0;                    /* maxmemory: native steps check their headroom */
static long long own_mm_slice = 0;        /* bytes each owner may add this period */
static unsigned long long own_mm_period = 0;
static _Atomic int own_mm_over = 0;       /* an owner went over its slice: every owner sends work to main */
static int own_mm_lease = 0;              /* maxmemory and owner-mm-lease: batched reads and SET bursts in the lease */
static long long own_aof_ops = 0;         /* main: ops folded into the AOF */
/* RL-g2b: the owners run the active expiry of their own keys (ownerExpireRun) until main's next stop; set
 * by main when it resumes them, read by the owners in and between windows. */
static _Atomic int own_expire = 0;
/* GET bursts (ownerGetBurst): the command a "GET"/"get" frame resolves to, when that is getCommand under
 * its own name (rename-command can move it); NULL turns bursts off. The shared name objects stand in
 * for the frame's argv[0] in slow commandlog entries. */
static struct serverCommand *own_burst_cmd = NULL;
static robj *own_burst_name[2]; /* "GET", "get" */
/* SET bursts (ownerSetBurst, D3-all-g10-setburst): the same for "SET"/"set" frames and setCommand. */
static struct serverCommand *own_setburst_cmd = NULL;
static robj *own_setburst_name[2]; /* "SET", "set" */

/* RL-g2c-owner-cron: whether the owners run their clients' cron this period (set by main at resume:
 * ownerCronOnOwners() and no pause-cron). Main's clientsCron passes over owner clients under the same rule. */
static _Atomic int own_cron = 0;
/* Main's wakes: what woke each iteration and how long the world was held (INFO). A cron stop is a stop of an
 * iteration that no file event woke and that ran serverCron or clientsTimeProc: the owners stopped for cron alone. */
static monotime own_stop_t0 = 0;
static int own_iter_events = 0, own_iter_cron = 0;
static long long own_held_us = 0, own_cron_stops = 0, own_cron_held_us = 0;
/* RL-g2m-fixed-rate-tail: stops by cause and the held times' log2 histogram (bucket b: [2^b, 2^(b+1)) us). */
enum { OWN_CAUSE_CRON, OWN_CAUSE_WAKE, OWN_CAUSE_FILE, OWN_CAUSE_OTHER, OWN_NCAUSES };
static const char *const own_cause_names[OWN_NCAUSES] = {"cron", "wake", "file", "other"};
#define OWN_HELD_BUCKETS 20
static int own_iter_wake = 0;
static long long own_cause_stops[OWN_NCAUSES], own_cause_held_us[OWN_NCAUSES];
static long long own_held_hist[OWN_HELD_BUCKETS], own_held_max_us = 0;
/* RL-g3b-light-cron: this iteration runs without the world (ownerAfterSleep chose it; ownerHoldWorld ends it). */
static int own_light = 0;
static _Atomic int own_tables = 0; /* the owners rightsize and rehash their own tables this period */
static _Atomic long long own_exp_min_other = LLONG_MAX; /* expires set by main and any thread but the owners */
static _Atomic long long own_fexp_min = LLONG_MAX; /* RL-g3f: hash-field expiry (0 once a hash with volatile fields is tracked) */
static long long own_light_iters = 0, own_light_full = 0, own_light_full_why[OWN_FULL_NCOUNT];
enum { OWN_NOLIGHT_CONFIG, OWN_NOLIGHT_VETO, OWN_NOLIGHT_AOF, OWN_NOLIGHT_MAXMEMORY, OWN_NOLIGHT_CHILD, OWN_NOLIGHT_REPL,
       OWN_NOLIGHT_PENDING, OWN_NOLIGHT_OTHER, OWN_NOLIGHT_NCOUNT };
static long long own_nolight[OWN_NOLIGHT_NCOUNT];
static const char *const own_full_names[OWN_FULL_NCOUNT] = {"timeproc", "shutdown", "verbose", "expire", "defrag",
                                                            "tables", "persist", "clients", "tls", "hotkeys"};
static const char *const own_nolight_names[OWN_NOLIGHT_NCOUNT] = {"config", "veto",  "aof",     "maxmemory",
                                                                  "child",  "repl", "pending", "other"};

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
    if (server.owner_tls_port) {
        if (server.owner_tls_port + server.owner_threads - 1 > 65535) {
            *err = "owner-tls-port + owner-threads - 1 must be a valid port";
            return C_ERR;
        }
        if (!server.tls_port) {
            *err = "owner-tls-port needs tls-port (the server's TLS context)";
            return C_ERR;
        }
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
void clientAcceptHandler(connection *conn); /* networking.c: the rest of an accept after the transport's */
static void ownerTlsRelease(client *c);
static void ownerTlsOnMain(client *c);
static void ownerBeforeTimeProc(aeEventLoop *el, aeTimeProc *proc); /* RL-g3b */
static int ownerLightWhyNot(void);                                 /* RL-g3b */

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

/* RL-g3i-accept-storm: the kernel grows a process's fd table by doubling, and in a process with more than one
 * thread each growth waits an RCU grace period (expand_fdtable: 50-100 ms on vkevo-build, fdgrow.py). Main
 * accepts with the world held, so a connection storm that took the server past 1024, 2048, 4096 or 8192 fds
 * stopped every owner for that long, once per doubling (the 30-110 ms holds of RL-2's storm cells, and of
 * memtier's first connections). Grown here once, at startup, to what the open-files limit allows for
 * maxclients: no accept grows it after that unless CONFIG SET maxclients raises the limit. */
static void ownerPresizeFdTable(int any_fd) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == -1 || rl.rlim_cur == RLIM_INFINITY) return;
    long long want = (long long)server.maxclients + CONFIG_MIN_RESERVED_FDS;
    if (want > (long long)rl.rlim_cur) want = (long long)rl.rlim_cur;
    int fd = want > 1024 ? fcntl(any_fd, F_DUPFD_CLOEXEC, (int)want - 1) : -1;
    if (fd == -1) {
        serverLog(LL_VERBOSE, "owner threads: fd table not presized (%lld): %s", want, strerror(errno));
        return;
    }
    close(fd);
    serverLog(LL_NOTICE, "owner threads: fd table presized for %lld fds (no growth while the world is held)", want);
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
    own_yielded = listCreate();
    own_rerun = listCreate();
    for (int k = 0; k < OWN_NCMDS; k++) {
        /* By original name: rename-command moves the same command under another name. */
        own_cmds[k] = lookupCommandByCStringLogic(server.orig_commands, own_kinds[k].name);
        serverAssert(own_cmds[k] != NULL);
        serverAssert(!(own_kinds[k].flags & OWN_K_SCRIPT) || k < OWN_SCRIPT_NCMDS);
        own_cmds[k]->owner_kind = k + 1; /* ownerKindOf */
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
    aeSetBeforeTimeProc(server.el, ownerBeforeTimeProc); /* RL-g3b */
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        o->id = i;
        o->cpu = ownerCpuOf(i);
        /* Whole tables: owner i owns tables [T*i/N, T*(i+1)/N), each 16384/T slots. */
        const int tables = 1 << OWNER_SHARD_BITS, per = CLUSTER_SLOTS / tables;
        o->slot_lo = (int)((long long)tables * i / own_n) * per;
        o->slot_hi = (int)((long long)tables * (i + 1) / own_n) * per - 1;
        o->tbl_lo = OWNER_TABLE_OF_SLOT(o->slot_lo);
        o->tbl_hi = OWNER_TABLE_OF_SLOT(o->slot_hi);
        o->exp_min = LLONG_MAX;
        atomic_store_explicit(&o->pub_exp_min, LLONG_MAX, memory_order_relaxed);
        o->inbox = listCreate();
        pthread_mutex_init(&o->inbox_mu, NULL);
        o->watched = zmalloc(sizeof(dict *) * server.dbnum);
        for (int j = 0; j < server.dbnum; j++) o->watched[j] = dictCreate(&keylistDictType);
        o->park_keys = zcalloc(sizeof(dict *) * server.dbnum);
        for (int j = 0; j < server.dbnum; j++) o->park_keys[j] = dictCreate(&keylistDictType);
        o->parked = listCreate();
        o->ready = listCreate();
        o->unparked = listCreate();
        o->park_to = raxNew();
        o->aofbuf = sdsempty();
        o->replbuf = sdsempty();
        o->sc = createClient(NULL); /* RL-g1f-owner-evalsha: a script's commands run on it */
        o->sc->flag.script = 1;
        o->sc->flag.argv_borrowed = 1; /* as VM_CallArgv's: the vector belongs to the script run */
        o->aof_first_db = -1;
        /* RL-g2b: the tables this owner expires keys in (its own: whole tables) */
        o->exp_t0 = OWNER_TABLE_OF_SLOT(o->slot_lo);
        o->exp_ntables = OWNER_TABLE_OF_SLOT(o->slot_hi) - o->exp_t0 + 1;
        o->exp_cur = zcalloc(sizeof(unsigned long long) * server.dbnum * o->exp_ntables);
        o->exp_at = zcalloc(sizeof(int) * server.dbnum);
        o->exp_ttl_sum = zcalloc(sizeof(long long) * server.dbnum);
        o->exp_ttl_n = zcalloc(sizeof(long long) * server.dbnum);
        o->fdmap_cap = 1024;
        o->fdmap = zcalloc(sizeof(client *) * o->fdmap_cap);
        o->epfd = epoll_create1(EPOLL_CLOEXEC);
        o->wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        struct epoll_event ev = {.events = EPOLLIN, .data.u64 = OWNER_WAKE_TOKEN};
        if (o->epfd == -1 || o->wakefd == -1 || epoll_ctl(o->epfd, EPOLL_CTL_ADD, o->wakefd, &ev) == -1) {
            serverLog(LL_WARNING, "owner threads: cannot create owner %d's event loop: %s", i, strerror(errno));
            exit(1);
        }
        /* RL-g2b: the owner's slow expire cycle timer (armed by the owner itself, ownerExpireArm) */
        o->exp_tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        struct epoll_event tev = {.events = EPOLLIN, .data.u64 = OWNER_EXPIRE_TOKEN};
        if (o->exp_tfd == -1 || epoll_ctl(o->epfd, EPOLL_CTL_ADD, o->exp_tfd, &tev) == -1) {
            serverLog(LL_WARNING, "owner threads: cannot create owner %d's expire timer: %s", i, strerror(errno));
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
        /* RL-g1d: its TLS twin, with the server's TLS context (one SSL_CTX, an SSL object per connection,
         * made on main at accept; CONFIG SET of the TLS files reaches new connections here as on tls-port). */
        if (server.owner_tls_port) {
            connListener l = {0};
            l.bindaddr = server.bindaddr;
            l.bindaddr_count = server.bindaddr_count;
            l.port = server.owner_tls_port + i;
            l.ct = connectionTypeTls();
            if (!l.ct || listenToPort(&l) == C_ERR) {
                serverLog(LL_WARNING, "owner threads: failed listening on TLS port %d for owner %d", l.port, i);
                exit(1);
            }
            o->tls_port = l.port;
            o->ntlfd = l.count;
            for (int j = 0; j < l.count; j++) {
                o->tlfd[j] = l.fd[j];
                if (aeCreateFileEvent(server.el, l.fd[j], AE_READABLE, ownerAcceptHandler,
                                      (void *)(intptr_t)(i | OWN_LISTEN_TLS)) == AE_ERR) {
                    serverLog(LL_WARNING, "owner threads: cannot watch owner %d's TLS listener", i);
                    exit(1);
                }
            }
        }
    }
    serverLog(LL_NOTICE, "owner threads: %d owners, ports %d-%d, slots split %d ways%s", own_n, server.owner_port,
              server.owner_port ? server.owner_port + own_n - 1 : 0, own_n,
              server.owner_adopt_all ? ", every TCP client adopted" : "");
    if (server.owner_tls_port)
        serverLog(LL_NOTICE, "owner threads: TLS ports %d-%d (handshake, reads and writes on the owners)",
                  server.owner_tls_port, server.owner_tls_port + own_n - 1);
    ownerPresizeFdTable(owners[0].epfd);
#endif
}

/* ------------------------------------------------------------------------------------------------
 * The world: main stops the owners whenever it runs
 * ---------------------------------------------------------------------------------------------- */

/* RL-g2f: the spin budgets' caps from the configs (CONFIG SET on main; read here without a lock). */
static inline int ownerSpinCap(const int *cfg) {
    return __atomic_load_n(cfg, __ATOMIC_RELAXED);
}

static inline int ownerSpinGrow(int b, int cap) {
    b += b / 4 + 1;
    return b > cap ? cap : b;
}

/* After a sleep of `slept` us that the budget did not cover: a sleep shorter than the cap means work
 * comes about that often, so the next spin covers twice that. */
static inline void ownerSpinSeed(int *budget, long long slept, int cap) {
    if (slept >= cap) return;
    int g = (int)(2 * slept + 1);
    if (g > cap) g = cap;
    if (g > *budget) *budget = g;
}

static void ownerWaitWorld(owner *o) {
    for (int i = 0; i < 4096; i++) {
        if (!atomic_load_explicit(&own_stop, memory_order_relaxed)) return;
        ownerCpuRelax();
    }
    /* RL-g2f: keep spinning on this owner's own core (outside the window: active is 0, main is not
     * waiting for it) while main's stops end within the budget, so main's resume has no one to wake. */
    int cap = o->spin_ok ? ownerSpinCap(&server.owner_spin_stop_us) : 0;
    monotime t0 = 0;
    if (cap > 0) {
        t0 = getMonotonicUs();
        int b = o->spin_stop_us < cap ? o->spin_stop_us : cap;
        if (b > 0) {
            o->stop_spins++;
            monotime end = t0 + (monotime)b;
            for (;;) {
                for (int i = 0; i < 64; i++) {
                    if (!atomic_load_explicit(&own_stop, memory_order_relaxed)) {
                        o->stop_spin_hits++;
                        o->spin_stop_us = ownerSpinGrow(b, cap);
                        return;
                    }
                    ownerCpuRelax();
                }
                if (getMonotonicUs() >= end) break;
            }
            o->spin_stop_us = b / 2;
        }
    }
    o->stop_sleeps++;
    pthread_mutex_lock(&own_wait_mu);
    atomic_fetch_add_explicit(&own_waiters, 1, memory_order_seq_cst);
    while (atomic_load_explicit(&own_stop, memory_order_seq_cst)) pthread_cond_wait(&own_wait_cv, &own_wait_mu);
    atomic_fetch_sub_explicit(&own_waiters, 1, memory_order_relaxed);
    pthread_mutex_unlock(&own_wait_mu);
    if (cap > 0) ownerSpinSeed(&o->spin_stop_us, (long long)(getMonotonicUs() - t0), cap);
}

static void ownerEnter(owner *o) {
    for (;;) {
        atomic_store_explicit(&o->active, 1, memory_order_relaxed);
        atomic_thread_fence(memory_order_seq_cst);
        if (!atomic_load_explicit(&own_stop, memory_order_relaxed)) break;
        atomic_store_explicit(&o->active, 0, memory_order_release);
        ownerWaitWorld(o);
    }
    /* Pairs with main's release of own_stop: everything main did before resuming is visible. */
    atomic_thread_fence(memory_order_acquire);
}

static void ownerLeave(owner *o) {
    atomic_store_explicit(&o->active, 0, memory_order_release);
}

/* RL-g4a-park-early: main asked for the world (ownerStopWorld set own_stop and waits for every active owner).
 * An owner that sees it in a window takes on no new work: it leaves at the next point where nothing of its own
 * is half done, and the work it did not start waits for its next window, which begins when main resumes.
 * Progress: after OWN_LEAVE_STREAK windows in a row that left work behind, the next one does all its work, as
 * before this gene, so nothing waits more than OWN_LEAVE_STREAK stops (each stop ~0.1-0.5 ms under load). */
#define OWN_LEAVE_STREAK 4 /* RL-g4a: windows in a row that may leave work for the next one */
static inline int ownerStopPending(owner *o) {
    if (!o->may_leave || !atomic_load_explicit(&own_stop, memory_order_relaxed)) return 0;
    o->left_early = 1;
    return 1;
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
            if (pu->c) pu->c->woff = base + (long long)at; /* RL-g2b: an active expire's unit has no client */
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
                if (pu->c) pu->c->woff = server.primary_repl_offset;
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

/* RL-g3b-light-cron, main, world stopped and the kvstores folded: with no key or field expire left anywhere, the
 * expire bound starts over (every expire set from now on lowers it again). */
static void ownerExpireBoundReset(void) {
    int keys = 0, fields = 0;
    for (int j = 0; j < server.dbnum; j++) {
        serverDb *db = server.db[j];
        if (!db) continue;
        keys |= kvstoreSize(db->expires) != 0;
        fields |= kvstoreSize(db->keys_with_volatile_items) != 0;
    }
    /* RL-g3f: the field bound starts over once no hash has volatile fields, whatever the keys' expires */
    if (!fields) atomic_store_explicit(&own_fexp_min, LLONG_MAX, memory_order_relaxed);
    if (keys || fields) return;
    atomic_store_explicit(&own_exp_min_other, LLONG_MAX, memory_order_relaxed);
    for (int i = 0; i < own_n; i++) {
        owners[i].exp_min = LLONG_MAX;
        atomic_store_explicit(&owners[i].pub_exp_min, LLONG_MAX, memory_order_relaxed);
    }
}

/* Main: wait until no owner is inside a window. From here until ownerResumeWorld main has the
 * keyspace, the clients and every owner's state to itself. */
static void ownerStopWorld(void) {
    if (own_held) return;
    atomic_store_explicit(&own_stop, 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    monotime t0 = getMonotonicUs();
    own_stop_t0 = t0;
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
    ownerScriptAnswerAll();
    ownerExpireBoundReset();
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
     * feeds the stream); being a replica is not, except for reads (RL-g7d, below). */
    if ((server.aof_state != AOF_OFF && server.aof_state != AOF_ON) ||
        (server.primary_host && !server.owner_replica_reads) ||
        (!server.owner_repl_native && (server.repl_backlog || listLength(server.replicas))))
        return OWN_VETO_ALL;
    if (server.loading || server.async_loading || server.paused_actions || server.import_mode) return OWN_VETO_ALL;
    /* maxmemory is served natively within each owner's headroom (ownerMmCmdFits), down to
     * OWN_MM_TIGHT; client eviction and batched eviction decide on main between commands from state
     * owners change. */
    if (server.maxmemory_clients || (server.maxmemory && server.maxmemory_eviction_batch)) return OWN_VETO_ALL;
    if (server.maxmemory && ownerMmHeadroom() / own_n < OWN_MM_TIGHT) return OWN_VETO_ALL;
    if (listLength(server.monitors) || !trackingIsIdle() || server.notify_keyspace_events) return OWN_VETO_ALL;
    if (server.watching_clients) return OWN_VETO_ALL;
    if (moduleCountLoaded() || moduleObservesWrites() || server.blocked_clients_by_type[BLOCKED_MODULE] ||
        server.busy_module_yield_flags)
        return OWN_VETO_ALL;
    if (server.latency_monitor_threshold || hotkeysEnabled() || bgIteration_iterationActive() ||
        throttle_anyThrottlers())
        return OWN_VETO_ALL;
    int v = 0;
    /* RL-g5c-block-scope: a client blocked on main no longer vetoes the whole native path; only the commands that
     * write a slot one of them waits on (or block there) go to main (ownerSlotBlocked, rebuilt at resume). */
    if (server.blocked_clients_by_type[BLOCKED_LIST] || server.blocked_clients_by_type[BLOCKED_ZSET] ||
        server.blocked_clients_by_type[BLOCKED_STREAM])
        v |= OWN_VETO_BLOCKED;
    if (writeCommandsDeniedByDiskError() != DISK_ERROR_TYPE_NONE || !checkGoodReplicasStatus())
        v |= OWN_VETO_WRITE;
    /* RL-g7d-replica-reads: a replica's owners run the read kinds (OWN_K_READ, PING, MULTI / DISCARD around queued
     * reads) and hand everything else to main, where processCommand answers writes as the stock replica does
     * (-READONLY, or a local write on a writable replica). Main applies the primary's stream with the world held, so
     * a native read sees the keyspace between two applied commands (a MULTI block is applied by its EXEC, whole):
     * a prefix of the stream. A lookup on a replica keeps an expired key (POLICY_KEEP_EXPIRED: the primary's DEL
     * removes it). While the link is down and replica-serve-stale-data is no, main answers every command
     * (MASTERDOWN), as stock. */
    if (server.primary_host) {
        if (server.repl_state != REPL_STATE_CONNECTED && !server.repl_serve_stale_data) return OWN_VETO_ALL;
        v |= OWN_VETO_WRITE;
    }
    return v;
}

/* RL-g5c-block-scope: the cluster slot of a key object (its string form). */
static int ownerKeySlotOf(robj *key) {
    robj *d = getDecodedObject(key);
    int slot = ownerKeyHashSlot(objectGetVal(d), sdslen(objectGetVal(d)));
    decrRefCount(d);
    return slot;
}

/* Main, world stopped: set own_blocked_slots to the slots a stock LIST/ZSET/STREAM client is blocked on
 * (db->blocking_keys of every db), or clear it. The owners read it in their next windows (ownerSlotBlocked). */
static void ownerRebuildBlockedSlots(int scoped) {
    if (!scoped) {
        if (own_blocked_any) {
            memset(own_blocked_slots, 0, sizeof(own_blocked_slots));
            own_blocked_any = 0;
        }
        return;
    }
    memset(own_blocked_slots, 0, sizeof(own_blocked_slots));
    own_blocked_any = 1;
    own_block_scoped++;
    for (int j = 0; j < server.dbnum; j++) {
        if (!server.db[j]) continue; /* databases are created when first used */
        dict *bk = server.db[j]->blocking_keys;
        if (!bk || dictSize(bk) == 0) continue;
        dictIterator *di = dictGetIterator(bk);
        dictEntry *de;
        while ((de = dictNext(di))) {
            int slot = ownerKeySlotOf(dictGetKey(de));
            if (slot >= 0) own_blocked_slots[slot >> 6] |= (uint64_t)1 << (slot & 63);
        }
        dictReleaseIterator(di);
    }
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
    /* RL-g4c-blocking-native: a hard veto (replica role, notifications, pause, ...) or maxmemory makes every parked
     * client a stock blocked client on main, which then vetoes the native path until they are served.
     * RL-g5c-block-scope: a scoped veto alone (OWN_VETO_BLOCKED) leaves them parked; their slots are never blocked. */
    if (atomic_load_explicit(&own_parked_total, memory_order_relaxed) &&
        ((own_last_veto & OWN_VETO_ALL) || server.maxmemory)) {
        ownerParkedToMain();
        own_last_veto = ownerComputeVeto();
    }
    atomic_store_explicit(&own_veto, own_last_veto, memory_order_relaxed);
    ownerRebuildBlockedSlots((own_last_veto & OWN_VETO_BLOCKED) != 0); /* RL-g5c-block-scope */
    own_aof = server.aof_state == AOF_ON;
    own_repl = !(own_last_veto & OWN_VETO_ALL) && server.primary_host == NULL &&
               (server.repl_backlog != NULL || listLength(server.replicas) != 0);
    own_keep = own_aof || own_repl;
    own_rec_fmt = own_aof ? server.aof_fd_fmt : AOF_FMT_RESP;
    own_split = own_aof && own_repl && own_rec_fmt != AOF_FMT_RESP;
    /* Each owner appends its own records at the end of its window, except under appendfsync always
     * (main writes every owner's records at its stop and fsyncs them once). */
    own_aof_drain = own_keep && !(own_aof && server.aof_fsync == AOF_FSYNC_ALWAYS);
    /* RL-g7b: replication without the AOF and without maxmemory (the owners' memory leases count each owner's
     * own allocations, and a combined append allocates for another owner): the drains combine. */
    own_combine = server.owner_repl_combine && own_repl && !own_aof && server.maxmemory == 0;
    /* RL-g2b: the owners expire their own keys while nothing vetoes the native path and the stock
     * cycle would run (activeExpireCycle's conditions on main); otherwise main expires every key. */
    int expire = server.owner_active_expire && !own_last_veto && server.active_expire_enabled && iAmPrimary() &&
                 !server.import_mode && !isPausedActions(PAUSE_ACTION_EXPIRE);
    if (expire && !atomic_load_explicit(&own_expire, memory_order_relaxed)) {
        /* An owner asleep with no timeout wakes to start its cycles. */
        for (int i = 0; i < own_n; i++) {
            uint64_t one = 1;
            if (write(owners[i].wakefd, &one, sizeof(one)) == -1) { /* a wakeup is pending anyway */
            }
        }
    }
    atomic_store_explicit(&own_expire, expire, memory_order_relaxed);
    ownerReleaseHeld();
    ownerMmPlan();
    ownerSettleCommandTables();
    ownerSettleTable(server.commands);
    ownerSettleTable(server.orig_commands);
    dbLanePhaseBegin();
    atomic_store_explicit(&own_cron, ownerCronOnOwners() && !server.pause_cron, memory_order_relaxed);
    atomic_store_explicit(&own_tables, ownerTablesOnOwners() && !server.pause_cron, memory_order_relaxed);
    if (own_stop_t0) {
        long long held = (long long)(getMonotonicUs() - own_stop_t0);
        own_held_us += held;
        if (own_iter_cron && !own_iter_events) {
            own_cron_stops++;
            own_cron_held_us += held;
        }
        /* RL-g2m: each stop by what woke main (cron alone, an owner's wake fd, another file event: a main-port
         * client or an accept, anything else), and the held times' log2 histogram. */
        int cause = own_iter_cron && !own_iter_events ? OWN_CAUSE_CRON
                    : own_iter_wake                    ? OWN_CAUSE_WAKE
                    : own_iter_events                  ? OWN_CAUSE_FILE
                                                       : OWN_CAUSE_OTHER;
        own_cause_stops[cause]++;
        own_cause_held_us[cause] += held;
        int b = 0;
        while (b < OWN_HELD_BUCKETS - 1 && (1LL << (b + 1)) <= held) b++;
        own_held_hist[b]++;
        if (held > own_held_max_us) own_held_max_us = held;
    }
    own_iter_cron = 0;
    own_iter_wake = 0;
    own_held = 0;
    atomic_store_explicit(&own_stop, 0, memory_order_seq_cst);
    if (atomic_load_explicit(&own_waiters, memory_order_seq_cst)) {
        monotime b0 = getMonotonicUs();
        pthread_mutex_lock(&own_wait_mu);
        pthread_cond_broadcast(&own_wait_cv);
        pthread_mutex_unlock(&own_wait_mu);
        own_resume_broadcasts++;
        own_resume_broadcast_us += (long long)(getMonotonicUs() - b0);
    }
    /* RL-g1g / RL-g2a: one wakeup per owner whose inbox main filled at this stop (ownerSendToOwner), not one per
     * client. The owner could not have taken them before now: the world was held. */
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        if (!o->inbox_wake) continue;
        o->inbox_wake = 0;
        uint64_t one = 1;
        if (write(o->wakefd, &one, sizeof(one)) == -1) { /* counter full: a wakeup is pending anyway */
        }
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
            /* RL-g3a: the latency histogram folds when something reads it (ownerFoldLatencyHistograms). */
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
        /* RL-g3b: folded, so nothing is left unfolded to publish (owners stopped) */
        atomic_store_explicit(&o->pub_cmds, 0, memory_order_relaxed);
        atomic_store_explicit(&o->pub_net_in, 0, memory_order_relaxed);
        atomic_store_explicit(&o->pub_net_out, 0, memory_order_relaxed);
        atomic_store_explicit(&o->pub_dirty, 0, memory_order_relaxed);
        /* RL-g2c: client memory the owner's cron measured (updateClientMemoryUsage's changes) */
        for (int t = 0; t < CLIENT_TYPE_COUNT; t++) {
            server.stat_clients_type_memory[t] += o->cmem[t];
            o->cmem[t] = 0;
        }
        if (o->exp_fold) { /* RL-g2b: the owner's active expiry */
            server.stat_expire_cycle_time_used += o->exp_us;
            server.stat_expired_time_cap_reached_count += o->exp_cap;
            o->exp_us = o->exp_cap = 0;
            for (int j = 0; j < server.dbnum; j++) {
                if (o->exp_ttl_n[j] && server.db[j]) {
                    /* as activeExpireCycleJob's running average: this sample at 2% */
                    long long avg = o->exp_ttl_sum[j] / o->exp_ttl_n[j], *cur = &server.db[j]->expiry[KEYS].avg_ttl;
                    *cur = *cur == 0 ? avg : avg + (long long)((*cur - avg) * 0.98);
                }
                o->exp_ttl_sum[j] = o->exp_ttl_n[j] = 0;
            }
            o->exp_fold = 0;
        }
        server.stat_numconnections += o->accepts_unfolded; /* RL-g2v: accepts the owner finished */
        o->accepts_unfolded = 0;
    }
    if (ownerExpiresKeys()) { /* RL-g2b: expired_stale_perc over every owner's keys */
        double stale = 0;
        for (int i = 0; i < own_n; i++) stale += owners[i].exp_stale;
        server.stat_expired_keys_stale_perc = stale / own_n;
    }
}

/* RL-g3a-cheap-stop: the owners' latency histograms go into the commands' histograms only when something reads
 * or resets them (INFO latencystats, LATENCY HISTOGRAM, CONFIG RESETSTAT: the points where D0's lanes fold theirs,
 * fastpathFoldLaneHistograms), not at every world stop. hdr_add walks every bucket of the owner's histogram and
 * hdr_reset clears all of them, about 0.5 us per owner and command kind with calls: at W=16 that was 103 us of
 * each 595 us stop. The counts are the same: a bucket's count is the sum of what every owner recorded in it, folded
 * once or at every stop. Main with the world held only (the owners write their histograms in their windows); from
 * anywhere else (an owner thread, a crash report) nothing folds and the readers see what is folded. */
void ownerFoldLatencyHistograms(void) {
    if (!own_n || own_self || !own_held) return;
    for (int i = 0; i < own_n; i++) {
        for (int k = 0; k < OWN_NCMDS; k++) {
            ownerCmdStat *cs = &owners[i].cs[k];
            if (!cs->hist || !cs->hist->total_count) continue;
            struct serverCommand *cmd = own_cmds[k];
            if (cmd->latency_histogram == NULL)
                hdr_init(LATENCY_HISTOGRAM_MIN_VALUE, LATENCY_HISTOGRAM_MAX_VALUE, LATENCY_HISTOGRAM_PRECISION,
                         &cmd->latency_histogram);
            hdr_add(cmd->latency_histogram, cs->hist);
            hdr_reset(cs->hist);
        }
    }
}

/* CONFIG RESETSTAT (resetCommandTableStats): what the owners recorded and main has not folded yet goes too. */
void ownerResetLatencyHistograms(void) {
    if (!own_n || own_self || !own_held) return;
    for (int i = 0; i < own_n; i++)
        for (int k = 0; k < OWN_NCMDS; k++)
            if (owners[i].cs[k].hist && owners[i].cs[k].hist->total_count) hdr_reset(owners[i].cs[k].hist);
}

static void ownerGuestAdd(client *c) {
    if (c->owner_guest_node) return;
    listAddNodeTail(own_guests, c);
    c->owner_guest_node = listLast(own_guests);
}

void ownerClientFreed(client *c) {
    if (c->owner_resume == OWN_RES_YIELDED || c->owner_resume == OWN_RES_RERUN) {
        list *l = c->owner_resume == OWN_RES_YIELDED ? own_yielded : own_rerun;
        listNode *ln = listSearchKey(l, c);
        if (ln) listDelNode(l, ln);
    }
    c->owner_resume = OWN_RES_NONE;
    if (c->owner_guest_node) {
        listDelNode(own_guests, c->owner_guest_node);
        c->owner_guest_node = NULL;
    }
}

static void ownerArmRead(client *c);
static int ownerGoHomeNow(client *c);
static int ownerGuestIdle(client *c, int replies);
static int ownerSendToOwner(client *c);

/* Main, world held: a client an owner handed over becomes a stock client on main. */
static void ownerTakeOne(client *c) {
    serverAssert(c->owner_tid == OWNER_IN_TRANSIT);
    c->owner_tid = OWNER_ON_MAIN;
    int why = c->owner_why;
    c->owner_why = 0;
    own_takes++;
    ownerTlsRelease(c);
    watchMoveToMain(c); /* main serves only stock watchers */
    if (why == OWN_WHY_ACCEPTED) {
        /* Its TLS handshake ended on the owner: the rest of the stock accept (an error is logged and the
         * client freed there), which adopts it again. createClient's read handler, for a guest. */
        if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
            freeClientAsync(c);
            return;
        }
        clientAcceptHandler(c->conn);
        if (c->owner_tid == OWNER_ON_MAIN) ownerTlsOnMain(c);
        return;
    }
    if (c->flag.close_asap) {
        /* The owner asked for the close (output buffer limit); main's queue frees it. */
        c->flag.close_asap = 0;
        freeClientAsync(c);
        return;
    }
    if (why == OWN_WHY_TIMEOUT) {
        /* RL-g2c: clientsCronHandleTimeout on its owner found it idle past the timeout. */
        serverLog(LL_VERBOSE, "Closing idle client");
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
    /* RL-g1g + RL-g2a: a TCP client handed over to run a command gets main's read handler on the connection
     * but not its epoll registration yet (connSetReadHandler registers only a change). Most go home right after
     * the command (ownerGoHomeNow, AOF off), or at the end of beforeSleep with the rest of a pipeline
     * (ownerGuestYield), and main never polls them; one that stays is registered then (ownerArmRead), before
     * main's next poll, in the state the command left: a command that dropped the handler (protectClient, a
     * blocked or throttled client) leaves it dropped. Inside processEventsWhileBlocked, for TLS (RL-g1d's owner
     * TLS ports) and with neither the AOF off nor owner-pipeline-resume, the stock registration as before. */
    int lazy = why == OWN_WHY_RUN && !ProcessingEventsWhileBlocked && c->owner_home >= 0 &&
               c->conn->type == connectionTypeTcp() && !c->conn->read_handler &&
               (server.aof_state == AOF_OFF || server.owner_pipeline_resume);
    if (lazy)
        c->conn->read_handler = readQueryFromClient;
    else if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
        freeClientAsync(c);
        return;
    }
    ownerTlsOnMain(c);
    if (c->owner_home >= 0) ownerGuestAdd(c);
    if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
    if (why == OWN_WHY_QBLIMIT) {
        handleQbLimitReached(c);
        return;
    }
    if (why == OWN_WHY_IDLE) return; /* nothing read: main's own read handler takes the input */
    if (!ownerResumeOnMain(c) || !lazy) return; /* 0: freed or closing, nothing to poll */
    /* Stopped before the rest of a pipeline: it goes home at the end of beforeSleep (ownerResumeYielded). */
    if (c->owner_tid != OWNER_ON_MAIN || c->owner_resume == OWN_RES_YIELDED) return;
    if (!ownerGoHomeNow(c)) ownerArmRead(c);
}

/* RL-g2a: the epoll registration ownerTakeOne deferred, for a client that stays on main past its command. */
static void ownerArmRead(client *c) {
    connection *conn = c->conn;
    ConnectionCallbackFunc f = conn->read_handler;
    if (!f || (aeGetFileEvents(server.el, conn->fd) & AE_READABLE)) return;
    conn->read_handler = NULL;
    if (connSetReadHandler(conn, f) == C_ERR) freeClientAsync(c);
}

/* RL-g2a: main just ran a handed-over client's command. When nothing else about the client needs main
 * (ownerGuestIdle, unsent replies allowed), it goes home now with its replies, which its owner writes when it
 * takes the client (ownerRegister), so main makes no system call for it: no epoll registration, no write, and
 * one owner wakeup per stop (ownerResumeWorld). Main writes nothing to the AOF here (aof_state AOF_OFF), and the
 * owner runs only once main resumes the world, after this iteration's beforeSleep: the replies leave no earlier
 * than main would have sent them, and in the order main appended them. Not while the native path is vetoed
 * (guests stay on main then) or the owners sleep, nor for a client RL-g1g's pipeline path holds. */
static int ownerGoHomeNow(client *c) {
    if (server.aof_state != AOF_OFF || own_dormant || c->owner_resume || (ownerComputeVeto() & OWN_VETO_ALL) ||
        !ownerGuestIdle(c, 1))
        return 0;
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }
    ownerClientFreed(c); /* off the guest list */
    if (!ownerSendToOwner(c)) { /* RL-g1d: only a TLS transport can refuse, and lazy clients are TCP */
        ownerGuestAdd(c);
        if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
        return 0;
    }
    own_returns++;
    own_home_now++;
    return 1;
}

/* One client at a time: a client's command can run a long script, and main then serves the other
 * clients from inside it (processEventsWhileBlocked), including the ones still queued here.
 * RL-g3i-accept-storm: at most OWN_CLOSES_PER_STOP closes per stop. The owners hand every closed client to main,
 * and when a storm of clients disconnects at once main freed them all in one stop (10,000 in 60-110 ms with the
 * world held). The rest wait in order for the next stop, which main's own wakeup brings at once, after the owners ran. */
#define OWN_CLOSES_PER_STOP 256
static int own_stop_closes = 0; /* closes taken since the last stop began (ownerAfterSleep) */
static long long own_close_defers = 0; /* stops that left closes for the next one (INFO) */

static void ownerTakeHandoffs(void) {
    for (;;) {
        if (own_stop_closes >= OWN_CLOSES_PER_STOP) {
            pthread_mutex_lock(&own_main_mu);
            int left = listLength(own_main_inbox) != 0;
            pthread_mutex_unlock(&own_main_mu);
            if (left) {
                uint64_t one = 1;
                own_close_defers++;
                if (write(own_main_wakefd, &one, sizeof(one)) == -1) { /* a wakeup is pending anyway */
                }
            }
            return;
        }
        pthread_mutex_lock(&own_main_mu);
        listNode *ln = listFirst(own_main_inbox);
        client *c = ln ? listNodeValue(ln) : NULL;
        if (ln) listDelNode(own_main_inbox, ln);
        pthread_mutex_unlock(&own_main_mu);
        if (!c) return;
        if (c->owner_why == OWN_WHY_CLOSE) own_stop_closes++;
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
    own_iter_wake = 1; /* RL-g2m: this stop's cause */
    /* afterSleep already took the clients; anything newer arrives with the next wakeup. */
    ownerTakeHandoffs();
}

/* Main, world held: give a client to its home owner. Returns 0 when it cannot go (a TLS connection whose
 * transport cannot leave main's event loop now): it stays where it is. */
static int ownerSendToOwner(client *c) {
    serverAssert(own_held && c->owner_tid == OWNER_ON_MAIN && c->owner_home >= 0 && c->owner_home < own_n);
    owner *o = &owners[c->owner_home];
    /* RL-g1d: a TLS client's OpenSSL runs on its owner from now on; its socket leaves main's event loop. */
    if (connIsTLS(c->conn)) {
        if (connIOOwnerBegin(c->conn) != 0) return 0;
        c->owner_tls = 1;
    }
    connSetReadHandler(c->conn, NULL);
    connSetWriteHandler(c->conn, NULL);
    c->owner_tid = o->id;
    pthread_mutex_lock(&o->inbox_mu);
    listAddNodeTail(o->inbox, c);
    pthread_mutex_unlock(&o->inbox_mu);
    /* RL-g1g: the owner cannot run before main resumes the world: one wakeup then for all of them
     * (ownerResumeWorld). */
    o->inbox_wake = 1;
    return 1;
}

/* Main: the socket of a TLS client an owner had is main's event loop's again. */
static void ownerTlsRelease(client *c) {
    if (!c->owner_tls) return;
    c->owner_tls = 0;
    connIOOwnerEnd(c->conn);
}

/* Main, after a TLS client came back: register its events and any input OpenSSL already holds. */
static void ownerTlsOnMain(client *c) {
    if (connIsTLS(c->conn) && connGetState(c->conn) == CONN_STATE_CONNECTED) connUpdateState(c->conn);
}

/* Returns 1 when an owner took the client, 0 when it stays on main as a guest (it goes home later). */
/* ------------------------------------------------------------------------------------------------
 * RL-g7d-replica-reads: pacing the replica's apply
 *
 * With native reads on a replica (ownerComputeVeto: OWN_VETO_WRITE), every main iteration that reads the primary's
 * stream stops every owner to apply it. Main wakes as soon as the link has bytes, so at a steady stream it would
 * stop the owners once per few kB. After an iteration that applied stream bytes main takes the link out of its
 * event loop for owner-replica-pace-ms and then reads it again from a timer (the world held: a replica's iterations
 * always stop the owners, OWN_NOLIGHT_REPL), so the bytes that arrived meanwhile are applied in one stop
 * (readQueryFromClient reads up to REPL_MAX_READS_PER_IO_EVENT buffers of a primary per event). The replica applies
 * at most owner-replica-pace-ms later than it would have; offsets, acks and the order of the stream are unchanged.
 * Not while the native path is vetoed, owners sleep or main is inside a long command.
 * ---------------------------------------------------------------------------------------------- */
static long long own_rpace_timer = -1;
static uint64_t own_rpace_cid = 0;
static long long own_rpace_paced = 0, own_rpace_fires = 0, own_rpace_bytes = 0;

static long long ownerReplicaPaceProc(struct aeEventLoop *el, long long id, void *data) {
    UNUSED(el);
    UNUSED(id);
    UNUSED(data);
    own_rpace_timer = -1;
    own_rpace_fires++;
    client *c = server.primary;
    if (!c || c->id != own_rpace_cid || !c->conn || c->flag.close_asap) return AE_NOMORE;
    if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
        freeClientAsync(c);
        return AE_NOMORE;
    }
    readQueryFromClient(c->conn);
    return AE_NOMORE;
}

/* Main, readQueryFromClient of the primary client after it read (nread > 0) and ran its input. */
void ownerReplicaReadDone(client *c, long long nread) {
    if (!own_n || nread <= 0 || !server.owner_replica_pace_ms || own_dormant || ProcessingEventsWhileBlocked) return;
    if (c != server.primary || !c->conn || c->flag.close_asap || connGetState(c->conn) != CONN_STATE_CONNECTED) return;
    if ((ownerComputeVeto() & OWN_VETO_ALL) || own_rpace_timer != -1) return;
    own_rpace_bytes += nread;
    long long t = aeCreateTimeEvent(server.el, server.owner_replica_pace_ms, ownerReplicaPaceProc, NULL, NULL);
    if (t == AE_ERR) return;
    connSetReadHandler(c->conn, NULL);
    own_rpace_timer = t;
    own_rpace_cid = c->id;
    own_rpace_paced++;
}

int ownerAdoptAccepted(client *c) {
    /* Main is inside a long command (busy script, loading): owners stay stopped until it ends, so
     * the new client stays on main, where it can get BUSY or send SCRIPT KILL. Same while the
     * owners sleep under io-threads > 1. */
    if (ProcessingEventsWhileBlocked || own_dormant || (ownerComputeVeto() & OWN_VETO_ALL) || !ownerSendToOwner(c)) {
        ownerGuestAdd(c);
        return 0;
    }
    return 1;
}

/* acceptCommonHandler (main, world held), a client of an owner TLS port: its owner runs the TLS handshake
 * on its own thread and hands it back when it ends (OWN_WHY_ACCEPTED), and main runs clientAcceptHandler,
 * which adopts it again. Returns 0 when main accepts it the stock way instead (it is adopted after). */
int ownerAdoptHandshake(client *c) {
    if (ProcessingEventsWhileBlocked || own_dormant || (ownerComputeVeto() & OWN_VETO_ALL)) return 0;
    c->owner_hs = 1;
    if (ownerSendToOwner(c)) return 1;
    c->owner_hs = 0;
    return 0;
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
        ownerTlsRelease(c);
        return;
    }
    serverAssert(own_held && c->owner_tid >= 0 && c->owner_tid < own_n);
    owner *o = &owners[c->owner_tid];
    /* RL-g4c: freeClient() of a parked client (CLIENT KILL, ...): no longer parked. ownerTakeToMain makes every
     * parked client a stock blocked client before it takes one (ownerParkedToMain). */
    if (c->owner_park) ownerUnpark(o, c);
    if (c->owner_unpark_node) {
        listDelNode(o->unparked, c->owner_unpark_node);
        c->owner_unpark_node = NULL;
    }
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
    ownerTlsRelease(c);
}

/* What keeps a guest on main apart from input and output in flight: a pending close, a command running,
 * a transaction, watch, block, subscription, tracking, monitor or replication role, or a reply mode main
 * must honor. */
static int ownerGuestBound(client *c) {
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command || c->flag.protected) return 1;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 1;
    if (c->flag.pending_command || c->flag.executing_command || c->argc) return 1;
    if (c->flag.multi || (c->mstate && listLength(&c->mstate->watched_keys))) return 1;
    if (c->flag.blocked || c->flag.unblocked || c->flag.throttled || c->flag.pubsub || c->flag.tracking) return 1;
    if (c->pubsub_data && clientTotalPubSubSubscriptionCount(c)) return 1;
    if (c->flag.monitor || c->flag.replica || c->flag.primary || c->repl_data || c->slot_migration_job) return 1;
    if (c->flag.lua_debug || c->flag.fastpath || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next)
        return 1;
    if (c->deferred_reply_errors || (c->deferred_reply && listLength(c->deferred_reply))) return 1;
    if (getClientType(c) != CLIENT_TYPE_NORMAL || authRequired(c)) return 1;
    return 0;
}

/* Nothing about a guest's output needs main: its replies are written and no write handler waits. */
static int ownerGuestFlushed(client *c) {
    return !c->flag.pending_write && !clientHasPendingReplies(c) && !connHasWriteHandler(c->conn);
}

/* A guest may go home when nothing about it needs main: no input or output in flight, no
 * transaction, watch, block, subscription, tracking, monitor or replication role, no pending
 * close, and no reply mode main must honor. With replies set (RL-g2a, ownerGoHomeNow) unsent replies
 * may go along: its owner writes them. */
static int ownerGuestIdle(client *c, int replies) {
    if (replies ? connHasWriteHandler(c->conn) : !ownerGuestFlushed(c)) return 0;
    if (c->cmd_queue.off < c->cmd_queue.len) return 0;
    if (c->querybuf && sdslen(c->querybuf) > c->qb_pos) return 0;
    if (c->multibulklen || c->bulklen != -1 || c->reqtype) return 0; /* partial request */
    return !ownerGuestBound(c);
}

static void ownerReturnGuests(void) {
    listIter li;
    listNode *ln;
    listRewind(own_guests, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (c->owner_resume || !ownerGuestIdle(c, 0) || !ownerSendToOwner(c)) continue;
        listDelNode(own_guests, ln);
        c->owner_guest_node = NULL;
        own_returns++;
    }
}

/* Main, world held: client c of owner o becomes a guest on main. */
static void ownerTakeToMain(client *c) {
    if (c->owner_park) { /* RL-g4c: it goes to main as a stock blocked client, with every other parked one */
        ownerParkedToMain();
        return;
    }
    ownerReclaim(c);
    own_takes++;
    watchMoveToMain(c);
    if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) {
        freeClientAsync(c);
        return;
    }
    if (c->owner_hs) {
        /* Its TLS handshake goes on on main, as the stock accept (clientAcceptHandler adopts it after). */
        c->owner_hs = 0;
        if (connAccept(c->conn, clientAcceptHandler) == C_ERR) freeClientAsync(c);
        return;
    }
    ownerTlsOnMain(c);
    ownerGuestAdd(c);
    if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
    /* Sent home between two commands of a pipeline and not run there yet: main runs the rest. */
    if (c->owner_resume == OWN_RES_HOME) ownerRerunAdd(c);
}

/* Main inside a long command (processEventsWhileBlocked: busy script, loading, module yield): the
 * owners stay stopped until it ends, so main takes every owner client with input waiting and serves
 * it itself (BUSY replies, SCRIPT KILL, SHUTDOWN). Level-triggered epoll reports the input to main
 * even if the owner thread has the same event in hand; the owner then finds the client gone. */
static void ownerTakeReadable(void) {
    ownerParkedToMain(); /* RL-g4c: main serves them now */
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
        /* TLS clients whose input OpenSSL holds (no event reports it). */
        for (int k = 0; k < o->ntlsp; k++) {
            uint64_t d = o->tlsp[k];
            int fd = (int)(uint32_t)d;
            client *c = fd < o->fdmap_cap ? o->fdmap[fd] : NULL;
            if (!c || (uint32_t)c->id != (uint32_t)(d >> 32) || !c->owner_onloop) continue;
            ownerTakeToMain(c);
        }
        o->ntlsp = 0;
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
    own_stop_closes = 0; /* RL-g3i: this stop's close budget */
    if (ProcessingEventsWhileBlocked) {
        if (own_held) {
            ownerTakeHandoffs(); /* queued behind the command that is running now */
            ownerTakeReadable();
            ownerRerunOnMain();
        }
        return;
    }
    /* RL-g3b: a wake no file event caused may leave the owners running (the iteration stops them if it must). */
    if (!own_held && own_iter_events == 0) {
        int why = ownerLightWhyNot();
        if (why < 0) {
            own_light = 1;
            own_light_iters++;
            return;
        }
        own_nolight[why]++;
    }
    ownerStopWorld();
    ownerFoldStats();
    ownerTakeHandoffs();
    ownerRerunOnMain();
}

void ownerBeforeSleepEnd(void) {
    if (!own_n || ProcessingEventsWhileBlocked) return;
    /* While the native path is vetoed, guests stay on main: owners would only hand them back. */
    int home = !own_dormant && !(ownerComputeVeto() & OWN_VETO_ALL);
    if (listLength(own_yielded)) ownerResumeYielded(home);
    if (own_dormant) return;
    if (home) ownerReturnGuests();
    ownerResumeWorld();
}

/* RL-g2c-owner-cron (main). The owners run the per-client cron of the clients they serve unless owner-cron is off,
 * they sleep (io-threads > 1: main has every client), or maxmemory-clients is set: client eviction needs main to keep
 * every client's memory bucket current (clientsCron's updateClientMemUsageAndBucket), so main's cron then covers the
 * owner clients again, with the world stopped, as before. */
int ownerCronOnOwners(void) {
    return own_n && server.owner_cron && !own_dormant && !server.maxmemory_clients;
}

int ownerResidentClients(void) {
    int n = 0;
    for (int i = 0; i < own_n; i++) n += owners[i].nclients;
    return n;
}

/* Main, world held (INFO clients): the owners' recent peaks, with main's (getExpensiveClientsInfo). */
void ownerPeakMemInfo(size_t *in_usage, size_t *out_usage) {
    for (int i = 0; i < own_n; i++) {
        for (int j = 0; j < CLIENTS_PEAK_MEM_USAGE_SLOTS; j++) {
            if (owners[i].peak_in[j] > *in_usage) *in_usage = owners[i].peak_in[j];
            if (owners[i].peak_out[j] > *out_usage) *out_usage = owners[i].peak_out[j];
        }
    }
}

void ownerNoteWake(int numevents) {
    if (!own_n || ProcessingEventsWhileBlocked) return;
    own_iter_events = numevents;
}

void ownerNoteCron(void) {
    own_iter_cron = 1;
}

/* ------------------------------------------------------------------------------------------------
 * RL-g3b-light-cron: main iterations that leave the owners running
 *
 * Every main iteration used to stop every owner (ownerAfterSleep), timer wakes included. On an all-native load the
 * only main wakes left are serverCron's (hz times a second), and most of what a tick does touches no owner state:
 * the memory stats (jemalloc's mallctl and zmalloc's counters are thread-safe), the instantaneous metrics (from the
 * counters each owner publishes at the end of its windows), the cached time (main already writes it while the owners
 * run), replicationCron with no replication, the MIGRATE socket cache. So an iteration that no file event woke and
 * that has nothing pending on main for its own clients (ownerLightWhyNot) starts without a stop; any step that needs
 * the owners still (serverCronNeedsWorld in server.c; any other timer proc; beforeSleep's work) stops them first
 * (ownerHoldWorld) and the iteration goes on exactly as before. Two of serverCron's steps that need owner state every
 * tick get it without a stop: databasesCron's rightsize and rehash run on each owner for its own tables
 * (ownerTablesCron, in its windows), and the slow expire cycle is skipped while no key or field expire anyone set
 * (ownerNoteExpire) has come due, when it could expire nothing.
 * ---------------------------------------------------------------------------------------------- */

long long serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData);
long long clientsTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);

/* Main, afterSleep of an iteration no file event woke (owners running): -1 when it may run without the world, else
 * why not. Main runs only its own clients' work in such an iteration, so nothing may be pending for them, and every
 * feature whose periodic work touches owner state rules it out. */
static int ownerLightWhyNot(void) {
    if (!server.owner_light_cron || own_dormant || server.pause_cron) return OWN_NOLIGHT_CONFIG;
    if (own_last_veto != 0) return OWN_NOLIGHT_VETO;
    if (server.aof_state != AOF_OFF) return OWN_NOLIGHT_AOF;
    if (server.maxmemory) return OWN_NOLIGHT_MAXMEMORY;
    if (hasActiveChildProcess() || server.aof_rewrite_scheduled || server.rdb_bgsave_scheduled) return OWN_NOLIGHT_CHILD;
    if (server.primary_host || server.repl_backlog || listLength(server.replicas) ||
        server.failover_state != NO_FAILOVER)
        return OWN_NOLIGHT_REPL;
    if (listLength(own_guests) || listLength(server.clients_to_close) || listLength(server.clients_pending_write) ||
        listLength(server.unblocked_clients) || listLength(server.clients_waiting_acks) || connTypeHasPendingData() ||
        server.shutdown_asap)
        return OWN_NOLIGHT_PENDING;
    /* RL-g2m (port onto RL-1): RL-g1g's yielded and rerun clients are main's work with the world held too. */
    if (listLength(own_yielded) || listLength(own_rerun)) return OWN_NOLIGHT_PENDING;
    /* QoS polling (priority_apidata) carries internal links only: cluster bus, replication, slot migration. Static
     * modules (the scripting engine) have no event listeners; their modulesCron runs in the light tick. */
    if (server.cluster_enabled || moduleCountLoaded()) return OWN_NOLIGHT_OTHER;
    pthread_mutex_lock(&own_main_mu);
    int handed = listLength(own_main_inbox) != 0;
    pthread_mutex_unlock(&own_main_mu);
    if (handed) return OWN_NOLIGHT_PENDING;
    return -1;
}

int ownerLightIteration(void) {
    return own_light;
}

void ownerHoldWorld(int why) {
    if (!own_light) return;
    own_light = 0;
    own_light_full++;
    if (why >= 0 && why < OWN_FULL_NCOUNT) own_light_full_why[why]++;
    ownerStopWorld();
    ownerFoldStats();
    /* Clients handed over since afterSleep stay queued (their owners' wakeup ends main's next sleep at once): main is
     * inside a timer here, not where it runs clients' commands. */
}

void ownerLightEnd(void) {
    own_light = 0;
    own_iter_cron = 0;
}

/* The ae hook before each time event: a timer other than serverCron's and clientsTimeProc's (defrag, eviction,
 * module and other timers) gets the stock world. */
static void ownerBeforeTimeProc(aeEventLoop *el, aeTimeProc *proc) {
    UNUSED(el);
    if (own_light && proc != serverCron && proc != clientsTimeProc) ownerHoldWorld(OWN_FULL_TIMEPROC);
}

int ownerTablesOnOwners(void) {
    return ownerCronOnOwners() && server.owner_light_cron;
}

long long ownerExpireBound(void) {
    long long b = atomic_load_explicit(&own_exp_min_other, memory_order_relaxed);
    for (int i = 0; i < own_n; i++) {
        long long v = atomic_load_explicit(&owners[i].pub_exp_min, memory_order_relaxed);
        if (v < b) b = v;
    }
    return b;
}

/* Any thread: an expire was set (object.c, every write of a key's expire field; db.c, a hash with volatile fields
 * is tracked: 0, so the slow cycle runs). Owners keep their own minimum (published at the end of each window);
 * main and any other thread lower a shared one. */
/* RL-g3f-owner-expire-light: while the owners expire keys (RL-g2b, ownerExpiresKeys), main's slow cycle runs the
 * hash-field job alone, so a light tick needs the world for it only once a hash with volatile fields exists. */
long long ownerFieldExpireBound(void) {
    return atomic_load_explicit(&own_fexp_min, memory_order_relaxed);
}

void ownerNoteFieldExpire(void) {
    if (own_n && atomic_load_explicit(&own_fexp_min, memory_order_relaxed) != 0)
        atomic_store_explicit(&own_fexp_min, 0, memory_order_relaxed);
}

void ownerNoteExpire(long long when) {
    if (!own_n) return;
    owner *o = own_self;
    if (o) {
        if (when < o->exp_min) o->exp_min = when;
        return;
    }
    long long cur = atomic_load_explicit(&own_exp_min_other, memory_order_relaxed);
    while (when < cur && !atomic_compare_exchange_weak_explicit(&own_exp_min_other, &cur, when, memory_order_relaxed,
                                                                memory_order_relaxed)) {
    }
}

void ownerPublishedStats(long long *cmds, long long *net_in, long long *net_out, long long *dirty) {
    *cmds = *net_in = *net_out = *dirty = 0;
    for (int i = 0; i < own_n; i++) {
        *cmds += atomic_load_explicit(&owners[i].pub_cmds, memory_order_relaxed);
        *net_in += atomic_load_explicit(&owners[i].pub_net_in, memory_order_relaxed);
        *net_out += atomic_load_explicit(&owners[i].pub_net_out, memory_order_relaxed);
        *dirty += atomic_load_explicit(&owners[i].pub_dirty, memory_order_relaxed);
    }
}

static void ownerAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    int home = (int)((intptr_t)privdata & (OWN_LISTEN_TLS - 1)), tls = ((intptr_t)privdata & OWN_LISTEN_TLS) != 0;
    int cport, cfd, max = tls ? server.max_new_tls_conns_per_cycle : server.max_new_conns_per_cycle;
    char cip[NET_IP_STR_LEN];
    struct ClientFlags flags = {0};
    while (max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (anetRetryAcceptOnError(errno)) continue;
            if (errno != EWOULDBLOCK) serverLog(LL_WARNING, "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE, "Accepted %s:%d (owner %d%s)", cip, cport, home, tls ? ", TLS" : "");
        if (server.tcpkeepalive) anetKeepAlive(NULL, cfd, server.tcpkeepalive);
        owner_accept_home = home;
        acceptCommonHandler(tls ? connCreateAccepted(connectionTypeTls(), cfd, &server.tls_auth_clients)
                                : connCreateAccepted(connectionTypeTcp(), cfd, NULL),
                            flags, cip);
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
static int ownerResumeInput(owner *o, client *c);

/* Owner, in a window: leave a client to main. It is off this owner's loop when this returns. */
static int ownerHandOff(owner *o, client *c, int why) {
    /* RL-g4c: only a closing client leaves its owner parked (its socket closed, or its query buffer over the
     * limit); it is no longer parked, as freeClient() unblocks a stock blocked client. */
    if (c->owner_park) {
        serverAssert(why == OWN_WHY_CLOSE || why == OWN_WHY_QBLIMIT);
        ownerUnpark(o, c);
    }
    if (c->owner_unpark_node) {
        listDelNode(o->unparked, c->owner_unpark_node);
        c->owner_unpark_node = NULL;
    }
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

/* RL-g1d: a TLS client to read again in the next window: OpenSSL may hold input no event reports. */
static void ownerTlsPend(owner *o, client *c) {
    if (o->ntlsp == o->tlsp_cap) {
        o->tlsp_cap = o->tlsp_cap ? o->tlsp_cap * 2 : 64;
        o->tlsp = zrealloc(o->tlsp, sizeof(uint64_t) * o->tlsp_cap);
    }
    o->tlsp[o->ntlsp++] = ownerTag(c);
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
    /* TLS: the handshake's ClientHello raises EPOLLIN; input OpenSSL took off the socket on main does not. */
    if (c->owner_tls && !c->owner_hs && (connIOOwnerState(c->conn) & CONN_IO_BUFFERED_INPUT)) ownerTlsPend(o, c);
    /* Sent home between two commands of a pipeline (owner-pipeline-resume): run the rest now; the replies
     * main made for it go out with the ones its commands make here. */
    if (c->owner_resume == OWN_RES_HOME) {
        c->owner_resume = OWN_RES_NONE;
        if (ownerResumeInput(o, c)) o->wake_main = 1;
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
        /* RL-g4a: with a stop pending, the clients not registered yet go back to the inbox, in order, and the
         * wakeup is armed again, so this owner's next window (after main resumes) takes them first. They are
         * on no loop until then, as before this window: main finds them in the inbox (ownerReclaim,
         * ownerTakeReadable, ownerIOThreadsChanged). Main adds to the inbox only with the world held, so the
         * new inbox is empty here and keeps their order. */
        if (listLength(l) && ownerStopPending(o)) {
            long long left = (long long)listLength(l);
            pthread_mutex_lock(&o->inbox_mu);
            serverAssert(listLength(o->inbox) == 0);
            listRelease(o->inbox);
            o->inbox = l;
            pthread_mutex_unlock(&o->inbox_mu);
            uint64_t one = 1;
            if (write(o->wakefd, &one, sizeof(one)) == -1) { /* counter full: a wakeup is pending anyway */
            }
            atomic_fetch_add_explicit(&own_inbox_left, left, memory_order_relaxed);
            atomic_fetch_add_explicit(&own_early_leaves, 1, memory_order_relaxed);
            return;
        }
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
        c->flag.pushing || isDeferredReplyEnabled(c) || c->conn == NULL ||
        (c->conn->type != connectionTypeTcp() && !c->owner_tls)) /* RL-g2d: owner TLS writes gather */
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
 * no references to values): what _writeToClient's connWrite branch sends. 0 otherwise. RL-g2k: tls set, the
 * same for an owner TLS client whose transport takes the batch (ownerWriteMany encrypts them first). */
static int ownerPlainSpanOf(client *c, char **buf, size_t *len, int tls) {
    if (c->owner_nrefs || listLength(c->reply) || c->flag.buf_encoded || c->bufpos <= 0) return 0;
    if (tls ? !c->owner_tls : c->conn->type != connectionTypeTcp()) return 0;
    serverAssert(c->io_last_written.data_len == 0 || c->io_last_written.buf == c->buf);
    if ((size_t)c->bufpos <= c->io_last_written.data_len) return 0;
    *buf = c->buf + c->io_last_written.data_len;
    *len = (size_t)c->bufpos - c->io_last_written.data_len;
    return 1;
}

static int ownerPlainSpan(client *c, char **buf, size_t *len) {
    return ownerPlainSpanOf(c, buf, len, 0);
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
#ifdef HAVE_LIBURING
            /* RL-g2k: an owner TLS client's reply is encrypted into its transport's out buffer and sent with
             * the batch; what the send left goes out with its next write. */
            if (use && c->owner_tls && ownerPlainSpanOf(c, &buf, &len, 1)) {
                const char *ct;
                size_t ctlen;
                int dup = 0; /* once per batch: its encrypted reply is in flight */
                for (int j = 0; j < m && !dup; j++) dup = o->uops[j].c == c;
                if (dup) continue;
                if (connIOOwnerWritePrepare(c->conn, buf, len, &ct, &ctlen) <= 0) {
                    left |= ownerWrite(o, c);
                    continue;
                }
                struct io_uring_sqe *sqe = io_uring_get_sqe(&o->ring);
                serverAssert(sqe != NULL);
                io_uring_prep_send(sqe, c->conn->fd, ct, ctlen, MSG_DONTWAIT);
                io_uring_sqe_set_data64(sqe, (uint64_t)m);
                o->uops[m] = (ownerUringOp){.c = c, .buf = (char *)ct, .len = ctlen, .tls = 1, .ptlen = len};
                m++;
                continue;
            }
#endif
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
            if (op->tls) { /* RL-g2k */
                int rep = connIOOwnerWriteDone(op->c->conn, op->res);
                o->tls_uring_writes++;
                if (rep > 0) {
                    ownerWriteDone(o, op->c, rep, op->ptlen);
                    left |= ownerWriteTail(o, op->c);
                } else {
                    o->uring_redo++;
                    left |= ownerWrite(o, op->c);
                }
                continue;
            }
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
    int wrap = n > 1 && c && !(c->cmd->flags & CMD_TOUCHES_ARBITRARY_KEYS), repl = 0;
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
/* RL-g7b-repl-primary: with a replica every owner window that wrote ends here, so own_aof_mu is taken
 * once per such window by every owner (86,000 times a second at a W=16 primary's 3.2M ops/s), each
 * hold a few microseconds (the records' copy into the replication buffer). A pthread mutex sleeps on a
 * futex as soon as it is held: 21% of those takes went to sleep (1,150 sleeps a second per owner),
 * each a futex wait, a futex wake by the holder and a scheduler round trip, longer than the hold
 * itself. ownerAofLock first spins up to OWN_AOF_LOCK_SPINS tries (a few holds' worth), then
 * sleeps as before. The lock, what it covers and the order in which owners append are the same. */
#define OWN_AOF_LOCK_SPINS 512
static void ownerAofLock(void) {
    if (pthread_mutex_trylock(&own_aof_mu) == 0) {
        atomic_fetch_add_explicit(&own_aof_lock_takes, 1, memory_order_relaxed);
        return;
    }
    for (int i = 0; i < OWN_AOF_LOCK_SPINS; i++) {
        ownerCpuRelax();
        if (pthread_mutex_trylock(&own_aof_mu) == 0) {
            atomic_fetch_add_explicit(&own_aof_lock_spun, 1, memory_order_relaxed);
            return;
        }
    }
    pthread_mutex_lock(&own_aof_mu);
    atomic_fetch_add_explicit(&own_aof_lock_sleeps, 1, memory_order_relaxed);
}

/* RL-g7b-repl-primary: replication only (own_combine), flat combining on own_aof_mu. At a W=32 loopback primary
 * half of the owners' takes of the lock found it held and the lock went round 680,000 times a second, each time
 * pulling the mutex, the replication buffer's tail and the backlog's and the replica's state to another core: the
 * lock was the primary's limit, and owners waiting on it sat idle (futex) or spun. Here an owner that finds it
 * held posts its records (drain_req = 1) and waits on its own flag; whichever owner holds the lock appends its own
 * records and then every posted owner's, in owner order, each as ownerAofAppend appends one owner's (SELECT, the
 * bytes, the WAIT offsets), and marks each done (2) before letting go. Every owner's records still go in whole and
 * in its execution order, and owners still go one at a time, so the stream is one the lock could have produced:
 * the same bytes, each owner's in order. An owner touches another's records and clients only while that owner
 * waits for its flag (release / acquire), as a lock handoff would hand them over. A poster that sees the lock
 * free takes it and appends what is still posted. Under maxmemory (each owner's lease counts its own allocations)
 * or with the AOF this stays off: ownerDrainAof's path. */
#define OWN_COMBINE_SPINS 200000
static void ownerDrainCombined(owner *o) {
    int got = pthread_mutex_trylock(&own_aof_mu) == 0;
    if (got) {
        atomic_fetch_add_explicit(&own_aof_lock_takes, 1, memory_order_relaxed);
    } else {
        atomic_store_explicit(&o->drain_req, 1, memory_order_release);
        for (long i = 0;; i++) {
            if (atomic_load_explicit(&o->drain_req, memory_order_acquire) == 2) break;
            if ((i & 31) == 31 && pthread_mutex_trylock(&own_aof_mu) == 0) {
                got = 1;
                atomic_fetch_add_explicit(&own_aof_lock_spun, 1, memory_order_relaxed);
                break;
            }
            if (i >= OWN_COMBINE_SPINS) {
                pthread_mutex_lock(&own_aof_mu);
                got = 1;
                atomic_fetch_add_explicit(&own_aof_lock_sleeps, 1, memory_order_relaxed);
                break;
            }
            ownerCpuRelax();
        }
    }
    if (got) {
        replStreamSenderDeferWakes(1);
        if (atomic_load_explicit(&o->drain_req, memory_order_acquire) != 2) ownerAofAppend(o);
        long long served = 0;
        for (int i = 0; i < own_n; i++) {
            owner *x = &owners[i];
            if (x == o || atomic_load_explicit(&x->drain_req, memory_order_acquire) != 1) continue;
            ownerAofAppend(x);
            atomic_store_explicit(&x->drain_req, 2, memory_order_release);
            served++;
        }
        pthread_mutex_unlock(&own_aof_mu);
        replStreamSenderDeferWakes(0);
        replStreamSenderWakeDeferred();
        if (served) {
            atomic_fetch_add_explicit(&own_drain_combined, served, memory_order_relaxed);
            atomic_fetch_add_explicit(&own_drain_combines, 1, memory_order_relaxed);
        }
    }
    atomic_store_explicit(&o->drain_req, 0, memory_order_relaxed);
    o->unflushed = 0;
    if (o->nheld) ownerWriteHeld(o);
}

static void ownerDrainAof(owner *o) {
    if (own_combine) {
        ownerDrainCombined(o);
        return;
    }
    ownerAofLock();
    replStreamSenderDeferWakes(1);
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
    replStreamSenderDeferWakes(0);
    replStreamSenderWakeDeferred();
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

/* RL-g1b-native-list: what a list read or pop's reply can hold, bounded by the whole list at `key` in
 * reply form (an element's reply is at most 5x its stored bytes, for small integers, plus a bulk header).
 * Only maxmemory asks (ownerMmCmdFits). */
static size_t ownerStoredListReplyLen(client *c, int slot, robj *key) {
    void *found;
    if (slot < 0 || !kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(slot), objectGetVal(key), &found))
        return 0;
    robj *v = found;
    if (objectGetType(v) != OBJ_LIST) return 0;
    size_t bytes = 0;
    if (objectGetEncoding(v) == OBJ_ENCODING_LISTPACK)
        bytes = lpBytes(objectGetVal(v));
    else
        for (quicklistNode *n = ((quicklist *)objectGetVal(v))->head; n; n = n->next) bytes += n->sz;
    return 5 * bytes + 32 * listTypeLength(v) + 64;
}

/* RL-g1a-native-hash: what a native hash command on this owner's key can add beyond its arguments, or
 * SIZE_MAX when that has no cheap bound (main runs it). A listpack hash: a write can convert it to a
 * hashtable (each entry allocated, plus buckets: within 4x the listpack's bytes and 128 B an entry); a
 * read replies at most every field and value (the listpack's bytes plus 16 B of RESP framing each). A
 * hashtable hash: writes add their arguments' entries (resizes ask ownerResizeAllowed); the reads that
 * reply values (HGET, HMGET, HGETALL) are not bounded here. No key or another type: nothing beyond the
 * command's own bound (a new empty hash, or an error reply). */
static size_t ownerHashBound(client *c, int slot, robj *key, int write) {
    void *found;
    if (slot < 0 || !kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(slot), objectGetVal(key), &found))
        return 0;
    robj *v = found;
    if (objectGetType(v) != OBJ_HASH) return 0;
    size_t n = hashTypeLength(v);
    if (objectGetEncoding(v) == OBJ_ENCODING_LISTPACK) {
        size_t bytes = lpBytes(objectGetVal(v));
        return write ? 4 * bytes + 128 * n : 2 * bytes + 16 * n + 64;
    }
    return write ? 0 : SIZE_MAX;
}

/* RL-g1e-native-set-zset: what a native set or sorted-set command can add beyond what ownerMmCmdFits
 * counts for every command (its arguments, a reply block), or SIZE_MAX when there is no cheap bound and
 * main must run it. A write that outgrows a compact encoding (intset or listpack) rebuilds the whole
 * collection as a listpack, a hashtable or the sorted set's tree: 4x its bytes + 128 B an entry. A
 * sorted-set write into the tree can split a node per member: 1 KB per argument. SMEMBERS and ZRANGE
 * reply members: from a compact encoding at most 2x its bytes + 64 B an entry (framing, a score); from a
 * hashtable or a tree nothing cheap bounds the reply, so main runs them. */
static size_t ownerSetZsetBound(client *c, int slot, robj *key, int argc, int mm) {
    void *found;
    robj *v = NULL;
    if (slot >= 0 && kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(slot), objectGetVal(key), &found)) v = found;
    size_t bytes = 0, n = 0;
    int compact = 0, coll = v && (objectGetType(v) == OBJ_SET || objectGetType(v) == OBJ_ZSET);
    int enc = coll ? (int)objectGetEncoding(v) : -1;
    if (enc == OBJ_ENCODING_INTSET) {
        bytes = intsetBlobLen(objectGetVal(v));
        n = intsetLen(objectGetVal(v));
        compact = 1;
    } else if (enc == OBJ_ENCODING_LISTPACK) {
        bytes = lpBytes(objectGetVal(v));
        n = lpLength(objectGetVal(v));
        compact = 1;
    }
    switch (mm) {
    case OWN_MM_SET_W: return compact ? 4 * bytes + 128 * n : 0;
    case OWN_MM_ZSET_W: return (compact ? 4 * bytes + 128 * n : 0) + 1024 * (size_t)argc;
    case OWN_MM_COLL_R: return coll && !compact ? SIZE_MAX : 2 * bytes + 64 * n;
    default: return 0;
    }
}

/* RL-g3g-long-tail-rand: what SPOP, SRANDMEMBER, HRANDFIELD or ZRANDMEMBER [count] can add on this owner's
 * compact collection (intset or listpack) at argv[1]: up to k = |count| (1 without a count; with a positive count at
 * most the whole collection) elements in the reply, each at most the collection's bytes, with a field and a value or
 * a member and a score each (2x), plus SPOP's and the distinct samples' rebuild or helper table (4x the bytes + 128 B
 * an entry). A hashtable or a tree: SIZE_MAX (no cheap bound: main runs it). No key, another type or a bad count:
 * nothing beyond the command's own bound (an empty or error reply). */
static size_t ownerRandBound(client *c, int slot, robj **argv, int argc) {
    void *found;
    if (slot < 0 || !kvstoreHashtableFind(c->db->keys, OWNER_TABLE_OF_SLOT(slot), objectGetVal(argv[1]), &found))
        return 0;
    robj *v = found;
    int t = objectGetType(v), enc = objectGetEncoding(v);
    size_t bytes, n;
    if (t == OBJ_SET && enc == OBJ_ENCODING_INTSET) {
        bytes = intsetBlobLen(objectGetVal(v));
        n = intsetLen(objectGetVal(v));
    } else if ((t == OBJ_SET || t == OBJ_HASH || t == OBJ_ZSET) && enc == OBJ_ENCODING_LISTPACK) {
        bytes = lpBytes(objectGetVal(v));
        n = lpLength(objectGetVal(v));
    } else if (t == OBJ_SET || t == OBJ_HASH || t == OBJ_ZSET) {
        return SIZE_MAX;
    } else {
        return 0;
    }
    unsigned long long k = 1;
    if (argc >= 3) {
        long long cnt;
        if (!string2ll(objectGetVal(argv[2]), sdslen(objectGetVal(argv[2])), &cnt) || cnt == LLONG_MIN) return 0;
        k = cnt < 0 ? (unsigned long long)-cnt : (unsigned long long)cnt;
        if (cnt >= 0 && k > n) k = n;
    }
    size_t each = 2 * (bytes + 64);
    if (k > (SIZE_MAX / 4) / each) return SIZE_MAX;
    return 4 * bytes + 128 * n + (size_t)k * each;
}

/* What native command `kind` can add under maxmemory beyond what ownerMmCmdFits counts for every
 * command, by its own_kinds[].mm bound; SIZE_MAX when nothing cheap bounds it (main runs it). */
static size_t ownerMmMore(client *c, struct serverCommand *cmd, robj **argv, int argc, int slot, int kind) {
    switch (own_kinds[kind].mm) {
    case OWN_MM_VALUES: return ownerReplyValuesLen(c, cmd, argv, argc, slot);
    case OWN_MM_LIST: return ownerStoredListReplyLen(c, slot, argv[1]);
    case OWN_MM_HASH_W: return ownerHashBound(c, slot, argv[1], 1);
    case OWN_MM_HASH_R: return ownerHashBound(c, slot, argv[1], 0);
    case OWN_MM_SET_W:
    case OWN_MM_ZSET_W:
    case OWN_MM_COLL_R: return ownerSetZsetBound(c, slot, argv[1], argc, own_kinds[kind].mm);
    case OWN_MM_KEYVAL: return ownerStoredLenOf(c, slot, argv[1]);
    case OWN_MM_RAND: return ownerRandBound(c, slot, argv, argc);
    case OWN_MM_UNBOUND: return SIZE_MAX;
    default: return 0;
    }
}

/* EXEC (D3-all): what its queued commands can add, each bounded as ownerMmCmdFits bounds one command;
 * 3x the queued arguments covers their encoding and the values a queued SET makes a later GET return. */
static size_t ownerMmExecBound(owner *o, client *c) {
    multiState *ms = c->mstate;
    size_t b = 3 * ms->argv_len_sums;
    for (int j = 0; j < ms->count; j++) {
        multiCmd *mc = ms->commands + j;
        size_t more = ownerMmMore(c, mc->cmd, mc->argv, mc->argc, mc->slot, ownerKindOf(mc->cmd));
        if (more == SIZE_MAX) return SIZE_MAX;
        b += OWN_MM_CMD + 64 * (size_t)mc->argc + more;
        if (own_keep) b += OWN_MM_REC * (32 * (size_t)mc->argc + 64) + 2 * sizeof(serverOp);
    }
    if (own_keep) b += OWN_MM_REC * ms->argv_len_sums;
    if (server.latency_tracking_enabled)
        for (int k = 0; k < OWN_NCMDS; k++)
            if ((own_kinds[k].flags & OWN_K_TXN) && !o->cs[k].hist) b += OWN_MM_HISTOGRAM; /* a queued command's first histogram */
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
    size_t more = own_kinds[kind].mm == OWN_MM_EXEC ? ownerMmExecBound(o, c)
                                                    : ownerMmMore(c, c->parsed_cmd, c->argv, c->argc, c->slot, kind);
    if (more == SIZE_MAX) return 0;
    return ownerMmFits(o, b + more);
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

/* The cluster slot of a native candidate's keys, or -1, by its kind's key layout (own_kinds[].keys):
 * one key, or several (DEL, MGET, MSET, WATCH) when all hash to one slot (hash-tagged, as cluster mode
 * requires). Keys in two slots go to main even when one owner owns both: the slot cached on the command
 * must hold for every key (getKVStoreIndexUsingCachedSlot). */
static int ownerKeySlot(struct serverCommand *cmd, robj **argv, int argc, int read_flags) {
    if (!cmd || (read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY))) return -1;
    int kind = ownerKindOf(cmd);
    if (kind < 0) return -1;
    int step; /* distance between keys; 0: one key */
    switch (own_kinds[kind].keys) {
    case OWN_KEYS_ONE: step = 0; break;
    case OWN_KEYS_ALL: step = 1; break;
    case OWN_KEYS_PAIRS: step = 2; break;
    case OWN_KEYS_SCRIPT: {
        /* RL-g1f-owner-evalsha: a script's declared keys, all in one slot (at least one key). */
        long long nk;
        if (argc < 4 || !string2ll(objectGetVal(argv[2]), sdslen(objectGetVal(argv[2])), &nk) || nk < 1 ||
            nk > argc - 3)
            return -1;
        sds key = objectGetVal(argv[3]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        for (int j = 4; j < 3 + nk; j++) {
            key = objectGetVal(argv[j]);
            if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
        }
        return slot;
    }
    case OWN_KEYS_TWO: {
        /* RL-g2o: source and destination (RPOPLPUSH, LMOVE, SMOVE), in one slot. */
        if (argc < 3) return -1;
        sds key = objectGetVal(argv[1]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        key = objectGetVal(argv[2]);
        return ownerKeyHashSlot(key, sdslen(key)) == slot ? slot : -1;
    }
    case OWN_KEYS_NUMKEYS: {
        /* RL-g3g: numkeys at argv[1] (1 .. argc - 2), the keys right after it, all in one slot. A bad numkeys:
         * main replies the error. */
        long long nk;
        if (argc < 3 || !string2ll(objectGetVal(argv[1]), sdslen(objectGetVal(argv[1])), &nk) || nk < 1 ||
            nk > argc - 2)
            return -1;
        sds key = objectGetVal(argv[2]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        for (int j = 3; j < 2 + nk; j++) {
            key = objectGetVal(argv[j]);
            if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
        }
        return slot;
    }
    case OWN_KEYS_ARG2: {
        /* RL-g3g: OBJECT <subcommand> key. */
        if (argc < 3) return -1;
        sds key = objectGetVal(argv[2]);
        return ownerKeyHashSlot(key, sdslen(key));
    }
    case OWN_KEYS_BUTLAST: {
        /* RL-g4c: BLPOP key [key ...] timeout. */
        if (argc < 3) return -1;
        sds key = objectGetVal(argv[1]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        for (int j = 2; j < argc - 1; j++) {
            key = objectGetVal(argv[j]);
            if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
        }
        return slot;
    }
    case OWN_KEYS_NUMKEYS2: {
        /* RL-g4c: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT ...; a bad numkeys: main replies the error. */
        long long nk;
        if (argc < 4 || !string2ll(objectGetVal(argv[2]), sdslen(objectGetVal(argv[2])), &nk) || nk < 1 ||
            nk > argc - 3)
            return -1;
        sds key = objectGetVal(argv[3]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        for (int j = 4; j < 3 + nk; j++) {
            key = objectGetVal(argv[j]);
            if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
        }
        return slot;
    }
    case OWN_KEYS_STREAMS: {
        /* RL-g6c: XREAD / XREADGROUP, options as xreadCommand parses them: BLOCK (main's: it may block), COUNT n,
         * GROUP g c, NOACK, then STREAMS k1 .. kn id1 .. idn. The keys all in one slot; anything xreadCommand would
         * refuse (an unknown option, an odd tail, no STREAMS) goes to main, which replies the error. */
        for (int i = 1; i < argc; i++) {
            int more = argc - i - 1;
            const char *opt = objectGetVal(argv[i]);
            if (!strcasecmp(opt, "COUNT") && more) {
                i++;
            } else if (!strcasecmp(opt, "GROUP") && more >= 2) {
                i += 2;
            } else if (!strcasecmp(opt, "NOACK")) {
                continue;
            } else if (!strcasecmp(opt, "STREAMS") && more) {
                int n = argc - i - 1;
                if (n % 2) return -1;
                n /= 2;
                sds key = objectGetVal(argv[i + 1]);
                int slot = ownerKeyHashSlot(key, sdslen(key));
                for (int j = i + 2; j <= i + n; j++) {
                    key = objectGetVal(argv[j]);
                    if (ownerKeyHashSlot(key, sdslen(key)) != slot) return -1;
                }
                return slot;
            } else {
                return -1; /* BLOCK, or an error */
            }
        }
        return -1;
    }
    default: return -1;
    }
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

/* ------------------------------------------------------------------------------------------------
 * RL-g1f-owner-evalsha: EVAL / EVALSHA on the owner of their keys
 *
 * An owner runs EVALSHA (and EVAL) itself when every declared key is in one slot it owns, the script
 * was loaded with SCRIPT LOAD and has no shebang (the stock compatibility mode), the caller speaks
 * RESP2 and is outside MULTI, and nothing vetoes writes (ownerNativeKind). It runs the script in its
 * own Lua state (engine_lua.c luaOwnerStateCreate: the eval state's API, libraries and locked
 * globals) inside its window. No other thread touches this owner's keys meanwhile, so the run is
 * atomic with respect to every other command on those keys, as a script on main is.
 *
 * Script cache. Main's cache stays the only authority (SCRIPT LOAD / FLUSH / KILL / EXISTS, FUNCTION
 * and FCALL stay on main). An owner that lacks a sha asks main for it (o->swant) and hands that call to
 * main; main answers at its next stop (ownerScriptAnswerAll: the body, or none for a script owners do
 * not run) and the owner compiles the body into its state once. SCRIPT FLUSH and the delete of a loaded
 * script bump own_script_epoch (eval.c, main, owners stopped): an owner whose copy is for an older
 * epoch drops it, Lua state included, before its next script, and ignores answers for an older epoch.
 *
 * Inside a script, server.call / pcall run on the owner (ownerScriptCommand) only GET, SET, DEL, INCR,
 * INCRBY, DECR, DECRBY, EXPIRE, PEXPIRE, TTL, PTTL, EXISTS, MGET and MSET, with every key in a slot of
 * this owner (any of its slots: a script may touch keys it did not declare, as in standalone mode), on
 * the owner's script client, as call() runs a script's command: commandstats, dirty, lazy-expire DELs
 * and effects propagation (each write's argv as the command rewrote it, into this execution unit, so
 * a script's writes reach the AOF and the replication stream as one MULTI/EXEC block, in this owner's
 * order with its other writes, as propagatePendingCommands sends them).
 *
 * Abort. Before a write touches a key the owner keeps the key as it was (a copy; strings only: a write
 * to a key of another type aborts first). Anything else (another command, another owner's key, an
 * error reply, a Lua error, server.log / setresp / set_repl / acl_check_cmd / math.random, a reply
 * value that is not RESP2's plain types, OWN_SCRIPT_BUDGET_US of run time) aborts the run: the owner
 * puts every kept key back (value, encoding, expire), drops the run's write ops, stats and error
 * counts, keeps only the DELs of keys its lookups found expired (they are gone either way) and hands
 * the client to main with the command unrun, which runs it exactly as the stock server does. The
 * reply is built only once the run completed, so nothing of an aborted run is visible. A script that
 * aborted OWN_SCRIPT_ABORTS times goes to main from then on (until the next epoch).
 *
 * Time and SCRIPT KILL. An owner script never runs longer than OWN_SCRIPT_BUDGET_US (a count hook
 * checks): it aborts and main runs it, where the busy-script replies, script timeouts and SCRIPT KILL
 * work as stock. So SCRIPT KILL never needs to reach an owner, and a script never holds an owner's
 * window (which main waits for to stop the world) longer than the budget.
 *
 * shortcut: off under maxmemory (no bound on a script's growth), RESP2 callers only, EVAL only for
 * loaded scripts (an EVAL-cached script's LRU position moves with every run, on main), no commandlog
 * entries for a script's inner commands. Upgrade: a lease bound per script, RESP3 conversions.
 * ---------------------------------------------------------------------------------------------- */
#define OWN_SCRIPT_BUDGET_US 2000
#define OWN_SCRIPT_ABORTS 16
#define OWN_SCRIPT_UNDO_MAX 64  /* keys one run may write */
#define OWN_SCRIPT_OPS_MAX 256  /* write ops one run may propagate */
enum { OWN_SCR_PENDING = 1, OWN_SCR_READY, OWN_SCR_MAIN };

typedef struct ownerScript {
    int state;
    int ref; /* the compiled function in the owner's Lua state (OWN_SCR_READY) */
    int aborts;
} ownerScript;

typedef struct ownerScriptAnswer {
    sds sha;
    sds body; /* NULL: owners do not run it (or main has no such script) */
} ownerScriptAnswer;

typedef struct ownerUndo {
    robj *key;
    robj *val; /* a copy of the value before the run's first write of the key; NULL: no key */
    long long expire;
} ownerUndo;

typedef struct ownerScriptRunState {
    owner *o;
    serverDb *db;
    monotime start;
    int abort;
    int nundo, nops;
    ownerUndo undo[OWN_SCRIPT_UNDO_MAX];
    int ops[OWN_SCRIPT_OPS_MAX]; /* indices in ectx->also_propagate of the run's write ops */
    long long calls[OWN_SCRIPT_NCMDS], usec[OWN_SCRIPT_NCMDS];
} ownerScriptRunState;

sds evalScriptBodyForOwner(const char *sha);
void dictEntryDestructorSdsKeyHeapValue(void *entry);
void *luaOwnerStateCreate(void);
int luaOwnerCompile(void *state, const char *code, size_t code_len);
void luaOwnerStateFree(void *state);
int luaOwnerCallFunction(void *lua, int function_ref, robj **keys, size_t nkeys, robj **args, size_t nargs, void *run,
                         char **out, size_t *out_len);

static dictType ownerScriptDictType = {
    .entryGetKey = dictEntryGetKey,
    .hashFunction = dictCStrCaseHash, /* as main's cache: looked up by a plain C string */
    .keyCompare = dictSdsKeyCaseCompare,
    .entryDestructor = dictEntryDestructorSdsKeyHeapValue,
};

void ownerScriptsChanged(void) {
    own_script_epoch++;
}

static void ownerScriptAnswersFree(owner *o) {
    for (int j = 0; j < o->nsans; j++) {
        sdsfree(o->sans[j].sha);
        sdsfree(o->sans[j].body);
    }
    o->nsans = 0;
}

/* Main, owners stopped (ownerStopWorld): answer what each owner asked for since the last stop. */
static void ownerScriptAnswerAll(void) {
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        if (!o->nswant) continue;
        if (o->sans_epoch != own_script_epoch) ownerScriptAnswersFree(o); /* not taken, and stale */
        o->sans_epoch = own_script_epoch;
        for (int j = 0; j < o->nswant; j++) {
            if (o->nsans == o->sans_cap) {
                o->sans_cap = o->sans_cap ? o->sans_cap * 2 : 8;
                o->sans = zrealloc(o->sans, sizeof(ownerScriptAnswer) * o->sans_cap);
            }
            o->sans[o->nsans++] = (ownerScriptAnswer){.sha = o->swant[j], .body = evalScriptBodyForOwner(o->swant[j])};
        }
        o->nswant = 0;
    }
}

/* Owner: bring its copy of the cache up to date (epoch, main's answers). */
static void ownerScriptSync(owner *o) {
    if (o->script_epoch != own_script_epoch) {
        if (o->scripts) dictEmpty(o->scripts, NULL);
        if (o->lua) luaOwnerStateFree(o->lua);
        o->lua = NULL;
        o->script_epoch = own_script_epoch;
    }
    if (!o->nsans) return;
    for (int j = 0; j < o->nsans && o->sans_epoch == own_script_epoch; j++) {
        ownerScriptAnswer *a = o->sans + j;
        dictEntry *de = o->scripts ? dictFind(o->scripts, a->sha) : NULL;
        ownerScript *s = de ? dictGetVal(de) : NULL;
        if (!s || s->state != OWN_SCR_PENDING) continue;
        s->state = OWN_SCR_MAIN;
        if (!a->body) continue;
        if (!o->lua) o->lua = luaOwnerStateCreate();
        if (o->lua && (s->ref = luaOwnerCompile(o->lua, a->body, sdslen(a->body))) >= 0) s->state = OWN_SCR_READY;
    }
    ownerScriptAnswersFree(o);
}

/* ownerNativeKind: the script of this EVALSHA / EVAL is ready on this owner (o->cur_script). If the
 * owner has never seen the sha it asks main for it; this call goes to main either way. */
static int ownerScriptReady(owner *o, client *c) {
    char sha[41];
    sds a = objectGetVal(c->argv[1]);
    if (c->parsed_cmd->proc == evalShaCommand) {
        if (sdslen(a) != 40) return 0;
        for (int j = 0; j < 40; j++) sha[j] = tolower((unsigned char)a[j]);
        sha[40] = '\0';
    } else {
        sha1hex(sha, a, sdslen(a));
    }
    ownerScriptSync(o);
    if (!o->scripts) o->scripts = dictCreate(&ownerScriptDictType);
    dictEntry *de = dictFind(o->scripts, sha);
    if (de) {
        ownerScript *s = dictGetVal(de);
        if (s->state == OWN_SCR_READY) {
            o->cur_script = s;
            return 1;
        }
        o->script_misses++;
        return 0;
    }
    ownerScript *s = zcalloc(sizeof(*s));
    s->state = OWN_SCR_PENDING;
    dictAdd(o->scripts, sdsnew(sha), s);
    if (o->nswant == o->swant_cap) {
        o->swant_cap = o->swant_cap ? o->swant_cap * 2 : 8;
        o->swant = zrealloc(o->swant, sizeof(sds) * o->swant_cap);
    }
    o->swant[o->nswant++] = sdsnew(sha);
    o->script_misses++;
    o->wake_main = 1;
    return 0;
}

/* Keep key as it is before the run's first write of it (after its lazy expiry, as the write's own
 * lookup would see it). 0: the run cannot keep it (not a string, too many keys): abort. */
static int ownerUndoKeep(ownerScriptRunState *st, robj *key) {
    sds k = objectGetVal(key);
    for (int i = 0; i < st->nundo; i++)
        if (!sdscmp(objectGetVal(st->undo[i].key), k)) return 1;
    if (st->nundo == OWN_SCRIPT_UNDO_MAX) return 0;
    robj *val = lookupKeyWrite(st->db, key);
    if (val && objectGetType(val) != OBJ_STRING) return 0;
    ownerUndo *u = &st->undo[st->nundo++];
    u->key = createStringObject(k, sdslen(k));
    u->val = val ? dupStringObject(val) : NULL;
    u->expire = val ? getExpire(st->db, key) : -1;
    return 1;
}

/* Put every kept key back (restore) or just drop the copies. */
static void ownerUndoEnd(ownerScriptRunState *st, int restore) {
    for (int i = st->nundo - 1; i >= 0; i--) {
        ownerUndo *u = &st->undo[i];
        if (restore) {
            dbSyncDelete(st->db, u->key);
            if (u->val) {
                robj *v = u->val;
                dbAdd(st->db, u->key, &v);
                if (u->expire != -1) setExpire(NULL, st->db, u->key, u->expire);
                u->val = NULL;
            }
        }
        if (u->val) decrRefCount(u->val);
        decrRefCount(u->key);
    }
    st->nundo = 0;
}

static void ownerScriptFreeArgv(client *sc, robj **argv, int argc) {
    /* As a module's borrowed vector (VM_CallArgv): a command that rewrote it made sc->argv its own copy
     * and kept ours as original_argv; ours is freed here. */
    if (sc->original_argv) {
        for (int j = 0; j < sc->argc; j++)
            if (sc->argv[j]) decrRefCount(sc->argv[j]);
        zfree(sc->argv);
    }
    for (int j = 0; j < argc; j++) decrRefCount(argv[j]);
    zfree(argv);
    sc->original_argv = NULL;
    sc->original_argc = 0;
    sc->argv = NULL;
    sc->argc = sc->argv_len = 0;
    sc->argv_len_sum = 0;
    sc->cmd = NULL;
}

static int ownerReplyIsError(client *sc) {
    if (sc->bufpos) return sc->buf[0] == '-';
    if (listLength(sc->reply)) {
        clientReplyBlock *b = listNodeValue(listFirst(sc->reply));
        return b->used && b->buf[0] == '-';
    }
    return 0;
}

static void ownerScriptDropReply(client *sc) {
    sc->bufpos = 0;
    while (listLength(sc->reply)) listDelNode(sc->reply, listFirst(sc->reply));
    sc->reply_bytes = 0;
}

void ownerScriptAbort(void *run) {
    ((ownerScriptRunState *)run)->abort = 1;
}

int ownerScriptAborted(void *run) {
    return ((ownerScriptRunState *)run)->abort;
}

int ownerScriptOverBudget(void *run) {
    ownerScriptRunState *st = run;
    return (long long)(getMonotonicUs() - st->start) > OWN_SCRIPT_BUDGET_US;
}

/* server.call / pcall of a script on an owner (script_lua.c): run argv (owned from here on) as call()
 * runs a script's command and give its reply to the Lua handlers. -1: abort the run. */
int ownerScriptCommand(void *run, robj **argv, int argc, const ValkeyModuleReplyHandlers *h, void *hctx) {
    ownerScriptRunState *st = run;
    owner *o = st->o;
    client *sc = o->sc;
    struct serverCommand *cmd = st->abort ? NULL : lookupCommand(argv, argc);
    /* The kinds a script may run on an owner (OWN_K_SCRIPT), their keys by the kind's key layout. */
    int kind = cmd ? ownerKindOf(cmd) : -1;
    if (kind < 0 || !(own_kinds[kind].flags & OWN_K_SCRIPT) || !commandCheckArity(cmd, argc, NULL) ||
        (kind == OWN_MSET && argc % 2 == 0))
        goto abort;
    int last = own_kinds[kind].keys == OWN_KEYS_ONE ? 1 : argc - 1;
    int step = own_kinds[kind].keys == OWN_KEYS_PAIRS ? 2 : 1;
    for (int j = 1; j <= last; j += step) {
        sds key = objectGetVal(argv[j]);
        int slot = ownerKeyHashSlot(key, sdslen(key));
        if (slot < o->slot_lo || slot > o->slot_hi) goto abort; /* another owner's key: main */
    }
    if (cmd->flags & CMD_WRITE) {
        if (st->nops == OWN_SCRIPT_OPS_MAX) goto abort;
        for (int j = 1; j <= last; j += step)
            if (!ownerUndoKeep(st, argv[j])) goto abort;
    }
    sc->argv = argv;
    sc->argc = sc->argv_len = argc;
    sc->cmd = sc->lastcmd = sc->realcmd = cmd;
    sc->db = st->db;
    client *prev = ectx->executing_client;
    ectx->executing_client = sc;
    ectx->in_call++;
    long long dirty = ectx->dirty, errors = ectx->stat_total_error_replies;
    sc->flag.force_aof = sc->flag.force_repl = sc->flag.prevent_prop = 0;
    sc->flag.buffered_reply = 0;
    monotime t0 = getMonotonicUs();
    sc->flag.executing_command = 1;
    cmd->proc(sc);
    sc->flag.executing_command = 0;
    ustime_t duration = (ustime_t)(getMonotonicUs() - t0);
    ectx->in_call--;
    ectx->executing_client = prev;
    dirty = ectx->dirty - dirty;
    if (ectx->stat_total_error_replies != errors || ownerReplyIsError(sc)) {
        ownerScriptFreeArgv(sc, argv, argc);
        ownerScriptDropReply(sc);
        st->abort = 1;
        return -1;
    }
    /* call()'s propagation of a script's command (the script's run propagates PROPAGATE_AOF |
     * PROPAGATE_REPL): into this execution unit, after any lazy-expire DEL of its lookups. */
    int flags = dirty > 0 ? (PROPAGATE_AOF | PROPAGATE_REPL) : PROPAGATE_NONE;
    if (sc->flag.force_repl) flags |= PROPAGATE_REPL;
    if (sc->flag.force_aof) flags |= PROPAGATE_AOF;
    if (sc->flag.prevent_repl_prop || sc->flag.module_prevent_repl_prop) flags &= ~PROPAGATE_REPL;
    if (sc->flag.prevent_aof_prop || sc->flag.module_prevent_aof_prop) flags &= ~PROPAGATE_AOF;
    if (!sc->flag.prevent_prop && flags != PROPAGATE_NONE) {
        st->ops[st->nops++] = ectx->also_propagate.numops;
        alsoPropagate(sc->db->id, sc->argv, sc->argc, flags, -1);
    }
    sc->flag.force_aof = sc->flag.force_repl = sc->flag.prevent_prop = 0;
    st->calls[kind]++;
    st->usec[kind] += duration;
    if (server.latency_tracking_enabled) updateCommandLatencyHistogram(&o->cs[kind].hist, duration * 1000);
    if (ectx->current_client) ectx->current_client->commands_processed++;
    ectx->stat_numcommands++;
    invokeReplyHandlers(NULL, sc, h, hctx);
    sc->reply_bytes = 0;
    ownerScriptFreeArgv(sc, argv, argc);
    return 0;
abort:
    for (int j = 0; j < argc; j++) decrRefCount(argv[j]);
    zfree(argv);
    st->abort = 1;
    return -1;
}

/* EVALSHA / EVAL on this owner (ownerCall). 1: ran, the reply is on c. 0: aborted, nothing of the run
 * is left but its lazy-expire DELs; main runs the command. */
static int ownerScriptRun(owner *o, client *c) {
    ownerScript *s = o->cur_script;
    o->cur_script = NULL;
    long long nk = 0;
    string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &nk);
    ownerScriptRunState *st = zmalloc(sizeof(*st));
    st->o = o;
    st->db = c->db;
    st->start = getMonotonicUs();
    st->abort = st->nundo = st->nops = 0;
    memset(st->calls, 0, sizeof(st->calls));
    memset(st->usec, 0, sizeof(st->usec));
    long long dirty = ectx->dirty, hits = ectx->stat_keyspace_hits, misses = ectx->stat_keyspace_misses;
    long long errors = ectx->stat_total_error_replies, ncmds = ectx->stat_numcommands;
    long long processed = c->commands_processed;
    int nerrors = o->nerrors;
    client *sc = o->sc;
    sc->db = c->db;
    sc->user = c->user;
    sc->resp = 2;
    /* The keyspace lookups hash each key: a script's keys may be in several of this owner's slots. */
    int slot = c->slot;
    c->slot = -1;
    char *out = NULL;
    size_t out_len = 0;
    int r = luaOwnerCallFunction(o->lua, s->ref, c->argv + 3, (size_t)nk, c->argv + 3 + nk, (size_t)(c->argc - 3 - nk),
                                 st, &out, &out_len);
    c->slot = slot;
    if (r == 0 && !st->abort) {
        addReplyProto(c, out, out_len);
        zfree(out);
        for (int k = 0; k < OWN_SCRIPT_NCMDS; k++) {
            o->cs[k].calls += st->calls[k];
            o->cs[k].usec += st->usec[k];
        }
        ownerUndoEnd(st, 0);
        zfree(st);
        c->flag.prevent_prop = 1; /* as scriptPrepareForRun: the script's writes propagate, not the EVALSHA */
        o->script_runs++;
        return 1;
    }
    ownerUndoEnd(st, 1);
    serverOpArray *oa = &ectx->also_propagate;
    int w = 0, k = 0;
    for (int j = 0; j < oa->numops; j++) {
        serverOp *op = oa->ops + j;
        if (k < st->nops && st->ops[k] == j) {
            for (int a = 0; a < op->argc; a++) decrRefCount(op->argv[a]);
            zfree(op->argv);
            k++;
            continue;
        }
        oa->ops[w++] = *op;
    }
    oa->numops = w;
    ectx->dirty = dirty;
    ectx->stat_keyspace_hits = hits;
    ectx->stat_keyspace_misses = misses;
    ectx->stat_total_error_replies = errors;
    ectx->stat_numcommands = ncmds;
    c->commands_processed = processed;
    while (o->nerrors > nerrors) sdsfree(o->errors[--o->nerrors].msg);
    zfree(st);
    if (++s->aborts >= OWN_SCRIPT_ABORTS) s->state = OWN_SCR_MAIN;
    o->script_aborts++;
    return 0;
}

/* EXEC can run here: every queued command is a native data command on this owner's keys. That is
 * the only way an owner queues (ownerNativeKind), so this only guards the invariant. */
static int ownerTxnNative(owner *o, client *c) {
    for (int j = 0; j < c->mstate->count; j++) {
        multiCmd *mc = c->mstate->commands + j;
        int kind = ownerKindOf(mc->cmd);
        if (kind < 0 || !(own_kinds[kind].flags & OWN_K_TXN)) return 0;
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
    int kind = ownerKindOf(cmd);
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
    case OWN_CLUSTER_SLOTS:
        /* RL-g3i: ownerClusterCommand's reply reads the owners' fixed ports and slot ranges, the server's run id
         * and owner-announce-ip (changed only on main, with the world held); inside MULTI main queues it. */
        if (c->flag.multi || server.cluster_enabled || !server.owner_port) return -1;
        break;
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
    case OWN_EVALSHA:
    case OWN_EVAL:
        /* RL-g1f-owner-evalsha: RESP2 callers outside MULTI, no maxmemory (no bound on a script's
         * growth), no WATCHed keys on this owner (an aborted run's writes would have touched them), and
         * a script this owner has from main's cache (ownerScriptReady). */
        if (c->flag.multi || own_mm || c->resp != 2 || c->flag.lua_debug ||
            (veto & (OWN_VETO_ALL | OWN_VETO_WRITE)) || ((veto & OWN_VETO_BLOCKED) && ownerSlotBlocked(c->slot)) ||
            c->slot < o->slot_lo || c->slot > o->slot_hi || o->watchers || !ownerScriptReady(o, c))
            return -1;
        keyed = 1;
        break;
    default:
        /* RL-g1f: INCRBY, DECR and DECRBY only inside an owner's script. */
        if (own_kinds[kind].flags & OWN_K_INNER) return -1;
        /* RL-g4c: a TLS client's blocking pop goes to main (shortcut: a parked TLS client's reads are not
         * merged with ownerReadTls). */
        if ((own_kinds[kind].flags & OWN_K_BLOCK) && connIsTLS(c->conn)) return -1;
        /* Inside MULTI an owner queues only what ownerExec runs (OWN_K_TXN); main queues the rest. */
        if (c->flag.multi && !(own_kinds[kind].flags & OWN_K_TXN)) return -1;
        keyed = 1;
        break;
    }
    if (keyed) {
        if (!(own_kinds[kind].flags & OWN_K_READ) && (veto & OWN_VETO_WRITE)) return -1;
        /* RL-g5c-block-scope: while clients are blocked on main, a write to a slot one of them waits on (or a
         * blocking pop that would block there) goes to main so main serves them in order; reads stay native. */
        if ((veto & OWN_VETO_BLOCKED) && !(own_kinds[kind].flags & OWN_K_READ) && ownerSlotBlocked(c->slot))
            return -1;
        /* The slot ownerTryNative computed: the keys are hashed once per command. */
        if (c->slot < o->slot_lo || c->slot > o->slot_hi) return -1; /* another owner's key: main */
        /* RL-g1a-native-hash: a hash with fields that expire on their own anywhere in this key's table
         * (only main makes those: HSETEX, HEXPIRE, ...) sends the table's hash commands to main. */
        if (own_kinds[kind].family == OWN_FAM_HASH &&
            kvstoreHashtableSize(c->db->keys_with_volatile_items, OWNER_TABLE_OF_SLOT(c->slot)))
            return -1;
    }
    /* Under maxmemory the stock server may evict or refuse before any command (queued ones and EXEC
     * included): main decides that once the command does not fit this owner's headroom. */
    if (own_mm && !ownerMmCmdFits(o, c, kind)) {
        o->mm_refused_total++;
        return -1;
    }
    return kind;
}

/* One parsed command a guest's owner would run (the tests of ownerNativeKind main can make; the owner makes
 * them all again and hands back what it cannot run): a native data command on the home owner's keys (a read
 * under a write veto), PING, MULTI, EXEC or DISCARD. RL-1: by the kind table; inside a MULTI of the run only
 * the queueable kinds (OWN_K_TXN); not WATCH / UNWATCH, scripts (whether the owner has the script is the
 * owner's to know) or the script-only kinds. */
static int ownerGuestNativeNext(owner *o, parsedCommand *p, int veto, int *in_multi) {
    if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED) || p->argc == 0 || !p->cmd ||
        (p->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY)))
        return 0;
    int kind = ownerKindOf(p->cmd);
    if (kind < 0 || kind == OWN_WATCH || kind == OWN_UNWATCH || own_kinds[kind].family == OWN_FAM_SCRIPT ||
        (own_kinds[kind].flags & OWN_K_INNER))
        return 0;
    if (kind == OWN_MULTI) {
        if (*in_multi) return 0;
        *in_multi = 1;
        return 1;
    }
    if (kind == OWN_EXEC || kind == OWN_DISCARD) {
        int ok = *in_multi;
        *in_multi = 0;
        return ok;
    }
    if (*in_multi && !(own_kinds[kind].flags & OWN_K_TXN)) return 0;
    if (kind == OWN_PING) return 1;
    if (!(own_kinds[kind].flags & OWN_K_READ) && (veto & OWN_VETO_WRITE)) return 0;
    int slot = ownerKeySlot(p->cmd, p->argv, p->argc, p->read_flags);
    /* RL-g5c-block-scope: a write to a blocked slot goes to main, not home to the owner. */
    if ((veto & OWN_VETO_BLOCKED) && !(own_kinds[kind].flags & OWN_K_READ) && ownerSlotBlocked(slot)) return 0;
    return slot >= o->slot_lo && slot <= o->slot_hi;
}

/* Main, in processInputBufferLoop: 1 when a guest stops here and goes home, its owner running the rest of
 * its input (owner-pipeline-resume N). Between two commands (at_end 0): when the next N commands, parsed
 * whole in its command queue, are ones its owner runs, or the queue ends after one or more of them (a
 * shorter run between two other commands costs main less to run itself than a trip home: main runs only
 * while the owners are stopped). At the end of its input (at_end 1): always. Main already ran the commands
 * before, so the owner runs the next one after them, and the replies main made go home with the client
 * (ownerResumeYielded). Never when something about the client needs main (ownerGuestBound): MULTI .. EXEC
 * that main runs, WATCH through main, blocked, unblocked and throttled clients, pub/sub, tracking, MONITOR,
 * replication links, CLIENT REPLY OFF / SKIP, Lua debugging, deferred replies, a pending close or kill,
 * protected clients, a command being parsed; nor for unauthenticated or restricted ACL users, nor under
 * any veto (client pause, loading, busy scripts, io-threads > 1, ...). */
int ownerGuestYield(client *c, int at_end) {
    int need = server.owner_pipeline_resume;
    if (!need || own_no_yield || c->owner_resume) return 0;
    /* RL-1: not for TLS clients (RL-g1d's owner TLS ports): main runs the rest of a TLS guest's pipeline, as
     * the parent did. shortcut: the trip home of a TLS connection (IO ownership, OpenSSL's buffered input) is
     * not merged with this path yet. */
    if (connIsTLS(c->conn)) return 0;
    if (!own_held || own_dormant || ProcessingEventsWhileBlocked || c->owner_home < 0 || c->owner_home >= own_n ||
        c->owner_tid != OWNER_ON_MAIN)
        return 0;
    cmdQueue *q = &c->cmd_queue;
    if (at_end ? q->off < q->len : q->off >= q->len) return 0;
    int veto = own_last_veto; /* main's state as the owners last resumed: ownerBeforeSleepEnd checks it again */
    if (veto & OWN_VETO_ALL) return 0;
    if (ownerGuestBound(c)) return 0;
    if ((c->user && (c->user->flags & USER_FLAG_RETIRED)) || !ACLUserUnrestricted(c->user)) return 0;
    if (!at_end) {
        owner *o = &owners[c->owner_home];
        int run = 0, in_multi = 0;
        for (int i = q->off; i < q->len && run < need; i++, run++)
            if (!ownerGuestNativeNext(o, &q->cmds[i], veto, &in_multi)) break;
        if (run < need && (run == 0 || q->off + run < q->len)) return 0;
    }
    c->owner_resume = OWN_RES_YIELDED;
    listAddNodeTail(own_yielded, c);
    /* Its replies go home with it and its owner writes them with the next ones (ownerResumeYielded): main's
     * beforeSleep does not write them. */
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }
    return 1;
}

/* Main, at the end of beforeSleep (its AOF write done): the guests ownerGuestYield stopped go home with the
 * rest of their input, which their owner runs first thing (ownerRegister, ownerResumeInput), and with the
 * replies main made for them, which the owner writes ahead of its own (main's AOF has their writes now:
 * aofHoldsReplies is 0). The parsed commands go as an owner hands them to main (ownerHandOff): without
 * main's prefetch marks and slots. One that cannot go now (home 0: a veto or io-threads > 1; a socket main
 * is writing; replies the AOF still holds; a state ownerGuestBound keeps on main) has the rest run by main. */
static void ownerResumeYielded(int home) {
    listNode *ln;
    while ((ln = listFirst(own_yielded))) {
        client *c = listNodeValue(ln);
        listDelNode(own_yielded, ln);
        c->owner_resume = OWN_RES_NONE;
        if (!home || !c->owner_guest_node || c->owner_tid != OWNER_ON_MAIN || connHasWriteHandler(c->conn) ||
            ownerGuestBound(c) || (clientHasPendingReplies(c) && aofHoldsReplies())) {
            ownerRerunAdd(c);
            continue;
        }
        /* The inbox is the owner's only after main resumes the world, so the bookkeeping below is still
         * main's. A client that cannot go (RL-g1d: a TLS transport that cannot leave main now) stays. */
        c->owner_resume = OWN_RES_HOME;
        if (!ownerSendToOwner(c)) {
            c->owner_resume = OWN_RES_NONE;
            ownerRerunAdd(c);
            continue;
        }
        if (c->flag.pending_write) {
            listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
            c->flag.pending_write = 0;
        }
        listDelNode(own_guests, c->owner_guest_node);
        c->owner_guest_node = NULL;
        own_returns++;
        own_resumes++;
        if (c->querybuf && c->querybuf == thread_shared_qb) trimClientQueryBuffer(c);
        for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
            c->cmd_queue.cmds[i].slot = -1;
            c->cmd_queue.cmds[i].read_flags &= ~READ_FLAGS_PREFETCHED;
        }
    }
}

/* Main: a guest whose rest of input main runs at its next wakeup (ownerRerunOnMain), as the parent did
 * (all of it, no stop between commands), with a read handler and its replies in main's write queue. */
static void ownerRerunAdd(client *c) {
    c->owner_resume = OWN_RES_RERUN;
    listAddNodeTail(own_rerun, c);
    own_reruns++;
    /* RL-g2a: a lazy client has the handler on its connection but not in main's epoll yet (ownerArmRead). */
    if (!connHasReadHandler(c->conn)) {
        if (connSetReadHandler(c->conn, readQueryFromClient) == C_ERR) freeClientAsync(c);
    } else {
        ownerArmRead(c);
    }
    if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
    aeSetDontWait(server.el, 1);
}

/* Main, after taking the handed-off clients: run the rest of the input of the guests ownerRerunAdd kept. */
static void ownerRerunOnMain(void) {
    listNode *ln;
    while ((ln = listFirst(own_rerun))) {
        client *c = listNodeValue(ln);
        listDelNode(own_rerun, ln);
        c->owner_resume = OWN_RES_NONE;
        own_no_yield++;
        ownerResumeOnMain(c);
        own_no_yield--;
    }
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
static int ownerCall(owner *o, client *c, struct serverCommand *cmd, int kind, int top) {
    struct serverCommand *old_cmd = c->cmd, *old_lastcmd = c->lastcmd, *old_realcmd = c->realcmd;
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
    else if (kind == OWN_EVALSHA || kind == OWN_EVAL) {
        if (!ownerScriptRun(o, c)) {
            /* Aborted: main runs the command. Only its lookups' lazy-expire DELs are left to keep. */
            exitExecutionUnit();
            c->flag.executing_command = 0;
            c->flag.force_aof = c->flag.force_repl = c->flag.prevent_prop = 0;
            if (top && ectx->also_propagate.numops) ownerTakeProp(o, c);
            c->cmd = old_cmd;
            c->lastcmd = old_lastcmd;
            c->realcmd = old_realcmd;
            ectx->in_call--;
            ectx->executing_client = prev_client;
            return 0;
        }
    } else
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
    if (c->owner_park) {
        /* RL-g4c: parked, as call() leaves a blocked client: its stats come when it is served or times out. */
        ectx->in_call--;
        ectx->executing_client = prev_client;
        return 1;
    }
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
    o->fam_total[own_kinds[kind].family]++;
    if (own_kinds[kind].flags & OWN_K_TAIL) o->tail_total++;
    if (own_kinds[kind].flags & OWN_K_BLOCK) o->block_total++;
    ectx->in_call--;
    ectx->executing_client = prev_client;
    return 1;
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
        ownerCall(o, c, mc->cmd, ownerKindOf(mc->cmd), 0);
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

/* RL-g2u-tls-16k: the value's expire has passed at the command's time (commandTimeSnapshot: the read or
 * resume sets it for every command it runs, getCommand's too, since ownerCall does not move it). Only such
 * a key needs getCommand / mgetCommand (lookupKey may delete it and propagate the DEL, or keep it by mode).
 * For any other, expireIfNeeded returns KEY_VALID at once, so lookupKey does exactly what it does for a key
 * without an expire, and the GET and MGET paths here answer it the same way (big values by reference). */
static inline int ownerValExpired(const robj *val) {
    return ownerValHasExpire(val) && timestampIsExpired(objectGetExpire(val));
}

/* Reply to one GET from its completed find, with getCommand's effects (touch, hit or miss count,
 * the bulk or null reply). Returns 0, having done nothing, when getCommand must run it instead: a
 * key whose expire has passed (it may have to be deleted and the delete propagated), a value that is not a
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
    if (ownerValExpired(val) || objectGetType(val) != OBJ_STRING) return 0;
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
        o->fam_total[OWN_FAM_STRING] += n;
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
 * run it instead: a key whose expire has passed (lazy expire stays in lookupKey), a value that is not a
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
        if (ownerValExpired(val) || objectGetType(val) != OBJ_STRING) return 0;
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
    if (ownerValExpired(val) || objectGetType(val) != OBJ_STRING) return 0;
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
        o->fam_total[OWN_FAM_STRING] += n;
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
        o->fam_total[OWN_FAM_STRING] += n;
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
/* ------------------------------------------------------------------------------------------------
 * RL-g4c-blocking-native: blocking pops parked on the owner of their keys
 *
 * A blocking pop (BLPOP BRPOP BLMOVE BRPOPLPUSH BZPOPMIN BZPOPMAX BLMPOP BZMPOP) whose keys are all in one of
 * this owner's slots runs here by its stock proc. With data it runs as its non-blocking form. Without, the
 * proc's blockForKeys() parks the client on the owner (ownerBlockForKeys) instead of main's db->blocking_keys:
 * this owner's own registry (per db: key -> the clients parked on it, in the order they parked), its own
 * timeouts (a rax by deadline, run on the owner's clock in its windows), and c->flag.blocked set as for a
 * stock blocked client (no command of it runs; its input accumulates). Every key a parked client waits on
 * is in this owner's slots, so only this owner writes it, and signalKeyAsReady() from its own commands
 * (ownerSignalKeyReady) queues the key; right after that command (ownerTryNative) the parked clients of the
 * key run their command again, first parked first, as handleClientsBlockedOnKeys() after a stock command:
 * the served one pops, replies and propagates as its stock proc rewrites it (LPOP / RPOP, LMOVE, ZPOPMIN /
 * ZPOPMAX, [count]); one that finds nothing parks again at the tails, keeping its original timeout. Its
 * reply and the rest of its input go out at the end of the window (ownerRunUnparked).
 * Main never serves a parked client. Whenever main itself would touch one (a write of main to a key one waits
 * on, a blocking pop main blocks, CLIENT UNBLOCK of one, a veto or maxmemory, a role change, SWAPDB / FLUSHALL,
 * main taking clients during a long command), it first makes every parked client a stock blocked client on
 * main (ownerParkedToMain: in the order they parked, with their keys and timeouts), and the stock code does
 * the rest; that vetoes the native path until they are served, as before this gene. So a parked client and a
 * stock blocked client never wait at the same time.
 * ---------------------------------------------------------------------------------------------- */

typedef struct ownerPark {
    client *c;
    int nkeys, btype, dbid, slot;
    robj **keys;        /* its distinct keys (own copies) */
    listNode **nodes;   /* its node in each key's list of parked clients */
    listNode *all_node; /* its node in o->parked */
    long long timeout;  /* absolute, ms; 0: none */
    long long seq;      /* the order every owner's clients parked in (ownerParkedToMain keeps it) */
} ownerPark;

typedef struct ownerReady {
    serverDb *db;
    robj *key;
} ownerReady;

static int ownerResumeInput(owner *o, client *c);

static void ownerParkTimeKey(unsigned char *buf, long long timeout, uint64_t id) {
    uint64_t t = htonu64((uint64_t)timeout);
    id = htonu64(id);
    memcpy(buf, &t, 8);
    memcpy(buf + 8, &id, 8);
}

static long long ownerParkKeyTime(const unsigned char *buf) {
    uint64_t t;
    memcpy(&t, buf, 8);
    return (long long)ntohu64(t);
}

static robj *ownerKeyCopy(robj *key) {
    robj *d = getDecodedObject(key);
    robj *k = createStringObject(objectGetVal(d), sdslen(objectGetVal(d)));
    decrRefCount(d);
    return k;
}

/* blockForKeys() hook. Owner: park the client here, returns 1. Main: every parked client becomes a stock blocked
 * client first (the waiters of a key stay in the order they blocked), returns 0. */
int ownerBlockForKeys(client *c, int btype, robj **keys, int numkeys, long long timeout) {
    owner *o = own_self;
    if (!o) {
        /* RL-g5c-block-scope: main is about to block this client on its keys; the clients parked on those slots
         * (if any) become stock blocked clients first, so each slot's waiters keep the order they blocked. */
        if (own_n && atomic_load_explicit(&own_parked_total, memory_order_relaxed))
            for (int j = 0; j < numkeys; j++) ownerParkedToMainSlot(ownerKeySlotOf(keys[j]));
        return 0;
    }
    serverAssert(btype == BLOCKED_LIST || btype == BLOCKED_ZSET);
    /* A command run again after a wake keeps the timeout it parked with, as the stock reprocessing does. */
    if (c->flag.reexecuting_command) timeout = o->rerun_timeout;
    ownerPark *p = zcalloc(sizeof(*p));
    p->c = c;
    p->btype = btype;
    p->dbid = c->db->id;
    p->slot = c->slot;
    p->timeout = timeout;
    p->seq = atomic_fetch_add_explicit(&own_park_seq, 1, memory_order_relaxed);
    p->keys = zmalloc(sizeof(robj *) * numkeys);
    p->nodes = zmalloc(sizeof(listNode *) * numkeys);
    dict *d = o->park_keys[p->dbid];
    for (int j = 0; j < numkeys; j++) {
        int dup = 0;
        for (int k = 0; k < p->nkeys && !dup; k++) dup = equalStringObjects(p->keys[k], keys[j]);
        if (dup) continue; /* BLPOP k k 0 waits once on k, as the stock bstate->keys */
        robj *key = ownerKeyCopy(keys[j]);
        dictEntry *existing, *de = dictAddRaw(d, key, &existing);
        list *l;
        if (de) {
            incrRefCount(key); /* the registry's reference */
            l = listCreate();
            dictSetVal(d, de, l);
            o->nparked_keys++;
        } else {
            l = dictGetVal(existing);
        }
        listAddNodeTail(l, p);
        p->keys[p->nkeys] = key;
        p->nodes[p->nkeys] = listLast(l);
        p->nkeys++;
    }
    listAddNodeTail(o->parked, p);
    p->all_node = listLast(o->parked);
    if (timeout) {
        unsigned char tk[16];
        ownerParkTimeKey(tk, timeout, c->id);
        raxInsert(o->park_to, tk, sizeof(tk), p, NULL);
    }
    initClientBlockingState(c);
    c->bstate->btype = btype;
    c->bstate->timeout = timeout;
    c->flag.blocked = 1;
    c->owner_park = p;
    o->nparked++;
    atomic_fetch_add_explicit(&own_parked_total, 1, memory_order_relaxed);
    return 1;
}

/* The client is no longer parked (served, timed out, closing, freed, or going to main). Owner in its window, or
 * main with the world stopped. */
static void ownerUnpark(owner *o, client *c) {
    ownerPark *p = c->owner_park;
    dict *d = o->park_keys[p->dbid];
    for (int j = 0; j < p->nkeys; j++) {
        list *l = dictFetchValue(d, p->keys[j]);
        serverAssert(l != NULL);
        listDelNode(l, p->nodes[j]);
        if (listLength(l) == 0) {
            dictDelete(d, p->keys[j]);
            o->nparked_keys--;
        }
        decrRefCount(p->keys[j]);
    }
    if (p->timeout) {
        unsigned char tk[16];
        ownerParkTimeKey(tk, p->timeout, c->id);
        raxRemove(o->park_to, tk, sizeof(tk), NULL);
    }
    listDelNode(o->parked, p->all_node);
    zfree(p->keys);
    zfree(p->nodes);
    zfree(p);
    c->owner_park = NULL;
    c->flag.blocked = 0;
    c->bstate->btype = BLOCKED_NONE;
    c->bstate->timeout = 0;
    o->nparked--;
    atomic_fetch_sub_explicit(&own_parked_total, 1, memory_order_relaxed);
}

static void ownerUnparkedAdd(owner *o, client *c) {
    if (c->owner_unpark_node) return;
    listAddNodeTail(o->unparked, c);
    c->owner_unpark_node = listLast(o->unparked);
}

/* signalKeyAsReady() hook. Owner: queue the key when clients are parked on it here (served after the command),
 * returns 1. Main (world stopped): a key a parked client waits on is written by main: every parked client
 * becomes a stock blocked client, and the stock code goes on (returns 0). A deleted key never wakes a list or
 * sorted-set waiter (no unblock_on_nokey). */
int ownerSignalKeyReady(serverDb *db, robj *key, int type, int deleted) {
    owner *o = own_self;
    if (o) {
        if (!o->nparked || deleted || (type != OBJ_LIST && type != OBJ_ZSET)) return 1;
        if (!dictFind(o->park_keys[db->id], key)) return 1;
        if (listLength(o->ready) <= 64) {
            listIter li;
            listNode *ln;
            listRewind(o->ready, &li);
            while ((ln = listNext(&li))) {
                ownerReady *r = listNodeValue(ln);
                if (r->db == db && equalStringObjects(r->key, key)) return 1;
            }
        }
        ownerReady *r = zmalloc(sizeof(*r));
        r->db = db;
        r->key = ownerKeyCopy(key);
        listAddNodeTail(o->ready, r);
        return 1;
    }
    if (!own_n || deleted || (type != OBJ_LIST && type != OBJ_ZSET) || !own_held ||
        !atomic_load_explicit(&own_parked_total, memory_order_relaxed))
        return 0;
    int i = ownerOfKey(key);
    /* RL-g5c-block-scope: main wrote a key parked clients wait on; convert that slot's parked clients only. */
    if (i >= 0 && i < own_n && owners[i].park_keys && dictFind(owners[i].park_keys[db->id], key))
        ownerParkedToMainSlot(ownerKeySlotOf(key));
    return 0;
}

/* A parked client run again (a key it waits on is ready): served, or parked again at the tails. */
static void ownerWake(owner *o, ownerPark *p) {
    client *c = p->c;
    long long timeout = p->timeout;
    int slot = p->slot;
    ownerUnpark(o, c);
    o->park_wakes++;
    o->rerun_timeout = timeout;
    c->flag.reexecuting_command = 1;
    c->slot = slot;
    ownerCall(o, c, c->cmd, ownerKindOf(c->cmd), 1);
    c->flag.reexecuting_command = 0;
    ectx->current_client = NULL;
    if (c->owner_park) return; /* nothing left for it: parked again */
    commandProcessed(c);
    ownerUnparkedAdd(o, c);
}

/* After a command of this owner: serve the clients parked on the keys it made ready, key by key, each key's
 * clients first parked first, while the key still has something of their type (handleClientsBlockedOnKey). A
 * served BLMOVE's push can make another key ready: served in the same pass. */
static void ownerServeReady(owner *o) {
    if (o->serving) return;
    o->serving = 1;
    while (listLength(o->ready)) {
        listNode *ln = listFirst(o->ready);
        ownerReady *r = listNodeValue(ln);
        listDelNode(o->ready, ln);
        dict *d = o->park_keys[r->db->id];
        list *l = dictFetchValue(d, r->key);
        long count = l ? (long)listLength(l) : 0;
        while (count-- > 0) {
            l = dictFetchValue(d, r->key);
            if (!l) break;
            ownerPark *p = listNodeValue(listFirst(l));
            listRotateHeadToTail(l);
            /* As the stock loop: every one of the count is rotated, served only while the key has its type. */
            robj *v = lookupKeyReadWithFlags(r->db, r->key, LOOKUP_NOEFFECTS);
            int bt = !v ? -1 : objectGetType(v) == OBJ_LIST ? BLOCKED_LIST : objectGetType(v) == OBJ_ZSET ? BLOCKED_ZSET : -1;
            if (p->btype == bt) ownerWake(o, p);
        }
        decrRefCount(r->key);
        zfree(r);
    }
    o->serving = 0;
}

/* Owner, in its window: parked clients whose timeout passed (deadline < now, as handleBlockedClientsTimeout) get
 * the null array, counted once as the stock unblock on timeout counts them. */
static void ownerParkTimeouts(owner *o) {
    long long now = mstime();
    client *due[64];
    int n = 0;
    raxIterator ri;
    raxStart(&ri, o->park_to);
    raxSeek(&ri, "^", NULL, 0);
    while (n < 64 && raxNext(&ri)) {
        if (ownerParkKeyTime(ri.key) >= now) break;
        due[n++] = ((ownerPark *)ri.data)->c;
    }
    raxStop(&ri);
    for (int k = 0; k < n; k++) {
        client *c = due[k];
        if (!c->owner_park) continue;
        int kind = ownerKindOf(c->cmd);
        ownerUnpark(o, c);
        o->park_timeouts++;
        addReplyNullArray(c);
        ownerCmdStat *cs = &o->cs[kind];
        cs->calls++;
        if (server.latency_tracking_enabled) updateCommandLatencyHistogram(&cs->hist, 1000);
        c->commands_processed++;
        ectx->stat_numcommands++;
        o->native_total++;
        o->fam_total[own_kinds[kind].family]++;
        o->block_total++;
        commandProcessed(c);
        ownerUnparkedAdd(o, c);
    }
}

/* Owner, in its window: the earliest parked deadline (0: none). Kept for the next epoll_wait, which runs outside
 * the window, where main may change the registries. */
static long long ownerParkNext(owner *o) {
    if (!raxSize(o->park_to)) return 0;
    long long t = 0;
    raxIterator ri;
    raxStart(&ri, o->park_to);
    raxSeek(&ri, "^", NULL, 0);
    if (raxNext(&ri)) t = ownerParkKeyTime(ri.key);
    raxStop(&ri);
    return t;
}

static int ownerParkWaitMs(owner *o) {
    if (!o->park_next) return -1;
    long long d = o->park_next - mstime() + 1;
    if (d <= 0) return 0;
    return d > 1000000 ? 1000000 : (int)d;
}

/* Owner, in its window: the clients served or timed out in it get their replies out and the rest of their
 * input run (as processUnblockedClients()). Returns 1 when one left the owner. */
static int ownerRunUnparked(owner *o) {
    int handed = 0;
    listNode *ln;
    while ((ln = listFirst(o->unparked))) {
        client *c = listNodeValue(ln);
        listDelNode(o->unparked, ln);
        c->owner_unpark_node = NULL;
        if (c->owner_park || !c->owner_onloop) continue;
        handed |= ownerResumeInput(o, c);
    }
    return handed;
}

/* Owner: input of a parked client accumulates (its socket still reports end of file and errors), as a stock
 * blocked client's query buffer. Returns 1 when the client left the owner. */
static int ownerParkedRead(owner *o, client *c) {
    readToQueryBuf(c);
    if (c->nread <= 0) {
        if (c->nread == 0 || connGetState(c->conn) != CONN_STATE_CONNECTED) return ownerHandOff(o, c, OWN_WHY_CLOSE);
        trimClientQueryBuffer(c);
        return 0;
    }
    o->reads++;
    o->net_in += c->nread;
    c->net_input_bytes += c->nread;
    c->last_interaction = server.unixtime;
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) return ownerHandOff(o, c, OWN_WHY_QBLIMIT);
    trimClientQueryBuffer(c);
    return 0;
}

static int ownerParkSeqCmp(const void *a, const void *b) {
    long long x = (*(ownerPark *const *)a)->seq, y = (*(ownerPark *const *)b)->seq;
    return x < y ? -1 : x > y;
}

/* Main, world stopped: every parked client becomes a stock blocked client on main, in the order they parked,
 * with its keys and its timeout (blockForKeys); main serves them from here as the stock server does. */
static void ownerParkedToMainFilter(int only_slot) {
    if (!own_n || !atomic_load_explicit(&own_parked_total, memory_order_relaxed)) return;
    serverAssert(!own_self && own_held);
    long long n = 0, k = 0;
    for (int i = 0; i < own_n; i++) {
        listIter li;
        listNode *ln;
        listRewind(owners[i].parked, &li);
        while ((ln = listNext(&li))) {
            ownerPark *p = listNodeValue(ln);
            if (only_slot < 0 || p->slot == only_slot) n++;
        }
    }
    if (!n) return;
    ownerPark **all = zmalloc(sizeof(*all) * n);
    for (int i = 0; i < own_n; i++) {
        listIter li;
        listNode *ln;
        listRewind(owners[i].parked, &li);
        while ((ln = listNext(&li))) {
            ownerPark *p = listNodeValue(ln);
            if (only_slot < 0 || p->slot == only_slot) all[k++] = p;
        }
    }
    serverAssert(k == n);
    qsort(all, n, sizeof(*all), ownerParkSeqCmp);
    typedef struct {
        client *c;
        robj **keys;
        int nkeys, btype;
        long long timeout;
    } moving;
    moving *mv = zmalloc(sizeof(*mv) * n);
    for (k = 0; k < n; k++) {
        ownerPark *p = all[k];
        mv[k].c = p->c;
        mv[k].nkeys = p->nkeys;
        mv[k].btype = p->btype;
        mv[k].timeout = p->timeout;
        mv[k].keys = zmalloc(sizeof(robj *) * p->nkeys);
        for (int j = 0; j < p->nkeys; j++) {
            mv[k].keys[j] = p->keys[j];
            incrRefCount(p->keys[j]);
        }
    }
    zfree(all);
    for (k = 0; k < n; k++) ownerUnpark(&owners[mv[k].c->owner_tid], mv[k].c);
    for (k = 0; k < n; k++) {
        client *c = mv[k].c;
        ownerTakeToMain(c);
        if (!c->flag.close_asap) {
            c->flag.pending_command = 1;
            blockForKeys(c, mv[k].btype, mv[k].keys, mv[k].nkeys, mv[k].timeout, 0);
        }
        for (int j = 0; j < mv[k].nkeys; j++) decrRefCount(mv[k].keys[j]);
        zfree(mv[k].keys);
    }
    zfree(mv);
    own_parked_moved += n;
}

/* RL-g4c-blocking-native entry: every parked client (all owners, all slots). */
void ownerParkedToMain(void) { ownerParkedToMainFilter(-1); }
/* RL-g5c-block-scope: only the clients parked on one slot (main is about to block a client there, or wrote a key
 * there), so the parked clients on other slots keep running on their owners. */
static void ownerParkedToMainSlot(int slot) { ownerParkedToMainFilter(slot); }

long long ownerParkedCount(void) {
    long long n = 0;
    for (int i = 0; i < own_n; i++) n += owners[i].nparked;
    return n;
}

long long ownerParkedKeys(void) {
    long long n = 0;
    for (int i = 0; i < own_n; i++) n += owners[i].nparked_keys;
    return n;
}

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
        if (!ownerCall(o, c, c->parsed_cmd, kind, 1)) {
            ectx->current_client = NULL; /* an aborted script (ownerScriptRun): main runs it */
            return 0;
        }
        /* EXEC's bound is a sum over its queued commands: should it still have gone over, every
         * owner sends its work to main until main has looked, as after an oversized read. */
        if (own_mm && kind == OWN_EXEC) ownerMmAfterRead(o);
    }
    commandProcessed(c);
    ectx->current_client = NULL;
    /* RL-g4c-blocking-native: the clients parked on keys this command made ready are served now, before the next
     * command, as handleClientsBlockedOnKeys() after a stock command. */
    if (listLength(o->ready)) ownerServeReady(o);
    return 1;
}

/* Owner, in a window (owner-pipeline-resume): a client main sent home between two commands of a pipeline
 * (ownerGuestYield) has the rest of its input in its command queue, the next command parsed whole. Run
 * it now, as ownerReadPost runs a read's commands, until one needs main again: that one goes to main on
 * its own, after the replies of the ones run here. Returns 1 when the client left the owner. */
static int ownerResumeInput(owner *o, client *c) {
    ectx->cmd_time_snapshot = mstime();
    c->read_flags = authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    int to_main = 0;
    if (consumeCommandQueue(c)) {
        to_main = ownerRunParsedInput(c, ownerTryNative);
    } else if (c->querybuf && c->qb_pos < sdslen(c->querybuf)) {
        parseInputBuffer(c);
        prepareCommandQueue(c);
        if (own_mm) ownerMmAfterRead(o);
        to_main = ownerRunParsedInput(c, ownerTryNative);
    }
    trimCommandQueue(c);
    trimClientQueryBuffer(c);
    if (c->flag.close_asap) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    if (to_main) {
        if (clientHasPendingReplies(c) && !c->owner_held && !o->unflushed) ownerWriteOnce(o, c);
        return ownerHandOff(o, c, OWN_WHY_RUN);
    }
    if (clientHasPendingReplies(c)) return ownerReplyOut(o, c);
    return 0;
}

/* Error replies of native commands: the counts are kept here, the per-prefix stats fold on main. */
void ownerErrorReply(client *c, const char *s, size_t len, int flags) {
    owner *o = own_self;
    if (flags & ERR_REPLY_FLAG_NO_STATS_UPDATE) {
        int kind = c->realcmd ? ownerKindOf(c->realcmd) : -1;
        if (kind >= 0) o->cs[kind].failed++;
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
    if (c->owner_park) return ownerParkedRead(o, c); /* RL-g4c: input accumulates, as for a stock blocked client */
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

static int ownerReadTls(owner *o, client *c);

/* RL-g2v: whether the owner may finish a TLS accept itself. Main's part of the accept (clientAcceptHandler)
 * then would only count the connection: no protected-mode refusal is possible, no certificate names a
 * user, and no module could see the CLIENT_CHANGE event (a loaded or listening module vetoes the owners).
 * The configs it reads change only on main; a change racing a handshake is as racing a stock accept. */
static int ownerTlsAcceptLocal(client *c) {
    if (!server.owner_tls_local_accept || c->flag.close_asap) return 0;
    if (connGetState(c->conn) != CONN_STATE_CONNECTED) return 0;
    if (atomic_load_explicit(&own_veto, memory_order_relaxed) & OWN_VETO_ALL) return 0;
    if (server.protected_mode && (DefaultUser->flags & USER_FLAG_NOPASS)) return 0;
    if (server.tls_ctx_config.client_auth_user != TLS_CLIENT_FIELD_OFF) return 0;
    return 1;
}

/* RL-g1d, owner TLS ports: one step of a client's TLS handshake, on its owner (connAccept off main runs
 * SSL_accept and nothing else). Done or failed, main runs the rest of the accept (OWN_WHY_ACCEPTED).
 * RL-g2v: unless the owner may finish it itself (ownerTlsAcceptLocal): the client stays here and its
 * first request runs now, instead of after a trip to main and back (two world stops, and the main
 * thread's queue during a connection storm: 50-220 ms per new connection on loopback).
 * Returns 1 when the client left the owner. */
static int ownerHandshake(owner *o, client *c) {
    connAccept(c->conn, NULL);
    if (connGetState(c->conn) == CONN_STATE_ACCEPTING) {
        /* OpenSSL waits for the socket: readable (EPOLLIN, always on), or writable */
        ownerSetWantOut(o, c, (connIOOwnerState(c->conn) & CONN_IO_READ_WANTS_WRITE) != 0);
        return 0;
    }
    c->owner_hs = 0;
    o->tls_handshakes++;
    if (ownerTlsAcceptLocal(c)) {
        o->tls_local_accepts++;
        o->accepts_unfolded++;
        ownerSetWantOut(o, c, 0);
        /* The first request may have come with the client's Finished: read-ahead took it off the socket,
         * where no event reports it. */
        if (connIOOwnerState(c->conn) & CONN_IO_BUFFERED_INPUT) return ownerReadTls(o, c);
        return 0;
    }
    return ownerHandOff(o, c, OWN_WHY_ACCEPTED);
}

/* RL-g1d: a TLS client's read (syscalls: OpenSSL reads the socket itself). A read returns at most one
 * record, and records OpenSSL already took off the socket (read-ahead) raise no event, so it reads again
 * while some may wait, OWN_TLS_READS times, then in the next window (ownerTlsPend). Its replies go out
 * after each read, in order (ownerReplyOut). A read that needs the socket writable waits for EPOLLOUT.
 * Returns 1 when the client left the owner. */
static int ownerReadTls(owner *o, client *c) {
    for (int k = 0; k < OWN_TLS_READS; k++) {
        int r = ownerReadPre(o, c);
        if (r != OWN_READ_GO) return r;
        readToQueryBuf(c);
        int got = c->nread > 0;
        o->tls_reads++;
        /* RL-g2k: a client reading directly for its big input goes back to the batches once it stays small */
        if (c->owner_tls_direct && got)
            c->owner_tls_direct = c->nread >= OWN_TLS_BIG_READ ? OWN_TLS_DIRECT : c->owner_tls_direct - 1;
        if (ownerReadPost(o, c, 0)) return 1;
        int st = connIOOwnerState(c->conn);
        if (st & CONN_IO_READ_WANTS_WRITE) ownerSetWantOut(o, c, 1);
        if (!got || !(st & CONN_IO_BUFFERED_INPUT)) return 0;
    }
    ownerTlsPend(o, c);
    return 0;
}

/* RL-g2k: a TLS client's read in an io_uring batch: its ciphertext (if the recv got any) is pre-read, so this
 * SSL_read makes no system call; what OpenSSL leaves is kept before the scratch is reused. A reply that may
 * go out now joins the batch's writes, unless more input may wait (then on as ownerReadTls). Returns 1 when
 * the client left the owner. */
static int ownerReadTlsBatched(owner *o, client *c) {
    readToQueryBuf(c);
    connIOOwnerPrereadSettle(c->conn);
    int got = c->nread > 0;
    o->tls_reads++;
    int st = connIOOwnerState(c->conn);
    int more = got && (st & CONN_IO_BUFFERED_INPUT);
    if (ownerReadPost(o, c, !more)) return 1;
    if (st & CONN_IO_READ_WANTS_WRITE) ownerSetWantOut(o, c, 1);
    return more ? ownerReadTls(o, c) : 0;
}

/* RL-g1d: the TLS clients ownerTlsPend left for this window (main is not waiting). Returns 1 when any
 * left the owner. Clients pended again go to the next window. */
static int ownerTlsPending(owner *o) {
    int n = o->ntlsp, left = 0;
    for (int k = 0; k < n; k++) {
        uint64_t d = o->tlsp[k];
        int fd = (int)(uint32_t)d;
        client *c = fd < o->fdmap_cap ? o->fdmap[fd] : NULL;
        if (!c || (uint32_t)c->id != (uint32_t)(d >> 32) || !c->owner_onloop || !c->owner_tls || c->owner_hs) continue;
        left |= ownerReadTls(o, c);
    }
    memmove(o->tlsp, o->tlsp + n, sizeof(uint64_t) * (o->ntlsp - n));
    o->ntlsp -= n;
    return left;
}

/* owner-uring: a read of c joins the batch as o->uops[m] when it can go through io_uring (a TCP
 * client): returns 1 then. Into the client's own query buffer when it has one, else into the slot's
 * scratch, copied into the thread's shared query buffer when the batch runs (one client at a time
 * holds it, as with one read at a time). */
static int ownerReadQueue(owner *o, client *c, int m) {
    ownerUringOp *op = &o->uops[m];
    if (c->owner_tls) { /* RL-g2k: ciphertext into the slot's scratch; OpenSSL reads it from there */
        *op = (ownerUringOp){.c = c, .buf = o->uring_scratch + (size_t)m * PROTO_IOBUF_LEN, .len = PROTO_IOBUF_LEN,
                             .scratch = 1, .tls = 1};
        return 1;
    }
    if (c->conn->type != connectionTypeTcp()) return 0;
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
        if (op->tls) { /* RL-g2k: the next SSL_read takes the bytes from the scratch */
            o->tls_uring_reads++;
            if (op->res > 0)
                connIOOwnerPreread(c->conn, op->buf, (size_t)op->res, (size_t)op->res == op->len);
            else
                o->uring_redo++; /* the read again, from the socket: end of file, an error, nothing there */
            /* Input that fills a batch read (big requests) costs a copy here and a read for the rest of its
             * record: this client reads directly (OpenSSL into its own buffer) until its input stays small
             * (OWN_TLS_DIRECT reads in a row; ownerReadTls). */
            if (op->res > 0 && (size_t)op->res == op->len) c->owner_tls_direct = OWN_TLS_DIRECT;
            left |= ownerReadTlsBatched(o, c);
            continue;
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

/* RL-g3b-light-cron: databasesCron's rightsize and rehash steps for this owner's own tables, at the stock rates: each
 * kvstore of each db gets CRON_DICTS_PER_DB of its 1 << OWNER_SHARD_BITS tables checked a tick, so an owner checks its
 * share of them (every table about once in the stock sweep); the stock rehash budget (1% of a tick) is split across
 * the owners. Not while a child process runs (copy-on-write), as stock. The kvstores are in concurrent mode: a table's
 * rightsize and rehash touch only that table (its changes reach the kvstore at main's next fold). */
static void ownerTablesCron(owner *o) {
    if (hasActiveChildProcess()) return;
    int T = o->tbl_hi - o->tbl_lo + 1, ntab = 1 << OWNER_SHARD_BITS;
    if (T <= 0) return;
    int per = (CRON_DICTS_PER_DB * T + ntab - 1) / ntab;
    if (per > T) per = T;
    uint64_t budget = 0, spent = 0;
    if (server.activerehashing) {
        budget = (uint64_t)(1000000 / (server.hz > 0 ? server.hz : 1) / 100) / own_n;
        if (!budget) budget = 1;
    }
    for (int j = 0; j < server.dbnum; j++) {
        serverDb *db = server.db[j];
        if (!db) continue;
        kvstore *kvs[3] = {db->keys, db->expires, db->keys_with_volatile_items};
        for (int k = 0; k < 3; k++) {
            for (int t = 0; t < per; t++) kvstoreTryResizeHashtable(kvs[k], o->tbl_lo + (o->tbl_cursor + t) % T);
            o->tbl_checked += per;
            for (int d = o->tbl_lo; d <= o->tbl_hi && spent < budget; d++)
                spent += kvstoreRehashHashtable(kvs[k], d, budget - spent);
        }
    }
    o->tbl_cursor = (o->tbl_cursor + per) % T;
    o->tbl_rehash_us += (long long)spent;
}

/* RL-g2c-owner-cron: one client's share of clientsCron, on its owner, in a window. The stock checks in the stock order
 * (clientsCronHandleTimeout, clientsCronResizeQueryBuffer, clientsCronResizeOutputBuffer,
 * clientsCronTrackExpensiveClients, updateClientMemoryUsage, closeClientOnOutputBufferLimitReached); the parts that
 * write shared state keep it on the owner (the peaks, the memory-by-type changes) for main to read or fold under the
 * stop, and a client to close goes to main (as the owner's other closes do). Returns 1 when the client left. */
static int ownerClientCron(owner *o, client *c, mstime_t now_ms, int peak_slot) {
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    if (c->flag.close_asap) return ownerHandOff(o, c, OWN_WHY_CLOSE);
    /* An owner client is never a replica, primary, blocked or subscribed (those are main's); the stock test anyway. */
    if (server.maxidletime && !c->flag.replica && !mustObeyClient(c) && !c->flag.blocked && !c->flag.pubsub &&
        (server.unixtime - c->last_interaction > server.maxidletime)) {
        o->cron_timeouts++;
        return ownerHandOff(o, c, OWN_WHY_TIMEOUT);
    }
    if (c->owner_hs) return 0; /* a TLS handshake: no buffers yet */
    /* The thread's shared query buffer is never a client's between windows; never resize it anyway. */
    if (c->querybuf != thread_shared_qb) clientResizeQueryBuffer(c, server.unixtime - c->last_interaction);
    /* An expansion allocates: under maxmemory only within this owner's lease. */
    if (!own_mm || ownerMmFits(o, PROTO_REPLY_CHUNK_BYTES)) clientsCronResizeOutputBuffer(c, now_ms);
    size_t qb = c->querybuf ? sdsAllocSize(c->querybuf) : 0;
    size_t in = qb + c->argv_len_sum + (c->argv ? zmalloc_size(c->argv) : 0);
    size_t out = getClientOutputBufferMemoryUsage(c);
    if (in > o->peak_in[peak_slot]) o->peak_in[peak_slot] = in;
    if (out > o->peak_out[peak_slot]) o->peak_out[peak_slot] = out;
    /* updateClientMemoryUsage into this owner's changes. A client still in a memory usage bucket (maxmemory-clients
     * was on) is main's to update: the buckets are main's lists. */
    if (!c->mem_usage_bucket) {
        size_t mem = getClientMemoryUsage(c, NULL);
        int type = getClientType(c);
        o->cmem[c->last_memory_type] -= (long long)c->last_memory_usage;
        o->cmem[type] += (long long)mem;
        c->last_memory_type = type;
        c->last_memory_usage = mem;
    }
    /* On an owner the close is asynchronous (freeClientAsync flags it): main frees it once it has it. */
    if (closeClientOnOutputBufferLimitReached(c, 1)) {
        o->cron_closes++;
        return ownerHandOff(o, c, OWN_WHY_CLOSE);
    }
    return 0;
}

/* RL-g2c-owner-cron: a slice of this owner's clients' cron, once every 1000 / hz ms, as clientsCron spreads main's:
 * fdmap_cap / hz entries of the fd map from a cursor, so each client is looked at once a second. Returns 1 when a
 * client went to main. */
static int ownerClientsCron(owner *o, long long now_us) {
    int hz = server.hz > 0 ? server.hz : 1;
    o->cron_period_ms = 1000 / hz;
    o->cron_next_us = now_us + 1000000LL / hz;
    o->cron_slices++;
    if (atomic_load_explicit(&own_tables, memory_order_relaxed)) ownerTablesCron(o); /* RL-g3b */
    int slot = server.unixtime % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    int next = (slot + 1) % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    o->peak_in[next] = o->peak_out[next] = 0;
    if (!o->nclients) return 0;
    mstime_t now_ms = server.mstime;
    int handed = 0, span = (o->fdmap_cap + hz - 1) / hz;
    for (int k = 0; k < span; k++) {
        if (o->cron_fd >= o->fdmap_cap) o->cron_fd = 0;
        client *c = o->fdmap[o->cron_fd++];
        if (!c || !c->owner_onloop) continue;
        o->cron_clients++;
        handed |= ownerClientCron(o, c, now_ms, slot);
    }
    return handed;
}

/* Owner, outside a window: the epoll timeout until its next cron slice (-1 with no client). */
static int ownerCronTimeout(owner *o) {
    /* RL-g3f: while the owner's expire timer runs (RL-g2b, 1/hz), it wakes the owner for the cron slices too: one
     * tick a period instead of two (an epoll timeout and the timer, out of phase). */
    if (o->exp_hz > 0) return -1;
    /* RL-g3b: an owner with tables to keep wakes for them with no client too */
    if (!o->nclients && !atomic_load_explicit(&own_tables, memory_order_relaxed)) return -1;
    long long left = o->cron_next_us - (long long)getMonotonicUs();
    if (left <= 0) return 0;
    long long ms = (left + 999) / 1000, period = o->cron_period_ms ? o->cron_period_ms : 100;
    return ms > period ? (int)period : (int)ms;
}

/* RL-g2f: the owner's wait for work. Without spinning (owner-spin-us 0, or an owner that shares main's
 * core, or one that is not pinned) this is the parent's sleep in epoll_wait. With it, an owner that
 * finds nothing ready polls its own epoll (non-blocking, no lock taken while nothing is ready) for up
 * to its budget, a few pipeline flushes apart, and sleeps only when that found nothing. It runs
 * outside any window, so main's stops never wait for it. */
static int ownerExpireTimeout(owner *o); /* RL-g2b */

/* RL-2b: RL-g2f's spin and RL-g2p-busy-poll together. The spin is user space: non-blocking epoll_waits (timeout 0),
 * which the kernel never busy polls (ep_poll returns before ep_busy_loop when the caller does not wait). The wait
 * after it is the blocking epoll_wait, and that is where the kernel busy polls the set's NAPI queue for up to
 * owner-busy-poll-us (EPIOCSPARAMS, ownerBusyPollApply) before it sleeps. So an owner spins first, then busy polls,
 * then sleeps; either part is off on its own (owner-spin-us 0, owner-busy-poll-us 0), and both use one eligibility
 * rule (spin_ok == bp_ok: a core of its own). owner_epoll_waits counts the waits that could block. */
static inline int ownerWaitBlocking(owner *o, struct epoll_event *evs) {
    int tmo = ownerCronTimeout(o); /* RL-g2c: cron slices */
    /* RL-2e: RL-g4c's earliest parked deadline bounds the wait too (its replies are due then; the expire timer,
     * which ends a cron-less wait, ticks only every 1/hz). */
    int pt = ownerParkWaitMs(o);
    if (pt >= 0 && (tmo < 0 || pt < tmo)) tmo = pt;
    if (tmo) o->epoll_waits++;
    return epoll_wait(o->epfd, evs, OWNER_EVENTS, tmo);
}

static int ownerPoll(owner *o, struct epoll_event *evs) {
    if (o->ntlsp) return epoll_wait(o->epfd, evs, OWNER_EVENTS, 0); /* RL-g1d: TLS input may wait */
    if (ownerExpireTimeout(o) == 0) return epoll_wait(o->epfd, evs, OWNER_EVENTS, 0); /* RL-g2b: a cycle goes on */
    int cap = o->spin_ok ? ownerSpinCap(&server.owner_spin_us) : 0;
    o->waits++;
    if (cap <= 0) return ownerWaitBlocking(o, evs);
    int n = epoll_wait(o->epfd, evs, OWNER_EVENTS, 0);
    if (n != 0) return n;
    monotime t0 = getMonotonicUs();
    int b = o->spin_us < cap ? o->spin_us : cap;
    if (b > 0) {
        o->spins++;
        monotime end = t0 + (monotime)b, now;
        do {
            for (int i = 0; i < 32; i++) ownerCpuRelax();
            n = epoll_wait(o->epfd, evs, OWNER_EVENTS, 0);
            now = getMonotonicUs();
        } while (n == 0 && now < end);
        o->spin_time_us += (long long)(now - t0);
        if (n != 0) {
            if (n > 0) {
                o->spin_hits++;
                o->spin_us = ownerSpinGrow(b, cap);
            }
            return n;
        }
        o->spin_us = b / 2;
    }
    n = ownerWaitBlocking(o, evs);
    if (n > 0) ownerSpinSeed(&o->spin_us, (long long)(getMonotonicUs() - t0), cap);
    return n;
}

/* RL-g2p-busy-poll (owner-busy-poll-us, default 0 = off): NAPI busy polling on each owner's own epoll
 * set (EPIOCSPARAMS, Linux 6.9). An owner whose epoll_wait finds nothing ready then polls the NIC queue
 * its sockets' packets came in on (the set's NAPI id: that of the last socket that went ready) for up
 * to owner-busy-poll-us before it sleeps, instead of sleeping until the interrupt and the wakeup. With
 * the owner ports NIC-steered (owner i's flows on RX queue i, its IRQ on owner i's CPU) that is the
 * owner's own queue on its own core. The kernel ends the poll as soon as anything in the set is ready
 * (a socket, or main's wake eventfd) or the CPU is wanted, so main's wakeups are seen as before; the
 * owner polls outside its windows, so main's stop never waits for it. No NAPI id (loopback, a device
 * without NAPI): epoll does not poll and the option changes nothing. Only owners pinned to a CPU main
 * is not pinned to poll (bp_ok): an owner sharing main's CPU would take it from main. */
#ifndef EPIOCSPARAMS
struct epoll_params { /* linux/eventpoll.h (6.9), which does not go with sys/epoll.h */
    uint32_t busy_poll_usecs;
    uint16_t busy_poll_budget;
    uint8_t prefer_busy_poll;
    uint8_t __pad;
};
#define EPIOCSPARAMS _IOW(0x8A, 0x01, struct epoll_params)
#define EPIOCGPARAMS _IOR(0x8A, 0x02, struct epoll_params)
#endif

static int own_bp_ready = 0;        /* ownerThreadsStart ran: bp_ok is known */
static long long own_bp_errors = 0; /* EPIOCSPARAMS calls that failed (INFO) */
static int own_bp_errno = 0;        /* the last one's errno */

/* Main: owner-busy-poll-* onto every owner's epoll set (at start, and on CONFIG SET). A set that never
 * had it is left alone while the option is 0, so off is the parent's epoll set exactly. */
void ownerBusyPollApply(void) {
    if (!own_bp_ready) return;
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        int us = o->bp_ok ? server.owner_busy_poll_us : 0;
        if (!us && !o->bp_on) continue;
        struct epoll_params p = {0};
        p.busy_poll_usecs = (uint32_t)us;
        p.busy_poll_budget = us ? (uint16_t)server.owner_busy_poll_budget : 0;
        p.prefer_busy_poll = us && server.owner_busy_poll_prefer;
        if (ioctl(o->epfd, EPIOCSPARAMS, &p) == -1) {
            own_bp_errors++;
            own_bp_errno = errno;
            serverLog(LL_WARNING, "owner %d: owner-busy-poll-us %d: EPIOCSPARAMS: %s", i, us, strerror(errno));
            continue;
        }
        o->bp_on = us > 0;
    }
}

/* INFO: the owner thread's voluntary context switches (its sleeps), from /proc; 0 when unknown. */
static long long ownerSleeps(owner *o) {
    int tid = atomic_load_explicit(&o->tid, memory_order_relaxed);
    if (!tid) return 0;
    char path[64], line[128];
    snprintf(path, sizeof(path), "/proc/self/task/%d/status", tid);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    long long v = 0;
    while (fgets(line, sizeof(line), f)) {
        if (!strncmp(line, "voluntary_ctxt_switches:", 24)) {
            v = strtoll(line + 24, NULL, 10);
            break;
        }
    }
    fclose(f);
    return v;
}

/* INFO: TcpExt BusyPollRxPackets (packets busy polling took off a queue, this network namespace); -1
 * when the kernel does not have it. */
static long long ownerBusyPollRxPackets(void) {
    FILE *f = fopen("/proc/self/net/netstat", "r");
    if (!f) return -1;
    char *names = NULL, *vals = NULL;
    size_t nn = 0, nv = 0;
    long long v = -1;
    while (getline(&names, &nn, f) > 0 && getline(&vals, &nv, f) > 0) {
        if (strncmp(names, "TcpExt:", 7) || strncmp(vals, "TcpExt:", 7)) continue;
        char *sn, *sv;
        char *n = strtok_r(names + 7, " \n", &sn), *x = strtok_r(vals + 7, " \n", &sv);
        while (n && x) {
            if (!strcmp(n, "BusyPollRxPackets")) {
                v = strtoll(x, NULL, 10);
                break;
            }
            n = strtok_r(NULL, " \n", &sn);
            x = strtok_r(NULL, " \n", &sv);
        }
        break;
    }
    zlibc_free(names); /* getline's, from libc malloc */
    zlibc_free(vals);
    fclose(f);
    return v;
}

/* ------------------------------------------------------------------------------------------------
 * RL-g2b: active expiry on the owners
 *
 * Every key lives in a table one owner owns, so each owner runs the active expire cycle over its own
 * tables, on its own thread inside its windows, and main stops no owner for it. The algorithm and the
 * budgets are stock's (activeExpireCycle, activeExpireCycleJob's KEYS job), per owner: a slow cycle every
 * 1/hz s with ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC of the period, a fast cycle of
 * ACTIVE_EXPIRE_CYCLE_FAST_DURATION at most once per twice that after a cycle that stopped on its time
 * limit, rounds of keys_per_loop samples in at most 10x as many buckets, repeated while more than the
 * acceptable share was stale, active-expire-effort; with this owner's own cursors (one per db and table)
 * and stale estimate. A cycle runs in slices of at most OWN_EXPIRE_SLICE_US after each batch of client
 * work, and stops when main wants the world (it goes on in the next window). An expired key is deleted
 * as activeExpireCycleTryExpire deletes it (deleteExpiredKeyAndPropagate: lazyfree, WATCH, expired_keys,
 * its DEL or UNLINK), and the DEL/UNLINK goes into this owner's records as a native command's
 * (ownerTakeProp), so the AOF and the replicas get it in this owner's order, and replies after it wait
 * for the AOF as usual. Main folds the cycle time, time-limit stops, stale estimate and TTL sample into
 * the stock stats. While the owners do this (ownerExpiresKeys) main expires only hash fields. Whenever
 * the native path is vetoed (a replica, notifications, tracking, pause, modules, ...), expiry is off
 * or owner-active-expire is no, main runs the stock cycle for every key, as before. A replica never
 * expires keys: owners are vetoed there, and own_expire needs iAmPrimary.
 * ---------------------------------------------------------------------------------------------- */

#define OWN_EXPIRE_SLICE_US 100 /* the longest a cycle runs between two batches of client work */

int ownerExpiresKeys(void) {
    return own_n && !own_dormant && atomic_load_explicit(&own_expire, memory_order_relaxed);
}

typedef struct ownerExpireScan {
    owner *o;
    serverDb *db;
    mstime_t now;
    unsigned long sampled, expired;
    int mm_stop; /* under maxmemory: the next delete's records do not fit this owner's lease */
} ownerExpireScan;

/* expireScanCallback and activeExpireCycleTryExpire on the owner. */
static void ownerExpireScanCb(void *privdata, void *entry, int didx) {
    ownerExpireScan *d = privdata;
    owner *o = d->o;
    robj *val = entry;
    if (d->mm_stop) return;
    mstime_t when = objectGetExpire(val);
    if (d->now > when) {
        sds key = objectGetKey(val);
        /* Under maxmemory the records it adds must fit this owner's lease (the delete frees more). */
        if (own_mm && !ownerMmFits(o, OWN_MM_REC * (sdslen(key) + 64))) {
            d->mm_stop = 1;
            return;
        }
        enterExecutionUnit(0, 0); /* not main's cached clock: the owner's own time (d->now) */
        robj *keyobj = createStringObject(key, sdslen(key));
        deleteExpiredKeyAndPropagateWithDictIndex(d->db, keyobj, didx);
        decrRefCount(keyobj);
        ectx->dirty++;
        exitExecutionUnit();
        if (ectx->also_propagate.numops) ownerTakeProp(o, NULL); /* one op: no MULTI around it */
        d->expired++;
    } else if (when > d->now) {
        o->exp_ttl_sum[d->db->id] += when - d->now;
        o->exp_ttl_n[d->db->id]++;
    }
    d->sampled++;
}

/* This owner's keys with an expire in kvs, counted up to cap. */
static unsigned long ownerExpireCount(owner *o, kvstore *kvs, unsigned long cap) {
    unsigned long n = 0;
    for (int t = 0; t < o->exp_ntables && n < cap; t++) n += kvstoreHashtableSize(kvs, o->exp_t0 + t);
    return n;
}

/* One sampling round over this owner's tables of a db: up to num keys in at most num * 10 buckets,
 * from where the walk stopped; empty tables are skipped, as the kvstore cursor skips them. Returns 1
 * when the walk went past this owner's last table (activeExpireCycleJob's db_done). */
static int ownerExpireRound(owner *o, kvstore *kvs, int dbid, unsigned long num, ownerExpireScan *d) {
    unsigned long start = d->sampled;
    long checked = 0, max_buckets = (long)num * 10;
    while (d->sampled - start < num && checked < max_buckets && !d->mm_stop) {
        int t = o->exp_at[dbid], didx = o->exp_t0 + t;
        unsigned long long *cur = &o->exp_cur[dbid * o->exp_ntables + t];
        if (kvstoreHashtableSize(kvs, didx)) {
            *cur = kvstoreScan(kvs, *cur, didx, didx, ownerExpireScanCb, expireShouldSkipTableForSamplingCb, d);
            checked++;
        } else {
            *cur = 0;
        }
        if (*cur == 0) {
            o->exp_at[dbid] = t + 1 == o->exp_ntables ? 0 : t + 1;
            if (t + 1 == o->exp_ntables) return 1;
        }
    }
    return 0;
}

/* The cycle in progress ends: the stale estimate as activeExpireCycleJob updates it, 5% per cycle. */
static void ownerExpireEnd(owner *o) {
    double perc = o->exp_c_sampled ? (double)o->exp_c_expired / o->exp_c_sampled : 0;
    o->exp_stale = perc * 0.05 + o->exp_stale * 0.95;
    o->exp_left = 0;
}

static void ownerExpireBegin(owner *o, long long budget_us) {
    if (o->exp_left) ownerExpireEnd(o); /* its slices did not get to finish it within its period */
    o->exp_left = budget_us;
    o->exp_dbs = CRON_DBS_PER_CALL > server.dbnum || o->exp_timelimit ? server.dbnum : CRON_DBS_PER_CALL;
    o->exp_j = 0;
    o->exp_timelimit = 0;
    o->exp_c_sampled = o->exp_c_expired = 0;
}

/* Owner, in a window after its clients' work: start a cycle when one is due, and run the cycle in
 * progress for one slice. */
static void ownerExpireRun(owner *o) {
    monotime t0 = getMonotonicUs();
    int effort = server.active_expire_effort - 1;
    unsigned long acceptable = ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE - effort;
    unsigned long keys_per_loop = ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP + ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP / 4 * effort;
    if (o->exp_due) { /* serverCron's slow cycle, at this owner's 1/hz timer */
        o->exp_due = 0;
        long long period = 1000000 / server.hz;
        ownerExpireBegin(o, (long long)(ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC + 2 * effort) * period / 100);
        o->exp_cycles++;
    } else if (!o->exp_left) { /* beforeSleep's fast cycle */
        long long fast = ACTIVE_EXPIRE_CYCLE_FAST_DURATION + ACTIVE_EXPIRE_CYCLE_FAST_DURATION / 4 * effort;
        /* compared as activeExpireCycleJob compares them (a share against a percentage) */
        if (!o->exp_timelimit && o->exp_stale < acceptable) return;
        if (t0 < o->exp_last_fast + 2 * fast) return;
        o->exp_last_fast = t0;
        ownerExpireBegin(o, fast);
        o->exp_fast++;
    }
    long long slice = o->exp_left < OWN_EXPIRE_SLICE_US ? o->exp_left : OWN_EXPIRE_SLICE_US;
    client *cur_client = ectx->current_client, *exec_client = ectx->executing_client;
    ectx->current_client = ectx->executing_client = NULL;
    int out = 0; /* the slice is over: its time is up, main wants the world, or the lease is spent */
    while (!out && o->exp_dbs > 0 && o->exp_j < server.dbnum) {
        serverDb *db = server.db[o->exp_db % server.dbnum];
        if (!db || !ownerExpireCount(o, db->expires, 1)) {
            o->exp_db++;
            o->exp_j++;
            continue;
        }
        ownerExpireScan d = {.o = o, .db = db, .now = mstime()};
        ectx->cmd_time_snapshot = d.now;
        int repeat = 0;
        do {
            unsigned long num = ownerExpireCount(o, db->expires, keys_per_loop);
            if (num == 0) break;
            unsigned long s = d.sampled, e = d.expired;
            int done = ownerExpireRound(o, db->expires, db->id, num, &d);
            s = d.sampled - s;
            e = d.expired - e;
            repeat = !done && (s == 0 || e * 100 / s > acceptable);
            if ((long long)(getMonotonicUs() - t0) >= slice || d.mm_stop ||
                atomic_load_explicit(&own_stop, memory_order_relaxed))
                out = 1;
        } while (repeat && !out);
        o->exp_c_sampled += d.sampled;
        o->exp_c_expired += d.expired;
        o->exp_sampled += d.sampled;
        o->exp_keys += d.expired;
        if (!repeat) { /* this db is done for this cycle */
            o->exp_db++;
            o->exp_j++;
            o->exp_dbs--;
        }
    }
    ectx->current_client = cur_client;
    ectx->executing_client = exec_client;
    long long us = (long long)(getMonotonicUs() - t0);
    o->exp_left -= us;
    o->exp_us += us;
    o->exp_us_total += us;
    o->exp_slices++;
    o->exp_fold = 1;
    if (o->exp_dbs == 0 || o->exp_j >= server.dbnum) {
        ownerExpireEnd(o);
    } else if (o->exp_left <= 0) {
        o->exp_timelimit = 1; /* the next cycles do every db; fast cycles run */
        o->exp_cap++;
        ownerExpireEnd(o);
    }
}

/* The owner's epoll timeout: a cycle in progress goes on at once; the slow cycles come with the timer. */
static int ownerExpireTimeout(owner *o) {
    return o->exp_left && atomic_load_explicit(&own_expire, memory_order_relaxed) ? 0 : -1;
}

/* Owner, in a window: its slow-cycle timer follows own_expire and hz (period 1/hz; off when 0). */
static void ownerExpireArm(owner *o, int hz) {
    struct itimerspec its = {{0, 0}, {0, 0}};
    if (hz > 0) {
        long long ns = 1000000000LL / hz;
        its.it_interval.tv_sec = its.it_value.tv_sec = (time_t)(ns / 1000000000LL);
        its.it_interval.tv_nsec = its.it_value.tv_nsec = (long)(ns % 1000000000LL);
    }
    if (timerfd_settime(o->exp_tfd, 0, &its, NULL) == -1)
        serverLog(LL_WARNING, "owner %d: expire timer: %s", o->id, strerror(errno));
    o->exp_hz = hz;
    if (!hz) o->exp_due = 0;
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
    atomic_store_explicit(&o->tid, (int)gettid(), memory_order_relaxed);
    /* RL-g3g-long-tail-rand: this thread's own random states (mt19937-64.c), for SPOP, SRANDMEMBER, HRANDFIELD and
     * ZRANDMEMBER (hashtable, listpack and intset sampling): no random state is shared with main or another owner. */
    init_genrand64(o->rng_seed);
    threadRandomSeed((unsigned int)(o->rng_seed >> 32) ^ (unsigned int)o->rng_seed);
    ioThreadsMarkOwnerThread();
    initSharedQueryBuf();
#ifdef HAVE_LIBURING
    ownerUringInit(o);
#endif

    struct epoll_event evs[OWNER_EVENTS];
    for (;;) {
        int n = ownerPoll(o, evs);
        if (n < 0) {
            if (errno != EINTR) serverLog(LL_WARNING, "owner %d: epoll_wait: %s", o->id, strerror(errno));
            continue;
        }
        ownerEnter(o);
        o->leave_streak = o->left_early ? o->leave_streak + 1 : 0;
        o->may_leave = o->leave_streak < OWN_LEAVE_STREAK;
        o->left_early = 0;
        o->windows_total++;
        o->wake_main = 0;
        /* RL-g3h: a replica fell repl-flow-control-bytes behind: take no new client input this window. Writes,
         * clients main hands back, held replies, TLS handshakes and closing connections still go on. */
        int flow = atomic_load_explicit(&server.repl_flow_throttled, memory_order_acquire);
        int flow_level = atomic_load_explicit(&server.repl_flow_level, memory_order_acquire);
        int flow_skipped = 0;
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
        /* RL-g2s: TLS clients join the io_uring batches only when this wakeup brought enough of them to share
         * the batch's system calls. At a low rate a wakeup brings one or two (1.4 commands per wakeup at 10k
         * commands per second per owner): batching then saves no system call, holds each reply until the
         * batch's reads have run and adds an io_uring round trip for the read and one for the reply (the fleet
         * saw TLS p99 at that rate go from 0.35 to 0.53 ms). Such clients read and reply straight through
         * OpenSSL's socket calls, as RL-1 does; busy wakeups keep the batches. */
        int tls_batch = batch && n >= OWN_TLS_BATCH_MIN;
        int m = 0, stop = 0;
        for (int k = 0; k < n && !stop; k++) {
            uint64_t d = evs[k].data.u64;
            if (d == OWNER_WAKE_TOKEN) {
                ownerTakeInbox(o);
                continue;
            }
            if (d == OWNER_EXPIRE_TOKEN) { /* RL-g2b: a slow expire cycle is due */
                uint64_t ticks;
                if (read(o->exp_tfd, &ticks, sizeof(ticks)) == -1) { /* EAGAIN: already read */
                }
                o->exp_due = 1;
                o->cron_tick = 1; /* RL-g3f */
                continue;
            }
            int fd = (int)(uint32_t)d;
            client *c = fd < o->fdmap_cap ? o->fdmap[fd] : NULL;
            if (!c || (uint32_t)c->id != (uint32_t)(d >> 32)) continue; /* stale: main took it back */
            uint32_t e = evs[k].events;
            if (c->owner_hs) { /* RL-g1d: its TLS handshake, on this owner */
                handed |= ownerHandshake(o, c);
                continue;
            }
            /* RL-g1d: a TLS read that needed the socket writable goes on now */
            if (c->owner_tls && (e & EPOLLOUT) && (connIOOwnerState(c->conn) & CONN_IO_READ_WANTS_WRITE)) e |= EPOLLIN;
            if ((e & EPOLLOUT) && !c->owner_held && ownerWrite(o, c)) {
                handed = 1;
                continue;
            }
            if (flow && (e & EPOLLIN) && !(e & (EPOLLERR | EPOLLHUP))) {
                flow_skipped++; /* level-triggered: it comes back with the next epoll_wait */
                continue;
            }
            if (e & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                if (c->owner_tls && (!tls_batch || c->owner_deep || c->owner_tls_direct || !connIOOwnerBatchReady(c->conn))) {
                    handed |= ownerReadTls(o, c);
                } else if (!c->owner_tls && (!batch || c->owner_deep)) {
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
        /* RL-g4a: queued reads are not submitted yet (ownerReadQueue only chose each one's buffer): with a stop
         * pending they are dropped, their memory lease reservations given back, and the next window reads
         * them (epoll is level-triggered: the sockets are still readable). Nothing was read or run for them. */
        if (m && ownerStopPending(o)) {
            if (own_mm) o->mm_resv -= (size_t)m * OWN_MM_READ;
            atomic_fetch_add_explicit(&own_reads_left, m, memory_order_relaxed);
            atomic_fetch_add_explicit(&own_early_leaves, 1, memory_order_relaxed);
            m = 0;
        }
        if (m) handed |= ownerReadBatch(o, m);
        /* RL-g1d: TLS clients whose input OpenSSL may hold (none while main waits: next window). */
        if (o->ntlsp && !flow && !atomic_load_explicit(&own_stop, memory_order_relaxed)) handed |= ownerTlsPending(o);
        serverAssert(o->mm_resv == 0); /* every queued read ran, and gave its reservation back */
        /* RL-g4c-blocking-native: parked clients whose timeout passed get the null array; the clients served or
         * timed out in this window get their replies out and the rest of their input run. */
        if (raxSize(o->park_to)) ownerParkTimeouts(o);
        if (listLength(o->unparked)) handed |= ownerRunUnparked(o);
        o->park_next = ownerParkNext(o);
        /* RL-g2b: this owner's active expiry, after its clients' work (a slice; it stops when main waits). */
        int expire = atomic_load_explicit(&own_expire, memory_order_relaxed) &&
                     !atomic_load_explicit(&own_veto, memory_order_relaxed);
        if ((expire ? server.hz : 0) != o->exp_hz) ownerExpireArm(o, expire ? server.hz : 0);
        if (expire && !atomic_load_explicit(&own_stop, memory_order_relaxed)) ownerExpireRun(o);
        /* This window's AOF records: written here (everysec, no), unless main is stopping the world,
         * which folds them first thing and writes them with its own. */
        if (o->unflushed && own_aof_drain && !atomic_load_explicit(&own_stop, memory_order_relaxed))
            ownerDrainAof(o);
        if (o->nrefc) ownerRefsSettle(o); /* no value reference outlives the window */
        /* RL-g2c: this owner's clients' cron, when a slice is due (not while main waits: the next window). With the
         * owners' cron off (main runs it) the owner only looks again a period later. */
        {
            long long now_us = (long long)getMonotonicUs();
            /* RL-g3f: a tick of the expire timer runs a slice due within half a period (the timer and the slices'
             * deadline drift apart by the wakeup latency; a busy owner that ran one by its deadline skips the tick). */
            long long half_us = 500LL * (o->cron_period_ms ? o->cron_period_ms : 100);
            int tick = o->cron_tick;
            o->cron_tick = 0;
            if (now_us >= o->cron_next_us || (tick && now_us >= o->cron_next_us - half_us)) {
                if (!atomic_load_explicit(&own_cron, memory_order_relaxed))
                    o->cron_next_us = now_us + 1000LL * (o->cron_period_ms ? o->cron_period_ms : 100);
                else if (!atomic_load_explicit(&own_stop, memory_order_relaxed))
                    handed |= ownerClientsCron(o, now_us);
            }
        }
        /* RL-g3b: what main reads while the owners run (ownerPublishedStats, ownerExpireBound) */
        atomic_store_explicit(&o->pub_cmds, o->ectx.stat_numcommands, memory_order_relaxed);
        atomic_store_explicit(&o->pub_net_in, o->net_in, memory_order_relaxed);
        atomic_store_explicit(&o->pub_net_out, o->net_out, memory_order_relaxed);
        atomic_store_explicit(&o->pub_dirty, o->ectx.dirty, memory_order_relaxed);
        atomic_store_explicit(&o->pub_exp_min, o->exp_min, memory_order_relaxed);
        ownerLeave(o);
        if (handed || o->wake_main) {
            uint64_t one = 1;
            if (write(own_main_wakefd, &one, sizeof(one)) == -1) { /* a wakeup is pending anyway */
            }
        }
        if (flow_skipped || (flow && o->ntlsp)) {
            /* RL-g3h: the skipped sockets are still readable, so epoll_wait would return at once: nap outside
             * the window (main may stop the world meanwhile) until the sender clears the flag, at most
             * OWN_FLOW_NAP_US, then look again. */
            o->flow_skipped_total += flow_skipped;
            o->flow_naps_total++;
            struct timespec nap = {0, OWN_FLOW_NAP_US * 1000};
            nanosleep(&nap, NULL);
        } else if (flow_level > 0) {
            /* RL-g3h pacing: a nap of level x repl-flow-control-delay-us / 1000 after the window (outside it), in
             * steps of OWN_FLOW_NAP_US so it ends early once the sender lowers the level to 0. */
            long long us = (long long)flow_level * server.repl_flow_control_delay_us / 1000;
            o->flow_paced_total++;
            o->flow_paced_us_total += us;
            while (us > 0) {
                long long step = us < OWN_FLOW_NAP_US ? us : OWN_FLOW_NAP_US;
                struct timespec nap = {0, step * 1000};
                nanosleep(&nap, NULL);
                us -= step;
                if (!atomic_load_explicit(&server.repl_flow_level, memory_order_relaxed)) break;
            }
        }
    }
    return NULL;
}

void ownerThreadsStart(void) {
    /* RL-g2f / RL-g2p-busy-poll: main's CPUs are set by now (serverSetCpuAffinity runs just before this). An owner
     * spins, and busy polls, only on a core of its own: pinned, and not on a CPU main is pinned to (one rule for
     * both: spin_ok == bp_ok). */
    cpu_set_t mainset;
    CPU_ZERO(&mainset);
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    int main_pinned = sched_getaffinity(0, sizeof(mainset), &mainset) == 0 && CPU_COUNT(&mainset) < ncpu;
    int bp_ok = 0;
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        o->spin_ok = o->cpu >= 0 && !(main_pinned && CPU_ISSET(o->cpu, &mainset));
        o->spin_us = server.owner_spin_us;
        o->spin_stop_us = server.owner_spin_stop_us;
        o->bp_ok = o->spin_ok;
        bp_ok += o->bp_ok;
    }
    own_bp_ready = 1;
    ownerBusyPollApply();
    if (server.owner_busy_poll_us) {
        int on = 0;
        for (int i = 0; i < own_n; i++) on += owners[i].bp_on;
        serverLog(LL_NOTICE, "owner busy poll: %d us, budget %d, prefer %s: on %d of %d owners (%d may poll)",
                  server.owner_busy_poll_us, server.owner_busy_poll_budget,
                  server.owner_busy_poll_prefer ? "yes" : "no", on, own_n, bp_ok);
    }
    for (int i = 0; i < own_n; i++) {
        owners[i].rng_seed = genrand64_int64(); /* main's generator, seeded by the time at startup */
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
    if (server.owner_tls_port && port >= server.owner_tls_port && port < server.owner_tls_port + own_n)
        return port - server.owner_tls_port;
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
    /* RL-g1d: a TLS client gets the owners' TLS ports (as tls-cluster nodes answer TLS clients), plain
     * clients the plain ones; CLUSTER SHARDS lists both, as stock does. */
    int tls = server.owner_tls_port && c->conn && connIsTLS(c->conn);
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
            addReplyLongLong(c, tls ? o->tls_port : o->port);
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
            addReplyMapLen(c, o->tls_port ? 8 : 7);
            addReplyBulkCString(c, "id");
            addReplyBulkCBuffer(c, id, CLUSTER_NAMELEN);
            addReplyBulkCString(c, "port");
            addReplyLongLong(c, o->port);
            if (o->tls_port) {
                addReplyBulkCString(c, "tls-port");
                addReplyLongLong(c, o->tls_port);
            }
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
            s = sdscatprintf(s, "%s %s:%d@%d %s - 0 0 %d connected %d-%d\n", id, ip, tls ? o->tls_port : o->port,
                             o->port + 10000, i == me ? "myself,master" : "master", i + 1, o->slot_lo, o->slot_hi);
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
    long long flow_skipped = 0, flow_naps = 0, flow_paced = 0, flow_paced_us = 0;
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
        flow_skipped += owners[i].flow_skipped_total;
        flow_naps += owners[i].flow_naps_total;
        flow_paced += owners[i].flow_paced_total;
        flow_paced_us += owners[i].flow_paced_us_total;
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
                         own_last_veto & OWN_VETO_ALL ? "all" : own_last_veto & OWN_VETO_BLOCKED ? "scoped" :
                             own_last_veto & OWN_VETO_WRITE ? "writes" : "none",
                         native, own_takes, own_returns, adopted, windows, resident,
                         own_guests ? listLength(own_guests) : 0UL, own_stops, own_stop_wait_us, own_stop_wait_max_us);
    *info = sdscatprintf(*info, "owner_pipeline_resume:%d\r\nowner_pipeline_resumes:%lld\r\nowner_pipeline_reruns:%lld\r\n",
                         server.owner_pipeline_resume, own_resumes, own_reruns);
    /* RL-g2a: handed-over clients that went home right after their command, replies unsent (ownerGoHomeNow). */
    *info = sdscatprintf(*info, "owner_returns_with_replies:%lld\r\n", own_home_now);
    /* RL-g7d-replica-reads: native reads on a replica; paced applies of the primary's stream (timers armed, fired,
     * and the bytes of the reads that armed them). */
    *info = sdscatprintf(*info,
                         "owner_replica_reads:%s\r\nowner_replica_pace_ms:%d\r\nowner_replica_paced:%lld\r\n"
                         "owner_replica_pace_fires:%lld\r\nowner_replica_paced_bytes:%lld\r\n",
                         server.owner_replica_reads ? "yes" : "no", server.owner_replica_pace_ms, own_rpace_paced,
                         own_rpace_fires, own_rpace_bytes);
    /* RL-g4a: windows left early for a pending stop; inbox clients and queued reads left for the next window. */
    *info = sdscatprintf(*info, "owner_stop_early_leaves:%lld\r\nowner_stop_inbox_left:%lld\r\nowner_stop_reads_left:%lld\r\n",
                         atomic_load_explicit(&own_early_leaves, memory_order_relaxed),
                         atomic_load_explicit(&own_inbox_left, memory_order_relaxed),
                         atomic_load_explicit(&own_reads_left, memory_order_relaxed));
    /* RL-g3h: reads left unread while replica flow control was on, and the naps after such windows. */
    *info = sdscatprintf(*info, "owner_flow_skipped_reads:%lld\r\nowner_flow_naps:%lld\r\nowner_flow_paced:%lld\r\n"
                         "owner_flow_paced_us:%lld\r\n", flow_skipped, flow_naps, flow_paced, flow_paced_us);
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
    /* RL-1: native commands by family (own_kinds[].family). */
    for (int f = 0; f < OWN_NFAM; f++) {
        long long n = 0;
        for (int i = 0; i < own_n; i++) n += owners[i].fam_total[f];
        *info = sdscatprintf(*info, "owner_%s_commands:%lld\r\n", own_fam_names[f], n);
    }
    /* RL-g2o-long-tail: native long-tail commands (OWN_K_TAIL kinds; counted in their families too). */
    long long tail_n = 0;
    for (int i = 0; i < own_n; i++) tail_n += owners[i].tail_total;
    *info = sdscatprintf(*info, "owner_tail_commands:%lld\r\n", tail_n);
    /* RL-g4c-blocking-native: blocking pops completed natively, clients parked now, their wakes and timeouts, and the
     * parked clients main made stock blocked clients (ownerParkedToMain). */
    long long blk = 0, prk = 0, wk = 0, tmo = 0;
    for (int i = 0; i < own_n; i++) {
        blk += owners[i].block_total;
        prk += owners[i].nparked;
        wk += owners[i].park_wakes;
        tmo += owners[i].park_timeouts;
    }
    long long bslots = 0;
    for (int w = 0; w < OWN_BLOCKED_WORDS; w++) bslots += __builtin_popcountll(own_blocked_slots[w]);
    *info = sdscatprintf(*info,
                         "owner_block_commands:%lld\r\nowner_parked_clients:%lld\r\nowner_park_wakes:%lld\r\n"
                         "owner_park_timeouts:%lld\r\nowner_parked_to_main:%lld\r\n"
                         "owner_block_scoped:%lld\r\nowner_blocked_slots:%lld\r\n",
                         blk, prk, wk, tmo, own_parked_moved, own_block_scoped, bslots);
    /* RL-g1d: owner TLS ports; handshakes and TLS reads the owners ran. */
    long long tls_hs = 0, tls_rd = 0, tls_la = 0, tls_ur = 0, tls_uw = 0;
    for (int i = 0; i < own_n; i++) {
        tls_hs += owners[i].tls_handshakes;
        tls_rd += owners[i].tls_reads;
        tls_la += owners[i].tls_local_accepts;
        tls_ur += owners[i].tls_uring_reads;
        tls_uw += owners[i].tls_uring_writes;
    }
    *info = sdscatprintf(*info, "owner_tls_uring_reads:%lld\r\nowner_tls_uring_writes:%lld\r\n", tls_ur, tls_uw);
    *info = sdscatprintf(*info, "owner_tls_port:%d\r\nowner_tls_handshakes:%lld\r\nowner_tls_reads:%lld\r\n",
                         server.owner_tls_port, tls_hs, tls_rd);
    *info = sdscatprintf(*info, "owner_tls_write_tails:%lld\r\n", /* RL-g2d */
                         atomic_load_explicit(&server.stat_tls_write_tails, memory_order_relaxed));
    /* RL-g2b: active expiry on the owners: on now (yes) or main's (no); cycles, slices, keys, time. */
    long long ex_cyc = 0, ex_fast = 0, ex_sl = 0, ex_keys = 0, ex_samp = 0, ex_us = 0;
    for (int i = 0; i < own_n; i++) {
        ex_cyc += owners[i].exp_cycles;
        ex_fast += owners[i].exp_fast;
        ex_sl += owners[i].exp_slices;
        ex_keys += owners[i].exp_keys;
        ex_samp += owners[i].exp_sampled;
        ex_us += owners[i].exp_us_total;
    }
    *info = sdscatprintf(*info,
                         "owner_active_expire:%s\r\nowner_expire_cycles:%lld\r\nowner_expire_fast_cycles:%lld\r\n"
                         "owner_expire_slices:%lld\r\nowner_expired_keys:%lld\r\nowner_expire_sampled:%lld\r\n"
                         "owner_expire_usec:%lld\r\n",
                         ownerExpiresKeys() ? "yes" : "no", ex_cyc, ex_fast, ex_sl, ex_keys, ex_samp, ex_us);
    /* RL-g2v: TLS accepts the owners finished without a trip to main. */
    *info = sdscatprintf(*info, "owner_tls_local_accept:%d\r\nowner_tls_local_accepts:%lld\r\n",
                         server.owner_tls_local_accept, tls_la);
    /* RL-g3i: stops that left closed clients for the next stop (OWN_CLOSES_PER_STOP). */
    *info = sdscatprintf(*info, "owner_close_defers:%lld\r\n", own_close_defers);
    /* RL-g6a: TLS connections on the owner record path; KeyUpdates read / sent, fatal alerts, close_notify sent;
     * verify mode: records OpenSSL made too that were equal / not. */
    *info = sdscatprintf(*info,
                         "owner_tls_records:%s\r\nowner_tls_rec_conns:%lld\r\nowner_tls_rec_keyupdates_in:%lld\r\n"
                         "owner_tls_rec_keyupdates_out:%lld\r\nowner_tls_rec_fatal:%lld\r\nowner_tls_rec_closes:%lld\r\n"
                         "owner_tls_rec_verified:%lld\r\nowner_tls_rec_mismatch:%lld\r\n",
                         server.tls_owner_records ? (server.tls_owner_records_verify ? "verify" : "yes") : "no",
                         atomic_load_explicit(&server.stat_tls_rec_conns, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_keyupdates_in, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_keyupdates_out, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_fatal, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_closes, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_verified, memory_order_relaxed),
                         atomic_load_explicit(&server.stat_tls_rec_mismatch, memory_order_relaxed));
    /* RL-g1f-owner-evalsha: scripts run on owners, runs that aborted to main, calls an owner did not
     * have the script for (or had it marked for main). */
    long long s_runs = 0, s_aborts = 0, s_misses = 0;
    for (int i = 0; i < own_n; i++) {
        s_runs += owners[i].script_runs;
        s_aborts += owners[i].script_aborts;
        s_misses += owners[i].script_misses;
    }
    *info = sdscatprintf(*info, "owner_script_runs:%lld\r\nowner_script_aborts:%lld\r\nowner_script_misses:%lld\r\n",
                         s_runs, s_aborts, s_misses);
    /* RL-g2f: adaptive spinning. owner_spin_owners = owners allowed to spin (pinned, not on main's CPU);
     * waits = times an owner waited for work, spins / hits = idle spins and those that found work;
     * stop_spins / hits / sleeps = waits for main's stop to end; resume_broadcasts = resumes that had to
     * wake sleeping owners (main's time in those wakes: resume_broadcast_us). */
    long long sp = 0, sph = 0, spt = 0, wts = 0, ssp = 0, ssph = 0, ssl = 0;
    int spok = 0;
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        spok += o->spin_ok;
        sp += o->spins;
        sph += o->spin_hits;
        spt += o->spin_time_us;
        wts += o->waits;
        ssp += o->stop_spins;
        ssph += o->stop_spin_hits;
        ssl += o->stop_sleeps;
    }
    *info = sdscatprintf(*info,
                         "owner_spin_us:%d\r\nowner_spin_stop_us:%d\r\nowner_spin_owners:%d\r\nowner_waits:%lld\r\n"
                         "owner_spins:%lld\r\nowner_spin_hits:%lld\r\nowner_spin_time_us:%lld\r\n"
                         "owner_stop_spins:%lld\r\nowner_stop_spin_hits:%lld\r\nowner_stop_sleeps:%lld\r\n"
                         "owner_resume_broadcasts:%lld\r\nowner_resume_broadcast_us:%lld\r\n",
                         server.owner_spin_us, server.owner_spin_stop_us, spok, wts, sp, sph, spt, ssp, ssph, ssl,
                         own_resume_broadcasts, own_resume_broadcast_us);
    *info = sdscatprintf(*info, "owner_repl_native:%s\r\nowner_repl:%s\r\nowner_repl_ops:%lld\r\nowner_repl_bytes:%lld\r\n",
                         server.owner_repl_native ? "yes" : "no", own_repl ? "yes" : "no", own_repl_ops, own_repl_bytes);
    /* RL-g7b: how owners got own_aof_mu at the end of their windows (ownerAofLock) */
    *info = sdscatprintf(*info, "owner_drain_lock_takes:%lld\r\nowner_drain_lock_spun:%lld\r\nowner_drain_lock_sleeps:%lld\r\n",
                         atomic_load_explicit(&own_aof_lock_takes, memory_order_relaxed),
                         atomic_load_explicit(&own_aof_lock_spun, memory_order_relaxed),
                         atomic_load_explicit(&own_aof_lock_sleeps, memory_order_relaxed));
    *info = sdscatprintf(*info, "owner_repl_combine:%s\r\nowner_drain_combined:%lld\r\nowner_drain_combines:%lld\r\n",
                         own_combine ? "yes" : "no", atomic_load_explicit(&own_drain_combined, memory_order_relaxed),
                         atomic_load_explicit(&own_drain_combines, memory_order_relaxed));
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
    /* RL-g2c-owner-cron: the owners' clients cron, and main's wakes. owner_world_held_us: the time main held the
     * world (stop to resume); owner_cron_stops / _held_us: stops of iterations no file event woke that ran a cron
     * timer proc (serverCron, clientsTimeProc), i.e. the owners stopped for cron alone. */
    long long cc = 0, cs = 0, ct = 0, ccl = 0;
    for (int i = 0; i < own_n; i++) {
        cc += owners[i].cron_clients;
        cs += owners[i].cron_slices;
        ct += owners[i].cron_timeouts;
        ccl += owners[i].cron_closes;
    }
    *info = sdscatprintf(*info,
                         "owner_cron:%s\r\nowner_cron_clients:%lld\r\nowner_cron_slices:%lld\r\nowner_cron_timeouts:%lld\r\n"
                         "owner_cron_obuf_closes:%lld\r\nowner_world_held_us:%lld\r\nowner_cron_stops:%lld\r\n"
                         "owner_cron_stop_held_us:%lld\r\n",
                         atomic_load_explicit(&own_cron, memory_order_relaxed) ? "yes" : "no", cc, cs, ct, ccl,
                         own_held_us, own_cron_stops, own_cron_held_us);
    /* RL-g2m: world stops by cause (cron alone, an owner's wake, another file event, other) and the held-time
     * histogram (owner_world_held_hist: counts of [2^b, 2^(b+1)) us, b = 0..19). */
    for (int k = 0; k < OWN_NCAUSES; k++)
        *info = sdscatprintf(*info, "owner_stop_%s:stops=%lld,held_us=%lld\r\n", own_cause_names[k], own_cause_stops[k],
                             own_cause_held_us[k]);
    *info = sdscatprintf(*info, "owner_world_held_max_us:%lld\r\nowner_world_held_hist:", own_held_max_us);
    for (int b = 0; b < OWN_HELD_BUCKETS; b++) *info = sdscatprintf(*info, "%s%lld", b ? "/" : "", own_held_hist[b]);
    *info = sdscat(*info, "\r\n");
    /* RL-g3b-light-cron: main iterations run without the world (owner_light_iterations), those of them that had to
     * stop it and why (owner_light_full: a step that needed owner state), and timer wakes that could not start light
     * and why (owner_light_refused); the expire bound; the owners' table upkeep. */
    long long tchk = 0, trh = 0;
    for (int i = 0; i < own_n; i++) {
        tchk += owners[i].tbl_checked;
        trh += owners[i].tbl_rehash_us;
    }
    long long bound = ownerExpireBound();
    *info = sdscatprintf(*info, "owner_light_cron:%s\r\nowner_light_iterations:%lld\r\nowner_light_full:%lld\r\n",
                         server.owner_light_cron ? "yes" : "no", own_light_iters, own_light_full);
    *info = sdscatfmt(*info, "owner_light_full_why:");
    for (int k = 0; k < OWN_FULL_NCOUNT; k++)
        *info = sdscatprintf(*info, "%s%s=%lld", k ? "," : "", own_full_names[k], own_light_full_why[k]);
    *info = sdscatfmt(*info, "\r\nowner_light_refused:");
    for (int k = 0; k < OWN_NOLIGHT_NCOUNT; k++)
        *info = sdscatprintf(*info, "%s%s=%lld", k ? "," : "", own_nolight_names[k], own_nolight[k]);
    *info = sdscatprintf(*info,
                         "\r\nowner_expire_bound_ms:%lld\r\nowner_tables:%s\r\nowner_tables_checked:%lld\r\n"
                         "owner_tables_rehash_us:%lld\r\n",
                         bound == LLONG_MAX ? -1LL : bound,
                         atomic_load_explicit(&own_tables, memory_order_relaxed) ? "yes" : "no", tchk, trh);
    /* RL-g2p-busy-poll: owner_busy_poll_owners = owners whose epoll set busy polls now (read back with
     * EPIOCGPARAMS); owner_epoll_waits = epoll_waits that could block. With the option on, also the owner
     * threads' sleeps (voluntary context switches, /proc; they also sleep on main's stop) and the
     * kernel's TcpExt BusyPollRxPackets (packets busy polling took off a queue, this network namespace;
     * on loopback 0). Waits that found work without sleeping: about owner_epoll_waits - owner_sleeps. */
    long long waits = 0, sleeps = -1, bprx = -1;
    int bp_on = 0, bp_ok = 0;
    for (int i = 0; i < own_n; i++) {
        waits += owners[i].epoll_waits;
        bp_ok += owners[i].bp_ok;
    }
    if (server.owner_busy_poll_us) {
        sleeps = 0;
        for (int i = 0; i < own_n; i++) {
            owner *o = &owners[i];
            struct epoll_params p = {0};
            if (o->bp_on && ioctl(o->epfd, EPIOCGPARAMS, &p) == 0 && p.busy_poll_usecs) bp_on++;
            sleeps += ownerSleeps(o);
        }
        bprx = ownerBusyPollRxPackets();
    }
    *info = sdscatprintf(*info,
                         "owner_busy_poll_us:%d\r\nowner_busy_poll_budget:%d\r\nowner_busy_poll_prefer:%s\r\n"
                         "owner_busy_poll_owners:%d\r\nowner_busy_poll_may:%d\r\nowner_busy_poll_errors:%lld\r\n"
                         "owner_busy_poll_errno:%d\r\nowner_epoll_waits:%lld\r\nowner_sleeps:%lld\r\n"
                         "owner_busy_poll_rx_packets:%lld\r\n",
                         server.owner_busy_poll_us, server.owner_busy_poll_budget,
                         server.owner_busy_poll_prefer ? "yes" : "no", bp_on, bp_ok, own_bp_errors,
                         own_bp_errno, waits, sleeps, bprx);
    for (int i = 0; i < own_n; i++) {
        owner *o = &owners[i];
        *info = sdscatprintf(*info, "owner%d:port=%d,cpu=%d,slots=%d-%d,clients=%d,native=%lld,handoffs=%lld\r\n", i,
                             o->port, o->cpu, o->slot_lo, o->slot_hi, o->nclients, o->native_total, o->handoffs_total);
    }
}
