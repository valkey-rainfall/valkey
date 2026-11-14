# vkevo round 2, design D3: owner threads (one process, port per core, main-path fallback)

Engine change in `src/owner.c` (hooks in networking.c, server.c, db.c, cluster.c, config.c).

- `owner-threads N`: owner i owns about 16384/N slots, `[16*floor(1024*i/N), 16*floor(1024*(i+1)/N))`,
  of every db (the keyspace has 1024 tables of 16 slots; owners own whole tables) and serves the
  clients of port `owner-port + i` (default 6400) on its own epoll loop, pinned to the i-th CPU of
  `owner-cpulist`. Needs `io-threads 1` at startup (another io-threads, keyspace-shard-bits or
  cluster mode switches owners off). `CONFIG SET io-threads N>1` at runtime puts the owners to
  sleep (main takes their clients, CF's staged loop serves them); back at 1 they wake.
- Native on the owner, no barrier, no cross-core hop: GET, SET (all options), DEL, INCR, MGET,
  MSET, PING, when every key's slot is the owner's own (multi-key commands: one slot, hash-tagged).
  MULTI/EXEC/DISCARD too, while every queued command is one of those on the owner's keys (EXEC runs
  inside one owner window: atomic and isolated), and WATCH/UNWATCH of the owner's keys (each owner
  keeps its own watched-key registry; a watcher that falls back becomes a stock watcher on main).
- Everything else falls back to the main path: the client (socket, parsed input, transaction so
  far, unsent replies) moves to main, runs as a stock client with the owners stopped, and goes home
  when idle. That covers another owner's key, keys in two slots, any other command inside a
  transaction, EXEC with conditions, Lua, blocking, pub/sub, admin. No cross-owner locking.
- Main holds the world whenever it runs (afterSleep to the end of beforeSleep); owners run only
  while main sleeps. One process: one replication stream, one AOF, one global maxmemory.
- Replicas, maxmemory-clients, MONITOR, keyspace events, tracking, WATCH through main, keys with blocked clients,
  client pause, write-observing modules, latency monitor, hotkeys, forkless save: while any is on,
  owners hand every command to main (stock behavior, main-path speed).
- D3veto: `appendonly yes` and `maxmemory` stay native. AOF: each native command's propagated ops
  are kept per owner and replayed into the AOF buffer by main when it stops the world, before it
  runs anything; an owner's replies wait (`owner_held_replies`) until main's beforeSleep AOF write
  (fsync under `always`). maxmemory: a native read/command/resize runs only if main's last reading
  of used memory + what the owner added since (its own zmalloc counter) + a bound for the step
  stays under maxmemory, split per owner; under 2 MB of headroom per owner, everything goes to main,
  which evicts or refuses as stock (`owner_maxmemory_to_main` counts single refusals).
- D3-all (D3-g2 + D3mk + D3veto): a native EXEC under AOF keeps its queued commands' ops as one unit
  (MULTI/EXEC-wrapped when more than one, as the stock EXEC); under maxmemory MGET and EXEC are bounded
  by the stored values they can return, and owner GETs run one by one (no D3-g2 GET ring).
- D3-all-g2 (MGET run): an owner answers the client's current own-slot MGET and the own-slot MGETs
  queued behind it as one run through the GET ring: up to `prefetch-batch-max-size` key finds in
  flight across the MGETs (a whole MGET always fits), and each MGET's reply is gathered in one pass
  from its finds into one buffer (one `addReplyProto`, no second lookup). An MGET with a TTL key, a
  non-string, a reply over 4 KB or a large-request/-reply commandlog hit runs `mgetCommand` as before;
  cross-owner and two-slot MGETs still go to main. Off under maxmemory, like the GET ring.
- D3-all-g3 (D3-all-g2-parse + D3-all-g2-mget): an owner's MGET of up to 32 keys reaches the
  MGET run through the frame template (parseOwnerFrame builds the argv the run reads its keys
  from); a longer one through parseMultibulk, as before.
- D3-all-g4-k1 (argument recycling): a GET run (or an MGET run of one-key MGETs) gives each consumed
  argv back whole, its key object still in slot 1 (parseRecycleKeyArgv), and the GET frame template
  takes one back in one pop and rewrites only the key bytes (parsePopKeyArgv); one key length at a
  time, other argvs go back piece by piece as before. The recycler finds an argument's bytes inline.
- D3-all-g5 (D3-all-g4-k1 + D3-all-g4-aof): both, unchanged. A key argv is kept whole only when
  its key's refcount is 1 (parseArgReusable), so a key an AOF op still holds (a lazy-expire DEL
  of a GET key) goes back piece by piece; the owner's AOF records are bytes, not references.
- D3-all-g6-aoftail: under appendfsync everysec an owner's AOF drain also starts the background
  fsync once 32 MB are written and not fsynced and none runs (aofBackgroundFsyncIfDue), so each
  fsync, and the writes the stock loop postpones behind it, stay short at high write rates; below
  32 MB a second nothing changes. Write policy and reply release unchanged; never fsynced later.
- D3-all-g8-k1 (GET bursts): a read that starts with GET frames answers them before anything is
  parsed (ownerGetBurst): each `*2 $3 GET|get $n key` frame (key 1-64 B) is matched in the query
  buffer, its key copied into a GET-ring entry as an sds and its find started; replies (bulk or null)
  are formatted straight into the client's reply buffer, in order; no argv objects, no command lookup,
  no per-command reset; one run's commandstats / latency / slow-log accounting per burst of up to 256
  GETs (slow-log entries rebuilt from the frames). The burst stops before the first frame the GET ring
  would not answer (another spelling or command, a key with an expire, a non-string, another owner's
  key, a large-request or large-reply log entry, a partial frame) and the parser takes the rest as
  before. Off for a client in MULTI, AUTH-pending or with a key-pattern ACL user, under CLIENT REPLY
  OFF/SKIP, ASKING or CACHING, under maxmemory, when every native command is vetoed, and when
  `rename-command` moved GET. A burst starts only where a read starts at a frame (a pipeline that
  fits one read); a read that starts inside a frame parses as before. `INFO owners`:
  `owner_burst_commands`, `owner_bursts`. The GET and MGET runs and the bursts now answer a value
  whose object carries an expire field with no expire in it (an embedded value with room for the
  field, or a PERSISTed key); only a real expire goes to getCommand / mgetCommand.
- D3-all-g8-aofpace: automatic (growth) AOF rewrites are paced. After a successful rewrite that ran
  T, the next automatic one waits until T * `aof-rewrite-pace` / 100 has passed since it ended
  (default 200: rewriting at most a third of the time; 0 = upstream). The growth base includes what
  was written during the rewrite, so a faster rewrite only meant more rewrites, each a fork plus a
  copy-on-write of the whole dataset at millions of SETs/s. Pacing yields once growth reaches
  2 * `auto-aof-rewrite-percentage` + 100. BGREWRITEAOF, scheduled rewrites and the failure limiter
  are unchanged; every write is still in the AOF and fsynced on its policy. `INFO persistence`:
  `aof_rewrites_paced`, `aof_rewrite_pace_ms`, `aof_last_rewrite_ms`.
- D3-all-g8-iouring (`owner-uring yes`, the default; hidden, CONFIG SET at runtime): each owner has
  its own io_uring (liburing; `make USE_LIBURING=no` leaves it out). epoll still reports the ready
  sockets; the owner reads them 64 per `io_uring_enter` (recv, MSG_DONTWAIT), runs them in event
  order, then sends their replies 64 per `io_uring_enter` (plain `c->buf` bytes; replies with reply
  blocks or value references use writev as before). Held AOF replies go out the same way. Every op
  completes inside the call that submits it, so nothing is in flight outside an owner window; a recv
  or send that moved no bytes is redone by the stock read/write. Reads are not batched under
  maxmemory. A client whose last read ran `owner-uring-deep` (hidden, default 32) commands or more
  skips the batch (read, run, reply at once): deep pipelines already share their syscalls, and
  batching them cost W=16 K1 2-3% on loopback. `INFO owners`: `owner_uring`, `owner_uring_rings`,
  `owner_uring_enters|reads|writes`,
  `owner_uring_short_writes` (sends that moved less than asked), `owner_uring_redone` (ops redone by
  the syscall path).
- `CLUSTER SLOTS|SHARDS|NODES|INFO|MYID|KEYSLOT` on such a server announce the
  owners as cluster nodes (IP = `owner-announce-ip`, else the address the client connected to).
  D3 never answers MOVED or CROSSSLOT. Port 6379 stays the stock main path.
- `INFO owners`: native commands, handoffs to main, returns, world stops and stop wait, per owner.
- `owner-adopt-all yes` (hidden, tests): every TCP client is served by owner `id % N`.

Files here:

- `d3-launch.sh`: start/stop/status one server at a given W (owners on CPUs cpu0..cpu0+W-1, main on
  cpu0; the bio thread and the AOF-rewrite and RDB children on `--bg-cpu`, default 63 = the bench host's
  OS core, outside the owners, where D0's bench server has the same three).
- `d3-steer.sh` (root): ENA ntuple rule per owner port (`dst-port 6400+i -> queue i mod NQ`) and XPS
  by RX queue. Same mechanism as D1's d1-steer.sh. **Run `restore` after the D3 cells.**
- `cachecannon-d3.toml.tmpl`: cachecannon `cluster = true`, seed any owner port.
- `d3-check.py`: the correctness gate (topology, native vs fallback, MULTI atomic + isolated, WATCH,
  reply order, linearizability across paths, plain port, vetoes, expiry, stats, no crash).
- `d3-multikey-check.py` (D3mk): native single-owner MGET/MSET/DEL/MULTI/WATCH vs a model, partial
  groups never visible under writers on three paths, MULTI INCR pairs and WATCH CAS across paths.
- `d3veto-check.py`: AOF and maxmemory with native owners; starts its own loopback servers
  (`--bin --dir --n`): replies after the AOF write (DEBUG AOF-FLUSH-SLEEP), round-1 aoftest (every
  reply backed by the AOF file), replay vs a model (DEBUG LOADAOF, restart, BGREWRITEAOF), WAITAOF,
  eviction limit and key accounting, noeviction refusals, used_memory vs the plain path.
- `d3all-check.py` (D3-all): where the merged features meet; starts its own loopback servers like
  d3veto-check: native EXEC in the AOF (MULTI/EXEC-wrapped as the stock EXEC, replies after the AOF
  write, replay/LOADAOF/restart digests), GET-ring and MGET replies backed by the AOF, MGET/EXEC
  maxmemory bounds, eviction and noeviction under native transactions.
- `d3-mget-check.py` (D3-all-g2): the owner MGET run vs a model over ring sizes, raw reply bytes equal
  to the plain port's in RESP2 and RESP3, MGET commandstats / keyspace hits and misses / native counts,
  slow, large-reply and large-request commandlogs, touch and CLIENT NO-TOUCH.
- `d3-parse-check.py` (D3-all-parse): the owner frame templates (GET/MGET) against main's parser:
  the same bytes to every owner port and to the plain port (spellings, split reads, MGET 1-40 keys,
  handoffs, 40 KB key, malformed frames, large-request input bytes), reply for reply.
- `d3-recycle-check.py` (D3-all-g4-k1): the owner's argument recycling against main: own-slot
  pipelines of 1-400 GET/GeT/MGET/SET/INCR frames over key lengths 1-90 (runs and random, keys that
  differ in one byte), reply for reply with the plain port and all native; used_memory flat (< 256 KB)
  over ~450 K recycled GET/MGET-of-one frames per owner.
- `d3aof-check.py` (D3-all-aof): owner-formatted AOF records against the parent binary (`--ref-bin`):
  the same incr AOF bytes, replies and offset on a sequential workload, offset == records, mixed-db
  owner buffers vs a model after LOADAOF and restart, timestamps. `--fsync everysec` runs the
  sequential parts on the owners' own AOF writes (D3-all-g4-aof).
- `d3aofrace-check.py` (D3-all-g4-aof): owners write the AOF themselves (everysec, no) while the
  others run: every reply backed by the AOF files (base + incr, across 3 mid-run BGREWRITEAOFs) under
  appendfsync no, concurrent multi-db writers + plain-port and handoff writers + rewrites + an
  everysec/always toggle vs a model live, after LOADAOF and after restart, WAITAOF; `--bench` adds a
  valkey-benchmark SET load on every owner port; part `pace-everysec` checks the owners' fsync
  pacing with WAITAOF waits under a 4 KB SET load (D3-all-g6-aoftail). `d3veto-check.py` and `d3all-check.py` take
  `--fsync no` to run their AOF parts (aoftest, ring_backed) on the same path.
- `d3-burst-check.py` (D3-all-g8-k1): the owners' GET bursts against the parent's owner path
  (`--ref-bin`), both servers started here on loopback: the same bytes to owner 0 of each (key lengths
  1-70, values 0 B-1 MB, INT values, three spellings, 300 and 2000 frames, split reads, single GETs,
  mixed commands, expires, wrong type, another owner's key, RESP3, CLIENT REPLY SKIP/OFF, MULTI, a
  key-pattern ACL user, rename-command), reply bytes, keyspace hits/misses and cmdstat_get calls equal;
  slow, large-request and large-reply commandlog entries equal.
- `d3uring-check.py` (D3-all-g8-iouring): the owners' io_uring path on a running server, with
  owner-uring yes and no: the batched reads/writes are used, pipelines split at random byte offsets
  on concurrent connections reply for reply, a client with a 4 KB receive buffer that reads nothing
  for 1 s (partial sends, EAGAIN, EPOLLOUT), 1 MB and 8 MB arguments in pieces, 200 abrupt closes
  (half RST), and CONFIG SET owner-uring flipped every 20 ms under pipelines.

Bench host (W worker CPUs 0..W-1, bio + fork children on the OS core 63, NIC IRQs on 64-95):

```
d3-launch.sh start --bin ~/d3/src/valkey-server --w $W --bind "198.51.100.17 127.0.0.1" --announce-ip 198.51.100.17
sudo d3-steer.sh apply --n $W            # then the cells (cachecannon cluster = true, seed 198.51.100.17:6400)
sudo d3-steer.sh verify --secs 5         # during a cell: only queues 0..min(W,32)-1 carry packets
python3 d3-check.py --host 198.51.100.17 --port 6379 --owner-port 6400 --n $W --after-load
d3-launch.sh status                      # owner_handoffs_to_main stays ~ connection count: routing is right
sudo d3-steer.sh restore; d3-launch.sh stop
```

Limits (gen 0, native set widened in D3mk-g1 as above): every main iteration stops all owners
(fine while main is mostly idle; a busy 6379 or many cross-owner commands stop them often); a
fallback client returns home as soon as it is idle (no hysteresis); native commands are not
counted in CLUSTER SLOT-STATS; while a module is loaded, owners hand every command to main, but a
module's own GIL threads are not excluded from the owners' socket work.
