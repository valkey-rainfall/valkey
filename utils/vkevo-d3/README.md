# vkevo round 2, design D3: owner threads (one process, port per core, main-path fallback)

Engine change in `src/owner.c` (hooks in networking.c, server.c, db.c, cluster.c, config.c).

- `owner-threads N`: owner i owns about 16384/N slots, `[16*floor(1024*i/N), 16*floor(1024*(i+1)/N))`,
  of every db (the keyspace has 1024 tables of 16 slots; owners own whole tables) and serves the
  clients of port `owner-port + i` (default 6400) on its own epoll loop, pinned to the i-th CPU of
  `owner-cpulist`. Needs `io-threads 1` at startup (another io-threads, keyspace-shard-bits or
  cluster mode switches owners off). `CONFIG SET io-threads N>1` at runtime puts the owners to
  sleep (main takes their clients, CF's staged loop serves them); back at 1 they wake.
- Native on the owner, no barrier, no cross-core hop: GET, SET (all options), DEL with one key,
  INCR, PING, when the key's slot is the owner's own.
- Everything else falls back to the main path: the client (socket, parsed input, unsent replies)
  moves to main, runs as a stock client with the owners stopped, and goes home when idle. That
  covers another owner's key, multi-key commands, MULTI/EXEC, Lua, blocking, pub/sub, admin.
- Main holds the world whenever it runs (afterSleep to the end of beforeSleep); owners run only
  while main sleeps. One process: one replication stream, one AOF, one global maxmemory.
- Replicas, maxmemory-clients, MONITOR, keyspace events, tracking, WATCH, keys with blocked clients,
  client pause, write-observing modules, latency monitor, hotkeys, forkless save: while any is on,
  owners hand every command to main (stock behavior, main-path speed).
- D3veto: `appendonly yes` and `maxmemory` stay native. AOF: each native command's propagated ops
  are kept per owner and replayed into the AOF buffer by main when it stops the world, before it
  runs anything; an owner's replies wait (`owner_held_replies`) until main's beforeSleep AOF write
  (fsync under `always`). maxmemory: a native read/command/resize runs only if main's last reading
  of used memory + what the owner added since (its own zmalloc counter) + a bound for the step
  stays under maxmemory, split per owner; under 2 MB of headroom per owner, everything goes to main,
  which evicts or refuses as stock (`owner_maxmemory_to_main` counts single refusals).
- `CLUSTER SLOTS|SHARDS|NODES|INFO|MYID|KEYSLOT` on such a server announce the
  owners as cluster nodes (IP = `owner-announce-ip`, else the address the client connected to).
  D3 never answers MOVED or CROSSSLOT. Port 6379 stays the stock main path.
- `INFO owners`: native commands, handoffs to main, returns, world stops and stop wait, per owner.
- `owner-adopt-all yes` (hidden, tests): every TCP client is served by owner `id % N`.

Files here:

- `d3-launch.sh`: start/stop/status one server at a given W (owners on CPUs cpu0..cpu0+W-1, main
  and bio on cpu0).
- `d3-steer.sh` (root): ENA ntuple rule per owner port (`dst-port 6400+i -> queue i mod NQ`) and XPS
  by RX queue. Same mechanism as D1's d1-steer.sh. **Run `restore` after the D3 cells.**
- `cachecannon-d3.toml.tmpl`: cachecannon `cluster = true`, seed any owner port.
- `d3-check.py`: the correctness gate (topology, native vs fallback, MULTI atomic + isolated, WATCH,
  reply order, linearizability across paths, plain port, vetoes, expiry, stats, no crash).
- `d3veto-check.py`: AOF and maxmemory with native owners; starts its own loopback servers
  (`--bin --dir --n`): replies after the AOF write (DEBUG AOF-FLUSH-SLEEP), round-1 aoftest (every
  reply backed by the AOF file), replay vs a model (DEBUG LOADAOF, restart, BGREWRITEAOF), WAITAOF,
  eviction limit and key accounting, noeviction refusals, used_memory vs the plain path.

Bench host (W worker CPUs 0..W-1, NIC IRQs on 64-95):

```
d3-launch.sh start --bin ~/d3/src/valkey-server --w $W --bind "198.51.100.17 127.0.0.1" --announce-ip 198.51.100.17
sudo d3-steer.sh apply --n $W            # then the cells (cachecannon cluster = true, seed 198.51.100.17:6400)
sudo d3-steer.sh verify --secs 5         # during a cell: only queues 0..min(W,32)-1 carry packets
python3 d3-check.py --host 198.51.100.17 --port 6379 --owner-port 6400 --n $W --after-load
d3-launch.sh status                      # owner_handoffs_to_main stays ~ connection count: routing is right
sudo d3-steer.sh restore; d3-launch.sh stop
```

Limits (gen 0): the native set is GET/SET/DEL(1)/INCR/PING; every main iteration stops all owners
(fine while main is mostly idle; a busy 6379 or many cross-owner commands stop them often); a
fallback client returns home as soon as it is idle (no hysteresis); native commands are not
counted in CLUSTER SLOT-STATS; while a module is loaded, owners hand every command to main, but a
module's own GIL threads are not excluded from the owners' socket work.
