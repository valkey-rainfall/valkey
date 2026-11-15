#!/usr/bin/env python3
"""D3-all-g11-repl check: owners stay native with replicas attached (owner.c ownerAofAppend feeds the owners'
records into the replication stream). Every server binds 127.0.0.1 only.

Each suite starts a primary of --bin with owner-threads N (owner ports OP..OP+N-1) and 1-2 replicas (--replica-bin,
default --bin, no owner threads), runs random command streams on several connections at once (cluster-routed to the
owner ports, plus the plain port): native SET / SET PX / GET / DEL / INCR / MGET / MSET / MULTI..EXEC on one hash tag,
and fallbacks (cross-owner MSET / DEL, APPEND, EXPIRE / PEXPIRE, HSET, RPUSH, SET with a short PX whose keys expire for
real and are deleted lazily by owners and actively by main), then quiesces: every key is touched (EXISTS) so expired
ones are deleted and propagated, WAIT for every replica, and checks:
  .veto / .native     owner_veto none and owner_repl yes with replicas attached; the load ran natively
  .digest / .dbsize   every replica's DEBUG DIGEST and DBSIZE equal the primary's
  .offset             master_repl_offset = every replica's slave_repl_offset (and the primary's slaveN offset)
  .wait               WAIT counts: all replicas, then one replica stopped: one fewer (timeout), right after native
                      SETs on an owner connection (WAIT goes to main, the SETs' offsets come from the owner records)
Suites (fresh servers each):
  base      2 replicas; load; checks
  psync     1 replica; mid-load CLIENT KILL TYPE replica: partial resync (sync_partial_ok +1, sync_full +0); mid-load
            replica SHUTDOWN (saves its RDB) + restart: partial resync from the backlog; mid-load kill -9 + restart on
            an empty dir: full resync while owners run; a NEW replica started mid-load (full sync while owners
            run); backlog only (no replica) while the load runs: owners stay native, a reconnect is partial
  obuf      client-output-buffer-limit replica 256kb 0 0 under load: the replica is dropped (an owner's feed closes
            it, main frees it), reconnects; after the limit is restored and the load stops, it converges; no crash
  nosender  repl-stream-sender no: main writes the replica sockets (owners wake main); converges
  aof       appendonly yes everysec + 1 replica: converges; DEBUG LOADAOF on the primary keeps its digest
  aofalways appendonly yes always + 1 replica: converges
  evict     maxmemory 48mb allkeys-random: evictions (DELs from main) reach the replica; converges
  mm        maxmemory 1gb allkeys-lru (native within the headroom) + 1 replica: converges
  uringaof  appendonly everysec + owner-uring-aof yes + 1 replica: converges
  diskless  repl-diskless-sync no (disk RDB for the full sync); replica attached mid-load
  off       owner-repl-native no (owners vetoed with replicas, the main path): converges (reference behaviour)
  model     the same deterministic per-connection streams (each connection its own keys, no short TTLs) on this
            binary with owners native, on this binary with owner-repl-native no, and on --ref-bin (D3-all-g10, the
            parent: replicas veto owners) if given: the primaries' and replicas' digests are the same on all
Usage: d3-repl-check.py --bin B [--ref-bin R] [--replica-bin B] --dir D --port P --owner-port OP [--n 4]
                        [--cpus 1-4] [--main-cpu 0] [--replica-cpus 5-6] [--only suite,...] [--secs 6]
Prints PASS/FAIL/INFO lines and REPLCHECK {checks, fails}; exit 1 on any FAIL.
"""
import argparse
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS

A = None


def info(c, sec):
    return d3.info(c, sec)


def ostat(c):
    return d3.ostat(c)


class Proc:
    def __init__(self, name, binary, port, d, args, cpus):
        self.name, self.port, self.binary, self.cpus = name, port, binary, cpus
        self.dir = os.path.join(d, name)
        self.args = args
        self.p = None
        self.start(fresh=True)

    def start(self, fresh=False):
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        cmd = ["taskset", "-c", self.cpus, self.binary, "--port", str(self.port), "--bind", "127.0.0.1", "--io-threads",
               "1", "--save", "", "--appendonly", "no", "--enable-debug-command", "yes", "--dir", self.dir,
               "--logfile", os.path.join(self.dir, "log")] + self.args
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(400):
            try:
                c = d3.Conn("127.0.0.1", self.port, timeout=5)
                if c.cmd("PING") in ("PONG", b"PONG"):
                    c.close()
                    break
                c.close()
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        self.c = d3.Conn("127.0.0.1", self.port, timeout=120)

    def stop(self, nosave=True):
        try:
            self.c.cmd("SHUTDOWN", "NOSAVE" if nosave else "SAVE")
        except Exception:
            pass
        try:
            self.p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()

    def kill9(self):
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()

    def crashed(self):
        try:
            t = open(os.path.join(self.dir, "log"), errors="replace").read()
        except OSError:
            return 0
        return sum(t.count(w) for w in ("ASSERT", "crashed by signal", "Guru", "BUG REPORT"))

    def logtext(self):
        try:
            return open(os.path.join(self.dir, "log"), errors="replace").read()
        except OSError:
            return ""


class Primary(Proc):
    def __init__(self, name, binary, d, extra=()):
        args = ["--owner-threads", str(A.n), "--owner-port", str(A.owner_port), "--owner-cpulist", A.cpus,
                "--server-cpulist", str(A.main_cpu), "--repl-backlog-size", "16mb"] + list(extra)
        super().__init__(name, binary, A.port, d, args, "%s,%s" % (A.main_cpu, A.cpus))
        self.ranges = []
        raw = self.c.cmd("INFO", "owners")
        raw = raw.decode() if isinstance(raw, bytes) else raw
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))
        self.c.cmd("CONFIG", "SET", "repl-ping-replica-period", "1")

    def oport(self, key):
        s = d3.key_slot(key if isinstance(key, bytes) else key.encode())
        for i, (lo, hi) in enumerate(self.ranges):
            if lo <= s <= hi:
                return A.owner_port + i
        return A.port


def replica(name, idx, d, extra=()):
    return Proc(name, A.replica_bin, A.port + 1 + idx, d,
                ["--replicaof", "127.0.0.1", str(A.port), "--repl-diskless-load", "disabled"] + list(extra),
                A.replica_cpus)


def link_up(r, secs=60):
    t0 = time.time()
    while time.time() - t0 < secs:
        try:
            i = info(r.c, "replication")
            if i.get("master_link_status") == "up" and i.get("master_sync_in_progress", "0") == "0":
                return True
        except Exception:
            pass
        time.sleep(0.05)
    return False


def online(p, nrep, secs=60):
    t0 = time.time()
    while time.time() - t0 < secs:
        i = info(p.c, "replication")
        if sum(1 for k, v in i.items() if k.startswith("slave") and "state=online" in v) >= nrep:
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------- workload
def tag_for(p, owner, rnd):
    while True:
        t = "t%d" % rnd.randrange(1 << 30)
        if p.oport(t) == A.owner_port + owner:
            return t


class Loader:
    """nconn connections, each its own thread, random commands until stop; replies only checked for errors that
    should not happen (WRONGTYPE etc. are fine: keys of different types collide on purpose)."""

    def __init__(self, p, nconn=8, seed=1, short_ttl=True, plain_share=0.05, keyspace=4000):
        self.p, self.nconn, self.seed, self.short_ttl, self.plain_share = p, nconn, seed, short_ttl, plain_share
        self.keyspace = keyspace
        self.stop_ev = threading.Event()
        self.counts = [0] * nconn
        self.errors = []
        self.threads = []

    def gen(self, rnd, cid):
        ks = self.keyspace
        k = "k%d" % rnd.randrange(ks)
        r = rnd.random()
        if r < 0.30:
            return [("SET", k, "v%d" % rnd.randrange(1 << 20))]
        if r < 0.40:
            return [("GET", k)]
        if r < 0.45:
            return [("DEL", k)]
        if r < 0.50:
            return [("INCR", "c%d" % rnd.randrange(64))]
        t = "{g%d}" % rnd.randrange(ks // 8)
        if r < 0.55:
            return [("MSET", t + "a", "x%d" % rnd.randrange(999), t + "b", "y")]
        if r < 0.58:
            return [("MGET", t + "a", t + "b")]
        if r < 0.63:
            return [("MULTI",), ("SET", t + "a", "m%d" % cid), ("INCR", t + "n"), ("GET", t + "a"), ("EXEC",)]
        if r < 0.67 and self.short_ttl:
            return [("SET", "e%d" % rnd.randrange(ks), "short", "PX", str(rnd.randrange(1, 40)))]
        if r < 0.70:
            return [("SET", k, "long", "PXAT", "4102444800000")]
        if r < 0.74:
            return [("MSET", k, "x", "k%d" % rnd.randrange(ks), "y")]  # usually cross-owner: main
        if r < 0.77:
            return [("DEL", k, "k%d" % rnd.randrange(ks))]
        if r < 0.80:
            return [("APPEND", "a%d" % rnd.randrange(ks), "z")]
        if r < 0.83 and self.short_ttl:
            return [("PEXPIRE", k, str(rnd.randrange(1, 60)))]
        if r < 0.86:
            return [("EXPIRE", k, "100000")]
        if r < 0.89:
            return [("HSET", "h%d" % rnd.randrange(ks // 4), "f%d" % rnd.randrange(8), "v")]
        if r < 0.92:
            return [("RPUSH", "l%d" % rnd.randrange(ks // 4), "v")]
        if r < 0.95 and self.short_ttl:
            return [("GET", "e%d" % rnd.randrange(ks))]  # lazily expires on the owner
        return [("SET", k, "v%d" % cid)]

    def run(self, cid):
        rnd = random.Random(self.seed * 1000 + cid)
        conns = {}

        def conn(port):
            if port not in conns:
                conns[port] = d3.Conn("127.0.0.1", port, timeout=60)
            return conns[port]

        try:
            while not self.stop_ev.is_set():
                # each group (one command, or MULTI..EXEC) goes to its first key's owner (or the plain port),
                # pipelined per connection
                byport = {}
                for _ in range(rnd.randrange(1, 12)):
                    g = self.gen(rnd, cid)
                    key = g[1][1] if g[0][0] == "MULTI" else g[0][1]
                    port = A.port if rnd.random() < self.plain_share else self.p.oport(key)
                    byport.setdefault(port, []).extend(g)
                for port, batch in byport.items():
                    conn(port).send(batch)
                for port, batch in byport.items():
                    c = conn(port)
                    for cmd in batch:
                        x = c.read()
                        if isinstance(x, d3.Err) and not str(x).startswith(("WRONGTYPE", "ERR value is not an integer")):
                            self.errors.append("%s -> %s" % (cmd[0], x))
                    self.counts[cid] += len(batch)
        except Exception as e:  # noqa
            if not self.stop_ev.is_set():
                self.errors.append("conn %d: %r" % (cid, e))

    def start(self):
        for i in range(self.nconn):
            t = threading.Thread(target=self.run, args=(i,), daemon=True)
            t.start()
            self.threads.append(t)
        return self

    def stop(self):
        self.stop_ev.set()
        for t in self.threads:
            t.join(timeout=60)
        return sum(self.counts)


def quiesce(p, reps, tag, wait_secs=60):
    """touch every key (expired ones are deleted and the DELs propagated), wait for the replicas, compare"""
    time.sleep(0.1)
    cur = 0
    while True:
        cur, keys = p.c.cmd("SCAN", cur, "COUNT", 2000)
        cur = int(cur)
        if keys:
            p.c.send([("EXISTS", k) for k in keys])
            for _ in keys:
                p.c.read()
        if cur == 0:
            break
    n = len(reps)
    w = p.c.cmd("WAIT", n, wait_secs * 1000)
    d3.check(tag + ".wait_all", w == n, "WAIT %d -> %r" % (n, w))
    t0 = time.time()
    while time.time() - t0 < wait_secs:
        po = int(info(p.c, "replication")["master_repl_offset"])
        ro = [int(info(r.c, "replication").get("slave_repl_offset", -1)) for r in reps]
        if all(x == po for x in ro):
            break
        time.sleep(0.05)
    d3.check(tag + ".offset", all(x == po for x in ro), "primary %d replicas %s" % (po, ro))
    pd, pn = p.c.cmd("DEBUG", "DIGEST"), p.c.cmd("DBSIZE")
    for r in reps:
        rd, rn = r.c.cmd("DEBUG", "DIGEST"), r.c.cmd("DBSIZE")
        d3.check("%s.%s.digest" % (tag, r.name), rd == pd, "primary %s %s" % (pd, rd))
        d3.check("%s.%s.dbsize" % (tag, r.name), rn == pn, "primary %s %s" % (pn, rn))
    return pd, pn


def veto_check(p, tag, want_native=True):
    o = info(p.c, "owners")
    ok = (o.get("owner_veto") == "none" and o.get("owner_repl") == "yes") if want_native else o.get("owner_veto") == "all"
    d3.check(tag + ".veto", ok, "owner_veto %s owner_repl %s" % (o.get("owner_veto"), o.get("owner_repl")))


def load_for(p, secs, tag, **kw):
    n0 = ostat(p.c)
    L = Loader(p, **kw).start()
    time.sleep(secs)
    n = L.stop()
    n1 = ostat(p.c)
    nat = n1.get("owner_native_commands", 0) - n0.get("owner_native_commands", 0)
    rops = n1.get("owner_repl_ops", 0) - n0.get("owner_repl_ops", 0)
    d3.check(tag + ".load_errors", not L.errors, L.errors[:3])
    print("INFO %s.load commands %d native %d owner_repl_ops %d" % (tag, n, nat, rops), flush=True)
    return n, nat, rops


def wait_counts(p, reps, tag, want_native=True):
    """WAIT right after native SETs on an owner connection; then one replica stopped"""
    k = "wk%d" % random.randrange(1 << 20)
    c = d3.Conn("127.0.0.1", p.oport(k), timeout=60)
    n0 = ostat(p.c).get("owner_native_commands", 0)
    c.send([("SET", k, "1"), ("SET", k, "2"), ("WAIT", len(reps), 5000)])
    out = [c.read() for _ in range(3)]
    n1 = ostat(p.c).get("owner_native_commands", 0)
    d3.check(tag + ".wait_owner_conn", out == ["OK", "OK", len(reps)] and (n1 - n0 >= 2 or not want_native),
             "%r native +%d" % (out, n1 - n0))
    return c


def suite_base(name, extra=(), nrep=2, secs=None, want_native=True, rextra=()):
    secs = secs or A.secs
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d, extra)
    reps = [replica("r%d" % i, i, d, rextra) for i in range(nrep)]
    d3.check(name + ".online", online(p, nrep) and all(link_up(r) for r in reps))
    veto_check(p, name + ".attached", want_native)
    n, nat, rops = load_for(p, secs, name)
    if want_native:
        d3.check(name + ".native", nat > n // 4 and rops > 0, "native %d of %d, owner_repl_ops %d" % (nat, n, rops))
    quiesce(p, reps, name)
    c = wait_counts(p, reps, name, want_native)
    reps[-1].stop()
    c.send([("SET", "wz", "1"), ("WAIT", len(reps), 300)])
    out = [c.read() for _ in range(2)]
    d3.check(name + ".wait_one_down", out == ["OK", len(reps) - 1], out)
    for x in [p] + reps:
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()
    return p


def stat(p, k):
    return int(info(p.c, "stats").get(k, 0))


def flood(p, prefix, n, size, chunk=500):
    """n SETs of size-byte values on keys prefix<i>, each through its owner's port, pipelined chunk at a time"""
    val = "x" * size
    byport = {}
    for i in range(n):
        k = "%s%d" % (prefix, i)
        byport.setdefault(p.oport(k), []).append(k)
    for port, keys in byport.items():
        c = d3.Conn("127.0.0.1", port, timeout=120)
        for j in range(0, len(keys), chunk):
            part = keys[j:j + chunk]
            c.send([("SET", k, val) for k in part])
            for _ in part:
                c.read()
        c.close()


def suite_psync():
    name = "psync"
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d)
    r = replica("r0", 0, d)
    d3.check(name + ".online", online(p, 1) and link_up(r))
    veto_check(p, name)
    L = Loader(p, seed=7).start()
    time.sleep(1.5)
    # 1. link drop: partial
    f0, p0 = stat(p, "sync_full"), stat(p, "sync_partial_ok")
    p.c.cmd("CLIENT", "KILL", "TYPE", "replica")
    time.sleep(1.5)
    d3.check(name + ".clientkill.partial", link_up(r) and stat(p, "sync_partial_ok") == p0 + 1 and stat(p, "sync_full") == f0,
             "sync_full %d->%d partial_ok %d->%d" % (f0, stat(p, "sync_full"), p0, stat(p, "sync_partial_ok")))
    # 2. replica restart from its own RDB (SHUTDOWN saves it with the replication id/offset): partial
    f0, p0 = stat(p, "sync_full"), stat(p, "sync_partial_ok")
    r.stop(nosave=False)
    time.sleep(0.5)
    r.start()
    d3.check(name + ".restart.partial", link_up(r) and stat(p, "sync_partial_ok") == p0 + 1 and stat(p, "sync_full") == f0,
             "sync_full %d->%d partial_ok %d->%d" % (f0, stat(p, "sync_full"), p0, stat(p, "sync_partial_ok")))
    # 3. kill -9 and restart on an empty dir: full resync while the owners run
    f0 = stat(p, "sync_full")
    r.kill9()
    time.sleep(0.3)
    r.start(fresh=True)
    d3.check(name + ".kill9.full", link_up(r) and stat(p, "sync_full") == f0 + 1, "sync_full %d->%d" % (f0, stat(p, "sync_full")))
    # 4. a new replica mid-load
    r2 = replica("r1", 1, d)
    d3.check(name + ".newreplica.full", link_up(r2) and online(p, 2))
    time.sleep(1.0)
    veto_check(p, name + ".load")
    L.stop()
    d3.check(name + ".load_errors", not L.errors, L.errors[:3])
    quiesce(p, [r, r2], name + ".a")
    # 5. backlog only: both replicas stopped (saving); owners stay native; restarted, they resync partially
    r.stop(nosave=False)
    r2.stop(nosave=False)
    time.sleep(1.2)
    o = info(p.c, "owners")
    rp = info(p.c, "replication")
    L = Loader(p, seed=8).start()
    time.sleep(1.5)
    o2 = info(p.c, "owners")
    d3.check(name + ".backlog_only.native", rp.get("connected_slaves") == "0" and o2.get("owner_veto") == "none" and
             o2.get("owner_repl") == "yes" and int(o2.get("owner_repl_ops", 0)) > int(o.get("owner_repl_ops", 0)),
             "slaves %s veto %s repl %s ops %s->%s" % (rp.get("connected_slaves"), o2.get("owner_veto"), o2.get("owner_repl"),
                                                      o.get("owner_repl_ops"), o2.get("owner_repl_ops")))
    f0, p0 = stat(p, "sync_full"), stat(p, "sync_partial_ok")
    r.start()
    r2.start()
    ok = link_up(r) and link_up(r2)
    d3.check(name + ".backlog_only.partial", ok and stat(p, "sync_partial_ok") == p0 + 2 and stat(p, "sync_full") == f0,
             "sync_full %d->%d partial_ok %d->%d" % (f0, stat(p, "sync_full"), p0, stat(p, "sync_partial_ok")))
    time.sleep(1.0)
    L.stop()
    d3.check(name + ".load2_errors", not L.errors, L.errors[:3])
    quiesce(p, [r, r2], name + ".b")
    for x in (p, r, r2):
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()


def suite_obuf():
    name = "obuf"
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d)
    r = replica("r0", 0, d)
    d3.check(name + ".online", online(p, 1) and link_up(r))
    r.c.cmd("DEBUG", "SLEEP", "0")
    p.c.cmd("CONFIG", "SET", "client-output-buffer-limit", "replica 256kb 0 0")
    L = Loader(p, seed=11, nconn=12).start()
    # stall the replica so its output buffer grows past the limit: 40 MB of SETs while it sleeps
    r.c.send([("DEBUG", "SLEEP", "3")])
    time.sleep(0.2)
    flood(p, "ob", 10000, 4096)
    r.c.read()
    time.sleep(1.0)
    L.stop()
    d3.check(name + ".load_errors", not L.errors, L.errors[:3])
    log = p.logtext()
    dropped = "overcoming of output buffer limits" in log
    d3.check(name + ".dropped", dropped, "primary log: %s" % ("closed for obuf" if dropped else "no obuf close"))
    p.c.cmd("CONFIG", "SET", "client-output-buffer-limit", "replica 0 0 0")
    d3.check(name + ".reconnected", link_up(r, 60) and online(p, 1, 60))
    quiesce(p, [r], name)
    for x in (p, r):
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()


def suite_evict():
    name = "evict"
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d, ["--maxmemory", "48mb", "--maxmemory-policy", "allkeys-random"])
    r = replica("r0", 0, d)
    d3.check(name + ".online", online(p, 1) and link_up(r))
    # fill past maxmemory with 1 KB values through the owners, then the random load
    flood(p, "big", 80000, 1024)
    ev = stat(p, "evicted_keys")
    d3.check(name + ".evicted", ev > 0, "evicted_keys %d" % ev)
    load_for(p, 2, name)
    quiesce(p, [r], name)
    for x in (p, r):
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()


def suite_aof(fsync):
    name = "aof" if fsync == "everysec" else "aofalways"
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d, ["--appendonly", "yes", "--appendfsync", fsync])
    r = replica("r0", 0, d)
    d3.check(name + ".online", online(p, 1) and link_up(r))
    time.sleep(0.5)
    veto_check(p, name)
    load_for(p, A.secs, name)
    pd, pn = quiesce(p, [r], name)
    p.c.cmd("DEBUG", "LOADAOF")
    d3.check(name + ".loadaof.digest", p.c.cmd("DEBUG", "DIGEST") == pd and p.c.cmd("DBSIZE") == pn)
    for x in (p, r):
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()


def suite_newreplica(name, extra):
    d = os.path.join(A.dir, name)
    p = Primary("primary", A.bin, d, extra)
    r = replica("r0", 0, d)
    d3.check(name + ".online", online(p, 1) and link_up(r))
    L = Loader(p, seed=21).start()
    time.sleep(1.0)
    r2 = replica("r1", 1, d)
    d3.check(name + ".newreplica", link_up(r2) and online(p, 2))
    time.sleep(1.0)
    L.stop()
    d3.check(name + ".load_errors", not L.errors, L.errors[:3])
    quiesce(p, [r, r2], name)
    for x in (p, r, r2):
        d3.check("%s.%s.no_crash" % (name, x.name), x.crashed() == 0, x.crashed())
        x.stop()


# ---------------------------------------------------------------- model: same streams, three servers
def model_streams(p, nconn=8, per=3000, seed=99):
    """per-connection deterministic streams on disjoint keys (connection i only touches keys with prefix c<i>:)"""
    out = []
    for cid in range(nconn):
        rnd = random.Random(seed * 100 + cid)
        cmds = []
        for _ in range(per):
            k = "c%d:k%d" % (cid, rnd.randrange(300))
            t = "{c%d:g%d}" % (cid, rnd.randrange(40))
            r = rnd.random()
            if r < 0.35:
                cmds.append(("SET", k, "v%d" % rnd.randrange(1000)))
            elif r < 0.45:
                cmds.append(("INCR", "c%d:n%d" % (cid, rnd.randrange(8))))
            elif r < 0.50:
                cmds.append(("DEL", k))
            elif r < 0.58:
                cmds.append(("MSET", t + "a", "x%d" % rnd.randrange(99), t + "b", "y"))
            elif r < 0.64:
                cmds += [("MULTI",), ("SET", t + "a", "m"), ("INCR", t + "n"), ("EXEC",)]
            elif r < 0.70:
                cmds.append(("MSET", k, "x", "c%d:k%d" % (cid, rnd.randrange(300)), "y"))
            elif r < 0.75:
                cmds.append(("APPEND", k + ":a", "z"))
            elif r < 0.80:
                cmds.append(("SET", k, "t", "PXAT", "4102444800000"))
            elif r < 0.84:
                cmds.append(("HSET", "c%d:h%d" % (cid, rnd.randrange(20)), "f%d" % rnd.randrange(5), "v"))
            elif r < 0.88:
                cmds.append(("GET", k))
            else:
                cmds.append(("SET", k, "w%d" % cid))
        out.append(cmds)
    return out


def model_run(binary, label, extra, streams):
    d = os.path.join(A.dir, "model-" + label)
    p = Primary("primary", binary, d, extra)
    r = replica("r0", 0, d)
    d3.check("model.%s.online" % label, online(p, 1) and link_up(r))
    errs = []

    def run(cid):
        conns = {}
        cmds = streams[cid]
        i = 0
        while i < len(cmds):
            j = i + 1
            if cmds[i][0] == "MULTI":
                while cmds[j - 1][0] != "EXEC":
                    j += 1
            else:
                j = min(len(cmds), i + random.Random(i).randrange(1, 8))
                # do not split a MULTI block
                for q in range(i, j):
                    if cmds[q][0] == "MULTI":
                        j = q
                        break
                j = max(j, i + 1)
            batch = cmds[i:j]
            key = batch[0][1] if len(batch[0]) > 1 else batch[1][1]
            port = p.oport(key)
            c = conns.setdefault(port, d3.Conn("127.0.0.1", port, timeout=60))
            c.send(batch)
            for cmd in batch:
                x = c.read()
                if isinstance(x, d3.Err) and not str(x).startswith("WRONGTYPE"):
                    errs.append("%s %s" % (cmd[0], x))
            i = j

    n0 = ostat(p.c).get("owner_native_commands", 0)
    ts = [threading.Thread(target=run, args=(i,)) for i in range(len(streams))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    nat = ostat(p.c).get("owner_native_commands", 0) - n0
    d3.check("model.%s.errors" % label, not errs, errs[:3])
    pd, pn = quiesce(p, [r], "model." + label)
    print("INFO model.%s native %d digest %s dbsize %s" % (label, nat, pd, pn), flush=True)
    for x in (p, r):
        d3.check("model.%s.%s.no_crash" % (label, x.name), x.crashed() == 0, x.crashed())
        x.stop()
    return pd, pn, nat


def suite_model():
    # a throwaway primary for the routing table (the same N gives the same ranges on every binary)
    d = os.path.join(A.dir, "model-route")
    p = Primary("primary", A.bin, d)
    streams = model_streams(p)
    p.stop()
    res = {}
    res["native"] = model_run(A.bin, "native", [], streams)
    res["mainpath"] = model_run(A.bin, "mainpath", ["--owner-repl-native", "no"], streams)
    if A.ref_bin:
        res["g10"] = model_run(A.ref_bin, "g10", [], streams)
    d3.check("model.native_ran_natively", res["native"][2] > 1000, "native %d" % res["native"][2])
    d3.check("model.mainpath_vetoed", res["mainpath"][2] == 0, "native %d" % res["mainpath"][2])
    base = res["native"][:2]
    for k, v in res.items():
        d3.check("model.%s_vs_native.digest" % k, v[:2] == base, "%s %s" % (v[:2], base))


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin")
    ap.add_argument("--replica-bin")
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", default="1-4")
    ap.add_argument("--main-cpu", default="0")
    ap.add_argument("--replica-cpus", default="5-6")
    ap.add_argument("--secs", type=float, default=4.0)
    ap.add_argument("--only", default="")
    A = ap.parse_args()
    A.replica_bin = A.replica_bin or A.bin
    os.makedirs(A.dir, exist_ok=True)
    only = set(x for x in A.only.split(",") if x)
    want = lambda s: not only or s in only
    suites = [
        ("base", lambda: suite_base("base")),
        ("psync", suite_psync),
        ("obuf", suite_obuf),
        ("nosender", lambda: suite_base("nosender", ["--repl-stream-sender", "no"], nrep=1)),
        ("aof", lambda: suite_aof("everysec")),
        ("aofalways", lambda: suite_aof("always")),
        ("evict", suite_evict),
        ("mm", lambda: suite_base("mm", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"], nrep=1)),
        ("uringaof", lambda: suite_base("uringaof", ["--appendonly", "yes", "--appendfsync", "everysec",
                                                     "--owner-uring-aof", "yes"], nrep=1)),
        ("diskless", lambda: suite_newreplica("diskless", ["--repl-diskless-sync", "no"])),
        ("off", lambda: suite_base("off", ["--owner-repl-native", "no"], nrep=1, want_native=False)),
        ("model", suite_model),
    ]
    for s, fn in suites:
        if not want(s):
            continue
        t0 = time.time()
        try:
            fn()
        except Exception as e:  # noqa
            import traceback
            traceback.print_exc()
            d3.check(s + ".ran", False, repr(e))
        print("INFO suite %s %.1f s" % (s, time.time() - t0), flush=True)
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("REPLCHECK %s" % {"checks": len(d3.RESULTS), "fails": len(fails)}, flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
