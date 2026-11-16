#!/usr/bin/env python3
"""RL-g2a-cheap-handoff gate: handoff-heavy model check. Starts A = --bin and R = --ref-bin (the parent), both with the
same owner threads, and drives them with the same command streams from many connections at once.

Each connection (thread) has its own keys: 3 hash tags in its home owner's slots and 1 tag in another owner's slots
(cross-owner single-key commands), 8 keys per tag. Most connections use their home owner's port, every fifth one main's
port. Random pipelined batches (1-16 commands) mix native commands (GET SET INCR DEL EXISTS MGET MSET EXPIRE TTL
PERSIST TYPE PING) with non-native ones on the same keys (hashes, lists, zsets, sets, APPEND / STRLEN / GETRANGE /
SETRANGE, EVAL / EVALSHA, OBJECT ENCODING, cross-owner keys, cross-slot MGET, BLPOP on an empty list (blocks 10 ms)
and on a non-empty one, MULTI/EXEC mixing both kinds, CLIENT REPLY SKIP); one batch in ten is sent in two parts 3 ms
apart, split inside a command (the client is handed over with a partial request and stays on main). Every reply is
compared in pipeline order with R's (TTL / PTTL by sign): per-connection reply order and values. Shared counter: every
connection also INCRs one shared key, natively or through EVAL (a handoff when the key is another owner's): on A the
values are unique, increasing per connection, and the final value is the count. At the end: every key's type, value
digest and expire presence equal on A and R; commandstats calls / failed / rejected equal; A's handed-over clients went
home with their replies (owner_returns_with_replies).

Suites (--suite): plain; mm (maxmemory 1gb allkeys-lru); repl (A has a replica: its keyspace equals A's exactly);
aof (appendonly everysec: reload A from its AOF, keyspace equal; handed-over clients take the stock path there);
uringaofrepl (appendonly everysec with owner-uring-aof yes, so owners batch reads under the AOF, plus a replica:
replica exactly equal to A, then A reloaded from its AOF equal). With RL-g4a's counters A must have left windows
early for pending stops in every suite (early_leaves_exercised).
Prints PASS/FAIL lines and 'RLHANDOFF {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import multiprocessing
import os
import random
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS
Conn, Err, check = d3.Conn, d3.Err, d3.check
A = None
LUA_INCR = "return redis.call('incr', KEYS[1])"
LUA_GS = "local v = redis.call('get', KEYS[1]); redis.call('set', KEYS[2], v or 'nil'); return v"


class Srv:
    def __init__(self, name, binary, port, oport, args, cpus, d, fresh=True):
        self.name, self.port, self.oport_base = name, port, oport
        self.dir = os.path.join(d, name)
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        cmd = ["taskset", "-c", cpus, binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", os.path.join(self.dir, "log")] + args
        if oport:
            cmd += ["--owner-port", str(oport)]
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(600):
            try:
                c = Conn("127.0.0.1", port, timeout=5)
                r = c.cmd("PING")
                c.close()
                if r in ("PONG", b"PONG"):
                    break
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        self.c = Conn("127.0.0.1", port, timeout=120)
        for _ in range(600):
            if d3.info(self.c, "persistence").get("loading") == "0":
                break
            time.sleep(0.05)
        self.ranges = []
        raw = self.c.cmd("INFO", "owners")
        raw = raw.decode() if isinstance(raw, bytes) else raw
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))

    def stop(self):
        try:
            self.c.cmd("SHUTDOWN", "NOSAVE")
        except Exception:
            pass
        try:
            self.p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()

    def crashed(self):
        try:
            t = open(os.path.join(self.dir, "log"), errors="replace").read()
        except OSError:
            return 0
        return sum(t.count(w) for w in ("ASSERT", "crashed by signal", "Guru", "BUG REPORT"))


def tags_in(rng, lo, hi, n, prefix):
    out = []
    while len(out) < n:
        t = b"%s%d" % (prefix, rng.randrange(1 << 30))
        if lo <= d3.key_slot(b"{" + t + b"}") <= hi:
            out.append(t)
    return out


def norm(cmd, r):
    n = cmd[0].upper()
    if n in ("TTL", "PTTL") and isinstance(r, int):
        return -1 if r == -1 else (-2 if r == -2 else "pos")
    if isinstance(r, Err):
        return ("ERR", str(r))
    return r


class Gen:
    def __init__(self, rng, home, far, sha):
        self.rng, self.home, self.far, self.sha = rng, home, far, sha
        self.keys = [b"{%s}:%d" % (t, j) for t in home + far for j in range(8)]

    def k(self, far=False):
        tags = self.far if far else self.home
        return b"{%s}:%d" % (self.rng.choice(tags), self.rng.randrange(8))

    def same_tag(self, n):
        t = self.rng.choice(self.home)
        return [b"{%s}:%d" % (t, self.rng.randrange(8)) for _ in range(n)]

    def one(self):
        R = self.rng
        x = R.random()
        k = self.k(far=R.random() < 0.12)
        v = "v%d" % R.randrange(1000)
        if x < 0.10:
            return ("SET", k, v)
        if x < 0.17:
            return ("GET", k)
        if x < 0.21:
            return ("INCR", k)
        if x < 0.23:
            return ("DEL", k)
        if x < 0.25:
            return ("EXISTS",) + tuple(self.same_tag(2))
        if x < 0.27:
            return ("MGET",) + tuple(self.same_tag(3))
        if x < 0.28:
            ks = self.same_tag(2)
            return ("MSET", ks[0], v, ks[1], v + "b")
        if x < 0.30:
            return ("EXPIRE", k, 5000)
        if x < 0.32:
            return ("TTL", k)
        if x < 0.33:
            return ("PERSIST", k)
        if x < 0.35:
            return ("TYPE", k)
        if x < 0.36:
            return ("PING",)
        # non-native on the same keys
        if x < 0.42:
            return ("HSET", k, "f%d" % R.randrange(4), v)
        if x < 0.46:
            return ("HGET", k, "f%d" % R.randrange(4))
        if x < 0.48:
            return ("HINCRBY", k, "n", 1)
        if x < 0.50:
            return ("HGETALL", k)
        if x < 0.54:
            return ("LPUSH", k, v)
        if x < 0.57:
            return ("RPOP", k)
        if x < 0.59:
            return ("LRANGE", k, 0, -1)
        if x < 0.62:
            return ("ZADD", k, R.randrange(100), "m%d" % R.randrange(6))
        if x < 0.64:
            return ("ZRANGE", k, 0, -1, "WITHSCORES")
        if x < 0.66:
            return ("SADD", k, "s%d" % R.randrange(6))
        if x < 0.67:
            return ("SCARD", k)
        if x < 0.69:
            return ("APPEND", k, "ab")
        if x < 0.70:
            return ("STRLEN", k)
        if x < 0.71:
            return ("GETRANGE", k, 0, 3)
        if x < 0.72:
            return ("SETRANGE", k, 2, "zz")
        if x < 0.77:
            return ("EVAL", LUA_INCR, 1, k)
        if x < 0.81:
            return ("EVALSHA", self.sha, 1, k)
        if x < 0.83:
            ks = self.same_tag(2)
            return ("EVAL", LUA_GS, 2, ks[0], ks[1])
        if x < 0.85:
            return ("OBJECT", "ENCODING", k)
        if x < 0.87:
            return ("MGET", self.k(), self.k(far=True))  # two slots: main
        if x < 0.88:
            # rare: a client blocked on a list vetoes the native path for everyone (blocked_clients_by_type)
            if R.random() < 0.02:
                return ("BLPOP", self.k() + b":empty", "0.01")  # blocks on main 10 ms, nil
            return ("LLEN", self.k())
        if x < 0.89:
            if R.random() < 0.05:
                return ("BLPOP", self.k(), "0.01")
            return ("HLEN", self.k())
        if x < 0.92:
            return ("HSET", self.k(far=True), "g", v)
        return ("GET", self.k(far=True))

    def batch(self):
        R = self.rng
        n = R.randrange(1, 17)
        cmds = []
        if R.random() < 0.04:  # a transaction mixing native and non-native commands
            cmds.append(("MULTI",))
            cmds += [c for c in (self.one() for _ in range(R.randrange(1, 6))) if c[0] not in ("BLPOP", "TTL")]
            cmds.append(("EXEC",))
        while len(cmds) < n:
            if R.random() < 0.01 and len(cmds) < n - 1:
                cmds.append(("CLIENT", "REPLY", "SKIP"))
                cmds.append(self.one())
                continue
            cmds.append(self.one())
        return cmds


def expected_replies(cmds):
    """Indexes of the commands that get a reply (CLIENT REPLY SKIP and the command after it do not)."""
    out, skip = [], False
    for i, c in enumerate(cmds):
        if c[0] == "CLIENT" and c[2] == "SKIP":
            skip = True
            continue
        if skip:
            skip = False
            continue
        out.append(i)
    return out


def worker(wid, a, r, out, stop_at, seed, shared):
    rng = random.Random(seed)
    n_own = len(a.ranges)
    hi_owner = wid % n_own
    lo, hi = a.ranges[hi_owner]
    flo, fhi = a.ranges[(hi_owner + 1) % n_own]
    home = tags_in(rng, lo, hi, 3, b"w%d-" % wid)
    far = tags_in(rng, flo, fhi, 1, b"f%d-" % wid)
    on_main = wid % 5 == 4
    pa = a.port if on_main else a.oport_base + hi_owner
    pr = r.port if on_main else r.oport_base + hi_owner
    ca, cr = Conn("127.0.0.1", pa), Conn("127.0.0.1", pr)
    g = Gen(rng, home, far, A.sha)
    diffs, shared_vals = [], []
    n_cmds = n_batches = n_split = 0
    while time.time() < stop_at and len(diffs) < 10:
        cmds = g.batch()
        sh = -1
        if rng.random() < 0.25:  # the shared counter, natively or through a script
            sh = rng.randrange(len(cmds) + 1)
            if sh > 0 and cmds[sh - 1][0] == "CLIENT":
                sh += 1  # after the command whose reply CLIENT REPLY SKIP drops
            cmds.insert(sh, ("INCR", shared) if rng.random() < 0.5 else ("EVAL", LUA_INCR, 1, shared))
        if any(c[0] == "MULTI" for c in cmds) and sh >= 0:
            # inside MULTI the counter's reply is QUEUED: drop it from this batch
            cmds.pop(sh)
            sh = -1
        raw = b"".join(Conn.enc(c) for c in cmds)
        if rng.random() < 0.1 and len(raw) > 8:
            cut = rng.randrange(1, len(raw))
            ca.s.sendall(raw[:cut])
            cr.s.sendall(raw[:cut])
            time.sleep(0.003)
            ca.s.sendall(raw[cut:])
            cr.s.sendall(raw[cut:])
            n_split += 1
        else:
            ca.s.sendall(raw)
            cr.s.sendall(raw)
        idx = expected_replies(cmds)
        ra = [ca.read() for _ in idx]
        rr = [cr.read() for _ in idx]
        for j, i in enumerate(idx):
            if i == sh:
                shared_vals.append(ra[j])
                continue
            x, y = norm(cmds[i], ra[j]), norm(cmds[i], rr[j])
            if x != y:
                diffs.append((cmds[i], ra[j], rr[j], n_batches))
        n_cmds += len(cmds)
        n_batches += 1
    out[wid] = dict(cmds=n_cmds, batches=n_batches, split=n_split, diffs=diffs, shared=shared_vals,
                    keys=g.keys + [b"{%s}:%d:empty" % (t, j) for t in home for j in range(8)])
    ca.close()
    cr.close()


def snapshot(port, keys):
    c = Conn("127.0.0.1", port)
    out = {}
    for i in range(0, len(keys), 200):
        part = keys[i:i + 200]
        cmds = []
        for k in part:
            cmds += [("TYPE", k), ("DEBUG", "DIGEST-VALUE", k), ("PEXPIRETIME", k)]
        c.send(cmds)
        res = [c.read() for _ in cmds]
        for j, k in enumerate(part):
            ty, dg, px = res[3 * j:3 * j + 3]
            if ty != "none":
                out[k] = (ty, dg[0] if isinstance(dg, list) else dg, px)
    c.close()
    return out


def cmdstats(c):
    i = d3.info(c, "commandstats")
    out = {}
    for name, v in i.items():
        if not name.startswith("cmdstat_"):
            continue
        n = name[len("cmdstat_"):]
        if n in ("info", "debug", "config", "replconf", "psync", "sync", "replicaof", "shutdown", "command", "ping"):
            continue
        kv = dict(x.split("=") for x in v.split(",") if "=" in x)
        out[n] = (int(kv.get("calls", 0)), int(kv.get("rejected_calls", 0)), int(kv.get("failed_calls", 0)))
    return out


def bg_keys(srv):
    """The background load's keys (bg_worker's tags, same seeds)."""
    out = []
    for j in range(A.bg):
        rng = random.Random(7000 + j)
        for lo, hi in srv.ranges:
            t = tags_in(rng, lo, hi, 1, b"bg%d-" % (7000 + j))[0]
            out += [b"{%s}:%d" % (t, x) for x in range(16)] + [b"{%s}:%d:n" % (t, x) for x in range(16)]
    return out


def owner_args(cpus, main, bg):
    return ["--owner-threads", str(A.n), "--owner-cpulist", cpus, "--server-cpulist", main, "--bio-cpulist", bg,
            "--aof-rewrite-cpulist", bg, "--bgsave-cpulist", bg]


def bg_worker(ports, ranges, stop_at, seed, counter):
    """RL-g4a stress: pipelined native writes and reads (32 per pipeline) on every owner port of A, on keys of its own
    tags, so owner windows hold full read batches while the checker's handoffs make main stop the world often."""
    rng = random.Random(seed)
    conns = []
    for i, (lo, hi) in enumerate(ranges):
        t = tags_in(rng, lo, hi, 1, b"bg%d-" % seed)[0]
        conns.append((Conn("127.0.0.1", ports[i]), [b"{%s}:%d" % (t, j) for j in range(16)]))
    n = 0
    while time.time() < stop_at:
        for c, ks in conns:
            batch = []
            for _ in range(32):
                k = rng.choice(ks)
                x = rng.random()
                batch.append(("SET", k, "b%d" % rng.randrange(1000)) if x < 0.4 else ("GET", k) if x < 0.7 else
                             ("INCR", k + b":n") if x < 0.85 else ("MSET", ks[0], "m", ks[1], "n") if x < 0.95 else ("DEL", k))
            c.send(batch)
            for _ in batch:
                c.read()
            n += len(batch)
    with counter.get_lock():
        counter.value += n


def suite(name):
    extra = {"plain": ["--appendonly", "no"],
             "mm": ["--appendonly", "no", "--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"],
             "repl": ["--appendonly", "no", "--repl-backlog-size", "64mb"],
             "aof": ["--appendonly", "yes", "--appendfsync", "everysec"],
             # RL-g4a: owners batch their reads under the AOF too (owner-uring-aof), with a replica
             "uringaofrepl": ["--appendonly", "yes", "--appendfsync", "everysec", "--owner-uring-aof", "yes",
                              "--repl-backlog-size", "64mb"]}[name]
    a = Srv("a-" + name, A.bin, A.port, A.owner_port, owner_args(A.a_cpus, A.a_main, A.a_bg) + extra,
            "%s,%s" % (A.a_cpus, A.a_bg), A.dir)
    r = Srv("r-" + name, A.ref_bin, A.ref_port, A.ref_owner_port, owner_args(A.r_cpus, A.r_main, A.r_bg) + extra,
            "%s,%s" % (A.r_cpus, A.r_bg), A.dir)
    rep = None
    if name in ("repl", "uringaofrepl"):
        rep = Srv("rep-" + name, A.bin, A.replica_port, 0, ["--appendonly", "no"], A.rep_cpus, A.dir)
        rep.c.cmd("REPLICAOF", "127.0.0.1", A.port)
        for _ in range(400):
            if d3.info(rep.c, "replication").get("master_link_status") == "up":
                break
            time.sleep(0.05)
    for s in (a, r):
        s.c.cmd("SCRIPT", "LOAD", LUA_INCR)
    va, vr = d3.info(a.c, "owners").get("owner_veto"), d3.info(r.c, "owners").get("owner_veto")
    check("%s.owners_native" % name, va == "none" and vr == "none" and len(a.ranges) == A.n, "veto A %s R %s" % (va, vr))
    rng = random.Random(A.seed)
    lo, hi = a.ranges[-1]
    shared = b"{%s}:ctr" % tags_in(rng, lo, hi, 1, b"shared")[0]
    s0 = d3.ostat(a.c)
    st0a, st0r = cmdstats(a.c), cmdstats(r.c)
    out = {}
    stop_at = time.time() + A.secs
    ths = [threading.Thread(target=worker, args=(w, a, r, out, stop_at, A.seed * 1000 + w, shared))
           for w in range(A.threads)]
    bgn = multiprocessing.Value("q", 0)
    bgs = [multiprocessing.Process(target=bg_worker, args=([A.owner_port + i for i in range(len(a.ranges))], a.ranges,
                                                           stop_at, 7000 + j, bgn)) for j in range(A.bg)]
    for p in bgs:
        p.start()
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    for p in bgs:
        p.join()
    if A.bg:
        check("%s.bg_load_ran" % name, bgn.value > 10000, "%d background native commands on A's owner ports" % bgn.value)
    s1 = d3.ostat(a.c)
    st1a, st1r = cmdstats(a.c), cmdstats(r.c)
    n = sum(o["cmds"] for o in out.values())
    diffs = [d for o in out.values() for d in o["diffs"]]
    check("%s.replies_equal_parent" % name, not diffs and n > 5000 and len(out) == A.threads,
          diffs[:3] if diffs else "%d commands in %d pipelined batches on %d connections (%d split mid-command)" %
          (n, sum(o["batches"] for o in out.values()), len(out), sum(o["split"] for o in out.values())))
    vals = [v for o in out.values() for v in o["shared"]]
    per_conn_inc = all(all(isinstance(x, int) for x in o["shared"]) and
                       all(x < y for x, y in zip(o["shared"], o["shared"][1:])) for o in out.values())
    final = int(a.c.cmd("GET", shared) or 0)
    check("%s.shared_counter_linearizable" % name,
          per_conn_inc and len(set(vals)) == len(vals) and final == len(vals) and sorted(vals) == list(range(1, final + 1)),
          "%d increments (INCR on the owner, EVAL through main), values unique 1..%d, increasing per connection: %s" %
          (len(vals), final, per_conn_inc))
    da = {k: tuple(x - y for x, y in zip(st1a[k], st0a.get(k, (0, 0, 0)))) for k in st1a}
    dr = {k: tuple(x - y for x, y in zip(st1r[k], st0r.get(k, (0, 0, 0)))) for k in st1r}
    da = {k: v for k, v in da.items() if v != (0, 0, 0)}
    dr = {k: v for k, v in dr.items() if v != (0, 0, 0)}
    bad = {k: (da.get(k), dr.get(k)) for k in set(da) | set(dr) if da.get(k) != dr.get(k)}
    if not A.bg:  # the background load runs on A only
        check("%s.commandstats_equal_parent" % name, not bad, bad if bad else "%d commands' calls / rejected / failed" % len(da))
    hand = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
    stops = max(1, s1["owner_world_stops"] - s0["owner_world_stops"])
    # RL-1b: with RL-g1g in the tree, a client with input left goes home by RL-g1g's resume (the rest of its
    # input and main's replies, at the end of beforeSleep) before ownerGoHomeNow can take it: both count.
    # RL-g4a: no trip home by ownerGoHomeNow under the AOF (aof, uringaofrepl).
    wr = s1.get("owner_returns_with_replies", 0) - s0.get("owner_returns_with_replies", 0)
    rs = s1.get("owner_pipeline_resumes", 0) - s0.get("owner_pipeline_resumes", 0)
    home = wr + rs
    want_home = name not in ("aof", "uringaofrepl")
    check("%s.handoffs_went_home" % name, hand > 1000 and ((home > 0.5 * hand) if want_home else wr == 0),
          "handoffs %d (%.1f per stop), home with replies %d + pipeline resumes %d, returns %d" %
          (hand, hand / stops, wr, rs, s1["owner_returns_from_main"] - s0["owner_returns_from_main"]))
    if "owner_stop_early_leaves" in s1:
        # RL-g4a: owners left windows early for pending stops during the run (inbox clients, unsubmitted reads)
        el = s1["owner_stop_early_leaves"] - s0.get("owner_stop_early_leaves", 0)
        il = s1["owner_stop_inbox_left"] - s0.get("owner_stop_inbox_left", 0)
        rl = s1["owner_stop_reads_left"] - s0.get("owner_stop_reads_left", 0)
        check("%s.early_leaves_exercised" % name, el > 0, "early leaves %d, inbox clients left %d, reads left %d" % (el, il, rl))
    keys = sorted(set(k for o in out.values() for k in o["keys"])) + [shared]
    sa, sr = snapshot(a.port, keys), snapshot(r.port, keys)
    badk = [(k, sa.get(k), sr.get(k)) for k in set(sa) | set(sr)
            if sa.get(k, (None,) * 3)[:2] != sr.get(k, (None,) * 3)[:2] or
            (sa.get(k, (0, 0, -1))[2] > 0) != (sr.get(k, (0, 0, -1))[2] > 0)]
    check("%s.keyspace_equal_parent" % name, not badk and len(sa) > 100,
          badk[:3] if badk else "%d keys: type, digest, expire presence equal (%s)" %
          (len(sa), ", ".join("%s %d" % (t, sum(1 for v in sa.values() if v[0] == t))
                             for t in sorted(set(v[0] for v in sa.values())))))
    if rep:
        for _ in range(600):
            off = int(d3.info(a.c, "replication")["master_repl_offset"])
            ri = d3.info(rep.c, "replication")
            if ri.get("master_link_status") == "up" and int(ri.get("slave_repl_offset", -1)) == off:
                break
            time.sleep(0.05)
        sp = snapshot(rep.port, keys)
        sa = snapshot(a.port, keys)
        badr = [(k, sa.get(k), sp.get(k)) for k in set(sa) | set(sp) if sa.get(k) != sp.get(k)]
        check("%s.replica_keyspace_equal" % name, not badr and len(sa) > 100,
              badr[:3] if badr else "%d keys exactly equal on the replica (PEXPIRETIME too), link %s" %
              (len(sa), ri.get("master_link_status")))
        bga = snapshot(a.port, bg_keys(a))
        bgp = snapshot(rep.port, bg_keys(a))
        check("%s.replica_dbsize_and_bg_keys_equal" % name, a.c.cmd("DBSIZE") == rep.c.cmd("DBSIZE") and bga == bgp,
              "dbsize %s / %s, %d background keys equal" % (a.c.cmd("DBSIZE"), rep.c.cmd("DBSIZE"), len(bga)))
    if name in ("aof", "uringaofrepl"):
        sa = snapshot(a.port, keys)
        bga, dba = snapshot(a.port, bg_keys(a)), a.c.cmd("DBSIZE")
        a.stop()
        b = Srv("a-" + name, A.bin, A.port, A.owner_port, owner_args(A.a_cpus, A.a_main, A.a_bg) + extra,
                "%s,%s" % (A.a_cpus, A.a_bg), A.dir, fresh=False)
        sb = snapshot(b.port, keys)
        badl = [(k, sa.get(k), sb.get(k)) for k in set(sa) | set(sb) if sa.get(k) != sb.get(k)]
        check("%s.reload_keyspace_equal" % name, not badl and len(sa) > 100,
              badl[:3] if badl else "%d keys exactly equal after reloading A's AOF" % len(sa))
        bgb = snapshot(b.port, bg_keys(b))
        check("%s.reload_dbsize_and_bg_keys_equal" % name, b.c.cmd("DBSIZE") == dba and bgb == bga,
              "dbsize %s / %s, %d background keys equal" % (dba, b.c.cmd("DBSIZE"), len(bga)))
        a = b
    for s in (a, r, rep):
        if s:
            s.stop()
            check("%s.no_crash.%s" % (name, s.name), s.crashed() == 0, "crash lines %d" % s.crashed())
            shutil.rmtree(os.path.join(s.dir, "appendonlydir"), ignore_errors=True)


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--ref-port", type=int, required=True)
    ap.add_argument("--ref-owner-port", type=int, required=True)
    ap.add_argument("--replica-port", type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--a-cpus", required=True)
    ap.add_argument("--a-main", required=True)
    ap.add_argument("--a-bg", required=True)
    ap.add_argument("--r-cpus", required=True)
    ap.add_argument("--r-main", required=True)
    ap.add_argument("--r-bg", required=True)
    ap.add_argument("--rep-cpus", required=True)
    ap.add_argument("--threads", type=int, default=24)
    ap.add_argument("--secs", type=float, default=20)
    ap.add_argument("--seed", type=int, default=3)
    ap.add_argument("--suite", default="plain,mm,repl,aof,uringaofrepl")
    ap.add_argument("--bg", type=int, default=0, help="RL-g4a stress: background processes of pipelined native load on A")
    A = ap.parse_args()
    import hashlib
    A.sha = hashlib.sha1(LUA_INCR.encode()).hexdigest()
    os.makedirs(A.dir, exist_ok=True)
    for s in A.suite.split(","):
        suite(s)
    fails = [x for x in d3.RESULTS if not x["ok"]]
    print("RLHANDOFF " + json.dumps(dict(pass_=len(d3.RESULTS) - len(fails), fail=len(fails))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
