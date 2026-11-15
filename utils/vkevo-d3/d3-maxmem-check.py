#!/usr/bin/env python3
"""D3-all-g11-maxmem check for the owners' memory leases (owner.c: under maxmemory an owner batches its reads and
runs SET bursts while its lease has room, OWN_MM_LEASE_ROOM; each queued read reserves OWN_MM_READ, each burst SET
is checked against the parser path's bound), against the parent's owner path (D3-all-g10-setburst: reads one at a
time, no SET bursts under maxmemory).

Starts the tree's server (--bin) and the parent's (--ref-bin) with owner-threads 4 on 127.0.0.1 and runs the same
traffic on both. Parts (--parts, default all):
  stream   no eviction (maxmemory 1gb allkeys-lru, well above the data): 16 connections, 4 per owner port, each on
           its own keys, rounds of 1-16 commands (mostly plain SETs: bursts; SET with options, integer and 200 B
           values, GET, INCR, DEL, MGET: the parser) sent concurrently, so owners batch several ready reads. Reply
           bytes per connection identical; DBSIZE, DEBUG DIGEST and every key's OBJECT ENCODING identical; on the
           tree SET bursts and io_uring reads ran and every window under maxmemory was a lease window; evicted 0.
  evict    for each policy (allkeys-lru, volatile-lru, noeviction): a base dataset (half the keys with a far TTL),
           maxmemory set to used + 6 MB (under OWN_MM_TIGHT per owner: main serves), then 4 connections (one per
           owner) pipeline 100,000 new 90 B SETs (about 15 MB) in rounds of 50; and for noeviction and
           allkeys-lru again with used + 192 MB and 1,600,000 SETs (about 240 MB): the tree starts inside its leases
           (batched reads, SET bursts), passes through the exact path and the tight veto, then fills / evicts.
           A sampler reads used_memory every 10 ms (INFO on main, owners stopped). Checks per server:
           max(used_memory - maxmemory) <= SLACK (1 MB, OWN_MM_SLACK); DBSIZE + evicted_keys == keys written
           (allkeys-lru); volatile-lru evicts only TTL keys (every persistent key still there); noeviction evicts
           none, refuses with -OOM, and refuses nothing while far below the limit; tree vs ref: OK and OOM counts
           and the first -OOM's position within SLACK's worth of SETs.
  config   CONFIG SET maxmemory while 8 writer threads SET their own keys through owner ports: 0 -> used + 256 MB
           (leases) -> used + 2 MB (tight: main) -> used - 8 MB (below usage: main evicts, allkeys-lru) -> 0 -> 4gb
           -> noeviction below usage (-OOM) -> 0. Per connection every reply is +OK or -OOM in order; each key's
           final value is its writer's last acknowledged value, or the key was evicted; used_memory within
           maxmemory + SLACK once eviction settles; owner_mm_lease_bytes follows maxmemory; no crash.
  multi    MULTI under maxmemory, tree vs ref byte for byte: native EXEC (one owner's keys) with room; noeviction
           full: SET inside MULTI -> -OOM and EXECABORT; maxmemory lowered between MULTI and EXEC (EXEC -> -OOM);
           allkeys-lru full: EXEC evicts and runs; then MULTI with room again.
Usage: d3-maxmem-check.py --bin <tree>/src/valkey-server --ref-bin <g10>/src/valkey-server --dir D --port P
                          --owner-port OP [--cpus 8-11] [--main-cpu 16] [--parts stream,evict,config,multi]
Prints PASS/FAIL/INFO lines and MAXMEMCHECK {checks, fails}; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS
bc = __import__("d3-burst-check")  # Raw, Server, key_in, ALNUM

ALNUM = bc.ALNUM
SLACK = 1 << 20  # OWN_MM_SLACK: the bound this branch states (owners stay below maxmemory - SLACK)
FAR = 4102444800000


def enc(*args):
    return d3.Conn.enc(args)


def info_int(c, section, key):
    return int(d3.info(c, section)[key])


def rval(rng, n):
    return bytes(rng.choice(ALNUM) for _ in range(n))


class Pair:
    def __init__(self, a, name, extra=()):
        d = os.path.join(a.dir, name)
        self.tree = bc.Server("tree", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, d, extra)
        self.ref = bc.Server("ref", a.ref_bin, a.port + 10, a.owner_port + 20, a.cpus, a.main_cpu, d, extra)
        self.srvs = [self.tree, self.ref]
        self.name = name
        d3.check("%s.topology" % name, self.tree.ranges == self.ref.ranges and len(self.tree.ranges) == 4,
                 self.tree.ranges)

    def both(self, *args):
        return [s.plain.cmd(*args) for s in self.srvs]

    def stop(self):
        for s in self.srvs:
            s.stop()
            d3.check("%s.%s.no_crash" % (self.name, s.name), s.crashed() == 0, "crash lines %d" % s.crashed())


def owner_port(s, i):
    return s.oport + i


# ---------------------------------------------------------------------------------------------------------------
def part_stream(a):
    pr = Pair(a, "stream", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"])
    rng = random.Random(20261115)
    o0 = {s.name: d3.ostat(s.plain) for s in pr.srvs}
    nconn, rounds = 16, 150
    # per connection: its owner, its keys, and the rounds (bytes, replies) it sends
    plans = []
    for ci in range(nconn):
        oi = ci % 4
        lo, hi = pr.tree.ranges[oi]
        keys = [bc.key_in(rng, rng.choice([8, 16, 24, 40]), lo, hi) for _ in range(40)]
        tag = b"{" + bc.key_in(rng, 5, lo, hi) + b"}"
        rs = []
        for _ in range(rounds):
            cmds = []
            for _ in range(rng.choice([1, 1, 1, 2, 4, 8, 16])):
                k = rng.choice(keys)
                x = rng.random()
                if x < 0.70:
                    cmds.append(("SET", k, rval(rng, rng.choice([1, 16, 16, 50, 99]))))
                elif x < 0.74:
                    cmds.append(("SET", k, b"%d" % rng.randint(-1000, 10**9)))
                elif x < 0.78:
                    cmds.append(("SET", k, rval(rng, 200)))
                elif x < 0.82:
                    cmds.append(("SET", k, rval(rng, 16), "PXAT", str(FAR)))
                elif x < 0.86:
                    cmds.append(("GET", k))
                elif x < 0.90:
                    cmds.append(("INCR", tag + b"c"))
                elif x < 0.93:
                    cmds.append(("DEL", k))
                elif x < 0.96:
                    cmds.append(("SET", k, rval(rng, 8), "GET"))
                else:
                    cmds.append(("MGET", tag + b"a", tag + b"b"))
            rs.append((b"".join(enc(*c) for c in cmds), len(cmds)))
        plans.append((oi, rs))
    got = {}

    def run(s, ci, oi, rs, out):
        r = bc.Raw(owner_port(s, oi))
        b = []
        for data, n in rs:
            r.send(data)
            b.extend(r.replies(n))
        r.close()
        out[ci] = b"".join(b)

    for s in pr.srvs:
        out = {}
        ths = [threading.Thread(target=run, args=(s, ci, oi, rs, out)) for ci, (oi, rs) in enumerate(plans)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        got[s.name] = out
    same = [ci for ci in range(nconn) if got["tree"].get(ci) == got["ref"].get(ci)]
    d3.check("stream.replies", len(same) == nconn and all(got["tree"].get(ci) for ci in range(nconn)),
             "%d/%d connections byte-identical, %d reply bytes" % (len(same), nconn,
                                                                    sum(len(v) for v in got["tree"].values())))
    dbs = pr.both("DBSIZE")
    dg = pr.both("DEBUG", "DIGEST")
    d3.check("stream.keyspace", dbs[0] == dbs[1] and dg[0] == dg[1] and dbs[0] > 100, "dbsize %s digest %s" % (dbs, dg))
    keys = sorted(pr.tree.plain.cmd("KEYS", "*"))
    encs = [[s.plain.cmd("OBJECT", "ENCODING", k) for k in keys] for s in pr.srvs]
    ttls = [[s.plain.cmd("PEXPIRETIME", k) for k in keys] for s in pr.srvs]
    d3.check("stream.encodings_ttls", encs[0] == encs[1] and ttls[0] == ttls[1], "%d keys" % len(keys))
    o1 = {s.name: d3.ostat(s.plain) for s in pr.srvs}
    t = {k: o1["tree"].get(k, 0) - o0["tree"].get(k, 0) for k in o1["tree"]}
    r = {k: o1["ref"].get(k, 0) - o0["ref"].get(k, 0) for k in o1["ref"]}
    d3.check("stream.tree_lease_path", t.get("owner_setburst_commands", 0) > 1000 and t.get("owner_uring_reads", 0) > 500
             and t.get("owner_mm_exact_windows", 0) == 0 and t.get("owner_mm_lease_windows", 0) > 0,
             "tree: setburst +%d uring_reads +%d lease_windows +%d exact_windows +%d" % (
                 t.get("owner_setburst_commands", 0), t.get("owner_uring_reads", 0), t.get("owner_mm_lease_windows", 0),
                 t.get("owner_mm_exact_windows", 0)))
    d3.check("stream.ref_exact_path", r.get("owner_setburst_commands", 0) == 0 and r.get("owner_uring_reads", 0) == 0,
             "ref: setburst +%d uring_reads +%d" % (r.get("owner_setburst_commands", 0), r.get("owner_uring_reads", 0)))
    evk = [int(d3.info(s.plain, "stats")["evicted_keys"]) for s in pr.srvs]
    d3.check("stream.no_eviction", evk == [0, 0], evk)
    tn = t.get("owner_native_commands", 0)
    d3.check("stream.native", tn > 0.9 * nconn * rounds, "tree native +%d, maxmemory_to_main +%d" % (
        tn, t.get("owner_maxmemory_to_main", 0)))
    li = d3.info(pr.tree.plain, "owners")
    d3.check("stream.info", li.get("owner_mm_lease") == "yes" and int(li.get("owner_mm_lease_bytes", 0)) > 100 << 20
             and li.get("owner_uring_batching") == "yes",
             "owner_mm_lease %s bytes %s batching %s" % (li.get("owner_mm_lease"), li.get("owner_mm_lease_bytes"),
                                                         li.get("owner_uring_batching")))
    pr.stop()


# ---------------------------------------------------------------------------------------------------------------
class Sampler:
    """used_memory and maxmemory every 10 ms over its own plain connection (INFO runs on main, owners stopped)."""

    def __init__(self, s):
        self.c = d3.Conn("127.0.0.1", s.port)
        self.over = -(1 << 62)
        self.n = 0
        self.go = True
        self.t = threading.Thread(target=self.loop)
        self.t.start()

    def loop(self):
        while self.go:
            m = d3.info(self.c, "memory")
            mm = int(m["maxmemory"])
            if mm:
                self.over = max(self.over, int(m["used_memory"]) - mm)
                self.n += 1
            time.sleep(0.01)

    def stop(self):
        self.go = False
        self.t.join()
        self.c.close()
        return self.over, self.n


def part_evict(a):
    # (label, policy, headroom over the base data, new SETs): 6 MB starts under OWN_MM_TIGHT per owner (main serves);
    # 192 MB starts with leases of ~48 MB (batched + bursts), passes through the exact path (< OWN_MM_LEASE_ROOM)
    # and the tight veto, and fills up
    for lab, pol, room, total in (("allkeys-lru", "allkeys-lru", 6 << 20, 100000),
                                  ("volatile-lru", "volatile-lru", 6 << 20, 100000),
                                  ("noeviction", "noeviction", 6 << 20, 100000),
                                  ("noeviction-lease", "noeviction", 192 << 20, 1600000),
                                  ("allkeys-lru-lease", "allkeys-lru", 192 << 20, 1600000)):
        part_evict_one(a, lab, pol, room, total)


def part_evict_one(a, lab, pol, room, total):
    if True:
        pr = Pair(a, "evict-" + lab, ["--maxmemory-policy", pol])
        rng = random.Random(7 + len(pol))
        base = 20000
        bkeys = [b"base:%d:" % i + rval(rng, 8) for i in range(base)]
        for s in pr.srvs:
            pipe = []
            for i, k in enumerate(bkeys):
                pipe.append(("SET", k, rval(random.Random(i), 100)) + (("PXAT", str(FAR)) if i % 2 else ()))
                if len(pipe) == 1000:
                    s.plain.send(pipe)
                    [s.plain.read() for _ in pipe]
                    pipe = []
        used = max(info_int(s.plain, "memory", "used_memory") for s in pr.srvs)
        limit = used + room
        pr.both("CONFIG", "SET", "maxmemory", str(limit))
        time.sleep(0.3)
        o0 = d3.ostat(pr.tree.plain)
        # the new keys, per owner, the same on both servers
        per = total // 4
        streams = []
        for oi in range(4):
            lo, hi = pr.tree.ranges[oi]
            r2 = random.Random(1000 + oi)
            ks = [b"{%s}" % bc.key_in(r2, 5, lo, hi) for _ in range(64)]
            streams.append([ks[j % 64] + b":%d" % j for j in range(per)])
        vals = [rval(rng, 90) for _ in range(64)]  # 90 B: SET bursts take values up to 99 B
        res = {}
        for s in pr.srvs:
            smp = Sampler(s)
            out = {}

            def run(oi):
                r = bc.Raw(owner_port(s, oi))
                reps = []
                ks = streams[oi]
                for j in range(0, len(ks), 50):
                    chunk = ks[j:j + 50]
                    r.send(b"".join(enc("SET", k, vals[(j + i) % 64]) for i, k in enumerate(chunk)))
                    reps.extend(r.replies(len(chunk)))
                r.close()
                out[oi] = reps

            t0 = time.time()
            ths = [threading.Thread(target=run, args=(oi,)) for oi in range(4)]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
            dt = time.time() - t0
            over, n = smp.stop()
            st = d3.info(s.plain, "stats")
            reps = [x for oi in range(4) for x in out[oi]]
            ok = sum(1 for x in reps if x == b"+OK\r\n")
            oom = sum(1 for x in reps if x.startswith(b"-OOM"))
            first = [next((j for j, x in enumerate(out[oi]) if x.startswith(b"-OOM")), None) for oi in range(4)]
            res[s.name] = dict(ok=ok, oom=oom, other=len(reps) - ok - oom, over=over, samples=n,
                               evicted=int(st["evicted_keys"]), dbsize=s.plain.cmd("DBSIZE"), secs=round(dt, 2),
                               first=first, ops=round(len(reps) / dt))
            d3.check("evict.%s.%s.bound" % (lab, s.name), over <= SLACK and n > 10,
                     "max used_memory - maxmemory %d B over %d samples (slack %d)" % (over, n, SLACK))
            d3.check("evict.%s.%s.replies" % (lab, s.name), res[s.name]["other"] == 0, res[s.name])
            if pol == "allkeys-lru":
                d3.check("evict.%s.%s.accounting" % (lab, s.name),
                         res[s.name]["dbsize"] + res[s.name]["evicted"] == base + 4 * per and oom == 0
                         and res[s.name]["evicted"] > 0, res[s.name])
            elif pol == "volatile-lru":
                missing = 0
                pk = [bkeys[i] for i in range(0, base, 2)]
                for j in range(0, len(pk), 1000):
                    missing += sum(1 for x in s.plain.cmd("MGET", *pk[j:j + 1000]) if x is None)
                d3.check("evict.%s.%s.only_volatile" % (lab, s.name), missing == 0 and res[s.name]["evicted"] > 0
                         and res[s.name]["evicted"] <= base // 2, "persistent missing %d, evicted %d, oom %d" % (
                             missing, res[s.name]["evicted"], oom))
            else:
                d3.check("evict.%s.%s.no_evict_oom" % (lab, s.name), res[s.name]["evicted"] == 0 and oom > 0
                         and min(f for f in first if f is not None) > 1000, res[s.name])
            print("INFO evict.%s.%s %s" % (lab, s.name, json.dumps(res[s.name])), flush=True)
        t, r = res["tree"], res["ref"]
        tol = SLACK // 120 + 200  # SETs that fit in the slack (about 120 B per new key), plus pipeline jitter
        d3.check("evict.%s.tree_vs_ref" % lab, abs(t["ok"] - r["ok"]) <= max(tol, r["ok"] // 50 if pol != "noeviction" else tol)
                 and abs(t["oom"] - r["oom"]) <= max(tol, r["oom"] // 50 if pol != "noeviction" else tol),
                 "ok %d vs %d, oom %d vs %d, evicted %d vs %d (tol %d)" % (t["ok"], r["ok"], t["oom"], r["oom"],
                                                                          t["evicted"], r["evicted"], tol))
        if room > 64 << 20:  # the tree went through its leases first
            o1 = d3.ostat(pr.tree.plain)
            dl = {k: o1.get(k, 0) - o0.get(k, 0) for k in ("owner_mm_lease_windows", "owner_mm_exact_windows",
                                                           "owner_setburst_commands", "owner_uring_reads",
                                                           "owner_native_commands", "owner_maxmemory_to_main")}
            d3.check("evict.%s.tree_lease_then_exact" % lab, dl["owner_mm_lease_windows"] > 0 and dl["owner_setburst_commands"] > 0
                     and dl["owner_uring_reads"] > 0, dl)
        if pol == "noeviction":
            ft = sum(f for f in t["first"] if f is not None)
            fr = sum(f for f in r["first"] if f is not None)
            d3.check("evict.%s.first_oom" % lab, abs(ft - fr) <= tol, "first -OOM at %s (sum %d) vs %s (sum %d)" % (
                t["first"], ft, r["first"], fr))
        pr.stop()


# ---------------------------------------------------------------------------------------------------------------
def part_config(a):
    s = bc.Server("tree", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, os.path.join(a.dir, "config"),
                  ["--maxmemory-policy", "allkeys-lru"])
    rng = random.Random(55)
    for i in range(0, 200000, 2000):
        s.plain.send([("SET", b"pre:%d" % j, rval(rng, 64)) for j in range(i, i + 2000)])
        [s.plain.read() for _ in range(2000)]
    go = [True]
    state = {}
    errs = []

    def writer(wi):
        oi = wi % 4
        lo, hi = s.ranges[oi]
        r2 = random.Random(900 + wi)
        ks = [bc.key_in(r2, 12, lo, hi) for _ in range(500)]
        r = bc.Raw(owner_port(s, oi))
        last = {}
        seq = 0
        while go[0]:
            batch = []
            for _ in range(r2.choice([1, 4, 16])):
                seq += 1
                k = ks[r2.randrange(len(ks))]
                batch.append((k, b"w%d:%d:" % (wi, seq) + rval(r2, 40)))
            r.send(b"".join(enc("SET", k, v) for k, v in batch))
            for (k, v), x in zip(batch, r.replies(len(batch))):
                if x == b"+OK\r\n":
                    last[k] = v
                elif not x.startswith(b"-OOM"):
                    errs.append(x[:80])
        r.close()
        state[wi] = last

    ths = [threading.Thread(target=writer, args=(wi,)) for wi in range(8)]
    for t in ths:
        t.start()
    time.sleep(0.5)
    steps = []

    def used():
        return info_int(s.plain, "memory", "used_memory")

    def setmm(v, pol=None):
        if pol:
            s.plain.cmd("CONFIG", "SET", "maxmemory-policy", pol)
        s.plain.cmd("CONFIG", "SET", "maxmemory", str(v))
        time.sleep(0.4)
        oi = d3.info(s.plain, "owners")
        m = d3.info(s.plain, "memory")
        steps.append(dict(mm=v, pol=pol, used=int(m["used_memory"]), veto=oi.get("owner_veto"),
                          lease=int(oi.get("owner_mm_lease_bytes", -1)), native=int(oi.get("owner_native_commands", 0))))
        return steps[-1]

    s1 = setmm(used() + (256 << 20))
    d3.check("config.lease_granted", s1["veto"] == "none" and s1["lease"] > 32 << 20, s1)
    n0 = s1["native"]
    time.sleep(0.5)
    n1 = int(d3.info(s.plain, "owners")["owner_native_commands"])
    d3.check("config.lease_native", n1 > n0, "native +%d in 0.5 s inside the lease" % (n1 - n0))
    s2 = setmm(used() + (2 << 20))
    d3.check("config.tight_to_main", s2["veto"] == "all", s2)
    s3 = setmm(used() - (8 << 20))
    time.sleep(1.0)
    m3 = d3.info(s.plain, "memory")
    ev3 = int(d3.info(s.plain, "stats")["evicted_keys"])
    d3.check("config.below_usage_evicts", ev3 > 0 and int(m3["used_memory"]) <= s3["mm"] + SLACK,
             "evicted %d, used %s vs maxmemory %d" % (ev3, m3["used_memory"], s3["mm"]))
    s4 = setmm(0)
    d3.check("config.off_native", s4["veto"] == "none" and s4["lease"] == 0, s4)
    s5 = setmm(4 << 30)
    d3.check("config.big_lease", s5["veto"] == "none" and s5["lease"] > 512 << 20, s5)
    s6 = setmm(used() - (4 << 20), "noeviction")
    time.sleep(0.5)
    s7 = setmm(0, "allkeys-lru")
    time.sleep(0.3)
    go[0] = False
    for t in ths:
        t.join()
    d3.check("config.replies", not errs, "unexpected replies %s" % errs[:3])
    # every key: its writer's last acknowledged value, or evicted (only allkeys-lru phases evict)
    bad, gone, n = 0, 0, 0
    for wi, last in state.items():
        ks = list(last)
        for j in range(0, len(ks), 500):
            got = s.plain.cmd("MGET", *ks[j:j + 500])
            for k, g in zip(ks[j:j + 500], got):
                n += 1
                if g is None:
                    gone += 1
                elif g != last[k]:
                    bad += 1
    d3.check("config.values", bad == 0 and n > 1000, "%d keys checked, %d wrong, %d evicted" % (n, bad, gone))
    print("INFO config.steps %s" % json.dumps(steps), flush=True)
    del s6, s7
    s.stop()
    d3.check("config.no_crash", s.crashed() == 0, "crash lines %d" % s.crashed())


# ---------------------------------------------------------------------------------------------------------------
def part_multi(a):
    pr = Pair(a, "multi", ["--maxmemory", "1gb", "--maxmemory-policy", "noeviction"])
    rng = random.Random(77)
    lo, hi = pr.tree.ranges[0]
    tag = b"{" + bc.key_in(rng, 5, lo, hi) + b"}"
    tagged = [tag + b":%d" % i for i in range(6)]
    out = {}

    def txn(s, cmds):
        r = bc.Raw(owner_port(s, 0))
        r.send(b"".join(enc(*c) for c in cmds))
        rep = b"".join(r.replies(len(cmds)))
        r.close()
        return rep

    def case(name, cmds, before=None):
        reps = []
        for s in pr.srvs:
            if before:
                before(s)
            reps.append(txn(s, cmds))
        d3.check("multi.%s" % name, reps[0] == reps[1], reps[0][:200] if reps[0] == reps[1] else reps)
        out[name] = reps[0]

    k0, k1 = tagged[0], tagged[1]
    room = [("MULTI",), ("SET", k0, "v0"), ("INCR", k1), ("GET", k0), ("SET", k0, rval(rng, 16)), ("EXEC",)]
    o0 = d3.ostat(pr.tree.plain)
    case("room", room)
    o1 = d3.ostat(pr.tree.plain)
    d3.check("multi.room_native", o1.get("owner_native_commands", 0) > o0.get("owner_native_commands", 0),
             "native +%d" % (o1.get("owner_native_commands", 0) - o0.get("owner_native_commands", 0)))

    def fill(s):  # noeviction: fill to over maxmemory through the plain port, then lower maxmemory under usage
        u = info_int(s.plain, "memory", "used_memory")
        s.plain.cmd("CONFIG", "SET", "maxmemory", str(u - (1 << 20)))

    case("full_queued_oom", [("MULTI",), ("SET", k0, "x"), ("GET", k0), ("EXEC",)], before=fill)
    case("full_read_only", [("MULTI",), ("GET", k0), ("EXEC",)])

    def roomy(s):
        s.plain.cmd("CONFIG", "SET", "maxmemory", "1gb")

    case("room_again", room, before=roomy)
    # maxmemory lowered between MULTI and EXEC: EXEC is refused as a whole
    reps = []
    for s in pr.srvs:
        s.plain.cmd("CONFIG", "SET", "maxmemory", "1gb")
        r = bc.Raw(owner_port(s, 0))
        r.send(b"".join(enc(*c) for c in [("MULTI",), ("SET", k0, "late"), ("INCR", k1)]))
        a1 = b"".join(r.replies(3))
        fill(s)
        r.send(enc("EXEC"))
        a2 = b"".join(r.replies(1))
        r.send(enc("GET", k0))
        a3 = b"".join(r.replies(1))
        r.close()
        reps.append(a1 + a2 + a3)
    d3.check("multi.lowered_before_exec", reps[0] == reps[1] and b"OOM" in reps[0],
             reps[0][:200] if reps[0] == reps[1] else reps)
    # allkeys-lru, full: EXEC evicts and runs
    reps = []
    for s in pr.srvs:
        s.plain.cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
        s.plain.cmd("CONFIG", "SET", "maxmemory", "1gb")
        pipe = [("SET", b"fill:%d" % i, rval(random.Random(i), 200)) for i in range(20000)]
        s.plain.send(pipe)
        [s.plain.read() for _ in pipe]
        fill(s)
        reps.append(txn(s, [("MULTI",), ("SET", k0, "lru"), ("GET", k0), ("EXEC",)]))
    evk = [int(d3.info(s.plain, "stats")["evicted_keys"]) for s in pr.srvs]
    d3.check("multi.lru_full_exec", reps[0] == reps[1] and b"lru" in reps[0] and min(evk) > 0,
             "%r evicted %s" % (reps[0][:120], evk))
    pr.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--cpus", default="8-11")
    ap.add_argument("--main-cpu", default="16")
    ap.add_argument("--n", type=int, default=4)  # the gate passes it; the servers here always run 4 owners
    ap.add_argument("--parts", default="stream,evict,config,multi")
    a = ap.parse_args()
    parts = dict(stream=part_stream, evict=part_evict, config=part_config, multi=part_multi)
    for p in a.parts.split(","):
        try:
            parts[p](a)
        except Exception as e:  # a part that cannot finish is a FAIL, the others still run
            d3.check("%s.ran" % p, False, repr(e)[:300])
    fails = [r["check"] for r in d3.RESULTS if not r["ok"]]
    print("MAXMEMCHECK " + json.dumps({"checks": len(d3.RESULTS), "fails": fails}))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
