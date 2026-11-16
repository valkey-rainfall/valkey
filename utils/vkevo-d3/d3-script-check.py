#!/usr/bin/env python3
"""RL-g1f-owner-evalsha: EVALSHA / EVAL on the owners, checked against the parent's main path.

Starts this build (--bin, owner-threads N) and the reference (--ref-bin: the parent, whose scripts always run
on main) side by side and runs on both, through owner ports and the main port:
  model      many connections, each with its own key group (several slots of several owners), random
             pipelined sequences of EVALSHA of rmw.lua and test scripts (native ones and every abort kind:
             another owner's key, a non-native command after writes, a write to a hash, pcall'ed errors,
             Lua errors, globals, math.random, error / RESP3-only returns, the time budget) mixed with
             GET / SET / HSET / DEL / INCR / EXPIRE on the same keys: every reply equal, then the final
             keyspace equal (type, encoding, value, TTL present and within 60 s)
  native     the build ran scripts on owners (owner_script_runs grew), and aborted some (owner_script_aborts)
  atomic     concurrent EVALSHA writers keep two keys equal (one script writes both); script readers
             never see them differ; INCRBY in scripts + native INCR on one key give unique values 1..total
  flush      SCRIPT FLUSH / SCRIPT LOAD on main while owners run EVALSHA: after FLUSH returned, an
             EVALSHA through any owner port gets NOSCRIPT; after LOAD returned it runs again
  expire     keys that expired before a script touched them: replies and keyspace equal the reference's
  replica    (--replica) a replica of the build: after the run its DEBUG DIGEST equals the primary's
  aof        (--aof) appendonly everysec: a fresh server loaded from the build's AOF has its DEBUG DIGEST
Prints PASS/FAIL lines and 'SCRIPTCHECK {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import importlib.util

_spec = importlib.util.spec_from_file_location("d3check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                      "d3-check.py"))
d3 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(d3)
Conn, Err, key_slot = d3.Conn, d3.Err, d3.key_slot

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(dict(check=name, ok=bool(ok), detail=str(detail)[:400]))
    print(("PASS " if ok else "FAIL ") + name + (": " + str(detail) if detail != "" else ""), flush=True)
    return ok


RMW = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "rmw.lua")).read() if os.path.exists(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "rmw.lua")) else None

SCRIPTS = {
    # native: two keys of one slot, MGET reply
    "pair": "local v = redis.call('GET', KEYS[1]); redis.call('SET', KEYS[2], (v or '') .. ARGV[1]); "
            "return redis.call('MGET', KEYS[1], KEYS[2])",
    # native: counters, EXPIRE, TTL as boolean, EXISTS, DEL, nested table reply
    "cnt": "local n = redis.call('INCRBY', KEYS[1], tonumber(ARGV[1])); redis.call('EXPIRE', KEYS[1], 100000); "
           "local d = redis.call('DECR', KEYS[2]); "
           "return {n, d, redis.call('TTL', KEYS[1]) > 0, redis.call('EXISTS', KEYS[1], KEYS[2]), {ARGV[1], false}}",
    "del": "local e = redis.call('DEL', KEYS[1], KEYS[2]); redis.call('SET', KEYS[1], ARGV[1], 'EX', 100000); "
           "redis.call('PEXPIRE', KEYS[2], 100000000); return e",
    "status": "if ARGV[1] == '1' then return redis.status_reply('FINE') elseif ARGV[1] == '2' then return nil "
              "elseif ARGV[1] == '3' then return {1, 'a', {2, false}, true} elseif ARGV[1] == '4' then return 3.7 "
              "else return redis.error_reply('BAD thing') end",
    "mset": "redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[2]); return redis.call('INCR', KEYS[1])",
    # aborts after writes: a non-native command, a hash write, another owner's key, an error after a write
    "hset": "redis.call('INCRBY', KEYS[1], 1); redis.call('HSET', KEYS[2], 'f', ARGV[1]); "
            "return redis.call('GET', KEYS[1])",
    "foreign": "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCR', ARGV[2])",
    "strlen": "redis.call('DEL', KEYS[1]); redis.call('SET', KEYS[1], ARGV[1]); return redis.call('STRLEN', KEYS[1])",
    "errafter": "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCR', KEYS[1])",
    "pcallerr": "redis.call('SET', KEYS[2], 'x'); local r = redis.pcall('INCR', KEYS[2]); "
                "if type(r) == 'table' then return r.err end; return r",
    # aborts before writes
    "time": "return #redis.call('TIME')",
    "rand": "return math.random(10) > 0",
    "glob": "x = 1; return 1",
    "luaerr": "return nil + 1",
    "log": "redis.log(redis.LOG_DEBUG, 'hi'); return redis.call('GET', KEYS[1])",
    # the time budget: long loops abort on owners and run on main
    "loop": "local s = 0; for i = 1, tonumber(ARGV[1]) do s = s + i % 7 end; redis.call('SET', KEYS[1], s); return s",
}
ABORT_SCRIPTS = {"hset", "foreign", "strlen", "errafter", "pcallerr", "time", "rand", "glob", "luaerr", "log"}


class Server:
    def __init__(self, a, binpath, port, oport, cpus, main_cpu, tag, extra=()):
        self.bin, self.port, self.oport, self.n, self.tag = binpath, port, oport, a.n, tag
        self.dir = os.path.join(a.dir, tag)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        args = [binpath, "--port", str(port), "--bind", "127.0.0.1", "--owner-threads", str(a.n),
                "--owner-port", str(oport), "--owner-cpulist", cpus, "--server-cpulist", str(main_cpu),
                "--save", "", "--appendonly", "no", "--enable-debug-command", "yes", "--daemonize", "no",
                "--dir", self.dir, "--logfile", os.path.join(self.dir, "log")] + list(extra)
        self.p = subprocess.Popen(args)
        for _ in range(200):
            try:
                c = Conn("127.0.0.1", port, timeout=5)
                if c.cmd("PING") == "PONG":
                    c.close()
                    break
            except Exception:
                time.sleep(0.05)
        self.owner_of = []
        c = Conn("127.0.0.1", port)
        for _ in range(100):
            o = info(c, "owners")
            if o.get("owner_veto") == "none":
                break
            time.sleep(0.05)
        c.close()

    def conn(self, owner=None):
        return Conn("127.0.0.1", self.port if owner is None else self.oport + owner)

    def stop(self):
        try:
            c = Conn("127.0.0.1", self.port, timeout=10)
            c.send([("SHUTDOWN", "NOSAVE")])
            c.close()
        except Exception:
            pass
        try:
            self.p.wait(30)
        except Exception:
            self.p.kill()

    def crashed(self):
        try:
            return sum(1 for l in open(os.path.join(self.dir, "log")) if "BUG REPORT" in l or "ASSERT" in l or "crashed" in l)
        except Exception:
            return -1


def info(c, section):
    raw = c.cmd("INFO", section)
    raw = raw.decode() if isinstance(raw, bytes) else raw
    out = {}
    for l in raw.split("\r\n"):
        if ":" in l and not l.startswith("#"):
            k, v = l.split(":", 1)
            out[k] = v
    return out


def owner_of_slot(slot, n):
    tables = 1 << 10
    per = 16384 // tables
    t = slot // per
    for i in range(n):
        if t < tables * (i + 1) // n:
            return i
    return n - 1


def tag_for_owner(owner, n, rnd, avoid=()):
    while True:
        t = "t%d" % rnd.randrange(1 << 30)
        if owner_of_slot(key_slot(t.encode()), n) == owner and t not in avoid:
            return t


def gen_ops(rnd, groups, foreign_keys, nops, shas):
    """One connection's sequence: (port_choice, args) items; port_choice: 'own' | 'main' | 'other'."""
    ops = []
    names = list(shas)
    for _ in range(nops):
        tag = rnd.choice(groups)
        ka, kb = "{%s}:a" % tag, "{%s}:b" % tag
        r = rnd.random()
        if r < 0.55:
            name = rnd.choice(names)
            if name in ABORT_SCRIPTS and rnd.random() < 0.6:
                name = rnd.choice(["pair", "cnt", "rmw" if "rmw" in shas else "pair", "mset"])
            sha = shas[name]
            if name == "rmw":
                args = ("EVALSHA", sha, 1, ka, 100000)
            elif name == "foreign":
                args = ("EVALSHA", sha, 1, ka, "v%d" % rnd.randrange(100), rnd.choice(foreign_keys))
            elif name == "loop":
                args = ("EVALSHA", sha, 1, ka, rnd.choice([10, 1000, 1000000]))
            elif name in ("cnt",):
                args = ("EVALSHA", sha, 2, ka, kb, rnd.randrange(1, 5))
            elif name == "status":
                args = ("EVALSHA", sha, 1, ka, rnd.choice("12345"))
            elif name == "mset":
                args = ("EVALSHA", sha, 2, ka, kb, rnd.randrange(100), "m%d" % rnd.randrange(100))
            else:
                args = ("EVALSHA", sha, 2, ka, kb, "v%d" % rnd.randrange(1000))
            if rnd.random() < 0.05:  # EVAL of a loaded body runs on the owner too
                body = RMW if name == "rmw" else SCRIPTS.get(name)
                if body:
                    args = ("EVAL", body) + args[2:]
        elif r < 0.65:
            args = ("GET", rnd.choice([ka, kb]))
        elif r < 0.73:
            args = ("SET", rnd.choice([ka, kb]), rnd.choice(["7", "v%d" % rnd.randrange(100), "x" * 70]))
        elif r < 0.78:
            args = ("HSET", kb, "f", "h%d" % rnd.randrange(10))
        elif r < 0.83:
            args = ("DEL", rnd.choice([ka, kb]))
        elif r < 0.90:
            args = ("INCR", ka)
        elif r < 0.95:
            args = ("EXPIRE", rnd.choice([ka, kb]), 100000)
        else:
            args = ("TYPE", kb)
        port = rnd.choices(["own", "main", "other"], [0.75, 0.15, 0.10])[0]
        ops.append((tag, port, args))
    return ops


def run_conn(srv, n, ops, rnd_seed, out, idx):
    rnd = random.Random(rnd_seed)
    conns = {}
    replies = []

    def get(port_key):
        if port_key not in conns:
            conns[port_key] = srv.conn(port_key)
        return conns[port_key]

    i = 0
    try:
        while i < len(ops):
            tag, port, _ = ops[i]
            owner = owner_of_slot(key_slot(tag.encode()), n)
            pk = owner if port == "own" else (None if port == "main" else (owner + 1) % n)
            batch = [ops[i][2]]
            i += 1
            depth = rnd.choice([1, 1, 2, 5, 16])
            while i < len(ops) and len(batch) < depth and ops[i][1] == port and ops[i][0] == tag:
                batch.append(ops[i][2])
                i += 1
            c = get(pk)
            c.send(batch)
            for _ in batch:
                replies.append(c.read())
    except Exception as e:
        replies.append("EXC %r" % e)
    for c in conns.values():
        c.close()
    out[idx] = replies


def norm(v):
    if isinstance(v, Err):
        return "ERR:" + v
    if isinstance(v, list):
        return [norm(x) for x in v]
    return v


def keyspace(srv):
    c = srv.conn()
    out = {}
    cur = "0"
    while True:
        r = c.cmd("SCAN", cur, "COUNT", 1000)
        cur = r[0].decode()
        for k in r[1]:
            t = c.cmd("TYPE", k)
            enc = c.cmd("OBJECT", "ENCODING", k)
            enc = enc.decode() if isinstance(enc, bytes) else enc
            if t == "string":
                v = c.cmd("GET", k)
            elif t == "hash":
                v = sorted((c.cmd("HGETALL", k) or []))
            else:
                v = None
            out[k] = (t, enc, v, c.cmd("PTTL", k))
        if cur == "0":
            break
    c.close()
    return out


def owners_info(srv):
    c = srv.conn()
    o = info(c, "owners")
    c.close()
    return o


def phase_model(a, A, B, shas_a, rnd):
    n = a.n
    groups_per_conn = 4
    plans = []
    used = set()
    for ci in range(a.conns):
        groups = []
        for g in range(groups_per_conn):
            t = tag_for_owner(rnd.randrange(n), n, rnd, used)
            used.add(t)
            groups.append(t)
        plans.append(groups)
    # keys of other owners for the 'foreign' script: per connection, a private key in another owner's slot
    seqs, seeds = [], []
    for ci, groups in enumerate(plans):
        owner0 = owner_of_slot(key_slot(groups[0].encode()), n)
        ft = tag_for_owner((owner0 + 1) % n, n, rnd, used)
        used.add(ft)
        crnd = random.Random(a.seed * 1000 + ci)
        seqs.append(gen_ops(crnd, groups, ["{%s}:f" % ft], a.ops, shas_a))
        seeds.append(a.seed * 7 + ci)
    o0 = owners_info(A)
    res = {}
    for srv, tag in ((A, "A"), (B, "B")):
        out = [None] * a.conns
        ths = [threading.Thread(target=run_conn, args=(srv, n, seqs[ci], seeds[ci], out, ci)) for ci in range(a.conns)]
        t0 = time.time()
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        res[tag] = (out, time.time() - t0)
    o1 = owners_info(A)
    bad = []
    total = 0
    for ci in range(a.conns):
        ra, rb = res["A"][0][ci], res["B"][0][ci]
        total += len(ra)
        if len(ra) != len(rb):
            bad.append((ci, "len", len(ra), len(rb)))
            continue
        for j, (x, y) in enumerate(zip(ra, rb)):
            if norm(x) != norm(y):
                bad.append((ci, j, seqs[ci][j][2][:3], norm(x), norm(y)))
                break
    check("model.replies", not bad and total == a.conns * a.ops,
          "%d replies on %d connections equal (build %.1fs, ref %.1fs)" % (total, a.conns, res["A"][1], res["B"][1])
          if not bad else bad[:3])
    ka, kb = keyspace(A), keyspace(B)
    diff = []
    for k in sorted(set(ka) | set(kb)):
        x, y = ka.get(k), kb.get(k)
        if x is None or y is None or x[:3] != y[:3] or (x[3] < 0) != (y[3] < 0) or abs(x[3] - y[3]) > 60000:
            diff.append((k, x, y))
    check("model.keyspace", not diff and ka, "%d keys equal (type, encoding, value, TTL)" % len(ka) if not diff else diff[:3])
    runs = int(o1["owner_script_runs"]) - int(o0["owner_script_runs"])
    aborts = int(o1["owner_script_aborts"]) - int(o0["owner_script_aborts"])
    misses = int(o1["owner_script_misses"]) - int(o0["owner_script_misses"])
    evals = sum(1 for s in seqs for op in s if op[2][0] in ("EVALSHA", "EVAL"))
    check("model.native", runs > evals * 0.15 and aborts > 0,
          "scripts %d: owner runs +%d, aborts +%d, misses +%d" % (evals, runs, aborts, misses))
    return seqs


def phase_atomic(a, A, shas):
    n = a.n
    rnd = random.Random(a.seed + 5)
    tag = tag_for_owner(rnd.randrange(n), n, rnd)
    owner = owner_of_slot(key_slot(tag.encode()), n)
    ka, kb, kc = "{%s}:a" % tag, "{%s}:b" % tag, "{%s}:c" % tag
    c = A.conn()
    w = c.cmd("SCRIPT", "LOAD", "local n = redis.call('INCRBY', KEYS[1], 1); redis.call('SET', KEYS[2], n); "
                                "redis.call('INCR', KEYS[3]); return n").decode()
    r = c.cmd("SCRIPT", "LOAD", "return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}").decode()
    c.cmd("DEL", ka, kb, kc)
    c.close()
    per, nw, nr, ni = 400, 8, 4, 4
    got, mism, errs = [], [], []
    lock = threading.Lock()

    def writer(i):
        cc = A.conn(owner if i % 4 else None)
        mine = []
        for _ in range(per // 8):
            cc.send([("EVALSHA", w, 3, ka, kb, kc)] * 8)
            mine += [cc.read() for _ in range(8)]
        cc.close()
        with lock:
            got.extend(mine)

    def incr(i):
        cc = A.conn(owner if i % 2 else None)
        mine = []
        for _ in range(per // 8):
            cc.send([("INCR", kc)] * 8)
            mine += [cc.read() for _ in range(8)]
        cc.close()
        with lock:
            got.append(("incr", mine))

    stop = [False]

    def reader(i):
        cc = A.conn(owner)
        while not stop[0]:
            v = cc.cmd("EVALSHA", r, 2, ka, kb)
            if isinstance(v, Err):
                errs.append(v)
            elif v[0] != v[1]:
                mism.append(v)
        cc.close()

    ths = [threading.Thread(target=writer, args=(i,)) for i in range(nw)] + \
          [threading.Thread(target=incr, args=(i,)) for i in range(ni)]
    rths = [threading.Thread(target=reader, args=(i,)) for i in range(nr)]
    for t in rths + ths:
        t.start()
    for t in ths:
        t.join()
    stop[0] = True
    for t in rths:
        t.join()
    wvals = sorted(x for x in got if not isinstance(x, tuple))
    ok_w = wvals == list(range(1, nw * per + 1))
    c = A.conn()
    fa, fb, fc = c.cmd("GET", ka), c.cmd("GET", kb), c.cmd("GET", kc)
    c.close()
    check("atomic.script_writers", ok_w and fa == fb == str(nw * per).encode(),
          "%d EVALSHA INCRBY values unique 1..%d, a == b == %s" % (len(wvals), nw * per, fa))
    check("atomic.script_readers", not mism and not errs, "readers saw a != b %d times, errors %d" % (len(mism), len(errs)))
    check("atomic.incr_mix", int(fc) == nw * per + ni * per,
          "c = %s after %d script INCRs + %d native/main INCRs" % (fc, nw * per, ni * per))


def phase_flush(a, A, shas):
    n = a.n
    body = SCRIPTS["pair"]
    sha = shas["pair"]
    rnd = random.Random(a.seed + 9)
    tags = [tag_for_owner(i, n, rnd) for i in range(n)]
    stop = [False]
    errors = []

    def load(i):
        cc = A.conn(i % n)
        t = tags[i % n]
        while not stop[0]:
            v = cc.cmd("EVALSHA", sha, 2, "{%s}:a" % t, "{%s}:b" % t, "z")
            if isinstance(v, Err) and not v.startswith("NOSCRIPT"):
                errors.append(v)
        cc.close()

    ths = [threading.Thread(target=load, args=(i,)) for i in range(2 * n)]
    for t in ths:
        t.start()
    ctl = A.conn()
    probes = [A.conn(i) for i in range(n)]
    bad = []
    rounds = 60
    for rr in range(rounds):
        ctl.cmd("SCRIPT", "FLUSH", rnd.choice(["SYNC", "ASYNC"]))
        for i, p in enumerate(probes):
            t = tags[i]
            v = p.cmd("EVALSHA", sha, 2, "{%s}:a" % t, "{%s}:b" % t, "q")
            if not (isinstance(v, Err) and v.startswith("NOSCRIPT")):
                bad.append(("after flush", rr, i, v))
        ctl.cmd("SCRIPT", "LOAD", body)
        for i, p in enumerate(probes):
            t = tags[i]
            for _ in range(3):  # the first call after a reload fetches the body (runs on main), the next on the owner
                v = p.cmd("EVALSHA", sha, 2, "{%s}:a" % t, "{%s}:b" % t, "q")
                if isinstance(v, Err):
                    bad.append(("after load", rr, i, v))
        time.sleep(0.005)
    stop[0] = True
    for t in ths:
        t.join()
    for p in probes:
        p.close()
    ctl.close()
    check("flush.noscript_after_flush", not bad, "%d rounds x %d owners" % (rounds, n) if not bad else bad[:3])
    check("flush.load_errors", not errors, errors[:3])


def phase_expire(a, A, B, shas):
    n = a.n
    rnd = random.Random(a.seed + 11)
    out = {}
    for srv, tag in ((A, "A"), (B, "B")):
        rr = random.Random(a.seed + 12)
        tags = [tag_for_owner(rr.randrange(n), n, rr) for _ in range(40)]
        c = srv.conn()
        for t in tags:
            c.cmd("SET", "{%s}:a" % t, "5", "PX", 30)
            c.cmd("SET", "{%s}:b" % t, "x", "PX", 30)
        c.close()
        time.sleep(0.3)
        reps = []
        for i, t in enumerate(tags):
            cc = srv.conn(owner_of_slot(key_slot(t.encode()), n))
            name = ["pair", "cnt", "rmw" if "rmw" in shas else "pair", "hset", "del"][i % 5]
            if name == "cnt":
                reps.append(cc.cmd("EVALSHA", shas[name], 2, "{%s}:a" % t, "{%s}:b" % t, 2))
            elif name == "rmw":
                reps.append(cc.cmd("EVALSHA", shas[name], 1, "{%s}:a" % t, 100000))
            else:
                reps.append(cc.cmd("EVALSHA", shas[name], 2, "{%s}:a" % t, "{%s}:b" % t, "e"))
            cc.close()
        out[tag] = [norm(x) for x in reps]
    check("expire.replies", out["A"] == out["B"], "40 scripts over expired keys" if out["A"] == out["B"] else
          [(i, x, y) for i, (x, y) in enumerate(zip(out["A"], out["B"])) if x != y][:3])


def phase_watch(a, A, B, shas):
    """WATCH on an owner, then scripts from another connection that write / do not write the key (one of them an
    aborting run that wrote the key before its abort): EXEC fails / runs exactly as on the reference."""
    n = a.n
    out = {}
    for srv, tag in ((A, "A"), (B, "B")):
        rr = random.Random(a.seed + 21)
        reps = []
        for i in range(24):
            t = tag_for_owner(rr.randrange(n), n, rr)
            ka, kb = "{%s}:a" % t, "{%s}:b" % t
            ow = owner_of_slot(key_slot(t.encode()), n)
            w, x = srv.conn(ow), srv.conn(ow)
            w.cmd("SET", ka, "1")
            reps.append(w.cmd("WATCH", ka))
            name = ["pair", "hset", "cnt", "status"][i % 4]
            if name == "cnt":
                reps.append(norm(x.cmd("EVALSHA", shas[name], 2, kb, "{%s}:c" % t, 1)))  # does not touch ka
            elif name == "status":
                reps.append(norm(x.cmd("EVALSHA", shas[name], 1, kb, "1")))
            else:
                reps.append(norm(x.cmd("EVALSHA", shas[name], 2, ka, kb, "w")))
            w.send([("MULTI",), ("INCR", ka), ("EXEC",)])
            reps.append([norm(w.read()) for _ in range(3)])
            w.close()
            x.close()
        out[tag] = reps
    check("watch.exec", out["A"] == out["B"], "24 WATCH / script / EXEC rounds" if out["A"] == out["B"] else
          [(i, x, y) for i, (x, y) in enumerate(zip(out["A"], out["B"])) if x != y][:3])


def phase_kill(a, A, B):
    """A script that never ends: on the build an owner aborts it at its time budget and main runs it, where the
    busy replies and SCRIPT KILL work as on the reference."""
    n = a.n
    body = "local n = tonumber(ARGV[1]); local i = 0; while n == 0 or i < n do i = i + 1 end; return i"
    out = {}
    for srv, tag in ((A, "A"), (B, "B")):
        rr = random.Random(a.seed + 31)
        t = tag_for_owner(rr.randrange(n), n, rr)
        ctl = srv.conn()
        ctl.cmd("CONFIG", "SET", "busy-reply-threshold", "200")
        sha = ctl.cmd("SCRIPT", "LOAD", body).decode()
        x = srv.conn(owner_of_slot(key_slot(t.encode()), n))
        warm = [x.cmd("EVALSHA", sha, 1, "{%s}:a" % t, 10) for _ in range(3)]  # the owner has it after the first
        x.send([("EVALSHA", sha, 1, "{%s}:a" % t, 0)])
        time.sleep(0.8)
        busy = ctl.cmd("PING")
        kill = ctl.cmd("SCRIPT", "KILL")
        r = x.read()
        after = x.cmd("PING")
        ctl.cmd("CONFIG", "SET", "busy-reply-threshold", "5000")
        x.close()
        ctl.close()
        out[tag] = [warm, norm(busy)[:40], norm(kill), norm(r)[:60], norm(after)]
    check("kill.busy_script", out["A"] == out["B"] and str(out["A"][1]).startswith("ERR:BUSY"),
          "build %s ref %s" % (out["A"], out["B"]))


def digest(srv_port):
    c = Conn("127.0.0.1", srv_port)
    d = c.cmd("DEBUG", "DIGEST")
    c.close()
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, default=25400)
    ap.add_argument("--owner-port", type=int, default=25420)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", default="24-27")
    ap.add_argument("--main-cpu", default="28")
    ap.add_argument("--ref-cpus", default="29-32")
    ap.add_argument("--ref-main-cpu", default="33")
    ap.add_argument("--replica-cpu", default="34")
    ap.add_argument("--conns", type=int, default=24)
    ap.add_argument("--ops", type=int, default=1500)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--aof", action="store_true")
    ap.add_argument("--replica", action="store_true")
    ap.add_argument("--suites", default="model,atomic,flush,expire,watch,kill")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    extra = ["--appendonly", "yes", "--appendfsync", "everysec"] if a.aof else []
    A = Server(a, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, "build", extra)
    B = Server(a, a.ref_bin, a.port + 10, a.owner_port + 20, a.ref_cpus, a.ref_main_cpu, "ref")
    R = None
    try:
        if a.replica:
            rdir = os.path.join(a.dir, "replica")
            shutil.rmtree(rdir, ignore_errors=True)
            os.makedirs(rdir)
            R = subprocess.Popen([a.bin, "--port", str(a.port + 1), "--bind", "127.0.0.1", "--save", "",
                                  "--server-cpulist", a.replica_cpu, "--dir", rdir, "--logfile",
                                  os.path.join(rdir, "log"), "--enable-debug-command", "yes",
                                  "--replicaof", "127.0.0.1", str(a.port)])
            c = A.conn()
            c.cmd("CONFIG", "SET", "repl-diskless-sync-delay", "0")
            c.cmd("CONFIG", "SET", "client-output-buffer-limit", "replica 0 0 0")
            c.cmd("CONFIG", "SET", "repl-backlog-size", "256mb")
            up = False
            for _ in range(400):
                try:
                    rc = Conn("127.0.0.1", a.port + 1, timeout=5)
                    up = info(rc, "replication").get("master_link_status") == "up"
                    rc.close()
                except Exception:
                    pass
                if up:
                    break
                time.sleep(0.05)
            o = info(c, "owners")
            c.close()
            check("replica.owners_native", up and o.get("owner_veto") == "none",
                  "link up %s, veto %s with a replica" % (up, o.get("owner_veto")))
        shas = {}
        for srv in (A, B):
            c = srv.conn()
            shas = {k: c.cmd("SCRIPT", "LOAD", v).decode() for k, v in SCRIPTS.items()}
            if RMW:
                shas["rmw"] = c.cmd("SCRIPT", "LOAD", RMW).decode()
            c.close()
        rnd = random.Random(a.seed)
        suites = a.suites.split(",")
        if "model" in suites:
            phase_model(a, A, B, shas, rnd)
        if "expire" in suites:
            phase_expire(a, A, B, shas)
        if "atomic" in suites:
            phase_atomic(a, A, shas)
        if "watch" in suites:
            phase_watch(a, A, B, shas)
        if "kill" in suites:
            phase_kill(a, A, B)
        if "flush" in suites:
            phase_flush(a, A, shas)
        if R is not None:
            c = A.conn()
            ok = False
            for _ in range(400):
                p = int(info(c, "replication")["master_repl_offset"])
                rc = Conn("127.0.0.1", a.port + 1)
                ro = int(info(rc, "replication").get("slave_repl_offset", -1))
                rc.close()
                if p == ro:
                    ok = True
                    break
                time.sleep(0.05)
            sync_full = info(c, "stats").get("sync_full")
            c.close()
            d1, d2 = digest(a.port), digest(a.port + 1)
            check("replica.digest", ok and d1 == d2 and sync_full == "1",
                  "offset caught up %s, full syncs %s (the first only), digest %s / %s" % (ok, sync_full, d1, d2))
        if a.aof:
            d1 = digest(a.port)
            c = A.conn()
            c.cmd("BGREWRITEAOF") if a.seed % 2 else None
            time.sleep(1.5)  # everysec: the last second is written and fsynced
            c.close()
            A.stop()
            fdir = os.path.join(a.dir, "aofload")
            shutil.rmtree(fdir, ignore_errors=True)
            shutil.copytree(os.path.join(A.dir, "appendonlydir"), os.path.join(fdir, "appendonlydir"))
            F = subprocess.Popen([a.bin, "--port", str(a.port + 2), "--bind", "127.0.0.1", "--save", "", "--appendonly",
                                  "yes", "--server-cpulist", a.replica_cpu, "--dir", fdir, "--logfile",
                                  os.path.join(fdir, "log"), "--enable-debug-command", "yes"])
            d2 = None
            for _ in range(200):
                try:
                    c = Conn("127.0.0.1", a.port + 2, timeout=5)
                    if info(c, "persistence").get("loading") == "0":
                        d2 = c.cmd("DEBUG", "DIGEST")
                        c.close()
                        break
                    c.close()
                except Exception:
                    pass
                time.sleep(0.05)
            c = Conn("127.0.0.1", a.port + 2)
            c.send([("SHUTDOWN", "NOSAVE")])
            c.close()
            F.wait(30)
            check("aof.reload_digest", d1 == d2, "build %s, reloaded %s" % (d1, d2))
        check("no_crash", A.crashed() == 0 and B.crashed() == 0, "crash lines build %s ref %s" % (A.crashed(), B.crashed()))
    finally:
        A.stop()
        B.stop()
        if R is not None:
            try:
                c = Conn("127.0.0.1", a.port + 1, timeout=5)
                c.send([("SHUTDOWN", "NOSAVE")])
                c.close()
            except Exception:
                pass
            R.wait(30)
    nfail = sum(1 for r in RESULTS if not r["ok"])
    print("SCRIPTCHECK " + json.dumps(dict(pass_=len(RESULTS) - nfail, fail=nfail)))
    sys.exit(1 if nfail else 0)


if __name__ == "__main__":
    main()
