#!/usr/bin/env python3
"""RL-g6g-scripts: realistic Lua scripts on the owners, checked against the parent's main path.

Builds on d3-script-check.py (RL-g1f's checker: Server, model, expire, atomic, watch, kill, flush, replica, aof) and
runs it with this gene's script set, then adds:
  model      d3-script-check's model with the realistic shapes and type scripts added: lists, hashes, sets, sorted
             sets (job queue add / take / done, sliding-window limiter, lock release, LMOVE / RPOPLPUSH, HINCRBY,
             HGETALL, ZRANGEBYSCORE WITHSCORES, ZPOPMIN, SMISMEMBER, TYPE, TIME as a value-free check), aborts of
             every new kind (WRONGTYPE after writes, a value above the copy caps, a hash with field expiry, a
             foreign key after list writes), EVAL of bodies nobody loaded; a share of the connections speak RESP3
             (HELLO 3) and their replies are compared as raw bytes. Keyspace compared with list / set / zset values.
  jobs       concurrent clients on owner ports and main: job queues (add / take / done) and a rate limiter with
             TIME; conservation (every added job id is in wait, active or done exactly once), an atomic reader
             script (llen wait + llen active + done counter == id counter at every read), limiter windows never
             above their limit
  evallru    EVAL of a hot body through the owners while main EVALs 600 distinct cold bodies (the 500-entry LRU
             evicts): EVALSHA of the hot sha, the cold ones and INFO evicted_scripts equal the reference's
  --bg N     N load threads (pipelined plain commands on other keys through every owner port) during model / jobs
Prints PASS/FAIL lines and 'RLSCRIPT {json}'; exit 1 on any FAIL.
"""
import argparse
import importlib.util
import json
import os
import random
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
U = os.environ.get("VKEVO_D3", HERE)
_spec = importlib.util.spec_from_file_location("d3script", os.path.join(U, "d3-script-check.py"))
ds = importlib.util.module_from_spec(_spec)
sys.argv_saved = sys.argv
_spec.loader.exec_module(ds)
Conn, Err, key_slot = ds.Conn, ds.Err, ds.key_slot

NEW = {
    "jadd": "local q = KEYS[1] local id = redis.call('INCR', q .. ':id') "
            "redis.call('HSET', q .. ':job:' .. id, 'data', ARGV[1], 'ts', ARGV[2]) "
            "redis.call('LPUSH', q .. ':wait', id) return id",
    "jtake": "local q = KEYS[1] local id = redis.call('LMOVE', q .. ':wait', q .. ':active', 'RIGHT', 'LEFT') "
             "if not id then return false end redis.call('HSET', q .. ':job:' .. id, 'processedOn', ARGV[1]) "
             "redis.call('ZADD', q .. ':locks', ARGV[1], id) return {id, redis.call('HGETALL', q .. ':job:' .. id)}",
    "jdone": "local q = KEYS[1] local id = redis.call('RPOP', q .. ':active') if not id then return 0 end "
             "redis.call('ZREM', q .. ':locks', id) redis.call('INCR', q .. ':done') "
             "return redis.call('DEL', q .. ':job:' .. id)",
    "jread": "local q = KEYS[1] return {redis.call('LLEN', q .. ':wait'), redis.call('LLEN', q .. ':active'), "
             "tonumber(redis.call('GET', q .. ':done') or '0'), tonumber(redis.call('GET', q .. ':id') or '0'), "
             "redis.call('ZCARD', q .. ':locks')}",
    "rlim": "local t = redis.call('TIME') local now = t[1] * 1000 + math.floor(t[2] / 1000) local win = tonumber(ARGV[1]) "
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - win) local n = redis.call('ZCARD', KEYS[1]) "
            "if n < tonumber(ARGV[2]) then redis.call('ZADD', KEYS[1], now, t[1] .. '-' .. t[2] .. '-' .. ARGV[3]) "
            "redis.call('PEXPIRE', KEYS[1], win) return 1 end return 0",
    # deterministic users of the clock (the model compares values): TIME's shape only
    "tshape": "local t = redis.call('TIME') redis.call('RPUSH', KEYS[1], ARGV[1]) "
              "return {#t, tonumber(t[2]) < 1000000, redis.call('LLEN', KEYS[1])}",
    "unlock": "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
    "lmv": "redis.call('RPUSH', KEYS[1], ARGV[1], ARGV[1] .. 'x') local a = redis.call('RPOPLPUSH', KEYS[1], KEYS[2]) "
           "local b = redis.call('LMOVE', KEYS[2], KEYS[1], 'LEFT', 'RIGHT') redis.call('LTRIM', KEYS[1], -20, -1) "
           "return {a, b, redis.call('LRANGE', KEYS[1], 0, -1), redis.call('LPOS', KEYS[1], ARGV[1])}",
    "hcnt": "local n = redis.call('HINCRBY', KEYS[1], 'n', tonumber(ARGV[1])) redis.call('HSETNX', KEYS[1], 'f', ARGV[2]) "
            "redis.call('HDEL', KEYS[1], 'gone') return {n, redis.call('HLEN', KEYS[1]), redis.call('HMGET', KEYS[1], 'n', 'f', 'x'), "
            "redis.call('HEXISTS', KEYS[1], 'f'), redis.call('TYPE', KEYS[1])}",
    "zwin": "redis.call('ZADD', KEYS[1], ARGV[1], 'm' .. ARGV[1], ARGV[1] + 1, 'n' .. ARGV[1]) "
            "redis.call('ZINCRBY', KEYS[1], 2, 'm' .. ARGV[1]) local c = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1] - 20) "
            "return {c, redis.call('ZCARD', KEYS[1]), redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1] - 5, '+inf', 'WITHSCORES'), "
            "redis.call('ZSCORE', KEYS[1], 'm' .. ARGV[1]), redis.call('ZRANK', KEYS[1], 'n' .. ARGV[1]), redis.call('ZCOUNT', KEYS[1], 0, 1000)}",
    "zpop": "redis.call('ZADD', KEYS[1], 5, 'a', 6, 'b') local p = redis.call('ZPOPMIN', KEYS[1]) "
            "redis.call('SADD', KEYS[2], ARGV[1], 'k') redis.call('SREM', KEYS[2], 'zz') "
            "return {p, redis.call('SCARD', KEYS[2]), redis.call('SMISMEMBER', KEYS[2], ARGV[1], 'no'), redis.call('SISMEMBER', KEYS[2], 'k')}",
    "mixed": "redis.call('SET', KEYS[1] .. ':s', ARGV[1], 'EX', 100000) redis.call('APPEND', KEYS[1] .. ':s', 'z') "
             "redis.call('LPUSH', KEYS[1] .. ':l', ARGV[1]) redis.call('HSET', KEYS[1] .. ':h', ARGV[1], 1) "
             "redis.call('PERSIST', KEYS[1] .. ':s') redis.call('EXPIREAT', KEYS[1] .. ':l', 4000000000) "
             "return {redis.call('STRLEN', KEYS[1] .. ':s'), redis.call('TTL', KEYS[1] .. ':s'), redis.call('LINDEX', KEYS[1] .. ':l', 0), "
             "redis.call('GETSET', KEYS[1] .. ':g', ARGV[1]), redis.call('SETNX', KEYS[1] .. ':n', 1)}",
    "nils": "if ARGV[1] == '1' then return false elseif ARGV[1] == '2' then return {1, false, 'x'} "
            "elseif ARGV[1] == '3' then return true elseif ARGV[1] == '4' then return {redis.call('LPOP', KEYS[1] .. ':nolist')} "
            "else return redis.call('HGET', KEYS[1] .. ':nohash', 'f') end",
    # aborts of this gene's kinds (main runs them; replies and keyspace must still equal the reference's)
    "wrongt": "redis.call('LPUSH', KEYS[1], ARGV[1]) redis.call('ZADD', KEYS[2], 1, ARGV[1]) return redis.call('HSET', KEYS[1], 'f', 1)",
    "bigl": "local t = {} for i = 1, 300 do t[i] = i end redis.call('RPUSH', KEYS[1], unpack(t)) local n = redis.call('LLEN', KEYS[1]) "
            "if n > 5000 then redis.call('LTRIM', KEYS[1], 0, 99) end return n",
    "lforeign": "redis.call('LPUSH', KEYS[1], ARGV[1]) redis.call('HSET', KEYS[2], 'f', ARGV[1]) return redis.call('RPUSH', ARGV[2], 1)",
    "hvol": "redis.call('HSET', KEYS[1], 'v', ARGV[1]) redis.call('HEXPIRE', KEYS[1], 100000, 'FIELDS', 1, 'v') "
            "return redis.call('HGET', KEYS[1], 'v')",
    "bigz": "local t = {} for i = 1, 200 do t[2 * i - 1] = i t[2 * i] = ARGV[1] .. i end redis.call('ZADD', KEYS[1], unpack(t)) "
            "local n = redis.call('ZCARD', KEYS[1]) if n > 700 then redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -50) end return n",
    "spop": "redis.call('SADD', KEYS[1], 'a', 'b') return redis.call('SCARD', KEYS[1]) + #redis.call('SPOP', KEYS[1], 5)",
}
NEW_ABORTS = {"wrongt", "lforeign", "spop", "hvol"}
NOW_NATIVE = {"hset", "strlen", "time"}  # RL-g1f's abort scripts this gene runs on the owner


def install():
    ds.SCRIPTS.update(NEW)
    ds.ABORT_SCRIPTS = (ds.ABORT_SCRIPTS - NOW_NATIVE) | NEW_ABORTS
    old_gen = ds.gen_ops

    def gen_ops(rnd, groups, foreign_keys, nops, shas):
        ops = old_gen(rnd, groups, foreign_keys, nops, {k: v for k, v in shas.items() if k not in NEW})
        out = []
        for tag, port, args in ops:
            if rnd.random() < 0.45:  # replace by one of this gene's scripts on the same group
                name = rnd.choice(sorted(set(NEW) - {"rlim"}))  # rlim's members carry the clock
                if name in NEW_ABORTS and rnd.random() < 0.6:
                    name = rnd.choice(["jadd", "jtake", "jdone", "lmv", "hcnt", "zwin"])
                ka, kb = "{%s}:a" % tag, "{%s}:b" % tag
                q = "{%s}" % tag
                v = "v%d" % rnd.randrange(50)
                if name in ("jadd",):
                    a = (1, q, v, rnd.randrange(1000))
                elif name in ("jtake",):
                    a = (1, q, rnd.randrange(1000))
                elif name in ("jdone", "jread"):
                    a = (1, q)
                elif name == "rlim":
                    a = (1, "{%s}:r" % tag, 1000, 1000000, v)  # never the limit: its reply is always 1
                elif name in ("tshape", "unlock", "nils", "bigl", "bigz", "mixed"):
                    a = (1, {"tshape": "{%s}:t" % tag, "bigl": "{%s}:big" % tag, "bigz": "{%s}:bz" % tag,
                             "mixed": q}.get(name, ka),
                         rnd.choice(["1", "2", "3", "4", "5", v]) if name in ("nils", "unlock") else v)
                elif name in ("lmv", "zpop"):
                    a = (2, "{%s}:l1" % tag if name == "lmv" else "{%s}:z" % tag,
                         "{%s}:l2" % tag if name == "lmv" else "{%s}:s" % tag, v)
                elif name == "hcnt":
                    a = (1, "{%s}:h" % tag, rnd.randrange(1, 9), v)
                elif name == "zwin":
                    a = (1, "{%s}:z2" % tag, rnd.randrange(10, 400))
                elif name == "lforeign":
                    a = (2, "{%s}:l1" % tag, "{%s}:h" % tag, v, rnd.choice(foreign_keys))
                elif name == "hvol":
                    a = (1, "{%s}:hv" % tag, v)
                else:  # wrongt, spop
                    a = (2, "{%s}:w1" % tag, "{%s}:w2" % tag, v) if name == "wrongt" else (1, "{%s}:sp" % tag)
                # EVAL of a body nobody loaded (the cache keeps it after the first): 30% of them
                if rnd.random() < 0.3:  # a variant of the body nobody loaded (EVAL caches it)
                    args = ("EVAL", NEW[name] + " -- e") + a
                else:
                    args = ("EVALSHA", shas[name]) + a
            out.append((tag, port, args))
        return out

    ds.gen_ops = gen_ops


def raw_read(f):
    """One reply as raw bytes (RESP2 or RESP3)."""
    line = f.readline()
    if not line:
        raise EOFError("closed")
    t = line[:1]
    out = [line]
    if t in (b"$", b"=", b"!"):
        n = int(line[1:-2])
        if n >= 0:
            out.append(f.read(n + 2))
    elif t in (b"*", b"~", b">", b"%", b"|"):
        n = int(line[1:-2])
        if n > 0:
            for _ in range(n * (2 if t in (b"%", b"|") else 1)):
                out.append(raw_read(f))
    return b"".join(out)


def keyspace_full(srv):
    c = srv.conn()
    out = {}
    cur = "0"
    while True:
        r = c.cmd("SCAN", cur, "COUNT", 1000)
        cur = r[0].decode()
        for k in r[1]:
            if k.startswith(b"bg"):  # --bg load keys (the build only)
                continue
            t = c.cmd("TYPE", k)
            enc = c.cmd("OBJECT", "ENCODING", k)
            enc = enc.decode() if isinstance(enc, bytes) else enc
            if t == "string":
                v = c.cmd("GET", k)
            elif t == "hash":
                v = sorted(zip(*[iter(c.cmd("HGETALL", k) or [])] * 2))
            elif t == "list":
                v = c.cmd("LRANGE", k, 0, -1)
            elif t == "set":
                v = sorted(c.cmd("SMEMBERS", k) or [])
            elif t == "zset":
                v = c.cmd("ZRANGE", k, 0, -1, "WITHSCORES")
            else:
                v = None
            out[k] = (t, enc, v, c.cmd("PTTL", k))
        if cur == "0":
            break
    c.close()
    return out


def setup_vol(srv, n, tags):
    """Hashes with field expiry in some tags' tables (only main makes them): scripts on those tables' hashes go
    to main (hvol)."""
    c = srv.conn()
    for t in tags:
        c.cmd("HSET", "{%s}:hx" % t, "f", "1", "g", "2")
        c.cmd("HEXPIRE", "{%s}:hx" % t, 100000, "FIELDS", 1, "f")
    c.close()


def phase_resp3(a, A, B, shas, rnd):
    """RESP3 connections (HELLO 3) through the owner ports and main: every reply byte-equal to the reference's."""
    n = a.n
    names = sorted(set(NEW) - {"rlim", "lforeign", "hvol"}) + ["pair", "cnt", "status", "rmw" if "rmw" in shas else "pair"]
    plans = []
    for ci in range(8):
        tag = ds.tag_for_owner(rnd.randrange(n), n, rnd)
        ops = []
        for _ in range(a.ops // 3):
            name = rnd.choice(names)
            q, ka, kb, v = "{%s}" % tag, "{%s}:a" % tag, "{%s}:b" % tag, "v%d" % rnd.randrange(30)
            if name in ("jadd", "jtake", "jdone", "jread"):
                args = (1, q, v, 1) if name == "jadd" else (1, q, 5) if name == "jtake" else (1, q)
            elif name in ("lmv", "zpop", "wrongt", "pair", "cnt"):
                lt = {"lmv": ("{%s}:l1" % tag, "{%s}:l2" % tag), "zpop": ("{%s}:z" % tag, "{%s}:s" % tag),
                      "wrongt": ("{%s}:w1" % tag, "{%s}:w2" % tag)}.get(name, (ka, kb))
                args = (2,) + lt + ((2,) if name == "cnt" else (v,))
            elif name == "rmw":
                args = (1, ka, 100000)
            elif name == "loop":
                args = (1, ka, 10)
            elif name == "rlim":
                args = (1, "{%s}:r" % tag, 1000, 1000000, v)
            elif name in ("nils", "status"):
                args = (1, ka, rnd.choice("12345"))
            elif name == "hcnt":
                args = (1, "{%s}:h" % tag, 2, v)
            elif name == "zwin":
                args = (1, "{%s}:z2" % tag, rnd.randrange(10, 400))
            elif name in ("bigl", "bigz", "tshape", "spop"):
                args = (1, "{%s}:%s" % (tag, name), v)
            elif name == "mixed":
                args = (1, q, v)
            else:
                args = (1, ka, v)
            body = ds.SCRIPTS.get(name) or ds.RMW
            ops.append(("EVAL", body) + args if rnd.random() < 0.3 else ("EVALSHA", shas[name]) + args)
        plans.append((tag, ops, rnd.random() < 0.8))
    res = {}
    o0 = ds.owners_info(A)
    for srv, label in ((A, "A"), (B, "B")):
        outs = []
        for tag, ops, own in plans:
            c = srv.conn(ds.owner_of_slot(key_slot(tag.encode()), n) if own else None)
            hello = c.cmd("HELLO", 3)
            got = []
            for i in range(0, len(ops), 4):
                batch = ops[i:i + 4]
                c.send(batch)
                got += [raw_read(c.f) for _ in batch]
            c.close()
            outs.append(got)
        res[label] = outs
    o1 = ds.owners_info(A)
    bad = []
    tot = 0
    for ci in range(len(plans)):
        for j, (x, y) in enumerate(zip(res["A"][ci], res["B"][ci])):
            tot += 1
            if x != y:
                bad.append((ci, j, plans[ci][1][j][:1] + plans[ci][1][j][2:4], x[:120], y[:120]))
    nulls = sum(r.count(b"_\r\n") for rr in res["A"] for r in rr)
    runs = int(o1["owner_script_runs"]) - int(o0["owner_script_runs"])
    ds.check("resp3.raw_replies", not bad and tot,
             "%d RESP3 replies byte-equal (%d with a RESP3 null), owner runs +%d" % (tot, nulls, runs) if not bad else bad[:3])
    ds.check("resp3.native", runs > tot * 0.1, "owner script runs +%d of %d RESP3 calls (the rest: aborts of the test's "
             "abort scripts and WRONGTYPE mixes, first calls per sha, main-port connections)" % (runs, tot))


def phase_jobs(a, A, shas):
    n = a.n
    rnd = random.Random(a.seed + 41)
    queues = [ds.tag_for_owner(i % n, n, rnd) for i in range(2 * n)]
    lims = [ds.tag_for_owner(i % n, n, rnd) for i in range(n)]
    c = A.conn()
    for t in queues:
        c.cmd("DEL", "{%s}:id" % t, "{%s}:wait" % t, "{%s}:active" % t, "{%s}:done" % t, "{%s}:locks" % t)
    c.close()
    stop = [False]
    bad, errs, added, lim_bad = [], [], [0], []
    lock = threading.Lock()
    per = a.ops // 2

    def worker(i):
        rr = random.Random(a.seed * 100 + i)
        cc = A.conn(None if i % 5 == 0 else (i % n))
        home = [q for j, q in enumerate(queues) if j % n == i % n]  # clients route by slot: mostly their owner's
        mine = 0
        for _ in range(per // 4):
            batch = []
            for _ in range(4):
                t = rr.choice(home) if rr.random() < 0.8 else rr.choice(queues)
                k = rr.random()
                if k < 0.4:
                    batch.append(("EVALSHA", shas["jadd"], 1, "{%s}" % t, "d", 1))
                elif k < 0.7:
                    batch.append(("EVALSHA", shas["jtake"], 1, "{%s}" % t, 2))
                elif k < 0.9:
                    batch.append(("EVALSHA", shas["jdone"], 1, "{%s}" % t))
                else:
                    lt = rr.choice(lims)
                    batch.append(("EVALSHA", shas["rlim"], 1, "{%s}:r" % lt, 200, 25, "w%d" % i))
            cc.send(batch)
            for b in batch:
                r = cc.read()
                if isinstance(r, Err):
                    errs.append(r)
                elif b[1] == shas["jadd"]:
                    mine += 1
        cc.close()
        with lock:
            added[0] += mine

    def reader(i):
        cc = A.conn(i % n)
        rr = random.Random(i)
        while not stop[0]:
            t = rr.choice(queues)
            v = cc.cmd("EVALSHA", shas["jread"], 1, "{%s}" % t)
            if isinstance(v, Err):
                errs.append(v)
            elif v[0] + v[1] + v[2] != v[3] or v[4] != v[1]:
                bad.append((t, v))
            lt = rr.choice(lims)
            z = cc.cmd("ZCARD", "{%s}:r" % lt)
            if isinstance(z, int) and z > 25:
                lim_bad.append(z)
        cc.close()

    ths = [threading.Thread(target=worker, args=(i,)) for i in range(4 * n)]
    rths = [threading.Thread(target=reader, args=(i,)) for i in range(n)]
    o0 = ds.owners_info(A)
    for t in rths + ths:
        t.start()
    for t in ths:
        t.join()
    stop[0] = True
    for t in rths:
        t.join()
    o1 = ds.owners_info(A)
    c = A.conn()
    tot_id = tot = 0
    dup = 0
    for t in queues:
        ids = int(c.cmd("GET", "{%s}:id" % t) or 0)
        w = [int(x) for x in c.cmd("LRANGE", "{%s}:wait" % t, 0, -1)]
        ac = [int(x) for x in c.cmd("LRANGE", "{%s}:active" % t, 0, -1)]
        d = int(c.cmd("GET", "{%s}:done" % t) or 0)
        jobs = sum(1 for x in w + ac if c.cmd("EXISTS", "{%s}:job:%d" % (t, x)))
        dup += len(w + ac) - len(set(w + ac))
        if len(w) + len(ac) + d != ids or jobs != len(w) + len(ac):
            bad.append(("final", t, ids, len(w), len(ac), d, jobs))
        tot_id += ids
        tot += len(w) + len(ac) + d
    c.close()
    runs = int(o1["owner_script_runs"]) - int(o0["owner_script_runs"])
    ds.check("jobs.conservation", not bad and not dup and tot_id == added[0] == tot,
             "%d jobs added, wait + active + done = %d, no duplicate, every queued job has its hash" % (added[0], tot)
             if not bad else bad[:3])
    ds.check("jobs.atomic_reads", not bad and not errs, "reader script never saw a torn state; errors %d" % len(errs))
    ds.check("jobs.limiter", not lim_bad, "rate-limit windows never above 25 (%d over)" % len(lim_bad))
    dd = lambda k: int(o1.get(k, 0)) - int(o0.get(k, 0))
    ds.check("jobs.native", runs > per, "owner script runs +%d of %d calls (aborts +%d, misses +%d, handoffs +%d)" % (
        runs, 4 * n * per, dd("owner_script_aborts"), dd("owner_script_misses"), dd("owner_handoffs_to_main")))


def phase_evallru(a, A, B):
    n = a.n
    hot = "redis.call('INCR', KEYS[1]) return redis.call('GET', KEYS[1])"
    out = {}
    for srv, label in ((A, "A"), (B, "B")):
        rr = random.Random(a.seed + 51)
        t = ds.tag_for_owner(rr.randrange(n), n, rr)
        ow = ds.owner_of_slot(key_slot(t.encode()), n)
        c, h = srv.conn(), srv.conn(ow)
        c.cmd("SCRIPT", "FLUSH")
        ev0 = int(ds.info(c, "stats").get("evicted_scripts", 0))
        reps = []
        for i in range(600):
            c.cmd("EVAL", "return %d" % i, 0)
            if i % 50 == 0:
                for _ in range(3):
                    reps.append(h.cmd("EVAL", hot, 1, "{%s}:hot" % t))
                time.sleep(0.01)  # main's next stop moves the owners' runs to the LRU tail
        sha = c.cmd("SCRIPT", "LOAD", "return 'x'")  # a stop between the owners' runs and the checks below
        import hashlib
        hsha = hashlib.sha1(hot.encode()).hexdigest()
        reps.append(ds.norm(h.cmd("EVALSHA", hsha, 1, "{%s}:hot" % t)))
        cold = [ds.norm(c.cmd("EVALSHA", hashlib.sha1(("return %d" % i).encode()).hexdigest(), 0)) for i in (0, 50, 99, 100, 200, 599)]
        ev = int(ds.info(c, "stats").get("evicted_scripts", -1)) - ev0
        c.close()
        h.close()
        out[label] = (reps, cold, ev)
    ds.check("evallru.equal", out["A"] == out["B"] and str(out["A"][1][0]).startswith("ERR:NOSCRIPT"),
             "hot EVALSHA %s, cold %s, evicted %d" % (out["A"][0][-1], out["A"][1], out["A"][2]) if out["A"] == out["B"]
             else (out["A"], out["B"]))


def bg_load(srv, n, stop, counter):
    def run(i):
        rr = random.Random(1000 + i)
        cc = srv.conn(i % n)
        try:
            while not stop[0]:
                k = "%d:%d" % (i, rr.randrange(5000))
                cc.send([("SET", "bg:" + k, "x" * 64), ("GET", "bg:" + k), ("LPUSH", "bgl:" + k, 1), ("RPOP", "bgl:" + k),
                         ("HSET", "bgh:" + k, "f", 1), ("ZADD", "bgz:" + k, 1, "m")] * 4)
                for _ in range(24):
                    cc.read()
                counter[0] += 24
        except Exception:
            pass
        cc.close()
    ths = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(n * 2)]
    for t in ths:
        t.start()
    return ths


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, default=24600)
    ap.add_argument("--owner-port", type=int, default=24620)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", default="40-43")
    ap.add_argument("--main-cpu", default="44")
    ap.add_argument("--ref-cpus", default="45-48")
    ap.add_argument("--ref-main-cpu", default="49")
    ap.add_argument("--replica-cpu", default="50")
    ap.add_argument("--conns", type=int, default=16)
    ap.add_argument("--ops", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--aof", action="store_true")
    ap.add_argument("--replica", action="store_true")
    ap.add_argument("--bg", type=int, default=0)
    ap.add_argument("--suites", default="model,resp3,jobs,evallru,expire,atomic,watch,kill,flush")
    a = ap.parse_args()
    install()
    os.makedirs(a.dir, exist_ok=True)
    ds.keyspace = keyspace_full
    # d3-script-check's main with this gene's phases: run it with our args, then ours on the same servers.
    extra = ["--appendonly", "yes", "--appendfsync", "everysec"] if a.aof else []
    A = ds.Server(a, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, "build", extra)
    B = ds.Server(a, a.ref_bin, a.port + 10, a.owner_port + 20, a.ref_cpus, a.ref_main_cpu, "ref")
    R = None
    stop = [False]
    bgc = [0]
    try:
        if a.replica:
            import shutil
            import subprocess
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
                    up = ds.info(rc, "replication").get("master_link_status") == "up"
                    rc.close()
                except Exception:
                    pass
                if up:
                    break
                time.sleep(0.05)
            o = ds.info(c, "owners")
            c.close()
            ds.check("replica.owners_native", up and o.get("owner_veto") == "none",
                     "link up %s, veto %s with a replica" % (up, o.get("owner_veto")))
        shas = {}
        for srv in (A, B):
            c = srv.conn()
            shas = {k: c.cmd("SCRIPT", "LOAD", v).decode() for k, v in ds.SCRIPTS.items()}
            if ds.RMW:
                shas["rmw"] = c.cmd("SCRIPT", "LOAD", ds.RMW).decode()
            c.close()
        rnd = random.Random(a.seed)
        vt = [ds.tag_for_owner(i % a.n, a.n, rnd) for i in range(a.n)]
        for srv in (A, B):
            setup_vol(srv, a.n, vt)
        suites = a.suites.split(",")
        if a.bg:
            bg_load(A, a.n, stop, bgc)
        if "model" in suites:
            ds.phase_model(a, A, B, shas, rnd)
        if "resp3" in suites:
            phase_resp3(a, A, B, shas, rnd)
        if "jobs" in suites:
            phase_jobs(a, A, shas)
        if "evallru" in suites:
            phase_evallru(a, A, B)
        if "expire" in suites:
            ds.phase_expire(a, A, B, shas)
        if "atomic" in suites:
            ds.phase_atomic(a, A, shas)
        if "watch" in suites:
            ds.phase_watch(a, A, B, shas)
        if "kill" in suites:
            ds.phase_kill(a, A, B)
        if "flush" in suites:
            ds.phase_flush(a, A, shas)
        stop[0] = True
        time.sleep(0.3)
        if a.bg:
            ds.check("bg.load", bgc[0] > 0, "background load commands %d" % bgc[0])
        if R is not None:
            c = A.conn()
            ok = False
            for _ in range(600):
                p = int(ds.info(c, "replication")["master_repl_offset"])
                rc = Conn("127.0.0.1", a.port + 1)
                ro = int(ds.info(rc, "replication").get("slave_repl_offset", -1))
                rc.close()
                if p == ro:
                    ok = True
                    break
                time.sleep(0.05)
            c.close()
            d1, d2 = ds.digest(a.port), ds.digest(a.port + 1)
            ds.check("replica.digest", ok and d1 == d2, "offset caught up %s, digest %s / %s" % (ok, d1, d2))
        if a.aof:
            import shutil
            import subprocess
            d1 = ds.digest(a.port)
            c = A.conn()
            if a.seed % 2:
                c.cmd("BGREWRITEAOF")
            time.sleep(1.5)
            c.close()
            A.stop()
            fdir = os.path.join(a.dir, "aofload")
            shutil.rmtree(fdir, ignore_errors=True)
            shutil.copytree(os.path.join(A.dir, "appendonlydir"), os.path.join(fdir, "appendonlydir"))
            F = subprocess.Popen([a.bin, "--port", str(a.port + 2), "--bind", "127.0.0.1", "--save", "", "--appendonly",
                                  "yes", "--server-cpulist", a.replica_cpu, "--dir", fdir, "--logfile",
                                  os.path.join(fdir, "log"), "--enable-debug-command", "yes"])
            d2 = None
            for _ in range(400):
                try:
                    c = Conn("127.0.0.1", a.port + 2, timeout=5)
                    if ds.info(c, "persistence").get("loading") == "0":
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
            ds.check("aof.reload_digest", d1 == d2, "build %s, reloaded %s" % (d1, d2))
            shutil.rmtree(fdir, ignore_errors=True)
        ds.check("no_crash", A.crashed() == 0 and B.crashed() == 0, "crash lines build %s ref %s" % (A.crashed(), B.crashed()))
    finally:
        stop[0] = True
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
    nfail = sum(1 for r in ds.RESULTS if not r["ok"])
    print("RLSCRIPT " + json.dumps(dict(pass_=len(ds.RESULTS) - nfail, fail=nfail, seed=a.seed, bg=a.bg, aof=a.aof,
                                        replica=a.replica)))
    sys.exit(1 if nfail else 0)


if __name__ == "__main__":
    main()
