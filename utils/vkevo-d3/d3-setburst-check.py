#!/usr/bin/env python3
"""D3-all-g10-setburst check for the owners' SET bursts (D3-all-g11-maxmem: suites mm / mmaofes / mmoff added for
memory leases; run it with --ref-bin = D3-all-g10-setburst, whose SETs under maxmemory take the parser's path) (owner.c ownerSetBurst: the leading plain
"SET key value" frames of a read run straight off the query buffer), against the parent's owner path
(D3-all-g9-fold), byte for byte.

Starts two servers with owner-threads 4 on 127.0.0.1, the tree's (--bin) and the parent's (--ref-bin), sets
up the same data on both through the plain port, sends the same bytes to owner 0's port of each and
compares the reply bytes and what each case added to keyspace hits / misses, cmdstat_set / cmdstat_get
calls and the dirty count (rdb_changes_since_last_save). At the end of each suite it compares the whole
keyspace (every key's TYPE, OBJECT ENCODING, PEXPIRETIME, MEMORY USAGE and DEBUG DIGEST-VALUE) and, with
appendonly on, the incremental AOF files byte for byte, then DEBUG LOADAOF on both and the keyspace again.
TTLs are set with absolute times only (PEXPIREAT, PXAT) and active expire is off (DEBUG SET-ACTIVE-EXPIRE 0),
so the AOF of both servers is the same bytes.
Suites (fresh servers each):
  plain     appendonly no: every case below
  aof       appendonly yes, appendfsync always: every case + WAITAOF after bursts
  aofes     appendonly yes, appendfsync everysec: every case + WAITAOF
  mm        maxmemory 1gb allkeys-lru (D3-all-g11-maxmem: inside the memory lease SET bursts run): every case
  mmaofes   maxmemory 1gb allkeys-lru + appendonly everysec: every case + WAITAOF (AOF bytes as the ref's)
  mmoff     maxmemory 1gb allkeys-lru + owner-mm-lease no on the tree's server only: no SET burst runs (the tree's
            owner_setburst_commands +0), same replies
  notify    notify-keyspace-events KEA: no SET burst; the keyspace events a subscriber got are the same bytes
  replica   a replica attached to each: no SET burst; same replies; the replica's DEBUG DIGEST is its primary's
  rename    rename-command set myset: no SET burst; SET unknown, MYSET runs
  off       owner-set-burst no on the tree's server only: no SET burst; same replies
Cases: fresh (the first SETs of an empty server: slots with no table), keys 1-65 B, values 0-100 B, integer and
non-canonical number values, SET / set / Set; rounds (each read one pipeline: bursts run); chunks (random 1-200
byte writes); deep (2000 in one write); dup (one key set twice in a burst, GET between); mixed (random
pipelines of SET with options, GET, INCR, DEL, APPEND, MGET, MSET, EXISTS, TYPE, STRLEN, PEXPIREAT, PERSIST,
RPUSH, HSET, PING); overwrite (lists, hashes incl. a field TTL, sets, zsets, streams overwritten by SET);
ttl (far TTL cleared, expired key lazily deleted through the parser, PERSISTed field, SET KEEPTTL / PXAT);
watch (WATCH aborted by another client's burst, by the client's own burst, not aborted by other keys);
multi; reply (CLIENT REPLY SKIP / OFF / ON); resp3; acl (a key-pattern user: NOPERM; an unrestricted
non-default user); cross (another owner's keys in the pipeline); tracking (an invalidation push for a key a
burst sets); slowlog (every SET logged with its arguments); reqlog (request-larger-than between frame sizes,
reply-larger-than under +OK: bursts off).
Usage: d3-setburst-check.py --bin <tree>/src/valkey-server --ref-bin <parent>/src/valkey-server --dir D
                            --port P --owner-port OP [--cpus 8-11] [--main-cpu 16] [--only suite,...]
Prints PASS/FAIL/INFO lines and SETBURSTCHECK {checks, fails}; exit 1 on any FAIL.
"""
import argparse
import glob
import json
import os
import random
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS
bc = __import__("d3-burst-check")  # Raw, Server, key_in, ALNUM

ALNUM = bc.ALNUM
FAR = 4102444800000  # 2100-01-01 in ms: a TTL that never ends in a run


def enc(*args):
    return d3.Conn.enc(args)


def ostat(s, k):
    return int(d3.ostat(s.plain).get(k, 0))


def stats(s):
    st = d3.info(s.plain, "stats")
    cs = d3.info(s.plain, "commandstats")
    pe = d3.info(s.plain, "persistence")

    def calls(name):
        return int(cs[name].split("calls=")[1].split(",")[0]) if name in cs else 0

    return dict(hits=int(st["keyspace_hits"]), misses=int(st["keyspace_misses"]), set_calls=calls("cmdstat_set"),
                get_calls=calls("cmdstat_get"), dirty=int(pe["rdb_changes_since_last_save"]),
                sb=ostat(s, "owner_setburst_commands"))


class Suite:
    def __init__(self, name, a, extra=(), extra_tree=(), aof=False):
        self.name, self.a, self.aof = name, a, aof
        d = os.path.join(a.dir, name)
        self.tree = bc.Server("sb", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, d, list(extra) + list(extra_tree))
        self.ref = bc.Server("ref", a.ref_bin, a.port + 10, a.owner_port + 20, a.cpus, a.main_cpu, d, list(extra))
        self.srvs = [self.tree, self.ref]
        d3.check("sb.%s.topology" % name, self.tree.ranges == self.ref.ranges and len(self.tree.ranges) == 4,
                 self.tree.ranges)
        for s in self.srvs:
            s.plain.cmd("DEBUG", "SET-ACTIVE-EXPIRE", "0")
            s.plain.cmd("CONFIG", "SET", "slowlog-max-len", "100000")
        self.sb_total = 0

    def setup(self, cmds):
        outs = []
        for s in self.srvs:
            s.plain.send(cmds)
            outs.append([s.plain.read() for _ in cmds])
        same = [repr(x) for x in outs[0]] == [repr(x) for x in outs[1]]
        if not same:
            d3.check("sb.%s.setup" % self.name, False, "%r / %r" % (outs[0][:5], outs[1][:5]))
        return outs[0]

    def case(self, name, data=None, nrep=0, chunks=None, prelude=None, port_idx=0, rounds=None):
        """data (nrep replies) or rounds [(bytes, replies)] to owner port_idx of both; the replies and the
        stats deltas must match. Returns the SET bursts' commands on the tree's server."""
        outs, deltas = [], []
        for s in self.srvs:
            st0 = stats(s)
            r = bc.Raw(s.oport + port_idx)
            if prelude:
                r.send(prelude[0])
                r.replies(prelude[1])
            got = []
            if rounds:
                for rb, rn in rounds:
                    r.send(rb)
                    got += r.replies(rn)
            else:
                r.send(data, chunks, random.Random(len(data)))
                got = r.replies(nrep)
            r.close()
            st1 = stats(s)
            outs.append(got)
            deltas.append({k: st1[k] - st0[k] for k in st0})
        n = len(outs[0])
        same = outs[0] == outs[1]
        first = next((i for i in range(n) if outs[0][i] != outs[1][i]), -1) if not same else -1
        tag = "sb.%s.%s" % (self.name, name)
        d3.check(tag + ".replies", same, "%d replies, %d bytes" % (n, sum(map(len, outs[0]))) if same else
                 "reply %d: tree %r ref %r" % (first, outs[0][first][:120], outs[1][first][:120]))
        plain = [{k: v for k, v in dl.items() if k != "sb"} for dl in deltas]
        d3.check(tag + ".stats", plain[0] == plain[1], "tree %s ref %s" % (deltas[0], plain[1]))
        self.sb_total += deltas[0]["sb"]
        return deltas[0]["sb"]

    def keyspace(self, tag):
        views = []
        for s in self.srvs:
            keys = sorted(s.plain.cmd("KEYS", "*"))
            v = {}
            for k in keys:
                s.plain.send([("TYPE", k), ("OBJECT", "ENCODING", k), ("PEXPIRETIME", k), ("MEMORY", "USAGE", k),
                              ("DEBUG", "DIGEST-VALUE", k)])
                v[k] = tuple(repr(s.plain.read()) for _ in range(5))
            views.append(v)
        a, b = views
        diff = [k for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]
        d3.check("sb.%s.keyspace%s" % (self.name, tag), not diff and len(a) > 0,
                 "%d keys (type, encoding, pexpiretime, memory usage, digest)" % len(a) if not diff else
                 "%d differ, first %r: tree %r ref %r" % (len(diff), diff[0], a.get(diff[0]), b.get(diff[0])))

    def aof_files(self):
        out = []
        for s in self.srvs:
            files = sorted(glob.glob(os.path.join(s.dir, "appendonlydir", "*.incr.aof")))
            out.append([(os.path.basename(f), open(f, "rb").read()) for f in files])
        return out

    def finish(self):
        self.keyspace("")
        if self.aof:
            time.sleep(1.2)
            fa, fb = self.aof_files()
            same = fa == fb and fa and len(fa[-1][1]) > 0
            detail = "%d incr file(s), %d bytes" % (len(fa), sum(len(x[1]) for x in fa))
            if not same and fa and fb and len(fa) == len(fb):
                x, y = fa[-1][1], fb[-1][1]
                i = next((j for j in range(min(len(x), len(y))) if x[j] != y[j]), min(len(x), len(y)))
                detail = "differ at byte %d: tree %r ref %r (sizes %d / %d)" % (i, x[max(0, i - 60):i + 60],
                                                                              y[max(0, i - 60):i + 60], len(x), len(y))
            elif not same:
                detail = "tree %r ref %r" % ([(n, len(b)) for n, b in fa], [(n, len(b)) for n, b in fb])
            d3.check("sb.%s.aof_bytes" % self.name, same, detail)
            for s in self.srvs:
                s.plain.cmd("DEBUG", "LOADAOF")
                s.plain.cmd("DEBUG", "SET-ACTIVE-EXPIRE", "0")
            self.keyspace(".after_loadaof")

    def stop(self):
        for s in self.srvs:
            alive = all(d3.Conn("127.0.0.1", s.oport + i).cmd("PING") == "PONG" for i in range(4))
            d3.check("sb.%s.%s.alive" % (self.name, s.name), alive and s.plain.cmd("PING") == "PONG")
            s.stop()
            d3.check("sb.%s.%s.no_crash" % (self.name, s.name), s.crashed() == 0, "crash lines %d" % s.crashed())


def run_cases(su, rng, full=True):
    tree, ref = su.tree, su.ref
    lo0, hi0 = tree.ranges[0]
    lo1, hi1 = tree.ranges[1]
    key_in = bc.key_in
    keys = [key_in(rng, L, lo0, hi0) for L in range(1, 66)]  # 64 is the burst's longest
    keys += [k for k in (key_in(rng, 12, lo0, hi0, b"{") for _ in range(4)) if lo0 <= d3.key_slot(k) <= hi0]
    tagn = next(i for i in range(100000) if lo0 <= d3.key_slot(b"tg%d" % i) <= hi0)
    keys += [b"{tg%d}:%d" % (tagn, j) for j in range(6)]
    other = [key_in(rng, 16, lo1, hi1) for _ in range(6)]
    vlens = [1, 2, 9, 10, 11, 15, 16, 17, 40, 50, 63, 64, 65, 80, 98, 99]
    odd_vals = [b"", b"0", b"7", b"-7", b"12345", b"007", b"1.5", b"-0", b"+1", b" 1", b"99999999999999999999",
                b"9223372036854775807", b"x" * 100, b"y" * 120]

    def val(i):
        if i % 9 == 4:
            return odd_vals[(i // 9) % len(odd_vals)]
        return bytes(rng.choice(ALNUM) for _ in range(vlens[i % len(vlens)]))

    def setf(k, v, sp=b"SET"):
        return enc(sp, k, v)

    # fresh: the first SETs of an empty server (no table yet for most slots), then GETs of every key
    frames = []
    for i in range(240):
        k = keys[i % len(keys)]
        sp = b"SET" if i % 6 else (b"set" if i % 12 else b"Set")
        frames.append(setf(k, val(i), sp))
    frames[0] = setf(keys[0], b"first")
    b_fresh = su.case("fresh", b"".join(frames), 240)
    su.case("fresh_get", b"".join(enc("GET", k) for k in keys), len(keys))

    # rounds: each read one pipeline that starts with a SET frame (100 frames fit one 16 KB read)
    rounds = []
    burstable = [k for k in keys if len(k) <= 64]
    for _ in range(10):
        rounds.append((b"".join(setf(rng.choice(burstable), bytes(rng.choice(ALNUM) for _ in range(rng.choice(vlens))),
                                     b"set" if rng.random() < 0.2 else b"SET") for _ in range(100)), 100))
    b_rounds = su.case("rounds", rounds=rounds)
    short = [k for k in burstable if len(k) <= 16]
    b_rounds += su.case("deep_rounds", rounds=[(b"".join(setf(rng.choice(short), bytes(rng.choice(ALNUM) for _ in range(16)))
                                                         for _ in range(250)), 250) for _ in range(4)])
    su.case("chunks", b"".join(setf(rng.choice(keys), val(i), b"SET" if i % 7 else b"set") for i in range(300)), 300,
            chunks=200)
    su.case("chunks_small", b"".join(setf(rng.choice(keys), val(i)) for i in range(60)), 60, chunks=7)
    b_deep = su.case("deep", b"".join(setf(rng.choice(burstable), bytes(rng.choice(ALNUM) for _ in range(16)))
                                      for _ in range(2000)), 2000)
    # one SET per write: a burst of one each
    su.case("single", rounds=[(setf(k, b"s1-" + k[:8]), 1) for k in keys[:40]])

    # dup: one key set twice in one burst, a GET between and after
    a0, a1, a2 = keys[15], keys[20], keys[40]
    dup = b"".join([setf(a0, "d1"), setf(a0, "d2"), enc("GET", a0), setf(a0, "d3"), setf(a1, "d4"), setf(a0, "d5"),
                    enc("GET", a0), enc("GET", a1)] * 4)
    su.case("dup", dup, 32)

    # mixed: random pipelines of SET (plain and with options) and other commands on the same keys
    pool = keys[:30] + other[:2] + [b"cnt{tg%d}" % tagn]

    def rnd_cmd(r):
        k = r.choice(pool)
        x = r.random()
        if x < 0.45:
            return setf(k, bytes(r.choice(ALNUM) for _ in range(r.choice(vlens))), b"SET" if r.random() < 0.8 else b"set")
        if x < 0.50:
            return setf(k, b"%d" % r.randint(-50, 10**6))
        opts = [("NX",), ("XX",), ("GET",), ("KEEPTTL",), ("PXAT", str(FAR)), ("EXAT", str(FAR // 1000))]
        if x < 0.58:
            return enc("SET", k, "o%d" % r.randint(0, 99), *r.choice(opts))
        menu = [lambda: enc("GET", k), lambda: enc("INCR", k), lambda: enc("DEL", k), lambda: enc("APPEND", k, "ap"),
                lambda: enc("MGET", k, r.choice(pool)), lambda: enc("EXISTS", k), lambda: enc("TYPE", k),
                lambda: enc("STRLEN", k), lambda: enc("PEXPIREAT", k, str(FAR - r.randint(0, 1000))),
                lambda: enc("PERSIST", k), lambda: enc("PEXPIRETIME", k), lambda: enc("PING"),
                lambda: enc("RPUSH", k, "l"), lambda: enc("HSET", k, "f", "v"), lambda: enc("OBJECT", "ENCODING", k),
                lambda: enc("MSET", k, "m1", k, "m2"), lambda: enc("GETDEL", k), lambda: enc("SETNX", k, "nx")]
        return r.choice(menu)()

    for seed in range(3 if full else 1):
        r = random.Random(1000 + seed)
        cmds = [rnd_cmd(r) for _ in range(400)]
        su.case("mixed%d" % seed, b"".join(cmds), 400)
        su.case("mixed%d_chunks" % seed, b"".join(cmds), 400, chunks=150)
        # the same commands as rounds of 20 (each read starts at a frame)
        su.case("mixed%d_rounds" % seed, rounds=[(b"".join(cmds[j:j + 20]), 20) for j in range(0, 400, 20)])

    if not full:
        return b_fresh + b_rounds + b_deep
    # overwrite: other types overwritten by a burst's SET
    ow = [key_in(rng, 16, lo0, hi0, b"ow") for _ in range(7)]
    su.setup([("RPUSH", ow[0], "a", "b", "c"), ("HSET", ow[1], "f1", "v1", "f2", "v2"), ("SADD", ow[2], "m1", "m2"),
              ("ZADD", ow[3], "1", "z1"), ("XADD", ow[4], "1-1", "f", "v"), ("HSET", ow[5], "hf", "hv", "hg", "hw"),
              ("RPUSH", ow[6], *["e%d" % i for i in range(300)])])
    hx = su.setup([("HPEXPIREAT", ow[5], str(FAR), "FIELDS", "1", "hf")])
    print("INFO sb.%s.hpexpireat %r" % (su.name, hx))
    owr = [setf(a1, "pre")]
    for k in ow:
        owr += [setf(k, b"now-a-string-" + k[:4]), setf(a2, "mid")]
    owr += [enc("TYPE", k) for k in ow] + [enc("GET", k) for k in ow]
    su.case("overwrite", b"".join(owr), len(owr))

    # ttl: far TTLs cleared by a burst SET, an expired key lazily deleted (the parser's), a PERSISTed key
    # (expire field, no expire), SET KEEPTTL / PXAT (the parser's)
    tk = [key_in(rng, 16, lo0, hi0, b"ttl") for _ in range(8)]
    soon = int(time.time() * 1000) + 150
    su.setup([("SET", tk[0], "t0"), ("PEXPIREAT", tk[0], str(FAR)), ("SET", tk[1], "t1"), ("PEXPIREAT", tk[1], str(soon)),
              ("SET", tk[2], "t2", "PXAT", str(FAR)), ("PERSIST", tk[2]), ("SET", tk[3], "t3", "PXAT", str(FAR)),
              ("SET", tk[4], "t4"), ("PEXPIREAT", tk[4], str(soon)), ("SET", tk[5], "x" * 90, "PXAT", str(FAR)),
              ("PERSIST", tk[5]), ("SET", tk[6], "t6", "PXAT", str(FAR)), ("SET", tk[7], "t7")])
    time.sleep(0.4)
    ttl = [setf(tk[0], "n0"), setf(tk[1], "n1"), setf(tk[2], "n2"), enc("SET", tk[3], "n3", "KEEPTTL"), setf(tk[5], "n5"),
           setf(a1, "tt"), enc("SET", tk[6], "n6", "PXAT", str(FAR - 5)), setf(tk[7], "n7")]
    ttl += [enc("PEXPIRETIME", k) for k in tk] + [enc("GET", k) for k in tk] + [enc("EXISTS", tk[4])]
    su.case("ttl", b"".join(ttl), len(ttl))

    # watch: aborted by another client's burst; by the client's own burst; not by other keys
    wk = [key_in(rng, 16, lo0, hi0, b"wk") for _ in range(4)]
    su.setup([("SET", k, "w") for k in wk])
    outs = []
    for s in su.srvs:
        w, b = bc.Raw(s.oport), bc.Raw(s.oport)
        got = []
        w.send(enc("WATCH", wk[0], wk[1]))
        got += w.replies(1)
        b.send(b"".join([setf(a1, "b1"), setf(wk[1], "hit"), setf(a2, "b2")]))
        got += b.replies(3)
        w.send(b"".join([enc("MULTI"), enc("SET", a0, "inside"), enc("EXEC")]))
        got += w.replies(3)
        w.send(enc("WATCH", wk[2]))
        got += w.replies(1)
        b.send(b"".join([setf(a1, "b3"), setf(wk[3], "nohit"), setf(a2, "b4")]))
        got += b.replies(3)
        w.send(b"".join([enc("MULTI"), enc("GET", wk[2]), enc("EXEC")]))
        got += w.replies(3)
        w.send(enc("WATCH", wk[3]))
        got += w.replies(1)
        w.send(b"".join([setf(a1, "self1"), setf(wk[3], "self"), setf(a2, "self2")]))
        got += w.replies(3)
        w.send(b"".join([enc("MULTI"), enc("GET", wk[3]), enc("EXEC")]))
        got += w.replies(3)
        w.close()
        b.close()
        outs.append(got)
    d3.check("sb.%s.watch.replies" % su.name, outs[0] == outs[1] and outs[0][6] in (b"*-1\r\n", b"_\r\n") and
             outs[0][-1] in (b"*-1\r\n", b"_\r\n") and outs[0][13].startswith(b"*1"),
             "%r" % [x[:20] for x in outs[0]] if outs[0] == outs[1] else "tree %r ref %r" % (outs[0], outs[1]))

    su.case("multi", b"".join([setf(a0, "m0"), enc("MULTI"), setf(a1, "m1"), setf(a2, "m2"), enc("EXEC"), setf(a0, "m3"),
                               enc("GET", a1)]), 7)
    su.case("reply_skip", b"".join([enc("CLIENT", "REPLY", "SKIP"), setf(a1, "r1"), setf(a2, "r2"), setf(a1, "r3")]), 2)
    su.case("reply_off", b"".join([enc("CLIENT", "REPLY", "OFF"), setf(a1, "r4"), setf(a2, "r5"), enc("CLIENT", "REPLY", "ON"),
                                   setf(a1, "r6"), enc("GET", a1)]), 3)
    su.case("resp3", b"".join([setf(a1, "p3"), enc("GET", a1), setf(a2, "q3"), enc("GET", keys[60])]), 4,
            prelude=(enc("HELLO", "3"), 1))

    ka = [key_in(rng, 16, lo0, hi0, b"ka") for _ in range(2)]
    kb = key_in(rng, 16, lo0, hi0, b"kb")
    su.setup([("ACL", "SETUSER", "bu", "on", ">pw", "~ka*", "+@all"),
              ("ACL", "SETUSER", "bfull", "on", ">pw", "~*", "&*", "+@all")])
    su.case("acl", b"".join([setf(ka[0], "A0"), setf(kb, "B"), setf(ka[1], "A1"), enc("GET", ka[0])]), 4,
            prelude=(enc("AUTH", "bu", "pw"), 1))
    su.case("acl_full", b"".join([setf(ka[0], "F0"), setf(kb, "FB"), setf(ka[1], "F1"), enc("GET", kb)]), 4,
            prelude=(enc("AUTH", "bfull", "pw"), 1))

    cross = []
    for i in range(30):
        cross.append(setf(other[i % len(other)] if i % 4 == 2 else keys[i], b"c%d" % i))
    cross += [enc("GET", k) for k in other]
    su.case("cross", b"".join(cross), len(cross))
    su.case("cross_owner1", b"".join(setf(k, b"o1-" + k[:4]) for k in other + keys[:6]), 12, port_idx=1)

    # tracking: a client tracking a key gets the same invalidation push when a burst's frame sets it
    outs = []
    for s in su.srvs:
        t = bc.Raw(s.oport)
        t.send(enc("HELLO", "3"))
        t.replies(1)
        t.send(enc("CLIENT", "TRACKING", "on"))
        t.replies(1)
        t.send(enc("GET", a1))
        t.replies(1)
        b = bc.Raw(s.oport)
        b.send(b"".join([setf(a2, "tr0"), setf(a1, "tr1"), setf(a2, "tr2")]))
        got = b.replies(3)
        t.s.settimeout(0.5)
        try:
            got += t.replies(1)
        except (socket.timeout, OSError):
            got.append(b"<no push>")
        t.close()
        b.close()
        outs.append(got)
    d3.check("sb.%s.tracking.push" % su.name, outs[0] == outs[1] and outs[0][-1].startswith(b">"),
             "%r" % outs[0] if outs[0] == outs[1] else "tree %r ref %r" % (outs[0], outs[1]))

    # slowlog: every SET logged with its own arguments
    def logs(s, kind):
        ents = s.plain.cmd("SLOWLOG", "GET", "100000") if kind == "slow" else s.plain.cmd("COMMANDLOG", "GET", "100000", kind)
        if isinstance(ents, d3.Err):
            return ents
        return sorted([tuple(e[3]) for e in ents if e[3] and e[3][0].lower() == b"set"])

    for s in su.srvs:
        s.plain.cmd("SLOWLOG", "RESET")
        s.plain.cmd("CONFIG", "SET", "slowlog-log-slower-than", "0")
    su.case("slowlog", b"".join(setf(keys[i % 60], b"sl%d" % i, b"set" if i % 3 == 0 else b"SET") for i in range(600)), 600)
    for s in su.srvs:
        s.plain.cmd("CONFIG", "SET", "slowlog-log-slower-than", "10000")
    L = [logs(s, "slow") for s in su.srvs]
    d3.check("sb.%s.slowlog.entries" % su.name, L[0] == L[1] and len(L[0]) == 600,
             "tree %d ref %d SET entries" % (len(L[0]), len(L[1])))

    for kind, cfg, v in (("large-request", "commandlog-request-larger-than", "45"),
                         ("large-reply", "commandlog-reply-larger-than", "5")):
        for s in su.srvs:
            s.plain.cmd("CONFIG", "SET", "commandlog-%s-max-len" % kind, "100000")
            s.plain.cmd("COMMANDLOG", "RESET", kind)
            s.plain.cmd("CONFIG", "SET", cfg, v)
        su.case("reqlog_" + kind, b"".join(setf(keys[i % 64], bytes(rng.choice(ALNUM) for _ in range(i % 30 + 1)))
                                          for i in range(200)), 200)
        for s in su.srvs:
            s.plain.cmd("CONFIG", "SET", cfg, "1mb")
        L = [logs(s, kind) for s in su.srvs]
        if isinstance(L[0], d3.Err):
            d3.check("sb.%s.%s.entries" % (su.name, kind), repr(L[0]) == repr(L[1]), L)
        else:
            d3.check("sb.%s.%s.entries" % (su.name, kind), L[0] == L[1] and len(L[0]) > 0,
                     "tree %d ref %d SET entries" % (len(L[0]), len(L[1])))

    if su.aof:
        su.case("waitaof", b"".join([setf(a1, "wa1"), setf(a2, "wa2"), setf(a0, "wa3"), enc("WAITAOF", "1", "0", "0")]), 4)
    return b_fresh + b_rounds + b_deep


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
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    only = set(x for x in a.only.split(",") if x)
    aofargs = ["--appendonly", "yes", "--auto-aof-rewrite-percentage", "0"]

    def want(n):
        return not only or n in only

    mmargs = ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"]
    for name, extra, aof in (("plain", [], False),
                             ("aof", aofargs + ["--appendfsync", "always"], True),
                             ("aofes", aofargs + ["--appendfsync", "everysec"], True),
                             ("mm", mmargs, False),
                             ("mmaofes", mmargs + aofargs + ["--appendfsync", "everysec"], True)):
        if not want(name):
            continue
        su = Suite(name, a, extra, aof=aof)
        rng = random.Random(20261115 + len(name))
        ran = run_cases(su, rng, full=True)
        d3.check("sb.%s.ran" % name, ran >= 1000 and su.sb_total >= 1500,
                 "owner_setburst_commands +%d in fresh/rounds/deep, +%d in all" % (ran, su.sb_total))
        su.finish()
        su.stop()

    # suites where no SET burst may run
    for name, extra, extra_tree in (("mmoff", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"],
                                     ["--owner-mm-lease", "no"]),
                                    ("notify", ["--notify-keyspace-events", "KEA"], []),
                                    ("off", [], ["--owner-set-burst", "no"])):
        if not want(name):
            continue
        su = Suite(name, a, extra, extra_tree)
        subs = []
        if name == "notify":
            for s in su.srvs:
                r = bc.Raw(s.port)
                r.send(enc("PSUBSCRIBE", "__key*__:*"))
                r.replies(1)
                subs.append(r)
        run_cases(su, random.Random(20261115 + len(name)), full=False)
        d3.check("sb.%s.no_burst" % name, su.sb_total == 0, "owner_setburst_commands +%d" % su.sb_total)
        if subs:
            got = []
            for r in subs:
                r.s.settimeout(0.5)
                b = b""
                try:
                    while True:
                        d = r.s.recv(1 << 20)
                        if not d:
                            break
                        b += r.buf + d
                        r.buf = b""
                except (socket.timeout, OSError):
                    pass
                got.append(r.buf + b)
                r.close()
            d3.check("sb.notify.events", got[0] == got[1] and got[0].count(b"pmessage") > 500,
                     "%d bytes, %d events" % (len(got[0]), got[0].count(b"pmessage")) if got[0] == got[1] else
                     "tree %d bytes ref %d bytes" % (len(got[0]), len(got[1])))
        su.finish()
        su.stop()

    if want("rename"):
        su = Suite("rename", a, ["--rename-command", "set", "myset"])
        lo0, hi0 = su.tree.ranges[0]
        rng = random.Random(7)
        ks = [bc.key_in(rng, 16, lo0, hi0) for _ in range(6)]
        data = b"".join([enc("SET", ks[0], "r0"), enc("MYSET", ks[1], "r1"), enc("myset", ks[2], "r2"),
                         enc("set", ks[3], "r3"), enc("GET", ks[1]), enc("GET", ks[0])] * 5)
        su.case("rename", data, 30)
        d3.check("sb.rename.no_burst", su.sb_total == 0, "owner_setburst_commands +%d" % su.sb_total)
        su.finish()
        su.stop()

    if want("replica"):
        su = Suite("replica", a)
        reps = []
        for i, s in enumerate(su.srvs):
            rp = a.port + 1 + 10 * i
            rd = os.path.join(s.dir, "replica")
            os.makedirs(rd, exist_ok=True)
            p = subprocess.Popen([a.bin if i == 0 else a.ref_bin, "--port", str(rp), "--bind", "127.0.0.1", "--save", "",
                                  "--appendonly", "no", "--replicaof", "127.0.0.1", str(s.port), "--dir", rd,
                                  "--enable-debug-command", "yes", "--logfile", os.path.join(rd, "log")],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            c = None
            for _ in range(200):
                try:
                    c = d3.Conn("127.0.0.1", rp, timeout=5)
                    if "master_link_status:up" in c.cmd("INFO", "replication").decode():
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            reps.append((p, c))
        time.sleep(0.5)
        run_cases(su, random.Random(99), full=False)
        d3.check("sb.replica.no_burst", su.sb_total == 0, "owner_setburst_commands +%d" % su.sb_total)
        for s, (p, c) in zip(su.srvs, reps):
            s.plain.cmd("WAIT", "1", "2000")
            dg = [s.plain.cmd("DEBUG", "DIGEST"), c.cmd("DEBUG", "DIGEST")]
            d3.check("sb.replica.%s.digest" % s.name, dg[0] == dg[1], dg)
            try:
                c.cmd("SHUTDOWN", "NOSAVE")
            except Exception:
                pass
            p.wait(timeout=20)
        su.finish()
        su.stop()

    fails = [r["check"] for r in d3.RESULTS if not r["ok"]]
    print("SETBURSTCHECK " + json.dumps({"checks": len(d3.RESULTS), "fails": fails}))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
