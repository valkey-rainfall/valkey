#!/usr/bin/env python3
"""RL-g2u-tls-16k gene check: owner GET bursts, GET rings and MGET rings answer keys whose TTL has not passed (values
from owner-bigreply-min up by reference); a key whose expire has passed still goes to getCommand / mgetCommand.

Starts its own servers: A = --bin with --n owner threads, owner plain and owner TLS ports, appendonly always
(--aof) and a replica of A (no owners); R = --ref-bin with owner-threads 0 (the stock main path). Active expiry
is off on A, R and the replica (DEBUG SET-ACTIVE-EXPIRE 0), so every expiry is a lazy one.

  model   --threads workers, each with its own keys under one hash tag in one owner's slots: values of 0-300 B,
          integers, 2.1 KB, 16 KB, 64 KB; keys with no TTL, a long TTL or a short one (300-900 ms) that passes
          during the run. Random pipelined batches (1-16 commands) of GET / MGET (1-6 keys, repeats) / SET
          [EX|PX|KEEPTTL] / PEXPIRE / PERSIST / DEL / MULTI GET.. EXEC, each sent to A (on the owner's plain or TLS
          port, 10% on A's main port) and to R: every reply equal, and equal to a model (keys within 60 ms of
          their expiry are not touched, so both sides agree). Then, past every short TTL: every key touched with
          GET / MGET (expired ones are deleted lazily on both), keyspace (type, digest, PEXPIRETIME +-150 ms),
          DBSIZE, expired_keys, keyspace_hits / keyspace_misses equal R's; owner_native_commands and
          owner_ref_replies grew.
  edge    A only: keys set with PXAT now + 15..90 ms, then GET / MGET of them as fast as possible until all
          passed: a reply is the value when the expire is after the reply arrived, nil when it was before the
          request was sent (either in between); afterwards every key is nil and gone.
  repl    the replica caught up: keyspace exactly equal to A's (PEXPIRETIME too), DBSIZE equal.
  aof     A shut down, a fresh server on its AOF dir: keyspace exactly equal.
Prints PASS/FAIL lines and 'TTLGET {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import socket
import ssl
import sys
import threading
import time

A = None


def now_ms():
    return int(time.time() * 1000)


class TlsConn:
    """d3-check's Conn over TLS (same send / read / cmd)."""

    def __init__(self, host, port, ca, timeout=60):
        s = socket.create_connection((host, port), timeout=timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.load_verify_locations(ca)
        self.s = ctx.wrap_socket(s, server_hostname=host)
        self.f = self.s.makefile("rb", buffering=1 << 20)

    send = None  # set below from d3.Conn
    read = None
    cmd = None

    def close(self):
        self.s.close()


SHORT = (300, 900)   # ms
LONG = (600, 3600)   # s
MARGIN = 60          # ms around an expiry in which a key is not touched
TOL_MS = 150
VAL_SIZES = [0, 1, 7, 64, 300, 2100, 16384, 16384, 65536]


def mkval(rng, i):
    k = i % 10
    if k == 9:
        return b"%d" % rng.randrange(-10 ** 12, 10 ** 12)
    n = VAL_SIZES[k]
    if n <= 300:
        n = rng.randrange(0, 301)
    b = bytes([97 + rng.randrange(26)])
    return b * n


def model_worker(w, out, stop_at, seed):
    rng = random.Random(seed)
    oi = w % A.n
    lo, hi = A.a.ranges[oi]
    tag = None
    while tag is None:
        t = b"w%d-%d" % (w, rng.randrange(1 << 30))
        if lo <= d3.key_slot(b"{" + t + b"}") <= hi:
            tag = t
    keys = [b"{%s}:k%d" % (tag, i) for i in range(24)]
    model = {}  # key -> [value, expire_at_ms or -1]
    ca_own = d3.Conn("127.0.0.1", A.owner_port + oi)
    ca_tls = TlsConn("127.0.0.1", A.owner_tls_port + oi, A.ca)
    ca_main = d3.Conn("127.0.0.1", A.port)
    cr = d3.Conn("127.0.0.1", A.ref_port)
    o = dict(ops=0, batches=0, tls_batches=0, main_batches=0, skipped=0, diffs=[], issues=[], keys=keys,
             gets=0, mgets=0, ttl_hits=0)
    out[w] = o
    vi = 0

    def live(k, t):
        m = model.get(k)
        if m is None:
            return None
        if m[1] >= 0 and m[1] < t:
            return None
        return m

    def touchable(k, t, span):
        m = model.get(k)
        if m is None or m[1] < 0:
            return True
        return not (m[1] - MARGIN - span <= t <= m[1] + MARGIN)

    while time.time() < stop_at:
        t0 = now_ms()
        span = 40  # ms a batch may take on both sides
        n = rng.randrange(1, 17)
        cmds, exp = [], []
        for _ in range(n):
            k = rng.choice(keys)
            if not touchable(k, t0, span):
                o["skipped"] += 1
                continue
            p = rng.random()
            if p < 0.40:
                m = live(k, t0)
                cmds.append(("GET", k))
                exp.append(m[0] if m else None)
                o["gets"] += 1
                if m and m[1] >= 0:
                    o["ttl_hits"] += 1
            elif p < 0.58:
                ks = [rng.choice(keys) for _ in range(rng.randrange(1, 7))]
                if not all(touchable(x, t0, span) for x in ks):
                    o["skipped"] += 1
                    continue
                cmds.append(tuple(["MGET"] + ks))
                exp.append([(live(x, t0) or [None])[0] for x in ks])
                o["mgets"] += 1
            elif p < 0.80:
                vi += 1
                v = mkval(rng, vi)
                q = rng.random()
                if q < 0.35:
                    s = rng.randrange(*LONG)
                    cmds.append(("SET", k, v, "EX", s))
                    model[k] = [v, t0 + s * 1000]
                elif q < 0.70:
                    ms = rng.randrange(*SHORT)
                    cmds.append(("SET", k, v, "PX", ms))
                    model[k] = [v, t0 + ms]
                elif q < 0.85 and live(k, t0):
                    cmds.append(("SET", k, v, "KEEPTTL"))
                    model[k] = [v, model[k][1]]
                else:
                    cmds.append(("SET", k, v))
                    model[k] = [v, -1]
                exp.append("OK")
            elif p < 0.86:
                m = live(k, t0)
                ms = rng.randrange(*SHORT) if rng.random() < 0.6 else rng.randrange(*LONG) * 1000
                cmds.append(("PEXPIRE", k, ms))
                exp.append(1 if m else 0)
                if m:
                    m[1] = t0 + ms
            elif p < 0.90:
                m = live(k, t0)
                cmds.append(("PERSIST", k))
                exp.append(1 if (m and m[1] >= 0) else 0)
                if m:
                    m[1] = -1
            elif p < 0.94:
                m = live(k, t0)
                cmds.append(("DEL", k))
                exp.append(1 if m else 0)
                model.pop(k, None)
            else:
                ks = [rng.choice(keys) for _ in range(rng.randrange(1, 4))]
                if not all(touchable(x, t0, span) for x in ks):
                    o["skipped"] += 1
                    continue
                cmds.append(("MULTI",))
                exp.append("OK")
                for x in ks:
                    cmds.append(("GET", x))
                    exp.append("QUEUED")
                cmds.append(("EXEC",))
                exp.append([(live(x, t0) or [None])[0] for x in ks])
        if not cmds:
            continue
        q = rng.random()
        if q < 0.10:
            ca, o["main_batches"] = ca_main, o["main_batches"] + 1
        elif q < 0.55:
            ca, o["tls_batches"] = ca_tls, o["tls_batches"] + 1
        else:
            ca = ca_own
        ca.send(cmds)
        cr.send(cmds)
        ra = [ca.read() for _ in cmds]
        rr = [cr.read() for _ in cmds]
        t1 = now_ms()
        o["ops"] += len(cmds)
        o["batches"] += 1
        if t1 - t0 > span:
            o["slow"] = o.get("slow", 0) + 1  # replies still checked against R; model may lag at the edges
        for c, x, y, e in zip(cmds, ra, rr, exp):
            if x != y:
                o["diffs"].append((c[0], c[1][:40] if len(c) > 1 else b"", str(x)[:80], str(y)[:80]))
            elif t1 - t0 <= span and x != e:
                o["issues"].append((c[0], c[1][:40] if len(c) > 1 else b"", str(x)[:80], str(e)[:80]))
    for c in (ca_own, ca_tls, ca_main, cr):
        c.close()
    o["model"] = model


def edge_worker(w, out, seed):
    rng = random.Random(seed)
    oi = w % A.n
    lo, hi = A.a.ranges[oi]
    tag = None
    while tag is None:
        t = b"e%d-%d" % (w, rng.randrange(1 << 30))
        if lo <= d3.key_slot(b"{" + t + b"}") <= hi:
            tag = t
    keys = [b"{%s}:e%d" % (tag, i) for i in range(12)]
    c = TlsConn("127.0.0.1", A.owner_tls_port + oi, A.ca) if w % 2 else d3.Conn("127.0.0.1", A.owner_port + oi)
    o = dict(replies=0, valid=0, nil=0, between=0, issues=[], keys=keys)
    out[w] = o
    for rnd in range(6):
        t = now_ms()
        exp = {}
        cmds = []
        for i, k in enumerate(keys):
            at = t + rng.randrange(15, 90)
            v = mkval(rng, i)
            exp[k] = (v, at)
            cmds.append(("SET", k, v, "PXAT", at))
        c.send(cmds)
        for _ in cmds:
            c.read()
        last = max(a for _, a in exp.values())
        while now_ms() <= last + 5:
            batch = []
            for _ in range(rng.randrange(1, 9)):
                if rng.random() < 0.7:
                    batch.append(("GET", rng.choice(keys)))
                else:
                    batch.append(tuple(["MGET"] + [rng.choice(keys) for _ in range(rng.randrange(1, 5))]))
            ts = now_ms()
            c.send(batch)
            rs = [c.read() for _ in batch]
            tr = now_ms() + 1
            for b, r in zip(batch, rs):
                ks, vals = (b[1:], r) if b[0] == "MGET" else ((b[1],), [r])
                for k, x in zip(ks, vals):
                    v, at = exp[k]
                    o["replies"] += 1
                    if at > tr:
                        o["valid"] += 1
                        if x != v:
                            o["issues"].append(("early-nil", k[-12:], at - ts, str(x)[:20]))
                    elif at < ts:
                        o["nil"] += 1
                        if x is not None:
                            o["issues"].append(("late-value", k[-12:], ts - at))
                    else:
                        o["between"] += 1
                        if x is not None and x != v:
                            o["issues"].append(("wrong-value", k[-12:]))
        c.send([("GET", k) for k in keys])
        rs = [c.read() for _ in keys]
        if any(r is not None for r in rs):
            o["issues"].append(("after-all-passed", sum(1 for r in rs if r is not None)))
        c.send([("EXISTS", k) for k in keys])
        rs = [c.read() for _ in keys]
        if any(rs):
            o["issues"].append(("still-there", sum(rs)))
    c.close()


def stats2(c):
    i = d3.info(c, "stats")
    return {k: int(i[k]) for k in ("keyspace_hits", "keyspace_misses", "expired_keys")}


def main():
    global A, d3, ks
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)   # A main; +1 TLS; owners +10..; owner TLS +20..
    ap.add_argument("--ref-port", type=int, required=True)
    ap.add_argument("--replica-port", type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", required=True)
    ap.add_argument("--main-cpu", required=True)
    ap.add_argument("--bg-cpu", required=True)
    ap.add_argument("--ref-cpus", required=True)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--secs", type=float, default=25)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--tls-dir", default="/home/ec2-user/ns-tls")
    ap.add_argument("--d3dir", required=True)            # utils/vkevo-d3 of the tree (d3-check, rl-keyspace-check)
    A = ap.parse_args()
    sys.path.insert(0, A.d3dir)
    d3 = __import__("d3-check")
    ks = __import__("rl-keyspace-check")
    ks.A = A
    for f in ("send", "read", "cmd"):
        setattr(TlsConn, f, getattr(d3.Conn, f))
    TlsConn.enc = staticmethod(d3.Conn.enc)
    A.owner_port, A.owner_tls_port, A.ca = A.port + 10, A.port + 20, os.path.join(A.tls_dir, "ca.crt")
    tls = ["--tls-port", str(A.port + 1), "--owner-tls-port", str(A.owner_tls_port),
           "--tls-cert-file", os.path.join(A.tls_dir, "server.crt"), "--tls-key-file", os.path.join(A.tls_dir, "server.key"),
           "--tls-ca-cert-file", A.ca, "--tls-auth-clients", "no"]
    oargs = ["--owner-threads", str(A.n), "--owner-port", str(A.owner_port), "--owner-cpulist", A.cpus,
             "--server-cpulist", str(A.main_cpu), "--bio-cpulist", str(A.bg_cpu), "--aof-rewrite-cpulist", str(A.bg_cpu),
             "--bgsave-cpulist", str(A.bg_cpu), "--appendonly", "yes", "--appendfsync", "always",
             "--repl-backlog-size", "64mb"] + tls
    cpus = "%s,%s,%s" % (A.main_cpu, A.bg_cpu, A.cpus)
    a = ks.Srv("a", A.bin, A.port, oargs, cpus, A.dir)
    A.a = a
    r = ks.Srv("r", A.ref_bin, A.ref_port, ["--owner-threads", "0", "--appendonly", "no"], A.ref_cpus, A.dir)
    rep = ks.Srv("rep", A.bin, A.replica_port, ["--appendonly", "no"], A.ref_cpus, A.dir)
    rep.c.cmd("REPLICAOF", "127.0.0.1", A.port)
    for _ in range(400):
        if d3.info(rep.c, "replication").get("master_link_status") == "up":
            break
        time.sleep(0.05)
    for s in (a, r, rep):
        s.c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)
    v = d3.info(a.c, "owners")
    d3.check("setup.owners", len(a.ranges) == A.n and v.get("owner_veto") in ("none", "0") and
             v.get("owner_bigreply_min", "0") != "0",
             "ranges %s veto %s bigreply_min %s tls_port %s" % (a.ranges, v.get("owner_veto"),
                                                                 v.get("owner_bigreply_min"), v.get("owner_tls_port")))
    s0a, s0r, o0 = stats2(a.c), stats2(r.c), d3.ostat(a.c)
    ia0 = d3.info(a.c, "owners")
    out = {}
    stop_at = time.time() + A.secs
    ths = [threading.Thread(target=model_worker, args=(w, out, stop_at, A.seed * 1000 + w)) for w in range(A.threads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    ops = sum(o["ops"] for o in out.values())
    diffs = [d for o in out.values() for d in o["diffs"]]
    issues = [d for o in out.values() for d in o["issues"]]
    d3.check("model.replies_equal_ref", not diffs and ops > 2000,
             diffs[:3] if diffs else "%d ops in %d batches (%d over TLS, %d on main's port), GET %d (%d of keys with a TTL),"
             " MGET %d, %d skipped picks, %d slow batches" %
             (ops, sum(o["batches"] for o in out.values()), sum(o["tls_batches"] for o in out.values()),
              sum(o["main_batches"] for o in out.values()), sum(o["gets"] for o in out.values()),
              sum(o["ttl_hits"] for o in out.values()), sum(o["mgets"] for o in out.values()),
              sum(o["skipped"] for o in out.values()), sum(o.get("slow", 0) for o in out.values())))
    d3.check("model.replies_match_model", not issues, issues[:3] if issues else "%d ops" % ops)
    # past every short TTL: touch every key with GET / MGET on both (A on the owner ports: the expired ones go to
    # getCommand, which deletes them and propagates the DEL), then compare
    time.sleep((SHORT[1] + 2 * MARGIN + 300) / 1000.0)
    keys = sorted(k for o in out.values() for k in o["keys"])
    for w, o in out.items():
        oi = w % A.n
        for port, c in ((A.ref_port, d3.Conn("127.0.0.1", A.ref_port)), (A.owner_port + oi, d3.Conn("127.0.0.1", A.owner_port + oi))):
            ks_ = o["keys"]
            c.send([("GET", k) for k in ks_[:12]] + [tuple(["MGET"] + ks_[12:])])
            for _ in range(13):
                c.read()
            c.close()
    sa, sr = ks.snapshot(a, keys), ks.snapshot(r, keys)
    bad = ks.compare_snap(sa, sr, TOL_MS)
    d3.check("model.keyspace_equal_ref", not bad and len(sa) > 0,
             bad[:3] if bad else "%d of %d keys present, types/digests equal, expire times within %d ms (%d with an expire)"
             % (len(sa), len(keys), TOL_MS, sum(1 for v in sa.values() if v[2] > 0)))
    s1a, s1r, o1 = stats2(a.c), stats2(r.c), d3.ostat(a.c)
    ia1 = d3.info(a.c, "owners")
    da = {k: s1a[k] - s0a[k] for k in s1a}
    dr = {k: s1r[k] - s0r[k] for k in s1r}
    d3.check("model.stats_equal_ref", da == dr and a.c.cmd("DBSIZE") == r.c.cmd("DBSIZE") and da["expired_keys"] > 0,
             "A %s / R %s; dbsize %s / %s" % (da, dr, a.c.cmd("DBSIZE"), r.c.cmd("DBSIZE")))
    nat = o1["owner_native_commands"] - o0["owner_native_commands"]
    refs = int(ia1.get("owner_ref_replies", 0)) - int(ia0.get("owner_ref_replies", 0))
    gets = sum(o["gets"] + o["mgets"] for o in out.values())
    d3.check("model.ran_native", nat > 0.6 * ops and refs > 0,
             "native +%d of %d ops (GET + MGET %d); value references +%d; handoffs +%d" %
             (nat, ops, gets, refs, o1["owner_handoffs_to_main"] - o0["owner_handoffs_to_main"]))
    # edge: expiries a few ms after the SET, read as they pass
    eo = {}
    e0 = stats2(a.c)["expired_keys"]
    ths = [threading.Thread(target=edge_worker, args=(w, eo, A.seed * 7000 + w)) for w in range(A.threads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    eis = [d for o in eo.values() for d in o["issues"]]
    d3.check("edge.replies_by_time", not eis and sum(o["valid"] for o in eo.values()) > 100 and
             sum(o["nil"] for o in eo.values()) > 100,
             eis[:3] if eis else "%d key replies: %d before the expire (value), %d after (nil), %d in the window" %
             (sum(o["replies"] for o in eo.values()), sum(o["valid"] for o in eo.values()),
              sum(o["nil"] for o in eo.values()), sum(o["between"] for o in eo.values())))
    e1 = stats2(a.c)["expired_keys"]
    nkeys = sum(len(o["keys"]) for o in eo.values()) * 6
    d3.check("edge.expired_lazily", e1 - e0 == nkeys, "expired_keys +%d for %d keys set" % (e1 - e0, nkeys))
    allkeys = keys + sorted(k for o in eo.values() for k in o["keys"])
    # repl: the replica caught up and holds A's keyspace exactly (the lazy DELs reached it)
    for _ in range(600):
        off = int(d3.info(a.c, "replication")["master_repl_offset"])
        ri = d3.info(rep.c, "replication")
        if ri.get("master_link_status") == "up" and int(ri.get("slave_repl_offset", -1)) == off:
            break
        time.sleep(0.05)
    sa = ks.snapshot(a, allkeys)
    sp = ks.snapshot(rep, allkeys)
    bad = ks.compare_snap(sa, sp, 0)
    d3.check("repl.keyspace_equal_primary", not bad and len(sa) > 0 and a.c.cmd("DBSIZE") == rep.c.cmd("DBSIZE"),
             bad[:3] if bad else "%d keys exactly equal (PEXPIRETIME too); dbsize %s / %s" %
             (len(sa), a.c.cmd("DBSIZE"), rep.c.cmd("DBSIZE")))
    # aof: reload into a fresh server
    a.stop()
    b = ks.Srv("a", A.bin, A.port, oargs, cpus, A.dir, fresh=False)
    b.c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)
    sb = ks.snapshot(b, allkeys)
    bad = ks.compare_snap(sa, sb, 0)
    d3.check("aof.reload_keyspace_equal", not bad and len(sa) > 0,
             bad[:3] if bad else "%d keys exactly equal after reloading the AOF" % len(sa))
    for s in (b, r, rep):
        s.stop()
        d3.check("no_crash.%s" % s.name, s.crashed() == 0, "crash lines %d" % s.crashed())
    import shutil
    shutil.rmtree(os.path.join(b.dir, "appendonlydir"), ignore_errors=True)
    fails = [x for x in d3.RESULTS if not x["ok"]]
    print("TTLGET " + json.dumps(dict(pass_=len(d3.RESULTS) - len(fails), fail=len(fails), ops=ops)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
