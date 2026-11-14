#!/usr/bin/env python3
"""D3-all gate: where the merged features meet (D3-g2 GET ring, D3mk native multi-key/MULTI, D3veto AOF +
maxmemory). Starts its own loopback servers (as d3veto-check.py, whose helpers it uses) and checks:

  aof.*  (appendonly yes, appendfsync always)
    exec_native        native MULTI/EXEC on every owner's own tag: no handoff, native counter moves
    exec_wrap          the AOF has a native EXEC with 2+ writes as MULTI, its writes in order, EXEC, and
                       a one-write EXEC unwrapped: the same shapes the stock EXEC (plain port) writes
    exec_reply_after_write  with DEBUG AOF-FLUSH-SLEEP S, a native EXEC's reply takes >= S
    ring_backed        per owner, transactions INCR a, INCR b natively while readers pipeline GET a
                       (the GET ring) and MGET a b: every reply value is backed by the AOF file when it
                       arrives, and MGET always sees a == b
    exec_replay        native, handed-off and plain-port transactions + BGREWRITEAOF: a == b == the
                       transaction count per owner; DEBUG LOADAOF and a restart give the same DEBUG DIGEST
  mm.*   (maxmemory)
    native             owner_veto none; native EXEC and MGET with room
    mget_bound         an MGET whose stored values exceed an owner's headroom slice goes to main
                       (owner_maxmemory_to_main), a small one stays native; both replies correct
    exec_bound         the same for an EXEC whose queued GETs exceed the slice
    exec_evict         allkeys-lru 64 MB, 8-key MSET transactions of 16 KB values from every owner:
                       sampled used_memory <= maxmemory + 2 MB, dbsize + evicted == acked keys, every
                       present key holds its value
    exec_noeviction    noeviction 40 MB: every transaction is all-or-nothing (acked: 8 keys present;
                       refused: none), refusals happen

Usage: d3all-check.py --bin src/valkey-server --dir /tmp/x --n 4 --port 36379 --owner-port 36400
                      --cpus 48-51 --main-cpu 56 [--only aof|mm]
Prints PASS/FAIL lines and a final 'D3ALL {json}' line; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
veto = __import__("d3veto-check")
d3 = veto.d3
Conn, Err, check, info, ostat = d3.Conn, d3.Err, d3.check, d3.info, d3.ostat
Server, owner_ranges, owner_tags, pipeline = veto.Server, veto.owner_ranges, veto.owner_tags, veto.pipeline
AofFile, rec, wait_rewrite, wait_no_bio_fsync = veto.AofFile, veto.rec, veto.wait_rewrite, veto.wait_no_bio_fsync
Sampler, settle_used = veto.Sampler, veto.settle_used

ARGS = None
MULTI, EXEC = rec(b"MULTI"), rec(b"EXEC")


def tk(tag, name):
    return b"{%s}%s" % (tag, name)


def txn(c, cmds):
    """MULTI, cmds, EXEC pipelined; returns EXEC's reply (QUEUED replies checked)."""
    c.send([("MULTI",)] + cmds + [("EXEC",)])
    r = [c.read() for _ in range(len(cmds) + 2)]
    if r[0] != "OK" or any(x != "QUEUED" for x in r[1:-1]):
        raise RuntimeError("txn replies %r" % (r[:4],))
    return r[-1]


def wrap_shape(data, recs):
    """Where the records `recs` sit contiguously in the AOF: (found, wrapped in MULTI .. EXEC)."""
    i = data.find(recs[0])
    if i < 0:
        return False, None
    j = i
    for r in recs:
        if not data.startswith(r, j):
            return False, None
        j += len(r)
    before = bytes(data[max(0, i - 64):i])
    sel = rec(b"SELECT", b"0")
    if before.endswith(sel):
        before = before[:-len(sel)]
    pre, post = before.endswith(MULTI), data.startswith(EXEC, j)
    return True, (pre and post) if pre == post else "half"


def aof_checks():
    srv = Server("all-aof", ["--appendonly", "yes", "--appendfsync", "always", "--auto-aof-rewrite-percentage", "0"])
    pid = srv.p.pid
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    check("aof.veto_none", info(adm, "owners").get("owner_veto") == "none", info(adm, "owners").get("owner_veto"))

    # exec_native + exec_wrap: three transaction shapes per owner natively, the same on the plain port
    shapes = []  # (path, recs, want_wrapped)
    o0 = ostat(adm)
    sent = 0
    for path in ("own", "plain"):
        for i in range(N):
            t = owner_tags(rng, i)[0]
            c = srv.conn(i if path == "own" else None)
            s = b"%s%d" % (path.encode(), i)
            a, cc, x, y, one = (tk(t, b"a" + s), tk(t, b"c" + s), tk(t, b"x" + s), tk(t, b"y" + s), tk(t, b"s" + s))
            r1 = txn(c, [("SET", a, b"A" + s), ("INCR", cc), ("MSET", x, b"1", y, b"2"), ("GET", a)])
            r2 = txn(c, [("SET", one, b"S" + s), ("GET", one)])
            r3 = txn(c, [("GET", a), ("PING",)])
            ok = r1 == ["OK", 1, "OK", b"A" + s] and r2 == ["OK", b"S" + s] and r3 == [b"A" + s, "PONG"]
            if not ok:
                check("aof.exec_replies_" + path, False, (r1, r2, r3))
            if path == "own":
                sent += 14  # MULTI and EXEC count once each, a queued command when EXEC runs it
            shapes.append((path, [rec(b"SET", a, b"A" + s), rec(b"INCR", cc), rec(b"MSET", x, b"1", y, b"2")], True))
            shapes.append((path, [rec(b"SET", one, b"S" + s)], False))
            c.close()
        if path == "own":
            o1 = ostat(adm)
            dn = o1["owner_native_commands"] - o0["owner_native_commands"]
            dh = o1["owner_handoffs_to_main"] - o0["owner_handoffs_to_main"]
            # each connection's close is one handoff
            check("aof.exec_native", dn >= sent and dh <= N, "native +%d for %d sent, handoffs +%d" % (dn, sent, dh))
    af = AofFile(srv)
    af.count(b"x")
    data = af.data
    bad = []
    for path, recs, want in shapes:
        found, wrapped = wrap_shape(data, recs)
        if not found or wrapped != want:
            bad.append((path, recs[0][:60], found, wrapped, want))
    check("aof.exec_wrap", not bad, "%d shapes, bad %s" % (len(shapes), bad[:3]))

    # exec_reply_after_write
    S = 300
    worst = []
    for i in range(N):
        t = owner_tags(rng, i)[1]
        c = srv.conn(i)
        k, n = tk(t, b"raw%d" % i), tk(t, b"rawn%d" % i)
        c.cmd("GET", k)
        wait_no_bio_fsync(adm)
        adm.cmd("DEBUG", "AOF-FLUSH-SLEEP", S * 1000)
        t0 = time.monotonic()
        r = txn(c, [("SET", k, b"v"), ("INCR", n)])
        ms = (time.monotonic() - t0) * 1000
        adm.cmd("DEBUG", "AOF-FLUSH-SLEEP", 0)
        worst.append((round(ms), r))
        c.close()
    check("aof.exec_reply_after_write", all(ms >= S - 50 and r == ["OK", 1] for ms, r in worst),
          "ms per owner %s (flush sleep %d ms)" % ([w[0] for w in worst], S))

    # ring_backed: replies of ring GETs and MGETs never show an INCR the AOF does not have yet
    lock = threading.Lock()
    bad, nrep, errs = [], [0], []
    W = 600

    def pair(i):
        t = owner_tags(rng, i)[2]
        return tk(t, b"ra%d" % i), tk(t, b"rb%d" % i)

    def writer(i):
        try:
            a, b = pair(i)
            c = srv.conn(i)
            pa = rec(b"INCR", a)
            for j in range(W):
                r = txn(c, [("INCR", a), ("INCR", b)])
                have = af.count(pa)
                if r[0] != r[1] or have < r[0]:
                    with lock:
                        bad.append(("txn", i, r, have))
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def reader(i, stop):
        try:
            a, b = pair(i)
            c = srv.conn(i)
            pa = rec(b"INCR", a)
            while not stop.is_set():
                cmds = [("GET", a)] * 32 + [("MGET", a, b)] * 8
                c.send(cmds)
                rs = [c.read() for _ in cmds]
                have = af.count(pa)
                vals = [int(x) if x is not None else 0 for x in rs[:32]]
                pairs = [tuple(int(v) if v is not None else 0 for v in m) for m in rs[32:]]
                with lock:
                    nrep[0] += len(cmds)
                    if max(vals + [p[0] for p in pairs]) > have:
                        bad.append(("read", i, max(vals), have))
                    if any(p[0] != p[1] for p in pairs):
                        bad.append(("mget", i, pairs[:2]))
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    stop = threading.Event()
    ws = [threading.Thread(target=writer, args=(i,)) for i in range(N)]
    rs = [threading.Thread(target=reader, args=(i, stop)) for i in range(N)]
    for t in ws + rs:
        t.start()
    for t in ws:
        t.join()
    stop.set()
    for t in rs:
        t.join()
    finals = [[int(adm.cmd("GET", k)) for k in pair(i)] for i in range(N)]
    check("aof.ring_backed", not bad and not errs and all(f == [W, W] for f in finals),
          "%d read replies, %d bad %s, errs %s, finals %s" % (nrep[0], len(bad), bad[:3], errs[:2], finals))

    # exec_replay: transactions on three paths, BGREWRITEAOF in the middle, digest after LOADAOF/restart
    adm.cmd("FLUSHALL")
    T = 300
    errs = []

    def keys_r(i):
        t = owner_tags(rng, i)[3]
        return [tk(t, n + b"%d" % i) for n in (b"pa", b"pb", b"pm1", b"pm2", b"pd")]

    def load(i, port_owner, n):
        try:
            a, b, m1, m2, d = keys_r(i)
            c = srv.conn(port_owner)
            for j in range(n):
                r = txn(c, [("INCR", a), ("INCR", b), ("MSET", m1, b"%d" % j, m2, b"%d" % j), ("DEL", d),
                            ("SET", d, b"x%d" % j)])
                if r[0] != r[1] or r[2] != "OK":
                    errs.append(("txn", i, r))
                    return
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    ths = [threading.Thread(target=load, args=(i, i, T)) for i in range(N)]
    ths += [threading.Thread(target=load, args=(i, None, 50)) for i in range(N)]          # plain port
    ths += [threading.Thread(target=load, args=(i, (i + 1) % N, 50)) for i in range(N)]   # other owner
    for t in ths:
        t.start()
    time.sleep(0.2)
    adm.cmd("BGREWRITEAOF")
    for t in ths:
        t.join()
    check("aof.exec_replay_load", not errs, errs[:3])
    check("aof.exec_replay_rewrite_done", wait_rewrite(adm), info(adm, "persistence").get("aof_last_bgrewrite_status"))

    def state():
        out = []
        for i in range(N):
            a, b, m1, m2, d = keys_r(i)
            out.append(tuple(adm.cmd("GET", k) for k in (a, b, m1, m2)) + (adm.cmd("EXISTS", d),))
        return out

    want = T + 100
    s_live = state()
    ok = all(int(s[0]) == want and int(s[1]) == want and s[2] == s[3] and s[4] == 1 for s in s_live)
    check("aof.exec_replay_live", ok, s_live[:2])
    o_pre = ostat(adm)
    d_live = adm.cmd("DEBUG", "DIGEST")
    adm.cmd("DEBUG", "LOADAOF")
    d_load = adm.cmd("DEBUG", "DIGEST")
    check("aof.exec_replay_loadaof_digest", d_load == d_live and state() == s_live, (d_live, d_load))
    adm.close()
    srv.stop()
    srv.start()
    adm = srv.conn()
    d_restart = adm.cmd("DEBUG", "DIGEST")
    check("aof.exec_replay_restart_digest", d_restart == d_live and state() == s_live, (d_live, d_restart))
    check("aof.counters", o_pre.get("owner_aof_ops", 0) > 0 and o_pre.get("owner_held_replies", 0) > 0,
          {k: o_pre.get(k) for k in ("owner_aof_ops", "owner_held_replies", "owner_native_commands")})
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("aof.no_crash", crash == 0, "crash lines %d, first pid %d" % (crash, pid))


def mm_checks():
    MB = 1 << 20
    N = ARGS.n
    srv = Server("all-mm", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"])
    pid = srv.p.pid
    adm = srv.conn()
    rng = owner_ranges(adm)
    check("mm.veto_none", info(adm, "owners").get("owner_veto") == "none", info(adm, "owners").get("owner_veto"))

    # native: EXEC and MGET with room
    o0 = ostat(adm)
    for i in range(N):
        t = owner_tags(rng, i)[0]
        c = srv.conn(i)
        r = txn(c, [("SET", tk(t, b"n1"), b"v"), ("MSET", tk(t, b"n2"), b"a", tk(t, b"n3"), b"b")])
        m = c.cmd("MGET", tk(t, b"n1"), tk(t, b"n2"), tk(t, b"n3"))
        if r != ["OK", "OK"] or m != [b"v", b"a", b"b"]:
            check("mm.native_replies", False, (r, m))
        c.close()
    o1 = ostat(adm)
    dn = o1["owner_native_commands"] - o0["owner_native_commands"]
    dh = o1["owner_handoffs_to_main"] - o0["owner_handoffs_to_main"]
    check("mm.native", dn >= 5 * N and dh <= N, "native +%d for %d sent, handoffs +%d" % (dn, 5 * N, dh))

    # mget_bound / exec_bound: stored values bigger than an owner's slice send the command to main
    t = owner_tags(rng, 0)[1]
    big = [tk(t, b"big%d" % j) for j in range(8)]
    vals = [(b"%d|" % j).ljust(MB, b"b") for j in range(8)]
    for k, v in zip(big, vals):
        adm.cmd("SET", k, v)
    used = settle_used(adm)
    adm.cmd("CONFIG", "SET", "maxmemory", used + N * 4 * MB + MB)  # each owner's slice ~4 MB (> 2 MB tight)
    adm.cmd("PING")
    time.sleep(0.05)
    veto_now = info(adm, "owners").get("owner_veto")
    c = srv.conn(0)
    s0, e0 = ostat(adm), int(info(adm, "stats")["evicted_keys"])
    small = c.cmd("MGET", tk(t, b"nokey"), tk(t, b"nokey2"))
    s1 = ostat(adm)
    m = c.cmd("MGET", *big)
    s2 = ostat(adm)
    x = txn(c, [("GET", k) for k in big[:6]])
    s3 = ostat(adm)
    e1 = int(info(adm, "stats")["evicted_keys"])
    c.close()
    d_small = s1["owner_maxmemory_to_main"] - s0["owner_maxmemory_to_main"]
    d_mget = s2["owner_maxmemory_to_main"] - s1["owner_maxmemory_to_main"]
    d_exec = s3["owner_maxmemory_to_main"] - s2["owner_maxmemory_to_main"]
    check("mm.mget_bound", veto_now == "none" and small == [None, None] and d_small == 0 and d_mget >= 1
          and m == vals and e1 == e0,
          "veto %s, to_main small +%d big +%d, reply ok %s, evicted +%d" % (veto_now, d_small, d_mget, m == vals, e1 - e0))
    check("mm.exec_bound", d_exec >= 1 and x == vals[:6], "to_main +%d, reply ok %s" % (d_exec, x == vals[:6]))
    adm.cmd("CONFIG", "SET", "maxmemory", "1gb")
    adm.cmd("FLUSHALL")
    adm.close()
    srv.stop()

    # exec_evict and exec_noeviction: 8-key MSET transactions of 16 KB values from every owner
    V = 16384

    def txn_load(srv, rng, policy_tag, per_conn, conns):
        acked, refused, errs = {}, [], []
        lock = threading.Lock()

        def run(i, p):
            try:
                c = srv.conn(i)
                tags = owner_tags(rng, i)
                for j in range(per_conn):
                    t = tags[(p * per_conn + j) % 32]
                    ks = [tk(t, b"%s:%d:%d:%d:%d" % (policy_tag, i, p, j, q)) for q in range(8)]
                    kv = []
                    for k in ks:
                        kv += [k, (b"%s|" % k).ljust(V, b"x")]
                    c.send([("MULTI",), ("MSET",) + tuple(kv), ("EXEC",)])
                    r = [c.read() for _ in range(3)]
                    with lock:
                        if r[2] == ["OK"]:
                            acked.update({k: (b"%s|" % k).ljust(V, b"x") for k in ks})
                        else:
                            refused.append((ks, r))
                c.close()
            except Exception as e:  # noqa
                errs.append(repr(e))

        ths = [threading.Thread(target=run, args=(i, p)) for i in range(N) for p in range(conns)]
        for th in ths:
            th.start()
        for th in ths:
            th.join()
        return acked, refused, errs

    srv = Server("all-evict", ["--maxmemory", "64mb", "--maxmemory-policy", "allkeys-lru"])
    adm = srv.conn()
    rng = owner_ranges(adm)
    sm = Sampler(srv)
    sm.start()
    o0 = ostat(adm)
    per = (160 * MB) // (8 * V) // (2 * N)
    acked, refused, errs = txn_load(srv, rng, b"ev", per, 2)
    peak = sm.end()
    o1 = ostat(adm)
    ev = int(info(adm, "stats")["evicted_keys"])
    dbs = adm.cmd("DBSIZE")
    ks = list(acked)
    got = pipeline(adm, [("GET", k) for k in ks], depth=200)
    wrong = sum(1 for k, g in zip(ks, got) if g is not None and g != acked[k])
    check("mm.exec_evict", not errs and not refused and peak <= 64 * MB + 2 * MB and ev > 0 and dbs + ev == len(acked)
          and wrong == 0,
          "peak %.2f MB, acked keys %d, dbsize %d + evicted %d, wrong %d, refused %d, errs %s, native +%d"
          % (peak / MB, len(acked), dbs, ev, wrong, len(refused), errs[:2],
             o1["owner_native_commands"] - o0["owner_native_commands"]))
    crash = srv.crashed()
    adm.close()
    srv.stop()
    check("mm.evict_no_crash", crash == 0, crash)

    srv = Server("all-noev", ["--maxmemory", "40mb", "--maxmemory-policy", "noeviction"])
    adm = srv.conn()
    rng = owner_ranges(adm)
    per = (80 * MB) // (8 * V) // (2 * N)
    acked, refused, errs = txn_load(srv, rng, b"ne", per, 2)
    ks = list(acked)
    got = pipeline(adm, [("GET", k) for k in ks], depth=200)
    missing = sum(1 for k, g in zip(ks, got) if g != acked[k])
    rk = [k for r in refused for k in r[0]]
    present = sum(1 for x in pipeline(adm, [("EXISTS", k) for k in rk], depth=200) if x) if rk else 0
    kinds = sorted({str(r[1][2])[:40] if not isinstance(r[1][1], Err) else "queue:" + str(r[1][1])[:30]
                    for r in refused})
    check("mm.exec_noeviction", not errs and refused and missing == 0 and present == 0,
          "acked txns %d, refused %d %s, acked keys missing %d, refused keys present %d, errs %s"
          % (len(acked) // 8, len(refused), kinds[:3], missing, present, errs[:2]))
    crash = srv.crashed()
    adm.close()
    srv.stop()
    check("mm.no_crash", crash == 0, "crash lines %d, first pid %d" % (crash, pid))


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=36379)
    ap.add_argument("--owner-port", type=int, default=36400)
    ap.add_argument("--cpus", default="48-51")
    ap.add_argument("--main-cpu", default="56")
    ap.add_argument("--only", choices=["aof", "mm"])
    ARGS = ap.parse_args()
    veto.ARGS = ARGS
    random.seed(7)
    if ARGS.only in (None, "aof"):
        aof_checks()
    if ARGS.only in (None, "mm"):
        mm_checks()
    res = d3.RESULTS
    fails = [r["check"] for r in res if not r["ok"]]
    print("D3ALL " + json.dumps(dict(n=ARGS.n, checks=len(res), fails=fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
