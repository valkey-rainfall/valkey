#!/usr/bin/env python3
"""D3mk gate: native single-owner MGET / MSET / DEL / MULTI-EXEC against a model and under concurrency.

Server: owner-threads N (owner-port P..P+N-1, plain port). Keys are hash-tagged {t...} so a group of keys
shares one slot; a tag is "own" for owner i when its slot is in owner i's range (CLUSTER SLOTS).

  native     own-tag MGET-8 / MSET-8 / DEL-8 / MULTI(SET INCR GET MGET MSET DEL PING)EXEC on owner i's port:
             replies right, owner_native_commands grows, owner_handoffs_to_main stays flat
  model      deep pipelines per connection (every owner port + the plain port) of random single- and
             multi-key commands and transactions on the connection's own tags, plus other owners' tags
             and two-slot groups (main fallback mid-pipeline and mid-transaction), reply by reply vs a model
  atomic     8-key groups written only as a whole (MULTI SET x8 EXEC, MSET-8) by writers on the owner
             port, another owner's port and the plain port; readers on all three paths (MGET-8 and
             MULTI GET x8 EXEC) never see the 8 values differ (no partial MULTI/MSET is visible)
  lin        MULTI INCR a INCR b EXEC from all three paths: every reply has a == b, values unique 1..total,
             increasing per connection; a read issued after an EXEC returned v sees >= v on every path
  fallback   cross-owner MGET/MSET/DEL and MULTI, two-slot same-owner groups: right replies (no CROSSSLOT:
             the server is standalone), counted as handoffs
  errors     queued-command error inside a native EXEC, EXECABORT after a queue-time error, DISCARD,
             EXEC/DISCARD without MULTI, nested MULTI; commandstats for mget/mset/multi/exec/incr
             match the stock semantics (failed_calls of the queued command, not of EXEC)

Usage: d3-multikey-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines and 'D3MK {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")
Conn, Err, key_slot = d3.Conn, d3.Err, d3.key_slot

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(dict(check=name, ok=bool(ok), detail=str(detail)[:400]))
    print(("PASS " if ok else "FAIL ") + name + (": " + str(detail) if detail != "" else ""), flush=True)
    return ok


def ostat(c):
    raw = c.cmd("INFO", "owners")
    raw = raw.decode() if isinstance(raw, bytes) else raw
    out = {}
    for l in raw.split("\r\n"):
        if ":" in l and l.startswith("owner_"):
            k, v = l.split(":", 1)
            if v.lstrip("-").isdigit():
                out[k] = int(v)
    return out


def cmdstat(c, name):
    raw = c.cmd("INFO", "commandstats")
    raw = raw.decode() if isinstance(raw, bytes) else raw
    for l in raw.split("\r\n"):
        if l.startswith("cmdstat_%s:" % name):
            return {k: int(float(v)) for k, v in (x.split("=") for x in l.split(":", 1)[1].split(","))
                    if k in ("calls", "failed_calls", "rejected_calls")}
    return {"calls": 0, "failed_calls": 0, "rejected_calls": 0}


class Model:
    """Expected replies for one connection's private keys (strings only)."""

    def __init__(self):
        self.d = {}
        self.multi = None  # list of queued commands while in MULTI

    def apply(self, cmd):
        op = cmd[0].upper() if isinstance(cmd[0], str) else cmd[0].decode().upper()
        a = cmd[1:]
        if self.multi is not None:
            if op == "EXEC":
                q, self.multi = self.multi, None
                return [self.apply(x) for x in q]
            if op == "DISCARD":
                self.multi = None
                return "OK"
            self.multi.append(cmd)
            return "QUEUED"
        if op == "MULTI":
            self.multi = []
            return "OK"
        if op == "SET":
            self.d[a[0]] = a[1]
            return "OK"
        if op == "GET":
            return self.d.get(a[0])
        if op == "INCR":
            v = int(self.d.get(a[0], b"0")) + 1
            self.d[a[0]] = b"%d" % v
            return v
        if op == "DEL":
            n = 0
            for k in a:
                if k in self.d:
                    del self.d[k]
                    n += 1
            return n
        if op == "MGET":
            return [self.d.get(k) for k in a]
        if op == "MSET":
            for i in range(0, len(a), 2):
                self.d[a[i]] = a[i + 1]
            return "OK"
        if op == "PING":
            return "PONG"
        if op == "EXISTS":
            return sum(1 for k in a if k in self.d)
        raise ValueError(op)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--model-ops", type=int, default=4000)
    ap.add_argument("--atomic-secs", type=float, default=4.0)
    ap.add_argument("--lin-txns", type=int, default=1500)
    a = ap.parse_args()
    H, N = a.host, a.n
    ports = [a.owner_port + i for i in range(N)]
    rnd = random.Random(11)
    adm = Conn(H, a.port)
    pid0 = d3.info(adm, "server")["process_id"]
    adm.cmd("FLUSHALL")

    sl = Conn(H, ports[0])
    owner = [None] * 16384
    for ent in sl.cmd("CLUSTER", "SLOTS"):
        for s in range(ent[0], ent[1] + 1):
            owner[s] = int(ent[2][1])
    sl.close()

    tag_cache = {}

    def tags_for(port, n, prefix=b"t"):
        """n distinct hash tags whose slot is owned by port, one slot each."""
        ck = (port, n, prefix)
        if ck in tag_cache:
            return tag_cache[ck]
        out, seen, i = [], set(), 0
        while len(out) < n:
            t = b"%s%d" % (prefix, i)
            i += 1
            s = key_slot(b"{%s}" % t)
            if owner[s] == port and s not in seen:
                seen.add(s)
                out.append(t)
        tag_cache[ck] = out
        return out

    def group(tag, n=8, name=b"k"):
        return [b"{%s}:%s%d" % (tag, name, j) for j in range(n)]

    other = lambda p: ports[(ports.index(p) + 1) % N]

    # --- native: own-tag multi-key commands and transactions on every owner port, no handoff
    bad, total_cmds = [], 0
    s0 = ostat(adm)
    for p in ports:
        c = Conn(H, p)
        c.cmd("PING")
        m = Model()
        tg = tags_for(p, 4, b"nat")
        cmds = []
        for r in range(50):
            g = group(tg[r % 4])
            kv = []
            for j, k in enumerate(g):
                kv += [k, b"v%d.%d" % (r, j)]
            cmds += [("MSET",) + tuple(kv), ("MGET",) + tuple(g), ("DEL",) + tuple(g[:3]), ("MGET",) + tuple(g),
                     ("MULTI",), ("SET", g[0], b"x%d" % r), ("INCR", b"{%s}:c" % tg[r % 4]), ("GET", g[0]),
                     ("MGET", g[0], g[5]), ("MSET", g[1], b"y", g[2], b"z"), ("DEL", g[7]), ("PING",), ("EXEC",)]
        want = [m.apply(x) for x in cmds]
        c.send(cmds)
        got = [c.read() for _ in want]
        total_cmds += len(cmds)
        if got != want:
            i = next(i for i, (x, y) in enumerate(zip(got, want)) if x != y)
            bad.append((p, i, cmds[i][0], got[i], want[i]))
        c.close()
    s1 = ostat(adm)
    dn = s1["owner_native_commands"] - s0["owner_native_commands"]
    dh = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
    check("native.replies_match_model", not bad, bad[:3])
    # 13 commands a round, all calls but the 7 queued ones, plus EXEC's 7 queued calls: native >= sent
    check("native.served_on_owner", dn >= total_cmds and dh <= N,
          "native +%d for %d commands sent, handoffs +%d (N=%d connection closes allowed)" % (dn, total_cmds, dh, N))

    # --- model: deep random pipelines, own tags + other owners' tags + two-slot groups, all ports
    errs = []

    def model_worker(idx):
        port = (ports + [a.port])[idx % (N + 1)]
        home = port if port != a.port else ports[idx % N]
        rr = random.Random(1000 + idx)
        # per-connection tags: no two connections share a key, so each model is exact
        own_tags = tags_for(home, 3, b"mo%d." % idx)
        far_tags = tags_for(other(home), 2, b"mf%d." % idx)
        two = tags_for(home, 2, b"mt%d." % idx)  # two slots, same owner: keys of both in one command
        groups = [group(t, 4) for t in own_tags] + [group(t, 4) for t in far_tags] + [group(two[0], 2) + group(two[1], 2)]
        weights = [5, 5, 5, 1, 1, 1]
        m = Model()
        c = Conn(H, port)
        c.cmd("PING")
        cmds = []

        def one(gk):
            op = rr.choice(["SET", "GET", "INCR", "MGET", "MSET", "DEL", "PING", "EXISTS"])
            k = rr.choice(gk)
            if op == "SET":
                return ("SET", k, b"s%d" % rr.randrange(1000))
            if op == "GET":
                return ("GET", k)
            if op == "INCR":
                return ("INCR", gk[0] + b":n")
            if op == "MGET":
                return ("MGET",) + tuple(rr.sample(gk, rr.randrange(1, len(gk) + 1)))
            if op == "MSET":
                ks = rr.sample(gk, rr.randrange(1, len(gk) + 1))
                kv = []
                for x in ks:
                    kv += [x, b"m%d" % rr.randrange(1000)]
                return ("MSET",) + tuple(kv)
            if op == "DEL":
                return ("DEL",) + tuple(rr.sample(gk, rr.randrange(1, len(gk) + 1)))
            if op == "EXISTS":
                return ("EXISTS",) + tuple(rr.sample(gk, rr.randrange(1, len(gk) + 1)))
            return ("PING",)

        n = 0
        while n < a.model_ops:
            gk = rr.choices(groups, weights)[0]
            if rr.random() < 0.3:
                body = [one(gk) for _ in range(rr.randrange(1, 6))]
                if rr.random() < 0.15:  # a command on another group inside the transaction
                    body.insert(rr.randrange(len(body) + 1), one(rr.choice(groups)))
                end = ("DISCARD",) if rr.random() < 0.1 else ("EXEC",)
                cmds += [("MULTI",)] + body + [end]
                n += len(body) + 2
            else:
                cmds.append(one(gk))
                n += 1
        want = [m.apply(x) for x in cmds]
        for i in range(0, len(cmds), 500):
            c.send(cmds[i:i + 500])
        got = [c.read() for _ in want]
        nbad = sum(1 for x, y in zip(got, want) if x != y)
        if nbad:
            i = next(i for i, (x, y) in enumerate(zip(got, want)) if x != y)
            errs.append((idx, port, nbad, i, cmds[i], got[i], want[i]))
        # main reads back every key unchanged (own connection: workers run concurrently)
        allk = sorted(set(k for g in groups for k in g) | set(g[0] + b":n" for g in groups))
        mc = Conn(H, a.port)
        back = mc.cmd("MGET", *allk)
        mc.close()
        if back != [m.d.get(k) for k in allk]:
            errs.append((idx, port, "readback"))
        c.close()

    s0 = ostat(adm)
    th = [threading.Thread(target=model_worker, args=(i,)) for i in range(2 * (N + 1))]
    [t.start() for t in th]
    [t.join() for t in th]
    s1 = ostat(adm)
    check("model.random_pipelines_all_paths", not errs,
          "%d conns x %d ops, native +%d, handoffs +%d, mismatches %s" %
          (len(th), a.model_ops, s1["owner_native_commands"] - s0["owner_native_commands"],
           s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"], errs[:2]))

    # --- atomic: 8-key groups written only as a whole, from three paths; readers on three paths
    p = ports[N // 2]
    tg = tags_for(p, 1, b"at")[0]
    g = group(tg)
    adm.cmd("MSET", *[x for k in g for x in (k, b"init")])
    stop = threading.Event()
    werr, rbad, rcount, wcount = [], [], [0], [0]
    lock = threading.Lock()

    def writer(port, wid):
        c = Conn(H, port)
        i = 0
        try:
            while not stop.is_set():
                i += 1
                v = b"w%d.%d" % (wid, i)
                if i % 2:
                    r = c.cmd("MSET", *[x for k in g for x in (k, v)])
                    ok = r == "OK"
                else:
                    c.send([("MULTI",)] + [("SET", k, v) for k in g] + [("EXEC",)])
                    r = [c.read() for _ in range(len(g) + 2)]
                    ok = r[-1] == ["OK"] * len(g)
                if not ok:
                    werr.append((port, r))
                    return
        finally:
            with lock:
                wcount[0] += i
            c.close()

    def reader(port):
        c = Conn(H, port)
        i = 0
        try:
            while not stop.is_set():
                i += 1
                if i % 2:
                    vals = c.cmd("MGET", *g)
                else:
                    c.send([("MULTI",)] + [("GET", k) for k in g] + [("EXEC",)])
                    vals = [c.read() for _ in range(len(g) + 2)][-1]
                if not isinstance(vals, list) or len(set(vals)) != 1:
                    rbad.append((port, vals))
        finally:
            with lock:
                rcount[0] += i
            c.close()

    s0 = ostat(adm)
    paths = [p, other(p), a.port]
    th = [threading.Thread(target=writer, args=(x, j)) for j, x in enumerate(paths)]
    th += [threading.Thread(target=reader, args=(x,)) for x in paths for _ in range(2)]
    [t.start() for t in th]
    time.sleep(a.atomic_secs)
    stop.set()
    [t.join() for t in th]
    s1 = ostat(adm)
    check("atomic.no_partial_group_visible", not rbad and not werr,
          "%d writes, %d reads on ports %s, partial reads %d %s, writer errors %s, native +%d" %
          (wcount[0], rcount[0], paths, len(rbad), rbad[:2], werr[:2],
           s1["owner_native_commands"] - s0["owner_native_commands"]))
    check("atomic.native_path_exercised", s1["owner_native_commands"] - s0["owner_native_commands"] > 1000,
          s1["owner_native_commands"] - s0["owner_native_commands"])

    # --- lin: MULTI INCR a INCR b EXEC from three paths; a == b in every reply, unique, increasing
    lt = tags_for(ports[0], 1, b"lin")[0]
    ka, kb = b"{%s}:a" % lt, b"{%s}:b" % lt
    adm.cmd("DEL", ka, kb)
    lpaths = [ports[0], ports[0], other(ports[0]), a.port]
    seen = [[] for _ in lpaths]
    lerr = []

    def lin_worker(t):
        c = Conn(H, lpaths[t])
        for _ in range(0, a.lin_txns, 25):
            c.send([y for _ in range(25) for y in (("MULTI",), ("INCR", ka), ("INCR", kb), ("EXEC",))])
            for _ in range(25):
                r = [c.read() for _ in range(4)][-1]
                if not (isinstance(r, list) and len(r) == 2 and r[0] == r[1]):
                    lerr.append(r)
                else:
                    seen[t].append(r[0])
        c.close()

    s0 = ostat(adm)
    th = [threading.Thread(target=lin_worker, args=(t,)) for t in range(len(lpaths))]
    [t.start() for t in th]
    [t.join() for t in th]
    s1 = ostat(adm)
    allv = sorted(v for s in seen for v in s)
    total = len(lpaths) * a.lin_txns
    mono = all(all(x < y for x, y in zip(s, s[1:])) for s in seen)
    fin = adm.cmd("MGET", ka, kb)
    check("lin.multi_incr_pairs_mixed_paths",
          not lerr and mono and allv == list(range(1, total + 1)) and fin == [b"%d" % total] * 2,
          "%d txns on %s: a!=b %d, increasing=%s, unique=%s, final=%s, native +%d" %
          (total, lpaths, len(lerr), mono, allv == list(range(1, total + 1)), fin,
           s1["owner_native_commands"] - s0["owner_native_commands"]))
    # real time: after an EXEC returned v (any path), a read from every path sees >= v
    wr = [Conn(H, ports[0]), Conn(H, a.port), Conn(H, other(ports[0]))]
    rd = [Conn(H, ports[0]), Conn(H, a.port), Conn(H, other(ports[0]))]
    rt_bad = 0
    for i in range(600):
        w = wr[i % 3]
        w.send([("MULTI",), ("INCR", ka), ("INCR", kb), ("EXEC",)])
        v = [w.read() for _ in range(4)][-1][0]
        for r in rd:
            x = r.cmd("MGET", ka, kb)
            if int(x[0]) < v or x[0] != x[1]:
                rt_bad += 1
    check("lin.real_time_reads_after_exec", rt_bad == 0, "%d stale or split reads of %d" % (rt_bad, 600 * 3))
    [x.close() for x in wr + rd]

    # --- fallback: cross-owner and two-slot multi-key commands and transactions
    p0 = ports[0]
    c = Conn(H, p0)
    c.cmd("PING")
    mine = group(tags_for(p0, 1, b"fb")[0], 2)
    far = group(tags_for(other(p0), 1, b"fb")[0], 2)
    two = tags_for(p0, 2, b"fs")
    tw = [group(two[0], 1)[0], group(two[1], 1)[0]]
    s0 = ostat(adm)
    r = [c.cmd("MSET", mine[0], "a", far[0], "b"), c.cmd("MGET", mine[0], far[0], far[1]),
         c.cmd("MSET", tw[0], "x", tw[1], "y"), c.cmd("MGET", tw[0], tw[1]), c.cmd("DEL", mine[0], far[0], tw[0], tw[1])]
    c.send([("MULTI",), ("SET", mine[1], "m"), ("SET", far[1], "f"), ("MGET", mine[1], far[1]), ("EXEC",)])
    r.append([c.read() for _ in range(5)])
    s1 = ostat(adm)
    want = ["OK", [b"a", b"b", None], "OK", [b"x", b"y"], 4, ["OK", "QUEUED", "QUEUED", "QUEUED", ["OK", "OK", [b"m", b"f"]]]]
    check("fallback.cross_owner_and_two_slot_correct", r == want and
          (N == 1 or s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"] >= 2), (r, s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    time.sleep(0.3)
    s0 = ostat(adm)
    r = [c.cmd("MGET", mine[0], mine[1]) for _ in range(20)]
    s1 = ostat(adm)
    check("fallback.client_back_home_native", r == [[None, b"m"]] * 20 and
          s1["owner_native_commands"] - s0["owner_native_commands"] >= 20, s1["owner_native_commands"] - s0["owner_native_commands"])

    # --- errors and stats inside native transactions
    k = group(tags_for(p0, 1, b"er")[0], 2)
    adm.cmd("CONFIG", "RESETSTAT")
    s0 = ostat(adm)
    c.send([("MULTI",), ("SET", k[0], "notanint"), ("INCR", k[0]), ("MGET", k[0], k[1]), ("EXEC",)])
    r1 = [c.read() for _ in range(5)]
    s1 = ostat(adm)
    ex = r1[-1]
    inner_err = isinstance(ex, list) and len(ex) == 3 and ex[0] == "OK" and isinstance(ex[1], Err) and ex[2] == [b"notanint", None]
    st = {n: cmdstat(adm, n) for n in ("multi", "exec", "incr", "mget", "set")}
    check("errors.queued_error_inside_native_exec", inner_err and
          s1["owner_handoffs_to_main"] == s0["owner_handoffs_to_main"], (r1, s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    check("errors.commandstats_as_stock",
          st["multi"]["calls"] == 1 and st["exec"] == {"calls": 1, "failed_calls": 0, "rejected_calls": 0} and
          st["incr"]["calls"] == 1 and st["incr"]["failed_calls"] == 1 and st["mget"]["calls"] == 1 and st["set"]["calls"] == 1, st)
    c.send([("MULTI",), ("SET", k[0], "1"), ("GET",), ("EXEC",), ("GET", k[0])])
    r2 = [c.read() for _ in range(5)]
    check("errors.execabort_after_queue_error", r2[0] == "OK" and r2[1] == "QUEUED" and isinstance(r2[2], Err) and
          isinstance(r2[3], Err) and r2[3].startswith("EXECABORT") and r2[4] == b"notanint", r2)
    c.send([("MULTI",), ("SET", k[0], "2"), ("DISCARD",), ("GET", k[0]), ("EXEC",), ("DISCARD",), ("MULTI",), ("MULTI",),
            ("SET", k[1], "q"), ("EXEC",), ("GET", k[1])])
    r3 = [c.read() for _ in range(11)]
    # EXEC / DISCARD without MULTI: errors; a nested MULTI is rejected and aborts the transaction
    check("errors.discard_and_misuse", r3[:4] == ["OK", "QUEUED", "OK", b"notanint"] and isinstance(r3[4], Err) and
          isinstance(r3[5], Err) and r3[6] == "OK" and isinstance(r3[7], Err) and r3[8] == "QUEUED" and
          isinstance(r3[9], Err) and r3[9].startswith("EXECABORT") and r3[10] is None, r3)
    c.close()

    # --- watch: WATCH/UNWATCH of own keys on the owner; a write from every path aborts the EXEC
    def iget(section, field):
        return d3.info(adm, section).get(field)

    p = ports[0]
    wt = tags_for(p, 1, b"wa")[0]
    wk, wk2, wn = b"{%s}:w" % wt, b"{%s}:w2" % wt, b"{%s}:n" % wt
    far_k = b"{%s}:x" % tags_for(other(p), 1, b"wa")[0]
    c = Conn(H, p)
    c.cmd("PING")
    time.sleep(0.2)
    writers = {"same_owner": Conn(H, p), "plain": Conn(H, a.port), "other_owner": Conn(H, other(p))}
    res, ok = {}, True
    for name, wr in writers.items():
        adm.cmd("SET", wk, "0")
        time.sleep(0.1)
        s0 = ostat(adm)
        r = [c.cmd("WATCH", wk, wk2), c.cmd("GET", wk)]
        during = (iget("clients", "watching_clients"), iget("clients", "total_watched_keys"), iget("owners", "owner_veto"))
        wr.cmd("SET", wk, name)
        c.send([("MULTI",), ("SET", wk, "mine"), ("EXEC",)])
        r += [c.read() for _ in range(3)] + [c.cmd("GET", wk)]
        after = iget("clients", "watching_clients")
        s1 = ostat(adm)
        dh = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
        res[name] = (r, during, after, dh)
        # the watcher stays on its owner: only the other owner's writer hands off
        ok &= r == ["OK", b"0", "OK", "QUEUED", None, name.encode()] and during == ("1", "2", "none") and \
            after == "0" and dh == (1 if name == "other_owner" else 0)
    check("watch.native_aborts_on_write_from_every_path", ok, res)
    s0 = ostat(adm)
    c.send([("WATCH", wk), ("GET", wk), ("MULTI",), ("INCR", wn), ("EXEC",), ("WATCH", wk), ("UNWATCH",)])
    r = [c.read() for _ in range(7)]
    writers["same_owner"].cmd("SET", wk, "z")
    c.send([("MULTI",), ("SET", wk2, "y"), ("EXEC",)])
    r += [c.read() for _ in range(3)]
    s1 = ostat(adm)
    check("watch.untouched_and_unwatch_succeed_native",
          r == ["OK", b"other_owner", "OK", "QUEUED", [1], "OK", "OK", "OK", "QUEUED", ["OK"]] and
          s1["owner_handoffs_to_main"] == s0["owner_handoffs_to_main"] and iget("clients", "watching_clients") == "0",
          (r, s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    # a native watcher that falls back (another owner's key) becomes a stock watcher on main
    r = [c.cmd("WATCH", wk), c.cmd("GET", far_k)]
    moved = (iget("clients", "watching_clients"), iget("owners", "owner_veto"))
    writers["same_owner"].cmd("SET", wk, "after-move")
    c.send([("MULTI",), ("SET", wk, "mine"), ("EXEC",)])
    r += [c.read() for _ in range(3)] + [c.cmd("GET", wk)]
    time.sleep(0.3)
    s0 = ostat(adm)
    home = [c.cmd("GET", wk) for _ in range(20)]
    s1 = ostat(adm)
    check("watch.fallback_moves_watcher_to_main",
          r == ["OK", None, "OK", "QUEUED", None, b"after-move"] and moved == ("1", "all") and
          iget("clients", "watching_clients") == "0" and s1["owner_native_commands"] - s0["owner_native_commands"] >= 20,
          (r, moved, s1["owner_native_commands"] - s0["owner_native_commands"]))
    # an expired watched key fails the EXEC; FLUSHALL touches native watchers
    adm.cmd("PSETEX", wk, 300, "v")
    r = [c.cmd("WATCH", wk)]
    time.sleep(0.5)
    c.send([("MULTI",), ("GET", wk), ("EXEC",)])
    r += [c.read() for _ in range(3)]
    c.cmd("SET", wk, "1")
    r += [c.cmd("WATCH", wk), adm.cmd("FLUSHALL")]
    c.send([("MULTI",), ("SET", wk, "2"), ("EXEC",)])
    r += [c.read() for _ in range(3)] + [c.cmd("GET", wk)]
    check("watch.expired_key_and_flushall_abort", r == ["OK", "OK", "QUEUED", None, "OK", "OK", "OK", "QUEUED", None, None], r)
    [w.close() for w in writers.values()]
    c.close()

    # --- watch CAS: WATCH k; GET k; MULTI; SET k v+1; EXEC from the owner, another owner and main
    ck = b"{%s}:cas" % tags_for(ports[N // 2], 1, b"cas")[0]
    adm.cmd("SET", ck, 0)
    cpaths = [ports[N // 2], ports[N // 2], other(ports[N // 2]), a.port]
    wins, tries, cerr = [0] * len(cpaths), [0] * len(cpaths), []

    def cas_worker(t):
        cc = Conn(H, cpaths[t])
        while wins[t] < 150 and tries[t] < 30000:
            tries[t] += 1
            cc.send([("WATCH", ck), ("GET", ck)])
            v = [cc.read() for _ in range(2)][1]
            cc.send([("MULTI",), ("SET", ck, int(v) + 1), ("EXEC",)])
            r = [cc.read() for _ in range(3)][2]
            if r == ["OK"]:
                wins[t] += 1
            elif r is not None:
                cerr.append(r)
        cc.close()

    s0 = ostat(adm)
    th = [threading.Thread(target=cas_worker, args=(t,)) for t in range(len(cpaths))]
    [t.start() for t in th]
    [t.join() for t in th]
    s1 = ostat(adm)
    fin = int(adm.cmd("GET", ck))
    check("watch.cas_no_lost_update_mixed_paths", not cerr and fin == sum(wins) and min(wins) == 150,
          "wins %s tries %s final %d errors %s native +%d watching_clients %s" %
          (wins, tries, fin, cerr[:2], s1["owner_native_commands"] - s0["owner_native_commands"],
           iget("clients", "watching_clients")))
    check("watch.registries_empty_after", iget("clients", "watching_clients") == "0" and
          iget("clients", "total_watched_keys") == "0", (iget("clients", "watching_clients"), iget("clients", "total_watched_keys")))

    alive = {}
    for x in ports + [a.port]:
        try:
            cc = Conn(H, x, timeout=5)
            alive[x] = cc.cmd("PING") == "PONG"
            cc.close()
        except Exception as e:
            alive[x] = repr(e)
    check("no_crash.same_pid_every_port_PONG", d3.info(adm, "server")["process_id"] == pid0 and all(v is True for v in alive.values()), alive)
    nfail = sum(1 for r in RESULTS if not r["ok"])
    print("D3MK " + json.dumps(dict(n=N, passed=len(RESULTS) - nfail, failed=nfail)))
    sys.exit(1 if nfail else 0)


if __name__ == "__main__":
    main()
