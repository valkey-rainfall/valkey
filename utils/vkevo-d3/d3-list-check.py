#!/usr/bin/env python3
"""RL-g1b-native-list gate: owner threads run single-key list commands (LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX)
natively. Starts its own servers (127.0.0.1 only) and stops them by their own connection or PID.

Suites:
  native   each list command on an owner's own key through its owner port runs on the owner (owner_native_commands
           grows by the count, handoffs stay flat); the reference binary (the parent) hands them to main
  model    8 connections at once, each a random stream on its own keys (LPUSH RPUSH LPOP RPOP with and without count,
           LRANGE LLEN LINDEX, DEL, SET/GET collisions (WRONGTYPE), PEXPIREAT, PERSIST, a 1 ms PEXPIRE then a sleep
           (lazy expiry on the owner), pushes past the listpack limit (quicklist) and pops back, MULTI/EXEC, LMOVE,
           OBJECT ENCODING, arity and syntax errors), batches pipelined to the owner port of their first key or the
           plain port. Runs: this build with owners (AOF everysec + a replica; AOF always; AOF everysec RESP + a
           replica), this build with owners vetoed (the main path) and the reference binary. Every reply and the final
           keyspace (type, encoding, contents, expire time) equal across runs; each replica equals its primary (digest
           and dump); each AOF reloaded into a fresh server equals the primary
  blocked  clients blocked on main (BLPOP BRPOP BLMOVE BLMPOP, connected to owner ports or the plain port) woken by
           pushes from owner ports, single and inside MULTI; no native command while a client is blocked (the veto),
           native again afterwards; blocking timeouts; 8 blocker/pusher pairs x rounds (half the pushes wait for the
           block, half race it) under a native list load whose every reply is checked against a model; no lost
           wakeup; a replica equals the primary at the end

Usage: d3-list-check.py --bin B --ref-bin PARENT --dir D --port P --owner-port OP --n 4 --cpus 20-23 --main-cpu 24
       --replica-cpus 29-30 [--suites native,model,blocked]
PASS/FAIL lines, a final 'LISTCHECK {json}' line; exit 1 on any FAIL.
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
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS

A = None
check = d3.check


class Srv:
    def __init__(self, name, binary, port, d, args, cpus, fresh=True):
        self.name, self.binary, self.port, self.cpus, self.args = name, binary, port, cpus, list(args)
        self.dir = os.path.join(d, name)
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        cmd = ["taskset", "-c", cpus, binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", os.path.join(self.dir, "log")] + self.args
        if "--appendonly" not in self.args:
            cmd += ["--appendonly", "no"]
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(600):
            try:
                c = d3.Conn("127.0.0.1", port, timeout=5)
                r = c.cmd("PING")
                c.close()
                if r == "PONG":
                    break
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        self.c = d3.Conn("127.0.0.1", port, timeout=120)
        self.ranges = []
        raw = self.c.cmd("INFO", "owners")
        raw = raw.decode() if isinstance(raw, bytes) else raw
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))

    def oport(self, key):
        s = d3.key_slot(key if isinstance(key, bytes) else key.encode())
        for i, (lo, hi) in enumerate(self.ranges):
            if lo <= s <= hi:
                return A.owner_port + i
        return self.port

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


def primary(name, binary, d, extra=(), fresh=True):
    args = ["--owner-threads", str(A.n), "--owner-port", str(A.owner_port), "--owner-cpulist", A.cpus,
            "--server-cpulist", str(A.main_cpu), "--repl-backlog-size", "16mb"] + list(extra)
    return Srv(name, binary, A.port, d, args, "%s,%s" % (A.main_cpu, A.cpus), fresh)


def replica(name, d):
    r = Srv(name, A.bin, A.port + 1, d, ["--replicaof", "127.0.0.1", str(A.port), "--repl-diskless-load", "disabled"],
            A.replica_cpus)
    t0 = time.time()
    while time.time() - t0 < 60:
        i = d3.info(r.c, "replication")
        if i.get("master_link_status") == "up" and i.get("master_sync_in_progress", "0") == "0":
            break
        time.sleep(0.05)
    return r


def norm(x):
    if isinstance(x, d3.Err):
        return ("ERR", str(x))
    if isinstance(x, list):
        return [norm(y) for y in x]
    return x


def dump(c):
    keys, cur = [], 0
    while True:
        cur, ks = c.cmd("SCAN", cur, "COUNT", 1000)
        cur = int(cur)
        keys += ks
        if cur == 0:
            break
    out = []
    for k in sorted(set(keys)):
        c.send([("TYPE", k), ("OBJECT", "ENCODING", k), ("PEXPIRETIME", k)])
        t, e, x = c.read(), c.read(), c.read()
        if t == "list":
            v = c.cmd("LRANGE", k, 0, -1)
        elif t == "string":
            v = c.cmd("GET", k)
        else:
            v = norm(c.cmd("DUMP", k))
        out.append((k, t, e, x, v))
    return out


def sync_replica(p, r, tag):
    t0 = time.time()
    po = ro = -1
    while time.time() - t0 < 60:
        po = int(d3.info(p.c, "replication")["master_repl_offset"])
        ro = int(d3.info(r.c, "replication").get("slave_repl_offset", -1))
        if po == ro:
            break
        time.sleep(0.05)
    check(tag + ".replica_offset", po == ro, "primary %d replica %d" % (po, ro))


def diff_dumps(a, b):
    da = {x[0]: x for x in a}
    db = {x[0]: x for x in b}
    bad = [k for k in sorted(set(da) | set(db)) if da.get(k) != db.get(k)]
    return "%d keys differ, first: %s" % (len(bad), [(da.get(k), db.get(k)) for k in bad[:2]]) if bad else ""


# ---------------------------------------------------------------- native
LIST_CMDS = ["LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX"]


def key_of_owner(p, i, prefix):
    j = 0
    while True:
        k = "%s%d" % (prefix, j)
        if p.oport(k) == A.owner_port + i:
            return k
        j += 1


def suite_native():
    for label, binary in (("this", A.bin), ("ref", A.ref_bin)):
        if not binary:
            continue
        d = os.path.join(A.dir, "native-" + label)
        p = primary("p", binary, d)
        res = {}
        for i in range(A.n):
            k = key_of_owner(p, i, "nat%d:" % i)
            c = d3.Conn("127.0.0.1", A.owner_port + i)
            c.cmd("RPUSH", k, *["e%d" % j for j in range(50)])
            time.sleep(0.2)
            for cmd in LIST_CMDS:
                args = {"LPUSH": (k, "a"), "RPUSH": (k, "b"), "LPOP": (k,), "RPOP": (k,), "LLEN": (k,),
                        "LRANGE": (k, 0, 5), "LINDEX": (k, 3)}[cmd]
                s0 = d3.ostat(p.c)
                c.send([(cmd,) + args] * 200)
                rr = [c.read() for _ in range(200)]
                s1 = d3.ostat(p.c)
                nat = s1["owner_native_commands"] - s0["owner_native_commands"]
                ho = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
                errs = [x for x in rr if isinstance(x, d3.Err)]
                res.setdefault(cmd, []).append((nat, ho, len(errs)))
            c.close()
        for cmd in LIST_CMDS:
            nat = sum(x[0] for x in res[cmd])
            ho = sum(x[1] for x in res[cmd])
            er = sum(x[2] for x in res[cmd])
            want = 200 * A.n
            if label == "this":
                check("native.%s.on_owner" % cmd, nat >= want and ho <= A.n and er == 0,
                      "native +%d of %d, handoffs +%d, errors %d" % (nat, want, ho, er))
            else:
                check("native.ref.%s.hands_to_main" % cmd, nat < want // 10 and er == 0,
                      "parent native +%d of %d, handoffs +%d" % (nat, want, ho))
        check("native.%s.no_crash" % label, p.crashed() == 0, p.crashed())
        p.stop()


# ---------------------------------------------------------------- model
def elem(rnd):
    r = rnd.random()
    if r < 0.3:
        return str(rnd.randrange(-300, 300))
    if r < 0.4:
        return str(rnd.randrange(1 << 40))
    if r < 0.5:
        return "0%d" % rnd.randrange(100)
    if r < 0.8:
        return "e%d" % rnd.randrange(1000)
    return "x" * rnd.randrange(30, 70) + str(rnd.randrange(100))


def model_streams(nconn, per, seed):
    out = []
    for cid in range(nconn):
        rnd = random.Random(seed * 1000 + cid)
        cmds = []
        while len(cmds) < per:
            k = "c%d:l%d" % (cid, rnd.randrange(30))
            k2 = "c%d:l%d" % (cid, rnd.randrange(30))
            t = "{c%d:t%d}" % (cid, rnd.randrange(6))
            r = rnd.random()
            if r < 0.16:
                cmds.append(("LPUSH", k) + tuple(elem(rnd) for _ in range(rnd.randrange(1, 4))))
            elif r < 0.30:
                cmds.append(("RPUSH", k) + tuple(elem(rnd) for _ in range(rnd.randrange(1, 4))))
            elif r < 0.38:
                cmds.append(("LPOP", k) + rnd.choice([(), (), (), ("0",), ("1",), ("3",), ("100",), ("-1",), ("x",)]))
            elif r < 0.48:
                cmds.append(("RPOP", k) + rnd.choice([(), (), (), ("0",), ("2",), ("50",), ("-2",), ("y",)]))
            elif r < 0.56:
                cmds.append(("LRANGE", k, str(rnd.randrange(-20, 20)), str(rnd.randrange(-20, 20))))
            elif r < 0.61:
                cmds.append(("LLEN", k))
            elif r < 0.66:
                cmds.append(("LINDEX", k, rnd.choice([str(rnd.randrange(-12, 12)), str(rnd.randrange(-12, 12)), "z"])))
            elif r < 0.68:
                cmds.append(("DEL", k))
            elif r < 0.70:
                cmds.append(("SET", k, "s%d" % rnd.randrange(99)))
            elif r < 0.71:
                cmds.append(("GET", k))
            elif r < 0.73:
                cmds.append(("PEXPIREAT", k, str(4102444800000 + rnd.randrange(1000))))
            elif r < 0.74:
                cmds.append(("PERSIST", k))
            elif r < 0.76:
                cmds += [("PEXPIRE", k, "1"), ("SLEEP", 4), (rnd.choice(["LLEN", "RPOP", "LPOP"]), k)]
            elif r < 0.78:
                cmds += [("RPUSH", k) + tuple("q%d-%s" % (j, "y" * 60) for j in range(160)), ("OBJECT", "ENCODING", k),
                         ("LPOP", k, str(rnd.randrange(100, 200))), ("OBJECT", "ENCODING", k)]
            elif r < 0.82:
                cmds += [("MULTI",), ("LPUSH", k, elem(rnd)), ("RPOP", k), ("LLEN", k), ("LRANGE", k, "0", "-1"),
                         ("EXEC",)]
            elif r < 0.84:
                cmds += [("MULTI",), ("RPUSH", k, elem(rnd)), ("LPOP", k2), ("LINDEX", k2, "0"), ("EXEC",)]
            elif r < 0.86:
                cmds += [("RPUSH", t + "a", elem(rnd)), ("LMOVE", t + "a", t + "b", "LEFT", "RIGHT"),
                         ("LRANGE", t + "b", "0", "-1")]
            elif r < 0.88:
                cmds.append(("LPUSH", t + rnd.choice("ab"), elem(rnd), elem(rnd)))
            elif r < 0.89:
                cmds.append(("LPUSH", k))  # arity error
            elif r < 0.90:
                cmds.append(("LRANGE", k, "a", "1"))
            elif r < 0.92:
                cmds.append(("OBJECT", "ENCODING", k))
            else:
                cmds.append(("RPOP", t + rnd.choice("ab")))
        out.append(cmds)
    return out


def run_streams(p, streams):
    replies = [None] * len(streams)
    fails = []

    def run(cid):
        conns = {}
        cmds = streams[cid]
        rnd = random.Random(cid)
        got = []
        i = 0
        try:
            while i < len(cmds):
                if cmds[i][0] == "SLEEP":
                    time.sleep(cmds[i][1] / 1000.0)
                    i += 1
                    continue
                if cmds[i][0] == "MULTI":
                    j = i + 1
                    while cmds[j - 1][0] != "EXEC":
                        j += 1
                else:
                    j = min(len(cmds), i + rnd.randrange(1, 9))
                    for q in range(i + 1, j):
                        if cmds[q][0] in ("MULTI", "SLEEP"):
                            j = q
                            break
                batch = cmds[i:j]
                key = batch[0][1] if len(batch[0]) > 1 else batch[1][1]
                port = p.port if rnd.random() < 0.1 else p.oport(key)
                c = conns.get(port)
                if c is None:
                    c = conns[port] = d3.Conn("127.0.0.1", port, timeout=60)
                c.send(batch)
                for _ in batch:
                    got.append(norm(c.read()))
                i = j
        except Exception as e:  # noqa: BLE001
            fails.append("conn %d: %r" % (cid, e))
        replies[cid] = got
        for c in conns.values():
            c.close()

    ts = [threading.Thread(target=run, args=(i,)) for i in range(len(streams))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return replies, fails


def model_run(label, binary, extra, streams, with_replica, aof):
    d = os.path.join(A.dir, "model-" + label)
    ex = list(extra)
    if aof:
        ex += ["--appendonly", "yes", "--appendfsync", aof]
    p = primary("p", binary, d, ex)
    r = replica("r", d) if with_replica else None
    n0 = d3.ostat(p.c)
    replies, fails = run_streams(p, streams)
    n1 = d3.ostat(p.c)
    nat = n1.get("owner_native_commands", 0) - n0.get("owner_native_commands", 0)
    check("model.%s.ran" % label, not fails, fails[:3])
    time.sleep(0.3)
    pdump = dump(p.c)
    pdig = p.c.cmd("DEBUG", "DIGEST")
    veto = d3.info(p.c, "owners").get("owner_veto")
    print("INFO model.%s native %d keys %d digest %s veto %s" % (label, nat, len(pdump), pdig, veto), flush=True)
    if r:
        sync_replica(p, r, "model." + label)
        check("model.%s.replica_digest" % label, r.c.cmd("DEBUG", "DIGEST") == pdig)
        rd = dump(r.c)
        check("model.%s.replica_keyspace" % label, rd == pdump, diff_dumps(pdump, rd))
        check("model.%s.replica.no_crash" % label, r.crashed() == 0, r.crashed())
        r.stop()
    check("model.%s.no_crash" % label, p.crashed() == 0, p.crashed())
    p.stop()
    if aof:
        q = primary("p", binary, d, list(extra) + ["--appendonly", "yes"], fresh=False)
        qd = dump(q.c)
        check("model.%s.aof_reload_keyspace" % label, qd == pdump, diff_dumps(pdump, qd))
        check("model.%s.aof_reload_digest" % label, q.c.cmd("DEBUG", "DIGEST") == pdig)
        q.stop()
    shutil.rmtree(d, ignore_errors=True)
    return replies, pdump, nat


def suite_model():
    streams = model_streams(8, A.per, 7)
    runs = [("native", A.bin, [], True, "everysec"),
            ("native_always", A.bin, [], False, "always"),
            ("native_resp", A.bin, ["--aof-incr-format", "resp"], True, "everysec"),
            ("native_noaof", A.bin, [], True, None),
            ("mainpath", A.bin, ["--owner-repl-native", "no"], True, "everysec")]
    if A.ref_bin:
        runs.append(("parent", A.ref_bin, [], True, "everysec"))
    res = {}
    for label, binary, extra, rep, aof in runs:
        res[label] = model_run(label, binary, extra, streams, rep, aof)
    base = "parent" if A.ref_bin else "mainpath"
    br, bd, _ = res[base]
    for label, (rr, dd, nat) in res.items():
        if label == base:
            continue
        bad = [(cid, j) for cid in range(len(rr)) for j in range(max(len(rr[cid]), len(br[cid])))
               if j >= len(rr[cid]) or j >= len(br[cid]) or rr[cid][j] != br[cid][j]]
        detail = ""
        if bad:
            cid, j = bad[0]
            detail = "%d replies differ; first conn %d #%d: %r vs %r" % (
                len(bad), cid, j, rr[cid][j] if j < len(rr[cid]) else None, br[cid][j] if j < len(br[cid]) else None)
        check("model.%s_vs_%s.replies" % (label, base), not bad, detail or "%d replies" % sum(len(x) for x in rr))
        check("model.%s_vs_%s.keyspace" % (label, base), dd == bd, diff_dumps(bd, dd) or "%d keys" % len(dd))
    check("model.native_ran_natively", res["native"][2] > A.per * 8 * 0.3, "native %d" % res["native"][2])
    check("model.mainpath_vetoed", res["mainpath"][2] == 0, "native %d" % res["mainpath"][2])
    if A.ref_bin:
        print("INFO model native commands: this %d, parent %d" % (res["native"][2], res["parent"][2]), flush=True)
        check("model.more_native_than_parent", res["native"][2] > res["parent"][2] * 1.3,
              "this %d parent %d" % (res["native"][2], res["parent"][2]))


# ---------------------------------------------------------------- blocked
def blocked_count(c):
    return int(d3.info(c, "clients")["blocked_clients"])


def wait_blocked(c, n, secs=10):
    t0 = time.time()
    while time.time() - t0 < secs:
        if blocked_count(c) >= n:
            return True
        time.sleep(0.002)
    return False


def suite_blocked():
    d = os.path.join(A.dir, "blocked")
    p = primary("p", A.bin, d)
    r = replica("r", d)
    adm = p.c
    # B1: each blocking command, blocker on an owner port or the plain port, pushed from an owner port
    cases = [("BLPOP", lambda q, x: ("BLPOP", q, 0), lambda q, x, v: v == [q.encode(), b"a"]),
             ("BRPOP", lambda q, x: ("BRPOP", q, 0), lambda q, x, v: v == [q.encode(), b"c"]),
             ("BLMOVE", lambda q, x: ("BLMOVE", q, x, "LEFT", "RIGHT", 0), lambda q, x, v: v == b"a"),
             ("BLMPOP", lambda q, x: ("BLMPOP", 0, 1, q, "LEFT", "COUNT", 2), lambda q, x, v: v == [q.encode(), [b"a", b"b"]])]
    for name, mk, ok in cases:
        for where in ("owner", "plain"):
            for how in ("single", "multi"):
                q = "{bq:%s:%s:%s}q" % (name, where, how)
                x = "{bq:%s:%s:%s}x" % (name, where, how)
                bport = p.oport(q) if where == "owner" else p.port
                # the blocker on a different owner than the key's when it can be
                if where == "owner" and A.n > 1:
                    bport = A.owner_port + ((p.oport(q) - A.owner_port + 1) % A.n)
                b = d3.Conn("127.0.0.1", bport, timeout=10)
                pc = d3.Conn("127.0.0.1", p.oport(q), timeout=10)
                b.send([mk(q, x)])
                blocked = wait_blocked(adm, 1)
                if how == "single":
                    pr = [pc.cmd("RPUSH", q, "a", "b", "c")]
                else:
                    pc.send([("MULTI",), ("RPUSH", q, "a", "b", "c"), ("LLEN", q), ("EXEC",)])
                    pr = [pc.read() for _ in range(4)]
                try:
                    v = b.read()
                except Exception as e:  # noqa: BLE001
                    v = "lost wakeup: %r" % e
                rest = pc.cmd("LRANGE", q, 0, -1)
                dest = pc.cmd("LRANGE", x, 0, -1)
                want_rest = {"BLPOP": [b"b", b"c"], "BRPOP": [b"a", b"b"], "BLMOVE": [b"b", b"c"], "BLMPOP": [b"c"]}[name]
                good = blocked and ok(q, x, v) and rest == want_rest and (name != "BLMOVE" or dest == [b"a"])
                check("blocked.%s.%s.%s" % (name, where, how), good,
                      "blocked %s push %r got %r rest %r dest %r" % (blocked, pr, v, rest, dest))
                pc.cmd("DEL", q, x)
                b.close()
                pc.close()
    # B2: while a client is blocked nothing runs natively (veto); native again once it is woken
    q = "{bq:veto}q"
    k0 = key_of_owner(p, 0, "vk:")
    kl = key_of_owner(p, 0, "vkl:")
    oc = d3.Conn("127.0.0.1", A.owner_port, timeout=10)
    oc.cmd("SET", k0, "x")
    time.sleep(0.3)
    b = d3.Conn("127.0.0.1", p.port, timeout=10)
    b.send([("BLPOP", q, 0)])
    wait_blocked(adm, 1)
    s0 = d3.ostat(adm)
    oc.send([("RPUSH", kl, "v")] * 100 + [("SET", k0, "y")] * 100)
    rr = [oc.read() for _ in range(200)]
    s1 = d3.ostat(adm)
    veto = d3.info(adm, "owners").get("owner_veto")
    check("blocked.veto_while_blocked", s1["owner_native_commands"] == s0["owner_native_commands"] and veto == "all"
          and rr[:100] == list(range(1, 101)) and rr[100:] == ["OK"] * 100,
          "native +%d veto %s" % (s1["owner_native_commands"] - s0["owner_native_commands"], veto))
    pq = d3.Conn("127.0.0.1", p.oport(q), timeout=10)
    pq.cmd("LPUSH", q, "wake")
    v = b.read()
    check("blocked.veto_wake", v == [q.encode(), b"wake"], v)
    time.sleep(0.5)
    s0 = d3.ostat(adm)
    oc.send([("RPUSH", kl, "v")] * 100 + [("LPOP", kl)] * 100)
    rr = [oc.read() for _ in range(200)]
    s1 = d3.ostat(adm)
    check("blocked.native_after_wake", s1["owner_native_commands"] - s0["owner_native_commands"] >= 150
          and rr[:100] == list(range(101, 201)), "native +%d" % (s1["owner_native_commands"] - s0["owner_native_commands"]))
    # B3: timeouts
    t0 = time.time()
    v = b.cmd("BLPOP", "{bq:to}q", "0.2")
    el = time.time() - t0
    check("blocked.timeout", v is None and 0.15 <= el < 3, "got %r after %.2f s" % (v, el))
    time.sleep(0.3)
    s0 = d3.ostat(adm)
    oc.send([("LLEN", kl)] * 100)
    rr = [oc.read() for _ in range(100)]
    s1 = d3.ostat(adm)
    check("blocked.native_after_timeout", s1["owner_native_commands"] - s0["owner_native_commands"] >= 90 and
          rr == [100] * 100, "native +%d" % (s1["owner_native_commands"] - s0["owner_native_commands"]))
    for c in (b, oc, pq):
        c.close()
    # B4: pairs under a native list load
    stop = threading.Event()
    errs = []
    loads = [0] * 4
    n_nat0 = d3.ostat(adm)["owner_native_commands"]

    def loader(i):
        rnd = random.Random(100 + i)
        conns = {}
        model = {}
        try:
            while not stop.is_set():
                k = "ld%d:%d" % (i, rnd.randrange(40))
                port = p.oport(k)
                c = conns.get(port) or conns.setdefault(port, d3.Conn("127.0.0.1", port, timeout=30))
                m = model.setdefault(k, [])
                op = rnd.random()
                if op < 0.4:
                    e = "v%d" % rnd.randrange(1000)
                    m.insert(0, e.encode())
                    want, got = len(m), c.cmd("LPUSH", k, e)
                elif op < 0.75:
                    want = m.pop() if m else None
                    got = c.cmd("RPOP", k)
                elif op < 0.9:
                    want, got = len(m), c.cmd("LLEN", k)
                else:
                    want, got = list(m), c.cmd("LRANGE", k, 0, -1)
                loads[i] += 1
                if got != want:
                    errs.append("load %d %s: got %r want %r" % (i, k, got, want))
                    return
        except Exception as e:  # noqa: BLE001
            errs.append("load %d: %r" % (i, e))
        finally:
            for c in conns.values():
                c.close()

    lost = []
    rounds = [0] * A.pairs

    def pair(i):
        rnd = random.Random(200 + i)
        q = "pq%d" % i
        try:
            for rd in range(A.rounds):
                bport = rnd.choice([p.port] + [A.owner_port + j for j in range(A.n)])
                b = d3.Conn("127.0.0.1", bport, timeout=10)
                pc = d3.Conn("127.0.0.1", p.oport(q), timeout=10)
                cid = b.cmd("CLIENT", "ID")
                kind = rnd.choice(["BLPOP", "BRPOP", "BLMPOP"])
                cmd = ("BLMPOP", 0, 1, q, "LEFT") if kind == "BLMPOP" else (kind, q, 0)
                b.send([cmd])
                v = "r%d-%d" % (i, rd)
                if rnd.random() < 0.5:  # wait for the block, else race it
                    t0 = time.time()
                    while time.time() - t0 < 10:
                        cl = pc.cmd("CLIENT", "LIST", "ID", cid)
                        if b"flags=b" in cl:
                            break
                        time.sleep(0.001)
                pc.cmd("RPUSH", q, v)
                try:
                    got = b.read()
                except Exception as e:  # noqa: BLE001
                    got = "timeout %r" % e
                want = [q.encode(), [v.encode()]] if kind == "BLMPOP" else [q.encode(), v.encode()]
                if got != want:
                    lost.append("pair %d round %d %s: got %r want %r" % (i, rd, kind, got, want))
                    return
                rounds[i] += 1
                b.close()
                pc.close()
        except Exception as e:  # noqa: BLE001
            lost.append("pair %d: %r" % (i, e))

    lt = [threading.Thread(target=loader, args=(i,)) for i in range(4)]
    pt = [threading.Thread(target=pair, args=(i,)) for i in range(A.pairs)]
    for t in lt + pt:
        t.start()
    for t in pt:
        t.join()
    stop.set()
    for t in lt:
        t.join()
    n_nat = d3.ostat(adm)["owner_native_commands"] - n_nat0
    left = [adm.cmd("LLEN", "pq%d" % i) for i in range(A.pairs)]
    check("blocked.pairs_no_lost_wakeup", not lost and sum(rounds) == A.pairs * A.rounds and left == [0] * A.pairs,
          "%s rounds %d left %s" % (lost[:2], sum(rounds), left))
    check("blocked.load_model", not errs and sum(loads) > 0, "%s ops %d native %d" % (errs[:2], sum(loads), n_nat))
    print("INFO blocked.pairs rounds %d, load ops %d, native during %d" % (sum(rounds), sum(loads), n_nat), flush=True)
    time.sleep(0.2)
    sync_replica(p, r, "blocked")
    pd = p.c.cmd("DEBUG", "DIGEST")
    check("blocked.replica_digest", r.c.cmd("DEBUG", "DIGEST") == pd, pd)
    check("blocked.no_crash", p.crashed() == 0 and r.crashed() == 0, (p.crashed(), r.crashed()))
    r.stop()
    p.stop()


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin")
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", required=True)
    ap.add_argument("--main-cpu", required=True)
    ap.add_argument("--replica-cpus", required=True)
    ap.add_argument("--per", type=int, default=3000)
    ap.add_argument("--pairs", type=int, default=8)
    ap.add_argument("--rounds", type=int, default=150)
    ap.add_argument("--suites", default="native,model,blocked")
    A = ap.parse_args()
    os.makedirs(A.dir, exist_ok=True)
    for s in A.suites.split(","):
        globals()["suite_" + s]()
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("LISTCHECK " + json.dumps({"pass": len(d3.RESULTS) - len(fails), "fail": len(fails),
                                     "failed": [r["check"] for r in fails]}), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
