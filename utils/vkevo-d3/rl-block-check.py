#!/usr/bin/env python3
"""RL-g4c-blocking-native model check: blocking pops parked on the owner of their keys vs the parent's main path.

Starts the candidate (--bin) and the reference (--ref-bin, the parent RL-g3g-long-tail-rand, whose owners hand every
blocking pop to main, where it blocks as in the stock server), and runs the same deterministic scenario on both:
  steps      each step: a key group in one slot (hash tag), 1-4 consumers sent one blocking pop each, in order
             (BLPOP BRPOP BLMOVE BRPOPLPUSH BZPOPMIN BZPOPMAX BLMPOP BZMPOP, one to three keys of the group, counts),
             mostly on the owner port of the group (native: they park there) and sometimes on the plain port (main:
             it blocks there, and every parked client becomes a stock blocked client); after each, the checker waits
             until INFO blocked_clients counts it (or its reply came: the key had data). Then one action: LPUSH /
             RPUSH / ZADD of 1-4 elements (owner port, or the plain port: a write of main), a native MULTI/EXEC of
             pushes, LMOVE from a helper list, SET (wrong type) then DEL then a push, RENAME of a helper into place
             (main), CLIENT UNBLOCK [TIMEOUT|ERROR] or CLIENT KILL of a consumer (main), a consumer closing its socket,
             or a timeout step (every consumer with a 50 ms timeout, no action). Then every consumer's reply is read
             (or 'pending'); the pending ones are drained in order: pushes on the owner port (each serves the first
             parked) or CLIENT UNBLOCK. Every reply, in order, must equal the reference's (FIFO among the blocked,
             the served-by order, what each popped, BLMOVE's move, errors).
  stats      INFO commandstats calls / failed of every blocking command equal (counted once, on serve or timeout);
             blocked_clients 0 at the end; the candidate parked natively (owner_park_wakes, owner_block_commands > 0).
  keyspace   type, encoding, TTL presence, value and DEBUG DIGEST equal on both.
  stress     T producers push unique values to 16 keys of 4 groups (owner ports) while T consumers pop them with
             50 ms timeouts (BLPOP BRPOP BZPOPMIN BLMPOP COUNT, BLMOVE between keys of a group); afterwards every value
             pushed was delivered exactly once or is still stored (conservation), on both servers.
Suites (--suites): plain, aofrepl (appendonly everysec + a replica each: the replica's keyspace equals its
primary's; AOF reloaded into a fresh server and compared), aofresp (appendonly always, resp records, reloaded).
Prints PASS / FAIL lines and 'RLBLOCK {json}'; exit 1 on any FAIL.
"""
import argparse
import importlib.util
import json
import os
import random
import shutil
import socket
import sys
import threading
import time

_spec = importlib.util.spec_from_file_location("rltail", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                       "rl-tail-check.py"))
T = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(T)
Conn, Err, Server, check, info, dump, diff_dumps, values, wait_repl = (T.Conn, T.Err, T.Server, T.check, T.info, T.dump,
                                                                      T.diff_dumps, T.values, T.wait_repl)
key_slot, owner_of_slot = T.key_slot, T.owner_of_slot


def _conn_close(self):
    """Close the connection for real. rl-tail-check's Conn.close() closes the socket object only, and its makefile()
    reader keeps the descriptor (and the TCP connection) open until it is garbage collected: a consumer the scenario
    closed stayed connected, and blocked, until the next step dropped it."""
    self.f.close()
    self.s.close()


Conn.close = _conn_close

SUITES = {
    "plain": ["--zset-max-listpack-entries", "16", "--list-max-listpack-size", "8"],
    "aofrepl": ["--appendonly", "yes", "--appendfsync", "everysec"],
    "aofresp": ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp"],
}
BLOCKCMDS = ["blpop", "brpop", "blmove", "brpoplpush", "bzpopmin", "bzpopmax", "blmpop", "bzmpop"]


def group_keys(g, n):
    """Keys of group g (one slot), and the owner of that slot."""
    tag = b"{bg%d}" % g
    ks = [tag + b":l%d" % j for j in range(3)] + [tag + b":z%d" % j for j in range(2)] + [tag + b":h", tag + b":s"]
    return ks, owner_of_slot(key_slot(ks[0]), n)


def blocked(adm):
    return int(info(adm, "clients")["blocked_clients"])


def wait_blocked(adm, want, c, secs=10.0):
    """Until blocked_clients == want, or c has a reply waiting. Returns 'blocked' or the reply."""
    t0 = time.time()
    while time.time() - t0 < secs:
        if blocked(adm) >= want:
            return "blocked"
        c.s.settimeout(0.005)
        try:
            data = c.s.recv(1, socket.MSG_PEEK)
            if data:
                c.s.settimeout(30)
                return c.read()
        except (socket.timeout, BlockingIOError):
            pass
        finally:
            c.s.settimeout(30)
    return "nostate"


def try_read(c, secs=0.1):
    c.s.settimeout(secs)
    try:
        return c.read()
    except socket.timeout:
        return "pending"
    except (EOFError, OSError):
        return "closed"
    finally:
        c.s.settimeout(30)


def norm(rep):
    if isinstance(rep, list):
        return [norm(x) for x in rep]
    return rep


def consumer_cmd(r, ks, timeout):
    lists, zs = ks[:3], ks[3:5]
    x = r.random()
    t = timeout
    if x < 0.22:
        return ["BLPOP"] + r.sample(lists, r.choice([1, 1, 2, 3])) + [t]
    if x < 0.38:
        return ["BRPOP"] + r.sample(lists, r.choice([1, 2])) + [t]
    if x < 0.52:
        src, dst = r.sample(lists, 2) if r.random() < 0.8 else (lists[0], lists[0])
        return ["BLMOVE", src, dst, r.choice(["LEFT", "RIGHT"]), r.choice(["LEFT", "RIGHT"]), t]
    if x < 0.60:
        src, dst = r.sample(lists, 2)
        return ["BRPOPLPUSH", src, dst, t]
    if x < 0.72:
        return ["BZPOPMIN"] + r.sample(zs, r.choice([1, 2])) + [t]
    if x < 0.80:
        return ["BZPOPMAX", r.choice(zs), t]
    if x < 0.90:
        k = r.sample(lists, r.choice([1, 2]))
        return ["BLMPOP", t, str(len(k))] + k + [r.choice(["LEFT", "RIGHT"])] + (["COUNT", r.choice(["1", "2", "5"])]
                                                                                 if r.random() < 0.6 else [])
    k = r.sample(zs, r.choice([1, 2]))
    return ["BZMPOP", t, str(len(k))] + k + [r.choice(["MIN", "MAX"])] + (["COUNT", "2"] if r.random() < 0.5 else [])


def push_for(r, key, n, serial):
    vals = [b"v%d" % (serial + j) for j in range(n)]
    if b":z" in key:
        return ["ZADD", key] + sum(([b"%d" % (r.randrange(100)), v] for v in vals), [])
    return [r.choice(["LPUSH", "RPUSH"]), key] + vals


def scenario(srv, a, seed):
    """The deterministic steps; returns the list of recorded events."""
    r = random.Random(seed)
    adm = Conn(srv.port)
    ev = []
    serial = [0]

    def nxt(n):
        serial[0] += n
        return serial[0] - n

    for step in range(a.steps):
        g = r.randrange(a.groups)
        ks, own = group_keys(g, srv.n)
        oport = srv.oport + own
        timeout_step = r.random() < 0.12
        nc = r.choice([1, 1, 2, 3, 4])
        cons = []
        base = blocked(adm)
        for i in range(nc):
            port = srv.port if r.random() < 0.15 else oport
            c = Conn(port)
            cid = c.cmd("CLIENT", "ID")
            cmd = consumer_cmd(r, ks, "0.05" if timeout_step else "0")
            c.send([cmd])
            st = wait_blocked(adm, base + 1, c)
            if st == "blocked":
                base += 1
                cons.append([c, cid, cmd, None])
            else:
                ev.append(("immediate", step, i, cmd[0], norm(st)))
                cons.append([c, cid, cmd, "done"])
        act = "timeout" if timeout_step else r.choice(["push", "push", "push", "pushmain", "multi", "lmove", "wrongtype",
                                                       "rename", "unblock", "kill", "close"])
        ev.append(("act", step, act))
        if act == "timeout":
            time.sleep(0.2)
        elif act in ("push", "pushmain"):
            c = Conn(oport if act == "push" else srv.port)
            for _ in range(r.choice([1, 1, 2])):
                k = r.choice(ks[:5])
                n = r.choice([1, 2, 4])
                ev.append(("push", step, norm(c.cmd(*push_for(r, k, n, nxt(n))))))
            c.close()
        elif act == "multi":
            c = Conn(oport)
            cmds = [["MULTI"]] + [push_for(r, r.choice(ks[:3]), 1, nxt(1)) for _ in range(r.choice([1, 2, 3]))] + [["EXEC"]]
            c.send(cmds)
            ev.append(("multi", step, [norm(c.read()) for _ in cmds]))
            c.close()
        elif act == "lmove":
            c = Conn(oport)
            h = ks[0][:ks[0].index(b"}") + 1] + b":src"
            c.cmd("RPUSH", h, b"m%d" % nxt(1), b"m%d" % nxt(1))
            ev.append(("lmove", step, norm(c.cmd("LMOVE", h, r.choice(ks[:3]), "LEFT", "RIGHT"))))
            c.close()
        elif act == "wrongtype":
            c = Conn(oport)
            k = r.choice(ks[:3])
            ev.append(("wrongtype", step, norm(c.cmd("SET", k, "str")), norm(c.cmd("DEL", k)),
                       norm(c.cmd(*push_for(r, k, 1, nxt(1))))))
            c.close()
        elif act == "rename":
            c = Conn(srv.port)
            h = ks[0][:ks[0].index(b"}") + 1] + b":tmp"
            k = r.choice(ks[:3])
            c.cmd("RPUSH", h, b"r%d" % nxt(1))
            ev.append(("rename", step, norm(c.cmd("RENAME", h, k))))
            c.close()
        elif act in ("unblock", "kill", "close"):
            live = [x for x in cons if x[3] is None]
            if live:
                x = r.choice(live)
                if act == "close":
                    x[0].close()
                    x[3] = "closed"
                    time.sleep(0.05)
                else:
                    c = Conn(srv.port)
                    if act == "unblock":
                        ev.append(("unblock", step, norm(c.cmd("CLIENT", "UNBLOCK", str(x[1]),
                                                               r.choice(["TIMEOUT", "ERROR"])))))
                    else:
                        ev.append(("kill", step, norm(c.cmd("CLIENT", "KILL", "ID", str(x[1])))))
                        x[3] = "killed"
                    c.close()
        time.sleep(0.03)
        for i, x in enumerate(cons):
            if x[3] is None:
                rep = try_read(x[0])
                if rep != "pending":
                    x[3] = "done"
                    ev.append(("reply", step, i, x[2][0], norm(rep)))
        # drain the pending ones, in order
        for i, x in enumerate(cons):
            if x[3] is not None:
                continue
            if r.random() < 0.8:
                c = Conn(oport)
                cmd = x[2]
                k = cmd[3] if cmd[0] in ("BLMPOP", "BZMPOP") else cmd[1]
                c.cmd(*push_for(r, k, 1, nxt(1)))
                c.close()
            else:
                c = Conn(srv.port)
                c.cmd("CLIENT", "UNBLOCK", str(x[1]))
                c.close()
            rep = try_read(x[0], 1.0)
            x[3] = "done"
            ev.append(("drain", step, i, x[2][0], norm(rep)))
            # a push may have served a later consumer too (they wait on the same key): read those now
            for j, y in enumerate(cons):
                if y[3] is None:
                    rep2 = try_read(y[0], 0.05)
                    if rep2 != "pending":
                        y[3] = "done"
                        ev.append(("reply", step, j, y[2][0], norm(rep2)))
        for x in cons:
            if x[3] not in ("closed", "killed"):
                x[0].close()
        # a closed or killed consumer's slot: wait until the server forgot it
        for _ in range(500):  # up to 5 s on a loaded host
            if blocked(adm) == 0:
                break
            time.sleep(0.01)
    adm.close()
    return ev


def stress(srv, a, seed):
    """Producers and consumers at once on 4 groups; conservation of every pushed value."""
    groups = [group_keys(g, srv.n) for g in range(4)]
    pushed, delivered, lock, stop = [], [], threading.Lock(), [False]

    def producer(i):
        r = random.Random(seed * 100 + i)
        ks, own = groups[i % 4]
        c = Conn(srv.oport + own)
        n = 0
        while not stop[0]:
            k = r.choice(ks[:5])
            vs = [b"p%d-%d" % (i, n + j) for j in range(r.choice([1, 2]))]
            n += len(vs)
            if b":z" in k:
                c.cmd("ZADD", k, *sum(([b"1", v] for v in vs), []))
            else:
                c.cmd(r.choice(["LPUSH", "RPUSH"]), k, *vs)
            with lock:
                pushed.extend(vs)
            time.sleep(r.random() * 0.002)
        c.close()

    def consumer(i):
        r = random.Random(seed * 1000 + i)
        ks, own = groups[i % 4]
        c = Conn(srv.oport + own)
        got = []
        while not stop[0]:
            x = r.random()
            if x < 0.3:
                rep = c.cmd("BLPOP", *r.sample(ks[:3], 2), "0.05")
                if rep:
                    got.append(rep[1])
            elif x < 0.45:
                rep = c.cmd("BRPOP", r.choice(ks[:3]), "0.05")
                if rep:
                    got.append(rep[1])
            elif x < 0.65:
                rep = c.cmd("BZPOPMIN", *ks[3:5], "0.05")
                if rep:
                    got.append(rep[1])
            elif x < 0.8:
                rep = c.cmd("BLMPOP", "0.05", "1", r.choice(ks[:3]), "LEFT", "COUNT", "3")
                if rep:
                    got.extend(rep[1])
            else:
                src, dst = r.sample(ks[:3], 2)
                c.cmd("BLMOVE", src, dst, "RIGHT", "LEFT", "0.05")
        c.close()
        with lock:
            delivered.extend(got)

    ths = [threading.Thread(target=producer, args=(i,)) for i in range(a.threads)] + \
          [threading.Thread(target=consumer, args=(i,)) for i in range(a.threads)]
    for t in ths:
        t.start()
    time.sleep(a.stress_secs)
    stop[0] = True
    for t in ths:
        t.join()
    c = Conn(srv.port)
    left = []
    for ks, _ in groups:
        for k in ks[:3]:
            left += c.cmd("LRANGE", k, "0", "-1")
        for k in ks[3:5]:
            left += c.cmd("ZRANGE", k, "0", "-1")
    c.close()
    # only the stress values (p<i>-<n>): the groups also hold what the scenario left in them
    delivered = [v for v in delivered if v.startswith(b"p")]
    left = [v for v in left if v.startswith(b"p")]
    ok = sorted(pushed) == sorted(delivered + left)
    dup = len(delivered) - len(set(delivered))
    return ok, dup, len(pushed), len(delivered), len(left)


def cmdstats(srv):
    c = Conn(srv.port)
    out = {}
    for k, v in info(c, "commandstats").items():
        name = k[len("cmdstat_"):]
        if name in BLOCKCMDS:
            f = dict(p.split("=", 1) for p in v.split(","))
            out[name] = (f.get("calls"), f.get("failed_calls"), f.get("rejected_calls"))
    i = info(c, "owners")
    cl = info(c, "clients")
    c.close()
    return out, i, cl


def suite(a, name, seed):
    extra = SUITES[name]
    cand = Server(a, name + "-cand", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra, a.n)
    ref = Server(a, name + "-ref", a.ref_bin, a.ref_port, a.ref_owner_port, a.ref_cpus, a.ref_main_cpu, extra, a.n)
    reps = []
    cand.start()
    ref.start()
    try:
        if name == "aofrepl":
            reps = [T.start_replica(a, name + "-cand-replica", a.replica_port, cand, extra),
                    T.start_replica(a, name + "-ref-replica", a.replica_port + 1, ref, extra)]
            check(name + ".replica_synced", wait_repl(cand, reps[0])[0] and wait_repl(ref, reps[1])[0])
        ec = scenario(cand, a, seed)
        er = scenario(ref, a, seed)
        mism = [i for i in range(min(len(ec), len(er))) if ec[i] != er[i]]
        det = ""
        if mism or len(ec) != len(er):
            i = mism[0] if mism else min(len(ec), len(er))
            det = "first difference #%d: cand %r ref %r" % (i, ec[i] if i < len(ec) else None, er[i] if i < len(er) else None)
        if mism or len(ec) != len(er):  # both event lists, for the record
            for tag, e in (("cand", ec), ("ref", er)):
                with open(os.path.join(a.dir, "%s-%s-events.txt" % (name, tag)), "w") as f:
                    for x in e:
                        f.write(repr(x) + "\n")
        nb = sum(1 for e in ec if e[0] in ("reply", "drain", "immediate"))
        check(name + ".scenario.replies_equal", not mism and len(ec) == len(er),
              "%d steps, %d events, %d blocking replies; %s" % (a.steps, len(ec), nb, det))
        sc, oc, cc = cmdstats(cand)
        sr, _, cr = cmdstats(ref)
        check(name + ".stats.blocking_calls_equal", sc == sr, "cand %s ref %s" % (sc, sr))
        check(name + ".stats.no_blocked_left", cc.get("blocked_clients") == "0" and cr.get("blocked_clients") == "0",
              "cand %s ref %s" % (cc.get("blocked_clients"), cr.get("blocked_clients")))
        check(name + ".native.parked_on_owners", int(oc.get("owner_park_wakes", 0)) > 0 and
              int(oc.get("owner_block_commands", 0)) > 0,
              "owner_block_commands %s park_wakes %s park_timeouts %s parked_to_main %s" % (
                  oc.get("owner_block_commands"), oc.get("owner_park_wakes"), oc.get("owner_park_timeouts"),
                  oc.get("owner_parked_to_main")))
        okc = stress(cand, a, seed)
        okr = stress(ref, a, seed)
        check(name + ".stress.conservation", okc[0] and okc[1] == 0 and okr[0] and okr[1] == 0,
              "cand ok %s dup %d pushed %d delivered %d left %d; ref ok %s dup %d pushed %d" % (okc + okr[:3]))
        dc, gc = dump(cand)
        dr, gr = dump(ref)
        # the stress phase's values are timing-dependent: compare the scenario's groups (bg4..) and every key's type
        sel = lambda d: {k: v for k, v in d.items() if not any(k.startswith(b"{bg%d}" % g) for g in range(4))}
        check(name + ".keyspace.equal", sel(dc) == sel(dr),
              "%d / %d keys outside the stress groups; diff %s" % (len(sel(dc)), len(sel(dr)), diff_dumps(sel(dc), sel(dr))))
        if reps:
            for tag, s, rp in (("cand", cand, reps[0]), ("ref", ref, reps[1])):
                ok, off = wait_repl(s, rp)
                d, g = dump(s)
                dp, gp = dump(rp)
                check(name + ".replica.keyspace_equal." + tag, ok and values(dp) == values(d) and gp == g,
                      "offset %s, %d keys, digest %s; diff %s" % (off, len(dp), gp == g, diff_dumps(values(d), values(dp))))
        if "--appendonly" in extra:
            time.sleep(1.5)
            for tag, s in (("cand", cand), ("ref", ref)):
                d, g = dump(s)
                s.stop()
                rdir = os.path.join(a.dir, s.name + "-reload")
                shutil.rmtree(rdir, ignore_errors=True)
                shutil.copytree(s.dir, rdir)
                os.remove(os.path.join(rdir, "log"))
                rl = Server(a, s.name + "-reload", s.bin, s.port, s.oport, s.cpus, s.main_cpu, extra, a.n)
                rl.start(fresh=False)
                dl, gl = dump(rl)
                check(name + ".aof.reload_keyspace_equal." + tag, values(dl) == values(d) and gl == g and rl.crashed() == 0,
                      "%d keys, digest %s; diff %s" % (len(dl), gl == g, diff_dumps(values(d), values(dl))))
                rl.stop()
                shutil.rmtree(os.path.join(rdir, "appendonlydir"), ignore_errors=True)
        check(name + ".no_crash", cand.crashed() == 0 and ref.crashed() == 0 and all(x.crashed() == 0 for x in reps),
              "cand %d ref %d" % (cand.crashed(), ref.crashed()))
    finally:
        for s in [cand, ref] + reps:
            s.stop()
        for d in os.listdir(a.dir):
            shutil.rmtree(os.path.join(a.dir, d, "appendonlydir"), ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=22600)
    ap.add_argument("--owner-port", type=int, default=22610)
    ap.add_argument("--ref-port", type=int, default=22601)
    ap.add_argument("--ref-owner-port", type=int, default=22620)
    ap.add_argument("--replica-port", type=int, default=22602)
    ap.add_argument("--replica-cpu", default="59")
    ap.add_argument("--cpus", default="52-55")
    ap.add_argument("--main-cpu", default="56")
    ap.add_argument("--bg-cpu", default="57")
    ap.add_argument("--ref-cpus", default="52-55")
    ap.add_argument("--ref-main-cpu", default="58")
    ap.add_argument("--steps", type=int, default=80)
    ap.add_argument("--groups", type=int, default=12)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--stress-secs", type=float, default=4)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--suites", default="plain,aofrepl,aofresp")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    t0 = time.time()
    for i, s in enumerate([x for x in a.suites.split(",") if x]):
        try:
            suite(a, s, a.seed + i)
        except Exception as e:  # a crash or a hang is a FAIL, not a skip
            import traceback
            traceback.print_exc()
            check(s + ".ran", False, repr(e))
    fails = [r for r in T.RESULTS if not r["ok"]]
    print("RLBLOCK " + json.dumps(dict(passed=len(T.RESULTS) - len(fails), failed=len(fails),
                                       secs=round(time.time() - t0, 1))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
