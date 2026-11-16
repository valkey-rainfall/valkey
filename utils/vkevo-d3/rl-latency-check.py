#!/usr/bin/env python3
"""RL-g3a-cheap-stop gate: the owners' latency histograms fold when read. Starts A = --bin and R = --ref-bin (the
parent, which folds at every world stop), both with 4 owner threads, and checks on each:
  load       12 connections on the owner ports run native commands (GET SET INCR DEL MGET MSET EXPIRE TTL PTTL PERSIST
             EXISTS TYPE PING, GET and SET bursts) and some handed-off ones (HSET, LPUSH, EVAL) for --secs, while a
             reader on the main port calls LATENCY HISTOGRAM and INFO latencystats in a loop: every reply parses and each
             command's histogram count never goes down
  exact      right after the load, with no pause: for every command with calls, LATENCY HISTOGRAM's calls equal
             INFO commandstats' calls, and INFO latencystats has the command (A and R the same)
  pipelined  on one connection: N GETs on an owner port, then at once LATENCY HISTOGRAM get on the main port: +N
  resetstat  CONFIG RESETSTAT right after owner traffic, then M GETs on an owner port: the histogram counts M (nothing
             the owners recorded before the reset comes back) and commandstats M
  tracking   CONFIG SET latency-tracking no: INFO latencystats is empty, owner calls still count in commandstats;
             back on: counting resumes
Prints PASS/FAIL lines and 'RLLATENCY {json}'; exit 1 on any FAIL.
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
d3 = __import__("d3-check")
Conn, Err, check = d3.Conn, d3.Err, d3.check
A = None


class Srv:
    def __init__(self, name, binary, port, oport, cpus, main, bg):
        self.name, self.port, self.oport = name, port, oport
        self.dir = os.path.join(A.dir, name)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        cmd = ["taskset", "-c", "%s,%s" % (cpus, bg), binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "",
               "--appendonly", "no", "--enable-debug-command", "yes", "--dir", self.dir, "--logfile",
               os.path.join(self.dir, "log"), "--owner-threads", "4", "--owner-port", str(oport), "--owner-cpulist", cpus,
               "--server-cpulist", main, "--bio-cpulist", bg]
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(400):
            try:
                c = Conn("127.0.0.1", port, timeout=5)
                if c.cmd("PING") in ("PONG", b"PONG"):
                    c.close()
                    break
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        self.c = Conn("127.0.0.1", port, timeout=60)
        self.ranges = []
        raw = self.c.cmd("INFO", "owners").decode()
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))

    def stop(self):
        try:
            self.c.cmd("SHUTDOWN", "NOSAVE")
        except Exception:
            pass
        self.p.wait(timeout=60)

    def crashed(self):
        t = open(os.path.join(self.dir, "log"), errors="replace").read()
        return sum(t.count(w) for w in ("ASSERT", "crashed by signal", "Guru", "BUG REPORT"))


def tag_in(rng, lo, hi, prefix):
    while True:
        t = b"%s%d" % (prefix, rng.randrange(1 << 30))
        if lo <= d3.key_slot(b"{" + t + b"}") <= hi:
            return t


def hist(c):
    """{command: calls} from LATENCY HISTOGRAM (RESP2: flat map)."""
    r = c.cmd("LATENCY", "HISTOGRAM")
    out = {}
    for i in range(0, len(r), 2):
        name = r[i].decode() if isinstance(r[i], bytes) else r[i]
        body = r[i + 1]
        for j in range(0, len(body), 2):
            k = body[j].decode() if isinstance(body[j], bytes) else body[j]
            if k == "calls":
                out[name] = body[j + 1]
    return out


def cstats(c):
    out = {}
    for k, v in d3.info(c, "commandstats").items():
        if k.startswith("cmdstat_"):
            out[k[8:]] = int(dict(x.split("=") for x in v.split(","))["calls"])
    return out


def lstats(c):
    return {k[len("latency_percentiles_usec_"):] for k in d3.info(c, "latencystats") if k.startswith("latency_percentiles_usec_")}


ADMIN = {"latency", "info", "config", "client", "hello", "command", "debug", "shutdown"}


def admin(k):
    return k.split("|")[0] in ADMIN


def loader(s, wid, stop_at, out, rng):
    lo, hi = s.ranges[wid % len(s.ranges)]
    t = tag_in(rng, lo, hi, b"lt%d-" % wid)
    ks = [b"{%s}:%d" % (t, j) for j in range(8)]
    c = Conn("127.0.0.1", s.oport + wid % len(s.ranges))
    n = 0
    while time.time() < stop_at:
        k = rng.choice(ks)
        batch = rng.choice([
            [("SET", k, "v")] * 8, [("GET", k)] * 8,
            [("INCR", k + b"n"), ("DEL", k), ("MGET", ks[0], ks[1]), ("MSET", ks[2], "a", ks[3], "b")],
            [("EXPIRE", k, 100), ("TTL", k), ("PTTL", k), ("PERSIST", k), ("EXISTS", k, ks[1]), ("TYPE", k), ("PING",)],
            [("HSET", k + b"h", "f", "v"), ("LPUSH", k + b"l", "x"), ("EVAL", "return 1", 0)]])
        c.send(batch)
        for _ in batch:
            c.read()
        n += len(batch)
    out[wid] = n
    c.close()


def reader(s, stop_at, res):
    c = Conn("127.0.0.1", s.port)
    prev, bad, n = {}, [], 0
    while time.time() < stop_at:
        h = hist(c)
        lstats(c)
        for k, v in h.items():
            if v < prev.get(k, 0):
                bad.append((k, prev[k], v))
            prev[k] = v
        n += 1
    res.append((n, bad))
    c.close()


def run(s, tag):
    rng = random.Random(A.seed)
    stop_at = time.time() + A.secs
    out, res = {}, []
    ths = [threading.Thread(target=loader, args=(s, w, stop_at, out, random.Random(A.seed * 100 + w))) for w in range(12)]
    ths.append(threading.Thread(target=reader, args=(s, stop_at, res)))
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    n, bad = res[0]
    check("%s.load.reader_monotonic" % tag, not bad and n > 20, bad[:3] if bad else "%d LATENCY HISTOGRAM + INFO latencystats reads during %d commands" % (n, sum(out.values())))
    h, cs, ls = hist(s.c), cstats(s.c), lstats(s.c)
    diff = {k: (h.get(k), v) for k, v in cs.items() if not admin(k) and h.get(k) != v}
    miss = [k for k, v in cs.items() if not admin(k) and v and k not in ls]
    check("%s.exact.histogram_calls_equal_commandstats" % tag, not diff and len(cs) > 10, diff if diff else
          "%d commands: %s" % (len([k for k in cs if not admin(k)]), ", ".join("%s %d" % (k, cs[k]) for k in sorted(cs) if k in ("get", "set", "expire", "ttl", "type", "hset", "eval"))))
    check("%s.exact.latencystats_has_every_command" % tag, not miss, miss)
    # pipelined: N GETs on an owner port, then the histogram on main at once
    lo, hi = s.ranges[1]
    k = b"{%s}:p" % tag_in(rng, lo, hi, b"pp")
    c = Conn("127.0.0.1", s.oport + 1)
    g0 = hist(s.c).get("get", 0)
    c.send([("GET", k)] * 500)
    for _ in range(500):
        c.read()
    g1 = hist(s.c).get("get", 0)
    check("%s.pipelined.histogram_sees_owner_gets_at_once" % tag, g1 - g0 == 500, "+%d" % (g1 - g0))
    # resetstat right after owner traffic
    c.send([("SET", k, "x")] * 300)
    for _ in range(300):
        c.read()
    s.c.cmd("CONFIG", "RESETSTAT")
    c.send([("GET", k)] * 200)
    for _ in range(200):
        c.read()
    h, cs = hist(s.c), cstats(s.c)
    check("%s.resetstat.nothing_comes_back" % tag, h.get("get") == 200 and cs.get("get") == 200 and "set" not in h,
          "get hist %s cmdstat %s, set hist %s" % (h.get("get"), cs.get("get"), h.get("set")))
    # tracking off / on
    s.c.cmd("CONFIG", "SET", "latency-tracking", "no")
    c.send([("GET", k)] * 100)
    for _ in range(100):
        c.read()
    ls, cs, h = lstats(s.c), cstats(s.c), hist(s.c)
    s.c.cmd("CONFIG", "SET", "latency-tracking", "yes")
    c.send([("GET", k)] * 50)
    for _ in range(50):
        c.read()
    h2 = hist(s.c)
    check("%s.tracking.off_then_on" % tag, not ls and cs.get("get") == 300 and h.get("get") == 200 and h2.get("get") == 250,
          "latencystats %d lines, get cmdstat %s, hist off %s, hist on %s" % (len(ls), cs.get("get"), h.get("get"), h2.get("get")))
    c.close()


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
    ap.add_argument("--a-cpus", required=True)
    ap.add_argument("--a-main", required=True)
    ap.add_argument("--a-bg", required=True)
    ap.add_argument("--r-cpus", required=True)
    ap.add_argument("--r-main", required=True)
    ap.add_argument("--r-bg", required=True)
    ap.add_argument("--secs", type=float, default=8)
    ap.add_argument("--seed", type=int, default=4)
    A = ap.parse_args()
    os.makedirs(A.dir, exist_ok=True)
    for tag, binary, port, op, cpus, m, bg in (("a", A.bin, A.port, A.owner_port, A.a_cpus, A.a_main, A.a_bg),
                                               ("ref", A.ref_bin, A.ref_port, A.ref_owner_port, A.r_cpus, A.r_main, A.r_bg)):
        s = Srv(tag, binary, port, op, cpus, m, bg)
        run(s, tag)
        s.stop()
        check("%s.no_crash" % tag, s.crashed() == 0, "crash lines %d" % s.crashed())
    fails = [x for x in d3.RESULTS if not x["ok"]]
    print("RLLATENCY " + json.dumps(dict(pass_=len(d3.RESULTS) - len(fails), fail=len(fails))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
