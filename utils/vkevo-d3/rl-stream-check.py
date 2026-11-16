#!/usr/bin/env python3
"""RL-g6c-native-streams model check: the stream commands on the owners against the parent's main path.

  rl-stream-check.py --bin CHILD --ref-bin PARENT --dir D --port P --owner-port OP [--n 4] --cpus 32-35 --main-cpu 36
      --bio-cpu 37 --load-cpus 38-39 --seed S [--ops 3000] [--tops 1500] [--memtier PATH]

Servers (127.0.0.1; appendonly always with RESP incr records; stream-node-max-entries 8 so nodes split and trims cut
whole nodes): the candidate (owners) at P / OP+, its replica at P+1 / OP+30+ (the candidate binary, owners on; a replica
runs everything on main), the parent at P+20 / OP+20+ and its replica at P+21 / OP+50+, the AOF reloads of the
candidate at P+2 / OP+40+ and of the parent at P+3 / OP+60+.
  A  lockstep: one seeded random sequence of stream commands over 20 stream keys and 4 one-slot pairs, one command at
     a time: on the candidate through the key's owner port (most), main, or another owner's port (handed to main); on
     the parent through main. Every reply equal (times masked: XPENDING's idle, XINFO's seen / active / idle /
     inactive times, any integer >= 1e12).
  B  under load: 6 threads at once, each with its own keys and its own seeded sequence, plus a memtier GET / SET load
     on the candidate's owner ports; each thread's replies equal the parent's same thread.
  C  generated IDs: XADD * and <ms>-* on the candidate's and the parent's own keys: valid, increasing replies.
  Then: the native share (owner_stream_commands vs stream commands sent to their owner without BLOCK / MULTI); XINFO
  STREAM FULL (+ TYPE, XLEN) of every key equal on the candidate and the parent, the candidate's replica equal to the
  parent's replica and the candidate's AOF reload to the parent's (stock leaves some consumer state off the stream);
  propagation: the RESP AOF records of the model keys, per key in order, equal to the parent's byte for byte except
  the clock (XCLAIM's TIME value, any integer >= 1e12, and phase C's generated IDs, which each side's replies give);
  no XADD record keeps '*' or '~'.
Prints PASS/FAIL/INFO lines and 'RLSTREAMCHECK {json}'; exit 1 on any FAIL.
"""
import argparse
import importlib.util
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("rlbp", os.path.join(HERE, "rl-busypoll-check.py"))
bp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bp)
check, note, info, ival = bp.check, bp.note, bp.info, bp.ival


class Conn(bp.Conn):
    def close(self):  # the makefile reader holds the descriptor too (RL-g4c's lesson)
        for x in (self.f, self.s):
            try:
                x.close()
            except OSError:
                pass


class Server(bp.Server):
    def c(self, port=None):
        return Conn(port or self.port)

    def start(self):
        # the pid file may land a moment after the port answers
        pf = os.path.join(self.dir, "pid")
        try:
            return bp.Server.start(self)
        except FileNotFoundError:
            for _ in range(50):
                if os.path.exists(pf) and open(pf).read().strip():
                    self.pid = int(open(pf).read().strip())
                    return self
                time.sleep(0.1)
            raise


def crc16(b):
    crc = 0
    for ch in b:
        crc ^= ch << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(k):
    k = k if isinstance(k, bytes) else k.encode()
    s = k.find(b"{")
    if s != -1:
        e = k.find(b"}", s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) & 16383


def owner_ranges(s):
    out = []
    for k, v in s.owners().items():
        if k.startswith("owner") and k[5:].isdigit():
            f = dict(x.split("=", 1) for x in v.split(","))
            lo, hi = f["slots"].split("-")
            out.append((int(lo), int(hi), int(f["port"])))
    return sorted(out)


# ------------------------------------------------------------------------------------------------ the model

FIELDS = [b"f", b"g", b"name", b"n", b"ts", b"payload"]


class Gen:
    """A seeded random stream workload over keys; ops are lists of argv (bytes). A MULTI block is one item."""

    def __init__(self, seed, keys, pairs, block=True):
        self.r = random.Random(seed)
        self.block = block
        self.keys, self.pairs = keys, pairs
        self.ms = {k: 1 for k in keys + [x for p in pairs for x in p]}
        self.ids = {k: [] for k in self.ms}

    def val(self):
        r = self.r
        c = r.random()
        if c < 0.3:
            return b"%d" % r.randrange(-1000, 100000)
        if c < 0.4:
            return b"%d" % r.randrange(1 << 40)
        return bytes(r.choice(b"abcdefxyz") for _ in range(r.choice((1, 4, 16, 40, 120))))

    def fv(self):
        n = self.r.choice((1, 1, 2, 3))
        if self.r.random() < 0.5:
            f = FIELDS[:n]
        else:
            f = self.r.sample(FIELDS, n)
        out = []
        for x in f:
            out += [x, self.val()]
        return out

    def some_id(self, k):
        r = self.r
        c = r.random()
        if self.ids[k] and c < 0.75:
            return r.choice(self.ids[k])
        if c < 0.85:
            return b"0-0"
        return b"%d-%d" % (r.randrange(1, self.ms[k] + 3), r.randrange(0, 3))

    def trim(self, k):
        r = self.r
        if r.random() < 0.5:
            out = [b"MAXLEN"]
            thr = b"%d" % r.choice((0, 1, 3, 8, 12, 20, 40))
        else:
            out = [b"MINID"]
            thr = self.some_id(k)
        op = r.choice((None, b"=", b"~", b"~"))
        if op:
            out.append(op)
        out.append(thr)
        if op == b"~" and r.random() < 0.4:
            out += [b"LIMIT", b"%d" % r.choice((0, 1, 8, 16, 100))]
        return out

    def xadd(self, k, auto=False):
        r = self.r
        a = [b"XADD", k]
        if r.random() < 0.05:
            a.append(b"NOMKSTREAM")
        if r.random() < 0.25:
            a += self.trim(k)
        if auto:
            a.append(b"*")
        else:
            c = r.random()
            if c < 0.06 and self.ids[k]:
                a.append(r.choice(self.ids[k]))  # equal or smaller: an error
            elif c < 0.3:
                self.ms[k] += r.choice((0, 1, 1, 2))
                a.append(b"%d-*" % self.ms[k])
            else:
                self.ms[k] += r.choice((0, 1, 1, 3))
                sid = b"%d-%d" % (self.ms[k], r.randrange(1, 4))
                self.ids[k].append(sid)
                a.append(sid)
        return a + self.fv()

    def op(self):
        r = self.r
        k = r.choice(self.keys)
        g = r.choice((b"g1", b"g1", b"g2"))
        cn = r.choice((b"c1", b"c1", b"c2", b"c3"))
        c = r.random()
        if c < 0.30:
            return [self.xadd(k)]
        if c < 0.37:
            return [[b"XRANGE", k] + r.choice(([b"-", b"+"], [self.some_id(k), b"+"], [b"(" + self.some_id(k), b"+"],
                                               [b"-", self.some_id(k)])) + r.choice(([], [b"COUNT", b"%d" % r.randrange(0, 6)]))]
        if c < 0.40:
            return [[b"XREVRANGE", k, b"+", b"-"] + r.choice(([], [b"COUNT", b"3"]))]
        if c < 0.42:
            return [[b"XLEN", k]]
        if c < 0.44:
            return [[b"XDEL", k] + [self.some_id(k) for _ in range(r.randrange(1, 4))]]
        if c < 0.46:
            ids = [self.some_id(k) for _ in range(r.randrange(1, 3))]
            return [[b"XDELEX", k] + r.choice(([], [b"KEEPREF"], [b"DELREF"], [b"ACKED"])) + [b"IDS", b"%d" % len(ids)] + ids]
        if c < 0.49:
            return [[b"XTRIM", k] + self.trim(k)]
        if c < 0.53:
            p = r.choice(self.pairs) if r.random() < 0.3 else [k]
            ids = [r.choice((b"0", b"$", b"+", self.some_id(x))) for x in p]
            return [[b"XREAD", b"COUNT", b"%d" % r.randrange(1, 6), b"STREAMS"] + list(p) + ids]
        if c < 0.63:
            p = r.choice(self.pairs) if r.random() < 0.3 else [k]
            ids = [r.choice((b">", b">", b">", b"0", self.some_id(x))) for x in p]
            return [[b"XREADGROUP", b"GROUP", g, cn, b"COUNT", b"%d" % r.randrange(1, 8)] +
                    ([b"NOACK"] if r.random() < 0.15 else []) + [b"STREAMS"] + list(p) + ids]
        if c < 0.69:
            return [[b"XACK", k, g] + [self.some_id(k) for _ in range(r.randrange(1, 4))]]
        if c < 0.70:
            ids = [self.some_id(k) for _ in range(r.randrange(1, 3))]
            return [[b"XACKDEL", k, g] + r.choice(([], [b"KEEPREF"], [b"DELREF"], [b"ACKED"])) + [b"IDS", b"%d" % len(ids)] + ids]
        if c < 0.74:
            if r.random() < 0.5:
                return [[b"XPENDING", k, g]]
            return [[b"XPENDING", k, g] + ([b"IDLE", b"0"] if r.random() < 0.3 else []) + [b"-", b"+", b"%d" % r.randrange(1, 10)] +
                    ([cn] if r.random() < 0.4 else [])]
        if c < 0.78:
            a = [b"XCLAIM", k, g, cn, b"0"] + [self.some_id(k) for _ in range(r.randrange(1, 4))]
            if r.random() < 0.3:
                a += r.choice(([b"IDLE", b"5000"], [b"TIME", b"1000"]))
            if r.random() < 0.3:
                a += [b"RETRYCOUNT", b"%d" % r.randrange(0, 5)]
            for opt in (b"FORCE", b"JUSTID"):
                if r.random() < 0.25:
                    a.append(opt)
            if r.random() < 0.15:
                a += [b"LASTID", self.some_id(k)]
            return [a]
        if c < 0.82:
            return [[b"XAUTOCLAIM", k, g, cn, b"0", r.choice((b"0-0", self.some_id(k)))] +
                    r.choice(([], [b"COUNT", b"%d" % r.randrange(1, 6)])) + ([b"JUSTID"] if r.random() < 0.3 else [])]
        if c < 0.86:
            s = r.random()
            if s < 0.4:
                return [[b"XGROUP", b"CREATE", k, g, r.choice((b"0", b"$", self.some_id(k)))] +
                        ([b"MKSTREAM"] if r.random() < 0.5 else []) + ([b"ENTRIESREAD", b"%d" % r.randrange(0, 5)] if r.random() < 0.2 else [])]
            if s < 0.6:
                return [[b"XGROUP", b"SETID", k, g, r.choice((b"0", b"$", self.some_id(k)))] +
                        ([b"ENTRIESREAD", b"%d" % r.randrange(0, 5)] if r.random() < 0.2 else [])]
            if s < 0.7:
                return [[b"XGROUP", b"DESTROY", k, g]]
            if s < 0.85:
                return [[b"XGROUP", b"CREATECONSUMER", k, g, cn]]
            return [[b"XGROUP", b"DELCONSUMER", k, g, cn]]
        if c < 0.91:
            s = r.random()
            if s < 0.5:
                return [[b"XINFO", b"STREAM", k] + r.choice(([], [b"FULL"], [b"FULL", b"COUNT", b"3"]))]
            if s < 0.75:
                return [[b"XINFO", b"GROUPS", k]]
            return [[b"XINFO", b"CONSUMERS", k, g]]
        if c < 0.92:
            self.ms[k] += 5
            return [[b"XSETID", k, b"%d-0" % self.ms[k]] + ([b"ENTRIESADDED", b"%d" % (1000 + self.ms[k])] if r.random() < 0.3 else [])]
        if c < 0.94:
            return [[b"MULTI"], self.xadd(k), [b"XREADGROUP", b"GROUP", g, cn, b"COUNT", b"2", b"STREAMS", k, b">"],
                    [b"XLEN", k], [b"EXEC"]]
        if c < 0.96 and not self.block:
            return [[b"XLEN", k]]
        if c < 0.95:
            # BLOCK stays on main: a short block with nothing new times out (null), with data it serves at once
            return [[b"XREADGROUP", b"GROUP", g, cn, b"BLOCK", b"5", b"COUNT", b"1", b"STREAMS", k, b">"]]
        if c < 0.96:
            return [[b"XREAD", b"BLOCK", b"5", b"STREAMS", k, b"$"]]
        if c < 0.97:
            return [[b"TYPE", k]]
        if c < 0.975:
            return [[b"DEL", k]]
        if c < 0.985:
            return [[b"XADD", k + b":str", b"1-1", b"f", b"v"]]  # WRONGTYPE once the string exists
        if c < 0.99:
            return [[b"SET", k + b":str", b"x"]]
        # errors xreadCommand / the parsers give
        return [r.choice(([b"XREAD", b"STREAMS", k], [b"XREADGROUP", b"GROUP", g, b"STREAMS", k, b">"], [b"XREAD", b"COUNT", b"x", b"STREAMS", k, b"0"],
                          [b"XADD", k, b"0-0", b"f", b"v"], [b"XRANGE", k, b"x", b"+"], [b"XGROUP", b"HELP"], [b"XINFO", b"STREAM"]))]


TIMEKEYS = {b"seen-time", b"active-time", b"idle", b"inactive"}


def mask(x):
    if isinstance(x, int):
        return "T" if x >= 10 ** 12 else x
    if isinstance(x, list):
        out = []
        for i, v in enumerate(x):
            if i % 2 == 1 and isinstance(x[i - 1], bytes) and x[i - 1] in TIMEKEYS and len(x) % 2 == 0:
                out.append("T")
            else:
                out.append(mask(v))
        return out
    return x


def norm(argv, rep):
    name = argv[0].upper()
    if name == b"XPENDING" and len(argv) > 3 and isinstance(rep, list):
        rep = [[e[0], e[1], "T", e[3]] if isinstance(e, list) and len(e) == 4 else e for e in rep]
    if isinstance(rep, bp.Err):
        return ("ERR", str(rep))
    return mask(rep)


def run_ops(conn_for, ops, out):
    """ops: list of blocks (lists of argv). conn_for(block) -> Conn. Appends (argv, normalized reply)."""
    for block in ops:
        c = conn_for(block)
        for argv in block:
            out.append((argv, norm(argv, c.cmd(*argv))))


def block_key(block):
    a = block[0] if block[0][0] != b"MULTI" else block[1]
    n = a[0].upper()
    if n in (b"XGROUP", b"XINFO"):
        return a[2] if len(a) > 2 else None
    if n in (b"XREAD", b"XREADGROUP"):
        for i, x in enumerate(a):
            if x.upper() == b"STREAMS" and i + 1 < len(a):
                return a[i + 1]
        return None
    return a[1] if len(a) > 1 else None


def streams_keys(a):
    """The keys owner.c's OWN_KEYS_STREAMS layout finds (xreadCommand's option walk), or None (main's)."""
    i = 1
    while i < len(a):
        more = len(a) - i - 1
        o = a[i].upper()
        if o == b"COUNT" and more:
            i += 2
        elif o == b"GROUP" and more >= 2:
            i += 3
        elif o == b"NOACK":
            i += 1
        elif o == b"STREAMS" and more:
            n = len(a) - i - 1
            return None if n % 2 else a[i + 1:i + 1 + n // 2]
        else:
            return None
    return None


MULTI_NATIVE = False  # RL-2s: the candidate queues stream commands natively in MULTI (RL-g6h: owner_txn_commands)


def streams_native_expected(block, ranges, port):
    """How many commands of this block the owner at `port` should run natively (0: main runs them)."""
    if block[0][0] == b"MULTI":
        # RL-2s: with RL-g6h an owner runs a MULTI block of native stream commands on its own key at EXEC (each one
        # counted in owner_stream_commands); before it, main ran every MULTI block
        if not MULTI_NATIVE:
            return 0
        inner = block[1:-1]
        if not all(streams_native_expected([a], ranges, port) for a in inner):
            return 0
        return len(inner)
    a = block[0]
    n = a[0].upper()
    if n in (b"TYPE", b"DEL", b"SET") or a[-1:] == [b"HELP"]:
        return 0
    if n in (b"XREAD", b"XREADGROUP"):
        ks = streams_keys(a)
        if not ks or len({key_slot(x) for x in ks}) != 1:
            return 0
        k = ks[0]
    else:
        k = block_key(block)
    if k is None:
        return 0
    s = key_slot(k)
    return 1 if any(lo <= s <= hi and p == port for lo, hi, p in ranges) else 0


# ------------------------------------------------------------------------------------------------ AOF records

def aof_records(d):
    ad = os.path.join(d, "appendonlydir")
    files = sorted([f for f in os.listdir(ad) if ".incr.aof" in f], key=lambda f: int(f.split(".")[-3]))
    data = b"".join(open(os.path.join(ad, f), "rb").read() for f in files)
    recs, i = [], 0
    while i < len(data):
        if data[i:i + 1] != b"*":
            raise ValueError("bad AOF at %d: %r" % (i, data[i:i + 40]))
        e = data.index(b"\r\n", i)
        n = int(data[i + 1:e])
        i = e + 2
        argv = []
        for _ in range(n):
            e = data.index(b"\r\n", i)
            ln = int(data[i + 1:e])
            argv.append(data[e + 2:e + 2 + ln])
            i = e + 2 + ln + 2
        recs.append(argv)
    return recs


def aof_units(recs):
    units, cur = [], None
    for a in recs:
        n = a[0].upper()
        if n == b"SELECT":
            continue
        if n == b"MULTI":
            cur = []
        elif n == b"EXEC":
            units.append(cur)
            cur = None
        elif cur is not None:
            cur.append(a)
        else:
            units.append([a])
    return units


def rec_keys(a):
    n = a[0].upper()
    if n == b"XGROUP":
        return [a[2]]
    if n in (b"DEL", b"UNLINK"):
        return a[1:]
    return [a[1]] if len(a) > 1 else []


def mask_rec(a, gen_ids):
    out, after_time = [], False
    for j, x in enumerate(a):
        if after_time:
            out.append(b"T")
        elif x.isdigit() and len(x) >= 13:
            out.append(b"T")
        elif gen_ids is not None and x in gen_ids:
            out.append(b"GEN")
        else:
            out.append(x)
        after_time = x.upper() == b"TIME" and a[0].upper() == b"XCLAIM"
    return out


def per_key(units, model, gen_ids):
    out = {}
    for u in units:
        ks = set()
        for a in u:
            ks.update(rec_keys(a))
        if not ks or not all(model(k) for k in ks):
            continue
        mu = [mask_rec(a, gen_ids) for a in u]
        for k in ks:
            out.setdefault(k, []).append(mu)
    return out


# ------------------------------------------------------------------------------------------------ run

def first_diff(x, y, path=()):
    """The first place two replies differ: (path, x part, y part), or None."""
    if type(x) != type(y):
        return (path, x, y)
    if isinstance(x, (list, tuple)):
        if len(x) != len(y):
            return (path + ("len",), len(x), len(y))
        for i, (p, q) in enumerate(zip(x, y)):
            label = x[i - 1] if i % 2 == 1 and isinstance(x[i - 1], bytes) else i
            d = first_diff(p, q, path + (label,))
            if d:
                return d
        return None
    return None if x == y else (path, x, y)


def full_state(s, keys):
    c = s.c()
    try:
        out = {}
        for k in keys:
            t = c.cmd("TYPE", k)
            if t == "stream":
                out[k] = (t, c.cmd("XLEN", k), mask(c.cmd("XINFO", "STREAM", k, "FULL", "COUNT", "0")))
            else:
                out[k] = (t,)
        return out
    finally:
        c.close()


def wait_replica(prim, rep, secs=30):
    end = time.time() + secs
    while time.time() < end:
        a, b = prim.c(), rep.c()
        try:
            pi, ri = info(a, "replication"), info(b, "replication")
        finally:
            a.close()
            b.close()
        if ri.get("master_link_status") == "up" and ival(pi, "master_repl_offset") == ival(ri, "master_repl_offset"):
            return True
        time.sleep(0.2)
    return False


def memtier_load(a, ports, secs):
    if not os.path.exists(a.memtier):
        return None
    return subprocess.Popen(["taskset", "-c", a.load_cpus, a.memtier, "-s", "127.0.0.1", "-p", str(ports[0]), "--cluster-mode",
                             "-t", "2", "-c", "8", "--ratio", "1:3", "-d", "64", "--key-maximum", "50000",
                             "--key-pattern", "R:R", "--test-time", str(secs), "--hide-histogram"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser()
    for x in ("--bin", "--ref-bin", "--dir", "--cpus", "--load-cpus"):
        ap.add_argument(x, required=True)
    for x in ("--port", "--owner-port", "--main-cpu", "--bio-cpu", "--seed"):
        ap.add_argument(x, type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--ops", type=int, default=3000)
    ap.add_argument("--tops", type=int, default=1500)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--memtier", default="/home/ec2-user/ns/memtier_benchmark")
    a = ap.parse_args()
    a.all_cpus = "%s,%d,%d" % (a.cpus, a.main_cpu, a.bio_cpu)
    os.makedirs(a.dir, exist_ok=True)
    aof = ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp", "--aof-use-rdb-preamble", "no",
           "--stream-node-max-entries", "8", "--repl-diskless-sync-delay", "0"]
    servers = []
    try:
        cand = Server(a, "cand", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, aof).start()
        servers.append(cand)
        rep = Server(a, "replica", a.bin, a.port + 1, a.owner_port + 30, a.cpus, a.main_cpu,
                     ["--stream-node-max-entries", "8", "--replicaof", "127.0.0.1", str(a.port)]).start()
        servers.append(rep)
        ref = Server(a, "ref", a.ref_bin, a.port + 20, a.owner_port + 20, a.cpus, a.main_cpu, aof).start()
        servers.append(ref)
        rrep = Server(a, "ref-replica", a.ref_bin, a.port + 21, a.owner_port + 50, a.cpus, a.main_cpu,
                      ["--stream-node-max-entries", "8", "--replicaof", "127.0.0.1", str(a.port + 20)]).start()
        servers.append(rrep)
        # both replicas synced before the first write: from here on they see only what is propagated
        if not (wait_replica(cand, rep) and wait_replica(ref, rrep)):
            raise RuntimeError("a replica did not sync")
        ranges = owner_ranges(cand)
        oports = [p for _, _, p in ranges]
        rng = random.Random(a.seed)
        keys = [b"{xs%d.%d}" % (a.seed, i) for i in range(20)]
        pairs = [[b"{xm%d.%d}:a" % (a.seed, i), b"{xm%d.%d}:b" % (a.seed, i)] for i in range(4)]
        pairs.append([keys[0], keys[1]])  # two slots: main
        o0 = cand.owners()
        global MULTI_NATIVE
        MULTI_NATIVE = "owner_txn_commands" in o0  # RL-2s: RL-g6h's counter
        cconns = {p: cand.c(p) for p in [a.port] + oports}
        rconn = ref.c()
        sent = {"native": 0, "total": 0}

        def cand_conn(block, conns=cconns, stats=sent):
            k = block_key(block)
            x = rng.random()
            if k is None or x < 0.05:
                p = a.port
            elif x < 0.10:
                p = rng.choice(oports)
            else:
                s = key_slot(k)
                p = [pp for lo, hi, pp in ranges if lo <= s <= hi][0]
            stats["total"] += len(block)
            if p != a.port:
                stats["native"] += streams_native_expected(block, ranges, p)
            return conns[p]

        # A: lockstep
        g = Gen(a.seed, keys, [p for p in pairs if len(p) == 2])
        ops = [g.op() for _ in range(a.ops)]
        rc, rr = [], []
        run_ops(cand_conn, ops, rc)
        run_ops(lambda b: rconn, ops, rr)
        diff = [(i, x[0], x[1], y[1]) for i, (x, y) in enumerate(zip(rc, rr)) if x[1] != y[1]]
        check("A.lockstep_replies", not diff and len(rc) == len(rr), "%d commands; first diff %s" % (len(rc), diff[:1]))

        # B: under load, threads on their own keys
        tkeys = [[b"{xt%d.%d.%d}" % (a.seed, t, i) for i in range(4)] for t in range(a.threads)]
        tpairs = [[[b"{xu%d.%d}:a" % (a.seed, t), b"{xu%d.%d}:b" % (a.seed, t)]] for t in range(a.threads)]
        tops = []
        for t in range(a.threads):
            gt = Gen(a.seed * 1000 + t, tkeys[t], tpairs[t], block=False)  # a block on main vetoes every owner
            tops.append([gt.op() for _ in range(a.tops)])
        tres = {"cand": [None] * a.threads, "ref": [None] * a.threads}

        def worker(side, t):
            if side == "cand":
                conns = {p: cand.c(p) for p in [a.port] + oports}
                st = {"native": 0, "total": 0}
                trng = random.Random(a.seed * 7 + t)

                def cf(block):
                    k = block_key(block)
                    if k is None or trng.random() < 0.05:
                        p = a.port
                    else:
                        s = key_slot(k)
                        p = [pp for lo, hi, pp in ranges if lo <= s <= hi][0]
                    st["total"] += len(block)
                    if p != a.port:
                        st["native"] += streams_native_expected(block, ranges, p)
                    return conns[p]
                out = []
                run_ops(cf, tops[t], out)
                tres[side][t] = (out, st)
            else:
                c = ref.c()
                out = []
                run_ops(lambda b: c, tops[t], out)
                tres[side][t] = (out, None)
                c.close()
                return
            for c in conns.values():
                c.close()

        mt = memtier_load(a, oports, 600)
        time.sleep(1)
        ths = [threading.Thread(target=worker, args=("cand", t)) for t in range(a.threads)]
        t0 = time.time()
        for x in ths:
            x.start()
        for x in ths:
            x.join()
        tb = time.time() - t0
        if mt:
            mt.terminate()
            mt.wait()
        ths = [threading.Thread(target=worker, args=("ref", t)) for t in range(a.threads)]
        for x in ths:
            x.start()
        for x in ths:
            x.join()
        bad = []
        for t in range(a.threads):
            xc, xr = tres["cand"][t][0], tres["ref"][t][0]
            d = [(i, p[0], p[1], q[1]) for i, (p, q) in enumerate(zip(xc, xr)) if p[1] != q[1]]
            if d or len(xc) != len(xr):
                bad.append((t, d[:1]))
            sent["native"] += tres["cand"][t][1]["native"]
            sent["total"] += tres["cand"][t][1]["total"]
        check("B.under_load_replies", not bad, "%d threads x %d blocks in %.1f s with a memtier load %s; bad %s" % (
            a.threads, a.tops, tb, "on" if mt else "missing", bad[:1]))

        # C: generated IDs
        akeys = [b"{xa%d.%d}" % (a.seed, i) for i in range(4)]
        gen_c, gen_r = set(), set()
        okc = okr = True
        for side, conn_of, gen in (("cand", lambda k: cconns[[pp for lo, hi, pp in ranges if lo <= key_slot(k) <= hi][0]], gen_c),
                                   ("ref", lambda k: rconn, gen_r)):
            for k in akeys:
                last = (0, 0)
                for j in range(60):
                    if j % 3 == 2:
                        argv = [b"XADD", k, b"%d-*" % (10 ** 13 + j)]
                    else:
                        argv = [b"XADD", k] + ([b"MAXLEN", b"~", b"10"] if j % 4 == 0 else []) + [b"*"]
                    rp = conn_of(k).cmd(*(argv + [b"f", b"%d" % j]))
                    if not isinstance(rp, bytes) or b"-" not in rp:
                        okc = okr = False
                        continue
                    ms, sq = (int(x) for x in rp.split(b"-"))
                    if (ms, sq) <= last:
                        if side == "cand":
                            okc = False
                        else:
                            okr = False
                    last = (ms, sq)
                    gen.add(rp)
        check("C.generated_ids", okc and okr, "%d candidate, %d parent replies valid and increasing" % (len(gen_c), len(gen_r)))

        o1 = cand.owners()
        nat = ival(o1, "owner_stream_commands") - ival(o0, "owner_stream_commands")
        exp_n = sent["native"] + len(akeys) * 60
        check("native.share", nat >= exp_n * 0.99 and nat <= exp_n + 50,
              "owner_stream_commands +%d, expected %d (of %d commands sent; main ran the rest: BLOCK, MULTI, other "
              "owners' keys, main's port); handoffs +%d" % (nat, exp_n, sent["total"] + len(akeys) * 60,
                                                            ival(o1, "owner_handoffs_to_main") - ival(o0, "owner_handoffs_to_main")))
        for c in list(cconns.values()) + [rconn]:
            c.close()

        # state: candidate, parent, replica, AOF reload
        allk = keys + [x for p in pairs for x in p] + [x for t in tkeys for x in t] + [x for p in tpairs for q in p for x in q]
        allk = sorted(set(allk))
        sc, sr = full_state(cand, allk), full_state(ref, allk)
        dk = [k for k in allk if sc.get(k) != sr.get(k)]
        check("state.cand_vs_parent", not dk, "%d keys (%d streams) XINFO STREAM FULL equal; first diff %s" % (
            len(allk), sum(1 for v in sc.values() if v[0] == "stream"), [(k, first_diff(sc.get(k), sr.get(k))) for k in dk[:3]]))
        # Stock does not carry everything to a replica or the AOF (a consumer an XREADGROUP made without
        # delivering anything, the delivery count of history reads, ...): the candidate's replica and AOF reload are
        # compared with the parent's own replica and AOF reload on the model keys, and with the candidate itself on the
        # generated-ID keys (XADD only).
        okrep = wait_replica(cand, rep) and wait_replica(ref, rrep)
        sp, srp = full_state(rep, allk + akeys), full_state(rrep, allk)
        sc2 = full_state(cand, allk + akeys)
        dk = [k for k in allk if sp.get(k) != srp.get(k)] + [k for k in akeys if sp.get(k) != sc2.get(k)]
        own_c = sum(1 for k in allk if sp.get(k) != sc2.get(k))
        own_r = sum(1 for k in allk if srp.get(k) != sr.get(k))
        check("state.replica", okrep and not dk, "replicas caught up %s; candidate's replica equal to the parent's replica on "
              "%d model keys and to the candidate on %d generated-ID keys; keys where a replica differs from its primary "
              "(stock propagation): candidate %d, parent %d; first diff %s" % (
                  okrep, len(allk) - len([k for k in dk if k in allk]), len(akeys), own_c, own_r,
                  [(k, first_diff(sp.get(k), srp.get(k) if k in allk else sc2.get(k))) for k in dk[:3]]))

        # propagation
        rcand, rref = aof_records(cand.dir), aof_records(ref.dir)
        model = lambda k: k.startswith(b"{xs") or k.startswith(b"{xm") or k.startswith(b"{xt") or k.startswith(b"{xu")
        pc, pr = per_key(aof_units(rcand), model, None), per_key(aof_units(rref), model, None)
        dk = [k for k in sorted(set(pc) | set(pr)) if pc.get(k) != pr.get(k)]
        nrec = sum(len(u) for v in pc.values() for u in v)
        first = None
        if dk:
            x, y = pc.get(dk[0], []), pr.get(dk[0], [])
            j = next((i for i in range(min(len(x), len(y))) if x[i] != y[i]), min(len(x), len(y)))
            first = (dk[0], j, x[j] if j < len(x) else None, y[j] if j < len(y) else None)
        check("aof.byte_equal_per_key", not dk and pc, "%d keys, %d records (units in order per key) equal to the parent's "
              "except the clock; first diff %s" % (len(pc), nrec, first))
        amodel = lambda k: k.startswith(b"{xa")
        ac = per_key(aof_units(rcand), amodel, gen_c)
        ar = per_key(aof_units(rref), amodel, gen_r)
        check("aof.generated_ids", ac == ar and len(ac) == len(akeys) and all(len(v) == 60 for v in ac.values()),
              "XADD * / <ms>-* records: the generated ID in each (masked GEN), trims rewritten; %s" % (
                  {k.decode(): len(v) for k, v in ac.items()}))
        xadds = [r for r in rcand if r[0].upper() == b"XADD"]
        badx = [r for r in xadds if b"*" in r[2:] or b"~" in r[2:] or any(x.endswith(b"-*") for x in r[2:])]
        check("aof.xadd_rewritten", xadds and not badx, "%d XADD records, %d with * or ~ left: %s" % (len(xadds), len(badx), badx[:1]))

        # AOF reload: the candidate's AOF into the candidate binary, the parent's into the parent binary
        for x in servers:
            x.stop()
        servers = []
        rl = Server(a, "reload", a.bin, a.port + 2, a.owner_port + 40, a.cpus, a.main_cpu,
                    ["--appendonly", "yes", "--aof-incr-format", "resp", "--stream-node-max-entries", "8"])
        shutil.copytree(os.path.join(cand.dir, "appendonlydir"), os.path.join(rl.dir, "appendonlydir"))
        rl.start()
        servers.append(rl)
        rrl = Server(a, "ref-reload", a.ref_bin, a.port + 3, a.owner_port + 60, a.cpus, a.main_cpu,
                     ["--appendonly", "yes", "--aof-incr-format", "resp", "--stream-node-max-entries", "8"])
        shutil.copytree(os.path.join(ref.dir, "appendonlydir"), os.path.join(rrl.dir, "appendonlydir"))
        rrl.start()
        servers.append(rrl)
        sl, srl = full_state(rl, allk + akeys), full_state(rrl, allk)
        dk = [k for k in allk if sl.get(k) != srl.get(k)] + [k for k in akeys if sl.get(k) != sc2.get(k)]
        own_c = sum(1 for k in allk if sl.get(k) != sc2.get(k))
        own_r = sum(1 for k in allk if srl.get(k) != sr.get(k))
        check("state.aof_reload", not dk, "the candidate's AOF reloaded equal to the parent's AOF reloaded on %d model keys "
              "and to the candidate on %d generated-ID keys; keys where a reload differs from its server (stock): "
              "candidate %d, parent %d; first diff %s" % (
                  len(allk) - len([k for k in dk if k in allk]), len(akeys), own_c, own_r,
                  [(k, first_diff(sl.get(k), srl.get(k) if k in allk else sc2.get(k))) for k in dk[:3]]))
    except Exception as e:
        import traceback
        traceback.print_exc()
        check("ran", False, repr(e))
    finally:
        for s in servers:
            try:
                s.stop()
            except Exception:
                pass
        for s in ("cand", "replica", "ref", "ref-replica", "reload", "ref-reload"):
            d = os.path.join(a.dir, s, "appendonlydir")
            if os.path.isdir(d):
                shutil.rmtree(d)
    bad = [r for r in bp.RESULTS if not r["ok"]]
    print("RLSTREAMCHECK " + json.dumps(dict(seed=a.seed, passed=len(bp.RESULTS) - len(bad), failed=len(bad))), flush=True)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
