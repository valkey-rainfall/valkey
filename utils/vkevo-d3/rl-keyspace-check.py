#!/usr/bin/env python3
"""RL-g1c-native-keyspace gate: owners run EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / PERSIST / TTL / PTTL / EXISTS /
TYPE natively. Starts its own servers: A = --bin with owner threads (the build under test), R = --ref-bin with
owner-threads 0 (the parent's stock main path), and per suite a replica of A and an AOF reload of A.

Suites (--suite, default all):
  plain     no AOF, no replica
  mm        maxmemory 1gb allkeys-lru (owners run inside their memory leases; nothing is evicted)
  aofrepl   A with appendonly everysec (compact incr records) and a replica (records encoded twice, split)
  aofresp   A with appendonly always and aof-incr-format resp
Each suite:
  cases     fixed sequences on an owner port of A and on R, every reply equal (TTL +-1 s, PTTL +-150 ms):
            NX / XX / GT / LT, flag conflicts, bad numbers, overflow, negative / zero / past times (key deleted,
            DEL record), PERSIST, TTL / PTTL of missing / persistent keys, TYPE of every type, EXISTS with
            repeated and same-slot keys, cross-slot EXISTS (main), EXPIRE on a hash and a list, MULTI with EXPIRE
            (main), lazy expiry with active expiry off for every touched command (expired_keys +1 each);
            native: owner_native_commands grows by the case count and the handoffs do not; commandstats calls
            and errorstats of the touched commands equal R's
  model     --threads connections per server, each with its own keys (hash tags in one owner's slots, 16 keys
            per tag), random batches (1-8 commands, pipelined) of SET [EX|PX|KEEPTTL] / GET / EXPIRE / PEXPIRE /
            EXPIREAT / PEXPIREAT (random NX/XX/GT/LT, short 60-400 ms, long, zero, negative, past) / TTL / PTTL /
            PERSIST / EXISTS (1-3 keys) / TYPE / DEL / HSET / RPUSH; 85% of A's batches on the key's owner port,
            15% on A's main port; each batch to A and R, every reply equal (TTL / PTTL within tolerance) and
            consistent with a model (keys within 40 ms of their expiry are not touched, so both sides agree);
            then every key's type, value digest and expire time equal (+-150 ms), expired_keys and DBSIZE equal
  repl      (aofrepl) the replica caught up: every key's type, digest and PEXPIRETIME equal to A's exactly, DBSIZE
  aof       (aofrepl, aofresp) A shut down, a fresh server on its AOF dir: the same keyspace exactly
Prints PASS/FAIL lines and 'RLKEYSPACE {json}'; exit 1 on any FAIL.
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
Conn, Err, check = d3.Conn, d3.Err, d3.check

A = None
LONG = (60, 3600)        # seconds
SHORT_MS = (60, 400)     # milliseconds
MARGIN = 40              # ms around an expiry in which a key is not touched
TOL_MS = 150             # A and R run each command a few ms apart


def now_ms():
    return int(time.time() * 1000)


class Srv:
    def __init__(self, name, binary, port, args, cpus, d, fresh=True):
        self.name, self.binary, self.port, self.args, self.cpus = name, binary, port, args, cpus
        self.dir = os.path.join(d, name)
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        cmd = ["taskset", "-c", cpus, binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", os.path.join(self.dir, "log")] + args
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(600):
            try:
                c = Conn("127.0.0.1", port, timeout=5)
                r = c.cmd("PING")
                c.close()
                if r in ("PONG", b"PONG"):
                    break
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        self.c = Conn("127.0.0.1", port, timeout=120)
        for _ in range(600):  # AOF loading
            if d3.info(self.c, "persistence").get("loading") == "0":
                break
            time.sleep(0.05)
        self.ranges = []
        raw = self.c.cmd("INFO", "owners")
        raw = raw.decode() if isinstance(raw, bytes) else raw
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))

    def oport(self, key):
        s = d3.key_slot(key)
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


def cmdstats(c, names):
    i = d3.info(c, "commandstats")
    out = {}
    for n in names:
        v = i.get("cmdstat_" + n, "")
        kv = dict(x.split("=") for x in v.split(",") if "=" in x)
        out[n] = (int(kv.get("calls", 0)), int(kv.get("rejected_calls", 0)), int(kv.get("failed_calls", 0)))
    return out


def errstats(c):
    i = d3.info(c, "errorstats")
    return {k: v for k, v in i.items() if k.startswith("errorstat_")}


def stat(c, k):
    return int(d3.info(c, "stats")[k])


NAMES = ["expire", "pexpire", "expireat", "pexpireat", "persist", "ttl", "pttl", "exists", "type"]


def tags_in(rng, lo, hi, n, prefix):
    out = []
    while len(out) < n:
        t = b"%s%d" % (prefix, rng.randrange(1 << 30))
        if lo <= d3.key_slot(b"{" + t + b"}") <= hi:
            out.append(t)
    return out


def same(a, b, kind):
    """A's reply vs R's: exact, except TTL / PTTL values within the tolerance."""
    if kind == "ttl" and isinstance(a, int) and isinstance(b, int) and a >= 0 and b >= 0:
        return abs(a - b) <= 1
    if kind == "pttl" and isinstance(a, int) and isinstance(b, int) and a >= 0 and b >= 0:
        return abs(a - b) <= TOL_MS
    if isinstance(a, Err) and isinstance(b, Err):
        return str(a) == str(b)
    return a == b and type(a) is type(b) if not isinstance(a, (Err,)) else False


def kind_of(cmd):
    n = cmd[0].lower() if isinstance(cmd[0], str) else cmd[0].decode().lower()
    return {"ttl": "ttl", "pttl": "pttl"}.get(n, "x")


# ------------------------------------------------------------------------------------------------ cases
def cases(a, r):
    rng = random.Random(A.seed)
    lo, hi = a.ranges[0]
    t = tags_in(rng, lo, hi, 4, b"case")
    k = [b"{%s}:k%d" % (t[0], j) for j in range(8)]
    h, l, z = b"{%s}:h" % t[1], b"{%s}:l" % t[1], b"{%s}:z" % t[1]
    ca = Conn("127.0.0.1", a.oport(k[0]))
    cr = Conn("127.0.0.1", r.port)
    now_s = int(time.time())
    seq = [
        ("EXPIRE", k[0], 100), ("SET", k[0], "v"), ("EXPIRE", k[0], 100), ("TTL", k[0]), ("PTTL", k[0]),
        ("EXPIRE", k[0], 50, "NX"), ("EXPIRE", k[0], 300, "XX"), ("TTL", k[0]),
        ("EXPIRE", k[0], 10, "GT"), ("EXPIRE", k[0], 400, "GT"), ("EXPIRE", k[0], 500, "LT"),
        ("EXPIRE", k[0], 20, "LT"), ("TTL", k[0]), ("EXPIRE", k[0], 10, "NX", "XX"), ("EXPIRE", k[0], 10, "GT", "LT"),
        ("EXPIRE", k[0], 10, "NX", "GT"), ("EXPIRE", k[0], "abc"), ("EXPIRE", k[0], 10, "BOGUS"),
        ("EXPIRE", k[0], 9223372036854775807), ("PEXPIRE", k[0], 9223372036854775807),
        ("PEXPIREAT", k[0], 9223372036854775807), ("EXPIREAT", k[0], -9223372036854775807), ("TTL", k[0]),
        ("PERSIST", k[0]), ("PERSIST", k[0]), ("TTL", k[0]), ("PTTL", k[0]), ("TTL", k[7]), ("PTTL", k[7]),
        ("PERSIST", k[7]), ("EXPIRE", k[0], 10, "XX"), ("EXPIRE", k[0], 10, "GT"), ("EXPIRE", k[0], 10, "LT"),
        ("TTL", k[0]), ("PERSIST", k[0]),
        ("SET", k[1], "v1"), ("EXPIRE", k[1], -5), ("EXISTS", k[1]), ("GET", k[1]),
        ("SET", k[1], "v1"), ("EXPIRE", k[1], 0), ("TYPE", k[1]),
        ("SET", k[2], "v2"), ("EXPIREAT", k[2], now_s - 10), ("TTL", k[2]),
        ("SET", k[2], "v2"), ("PEXPIREAT", k[2], 1), ("EXISTS", k[2]),
        ("SET", k[3], "v3"), ("PEXPIRE", k[3], -1), ("TYPE", k[3]),
        ("SET", k[3], "v3"), ("EXPIREAT", k[3], now_s + 1000), ("PEXPIREAT", k[3], now_s * 1000 + 2000000, "GT"),
        ("TTL", k[3]), ("SET", k[3], "v4", "KEEPTTL"), ("TTL", k[3]), ("SET", k[3], "v5"), ("TTL", k[3]),
        ("SET", k[4], 12345), ("EXPIRE", k[4], 1000), ("TYPE", k[4]), ("GET", k[4]),
        ("SET", k[5], "x" * 300), ("PEXPIRE", k[5], 900000), ("PTTL", k[5]), ("GET", k[5]),
        ("EXISTS", k[4], k[4], k[5], k[6]), ("EXISTS", k[6]), ("TYPE", k[6]),
        ("HSET", h, "f", "v"), ("RPUSH", l, "a", "b"), ("ZADD", z, 1, "m"), ("TYPE", h), ("TYPE", l), ("TYPE", z),
        ("TYPE", k[4]), ("EXPIRE", h, 777), ("TTL", h), ("PEXPIRE", l, 888000), ("PTTL", l), ("PERSIST", h),
        ("TTL", h), ("EXISTS", h, l, z, k[4]), ("EXPIRE", z, -1), ("TYPE", z), ("EXISTS", z),
        ("TYPE", h, "extra"), ("TTL",), ("EXISTS",), ("PERSIST", k[0], k[1]),
    ]
    s0 = d3.ostat(a.c)
    st0a, st0r = cmdstats(a.c, NAMES), cmdstats(r.c, NAMES)
    e0a, e0r = errstats(a.c), errstats(r.c)
    ra, rr = [], []
    for cmd in seq:
        ra.append(ca.cmd(*cmd))
        rr.append(cr.cmd(*cmd))
    s1 = d3.ostat(a.c)
    bad = [(cmd, x, y) for cmd, x, y in zip(seq, ra, rr) if not same(x, y, kind_of(cmd))]
    check("cases.replies_equal_ref", not bad, bad[:4] if bad else "%d commands" % len(seq))
    check("cases.handoffs_only_for_non_native", s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"] <= 10,
          "native +%d, handoffs +%d (HSET, RPUSH, ZADD, 4 arity errors, cross-slot EXISTS)" %
          (s1["owner_native_commands"] - s0["owner_native_commands"],
           s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    # only key-level commands on this owner's keys, one at a time and pipelined: every one native, no handoff
    time.sleep(0.2)
    nseq = []
    for j in range(40):
        kk = k[j % 6]
        nseq += [("EXPIRE", kk, 1000 + j), ("PEXPIRE", kk, 900000 + j, "GT"), ("EXPIREAT", kk, now_s + 5000, "LT"),
                 ("PEXPIREAT", kk, now_s * 1000 + 4000000 + j), ("TTL", kk), ("PTTL", kk), ("PERSIST", kk),
                 ("EXISTS", kk, k[(j + 1) % 6]), ("TYPE", kk)]
    s0 = d3.ostat(a.c)
    ra = [ca.cmd(*cmd) for cmd in nseq[:90]]
    ca.send(nseq[90:])
    ra += [ca.read() for _ in nseq[90:]]
    s1 = d3.ostat(a.c)
    rr = [cr.cmd(*cmd) for cmd in nseq]
    bad = [(cmd, x, y) for cmd, x, y in zip(nseq, ra, rr) if not same(x, y, kind_of(cmd))]
    dn = s1["owner_native_commands"] - s0["owner_native_commands"]
    dh = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
    check("cases.native_on_owner", not bad and dn == len(nseq) and dh == 0,
          bad[:3] if bad else "native +%d of %d, handoffs +%d" % (dn, len(nseq), dh))
    st1a, st1r = cmdstats(a.c, NAMES), cmdstats(r.c, NAMES)
    da = {n: tuple(x - y for x, y in zip(st1a[n], st0a[n])) for n in NAMES}
    dr = {n: tuple(x - y for x, y in zip(st1r[n], st0r[n])) for n in NAMES}
    check("cases.commandstats_equal_ref", da == dr, (da, dr) if da != dr else da)
    time.sleep(0.15)  # errorstats fold on main's next stop
    e1a, e1r = errstats(a.c), errstats(r.c)
    dea = {x: v for x, v in e1a.items() if e0a.get(x) != v}
    der = {x: v for x, v in e1r.items() if e0r.get(x) != v}
    dea2 = {x: int(v.split("=")[1]) - int(e0a.get(x, "count=0").split("=")[1]) for x, v in dea.items()}
    der2 = {x: int(v.split("=")[1]) - int(e0r.get(x, "count=0").split("=")[1]) for x, v in der.items()}
    check("cases.errorstats_equal_ref", dea2 == der2, (dea2, der2))

    # MULTI with key-level commands on an owner port: main runs the transaction
    seqm = [("SET", k[6], "m"), ("MULTI",), ("EXPIRE", k[6], 500), ("TTL", k[6]), ("PERSIST", k[6]), ("EXISTS", k[6]),
            ("TYPE", k[6]), ("PEXPIRE", k[6], -1), ("EXISTS", k[6]), ("EXEC",), ("EXISTS", k[6])]
    ra = [ca.cmd(*cmd) for cmd in seqm]
    rr = [cr.cmd(*cmd) for cmd in seqm]
    check("cases.multi_with_key_level", ra == rr and ra[-2] == [1, 500, 1, 1, "string", 1, 0], (ra[-2:], rr[-2:]))

    # lazy expiry on the owner for every touched command (active expiry off on both)
    for c in (a.c, r.c):
        c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)
    lazy = [("TTL",), ("PTTL",), ("EXISTS",), ("TYPE",), ("PERSIST",), ("EXPIRE", 100), ("PEXPIRE", 100000),
            ("EXPIREAT", now_s + 1000), ("PEXPIREAT", now_s * 1000 + 1000000), ("EXPIRE", 100, "XX"), ("GET",)]
    ok, det = True, []
    for j, tail in enumerate(lazy):
        kk = b"{%s}:lazy%d" % (t[2], j)
        for c in (ca, cr):
            c.cmd("SET", kk, "v", "PX", 30)
        time.sleep(0.08)
        x0, y0 = stat(a.c, "expired_keys"), stat(r.c, "expired_keys")
        s0 = d3.ostat(a.c)
        xa = ca.cmd(tail[0], kk, *tail[1:])
        xr = cr.cmd(tail[0], kk, *tail[1:])
        s1 = d3.ostat(a.c)
        time.sleep(0.12)
        x1, y1 = stat(a.c, "expired_keys"), stat(r.c, "expired_keys")
        ex = [ca.cmd("EXISTS", kk), cr.cmd("EXISTS", kk)]
        good = xa == xr and x1 - x0 == 1 and y1 - y0 == 1 and ex == [0, 0] and \
            s1["owner_native_commands"] - s0["owner_native_commands"] >= 1
        ok &= good
        det.append((tail[0], xa, xr, x1 - x0, y1 - y0, ex))
    for c in (a.c, r.c):
        c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 1)
    check("cases.lazy_expire_on_owner", ok, [d for d in det if not (d[1] == d[2] and d[3] == 1 and d[4] == 1)] or
          "%d commands, expired_keys +1 each on both" % len(lazy))
    # cross-slot EXISTS on the owner port (main), same-slot EXISTS native
    kx = [b"{%s}:x" % tt for tt in tags_in(rng, 0, 16383, 3, b"cross")]
    r1 = [ca.cmd("SET", kx[0], 1), ca.cmd("EXISTS", kx[0], kx[1], kx[2], k[4])]
    r2 = [cr.cmd("SET", kx[0], 1), cr.cmd("EXISTS", kx[0], kx[1], kx[2], k[4])]
    check("cases.exists_cross_slot", r1 == r2 == ["OK", 2], (r1, r2))
    ca.close()
    cr.close()


# ------------------------------------------------------------------------------------------------ model
class Model:
    """Per key: (type, value, expire) with expire = None or [lo, hi] absolute ms (relative TTLs: the batch's send
    and reply times bound the server's clock)."""

    def __init__(self):
        self.k = {}

    def alive(self, key, t):
        e = self.k.get(key)
        if e is None:
            return False
        ex = e[2]
        return ex is None or ex[0] > t

    def ambiguous(self, key, t0, t1):
        e = self.k.get(key)
        if e is None or e[2] is None:
            return False
        lo, hi = e[2]
        return lo - MARGIN <= t1 and hi + MARGIN >= t0

    def drop_expired(self, t):
        for key in [x for x, e in self.k.items() if e[2] is not None and e[2][1] < t]:
            del self.k[key]


def gen_op(rng, keys_of_tag, tags, m, t):
    tag = rng.choice(tags)
    key = rng.choice(keys_of_tag[tag])
    x = rng.random()
    if x < 0.13:
        y = rng.random()
        if y < 0.15:
            return ("SET", key, "v%d" % rng.randrange(1000), "PX", rng.randrange(*SHORT_MS))
        if y < 0.35:
            return ("SET", key, "v%d" % rng.randrange(1000), "EX", rng.randrange(*LONG))
        if y < 0.45:
            return ("SET", key, "k%d" % rng.randrange(1000), "KEEPTTL")
        return ("SET", key, "v%d" % rng.randrange(1000))
    if x < 0.20:
        return ("GET", key)
    flag = []
    if rng.random() < 0.3:
        flag = [rng.choice(["NX", "XX", "GT", "LT"])]
        if rng.random() < 0.03:
            flag.append(rng.choice(["NX", "XX", "GT", "LT"]))
    if x < 0.32:
        y = rng.random()
        v = rng.randrange(*LONG) if y < 0.8 else (0 if y < 0.9 else -rng.randrange(1, 100))
        return ("EXPIRE", key, v, *flag)
    if x < 0.42:
        y = rng.random()
        v = rng.randrange(*SHORT_MS) if y < 0.4 else (rng.randrange(LONG[0] * 1000, LONG[1] * 1000) if y < 0.85 else
                                                     -rng.randrange(1, 100000))
        return ("PEXPIRE", key, v, *flag)
    if x < 0.47:
        y = rng.random()
        v = t // 1000 + rng.randrange(*LONG) if y < 0.8 else t // 1000 - rng.randrange(2, 1000)
        return ("EXPIREAT", key, v, *flag)
    if x < 0.53:
        y = rng.random()
        v = t + rng.randrange(*SHORT_MS) + 50 if y < 0.4 else (t + rng.randrange(LONG[0] * 1000, LONG[1] * 1000) if y < 0.85
                                                              else t - rng.randrange(1000, 100000))
        return ("PEXPIREAT", key, v, *flag)
    if x < 0.62:
        return ("TTL", key)
    if x < 0.70:
        return ("PTTL", key)
    if x < 0.76:
        return ("PERSIST", key)
    if x < 0.85:
        n = rng.randrange(1, 4)
        return ("EXISTS",) + tuple(rng.choice(keys_of_tag[tag]) for _ in range(n))
    if x < 0.92:
        return ("TYPE", key)
    if x < 0.95:
        return ("DEL", key)
    if x < 0.975:
        return ("HSET", key, "f", "v")
    if x < 0.99:
        return ("RPUSH", key, "e")
    return ("EXPIRE", key, "notanumber")


def keys_of(op):
    if op[0] == "EXISTS":
        return list(op[1:])
    return [op[1]]


def model_apply(m, op, ra, t0, t1, issues):
    """Updates the model from A's reply (already compared with R's) and checks it against the model."""
    name, key = op[0], op[1]
    e = m.k.get(key)
    alive = m.alive(key, t0)
    exp = None if not alive else e[2]

    def bad(why):
        issues.append((op, ra, why))

    if name == "SET":
        if isinstance(ra, Err):
            if not (alive and e[0] != "string") and "WRONGTYPE" not in ra:
                bad("set err")
            return
        if ra != "OK":
            bad("set reply")
        args = [str(x).upper() if isinstance(x, str) else x for x in op[3:]]
        if "PX" in args:
            v = op[4]
            m.k[key] = ("string", op[2], [t0 + v, t1 + v])
        elif "EX" in args:
            v = op[4] * 1000
            m.k[key] = ("string", op[2], [t0 + v, t1 + v])
        elif "KEEPTTL" in args:
            m.k[key] = ("string", op[2], exp)
        else:
            m.k[key] = ("string", op[2], None)
        return
    if name == "GET":
        want = None if not alive else (op and e[1].encode() if e[0] == "string" else "WRONGTYPE")
        if want == "WRONGTYPE":
            if not (isinstance(ra, Err) and "WRONGTYPE" in ra):
                bad("get wrongtype")
        elif ra != want:
            bad("get value %r" % (want,))
        return
    if name in ("EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"):
        if isinstance(ra, Err):
            return  # compared with R's; flag errors keep the key as it was
        if not alive:
            if ra != 0:
                bad("expire on missing")
            if e is not None:
                del m.k[key]
            return
        if ra == 0:
            return
        v = op[2]
        if name == "EXPIRE":
            when = [t0 + v * 1000, t1 + v * 1000]
        elif name == "PEXPIRE":
            when = [t0 + v, t1 + v]
        elif name == "EXPIREAT":
            when = [v * 1000, v * 1000]
        else:
            when = [v, v]
        if when[1] <= t0 or (when[0] <= t1 and v <= 0):
            del m.k[key]  # already expired: deleted
        else:
            m.k[key] = (e[0], e[1], when)
        return
    if name == "PERSIST":
        want = 1 if alive and exp is not None else 0
        if ra != want:
            bad("persist %d" % want)
        if alive and ra == 1:
            m.k[key] = (e[0], e[1], None)
        return
    if name in ("TTL", "PTTL"):
        if not alive:
            if ra != -2:
                bad("ttl missing")
        elif exp is None:
            if ra != -1:
                bad("ttl none")
        else:
            lo, hi = exp[0] - t1, exp[1] - t0
            if name == "TTL":
                lo, hi = (lo + 500) // 1000 - 1, (hi + 500) // 1000 + 1
            else:
                lo, hi = lo - 5, hi + 5
            if not (isinstance(ra, int) and lo <= ra <= hi):
                bad("ttl range %d..%d" % (lo, hi))
        return
    if name == "EXISTS":
        want = sum(1 for k in op[1:] if m.alive(k, t0))
        if ra != want:
            bad("exists %d" % want)
        return
    if name == "TYPE":
        want = e[0] if alive else "none"
        if ra != want:
            bad("type %s" % want)
        return
    if name == "DEL":
        want = 1 if alive else 0
        if ra != want:
            bad("del %d" % want)
        m.k.pop(key, None)
        return
    if name in ("HSET", "RPUSH"):
        typ = "hash" if name == "HSET" else "list"
        if alive and e[0] != typ:
            if not (isinstance(ra, Err) and "WRONGTYPE" in ra):
                bad("wrongtype")
            return
        if not alive:
            m.k[key] = (typ, None, None)
        return


def model_worker(wid, a, r, out, stop_at, seed):
    rng = random.Random(seed)
    lo, hi = a.ranges[wid % len(a.ranges)]
    tags = tags_in(rng, lo, hi, 8, b"m%d-" % wid)
    keys_of_tag = {tg: [b"{%s}:%d" % (tg, j) for j in range(16)] for tg in tags}
    ca_own = Conn("127.0.0.1", a.oport(keys_of_tag[tags[0]][0]))
    ca_main = Conn("127.0.0.1", a.port)
    cr = Conn("127.0.0.1", r.port)
    m = Model()
    issues, diffs = [], []
    n_ops = n_batches = n_main = skipped = 0
    while time.time() < stop_at and len(diffs) < 20 and len(issues) < 20:
        t = now_ms()
        batch = []
        used = set()
        for _ in range(rng.randrange(1, 9)):
            for _try in range(6):
                op = gen_op(rng, keys_of_tag, tags, m, t)
                ks = keys_of(op)
                # touch no key within MARGIN of its expiry during the batch (+50 ms for the batch itself), and one
                # op per key per batch (the model updates after the batch)
                if any(m.ambiguous(k, t, t + 50) for k in ks) or any(k in used for k in ks):
                    skipped += 1
                    continue
                used.update(ks)
                batch.append(op)
                break
        if not batch:
            time.sleep(0.005)
            continue
        ca = ca_main if rng.random() < 0.15 else ca_own
        n_main += ca is ca_main
        t0 = now_ms()
        ca.send(batch)
        cr.send(batch)
        ra = [ca.read() for _ in batch]
        rr = [cr.read() for _ in batch]
        t1 = now_ms()
        if t1 - t0 > 45:  # a slow batch: the ambiguity window did not cover it; resync from scratch
            pass
        for op, x, y in zip(batch, ra, rr):
            if not same(x, y, kind_of(op)):
                diffs.append((op, x, y, t1 - t0))
            model_apply(m, op, x, t0, t1, issues)
        m.drop_expired(t0 - MARGIN)
        n_ops += len(batch)
        n_batches += 1
        if rng.random() < 0.02:
            time.sleep(rng.random() * 0.3)
    keys = [k for tg in tags for k in keys_of_tag[tg]]
    out[wid] = dict(ops=n_ops, batches=n_batches, main_batches=n_main, skipped=skipped, diffs=diffs, issues=issues,
                    keys=keys, model=m)
    for c in (ca_own, ca_main, cr):
        c.close()


def snapshot(srv, keys, exact_port=None):
    """{key: (type, digest, pexpiretime)} for keys that exist."""
    c = Conn("127.0.0.1", exact_port or srv.port)
    out = {}
    for i in range(0, len(keys), 200):
        part = keys[i:i + 200]
        cmds = []
        for k in part:
            cmds += [("TYPE", k), ("DEBUG", "DIGEST-VALUE", k), ("PEXPIRETIME", k)]
        c.send(cmds)
        res = [c.read() for _ in cmds]
        for j, k in enumerate(part):
            ty, dg, px = res[3 * j:3 * j + 3]
            if ty != "none":
                out[k] = (ty, dg[0] if isinstance(dg, list) else dg, px)
    c.close()
    return out


def compare_snap(sa, sr, tol):
    bad = []
    for k in set(sa) | set(sr):
        x, y = sa.get(k), sr.get(k)
        if x is None or y is None or x[:2] != y[:2]:
            bad.append((k, x, y))
        elif (x[2] < 0) != (y[2] < 0) or abs(x[2] - y[2]) > tol:
            bad.append((k, x, y))
    return bad


def model_run(a, r, tag):
    stop_at = time.time() + A.secs
    out = {}
    ths = [threading.Thread(target=model_worker, args=(w, a, r, out, stop_at, A.seed * 1000 + w))
           for w in range(A.threads)]
    s0 = d3.ostat(a.c)
    st0 = cmdstats(a.c, NAMES)
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    s1 = d3.ostat(a.c)
    st1 = cmdstats(a.c, NAMES)
    ops = sum(o["ops"] for o in out.values())
    diffs = [d for o in out.values() for d in o["diffs"]]
    issues = [d for o in out.values() for d in o["issues"]]
    check("%s.model.replies_equal_ref" % tag, not diffs and ops > 1000,
          diffs[:3] if diffs else "%d ops in %d batches (%d on main's port), %d skipped picks" %
          (ops, sum(o["batches"] for o in out.values()), sum(o["main_batches"] for o in out.values()),
           sum(o["skipped"] for o in out.values())))
    check("%s.model.replies_match_model" % tag, not issues, issues[:3] if issues else "%d ops" % ops)
    dn = s1["owner_native_commands"] - s0["owner_native_commands"]
    calls = {n: st1[n][0] - st0[n][0] for n in NAMES}
    check("%s.model.ran_native" % tag, dn > 0.5 * ops, "native +%d of %d ops; key-level calls %s; handoffs +%d" %
          (dn, ops, calls, s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    # let every short TTL pass, touch every key (lazy deletes) on both, compare the keyspaces
    time.sleep((SHORT_MS[1] + 2 * MARGIN + 200) / 1000.0)
    keys = sorted(k for o in out.values() for k in o["keys"])
    for srv in (a, r):
        c = Conn("127.0.0.1", srv.port)
        for i in range(0, len(keys), 500):
            c.send([("EXISTS", k) for k in keys[i:i + 500]])
            for _ in keys[i:i + 500]:
                c.read()
        c.close()
    sa, sr = snapshot(a, keys), snapshot(r, keys)
    bad = compare_snap(sa, sr, TOL_MS)
    check("%s.model.keyspace_equal_ref" % tag, not bad and len(sa) > 0,
          bad[:3] if bad else "%d of %d keys present, types/digests equal, expire times within %d ms (%d with an expire)"
          % (len(sa), len(keys), TOL_MS, sum(1 for v in sa.values() if v[2] > 0)))
    da, dr = a.c.cmd("DBSIZE"), r.c.cmd("DBSIZE")
    ea, er = stat(a.c, "expired_keys"), stat(r.c, "expired_keys")
    check("%s.model.dbsize_expired_equal_ref" % tag, da == dr and ea == er,
          "dbsize %s / %s, expired_keys %d / %d" % (da, dr, ea, er))
    return keys, sa


def repl_check(a, rep, keys, tag):
    for _ in range(600):
        off = int(d3.info(a.c, "replication")["master_repl_offset"])
        ri = d3.info(rep.c, "replication")
        if ri.get("master_link_status") == "up" and int(ri.get("slave_repl_offset", -1)) == off:
            break
        time.sleep(0.05)
    ri = d3.info(rep.c, "replication")
    sa, sp = snapshot(a, keys), snapshot(rep, keys)
    bad = compare_snap(sa, sp, 0)
    check("%s.repl.keyspace_equal_primary" % tag, not bad and ri.get("master_link_status") == "up" and len(sa) > 0,
          bad[:3] if bad else "%d keys exactly equal (PEXPIRETIME too); link %s, offsets %s / %s, sync_full %s" %
          (len(sa), ri.get("master_link_status"), d3.info(a.c, "replication")["master_repl_offset"],
           ri.get("slave_repl_offset"), d3.info(a.c, "stats").get("sync_full")))
    check("%s.repl.dbsize_equal" % tag, a.c.cmd("DBSIZE") == rep.c.cmd("DBSIZE"),
          "%s / %s" % (a.c.cmd("DBSIZE"), rep.c.cmd("DBSIZE")))


def owner_args():
    return ["--owner-threads", str(A.n), "--owner-port", str(A.owner_port), "--owner-cpulist", A.cpus,
            "--server-cpulist", str(A.main_cpu), "--bio-cpulist", str(A.bg_cpu), "--aof-rewrite-cpulist", str(A.bg_cpu),
            "--bgsave-cpulist", str(A.bg_cpu)]


def suite(name):
    extra = {"plain": ["--appendonly", "no"],
             "mm": ["--appendonly", "no", "--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"],
             "aofrepl": ["--appendonly", "yes", "--appendfsync", "everysec"],
             "aofresp": ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp"]}[name]
    cpus = "%s,%s,%s" % (A.main_cpu, A.bg_cpu, A.cpus)
    a = Srv("a-" + name, A.bin, A.port, owner_args() + extra + ["--repl-backlog-size", "64mb"], cpus, A.dir)
    r = Srv("r-" + name, A.ref_bin, A.ref_port, ["--owner-threads", "0", "--appendonly", "no"], A.ref_cpus, A.dir)
    rep = None
    if name == "aofrepl":
        rep = Srv("rep-" + name, A.bin, A.replica_port, ["--appendonly", "no"], A.ref_cpus, A.dir)
        rep.c.cmd("REPLICAOF", "127.0.0.1", A.port)
        for _ in range(400):
            if d3.info(rep.c, "replication").get("master_link_status") == "up":
                break
            time.sleep(0.05)
    v = d3.info(a.c, "owners")
    check("%s.owners_native_not_vetoed" % name, v.get("owner_veto") in ("none", "0"),
          "veto %s, split %s" % (v.get("owner_veto"), v.get("owner_repl_split")))
    cases(a, r)
    keys, sa = model_run(a, r, name)
    if rep:
        v = d3.info(a.c, "owners")
        check("%s.repl.split_on" % name, v.get("owner_repl_split") == "yes", v.get("owner_repl_split"))
        repl_check(a, rep, keys, name)
    if name not in ("plain", "mm"):
        info_p = d3.info(a.c, "persistence")
        sa = snapshot(a, keys)
        a.stop()
        b = Srv("a-" + name, A.bin, A.port, owner_args() + extra, cpus, A.dir, fresh=False)
        sb = snapshot(b, keys)
        bad = compare_snap(sa, sb, 0)
        check("%s.aof.reload_keyspace_equal" % name, not bad and len(sa) > 0,
              bad[:3] if bad else "%d keys exactly equal after reloading the AOF (incr format %s, aof_rewrites %s)" %
              (len(sa), info_p.get("aof_incr_format_open", "?"), info_p.get("aof_rewrites")))
        a = b
    for s in (a, r, rep):
        if s:
            s.stop()
            check("%s.no_crash.%s" % (name, s.name), s.crashed() == 0, "crash lines %d" % s.crashed())
            shutil.rmtree(os.path.join(s.dir, "appendonlydir"), ignore_errors=True)


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
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
    ap.add_argument("--suite", default="plain,mm,aofrepl,aofresp")
    A = ap.parse_args()
    os.makedirs(A.dir, exist_ok=True)
    for s in A.suite.split(","):
        suite(s)
    fails = [x for x in d3.RESULTS if not x["ok"]]
    print("RLKEYSPACE " + json.dumps(dict(pass_=len(d3.RESULTS) - len(fails), fail=len(fails))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
