#!/usr/bin/env python3
"""D3-all-g8-k1 check for the owners' GET bursts (owner.c ownerGetBurst: the leading GET frames of a read
answered straight off the query buffer), against the parent's owner path, byte for byte.

Starts two servers with owner-threads 4 on 127.0.0.1: the tree's (--bin, bursts) and the parent's
(--ref-bin, D3-all-g7-rwchild: the gated owner GET ring), sets up the same data on both through the plain
port, sends the same bytes to owner 0's port of each and compares the reply bytes, and the keyspace
hits / misses and cmdstat_get calls each case added:
  basic      own-slot GETs of keys 1-70 B (64 is the burst's longest), values 0 B to 20 KB (the 1, 2 and 3
             digit length headers, a value past the 16 KB reply buffer), INT values, missing keys, GET /
             get / GeT spellings, 300 frames (past the 256 GETs of one burst)
  chunks     the same bytes in random 1-200 byte writes (frames split across reads)
  deep       2000 GETs in one write; 1 GET per write
  mixed      GET / SET / INCR / DEL / MGET / PING of the same keys in one pipeline (order)
  big        values over commandlog-reply-larger-than (1 MB): the stock path, between burst GETs
  ttl        keys with an expire (valid, already expired: lazy expire, PERSISTed), wrong type, another
             owner's key
  expire_field  values whose object has an expire field holding no expire (90-109 B): bursts and MGET
             runs answer them now; replies as the parent's (which leaves them to getCommand)
  resp3      HELLO 3, then nulls and values
  reply      CLIENT REPLY SKIP / OFF / ON around GETs; MULTI GET GET EXEC
  acl        a user with key patterns: NOPERM in a GET pipeline
  slowlog    commandlog-execution-slower-than 0: one entry per burst GET, same arguments as the parent's
  reqlog     commandlog-request-larger-than / -reply-larger-than small: logged GETs as the parent's
  rename     rename-command get myget (bursts off): GET is unknown, MYGET answers
  bursts     the tree's owner_burst_commands grew; no crash; every port answers
Usage: d3-burst-check.py --bin <tree>/src/valkey-server --ref-bin <parent>/src/valkey-server --dir D
                         --port P --owner-port OP [--cpus 8-11] [--main-cpu 16]
Prints PASS/FAIL/INFO lines and BURSTCHECK {checks, fails}; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, RESULTS

ALNUM = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def reply_end(buf, i):
    """The index after one complete RESP2/3 reply at buf[i:], or -1 when it is not complete yet."""
    if i >= len(buf):
        return -1
    t = buf[i:i + 1]
    j = buf.find(b"\r\n", i)
    if j < 0:
        return -1
    if t in (b"+", b"-", b":", b"_", b",", b"#", b"("):
        return j + 2
    n = int(buf[i + 1:j])
    if t in (b"$", b"=", b"!"):
        if n < 0:
            return j + 2
        e = j + 2 + n + 2
        return e if len(buf) >= e else -1
    if t in (b"*", b"~", b">", b"%", b"|"):
        if n < 0:
            return j + 2
        k = j + 2
        for _ in range(n * 2 if t in (b"%", b"|") else n):
            k = reply_end(buf, k)
            if k < 0:
                return -1
        return k
    raise ValueError("bad reply at %d: %r" % (i, buf[i:i + 40]))


class Raw:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def send(self, data, chunks=None, rng=None):
        if not chunks:
            self.s.sendall(data)
            return
        i = 0
        while i < len(data):
            n = rng.randint(1, chunks)
            self.s.sendall(data[i:i + n])
            i += n
            time.sleep(0.0005)

    def replies(self, n):
        """The raw bytes of the next n replies."""
        out, k = [], 0
        while len(out) < n:
            e = reply_end(self.buf, k)
            if e < 0:
                d = self.s.recv(1 << 20)
                if not d:
                    raise EOFError("closed after %d of %d replies" % (len(out), n))
                self.buf += d
                continue
            out.append(self.buf[k:e])
            k = e
        self.buf = self.buf[k:]
        return out

    def close(self):
        self.s.close()


class Server:
    def __init__(self, name, binary, port, oport, cpus, main_cpu, d, extra=()):
        self.name, self.port, self.oport = name, port, oport
        self.dir = os.path.join(d, name)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        args = [binary, "--port", str(port), "--bind", "127.0.0.1", "--owner-threads", "4", "--owner-port", str(oport),
                "--owner-cpulist", cpus, "--server-cpulist", str(main_cpu), "--io-threads", "1", "--save", "",
                "--appendonly", "no", "--enable-debug-command", "yes", "--dir", self.dir,
                "--logfile", os.path.join(self.dir, "log")] + list(extra)
        self.p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(200):
            try:
                c = d3.Conn("127.0.0.1", port, timeout=5)
                if c.cmd("PING") == "PONG":
                    c.close()
                    break
            except OSError:
                time.sleep(0.05)
        self.plain = d3.Conn("127.0.0.1", port)
        self.ranges = []
        raw = self.plain.cmd("INFO", "owners").decode()
        for l in raw.split("\r\n"):
            if l.startswith("owner") and ":port=" in l and "slots=" in l:
                lo, hi = l.split("slots=")[1].split(",")[0].split("-")
                self.ranges.append((int(lo), int(hi)))

    def stats(self):
        st = d3.info(self.plain, "stats")
        cs = d3.info(self.plain, "commandstats")
        calls = 0
        if "cmdstat_get" in cs:
            calls = int(cs["cmdstat_get"].split("calls=")[1].split(",")[0])
        ow = d3.ostat(self.plain)
        return dict(hits=int(st["keyspace_hits"]), misses=int(st["keyspace_misses"]), get_calls=calls,
                    burst=ow.get("owner_burst_commands", 0))

    def stop(self):
        try:
            self.plain.cmd("SHUTDOWN", "NOSAVE")
        except Exception:
            pass
        try:
            self.p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.p.kill()

    def crashed(self):
        try:
            t = open(os.path.join(self.dir, "log"), errors="replace").read()
        except OSError:
            return 0
        return sum(t.count(w) for w in ("ASSERT", "crashed", "Guru", "BUG REPORT"))


def enc(*args):
    return d3.Conn.enc(args)


def get(k, spelling=b"GET"):
    return enc(spelling, k)


def key_in(rng, length, lo, hi, prefix=b""):
    for _ in range(100000):
        k = prefix + bytes(rng.choice(ALNUM) for _ in range(max(0, length - len(prefix))))
        if len(k) == length and lo <= d3.key_slot(k) <= hi:
            return k
    raise RuntimeError("no key of length %d in %d-%d" % (length, lo, hi))


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
    a = ap.parse_args()
    rng = random.Random(20261114 + 8)
    srvs = [Server("g8", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, a.dir),
            Server("ref", a.ref_bin, a.port + 10, a.owner_port + 20, a.cpus, a.main_cpu, a.dir)]
    g8, ref = srvs
    d3.check("burst.topology", g8.ranges == ref.ranges and len(g8.ranges) == 4, g8.ranges)
    lo0, hi0 = g8.ranges[0]
    lo1, hi1 = g8.ranges[1]

    # Data: own-slot keys of owner 0 of every length 1-70, values of the lengths the headers branch on
    vlens = [0, 1, 9, 10, 11, 99, 100, 101, 999, 1000, 16000, 16384, 20000]
    keys = [key_in(rng, L, lo0, hi0) for L in range(1, 71)]
    tagged = [key_in(rng, 12, lo0, hi0, b"{") for _ in range(4)] + [b"{t}" + key_in(rng, 6, 0, 16383)]
    tagged = [k for k in tagged if lo0 <= d3.key_slot(k) <= hi0]
    keys += tagged
    missing = [key_in(rng, L, lo0, hi0, b"miss") for L in (5, 16, 40)]
    other = [key_in(rng, 16, lo1, hi1) for _ in range(3)]
    vals = {}
    for i, k in enumerate(keys):
        if i % 7 == 3:
            vals[k] = b"%d" % rng.choice([0, 7, -7, 12345, 10**12])
        else:
            L = vlens[i % len(vlens)]
            vals[k] = bytes(rng.choice(ALNUM) for _ in range(L))
    for k in other:
        vals[k] = b"other-" + k

    def setup(cmds):
        for s in srvs:
            s.plain.send(cmds)
            for _ in cmds:
                s.plain.read()

    setup([("SET", k, v) for k, v in vals.items()])

    def case(name, data, nrep, chunks=None, prelude=None, port_idx=0, after=None, rounds=None):
        """Send data to owner port_idx of both servers; compare the n replies and the stats deltas. rounds:
        a list of (bytes, replies) sent one after another, each once the replies before it arrived (so
        each read starts at a frame: a burst can take it)."""
        outs, deltas = [], []
        for s in srvs:
            st0 = s.stats()
            r = Raw(s.oport + port_idx)
            if prelude:
                r.send(prelude[0])
                r.replies(prelude[1])
            if rounds:
                got = []
                for rb, rn in rounds:
                    r.send(rb)
                    got += r.replies(rn)
            else:
                r.send(data, chunks, random.Random(len(data)))
                got = r.replies(nrep)
            r.close()
            if after:
                after(s)
            st1 = s.stats()
            outs.append(got)
            deltas.append({k: st1[k] - st0[k] for k in ("hits", "misses", "get_calls")})
            deltas[-1]["burst"] = st1["burst"] - st0["burst"]
        same = outs[0] == outs[1]
        first = next((i for i in range(nrep) if outs[0][i] != outs[1][i]), -1) if not same else -1
        d3.check("burst.%s.replies" % name, same,
                 "%d replies, %d bytes" % (nrep, sum(map(len, outs[0]))) if same else
                 "reply %d: g8 %r ref %r" % (first, outs[0][first][:120], outs[1][first][:120]))
        plain = [{k: v for k, v in dl.items() if k != "burst"} for dl in deltas]
        d3.check("burst.%s.stats" % name, plain[0] == plain[1], "g8 %s ref %s" % (deltas[0], plain[1]))
        return deltas[0]["burst"]

    # basic: 300 own-slot frames, three spellings, hits and misses: as one write, and as rounds of 100 that
    # each start with a GET frame (each read starts at a frame: bursts take them up to the first GeT)
    frames, n = [], 0
    pool = keys + missing
    for i in range(300):
        k = pool[i % len(pool)] if i < len(pool) else rng.choice(pool)
        sp = b"GET" if i % 5 else (b"get" if i % 10 else b"GeT")
        frames.append(get(k, sp))
    frames[0] = get(pool[0])
    basic = b"".join(frames)
    case("basic", basic, 300)
    burstable = [k for k in pool if len(k) <= 64]
    rframes = []
    for j in range(3):
        for i in range(100):
            if i == 90:
                rframes.append(get(rng.choice(burstable), b"GeT"))  # the parser's from here
            elif i == 95:
                rframes.append(get(keys[66]))  # a 67-byte key
            else:
                rframes.append(get(rng.choice(burstable), b"get" if i % 7 == 1 else b"GET"))
    b_basic = case("basic_rounds", None, 300, rounds=[(b"".join(rframes[j:j + 100]), 100) for j in (0, 100, 200)])
    d3.check("burst.basic.ran", b_basic >= 200, "owner_burst_commands +%d of 300 frames" % b_basic)
    case("chunks", basic, 300, chunks=200)
    case("chunks_small", b"".join(frames[:60]), 60, chunks=7)

    # deep: rounds of 400 GETs of short keys (one read each: past the 256 GETs of one burst), then 2000 in one write
    short = [k for k in pool if len(k) <= 16]
    rounds = []
    for _ in range(5):
        rounds.append((b"".join(get(rng.choice(short), b"GET" if rng.random() < 0.8 else b"get") for _ in range(400)), 400))
    b_deep = case("deep_rounds", None, 2000, rounds=rounds)
    d3.check("burst.deep.ran", b_deep >= 1000, "owner_burst_commands +%d of 2000" % b_deep)
    deep = b"".join(get(rng.choice(pool), b"GET" if rng.random() < 0.8 else b"get") for _ in range(2000))
    case("deep", deep, 2000)

    # one GET per write, a burst of one each
    outs = []
    for s in srvs:
        r = Raw(s.oport)
        got = []
        for k in pool[:40]:
            r.send(get(k))
            got += r.replies(1)
        r.close()
        outs.append(got)
    d3.check("burst.single.replies", outs[0] == outs[1], "40 single GETs")

    # values with an expire field but no expire (embedded values with room for the field: the runs used to
    # leave them to getCommand / mgetCommand): bursts and MGET runs answer them, as the parent's replies
    nf = [key_in(rng, 16, lo0, hi0, b"nf") for _ in range(20)]
    tg = b"{nf%d}" % next(i for i in range(10000) if lo0 <= d3.key_slot(b"nf%d" % i) <= hi0)
    nft = [tg + b":%d" % j for j in range(8)]
    setup([("SET", k, b"y" * (90 + j)) for j, k in enumerate(nf + nft)])
    b_nf = case("expire_field", b"".join(get(k) for k in nf), 20)
    d3.check("burst.expire_field.ran", b_nf == 20, "owner_burst_commands +%d of 20" % b_nf)
    case("expire_field_mget", b"".join([enc("MGET", *nft), enc("MGET", *nft[:3]), get(nft[0])]), 3)

    a0, a1, a2 = keys[15], keys[20], keys[63]
    mixed = b"".join([get(a0), enc("SET", a0, "x1"), get(a0), enc("INCR", "cnt{%s}" % a0.decode()), get(a1),
                      enc("DEL", a0), get(a0), enc("MGET", a0, a1), get(a1), enc("PING"), get(a0),
                      enc("SET", a0, vals[a0]), get(a0), get(a2)])
    case("mixed", mixed, 14)

    big = key_in(rng, 16, lo0, hi0, b"big")
    big2 = key_in(rng, 16, lo0, hi0, b"big")
    setup([("SET", big, b"B" * (1 << 20)), ("SET", big2, b"C" * ((1 << 20) + 100))])
    case("big", b"".join([get(a1), get(big), get(a2), get(big2), get(a1)] * 3), 15)

    t1 = key_in(rng, 16, lo0, hi0, b"ttl")
    t2 = key_in(rng, 16, lo0, hi0, b"ttl")
    t3 = key_in(rng, 16, lo0, hi0, b"ttl")
    lst = key_in(rng, 16, lo0, hi0, b"lst")
    setup([("SET", t1, "v1", "EX", "100"), ("RPUSH", lst, "x"), ("SET", t3, "v3", "EX", "100"), ("PERSIST", t3)])
    setup([("SET", t2, "v2", "PX", "1")])
    time.sleep(0.05)
    case("ttl", b"".join([get(a1), get(t1), get(t2), get(a2), get(lst), get(a1), get(other[0]), get(a2), get(t3),
                          get(a1)]), 10)
    ex = [s.plain.cmd("EXISTS", t2) for s in srvs]
    d3.check("burst.ttl.lazy_expire", ex == [0, 0], ex)

    case("resp3", b"".join([get(missing[0]), get(a1), get(keys[3]), get(missing[1])]), 4,
         prelude=(enc("HELLO", "3"), 1))
    case("reply_skip", b"".join([enc("CLIENT", "REPLY", "SKIP"), get(a1), get(a2), get(a1)]), 2)
    case("reply_off", b"".join([enc("CLIENT", "REPLY", "OFF"), get(a1), get(a2), enc("CLIENT", "REPLY", "ON"),
                                get(a1)]), 2)
    case("multi", b"".join([enc("MULTI"), get(a1), get(a2), enc("EXEC"), get(a1)]), 5)

    ka = [key_in(rng, 16, lo0, hi0, b"ka") for _ in range(2)]
    kb = key_in(rng, 16, lo0, hi0, b"kb")
    setup([("SET", ka[0], "A0"), ("SET", ka[1], "A1"), ("SET", kb, "B"),
           ("ACL", "SETUSER", "bu", "on", ">pw", "~ka*", "+@all")])
    case("acl", b"".join([get(ka[0]), get(kb), get(ka[1])]), 3, prelude=(enc("AUTH", "bu", "pw"), 1))

    # slowlog: every burst GET logged with its own arguments
    def logs(s, kind):
        if kind == "slow":
            ents = s.plain.cmd("SLOWLOG", "GET", "100000")
        else:
            ents = s.plain.cmd("COMMANDLOG", "GET", "100000", kind)
        if isinstance(ents, d3.Err):
            return ents
        return sorted([tuple(e[3]) for e in ents if e[3] and e[3][0].lower() == b"get"])

    for s in srvs:
        s.plain.cmd("CONFIG", "SET", "slowlog-max-len", "100000")
        s.plain.cmd("SLOWLOG", "RESET")
        s.plain.cmd("CONFIG", "SET", "slowlog-log-slower-than", "0")
    slow = b"".join(get(pool[i % len(pool)], b"get" if i % 3 == 0 else b"GET") for i in range(600))
    case("slowlog", slow, 600)
    for s in srvs:
        s.plain.cmd("CONFIG", "SET", "slowlog-log-slower-than", "10000")
    L = [logs(s, "slow") for s in srvs]
    d3.check("burst.slowlog.entries", L[0] == L[1] and len(L[0]) == 600,
             "g8 %d ref %d GET entries" % (len(L[0]), len(L[1])))

    for kind, cfg, val in (("large-request", "commandlog-request-larger-than", "40"),
                           ("large-reply", "commandlog-reply-larger-than", "30")):
        for s in srvs:
            s.plain.cmd("CONFIG", "SET", "commandlog-%s-max-len" % kind, "100000")
            s.plain.cmd("COMMANDLOG", "RESET", kind)
            s.plain.cmd("CONFIG", "SET", cfg, val)
        case("reqlog_" + kind, b"".join(get(pool[i % len(pool)]) for i in range(200)), 200)
        for s in srvs:
            s.plain.cmd("CONFIG", "SET", cfg, "1mb")
        L = [logs(s, kind) for s in srvs]
        if isinstance(L[0], d3.Err):
            print("INFO burst.%s: COMMANDLOG GET unsupported: %s" % (kind, L[0]))
            d3.check("burst.%s.entries" % kind, L[0] == L[1], L)
        else:
            d3.check("burst.%s.entries" % kind, L[0] == L[1] and len(L[0]) > 0,
                     "g8 %d ref %d GET entries" % (len(L[0]), len(L[1])))

    b_end = g8.stats()["burst"]
    d3.check("burst.ran", b_end > 2000, "owner_burst_commands %d" % b_end)
    for s in srvs:
        alive = all(d3.Conn("127.0.0.1", s.oport + i).cmd("PING") == "PONG" for i in range(4))
        d3.check("burst.%s.alive" % s.name, alive and s.plain.cmd("PING") == "PONG")
        s.stop()
        d3.check("burst.%s.no_crash" % s.name, s.crashed() == 0, "crash lines %d" % s.crashed())

    # rename-command get myget: the bursts are off, GET is unknown and MYGET answers, as on the parent
    srvs[:] = [Server("g8r", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, a.dir, ["--rename-command", "get", "myget"]),
               Server("refr", a.ref_bin, a.port + 10, a.owner_port + 20, a.cpus, a.main_cpu, a.dir,
                      ["--rename-command", "get", "myget"])]
    setup([("SET", a1, "R1"), ("SET", a2, "R2")])
    case("rename", b"".join([get(a1), enc("MYGET", a1), get(a2, b"get"), enc("myget", a2), get(a1)]), 5)
    d3.check("burst.rename.off", srvs[0].stats()["burst"] == 0, "owner_burst_commands %d" % srvs[0].stats()["burst"])
    for s in srvs:
        s.stop()
        d3.check("burst.%s.no_crash" % s.name, s.crashed() == 0, "crash lines %d" % s.crashed())

    fails = [r["check"] for r in d3.RESULTS if not r["ok"]]
    print("BURSTCHECK " + json.dumps({"checks": len(d3.RESULTS), "fails": fails}))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
