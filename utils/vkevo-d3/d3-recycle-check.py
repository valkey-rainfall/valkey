#!/usr/bin/env python3
"""D3-all-g4-k1 check for the owner's argument recycling (networking.c: the kept name + key argvs of
parseRecycleKeyArgv / parsePopKeyArgv, and the per-length string classes of parseCreateStringObject /
parseRecycleArg), against a running server started with owner-threads N.

  reply    per owner: own-slot pipelines of 1 to 400 frames (past the 64-deep stacks) mixing GET / get,
           GeT (the generic parser), MGET of one key and of 2-9 keys, SET, INCR and PING, with key
           lengths 1 to 90 in runs and at random (the kept argvs hold one key length at a time, the
           string classes share a class by length & 3, keys over 64 B are never kept), keys that differ
           only in their last byte and values rewritten inside the pipeline. The same frames on the plain
           port (main's parser, no owner recycler) must give the same replies, reply for reply, and the
           own-slot frames must all run native on the owner.
  memory   per owner: used_memory (read on the plain port, all test connections closed) after a warm-up
           and after 20x as many more recycled GET and MGET-of-one frames grows by less than 256 KB (an
           argument object or argv leaked per command would be over 10 MB).

Usage: d3-recycle-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, ostat, info, RESULTS

ALNUM = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
MAXLEN = 90


def frame(*args):
    return d3.Conn.enc(args)


def keys_in(rng, lo, hi, length, tag, want):
    """Up to `want` distinct keys of exactly `length` bytes in slots lo..hi. Long enough keys carry the
    owner's hash tag (one slot, so MGETs of them stay native); short ones are searched raw. Siblings
    differ only in their last byte."""
    out = []
    if length >= len(tag) + 1:
        base = tag + bytes(rng.choice(ALNUM) for _ in range(length - len(tag) - 1))
        for ch in ALNUM:
            out.append(base + bytes([ch]))
            if len(out) == want:
                break
        return out
    for _ in range(200):
        base = bytes(rng.choice(ALNUM) for _ in range(length - 1))
        for ch in ALNUM:
            k = base + bytes([ch])
            if lo <= d3.key_slot(k) <= hi and k not in out:
                out.append(k)
                if len(out) == want:
                    return out
    return out


def read_n(conn, n):
    return [conn.read() for _ in range(n)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    a = ap.parse_args()
    rng = random.Random(20261114 + 4)
    plain = d3.Conn(a.host, a.port)
    ranges = sorted((int(s[2][1]), int(s[0]), int(s[1])) for s in d3.Conn(a.host, a.owner_port).cmd("CLUSTER", "SLOTS"))
    d3.check("recycle.topology", len(ranges) == a.n, ranges[:3])

    keys = {}  # port -> {length: [keys]}
    tagged = {}  # port -> keys that share the owner's tag (MGET-able)
    for port, lo, hi in ranges:
        i = 0
        while True:
            t = b"{rc%d}" % i
            if lo <= d3.key_slot(t) <= hi:
                break
            i += 1
        keys[port] = {}
        for L in range(1, MAXLEN + 1):
            ks = keys_in(rng, lo, hi, L, t, 4)
            if ks:
                keys[port][L] = ks
        tagged[port] = [k for L, ks in keys[port].items() for k in ks if k.startswith(t)]
        for L, ks in keys[port].items():
            for j, k in enumerate(ks):
                if j == 3:
                    continue  # missing
                if j == 2:
                    plain.cmd("SET", k, b"%d" % (rng.randrange(10**9)))  # INT encoded
                else:
                    plain.cmd("SET", k, k[::-1] + b"#" * rng.choice([0, 1, 30, 100]))
    d3.check("recycle.keys", all(len(keys[p]) == MAXLEN for p in keys),
             {p: len(keys[p]) for p in keys})

    def pipeline(port, nframes, mode):
        """Frames for one own-slot pipeline; every write key is SET before it is read in it and the
        counter is reset first, so both sides see the same replies."""
        allk = [k for ks in keys[port].values() for k in ks]
        wk = rng.sample(tagged[port], 3)
        cnt = tagged[port][0] + b":cnt"
        out = [frame(b"SET", cnt, b"0")] + [frame(b"SET", k, b"w%d" % j) for j, k in enumerate(wk)]
        L = rng.randint(1, MAXLEN)
        while len(out) < nframes:
            if mode == "runs" and rng.random() < 0.08:
                L = rng.randint(1, MAXLEN)  # switch the run's key length
            ks = keys[port][L] if mode == "runs" else keys[port][rng.randint(1, MAXLEN)]
            r = rng.random()
            if r < 0.50:
                out.append(frame(rng.choice([b"GET", b"get"]), rng.choice(ks)))
            elif r < 0.58:
                out.append(frame(b"GeT", rng.choice(ks)))
            elif r < 0.70:
                out.append(frame(rng.choice([b"MGET", b"mget"]), rng.choice(ks)))
            elif r < 0.80:
                out.append(frame(b"MGET", *rng.sample(tagged[port], rng.randint(2, 9))))
            elif r < 0.88:
                k = rng.choice(wk)
                out.append(frame(b"SET", k, b"v" * rng.randint(1, 40)))
                out.append(frame(b"GET", k))
            elif r < 0.95:
                out.append(frame(b"INCR", cnt))
            elif r < 0.98:
                out.append(frame(b"GET", rng.choice(wk)))
            else:
                out.append(frame(b"PING"))
            if len(allk) and rng.random() < 0.02:
                out.append(frame(b"GET", rng.choice(allk)))
        return out

    # 1. replies: owner vs plain, reply for reply; own-slot frames native
    for port, _, _ in ranges:
        bad = total = 0
        first = ""
        nat0 = d3.ostat(plain).get("owner_native_commands", 0)
        for nframes in (1, 2, 7, 63, 64, 65, 129, 200, 400, 400):
            for mode in ("runs", "random"):
                frames = pipeline(port, nframes, mode)
                data = b"".join(frames)
                got = {}
                for side, p in (("owner", port), ("plain", a.port)):
                    c = d3.Conn(a.host, p)
                    c.cmd("PING")
                    c.s.sendall(data)
                    got[side] = read_n(c, len(frames))
                    c.close()
                for i, (x, y) in enumerate(zip(got["owner"], got["plain"])):
                    total += 1
                    if x != y:
                        bad += 1
                        first = first or "%s n=%d #%d %r: owner %r plain %r" % (
                            mode, nframes, i, frames[i][:70], str(x)[:80], str(y)[:80])
        nat = d3.ostat(plain).get("owner_native_commands", 0) - nat0
        d3.check("recycle.replies_port_%d" % port, bad == 0 and total > 0, "%d/%d differ %s" % (bad, total, first))
        d3.check("recycle.native_port_%d" % port, nat >= total, "%d native for %d own-slot frames" % (nat, total))

    # 2. memory: recycled GET and MGET-of-one traffic does not grow used_memory
    def traffic(port, batches, L):
        c = d3.Conn(a.host, port)
        ks = keys[port][L]
        batch = b"".join(frame(b"GET" if j % 4 else b"MGET", ks[j % len(ks)]) for j in range(64))
        for _ in range(batches):
            c.s.sendall(batch)
            read_n(c, 64)
        c.close()

    def used():
        time.sleep(0.3)
        return int(d3.info(plain, "memory")["used_memory"])

    for port, _, _ in ranges:
        lens = [16, 16, 7, 33, 64, 65, 16]
        for L in lens:
            traffic(port, 50, L)  # warm-up: lazy allocations, the stacks fill
        m0 = used()
        for L in lens:
            traffic(port, 1000, L)
        m1 = used()
        d3.check("recycle.memory_port_%d" % port, m1 - m0 < 256 * 1024,
                 "used_memory %d -> %d (%+d B) over %d commands" % (m0, m1, m1 - m0, 64 * 1000 * len(lens)))

    for port, _, _ in ranges:
        d3.check("recycle.port_%d_alive" % port, d3.Conn(a.host, port).cmd("PING") == "PONG")
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("D3RECYCLE pass=%d fail=%d" % (len(d3.RESULTS) - len(fails), len(fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
