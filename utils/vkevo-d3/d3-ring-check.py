#!/usr/bin/env python3
"""D3-g2 check for the owner GET ring (owner.c ownerGetRun), against a running server started with
owner-threads N and the default configs.

On every owner port, pipelines of GETs (1 to 300 deep, ring sizes prefetch-batch-max-size 16, 0, 1,
2, 128) over this test's keys: short, long (RAW) and integer strings, missing keys, a list
(WRONGTYPE), keys with a TTL (valid and expired) and some other owners' keys (handoff mid-run),
checked reply by reply. Then, through the ring: GET commandstats calls and keyspace hits/misses
match what was sent, the slow commandlog at threshold 0 holds one entry per GET, the large-reply
and large-request commandlogs catch exactly the GETs over their thresholds, a GET touches its key
(OBJECT IDLETIME) and a CLIENT NO-TOUCH client's GET does not.

Usage: d3-ring-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, RESULTS


def owner_ranges(c):
    return sorted((int(s[2][1]), int(s[0]), int(s[1])) for s in c.cmd("CLUSTER", "SLOTS"))  # (port, lo, hi)


def own_keys(prefix, lo, hi, n, pad=b""):
    out, i = [], 0
    while len(out) < n:
        k = b"%s%s:%d" % (prefix, pad, i)
        i += 1
        if lo <= d3.key_slot(k) <= hi:
            out.append(k)
    return out


def info(c, section):
    return dict(l.split(":", 1) for l in c.cmd("INFO", section).decode().splitlines() if ":" in l)


def cmdlog(plain, typ):
    return [e[3] for e in plain.cmd("COMMANDLOG", "GET", "-1", typ)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    a = ap.parse_args()
    rng = random.Random(20261114)
    plain = d3.Conn(a.host, a.port)
    ranges = owner_ranges(d3.Conn(a.host, a.owner_port))
    d3.check("ring.topology", len(ranges) == a.n, ranges[:3])
    pre = b"rg%d" % rng.randrange(10**9)
    model, keys = {}, {}
    for port, lo, hi in ranges:
        ks = own_keys(pre, lo, hi, 60)
        keys[port] = ks
        for i, k in enumerate(ks):
            kind = i % 10
            if kind < 4:
                model[k] = b"s%d" % i                                   # EMBSTR
            elif kind == 4:
                model[k] = b"L" * 120 + b"%d" % i                       # RAW
            elif kind == 5:
                model[k] = b"%d" % (1000 + i)                           # INT encoded
            elif kind == 6:
                plain.cmd("RPUSH", k, "x"); model[k] = d3.Err("WRONGTYPE")  # a list
                continue
            elif kind == 7:
                plain.cmd("SET", k, "t%d" % i, "EX", 1000); model[k] = b"t%d" % i  # valid TTL
                continue
            elif kind == 8:
                plain.cmd("SET", k, "gone", "PX", 1); model[k] = None   # expires
                continue
            else:
                model[k] = None                                         # missing
                continue
            plain.cmd("SET", k, model[k])
    time.sleep(0.05)

    def same(g, w):
        if isinstance(w, d3.Err):
            return isinstance(g, d3.Err) and g.startswith("WRONGTYPE")
        return g == w

    start_size = plain.cmd("CONFIG", "GET", "prefetch-batch-max-size")[1].decode()
    for size in ["16", "0", "1", "2", "128", start_size]:
        plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", size)
        bad = total = 0
        first = ""
        for port, _, _ in ranges:
            c = d3.Conn(a.host, port)
            other = [k for p, v in keys.items() if p != port for k in v[:10]]
            for depth, fpct in ((300, 0.0), (64, 0.0), (17, 0.0), (5, 0.0), (1, 0.0), (200, 0.05)):
                ks = [rng.choice(other) if other and rng.random() < fpct else rng.choice(keys[port]) for _ in range(depth)]
                c.send([("GET", k) for k in ks])
                for k in ks:
                    g = c.read()
                    total += 1
                    if not same(g, model[k]):
                        bad += 1
                        first = first or "port %d GET %r got %r want %r" % (port, k, g, model[k])
            c.close()
        d3.check("ring.replies_size_%s" % size, bad == 0 and total > 0, "%d/%d mismatches %s" % (bad, total, first))
    plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", start_size)

    # commandstats and keyspace hits/misses: only own-slot string hits and misses (no fallback)
    port0 = ranges[0][0]
    hitk = [k for k in keys[port0] if isinstance(model[k], bytes) and not model[k].startswith(b"t")]
    missk = [b"%s:nokey:%d" % (pre, i) for i in range(5000)]
    missk = [k for k in missk if ranges[0][1] <= d3.key_slot(k) <= ranges[0][2]][:40]
    c = d3.Conn(a.host, port0)
    seq = [rng.choice(hitk + missk) for _ in range(500)]
    plain.cmd("CONFIG", "RESETSTAT")
    c.send([("GET", k) for k in seq])
    got = [c.read() for _ in seq]
    st, cs = info(plain, "stats"), info(plain, "commandstats").get("cmdstat_get", "")
    nh = sum(1 for k in seq if k in hitk)
    d3.check("ring.replies_stats_run", all(g == (model[k] if k in hitk else None) for k, g in zip(seq, got)))
    d3.check("ring.commandstats_calls", ("calls=%d," % len(seq)) in cs, cs)
    d3.check("ring.keyspace_hits_misses", int(st["keyspace_hits"]) == nh and int(st["keyspace_misses"]) == len(seq) - nh,
             (st["keyspace_hits"], st["keyspace_misses"], nh, len(seq) - nh))

    # slow commandlog at threshold 0: one entry per GET of the run
    plain.cmd("CONFIG", "SET", "commandlog-slow-execution-max-len", 1000)
    plain.cmd("CONFIG", "SET", "commandlog-execution-slower-than", 0)
    plain.cmd("COMMANDLOG", "RESET", "slow")
    run = hitk[:50]
    c.send([("GET", k) for k in run]); [c.read() for _ in run]
    gets = [e for e in cmdlog(plain, "slow") if e and e[0].upper() == b"GET"]
    d3.check("ring.slowlog_every_get", sorted(e[1] for e in gets) == sorted(run), "%d GET entries of %d" % (len(gets), len(run)))
    plain.cmd("CONFIG", "SET", "commandlog-execution-slower-than", 10000)
    plain.cmd("CONFIG", "SET", "commandlog-slow-execution-max-len", 128)

    # large-reply (threshold 100: the RAW values only) and large-request (a 200-byte key)
    plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", 100)
    plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", 150)
    plain.cmd("COMMANDLOG", "RESET", "large-reply")
    plain.cmd("COMMANDLOG", "RESET", "large-request")
    bigk = own_keys(pre, ranges[0][1], ranges[0][2], 1, pad=b"K" * 200)[0]
    plain.cmd("SET", bigk, "b")
    run = hitk[:30] + [bigk] + hitk[30:40]
    c.send([("GET", k) for k in run])
    got = [c.read() for _ in run]
    rep = sorted(e[1] for e in cmdlog(plain, "large-reply") if e and e[0].upper() == b"GET")
    req = [e[1] for e in cmdlog(plain, "large-request") if e and e[0].upper() == b"GET"]
    want_rep = sorted(k for k in run if k != bigk and len(model[k]) > 90)
    # the log keeps the first 128 bytes of a long argument
    d3.check("ring.large_logs", got[30] == b"b" and rep == want_rep and len(req) == 1 and req[0][:120] == bigk[:120],
             "reply %d/%d request %r" % (len(rep), len(want_rep), [r[:12] for r in req]))
    plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", 1048576)
    plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", 1048576)

    # touch: a GET run resets the idle time; a CLIENT NO-TOUCH client's does not
    ta, tb = hitk[0], hitk[1]
    nt = d3.Conn(a.host, port0)
    nt.cmd("CLIENT", "NO-TOUCH", "ON")
    time.sleep(3.2)
    c.send([("GET", ta), ("GET", ta)]); [c.read() for _ in range(2)]
    nt.send([("GET", tb), ("GET", tb)]); [nt.read() for _ in range(2)]
    ia, ib = plain.cmd("OBJECT", "IDLETIME", ta), plain.cmd("OBJECT", "IDLETIME", tb)
    d3.check("ring.touch_and_no_touch", ia <= 1 and ib >= 2, (ia, ib))
    nt.close()
    c.close()
    for port, _, _ in ranges:
        d3.check("ring.port_%d_alive" % port, d3.Conn(a.host, port).cmd("PING") == "PONG")
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("D3RING pass=%d fail=%d" % (len(d3.RESULTS) - len(fails), len(fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
