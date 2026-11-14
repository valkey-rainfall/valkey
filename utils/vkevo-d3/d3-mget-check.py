#!/usr/bin/env python3
"""D3-all-g2 check for the owner MGET run (owner.c ownerMgetRun), against a running server started
with owner-threads N and the default configs.

On every owner port, pipelines of MGETs (1 to 200 deep, 1 to 20 keys each, ring sizes
prefetch-batch-max-size 16, 0, 1, 2, 128) over hash-tagged keys of that owner: short, long (RAW),
3000-byte (a reply over the run's 4 KB buffer) and integer strings, missing keys, a list (null in an
MGET), keys with a TTL (valid and expired), with other owners' MGETs, two-slot MGETs, GETs and
130-key MGETs mixed in (the run ends there), checked reply by reply. The raw reply bytes equal the
plain port's (stock mgetCommand) in RESP2 and RESP3. Then, through the run: MGET commandstats calls,
keyspace hits/misses and owner_native_commands match what was sent with no handoff, the slow
commandlog at threshold 0 holds one entry per MGET, the large-reply and large-request commandlogs
catch exactly the MGETs over their thresholds (reply and request sizes computed here), an MGET touches its keys (OBJECT IDLETIME) and a
CLIENT NO-TOUCH client's does not.

Usage: d3-mget-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, key_slot, check, RESULTS


def owner_ranges(c):
    return sorted((int(s[2][1]), int(s[0]), int(s[1])) for s in c.cmd("CLUSTER", "SLOTS"))  # (port, lo, hi)


def own_tags(prefix, lo, hi, n):
    out, i = [], 0
    while len(out) < n:
        t = b"%s%d" % (prefix, i)
        i += 1
        if lo <= d3.key_slot(b"{%s}" % t) <= hi:
            out.append(t)
    return out


def info(c, section):
    return dict(l.split(":", 1) for l in c.cmd("INFO", section).decode().splitlines() if ":" in l)


def cmdlog(plain, typ):
    return [e[3] for e in plain.cmd("COMMANDLOG", "GET", "-1", typ)]


def read_raw(f):
    """The raw bytes of one reply."""
    line = f.readline()
    if not line:
        raise EOFError("connection closed")
    t, n = line[:1], line[1:-2]
    if t in (b"$", b"=", b"!"):
        k = int(n)
        return line + (f.read(k + 2) if k >= 0 else b"")
    if t in (b"*", b">", b"~"):
        k = int(n)
        return line + b"".join(read_raw(f) for _ in range(max(k, 0)))
    if t == b"%":
        return line + b"".join(read_raw(f) for _ in range(2 * int(n)))
    return line


def resp2_len(vals):
    return len(b"*%d\r\n" % len(vals)) + sum(5 if v is None else len(b"$%d\r\n" % len(v)) + len(v) + 2 for v in vals)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    a = ap.parse_args()
    rng = random.Random(20261115)
    plain = d3.Conn(a.host, a.port)
    ranges = owner_ranges(d3.Conn(a.host, a.owner_port))
    d3.check("mget.topology", len(ranges) == a.n, ranges[:3])
    pre = b"mg%d_" % rng.randrange(10**9)
    model = {}          # key -> what an MGET returns for it
    groups = {}         # port -> [[keys of one tag], ...]
    for port, lo, hi in ranges:
        groups[port] = []
        for t in own_tags(pre, lo, hi, 4):
            ks = [b"{%s}:%d" % (t, i) for i in range(24)]
            groups[port].append(ks)
            for i, k in enumerate(ks):
                kind = i % 12
                if kind < 4:
                    model[k] = b"s%d" % i                                       # EMBSTR
                elif kind == 4:
                    model[k] = b"L" * 120 + b"%d" % i                           # RAW
                elif kind == 5:
                    model[k] = b"%d" % (1000 + i)                               # INT encoded
                elif kind == 6:
                    model[k] = b"B" * 3000                                      # over half the run buffer
                elif kind == 7:
                    plain.cmd("RPUSH", k, "x"); model[k] = None                 # a list: null in MGET
                    continue
                elif kind == 8:
                    plain.cmd("SET", k, "t%d" % i, "EX", 1000); model[k] = b"t%d" % i  # valid TTL
                    continue
                elif kind == 9:
                    plain.cmd("SET", k, "gone", "PX", 1); model[k] = None       # expires
                    continue
                else:
                    model[k] = None                                             # missing
                    continue
                plain.cmd("SET", k, model[k])
    time.sleep(0.05)

    def pipeline(port, depth, fallback):
        """MGETs for one owner port: mostly one own tag, some the run must leave to another path
        (with fallback: some that main runs, which hands the client to main mid-pipeline)."""
        others = [g for p, gs in groups.items() if p != port for g in gs]
        cmds = []
        for _ in range(depth):
            r = rng.random()
            g = rng.choice(groups[port])
            if not fallback and r < 0.08:
                r = 1.0
            if r < 0.04 and others:
                cmds.append(("MGET",) + tuple(rng.sample(rng.choice(others), rng.randint(1, 8))))  # other owner
            elif r < 0.08:
                g2 = rng.choice([x for x in groups[port] if x is not g] or [g])
                cmds.append(("MGET", rng.choice(g), rng.choice(g2)))          # two slots (or one)
            elif r < 0.11:
                cmds.append(("GET", rng.choice([k for k in g if isinstance(model[k], bytes)])))
            elif r < 0.13:
                cmds.append(("MGET",) + tuple(rng.choice(g) for _ in range(130)))  # over the ring
            else:
                cmds.append(("MGET",) + tuple(rng.choice(g) for _ in range(rng.randint(1, 20))))
        return cmds

    def want(cmd):
        return model[cmd[1]] if cmd[0] == "GET" else [model[k] for k in cmd[1:]]

    start_size = plain.cmd("CONFIG", "GET", "prefetch-batch-max-size")[1].decode()
    for size in ["16", "0", "1", "2", "128", start_size]:
        plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", size)
        bad = total = 0
        first = ""
        for port, _, _ in ranges:
            c = d3.Conn(a.host, port)
            for depth, fb in ((200, 0), (200, 1), (64, 0), (64, 1), (17, 0), (5, 1), (1, 0)):
                cmds = pipeline(port, depth, fb)
                c.send(cmds)
                for cmd in cmds:
                    g = c.read()
                    total += 1
                    if g != want(cmd):
                        bad += 1
                        first = first or "port %d %r got %r want %r" % (port, cmd[:4], g, want(cmd))
            c.close()
        d3.check("mget.replies_size_%s" % size, bad == 0 and total > 0, "%d/%d mismatches %s" % (bad, total, first))
    plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", start_size)

    # raw bytes == the plain port's (stock mgetCommand), RESP2 and RESP3
    for proto in (2, 3):
        diff = sent = 0
        o0 = d3.ostat(plain)
        for port, _, _ in ranges:
            oc, pc = d3.Conn(a.host, port), d3.Conn(a.host, a.port)
            for x in (oc, pc):
                x.send([("HELLO", proto)]); read_raw(x.f)
            time.sleep(0.1)  # HELLO ran on main: let the client go back to its owner
            cmds = pipeline(port, 120, 0)
            oc.send(cmds); pc.send(cmds)
            ro = b"".join(read_raw(oc.f) for _ in cmds)
            rp = b"".join(read_raw(pc.f) for _ in cmds)
            diff += ro != rp
            sent += len(cmds)
            oc.close(); pc.close()
        dn = d3.ostat(plain)["owner_native_commands"] - o0["owner_native_commands"]
        d3.check("mget.raw_bytes_resp%d" % proto, diff == 0 and dn >= sent // 2,
                 "%d ports differ, native +%d for %d sent on owner ports" % (diff, dn, sent))

    # commandstats, keyspace hits/misses, all native: own-tag MGETs of strings without a TTL and missing keys
    port0 = ranges[0][0]
    g0 = groups[port0][0]
    simple = [k for k in g0 if model[k] is None and not plain.cmd("EXISTS", k)] + \
             [k for k in g0 if isinstance(model[k], bytes) and not model[k].startswith((b"t", b"B"))]
    c = d3.Conn(a.host, port0)
    c.cmd("PING")
    seq = [("MGET",) + tuple(rng.choice(simple) for _ in range(rng.randint(1, 12))) for _ in range(300)]
    plain.cmd("CONFIG", "RESETSTAT")
    o0 = d3.ostat(plain)
    c.send(seq)
    got = [c.read() for _ in seq]
    o1 = d3.ostat(plain)
    st, cs = info(plain, "stats"), info(plain, "commandstats").get("cmdstat_mget", "")
    nh = sum(1 for cmd in seq for k in cmd[1:] if model[k] is not None)
    nk = sum(len(cmd) - 1 for cmd in seq)
    d3.check("mget.replies_stats_run", all(g == want(cmd) for cmd, g in zip(seq, got)))
    d3.check("mget.commandstats_calls", ("calls=%d," % len(seq)) in cs, cs)
    d3.check("mget.keyspace_hits_misses", int(st["keyspace_hits"]) == nh and int(st["keyspace_misses"]) == nk - nh,
             (st["keyspace_hits"], st["keyspace_misses"], nh, nk - nh))
    dn = o1["owner_native_commands"] - o0["owner_native_commands"]
    dh = o1["owner_handoffs_to_main"] - o0["owner_handoffs_to_main"]
    d3.check("mget.native_no_handoff", dn >= len(seq) and dh == 0, "native +%d handoffs +%d for %d" % (dn, dh, len(seq)))

    # slow commandlog at threshold 0: one entry per MGET of the run
    plain.cmd("CONFIG", "SET", "commandlog-slow-execution-max-len", 1000)
    plain.cmd("CONFIG", "SET", "commandlog-execution-slower-than", 0)
    plain.cmd("COMMANDLOG", "RESET", "slow")
    run = seq[:40]
    c.send(run); [c.read() for _ in run]
    ents = [e for e in cmdlog(plain, "slow") if e and e[0].upper() == b"MGET"]
    d3.check("mget.slowlog_every_mget", sorted(tuple(e[1:]) for e in ents) == sorted(tuple(k for k in cmd[1:]) for cmd in run),
             "%d MGET entries of %d" % (len(ents), len(run)))
    plain.cmd("CONFIG", "SET", "commandlog-execution-slower-than", 10000)
    plain.cmd("CONFIG", "SET", "commandlog-slow-execution-max-len", 128)

    # large-reply (threshold 300 bytes) and large-request (a 200-byte key)
    plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", 200)
    plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", 150)
    plain.cmd("COMMANDLOG", "RESET", "large-reply")
    plain.cmd("COMMANDLOG", "RESET", "large-request")
    bigk = b"{%s}:%s" % (g0[0][1:g0[0].index(b"}")], b"K" * 200)
    plain.cmd("SET", bigk, "b")
    model[bigk] = b"b"
    raw = [k for k in g0 if isinstance(model[k], bytes) and model[k].startswith(b"L")]
    # every other MGET holds two RAW values (a reply over 200 bytes), the rest only short ones
    run = [("MGET",) + tuple(rng.sample(raw, 2) * (i % 2) + [rng.choice(simple) for _ in range(rng.randint(1, 4))])
           for i in range(30)]
    run = run[:15] + [("MGET", bigk, simple[0])] + run[15:]
    c.send(run)
    got = [c.read() for _ in run]
    rep = sorted(tuple(e[1:]) for e in cmdlog(plain, "large-reply") if e and e[0].upper() == b"MGET")
    req = [e[1] for e in cmdlog(plain, "large-request") if e and e[0].upper() == b"MGET"]
    want_rep = sorted(tuple(cmd[1:]) for cmd in run if cmd[1] != bigk and resp2_len(want(cmd)) >= 200)
    want_req = [cmd for cmd in run if len(d3.Conn.enc(cmd)) >= 150]
    # the log keeps the first 128 bytes of a long argument
    d3.check("mget.large_logs", all(g == want(cmd) for cmd, g in zip(run, got)) and rep == want_rep and len(want_rep) > 0
             and len(req) == len(want_req) and sum(1 for r in req if r[:120] == bigk[:120]) == 1,
             "reply %d/%d request %d/%d" % (len(rep), len(want_rep), len(req), len(want_req)))
    plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", 1048576)
    plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", 1048576)

    # touch: an MGET run resets the idle time of its keys; a CLIENT NO-TOUCH client's does not
    hits = [k for k in simple if model[k] is not None]
    ta, tb = hits[0], hits[1]
    nt = d3.Conn(a.host, port0)
    nt.cmd("CLIENT", "NO-TOUCH", "ON")
    time.sleep(3.2)
    c.send([("MGET", ta, ta), ("MGET", simple[0], ta)]); [c.read() for _ in range(2)]
    nt.send([("MGET", tb, tb), ("MGET", tb)]); [nt.read() for _ in range(2)]
    ia, ib = plain.cmd("OBJECT", "IDLETIME", ta), plain.cmd("OBJECT", "IDLETIME", tb)
    d3.check("mget.touch_and_no_touch", ia <= 1 and ib >= 2, (ia, ib))
    nt.close()
    c.close()
    for port, _, _ in ranges:
        d3.check("mget.port_%d_alive" % port, d3.Conn(a.host, port).cmd("PING") == "PONG")
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("D3MGET pass=%d fail=%d" % (len(d3.RESULTS) - len(fails), len(fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
