#!/usr/bin/env python3
"""D3-g1 check for the owner-side key prefetch and the one-slot-hash-per-command cache (owner.c
ownerPrefetch / ownerKeySlot), against a running server started with owner-threads N.

On every owner port, deep pipelines (longer than any prefetch batch) of SET/GET/INCR/DEL/PING on
this test's own keys, some of them in another owner's slots so the client is handed to main
mid-pipeline (the cached slots must not change what main does), are checked reply by reply
against a local model. The rounds run with prefetch-batch-max-size 16, 0, 1, 2, 128 and back to
the starting value (set on the plain port), so the per-owner batch is rebuilt at runtime.

Usage: d3-prefetch-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, ostat, RESULTS


def owner_ranges(c):
    slots = c.cmd("CLUSTER", "SLOTS")
    return sorted((int(s[2][1]), int(s[0]), int(s[1])) for s in slots)  # (port, lo, hi)


def keys_by_owner(prefix, ranges, per_owner):
    out = {port: [] for port, _, _ in ranges}
    i = 0
    while any(len(v) < per_owner for v in out.values()):
        k = b"%s:%d" % (prefix, i)
        i += 1
        s = d3.key_slot(k)
        for port, lo, hi in ranges:
            if lo <= s <= hi and len(out[port]) < per_owner:
                out[port].append(k)
    return out


def pipeline(rng, own, other, model, depth, foreign_pct):
    cmds, want = [], []
    for _ in range(depth):
        k = rng.choice(other) if other and rng.random() < foreign_pct else rng.choice(own)
        op = rng.choice(("SET", "GET", "GET", "INCR", "DEL", "PING"))
        if op == "SET":
            v = b"v%d" % rng.randrange(10**6)
            cmds.append(("SET", k, v)); want.append("OK"); model[k] = v
        elif op == "GET":
            cmds.append(("GET", k)); want.append(model.get(k))
        elif op == "INCR":
            cur = model.get(k)
            if cur is not None and not cur.lstrip(b"-").isdigit():
                cmds.append(("SET", k, b"0")); want.append("OK"); cur = b"0"
            n = int(cur or 0) + 1
            cmds.append(("INCR", k)); want.append(n); model[k] = b"%d" % n
        elif op == "DEL":
            cmds.append(("DEL", k)); want.append(1 if k in model else 0); model.pop(k, None)
        else:
            cmds.append(("PING",)); want.append("PONG")
    return cmds, want


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
    d3.check("prefetch.topology", len(ranges) == a.n, ranges[:3])
    prefix = b"pf%d" % rng.randrange(10**9)
    kb = keys_by_owner(prefix, ranges, 300)
    model = {}
    start_size = plain.cmd("CONFIG", "GET", "prefetch-batch-max-size")[1].decode()
    before = d3.ostat(plain)
    sizes = ["16", "0", "1", "2", "128", start_size]
    for size in sizes:
        d3.check("prefetch.config_set_%s" % size, plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", size) == "OK")
        bad = total = 0
        first_bad = ""
        for port, _, _ in ranges:
            own = kb[port]
            other = [k for p, v in kb.items() if p != port for k in v[:40]]
            c = d3.Conn(a.host, port)
            # own keys only (stays on the owner: the prefetch path), then 10% and 50% other owners' keys
            for depth, fpct in ((200, 0.0), (64, 0.0), (17, 0.0), (300, 0.1), (100, 0.5)):
                cmds, want = pipeline(rng, own, other, model, depth, fpct)
                c.send(cmds)
                got = [c.read() for _ in cmds]
                for i, (g, w) in enumerate(zip(got, want)):
                    total += 1
                    if g != w:
                        bad += 1
                        if not first_bad:
                            first_bad = "port %d cmd %r got %r want %r" % (port, cmds[i], g, w)
            c.close()
        d3.check("prefetch.replies_batch_%s" % size, bad == 0 and total > 0, "%d/%d mismatches %s" % (bad, total, first_bad))
    # every key of the model, read back on the plain port (main path)
    ks = list(model)
    plain.send([("GET", k) for k in ks])
    got = [plain.read() for _ in ks]
    miss = [k for k, g in zip(ks, got) if g != model[k]]
    d3.check("prefetch.model_matches_main", not miss, "%d keys, %d differ %r" % (len(ks), len(miss), miss[:2]))
    after = d3.ostat(plain)
    nat = after.get("owner_native_commands", 0) - before.get("owner_native_commands", 0)
    d3.check("prefetch.ran_native", nat > 0, "owner_native_commands +%d" % nat)
    for port, _, _ in ranges:
        d3.check("prefetch.port_%d_alive" % port, d3.Conn(a.host, port).cmd("PING") == "PONG")
    plain.cmd("CONFIG", "SET", "prefetch-batch-max-size", start_size)
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("D3PREFETCH pass=%d fail=%d" % (len(d3.RESULTS) - len(fails), len(fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
