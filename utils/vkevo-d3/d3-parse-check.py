#!/usr/bin/env python3
"""D3-all-g2-parse check for the owner frame templates (networking.c parseOwnerFrame), against a running
server started with owner-threads N.

Differential: the same bytes go to every owner port (an owner parses them, templates on) and to the
plain port (main parses them, templates off), and the replies must be equal, reply for reply.
  - read streams: GET and MGET frames in several spellings, own-slot keys (native), other owners' keys
    (handoff), missing keys, the empty key, a 40 KB key (over PROTO_MBULK_BIG_ARG), MGET of 1 to 40 keys
    (the template takes up to 32), GET with a wrong arity, PING; cut into chunks of 1 byte to 2 KB with
    pauses, so frames arrive split anywhere. The own-slot-only streams must all run native (the owner
    parsed every frame); the mixed ones hand the client to main part way
  - malformed frames after a valid GET (bad CRLF after a GET or an MGET key, '%' for '$', a leading
    zero, a negative or non-numeric length, a broken key at the template's cap): the error and the close
  - input bytes: the large-request commandlog at a threshold equal to one GET frame's and one MGET
    frame's exact length logs the same commands on both sides (and not all of them)

Usage: d3-parse-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import argparse
import os
import random
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, RESULTS

GETS = [b"GET", b"get", b"Get"]
MGETS = [b"MGET", b"mget", b"mGeT"]


def frame(*args):
    return d3.Conn.enc(args)


def tag_in(lo, hi, base):
    i = 0
    while True:
        t = b"%s%d" % (base, i)
        if lo <= d3.key_slot(b"{%s}" % t) <= hi:
            return t
        i += 1


def send_chunks(conn, data, rng, maxchunk, pause):
    i = 0
    while i < len(data):
        n = rng.randint(1, maxchunk)
        conn.s.sendall(data[i:i + n])
        i += n
        if pause and rng.random() < 0.5:
            time.sleep(pause)


def read_n(conn, n):
    return [conn.read() for _ in range(n)]


def read_to_close(conn):
    out = []
    try:
        while True:
            out.append(conn.read())
    except (EOFError, ConnectionError, socket.timeout, OSError):
        pass
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    a = ap.parse_args()
    rng = random.Random(20261114)
    plain = d3.Conn(a.host, a.port)
    ranges = sorted((int(s[2][1]), int(s[0]), int(s[1])) for s in d3.Conn(a.host, a.owner_port).cmd("CLUSTER", "SLOTS"))
    d3.check("parse.topology", len(ranges) == a.n, ranges[:3])
    pre = b"pf%d" % rng.randrange(10**9)

    own, other = {}, {}
    for port, lo, hi in ranges:
        t = tag_in(lo, hi, pre + b"t")
        ks = [b"{%s}:k%d" % (t, j) for j in range(48)]
        for j, k in enumerate(ks):
            if j % 3 == 0:
                continue  # missing
            plain.cmd("SET", k, b"v%d" % j if j % 3 == 1 else (b"R" * 100 + b"%d" % j if j % 2 else b"%d" % (10**6 + j)))
        own[port] = ks
    for port in own:
        other[port] = [k for p, ks in own.items() if p != port for k in ks[:12]]
    big = b"{%s}" % tag_in(ranges[0][1], ranges[0][2], pre + b"b") + b"B" * 40000
    plain.cmd("SET", big, "big")
    plain.cmd("SET", b"", "empty")
    extra = [b"", big, pre + b":nokey"]

    def gen(port, nframes, mixed):
        """Own-slot frames only (every one native on the owner: it parses the whole stream), or mixed
        with other owners' keys, the empty and the 40 KB key and wrong arities (handoffs to main)."""
        out = []
        mine = own[port] + ([b"", big] if port == ranges[0][0] else [])
        for _ in range(nframes):
            r = rng.random()
            if r < 0.45 or (not mixed and r < 0.55):
                out.append(frame(rng.choice(GETS), rng.choice(mine)))
            elif r < 0.55:
                out.append(frame(rng.choice(GETS), rng.choice((other[port] or own[port]) + extra)))
            elif r < 0.87 or (not mixed and r < 0.95):
                n = rng.choice([1, 2, 3, 7, 8, 16, 31, 32, 33, 40])
                out.append(frame(rng.choice(MGETS), *[rng.choice(own[port]) for _ in range(n)]))
            elif r < 0.95:
                if rng.random() < 0.5:
                    pool = own[port] + other[port] + extra
                    out.append(frame(rng.choice(MGETS), *[rng.choice(pool) for _ in range(rng.randint(1, 10))]))
                else:
                    k = rng.choice(own[port])
                    out.append(frame(rng.choice(GETS), k, k))  # wrong arity
            else:
                out.append(frame(b"PING"))
        return out

    # 1. read streams, owner vs plain
    shapes = [("whole", 300, 1 << 30, 0), ("frames", 300, None, 0), ("2k", 300, 2048, 0.0005),
              ("64", 200, 64, 0.0002), ("1byte", 40, 1, 0.0002)]
    for port, _, _ in ranges:
        bad = total = sent_own = 0
        first = ""
        nat0 = d3.ostat(plain).get("owner_native_commands", 0)
        for mixed in (False, True):
            for name, nframes, maxchunk, pause in (shapes if not mixed else shapes[:1] + shapes[3:4]):
                frames = gen(port, nframes, mixed)
                data = b"".join(frames)
                got = {}
                for side, p in (("owner", port), ("plain", a.port)):
                    c = d3.Conn(a.host, p)
                    c.cmd("PING")
                    if maxchunk is None:  # frame by frame
                        for f in frames:
                            c.s.sendall(f)
                    else:
                        send_chunks(c, data, rng, maxchunk, pause)
                    got[side] = read_n(c, len(frames))
                    c.close()
                for i, (x, y) in enumerate(zip(got["owner"], got["plain"])):
                    total += 1
                    if x != y:
                        bad += 1
                        first = first or "%s mixed=%s #%d %r: owner %r plain %r" % (
                            name, mixed, i, frames[i][:60], str(x)[:80], str(y)[:80])
            if not mixed:
                sent_own = total
                nat = d3.ostat(plain).get("owner_native_commands", 0) - nat0
        d3.check("parse.stream_port_%d" % port, bad == 0 and total > 0, "%d/%d differ %s" % (bad, total, first))
        # each own-only stream's frames ran native (+1 PING per connection): the owner parsed them all
        d3.check("parse.native_port_%d" % port, nat >= sent_own, "%d native for %d own-slot frames" % (nat, sent_own))

    # 2. malformed frames after a valid GET: same replies, then the close
    port0 = ranges[0][0]
    k0 = own[port0][1]
    cap = b"*33\r\n$4\r\nMGET\r\n" + b"$1\r\na\r\n" * 31 + b"$1\r\nbXY"
    bad_frames = [b"*2\r\n$3\r\nGET\r\n$5\r\nabcdeXY", b"*3\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nbXY",
                  b"*3\r\n$4\r\nmget\r\n$1\r\na\r\n%1\r\nb\r\n", b"*2\r\n$3\r\nGET\r\n$01\r\na\r\n",
                  b"*2\r\n$3\r\nget\r\n$-1\r\n", b"*2\r\n$4\r\nMGET\r\n$1x\r\na\r\n", cap,
                  b"*2\r\n$3\r\nGET\r\nfoo\r\n"]
    bad = 0
    first = ""
    for i, bf in enumerate(bad_frames):
        for split in (False, True):
            data = frame(b"GET", k0) + bf
            res = {}
            for side, p in (("owner", port0), ("plain", a.port)):
                c = d3.Conn(a.host, p, timeout=5)
                c.cmd("PING")
                try:  # the server may close as soon as it sees the error
                    send_chunks(c, data, rng, 1 if split else 1 << 30, 0.0002 if split else 0)
                except OSError:
                    pass
                res[side] = read_to_close(c)
                c.close()
            ok = res["owner"] == res["plain"] and len(res["owner"]) == 2 and isinstance(res["owner"][1], d3.Err)
            if not ok:
                bad += 1
                first = first or "#%d split=%s owner %r plain %r" % (i, split, res["owner"], res["plain"])
    d3.check("parse.malformed_same_error_and_close", bad == 0, "%d/%d differ %s" % (bad, 2 * len(bad_frames), first))

    # 3. input bytes: large-request commandlog at an exact frame length
    old = plain.cmd("CONFIG", "GET", "commandlog-request-larger-than")[1].decode()
    t0 = tag_in(ranges[0][1], ranges[0][2], pre + b"r")
    gk = [b"{%s}:" % t0 + b"g" * L for L in range(56, 61)]
    for k in gk:
        plain.cmd("SET", k, "x")
    mk = [[b"{%s}:" % t0 + b"m" * L, b"{%s}:a" % t0, b"{%s}:b" % t0] for L in range(56, 61)]
    for name, cmds in (("get", [(b"GET", k) for k in gk]), ("mget", [(b"MGET",) + tuple(ks) for ks in mk])):
        thr = len(frame(*cmds[2]))
        plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", thr)
        logged = {}
        for side, p in (("owner", port0), ("plain", a.port)):
            plain.cmd("COMMANDLOG", "RESET", "large-request")
            c = d3.Conn(a.host, p)
            c.send(cmds)
            read_n(c, len(cmds))
            c.close()
            logged[side] = sorted(e[3][1] for e in plain.cmd("COMMANDLOG", "GET", "-1", "large-request")
                                  if e[3] and e[3][0].upper() in (b"GET", b"MGET"))
        o, p_ = logged["owner"], logged["plain"]
        d3.check("parse.input_bytes_%s" % name, o == p_ and 0 < len(o) < len(cmds),
                 "threshold %d: owner %d plain %d of %d logged" % (thr, len(o), len(p_), len(cmds)))
    plain.cmd("CONFIG", "SET", "commandlog-request-larger-than", old)

    for port, _, _ in ranges:
        d3.check("parse.port_%d_alive" % port, d3.Conn(a.host, port).cmd("PING") == "PONG")
    fails = [r for r in d3.RESULTS if not r["ok"]]
    print("D3PARSE pass=%d fail=%d" % (len(d3.RESULTS) - len(fails), len(fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
