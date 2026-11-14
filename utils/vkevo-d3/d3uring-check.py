#!/usr/bin/env python3
"""D3-all-g8-iouring: the owner io_uring read/write path against a running server (owner-threads N,
owner-uring yes). Every check goes through the owner ports with keys each port's owner owns, so the
batched reads and writes are the ones under test; each runs three ways: [all] owner-uring yes with
owner-uring-deep 0 (every read batched), [yes] the server's owner-uring-deep (deep pipelines on the syscall
path, clients moving between the two), [no] owner-uring no.

  path          owner_uring_reads / owner_uring_writes grow while native commands run (the path is used)
  order         concurrent connections, random pipelines of SET/GET/INCR/MGET/DEL/PING split at random byte
                offsets (mid-command, mid-argument) across several sends: every reply in order and right
  backpressure  a client with a 4 KB receive buffer sends 60 k pipelined GETs and reads nothing for 1 s
                (1 KB values: writev replies; 100 B values, 20 GETs per send, 2.5 s: io_uring sends, which must come
                back short against the full socket: partial writes, EAGAIN, EPOLLOUT), then reads all
                replies: count, order and values
  bigarg        SET of 1 MB and 8 MB values sent in 64 KB pieces with pauses (the big-argument query buffer
                read path), then GET: the same bytes
  aborts        200 connections send a pipeline and close at once (half with RST): the server keeps
                serving, every port answers, resident owner clients drop back
  toggle        CONFIG SET owner-uring (and owner-uring-deep 0 / default) flips every 20 ms while 8 connections run pipelines: every reply right
  no_crash      same pid, every port answers PING

Usage: d3uring-check.py --port 6379 --owner-port 6400 --n 4. PASS/FAIL lines, a final URINGCHECK {json};
exit 1 on any FAIL.
"""
import argparse
import json
import random
import socket
import struct
import sys
import threading
import time


def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}", s + 1)
        if e > s + 1:
            key = key[s + 1:e]
    return crc16(key) & 16383


def enc(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Reader:
    def __init__(self, s):
        self.s = s
        self.f = s.makefile("rb", buffering=1 << 20)

    def close(self):
        # the makefile object keeps the socket's fd open past socket.close(): close both
        self.f.close()
        self.s.close()

    def read(self):
        line = self.f.readline()
        if not line:
            raise EOFError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t in (b"+", b"-", b":"):
            return (t, rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return (b"$", None)
            d = self.f.read(n + 2)
            return (b"$", d[:-2])
        if t == b"*":
            n = int(rest)
            return (b"*", None if n < 0 else [self.read() for _ in range(n)])
        raise ValueError("bad reply %r" % line)


def conn(port, rcvbuf=None, timeout=60):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if rcvbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.settimeout(timeout)
    s.connect(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def cmd(port, *args):
    s = conn(port)
    s.sendall(enc(*args))
    rd = Reader(s)
    r = rd.read()
    rd.close()
    return r


def info_owners(port):
    t, v = cmd(port, "INFO", "owners")
    out = {}
    for line in v.decode().splitlines():
        if ":" in line:
            k, x = line.split(":", 1)
            out[k] = int(x) if x.lstrip("-").isdigit() else x
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    random.seed(a.seed)
    fails = []
    checks = 0

    def res(name, ok, detail=""):
        nonlocal checks
        checks += 1
        print(("PASS" if ok else "FAIL"), name, detail, flush=True)
        if not ok:
            fails.append(name)

    t, slots = cmd(a.port, "CLUSTER", "SLOTS")
    ranges = []
    for ent in slots:
        lo, hi = ent[1][0][1], ent[1][1][1]
        port = int(ent[1][2][1][1][1])
        ranges.append((int(lo), int(hi), port))
    ports = sorted(set(p for _, _, p in ranges))

    def owner_port(slot):
        for lo, hi, p in ranges:
            if lo <= slot <= hi:
                return p
        raise KeyError(slot)

    keys = {p: [] for p in ports}
    tags = {p: None for p in ports}
    i = 0
    while min(len(v) for v in keys.values()) < 64 or any(v is None for v in tags.values()):
        k = b"u:%d" % i
        p = owner_port(key_slot(k))
        if len(keys[p]) < 64:
            keys[p].append(k)
        tg = b"{t%d}" % i
        pt = owner_port(key_slot(tg))
        if tags[pt] is None:
            tags[pt] = tg
        i += 1
    pid0 = cmd(a.port, "INFO", "server")[1].decode()
    pid0 = [x for x in pid0.splitlines() if x.startswith("process_id:")][0]

    def pipeline_worker(port, kk, tag, rounds, errs, seed):
        rnd = random.Random(seed)
        s = conn(port)
        r = Reader(s)
        model = {}
        for rnd_i in range(rounds):
            buf, exp = [], []
            for _ in range(rnd.randint(1, 300)):
                op = rnd.random()
                k = rnd.choice(kk)
                if op < 0.3:
                    v = b"v%d-%s" % (rnd_i, b"x" * rnd.randint(0, 200))
                    buf.append(enc("SET", k, v)); exp.append((b"+", b"OK")); model[k] = v
                elif op < 0.6:
                    buf.append(enc("GET", k)); exp.append((b"$", model.get(k)))
                elif op < 0.7:
                    c = tag + b":c"
                    n = int(model.get(c, b"0")) + 1
                    buf.append(enc("INCR", c)); exp.append((b":", b"%d" % n)); model[c] = b"%d" % n
                elif op < 0.8:
                    ks = [tag + b":%d" % j for j in range(8)]
                    buf.append(enc("MGET", *ks)); exp.append((b"*", [(b"$", model.get(x)) for x in ks]))
                elif op < 0.85:
                    ks = [tag + b":%d" % j for j in range(8)]
                    vals = [b"m%d" % rnd.randint(0, 1 << 30) for _ in ks]
                    args = []
                    for x, v in zip(ks, vals):
                        args += [x, v]; model[x] = v
                    buf.append(enc("MSET", *args)); exp.append((b"+", b"OK"))
                elif op < 0.92:
                    buf.append(enc("DEL", k)); exp.append((b":", b"1" if k in model else b"0")); model.pop(k, None)
                else:
                    buf.append(enc("PING")); exp.append((b"+", b"PONG"))
            data = b"".join(buf)
            # split at random byte offsets, with tiny pauses, so commands and arguments straddle reads
            pos = 0
            while pos < len(data):
                step = rnd.choice([1, 3, 17, 512, 4096, 65536])
                s.sendall(data[pos:pos + step])
                pos += step
                if rnd.random() < 0.05:
                    time.sleep(0.001)
            for e in exp:
                g = r.read()
                if g != e:
                    errs.append("port %d round %d: got %r want %r" % (port, rnd_i, g, e))
                    r.close()
                    return
        r.close()

    # per-worker hash tags that land on each port's owner (MGET/MSET/INCR stay native)
    wtags = {}
    for p in ports:
        wtags[p] = []
    i = 0
    while min(len(v) for v in wtags.values()) < 8:
        tg = b"{w%d}" % i
        pt = owner_port(key_slot(tg))
        if len(wtags[pt]) < 8:
            wtags[pt].append(tg)
        i += 1

    def run_order2(rounds=30, per_port=3):
        errs = []
        th = []
        for p in ports:
            for j in range(per_port):
                kk = keys[p][j * 16:(j + 1) * 16]
                th.append(threading.Thread(target=pipeline_worker,
                                           args=(p, kk, wtags[p][j], rounds, errs, random.random())))
        for t_ in th:
            t_.start()
        for t_ in th:
            t_.join()
        return errs

    def reset():
        for p in ports:
            for k in keys[p]:
                cmd(p, "DEL", k)
            for tg in wtags[p]:
                cmd(p, "DEL", tg + b":c", *[tg + b":%d" % j for j in range(8)])

    def clients_detail():
        t, v = cmd(a.port, "CLIENT", "LIST")
        lines = [x for x in v.decode(errors="replace").splitlines() if "cmd=client|list" not in x]
        return " | ".join(x[:220] for x in lines[:4])

    t, deep0 = cmd(a.port, "CONFIG", "GET", "owner-uring-deep")
    deep0 = deep0[1][1] if deep0 and len(deep0) > 1 else b"32"
    # all: owner-uring yes and owner-uring-deep 0 (every read batched); yes: the server's own owner-uring-deep
    # (deep pipelines take the syscall path, clients move between the two); no: the syscall path only
    for mode in ("all", "yes", "no"):
        cmd(a.port, "CONFIG", "SET", "owner-uring", "no" if mode == "no" else "yes")
        cmd(a.port, "CONFIG", "SET", "owner-uring-deep", b"0" if mode == "all" else deep0)
        reset()
        b = info_owners(a.port)
        errs = run_order2()
        aft = info_owners(a.port)
        res("order[%s]" % mode, not errs, "; ".join(errs[:3]) or "%d conns x 30 rounds" % (len(ports) * 3))
        dr = aft.get("owner_uring_reads", 0) - b.get("owner_uring_reads", 0)
        dw = aft.get("owner_uring_writes", 0) - b.get("owner_uring_writes", 0)
        dn = aft["owner_native_commands"] - b["owner_native_commands"]
        if mode != "no":
            res("path[%s]" % mode, aft.get("owner_uring_rings") == a.n and dr > 0 and dw > 0 and dn > 0,
                "rings %s reads +%d writes +%d native +%d" % (aft.get("owner_uring_rings"), dr, dw, dn))
        else:
            res("path[no]", dr == 0 and dw == 0 and dn > 0, "reads +%d writes +%d native +%d" % (dr, dw, dn))

        # backpressure: fill the owner's socket send buffer. 1 KB values: replies with value references
        # (writev path); 100 B values sent 100 GETs at a time: replies are plain c->buf bytes (io_uring
        # sends), which meet the full socket (short sends, EAGAIN, then EPOLLOUT).
        p = ports[0]
        for vsize, per in ((1024, 1000), (100, 20)):
            big = b"B" * vsize
            k = keys[p][-1]
            cmd(p, "SET", k, big)
            s = conn(p, rcvbuf=4096, timeout=120)
            r = Reader(s)
            n = 60000
            chunk = enc("GET", k) * per
            sender_err = []

            def sender():
                try:
                    for i_ in range(n // per):
                        s.sendall(chunk)
                        if per < 1000:
                            time.sleep(0.0005)  # one read per send: each read's replies fit c->buf
                except Exception as e:  # noqa: BLE001
                    sender_err.append(repr(e))

            st = threading.Thread(target=sender)
            b = info_owners(a.port)
            st.start()
            time.sleep(1.0 if per == 1000 else 2.5)
            bad = 0
            got = 0
            try:
                for _ in range(n):
                    g = r.read()
                    got += 1
                    if g != (b"$", big):
                        bad += 1
            except Exception as e:  # noqa: BLE001
                sender_err.append("read: %r" % e)
            st.join()
            r.close()
            aft = info_owners(a.port)
            dws = aft.get("owner_uring_short_writes", 0) - b.get("owner_uring_short_writes", 0)
            dwu = aft.get("owner_uring_writes", 0) - b.get("owner_uring_writes", 0)
            ok = got == n and bad == 0 and not sender_err
            if mode == "all" and vsize == 100:
                ok = ok and dwu > 0 and dws > 0  # the io_uring sends met the full socket
            res("backpressure[%s,%dB]" % (mode, vsize), ok,
                "replies %d/%d bad %d %s io_uring sends +%d short +%d" % (got, n, bad, ";".join(sender_err), dwu, dws))

        # big arguments in pieces
        ok = True
        detail = []
        for size in (1 << 20, 8 << 20):
            v = bytes(random.getrandbits(8) for _ in range(4096)) * (size // 4096)
            k = keys[p][-2]
            s = conn(p, timeout=120)
            r = Reader(s)
            data = enc("SET", k, v) + enc("GET", k) + enc("STRLEN", k)
            for off in range(0, len(data), 65536):
                s.sendall(data[off:off + 65536])
                if off % (1 << 20) == 0:
                    time.sleep(0.01)
            r1, r2, r3 = r.read(), r.read(), r.read()
            r.close()
            good = r1 == (b"+", b"OK") and r2 == (b"$", v) and r3 == (b":", b"%d" % size)
            ok &= good
            detail.append("%d B %s" % (size, "ok" if good else "BAD"))
        res("bigarg[%s]" % mode, ok, ", ".join(detail))

        # abrupt closes mid-pipeline
        for j in range(200):
            p2 = ports[j % len(ports)]
            s = conn(p2)
            s.sendall(enc("SET", keys[p2][j % 64], b"z" * 100) * 50 + enc("GET", keys[p2][0]) * 200)
            if j % 2:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.close()
        time.sleep(0.5)
        alive = all(cmd(pp, "PING") == (b"+", b"PONG") for pp in ports)
        resident = None
        for _ in range(50):
            resident = info_owners(a.port).get("owner_resident_clients")
            if resident == 0:
                break
            time.sleep(0.1)
        res("aborts[%s]" % mode, alive and resident == 0,
            "ports alive %s, resident owner clients after %s%s" % (alive, resident,
                                                                   "" if resident == 0 else ": " + clients_detail()))

    # runtime toggle under load
    cmd(a.port, "CONFIG", "SET", "owner-uring", "yes")
    stop = [False]
    errs = []

    def toggler():
        m = 0
        while not stop[0]:
            cmd(a.port, "CONFIG", "SET", "owner-uring", "no" if m % 2 == 0 else "yes")
            cmd(a.port, "CONFIG", "SET", "owner-uring-deep", b"0" if m % 4 < 2 else deep0)
            m += 1
            time.sleep(0.02)

    tt = threading.Thread(target=toggler)
    tt.start()
    for _ in range(2):
        reset()
        errs += run_order2(rounds=20, per_port=2)
    stop[0] = True
    tt.join()
    cmd(a.port, "CONFIG", "SET", "owner-uring", "yes")
    cmd(a.port, "CONFIG", "SET", "owner-uring-deep", deep0)
    res("toggle", not errs, "; ".join(errs[:3]) or "ok")

    pid1 = cmd(a.port, "INFO", "server")[1].decode()
    pid1 = [x for x in pid1.splitlines() if x.startswith("process_id:")][0]
    alive = all(cmd(pp, "PING") == (b"+", b"PONG") for pp in ports) and cmd(a.port, "PING") == (b"+", b"PONG")
    res("no_crash", pid0 == pid1 and alive, "%s -> %s" % (pid0, pid1))
    print("URINGCHECK " + json.dumps({"checks": checks, "fails": fails}))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
