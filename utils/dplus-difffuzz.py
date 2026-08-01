#!/usr/bin/env python3
"""V4 differential fuzzer: ownership-ON vs ownership-OFF oracle comparison.

The ownership-OFF build (same binary, config gate off) is a reference
implementation for the claim "ownership-ON is observably identical".

Phase A (deterministic oracle): N clients, each with a seeded command stream.
  Writers touch disjoint key regions; readers touch a FROZEN preloaded region.
  Every client's reply stream must be byte-identical between the two servers.

Phase B (contended, digest oracle): readers GET keys that writers concurrently
  mutate. Reply CONTENT is timing-dependent in both modes (not compared;
  RESP validity only), but each key is written by exactly one client in a
  fixed order, so after quiescing the keyspace digest must match.

Usage: dplus-difffuzz.py --port P --seed S [--ops N] --out FILE
  Run once against each server config with the same seed, then diff the
  output files and compare the DIGEST lines.
"""
import argparse
import hashlib
import random
import socket
import threading


def resp_encode(args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class RespConn:
    """Minimal RESP2 client over a raw socket."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.buf = b""

    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def read_reply(self):
        line = self._read_line()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-", b":"):
            return line
        if t == b"$":
            n = int(rest)
            if n == -1:
                return line
            data = self._read_exact(n + 2)[:-2]
            return line + b" " + data
        if t == b"*":
            n = int(rest)
            if n == -1:
                return line
            return line + b" [" + b" | ".join(self.read_reply() for _ in range(n)) + b"]"
        raise ValueError(f"bad reply type: {line!r}")

    def pipeline(self, cmds):
        self.sock.sendall(b"".join(resp_encode(c) for c in cmds))
        return [self.read_reply() for _ in cmds]


SHARED_KEYS = 2000
REGION_KEYS = 3000


def preload(port):
    c = RespConn(port)
    cmds = []
    rng = random.Random(0xF00D)
    for i in range(SHARED_KEYS):
        val = ("v%d-" % i) * rng.randint(1, 40)  # 3B..~280B: embstr + raw
        cmds.append(["SET", "shared:%d" % i, val])
    for i in range(0, len(cmds), 500):
        c.pipeline(cmds[i : i + 500])
    return c


def writer_stream(rng, region, ops):
    for _ in range(ops):
        k = "%s:%d" % (region, rng.randrange(REGION_KEYS))
        op = rng.random()
        if op < 0.45:
            yield ["SET", k, "x" * rng.randint(1, 2048)]
        elif op < 0.6:
            yield ["APPEND", k, "y" * rng.randint(1, 64)]
        elif op < 0.75:
            yield ["INCR", k + ":ctr"]
        elif op < 0.9:
            yield ["DEL", k]
        else:
            yield ["GET", k]  # writer reading its own region: deterministic


def reader_stream(rng, ops, contended_region=None):
    for _ in range(ops):
        if contended_region and rng.random() < 0.7:
            k = "%s:%d" % (contended_region, rng.randrange(REGION_KEYS))
        else:
            k = "shared:%d" % rng.randrange(SHARED_KEYS)
        op = rng.random()
        if op < 0.7:
            yield ["GET", k]
        elif op < 0.85:
            yield ["EXISTS", k]
        else:
            yield ["STRLEN", k]


def run_client(port, stream, out_hashes, idx, compare_replies):
    conn = RespConn(port)
    h = hashlib.sha256()
    batch = []
    for cmd in stream:
        batch.append(cmd)
        if len(batch) == 20:
            for r in conn.pipeline(batch):
                if compare_replies:
                    h.update(r)
                    h.update(b"\x00")
            batch = []
    if batch:
        for r in conn.pipeline(batch):
            if compare_replies:
                h.update(r)
                h.update(b"\x00")
    out_hashes[idx] = h.hexdigest() if compare_replies else "validity-only-ok"


def campaign(port, seed, ops):
    lines = []
    preload(port)

    # Phase A: deterministic streams, byte-compared reply hashes
    streams = [
        ("A-writer-0", writer_stream(random.Random(seed + 1), "wa0", ops)),
        ("A-writer-1", writer_stream(random.Random(seed + 2), "wa1", ops)),
        ("A-reader-0", reader_stream(random.Random(seed + 3), ops)),
        ("A-reader-1", reader_stream(random.Random(seed + 4), ops)),
    ]
    hashes = [None] * len(streams)
    threads = [
        threading.Thread(target=run_client, args=(port, s, hashes, i, True))
        for i, (_, s) in enumerate(streams)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for (name, _), hx in zip(streams, hashes):
        lines.append(f"PHASE_A {name} {hx}")

    # Phase B: contended — readers target writer regions; validity only
    streams_b = [
        ("B-writer-0", writer_stream(random.Random(seed + 11), "wb0", ops), False),
        ("B-writer-1", writer_stream(random.Random(seed + 12), "wb1", ops), False),
        ("B-reader-0", reader_stream(random.Random(seed + 13), ops, "wb0"), False),
        ("B-reader-1", reader_stream(random.Random(seed + 14), ops, "wb1"), False),
    ]
    hashes_b = [None] * len(streams_b)
    threads = [
        threading.Thread(target=run_client, args=(port, s, hashes_b, i, cmp))
        for i, (_, s, cmp) in enumerate(streams_b)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lines.append("PHASE_B completed (validity-only)")

    # Quiesce, then state digest — must match across servers
    c = RespConn(port)
    (digest,) = c.pipeline([["DEBUG", "DIGEST"]])
    (dbsize,) = c.pipeline([["DBSIZE"]])
    lines.append(f"DIGEST {digest.decode(errors='replace')}")
    lines.append(f"DBSIZE {dbsize.decode(errors='replace')}")
    (ping,) = c.pipeline([["PING"]])
    lines.append(f"ALIVE {ping.decode(errors='replace')}")
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--ops", type=int, default=20000)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    lines = campaign(args.port, args.seed, args.ops)
    with open(args.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
