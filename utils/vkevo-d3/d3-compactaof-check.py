#!/usr/bin/env python3
"""D3-all-g11-binaof gate: compact incr AOF records (aof-incr-format compact) against the parent's RESP.

Starts its own servers (owner-threads N, loopback 127.0.0.1, ports < 32768, --dir under --dir) and checks:

  cx.stream.*      a seeded random command stream (SET plain / int / empty / binary / 70 KB values and
                   EX / PX / KEEPTTL / NX / XX / GET options, DEL one and many keys, INCR / INCRBY / DECR,
                   APPEND, SETRANGE, EXPIRE / PEXPIRE / PERSIST, GETDEL, UNLINK, MSET of one tag (native)
                   and of other owners' keys (main), MULTI/EXEC on an owner and on the plain port, another
                   owner's key through an owner port, HSET / LPUSH / SADD / ZADD, SELECT 1 on the plain port)
                   run sequentially (each reply read first, so the AOF order is fixed) on --bin (compact,
                   its default) and on --ref-bin (RESP): replies equal; the compact incr file is well-formed
                   and its records decode to the ref's RESP records argv for argv (absolute expire times
                   masked); full keyspace (type, encoding, value digest, TTL present) equal; each server
                   restarted on its own AOF (a fresh process): keyspace incl. PEXPIRETIME equal to its live
                   one and DEBUG DIGEST equal; LOADAOF digest equal; INFO aof_incr_format_open
  cx.resp.*        --bin with aof-incr-format resp: incr file byte for byte the ref's (times masked)
  cx.always.*      the stream under appendfsync always: records and keyspace as above
  cx.trunc.*       the compact incr file of the stream cut at random byte offsets (and inside the header,
                   at record boundaries, inside a MULTI): aof-load-truncated yes loads exactly the whole
                   records before the cut minus an open MULTI (digest equal to a replay of those records
                   on a plain ref server), truncates the file to their end and appends after it; aof-load-
                   truncated no refuses a cut record and loads a cut on a record boundary; valkey-check-aof
                   says invalid, --fix truncates to the same offset
  cx.rewrite.*     writes on every owner while BGREWRITEAOF runs 3 times and automatic rewrites (min size
                   1 MB) fire: every incr file compact; live == LOADAOF == restart digests; check-aof ok
  cx.mixed.*       upgrade path: the ref writes a RESP AOF; --bin opens it and keeps appending RESP to that
                   file; a rewrite killed mid-way leaves RESP incr + compact incr in one manifest; more writes;
                   LOADAOF / restart digests equal live; check-aof ok; a clean rewrite then leaves one compact
                   incr; the ref (RESP-only) refuses to load a compact incr file
  cx.mm.*, cx.mmresp.*  (D3-all-g13-fold) the stream under maxmemory 1gb + appendonly everysec, where the owners
                   run SET bursts inside their memory leases (D3-all-g11-maxmem): compact records decode to the
                   ref's; with aof-incr-format resp the incr file is the ref's byte for byte; SET bursts and lease
                   windows ran, nothing evicted
  cx.mmswitch.*    (D3-all-g13-fold) aof-incr-format flipped resp / compact at 6 rewrites while 2 writers per owner
                   pipeline SET bursts inside the leases: every incr and history file has its manifest's format and
                   decodes whole, the open format follows the config at each rewrite, each key = its writer's last
                   acknowledged value, live / LOADAOF / restart digests equal, check-aof ok

Usage: d3-compactaof-check.py --bin <tree>/src/valkey-server --ref-bin <parent>/src/valkey-server --dir /tmp/x
       --n 4 --port 23379 --owner-port 23400 --cpus a-b --main-cpu m [--seed 11] [--ops 3000] [--cuts 24]
Prints PASS/FAIL lines and a final 'D3COMPACTAOF {json}' line; exit 1 on any FAIL.
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
d3 = __import__("d3-check")
import aofcompact as ac  # noqa: E402

Conn, check, info, ostat, key_slot = d3.Conn, d3.check, d3.info, d3.ostat, d3.key_slot
ARGS = None


class Server:
    def __init__(self, name, binary, extra=(), fresh=True, owners=True, start=True):
        self.dir = os.path.join(ARGS.dir, name)
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        self.bin, self.extra, self.owners = binary, list(extra), owners
        self.p = None
        if start:
            self.start()

    def cmdline(self):
        cmd = [self.bin, "--port", str(ARGS.port), "--bind", "127.0.0.1", "--save", "", "--protected-mode", "no",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", "log", "--daemonize", "no",
               "--server-cpulist", ARGS.main_cpu, "--bio-cpulist", ARGS.main_cpu, "--aof-rewrite-cpulist",
               ARGS.main_cpu, "--appendonly", "yes", "--auto-aof-rewrite-percentage", "0"]
        if self.owners:
            cmd += ["--owner-threads", str(ARGS.n), "--owner-port", str(ARGS.owner_port), "--owner-cpulist", ARGS.cpus]
        return cmd + self.extra

    def start(self, expect_fail=False):
        self.p = subprocess.Popen(self.cmdline(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(600):
            if self.p.poll() is not None:
                if expect_fail:
                    return False
                raise RuntimeError("server exited rc=%s: %s" % (self.p.returncode, self.log_tail()))
            try:
                c = Conn("127.0.0.1", ARGS.port, timeout=10)
                if c.cmd("PING") == "PONG":
                    while info(c, "persistence").get("loading") == "1":
                        time.sleep(0.05)
                    c.close()
                    return True
                c.close()
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("server did not start")

    def log_tail(self, n=6):
        try:
            return " | ".join(l.strip() for l in open(os.path.join(self.dir, "log"), errors="replace").readlines()[-n:])
        except OSError:
            return ""

    def log_has(self, s):
        try:
            return s in open(os.path.join(self.dir, "log"), errors="replace").read()
        except OSError:
            return False

    def conn(self, owner=None):
        return Conn("127.0.0.1", ARGS.port if owner is None else ARGS.owner_port + owner)

    def stop(self):
        if self.p is None or self.p.poll() is not None:
            return
        try:
            c = self.conn()
            c.send([("SHUTDOWN",)])
            c.close()
        except OSError:
            pass
        self.p.wait(timeout=120)

    def kill(self):
        if self.p and self.p.poll() is None:
            self.p.kill()
            self.p.wait()

    def crashed(self):
        try:
            return sum(1 for l in open(os.path.join(self.dir, "log"), errors="replace")
                       if "ASSERT" in l or "crashed" in l or "BUG REPORT" in l or "Guru" in l)
        except OSError:
            return -1

    def aofdir(self):
        return os.path.join(self.dir, "appendonlydir")

    def incr_files(self):
        return ac.manifest_incr(self.aofdir())

    def last_incr(self):
        name, fmt = self.incr_files()[-1]
        return os.path.join(self.aofdir(), name), fmt


def owner_ranges(c):
    return sorted(((int(s[0]), int(s[1]), int(s[2][1]) - ARGS.owner_port) for s in c.cmd("CLUSTER", "SLOTS")),
                  key=lambda r: r[2])


def owner_of(rng, key):
    s = key_slot(key)
    return next(o for lo, hi, o in rng if lo <= s <= hi)


def drain(c):
    for _ in range(1000):
        p = info(c, "persistence")
        if p.get("aof_buffer_length", "0") == "0" and p.get("aof_pending_bio_fsync", "0") == "0" and \
                p.get("aof_rewrite_in_progress") == "0" and p.get("aof_rewrite_scheduled") == "0":
            return p
        time.sleep(0.01)
    return info(c, "persistence")


# ------------------------------------------------------------------------------------------------
# The random stream
# ------------------------------------------------------------------------------------------------

def make_stream(seed, nops, rng):
    """[(where, args)]: where = owner index, 'plain' (plain port, db 0) or 'plain1' (plain port, db 1)."""
    R = random.Random(seed)
    N = ARGS.n
    tags = {}
    for o in range(N):
        tags[o], i = [], 0
        lo, hi = [(lo, hi) for lo, hi, x in rng if x == o][0]
        while len(tags[o]) < 6:
            t = b"t%d-%d" % (o, i)
            i += 1
            if lo <= key_slot(b"{%s}" % t) <= hi:
                tags[o].append(t)

    def key(o, kind=b"k"):
        return b"{%s}%s%d" % (R.choice(tags[o]), kind, R.randrange(40))

    def val():
        r = R.random()
        if r < 0.15:
            return str(R.randrange(-1000, 100000)).encode()
        if r < 0.2:
            return b""
        if r < 0.27:
            return bytes(R.randrange(256) for _ in range(R.randrange(1, 40)))  # binary incl. \r\n \xfe \x00
        if r < 0.29:
            return bytes(R.randrange(256) for _ in range(70000))
        return b"v%d-" % R.randrange(10 ** 6) + b"x" * R.randrange(0, 120)

    out = []
    for _ in range(nops):
        o = R.randrange(N)
        k = key(o)
        r = R.random()
        if r < 0.30:
            opt = R.choice([[], [], [], ["EX", R.randrange(5000, 90000)], ["PX", R.randrange(5000000, 90000000)],
                            ["KEEPTTL"], ["NX"], ["XX"], ["GET"]])
            out.append((o, ["SET", k, val()] + opt))
        elif r < 0.36:
            out.append((o, ["SET" if R.random() < 0.5 else "set", k, val()]))
        elif r < 0.42:
            out.append((o, ["DEL", k] + ([key(o)] if R.random() < 0.3 else [])))
        elif r < 0.50:
            c = b"{%s}c%d" % (tags[o][0], R.randrange(5))
            out.append((o, R.choice([["INCR", c], ["INCRBY", c, R.randrange(-50, 50)], ["DECR", c], ["incr", c]])))
        elif r < 0.55:
            out.append((o, ["APPEND", k, val()[:30]]))
        elif r < 0.58:
            out.append((o, ["SETRANGE", k, R.randrange(0, 20), b"rng"]))
        elif r < 0.64:
            out.append((o, R.choice([["EXPIRE", k, R.randrange(5000, 90000)], ["PEXPIRE", k, R.randrange(5000000, 90000000)],
                                     ["PERSIST", k]])))
        elif r < 0.67:
            out.append((o, R.choice([["GETDEL", k], ["UNLINK", k]])))
        elif r < 0.71:
            t = R.choice(tags[o])
            out.append((o, ["MSET"] + sum([[b"{%s}m%d" % (t, j), val()[:20]] for j in range(R.randrange(1, 4))], [])))
        elif r < 0.73:  # cross-owner multi-key: main
            o2 = (o + 1) % N
            out.append(("plain", ["MSET", key(o), b"x1", key(o2), b"x2"]))
            out.append(("plain", ["DEL", key(o), key(o2)]))
        elif r < 0.78:
            t = R.choice(tags[o])
            out += [(o, ["MULTI"]), (o, ["SET", b"{%s}mx" % t, val()[:40]]), (o, ["INCR", b"{%s}c0" % t]),
                    (o, ["APPEND", b"{%s}mx" % t, b"+"]), (o, ["EXEC"])]
        elif r < 0.80:
            out += [("plain", ["MULTI"]), ("plain", ["SET", key(o), b"pm"]), ("plain", ["INCR", key(o, b"pc")]),
                    ("plain", ["EXEC"])]
        elif r < 0.83:  # another owner's key through this owner's port: main runs it
            out.append((o, ["SET", key((o + 1) % N), val()]))
        elif r < 0.91:
            t = R.choice(tags[o])
            out.append((o, R.choice([["HSET", b"{%s}h" % t, b"f%d" % R.randrange(9), val()[:20]],
                                     ["LPUSH", b"{%s}l" % t, val()[:10], val()[:10]],
                                     ["SADD", b"{%s}s" % t, b"m%d" % R.randrange(30)],
                                     ["ZADD", b"{%s}z" % t, R.randrange(100), b"m%d" % R.randrange(30)],
                                     ["HINCRBY", b"{%s}h" % t, b"n", 3]])))
        else:
            out.append(("plain1", R.choice([["SET", b"db1:%d" % R.randrange(50), val()],
                                            ["INCR", b"db1:c"], ["DEL", b"db1:%d" % R.randrange(50)]])))
    return out


def run_stream(srv, stream):
    adm = srv.conn()
    adm.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)
    cs = [srv.conn(i) for i in range(ARGS.n)]
    p1 = srv.conn()
    p1.cmd("SELECT", 1)
    replies = []
    for where, args in stream:
        c = adm if where == "plain" else p1 if where == "plain1" else cs[where]
        r = c.cmd(*args)
        replies.append(repr(r))
    for c in cs + [p1]:
        c.close()
    drain(adm)
    adm.close()
    return replies


def keyspace(c, exact_ttl):
    """{(db, key): (type, encoding, value digest, ttl)} over dbs 0 and 1."""
    out = {}
    for db in (0, 1):
        c.cmd("SELECT", db)
        cur = b"0"
        keys = []
        while True:
            cur, ks = c.cmd("SCAN", cur, "COUNT", 1000)
            keys += ks
            if cur in (b"0", "0"):
                break
        for k in keys:
            c.send([("TYPE", k), ("OBJECT", "ENCODING", k), ("DEBUG", "DIGEST-VALUE", k), ("PEXPIRETIME", k)])
            t, e, dg, ex = [c.read() for _ in range(4)]
            ttl = ex if exact_ttl else (ex > 0)
            out[(db, k)] = (repr(t), repr(e), repr(dg), ttl)
    c.cmd("SELECT", 0)
    return out


def ks_diff(a, b):
    d = [k for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]
    return "%d keys" % len(a) if not d else "%d of %d/%d differ, first %r: %r / %r" % (len(d), len(a), len(b), d[0],
                                                                                      a.get(d[0]), b.get(d[0]))


def mask(recs):
    """Absolute expire times differ between two runs: mask PEXPIREAT's time and the arg after PXAT/EXAT."""
    out = []
    for r in recs:
        r = list(r)
        if r[0].upper() == b"PEXPIREAT" and len(r) >= 3:
            r[2] = b"T"
        for j in range(1, len(r)):
            if r[j - 1].upper() in (b"PXAT", b"EXAT"):
                r[j] = b"T"
        out.append(r)
    return out


def file_records(srv):
    """[(name, fmt, records)] of every incr file; each compact file must start with the magic."""
    out = []
    for name, fmt in srv.incr_files():
        data = open(os.path.join(srv.aofdir(), name), "rb").read()
        if fmt == "compact" and not ac.is_compact(data):
            raise ValueError("%s listed compact without header" % name)
        recs, _ = ac.parse_any(data)
        out.append((name, fmt, recs))
    return out


def digest(c):
    return c.cmd("DEBUG", "DIGEST")


# ------------------------------------------------------------------------------------------------
# Parts
# ------------------------------------------------------------------------------------------------

def part_stream(tag, extra, bin_extra=()):
    stream = None
    res = {}
    for side, binary, more in (("ref", ARGS.ref_bin, []), ("bin", ARGS.bin, list(bin_extra))):
        srv = Server("%s-%s" % (tag, side), binary, extra + more)
        adm = srv.conn()
        rng = owner_ranges(adm)
        if stream is None:
            stream = make_stream(ARGS.seed, ARGS.ops, rng)
        replies = run_stream(srv, stream)
        p = drain(adm)
        live = keyspace(adm, True)
        d_live = digest(adm)
        fr = file_records(srv)
        o = ostat(adm)
        evicted = int(info(adm, "stats").get("evicted_keys", 0))
        fmt_open = p.get("aof_incr_format_open")
        adm.cmd("DEBUG", "LOADAOF")
        d_load = digest(adm)
        adm.close()
        srv.stop()
        srv.start()  # a fresh process on the same AOF
        adm = srv.conn()
        d_restart = digest(adm)
        rest = keyspace(adm, True)
        adm.close()
        crash = srv.crashed()
        srv.stop()
        res[side] = dict(srv=srv, replies=replies, live=live, d=(d_live, d_load, d_restart), rest=rest, fr=fr,
                         native=o.get("owner_native_commands", 0), aof_ops=o.get("owner_aof_ops", 0), crash=crash,
                         setburst=o.get("owner_setburst_commands", 0), lease_w=o.get("owner_mm_lease_windows", 0),
                         evicted=evicted,
                         fmt_open=fmt_open, size=sum(os.path.getsize(os.path.join(srv.aofdir(), n))
                                                     for n, _ in srv.incr_files()))
    a, b = res["ref"], res["bin"]
    want_fmt = "resp" if "resp" in bin_extra else "compact"
    check("cx.%s.native" % tag, b["native"] > 0 and b["aof_ops"] > 0, "owner native %d aof_ops %d" % (b["native"], b["aof_ops"]))
    check("cx.%s.replies" % tag, a["replies"] == b["replies"], "%d replies" % len(b["replies"]))
    check("cx.%s.format" % tag, all(f == want_fmt for _, f, _ in b["fr"]) and b["fmt_open"] == want_fmt,
          "files %s open %s" % ([(n, f) for n, f, _ in b["fr"]], b["fmt_open"]))
    ra = mask(sum((r for _, _, r in a["fr"]), []))
    rb = mask(sum((r for _, _, r in b["fr"]), []))
    rd = next((j for j in range(min(len(ra), len(rb))) if ra[j] != rb[j]), None)
    check("cx.%s.records_equal" % tag, ra == rb and len(rb) > 0,
          "%d / %d records, incr bytes ref %d bin %d (%.1f%%); first diff %s %r / %r" % (
              len(ra), len(rb), a["size"], b["size"], 100.0 * b["size"] / max(1, a["size"]), rd,
              ra[rd][:4] if rd is not None and rd < len(ra) else None, rb[rd][:4] if rd is not None and rd < len(rb) else None))
    la = {k: v[:3] + (v[3] > 0,) for k, v in a["live"].items()}
    lb = {k: v[:3] + (v[3] > 0,) for k, v in b["live"].items()}
    check("cx.%s.keyspace_vs_ref" % tag, la == lb and len(la) > 0, ks_diff(la, lb))
    for side in ("ref", "bin"):
        s = res[side]
        check("cx.%s.%s.restart_keyspace" % (tag, side), s["live"] == s["rest"], ks_diff(s["live"], s["rest"]))
        check("cx.%s.%s.digests" % (tag, side), s["d"][0] == s["d"][1] == s["d"][2] and s["crash"] == 0,
              "live/loadaof/restart %s crash %d" % (s["d"], s["crash"]))
    return res, stream


def part_resp_bytes(tag="resp", extra=("--appendfsync", "everysec")):
    """--bin with aof-incr-format resp: the incr file is the ref's byte for byte (expire times masked)."""
    res, _ = part_stream(tag, list(extra), ["--aof-incr-format", "resp"])
    da = b"".join(open(os.path.join(res["ref"]["srv"].aofdir(), n), "rb").read() for n, _ in res["ref"]["srv"].incr_files())
    db = b"".join(open(os.path.join(res["bin"]["srv"].aofdir(), n), "rb").read() for n, _ in res["bin"]["srv"].incr_files())
    ra, _ = ac.parse_resp(da)
    rb, _ = ac.parse_resp(db)
    same_shape = [len(x) for x in ra] == [len(x) for x in rb] and \
        [[len(y) for y in x] for x in mask(ra)] == [[len(y) for y in x] for x in mask(rb)]
    check("cx.%s.bytes_identical" % tag, (da == db or (mask(ra) == mask(rb) and same_shape)) and not ac.is_compact(db),
          "%d / %d bytes%s" % (len(da), len(db), "" if da == db else " (equal after masking expire times)"))
    return res


MM = ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"]


def part_mm():
    """D3-all-g13-fold: the stream under maxmemory + appendonly everysec, where the owners run SET bursts inside
    their memory leases (D3-all-g11-maxmem): compact records decode to the ref's RESP records (part_stream's
    checks), and with aof-incr-format resp the incr file is the ref's byte for byte; in both, SET bursts and lease
    windows ran on --bin and nothing was evicted."""
    res, _ = part_stream("mm", ["--appendfsync", "everysec"] + MM)
    res2 = part_resp_bytes("mmresp", ["--appendfsync", "everysec"] + MM)
    for tag, r in (("mm", res), ("mmresp", res2)):
        b = r["bin"]
        check("cx.%s.leases" % tag, b["setburst"] > 0 and b["lease_w"] > 0 and b["evicted"] == 0,
              "owner_setburst_commands %d owner_mm_lease_windows %d evicted %d" % (b["setburst"], b["lease_w"], b["evicted"]))


def manifest_all(dirpath):
    """[(file name, type, 'compact'|'resp')] of every line of the AOF dir's manifest (base, history, incr)."""
    man = [f for f in os.listdir(dirpath) if f.endswith(".manifest")][0]
    out = []
    for l in open(os.path.join(dirpath, man)):
        p = l.split()
        kv = dict(zip(p[0::2], p[1::2]))
        if "file" in kv:
            out.append((kv["file"], kv.get("type"), kv.get("format", "resp")))
    return out


def part_mmswitch():
    """D3-all-g13-fold: aof-incr-format switched at each of 6 rewrites (resp, compact, ...) while 2 writers per owner
    pipeline plain 16 B SETs (SET bursts) under maxmemory 1gb + appendonly everysec, i.e. inside the owners' memory
    leases. History files are kept (aof-disable-auto-gc yes) so every incr file the run opened can be read: each one
    has the format its manifest line names (a compact one starts with the header) and decodes whole; the open format
    after each rewrite is the one configured before it; every key holds its writer's last acknowledged value; live
    == LOADAOF == restart digests; valkey-check-aof ok; SET bursts and lease windows ran; nothing evicted."""
    srv = Server("mmswitch", ARGS.bin, ["--appendfsync", "everysec", "--aof-disable-auto-gc", "yes"] + MM)
    adm = srv.conn()
    rng = owner_ranges(adm)
    o0 = ostat(adm)
    stop = threading.Event()
    nw = 2 * ARGS.n
    errs, counts, last = [], [0] * nw, [dict() for _ in range(nw)]

    def writer(w):
        o = w % ARGS.n
        try:
            c = srv.conn(o)
            tag = next(b"s%d-%d" % (w, i) for i in range(10 ** 6) if owner_of(rng, b"{s%d-%d}" % (w, i)) == o)
            j = 0
            while not stop.is_set():
                kv = []
                for _ in range(48):
                    j += 1
                    kv.append((b"{%s}k%d" % (tag, j % 3000), b"v%015d" % j))
                c.send([("SET", k, v) for k, v in kv])
                for k, v in kv:
                    r = c.read()
                    if r == "OK":
                        last[w][k] = v
                    elif len(errs) < 5:
                        errs.append((k, r))
                counts[w] += len(kv)
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    th = [threading.Thread(target=writer, args=(w,)) for w in range(nw)]
    for t in th:
        t.start()
    want, seen = ["resp", "compact", "resp", "compact", "resp", "compact"], []
    for f in want:
        time.sleep(0.4)
        adm.cmd("CONFIG", "SET", "aof-incr-format", f)
        for _ in range(400):
            p = info(adm, "persistence")
            if p.get("aof_rewrite_in_progress") == "0" and p.get("aof_rewrite_scheduled") == "0":
                break
            time.sleep(0.02)
        adm.cmd("BGREWRITEAOF")
        for _ in range(1000):
            p = info(adm, "persistence")
            if p.get("aof_rewrite_in_progress") == "0" and p.get("aof_rewrite_scheduled") == "0":
                break
            time.sleep(0.02)
        seen.append(p.get("aof_incr_format_open"))
    time.sleep(0.4)
    stop.set()
    for t in th:
        t.join()
    p = drain(adm)
    o1 = ostat(adm)
    st = info(adm, "stats")
    check("cx.mmswitch.load", not errs and sum(counts) > 0, "%d SETs, errors %s" % (sum(counts), errs[:2]))
    sb = o1.get("owner_setburst_commands", 0) - o0.get("owner_setburst_commands", 0)
    lw = o1.get("owner_mm_lease_windows", 0) - o0.get("owner_mm_lease_windows", 0)
    check("cx.mmswitch.leases", sb > sum(counts) // 2 and lw > 0 and st.get("evicted_keys") == "0",
          "owner_setburst_commands +%d of %d SETs, owner_mm_lease_windows +%d, evicted %s" % (
              sb, sum(counts), lw, st.get("evicted_keys")))
    check("cx.mmswitch.switches", seen == want and p.get("aof_last_bgrewrite_status") == "ok" and
          p.get("aof_last_write_status") == "ok" and int(p.get("aof_rewrites", 0)) >= len(want),
          "open format after each rewrite %s (want %s), rewrites %s, status %s / %s" % (
              seen, want, p.get("aof_rewrites"), p.get("aof_last_bgrewrite_status"), p.get("aof_last_write_status")))
    files, bad, nrec, fmts = [x for x in manifest_all(srv.aofdir()) if x[1] in ("h", "i") and ".base." not in x[0]], [], 0, []
    for name, typ, fmt in files:
        path = os.path.join(srv.aofdir(), name)
        if not os.path.exists(path):
            continue
        data = open(path, "rb").read()
        try:
            if (fmt == "compact") != ac.is_compact(data) and data:
                raise ValueError("manifest says %s, header says %s" % (fmt, "compact" if ac.is_compact(data) else "resp"))
            recs, _ = ac.parse_any(data)
            nrec += len(recs)
            fmts.append(fmt)
        except Exception as e:  # noqa
            bad.append((name, typ, fmt, repr(e)[:120]))
    check("cx.mmswitch.files", not bad and "compact" in fmts and "resp" in fmts and nrec > 0,
          "%d incr/history files (%s), %d records decoded, bad %s" % (len(fmts), "".join(f[0] for f in fmts), nrec, bad[:2]))
    got_bad = 0
    for w in range(nw):
        ks = list(last[w])
        for i in range(0, len(ks), 500):
            part = ks[i:i + 500]
            adm.send([("GET", k) for k in part])
            vals = [adm.read() for _ in part]
            got_bad += sum(1 for k, v in zip(part, vals) if v != last[w][k])
    check("cx.mmswitch.model", got_bad == 0 and sum(len(x) for x in last) > 0,
          "%d keys, %d not their last acknowledged value" % (sum(len(x) for x in last), got_bad))
    d_live = digest(adm)
    adm.cmd("DEBUG", "LOADAOF")
    d_load = digest(adm)
    adm.close()
    srv.stop()
    rc, out = check_aof_tool(srv.aofdir(), False)
    check("cx.mmswitch.check_aof", rc == 0, out.strip().splitlines()[-1:] if out else rc)
    srv.start()
    adm = srv.conn()
    d_restart = digest(adm)
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("cx.mmswitch.digests", d_live == d_load == d_restart and crash == 0, (d_live, d_load, d_restart, crash))


def replay_digest(recs):
    """DEBUG DIGEST after replaying records on a fresh ref server without owners or AOF."""
    srv = Server("replay", ARGS.ref_bin, ["--appendonly", "no"], owners=False)
    c = srv.conn()
    db = 0
    for r in recs:
        c.send([r])
        x = c.read()
        if r[0].upper() == b"SELECT":
            db = int(r[1])
        if isinstance(x, d3.Err) and r[0].upper() != b"EXEC":
            pass
    del db
    c.cmd("SELECT", 0)  # the records may end in another db; DBSIZE counts db 0 as the loaded server's
    dg = digest(c)
    n = c.cmd("DBSIZE")
    c.close()
    srv.stop()
    return dg, n


def complete_prefix(data, cut):
    """(records, end): the whole records in data[:cut] minus an open MULTI, and the offset after them."""
    if cut < len(ac.MAGIC):
        return [], 0
    d = data[:cut]
    recs, ends, i = [], [], len(ac.MAGIC)
    while i < cut:
        try:
            kind, p, j = ac.decode_one(d, i)
        except ac.Truncated:
            break
        if kind == "rec":
            recs.append(p)
            ends.append(j)
        i = j
    # drop an open MULTI
    open_at = None
    for idx, r in enumerate(recs):
        if r[0].upper() == b"MULTI":
            open_at = idx
        elif r[0].upper() == b"EXEC":
            open_at = None
    if open_at is not None:
        end = ends[open_at - 1] if open_at > 0 else len(ac.MAGIC)
        return recs[:open_at], end
    return recs, (ends[-1] if ends else len(ac.MAGIC))


def check_aof_tool(dirpath, fix):
    man = [f for f in os.listdir(dirpath) if f.endswith(".manifest")][0]
    cmd = [os.path.join(os.path.dirname(ARGS.bin), "valkey-check-aof")] + (["--fix"] if fix else []) + \
        [os.path.join(dirpath, man)]
    p = subprocess.run(cmd, input=b"y\n", capture_output=True, timeout=120)
    return p.returncode, p.stdout.decode(errors="replace")


def part_trunc(res):
    """Cut the compact incr file of the stream run at many offsets."""
    src = res["bin"]["srv"]
    path, fmt = src.last_incr()
    data = open(path, "rb").read()
    name = os.path.basename(path)
    R = random.Random(ARGS.seed + 1)
    # record boundaries and MULTI positions
    bounds, i, multis = [], len(ac.MAGIC), []
    while i < len(data):
        kind, p, j = ac.decode_one(data, i)
        if kind == "rec" and p[0].upper() == b"MULTI":
            multis.append((i, j))
        bounds.append(j)
        i = j
    cuts = [0, 3, 7, 8, 9] + [bounds[len(bounds) // 3], bounds[len(bounds) // 2]] + \
        [m[1] + 1 for m in multis[:2]] + [m[0] for m in multis[2:3]] + \
        sorted(R.randrange(8, len(data)) for _ in range(ARGS.cuts))
    ok_load = ok_size = ok_dg = ok_append = ok_refuse = ok_tool = 0
    n = 0
    bad = []
    for cut in cuts:
        n += 1
        recs, end = complete_prefix(data, cut)
        clean = cut == end  # the cut falls on a record boundary outside a MULTI (or the file is empty)
        exp_dg, exp_n = replay_digest(recs)
        # aof-load-truncated yes
        t = Server("trunc", ARGS.bin, ["--aof-load-truncated", "yes"], start=False)
        shutil.copytree(src.aofdir(), t.aofdir())
        with open(os.path.join(t.aofdir(), name), "r+b") as f:
            f.truncate(cut)
        t.start()
        c = t.conn()
        dg, kn = digest(c), c.cmd("DBSIZE")
        size_after = os.path.getsize(os.path.join(t.aofdir(), name))
        want_size = end if cut >= len(ac.MAGIC) else len(ac.MAGIC)  # a cut header is truncated to 0 and rewritten
        ok_load += 1
        if dg == exp_dg and kn == exp_n:
            ok_dg += 1
        else:
            bad.append(("digest", cut, end, kn, exp_n))
        if size_after == want_size:
            ok_size += 1
        else:
            bad.append(("size", cut, size_after, want_size))
        c.cmd("SET", "after-cut", cut)
        drain(c)
        d_after = digest(c)
        c.close()
        t.stop()
        t.start()
        c = t.conn()
        if digest(c) == d_after and c.cmd("GET", "after-cut") == str(cut).encode():
            ok_append += 1
        else:
            bad.append(("append", cut))
        c.close()
        t.stop()
        # aof-load-truncated no: a cut record is refused, a cut on a boundary loads
        t2 = Server("trunc-no", ARGS.bin, ["--aof-load-truncated", "no"], start=False)
        shutil.copytree(src.aofdir(), t2.aofdir())
        with open(os.path.join(t2.aofdir(), name), "r+b") as f:
            f.truncate(cut)
        started = t2.start(expect_fail=True)
        if started:
            t2.stop()
        if started == clean:
            ok_refuse += 1
        else:
            bad.append(("load-truncated-no", cut, end, started, t2.log_tail(2)))
        # valkey-check-aof on a copy: invalid unless on a boundary; --fix truncates to `end`
        t3dir = os.path.join(ARGS.dir, "trunc-tool")
        shutil.rmtree(t3dir, ignore_errors=True)
        shutil.copytree(src.aofdir(), t3dir)
        with open(os.path.join(t3dir, name), "r+b") as f:
            f.truncate(cut)
        rc1, out1 = check_aof_tool(t3dir, False)
        rc2, out2 = check_aof_tool(t3dir, True)
        fixed = os.path.getsize(os.path.join(t3dir, name))
        want_fixed = end if cut >= len(ac.MAGIC) else 0
        tool_ok = (rc1 == 0) == clean and rc2 == 0 and fixed == (cut if clean else want_fixed)
        if tool_ok:
            ok_tool += 1
        else:
            bad.append(("check-aof", cut, end, rc1, rc2, fixed, out1.strip().splitlines()[-1:] if out1 else ""))
    check("cx.trunc.loaded", ok_load == n, "%d / %d cuts loaded with aof-load-truncated yes" % (ok_load, n))
    check("cx.trunc.dataset", ok_dg == n, "%d / %d digests = replay of the whole records before the cut; %s" % (ok_dg, n, bad[:3]))
    check("cx.trunc.file_size", ok_size == n, "%d / %d files truncated to the last whole record (outside MULTI)" % (ok_size, n))
    check("cx.trunc.append_after", ok_append == n, "%d / %d appended after the cut and reloaded" % (ok_append, n))
    check("cx.trunc.load_truncated_no", ok_refuse == n, "%d / %d (cut records refused, boundaries loaded)" % (ok_refuse, n))
    check("cx.trunc.check_aof", ok_tool == n, "%d / %d valkey-check-aof verdicts and --fix offsets; %s" % (
        ok_tool, n, [b for b in bad if b[0] == "check-aof"][:2]))


def part_rewrite():
    srv = Server("rewrite", ARGS.bin, ["--appendfsync", "everysec", "--auto-aof-rewrite-percentage", "100",
                                       "--auto-aof-rewrite-min-size", "1mb"])
    adm = srv.conn()
    rng = owner_ranges(adm)
    stop = threading.Event()
    errs, counts = [], [0] * ARGS.n

    def writer(o):
        try:
            c = srv.conn(o)
            tag = next(b"w%d-%d" % (o, i) for i in range(10 ** 6) if owner_of(rng, b"{w%d-%d}" % (o, i)) == o)
            j = 0
            while not stop.is_set():
                cmds = []
                for _ in range(64):
                    j += 1
                    cmds.append(("SET", b"{%s}k%d" % (tag, j % 5000), b"v%d" % j) if j % 3 else ("INCR", b"{%s}c" % tag))
                c.send(cmds)
                for _ in cmds:
                    c.read()
                counts[o] += len(cmds)
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    th = [threading.Thread(target=writer, args=(o,)) for o in range(ARGS.n)]
    for t in th:
        t.start()
    for _ in range(3):
        time.sleep(1.0)
        for _ in range(100):
            if info(adm, "persistence").get("aof_rewrite_in_progress") == "0":
                break
            time.sleep(0.05)
        adm.cmd("BGREWRITEAOF")
    time.sleep(1.5)
    stop.set()
    for t in th:
        t.join()
    p = drain(adm)
    check("cx.rewrite.load", not errs and sum(counts) > 0, "%d writes, errors %s" % (sum(counts), errs[:2]))
    rw = int(p.get("aof_rewrites", 0))
    files = srv.incr_files()
    check("cx.rewrite.rewrites", rw >= 3 and p.get("aof_last_bgrewrite_status") == "ok",
          "aof_rewrites %d status %s" % (rw, p.get("aof_last_bgrewrite_status")))
    check("cx.rewrite.compact_files", files and all(f == "compact" for _, f in files) and
          p.get("aof_incr_format_open") == "compact", "incr files %s" % files)
    d_live = digest(adm)
    adm.cmd("DEBUG", "LOADAOF")
    d_load = digest(adm)
    adm.close()
    srv.stop()
    rc, out = check_aof_tool(srv.aofdir(), False)
    check("cx.rewrite.check_aof", rc == 0 and "valid" in out, out.strip().splitlines()[-1:] if out else rc)
    srv.start()
    adm = srv.conn()
    d_restart = digest(adm)
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("cx.rewrite.digests", d_live == d_load == d_restart and crash == 0, (d_live, d_load, d_restart, crash))


def child_pid(srv):
    out = subprocess.run(["pgrep", "-P", str(srv.p.pid)], capture_output=True, text=True).stdout.split()
    return [int(x) for x in out]


def part_mixed(stream):
    # 1. the ref (RESP-only parent) writes a RESP AOF
    ref = Server("mixed", ARGS.ref_bin, ["--appendfsync", "everysec"])
    half = len(stream) // 3
    run_stream(ref, stream[:half])
    ref.stop()
    # 2. --bin (compact default) opens it: the last incr file stays RESP
    srv = Server("mixed", ARGS.bin, ["--appendfsync", "everysec"], fresh=False)
    adm = srv.conn()
    p = info(adm, "persistence")
    check("cx.mixed.keeps_resp", p.get("aof_incr_format_open") == "resp" and all(f == "resp" for _, f in srv.incr_files()),
          "open %s files %s" % (p.get("aof_incr_format_open"), srv.incr_files()))
    run_stream(srv, stream[half:2 * half])
    # 3. a rewrite killed mid-way: its new compact incr file stays next to the RESP one
    adm.cmd("CONFIG", "SET", "rdb-key-save-delay", 100000)
    adm.cmd("BGREWRITEAOF")
    killed = 0
    for _ in range(100):
        kids = child_pid(srv)
        if kids:
            for k in kids:
                try:
                    os.kill(k, 9)
                    killed += 1
                except OSError:
                    pass
            break
        time.sleep(0.02)
    for _ in range(200):
        if info(adm, "persistence").get("aof_rewrite_in_progress") == "0":
            break
        time.sleep(0.05)
    adm.cmd("CONFIG", "SET", "rdb-key-save-delay", 0)
    files = srv.incr_files()
    check("cx.mixed.manifest", killed > 0 and len(files) >= 2 and files[0][1] == "resp" and files[-1][1] == "compact",
          "killed %d, incr files %s" % (killed, files))
    run_stream(srv, stream[2 * half:])
    drain(adm)
    d_live = digest(adm)
    live = keyspace(adm, True)
    adm.cmd("DEBUG", "LOADAOF")
    d_load = digest(adm)
    adm.close()
    srv.stop()
    rc, out = check_aof_tool(srv.aofdir(), False)
    check("cx.mixed.check_aof", rc == 0, out.strip().splitlines()[-1:] if out else rc)
    man = open(os.path.join(srv.aofdir(), [f for f in os.listdir(srv.aofdir()) if f.endswith(".manifest")][0])).read()
    srv.start()
    adm = srv.conn()
    d_restart = digest(adm)
    rest = keyspace(adm, True)
    check("cx.mixed.digests", d_live == d_load == d_restart and live == rest,
          "live/loadaof/restart %s; %s; manifest %r" % ((d_live, d_load, d_restart), ks_diff(live, rest), man[-160:]))
    # 4. a clean rewrite: one compact incr file
    adm.cmd("BGREWRITEAOF")
    p = drain(adm)
    files = srv.incr_files()
    check("cx.mixed.after_rewrite", len(files) == 1 and files[0][1] == "compact" and digest(adm) == d_live and
          p.get("aof_last_bgrewrite_status") == "ok", "incr files %s" % files)
    adm.cmd("SET", "post-rewrite", "1")
    drain(adm)
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("cx.mixed.no_crash", crash == 0, "crash lines %d" % crash)
    # 5. the RESP-only ref refuses a compact incr file (it does not misread it)
    ref2 = Server("mixed", ARGS.ref_bin, ["--appendfsync", "everysec"], fresh=False, start=False)
    started = ref2.start(expect_fail=True)
    if started:
        ref2.stop()
    check("cx.mixed.old_server_refuses", not started and ref2.log_has("Bad file format"),
          "started=%s log: %s" % (started, ref2.log_tail(3)))


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=23379)
    ap.add_argument("--owner-port", type=int, default=23400)
    ap.add_argument("--cpus", default="1-4")
    ap.add_argument("--main-cpu", default="0")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--ops", type=int, default=3000)
    ap.add_argument("--cuts", type=int, default=24)
    ap.add_argument("--parts", default="stream,resp,always,trunc,rewrite,mixed,mm,mmswitch")
    ARGS = ap.parse_args()
    os.makedirs(ARGS.dir, exist_ok=True)
    parts = ARGS.parts.split(",")
    res, stream = part_stream("stream", ["--appendfsync", "everysec"])
    if "resp" in parts:
        part_resp_bytes()
    if "always" in parts:
        part_stream("always", ["--appendfsync", "always"])
    if "trunc" in parts:
        part_trunc(res)
    if "rewrite" in parts:
        part_rewrite()
    if "mixed" in parts:
        part_mixed(stream)
    if "mm" in parts:
        part_mm()
    if "mmswitch" in parts:
        part_mmswitch()
    shutil.rmtree(ARGS.dir, ignore_errors=True)
    r = d3.RESULTS
    fails = [x["check"] for x in r if not x["ok"]]
    print("D3COMPACTAOF " + json.dumps(dict(n=ARGS.n, checks=len(r), fails=fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
