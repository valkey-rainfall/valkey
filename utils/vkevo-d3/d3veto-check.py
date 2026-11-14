#!/usr/bin/env python3
"""D3veto gate: native owner commands with appendonly yes and with maxmemory set.

Starts its own servers (owner-threads N, loopback) from --bin, in --dir, and checks:

  aof.*   (appendonly yes, appendfsync always, then everysec)
    native            owner_veto is none and owner ports run GET/SET/INCR natively
    reply_after_write with DEBUG AOF-FLUSH-SLEEP S, a native write's reply (and a GET pipelined behind
                      it) takes >= S: it left only after main's AOF write, under always and everysec
    aoftest           round-1 discipline: per owner, a writer INCRs its counter natively while a reader
                      GETs it natively (plus INCRs of one counter from the plain port and another owner's
                      port); every INCR and GET reply value v is backed by >= v INCR records in the AOF
                      file when the reply arrives
    replay            mixed load (native SET/INCR/SET EX/DEL/lazy expiry, SELECT 1 on an owner port,
                      INCRBY/MSET/LPUSH on the plain port, other owners' keys via handoff, a
                      BGREWRITEAOF in the middle): the dataset equals a client-side model; DEBUG LOADAOF
                      and a restart from the AOF give the same DEBUG DIGEST and the model again
    lazy_expire_record a native GET of an expired key writes its DEL/UNLINK to the AOF before the reply
    waitaof           native SET then WAITAOF 1 0 on an owner port answers [1, 0]
  mm.*    (maxmemory)
    native            owner_veto is none, native SETs under maxmemory with room
    accounting        used_memory growth of 100k keys loaded natively matches the same load on the plain
                      port within 3%, and FLUSHALL brings it back to the baseline (no leak)
    evict             allkeys-lru, 64 MB, 160 MB of unique writes on all owner ports: sampled
                      used_memory <= maxmemory + 2 MB, keys evicted, dbsize + evicted == acked writes,
                      every present key holds its value (a stock-path server's peak is shown too)
    noeviction        40 MB noeviction: OOM refusals happen, every acked key present with its value,
                      every refused key absent, GET still answered on owner ports
    lowered           CONFIG SET maxmemory below used: native writes go to main, which evicts below it

Usage: d3veto-check.py --bin src/valkey-server --dir /tmp/x --n 4 --port 26379 --owner-port 26400
                       --cpus 48-51 --main-cpu 56 [--only aof|mm]
Prints PASS/FAIL lines and a final 'D3VETO {json}' line; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")
Conn, Err, key_slot, check, info, ostat = d3.Conn, d3.Err, d3.key_slot, d3.check, d3.info, d3.ostat

ARGS = None


class Server:
    def __init__(self, name, extra, owners=True):
        self.dir = os.path.join(ARGS.dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self.extra = list(extra)
        self.owners = owners
        self.start()

    def start(self):
        cmd = [ARGS.bin, "--port", str(ARGS.port), "--bind", "127.0.0.1", "--save", "",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", "log", "--daemonize", "no",
               "--server-cpulist", ARGS.main_cpu, "--bio-cpulist", ARGS.main_cpu]
        if self.owners:
            cmd += ["--owner-threads", str(ARGS.n), "--owner-port", str(ARGS.owner_port), "--owner-cpulist", ARGS.cpus]
        self.p = subprocess.Popen(cmd + self.extra, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(300):
            try:
                c = Conn("127.0.0.1", ARGS.port)
                if c.cmd("PING") == "PONG":
                    while info(c, "persistence").get("loading") == "1":
                        time.sleep(0.05)
                    c.close()
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("server did not start: " + " ".join(cmd + self.extra))

    def conn(self, owner=None):
        return Conn("127.0.0.1", ARGS.port if owner is None else ARGS.owner_port + owner)

    def stop(self):
        try:
            c = self.conn()
            c.send([("SHUTDOWN",)])
            c.close()
        except OSError:
            pass
        self.p.wait(timeout=60)

    def crashed(self):
        try:
            return sum(1 for l in open(os.path.join(self.dir, "log"), errors="replace")
                       if "ASSERT" in l or "crashed" in l or "BUG REPORT" in l or "Guru" in l)
        except OSError:
            return -1


def owner_ranges(c):
    out = []
    for s in c.cmd("CLUSTER", "SLOTS"):
        out.append((int(s[0]), int(s[1]), int(s[2][1]) - ARGS.owner_port))
    return sorted(out, key=lambda r: r[2])


_CRC = []
for _b in range(256):
    _c = _b << 8
    for _ in range(8):
        _c = ((_c << 1) ^ 0x1021) if _c & 0x8000 else (_c << 1)
    _CRC.append(_c & 0xFFFF)


def slot_of(tag):
    crc = 0
    for b in tag:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC[((crc >> 8) ^ b) & 0xFF]
    return crc & 16383


_TAGS = {}


def owner_tags(ranges, owner):
    """32 hash tags whose slots the owner owns (spread over several of its keyspace tables)."""
    if owner not in _TAGS:
        lo, hi = [(lo, hi) for lo, hi, o in ranges if o == owner][0]
        tags, i = [], 0
        while len(tags) < 32:
            t = b"o%d-%d" % (owner, i)
            i += 1
            if lo <= slot_of(t) <= hi:
                assert lo <= key_slot(b"{%s}" % t) <= hi
                tags.append(t)
        _TAGS[owner] = tags
    return _TAGS[owner]


def keys_of(ranges, owner, prefix, count):
    tags = owner_tags(ranges, owner)
    return [b"{%s}%s:%d" % (tags[j % 32], prefix, j) for j in range(count)]


def pipeline(c, cmds, depth=50):
    out = []
    for j in range(0, len(cmds), depth):
        part = cmds[j:j + depth]
        c.send(part)
        out.extend(c.read() for _ in part)
    return out


def wait_rewrite(c):
    for _ in range(1200):
        p = info(c, "persistence")
        if p.get("aof_rewrite_in_progress") == "0" and p.get("aof_rewrite_scheduled") == "0":
            return True
        time.sleep(0.05)
    return False


def wait_no_bio_fsync(c):
    for _ in range(500):
        if info(c, "persistence").get("aof_pending_bio_fsync", "0") == "0":
            return
        time.sleep(0.01)


class AofFile:
    """Counts records in the current incr AOF file as it grows (thread-safe)."""

    def __init__(self, srv):
        self.dir = os.path.join(srv.dir, "appendonlydir")
        self.lock = threading.Lock()
        man = [f for f in os.listdir(self.dir) if f.endswith(".manifest")][0]
        incr = [l.split()[1] for l in open(os.path.join(self.dir, man)) if l.split()[-1] == "i"][-1]
        self.path = os.path.join(self.dir, incr)
        self.data = bytearray()
        self.seen = {}  # pattern -> [count, bytes of data scanned]

    def count(self, pat):
        with self.lock:
            with open(self.path, "rb") as f:
                f.seek(len(self.data))
                self.data += f.read()
            cnt, upto = self.seen.get(pat, (0, 0))
            # matches ending past `upto` are the new ones; records never overlap
            cnt += bytes(self.data[max(0, upto - len(pat) + 1):]).count(pat)
            self.seen[pat] = (cnt, len(self.data))
            return cnt


def rec(*args):
    return Conn.enc(args)


# ------------------------------------------------------------------------------------------------
# AOF
# ------------------------------------------------------------------------------------------------

def aof_checks():
    srv = Server("aof", ["--appendonly", "yes", "--appendfsync", "always", "--auto-aof-rewrite-percentage", "0"])
    pid = srv.p.pid
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    o0 = ostat(adm)
    check("aof.veto_none", info(adm, "owners").get("owner_veto") == "none", info(adm, "owners").get("owner_veto"))

    # native: SET/GET/INCR on each owner's own keys run on the owner
    for i in range(N):
        c = srv.conn(i)
        ks = keys_of(rng, i, b"nat%d" % i, 100)
        pipeline(c, [("SET", k, b"v") for k in ks] + [("GET", k) for k in ks] + [("INCR", k + b":n") for k in ks])
        c.close()
    o1 = ostat(adm)
    dn = o1["owner_native_commands"] - o0["owner_native_commands"]
    # the INCR keys may sit on another owner; at least SET+GET of own keys are native
    check("aof.native", dn >= 200 * N, "native +%d for %d commands" % (dn, 300 * N))

    # reply_after_write: with every AOF flush delayed S ms, a native write's reply waits for it
    S = 300
    for mode in ("always", "everysec"):
        adm.cmd("CONFIG", "SET", "appendfsync", mode)
        worst = []
        for i in range(N):
            c = srv.conn(i)
            k = keys_of(rng, i, b"raw%d" % i, 1)[0]
            c.cmd("GET", k)  # warm
            wait_no_bio_fsync(adm)
            adm.cmd("DEBUG", "AOF-FLUSH-SLEEP", S * 1000)
            t0 = time.monotonic()
            c.send([("SET", k, mode), ("GET", k)])
            r1, r2 = c.read(), c.read()
            ms = (time.monotonic() - t0) * 1000
            adm.cmd("DEBUG", "AOF-FLUSH-SLEEP", 0)
            worst.append((round(ms), r1, r2))
            c.close()
        ok = all(ms >= S - 50 and r1 == "OK" and r2 == mode.encode() for ms, r1, r2 in worst)
        check("aof.reply_after_write.%s" % mode, ok, "ms per owner %s (flush sleep %d ms)" % ([w[0] for w in worst], S))
    adm.cmd("CONFIG", "SET", "appendfsync", "always")

    # aoftest: every reply is backed by the AOF when it arrives
    af = AofFile(srv)
    bad = []
    lock = threading.Lock()
    W = 1500
    ctr = [keys_of(rng, i, b"ctr%d" % i, 1)[0] for i in range(N)]
    pats = [rec(b"INCR", k) for k in ctr]
    replies = [0]

    def writer(i, port_owner, n, step):
        c = srv.conn(port_owner)
        for j in range(n):
            v = c.cmd("INCR", ctr[i])
            if j % step == 0:
                have = af.count(pats[i])
                if not isinstance(v, int) or have < v:
                    with lock:
                        bad.append(("incr", i, v, have))
            with lock:
                replies[0] += 1
        c.close()

    def reader(i, stop):
        c = srv.conn(i)
        while not stop.is_set():
            v = c.cmd("GET", ctr[i])
            v = int(v) if v is not None else 0
            have = af.count(pats[i])
            if have < v:
                with lock:
                    bad.append(("get", i, v, have))
            with lock:
                replies[0] += 1
        c.close()

    stop = threading.Event()
    ths = [threading.Thread(target=writer, args=(i, i, W, 1 if i == 0 else 3)) for i in range(N)]
    ths.append(threading.Thread(target=writer, args=(0, None, 400, 1)))           # plain port: main
    ths.append(threading.Thread(target=writer, args=(0, 1 % N, 300, 1)))          # other owner: handoff
    rds = [threading.Thread(target=reader, args=(i, stop)) for i in range(N)]
    for t in ths + rds:
        t.start()
    for t in ths:
        t.join()
    stop.set()
    for t in rds:
        t.join()
    finals = [int(adm.cmd("GET", k)) for k in ctr]
    exp = [W + (700 if i == 0 else 0) for i in range(N)]
    check("aof.aoftest", not bad and finals == exp,
          "%d replies checked, %d unbacked %s, finals %s expected %s, file records %s"
          % (replies[0], len(bad), bad[:3], finals, exp, [af.count(p) for p in pats]))

    # lazy expiry: a native GET of an expired key writes its DEL/UNLINK before the reply
    lz = []
    for i in range(N):
        c = srv.conn(i)
        k = keys_of(rng, i, b"lazy%d" % i, 1)[0]
        c.cmd("SET", k, b"v", "PX", 30)
        time.sleep(0.08)
        before = af.count(rec(b"DEL", k)) + af.count(rec(b"UNLINK", k)) + af.count(rec(b"del", k)) + af.count(rec(b"unlink", k))
        r = c.cmd("GET", k)
        after = af.count(rec(b"DEL", k)) + af.count(rec(b"UNLINK", k)) + af.count(rec(b"del", k)) + af.count(rec(b"unlink", k))
        lz.append((r, before, after))
        c.close()
    # main's active expiry may delete it first (its DEL is then in the AOF before the GET too)
    check("aof.lazy_expire_record", all(r is None and a >= 1 for r, b, a in lz), lz)

    # WAITAOF after a native write
    c = srv.conn(0)
    k = keys_of(rng, 0, b"wa", 1)[0]
    c.cmd("SET", k, b"v")
    wr = c.cmd("WAITAOF", 1, 0, 2000)
    check("aof.waitaof", wr == [1, 0], wr)
    c.close()

    # replay: mixed load, model, DEBUG LOADAOF, restart
    adm.cmd("FLUSHALL")
    model = {0: {}, 1: {}}   # db -> key -> ('s', value) | ('i', int) | ('l', [items])
    mlock = threading.Lock()
    errs = []

    def put(db, k, v):
        with mlock:
            model[db][k] = v

    def owner_load(i):
        try:
            c = srv.conn(i)
            ks = keys_of(rng, i, b"rk%d" % i, 2000)
            r = pipeline(c, [("SET", k, b"val:" + k) for k in ks])
            assert all(x == "OK" for x in r), r[:3]
            for k in ks:
                put(0, k, ("s", b"val:" + k))
            cs = keys_of(rng, i, b"rc%d" % i, 50)
            pipeline(c, [("INCR", cs[j % 50]) for j in range(500)], depth=10)
            ex = keys_of(rng, i, b"re%d" % i, 300)
            pipeline(c, [("SET", k, b"e", "EX", 1000) for k in ex])
            for k in ex:
                put(0, k, ("s", b"e"))
            r = pipeline(c, [("DEL", k) for k in ks[:200]])
            assert all(x == 1 for x in r), r[:3]
            with mlock:
                for k in ks[:200]:
                    model[0].pop(k, None)
            xs = keys_of(rng, i, b"rx%d" % i, 100)
            pipeline(c, [("SET", k, b"x", "PX", 50) for k in xs])
            d1 = srv.conn(i)
            d1.cmd("SELECT", 1)
            ds = keys_of(rng, i, b"rd%d" % i, 100)
            pipeline(d1, [("SET", k, b"db1:" + k) for k in ds])
            for k in ds:
                put(1, k, ("s", b"db1:" + k))
            d1.close()
            # another owner's port: these keys of owner i go through main
            oc = srv.conn((i + 1) % N)
            os_ = keys_of(rng, i, b"ro%d" % i, 200)
            pipeline(oc, [("SET", k, b"oth:" + k) for k in os_])
            for k in os_:
                put(0, k, ("s", b"oth:" + k))
            oc.close()
            time.sleep(0.2)
            r = pipeline(c, [("GET", k) for k in xs])  # lazily expired on the owner
            assert all(x is None for x in r), r[:3]
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def main_load():
        try:
            c = srv.conn()
            for i in range(N):
                cs = keys_of(rng, i, b"rc%d" % i, 50)
                pipeline(c, [("INCRBY", k, 5) for k in cs])
            c.cmd("BGREWRITEAOF")
            ms = [keys_of(rng, i, b"rm", 20) for i in range(N)]
            flat = [k for ks in ms for k in ks]
            for j in range(0, len(flat), 8):
                kv = []
                for k in flat[j:j + 8]:
                    kv += [k, b"m:" + k]
                c.cmd("MSET", *kv)
            for k in flat:
                put(0, k, ("s", b"m:" + k))
            ls = [b"rl:%d" % j for j in range(50)]
            pipeline(c, [("RPUSH", k, b"a", b"b", k) for k in ls])
            for k in ls:
                put(0, k, ("l", [b"a", b"b", k]))
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    ths = [threading.Thread(target=owner_load, args=(i,)) for i in range(N)] + [threading.Thread(target=main_load)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    for i in range(N):
        for j, k in enumerate(keys_of(rng, i, b"rc%d" % i, 50)):
            put(0, k, ("i", 10 + 5))
    check("aof.replay_load", not errs, errs[:3])
    check("aof.replay_rewrite_done", wait_rewrite(adm), info(adm, "persistence").get("aof_last_bgrewrite_status"))

    def model_ok(c, tag):
        miss = []
        for db in (0, 1):
            c.cmd("SELECT", db)
            ks = list(model[db].items())
            got = pipeline(c, [(("LRANGE", k, 0, -1) if v[0] == "l" else ("GET", k)) for k, v in ks], depth=200)
            for (k, v), g in zip(ks, got):
                want = v[1] if v[0] != "i" else str(v[1]).encode()
                if g != want:
                    miss.append((db, k, v, g))
            n = c.cmd("DBSIZE")
            if n != len(model[db]):
                miss.append(("dbsize", db, n, len(model[db])))
        c.cmd("SELECT", 0)
        return check("aof.replay_model_" + tag, not miss, "%d mismatches %s" % (len(miss), miss[:3]))

    model_ok(adm, "live")
    o_pre = ostat(adm)
    d_live = adm.cmd("DEBUG", "DIGEST")
    adm.cmd("DEBUG", "LOADAOF")
    d_load = adm.cmd("DEBUG", "DIGEST")
    check("aof.replay_loadaof_digest", d_load == d_live, (d_live, d_load))
    model_ok(adm, "loadaof")
    adm.close()
    srv.stop()
    srv.start()
    adm = srv.conn()
    d_restart = adm.cmd("DEBUG", "DIGEST")
    check("aof.replay_restart_digest", d_restart == d_live, (d_live, d_restart))
    model_ok(adm, "restart")
    check("aof.native_after_restart", info(adm, "owners").get("owner_veto") == "none", info(adm, "owners").get("owner_veto"))
    adm.close()
    check("aof.counters", o_pre.get("owner_aof_ops", 0) > 0 and o_pre.get("owner_held_replies", 0) > 0,
          {k: o_pre.get(k) for k in ("owner_aof_ops", "owner_held_replies", "owner_native_commands")})
    crash = srv.crashed()
    srv.stop()
    check("aof.no_crash", crash == 0, "crash lines %d, first pid %d" % (crash, pid))


# ------------------------------------------------------------------------------------------------
# maxmemory
# ------------------------------------------------------------------------------------------------

def load_unique(srv, rng, prefix, per_owner, vlen, depth, conns_per_owner, via_owner=True):
    """Unique keys, each written once; returns {key: reply}."""
    N = ARGS.n
    res = {}
    lock = threading.Lock()

    def run(i, part):
        c = srv.conn(i if via_owner else None)
        ks = keys_of(rng, i, b"%s%d" % (prefix, i), per_owner)[part::conns_per_owner]
        out = pipeline(c, [("SET", k, (b"%s|" % k).ljust(vlen, b"x")) for k in ks], depth=depth)
        with lock:
            res.update(zip(ks, out))
        c.close()

    ths = [threading.Thread(target=run, args=(i, p)) for i in range(N) for p in range(conns_per_owner)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    return res


class Sampler(threading.Thread):
    def __init__(self, srv):
        super().__init__()
        self.c, self.stop, self.peak, self.n = srv.conn(), threading.Event(), 0, 0

    def run(self):
        while not self.stop.is_set():
            u = int(info(self.c, "memory")["used_memory"])
            self.peak = max(self.peak, u)
            self.n += 1
            time.sleep(0.002)

    def end(self):
        self.stop.set()
        self.join()
        self.c.close()
        return self.peak


def settle_used(c):
    time.sleep(0.3)
    return int(info(c, "memory")["used_memory"])


def mm_checks():
    MB = 1 << 20
    srv = Server("mm", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"])
    pid = srv.p.pid
    adm = srv.conn()
    rng = owner_ranges(adm)
    check("mm.veto_none", info(adm, "owners").get("owner_veto") == "none", info(adm, "owners").get("owner_veto"))
    o0 = ostat(adm)
    load_unique(srv, rng, b"mn", 500, 16, 50, 1)
    o1 = ostat(adm)
    dn = o1["owner_native_commands"] - o0["owner_native_commands"]
    check("mm.native", dn >= 500 * ARGS.n, "native +%d of %d SETs, to main %d" %
          (dn, 500 * ARGS.n, o1.get("owner_maxmemory_to_main", 0) - o0.get("owner_maxmemory_to_main", 0)))

    # accounting: owner-port load vs plain-port load of the same keys
    adm.cmd("FLUSHALL", "SYNC")
    base = settle_used(adm)
    load_unique(srv, rng, b"ac", 25000, 100, 100, 1)
    d_own = settle_used(adm) - base
    adm.cmd("FLUSHALL", "SYNC")
    back1 = settle_used(adm) - base
    load_unique(srv, rng, b"ac", 25000, 100, 100, 1, via_owner=False)
    d_main = settle_used(adm) - base
    adm.cmd("FLUSHALL", "SYNC")
    back2 = settle_used(adm) - base
    rel = abs(d_own - d_main) / max(1, d_main)
    check("mm.accounting", rel <= 0.03 and back1 <= 512 * 1024 and back2 <= 512 * 1024,
          "growth owners %.2f MB, plain %.2f MB (%.2f%%), after FLUSHALL %+d / %+d bytes"
          % (d_own / MB, d_main / MB, 100 * rel, back1, back2))

    # evict: allkeys-lru at 64 MB, 160 MB of unique writes on every owner port
    LIM = 64 * MB
    adm.cmd("CONFIG", "SET", "maxmemory", LIM)
    adm.cmd("CONFIG", "RESETSTAT")
    o0 = ostat(adm)
    sm = Sampler(srv)
    sm.start()
    res = load_unique(srv, rng, b"ev", 160 * 1024 // ARGS.n, 1024, 16, 2)
    peak = sm.end()
    st = info(adm, "stats")
    o1 = ostat(adm)
    acked = sum(1 for v in res.values() if v == "OK")
    errs = [v for v in res.values() if v != "OK"]
    # lift the limit, then count: at the limit every command (these too) may evict first, as stock does
    adm.cmd("CONFIG", "SET", "maxmemory", 0)
    evicted = int(info(adm, "stats")["evicted_keys"])
    dbs = adm.cmd("DBSIZE")
    present = [k for k in res if res[k] == "OK"]
    got = pipeline(adm, [("GET", k) for k in present], depth=500)
    adm.cmd("CONFIG", "SET", "maxmemory", LIM)
    wrong = [(k, g) for k, g in zip(present, got) if g is not None and g != (b"%s|" % k).ljust(1024, b"x")]
    live = sum(1 for g in got if g is not None)
    check("mm.evict.limit", peak <= LIM + 2 * MB, "peak used %.2f MB, limit %.0f MB, %d samples" % (peak / MB, LIM / MB, sm.n))
    check("mm.evict.keys", evicted > 0 and not errs and dbs + evicted == acked and live == dbs and not wrong,
          "acked %d, errors %d %s, evicted %d, dbsize %d, readable %d, wrong values %d"
          % (acked, len(errs), errs[:2], evicted, dbs, live, len(wrong)))
    check("mm.evict.native_share", True, "native %d, to main for memory %d, handoffs %d" % (
        o1["owner_native_commands"] - o0["owner_native_commands"],
        o1.get("owner_maxmemory_to_main", 0) - o0.get("owner_maxmemory_to_main", 0),
        o1["owner_handoffs_to_main"] - o0["owner_handoffs_to_main"]))

    # noeviction at 40 MB: refusals happen; every acked key present, every refused key absent
    adm.cmd("FLUSHALL", "SYNC")
    adm.cmd("CONFIG", "SET", "maxmemory-policy", "noeviction")
    LIM2 = 40 * MB
    adm.cmd("CONFIG", "SET", "maxmemory", LIM2)
    sm = Sampler(srv)
    sm.start()
    res = load_unique(srv, rng, b"ne", 60 * 1024 // ARGS.n, 1024, 8, 1)
    peak = sm.end()
    ok = [k for k, v in res.items() if v == "OK"]
    oom = [k for k, v in res.items() if isinstance(v, Err) and v.startswith("OOM")]
    other = [v for v in res.values() if v != "OK" and not (isinstance(v, Err) and v.startswith("OOM"))]
    g_ok = pipeline(adm, [("EXISTS", k) for k in ok], depth=500)
    g_oom = pipeline(adm, [("EXISTS", k) for k in oom], depth=500)
    dbs = adm.cmd("DBSIZE")
    gets = []
    for i in range(ARGS.n):
        c = srv.conn(i)
        k = keys_of(rng, i, b"ne%d" % i, 1)[0]
        gets.append(c.cmd("GET", k))
        c.close()
    check("mm.noeviction", oom and not other and all(x == 1 for x in g_ok) and not any(g_oom) and dbs == len(ok)
          and peak <= LIM2 + 2 * MB and all(not isinstance(g, Err) for g in gets),
          "acked %d, OOM %d, other %s, dbsize %d, peak %.2f MB, owner GETs %s"
          % (len(ok), len(oom), other[:2], dbs, peak / MB, [type(g).__name__ for g in gets]))

    # lowered: maxmemory set below used; native writes then go to main, which evicts
    adm.cmd("FLUSHALL", "SYNC")
    adm.cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
    adm.cmd("CONFIG", "SET", "maxmemory", "1gb")
    load_unique(srv, rng, b"lw", 30 * 1024 // ARGS.n, 1024, 16, 1)
    used_before = settle_used(adm)
    ev0 = int(info(adm, "stats")["evicted_keys"])
    LIM3 = 16 * MB
    adm.cmd("CONFIG", "SET", "maxmemory", LIM3)
    o0 = ostat(adm)
    load_unique(srv, rng, b"lw2", 500, 64, 10, 1)
    used_after = settle_used(adm)
    ev1 = int(info(adm, "stats")["evicted_keys"])
    o1 = ostat(adm)
    check("mm.lowered", used_after <= LIM3 + 2 * MB and ev1 > ev0,
          "used %.1f MB -> %.1f MB under %d MB, evicted +%d, to main for memory +%d"
          % (used_before / MB, used_after / MB, LIM3 // MB, ev1 - ev0,
             o1.get("owner_maxmemory_to_main", 0) - o0.get("owner_maxmemory_to_main", 0)))
    crash = srv.crashed()
    adm.close()
    srv.stop()
    check("mm.no_crash", crash == 0, "crash lines %d, pid %d" % (crash, pid))

    # context: the same eviction load on the stock path (owner threads off) for its peak
    ref = Server("mm-stock", ["--maxmemory", str(LIM), "--maxmemory-policy", "allkeys-lru"], owners=False)
    rc = ref.conn()
    rrng = [(lo, hi, o) for lo, hi, o in rng]
    sm = Sampler(ref)
    sm.start()
    load_unique(ref, rrng, b"ev", 160 * 1024 // ARGS.n, 1024, 16, 2, via_owner=False)
    speak = sm.end()
    check("mm.evict.stock_peak_info", True, "stock path peak used %.2f MB, evicted %s" % (
        speak / MB, info(rc, "stats")["evicted_keys"]))
    rc.close()
    ref.stop()


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=26379)
    ap.add_argument("--owner-port", type=int, default=26400)
    ap.add_argument("--cpus", default="48-51")
    ap.add_argument("--main-cpu", default="56")
    ap.add_argument("--only", choices=["aof", "mm"])
    ARGS = ap.parse_args()
    random.seed(7)
    if ARGS.only in (None, "aof"):
        aof_checks()
    if ARGS.only in (None, "mm"):
        mm_checks()
    res = d3.RESULTS
    fails = [r["check"] for r in res if not r["ok"]]
    print("D3VETO " + json.dumps(dict(n=ARGS.n, checks=len(res), fails=fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
