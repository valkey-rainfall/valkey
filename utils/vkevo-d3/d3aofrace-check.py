#!/usr/bin/env python3
"""D3-all-g4-aof gate: under appendfsync everysec and no, each owner writes its own AOF records at the end of
its window while the other owners keep running (ownerDrainAof); main stops the world only for its own work.

Starts its own loopback servers (owner-threads N, appendonly yes, aof-use-rdb-preamble no so a rewritten
base file is plain commands, no automatic rewrite) from --bin and checks:

  race.backed.no         appendfsync no (nothing postpones a write, so every reply must be backed). Per owner:
                         a P1 writer and a depth-8 writer INCR the owner's 4 counters, a transaction writer runs
                         MULTI INCR a INCR b EXEC, a reader GETs the counters (the GET ring) and MGETs a b; the
                         plain port and another owner's port INCR owner 0's counter 0 meanwhile; BGREWRITEAOF 3
                         times during the run; with --bench, valkey-benchmark SETs on every owner port at once.
                         Every INCR / GET / MGET reply value v is backed when it arrives: the counter's value in
                         the current manifest's base file plus its INCR records in the manifest's incr files is
                         >= v. MGET a b answers a == b. The final counters equal the INCRs sent
  race.backed.everysec   the same under everysec, reported, not failed: a reply may leave behind a write that
                         flushAppendOnlyFile postponed while a background fsync runs, as on the stock loop
  race.model.<mode>      no, everysec: per owner and per db 0/1/2 a writer pipelines SET (the same key names in
                         every db, per-db values), INCR, DEL, MSET of one tag, MULTI SET INCR EXEC and SET EX;
                         the plain port writes db 3 and db 0 and other owners' keys (handoff to main);
                         BGREWRITEAOF 3 times during the run; under everysec also CONFIG SET appendfsync always
                         and back (main's write path and the owners' take turns while writes run); with --bench,
                         valkey-benchmark load on every owner port. Then: every model key equal live, after DEBUG
                         LOADAOF and after a restart from the AOF, dbsize of dbs 1-3 equal, and DEBUG DIGEST equal
                         in all three
  race.waitaof.everysec  after the load, a native SET then WAITAOF 1 0 3000 on every owner answers [1, 0]
  race.fsync_pace.everysec  (D3-all-g6-aoftail, part pace-everysec) owner 0 SETs 4 KB values as fast as it can
                         while a plain-port client does SET then WAITAOF 1 0 3000 20 times: each answers [1, 0]
                         and the 90th percentile wait is under 600 ms (owner drains start an fsync each 32 MB
                         unsynced; the once-a-second fsync alone puts it near 1 s)
  race.drains.<mode>     owner_aof_drains moved (the owners wrote the AOF themselves; owner_world_stops is
                         shown next to it), aof_last_write_status and aof_last_bgrewrite_status ok
  race.no_crash.<mode>   no crash lines in the server log

Usage: d3aofrace-check.py --bin src/valkey-server --dir /tmp/x --n 4 --port 27379 --owner-port 27400
                          --cpus 8-11 --main-cpu 16 [--bench src/valkey-benchmark] [--secs 4]
                          [--parts backed-no,backed-everysec,model-no,model-everysec,pace-everysec]
Prints PASS/FAIL (and INFO) lines and a final 'D3AOFRACE {json}' line; exit 1 on any FAIL. On a binary
without owner_aof_drains (the parent) race.drains fails by construction; the rest applies as is.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
veto = __import__("d3veto-check")
import aofcompact  # noqa: E402
d3 = veto.d3
Conn, Err, check, info, ostat = d3.Conn, d3.Err, d3.check, d3.info, d3.ostat
owner_ranges, owner_tags, keys_of, pipeline, wait_rewrite = (veto.owner_ranges, veto.owner_tags, veto.keys_of,
                                                             veto.pipeline, veto.wait_rewrite)
ARGS = None


def server(name, mode):
    return veto.Server(name, ["--appendonly", "yes", "--appendfsync", mode, "--aof-use-rdb-preamble", "no",
                              "--auto-aof-rewrite-percentage", "0"])


class Bench:
    """valkey-benchmark SET P1 on every owner port (cluster mode), keys apart from the checks' own."""

    def __init__(self):
        self.p = None
        if ARGS.bench:
            self.p = subprocess.Popen([ARGS.bench, "-h", "127.0.0.1", "-p", str(ARGS.owner_port), "--cluster",
                                       "-t", "set", "-P", "1", "-c", str(8 * ARGS.n), "--threads", "2",
                                       "-n", "100000000", "-r", "200000", "-d", "16", "-q"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self):
        if self.p:
            self.p.terminate()
            self.p.wait(timeout=30)


class AofView:
    """What the AOF on disk holds for some counters now: the value the manifest's base file sets plus the
    INCR records in its incr files. Files are read incrementally; thread-safe."""

    def __init__(self, srv, keys):
        self.dir = os.path.join(srv.dir, "appendonlydir")
        self.pats = {k: Conn.enc(("INCR", k)) for k in keys}
        self.lock = threading.Lock()
        self.base = {}  # base name -> {key: value}
        self.incr = {}  # incr name -> [bytearray, {key: (count, scanned)}]

    def manifest(self):
        man = [f for f in os.listdir(self.dir) if f.endswith(".manifest") and not f.startswith("temp-")][0]
        base, incrs = None, []
        for line in open(os.path.join(self.dir, man)):
            p = line.split()
            kv = dict(zip(p[0::2], p[1::2]))  # D3-all-g11-binaof: a compact line ends "format compact"
            if kv.get("type") == "b":
                base = kv["file"]
            elif kv.get("type") == "i":
                incrs.append(kv["file"])
        return base, incrs

    def base_values(self, name):
        if name not in self.base:
            vals, db = {}, 0
            recs = parse_aof(open(os.path.join(self.dir, name), "rb").read()) if name else []
            for r in recs:
                if r[0].upper() == b"SELECT":
                    db = int(r[1])
                elif r[0].upper() == b"SET" and db == 0 and r[1] in self.pats:
                    vals[r[1]] = int(r[2])
            self.base[name] = vals
        return self.base[name]

    def incr_count(self, name, key):
        ent = self.incr.setdefault(name, [bytearray(), {}])
        with open(os.path.join(self.dir, name), "rb") as f:
            f.seek(len(ent[0]))
            ent[0] += f.read()
        pat = self.pats[key]
        if aofcompact.is_compact(ent[0]):  # D3-all-g11-binaof: the compact record of the same INCR
            pat = aofcompact.encode(("INCR", key))
        cnt, upto = ent[1].get(key, (0, 0))
        cnt += ent[0].count(pat, max(0, upto - len(pat) + 1))  # records ending past `upto` are new
        ent[1][key] = (cnt, len(ent[0]))
        return cnt

    def backed(self, key):
        with self.lock:
            for _ in range(50):
                try:
                    base, incrs = self.manifest()
                    return self.base_values(base).get(key, 0) + sum(self.incr_count(n, key) for n in incrs)
                except (FileNotFoundError, IndexError, ValueError):
                    time.sleep(0.01)  # a rewrite switched the manifest under us
            raise RuntimeError("AOF files keep changing")


def parse_aof(data):
    if aofcompact.is_compact(data):  # D3-all-g11-binaof: a compact incr file decodes to the same argv
        return aofcompact.parse_any(data)[0]
    recs, i = [], 0
    while i < len(data):
        if data[i:i + 1] == b"#":
            i = data.index(b"\r\n", i) + 2
            continue
        if data[i:i + 1] != b"*":
            raise ValueError("bad record at %d" % i)
        j = data.index(b"\r\n", i)
        n, i, args = int(data[i + 1:j]), j + 2, []
        for _ in range(n):
            j = data.index(b"\r\n", i)
            ln, i = int(data[i + 1:j]), j + 2
            args.append(data[i:i + ln])
            i += ln + 2
        recs.append(args)
    return recs


def drain(adm):
    for _ in range(500):
        p = info(adm, "persistence")
        if p.get("aof_buffer_length", "0") == "0" and p.get("aof_pending_bio_fsync", "0") == "0":
            return
        time.sleep(0.01)


def rewrites(adm, stop, toggle):
    """BGREWRITEAOF three times while the load runs; under everysec flip appendfsync to always and back."""
    for t in range(3):
        if stop.wait(ARGS.secs / 4):
            break
        wait_rewrite(adm)
        adm.cmd("BGREWRITEAOF")
        if toggle and t == 1:
            adm.cmd("CONFIG", "SET", "appendfsync", "always")
            time.sleep(ARGS.secs / 8)
            adm.cmd("CONFIG", "SET", "appendfsync", "everysec")
    adm.close()


def backed(mode, strict):
    srv = server("backed-" + mode, mode)
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    ctr = {i: keys_of(rng, i, b"bc", 4) for i in range(N)}
    pair = {i: (b"{%s}pa" % owner_tags(rng, i)[5], b"{%s}pb" % owner_tags(rng, i)[5]) for i in range(N)}
    view = AofView(srv, [k for i in range(N) for k in ctr[i] + list(pair[i])])
    o0 = ostat(adm)
    bad, lock, errs = [], threading.Lock(), []
    seen = [0]
    sent = {k: 0 for i in range(N) for k in ctr[i] + list(pair[i])}
    stop = threading.Event()

    def note(kind, k, v):
        have = view.backed(k)
        with lock:
            seen[0] += 1
            if have < v:
                bad.append((kind, k, v, have))

    def writer(i, port, depth, keys, secs):
        try:
            c = srv.conn(port)
            n, t_end = 0, time.monotonic() + secs
            while time.monotonic() < t_end:
                ks = [keys[(n + j) % len(keys)] for j in range(depth)]
                c.send([("INCR", k) for k in ks])
                for k in ks:
                    v = c.read()
                    assert isinstance(v, int), v
                    note("incr", k, v)
                n += depth
            with lock:
                for j in range(n):
                    sent[keys[j % len(keys)]] += 1
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def txn(i, secs):
        try:
            c = srv.conn(i)
            a, b = pair[i]
            n, t_end = 0, time.monotonic() + secs
            while time.monotonic() < t_end:
                c.send([("MULTI",), ("INCR", a), ("INCR", b), ("EXEC",)])
                r = [c.read() for _ in range(4)]
                assert r[3] and r[3][0] == r[3][1], r
                note("exec", a, r[3][0])
                n += 1
            with lock:
                sent[a] += n
                sent[b] += n
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def reader(i):
        try:
            c = srv.conn(i)
            a, b = pair[i]
            while not stop.is_set():
                c.send([("GET", k) for k in ctr[i]] + [("MGET", a, b)])
                for k in ctr[i]:
                    v = c.read()
                    note("get", k, int(v) if v is not None else 0)
                m = c.read()
                va, vb = (int(x) if x is not None else 0 for x in m)
                if va != vb:
                    with lock:
                        bad.append(("mget_pair", a, va, vb))
                note("mget", a, va)
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    S = ARGS.secs
    ths = []
    for i in range(N):
        ths += [threading.Thread(target=writer, args=(i, i, 1, ctr[i], S)),
                threading.Thread(target=writer, args=(i, i, 8, ctr[i], S)),
                threading.Thread(target=txn, args=(i, S))]
    ths += [threading.Thread(target=writer, args=(0, None, 1, ctr[0][:1], S)),       # plain port: main
            threading.Thread(target=writer, args=(0, 1 % N, 1, ctr[0][:1], S))]      # other owner: handoff
    rds = [threading.Thread(target=reader, args=(i,)) for i in range(N)]
    bench = Bench()
    rw = threading.Thread(target=rewrites, args=(srv.conn(), stop, False))
    for t in ths + rds + [rw]:
        t.start()
    for t in ths:
        t.join()
    stop.set()
    for t in rds + [rw]:
        t.join()
    bench.stop()
    wait_rewrite(adm)
    finals = {k: int(adm.cmd("GET", k) or 0) for k in sent}
    o1 = ostat(adm)
    per = info(adm, "persistence")
    detail = "%d replies checked, %d unbacked %s, errors %s, rewrites %s" % (
        seen[0], len(bad), bad[:3], errs[:2], per.get("aof_rewrites"))
    if strict:
        check("race.backed." + mode, not bad and not errs and seen[0] > 1000, detail)
    else:
        check("race.backed_load." + mode, not errs and not [b for b in bad if b[0] == "mget_pair"], detail)
        print("INFO race.backed.%s: %d of %d replies left before their write (postponed behind a running "
              "background fsync, as the stock loop allows)" % (mode, len(bad), seen[0]), flush=True)
    check("race.backed_finals." + mode, finals == sent, [(k, finals[k], sent[k]) for k in sent if finals[k] != sent[k]][:3])
    drains(mode, o0, o1, per)
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("race.no_crash.backed." + mode, crash == 0, "crash lines %d" % crash)


def drains(mode, o0, o1, per):
    dd = o1.get("owner_aof_drains", 0) - o0.get("owner_aof_drains", 0)
    ds = o1.get("owner_world_stops", 0) - o0.get("owner_world_stops", 0)
    da = o1.get("owner_aof_ops", 0) - o0.get("owner_aof_ops", 0)
    check("race.drains." + mode, dd > 0 and per.get("aof_last_write_status") == "ok"
          and per.get("aof_last_bgrewrite_status") == "ok",
          "owner AOF writes %d, world stops %d, ops %d, write %s, bgrewrite %s, rewrites %s" % (
              dd, ds, da, per.get("aof_last_write_status"), per.get("aof_last_bgrewrite_status"),
              per.get("aof_rewrites")))


def pace():
    """everysec, D3-all-g6-aoftail: owner 0 writes 4 KB values as fast as it can (hundreds of MB/s of AOF)
    while a plain-port client does SET then WAITAOF 1 0 3000, 20 times. Owner drains start an fsync each
    time 32 MB are unsynced, so every wait is short; the stock once-a-second fsync alone puts p90 near 1 s."""
    srv = server("pace-everysec", "everysec")
    adm = srv.conn()
    keys = keys_of(owner_ranges(adm), 0, b"pace", 2048)
    stop, errs, sent, waits = threading.Event(), [], [0], []
    val = b"v" * 4096

    def heavy():
        try:
            c, n = srv.conn(0), 0
            while not stop.is_set() and n < (2 << 30) // len(val):  # at most 2 GB of values
                c.send([("SET", keys[(n + j) % len(keys)], val) for j in range(16)])
                for _ in range(16):
                    assert c.read() == "OK"
                n += 16
            sent[0] = n
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    th = threading.Thread(target=heavy)
    th.start()
    try:
        c = srv.conn()
        time.sleep(0.5)
        for _ in range(20):
            c.cmd("SET", b"fsync-pace", b"1")
            t0 = time.monotonic()
            r = c.cmd("WAITAOF", 1, 0, 3000)
            waits.append(round((time.monotonic() - t0) * 1000) if r == [1, 0] else repr(r))
            time.sleep(0.03)
        c.close()
    except Exception as e:  # noqa
        errs.append(repr(e))
    stop.set()
    th.join()
    per = info(adm, "persistence")
    ms = sorted(x for x in waits if isinstance(x, int))
    p90 = ms[min(len(ms) - 1, len(ms) * 9 // 10)] if ms else None
    check("race.fsync_pace.everysec", not errs and len(ms) == len(waits) == 20 and p90 < 600
          and per.get("aof_last_write_status") == "ok",
          "WAITAOF waits (ms) %s, p90 %s; %d SETs of 4 KB on owner 0, AOF %s B, errors %s" % (
              waits, p90, sent[0], per.get("aof_current_size"), errs[:2]))
    adm.close()
    crash = srv.crashed()
    srv.stop()
    shutil.rmtree(os.path.join(srv.dir, "appendonlydir"), ignore_errors=True)  # up to 2 GB
    check("race.no_crash.pace.everysec", crash == 0, "crash lines %d" % crash)


def model_run(mode):
    srv = server("model-" + mode, mode)
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    model = {db: {} for db in range(4)}
    mlock, errs = threading.Lock(), []
    stop = threading.Event()
    o0 = ostat(adm)
    S = ARGS.secs

    def owner_db(i, db):
        try:
            c = srv.conn(i)
            c.cmd("SELECT", db)
            ks = keys_of(rng, i, b"mk", 240)       # the same names in every db
            ctr = keys_of(rng, i, b"mc", 8)
            ex = keys_of(rng, i, b"mx", 20)
            tag = owner_tags(rng, i)[db + 7]
            ma, mb, mc = b"{%s}ma" % tag, b"{%s}mb" % tag, b"{%s}mc" % tag
            tk, tc = b"{%s}tk" % tag, b"{%s}tc" % tag
            mine, counts, r, t_end = {}, {k: 0 for k in ctr + [tc]}, 0, time.monotonic() + S
            while time.monotonic() < t_end:
                val = b"d%d:r%d:" % (db, r)
                res = pipeline(c, [("SET", k, val + k) for k in ks], depth=48)
                assert all(x == "OK" for x in res), res[:3]
                for k in ks:
                    mine[k] = val + k
                res = pipeline(c, [("INCR", ctr[j % 8]) for j in range(40)], depth=16)
                assert all(isinstance(x, int) for x in res), res[:3]
                for j in range(40):
                    counts[ctr[j % 8]] += 1
                gone = ks[(r * 17) % 200:(r * 17) % 200 + 20]
                res = pipeline(c, [("DEL", k) for k in gone], depth=20)
                assert all(x == 1 for x in res), res[:3]
                for k in gone:
                    mine.pop(k, None)
                assert c.cmd("MSET", ma, val + b"a", mb, val + b"b", mc, val + b"c") == "OK"
                mine.update({ma: val + b"a", mb: val + b"b", mc: val + b"c"})
                cmds = []
                for j in range(8):
                    cmds += [("MULTI",), ("SET", tk, val + b"t%d" % j), ("INCR", tc), ("EXEC",)]
                res = pipeline(c, cmds, depth=32)
                assert res[-1][0] == "OK" and isinstance(res[-1][1], int), res[-4:]
                counts[tc] += 8
                mine[tk] = val + b"t7"
                res = pipeline(c, [("SET", k, val, "EX", 10000) for k in ex], depth=20)
                for k in ex:
                    mine[k] = val
                r += 1
            for k, n in counts.items():
                mine[k] = b"%d" % n
            with mlock:
                model[db].update(mine)
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def plain():
        try:
            c = srv.conn()
            cross = [srv.conn((i + 1) % N) for i in range(N)]
            mine3, mine0, r, t_end = {}, {}, 0, time.monotonic() + S
            while time.monotonic() < t_end:
                c.cmd("SELECT", 3)
                ks = [b"plain3:%d" % j for j in range(150)]
                pipeline(c, [("SET", k, b"p3:%d" % r) for k in ks])
                mine3.update({k: b"p3:%d" % r for k in ks})
                c.cmd("SELECT", 0)
                ks = [b"plain0:%d" % j for j in range(150)]
                pipeline(c, [("SET", k, b"p0:%d" % r) for k in ks])
                mine0.update({k: b"p0:%d" % r for k in ks})
                for i in range(N):  # owner i's keys through owner i+1's port: main runs them
                    ks = keys_of(rng, i, b"xo", 40)
                    pipeline(cross[i], [("SET", k, b"x%d:%d" % (i, r)) for k in ks], depth=40)
                    mine0.update({k: b"x%d:%d" % (i, r) for k in ks})
                r += 1
            with mlock:
                model[3].update(mine3)
                model[0].update(mine0)
            for x in cross + [c]:
                x.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    ths = [threading.Thread(target=owner_db, args=(i, db)) for i in range(N) for db in range(3)]
    ths.append(threading.Thread(target=plain))
    bench = Bench()
    rw = threading.Thread(target=rewrites, args=(srv.conn(), stop, mode == "everysec"))
    for t in ths + [rw]:
        t.start()
    for t in ths:
        t.join()
    stop.set()
    rw.join()
    bench.stop()
    check("race.model_load." + mode, not errs, errs[:3])
    wait_rewrite(adm)
    o1 = ostat(adm)
    per = info(adm, "persistence")
    drains(mode, o0, o1, per)

    if mode == "everysec":
        wr = []
        for i in range(N):
            c = srv.conn(i)
            k = keys_of(rng, i, b"wa", 1)[0]
            c.cmd("SET", k, b"w")
            wr.append(c.cmd("WAITAOF", 1, 0, 3000))
            c.close()
            with mlock:
                model[0][k] = b"w"
        check("race.waitaof." + mode, all(x == [1, 0] for x in wr), wr)

    def model_ok(c, tag):
        miss = []
        for db in range(4):
            c.cmd("SELECT", db)
            ks = list(model[db].items())
            got = pipeline(c, [("GET", k) for k, _ in ks], depth=200)
            miss += [(db, k, v, g) for (k, v), g in zip(ks, got) if g != v]
            if db or not ARGS.bench:  # the benchmark's keys are in db 0
                n = c.cmd("DBSIZE")
                if n != len(model[db]):
                    miss.append(("dbsize", db, n, len(model[db])))
        c.cmd("SELECT", 0)
        return check("race.model_%s.%s" % (tag, mode), not miss, "%d mismatches %s; keys %s" % (
            len(miss), miss[:3], [len(model[db]) for db in range(4)]))

    drain(adm)
    model_ok(adm, "live")
    d_live = adm.cmd("DEBUG", "DIGEST")
    adm.cmd("DEBUG", "LOADAOF")
    d_load = adm.cmd("DEBUG", "DIGEST")
    check("race.loadaof_digest." + mode, d_load == d_live, (d_live, d_load))
    model_ok(adm, "loadaof")
    adm.close()
    srv.stop()
    srv.start()
    adm = srv.conn()
    d_restart = adm.cmd("DEBUG", "DIGEST")
    check("race.restart_digest." + mode, d_restart == d_live, (d_live, d_restart))
    model_ok(adm, "restart")
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("race.no_crash.model." + mode, crash == 0, "crash lines %d" % crash)


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=27379)
    ap.add_argument("--owner-port", type=int, default=27400)
    ap.add_argument("--cpus", default="8-11")
    ap.add_argument("--main-cpu", default="16")
    ap.add_argument("--bench", default="")
    ap.add_argument("--secs", type=float, default=4.0)
    ap.add_argument("--parts", default="backed-no,backed-everysec,model-no,model-everysec,pace-everysec")
    ARGS = ap.parse_args()
    veto.ARGS = ARGS
    parts = ARGS.parts.split(",")
    if "backed-no" in parts:
        backed("no", True)
    if "backed-everysec" in parts:
        backed("everysec", False)
    if "model-no" in parts:
        model_run("no")
    if "model-everysec" in parts:
        model_run("everysec")
    if "pace-everysec" in parts:
        pace()
    res = d3.RESULTS
    fails = [r["check"] for r in res if not r["ok"]]
    print("D3AOFRACE " + json.dumps(dict(n=ARGS.n, checks=len(res), fails=fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
