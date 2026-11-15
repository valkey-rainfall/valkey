#!/usr/bin/env python3
"""D3-all-aof gate: owners format their native writes' AOF records; main only appends the bytes.

Starts its own servers (owner-threads N, loopback, appendonly yes, no automatic rewrite) and checks:

  aof.bytes_identical  a sequential mixed workload (each reply is read before the next command, so the
                       AOF order is fixed) on every owner port and the plain port: SET (plain, shared-int
                       and int-encoded values, a 64 KB value, PX), INCR, DEL, MSET of one hash tag,
                       native MULTI/EXEC with 3 writes, 1 write and none, SELECT on owner ports (native
                       writes in db 1) and on the plain port (db 2), another owner's key through an owner
                       port (main runs it), lazy expiry (active expiry off), WAITAOF. The replies, the
                       incr AOF file and master_repl_offset of --bin equal --ref-bin's (PXAT times masked)
  aof.offset_records   master_repl_offset == the number of non-SELECT records in the incr AOF (every
                       record steps the offset once, WAITAOF's view), for --bin and --ref-bin; under
                       --fsync everysec each blocked WAITAOF adds one REPLCONF GETACK step (no record), so
                       there the two binaries' differences must be equal
  aof.multidb          appendfsync everysec. Every owner: 3 connections in db 0/1/2 pipeline writes to the
                       SAME key names with per-db values at once (SET, INCR, DEL, MULTI/SET/INCR/EXEC),
                       so one owner's buffer mixes dbs within a fold; the plain port writes db 3 and db 0
                       meanwhile. The dataset equals a client-side model per db live, after DEBUG LOADAOF
                       and after a restart; DEBUG DIGEST equal in all three; offset == records
  aof.timestamp        aof-timestamp-enabled yes, the sequential workload over 3 s: #TS annotations are
                       written, the records (annotations dropped) equal --ref-bin's, LOADAOF and restart
                       digests equal the live one

Usage: d3aof-check.py --bin src/valkey-server --ref-bin <parent>/src/valkey-server --dir /tmp/x --n 4
                      --port 47379 --owner-port 47400 --cpus 48-51 --main-cpu 56 [--fsync always|everysec]
--fsync is the appendfsync of bytes_identical and timestamp (default always; everysec takes the owners'
own AOF writes in D3-all-g4-aof). Prints PASS/FAIL lines and a final 'D3AOF {json}' line; exit 1 on any FAIL.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")
import aofcompact  # noqa: E402  (D3-all-g11-binaof: compact incr files)
Conn, Err, key_slot, check, info, ostat = d3.Conn, d3.Err, d3.key_slot, d3.check, d3.info, d3.ostat

ARGS = None


class Server:
    def __init__(self, name, binary, extra):
        self.dir = os.path.join(ARGS.dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self.bin, self.extra = binary, list(extra)
        self.start()

    def start(self):
        cmd = [self.bin, "--port", str(ARGS.port), "--bind", "127.0.0.1", "--save", "",
               "--enable-debug-command", "yes", "--dir", self.dir, "--logfile", "log", "--daemonize", "no",
               "--server-cpulist", ARGS.main_cpu, "--bio-cpulist", ARGS.main_cpu,
               "--owner-threads", str(ARGS.n), "--owner-port", str(ARGS.owner_port), "--owner-cpulist", ARGS.cpus,
               "--appendonly", "yes", "--auto-aof-rewrite-percentage", "0"]
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

    def incr_path(self):
        d = os.path.join(self.dir, "appendonlydir")
        man = [f for f in os.listdir(d) if f.endswith(".manifest")][0]
        incr = [l.split()[1] for l in open(os.path.join(d, man)) if " type i" in l][-1]  # g11: + "format compact"
        return os.path.join(d, incr)


def owner_ranges(c):
    return sorted(((int(s[0]), int(s[1]), int(s[2][1]) - ARGS.owner_port) for s in c.cmd("CLUSTER", "SLOTS")),
                  key=lambda r: r[2])


def owner_tags(rng, owner, count=32):
    lo, hi = [(lo, hi) for lo, hi, o in rng if o == owner][0]
    tags, i = [], 0
    while len(tags) < count:
        t = b"a%d-%d" % (owner, i)
        i += 1
        if lo <= key_slot(b"{%s}" % t) <= hi:
            tags.append(t)
    return tags


def keys_of(rng, owner, prefix, count):
    tags = owner_tags(rng, owner)
    return [b"{%s}%s:%d" % (tags[j % 32], prefix, j) for j in range(count)]


def pipeline(c, cmds, depth=50):
    out = []
    for j in range(0, len(cmds), depth):
        part = cmds[j:j + depth]
        c.send(part)
        out.extend(c.read() for _ in part)
    return out


PXAT = re.compile(rb"(\$4\r\nPXAT\r\n\$\d+\r\n)\d+", re.I)


def parse_aof(data):
    """-> (records as lists of args, annotation count). Asserts the file is well-formed RESP (or, from
    D3-all-g11-binaof, well-formed compact records, decoded to the same argv)."""
    if aofcompact.is_compact(data):
        return aofcompact.parse_any(data)
    recs, ann, i = [], 0, 0
    while i < len(data):
        if data[i:i + 1] == b"#":
            i = data.index(b"\r\n", i) + 2
            ann += 1
            continue
        if data[i:i + 1] != b"*":
            raise ValueError("bad record at %d: %r" % (i, data[i:i + 20]))
        j = data.index(b"\r\n", i)
        n, i, args = int(data[i + 1:j]), j + 2, []
        for _ in range(n):
            if data[i:i + 1] != b"$":
                raise ValueError("bad bulk at %d: %r" % (i, data[i:i + 20]))
            j = data.index(b"\r\n", i)
            ln, i = int(data[i + 1:j]), j + 2
            args.append(data[i:i + ln])
            if data[i + ln:i + ln + 2] != b"\r\n":
                raise ValueError("bad bulk end at %d" % (i + ln))
            i += ln + 2
        recs.append(args)
    return recs, ann


def drain(adm):
    """Until the AOF buffer is written and no background fsync is pending (everysec may release a reply
    behind a postponed write, as the stock loop does)."""
    for _ in range(500):
        p = info(adm, "persistence")
        if p.get("aof_buffer_length", "0") == "0" and p.get("aof_pending_bio_fsync", "0") == "0":
            return
        time.sleep(0.01)


def offset_minus_records(srv, adm):
    drain(adm)
    recs, _ = parse_aof(open(srv.incr_path(), "rb").read())
    n = sum(1 for r in recs if r[0].upper() != b"SELECT")
    return int(info(adm, "replication")["master_repl_offset"]) - n, n


def seq_workload(srv, rounds=3, gap=0.0):
    """Sequential: every command waits for its reply. Returns the replies."""
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    out = [adm.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)]
    cs = [srv.conn(i) for i in range(N)]
    db1 = [srv.conn(i) for i in range(N)]
    for i in range(N):
        out.append(db1[i].cmd("SELECT", 1))  # handed to main and back
    big = b"B" * 65536
    for r in range(rounds):
        for i in range(N):
            c = cs[i]
            k = keys_of(rng, i, b"s%d" % r, 8)
            t = owner_tags(rng, i)[r]
            ma, mb, mc = b"{%s}ma" % t, b"{%s}mb" % t, b"{%s}mc" % t
            for cmd in [("SET", k[0], b"v%d" % r), ("INCR", k[1]), ("INCR", k[1]), ("SET", k[2], 12),
                        ("SET", k[3], 123456789), ("SET", k[4], big if r == 0 else b"small"), ("DEL", k[0]),
                        ("DEL", k[0]), ("MSET", ma, 1, mb, 2, mc, b"x"), ("DEL", ma, mb),
                        ("MULTI",), ("SET", ma, b"t"), ("INCR", mb), ("DEL", mc), ("EXEC",),
                        ("MULTI",), ("SET", mc, b"one"), ("EXEC",),
                        ("MULTI",), ("GET", mc), ("EXEC",),
                        ("SET", k[5], b"px", "PX", 40), ("GET", k[3]), ("MGET", ma, mb, mc)]:
                out.append(c.cmd(*cmd))
            # db 1 natively on the same owner, then db 0 again on the next one
            out.append(db1[i].cmd("SET", k[6], b"db1-%d" % r))
            out.append(db1[i].cmd("INCR", k[7]))
            out.append(cs[(i + 1) % N].cmd("SET", k[7], b"db0-%d" % r))
            # another owner's key through this owner's port: main runs it
            out.append(cs[i].cmd("SET", keys_of(rng, (i + 1) % N, b"x%d" % r, 1)[0], b"via-%d" % i))
            out.append(cs[i].cmd("WAITAOF", 1, 0, 2000))
        # the plain port: db 2, then db 0
        out.append(adm.cmd("SELECT", 2))
        out.append(adm.cmd("SET", b"plain2:%d" % r, b"p"))
        out.append(adm.cmd("SELECT", 0))
        out.append(adm.cmd("INCRBY", b"plain0:%d" % r, 7))
        out.append(db1[0].cmd("SET", keys_of(rng, 0, b"after%d" % r, 1)[0], b"z"))
        if gap:
            time.sleep(gap)
    time.sleep(0.1)
    for r in range(rounds):
        for i in range(N):
            out.append(cs[i].cmd("GET", keys_of(rng, i, b"s%d" % r, 8)[5]))  # lazily expired on the owner
    out.append(cs[0].cmd("WAITAOF", 1, 0, 2000))
    for c in cs + db1:
        c.close()
    adm.close()
    return out


def bytes_identical():
    res = {}
    for tag, binary in (("ref", ARGS.ref_bin), ("new", ARGS.bin)):
        srv = Server("seq-" + tag, binary, ["--appendfsync", ARGS.fsync])
        replies = seq_workload(srv)
        adm = srv.conn()
        veto = info(adm, "owners").get("owner_veto")
        o = ostat(adm)
        omr, nrec = offset_minus_records(srv, adm)
        off = int(info(adm, "replication")["master_repl_offset"])
        data = open(srv.incr_path(), "rb").read()
        adm.close()
        crash = srv.crashed()
        srv.stop()
        res[tag] = dict(replies=replies, data=data, masked=PXAT.sub(rb"\1X", data), off=off, omr=omr, nrec=nrec,
                        veto=veto, native=o.get("owner_native_commands", 0), aof_ops=o.get("owner_aof_ops", 0),
                        crash=crash)
    a, b = res["ref"], res["new"]
    check("aof.seq_native", a["veto"] == "none" and b["veto"] == "none" and b["native"] > 0 and b["aof_ops"] > 0,
          "veto %s/%s native %d/%d aof_ops %d/%d" % (a["veto"], b["veto"], a["native"], b["native"], a["aof_ops"],
                                                     b["aof_ops"]))
    check("aof.seq_replies_identical", a["replies"] == b["replies"],
          "%d replies; first diff %s" % (len(b["replies"]), next(((j, x, y) for j, (x, y) in
                                                                  enumerate(zip(a["replies"], b["replies"])) if x != y),
                                                                 None)))
    diff = next((j for j in range(min(len(a["masked"]), len(b["masked"]))) if a["masked"][j] != b["masked"][j]), None)
    recs, _ = parse_aof(b["data"])
    sel = sum(1 for r in recs if r[0].upper() == b"SELECT")
    multi = sum(1 for r in recs if r[0].upper() == b"MULTI")
    if aofcompact.is_compact(a["data"]) or aofcompact.is_compact(b["data"]):
        # D3-all-g11-binaof: a compact file holds other bytes by design; its records must decode to the
        # ref's argv, record for record (PXAT times masked), and the file must be well-formed compact.
        def mrec(data):
            return [[b"X" if j and r[j - 1].upper() == b"PXAT" else x for j, x in enumerate(r)]
                    for r in parse_aof(data)[0]]
        ra, rb = mrec(a["data"]), mrec(b["data"])
        rd = next((j for j in range(min(len(ra), len(rb))) if ra[j] != rb[j]), None)
        check("aof.bytes_identical", ra == rb and len(b["data"]) > 0,
              "SEMANTIC (compact): %d / %d bytes, %d / %d records (%d SELECT, %d MULTI) decode equal; first diff "
              "%s %r / %r" % (len(a["data"]), len(b["data"]), len(ra), len(rb), sel, multi, rd,
                              ra[rd] if rd is not None and rd < len(ra) else None,
                              rb[rd] if rd is not None and rd < len(rb) else None))
    else:
        check("aof.bytes_identical", a["masked"] == b["masked"] and len(b["data"]) > 0,
              "%d / %d bytes, %d records (%d SELECT, %d MULTI), first diff at %s %r / %r" % (
                  len(a["data"]), len(b["data"]), len(recs), sel, multi, diff,
                  a["masked"][diff - 40:diff + 40] if diff is not None else b"", b["masked"][diff - 40:diff + 40] if diff is not None else b""))
    check("aof.offset_identical", a["off"] == b["off"], "master_repl_offset %d / %d" % (a["off"], b["off"]))
    # A WAITAOF that blocks (everysec: no fsync yet) has main send REPLCONF GETACK, which steps the offset
    # once with no replicas (replicationFeedReplicas) and writes no AOF record; under always none blocks.
    check("aof.offset_records", a["omr"] == b["omr"] and (ARGS.fsync != "always" or a["omr"] == 0),
          "offset - records: ref %d (%d records), new %d (%d records)" % (a["omr"], a["nrec"], b["omr"], b["nrec"]))
    check("aof.seq_no_crash", a["crash"] == 0 and b["crash"] == 0, (a["crash"], b["crash"]))


def multidb():
    srv = Server("multidb", ARGS.bin, ["--appendfsync", "everysec"])
    adm = srv.conn()
    rng = owner_ranges(adm)
    N = ARGS.n
    model = {db: {} for db in range(4)}
    mlock = threading.Lock()
    errs = []
    go = threading.Barrier(3 * N + 1)

    def owner_db(i, db):
        try:
            c = srv.conn(i)
            c.cmd("SELECT", db)
            ks = keys_of(rng, i, b"md", 600)  # the same names in every db
            ctr = keys_of(rng, i, b"mc", 20)
            go.wait()
            r = pipeline(c, [("SET", k, b"db%d:%s" % (db, k)) for k in ks], depth=64)
            assert all(x == "OK" for x in r), r[:3]
            r = pipeline(c, [("INCR", ctr[j % 20]) for j in range(400)], depth=32)
            assert all(isinstance(x, int) for x in r), r[:3]
            r = pipeline(c, [("DEL", k) for k in ks[:100]], depth=64)
            assert all(x == 1 for x in r), r[:3]
            t = owner_tags(rng, i)[db]
            tk, tc = b"{%s}tk" % t, b"{%s}tc" % t
            cmds = []
            for j in range(60):
                cmds += [("MULTI",), ("SET", tk, b"db%d-t%d" % (db, j)), ("INCR", tc), ("EXEC",)]
            r = pipeline(c, cmds, depth=40)
            assert r[-1] == ["OK", 60], r[-4:]
            with mlock:
                for k in ks[100:]:
                    model[db][k] = b"db%d:%s" % (db, k)
                for k in ctr:
                    model[db][k] = b"20"
                model[db][tk] = b"db%d-t59" % db
                model[db][tc] = b"60"
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    def plain():
        try:
            c = srv.conn()
            go.wait()
            for db in (3, 0):
                c.cmd("SELECT", db)
                ks = [b"plain:%d:%d" % (db, j) for j in range(500)]
                r = pipeline(c, [("SET", k, b"p%d" % db) for k in ks], depth=50)
                assert all(x == "OK" for x in r), r[:3]
                with mlock:
                    for k in ks:
                        model[db][k] = b"p%d" % db
            c.close()
        except Exception as e:  # noqa
            errs.append(repr(e))

    ths = [threading.Thread(target=owner_db, args=(i, db)) for i in range(N) for db in (0, 1, 2)]
    ths.append(threading.Thread(target=plain))
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    check("aof.multidb_load", not errs, errs[:3])

    def model_ok(tag):
        miss = []
        for db in range(4):
            adm.cmd("SELECT", db)
            ks = list(model[db].items())
            got = pipeline(adm, [("GET", k) for k, _ in ks], depth=200)
            miss += [(db, k, v, g) for (k, v), g in zip(ks, got) if g != v]
            n = adm.cmd("DBSIZE")
            if n != len(model[db]):
                miss.append(("dbsize", db, n, len(model[db])))
        adm.cmd("SELECT", 0)
        return check("aof.multidb_model_" + tag, not miss, "%d mismatches %s; keys %s" % (
            len(miss), miss[:3], [len(model[db]) for db in range(4)]))

    o = ostat(adm)
    check("aof.multidb_native", info(adm, "owners").get("owner_veto") == "none" and o.get("owner_aof_ops", 0) > 0,
          {k: o.get(k) for k in ("owner_aof_ops", "owner_held_replies", "owner_native_commands", "owner_world_stops")})
    model_ok("live")
    omr, nrec = offset_minus_records(srv, adm)
    recs, _ = parse_aof(open(srv.incr_path(), "rb").read())
    sel = sum(1 for r in recs if r[0].upper() == b"SELECT")
    check("aof.multidb_offset_records", omr == 0, "offset - records %d (%d records, %d SELECT)" % (omr, nrec, sel))
    d_live = adm.cmd("DEBUG", "DIGEST")
    adm.cmd("DEBUG", "LOADAOF")
    d_load = adm.cmd("DEBUG", "DIGEST")
    check("aof.multidb_loadaof_digest", d_load == d_live, (d_live, d_load))
    model_ok("loadaof")
    adm.close()
    srv.stop()
    srv.start()
    adm = srv.conn()
    d_restart = adm.cmd("DEBUG", "DIGEST")
    check("aof.multidb_restart_digest", d_restart == d_live, (d_live, d_restart))
    model_ok("restart")
    adm.close()
    crash = srv.crashed()
    srv.stop()
    check("aof.multidb_no_crash", crash == 0, "crash lines %d" % crash)


def timestamp():
    res = {}
    for tag, binary in (("ref", ARGS.ref_bin), ("new", ARGS.bin)):
        srv = Server("ts-" + tag, binary, ["--appendfsync", ARGS.fsync, "--aof-timestamp-enabled", "yes"])
        seq_workload(srv, rounds=3, gap=1.1)
        recs, ann = parse_aof(open(srv.incr_path(), "rb").read())
        adm = srv.conn()
        d_live = adm.cmd("DEBUG", "DIGEST")
        adm.cmd("DEBUG", "LOADAOF")
        d_load = adm.cmd("DEBUG", "DIGEST")
        adm.close()
        srv.stop()
        srv.start()
        adm = srv.conn()
        d_restart = adm.cmd("DEBUG", "DIGEST")
        adm.close()
        crash = srv.crashed()
        srv.stop()
        masked = [[b"X" if j and a[j - 1].upper() == b"PXAT" else x for j, x in enumerate(a)] for a in recs]
        res[tag] = dict(recs=masked, ann=ann, digests=(d_live, d_load, d_restart), crash=crash)
    a, b = res["ref"], res["new"]
    check("aof.timestamp_annotations", b["ann"] >= 2, "#TS lines ref %d new %d" % (a["ann"], b["ann"]))
    check("aof.timestamp_records_identical", a["recs"] == b["recs"], "%d / %d records" % (len(a["recs"]), len(b["recs"])))
    d = b["digests"]
    check("aof.timestamp_replay", d[0] == d[1] == d[2] and b["crash"] == 0, (d, b["crash"]))


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=47379)
    ap.add_argument("--owner-port", type=int, default=47400)
    ap.add_argument("--cpus", default="48-51")
    ap.add_argument("--main-cpu", default="56")
    ap.add_argument("--fsync", choices=["always", "everysec"], default="always")  # bytes_identical, timestamp
    ARGS = ap.parse_args()
    bytes_identical()
    multidb()
    timestamp()
    res = d3.RESULTS
    fails = [r["check"] for r in res if not r["ok"]]
    print("D3AOF " + json.dumps(dict(n=ARGS.n, checks=len(res), fails=fails)))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
