#!/usr/bin/env python3
"""RL-g6h-multi-native model check: MULTI/EXEC on the owners against the parent's main path.

  rl-multi-check.py --bin CHILD --ref-bin PARENT --dir D --port P --owner-port OP [--n 4] --cpus 32-35 --main-cpu 36
      --bio-cpu 37 --load-cpus 38-39 --seed S [--ops 2500] [--tops 1200] [--memtier PATH]

Servers (127.0.0.1, appendonly always RESP, stream-node-max-entries 8): the candidate (owners) at P / OP+, its
replica at P+1 / OP+30+, the parent at P+20 / OP+20+ and its replica at P+21 / OP+50+; AOF reloads of each at P+2 and
P+3. Every reply uses explicit stream IDs and no wall-clock field, so replies are deterministic. A.digest / B compare
the whole db (DEBUG DIGEST, incl. expire markers) before any background load; the final primary / replica / reload
checks compare DEBUG DIGEST-VALUE over the explicit model keyspace, so the candidate-only memtier load below is
excluded.
  A  lockstep: one seeded sequence of MULTI/EXEC blocks (2-6 one-slot commands across strings, counters, hashes, lists,
     sets, zsets, keyspace, the long-tail kinds and streams), interleaved with single commands, DISCARD, WATCH ... EXEC
     and WATCH ... (touch) ... EXEC races, nested MULTI, EXEC without MULTI, a queued bad-arity / unknown command
     (EXECABORT) and queued runtime errors (WRONGTYPE, INCR on a non-int): candidate (most through the key's owner port,
     some main, some another owner) vs parent (main). Every reply equal; cand DIGEST == parent DIGEST.
  B  concurrency / atomicity: many connections each run MULTI {INCR a; INCR b; RPUSH log} on shared one-slot keys; at
     the end a == b == llen(log) on both (no interleaving lost a half-transaction), and cand DIGEST == parent DIGEST.
  C  under load: 6 threads of MULTI blocks on their own keys plus a memtier load on the owner ports; each thread's
     replies equal the parent's.
  Then: native share (owner_txn_commands vs the queued commands sent to an owner); DEBUG DIGEST of the candidate vs the
  parent (primary), the candidate's replica vs the parent's replica, the candidate's AOF reload vs the parent's.
Prints PASS/FAIL/INFO and 'RLMULTICHECK {json}'; exit 1 on any FAIL.
"""
import argparse, importlib.util, json, os, random, shutil, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("rlbp", os.path.join(HERE, "rl-busypoll-check.py"))
bp = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(bp)
check, note, info, ival = bp.check, bp.note, bp.info, bp.ival


class Conn(bp.Conn):
    def close(self):
        for x in (self.f, self.s):
            try:
                x.close()
            except OSError:
                pass


class Server(bp.Server):
    def c(self, port=None):
        return Conn(port or self.port)

    def start(self):
        try:
            return bp.Server.start(self)
        except FileNotFoundError:
            pf = os.path.join(self.dir, "pid")
            for _ in range(50):
                if os.path.exists(pf) and open(pf).read().strip():
                    self.pid = int(open(pf).read().strip()); return self
                time.sleep(0.1)
            raise


def crc16(b):
    crc = 0
    for ch in b:
        crc ^= ch << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(k):
    k = k if isinstance(k, bytes) else k.encode()
    s = k.find(b"{")
    if s != -1:
        e = k.find(b"}", s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) & 16383


def owner_ranges(s):
    out = []
    for k, v in s.owners().items():
        if k.startswith("owner") and k[5:].isdigit():
            f = dict(x.split("=", 1) for x in v.split(","))
            lo, hi = f["slots"].split("-")
            out.append((int(lo), int(hi), int(f["port"])))
    return sorted(out)


V = b"v" * 120


class Gen:
    def __init__(self, seed, groups):
        self.r = random.Random(seed)
        self.groups = groups
        self.sid = {}

    def val(self):
        r = self.r.random()
        if r < 0.4:
            return b"%d" % self.r.randrange(-1000, 100000)
        return bytes(self.r.choice(b"abcdefxyz") for _ in range(self.r.choice((1, 8, 48, 120))))

    def cmd(self, g, allow_stream=True):
        r = self.r
        k = "{%s}" % g
        c = r.random()
        if c < 0.12:
            return ["SET", k + ":s", self.val()]
        if c < 0.18:
            return ["APPEND", k + ":s", self.val()]
        if c < 0.22:
            return ["GETEX", k + ":s", "EX", "1000"]
        if c < 0.26:
            return ["GETDEL", k + ":s"]
        if c < 0.32:
            return ["INCR", k + ":c"]
        if c < 0.36:
            return ["INCRBYFLOAT", k + ":c", "1.5"]
        if c < 0.42:
            return ["HSET", k + ":h", "f%d" % r.randrange(6), self.val()]
        if c < 0.45:
            return ["HINCRBYFLOAT", k + ":h", "fl", "0.5"]
        if c < 0.48:
            return ["HDEL", k + ":h", "f%d" % r.randrange(6)]
        if c < 0.54:
            return ["LPUSH", k + ":l", self.val()]
        if c < 0.57:
            return ["LMOVE", k + ":l", k + ":l2", "LEFT", "RIGHT"]
        if c < 0.60:
            return ["LTRIM", k + ":l", "0", "20"]
        if c < 0.65:
            return ["SADD", k + ":t", "m%d" % r.randrange(20)]
        if c < 0.68:
            # SREM of a seeded member (not SPOP): the popped member is random per process, so
            # SPOP replies/digests would diverge between two independent servers; SREM is deterministic.
            return ["SREM", k + ":t", "m%d" % r.randrange(20)]
        if c < 0.72:
            return ["ZADD", k + ":z", str(r.randrange(100)), "m%d" % r.randrange(20)]
        if c < 0.75:
            return ["ZPOPMIN", k + ":z"]
        if c < 0.80:
            return ["EXPIRE", k + ":s", str(r.randrange(100, 9000))]
        if c < 0.82:
            return ["PERSIST", k + ":s"]
        if c < 0.85:
            return ["DEL", k + ":l2"]
        if not allow_stream or c < 0.90:
            return ["GET", k + ":s"]
        # streams with explicit, increasing IDs (deterministic replies)
        sx = k + ":x"
        self.sid[sx] = self.sid.get(sx, 0) + r.choice((1, 1, 2))
        sub = r.random()
        if sub < 0.4:
            return ["XADD", sx, "%d-1" % self.sid[sx], "f", self.val()]
        if sub < 0.5:
            return ["XADD", sx, "MAXLEN", "~", "5", "%d-1" % self.sid[sx], "f", self.val()]
        if sub < 0.6:
            return ["XLEN", sx]
        if sub < 0.7:
            return ["XTRIM", sx, "MAXLEN", "3"]
        if sub < 0.78:
            return ["XGROUP", "CREATE", sx, "g1", "0", "MKSTREAM"]
        if sub < 0.86:
            return ["XACK", sx, "g1", "%d-1" % max(1, self.sid[sx] - 1)]
        if sub < 0.93:
            return ["XSETID", sx, "%d-5" % (self.sid[sx] + 10)]
        return ["XDEL", sx, "%d-1" % max(1, self.sid[sx] - 1)]

    def block(self, g):
        n = self.r.randint(2, 6)
        return [self.cmd(g) for _ in range(n)]


def norm(rep):
    if isinstance(rep, bp.Err):
        return ("ERR", str(rep).split(" ")[0])
    if isinstance(rep, list):
        return [norm(x) for x in rep]
    return rep


def digest(c):
    return c.cmd("DEBUG", "DIGEST")


def kdigest(c, keys):
    # Per-key value digests over an explicit key set (DEBUG DIGEST-VALUE). Excludes unrelated keys such as the
    # memtier background-load keys, which hit the candidate only and would otherwise skew a whole-db DIGEST between
    # the candidate and the parent (and between their replicas / AOF reloads).
    out = []
    for i in range(0, len(keys), 256):
        out += c.cmd("DEBUG", "DIGEST-VALUE", *keys[i:i + 256])
    return out


# key suffixes the generator and the atomicity test create under each one-slot group
GEN_SUFFIXES = (":s", ":c", ":h", ":l", ":l2", ":t", ":z", ":x", ":w")
ATOM_SUFFIXES = (":a", ":b", ":log")


def model_keys(groups, tgroups):
    allk = []
    for g in groups:
        allk += ["{%s}%s" % (g, s) for s in GEN_SUFFIXES]
    for j in range(4):
        allk += ["{atom%d}%s" % (j, s) for s in ATOM_SUFFIXES]
    for tg in tgroups:
        for g in tg:
            allk += ["{%s}%s" % (g, s) for s in GEN_SUFFIXES]
    return sorted(set(allk))


def run_block(conn, block):
    out = []
    out.append(("MULTI", norm(conn.cmd("MULTI"))))
    for cmd in block:
        out.append((cmd[0], norm(conn.cmd(*cmd))))
    out.append(("EXEC", norm(conn.cmd("EXEC"))))
    return out


def main():
    ap = argparse.ArgumentParser()
    for x in ("--bin", "--ref-bin", "--dir", "--cpus", "--load-cpus"):
        ap.add_argument(x, required=True)
    for x in ("--port", "--owner-port", "--main-cpu", "--bio-cpu", "--seed"):
        ap.add_argument(x, type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--ops", type=int, default=2500)
    ap.add_argument("--tops", type=int, default=1200)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--memtier", default="/home/ec2-user/ns/memtier_benchmark")
    a = ap.parse_args()
    a.all_cpus = "%s,%d,%d" % (a.cpus, a.main_cpu, a.bio_cpu)
    os.makedirs(a.dir, exist_ok=True)
    aof = ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp", "--aof-use-rdb-preamble", "no",
           "--stream-node-max-entries", "8", "--repl-diskless-sync-delay", "0"]
    servers = []
    try:
        cand = Server(a, "cand", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, aof).start(); servers.append(cand)
        crep = Server(a, "crep", a.bin, a.port + 1, a.owner_port + 30, a.cpus, a.main_cpu,
                      ["--stream-node-max-entries", "8", "--replicaof", "127.0.0.1", str(a.port)]).start(); servers.append(crep)
        ref = Server(a, "ref", a.ref_bin, a.port + 20, a.owner_port + 20, a.cpus, a.main_cpu, aof).start(); servers.append(ref)
        rrep = Server(a, "rrep", a.ref_bin, a.port + 21, a.owner_port + 50, a.cpus, a.main_cpu,
                      ["--stream-node-max-entries", "8", "--replicaof", "127.0.0.1", str(a.port + 20)]).start(); servers.append(rrep)
        if not (bp_wait(cand, crep) and bp_wait(ref, rrep)):
            raise RuntimeError("a replica did not sync")
        ranges = owner_ranges(cand)
        oports = [p for _, _, p in ranges]
        rng = random.Random(a.seed)
        groups = ["m%d.%d" % (a.seed, i) for i in range(24)]
        cconns = {p: cand.c(p) for p in [a.port] + oports}
        rconn = ref.c()
        sent = {"queued": 0}

        def cand_conn(g):
            x = rng.random()
            if x < 0.06:
                return cconns[a.port]
            if x < 0.11:
                return cconns[rng.choice(oports)]
            s = key_slot("{%s}" % g)
            return cconns[[p for lo, hi, p in ranges if lo <= s <= hi][0]]

        # A: lockstep, interleaved blocks, singles and special scenarios
        g0 = Gen(a.seed, groups)
        rc, rr = [], []
        for i in range(a.ops):
            g = rng.choice(groups)
            cc, cr = cand_conn(g), rconn
            r = rng.random()
            if r < 0.55:
                blk = g0.block(g)
                rc += [(g, x) for x in run_block(cc, blk)]
                rr += [(g, x) for x in run_block(cr, blk)]
            elif r < 0.65:  # WATCH race: touch the key between WATCH and EXEC (same owner)
                k = "{%s}:s" % g
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("WATCH", norm(conn.cmd("WATCH", k)))))
                    acc.append((g, ("SET.race", norm(conn.cmd("SET", k, b"race")))))
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("SET", norm(conn.cmd("SET", k, b"inmulti")))))
                    acc.append((g, ("EXEC", norm(conn.cmd("EXEC")))))  # aborts: nil
            elif r < 0.72:  # WATCH clean
                k = "{%s}:c" % g
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("WATCH", norm(conn.cmd("WATCH", k)))))
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("INCR", norm(conn.cmd("INCR", k)))))
                    acc.append((g, ("EXEC", norm(conn.cmd("EXEC")))))
            elif r < 0.78:  # DISCARD
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("SET", norm(conn.cmd("SET", "{%s}:s" % g, b"x")))))
                    acc.append((g, ("DISCARD", norm(conn.cmd("DISCARD")))))
            elif r < 0.84:  # EXECABORT: a bad command queued
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("SET", norm(conn.cmd("SET", "{%s}:s" % g, b"y")))))
                    acc.append((g, ("NOSUCHCMD", norm(conn.cmd("NOSUCHCMD", "{%s}:s" % g)))))
                    acc.append((g, ("INCRarity", norm(conn.cmd("INCR")))))
                    acc.append((g, ("EXEC", norm(conn.cmd("EXEC")))))
            elif r < 0.90:  # runtime errors inside EXEC (WRONGTYPE, non-int INCR): EXEC runs, per-command errors
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("DEL", norm(conn.cmd("DEL", "{%s}:w" % g)))))
                    acc.append((g, ("LPUSH", norm(conn.cmd("LPUSH", "{%s}:w" % g, b"a")))))
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("INCRwt", norm(conn.cmd("INCR", "{%s}:w" % g)))))
                    acc.append((g, ("SETs", norm(conn.cmd("SET", "{%s}:s" % g, b"notanint")))))
                    acc.append((g, ("INCRnan", norm(conn.cmd("INCR", "{%s}:s" % g)))))
                    acc.append((g, ("GET", norm(conn.cmd("GET", "{%s}:s" % g)))))
                    acc.append((g, ("EXEC", norm(conn.cmd("EXEC")))))
            elif r < 0.94:  # nested MULTI and EXEC/DISCARD without MULTI
                for conn, acc in ((cc, rc), (cr, rr)):
                    acc.append((g, ("MULTI", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("MULTI2", norm(conn.cmd("MULTI")))))
                    acc.append((g, ("PING", norm(conn.cmd("PING")))))
                    acc.append((g, ("EXEC", norm(conn.cmd("EXEC")))))
                    acc.append((g, ("EXEC2", norm(conn.cmd("EXEC")))))
                    acc.append((g, ("DISCARD2", norm(conn.cmd("DISCARD")))))
            else:
                cmd = g0.cmd(g)
                rc.append((g, (cmd[0], norm(cc.cmd(*cmd)))))
                rr.append((g, (cmd[0], norm(cr.cmd(*cmd)))))
            sent["queued"] += 0
        diff = [(i, x, y) for i, (x, y) in enumerate(zip(rc, rr)) if x != y]
        check("A.lockstep_replies", not diff and len(rc) == len(rr), "%d reply points; first diff %s" % (len(rc), diff[:1]))
        for c in list(cconns.values()) + [rconn]:
            c.close()
        dc, dr = digest(cand.c()), digest(ref.c())
        check("A.digest", dc == dr and dc not in (b"0000000000000000000000000000000000000000", "0000000000000000000000000000000000000000"),
              "cand %s parent %s" % (dc, dr))

        # B: concurrency / atomicity on shared keys
        def spam(host, oport_of, port, nconn, rounds, res, idx):
            conns = []
            for j in range(nconn):
                g = "atom%d" % (j % 4)
                s = key_slot("{%s}" % g)
                p = port if host == "main" else oport_of(s)
                conns.append((Conn(p), g))
            for _ in range(rounds):
                for conn, g in conns:
                    conn.cmd("MULTI")
                    conn.cmd("INCR", "{%s}:a" % g)
                    conn.cmd("INCR", "{%s}:b" % g)
                    conn.cmd("RPUSH", "{%s}:log" % g, b"1")
                    conn.cmd("EXEC")
            for conn, _ in conns:
                conn.close()
            res[idx] = True

        oport_of = lambda s: [p for lo, hi, p in ranges if lo <= s <= hi][0]
        for srv, how in ((cand, "owner"), (ref, "main")):
            res = [False] * a.threads
            ths = [threading.Thread(target=spam, args=("owner" if srv is cand else "main",
                                                        oport_of, srv.port, 4, 120, res, t)) for t in range(a.threads)]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
        bad = []
        for srv in (cand, ref):
            cc = srv.c()
            for j in range(4):
                g = "atom%d" % j
                av = int(cc.cmd("GET", "{%s}:a" % g) or 0)
                bv = int(cc.cmd("GET", "{%s}:b" % g) or 0)
                lv = cc.cmd("LLEN", "{%s}:log" % g)
                if not (av == bv == lv):
                    bad.append((srv.name, g, av, bv, lv))
            cc.close()
        dcb, drb = digest(cand.c()), digest(ref.c())
        check("B.atomicity", not bad and dcb == drb, "a==b==llen on both, cand digest == parent %s; bad %s" % (dcb == drb, bad[:2]))

        # C: under load
        tgroups = [["t%d.%d" % (t, i) for i in range(4)] for t in range(a.threads)]
        tres = {"cand": [None] * a.threads, "ref": [None] * a.threads}

        def worker(side, t):
            gw = Gen(a.seed * 100 + t, tgroups[t])
            blocks = [gw.block(gw.r.choice(tgroups[t])) for _ in range(a.tops)]
            if side == "cand":
                conns = {p: cand.c(p) for p in [a.port] + oports}
                def cf(g):
                    s = key_slot("{%s}" % g); return conns[oport_of(s)]
                out = []
                for blk in blocks:
                    g = blk[0][1].split("}")[0][1:] if False else None
                    gg = tgroups[t][0]
                    out += run_block(cf(gg), blk)
                tres[side][t] = out
                for c in conns.values():
                    c.close()
            else:
                c = ref.c()
                out = []
                for blk in blocks:
                    out += run_block(c, blk)
                tres[side][t] = out
                c.close()

        mt = None
        if os.path.exists(a.memtier):
            mt = subprocess.Popen(["taskset", "-c", a.load_cpus, a.memtier, "-s", "127.0.0.1", "-p", str(oports[0]),
                                   "--cluster-mode", "-t", "2", "-c", "8", "--ratio", "1:3", "-d", "64",
                                   "--key-maximum", "40000", "--test-time", "600", "--hide-histogram"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)
        ths = [threading.Thread(target=worker, args=("cand", t)) for t in range(a.threads)]
        t0 = time.time()
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        tb = time.time() - t0
        if mt:
            mt.terminate(); mt.wait()
        ths = [threading.Thread(target=worker, args=("ref", t)) for t in range(a.threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        cbad = []
        for t in range(a.threads):
            d = [(i, p, q) for i, (p, q) in enumerate(zip(tres["cand"][t], tres["ref"][t])) if p != q]
            if d or len(tres["cand"][t]) != len(tres["ref"][t]):
                cbad.append((t, d[:1]))
        o1 = cand.owners()
        check("C.under_load_replies", not cbad, "%d threads x %d blocks in %.1f s, memtier %s; bad %s" % (
            a.threads, a.tops, tb, "on" if mt else "missing", cbad[:1]))
        check("native.txn", ival(o1, "owner_txn_commands") > 0,
              "owner_txn_commands %s, owner_handoffs_to_main %s" % (o1.get("owner_txn_commands"), o1.get("owner_handoffs_to_main")))

        # digests over the explicit model keyspace (not whole-db): the memtier load above writes only to the
        # candidate, so a whole-db DIGEST would differ on that noise; A.digest / B.atomicity already compared the
        # whole db (incl. expire markers) before any load.
        allk = model_keys(groups, tgroups)
        if not (bp_wait(cand, crep) and bp_wait(ref, rrep)):
            check("state.replica_sync", False, "a replica did not catch up")
        sc, sr = kdigest(cand.c(), allk), kdigest(ref.c(), allk)
        ndiff = sum(1 for x, y in zip(sc, sr) if x != y)
        check("state.cand_vs_parent", sc == sr, "%d model keys DIGEST-VALUE equal; %d differ" % (len(allk), ndiff))
        scr, srr = kdigest(crep.c(), allk), kdigest(rrep.c(), allk)
        check("state.replica", scr == srr, "%d model keys: cand replica == parent replica (%d differ); each vs its own primary: cand %s, parent %s" % (
            len(allk), sum(1 for x, y in zip(scr, srr) if x != y), scr == sc, srr == sr))
        for s in servers:
            s.stop()
        servers = []
        rl = Server(a, "creload", a.bin, a.port + 2, a.owner_port + 40, a.cpus, a.main_cpu,
                    ["--appendonly", "yes", "--aof-incr-format", "resp", "--stream-node-max-entries", "8"])
        shutil.copytree(os.path.join(cand.dir, "appendonlydir"), os.path.join(rl.dir, "appendonlydir"))
        rl.start(); servers.append(rl)
        rr2 = Server(a, "rreload", a.ref_bin, a.port + 3, a.owner_port + 60, a.cpus, a.main_cpu,
                     ["--appendonly", "yes", "--aof-incr-format", "resp", "--stream-node-max-entries", "8"])
        shutil.copytree(os.path.join(ref.dir, "appendonlydir"), os.path.join(rr2.dir, "appendonlydir"))
        rr2.start(); servers.append(rr2)
        slc, slr = kdigest(rl.c(), allk), kdigest(rr2.c(), allk)
        check("state.aof_reload", slc == slr, "%d model keys AOF reload DIGEST-VALUE equal; %d differ" % (
            len(allk), sum(1 for x, y in zip(slc, slr) if x != y)))
    except Exception as e:
        import traceback
        traceback.print_exc()
        check("ran", False, repr(e))
    finally:
        for s in servers:
            try:
                s.stop()
            except Exception:
                pass
        for nm in ("cand", "crep", "ref", "rrep", "creload", "rreload"):
            d = os.path.join(a.dir, nm, "appendonlydir")
            if os.path.isdir(d):
                shutil.rmtree(d)
    bad = [r for r in bp.RESULTS if not r["ok"]]
    print("RLMULTICHECK " + json.dumps(dict(seed=a.seed, passed=len(bp.RESULTS) - len(bad), failed=len(bad))), flush=True)
    sys.exit(1 if bad else 0)


def bp_wait(prim, rep, secs=30):
    end = time.time() + secs
    while time.time() < end:
        a, b = prim.c(), rep.c()
        try:
            pi, ri = info(a, "replication"), info(b, "replication")
        finally:
            a.close(); b.close()
        if ri.get("master_link_status") == "up" and ival(pi, "master_repl_offset") == ival(ri, "master_repl_offset"):
            return True
        time.sleep(0.2)
    return False


if __name__ == "__main__":
    main()
