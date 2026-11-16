#!/usr/bin/env python3
"""RL-g1e-native-set-zset model check: owners run single-key set and sorted-set commands natively.

Starts two servers with owner threads: the candidate (--bin) and the reference (--ref-bin, the parent
D3-all-g14-fold, whose owners hand every set / sorted-set command to main: the main path). Both get the same
deterministic workload, and every reply and the final keyspace must be equal:
  native     one connection per owner, only the gene's commands on that owner's own keys (pipelined): the
             candidate's owners run every one of them (owner_setzset_commands grows by the count, no handoff;
             under maxmemory SMEMBERS / ZRANGE of a hashtable or a tree go to main)
  disjoint   T connections at once, each on its own keys (spread over every owner's slots, so a connection's
             commands alternate between native runs and handoffs to main), random SADD SREM SISMEMBER
             SMEMBERS SCARD ZADD (NX XX GT LT CH INCR, bad combinations) ZREM ZRANGE (index, BYSCORE, BYLEX,
             REV, LIMIT, WITHSCORES, bad ranges) ZSCORE ZCARD ZINCRBY, plus SET/GET/DEL on the same keys
             (WRONGTYPE, type changes), OBJECT ENCODING / TYPE, EXPIRE / PERSIST, PEXPIRE 1 then an access
             20 ms later (lazy expiry on the owner), MULTI/EXEC with set commands inside (main), pipelines
             of 1-24 commands; members are small ints (intset), short strings and 70-byte strings, so the
             encodings convert (intset -> listpack -> hashtable, listpack -> tree)
  lockstep   S connections (owner ports and the plain port) take turns on a few shared keys, one command
             each in a fixed order: cross-connection visibility between owners and main
  keyspace   every key's TYPE, OBJECT ENCODING, TTL presence and value (sorted SMEMBERS, ZRANGE WITHSCORES)
             and DEBUG DIGEST equal on both
  native     (mixed) the candidate's owner_setzset_commands grew over the mixed phases, the reference has none
Suites (--suites): plain (small listpack limits), mm (maxmemory 1gb allkeys-lru, default limits),
aofrepl (appendonly everysec, compact incr format, a replica on each server attached before the run: the
replica's data equals its primary's; then each AOF is reloaded into a fresh server and compared), aofresp
(appendonly always, aof-incr-format resp, reloaded and compared). Replicas and reloads must hold the same data;
their encodings may differ from the live primary's only on the keys where the parent's do too (stock: a ZADD that
adds nothing can convert a listpack first, and is not propagated).
Prints PASS/FAIL lines and 'RLSETZSET {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


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


class Err(str):
    pass


class Conn:
    def __init__(self, port, timeout=60):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile("rb", buffering=1 << 20)

    @staticmethod
    def enc(args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    def send(self, cmds):
        self.s.sendall(b"".join(self.enc(c) for c in cmds))

    def read(self):
        line = self.f.readline()
        if not line:
            raise EOFError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Err(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError("bad reply %r" % line)

    def cmd(self, *args):
        self.send([args])
        return self.read()

    def close(self):
        self.s.close()


RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(dict(check=name, ok=bool(ok), detail=str(detail)[:600]))
    print(("PASS " if ok else "FAIL ") + name + (": " + str(detail)[:600] if detail != "" else ""), flush=True)
    return ok


def info(c, section):
    raw = c.cmd("INFO", section)
    out = {}
    for l in raw.decode().split("\r\n"):
        if ":" in l and not l.startswith("#"):
            k, v = l.split(":", 1)
            out[k] = v
    return out


class Server:
    def __init__(self, a, name, binary, port, oport, cpus, main_cpu, extra, n):
        self.a, self.name, self.bin, self.port, self.oport, self.n = a, name, binary, port, oport, n
        self.dir = os.path.join(a.dir, name)
        self.cpus, self.main_cpu, self.extra = cpus, main_cpu, list(extra)

    def start(self, fresh=True):
        if fresh:
            shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)
        cmd = [self.bin, "--port", str(self.port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
               "--enable-debug-command", "yes", "--daemonize", "no", "--dir", self.dir, "--logfile",
               os.path.join(self.dir, "log"), "--server-cpulist", str(self.main_cpu),
               "--bio-cpulist", str(self.a.bg_cpu), "--aof-rewrite-cpulist", str(self.a.bg_cpu),
               "--bgsave-cpulist", str(self.a.bg_cpu)]
        if self.n:
            cmd += ["--owner-threads", str(self.n), "--owner-port", str(self.oport), "--owner-cpulist", self.cpus]
        cmd += self.extra
        self.p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(300):
            try:
                c = Conn(self.port, timeout=2)
                r = c.cmd("PING")
                c.close()
                if r == "PONG":
                    return
            except (OSError, EOFError):
                pass
            time.sleep(0.05)
        raise RuntimeError("%s did not start" % self.name)

    def stop(self):
        try:
            c = Conn(self.port, timeout=10)
            c.send([("SHUTDOWN", "NOSAVE")])
            try:
                c.read()
            except (OSError, EOFError):
                pass
        except OSError:
            pass
        try:
            self.p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()

    def crashed(self):
        try:
            with open(os.path.join(self.dir, "log"), errors="replace") as f:
                return sum(1 for l in f if any(w in l for w in ("ASSERT", "crashed", "Guru", "BUG REPORT")))
        except OSError:
            return -1

    def port_of_slot(self, slot):
        if not self.n:
            return self.port
        for i in range(self.n):
            lo, hi = (1024 * i // self.n) * 16, (1024 * (i + 1) // self.n) * 16 - 1
            if lo <= slot <= hi:
                return self.oport + i
        return self.port


# ---------------------------------------------------------------------------------------------------------
# Workload: deterministic per connection (its own random generator); the same on both servers.

SMALL = [b"%d" % i for i in range(-40, 600)]
SHORT = [b"m%d" % i for i in range(160)]
LONG = [b"L%d-" % i + b"x" * 66 for i in range(12)]
LEX = [b"a", b"b", b"c", b"m1", b"m10", b"m5", b"zz"]


def member(r):
    x = r.random()
    if x < 0.45:
        return r.choice(SMALL)
    if x < 0.97:
        return r.choice(SHORT)
    return r.choice(LONG)


def score(r):
    x = r.random()
    if x < 0.5:
        return b"%d" % r.randrange(-50, 300)
    if x < 0.8:
        return b"%.3f" % (r.random() * 200 - 50)
    if x < 0.9:
        return r.choice([b"+inf", b"-inf", b"0", b"-0", b"1e3", b"3.5e-2"])
    return r.choice([b"nan", b"abc", b"1x", b"(1"]) if r.random() < 0.3 else b"%d" % r.randrange(5)


def rng_bound(r, lex=False):
    if lex:
        return r.choice([b"-", b"+", b"[a", b"(a", b"[m1", b"(m5", b"[zz", b"(c", b"bad"])
    return r.choice([b"-inf", b"+inf", b"0", b"(0", b"10", b"(100", b"250", b"-20", b"(-inf", b"x"])


def gen_cmd(r, keys, strkeys):
    """One random command (a list of args) on keys of this connection."""
    k = r.choice(keys)
    x = r.random()
    if x < 0.13:
        return ["SADD", k] + [member(r) for _ in range(r.choice([1, 1, 1, 2, 3, 8, 40]))]
    if x < 0.19:
        return ["SREM", k] + [member(r) for _ in range(r.choice([1, 1, 2, 5]))]
    if x < 0.27:
        return ["SISMEMBER", k, member(r)]
    if x < 0.31:
        return ["SMEMBERS", k]
    if x < 0.34:
        return ["SCARD", k]
    if x < 0.47:
        flags = []
        for f in ("NX", "XX", "GT", "LT", "CH"):
            if r.random() < 0.08:
                flags.append(f)
        n = r.choice([1, 1, 1, 2, 4, 30])
        if r.random() < 0.06:
            flags.append("INCR")
            n = r.choice([1, 1, 2])
        args = []
        for _ in range(n):
            args += [score(r) if r.random() < 0.97 else b"nan", member(r)]
        if r.random() < 0.02:
            args = args[:-1]  # odd: syntax error
        return ["ZADD", k] + flags + args
    if x < 0.52:
        return ["ZREM", k] + [member(r) for _ in range(r.choice([1, 1, 3]))]
    if x < 0.62:
        y = r.random()
        if y < 0.4:
            c = ["ZRANGE", k, b"%d" % r.randrange(-12, 12), b"%d" % r.randrange(-12, 40)]
            if r.random() < 0.3:
                c.append("REV")
        elif y < 0.75:
            lo, hi = rng_bound(r), rng_bound(r)
            c = ["ZRANGE", k, lo, hi, "BYSCORE"]
            if r.random() < 0.4:
                c = ["ZRANGE", k, hi, lo, "BYSCORE", "REV"]
            if r.random() < 0.4:
                c += ["LIMIT", b"%d" % r.randrange(-1, 6), b"%d" % r.randrange(-1, 8)]
        else:
            lo, hi = rng_bound(r, True), rng_bound(r, True)
            c = ["ZRANGE", k, lo, hi, "BYLEX"]
            if r.random() < 0.3:
                c = ["ZRANGE", k, hi, lo, "BYLEX", "REV"]
            if r.random() < 0.3:
                c += ["LIMIT", b"%d" % r.randrange(0, 4), b"%d" % r.randrange(-1, 5)]
        if r.random() < 0.35:
            c.append("WITHSCORES")
        if r.random() < 0.02:
            c.append("BOGUS")
        return c
    if x < 0.68:
        return ["ZSCORE", k, member(r)]
    if x < 0.71:
        return ["ZCARD", k]
    if x < 0.76:
        return ["ZINCRBY", k, score(r) if r.random() < 0.95 else b"+inf", member(r)]
    if x < 0.79:
        return ["SET", r.choice(strkeys + [k]), b"v%d" % r.randrange(100)]
    if x < 0.81:
        return ["GET", r.choice(strkeys + [k])]
    if x < 0.83:
        return ["DEL", k]
    if x < 0.86:
        return ["OBJECT", "ENCODING", k]
    if x < 0.88:
        return ["TYPE", k]
    if x < 0.90:
        return ["EXPIRE", k, "100000"]
    if x < 0.91:
        return ["PERSIST", k]
    if x < 0.94:
        return ["SADD", r.choice(strkeys), member(r)] if r.random() < 0.5 else ["ZADD", r.choice(strkeys), "1", "m"]
    return ["SISMEMBER", k, member(r)] if r.random() < 0.5 else ["ZSCORE", k, member(r)]


def norm(cmd, rep):
    """Replies that may differ in order only: SMEMBERS of a hashtable set (each process seeds its hash)."""
    if cmd[0] == "SMEMBERS" and isinstance(rep, list):
        return sorted(rep)
    if cmd[0] == "EXEC" and isinstance(rep, list):
        return [sorted(x) if isinstance(x, list) and all(isinstance(y, bytes) for y in x) else x for x in rep]
    return rep


def run_conn(port, seed, keys, strkeys, ops, out, idx, stats):
    """One connection's sequence; replies (normalized) into out[idx]."""
    r = random.Random(seed)
    c = Conn(port)
    reps = []
    done = 0
    try:
        while done < ops:
            x = r.random()
            if x < 0.01:
                # lazy expiry on whichever thread owns the key: PEXPIRE 1, wait, then an access
                k = r.choice(keys)
                reps.append(c.cmd("PEXPIRE", k, "1"))
                time.sleep(0.02)
                cmd = gen_cmd(r, [k], strkeys)
                reps.append(norm(cmd, c.cmd(*cmd)))
                done += 2
            elif x < 0.025:
                cmds = [["MULTI"], ["SADD", r.choice(keys), member(r)], ["ZADD", r.choice(keys), "5", member(r)],
                        ["SISMEMBER", r.choice(keys), member(r)], ["ZRANGE", r.choice(keys), "0", "-1", "WITHSCORES"],
                        ["EXEC"]]
                c.send(cmds)
                for cm in cmds:
                    reps.append(norm(cm, c.read()))
                done += len(cmds)
            else:
                n = r.choice([1, 1, 1, 1, 2, 5, 12, 24])
                cmds = [gen_cmd(r, keys, strkeys) for _ in range(n)]
                c.send(cmds)
                for cm in cmds:
                    reps.append(norm(cm, c.read()))
                done += n
    finally:
        c.close()
    out[idx] = reps
    stats[idx] = done


def owner_of_slot(slot, n):
    for i in range(n):
        if (1024 * i // n) * 16 <= slot <= (1024 * (i + 1) // n) * 16 - 1:
            return i
    return -1


def make_keys(tag, count, own=-1, n=1, share=0.7):
    """count keys; with own >= 0 about share of them in owner own's slots, the rest anywhere."""
    keys, j = [], 0
    want_own = int(count * share) if own >= 0 else 0
    while len(keys) < count:
        k = b"{%s:%d}k" % (tag.encode(), j)
        j += 1
        mine = own >= 0 and owner_of_slot(key_slot(k), n) == own
        if len(keys) < want_own and not mine:
            continue
        keys.append(k)
    return keys


def disjoint(srv, a, seed):
    """T connections at once, each on its own keys, connection i on owner port i % N (or the plain port)."""
    T = a.threads
    out, stats, ths = [None] * T, [0] * T, []
    for i in range(T):
        plain = i == T - 1
        keys = make_keys("t%d" % i, a.keys, -1 if plain else i % srv.n, srv.n)
        strkeys = [b"{t%d:s%d}str" % (i, j) for j in range(3)]
        port = srv.port if plain else srv.oport + (i % srv.n)
        ths.append(threading.Thread(target=run_conn, args=(port, seed * 1000 + i, keys, strkeys, a.ops, out, i, stats)))
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    return out, sum(stats)


NATIVE = {"SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD", "ZADD", "ZREM", "ZRANGE", "ZSCORE", "ZCARD", "ZINCRBY"}


def native_phase(srv, a, seed):
    """One connection per owner, each only on its own owner's keys and only the gene's commands (pipelined):
    replies, and how many of them the owners ran (owner_setzset_commands) with how many handoffs."""
    adm = Conn(srv.port)
    i0 = info(adm, "owners")
    conns, outs, sent = [], [], 0
    for i in range(srv.n):
        r = random.Random(seed * 31 + i)
        keys = make_keys("n%d" % i, 24, i, srv.n, 1.0)
        strkeys = make_keys("ns%d" % i, 2, i, srv.n, 1.0)
        c = Conn(srv.oport + i)
        reps = []
        for _ in range(a.native_ops // 8):
            cmds = []
            for _ in range(r.choice([1, 1, 4, 16])):
                while True:
                    cm = gen_cmd(r, keys, strkeys)
                    # (a ZADD with too few arguments fails its arity check, and those always go to main)
                    if cm[0] in NATIVE and not (cm[0] == "ZADD" and len(cm) < 4):
                        break
                cmds.append(cm)
            c.send(cmds)
            for cm in cmds:
                reps.append(norm(cm, c.read()))
            sent += len(cmds)
        conns.append(c)  # closed after the counters are read: a close is a handoff (main closes the socket)
        outs.append(reps)
    i1 = info(adm, "owners")
    adm.close()
    for c in conns:
        c.close()
    d = lambda k: int(i1.get(k, "-1")) - int(i0.get(k, "-1"))
    return outs, sent, d("owner_setzset_commands"), d("owner_handoffs_to_main")


def lockstep(srv, a, seed):
    """S connections take turns on shared keys in a fixed order."""
    r = random.Random(seed * 7 + 3)
    keys = make_keys("shared", 6) + [b"shared-%d" % j for j in range(6)]
    strkeys = [b"{shared:0}k"]
    conns = [Conn(srv.oport + (j % srv.n)) for j in range(a.lock_conns - 1)] + [Conn(srv.port)]
    reps = []
    for _ in range(a.lock_ops):
        c = r.choice(conns)
        cmd = gen_cmd(r, keys, strkeys)
        reps.append(norm(cmd, c.cmd(*cmd)))
    for c in conns:
        c.close()
    return reps


def dump(srv):
    """The keyspace: key -> (type, encoding, has ttl, value). Expired keys are removed first (TYPE)."""
    c = Conn(srv.port)
    keys, cur = [], b"0"
    while True:
        cur, ks = c.cmd("SCAN", cur, "COUNT", "1000")
        keys += ks
        if cur == b"0":
            break
    d = {}
    for k in sorted(set(keys)):
        t = c.cmd("TYPE", k)
        if t == "none":
            continue
        enc = c.cmd("OBJECT", "ENCODING", k)
        ttl = c.cmd("PTTL", k)
        if t == "set":
            v = sorted(c.cmd("SMEMBERS", k))
        elif t == "zset":
            v = c.cmd("ZRANGE", k, "0", "-1", "WITHSCORES")
        elif t == "string":
            v = c.cmd("GET", k)
        else:
            v = None
        d[k] = (t, enc, ttl > 0, v)
    dig = c.cmd("DEBUG", "DIGEST")
    c.close()
    return d, dig


def diff_dumps(a, b):
    ka, kb = set(a), set(b)
    out = []
    for k in sorted(ka ^ kb)[:3]:
        out.append(("only in " + ("cand" if k in ka else "ref"), k))
    for k in sorted(ka & kb):
        if a[k] != b[k]:
            out.append((k, a[k][:3], b[k][:3]))
            if len(out) > 5:
                break
    return out


def setzset_native(srv):
    c = Conn(srv.port)
    i = info(c, "owners")
    c.close()
    return int(i.get("owner_setzset_commands", "-1")), int(i.get("owner_native_commands", "0"))


def wait_repl(prim, rep, secs=60):
    p, q = Conn(prim.port), Conn(rep.port)
    t0 = time.time()
    try:
        while time.time() - t0 < secs:
            po = int(info(p, "replication")["master_repl_offset"])
            ri = info(q, "replication")
            if ri.get("master_link_status") == "up" and int(ri.get("slave_repl_offset", "-1")) == po:
                return True, po
            time.sleep(0.2)
        return False, (po, ri.get("master_link_status"), ri.get("slave_repl_offset"))
    finally:
        p.close()
        q.close()


SUITES = {
    "plain": ["--set-max-listpack-entries", "16", "--set-max-intset-entries", "24", "--zset-max-listpack-entries", "16"],
    "mm": ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"],
    "aofrepl": ["--appendonly", "yes", "--appendfsync", "everysec", "--set-max-listpack-entries", "32",
                "--zset-max-listpack-entries", "32"],
    "aofresp": ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp",
                "--set-max-listpack-entries", "16", "--zset-max-listpack-entries", "16"],
}


def start_replica(a, name, port, prim, extra):
    rep = Server(a, name, a.bin if "cand" in name else a.ref_bin, port, 0, "", a.replica_cpu,
                 [x for x in extra if x not in ("--appendonly", "yes", "everysec", "always", "--appendfsync")] +
                 ["--appendonly", "no"], 0)
    rep.start()
    q = Conn(rep.port)
    q.cmd("REPLICAOF", "127.0.0.1", str(prim.port))
    q.close()
    return rep


def values(d):
    """A dump without encodings: (type, has ttl, value)."""
    return {k: (v[0], v[2], v[3]) for k, v in d.items()}


def enc_diff(d1, d2):
    """Keys whose encoding differs between two dumps of the same data (both must hold the same keys)."""
    return sorted(k for k in d1 if k in d2 and d1[k][1] != d2[k][1])


def suite(a, name, seed):
    extra = SUITES[name]
    cand = Server(a, name + "-cand", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra, a.n)
    # the reference: the parent's main path for every set / sorted-set command
    ref = Server(a, name + "-ref", a.ref_bin, a.ref_port, a.ref_owner_port, a.ref_cpus, a.ref_main_cpu, extra, a.n)
    reps = []
    cand.start()
    ref.start()
    try:
        if name == "aofrepl":
            # a replica on each (no owner threads: a replica's owners are vetoed anyway)
            reps = [start_replica(a, name + "-cand-replica", a.replica_port, cand, extra),
                    start_replica(a, name + "-ref-replica", a.replica_port + 1, ref, extra)]
            ok = wait_repl(cand, reps[0])[0] and wait_repl(ref, reps[1])[0]
            check(name + ".replica_synced", ok)
        # native: the gene's commands on each owner's own keys run on the owners, no handoff
        nc, nsent, nz, nh = native_phase(cand, a, seed)
        nr, _, _, _ = native_phase(ref, a, seed)
        nm = [(i, j) for i in range(len(nc)) for j in range(len(nc[i])) if nc[i][j] != nr[i][j]]
        check(name + ".native.replies_equal", not nm and [len(x) for x in nc] == [len(x) for x in nr],
              "%d commands, mismatches %d %s" % (nsent, len(nm), ("cand %r ref %r" % (nc[nm[0][0]][nm[0][1]], nr[nm[0][0]][nm[0][1]])) if nm else ""))
        if name == "mm":  # SMEMBERS / ZRANGE of a hashtable or a tree go to main under maxmemory (with the rest of
            # that pipeline)
            check(name + ".native.owners_ran_them", nz >= 0.3 * nsent, "owner_setzset_commands +%d of %d, handoffs +%d" % (nz, nsent, nh))
        else:
            check(name + ".native.owners_ran_them", nz == nsent and nh == 0, "owner_setzset_commands +%d of %d, handoffs +%d" % (nz, nsent, nh))
        n0, _ = setzset_native(cand)
        out_c, sent = disjoint(cand, a, seed)
        out_r, _ = disjoint(ref, a, seed)
        mism = [(i, j) for i in range(a.threads) for j in range(min(len(out_c[i]), len(out_r[i])))
                if out_c[i][j] != out_r[i][j]]
        lens = [len(x) for x in out_c] == [len(x) for x in out_r]
        det = ""
        if mism:
            i, j = mism[0]
            det = "conn %d reply %d: cand %r ref %r" % (i, j, out_c[i][j], out_r[i][j])
        check(name + ".disjoint.replies_equal", lens and not mism,
              "%d conns, %d commands, mismatches %d %s" % (a.threads, sent, len(mism), det))
        lc = lockstep(cand, a, seed)
        lr = lockstep(ref, a, seed)
        lm = [j for j in range(len(lc)) if lc[j] != lr[j]]
        check(name + ".lockstep.replies_equal", not lm and len(lc) == len(lr),
              "%d commands, mismatches %d %s" % (len(lc), len(lm), ("#%d cand %r ref %r" % (lm[0], lc[lm[0]], lr[lm[0]])) if lm else ""))
        n1, nat = setzset_native(cand)
        rn, _ = setzset_native(ref)
        check(name + ".mixed.owners_ran_setzset", n1 - n0 > sent * 0.1 and rn == -1,
              "cand owner_setzset_commands +%d over ~%d mixed commands (native total %d); ref field %s" %
              (n1 - n0, sent, nat, "absent" if rn == -1 else rn))
        dc, gc = dump(cand)
        dr, gr = dump(ref)
        ntypes = {}
        for v in dc.values():
            ntypes[(v[0], v[1])] = ntypes.get((v[0], v[1]), 0) + 1
        check(name + ".keyspace.equal", dc == dr and gc == gr,
              "%d keys %s digest %s; diff %s" % (len(dc), sorted(ntypes.items()), gc == gr, diff_dumps(dc, dr)))
        # Replicas and AOF reloads: the same data; encodings may differ from the primary's only where the parent's
        # do too (stock: a no-op ZADD can convert a listpack before it finds nothing to add, and is not propagated)
        if reps:
            okc, offc = wait_repl(cand, reps[0])
            okr, _ = wait_repl(ref, reps[1])
            dpc, gpc = dump(reps[0])
            dpr, gpr = dump(reps[1])
            check(name + ".replica.keyspace_equal", okc and okr and values(dpc) == values(dc) and gpc == gc,
                  "offset %s, %d keys, digest %s; diff %s" % (offc, len(dpc), gpc == gc, diff_dumps(values(dc), values(dpc))))
            ec, er = enc_diff(dc, dpc), enc_diff(dr, dpr)
            check(name + ".replica.encodings_as_parent", ec == er,
                  "keys whose replica encoding differs from the primary's: cand %d %s, parent %d" % (len(ec), ec[:3], len(er)))
        if "--appendonly" in extra:
            time.sleep(1.5)
            reloads = []
            for s in (cand, ref):
                s.stop()
                # reload the AOF this server wrote (the candidate's: its owners' records) into a fresh server
                rdir = os.path.join(a.dir, s.name + "-reload")
                shutil.rmtree(rdir, ignore_errors=True)
                shutil.copytree(s.dir, rdir)
                os.remove(os.path.join(rdir, "log"))
                rl = Server(a, s.name + "-reload", s.bin, s.port, s.oport, s.cpus, s.main_cpu, extra, a.n)
                rl.start(fresh=False)
                reloads.append((rl,) + dump(rl))
                check(name + ".aof.reload_no_crash." + s.name, rl.crashed() == 0)
                rl.stop()
                shutil.rmtree(os.path.join(rdir, "appendonlydir"), ignore_errors=True)
            (_, dlc, glc), (_, dlr, glr) = reloads
            check(name + ".aof.reload_keyspace_equal", values(dlc) == values(dc) and glc == gc,
                  "%d keys, digest %s; diff %s" % (len(dlc), glc == gc, diff_dumps(values(dc), values(dlc))))
            ec, er = enc_diff(dc, dlc), enc_diff(dr, dlr)
            check(name + ".aof.reload_encodings_as_parent", ec == er,
                  "keys whose reloaded encoding differs from the live one: cand %d %s, parent %d" % (len(ec), ec[:3], len(er)))
        check(name + ".no_crash", cand.crashed() == 0 and ref.crashed() == 0 and all(r.crashed() == 0 for r in reps),
              "cand %d ref %d" % (cand.crashed(), ref.crashed()))
    finally:
        for s in [cand, ref] + reps:
            s.stop()
        for d in os.listdir(a.dir):
            shutil.rmtree(os.path.join(a.dir, d, "appendonlydir"), ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=24600)
    ap.add_argument("--owner-port", type=int, default=24610)
    ap.add_argument("--ref-port", type=int, default=24601)
    ap.add_argument("--ref-owner-port", type=int, default=24620)
    ap.add_argument("--replica-port", type=int, default=24602)
    ap.add_argument("--replica-cpu", default="39")
    ap.add_argument("--cpus", default="28-31")
    ap.add_argument("--main-cpu", default="32")
    ap.add_argument("--bg-cpu", default="33")
    ap.add_argument("--ref-cpus", default="34-37")
    ap.add_argument("--ref-main-cpu", default="38")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--keys", type=int, default=40)
    ap.add_argument("--ops", type=int, default=6000)
    ap.add_argument("--lock-conns", type=int, default=6)
    ap.add_argument("--lock-ops", type=int, default=4000)
    ap.add_argument("--native-ops", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--suites", default="plain,mm,aofrepl,aofresp")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    t0 = time.time()
    for i, s in enumerate(a.suites.split(",")):
        try:
            suite(a, s, a.seed + i)
        except Exception as e:  # a crash or a hang is a FAIL, not a skip
            check(s + ".ran", False, repr(e))
    fails = [r for r in RESULTS if not r["ok"]]
    print("RLSETZSET " + json.dumps(dict(passed=len(RESULTS) - len(fails), failed=len(fails),
                                         secs=round(time.time() - t0, 1))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
