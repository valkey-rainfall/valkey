#!/usr/bin/env python3
"""RL-g1a-native-hash: owners run single-key hash commands natively. Checked against the parent's main path.

Suites (each starts its own servers on 127.0.0.1, stops them by port or by the pid it recorded):
  model       random HSET / HMSET / HGET / HMGET / HDEL / HEXISTS / HLEN / HINCRBY / HGETALL / EXPIRE / PEXPIRE / DEL,
              type changes and type errors, wrong arity, overflow, listpack -> hashtable conversions, field TTLs
              (HPEXPIRE / HPERSIST: the volatile-table rule), MULTI blocks, SELECT 1, pipelines, over many keys and
              connections (the key's owner port, other owners' ports, the main port), sent in lockstep to --bin and
              --ref-bin (both owner-threads N; the parent runs hash commands on main): every reply equal, then the final
              keyspace (type, encoding, value, TTL, field TTLs) and commandstats / errorstats / hits / misses equal;
              owner_hash_commands shows the owners ran them
  model-mm    the same with maxmemory 1gb allkeys-lru on both (the owners' headroom checks for hash commands)
  concurrent  many connections HINCRBY / HSET the same hashes at once from every port: final sums exact, reads
              monotonic per connection and never below the connection's own last write; MULTI with HINCRBY
              atomic against the hammer
  notify      keyspace notifications on: owner-port HSET / HINCRBY / HDEL are notified (the veto still holds)
  repl        --bin primary (owners N) + a --bin replica; model writes + concurrent hash writers; after the replica
              caught up its keyspace equals the primary's. Also with appendonly everysec (compact AOF, owner_repl_split)
  aof         appendonly always (kill -9) and everysec (SHUTDOWN NOSAVE), model writes + concurrent writers + a
              BGREWRITEAOF midway; reload into a fresh --bin and a fresh --ref-bin: keyspace equals the one before
Prints PASS/FAIL lines and 'RLHASHCHECK {json}'; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
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
        if t in (b"$", b"="):
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t in (b"*", b">"):
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"%":
            n = int(rest)
            out = []
            for _ in range(n):
                out.append(self.read())
                out.append(self.read())
            return out
        if t == b"_":
            return None
        raise ValueError("bad reply %r" % line)

    def cmd(self, *args):
        self.send([args])
        return self.read()

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(dict(check=name, ok=bool(ok), detail=str(detail)[:400]))
    print(("PASS " if ok else "FAIL ") + name + (": " + str(detail)[:600] if detail != "" else ""), flush=True)
    return ok


def info(c, section):
    raw = c.cmd("INFO", section)
    raw = raw.decode() if isinstance(raw, bytes) else raw
    out = {}
    for line in raw.split("\r\n"):
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k] = v
    return out


def ival(d, k):
    try:
        return int(d.get(k, 0))
    except ValueError:
        return 0


# ------------------------------------------------------------------------------------------------ servers

class Server:
    def __init__(self, a, name, bin_, port, oport, cpus, main_cpu, extra=(), n=None, wipe=True):
        self.name, self.bin, self.port, self.oport = name, bin_, port, oport
        self.n = a.n if n is None else n
        self.dir = os.path.join(a.dir, name)
        if wipe and os.path.isdir(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir, exist_ok=True)
        self.cpus = cpus
        self.main_cpu = main_cpu
        self.extra = list(extra)
        self.a = a
        self.pid = None

    def start(self, wait_load=True):
        a = self.a
        args = ["taskset", "-c", a.all_cpus, self.bin, "--port", str(self.port), "--bind", "127.0.0.1",
                "--owner-threads", str(self.n), "--owner-port", str(self.oport), "--owner-cpulist", self.cpus,
                "--server-cpulist", str(self.main_cpu), "--bio-cpulist", str(a.bio_cpu),
                "--aof-rewrite-cpulist", str(a.bio_cpu), "--bgsave-cpulist", str(a.bio_cpu), "--save", "",
                "--enable-debug-command", "yes", "--daemonize", "yes", "--dir", self.dir,
                "--logfile", os.path.join(self.dir, "log"), "--pidfile", os.path.join(self.dir, "pid")]
        if "--appendonly" not in self.extra:
            args += ["--appendonly", "no"]
        args += self.extra
        subprocess.run(args, check=True)
        for _ in range(300):
            try:
                c = Conn(self.port, timeout=5)
                r = c.cmd("PING")
                if r == "PONG":
                    if wait_load:
                        while ival(info(c, "persistence"), "loading"):
                            time.sleep(0.05)
                    c.close()
                    break
                c.close()
            except (OSError, EOFError):
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError("%s did not start" % self.name)
        with open(os.path.join(self.dir, "pid")) as f:
            self.pid = int(f.read().strip())
        return self

    def owners(self):
        c = Conn(self.port)
        d = info(c, "owners")
        c.close()
        rng = []
        for i in range(self.n):
            v = d["owner%d" % i]
            kv = dict(x.split("=", 1) for x in v.split(","))
            lo, hi = kv["slots"].split("-")
            rng.append((int(kv["port"]), int(lo), int(hi)))
        return rng

    def stop(self, nosave=True):
        try:
            c = Conn(self.port, timeout=10)
            c.send([("SHUTDOWN", "NOSAVE") if nosave else ("SHUTDOWN",)])
            try:
                c.read()
            except (OSError, EOFError):
                pass
            c.close()
        except (OSError, EOFError):
            pass
        self._wait_gone()

    def kill9(self):
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self._wait_gone()

    def _wait_gone(self):
        for _ in range(200):
            try:
                if self.pid:
                    os.kill(self.pid, 0)
                time.sleep(0.05)
            except ProcessLookupError:
                break
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                time.sleep(0.05)
            except OSError:
                break

    def crash_lines(self):
        try:
            with open(os.path.join(self.dir, "log"), errors="replace") as f:
                return sum(1 for l in f if any(x in l for x in ("ASSERT", "crashed", "Guru", "BUG REPORT")))
        except OSError:
            return -1


# ------------------------------------------------------------------------------------------------ the model

class Keys:
    """Hash keys spread over every owner (and a few string keys), the same names for every server."""

    def __init__(self, owners, nhash=240, nstr=24):
        self.owners = owners  # [(port, lo, hi)]
        self.hash, self.str_ = [], []
        i = 0
        while len(self.hash) < nhash:
            self.hash.append(b"h:%d" % i)
            i += 1
        self.str_ = [b"s:%d" % j for j in range(nstr)]
        self.big = set(self.hash[::37])  # may grow past hash-max-listpack-entries
        self.ttl_keys = self.hash[::10]  # the only keys given long field TTLs (their tables go to main)

    def owner_of(self, key):
        s = key_slot(key)
        for i, (_, lo, hi) in enumerate(self.owners):
            if lo <= s <= hi:
                return i
        return -1


def rval(rng):
    r = rng.random()
    if r < 0.15:
        return str(rng.randrange(-500, 500)).encode()
    if r < 0.20:
        return b"x" * rng.randrange(65, 200)  # past hash-max-listpack-value: a hashtable from then on
    return b"v%d-" % rng.randrange(1000) + b"y" * rng.randrange(0, 40)


def rfield(rng, key, keys):
    if key in keys.big and rng.random() < 0.6:
        return b"g%d" % rng.randrange(400)
    return b"f%d" % rng.randrange(14)


class Gen:
    """Random ops on keys that are not inside a short-expiry window (both servers expire them by then)."""

    def __init__(self, rng, keys, ncon, field_ttl=True, writes_only=False):
        self.rng, self.keys, self.ncon = rng, keys, ncon
        self.blocked = {}
        self.hlen_unsafe = set()
        self.field_ttl = field_ttl
        self.writes_only = writes_only

    def key(self, pool=None):
        now = time.time()
        pool = pool or self.keys.hash
        for _ in range(50):
            k = self.rng.choice(pool)
            if self.blocked.get(k, 0) <= now:
                return k
        return None

    def op(self):
        """(args, normalize) for one op on a free key, or None."""
        rng = self.rng
        k = self.key()
        if k is None:
            return None
        r = rng.random()
        ro = not self.writes_only
        if r < 0.20:
            args = [b"HSET", k]
            for _ in range(rng.choice([1, 1, 1, 2, 3, 8 if k in self.keys.big else 2])):
                args += [rfield(rng, k, self.keys), rval(rng)]
            if rng.random() < 0.03:
                args = args[:3]  # wrong arity: the command's own error
            return args, None
        if r < 0.25:
            args = [rng.choice([b"HMSET", b"hmset"]), k]
            for _ in range(rng.choice([1, 2, 4])):
                args += [rfield(rng, k, self.keys), rval(rng)]
            return args, None
        if r < 0.36:
            inc = rng.choice([1, -1, 7, rng.randrange(-1000, 1000), 9223372036854775807]) if rng.random() < 0.97 \
                else b"notanumber"
            return [b"HINCRBY", k, b"f%d" % rng.randrange(4) if rng.random() < 0.8 else rfield(rng, k, self.keys),
                    inc], None
        if r < 0.43:
            return [b"HDEL", k] + [rfield(rng, k, self.keys) for _ in range(rng.choice([1, 1, 2, 3]))], None
        if r < 0.53 and ro:
            return [b"HGET", k, rfield(rng, k, self.keys)], None
        if r < 0.61 and ro:
            return [b"HMGET", k] + [rfield(rng, k, self.keys) for _ in range(rng.choice([1, 2, 3, 4]))], None
        if r < 0.65 and ro:
            return [b"HEXISTS", k, rfield(rng, k, self.keys)], None
        if r < 0.69 and ro:
            if k in self.hlen_unsafe:
                return [b"HEXISTS", k, b"f0"], None
            return [b"HLEN", k], None
        if r < 0.75 and ro:
            return [b"HGETALL", k], "pairs"
        if r < 0.77:
            return [b"EXPIRE", k, 100000], None
        if r < 0.78:
            return [b"PERSIST", k], None
        if r < 0.785:
            self.blocked[k] = time.time() + 0.12
            return [b"PEXPIRE", k, rng.randrange(1, 25)], None
        if r < 0.80:
            return [b"DEL", k], None
        if r < 0.81:
            return [b"SET", k, b"now-a-string"], None
        if r < 0.82 and ro:
            return [b"GET", k], None
        if r < 0.84:
            s = self.key(self.keys.str_)
            if s is None:
                return None
            return rng.choice([[b"SET", s, b"str"], [b"HSET", s, b"f", b"v"], [b"HGET", s, b"f"],
                               [b"HINCRBY", s, b"f", 1], [b"HGETALL", s], [b"HLEN", s], [b"HDEL", s, b"f"]]), None
        if r < 0.86 and ro:
            return [b"OBJECT", b"ENCODING", k], None
        if r < 0.87 and ro:
            return [b"TYPE", k], None
        if self.field_ttl and r < 0.885:
            k = self.key(self.keys.ttl_keys)
            if k is None:
                return None
            return [b"HPEXPIRE", k, 100000000, b"FIELDS", 1, rfield(rng, k, self.keys)], None
        if self.field_ttl and r < 0.90:
            k = self.key(self.keys.ttl_keys)
            if k is None:
                return None
            return [b"HPERSIST", k, b"FIELDS", 3, b"f0", b"f1", rfield(rng, k, self.keys)], None
        if self.field_ttl and r < 0.905:
            self.blocked[k] = time.time() + 0.12
            # HLEN counts expired fields until active expiry reclaims them (stock): no HLEN on this key from now on
            self.hlen_unsafe.add(k)
            return [b"HPEXPIRE", k, rng.randrange(1, 25), b"FIELDS", 1, b"f%d" % rng.randrange(4)], None
        if r < 0.92 and ro:
            return [b"HKEYS", k], "sorted"
        if r < 0.94:
            return [b"HSETNX", k, rfield(rng, k, self.keys), rval(rng)], None
        return [b"HSET", k, rfield(rng, k, self.keys), rval(rng)], None


def norm(reply, how):
    if isinstance(reply, Err):
        return ("ERR", str(reply))
    if how == "pairs" and isinstance(reply, list):
        return sorted(zip(reply[0::2], reply[1::2]))
    if how == "sorted" and isinstance(reply, list):
        return sorted(reply)
    return reply


def snapshot(port, dbs=(0, 1)):
    """{(db, key): (type, encoding, value, has_ttl, pttl, field_ttls)} through the main port."""
    c = Conn(port)
    out = {}
    for db in dbs:
        c.cmd("SELECT", db)
        cur = b"0"
        keys = []
        while True:
            r = c.cmd("SCAN", cur, "COUNT", 1000)
            cur, ks = r[0], r[1]
            keys += ks
            if cur == b"0":
                break
        for k in keys:
            t = c.cmd("TYPE", k)
            if t == "none":
                continue
            e = c.cmd("OBJECT", "ENCODING", k)
            if t == "hash":
                v = c.cmd("HGETALL", k)
                v = sorted(zip(v[0::2], v[1::2])) if isinstance(v, list) else v
                fields = [f for f, _ in v] if isinstance(v, list) else []
                ft = c.cmd("HPTTL", k, "FIELDS", len(fields), *fields) if fields else []
                ft = tuple(-1 if (isinstance(x, int) and x < 0) else 1 for x in ft) if isinstance(ft, list) else ft
            elif t == "string":
                v, ft = c.cmd("GET", k), ()
            else:
                v, ft = c.cmd("DEBUG", "DIGEST-VALUE", k), ()
            p = c.cmd("PTTL", k)
            out[(db, k)] = (t, e, v, p >= 0, p, ft)
    c.cmd("SELECT", 0)
    c.close()
    return out


def same_keyspace(a, b, tol_ms=5000, enc=True):
    """Differences between two snapshots (TTLs within tol_ms; enc=False: the encoding may differ, as after a
    reload, where an RDB base loads a small hashtable-encoded hash as a listpack)."""
    diff = []
    for k in sorted(set(a) | set(b)):
        if k not in a or k not in b:
            diff.append((k, "missing on " + ("first" if k not in a else "second")))
            continue
        x, y = a[k], b[k]
        if (x[0], x[2], x[3]) != (y[0], y[2], y[3]) or (enc and x[1] != y[1]) or x[5] != y[5] or \
                (x[3] and abs(x[4] - y[4]) > tol_ms):
            diff.append((k, x[:2], y[:2], x[3:5], y[3:5]))
    return diff


def conn_ports(srv_port, owners, n_per_owner=2, n_main=2):
    ports = []
    for p, _, _ in owners:
        ports += [p] * n_per_owner
    ports += [srv_port] * n_main
    return ports


def pick_conn(rng, keys, key, ports, owners):
    """A connection index: mostly one to the key's owner, else another owner's or the main port."""
    o = keys.owner_of(key)
    r = rng.random()
    if r < 0.7 and o >= 0:
        cands = [i for i, p in enumerate(ports) if p == owners[o][0]]
    elif r < 0.9:
        cands = [i for i, p in enumerate(ports) if p != owners[o][0] and p in [x[0] for x in owners]]
        cands = cands or list(range(len(ports)))
    else:
        cands = [i for i, p in enumerate(ports) if p not in [x[0] for x in owners]]
    return rng.choice(cands)


def stats_view(port):
    c = Conn(port)
    cs = info(c, "commandstats")
    es = info(c, "errorstats")
    st = info(c, "stats")
    c.close()
    skip = ("cmdstat_info", "cmdstat_scan", "cmdstat_select", "cmdstat_type", "cmdstat_object|encoding",
            "cmdstat_hgetall", "cmdstat_pttl", "cmdstat_hpttl", "cmdstat_get", "cmdstat_debug|digest-value",
            "cmdstat_ping", "cmdstat_dbsize", "cmdstat_config|set", "cmdstat_config|get")
    out = {}
    for k, v in cs.items():
        if k in skip:
            continue
        kv = dict(x.split("=", 1) for x in v.split(","))
        out[k] = (kv.get("calls"), kv.get("rejected_calls"), kv.get("failed_calls"))
    for k, v in es.items():
        out[k] = v
    for k in ("total_error_replies", "keyspace_hits", "keyspace_misses"):
        out[k] = st.get(k)
    return out


def run_model(a, tag, extra):
    rng = random.Random(a.seed)
    mine = Server(a, tag + "-mine", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra).start()
    ref = Server(a, tag + "-ref", a.ref_bin, a.port + 10, a.owner_port + 20, a.ref_cpus, a.ref_main_cpu, extra).start()
    try:
        own_m, own_r = mine.owners(), ref.owners()
        check(tag + ".topology_same", [(lo, hi) for _, lo, hi in own_m] == [(lo, hi) for _, lo, hi in own_r],
              own_m)
        keys = Keys(own_m)
        pm, pr = conn_ports(mine.port, own_m), conn_ports(ref.port, own_r)
        cm, cr = [Conn(p) for p in pm], [Conn(p) for p in pr]
        db = [0] * len(pm)
        adm_m, adm_r = Conn(mine.port), Conn(ref.port)
        o0 = info(adm_m, "owners")
        for s in keys.str_:
            adm_m.cmd("SET", s, "str")
            adm_r.cmd("SET", s, "str")
        gen = Gen(rng, keys, len(pm))
        nops = mismatches = hash_ops = multis = pipes = 0
        first_bad = []
        t_end = time.time() + a.secs
        while time.time() < t_end:
            r = rng.random()
            if r < 0.70:
                op = gen.op()
                if op is None:
                    continue
                args, how = op
                i = pick_conn(rng, keys, args[1], pm, own_m)
                batch = [(args, how)]
            elif r < 0.93:
                i = rng.randrange(len(pm))
                batch = [x for x in (gen.op() for _ in range(rng.randrange(2, 17))) if x]
                if not batch:
                    continue
                if rng.random() < 0.3:  # non-native commands mid-pipeline: handoffs between native ones
                    batch.insert(rng.randrange(len(batch) + 1), ([b"LPUSH", b"l:%d" % rng.randrange(20), b"x"], None))
                    ek = gen.key()
                    if ek is not None:
                        batch.insert(rng.randrange(len(batch) + 1), ([b"EXISTS", ek], None))
                pipes += 1
            elif r < 0.97:
                op = gen.op()
                if op is None:
                    continue
                i = pick_conn(rng, keys, op[0][1], pm, own_m)
                inner = [x for x in (gen.op() for _ in range(rng.randrange(1, 5))) if x and x[1] is None]
                inner.insert(0, op if op[1] is None else ([b"HLEN", op[0][1]], None))
                if rng.random() < 0.3:
                    inner.append(([b"SET", b"ms:%d" % rng.randrange(50), b"m"], None))
                batch = [([b"MULTI"], None)] + inner + [([b"EXEC"], None)]
                multis += 1
            else:
                i = rng.randrange(len(pm))
                db[i] = 1 - db[i]
                batch = [([b"SELECT", db[i]], None)]
            for args, _ in batch:
                if args[0].upper() in (b"HSET", b"HMSET", b"HGET", b"HMGET", b"HDEL", b"HEXISTS", b"HLEN",
                                       b"HINCRBY", b"HGETALL"):
                    hash_ops += 1
            cm[i].send([x[0] for x in batch])
            cr[i].send([x[0] for x in batch])
            for args, how in batch:
                x, y = cm[i].read(), cr[i].read()
                nops += 1
                if norm(x, how) != norm(y, how):
                    mismatches += 1
                    if len(first_bad) < 5:
                        first_bad.append((i, pm[i], [str(z)[:40] for z in args], str(x)[:120], str(y)[:120]))
        o1 = info(adm_m, "owners")
        native_hash = ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands")
        check(tag + ".replies_equal", mismatches == 0 and nops > 1000,
              "ops %d (pipelines %d, MULTI %d, hash commands %d), mismatches %d %s" %
              (nops, pipes, multis, hash_ops, mismatches, first_bad))
        check(tag + ".hash_native_on_owners", native_hash >= hash_ops * 0.1,
              "owner_hash_commands +%d of %d hash commands sent (pipelines on random ports hand off); veto %s; "
              "maxmemory_to_main %s" % (native_hash, hash_ops, o1.get("owner_veto"), o1.get("owner_maxmemory_to_main")))
        # Single hash commands on the key's owner port (keys without field TTLs): the owner runs nearly all.
        hset = (b"HSET", b"HMSET", b"hmset", b"HGET", b"HMGET", b"HDEL", b"HEXISTS", b"HLEN", b"HINCRBY", b"HGETALL")
        plain = [k for k in keys.hash if k not in keys.ttl_keys]
        g2 = Gen(rng, keys, len(pm), field_ttl=False)
        # The model phase's short-expiry state carries over: a key it gave a 1-25 ms TTL stays off limits for its
        # 0.12 s window (blocked), and a hash with a field that got a short TTL never gets HLEN (hlen_unsafe: HLEN counts
        # an expired field until active expiry reclaims it, which each server does at its own time). Without this the
        # phase could hit either right after the model phase and see the two servers expire at different moments.
        g2.blocked = gen.blocked
        g2.hlen_unsafe = gen.hlen_unsafe
        sent = bad2 = 0
        bad2_first = []
        o2 = info(adm_m, "owners")
        while sent < 3000:
            op = g2.op()
            if op is None or op[0][0] not in hset or op[0][1] not in plain:
                continue
            args, how = op
            o = keys.owner_of(args[1])
            i = next(j for j, p in enumerate(pm) if p == own_m[o][0])
            x = cm[i].cmd(*args)
            y = cr[i].cmd(*args)
            sent += 1
            if norm(x, how) != norm(y, how):
                bad2 += 1
                if len(bad2_first) < 3:
                    bad2_first.append((args, x, y))
        o3 = info(adm_m, "owners")
        nat2 = ival(o3, "owner_hash_commands") - ival(o2, "owner_hash_commands")
        thr = 0.3 if tag.endswith("mm") else 0.9  # under maxmemory value reads of hashtable hashes go to main
        check(tag + ".owner_port_hash_native", nat2 >= thr * sent and bad2 == 0,
              "owner_hash_commands +%d of %d single hash commands on the key's owner port, mismatches %d, "
              "handoffs +%d %s" % (nat2, sent, bad2,
                                   ival(o3, "owner_handoffs_to_main") - ival(o2, "owner_handoffs_to_main"),
                                   repr(bad2_first)[:600] if bad2_first else ""))
        check(tag + ".ref_runs_them_on_main", "owner_hash_commands" not in info(adm_r, "owners"),
              "parent has no owner_hash_commands")
        time.sleep(0.2)  # every short expiry (key or field) is past on both
        sm, sr = stats_view(mine.port), stats_view(ref.port)
        d = {k: (sm.get(k), sr.get(k)) for k in set(sm) | set(sr) if sm.get(k) != sr.get(k)}
        check(tag + ".commandstats_errorstats_equal", not d, d)
        km, kr = snapshot(mine.port), snapshot(ref.port)
        nh = sum(1 for v in km.values() if v[0] == "hash")
        enc = {}
        for v in km.values():
            if v[0] == "hash":
                enc[v[1]] = enc.get(v[1], 0) + 1
        diff = same_keyspace(km, kr)
        check(tag + ".final_keyspace_equal", not diff and nh > 50,
              "keys %d (hashes %d, encodings %s, with field TTLs %d), diffs %d %s" %
              (len(km), nh, enc, sum(1 for v in km.values() if 1 in v[5]), len(diff), diff[:4]))
        for c in cm + cr + [adm_m, adm_r]:
            c.close()
    finally:
        mine.stop()
        ref.stop()
        check(tag + ".no_crash", mine.crash_lines() == 0 and ref.crash_lines() == 0,
              (mine.crash_lines(), ref.crash_lines()))


# ------------------------------------------------------------------------------------------------ concurrency

def hammer_writers(srv_port, owners, keys, secs, nthreads=12, tag="", errs=None, uniq=True):
    """Each thread on its own connection (ports cycle over owners and main) HINCRBYs shared fields and HSETs unique
    ones, pipelined. Returns the expected per-(key, field) sums and unique-field counts, and read violations."""
    ports = [p for p, _, _ in owners] + [srv_port]
    expect = {}
    lock = threading.Lock()
    viol = []
    uniq_n = {}
    stop_at = time.time() + secs

    def worker(t):
        rng = random.Random(1000 + t)
        c = Conn(ports[t % len(ports)])
        mine = {}
        last_read = {}
        n = 0
        try:
            while time.time() < stop_at:
                batch, meta = [], []
                for _ in range(rng.randrange(1, 9)):
                    k = rng.choice(keys)
                    f = b"c%d" % rng.randrange(4)
                    r = rng.random()
                    if r < 0.6:
                        batch.append((b"HINCRBY", k, f, t + 1))
                        meta.append(("inc", k, f))
                    elif r < 0.85:
                        batch.append((b"HGET", k, f))
                        meta.append(("get", k, f))
                    elif uniq:
                        n += 1
                        batch.append((b"HSET", k, b"u%d-%d" % (t, n), b"v"))
                        meta.append(("uniq", k, None))
                    else:
                        batch.append((b"HMGET", k, f, b"c0"))
                        meta.append(("hmget", k, f))
                c.send(batch)
                for (kind, k, f), args in zip(meta, batch):
                    r = c.read()
                    if isinstance(r, Err):
                        viol.append(("error", args, str(r)))
                        continue
                    if kind == "inc":
                        mine[(k, f)] = mine.get((k, f), 0) + t + 1
                        if r < last_read.get((k, f), 0):
                            viol.append(("incr below an earlier read", k, f, r, last_read[(k, f)]))
                        last_read[(k, f)] = r
                    elif kind == "get":
                        v = int(r) if r is not None else 0
                        if v < last_read.get((k, f), 0):
                            viol.append(("read went back", k, f, v, last_read[(k, f)]))
                        last_read[(k, f)] = v
                    elif kind == "uniq":
                        if r != 1:
                            viol.append(("uniq HSET not new", k, r))
                        with lock:
                            uniq_n[k] = uniq_n.get(k, 0) + 1
        except Exception as e:  # noqa: BLE001
            viol.append(("exception", t, repr(e)))
        finally:
            c.close()
        with lock:
            for kf, v in mine.items():
                expect[kf] = expect.get(kf, 0) + v

    ths = [threading.Thread(target=worker, args=(t,)) for t in range(nthreads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    return expect, uniq_n, viol


def run_concurrent(a):
    tag = "concurrent"
    mine = Server(a, tag, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu).start()
    try:
        owners = mine.owners()
        adm = Conn(mine.port)
        keys = []
        for i in range(len(owners)):
            j = 0
            while len([k for k in keys if Keys(owners).owner_of(k) == i]) < 2:
                k = b"ch:%d:%d" % (i, j)
                if Keys(owners).owner_of(k) == i:
                    keys.append(k)
                j += 1
        o0 = info(adm, "owners")
        # MULTI atomicity against the hammer: one connection on the key's owner port
        k0 = keys[0]
        mviol = []
        mdone = [0]
        stop = threading.Event()

        def multi_loop():
            c = Conn(owners[Keys(owners).owner_of(k0)][0])
            try:
                while not stop.is_set():
                    c.send([("MULTI",), ("HINCRBY", k0, "m", 1), ("HINCRBY", k0, "m", 1), ("HGET", k0, "m"),
                            ("HSET", k0, "mm", "x"), ("EXEC",)])
                    r = [c.read() for _ in range(6)]
                    ex = r[5]
                    if r[0] != "OK" or r[1:5] != ["QUEUED"] * 4 or not isinstance(ex, list) or \
                            ex[1] != ex[0] + 1 or int(ex[2]) != ex[1]:
                        mviol.append(r)
                    mdone[0] += 1
            finally:
                c.close()

        def m_hammer(port):
            c = Conn(port)
            try:
                while not stop.is_set():
                    c.send([("HINCRBY", k0, "m", 1)] * 16)
                    for _ in range(16):
                        c.read()
            finally:
                c.close()

        mths = [threading.Thread(target=multi_loop)] + \
               [threading.Thread(target=m_hammer, args=(p,)) for p in [owners[Keys(owners).owner_of(k0)][0],
                                                                       owners[-1][0], mine.port]]
        for th in mths:
            th.start()
        expect, uniq_n, viol = hammer_writers(mine.port, owners, keys, a.secs)
        stop.set()
        for th in mths:
            th.join()
        check(tag + ".multi_atomic", not mviol and mdone[0] > 20, "EXECs %d, violations %d %s" %
              (mdone[0], len(mviol), mviol[:2]))
        check(tag + ".reads_monotonic_no_errors", not viol, viol[:4])
        bad = []
        for (k, f), v in expect.items():
            got = adm.cmd("HGET", k, f)
            if int(got or 0) != v:
                bad.append((k, f, got, v))
        for k in keys:
            n = adm.cmd("HLEN", k)
            want = len([1 for (kk, _) in expect if kk == k]) + uniq_n.get(k, 0) + (2 if k == k0 else 0)
            if n != want:
                bad.append((k, "HLEN", n, want))
        o1 = info(adm, "owners")
        check(tag + ".final_sums_exact", not bad and expect, "fields %d, bad %s" % (len(expect), bad[:4]))
        check(tag + ".native_ran", ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands") > 1000,
              "owner_hash_commands +%d, handoffs +%d" % (
                  ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands"),
                  ival(o1, "owner_handoffs_to_main") - ival(o0, "owner_handoffs_to_main")))
        adm.close()
    finally:
        mine.stop()
        check(tag + ".no_crash", mine.crash_lines() == 0, mine.crash_lines())


def run_notify(a):
    tag = "notify"
    mine = Server(a, tag, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu).start()
    try:
        owners = mine.owners()
        keys = Keys(owners)
        k = next(x for x in keys.hash if keys.owner_of(x) == 0)
        adm = Conn(mine.port)
        sub = Conn(mine.port)
        sub.cmd("SUBSCRIBE", b"__keyspace@0__:" + k)
        oc = Conn(owners[0][0])
        r0 = oc.cmd("HSET", k, "a", "1")
        check(tag + ".native_before", info(adm, "owners").get("owner_veto") == "none" and r0 == 1)
        adm.cmd("CONFIG", "SET", "notify-keyspace-events", "KEA")
        time.sleep(0.05)
        r = [oc.cmd("HSET", k, "b", "2"), oc.cmd("HINCRBY", k, "a", 5), oc.cmd("HDEL", k, "b"),
             oc.cmd("HGET", k, "a")]
        evs = []
        sub.s.settimeout(2)
        try:
            for _ in range(3):
                m = sub.read()
                evs.append(m[2])
        except (OSError, socket.timeout):
            pass
        veto = info(adm, "owners").get("owner_veto")
        check(tag + ".owner_writes_notified", r == [1, 6, 1, b"6"] and evs == [b"hset", b"hincrby", b"hdel"] and
              veto == "all", (r, evs, veto))
        adm.cmd("CONFIG", "SET", "notify-keyspace-events", "")
        time.sleep(0.05)
        h0 = ival(info(adm, "owners"), "owner_hash_commands")
        for _ in range(20):
            oc.cmd("HGET", k, "a")
        h1 = ival(info(adm, "owners"), "owner_hash_commands")
        check(tag + ".native_after", h1 - h0 >= 15, "owner_hash_commands +%d of 20" % (h1 - h0))
        for c in (adm, sub, oc):
            c.close()
    finally:
        mine.stop()
        check(tag + ".no_crash", mine.crash_lines() == 0, mine.crash_lines())


# ------------------------------------------------------------------------------------------------ durability

def drive_writes(srv, secs, seed, conc=True, midway=None):
    """Model ops (writes and reads) on srv over its owner and main ports, plus concurrent hash writers."""
    rng = random.Random(seed)
    owners = srv.owners()
    keys = Keys(owners)
    ports = conn_ports(srv.port, owners)
    cs = [Conn(p) for p in ports]
    gen = Gen(rng, keys, len(ports))
    t_end = time.time() + secs
    th = None
    res = {}
    if conc:
        ck = [b"cw:%d" % j for j in range(8)]

        def bg():
            res["c"] = hammer_writers(srv.port, owners, ck, secs, nthreads=6, uniq=False)
        th = threading.Thread(target=bg)
        th.start()
    did_mid = False
    n = 0
    while time.time() < t_end:
        if midway and not did_mid and time.time() > t_end - secs / 2:
            midway()
            did_mid = True
        batch = [x for x in (gen.op() for _ in range(rng.randrange(1, 12))) if x]
        if not batch:
            continue
        i = pick_conn(rng, keys, batch[0][0][1], ports, owners)
        cs[i].send([b[0] for b in batch])
        for _ in batch:
            cs[i].read()
            n += 1
    if th:
        th.join()
    for c in cs:
        c.close()
    time.sleep(0.2)
    return n, res.get("c")


def wait_replica(prim, rep, timeout=60):
    cp, cr = Conn(prim.port), Conn(rep.port)
    t0 = time.time()
    ok = False
    po = ro = 0
    while time.time() - t0 < timeout:
        po = ival(info(cp, "replication"), "master_repl_offset")
        ir = info(cr, "replication")
        ro = ival(ir, "master_repl_offset")
        if ir.get("master_link_status") == "up" and ro == po and po > 0:
            time.sleep(0.3)
            if ival(info(cp, "replication"), "master_repl_offset") == po:
                ok = True
                break
        time.sleep(0.1)
    cp.close()
    cr.close()
    return ok, po, ro


def run_repl(a, tag, extra):
    prim = Server(a, tag + "-primary", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra).start()
    rep = Server(a, tag + "-replica", a.bin, a.port + 1, a.owner_port + 60, a.ref_cpus, a.ref_main_cpu).start()
    try:
        cr = Conn(rep.port)
        cr.cmd("REPLICAOF", "127.0.0.1", prim.port)
        for _ in range(200):
            if info(cr, "replication").get("master_link_status") == "up":
                break
            time.sleep(0.05)
        cp = Conn(prim.port)
        o0 = info(cp, "owners")
        n, conc = drive_writes(prim, a.secs, a.seed + 1)
        o1 = info(cp, "owners")
        ok, po, ro = wait_replica(prim, rep)
        sp, sr = snapshot(prim.port), snapshot(rep.port)
        diff = same_keyspace(sp, sr)
        ir = info(cp, "stats")
        check(tag + ".replica_equal", ok and not diff and len(sp) > 50,
              "ops %d, offsets %d / %d, keys %d, diffs %d %s, sync_full %s partial_ok %s" %
              (n, po, ro, len(sp), len(diff), diff[:3], ir.get("sync_full"), ir.get("sync_partial_ok")))
        check(tag + ".native_with_replica", ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands") > 1000
              and o1.get("owner_repl") == "yes",
              "owner_hash_commands +%d, owner_repl %s, split %s, veto %s" % (
                  ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands"), o1.get("owner_repl"),
                  o1.get("owner_repl_split"), o1.get("owner_veto")))
        if conc:
            expect, _, viol = conc
            rc = Conn(rep.port)
            bad = [(k, f, v) for (k, f), v in expect.items() if int(rc.cmd("HGET", k, f) or 0) != v]
            rc.close()
            check(tag + ".replica_sums_exact", not bad and not viol and expect, (len(expect), bad[:3], viol[:3]))
        cp.close()
        cr.close()
    finally:
        rep.stop()
        prim.stop()
        check(tag + ".no_crash", prim.crash_lines() == 0 and rep.crash_lines() == 0,
              (prim.crash_lines(), rep.crash_lines()))


def run_aof(a, tag, fsync, more=()):
    extra = ["--appendonly", "yes", "--appendfsync", fsync] + list(more)
    srv = Server(a, tag, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra).start()

    def rewrite():
        c = Conn(srv.port)
        c.cmd("BGREWRITEAOF")
        c.close()
    try:
        c = Conn(srv.port)
        o0 = info(c, "owners")
        n, conc = drive_writes(srv, a.secs, a.seed + 2, midway=rewrite)
        o1 = info(c, "owners")
        for _ in range(300):
            if not ival(info(c, "persistence"), "aof_rewrite_in_progress"):
                break
            time.sleep(0.1)
        fmt = info(c, "persistence").get("aof_incr_format_open")
        c.close()
        before = snapshot(srv.port)
        check(tag + ".native_with_aof", ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands") > 1000,
              "owner_hash_commands +%d, owner_aof_ops %s, veto %s, incr format %s" % (
                  ival(o1, "owner_hash_commands") - ival(o0, "owner_hash_commands"), o1.get("owner_aof_ops"),
                  o1.get("owner_veto"), fmt))
    finally:
        if fsync == "always":
            srv.kill9()
        else:
            srv.stop()
    copy = srv.dir + "-copy"
    if os.path.isdir(copy):
        shutil.rmtree(copy)
    shutil.copytree(srv.dir, copy)
    reloads = {}
    for name, binp, d in (("mine", a.bin, srv.dir), ("parent", a.ref_bin, copy)):
        rl = Server(a, os.path.basename(d), binp, a.port + 2, a.owner_port + 40, a.ref_cpus, a.ref_main_cpu, extra,
                    wipe=False).start()
        try:
            after = snapshot(rl.port)
            reloads[name] = after
            diff = same_keyspace(before, after, tol_ms=60000, enc=False)
            nenc = sum(1 for k in before if k in after and before[k][1] != after[k][1])
            check("%s.reload_%s_equal" % (tag, name), not diff and len(before) > 50,
                  "ops %d, keys %d, diffs %d %s (encodings changed by the reload: %d)" %
                  (n, len(before), len(diff), diff[:3], nenc))
            if conc:
                expect, _, viol = conc
                cc = Conn(rl.port)
                bad = [(k, f, v) for (k, f), v in expect.items() if int(cc.cmd("HGET", k, f) or 0) != v]
                cc.close()
                check("%s.reload_%s_sums_exact" % (tag, name), not bad and not viol, (bad[:3], viol[:3]))
        finally:
            rl.stop()
            check("%s.reload_%s_no_crash" % (tag, name), rl.crash_lines() == 0, rl.crash_lines())
    if len(reloads) == 2:
        diff = same_keyspace(reloads["mine"], reloads["parent"], tol_ms=60000)
        check(tag + ".reloads_mine_parent_equal_incl_encoding", not diff, (len(diff), diff[:3]))
    check(tag + ".no_crash", srv.crash_lines() == 0, srv.crash_lines())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--port", type=int, default=20079)
    ap.add_argument("--owner-port", type=int, default=20100)
    ap.add_argument("--cpus", default="0-3")
    ap.add_argument("--main-cpu", type=int, default=8)
    ap.add_argument("--ref-cpus", default="4-7")
    ap.add_argument("--ref-main-cpu", type=int, default=9)
    ap.add_argument("--bio-cpu", type=int, default=10)
    ap.add_argument("--all-cpus", default="0-19")
    ap.add_argument("--secs", type=float, default=20)
    ap.add_argument("--seed", type=int, default=20261116)
    ap.add_argument("--suites", default="model,model-mm,concurrent,notify,repl,repl-aof,aof-always,aof-everysec,aof-resp")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    for s in a.suites.split(","):
        print("INFO suite %s" % s, flush=True)
        try:
            if s == "model":
                run_model(a, s, [])
            elif s == "model-mm":
                run_model(a, s, ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"])
            elif s == "concurrent":
                run_concurrent(a)
            elif s == "notify":
                run_notify(a)
            elif s == "repl":
                run_repl(a, s, [])
            elif s == "repl-aof":
                run_repl(a, s, ["--appendonly", "yes", "--appendfsync", "everysec"])
            elif s == "aof-always":
                run_aof(a, s, "always")
            elif s == "aof-everysec":
                run_aof(a, s, "everysec")
            elif s == "aof-resp":
                run_aof(a, s, "everysec", ["--aof-incr-format", "resp"])
            else:
                check("suite." + s, False, "unknown suite")
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            check(s + ".ran", False, repr(e))
    fails = [r for r in RESULTS if not r["ok"]]
    print("RLHASHCHECK " + json.dumps(dict(passed=len(RESULTS) - len(fails), failed=len(fails),
                                           fails=[r["check"] for r in fails])), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
