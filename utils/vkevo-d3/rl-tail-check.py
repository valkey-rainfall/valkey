#!/usr/bin/env python3
"""RL-g2o-long-tail model check: owners run the long tail of single-key / single-slot data commands natively.

Starts two servers with owner threads: the candidate (--bin) and the reference (--ref-bin, the parent RL-1, whose
owners hand every tail command to main: the main path). Both get the same deterministic workload, and every reply
and the final keyspace must be equal (the structure of rl-setzset-check.py):
  native     one connection per owner, only the tail commands on that owner's own keys (pipelined, source and
             destination in one slot): the candidate's owners run every one of them (owner_tail_commands grows
             by the count, no handoff; under maxmemory APPEND / SETRANGE / SMOVE and the unbounded reads go to
             main)
  disjoint   T connections at once, each on its own keys (spread over every owner's slots), random tail commands
             (GETEX all options incl. PXAT in the past, GETDEL, GETSET, SETNX, SETEX / PSETEX incl. bad expiry,
             APPEND, STRLEN, GETRANGE / SUBSTR, SETRANGE, INCRBY, DECR, DECRBY, INCRBYFLOAT incl. bad values,
             MSETNX, HSETNX, HINCRBYFLOAT, HKEYS, HVALS, HSTRLEN, LPUSHX, RPUSHX, LSET, LINSERT, LREM, LTRIM, LPOS,
             RPOPLPUSH, LMOVE, SMISMEMBER, SMOVE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX,
             ZREVRANGEBYLEX, ZRANK, ZREVRANK, ZCOUNT, ZLEXCOUNT, ZMSCORE, ZPOPMIN, ZPOPMAX, ZREMRANGEBY*,
             EXPIRETIME, PEXPIRETIME, UNLINK, TOUCH) mixed with the base commands of every type on the same keys
             (WRONGTYPE, type changes, encoding conversions), two-key commands mostly in one slot and sometimes
             across slots (main), PEXPIRE / PSETEX / GETEX PX 1 then an access 20 ms later (lazy expiry on the
             owner), big lists then UNLINK (lazy free from an owner), MULTI/EXEC with tail commands (main),
             pipelines of 1-24 commands
  lockstep   S connections (owner ports and the plain port) take turns on a few shared keys, one command each
  keyspace   every key's TYPE, OBJECT ENCODING, TTL presence and value, and DEBUG DIGEST equal on both
  mixed      the candidate's owner_tail_commands grew over the mixed phases; the reference has no such field
Suites (--suites): plain (small listpack limits), mm (maxmemory 1gb allkeys-lru), aofrepl (appendonly everysec,
a replica on each server: the replica's data equals its primary's; each AOF reloaded into a fresh server and
compared), aofresp (appendonly always, aof-incr-format resp, reloaded and compared).
Replies with a time in them (TTL, PTTL, EXPIRETIME, PEXPIRETIME) are compared by sign (the two servers run the
same workload one after the other). Prints PASS/FAIL lines and 'RLTAIL {json}'; exit 1 on any FAIL.
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


# ---------------------------------------------------------------------------------------------------------
# Workload: deterministic per connection (its own random generator); the same on both servers.

SMALL = [b"%d" % i for i in range(-20, 300)]
SHORT = [b"e%d" % i for i in range(60)]
LONG = [b"L%d-" % i + b"x" * 70 for i in range(6)]
LEX = [b"-", b"+", b"[a", b"(a", b"[e1", b"(e5", b"[zz", b"(c", b"bad"]
SC = [b"-inf", b"+inf", b"0", b"(0", b"10", b"(100", b"250", b"-20", b"(-inf", b"x"]
FLOATS = [b"1.5", b"-0.25", b"3", b"1e3", b"abc", b"1e400", b"0", b"-7.125"]


def elem(r):
    x = r.random()
    if x < 0.4:
        return r.choice(SMALL)
    if x < 0.97:
        return r.choice(SHORT)
    return r.choice(LONG)


def val(r):
    x = r.random()
    if x < 0.4:
        return b"%d" % r.randrange(-50, 1000)
    if x < 0.9:
        return b"v%d" % r.randrange(100) * r.choice([1, 1, 2, 8])
    return b"w" * r.choice([40, 80, 300])


def sib(k):
    """A second key in the same slot (same hash tag)."""
    return k + b":b"


def idx(r):
    return b"%d" % r.randrange(-8, 12)


TAIL = ["GETEX", "GETDEL", "GETSET", "SETNX", "SETEX", "PSETEX", "APPEND", "STRLEN", "GETRANGE", "SUBSTR", "SETRANGE",
        "INCRBY", "DECR", "DECRBY", "INCRBYFLOAT", "MSETNX", "HSETNX", "HINCRBYFLOAT", "HKEYS", "HVALS", "HSTRLEN",
        "LPUSHX", "RPUSHX", "LSET", "LINSERT", "LREM", "LTRIM", "LPOS", "RPOPLPUSH", "LMOVE", "SMISMEMBER", "SMOVE",
        "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANK", "ZREVRANK",
        "ZCOUNT", "ZLEXCOUNT", "ZMSCORE", "ZPOPMIN", "ZPOPMAX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK",
        "ZREMRANGEBYLEX", "EXPIRETIME", "PEXPIRETIME", "UNLINK", "TOUCH"]


def tail_cmd(r, k, k2):
    """One tail command (correct arity) on k (and k2: a second key, same slot unless the caller chose otherwise)."""
    c = r.choice(TAIL)
    if c == "GETEX":
        opt = r.choice([[], ["EX", "100000"], ["PX", "100000000"], ["EXAT", "4102444800"],
                        ["PXAT", "4102444800000"], ["PERSIST"], ["PXAT", "1"], ["EX", "-5"], ["EX", "abc"],
                        ["EX", "10", "PX", "10"]])
        return [c, k] + opt
    if c in ("GETDEL", "STRLEN", "HKEYS", "HVALS", "EXPIRETIME", "PEXPIRETIME", "DECR"):
        return [c, k]
    if c in ("GETSET", "SETNX", "APPEND"):
        return [c, k, val(r)]
    if c == "SETEX":
        return [c, k, r.choice([b"100000", b"100000", b"0", b"-1", b"abc"]), val(r)]
    if c == "PSETEX":
        return [c, k, r.choice([b"100000000", b"100000000", b"0", b"abc"]), val(r)]
    if c in ("GETRANGE", "SUBSTR"):
        return [c, k, idx(r), b"%d" % r.randrange(-3, 40)]
    if c == "SETRANGE":
        return [c, k, r.choice([b"0", b"3", b"17", b"60", b"-1", b"x"]), r.choice([b"", b"zz", val(r)])]
    if c in ("INCRBY", "DECRBY"):
        return [c, k, r.choice([b"1", b"7", b"-3", b"9223372036854775807", b"x"])]
    if c == "INCRBYFLOAT":
        return [c, k, r.choice(FLOATS)]
    if c == "MSETNX":
        return [c, k, val(r), k2, val(r)]
    if c == "HSETNX":
        return [c, k, b"f%d" % r.randrange(12), val(r)]
    if c == "HINCRBYFLOAT":
        return [c, k, b"f%d" % r.randrange(12), r.choice(FLOATS)]
    if c == "HSTRLEN":
        return [c, k, b"f%d" % r.randrange(12)]
    if c in ("LPUSHX", "RPUSHX"):
        return [c, k] + [elem(r) for _ in range(r.choice([1, 1, 3]))]
    if c == "LSET":
        return [c, k, idx(r), elem(r)]
    if c == "LINSERT":
        return [c, k, r.choice(["BEFORE", "AFTER", "bogus"]), elem(r), elem(r)]
    if c == "LREM":
        return [c, k, r.choice([b"0", b"1", b"-1", b"2", b"-2", b"x"]), elem(r)]
    if c == "LTRIM":
        return [c, k, idx(r), b"%d" % r.randrange(-5, 30)]
    if c == "LPOS":
        opt = []
        if r.random() < 0.3:
            opt += ["RANK", r.choice([b"1", b"-1", b"2", b"0"])]
        if r.random() < 0.3:
            opt += ["COUNT", r.choice([b"0", b"1", b"3"])]
        if r.random() < 0.2:
            opt += ["MAXLEN", r.choice([b"0", b"5"])]
        return [c, k, elem(r)] + opt
    if c == "RPOPLPUSH":
        return [c, k, r.choice([k, k2])]
    if c == "LMOVE":
        return [c, k, r.choice([k, k2]), r.choice(["LEFT", "RIGHT"]), r.choice(["LEFT", "RIGHT", "UP"])]
    if c == "SMISMEMBER":
        return [c, k] + [elem(r) for _ in range(r.choice([1, 2, 5]))]
    if c == "SMOVE":
        return [c, k, k2, elem(r)]
    if c == "ZREVRANGE":
        return [c, k, idx(r), b"%d" % r.randrange(-3, 30)] + (["WITHSCORES"] if r.random() < 0.4 else [])
    if c in ("ZRANGEBYSCORE", "ZREVRANGEBYSCORE"):
        x = [c, k, r.choice(SC), r.choice(SC)]
        if r.random() < 0.4:
            x.append("WITHSCORES")
        if r.random() < 0.4:
            x += ["LIMIT", b"%d" % r.randrange(-1, 5), b"%d" % r.randrange(-1, 8)]
        return x
    if c in ("ZRANGEBYLEX", "ZREVRANGEBYLEX"):
        x = [c, k, r.choice(LEX), r.choice(LEX)]
        if r.random() < 0.3:
            x += ["LIMIT", b"%d" % r.randrange(0, 4), b"%d" % r.randrange(-1, 5)]
        return x
    if c in ("ZRANK", "ZREVRANK"):
        return [c, k, elem(r)] + (["WITHSCORE"] if r.random() < 0.3 else [])
    if c == "ZCOUNT":
        return [c, k, r.choice(SC), r.choice(SC)]
    if c == "ZLEXCOUNT":
        return [c, k, r.choice(LEX), r.choice(LEX)]
    if c == "ZMSCORE":
        return [c, k] + [elem(r) for _ in range(r.choice([1, 2, 4]))]
    if c in ("ZPOPMIN", "ZPOPMAX"):
        return [c, k] + ([r.choice([b"1", b"3", b"0", b"-1", b"100"])] if r.random() < 0.5 else [])
    if c == "ZREMRANGEBYSCORE":
        return [c, k, r.choice(SC), r.choice(SC)]
    if c == "ZREMRANGEBYRANK":
        return [c, k, idx(r), b"%d" % r.randrange(-5, 5)]
    if c == "ZREMRANGEBYLEX":
        return [c, k, r.choice(LEX), r.choice(LEX)]
    if c in ("UNLINK", "TOUCH"):
        return [c, k] + ([k2] if r.random() < 0.4 else [])
    raise ValueError(c)


def base_cmd(r, k, strkeys):
    """A base command of some type on k, so the tail meets every type and encoding."""
    x = r.random()
    if x < 0.12:
        return ["SET", k, val(r)] + (["EX", "100000"] if r.random() < 0.2 else [])
    if x < 0.18:
        return ["GET", k]
    if x < 0.26:
        return ["HSET", k] + sum(([b"f%d" % r.randrange(12), val(r)] for _ in range(r.choice([1, 1, 4]))), [])
    if x < 0.30:
        return ["HGET", k, b"f%d" % r.randrange(12)]
    if x < 0.40:
        return [r.choice(["LPUSH", "RPUSH"]), k] + [elem(r) for _ in range(r.choice([1, 1, 2, 6, 80]))]
    if x < 0.44:
        return [r.choice(["LPOP", "RPOP"]), k]
    if x < 0.47:
        return ["LRANGE", k, "0", "-1"]
    if x < 0.55:
        return ["SADD", k] + [elem(r) for _ in range(r.choice([1, 1, 3, 20]))]
    if x < 0.63:
        args = []
        for _ in range(r.choice([1, 1, 3, 20])):
            args += [b"%d" % r.randrange(-30, 300), elem(r)]
        return ["ZADD", k] + args
    if x < 0.67:
        return ["INCR", r.choice(strkeys + [k])]
    if x < 0.71:
        return ["EXPIRE", k, "100000"]
    if x < 0.73:
        return ["PERSIST", k]
    if x < 0.77:
        return [r.choice(["TTL", "PTTL"]), k]
    if x < 0.80:
        return ["DEL", k]
    if x < 0.84:
        return ["TYPE", k]
    if x < 0.88:
        return ["OBJECT", "ENCODING", k]
    return ["EXISTS", k]


def gen_cmd(r, keys, strkeys):
    k = r.choice(keys)
    k2 = sib(k) if r.random() < 0.85 else r.choice(keys)
    if r.random() < 0.06:
        k = r.choice(strkeys)
    if r.random() < 0.55:
        return tail_cmd(r, k, k2)
    return base_cmd(r, k, strkeys)


TIMED = {"TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"}
UNORDERED = {"HKEYS", "HVALS", "SMEMBERS", "HGETALL"}


def norm(cmd, rep):
    """Replies with a time in them by sign; hashtable-ordered replies sorted."""
    if cmd[0] in TIMED and isinstance(rep, int):
        return rep if rep < 0 else "pos"
    if cmd[0] in UNORDERED and isinstance(rep, list):
        return sorted(rep)
    if cmd[0] == "EXEC" and isinstance(rep, list):
        return [sorted(x) if isinstance(x, list) and all(isinstance(y, bytes) for y in x) else x for x in rep]
    return rep


def run_conn(port, seed, keys, strkeys, ops, out, idx_, stats):
    r = random.Random(seed)
    c = Conn(port)
    reps = []
    done = 0
    try:
        while done < ops:
            x = r.random()
            if x < 0.015:
                # lazy expiry on whichever thread owns the key
                k = r.choice(keys)
                first = r.choice([["PEXPIRE", k, "1"], ["PSETEX", k, "1", val(r)], ["GETEX", k, "PX", "1"]])
                reps.append(norm(first, c.cmd(*first)))
                time.sleep(0.02)
                cmd = tail_cmd(r, k, sib(k))
                reps.append(norm(cmd, c.cmd(*cmd)))
                done += 2
            elif x < 0.03:
                k = r.choice(keys)
                cmds = [["MULTI"], tail_cmd(r, k, sib(k)), base_cmd(r, k, strkeys), tail_cmd(r, k, sib(k)), ["EXEC"]]
                c.send(cmds)
                got = [c.read() for _ in cmds]
                if isinstance(got[-1], list) and len(got[-1]) == len(cmds) - 2:
                    # EXEC's replies one by one, as the commands would reply on their own
                    got[-1] = [norm(cm, x) for cm, x in zip(cmds[1:-1], got[-1])]
                reps += got
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
    out[idx_] = reps
    stats[idx_] = done


def owner_of_slot(slot, n):
    for i in range(n):
        if (1024 * i // n) * 16 <= slot <= (1024 * (i + 1) // n) * 16 - 1:
            return i
    return -1


def make_keys(tag, count, own=-1, n=1, share=0.7):
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


def native_phase(srv, a, seed, mm):
    """One connection per owner, only tail commands on its own keys (both keys of a two-key command in one slot)."""
    adm = Conn(srv.port)
    i0 = info(adm, "owners")
    conns, outs, sent = [], [], 0
    for i in range(srv.n):
        r = random.Random(seed * 31 + i)
        keys = make_keys("n%d" % i, 24, i, srv.n, 1.0)
        c = Conn(srv.oport + i)
        reps = []
        # seed the keys with every type through native base commands
        for j, k in enumerate(keys):
            t = j % 5
            cm = [["SET", k, b"%d" % j], ["HSET", k, "f1", "1", "f2", "abc"], ["RPUSH", k, "e1", "e2", "e1", "7"],
                  ["SADD", k, "e1", "e2", "5"], ["ZADD", k, "1", "e1", "2", "e2", "3", "a"]][t]
            reps.append(c.cmd(*cm))
        for _ in range(a.native_ops // 8):
            cmds = [tail_cmd(r, r.choice(keys), None) for _ in range(r.choice([1, 1, 4, 16]))]
            for cm in cmds:  # the second key: the first key's sibling (same slot)
                for j in range(len(cm)):
                    if cm[j] is None:
                        cm[j] = sib(cm[1])
            c.send(cmds)
            for cm in cmds:
                reps.append(norm(cm, c.read()))
            sent += len(cmds)
        conns.append(c)
        outs.append(reps)
    i1 = info(adm, "owners")
    adm.close()
    for c in conns:
        c.close()
    d = lambda k: int(i1.get(k, "-1")) - int(i0.get(k, "-1"))
    return outs, sent, d("owner_tail_commands"), d("owner_handoffs_to_main")


def lockstep(srv, a, seed):
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
        elif t == "list":
            v = c.cmd("LRANGE", k, "0", "-1")
        elif t == "hash":
            h = c.cmd("HGETALL", k)
            v = sorted(zip(h[0::2], h[1::2]))
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


def tail_native(srv):
    c = Conn(srv.port)
    i = info(c, "owners")
    c.close()
    return int(i.get("owner_tail_commands", "-1")), int(i.get("owner_native_commands", "0"))


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


SMALLENC = ["--set-max-listpack-entries", "16", "--set-max-intset-entries", "24", "--zset-max-listpack-entries", "16",
            "--hash-max-listpack-entries", "8", "--list-max-listpack-size", "8"]
SUITES = {
    "plain": SMALLENC,
    "mm": ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"],
    "aofrepl": ["--appendonly", "yes", "--appendfsync", "everysec"] + SMALLENC,
    "aofresp": ["--appendonly", "yes", "--appendfsync", "always", "--aof-incr-format", "resp"] + SMALLENC,
}


def start_replica(a, name, port, prim, extra):
    rep = Server(a, name, a.bin if "cand" in name else a.ref_bin, port, 0, "", a.replica_cpu,
                 [x for x in extra if x not in ("--appendonly", "yes", "everysec", "always", "--appendfsync",
                                                "--aof-incr-format", "resp")] + ["--appendonly", "no"], 0)
    rep.start()
    q = Conn(rep.port)
    q.cmd("REPLICAOF", "127.0.0.1", str(prim.port))
    q.close()
    return rep


def values(d):
    return {k: (v[0], v[2], v[3]) for k, v in d.items()}


def enc_diff(d1, d2):
    return sorted(k for k in d1 if k in d2 and d1[k][1] != d2[k][1])


def suite(a, name, seed):
    extra = SUITES[name]
    cand = Server(a, name + "-cand", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra, a.n)
    ref = Server(a, name + "-ref", a.ref_bin, a.ref_port, a.ref_owner_port, a.ref_cpus, a.ref_main_cpu, extra, a.n)
    reps = []
    cand.start()
    ref.start()
    try:
        if name == "aofrepl":
            reps = [start_replica(a, name + "-cand-replica", a.replica_port, cand, extra),
                    start_replica(a, name + "-ref-replica", a.replica_port + 1, ref, extra)]
            ok = wait_repl(cand, reps[0])[0] and wait_repl(ref, reps[1])[0]
            check(name + ".replica_synced", ok)
        nc, nsent, nz, nh = native_phase(cand, a, seed, name == "mm")
        nr, _, _, _ = native_phase(ref, a, seed, name == "mm")
        nm = [(i, j) for i in range(len(nc)) for j in range(len(nc[i])) if j >= len(nr[i]) or nc[i][j] != nr[i][j]]
        check(name + ".native.replies_equal", not nm and [len(x) for x in nc] == [len(x) for x in nr],
              "%d commands, mismatches %d %s" % (nsent, len(nm), ("cand %r ref %r" % (nc[nm[0][0]][nm[0][1]], nr[nm[0][0]][nm[0][1]] if nm[0][1] < len(nr[nm[0][0]]) else None)) if nm else ""))
        if name == "mm":  # APPEND / SETRANGE / SMOVE and unbounded collection reads go to main under maxmemory
            check(name + ".native.owners_ran_them", nz >= 0.5 * nsent, "owner_tail_commands +%d of %d, handoffs +%d" % (nz, nsent, nh))
        else:
            check(name + ".native.owners_ran_them", nz == nsent and nh == 0, "owner_tail_commands +%d of %d, handoffs +%d" % (nz, nsent, nh))
        n0, _ = tail_native(cand)
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
        n1, nat = tail_native(cand)
        rn, _ = tail_native(ref)
        check(name + ".mixed.owners_ran_tail", n1 - n0 > sent * 0.1 and rn == -1,
              "cand owner_tail_commands +%d over ~%d mixed commands (native total %d); ref field %s" %
              (n1 - n0, sent, nat, "absent" if rn == -1 else rn))
        dc, gc = dump(cand)
        dr, gr = dump(ref)
        ntypes = {}
        for v in dc.values():
            ntypes[(v[0], v[1])] = ntypes.get((v[0], v[1]), 0) + 1
        check(name + ".keyspace.equal", dc == dr and gc == gr,
              "%d keys %s digest %s; diff %s" % (len(dc), sorted(ntypes.items()), gc == gr, diff_dumps(dc, dr)))
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
    ap.add_argument("--port", type=int, default=22600)
    ap.add_argument("--owner-port", type=int, default=22610)
    ap.add_argument("--ref-port", type=int, default=22601)
    ap.add_argument("--ref-owner-port", type=int, default=22620)
    ap.add_argument("--replica-port", type=int, default=22602)
    ap.add_argument("--replica-cpu", default="59")
    ap.add_argument("--cpus", default="52-55")
    ap.add_argument("--main-cpu", default="56")
    ap.add_argument("--bg-cpu", default="57")
    ap.add_argument("--ref-cpus", default="52-55")
    ap.add_argument("--ref-main-cpu", default="58")
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
    print("RLTAIL " + json.dumps(dict(passed=len(RESULTS) - len(fails), failed=len(fails),
                                      secs=round(time.time() - t0, 1))), flush=True)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
