#!/usr/bin/env python3
"""D3 (owner threads) correctness gate against a running server started with owner-threads N.

Checks, all through the owner ports (owner-port + i) and the plain port:
  topology        CLUSTER SLOTS is the same on every port, N contiguous owners, full coverage
  native          same-owner GET/SET/DEL/INCR/PING run on the owner (owner_native_commands grows by
                  the command count) with no world stop per command (owner_world_stops ~ flat)
  fallback        another owner's key, multi-key commands across owners, MULTI/EXEC, list/hash
                  commands on an owner port: served correctly (no MOVED/CROSSSLOT), counted as
                  handoffs, and the client comes back to its owner (native again afterwards)
  multi           MULTI/EXEC atomic and isolated while owners run native INCRs on the same keys;
                  WATCH aborts EXEC after a write from an owner port
  reply_order     deep pipelines mixing native and fallback commands on concurrent connections
  linearizable    concurrent INCR on one key from owner, other-owner and plain connections: values
                  unique 1..total, increasing per connection; real-time reads: a GET issued after an
                  INCR returned v (any path) never sees less than v
  plain_port      a plain client on the main port runs the full command set
  vetoes          MONITOR / keyspace notifications see owner-port writes; blocking BLPOP woken by a
                  push from an owner port; lazy expiry on the owner; errorstats from native errors
  after_load      (--after-load) every owner ran native commands
  no_crash        same pid, every port answers PING

Usage: d3-check.py --host 127.0.0.1 --port 6379 --owner-port 6400 --n 4 [--after-load]
Prints PASS/FAIL lines and a final 'D3CHECK {json}' line; exit 1 on any FAIL.
"""
import argparse
import json
import random
import socket
import sys
import threading
import time


def crc16(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(key: bytes) -> int:
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}", s + 1)
        if e > s + 1:
            key = key[s + 1:e]
    return crc16(key) & 16383


class Err(str):
    pass


class Conn:
    def __init__(self, host, port, timeout=60):
        self.s = socket.create_connection((host, port), timeout=timeout)
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
            return {self.read(): self.read() for _ in range(n)}
        if t == b"_":
            return None
        raise ValueError("bad reply %r" % line)

    def cmd(self, *args):
        self.send([args])
        return self.read()

    def close(self):
        self.s.close()


RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(dict(check=name, ok=bool(ok), detail=str(detail)[:400]))
    print(("PASS " if ok else "FAIL ") + name + (": " + str(detail) if detail != "" else ""), flush=True)
    return ok


def info(c, section):
    raw = c.cmd("INFO", section)
    raw = raw.decode() if isinstance(raw, bytes) else raw
    out = {}
    for l in raw.split("\r\n"):
        if ":" in l and not l.startswith("#"):
            k, v = l.split(":", 1)
            out[k] = v
    return out


def ostat(c):
    i = info(c, "owners")
    return {k: int(v) for k, v in i.items() if k.startswith("owner_") and v.lstrip("-").isdigit()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--owner-port", type=int, default=6400)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--after-load", action="store_true")
    ap.add_argument("--order-conns", type=int, default=16)
    ap.add_argument("--order-groups", type=int, default=3000)
    ap.add_argument("--lin-threads", type=int, default=12)
    ap.add_argument("--lin-incrs", type=int, default=2000)
    a = ap.parse_args()
    H, N = a.host, a.n
    ports = [a.owner_port + i for i in range(N)]
    rnd = random.Random(7)

    adm = Conn(H, a.port)
    pid0 = info(adm, "server")["process_id"]
    adm.cmd("FLUSHALL")

    # --- topology
    def slots_of(p):
        c = Conn(H, p)
        r = c.cmd("CLUSTER", "SLOTS")
        c.close()
        own = [None] * 16384
        for ent in r:
            for s in range(ent[0], ent[1] + 1):
                own[s] = int(ent[2][1])
        return own

    owner = slots_of(ports[0])
    same = all(slots_of(p) == owner for p in ports + [a.port])
    check("topology.same_view_on_every_port", same)
    check("topology.full_coverage", None not in owner, "%d unmapped" % owner.count(None))
    split = all(owner[s] == ports[i] for i in range(N) for s in (16384 * i // N, 16384 * (i + 1) // N - 1))
    check("topology.contiguous_split_N_ways", split and set(owner) == set(ports), {p: owner.count(p) for p in ports})

    key_cache = {}

    def key_for(port, prefix, exclude_slot=None):
        ck = (port, prefix, exclude_slot)
        if ck in key_cache:
            return key_cache[ck]
        for i in range(10 ** 6):
            k = b"%s:%d" % (prefix, i)
            s = key_slot(k)
            if owner[s] == port and s != exclude_slot:
                key_cache[ck] = k
                return k
        raise RuntimeError("no key")

    # --- native path: same-owner single-key commands, no world stop per command
    p = ports[-1]
    c = Conn(H, p)
    c.cmd("PING")  # settle: the connection is home
    time.sleep(0.2)
    keys = [key_for(p, b"nat%d" % j) for j in range(200)]
    ctrs = [key_for(p, b"natn%d" % j) for j in range(200)]
    s0 = ostat(adm)
    cmds, want = [], []
    for j, k in enumerate(keys):
        cmds += [("SET", k, b"v%d" % j), ("GET", k), ("INCR", ctrs[j]), ("DEL", ctrs[j]), ("PING",)]
        want += ["OK", b"v%d" % j, 1, 1, "PONG"]
    rounds = 20
    t0 = time.time()
    for _ in range(rounds):
        c.send(cmds)
        got = [c.read() for _ in want]
        if got != want:
            break
    dt = time.time() - t0
    s1 = ostat(adm)
    ncmds = rounds * len(cmds)
    dnat = s1["owner_native_commands"] - s0["owner_native_commands"]
    dstop = s1["owner_world_stops"] - s0["owner_world_stops"]
    dhand = s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]
    check("native.replies_correct", got == want)
    check("native.served_on_owner", dnat >= ncmds and dhand <= 1,
          "native +%d for %d cmds, handoffs +%d" % (dnat, ncmds, dhand))
    # Main still runs its crons (hz) and the INFO calls above: allow those, never one per command.
    check("native.no_world_stop_per_command", dstop < max(50, ncmds // 100),
          "world stops +%d over %d native cmds in %.2fs" % (dstop, ncmds, dt))
    c.close()

    # --- fallback: another owner's key, multi-key across owners, list/hash on an owner port
    p0, p1 = ports[0], ports[min(1, N - 1)]
    c = Conn(H, p0)
    k_other = key_for(p1, b"other")
    k_mine = key_for(p0, b"mine")
    s0 = ostat(adm)
    r = [c.cmd("SET", k_other, "x1"), c.cmd("GET", k_other), c.cmd("INCR", k_other + b"n")]
    s1 = ostat(adm)
    check("fallback.cross_owner_single_key_served", r == ["OK", b"x1", 1] and
          (N == 1 or s1["owner_handoffs_to_main"] > s0["owner_handoffs_to_main"]), (r, s1["owner_handoffs_to_main"] - s0["owner_handoffs_to_main"]))
    c2 = Conn(H, p1)
    check("fallback.cross_owner_write_visible_on_owner", c2.cmd("GET", k_other) == b"x1")
    c2.close()
    k3 = key_for(ports[-1], b"third")
    r = [c.cmd("MSET", k_mine, "a", k_other, "b", k3, "c"), c.cmd("MGET", k_mine, k_other, k3),
         c.cmd("DEL", k_mine, k_other, k3), c.cmd("EXISTS", k_mine, k_other, k3)]
    check("fallback.multikey_across_owners", r == ["OK", [b"a", b"b", b"c"], 3, 0], r)
    r = [c.cmd("RPUSH", k_mine + b"l", 1, 2, 3), c.cmd("LRANGE", k_mine + b"l", 0, -1),
         c.cmd("HSET", k_other + b"h", "f", "v"), c.cmd("HGETALL", k_other + b"h"),
         c.cmd("EXPIRE", k_mine + b"l", 100), c.cmd("TTL", k_mine + b"l") > 0]
    check("fallback.other_commands_on_owner_port", r == [3, [b"1", b"2", b"3"], 1, [b"f", b"v"], 1, True], r)
    # the client goes home: native again
    time.sleep(0.3)
    s0 = ostat(adm)
    r = [c.cmd("SET", k_mine, "home") for _ in range(50)]
    s1 = ostat(adm)
    check("fallback.client_returns_to_owner", s1["owner_native_commands"] - s0["owner_native_commands"] >= 45,
          "native +%d of 50, returns %d" % (s1["owner_native_commands"] - s0["owner_native_commands"], s1["owner_returns_from_main"]))
    c.close()

    # --- MULTI/EXEC: atomic + isolated against concurrent native INCRs on the same keys
    kx = key_for(p0, b"mx")
    ky = key_for(ports[-1], b"my")
    adm.cmd("DEL", kx, ky)
    stop = threading.Event()
    hammer_err = []

    def hammer(port, key):
        h = Conn(H, port)
        try:
            while not stop.is_set():
                h.send([("INCR", key)] * 32)
                for _ in range(32):
                    if not isinstance(h.read(), int):
                        hammer_err.append(key)
        finally:
            h.close()

    hs = [threading.Thread(target=hammer, args=(p0, kx)), threading.Thread(target=hammer, args=(ports[-1], ky))]
    [t.start() for t in hs]
    time.sleep(0.2)
    mc = Conn(H, ports[N // 2])
    iso_bad = atom_bad = 0
    for _ in range(300):
        r = [mc.cmd("MULTI"), mc.cmd("GET", kx), mc.cmd("GET", kx), mc.cmd("INCR", ky), mc.cmd("INCR", ky),
             mc.cmd("GET", ky), mc.cmd("EXEC")]
        ex = r[-1]
        if not isinstance(ex, list) or len(ex) != 5:
            atom_bad += 1
            continue
        if ex[0] != ex[1]:
            iso_bad += 1
        if not (ex[3] == ex[2] + 1 and int(ex[4]) == ex[3]):
            atom_bad += 1
    stop.set()
    [t.join() for t in hs]
    check("multi.isolated_from_owner_writes", iso_bad == 0, "%d of 300 EXECs saw a key change inside" % iso_bad)
    check("multi.atomic_against_owner_writes", atom_bad == 0 and not hammer_err,
          "%d of 300 EXECs not consecutive, hammer errors %d" % (atom_bad, len(hammer_err)))
    # WATCH: a native-eligible write from an owner port must abort the transaction
    kw = key_for(p0, b"w")
    mc.cmd("SET", kw, "1")
    w = Conn(H, p0)
    r = [mc.cmd("WATCH", kw), w.cmd("SET", kw, "2"), mc.cmd("MULTI"), mc.cmd("SET", kw, "3"), mc.cmd("EXEC"),
         mc.cmd("GET", kw)]
    check("multi.watch_aborts_on_owner_write", r[4] is None and r[5] == b"2", r)
    w.close()
    mc.close()

    # --- per-connection reply order: native and fallback commands interleaved, all ports
    errs = []

    def order_worker(idx):
        port = (ports + [a.port])[idx % (N + 1)]
        home = port if port != a.port else ports[0]
        c = Conn(H, port)
        ctr = key_for(home, b"ctr%d" % idx)
        c.cmd("DEL", ctr)
        cmds, want = [], []
        for j in range(a.order_groups):
            kj = key_for(home, b"o%d_%d" % (idx, j % 50))
            ko = key_for(ports[(ports.index(home) + 1) % N], b"q%d_%d" % (idx, j % 50))
            vj = b"v:%d:%d" % (idx, j)
            cmds += [("SET", kj, vj), ("GET", kj), ("INCR", ctr), ("ECHO", b"%d" % j), ("SET", ko, vj),
                     ("MGET", kj, ko)]
            want += ["OK", vj, j + 1, b"%d" % j, "OK", [vj, vj]]
        for i in range(0, len(cmds), 3000):
            c.send(cmds[i:i + 3000])
        got = [c.read() for _ in want]
        bad = sum(1 for x, y in zip(got, want) if x != y)
        if bad:
            first = next(i for i, (x, y) in enumerate(zip(got, want)) if x != y)
            errs.append((idx, port, bad, first, got[first], want[first]))
        c.close()

    th = [threading.Thread(target=order_worker, args=(i,)) for i in range(a.order_conns)]
    [t.start() for t in th]
    [t.join() for t in th]
    check("reply_order.mixed_native_fallback_pipelines", not errs,
          "%d conns x %d cmds, mismatches %s" % (a.order_conns, 6 * a.order_groups, errs[:3]))

    # --- linearizable: one key, INCRs from the owner, another owner's port and the plain port
    lk = key_for(ports[N // 2], b"lin")
    adm.cmd("DEL", lk)
    lports = [owner[key_slot(lk)], ports[(ports.index(owner[key_slot(lk)]) + 1) % N], a.port]
    seen = [[] for _ in range(a.lin_threads)]

    def lin_worker(t):
        cc = Conn(H, lports[t % len(lports)])
        for _ in range(0, a.lin_incrs, 50):
            cc.send([("INCR", lk)] * 50)
            seen[t] += [cc.read() for _ in range(50)]
        cc.close()

    th = [threading.Thread(target=lin_worker, args=(t,)) for t in range(a.lin_threads)]
    [t.start() for t in th]
    [t.join() for t in th]
    allv = sorted(v for s in seen for v in s)
    total = a.lin_threads * a.lin_incrs
    mono = all(all(x < y for x, y in zip(s, s[1:])) for s in seen)
    final = int(adm.cmd("GET", lk))
    check("linearizable.concurrent_INCR_mixed_paths", mono and allv == list(range(1, total + 1)) and final == total,
          "%d threads on ports %s x %d INCR: per-conn increasing=%s, unique 1..%d=%s, final=%d"
          % (a.lin_threads, lports, a.lin_incrs, mono, total, allv == list(range(1, total + 1)), final))
    # real-time order: a read that starts after an INCR returned v sees >= v, on every path
    rk = key_for(ports[0], b"rt")
    adm.cmd("SET", rk, 0)
    wr = Conn(H, owner[key_slot(rk)])
    readers = [Conn(H, owner[key_slot(rk)]), Conn(H, ports[-1]), Conn(H, a.port)]
    rt_bad = 0
    for i in range(1500):
        v = wr.cmd("INCR", rk)
        for rd in readers:
            r = int(rd.cmd("GET", rk))
            if r < v:
                rt_bad += 1
        if i % 3 == 0:
            readers[i % 3].cmd("INCR", rk)  # writes from other paths too
    check("linearizable.real_time_reads_after_writes", rt_bad == 0, "%d stale reads of %d" % (rt_bad, 1500 * 3))
    [x.close() for x in readers + [wr]]

    # --- plain port: full command set on the main path
    pc = Conn(H, a.port)
    pk = b"{plain}k"
    r = [pc.cmd("SET", pk, "1"), pc.cmd("INCRBY", pk, 4), pc.cmd("GET", pk), pc.cmd("MSET", pk + b"a", "x", pk + b"b", "y"),
         pc.cmd("MGET", pk + b"a", pk + b"b"), pc.cmd("LPUSH", pk + b"l", "a", "b"), pc.cmd("LRANGE", pk + b"l", 0, -1),
         pc.cmd("SADD", pk + b"s", "m"), pc.cmd("SMEMBERS", pk + b"s"), pc.cmd("ZADD", pk + b"z", 1, "m"),
         pc.cmd("ZRANGE", pk + b"z", 0, -1), pc.cmd("MULTI"), pc.cmd("INCR", pk), pc.cmd("GET", pk), pc.cmd("EXEC"),
         pc.cmd("EVAL", "return redis.call('incr', KEYS[1])", 1, pk), pc.cmd("PEXPIRE", pk, 100000), pc.cmd("PTTL", pk) > 0]
    check("plain_port.full_command_set", r == ["OK", 5, b"5", "OK", [b"x", b"y"], 2, [b"b", b"a"], 1, [b"m"], 1, [b"m"],
                                             "OK", "QUEUED", "QUEUED", [6, b"6"], 7, 1, True], r)
    n_keys = pc.cmd("DBSIZE")
    n_scan = 0
    cur = b"0"
    while True:
        cur, ks = pc.cmd("SCAN", cur, "COUNT", 1000)
        n_scan += len(ks)
        if cur == b"0":
            break
    check("plain_port.DBSIZE_matches_SCAN", n_keys == n_scan and n_keys > 0, (n_keys, n_scan))

    # --- vetoes: MONITOR sees owner-port writes; keyspace events; BLPOP woken from an owner port
    mon = Conn(H, a.port)
    check("veto.monitor_attach", mon.cmd("MONITOR") == "OK")
    time.sleep(0.2)
    km = key_for(p0, b"mon")
    oc = Conn(H, p0)
    oc.cmd("SET", km, "seen")
    mon.s.settimeout(5)
    seen_line = False
    try:
        for _ in range(20):
            line = mon.read()
            if isinstance(line, str) and km.decode() in line:
                seen_line = True
                break
    except Exception as e:
        seen_line = "exc %r" % e
    check("veto.monitor_sees_owner_port_write", seen_line is True, seen_line)
    mon.close()
    time.sleep(0.2)
    adm.cmd("CONFIG", "SET", "notify-keyspace-events", "KEA")
    sub = Conn(H, a.port)
    sub.cmd("SUBSCRIBE", "__keyevent@0__:set")
    time.sleep(0.2)
    oc.cmd("SET", km, "ev")
    sub.s.settimeout(5)
    try:
        msg = sub.read()
        ev_ok = isinstance(msg, list) and msg[-1] == km
    except Exception as e:
        ev_ok, msg = False, repr(e)
    check("veto.keyspace_event_for_owner_port_write", ev_ok, msg)
    sub.close()
    adm.cmd("CONFIG", "SET", "notify-keyspace-events", "")
    kb = key_for(p0, b"blk")
    bc = Conn(H, a.port)
    bc.send([("BLPOP", kb, 5)])
    time.sleep(0.3)
    oc.cmd("RPUSH", kb, "item")
    bc.s.settimeout(6)
    try:
        br = bc.read()
    except Exception as e:
        br = repr(e)
    check("veto.blpop_woken_by_owner_port_push", br == [kb, b"item"], br)
    bc.close()
    # lazy expiry on the owner, and the expired_keys stat
    ke = key_for(p0, b"exp")
    adm.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)
    e0 = int(info(adm, "stats")["expired_keys"])
    r = [oc.cmd("SET", ke, "v", "PX", 100), oc.cmd("GET", ke)]
    time.sleep(0.25)
    r += [oc.cmd("GET", ke), adm.cmd("EXISTS", ke)]
    e1 = int(info(adm, "stats")["expired_keys"])
    adm.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 1)
    check("expire.lazy_on_owner", r == ["OK", b"v", None, 0] and e1 > e0, (r, e1 - e0))
    # errorstats and failed_calls from a native error
    adm.cmd("CONFIG", "RESETSTAT")
    kn = key_for(p0, b"nan")
    oc.cmd("SET", kn, "abc")
    er = oc.cmd("INCR", kn)
    es = info(adm, "errorstats")
    cs = info(adm, "commandstats")
    check("stats.native_error_counted", isinstance(er, Err) and "errorstat_ERR" in es and
          "failed_calls=1" in cs.get("cmdstat_incr", ""), (er, es.get("errorstat_ERR"), cs.get("cmdstat_incr")))
    oc.close()

    if a.after_load:
        o = info(adm, "owners")
        per = {k: v for k, v in o.items() if k.startswith("owner") and k[5:].isdigit()}
        natives = {k: int(v.split("native=")[1].split(",")[0]) for k, v in per.items()}
        check("after_load.every_owner_ran_native_commands", len(natives) == N and all(x > 0 for x in natives.values()),
              natives)

    # --- no crash
    alive = {}
    for port in ports + [a.port]:
        c = Conn(H, port)
        alive[port] = c.cmd("PING") == "PONG" and info(c, "server")["process_id"] == pid0
        c.close()
    check("no_crash.same_pid_every_port_PONG", all(alive.values()), alive)
    adm.close()

    nfail = sum(1 for r in RESULTS if not r["ok"])
    print("D3CHECK " + json.dumps(dict(n=N, checks=len(RESULTS), failed=nfail, results=RESULTS)))
    sys.exit(1 if nfail else 0)


if __name__ == "__main__":
    main()
