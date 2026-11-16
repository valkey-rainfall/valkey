#!/usr/bin/env python3
"""RL-g2p-busy-poll gene check: owner-busy-poll-us (NAPI busy polling on each owner's epoll set, EPIOCSPARAMS).

  rl-busypoll-check.py --bin CHILD --ref-bin PARENT --dir D --port P --owner-port OP [--n 4] --cpus 32-35
      --main-cpu 36 --bio-cpu 37 --load-cpus 38-39

Checks (every server binds 127.0.0.1; ports P..P+3*16; stops what it started by port):
  layout    on with owners on CPUs of their own: all N poll; main pinned to owner 0's CPU (the fleet layout): N-1;
            owners not pinned: none. Read back from the kernel (INFO owner_busy_poll_owners uses EPIOCGPARAMS).
  off       owner-busy-poll-us 0: no owner polls, no ioctl error, the /proc counters are not read (-1).
  config    CONFIG SET on / off / budget range / prefer, read back each time.
  model     the same random command sequence (strings, counters, hashes, lists, sets, expiry, MULTI) over owner and
            main connections, on the child with busy polling on (prefer yes, budget 16) and on the parent: every reply
            and the final keyspace (type, value, has-TTL) equal.
  inert     loopback has no NAPI: under load with the option on, BusyPollRxPackets does not move, the owners still
            sleep (sleeps per wait about the same as with it off) and use no CPU once the load stops.
Prints PASS/FAIL/INFO lines and 'RLBUSYPOLLCHECK {json}'; exit 1 on any FAIL.
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
        self.s.sendall(self.enc(args))
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


def note(name, detail):
    print("INFO %s: %s" % (name, detail), flush=True)


def info(c, section):
    raw = c.cmd("INFO", section)
    raw = raw.decode() if isinstance(raw, bytes) else raw
    out = {}
    for line in raw.split("\r\n"):
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k] = v
    return out


def ival(d, k, dflt=0):
    try:
        return int(d.get(k, dflt))
    except ValueError:
        return dflt


class Server:
    def __init__(self, a, name, bin_, port, oport, cpus, main_cpu, extra=()):
        self.a, self.name, self.bin, self.port, self.oport = a, name, bin_, port, oport
        self.cpus, self.main_cpu, self.extra = cpus, main_cpu, list(extra)
        self.dir = os.path.join(a.dir, name)
        if os.path.isdir(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir, exist_ok=True)
        self.pid = None

    def start(self):
        a = self.a
        args = ["taskset", "-c", a.all_cpus, self.bin, "--port", str(self.port), "--bind", "127.0.0.1",
                "--owner-threads", str(a.n), "--owner-port", str(self.oport), "--server-cpulist", str(self.main_cpu),
                "--bio-cpulist", str(a.bio_cpu), "--save", "", "--appendonly", "no", "--enable-debug-command", "yes",
                "--daemonize", "yes", "--dir", self.dir, "--logfile", os.path.join(self.dir, "log"),
                "--pidfile", os.path.join(self.dir, "pid")]
        if self.cpus:
            args += ["--owner-cpulist", self.cpus]
        args += self.extra
        subprocess.run(args, check=True)
        for _ in range(300):
            try:
                c = Conn(self.port, timeout=5)
                if c.cmd("PING") == "PONG":
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

    def c(self, port=None):
        return Conn(port or self.port)

    def owners(self):
        c = self.c()
        try:
            return info(c, "owners")
        finally:
            c.close()

    def threads(self):
        out = {}
        for t in os.listdir("/proc/%d/task" % self.pid):
            try:
                comm = open("/proc/%d/task/%s/comm" % (self.pid, t)).read().strip()
                st = open("/proc/%d/task/%s/stat" % (self.pid, t)).read().rsplit(")", 1)[1].split()
                vol = 0
                for line in open("/proc/%d/task/%s/status" % (self.pid, t)):
                    if line.startswith("voluntary_ctxt_switches:"):
                        vol = int(line.split()[1])
                out[t] = (comm, int(st[11]) + int(st[12]), vol)
            except OSError:
                pass
        return out

    def owner_ticks(self):
        return sum(v[1] for v in self.threads().values() if v[0].startswith("owner_"))

    def stop(self):
        try:
            c = self.c()
            c.s.sendall(Conn.enc(["SHUTDOWN", "NOSAVE"]))
            c.close()
        except OSError:
            pass
        for _ in range(100):
            if not os.path.exists("/proc/%d" % self.pid):
                break
            time.sleep(0.1)
        if self.crash_lines():
            check("%s.no_crash" % self.name, False, "%d crash lines in its log" % self.crash_lines())

    def crash_lines(self):
        try:
            return sum(1 for line in open(os.path.join(self.dir, "log"))
                       if any(w in line for w in ("ASSERT", "crashed", "Guru", "BUG REPORT")))
        except OSError:
            return 0


def bp_keys(o):
    return {k: o.get(k) for k in ("owner_busy_poll_us", "owner_busy_poll_budget", "owner_busy_poll_prefer",
                                  "owner_busy_poll_owners", "owner_busy_poll_may", "owner_busy_poll_errors",
                                  "owner_busy_poll_errno", "owner_epoll_waits", "owner_sleeps",
                                  "owner_busy_poll_rx_packets")}


def cpu_range(s):
    lo, hi = s.split("-")
    return list(range(int(lo), int(hi) + 1))


# ------------------------------------------------------------------------------------------------ parts

def part_layout(a):
    cpus = cpu_range(a.cpus)
    for tag, ocpus, main, want in (("own", a.cpus, a.main_cpu, a.n), ("shared", a.cpus, cpus[0], a.n - 1),
                                   ("unpinned", "", a.main_cpu, 0)):
        s = Server(a, "layout-" + tag, a.bin, a.port, a.owner_port, ocpus, main, ["--owner-busy-poll-us", "50"]).start()
        try:
            o = s.owners()
            ok = (ival(o, "owner_busy_poll_owners") == want and ival(o, "owner_busy_poll_may") == want and
                  ival(o, "owner_busy_poll_errors") == 0 and ival(o, "owner_busy_poll_us") == 50)
            check("layout.%s" % tag, ok, "want %d polling owners: %s" % (want, bp_keys(o)))
        finally:
            s.stop()


def part_off_config(a):
    s = Server(a, "config", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu).start()
    try:
        o = s.owners()
        check("off.default", ival(o, "owner_busy_poll_us") == 0 and ival(o, "owner_busy_poll_owners") == 0 and
              ival(o, "owner_busy_poll_errors") == 0 and ival(o, "owner_sleeps") == -1 and
              ival(o, "owner_busy_poll_rx_packets") == -1 and ival(o, "owner_busy_poll_may") == a.n, bp_keys(o))
        c = s.c()
        steps = []
        r1 = c.cmd("CONFIG", "SET", "owner-busy-poll-us", "200")
        o1 = info(c, "owners")
        steps.append(ival(o1, "owner_busy_poll_owners") == a.n and r1 == "OK")
        r2 = c.cmd("CONFIG", "SET", "owner-busy-poll-budget", "65")
        steps.append(isinstance(r2, Err))
        r3 = c.cmd("CONFIG", "SET", "owner-busy-poll-budget", "64")
        r4 = c.cmd("CONFIG", "SET", "owner-busy-poll-prefer", "yes")
        o2 = info(c, "owners")
        steps.append(r3 == "OK" and r4 == "OK" and o2.get("owner_busy_poll_prefer") == "yes" and
                     ival(o2, "owner_busy_poll_budget") == 64 and ival(o2, "owner_busy_poll_owners") == a.n and
                     ival(o2, "owner_busy_poll_errors") == 0)
        # Hidden configs answer CONFIG GET by exact name only (a pattern skips them).
        gd = {}
        for name in ("owner-busy-poll-us", "owner-busy-poll-budget", "owner-busy-poll-prefer"):
            g = c.cmd("CONFIG", "GET", name)
            if isinstance(g, list) and len(g) == 2:
                gd[g[0]] = g[1]
        steps.append(gd.get(b"owner-busy-poll-us") == b"200" and gd.get(b"owner-busy-poll-budget") == b"64" and
                     gd.get(b"owner-busy-poll-prefer") == b"yes")
        c.cmd("CONFIG", "SET", "owner-busy-poll-us", "0")
        o3 = info(c, "owners")
        steps.append(ival(o3, "owner_busy_poll_owners") == 0 and ival(o3, "owner_sleeps") == -1)
        c.cmd("CONFIG", "SET", "owner-busy-poll-us", "50")
        o4 = info(c, "owners")
        steps.append(ival(o4, "owner_busy_poll_owners") == a.n and ival(o4, "owner_sleeps") > 0 and
                     ival(o4, "owner_busy_poll_rx_packets") >= 0 and ival(o4, "owner_busy_poll_errors") == 0)
        c.close()
        check("config.set_get_range_prefer_off_on", all(steps), "steps %s; after: %s" % (steps, bp_keys(o4)))
    finally:
        s.stop()


def gen_ops(rng, nkeys, nops):
    keys = ["k%d" % i for i in range(nkeys)]
    ops = []
    for _ in range(nops):
        k = rng.choice(keys)
        r = rng.random()
        if r < 0.15:
            ops.append(("SET", k, "v%d" % rng.randrange(1000)))
        elif r < 0.30:
            ops.append(("GET", k))
        elif r < 0.38:
            ops.append(("INCR", k + ":n"))
        elif r < 0.46:
            ops.append(("HSET", k + ":h", "f%d" % rng.randrange(8), "x%d" % rng.randrange(100)))
        elif r < 0.52:
            ops.append(("HGETALL", k + ":h"))
        elif r < 0.58:
            ops.append(("LPUSH", k + ":l", "e%d" % rng.randrange(100)))
        elif r < 0.62:
            ops.append(("LRANGE", k + ":l", "0", "-1"))
        elif r < 0.66:
            ops.append(("SADD", k + ":s", "m%d" % rng.randrange(20)))
        elif r < 0.69:
            ops.append(("SMEMBERS", k + ":s"))
        elif r < 0.72:
            ops.append(("ZADD", k + ":z", str(rng.randrange(100)), "m%d" % rng.randrange(20)))
        elif r < 0.75:
            ops.append(("ZRANGE", k + ":z", "0", "-1", "WITHSCORES"))
        elif r < 0.80:
            ops.append(("EXPIRE", k, "1000"))
        elif r < 0.83:
            ops.append(("TTL", k))
        elif r < 0.87:
            ops.append(("DEL", k))
        elif r < 0.90:
            ops.append(("EXISTS", k, k + ":n"))
        elif r < 0.94:
            ops.append([("MULTI",), ("INCR", k + ":n"), ("SET", k, "m%d" % rng.randrange(1000)), ("EXEC",)])
            continue
        else:
            ops.append(("PING",))
        ops[-1] = [ops[-1]]
    return ops


def norm(op, rep):
    if op[0] == "TTL" and isinstance(rep, int):
        return rep > 0 if rep >= 0 else rep
    if op[0] in ("SMEMBERS",) and isinstance(rep, list):
        return sorted(rep)
    if op[0] == "HGETALL" and isinstance(rep, list):
        return sorted(zip(rep[::2], rep[1::2]))
    return rep


def keyspace(c):
    out = {}
    for k in sorted(c.cmd("KEYS", "*")):
        t = c.cmd("TYPE", k)
        if t == "string":
            v = c.cmd("GET", k)
        elif t == "hash":
            r = c.cmd("HGETALL", k)
            v = sorted(zip(r[::2], r[1::2]))
        elif t == "list":
            v = c.cmd("LRANGE", k, "0", "-1")
        elif t == "set":
            v = sorted(c.cmd("SMEMBERS", k))
        elif t == "zset":
            v = c.cmd("ZRANGE", k, "0", "-1", "WITHSCORES")
        else:
            v = None
        out[k] = (t, v, c.cmd("TTL", k) > 0)
    return out


def run_model(s, seed, nconn_owner=2, nkeys=200, nops=400):
    rng = random.Random(seed)
    ports = [s.oport + i for i in range(s.a.n) for _ in range(nconn_owner)] + [s.port, s.port]
    conns = [s.c(p) for p in ports]
    progs = [gen_ops(random.Random(seed * 1000 + j), nkeys, nops) for j in range(len(conns))]
    replies = []
    pos = [0] * len(conns)
    # One connection at a time, in a seeded order, a block (one command, or a MULTI ... EXEC) at once: the
    # order is the same on both servers.
    while any(pos[j] < len(progs[j]) for j in range(len(conns))):
        j = rng.choice([j for j in range(len(conns)) if pos[j] < len(progs[j])])
        for op in progs[j][pos[j]]:
            replies.append((j, op, norm(op, conns[j].cmd(*op))))
        pos[j] += 1
    ks = keyspace(conns[-1])
    for c in conns:
        c.close()
    return replies, ks


def part_model(a):
    child = Server(a, "model-child", a.bin, a.port, a.owner_port, a.cpus, a.main_cpu,
                   ["--owner-busy-poll-us", "50", "--owner-busy-poll-prefer", "yes", "--owner-busy-poll-budget", "16"])
    parent = Server(a, "model-parent", a.ref_bin, a.port + 20, a.owner_port + 20, a.cpus, a.main_cpu)
    total = 0
    for seed in (1, 2, 3):
        child.start()
        try:
            rc, kc = run_model(child, seed)
            oc = child.owners()
        finally:
            child.stop()
        parent.start()
        try:
            rp, kp = run_model(parent, seed)
        finally:
            parent.stop()
        diff = [(i, x, y) for i, (x, y) in enumerate(zip(rc, rp)) if x != y]
        total += len(rc)
        check("model.seed%d" % seed, not diff and len(rc) == len(rp) and kc == kp and
              ival(oc, "owner_busy_poll_owners") == a.n and ival(oc, "owner_native_commands") > 0,
              "%d replies, %d keys, first diff %s; keyspace equal %s; native %s, polling owners %s" % (
                  len(rc), len(kc), diff[:1], kc == kp, oc.get("owner_native_commands"),
                  oc.get("owner_busy_poll_owners")))
    note("model", "%d replies compared" % total)


def load(ports, secs, nthreads=8):
    stop = time.time() + secs
    done = [0]
    lock = threading.Lock()

    def worker(i):
        c = Conn(ports[i % len(ports)])
        n = 0
        rng = random.Random(i)
        while time.time() < stop:
            k = "L%d" % rng.randrange(10000)
            c.s.sendall(Conn.enc(["SET", k, "x" * 32]) + Conn.enc(["GET", k]))
            c.read()
            c.read()
            n += 2
        c.close()
        with lock:
            done[0] += n

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(nthreads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return done[0]


def part_inert(a):
    res = {}
    for tag, extra in (("off", []), ("on", ["--owner-busy-poll-us", "200", "--owner-busy-poll-prefer", "yes"])):
        s = Server(a, "inert-" + tag, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, extra).start()
        try:
            ports = [s.oport + i for i in range(a.n)]
            t0 = s.threads()
            o0 = s.owners()
            os.sched_setaffinity(0, set(cpu_range(a.load_cpus)))
            n = load(ports, 4)
            o1 = s.owners()
            t1 = s.threads()
            tk0 = s.owner_ticks()
            time.sleep(2)
            tk1 = s.owner_ticks()
            waits = ival(o1, "owner_epoll_waits") - ival(o0, "owner_epoll_waits")
            sl = sum(v[2] - t0.get(k, (None, 0, 0))[2] for k, v in t1.items() if v[0].startswith("owner_"))
            res[tag] = dict(cmds=n, waits=waits, sleeps=sl, sleeps_per_wait=round(sl / max(1, waits), 3),
                            rx=(ival(o1, "owner_busy_poll_rx_packets") - ival(o0, "owner_busy_poll_rx_packets")),
                            idle_owner_ticks_2s=tk1 - tk0, polling=ival(o1, "owner_busy_poll_owners"))
        finally:
            s.stop()
    note("inert", json.dumps(res))
    on, off = res["on"], res["off"]
    check("inert.loopback_no_napi", on["polling"] == a.n and on["rx"] == 0, "BusyPollRxPackets delta %d" % on["rx"])
    check("inert.owners_still_sleep", on["sleeps_per_wait"] >= 0.5 * off["sleeps_per_wait"],
          "sleeps per wait on %.3f off %.3f" % (on["sleeps_per_wait"], off["sleeps_per_wait"]))
    check("inert.idle_no_cpu", on["idle_owner_ticks_2s"] <= 2, "owner CPU ticks in 2 s idle: %d" % on["idle_owner_ticks_2s"])
    check("inert.throughput", on["cmds"] >= 0.8 * off["cmds"], "commands on %d off %d" % (on["cmds"], off["cmds"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--cpus", required=True)
    ap.add_argument("--main-cpu", type=int, required=True)
    ap.add_argument("--bio-cpu", type=int, required=True)
    ap.add_argument("--load-cpus", required=True)
    a = ap.parse_args()
    a.all_cpus = "%s,%d,%d" % (a.cpus, a.main_cpu, a.bio_cpu)
    os.makedirs(a.dir, exist_ok=True)
    for part in (part_layout, part_off_config, part_model, part_inert):
        try:
            part(a)
        except Exception as e:  # a part that cannot run is a FAIL, the rest still run
            check(part.__name__ + ".ran", False, repr(e))
    bad = [r for r in RESULTS if not r["ok"]]
    print("RLBUSYPOLLCHECK " + json.dumps(dict(passed=len(RESULTS) - len(bad), failed=len(bad))), flush=True)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
