#!/usr/bin/env python3
"""RL-g2l-repl-speed checker: replicas synced by a diskless (socket) transfer are fed by the repl-stream-sender
once their command stream starts, and replication stays exact.

  rl-replspeed-check.py --bin CAND --stock-bin D0 --dir D --port P --owner-port OP --n 4 --cpus 18-21 --main-cpu 22
      --replica-cpus 34-35 [--secs 6] [--seeds 1,2,3]

Uses d3-repl-check.py's servers and mixed load (utils/vkevo-d3 next to --bin, or --utils). Per seed:
  sender     primary (CAND, owners) + r0 = the stock replica (D0, the suite's) + r1 = CAND with appendonly yes, both
             synced diskless (the default): after the stream starts the sender owns both (repl_sender_replicas 2) and
             main queues no replica write; under load the sender's bytes grow; converge: digest / dbsize / offsets /
             WAIT equal the primary's
  psync      CLIENT KILL TYPE replica under load: both reconnect by PSYNC (sync_partial_ok +2, sync_full +0), the sender
             owns them again; converge
  flip       repl-stream-sender no, then append, under load (the sender hands both back to main and adopts them
             again); converge
  late       r2 = D0 attached by a diskless full sync while the load runs (writes pile up behind its RDB and go out
             from its cursor once its first ACK starts the stream): the sender owns 3; converge
  aof        r1 DEBUG LOADAOF keeps the primary's digest
  crash      no ASSERT / crash line in any server's log
With --suites lanes (the replica side): rA = CAND as a replica with io-threads 4 (staged loop, parallel apply,
single-shard MULTI/EXEC on lanes), rB = the same with replica-parallel-apply-multi no, rC = D0; all converge to the
primary's digest under the mixed load (MULTI blocks on one hash tag included); rA applied transactions on lanes,
rB none."""
import argparse
import os
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--bin", required=True)
ap.add_argument("--stock-bin", required=True)
ap.add_argument("--utils")
ap.add_argument("--dir", required=True)
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--owner-port", type=int, required=True)
ap.add_argument("--n", type=int, default=4)
ap.add_argument("--cpus", default="1-4")
ap.add_argument("--main-cpu", default="0")
ap.add_argument("--replica-cpus", default="5-6")
ap.add_argument("--secs", type=float, default=6.0)
ap.add_argument("--seeds", default="1,2,3")
ap.add_argument("--psync-owns", type=int, default=1, help="1: a replica that continued by PSYNC must be on the sender (B2)")
ap.add_argument("--suites", default="sender", help="sender (the B1 gene), lanes (the replica's parallel apply), "
                "dualchan (dual-channel replicas under load, RL-g2q)")
A = ap.parse_args()
U = A.utils or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(A.bin))), "utils", "vkevo-d3")
sys.path.insert(0, U)
R = __import__("d3-repl-check")
d3 = R.d3
R.A = argparse.Namespace(n=A.n, owner_port=A.owner_port, cpus=A.cpus, main_cpu=A.main_cpu, port=A.port,
                         replica_bin=A.stock_bin, replica_cpus=A.replica_cpus, secs=A.secs)
info = R.info


def snd(p):
    i = info(p.c, "replication")
    return {k: int(i.get(k, -1)) for k in ("repl_sender_replicas", "repl_sender_bytes", "repl_sender_main_queued",
                                            "repl_sender_enabled")}


def stats(p):
    i = info(p.c, "stats")
    return int(i.get("sync_full", 0)), int(i.get("sync_partial_ok", 0))


def owned(p, want, secs=10):
    t0 = time.time()
    while time.time() - t0 < secs:
        s = snd(p)
        if s["repl_sender_replicas"] == want:
            return s
        time.sleep(0.05)
    return snd(p)


def rep(name, binary, idx, extra=()):
    return R.Proc(name, binary, A.port + 1 + idx, A.dir,
                  ["--replicaof", "127.0.0.1", str(A.port), "--repl-diskless-load", "disabled"] + list(extra),
                  A.replica_cpus)


def loading(p, secs, seed, mid=None):
    L = R.Loader(p, nconn=8, seed=seed).start()
    time.sleep(secs / 2)
    out = mid() if mid else None
    time.sleep(secs / 2)
    n = L.stop()
    return n, L.errors, out


def run(seed):
    tag = "s%d" % seed
    p = R.Primary("p%d" % seed, A.bin, A.dir, ["--client-output-buffer-limit", "replica 0 0 0", "--repl-backlog-size",
                                                "64mb"])
    R.flood(p, "f%d" % seed, 20000, 200)
    r0 = rep("r0-%d" % seed, A.stock_bin, 0)
    r1 = rep("r1-%d" % seed, A.bin, 1, ["--appendonly", "yes", "--appendfsync", "everysec"])
    procs = [p, r0, r1]
    try:
        d3.check(tag + ".sender.online", R.link_up(r0) and R.link_up(r1) and R.online(p, 2), "")
        s = owned(p, 2)
        d3.check(tag + ".sender.owns_diskless", s["repl_sender_enabled"] == 1 and s["repl_sender_replicas"] == 2, s)
        b0 = s["repl_sender_bytes"]
        n, errs, mid = loading(p, A.secs, seed, lambda: snd(p))
        s = snd(p)
        d3.check(tag + ".sender.load_errors", not errs, errs[:3])
        d3.check(tag + ".sender.bytes", s["repl_sender_bytes"] > b0 and mid["repl_sender_main_queued"] == 0,
                 "sender bytes %d -> %d, main queued mid-load %d (%d commands)" % (b0, s["repl_sender_bytes"],
                                                                                  mid["repl_sender_main_queued"], n))
        R.quiesce(p, [r0, r1], tag + ".sender")

        f0, ok0 = stats(p)
        n, errs, _ = loading(p, A.secs, seed + 100, lambda: p.c.cmd("CLIENT", "KILL", "TYPE", "replica"))
        d3.check(tag + ".psync.load_errors", not errs, errs[:3])
        d3.check(tag + ".psync.relinked", R.link_up(r0) and R.link_up(r1) and R.online(p, 2), "")
        f1, ok1 = stats(p)
        d3.check(tag + ".psync.partial", f1 == f0 and ok1 == ok0 + 2, "sync_full %d -> %d, sync_partial_ok %d -> %d"
                 % (f0, f1, ok0, ok1))
        s = owned(p, 2 if A.psync_owns else 0, secs=10 if A.psync_owns else 1)
        if A.psync_owns:
            d3.check(tag + ".psync.owns", s["repl_sender_replicas"] == 2, s)
        else:  # B1 alone: a replica that continued by PSYNC stays on main, as in the parent
            print("INFO %s.psync.owns sender owns %d of 2 after PSYNC (not checked)" % (tag, s["repl_sender_replicas"]), flush=True)
        R.quiesce(p, [r0, r1], tag + ".psync")

        def flip():
            p.c.cmd("CONFIG", "SET", "repl-stream-sender", "no")
            time.sleep(0.3)
            a = snd(p)
            p.c.cmd("CONFIG", "SET", "repl-stream-sender", "append")
            return a
        n, errs, mid = loading(p, A.secs, seed + 200, flip)
        d3.check(tag + ".flip.load_errors", not errs, errs[:3])
        s = owned(p, 2)
        d3.check(tag + ".flip.owns", mid["repl_sender_enabled"] == 0 and s["repl_sender_replicas"] == 2,
                 "off %s, on again %s" % (mid, s))
        R.quiesce(p, [r0, r1], tag + ".flip")

        f0, _ = stats(p)
        box = {}

        def late():
            box["r2"] = rep("r2-%d" % seed, A.stock_bin, 2)
            procs.append(box["r2"])
            return None
        n, errs, _ = loading(p, A.secs, seed + 300, late)
        r2 = box["r2"]
        d3.check(tag + ".late.load_errors", not errs, errs[:3])
        d3.check(tag + ".late.online", R.link_up(r2) and R.online(p, 3), "")
        f1, _ = stats(p)
        s = owned(p, 3)
        d3.check(tag + ".late.full_sync_owned", f1 == f0 + 1 and s["repl_sender_replicas"] == 3,
                 "sync_full %d -> %d, %s" % (f0, f1, s))
        pd, pn = R.quiesce(p, [r0, r1, r2], tag + ".late")

        r1.c.cmd("BGREWRITEAOF") if seed % 2 == 0 else None
        time.sleep(1.5)
        r1.c.cmd("DEBUG", "LOADAOF")
        d3.check(tag + ".aof.loadaof.digest", r1.c.cmd("DEBUG", "DIGEST") == pd and r1.c.cmd("DBSIZE") == pn,
                 "primary %s %s replica %s %s" % (pd, pn, r1.c.cmd("DEBUG", "DIGEST"), r1.c.cmd("DBSIZE")))
    finally:
        for x in reversed(procs):
            x.stop()
        bad = {x.name: x.crashed() for x in procs if x.crashed()}
        d3.check(tag + ".crash", not bad, bad)


def lanes(seed):
    """the replica's parallel apply (io-threads 4: staged loop, keyspace shards, lanes): rA = CAND with lanes and
    whole-transaction lanes (replica-parallel-apply-multi yes, the default), rB = CAND with the switch off, rC = the
    stock replica; all converge to the primary's digest; rA applied transactions on lanes, rB none"""
    tag = "s%d.lanes" % seed
    p = R.Primary("lp%d" % seed, A.bin, A.dir, ["--client-output-buffer-limit", "replica 0 0 0", "--repl-backlog-size",
                                                 "64mb"])
    R.flood(p, "f%d" % seed, 20000, 200)
    ra = rep("ra-%d" % seed, A.bin, 0, ["--io-threads", "4"])
    rb = rep("rb-%d" % seed, A.bin, 1, ["--io-threads", "4", "--replica-parallel-apply-multi", "no"])
    rc = rep("rc-%d" % seed, A.stock_bin, 2)
    procs = [p, ra, rb, rc]
    try:
        d3.check(tag + ".online", all(R.link_up(r) for r in (ra, rb, rc)) and R.online(p, 3), "")
        n, errs, _ = loading(p, A.secs, seed + 400)
        d3.check(tag + ".load_errors", not errs, errs[:3])
        R.quiesce(p, [ra, rb, rc], tag)
        sa, sb = info(ra.c, "stats"), info(rb.c, "stats")
        ta, tb = int(sa.get("repl_lane_txns", -1)), int(sb.get("repl_lane_txns", -1))
        pa, pb = int(sa.get("repl_parallel_applied", -1)), int(sb.get("repl_parallel_applied", -1))
        print("INFO %s commands %d rA lane txns %d parallel %d phases %s barriers %s | rB lane txns %d parallel %d "
              "phases %s barriers %s" % (tag, n, ta, pa, sa.get("repl_apply_phases"), sa.get("repl_lane_barriers"), tb,
                                         pb, sb.get("repl_apply_phases"), sb.get("repl_lane_barriers")), flush=True)
        d3.check(tag + ".txns_on_lanes", ta > 0 and pa > 0 and tb == 0, "rA %d / %d, rB %d / %d" % (ta, pa, tb, pb))
    finally:
        for x in reversed(procs):
            x.stop()
        bad = {x.name: x.crashed() for x in procs if x.crashed()}
        d3.check(tag + ".crash", not bad, bad)


def dualchan(seed):
    """dual-channel replication under load (RL-g2q-dualchan-fix): primary CAND (owners) with
    dual-channel-replication-enabled yes; three replicas attach at once while the mixed load runs (rC = D0, rD = CAND
    with repl-diskless-load disabled, rE = CAND with swapdb), as replication.tcl's issue #141 test does; every one
    syncs over its rdb channel, continues on its main channel, and must converge to the primary's digest, dbsize and
    offset with no protocol error; the sender owns all three; then CLIENT KILL TYPE replica under load and the same
    again (the reconnect is a dual-channel sync or a PSYNC)."""
    tag = "s%d.dualchan" % seed
    dc = ["--dual-channel-replication-enabled", "yes"]
    p = R.Primary("dp%d" % seed, A.bin, A.dir, ["--client-output-buffer-limit", "replica 0 0 0", "--repl-backlog-size",
                                                 "64mb"] + dc)
    R.flood(p, "f%d" % seed, 20000, 200)
    procs = [p]
    try:
        box = {}

        def attach():
            box["r"] = [rep("rc-%d" % seed, A.stock_bin, 0, dc), rep("rd-%d" % seed, A.bin, 1, dc),
                        R.Proc("re-%d" % seed, A.bin, A.port + 3, A.dir,
                               ["--replicaof", "127.0.0.1", str(A.port), "--repl-diskless-load", "swapdb"] + dc,
                               A.replica_cpus)]
            procs.extend(box["r"])
        n, errs, _ = loading(p, A.secs, seed + 500, attach)
        rs = box["r"]
        d3.check(tag + ".load_errors", not errs, errs[:3])
        d3.check(tag + ".online", all(R.link_up(r) for r in rs) and R.online(p, 3), "")
        s = owned(p, 3)
        d3.check(tag + ".sender_owns", s["repl_sender_replicas"] == 3, s)
        R.quiesce(p, rs, tag)
        n, errs, _ = loading(p, A.secs, seed + 600, lambda: p.c.cmd("CLIENT", "KILL", "TYPE", "replica"))
        d3.check(tag + ".kill.load_errors", not errs, errs[:3])
        d3.check(tag + ".kill.online", all(R.link_up(r) for r in rs) and R.online(p, 3), "")
        R.quiesce(p, rs, tag + ".kill")
        perr = [r.name for r in procs if "Protocol error" in r.logtext() or "panicked" in r.logtext()]
        d3.check(tag + ".no_protocol_error", not perr, perr)
    finally:
        for x in reversed(procs):
            x.stop()
        bad = {x.name: x.crashed() for x in procs if x.crashed()}
        d3.check(tag + ".crash", not bad, bad)


os.makedirs(A.dir, exist_ok=True)
for sd in [int(x) for x in A.seeds.split(",")]:
    if "sender" in A.suites:
        run(sd)
    if "lanes" in A.suites:
        lanes(sd)
    if "dualchan" in A.suites:
        dualchan(sd)
res = [r["ok"] for r in d3.RESULTS]
print("REPLSPEEDCHECK pass %d fail %d" % (sum(1 for r in res if r), sum(1 for r in res if not r)), flush=True)
sys.exit(0 if all(res) else 1)
