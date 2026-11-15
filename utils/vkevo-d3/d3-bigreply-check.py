#!/usr/bin/env python3
"""D3-all-g11-bigreply check for GET values the owners send from where they are stored (owner.c ownerRefAdd /
ownerRefsWrite / ownerRefsMaterialize), against the parent's owner path (D3-all-g9-fold, which copies every
value into the reply), byte for byte.

Starts two servers with owner-threads 4 on 127.0.0.1 with the same extra config: the tree's (--bin) and the
parent's (--ref-bin). Sets up the same data through the plain port, sends the same bytes to owner 0's port of
each and compares the reply bytes and the keyspace hits / misses, cmdstat_get calls and total_net_output_bytes
each case added. Suites (each on a fresh pair of servers):
  plain      default config. Cases:
    sizes      values of 0 B to 3 MB around every threshold (owner-bigreply-min 2048, the 16 KB reply buffer,
               OWN_REF_BYTES_MAX 1 MB), INT values, missing keys; one write (bursts), 1-300 byte writes (frames
               across reads: the GET ring), one GET per write
    min_*      CONFIG SET owner-bigreply-min 64 / 4096 / 0 (off) / 1024 / 2048 at run time, the sizes again
    many       300 x 4 KB GETs in one write (past OWN_REF_MAX 256), 200 x 8 KB (past 1 MB of references)
    overwrite  in one pipeline, right after a GET of a big value: SET (new value), DEL, APPEND, SETRANGE, EXPIRE,
               PERSIST, GETDEL, SET of an int, then GET again; the replies show each GET's value as it was
    spill      big GETs, an MGET whose reply spills into the reply list, more big GETs (references then reply
               blocks)
    handoff    big GETs, then a command the owner hands to main (OBJECT ENCODING, LPUSH, EXPIRE), then GETs
    modes      RESP3, CLIENT REPLY SKIP / OFF / ON, MULTI with big GETs, a commandlog-reply-larger-than below the
               values (the copying path), an ACL key pattern
    backpressure  a client with a 4 KB receive buffer pipelines 2000 GETs of 16-64 KB values and reads only after
               the server answered them all (cmdstat_get), while another client overwrites and deletes those keys
               in between: the replies are the values as they were at each GET, as the parent's
    close      clients that close (RST) mid-reply, after a partial read and before reading anything; 40 times,
               then every port answers
    concurrent 8 readers pipeline GETs of 48 keys on owner 0 while 4 writers SET / DEL / APPEND them (values of
               1.5-70 KB that name their key, version and length): every reply is a whole value of its key or nil
               (tree only)
    load       cachecannon 80/20 GET/SET of 4 KB values, 400 conns P10, then FLUSHALL: used_memory back to its
               start (no reference leaked) (tree only, when --cachecannon is given)
    end        OBJECT REFCOUNT 1 for every big key on both, the same DEBUG DIGEST, owner_ref_replies grew, no crash
    ix_*       (D3-all-g12-fold, run with --ref-bin D3-all-g10-setburst) a GET's reference against the writes that
               replace or change its value in the same window: GET big; SET big small (the SET-burst path) / SET big
               other-big / APPEND big x, and the reverse orders (the write first), over 8 keys of 2-100 KB in one
               write, in pieces of up to 200 bytes and one frame per write: replies and stats (incl. cmdstat_set)
               equal, owner_ref_replies and owner_setburst_commands grew, the keys as the commands left them, the
               keyspace (type, encoding, value, strlen, PTTL, DEBUG DIGEST-VALUE) equal, OBJECT REFCOUNT 1, and the
               tree's replies equal to a model's
    ixbp_*     the same interactions from a client with a 4 KB receive buffer, 3 x 200 of them on 200 keys of 16-46 KB
               (small requests first: GET then SET small, or SET 2 KB then GET then SET small; then values of 2.0-2.6 KB),
               read only after every GET ran and another client overwrote / deleted the keys: replies as the model
               on both (partial writes copy the referenced rest after its object was replaced); forward and reverse
    concurrent also 2 writers of plain small SETs (SET bursts) over the keys the readers GET by reference
  aof        appendonly yes everysec: SET then GET of big values in one pipeline (replies that wait for the AOF:
             copied before they wait), sizes, overwrite, ix_*, ixbp_* forward
  aofalways  appendonly yes always: the same but ixbp_*
  mm         maxmemory 1gb allkeys-lru: sizes, overwrite, ix_* (no references; SET bursts run inside the owners'
             memory leases since D3-all-g11-maxmem, so ix_* want owner_setburst_commands to grow and refs to stay 0)
Usage: d3-bigreply-check.py --bin <tree>/src/valkey-server --ref-bin <parent>/src/valkey-server --dir D
                            --port P --owner-port OP [--cpus 8-11] [--main-cpu 16] [--cachecannon PATH --cc-cpus 0-7]
Prints PASS/FAIL/INFO lines and BIGREPLYCHECK {checks, fails}; exit 1 on any FAIL.
"""
import argparse
import json
import os
import random
import socket
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
d3 = __import__("d3-check")  # Conn, Err, key_slot, check, info, ostat, RESULTS
bc = __import__("d3-burst-check")  # Raw, Server, reply_end, enc, key_in

ALNUM = bc.ALNUM
enc = bc.enc


def get(k):
    return enc(b"GET", k)


def stats(s):
    st = d3.info(s.plain, "stats")
    cs = d3.info(s.plain, "commandstats")
    calls = 0
    if "cmdstat_get" in cs:
        calls = int(cs["cmdstat_get"].split("calls=")[1].split(",")[0])
    scalls = 0
    if "cmdstat_set" in cs:
        scalls = int(cs["cmdstat_set"].split("calls=")[1].split(",")[0])
    ow = d3.ostat(s.plain)
    return dict(hits=int(st["keyspace_hits"]), misses=int(st["keyspace_misses"]), get_calls=calls,
                set_calls=scalls, net_out=int(st["total_net_output_bytes"]), refs=ow.get("owner_ref_replies", 0),
                copied=ow.get("owner_ref_copied", 0), setburst=ow.get("owner_setburst_commands", 0))


def fill(seed, n):
    """n deterministic bytes."""
    r = random.Random(seed)
    return bytes(r.choice(ALNUM) for _ in range(min(n, 4096))) * (n // 4096 + 1)


def body(n, seed):
    return fill(seed, n)[:n]


class Pair:
    def __init__(self, a, tag, extra=()):
        self.a = a
        self.tag = tag
        self.srvs = [bc.Server("tree-" + tag, a.bin, a.port, a.owner_port, a.cpus, a.main_cpu, a.dir, list(extra)),
                     bc.Server("ref-" + tag, a.ref_bin, a.port + 10, a.owner_port + 20, a.cpus, a.main_cpu, a.dir,
                               list(extra))]
        t, r = self.srvs
        d3.check("%s.topology" % tag, t.ranges == r.ranges and len(t.ranges) == 4, t.ranges)
        self.lo0, self.hi0 = t.ranges[0]
        self.lo1, self.hi1 = t.ranges[1]

    def setup(self, cmds):
        for s in self.srvs:
            s.plain.send(cmds)
            for _ in cmds:
                s.plain.read()

    def both(self, *args):
        return [s.plain.cmd(*args) for s in self.srvs]

    def case(self, name, data, nrep, chunks=None, prelude=None, rounds=None, single=None, want_refs=None,
             want_sb=None):
        """Send data to owner 0 of both servers; compare the replies and the stats deltas. rounds: (bytes, n)
        pairs sent one after another; single: frames sent one per write."""
        outs, deltas, nets = [], [], []
        for s in self.srvs:
            b0 = stats(s)
            st0 = stats(s)
            base = st0["net_out"] - b0["net_out"]  # the bytes of one stats() call's own INFO replies
            r = bc.Raw(s.oport)
            pre = 0
            if prelude:
                r.send(prelude[0])
                pre = sum(map(len, r.replies(prelude[1])))
            got = []
            if rounds:
                for rb, rn in rounds:
                    r.send(rb)
                    got += r.replies(rn)
            elif single:
                for fr in single:
                    r.send(fr)
                    got += r.replies(1)
            else:
                r.send(data, chunks, random.Random(len(data)))
                got = r.replies(nrep)
            r.close()
            time.sleep(0.02)
            st1 = stats(s)
            outs.append(got)
            deltas.append({k: st1[k] - st0[k] for k in st0})
            nets.append((st1["net_out"] - st0["net_out"] - base, pre + sum(map(len, got))))
        same = outs[0] == outs[1]
        first = next((i for i in range(min(len(outs[0]), len(outs[1]))) if outs[0][i] != outs[1][i]), -1)
        d3.check("%s.%s.replies" % (self.tag, name), same,
                 "%d replies, %d bytes" % (len(outs[0]), sum(map(len, outs[0]))) if same else
                 "reply %d: tree %r ref %r" % (first, outs[0][first][:100], outs[1][first][:100]))
        keys = ("hits", "misses", "get_calls", "set_calls")
        a, b = ({k: d[k] for k in keys} for d in deltas)
        d3.check("%s.%s.stats" % (self.tag, name), a == b, "tree %s ref %s" % (a, b))
        # total_net_output_bytes grew by the bytes the client read (give or take the INFO replies' digits)
        d3.check("%s.%s.net_out" % (self.tag, name), all(abs(x - y) <= 64 for x, y in nets),
                 "tree / ref (counted, read): %s" % nets)
        if want_refs is not None:
            d3.check("%s.%s.refs" % (self.tag, name), (deltas[0]["refs"] > 0) == want_refs,
                     "owner_ref_replies +%d (want %s) copied +%d" % (deltas[0]["refs"], "some" if want_refs else "none",
                                                                     deltas[0]["copied"]))
        if want_sb is not None:
            d3.check("%s.%s.setburst" % (self.tag, name), (deltas[0]["setburst"] > 0) == want_sb,
                     "owner_setburst_commands +%d (want %s), owner_ref_replies +%d" % (
                         deltas[0]["setburst"], "some" if want_sb else "none", deltas[0]["refs"]))
        return deltas[0]

    def stop(self):
        for s in self.srvs:
            alive = all(d3.Conn("127.0.0.1", s.oport + i).cmd("PING") == "PONG" for i in range(4))
            d3.check("%s.%s.alive" % (self.tag, s.name), alive and s.plain.cmd("PING") == "PONG")
            s.stop()
            d3.check("%s.%s.no_crash" % (self.tag, s.name), s.crashed() == 0, "crash lines %d" % s.crashed())


SIZES = [0, 1, 63, 64, 65, 100, 511, 512, 1000, 1023, 1024, 1025, 2047, 2048, 4095, 4096, 4097, 8192, 16000, 16370,
         16383, 16384, 16385, 20000, 65536, 100000, 524288, 1048575, 1048576, 1048577, 3 * 1048576]


def sized_keys(p, rng, prefix=b"sz"):
    ks, vals = [], {}
    for i, L in enumerate(SIZES):
        k = bc.key_in(rng, 16, p.lo0, p.hi0, prefix)
        ks.append(k)
        vals[k] = body(L, (prefix, L))
    for v in (b"0", b"12345", b"-7", b"%d" % 10**15):
        k = bc.key_in(rng, 16, p.lo0, p.hi0, prefix + b"i")
        ks.append(k)
        vals[k] = v
    return ks, vals


def sizes_cases(p, rng, tag, want_refs):
    ks, vals = sized_keys(p, rng, b"sz" + tag.encode())
    p.setup([("SET", k, v) for k, v in vals.items()])
    miss = [bc.key_in(rng, 16, p.lo0, p.hi0, b"miss") for _ in range(3)]
    order = ks + miss + ks[::-1] + [ks[i] for i in rng.sample(range(len(ks)), len(ks))]
    frames = [get(k) for k in order]
    data = b"".join(frames)
    p.case("sizes_%s" % tag, data, len(frames), want_refs=want_refs)
    p.case("sizes_%s_chunks" % tag, data, len(frames), chunks=300)
    p.case("sizes_%s_single" % tag, None, len(frames[:40]), single=frames[:40])
    return ks, vals


def overwrite_case(p, rng, tag):
    b1, b2, b3, b4, b5 = (bc.key_in(rng, 16, p.lo0, p.hi0, b"ow" + tag.encode()) for _ in range(5))
    p.setup([("SET", b1, body(4096, 1)), ("SET", b2, body(20000, 2)), ("SET", b3, body(1500, 3)),
             ("SET", b4, body(70000, 4)), ("SET", b5, body(3000, 5))])
    seq = [get(b1), enc("SET", b1, body(5000, 11)), get(b1), get(b2), enc("DEL", b2), get(b2), get(b3),
           enc("APPEND", b3, b"!" * 3000), get(b3), get(b4), enc("SETRANGE", b4, "10", "XYZ" * 500), get(b4),
           get(b5), enc("EXPIRE", b5, "1000"), get(b5), enc("PERSIST", b5), get(b5), enc("GETDEL", b5), get(b5),
           get(b1), enc("SET", b1, "12345"), get(b1), enc("INCR", b1), get(b1), enc("SET", b1, body(9000, 12)),
           get(b1), get(b1), enc("DEL", b1), get(b1), enc("SET", b2, body(2000, 13)), get(b2), enc("PING")]
    p.case("overwrite_%s" % tag, b"".join(seq), len(seq))
    p.case("overwrite_%s_chunks" % tag, b"".join(seq), len(seq), chunks=200)


# D3-all-g12-fold: a GET's value reference (g11-bigreply) against the writes that replace or change that value in
# the same window, SET bursts (g10-setburst, values of 1-99 B) among them. Each kind is one key's commands; a case
# runs it over every interaction key in one write, in random pieces of up to 200 bytes (frames across reads, SETs
# then parsed as well as burst) and one frame per write; replies and stats against --ref-bin byte for byte, then the
# keyspace of those keys (type, encoding, value, length, TTL, DEBUG DIGEST-VALUE) on both, and OBJECT REFCOUNT 1.
def small(seed):
    """A SET-burst value: 21-61 B, starts with a letter (never an integer), embeddable with a 16 B key."""
    r = random.Random(repr(seed))
    return b"s" + bytes(r.choice(ALNUM) for _ in range(r.randint(20, 60)))


IKINDS = ("get_setsmall", "get_setbig", "get_append", "setsmall_get", "setbig_get", "append_get", "mixed")


def ikind(kind, ks, i, ver=0):
    """Command tuples of one interaction for key ks[i]: the GET of the big value first, or the write first."""
    k = ks[i % len(ks)]
    sm = small((kind, i, ver))
    bg = body(2048 + 997 * (i % 9), (kind, i, ver))
    if kind == "get_setsmall":
        return [("GET", k), ("SET", k, sm), ("GET", k)]
    if kind == "get_setbig":
        return [("GET", k), ("SET", k, bg), ("GET", k)]
    if kind == "get_append":
        return [("GET", k), ("APPEND", k, b"x"), ("GET", k)]
    if kind == "setsmall_get":
        return [("SET", k, sm), ("GET", k), ("SET", k, bg), ("GET", k)]
    if kind == "setbig_get":
        return [("SET", k, bg), ("GET", k), ("GET", k)]
    if kind == "append_get":
        return [("APPEND", k, b"x"), ("GET", k), ("APPEND", k, b"y" * 3000), ("GET", k)]
    return [("GET", k), ("SET", k, sm), ("GET", k), ("SET", k, bg), ("GET", k), ("APPEND", k, b"x"), ("GET", k),
            ("GET", ks[(i + 1) % len(ks)]), ("SET", ks[(i + 1) % len(ks)], sm), ("GET", k)]


def simulate(state, cmds):
    """The replies a server owes cmds (GET, SET, APPEND, DEL) from state, which it updates."""
    out = []
    for c in cmds:
        op, k = c[0], c[1]
        if op == "GET":
            v = state.get(k)
            out.append(b"$-1\r\n" if v is None else b"$%d\r\n%s\r\n" % (len(v), v))
        elif op == "SET":
            state[k] = c[2]
            out.append(b"+OK\r\n")
        elif op == "APPEND":
            state[k] = state.get(k, b"") + c[2]
            out.append(b":%d\r\n" % len(state[k]))
        elif op == "DEL":
            out.append(b":%d\r\n" % (1 if state.pop(k, None) is not None else 0))
    return out


IBIG = [2048, 3000, 4096, 9000, 16384, 20000, 65536, 100000]


def keyspace(p, keys, name):
    """Every key's type, encoding, value, length, TTL and DEBUG DIGEST-VALUE equal on both; refcount 1 on both
    (asked first, in its own pipeline: main's own GET replies hold a reference until they are written)."""
    views, rcs = [], []
    for s in p.srvs:
        s.plain.send([("OBJECT", "REFCOUNT", k) for k in keys])
        rcs += [(s.name, k, s.plain.read()) for k in keys]
        cmds = []
        for k in keys:
            cmds += [("TYPE", k), ("OBJECT", "ENCODING", k), ("GET", k), ("STRLEN", k), ("PTTL", k),
                     ("DEBUG", "DIGEST-VALUE", k)]
        s.plain.send(cmds)
        views.append([s.plain.read() for _ in cmds])
    strip = views
    first = next((i for i in range(len(strip[0])) if strip[0][i] != strip[1][i]), -1)
    d3.check("%s.%s.keyspace" % (p.tag, name), strip[0] == strip[1],
             "%d keys: type, encoding, value, strlen, pttl, digest equal" % len(keys) if first < 0 else
             "key %r field %d: tree %r ref %r" % (keys[first // 6], first % 6, str(strip[0][first])[:80],
                                                  str(strip[1][first])[:80]))
    odd = [(n, k, x) for n, k, x in rcs if not (x in (1, None) or isinstance(x, d3.Err))]
    d3.check("%s.%s.refcount" % (p.tag, name), not odd,
             "OBJECT REFCOUNT: %s%s" % (sorted(set(str(x) for _, _, x in rcs)), " not 1: %s" % odd[:4] if odd else ""))


def interact_cases(p, rng, tag, want_refs, want_sb):
    ks = [bc.key_in(rng, 16, p.lo0, p.hi0, b"ix" + tag.encode()) for _ in range(len(IBIG))]
    init = {k: body(L, (b"ix", tag, j)) for j, (k, L) in enumerate(zip(ks, IBIG))}
    for kind in IKINDS:
        cmds = []
        for i in range(len(ks)):
            cmds += ikind(kind, ks, i)
        want = simulate(dict(init), cmds)
        frames = [enc(*c) for c in cmds]
        data = b"".join(frames)
        for form in ("one", "chunks", "single"):
            p.setup([("SET", k, v) for k, v in init.items()])
            name = "ix_%s_%s_%s" % (tag, kind, form)
            # A SET burst runs where a read starts at a plain SET frame, or (GET bursts on) after the GET burst
            # before it; under maxmemory GET bursts are off, so only a pipeline that starts with the SET bursts.
            sb_kind = kind == "setsmall_get" if tag == "mm" else "setsmall" in kind
            sb = want_sb if (form == "one" and sb_kind) else None
            wr = want_refs if (form == "one" and kind.startswith("get_")) else None
            if form == "one":
                p.case(name, data, len(frames), want_refs=wr, want_sb=sb)
            elif form == "chunks":
                p.case(name, data, len(frames), chunks=200)
            else:
                p.case(name, None, len(frames), single=frames)
            got = [s.plain.cmd("GET", k) for s in p.srvs for k in ks]
            st = dict(init)
            simulate(st, cmds)
            d3.check("%s.final" % name, got == [st.get(k) for k in ks] * 2, "the keys as the commands left them")
            keyspace(p, ks, name)
        # the model's replies: the pair agreed above, here both agree with what the commands owe
        p.setup([("SET", k, v) for k, v in init.items()])
        gots = []
        for s in p.srvs:
            r = bc.Raw(s.oport)
            r.send(data)
            gots.append(r.replies(len(frames)))
            r.close()
        got = gots[0]
        first = next((i for i in range(len(want)) if got[i] != want[i]), -1)
        d3.check("%s.ix_%s.model" % (p.tag, kind), gots[0] == want and gots[1] == want,
                 "%d replies as the model on both" % len(want) if first < 0 else
                 "reply %d (%r): tree %r model %r" % (first, cmds[first][:2], got[first][:60], want[first][:60]))


def interact_backpressure(p, rng, tag, reverse):
    """A client with a 4 KB receive buffer pipelines 600 interactions on 200 keys of 16-46 KB values (GET then SET
    of a small value, SET of another big value, APPEND; or the writes first with reverse) and reads only after the
    server ran them all; then another client overwrites and deletes the keys before the reads. The replies are what
    the commands owed at each step (the model) on both servers, the keyspace equal."""
    nm = "ixbp_%s_%s" % (tag, "rev" if reverse else "fwd")
    ks = [bc.key_in(rng, 16, p.lo0, p.hi0, b"ib" + tag.encode()) for _ in range(200)]
    init = {k: body(16384 + 150 * i, (b"ib", tag, i)) for i, k in enumerate(ks)}
    # Three phases in one pipeline. The first is all small requests, so the first reads (before the socket fills)
    # hold many references and the writes that replace their objects: forward GET then SET of a small value
    # (GET and SET bursts take turns), reverse SET of a 2 KB value, GET, SET small (the parser's path). The other
    # two replace the values with others of 2.0-2.6 KB (by reference too, small requests) and change them again;
    # forward's APPEND hands the client to main, which gets plain bytes (ownerHandOff copies the references).
    # D3-all-g14-fold: reverse runs its three phases three times (other values each round), ~5.6 MB of replies: one
    # round's 1.86 MB can fit in the server socket's autotuned send buffer (tcp_wmem max 4 MB) with no partial write
    # at all (g12 / g13 took their first partial write after 794 of 800 references; this tree's runs after none).
    cmds = []
    for rd in range(3 if reverse else 1):
        for ph in range(3):
            for i, k in enumerate(ks):
                sm, mid = small((nm, rd, ph, i)), body(2048 + 13 * (i % 40), (nm, rd, ph, i))
                if not reverse:
                    cmds += ([("GET", k), ("SET", k, sm)], [("SET", k, mid), ("GET", k), ("APPEND", k, b"x"), ("GET", k)],
                             [("GET", k), ("SET", k, sm)])[ph]
                else:
                    # APPEND would hand the client to main (not native on the owners): the writes here are SETs
                    cmds += ([("SET", k, mid), ("GET", k), ("SET", k, sm)],
                             [("SET", k, sm), ("GET", k), ("SET", k, mid), ("GET", k)],
                             [("SET", k, mid + b"z"), ("GET", k), ("GET", k), ("SET", k, sm)])[ph]
    want = b"".join(simulate(dict(init), cmds))
    final = dict(init)
    simulate(final, cmds)
    ngets = sum(1 for c in cmds if c[0] == "GET")
    outs, deltas = [], []
    for s in p.srvs:
        s.plain.send([("SET", k, v) for k, v in init.items()])
        for _ in ks:
            s.plain.read()
        st0 = stats(s)
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sk.connect(("127.0.0.1", s.oport))
        sk.settimeout(120)
        sk.sendall(b"".join(enc(*c) for c in cmds))
        t0 = time.time()
        while stats(s)["get_calls"] - st0["get_calls"] < ngets and time.time() - t0 < 90:
            time.sleep(0.05)
        answered = stats(s)["get_calls"] - st0["get_calls"]
        s.plain.send([("SET", k, b"late" + v[:50]) if i % 2 else ("DEL", k) for i, (k, v) in enumerate(init.items())])
        for _ in ks:
            s.plain.read()
        got = bytearray(len(want))
        mv, n = memoryview(got), 0
        while n < len(want):
            r = sk.recv_into(mv[n:], min(len(want) - n, 1 << 20))
            if not r:
                break
            n += r
        sk.settimeout(0.3)
        try:
            extra = sk.recv(100)
        except socket.timeout:
            extra = b""
        sk.close()
        outs.append(bytes(got[:n]) + extra)
        st1 = stats(s)
        deltas.append({k: st1[k] - st0[k] for k in st0})
        d3.check("%s.%s.%s.answered" % (p.tag, nm, s.name), answered == ngets,
                 "%d of %d GETs ran before the reads" % (answered, ngets))
    first = [next((i for i in range(min(len(o), len(want))) if o[i] != want[i]), -1) for o in outs]
    d3.check("%s.%s.replies" % (p.tag, nm), outs[0] == want and outs[1] == want,
             "%d commands, %d reply bytes as the model (read tree %d ref %d; first diff at %s)" % (
                 len(cmds), len(want), len(outs[0]), len(outs[1]), first))
    print("INFO %s.%s tree owner_ref_replies +%d copied +%d setburst +%d; ref setburst +%d" % (
        p.tag, nm, deltas[0]["refs"], deltas[0]["copied"], deltas[0]["setburst"], deltas[1]["setburst"]))
    if p.tag == "plain":
        d3.check("%s.%s.paths" % (p.tag, nm), deltas[0]["refs"] > 50 and deltas[0]["copied"] > 0,
                 "owner_ref_replies +%d, owner_ref_copied +%d (the socket was full)" % (deltas[0]["refs"],
                                                                                       deltas[0]["copied"]))
    keyspace(p, ks, nm)


def suite_plain(a, rng):
    p = Pair(a, "plain")
    ks, vals = sizes_cases(p, rng, "def", True)
    for m, want in (("64", True), ("4096", True), ("0", False), ("1024", True), ("2048", True)):
        r = p.both("CONFIG", "SET", "owner-bigreply-min", m)
        d3.check("plain.min_%s.set" % m, r[0] == "OK", r)  # the parent has no such config: an error there
        p.case("min_%s" % m, b"".join(get(k) for k in ks), len(ks), want_refs=want)
    # many references in one read: past OWN_REF_MAX and OWN_REF_BYTES_MAX
    m4 = [bc.key_in(rng, 16, p.lo0, p.hi0, b"m4") for _ in range(50)]
    m8 = [bc.key_in(rng, 16, p.lo0, p.hi0, b"m8") for _ in range(30)]
    p.setup([("SET", k, body(4096, k)) for k in m4] + [("SET", k, body(8192, k)) for k in m8])
    p.case("many_4k", b"".join(get(rng.choice(m4)) for _ in range(300)), 300, want_refs=True)
    p.case("many_8k", b"".join(get(rng.choice(m8)) for _ in range(200)), 200, want_refs=True)
    p.case("many_rounds", None, 600, rounds=[(b"".join(get(rng.choice(m4 + m8)) for _ in range(100)), 100)
                                             for _ in range(6)])
    overwrite_case(p, rng, "def")
    interact_cases(p, rng, "def", True, True)
    interact_backpressure(p, rng, "def", False)
    interact_backpressure(p, rng, "def", True)
    # spill: references, then an MGET whose reply goes to the reply list, then more big GETs
    tg = b"{sp%d}" % next(i for i in range(100000) if p.lo0 <= d3.key_slot(b"sp%d" % i) <= p.hi0)
    sp = [tg + b":%d" % j for j in range(8)]
    p.setup([("SET", k, body(6000, k)) for k in sp])
    seq = [get(m4[0]), get(m4[1]), get(m8[0]), enc("MGET", *sp), get(m4[2]), get(m8[1]), enc("MGET", *sp[:3]),
           get(m4[3]), get(sp[0])]
    p.case("spill", b"".join(seq), len(seq))
    seq2 = [get(m4[i % 50]) for i in range(5)] + [enc("MGET", *sp)] * 4 + [get(m8[i % 30]) for i in range(5)]
    p.case("spill2", b"".join(seq2), len(seq2))
    # handoff to main between big GETs
    lst = bc.key_in(rng, 16, p.lo0, p.hi0, b"lst")
    seq = [get(m4[0]), get(m8[0]), enc("OBJECT", "ENCODING", m4[0]), get(m4[1]), enc("LPUSH", lst, "x"), get(m8[1]),
           get(m4[2]), enc("EXPIRE", m4[3], "1000"), get(m4[3]), enc("TTL", m4[3]), get(m8[2])]
    p.case("handoff", b"".join(seq), len(seq))
    p.case("handoff_chunks", b"".join(seq), len(seq), chunks=150)
    # reply modes
    big = [m4[5], m8[5], ks[SIZES.index(100000)], ks[SIZES.index(1024)]]
    p.case("resp3", b"".join(get(k) for k in big + [b"nokey"]), 5, prelude=(enc("HELLO", "3"), 1))
    p.case("reply_skip", b"".join([enc("CLIENT", "REPLY", "SKIP"), get(big[0]), get(big[1]), get(big[2])]), 2)
    p.case("reply_off", b"".join([enc("CLIENT", "REPLY", "OFF"), get(big[0]), get(big[1]), enc("CLIENT", "REPLY", "ON"),
                                  get(big[2])]), 2)
    p.case("multi", b"".join([enc("MULTI"), get(big[0]), get(big[1]), enc("SET", big[0], body(5000, 99)), get(big[0]),
                              enc("EXEC"), get(big[0]), get(big[3])]), 8)
    p.setup([("SET", big[0], body(4096, b"restore"))])
    for s in p.srvs:
        s.plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", "2000")
    p.case("reqlog_reply", b"".join(get(k) for k in big + m4[:10]), 14)
    for s in p.srvs:
        s.plain.cmd("CONFIG", "SET", "commandlog-reply-larger-than", "1mb")
    ka = [bc.key_in(rng, 16, p.lo0, p.hi0, b"ka") for _ in range(2)]
    kb = bc.key_in(rng, 16, p.lo0, p.hi0, b"kb")
    p.setup([("SET", ka[0], body(5000, 1)), ("SET", ka[1], body(6000, 2)), ("SET", kb, body(7000, 3)),
             ("ACL", "SETUSER", "bu", "on", ">pw", "~ka*", "+@all")])
    p.case("acl", b"".join([get(ka[0]), get(kb), get(ka[1])]), 3, prelude=(enc("AUTH", "bu", "pw"), 1))

    # backpressure: a 4 KB receive buffer, 2000 GETs of 16-64 KB values, overwrites after the server answered them
    bp = [bc.key_in(rng, 16, p.lo0, p.hi0, b"bp") for _ in range(40)]
    bpv = {k: body(16384 + 600 * i, (b"bp", i)) for i, k in enumerate(bp)}
    order = [bp[i % 40] for i in range(2000)]
    want = b"".join(b"$%d\r\n%s\r\n" % (len(bpv[k]), bpv[k]) for k in order)
    outs, copied = [], []
    for s in p.srvs:
        s.plain.send([("SET", k, v) for k, v in bpv.items()])
        for _ in bp:
            s.plain.read()
        st0 = stats(s)
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sk.connect(("127.0.0.1", s.oport))
        sk.settimeout(120)
        sk.sendall(b"".join(get(k) for k in order))
        t0 = time.time()
        while stats(s)["get_calls"] - st0["get_calls"] < 2000 and time.time() - t0 < 60:
            time.sleep(0.05)
        answered = stats(s)["get_calls"] - st0["get_calls"]
        # every GET ran: now change the keys the unsent replies refer to
        s.plain.send([("SET", k, b"new" + v[:100]) if i % 2 else ("DEL", k) for i, (k, v) in enumerate(bpv.items())])
        for _ in bp:
            s.plain.read()
        got = bytearray(len(want))
        mv, n = memoryview(got), 0
        while n < len(want):
            k = sk.recv_into(mv[n:], min(len(want) - n, 1 << 20))
            if not k:
                break
            n += k
        sk.settimeout(0.3)
        try:
            extra = sk.recv(100)
        except socket.timeout:
            extra = b""
        sk.close()
        got = bytes(got[:n]) + extra
        st1 = stats(s)
        outs.append(got)
        copied.append(st1["copied"] - st0["copied"])
        d3.check("plain.backpressure.%s.answered" % s.name, answered == 2000, "%d GETs ran before the reads" % answered)
    d3.check("plain.backpressure.replies", outs[0] == want and outs[1] == want,
             "2000 replies, %d bytes, all as at their GET (read tree %d ref %d)" % (len(want), len(outs[0]), len(outs[1])))
    d3.check("plain.backpressure.copied", copied[0] > 0, "owner_ref_copied +%d (the socket was full)" % copied[0])

    # close: RST mid-reply, after a partial read, before any read
    cl = [bc.key_in(rng, 16, p.lo0, p.hi0, b"cl") for _ in range(20)]
    p.setup([("SET", k, body(30000 + 1000 * i, (b"cl", i))) for i, k in enumerate(cl)])
    for s in p.srvs:
        for j in range(40):
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sk.connect(("127.0.0.1", s.oport))
            sk.sendall(b"".join(get(cl[(i + j) % 20]) for i in range(300)))
            if j % 3 == 1:
                sk.recv(rng.randint(1, 20000))
            elif j % 3 == 2:
                time.sleep(0.01)
                sk.recv(65536)
            sk.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            sk.close()
        time.sleep(0.3)
        c = d3.Conn("127.0.0.1", s.oport)
        v = c.cmd("GET", cl[3])
        c.close()
        d3.check("plain.close.%s.serves" % s.name, v == body(33000, (b"cl", 3)), "a GET after 40 closes")
    p.case("after_close", b"".join(get(k) for k in cl), 20)

    end(p, ks + m4 + m8 + bp + cl, rng)  # before the tree-only cases below change the tree's data
    concurrent(p, rng)
    if a.cachecannon:
        load(p, a)
    p.stop()


def make_val(key, ver, n):
    head = b"%s|%d|%d|" % (key, ver, n)
    return (head + fill((key, ver), n))[:max(n, len(head))]


def valid(key, v):
    if v is None or v.strip(b"#") == b"":  # nil, or APPEND to a deleted key
        return True
    try:
        k, ver, n, _ = v.split(b"|", 3)
    except ValueError:
        return False
    if k != key:
        return False
    n = int(n)
    base = make_val(key, int(ver), n)
    rest = v[len(base):]
    return v[:len(base)] == base and rest.strip(b"#") == b""


def concurrent(p, rng):
    """8 readers and 4 writers on 48 keys of owner 0, 6 s: every reply is a whole value of its key, or nil."""
    s = p.srvs[0]
    keys = [bc.key_in(rng, 16, p.lo0, p.hi0, b"cc") for _ in range(48)]
    sizes = [1500, 4096, 9000, 20000, 70000]
    s.plain.send([("SET", k, make_val(k, 0, sizes[i % 5])) for i, k in enumerate(keys)])
    for _ in keys:
        s.plain.read()
    st0 = stats(s)
    stop = time.time() + 6
    bad, nread, nwrite = [], [0], [0]

    def reader(i):
        r = random.Random(i)
        c = d3.Conn("127.0.0.1", s.oport)
        while time.time() < stop:
            ks = [r.choice(keys) for _ in range(20)]
            c.send([("GET", k) for k in ks])
            for k in ks:
                v = c.read()
                nread[0] += 1
                if isinstance(v, d3.Err) or not valid(k, v):
                    bad.append((k, (v or b"")[:80]))
        c.close()

    def writer(i):
        r = random.Random(100 + i)
        c = d3.Conn("127.0.0.1", s.oport)
        ver = 1000 * (i + 1)
        while time.time() < stop:
            cmds = []
            for _ in range(8):
                k = r.choice(keys)
                x = r.random()
                ver += 1
                if x < 0.6:
                    cmds.append(("SET", k, make_val(k, ver, r.choice(sizes))))
                elif x < 0.8:
                    cmds.append(("DEL", k))
                else:
                    cmds.append(("APPEND", k, b"#" * r.choice([1, 100, 5000])))
            c.send(cmds)
            for _ in cmds:
                c.read()
                nwrite[0] += 1
        c.close()

    def burst_writer(i):
        # D3-all-g12-fold: pipelines of plain small SETs only (SET bursts) over the keys the readers GET by reference
        r = random.Random(200 + i)
        c = d3.Conn("127.0.0.1", s.oport)
        ver = 100000 * (i + 1)
        while time.time() < stop:
            cmds = []
            for _ in range(8):
                ver += 1
                k = r.choice(keys)
                cmds.append(("SET", k, make_val(k, ver, r.choice([30, 60, 90]))))
            c.send(cmds)
            for _ in cmds:
                c.read()
                nwrite[0] += 1
        c.close()

    ts = [threading.Thread(target=reader, args=(i,)) for i in range(8)] + \
         [threading.Thread(target=writer, args=(i,)) for i in range(4)] + \
         [threading.Thread(target=burst_writer, args=(i,)) for i in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    st1 = stats(s)
    d3.check("plain.concurrent.values", not bad and nread[0] > 1000,
             "%d GETs, %d writes, %d bad %s; owner_ref_replies +%d copied +%d" % (
                 nread[0], nwrite[0], len(bad), bad[:2], st1["refs"] - st0["refs"], st1["copied"] - st0["copied"]))
    d3.check("plain.concurrent.refs", st1["refs"] - st0["refs"] > 100, "owner_ref_replies +%d" % (st1["refs"] - st0["refs"]))
    d3.check("plain.concurrent.setburst", st1["setburst"] - st0["setburst"] > 100,
             "owner_setburst_commands +%d (small SETs over referenced values)" % (st1["setburst"] - st0["setburst"]))
    rc = [s.plain.cmd("OBJECT", "REFCOUNT", k) for k in keys]
    d3.check("plain.concurrent.refcount", all(x in (1, None) or isinstance(x, d3.Err) for x in rc),
             "OBJECT REFCOUNT of the 48 keys: %s" % sorted(set(map(str, rc))))
    s.plain.send([("DEL", k) for k in keys])
    for _ in keys:
        s.plain.read()


def load(p, a):
    s = p.srvs[0]
    s.plain.cmd("FLUSHALL", "SYNC")
    time.sleep(0.3)
    m0 = int(d3.info(s.plain, "memory")["used_memory"])
    toml = os.path.join(a.dir, "bigreply-load.toml")
    tmpl = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cachecannon-d3.toml.tmpl")).read()
    subs = dict(DURATION="6", WARMUP="1", THREADS="8", CLIENT_CPUS=a.cc_cpus, SEED="127.0.0.1:%d" % s.oport,
                CONNS="400", PIPELINE="10", KEYS="100000", DIST="uniform", GET="80", SET="20", VLEN="4096")
    for k, v in subs.items():
        tmpl = tmpl.replace("@%s@" % k, v)
    open(toml, "w").write(tmpl)
    st0 = stats(s)
    out = subprocess.run([a.cachecannon, toml], capture_output=True, text=True, timeout=300).stdout
    res = [json.loads(l) for l in out.splitlines() if '"type":"result"' in l]
    st1 = stats(s)
    ok = bool(res) and res[-1].get("err_pct", 1) == 0
    d3.check("plain.load.run", ok and st1["refs"] - st0["refs"] > 10000,
             "ops %s err_pct %s; owner_ref_replies +%d copied +%d" % (
                 round(res[-1]["throughput"]) if res else None, res[-1].get("err_pct") if res else None,
                 st1["refs"] - st0["refs"], st1["copied"] - st0["copied"]))
    s.plain.cmd("FLUSHALL", "SYNC")
    time.sleep(0.5)
    m1 = int(d3.info(s.plain, "memory")["used_memory"])
    d3.check("plain.load.memory", m1 - m0 < 8 << 20, "used_memory %d before the load, %d after FLUSHALL" % (m0, m1))


def end(p, keys, rng):
    for s in p.srvs:
        rc = [s.plain.cmd("OBJECT", "REFCOUNT", k) for k in keys]
        d3.check("%s.end.%s.refcount" % (p.tag, s.name), all(x in (1, None, 2147483647) or isinstance(x, d3.Err) for x in rc),
                 "OBJECT REFCOUNT of %d keys: %s" % (len(keys), sorted(set(map(str, rc)))))
    dg = p.both("DEBUG", "DIGEST")
    d3.check("%s.end.digest" % p.tag, dg[0] == dg[1], dg)
    ow = d3.ostat(p.srvs[0].plain)
    print("INFO %s.end owner_ref_replies %d bytes %d writes %d copied %d copied_bytes %d" % (
        p.tag, ow.get("owner_ref_replies", 0), ow.get("owner_ref_bytes", 0), ow.get("owner_ref_writes", 0),
        ow.get("owner_ref_copied", 0), ow.get("owner_ref_copied_bytes", 0)))


def suite_cfg(a, rng, tag, extra, want_refs):
    p = Pair(a, tag, extra)
    ks, vals = sizes_cases(p, rng, tag, want_refs)
    overwrite_case(p, rng, tag)
    interact_cases(p, rng, tag, want_refs, True)  # SET bursts run under appendonly and (D3-all-g11-maxmem) maxmemory
    if tag == "aof":
        interact_backpressure(p, rng, tag, False)
    # writes then reads of the same big keys in one pipeline: under appendonly the GET replies wait for the AOF
    w = [bc.key_in(rng, 16, p.lo0, p.hi0, b"aw" + tag.encode()) for _ in range(10)]
    seq = []
    for i, k in enumerate(w):
        seq += [enc("SET", k, body(3000 + 1000 * i, (tag, i))), get(k), get(ks[SIZES.index(4096)])]
    seq += [get(k) for k in w]
    d = p.case("setget", b"".join(seq), len(seq))
    p.case("setget_rounds", None, 3 * len(seq), rounds=[(b"".join(seq), len(seq))] * 3)
    if tag.startswith("aof"):
        # replies that wait for the AOF had their referenced values copied before they waited (ownerHold)
        d3.check("%s.setget.copied" % tag, d["copied"] > 0, "owner_ref_copied +%d" % d["copied"])
    end(p, ks + w, rng)
    p.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--ref-bin", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--owner-port", type=int, required=True)
    ap.add_argument("--cpus", default="8-11")
    ap.add_argument("--main-cpu", default="16")
    ap.add_argument("--n", type=int, default=4)  # the gate passes it; the servers here always run 4 owners
    ap.add_argument("--cachecannon", default="")
    ap.add_argument("--cc-cpus", default="0-7")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    rng = random.Random(20261115 + 11)
    suites = [("plain", lambda: suite_plain(a, rng)),
              ("aof", lambda: suite_cfg(a, rng, "aof", ["--appendonly", "yes", "--appendfsync", "everysec"], True)),
              ("aofalways", lambda: suite_cfg(a, rng, "aofalways", ["--appendonly", "yes", "--appendfsync", "always"], True)),
              ("mm", lambda: suite_cfg(a, rng, "mm", ["--maxmemory", "1gb", "--maxmemory-policy", "allkeys-lru"], False))]
    for name, fn in suites:
        if a.only and name not in a.only.split(","):
            continue
        fn()
    fails = [r["check"] for r in d3.RESULTS if not r["ok"]]
    print("BIGREPLYCHECK " + json.dumps({"checks": len(d3.RESULTS), "fails": fails}))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
