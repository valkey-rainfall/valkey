#!/usr/bin/env python3
"""RL-g7d-replica-reads: replica-read model check. A primary P (candidate, 2 owners), a candidate replica C (owners,
native reads on its owner ports) and a serial reference replica S (--ref-bin, owner-threads 0, io-threads 1: the stock
serial apply and read path), both REPLICAOF P.

Live phase (--secs): writer threads on P (each owns its keys; per-key versions increase) and reader threads on C
(owner ports routed by CLUSTER SLOTS so the reads run natively, plus main's port). Families:
  s:{g}:<i>   SET of a self-checking value (version, writer, filler of a length that depends on the version)
  a:{g}:0..3  only written by MULTI / EXEC blocks setting all four to one version (one slot)
  xa:<i>, xb:<i>  MULTI / EXEC across slots (main reads them with one MGET)
  o1:<i>, o2:<i>  SET o1 then SET o2 to one version as two commands; a reader reads o2 then o1
  c:<i>       INCR counters;  h:{g}  HSET of four fields to one version in one command;  db 1: d:<i> SETs
Reader invariants: no torn value (filler matches), per connection a key's version never goes back, the four a: keys
equal, xa = xb, o1 >= o2 read after it, the four h: fields equal, counters never go back.
Snapshots (--snaps): CLIENT PAUSE WRITE on P, wait until C and S reach P's offset, read every key of every family
(and db 1) from C, natively (owner ports by slot), and from S: replies equal; DBSIZE equal; unpause.
End: digests (DEBUG DIGEST) of P, C, S equal. Semantics on C's owner ports vs S: writes (-READONLY), GETEX, EVAL with a
write, READONLY / READWRITE, MULTI of reads, logically expired keys (P's active expiry off: GET nil, EXISTS 0, TTL -2,
DBSIZE still counts them), then replica-serve-stale-data no with P shut down (MASTERDOWN on both; C's veto 'all').
--load K: K extra threads of reads on C during the live phase. PASS / FAIL lines, RLREPLREAD summary lines.
"""
import argparse, os, random, socket, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--pport', type=int, default=22600)
ap.add_argument('--powner', type=int, default=22610)
ap.add_argument('--cport', type=int, default=22620)
ap.add_argument('--cowner', type=int, default=22630)
ap.add_argument('--sport', type=int, default=22605)
ap.add_argument('--pcpus', default='52-53')
ap.add_argument('--ccpus', default='54-57')
ap.add_argument('--scpu', default='58')
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--seed', type=int, default=7)
ap.add_argument('--secs', type=float, default=20)
ap.add_argument('--snaps', type=int, default=5)
ap.add_argument('--writers', type=int, default=3)
ap.add_argument('--readers', type=int, default=6)
ap.add_argument('--load', type=int, default=0)
ap.add_argument('--cextra', default='')
a = ap.parse_args()
FAILS = []
G, NI = 64, 64
snapconn = {}


def res(ok, name, detail=''):
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, detail), flush=True)
    if not ok:
        FAILS.append(name)


def enc(*args):
    out = [b'*%d\r\n' % len(args)]
    for x in args:
        x = x if isinstance(x, bytes) else str(x).encode()
        out.append(b'$%d\r\n%s\r\n' % (len(x), x))
    return b''.join(out)


class Conn:
    def __init__(self, port, timeout=20):
        self.s = socket.create_connection(('127.0.0.1', port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b''

    def _line(self):
        while b'\r\n' not in self.buf:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError('eof')
            self.buf += d
        i = self.buf.index(b'\r\n')
        line, self.buf = self.buf[:i], self.buf[i + 2:]
        return line

    def _n(self, n):
        while len(self.buf) < n + 2:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError('eof')
            self.buf += d
        v, self.buf = self.buf[:n], self.buf[n + 2:]
        return v

    def read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in (b'+', b'-', b':'):
            return (t + rest).decode()
        if t == b'$':
            n = int(rest)
            return None if n < 0 else self._n(n).decode(errors='replace')
        if t == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError('bad reply %r' % line[:60])

    def send(self, *args):
        self.s.sendall(enc(*args))

    def cmd(self, *args):
        self.send(*args)
        return self.read()

    def pipe(self, cmds):
        self.s.sendall(b''.join(enc(*c) for c in cmds))
        return [self.read() for _ in cmds]

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def crc16(b):
    crc = 0
    for x in b:
        crc ^= x << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def slot(key):
    k = key.encode()
    s = k.find(b'{')
    if s >= 0:
        e = k.find(b'}', s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) & 16383


def info(port, sec):
    c = Conn(port)
    txt = c.cmd('INFO', sec)
    c.close()
    return dict(l.split(':', 1) for l in txt.split('\r\n') if ':' in l and not l.startswith('#'))


def start(bin_, port, cpus, d, owners, oport, extra=''):
    os.makedirs(d, exist_ok=True)
    args = ['taskset', '-c', cpus, bin_, '--port', str(port), '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no',
            '--enable-debug-command', 'yes', '--daemonize', 'yes', '--dir', d, '--logfile', os.path.join(d, 'log')]
    if owners:
        lo = int(cpus.split('-')[0])
        args += ['--owner-threads', str(owners), '--owner-port', str(oport), '--owner-cpulist', cpus,
                 '--server-cpulist', str(lo), '--owner-busy-poll-us', '0']
    args += extra.split()
    subprocess.run(args, check=True)
    for _ in range(100):
        try:
            c = Conn(port)
            if c.cmd('PING') == '+PONG':
                c.close()
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError('server %d did not start' % port)


def stop(port):
    try:
        c = Conn(port)
        c.send('SHUTDOWN', 'NOSAVE')
        c.close()
    except OSError:
        pass
    for _ in range(100):
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            time.sleep(0.1)
        except OSError:
            return


def val(seq, w):
    f = str(seq * 7919 % 1000003)
    n = 10 + (seq * 37) % 400
    return 's:%d:%d:%s' % (seq, w, (f * (n // len(f) + 1))[:n])


def parse_val(v):
    """(version, ok) of a self-checking value; None for a missing key."""
    if v is None:
        return None, True
    try:
        _, sq, w, fill = v.split(':', 3)
        return int(sq), val(int(sq), int(w)) == v
    except ValueError:
        return -1, False


def keys_all():
    ks = []
    for g in range(G):
        ks += ['s:{%d}:%d' % (g, i) for i in range(4)] + ['a:{%d}:%d' % (g, i) for i in range(4)] + ['h:{%d}' % g]
    for i in range(NI):
        ks += ['xa:%d' % i, 'xb:%d' % i, 'o1:%d' % i, 'o2:%d' % i, 'c:%d' % i]
    return ks


def wait_offsets(ports, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        p = int(info(a.pport, 'replication')['master_repl_offset'])
        rs = [int(info(q, 'replication').get('master_repl_offset', -1)) for q in ports]
        if all(r == p for r in rs):
            return p
        time.sleep(0.05)
    return None


def main():
    os.makedirs(a.dir, exist_ok=True)
    r = random.Random(a.seed)
    start(a.bin, a.pport, a.pcpus, os.path.join(a.dir, 'p'), 2, a.powner)
    start(a.bin, a.cport, a.ccpus, os.path.join(a.dir, 'c'), a.n, a.cowner, a.cextra)
    start(a.ref_bin, a.sport, a.scpu, os.path.join(a.dir, 's'), 0, 0)
    try:
        run(r)
    finally:
        for q in (a.cport, a.sport, a.pport):
            stop(q)
        for t in ('p', 'c', 's'):
            crash = subprocess.run(['grep', '-cE', 'ASSERT|crashed|Guru|BUG REPORT', os.path.join(a.dir, t, 'log')],
                                   capture_output=True, text=True).stdout.strip()
            res(crash == '0', 'no_crash.%s' % t, crash)
    print('RLREPLREAD done fails=%d' % len(FAILS), flush=True)
    sys.exit(1 if FAILS else 0)


def run(r):
    pc = Conn(a.pport)
    pc.cmd('CONFIG', 'SET', 'repl-backlog-size', '64mb')
    for q in (a.cport, a.sport):
        Conn(q).cmd('REPLICAOF', '127.0.0.1', str(a.pport))
    off = wait_offsets([a.cport, a.sport])
    ci = info(a.cport, 'owners')
    res(off is not None and ci.get('owner_veto') == 'writes', 'setup.replica_native_reads', 'veto %s' % ci.get('owner_veto'))
    # owner port of each slot on C (CLUSTER SLOTS)
    cs = Conn(a.cport).cmd('CLUSTER', 'SLOTS')
    smap = []
    for ent in cs:
        smap.append((int(ent[0][1:]), int(ent[1][1:]), int(ent[2][1][1:])))

    def port_of(key):
        s = slot(key)
        for lo, hi, p in smap:
            if lo <= s <= hi:
                return p
        return a.cport
    stop_ev = threading.Event()
    paused = threading.Event()
    lock = threading.Lock()
    stats = {'writes': 0, 'reads': 0, 'viol': [], 'errors': []}

    def bad(msg):
        with lock:
            if len(stats['viol']) < 20:
                stats['viol'].append(msg)

    def writer(w):
        rw = random.Random(a.seed * 31 + w)
        c = Conn(a.pport, timeout=120)
        seq = 0
        mine = [g for g in range(G) if g % a.writers == w]
        mi = [i for i in range(NI) if i % a.writers == w]
        while not stop_ev.is_set():
            batch = []
            for _ in range(20):
                seq += 1
                x = rw.random()
                g = rw.choice(mine)
                i = rw.choice(mi)
                if x < 0.35:
                    k = 's:{%d}:%d' % (g, rw.randrange(4))
                    batch.append(('SET', k, val(seq, w)))
                elif x < 0.5:
                    batch += [('MULTI',)] + [('SET', 'a:{%d}:%d' % (g, j), val(seq, w)) for j in range(4)] + [('EXEC',)]
                elif x < 0.6:
                    batch += [('MULTI',), ('SET', 'xa:%d' % i, val(seq, w)), ('SET', 'xb:%d' % i, val(seq, w)), ('EXEC',)]
                elif x < 0.72:
                    batch += [('SET', 'o1:%d' % i, val(seq, w)), ('SET', 'o2:%d' % i, val(seq, w))]
                elif x < 0.82:
                    batch.append(('INCR', 'c:%d' % i))
                elif x < 0.92:
                    batch.append(('HSET', 'h:{%d}' % g) + tuple(x for j in range(4) for x in ('f%d' % j, val(seq, w))))
                elif x < 0.96:
                    batch += [('SELECT', '1'), ('SET', 'd:%d' % rw.randrange(200), val(seq, w)), ('SELECT', '0')]
                else:
                    k = 's:{%d}:%d' % (g, rw.randrange(4))
                    batch.append(('EXPIRE', k, '3600') if rw.random() < 0.7 else ('PERSIST', k))
            for v in c.pipe(batch):
                if isinstance(v, str) and v.startswith('-'):
                    with lock:
                        stats['errors'].append(v[:80])
            with lock:
                stats['writes'] += len(batch)
        c.close()

    def reader(k):
        rr = random.Random(a.seed * 97 + k)
        conns = {}

        def cn(port):
            if port not in conns:
                conns[port] = Conn(port, timeout=120)
            return conns[port]
        last = {}

        def seen(key, sq):
            if sq is None:
                return
            if last.get(key, -1) > sq:
                bad('reader %d: %s went back %d -> %d' % (k, key, last[key], sq))
            last[key] = sq
        while not stop_ev.is_set():
            x = rr.random()
            g, i = rr.randrange(G), rr.randrange(NI)
            mainport = rr.random() < 0.2
            if x < 0.3:
                key = 's:{%d}:%d' % (g, rr.randrange(4))
                v = cn(a.cport if mainport else port_of(key)).cmd('GET', key)
                sq, ok = parse_val(v)
                if not ok:
                    bad('torn %s %r' % (key, str(v)[:60]))
                seen(key, sq)
            elif x < 0.5:
                ks = ['a:{%d}:%d' % (g, j) for j in range(4)]
                vs = cn(a.cport if mainport else port_of(ks[0])).cmd('MGET', *ks)
                sqs = [parse_val(v)[0] for v in vs]
                if len(set(sqs)) != 1:
                    bad('MULTI not atomic a:{%d} %s' % (g, sqs))
                seen('a:{%d}' % g, sqs[0])
            elif x < 0.6:
                vs = cn(a.cport).cmd('MGET', 'xa:%d' % i, 'xb:%d' % i)
                sqs = [parse_val(v)[0] for v in vs]
                if sqs[0] != sqs[1]:
                    bad('cross-slot MULTI not atomic xa/xb:%d %s' % (i, sqs))
            elif x < 0.75:
                p2 = a.cport if mainport else port_of('o2:%d' % i)
                v2 = parse_val(cn(p2).cmd('GET', 'o2:%d' % i))[0]
                v1 = parse_val(cn(a.cport if mainport else port_of('o1:%d' % i)).cmd('GET', 'o1:%d' % i))[0]
                if v2 is not None and (v1 is None or v1 < v2):
                    bad('not a prefix: o2:%d=%s read before o1:%d=%s' % (i, v2, i, v1))
            elif x < 0.85:
                key = 'c:%d' % i
                v = cn(a.cport if mainport else port_of(key)).cmd('GET', key)
                seen(key, int(v) if v is not None else None)
            else:
                key = 'h:{%d}' % g
                vs = cn(a.cport if mainport else port_of(key)).cmd('HMGET', key, 'f0', 'f1', 'f2', 'f3')
                sqs = [parse_val(v)[0] for v in vs]
                if len(set(sqs)) != 1:
                    bad('HSET torn %s %s' % (key, sqs))
                seen(key, sqs[0])
            with lock:
                stats['reads'] += 1
        for c in conns.values():
            c.close()
    ths = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(a.writers)]
    ths += [threading.Thread(target=reader, args=(k,), daemon=True) for k in range(a.readers + a.load)]
    for t in ths:
        t.start()
    snaps_ok, snap_detail = 0, []
    t_end = time.time() + a.secs
    for sn in range(a.snaps):
        time.sleep(max(0.2, (t_end - time.time()) / (a.snaps - sn + 1)))
        adm = Conn(a.pport)
        adm.cmd('CLIENT', 'PAUSE', '30000', 'WRITE')
        off = wait_offsets([a.cport, a.sport])
        ok = off is not None
        diffs = []
        if ok:
            ks = keys_all()
            sc = Conn(a.sport)
            for db in ('0', '1'):
                kk = ks if db == '0' else ['d:%d' % j for j in range(200)]
                if db == '1':
                    sc.cmd('SELECT', '1')
                for key in kk:
                    p = port_of(key) if db == '0' else a.cport
                    q = sc.cmd('TYPE', key)
                    getter = ('HGETALL', key) if q == '+hash' else ('GET', key)
                    if db == '1':
                        if 'db1' not in snapconn:
                            snapconn['db1'] = Conn(a.cport)
                            snapconn['db1'].cmd('SELECT', '1')
                        vc, tc, pt = snapconn['db1'].pipe([getter, ('TYPE', key), ('PTTL', key)])
                    else:
                        conn = snapconn.setdefault(p, Conn(p))
                        vc, tc, pt = conn.pipe([getter, ('TYPE', key), ('PTTL', key)])
                    vs, ps = sc.cmd(*getter), sc.cmd('PTTL', key)
                    pos = lambda t: 'ttl' if isinstance(t, str) and t.startswith(':') and int(t[1:]) > 0 else t
                    if vc != vs or tc != q or pos(pt) != pos(ps):
                        diffs.append((db, key, str(vc)[:40], str(vs)[:40], tc, q))
                if db == '1':
                    sc.cmd('SELECT', '0')
            dc, ds = Conn(a.cport).cmd('DBSIZE'), sc.cmd('DBSIZE')
            if dc != ds:
                diffs.append(('dbsize', dc, ds))
            sc.close()
        adm.cmd('CLIENT', 'UNPAUSE')
        adm.close()
        if ok and not diffs:
            snaps_ok += 1
        snap_detail.append('off=%s diffs=%d%s' % (off, len(diffs), (' first %s' % (diffs[0],)) if diffs else ''))
    while time.time() < t_end:
        time.sleep(0.1)
    stop_ev.set()
    for t in ths:
        t.join(60)
    co = info(a.cport, 'owners')
    print('RLREPLREAD live: writes %d reads %d write errors %d; C owners: %s' % (
        stats['writes'], stats['reads'], len(stats['errors']),
        ' '.join('%s=%s' % (k, v) for k, v in co.items() if k in (
            'owner_veto', 'owner_native_commands', 'owner_handoffs_to_main', 'owner_world_stops', 'owner_replica_paced',
            'owner_replica_pace_fires'))), flush=True)
    res(not stats['viol'], 'live.reader_invariants', '; '.join(stats['viol'][:5]))
    res(not stats['errors'], 'live.writes_ok', str(stats['errors'][:3]))
    res(int(co.get('owner_native_commands', 0)) > stats['reads'] // 3, 'live.reads_native',
        'native %s of %d reads' % (co.get('owner_native_commands'), stats['reads']))
    res(snaps_ok == a.snaps, 'snapshots.equal_to_serial_replica', '%d / %d; %s' % (snaps_ok, a.snaps, ' | '.join(snap_detail)))
    off = wait_offsets([a.cport, a.sport])
    dg = [Conn(q).cmd('DEBUG', 'DIGEST') for q in (a.pport, a.cport, a.sport)]
    res(off is not None and dg[0] == dg[1] == dg[2], 'end.digest_equal', str(dg))
    semantics(port_of)


def semantics(port_of):
    s = Conn(a.sport)
    pairs = []
    key = 's:{1}:0'
    c = Conn(port_of(key))
    for cmd in (('SET', key, 'x'), ('GETEX', key, 'PX', '100'), ('INCR', 'c:1'), ('DEL', key),
                ('EVAL', "return redis.call('set', KEYS[1], 'y')", '1', key), ('READONLY',), ('READWRITE',),
                ('EVAL_RO', "return redis.call('get', KEYS[1]) ~= false", '1', key)):
        pairs.append((cmd[0], c.cmd(*cmd), s.cmd(*cmd)))
    c.send('MULTI'); c.send('GET', key); c.send('EXISTS', key); c.send('EXEC')
    s.send('MULTI'); s.send('GET', key); s.send('EXISTS', key); s.send('EXEC')
    pairs.append(('MULTI-reads', [c.read() for _ in range(4)], [s.read() for _ in range(4)]))
    # logically expired keys: the primary's active expiry off, keys with 150 ms TTLs
    p = Conn(a.pport)
    p.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    for j in range(20):
        p.cmd('SET', 'e:{%d}' % j, 'v', 'PX', '150')
    wait_offsets([a.cport, a.sport])
    time.sleep(0.4)
    for j in range(20):
        k = 'e:{%d}' % j
        cc = Conn(port_of(k))
        for cmd in (('GET', k), ('EXISTS', k), ('TTL', k), ('TYPE', k)):
            pairs.append(('expired-' + cmd[0], cc.cmd(*cmd), s.cmd(*cmd)))
        cc.close()
    pairs.append(('DBSIZE-with-expired', Conn(a.cport).cmd('DBSIZE'), s.cmd('DBSIZE')))
    p.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
    diff = [x for x in pairs if x[1] != x[2]]
    res(not diff, 'semantics.owner_port_equals_serial_replica', '%d pairs; %s' % (len(pairs), str(diff[:3])[:400]))
    # stale data off, primary gone: MASTERDOWN on both, C vetoed
    for q in (a.cport, a.sport):
        Conn(q).cmd('CONFIG', 'SET', 'replica-serve-stale-data', 'no')
    stop(a.pport)
    t0 = time.time()
    while time.time() - t0 < 10:
        if all(info(q, 'replication').get('master_link_status') == 'down' for q in (a.cport, a.sport)):
            break
        time.sleep(0.1)
    time.sleep(0.3)
    cc = Conn(port_of(key))
    rc, rs_ = cc.cmd('GET', key), s.cmd('GET', key)
    veto = info(a.cport, 'owners').get('owner_veto')
    res(rc == rs_ and str(rc).startswith('-MASTERDOWN') and veto == 'all', 'semantics.stale_masterdown',
        'cand %r ref %r veto %s' % (str(rc)[:50], str(rs_)[:50], veto))
    for q in (a.cport, a.sport):
        Conn(q).cmd('CONFIG', 'SET', 'replica-serve-stale-data', 'yes')
    time.sleep(0.3)
    rc, rs_ = cc.cmd('GET', 's:{2}:1'), s.cmd('GET', 's:{2}:1')
    veto = info(a.cport, 'owners').get('owner_veto')
    res(rc == rs_ and veto == 'writes', 'semantics.stale_served_again', 'equal %s veto %s' % (rc == rs_, veto))
    # promotion: C becomes a primary with the data it applied (= S's), its owners take writes natively
    dc, ds = Conn(a.cport).cmd('DEBUG', 'DIGEST'), s.cmd('DEBUG', 'DIGEST')
    Conn(a.cport).cmd('REPLICAOF', 'NO', 'ONE')
    time.sleep(0.3)
    w = cc.cmd('SET', key, 'after-promotion')
    g = Conn(port_of(key)).cmd('GET', key)
    o = info(a.cport, 'owners')
    res(dc == ds and w == '+OK' and g == 'after-promotion' and o.get('owner_veto') == 'none',
        'semantics.promoted', 'digest equal %s set %s get %s veto %s' % (dc == ds, w, g, o.get('owner_veto')))


if __name__ == '__main__':
    main()
