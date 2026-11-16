#!/usr/bin/env python3
"""RL-g2b-owner-expire: expiry model check, the candidate (--bin, owners expire their own keys) against the parent
(--ref-bin, main expires every key), same scenario, one after the other on the same CPUs and ports.

Per mode (plain, aof everysec, aof always, repl, mm) and design, a fresh server with W owners:
  fixed keys  F: f:1..f:5000 (no TTL), L: l:1..l:3000 (EX 3600), P: p:1..p:1000 (PX 500 then PERSIST via main),
              E: e:1..e:1000 (SET then PEXPIRE 800 via main)
  load        memtier GET-only on f:1..f:5000 through the owner ports (keyspace unchanged), the whole run
  churn       4 writer threads, SET c:<w>:<n> v PX <50..1500> pipelined to the owner of each key, 6 s; every key once
  probes      DBSIZE every 250 ms (main): safety dbsize >= fixed + keys surely alive (created before the probe, deadline
              after it); stale = dbsize - fixed - keys alive by the model (the active cycle's backlog)
Checks (both designs must pass; the candidate is compared with the parent):
  safety      never fewer keys than surely alive; at the end f:/l:/p: all there with value and TTL (l: PEXPIRETIME as set)
  accounting  expired_keys == (churn + E) - (dbsize - F - L - P)    (no other delete in the scenario; mm: + evicted_keys)
  effort      mean stale share over probes (stale / churn keys created) within 5 points of the parent's, final
              stale share (2.5 s after the last deadline) within 2 points of the parent's, or both under 1%
  native      candidate: owner_veto none, owner_active_expire yes, owner_expired_keys > 0, main expired almost nothing
  aof         (aof, always) incr file (resp): DEL/UNLINK records == expired_keys, each for a volatile key, at most once,
              after its key's SET; reload into a fresh server: no key comes back, l:/f:/p: keys equal
  repl        replica (candidate binary): expired_keys 0 on the replica; primary's active expiry off + 1000 keys PX 200:
              replica dbsize unchanged after their deadline; on again: replica follows; DEBUG DIGEST and dbsize equal
  mm          maxmemory 40mb allkeys-lru, 1 KB churn values: used_memory <= maxmemory + 4 MB at every probe; accounting
              with evicted_keys
Prints PASS/FAIL lines and RLEXPIRE summary lines.
"""
import argparse, json, os, random, socket, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--cli', required=True)
ap.add_argument('--memtier', default='/home/ec2-user/ns/memtier_benchmark')
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, default=20600)
ap.add_argument('--owner-port', type=int, default=20610)
ap.add_argument('--replica-port', type=int, default=20601)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--cpus', default='12-15')
ap.add_argument('--main-cpu', default='16')
ap.add_argument('--bg-cpu', default='17')
ap.add_argument('--replica-cpus', default='18')
ap.add_argument('--load-cpus', default='18-19')
ap.add_argument('--modes', default='plain,aof,always,repl,mm')
ap.add_argument('--secs', type=float, default=6)
ap.add_argument('--seed', type=int, default=11)
a = ap.parse_args()
FAILS = []


def out(ok, name, detail):
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, detail), flush=True)
    if not ok:
        FAILS.append(name)


# ---------------------------------------------------------------- RESP
class R:
    def __init__(self, port):
        self.s = socket.create_connection(('127.0.0.1', port), timeout=30)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile('rb')

    def send(self, cmds):
        b = bytearray()
        for c in cmds:
            b += b'*%d\r\n' % len(c)
            for x in c:
                x = x if isinstance(x, bytes) else str(x).encode()
                b += b'$%d\r\n%s\r\n' % (len(x), x)
        self.s.sendall(b)

    def read(self):
        l = self.f.readline()
        t, r = l[:1], l[1:-2]
        if t == b'+':
            return r.decode()
        if t == b'-':
            return Exception(r.decode())
        if t == b':':
            return int(r)
        if t == b'$':
            n = int(r)
            if n < 0:
                return None
            d = self.f.read(n + 2)[:-2]
            return d
        if t == b'*':
            n = int(r)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b'=':  # verbatim
            n = int(r)
            return self.f.read(n + 2)[:-2]
        raise RuntimeError('bad reply %r' % l)

    def do(self, *c):
        self.send([c])
        return self.read()

    def pipe(self, cmds):
        self.send(cmds)
        return [self.read() for _ in cmds]


def crc16(b):
    crc = 0
    for x in b:
        crc ^= x << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
    return crc


def slot(k):
    k = k if isinstance(k, bytes) else k.encode()
    s = k.find(b'{')
    if s >= 0:
        e = k.find(b'}', s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) & 16383


def info(r, sec):
    d = {}
    v = r.do('INFO', sec)
    for line in (v.decode() if isinstance(v, bytes) else str(v)).split('\r\n'):
        if ':' in line and not line.startswith('#'):
            k, x = line.split(':', 1)
            d[k] = x
    return d


# ---------------------------------------------------------------- servers
def start(binp, port, dirn, extra, cpus, main_cpu, oport=None):
    oport = oport or a.owner_port
    os.makedirs(dirn, exist_ok=True)
    args = ['taskset', '-c', '%s,%s,%s' % (cpus, main_cpu, a.bg_cpu), binp, '--port', str(port), '--bind', '127.0.0.1',
            '--save', '', '--dir', dirn, '--logfile', os.path.join(dirn, 'log'), '--daemonize', 'yes',
            '--enable-debug-command', 'yes', '--io-threads', '1', '--bio-cpulist', a.bg_cpu]
    args += ['--owner-threads', str(a.n), '--owner-port', str(oport), '--owner-cpulist', cpus, '--server-cpulist', main_cpu]
    args += extra
    subprocess.run(args, check=True)
    for _ in range(200):
        try:
            r = R(port)
            if r.do('PING') == 'PONG':
                return r
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError('server on %d did not start' % port)


def stop(port):
    try:
        R(port).send([['SHUTDOWN', 'NOSAVE']])
    except OSError:
        pass
    for _ in range(200):
        if subprocess.run(['ss', '-Htln', 'sport = :%d' % port], capture_output=True, text=True).stdout.strip() == '':
            return
        time.sleep(0.05)


def owner_ports(r):
    sl = r.do('CLUSTER', 'SLOTS')
    m = []
    for e in sl:
        m.append((e[0], e[1], e[2][1]))
    return m


def port_of(m, s):
    for lo, hi, p in m:
        if lo <= s <= hi:
            return p
    raise RuntimeError('no owner for slot %d' % s)


# ---------------------------------------------------------------- one run
def run(design, binp, mode):
    tag = '%s.%s' % (mode, design)
    d = os.path.join(a.dir, tag)
    subprocess.run(['rm', '-rf', d])
    extra = []
    if mode in ('aof', 'always'):
        extra = ['--appendonly', 'yes', '--appendfsync', 'everysec' if mode == 'aof' else 'always',
                 '--aof-incr-format', 'resp', '--aof-use-rdb-preamble', 'no']
    if mode == 'mm':
        extra = ['--maxmemory', '40mb', '--maxmemory-policy', 'allkeys-lru']
    m = start(binp, a.port, d, extra, a.cpus, a.main_cpu)
    omap = owner_ports(m)
    oc = {}
    for _, _, p in omap:
        oc[p] = R(p)
    rep = None
    if mode == 'repl':
        rep = start(a.bin, a.replica_port, d + '-replica', [], a.replica_cpus, a.replica_cpus, a.owner_port + 20)
        rep.do('REPLICAOF', '127.0.0.1', a.port)
        for _ in range(200):
            if info(rep, 'replication').get('master_link_status') == 'up':
                break
            time.sleep(0.05)
    val = lambda k: ('v-' + k).encode()
    big = b'x' * 1000

    def by_owner(keys):
        g = {}
        for k in keys:
            g.setdefault(port_of(omap, slot(k)), []).append(k)
        return g

    F, L, P, E = 5000, 3000, 1000, 1000
    for p, ks in by_owner(['f:%d' % i for i in range(1, F + 1)]).items():
        oc[p].pipe([['SET', k, val(k)] for k in ks])
    lexp = {}
    for p, ks in by_owner(['l:%d' % i for i in range(1, L + 1)]).items():
        oc[p].pipe([['SET', k, val(k), 'EX', 3600] for k in ks])
    m.pipe([['SET', 'p:%d' % i, 'pv', 'PX', 500] for i in range(1, P + 1)])
    m.pipe([['PERSIST', 'p:%d' % i] for i in range(1, P + 1)])
    m.pipe([['SET', 'e:%d' % i, 'ev'] for i in range(1, E + 1)])
    t_e_lo = time.time() + 0.8
    m.pipe([['PEXPIRE', 'e:%d' % i, 800] for i in range(1, E + 1)])
    t_e = time.time() + 0.8
    for i, x in enumerate(m.pipe([['PEXPIRETIME', 'l:%d' % i] for i in range(1, L + 1)])):
        lexp['l:%d' % (i + 1)] = x
    FIXED = F + L + P
    # read load on f:* through the owner ports (keyspace unchanged)
    mt = subprocess.Popen(['taskset', '-c', a.load_cpus, a.memtier, '-s', '127.0.0.1', '-p', str(a.owner_port),
                           '--cluster-mode', '--threads', '2', '--clients', '4', '--ratio', '0:1', '--key-prefix', 'f:',
                           '--key-minimum', '1', '--key-maximum', str(F), '--key-pattern', 'R:R', '--test-time',
                           str(int(a.secs + 6)), '--hide-histogram', '--distinct-client-seed'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    created = []  # (name, t_reply, deadline_lo, deadline_hi)
    lock = threading.Lock()
    stop_at = time.time() + a.secs

    def writer(w):
        rnd = random.Random(a.seed * 100 + w)
        conns = {p: R(p) for _, _, p in omap}
        n = 0
        while time.time() < stop_at:
            batch = {}
            for _ in range(200):
                k = 'c:%d:%d' % (w, n)
                n += 1
                ttl = rnd.randint(50, 1500)
                batch.setdefault(port_of(omap, slot(k)), []).append((k, ttl))
            for p, ks in batch.items():
                t0 = time.time()
                rs = conns[p].pipe([['SET', k, big if mode == 'mm' else b'cv', 'PX', ttl] for k, ttl in ks])
                t1 = time.time()
                assert all(x == 'OK' for x in rs), rs[:3]
                with lock:
                    for k, ttl in ks:
                        created.append((k, t1, t0 + ttl / 1000.0, t1 + ttl / 1000.0))
    probes = []

    def probe_once():
        t0 = time.time()
        n = m.do('DBSIZE')
        t1 = time.time()
        um = int(info(m, 'memory').get('used_memory', 0)) if mode == 'mm' else 0
        probes.append({'t': t0, 't1': t1, 'dbsize': n, 'um': um})

    ths = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in ths:
        t.start()
    while any(t.is_alive() for t in ths):
        probe_once()
        time.sleep(0.25)
    for t in ths:
        t.join()
    last = max(hi for _, _, _, hi in created)
    while time.time() < last + 2.5:
        probe_once()
        time.sleep(0.25)
    mt.wait()
    probe_once()
    # the model at each probe: keys surely alive (created before it, deadline surely after it) and alive (by the
    # latest deadline bound); the E keys by their bounds
    for p in probes:
        t0, t1 = p['t'], p['t1']
        sure = alive = cr = 0
        for k, tr, lo, hi in created:
            if tr < t0:
                cr += 1
                if lo > t1 + 0.002:
                    sure += 1
                if hi > t0:
                    alive += 1
        p['sure'] = sure + (E if t_e_lo > t1 + 0.002 else 0)
        p['alive'] = alive + (E if t_e > t0 else 0)
        p['created'] = cr
    res = {'design': design, 'mode': mode, 'created': len(created)}
    ev = int(info(m, 'stats').get('evicted_keys', 0))
    # safety: never fewer keys than surely alive (plus the fixed ones; mm: eviction may remove any key)
    if mode != 'mm':
        bad = [p for p in probes if p['dbsize'] < FIXED + p['sure']]
        out(not bad, '%s.safety_probes' % tag, '%d probes, %d below surely alive%s' % (len(probes), len(bad),
            (' first %s' % bad[0]) if bad else ''))
    st = [(p['dbsize'] - FIXED - p['alive']) / max(1, p['created']) for p in probes[4:-1] if p['created']]
    res['stale_mean'] = round(sum(st) / max(1, len(st)), 4)
    fin = probes[-1]
    res['stale_final'] = round((fin['dbsize'] - FIXED - fin['alive']) / max(1, len(created)), 4)
    # fixed keys
    r = m.pipe([['GET', 'f:%d' % i] for i in range(1, F + 1)])
    okf = all(x == val('f:%d' % (i + 1)) for i, x in enumerate(r))
    r = m.pipe([['PEXPIRETIME', 'l:%d' % i] for i in range(1, L + 1)])
    okl = all(x == lexp['l:%d' % (i + 1)] for i, x in enumerate(r))
    r = m.pipe([['TTL', 'p:%d' % i] for i in range(1, P + 1)])
    okp = all(x == -1 for x in r)
    if mode != 'mm':
        out(okf and okl and okp, '%s.fixed_keys' % tag, 'f %s l(PEXPIRETIME) %s p(persist) %s' % (okf, okl, okp))
    st_ = info(m, 'stats')
    ow = info(m, 'owners')
    exk = int(st_['expired_keys'])
    dbs = m.do('DBSIZE')
    res.update({'expired_keys': exk, 'evicted_keys': ev, 'dbsize': dbs, 'owner_expired_keys': int(ow.get('owner_expired_keys', 0)),
                'owner_active_expire': ow.get('owner_active_expire'), 'owner_veto': ow.get('owner_veto'),
                'expire_cycle_cpu_ms': int(st_.get('expire_cycle_cpu_milliseconds', 0))})
    want = len(created) + E + FIXED - dbs
    out(exk + ev == want, '%s.accounting' % tag, 'expired_keys %d + evicted %d == created %d + E %d - (dbsize %d - fixed %d) = %d'
        % (exk, ev, len(created), E, dbs, FIXED, want))
    if mode == 'mm':
        mx = max(p['um'] for p in probes)
        res['used_memory_max'] = mx
        out(mx <= 40 * 1024 * 1024 + 4 * 1024 * 1024, '%s.maxmemory' % tag, 'used_memory max %d, evicted %d' % (mx, ev))
    if design == 'cand':
        nat = ow.get('owner_veto') == 'none' and ow.get('owner_active_expire') == 'yes' and res['owner_expired_keys'] > 0
        out(nat, '%s.native' % tag, 'veto %s active_expire %s owner_expired_keys %d of expired_keys %d' %
            (ow.get('owner_veto'), ow.get('owner_active_expire'), res['owner_expired_keys'], exk))
    if mode in ('aof', 'always'):
        m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        time.sleep(1.5)  # owners stop at main's next resume; everysec fsync
        exk = int(info(m, 'stats')['expired_keys'])
        before = {}
        cur = b'0'
        while True:
            cur, ks = m.do('SCAN', cur, 'COUNT', 1000)
            for k in ks:
                before[k] = None
            if cur == b'0':
                break
        ks = list(before)
        for k, x in zip(ks, m.pipe([['PEXPIRETIME', k] for k in ks])):
            before[k] = x
        exk = int(info(m, 'stats')['expired_keys'])  # SCAN may have deleted some lazily (propagated too)
        time.sleep(1.2)
        recs = parse_aof(d)
        seen = {}
        dels, badk, dup, order = 0, 0, 0, 0
        for i, c in enumerate(recs):
            nm = c[0].upper()
            if nm in (b'SET',):
                seen.setdefault(c[1], i)
            elif nm in (b'DEL', b'UNLINK'):
                dels += 1
                k = c[1]
                if not (k.startswith(b'c:') or k.startswith(b'e:')):
                    badk += 1
                if seen.get(k, -2) == -1:
                    dup += 1
                if k not in seen:
                    order += 1
                seen[k] = -1
        out(dels == exk and not badk and not dup and not order, '%s.aof_records' % tag,
            'DEL/UNLINK records %d == expired_keys %d; non-volatile %d, twice %d, before its SET %d' % (dels, exk, badk, dup, order))
        res['aof_dels'] = dels
        stop(a.port)
        m = start(binp, a.port, d, extra, a.cpus, a.main_cpu)
        for _ in range(200):
            if info(m, 'persistence').get('loading') == '0':
                break
            time.sleep(0.05)
        after = {}
        cur = b'0'
        while True:
            cur, ks = m.do('SCAN', cur, 'COUNT', 1000)
            for k in ks:
                after[k] = None
            if cur == b'0':
                break
        ks = list(after)
        for k, x in zip(ks, m.pipe([['PEXPIRETIME', k] for k in ks])):
            after[k] = x
        back = [k for k in after if k not in before]
        diff = [k for k in after if k in before and after[k] != before[k]]
        stable = [k for k in before if not (k.startswith(b'c:') or k.startswith(b'e:'))]
        lost = [k for k in stable if k not in after]
        out(not back and not diff and not lost, '%s.aof_reload' % tag, 'keys %d before, %d after; came back %d, TTL changed %d, '
            'fixed lost %d' % (len(before), len(after), len(back), len(diff), len(lost)))
    if mode == 'repl':
        rexp = int(info(rep, 'stats')['expired_keys'])
        out(rexp == 0 and exk > 0, '%s.replica_no_expiry' % tag, 'replica expired_keys %d, primary %d' % (rexp, exk))
        m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        time.sleep(0.3)
        oc2 = {p: R(p) for _, _, p in omap}
        for p, ks in by_owner(['z:%d' % i for i in range(1000)]).items():
            oc2[p].pipe([['SET', k, 'zv', 'PX', 200] for k in ks])
        time.sleep(0.2)
        wait_offsets(m, rep)
        n0 = rep.do('DBSIZE')
        time.sleep(1.0)
        n1 = rep.do('DBSIZE')
        rexp = int(info(rep, 'stats')['expired_keys'])
        gz = rep.do('GET', 'z:1')
        out(n1 == n0 and rexp == 0 and gz is None, '%s.replica_holds' % tag, 'primary expiry off: replica dbsize %d -> %d '
            'after the deadlines, expired_keys %d, GET z:1 -> %r' % (n0, n1, rexp, gz))
        m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
        for _ in range(100):
            if rep.do('DBSIZE') <= n1 - 900:
                break
            time.sleep(0.05)
        n2 = rep.do('DBSIZE')
        m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
        time.sleep(0.3)
        wait_offsets(m, rep)
        dm, dr = m.do('DEBUG', 'DIGEST'), rep.do('DEBUG', 'DIGEST')
        sm, sr = m.do('DBSIZE'), rep.do('DBSIZE')
        out(n2 <= n1 - 900 and dm == dr and sm == sr, '%s.replica_follows' % tag, 'primary expiry on: replica %d -> %d; '
            'digest equal %s, dbsize %d / %d' % (n1, n2, dm == dr, sm, sr))
        stop(a.replica_port)
    stop(a.port)
    print('RLEXPIRE ' + json.dumps(res), flush=True)
    return res


def wait_offsets(m, rep):
    for _ in range(200):
        a1 = int(info(m, 'replication')['master_repl_offset'])
        b1 = int(info(rep, 'replication').get('master_repl_offset', -1))
        if a1 == b1:
            return
        time.sleep(0.05)


def parse_aof(d):
    ad = os.path.join(d, 'appendonlydir')
    recs = []
    for f in sorted(os.listdir(ad)):
        if not f.endswith('.incr.aof'):
            continue
        b = open(os.path.join(ad, f), 'rb').read()
        i = 0
        while i < len(b):
            if b[i:i + 1] == b'#':  # annotation
                i = b.index(b'\n', i) + 1
                continue
            assert b[i:i + 1] == b'*', (f, i, b[i:i + 20])
            j = b.index(b'\r\n', i)
            n = int(b[i + 1:j])
            i = j + 2
            c = []
            for _ in range(n):
                j = b.index(b'\r\n', i)
                ln = int(b[i + 1:j])
                c.append(b[j + 2:j + 2 + ln])
                i = j + 2 + ln + 2
            recs.append(c)
    return recs


summary = {}
for mode in a.modes.split(','):
    rc = run('cand', a.bin, mode)
    rp = run('parent', a.ref_bin, mode)
    ok = (rc['stale_mean'] <= rp['stale_mean'] + 0.05 and
          (rc['stale_final'] <= rp['stale_final'] + 0.02 or (rc['stale_final'] < 0.01 and rp['stale_final'] < 0.01)))
    out(ok, '%s.effort_vs_parent' % mode, 'stale share mean %.4f vs parent %.4f, final %.4f vs %.4f; expire_cycle_cpu_ms %d vs %d'
        % (rc['stale_mean'], rp['stale_mean'], rc['stale_final'], rp['stale_final'], rc['expire_cycle_cpu_ms'], rp['expire_cycle_cpu_ms']))
    summary[mode] = {'cand': rc, 'parent': rp}
print('RLEXPIRE_DONE fails=%d %s' % (len(FAILS), ' '.join(FAILS)), flush=True)
sys.exit(1 if FAILS else 0)
