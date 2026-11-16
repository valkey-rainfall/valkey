#!/usr/bin/env python3
"""RL-g1d-owner-tls model check: TLS clients on the owner TLS ports of the candidate vs the same per-connection
command sequences on the parent's main TLS port (the main path). Every reply and the final keyspace must be equal.

Each connection has its own keys (most in its owner's slots: native; the rest on other owners or multi-slot: the
main path), so its replies do not depend on how connections interleave. Commands: a typical mix (strings, counters,
MGET/MSET on one hash tag, hashes, lists, sets, zsets, EXPIRE/TTL/PERSIST/EXISTS/TYPE, MULTI/EXEC, EVAL, values of
20-60 KB that span TLS records), sent in pipelines of 1-24. Plain connections on the owner ports run the same way
next to them. Shared counters: every connection INCRs them; the totals must match.
Modes: plain | aof (appendonly yes, everysec; reload the AOF into a fresh server and compare) | repl (a replica of
the candidate; compare its keyspace). Optional parts: --hsdrop (clients drop mid-handshake all through the run),
--tlsconfig (CONFIG SET of the TLS files mid-run: new owner-TLS connections get the new cert, old ones go on).
Prints PASS/FAIL lines; exit 1 on any FAIL."""
import argparse, os, random, socket, ssl, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--port', type=int, default=23379)       # candidate: port, +1 tls-port, owners +21.., tls owners +121..
ap.add_argument('--ref-port', type=int, default=23679)   # parent: port, +1 tls-port, owners +21..
ap.add_argument('--cpus', default='0-3')
ap.add_argument('--main-cpu', default='4')
ap.add_argument('--ref-cpus', default='8-11')
ap.add_argument('--ref-main-cpu', default='12')
ap.add_argument('--tls-dir', default='/home/ec2-user/ns-tls')
ap.add_argument('--tls-dir2', default='')                # second cert set (same layout) for --tlsconfig
ap.add_argument('--mode', default='plain', choices=['plain', 'aof', 'repl'])
ap.add_argument('--conns', type=int, default=16)         # TLS connections per owner
ap.add_argument('--plain-conns', type=int, default=4)    # plain owner-port connections per owner
ap.add_argument('--ops', type=int, default=1500)         # commands per connection
ap.add_argument('--seed', type=int, default=1)
ap.add_argument('--hsdrop', action='store_true')
ap.add_argument('--hsdrop-secs', type=float, default=0)
ap.add_argument('--tlsconfig', action='store_true')
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
FAILS = []


def res(ok, name, detail=''):
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, detail), flush=True)
    if not ok:
        FAILS.append(name)


# ---- slots (CRC16 XMODEM, hash tags) ----
def crc16(b):
    crc = 0
    for x in b:
        crc ^= x << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
    return crc


def slot(key):
    k = key.encode()
    s = k.find(b'{')
    if s >= 0:
        e = k.find(b'}', s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) & 0x3fff


def owner_of(sl, n):
    tables = 1 << 10
    per = 16384 // tables
    t = sl // per
    for i in range(n):
        if tables * i // n <= t < tables * (i + 1) // n:
            return i


# ---- RESP ----
def enc(*args):
    out = [b'*%d\r\n' % len(args)]
    for x in args:
        x = x if isinstance(x, bytes) else str(x).encode()
        out.append(b'$%d\r\n%s\r\n' % (len(x), x))
    return b''.join(out)


class Conn:
    def __init__(self, port, tls, ca=None, hold=False):
        s = socket.create_connection(('127.0.0.1', port), timeout=60)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.load_verify_locations(ca or os.path.join(a.tls_dir, 'ca.crt'))
            s = ctx.wrap_socket(s, server_hostname='127.0.0.1')
        self.s = s
        self.f = s.makefile('rb')

    def send(self, data):
        self.s.sendall(data)

    def read(self):
        line = self.f.readline()
        if not line:
            raise EOFError('closed')
        t, rest = line[:1], line[1:-2]
        if t == b'+':
            return rest.decode()
        if t == b'-':
            return ('ERR', rest.decode())
        if t == b':':
            return int(rest)
        if t == b'$':
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            return d[:-2]
        if t == b'*':
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        if t == b'%':
            n = int(rest)
            return [[self.read(), self.read()] for _ in range(n)]
        if t == b'_':
            return None
        raise ValueError('bad reply %r' % line)

    def cmd(self, *args):
        self.send(enc(*args))
        return self.read()

    def close(self):
        try:
            self.f.close()   # makefile holds an io-ref; closing only the socket leaves the fd open
        except Exception:
            pass
        try:
            self.s.close()
        except Exception:
            pass


# ---- servers ----
def tlsargs(d):
    return ['--tls-cert-file', os.path.join(d, 'server.crt'), '--tls-key-file', os.path.join(d, 'server.key'),
            '--tls-ca-cert-file', os.path.join(d, 'ca.crt'), '--tls-auth-clients', 'no']


PROCS = []


def start(binary, port, cpus, main_cpu, owners, otls, tag, extra=()):
    d = os.path.join(a.dir, tag)
    os.makedirs(d, exist_ok=True)
    args = ['taskset', '-c', '%s,%s' % (cpus, main_cpu), binary, '--port', str(port), '--tls-port', str(port + 1),
            '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', d, '--logfile', os.path.join(d, 'log'),
            '--enable-debug-command', 'yes', '--daemonize', 'no'] + tlsargs(a.tls_dir)
    if owners:
        args += ['--io-threads', '1', '--owner-threads', str(a.n), '--owner-port', str(port + 21), '--owner-cpulist',
                 cpus, '--server-cpulist', main_cpu, '--bio-cpulist', main_cpu]
        if otls:
            args += ['--owner-tls-port', str(port + 121)]
    args += list(extra)
    p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append((p, port, d))
    for _ in range(200):
        try:
            c = Conn(port, False)
            c.cmd('PING')
            c.close()
            return d
        except Exception:
            time.sleep(0.05)
    res(False, 'start.' + tag)
    sys.exit(1)


def stop_all():
    for p, port, d in PROCS:
        try:
            c = Conn(port, False)
            c.send(enc('SHUTDOWN', 'NOSAVE'))
            c.close()
        except Exception:
            pass
        try:
            p.wait(10)
        except Exception:
            p.kill()


def crash_lines(d):
    try:
        t = open(os.path.join(d, 'log'), errors='replace').read()
    except Exception:
        return -1
    return sum(t.count(x) for x in ('ASSERT', 'crashed', 'Guru', 'BUG REPORT'))


def info(port, sec):
    c = Conn(port, False)
    t = c.cmd('INFO', sec).decode()
    c.close()
    return dict(l.split(':', 1) for l in t.split('\r\n') if ':' in l)


# ---- workload ----
SCRIPT = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], 1)"


def keyset(j, own, rng):
    """this connection's keys: 12 on its owner (native), 4 elsewhere, plus 2 hash tags on its owner and 1 off it"""
    mine, other, tags_mine, tags_other = [], [], [], []
    i = 0
    while len(mine) < 12 or len(other) < 4:
        k = 'c%d:k%d' % (j, i)
        i += 1
        if owner_of(slot(k), a.n) == own:
            if len(mine) < 12:
                mine.append(k)
        elif len(other) < 4:
            other.append(k)
    i = 0
    while len(tags_mine) < 2 or len(tags_other) < 1:
        t = 'c%dt%d' % (j, i)
        i += 1
        if owner_of(slot(t), a.n) == own:
            if len(tags_mine) < 2:
                tags_mine.append(t)
        elif len(tags_other) < 1:
            tags_other.append(t)
    return mine, other, tags_mine, tags_other


def gen_ops(j, own, nops, seed):
    rng = random.Random(seed * 1000003 + j)
    mine, other, tm, to = keyset(j, own, rng)
    ops = []

    def k():
        return rng.choice(mine) if rng.random() < 0.8 else rng.choice(other)

    def val():
        r = rng.random()
        if r < 0.03:
            return os.urandom(1).hex() * rng.randint(10000, 30000)  # 20-60 KB: spans TLS records
        return 'v%d' % rng.randint(0, 10 ** 6) + 'x' * rng.choice([0, 10, 100, 400, 900])

    while len(ops) < nops:
        r = rng.random()
        kk = k()
        if r < 0.20:
            ops.append(('GET', kk))
        elif r < 0.33:
            ops.append(('SET', kk, val()) if rng.random() < 0.7 else ('SET', kk, val(), 'EX', rng.randint(3600, 7200)))
        elif r < 0.36:
            ops.append(('DEL', kk))
        elif r < 0.42:
            ops.append(('INCR', 'n:' + kk))
        elif r < 0.44:
            ops.append(('INCRBY', 'n:' + kk, rng.randint(-5, 50)))
        elif r < 0.47:
            t = rng.choice(tm + to)
            ops.append(('MGET',) + tuple('{%s}:%d' % (t, x) for x in range(rng.randint(1, 6))))
        elif r < 0.50:
            t = rng.choice(tm + to)
            ops.append(('MSET',) + sum((('{%s}:%d' % (t, x), val()) for x in range(rng.randint(1, 4))), ()))
        elif r < 0.56:
            ops.append(('HSET', 'h:' + kk, 'f%d' % rng.randint(0, 9), val()))
        elif r < 0.59:
            ops.append(('HGET', 'h:' + kk, 'f%d' % rng.randint(0, 9)))
        elif r < 0.60:
            ops.append(('HGETALL', 'h:' + kk))
        elif r < 0.65:
            ops.append((rng.choice(['LPUSH', 'RPUSH']), 'l:' + kk, val()))
        elif r < 0.67:
            ops.append((rng.choice(['LPOP', 'RPOP']), 'l:' + kk))
        elif r < 0.69:
            ops.append(('LRANGE', 'l:' + kk, 0, rng.randint(0, 20)))
        elif r < 0.72:
            ops.append(('SADD', 's:' + kk, 'm%d' % rng.randint(0, 30)))
        elif r < 0.73:
            ops.append(('SISMEMBER', 's:' + kk, 'm%d' % rng.randint(0, 30)))
        elif r < 0.76:
            ops.append(('ZADD', 'z:' + kk, rng.randint(0, 100), 'm%d' % rng.randint(0, 30)))
        elif r < 0.77:
            ops.append(('ZRANGE', 'z:' + kk, 0, -1, 'WITHSCORES'))
        elif r < 0.80:
            ops.append(('EXPIRE', kk, rng.randint(3600, 7200)))
        elif r < 0.82:
            ops.append(('TTL', kk))
        elif r < 0.83:
            ops.append(('PERSIST', kk))
        elif r < 0.85:
            ops.append(('EXISTS', kk))
        elif r < 0.86:
            ops.append(('TYPE', kk))
        elif r < 0.87:
            ops.append(('STRLEN', kk))
        elif r < 0.88:
            ops.append(('APPEND', kk, 'a'))
        elif r < 0.92:
            # MULTI/EXEC: native only on its own keys, or mixed
            t = rng.choice(tm)
            body = [('SET', '{%s}:m' % t, val()), ('INCR', '{%s}:c' % t), ('GET', '{%s}:m' % t)]
            if rng.random() < 0.4:
                body.append(('HSET', '{%s}:h' % t, 'f', 'v'))
            ops.append(('MULTI',))
            ops.extend(body)
            ops.append(('EXEC',))
        elif r < 0.94:
            t = rng.choice(tm + to)
            ops.append(('EVAL', SCRIPT, 2, '{%s}:e1' % t, '{%s}:e2' % t, 'x%d' % rng.randint(0, 99)))
        elif r < 0.97:
            ops.append(('INCR', '{shared}:%d' % rng.randint(0, 3)))   # interleaving-dependent: not compared
        elif r < 0.98:
            ops.append(('PING',))
        else:
            ops.append(('ECHO', val()))
    return ops


def norm(op, rep):
    c = op[0]
    if c in ('TTL', 'PTTL') and isinstance(rep, int):
        return rep if rep < 0 else 'pos'
    if c == 'INCR' and op[1].startswith('{shared}'):
        return 'shared'
    if c == 'HGETALL' and isinstance(rep, list):
        return sorted(zip(rep[0::2], rep[1::2]))   # hashtable order follows the process's hash seed
    return rep


def run_conn(port, tls, ops, out, idx, rng_seed, errors):
    rng = random.Random(rng_seed)
    try:
        c = Conn(port, tls)
        reps = []
        i = 0
        while i < len(ops):
            n = rng.choice([1, 1, 1, 2, 4, 8, 16, 24])
            batch = ops[i:i + n]
            c.send(b''.join(enc(*op) for op in batch))
            for op in batch:
                reps.append(norm(op, c.read()))
            i += n
        c.close()
        out[idx] = reps
    except Exception as e:
        errors.append('%d: %r' % (idx, e))
        out[idx] = None


def run_all(cand_ports, ref_port, plan, tag):
    """plan: list of (owner, tls, ops). Candidate and parent at once, one thread per connection on each."""
    outc, outr, errs = [None] * len(plan), [None] * len(plan), []
    th = []
    for idx, (own, tls, ops) in enumerate(plan):
        th.append(threading.Thread(target=run_conn, args=(cand_ports[(own, tls)], tls, ops, outc, idx, idx, errs)))
        th.append(threading.Thread(target=run_conn, args=(ref_port, True, ops, outr, idx, idx + 7, errs)))
    t0 = time.time()
    for t in th:
        t.start()
    for t in th:
        t.join()
    bad = [i for i in range(len(plan)) if outc[i] != outr[i]]
    first = ''
    if bad:
        i = bad[0]
        for q, (x, y) in enumerate(zip(outc[i] or [], outr[i] or [])):
            if x != y:
                first = 'conn %d op %d %r: cand %r ref %r' % (i, q, plan[i][2][q][:3], str(x)[:120], str(y)[:120])
                break
    res(not bad and not errs, '%s.replies' % tag, '%d connections (%d TLS), %d replies, %.1f s, mismatched %d %s%s' % (
        len(plan), sum(1 for p in plan if p[1]), sum(len(p[2]) for p in plan), time.time() - t0, len(bad), first,
        (' errors %s' % errs[:3]) if errs else ''))


def keyspace(port):
    c = Conn(port, False)
    cur, keys = b'0', []
    while True:
        r = c.cmd('SCAN', cur, 'COUNT', 1000)
        cur, ks = r[0], r[1]
        keys += ks
        if cur == b'0':
            break
    snap = {}
    for k in keys:
        t = c.cmd('TYPE', k)
        e = c.cmd('OBJECT', 'ENCODING', k)
        # the value in a canonical form (DUMP of a hashtable encoding follows the process's hash seed)
        if t == 'string':
            v = c.cmd('GET', k)
        elif t == 'hash':
            r = c.cmd('HGETALL', k)
            v = sorted(r) if r and isinstance(r[0], list) else sorted(zip(r[0::2], r[1::2]))
        elif t == 'list':
            v = c.cmd('LRANGE', k, 0, -1)
        elif t == 'set':
            v = sorted(c.cmd('SMEMBERS', k))
        elif t == 'zset':
            v = c.cmd('ZRANGE', k, 0, -1, 'WITHSCORES')
        else:
            v = c.cmd('DUMP', k)
        ttl = c.cmd('PTTL', k)
        snap[k] = (t, e, v, ttl if ttl < 0 else 'pos')
    c.close()
    return snap


def cmp_keyspace(pa, pb, tag, strenc=True):
    """strenc False (an AOF reload): encoding is not compared. A reload rebuilds every value from its records and may
    pick a different encoding than the live server that passed through other intermediate states (embstr/raw,
    listpack/hashtable/quicklist/intset/skiplist); only the logical type, value and TTL must match, as on stock."""
    A, B = keyspace(pa), keyspace(pb)

    def key(v):
        return v if strenc or not v else (v[0],) + v[2:]   # drop the encoding field

    diff = [k for k in set(A) | set(B) if key(A.get(k)) != key(B.get(k))]
    encs = sum(1 for k in set(A) & set(B) if A[k] != B[k] and key(A[k]) == key(B[k]))
    res(not diff, tag, '%d keys vs %d, differing %d%s %s' % (
        len(A), len(B), len(diff), (', encoding-only (ignored) %d' % encs) if encs else '',
        ('e.g. %r: %r / %r' % (diff[0], str(A.get(diff[0]))[:80], str(B.get(diff[0]))[:80])) if diff else ''))


# ---- clients dropping mid-handshake ----
def client_hello():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    i, o = ssl.MemoryBIO(), ssl.MemoryBIO()
    obj = ctx.wrap_bio(i, o, server_hostname='127.0.0.1')
    try:
        obj.do_handshake()
    except ssl.SSLWantReadError:
        pass
    return o.read()


def hsdrop(ports, stop, counts):
    hello = client_hello()
    rng = random.Random(99)
    held = []
    while not stop.is_set():
        port = rng.choice(ports)
        kind = rng.randint(0, 5)
        try:
            s = socket.create_connection(('127.0.0.1', port), timeout=5)
            if kind == 0:
                pass                                   # connect, close: no ClientHello
            elif kind == 1:
                s.sendall(hello[:rng.randint(1, len(hello) - 1)])   # partial ClientHello
            elif kind == 2:
                s.sendall(hello)                       # full ClientHello, then drop after part of the reply
                s.recv(rng.randint(1, 600))
            elif kind == 3:
                s.sendall(b'PING\r\n')                 # plain text on a TLS port
                try:
                    s.recv(100)
                except Exception:
                    pass
            elif kind == 4:                            # RST mid-handshake
                s.sendall(hello)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b'\x01\x00\x00\x00\x00\x00\x00\x00')
            else:                                      # a full handshake, then close at once
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                s = ctx.wrap_socket(s, server_hostname='127.0.0.1')
            if rng.random() < 0.02 and len(held) < 20:
                held.append(s)                         # a handshake left hanging until the end
            else:
                s.close()
            counts[kind] += 1
        except Exception:
            counts[6] += 1
        time.sleep(0.002)
    for s in held:
        s.close()


# ---- main ----
cand = a.port
ref = a.ref_port
dc = start(a.bin, cand, a.cpus, a.main_cpu, True, True, 'cand',
           ['--appendonly', 'yes', '--appendfsync', 'everysec'] if a.mode == 'aof' else [])
dr = start(a.ref_bin, ref, a.ref_cpus, a.ref_main_cpu, True, False, 'ref')
inf = info(cand, 'owners')
res(inf.get('owner_tls_port') == str(cand + 121), 'setup.owner_tls_port', inf.get('owner_tls_port', ''))
rep_port = None
if a.mode == 'repl':
    rep_port = cand + 250
    start(a.bin, rep_port, a.ref_cpus.split('-')[0], a.ref_main_cpu, False, False, 'replica')
    rc = Conn(rep_port, False)
    rc.cmd('REPLICAOF', '127.0.0.1', cand)
    for _ in range(200):
        if info(rep_port, 'replication').get('master_link_status') == 'up':
            break
        time.sleep(0.1)
    rc.close()
    res(info(rep_port, 'replication').get('master_link_status') == 'up', 'repl.link_up')

# CLUSTER SLOTS / SHARDS / NODES: TLS clients get the owner TLS ports, plain clients the plain ones
c = Conn(cand + 1, True)
sl = c.cmd('CLUSTER', 'SLOTS')
sh = c.cmd('CLUSTER', 'SHARDS')
c.close()
c = Conn(cand, False)
slp = c.cmd('CLUSTER', 'SLOTS')
c.close()
res(sorted(x[2][1] for x in sl) == [cand + 121 + i for i in range(a.n)], 'topology.slots_tls', str([x[2][1] for x in sl]))
res(sorted(x[2][1] for x in slp) == [cand + 21 + i for i in range(a.n)], 'topology.slots_plain', str([x[2][1] for x in slp]))
n0 = dict((sh[0][3][0][q], sh[0][3][0][q + 1]) for q in range(0, len(sh[0][3][0]), 2)) if isinstance(sh[0], list) else {}
res(n0.get(b'tls-port') == cand + 121 and n0.get(b'port') == cand + 21, 'topology.shards', str(n0)[:200])

stop = threading.Event()
counts = [0] * 7
hth = None
if a.hsdrop:
    hth = threading.Thread(target=hsdrop, args=([cand + 121 + i for i in range(a.n)], stop, counts))
    hth.start()

ports = {}
for i in range(a.n):
    ports[(i, True)] = cand + 121 + i
    ports[(i, False)] = cand + 21 + i
for rnd in range(2):
    plan = []
    j = rnd * 10000
    for i in range(a.n):
        for q in range(a.conns):
            plan.append((i, True, gen_ops(j, i, a.ops, a.seed + rnd)))
            j += 1
        for q in range(a.plain_conns):
            plan.append((i, False, gen_ops(j, i, a.ops, a.seed + rnd)))
            j += 1
    if rnd == 1 and a.tlsconfig and a.tls_dir2:
        # CONFIG SET of the TLS files: existing owner-TLS connections go on; new ones get the new cert
        old = Conn(cand + 121, True)
        old.cmd('SET', 'tlscfg', 'before')
        c = Conn(cand, False)
        r = c.cmd('CONFIG', 'SET', 'tls-cert-file', os.path.join(a.tls_dir2, 'server.crt'), 'tls-key-file',
                  os.path.join(a.tls_dir2, 'server.key'), 'tls-ca-cert-file', os.path.join(a.tls_dir2, 'ca.crt'))
        c.close()
        time.sleep(1.0)
        okold = old.cmd('GET', 'tlscfg') == b'before'
        old.cmd('DEL', 'tlscfg')
        old.close()
        try:
            Conn(cand + 121 + 1, True).close()   # old CA: must fail now
            newold = 'accepted'
        except Exception:
            newold = 'refused'
        try:
            nc = Conn(cand + 121 + 1, True, ca=os.path.join(a.tls_dir2, 'ca.crt'))
            pong = nc.cmd('PING')
            nc.close()
        except Exception as e:
            pong = repr(e)
        res(r == 'OK' and okold and newold == 'refused' and pong == 'PONG', 'tlsconfig.owner_ports',
            'config set %r, old conn ok %s, old CA %s, new CA ping %r' % (r, okold, newold, pong))
        # the round itself keeps using the old CA for the owner-TLS model connections: switch back
        c = Conn(cand, False)
        c.cmd('CONFIG', 'SET', 'tls-cert-file', os.path.join(a.tls_dir, 'server.crt'), 'tls-key-file',
              os.path.join(a.tls_dir, 'server.key'), 'tls-ca-cert-file', os.path.join(a.tls_dir, 'ca.crt'))
        c.close()
        time.sleep(1.0)
    run_all(ports, ref + 1, plan, 'model.r%d' % rnd)

if hth and a.hsdrop_secs:
    time.sleep(a.hsdrop_secs)   # drops alone, after the model rounds
stop.set()
if hth:
    hth.join()
    res(sum(counts[:6]) > 0, 'hsdrop.done', 'by kind %s, connect errors %d' % (counts[:6], counts[6]))

# shared counters: totals equal (every INCR counted once)
for k in range(4):
    a1 = Conn(cand, False).cmd('GET', '{shared}:%d' % k)
    b1 = Conn(ref, False).cmd('GET', '{shared}:%d' % k)
    res(a1 == b1, 'shared.counter%d' % k, '%r %r' % (a1, b1))
time.sleep(0.5)
cmp_keyspace(cand, ref, 'keyspace.cand_vs_ref')
inf = info(cand, 'owners')
cl = info(cand, 'clients')
res(int(inf.get('owner_tls_handshakes', 0)) > 0 and int(inf.get('owner_native_commands', 0)) > 0, 'owners.counters',
    'tls handshakes %s, tls reads %s, native %s, handoffs %s, returns %s' % (
        inf.get('owner_tls_handshakes'), inf.get('owner_tls_reads'), inf.get('owner_native_commands'),
        inf.get('owner_handoffs_to_main'), inf.get('owner_returns_from_main')))
for _ in range(50):
    cl = info(cand, 'clients')
    if int(cl['connected_clients']) <= 2:
        break
    time.sleep(0.1)
res(int(cl['connected_clients']) <= 2, 'clients.cleaned_up', 'connected_clients %s' % cl['connected_clients'])
if int(cl['connected_clients']) > 2:
    c = Conn(cand, False)
    print('CLIENTLIST\n' + c.cmd('CLIENT', 'LIST').decode(), flush=True)
    c.close()
if a.mode == 'repl':
    for _ in range(300):
        p = int(info(cand, 'replication')['master_repl_offset'])
        r = int(info(rep_port, 'replication').get('slave_repl_offset', -1))
        if p == r:
            break
        time.sleep(0.1)
    res(p == r, 'repl.offset', '%d %d' % (p, r))
    cmp_keyspace(cand, rep_port, 'repl.keyspace')
if a.mode == 'aof':
    c = Conn(cand, False)
    c.cmd('CONFIG', 'SET', 'appendfsync', 'always')
    c.cmd('SET', 'aof:barrier', '1')
    c.close()
    time.sleep(1.5)
    import shutil
    dcopy = os.path.join(a.dir, 'aofload')
    shutil.rmtree(dcopy, ignore_errors=True)
    shutil.copytree(os.path.join(dc, 'appendonlydir'), os.path.join(dcopy, 'appendonlydir'))
    lp = cand + 400
    p = subprocess.Popen(['taskset', '-c', a.ref_cpus, a.bin, '--port', str(lp), '--bind', '127.0.0.1', '--save', '',
                          '--appendonly', 'yes', '--dir', dcopy, '--logfile', os.path.join(dcopy, 'log')],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append((p, lp, dcopy))
    for _ in range(300):
        try:
            if Conn(lp, False).cmd('PING') == 'PONG' and info(lp, 'persistence').get('loading') == '0':
                break
        except Exception:
            pass
        time.sleep(0.1)
    cmp_keyspace(cand, lp, 'aof.reload_keyspace', strenc=False)
stop_all()
for p, port, d in PROCS:
    res(crash_lines(d) == 0, 'crash.%s' % os.path.basename(d), 'lines %d' % crash_lines(d))
print('TLSOWNERCHECK mode=%s fails=%d %s' % (a.mode, len(FAILS), FAILS[:10]), flush=True)
sys.exit(1 if FAILS else 0)
