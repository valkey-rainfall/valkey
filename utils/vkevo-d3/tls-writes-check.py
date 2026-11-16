#!/usr/bin/env python3
"""RL-g2d-tls-writes gene check: gathered TLS writes on the owner TLS ports (records of one write in one write(),
the unsent rest kept as a tail and sent first by whichever thread writes next; big values by reference).
One candidate server (owners on, owner TLS ports on), loopback. Parts:
  size     values of 1 B .. 1 MB (record edges 16376-16394, 64 KB edges): GET / pipelined GETs / MGET on the owner
           TLS port, the main TLS port and the plain owner port must return the exact bytes
  slow     slow readers (4 KB receive buffer) pipelining MBs of big GETs: every reply exact and in order, other
           clients of the same owner unaffected; the tail path must have run (owner_tls_write_tails > 0)
  obuf     client-output-buffer-limit still disconnects a TLS client that does not read
  drop     clients dropping (close / RST) mid-reply, many at once: server alive, connections cleaned up
  handoff  a slow reader whose pipeline mixes native GETs (owner, gathered, tails) with commands that go to main
           (lists, hashes, big LRANGE replies written by main): every reply exact and in order
  tlscfg   CONFIG SET of the TLS files during big-value traffic: old connections go on, new ones get the new cert
  model    1500 random SET/GET/MGET/APPEND/GETRANGE of random sizes (pipelines 1-16) vs a dict model
Prints PASS/FAIL lines; exit 1 on any FAIL."""
import argparse, os, random, socket, ssl, struct, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--port', type=int, default=24200)   # plain; +1 TLS; owners +20..; owner TLS +40..
ap.add_argument('--cpus', default='0-3')
ap.add_argument('--main-cpu', default='4')
ap.add_argument('--bio-cpu', default='5')
ap.add_argument('--tls-dir', default='/home/ec2-user/ns-tls')
ap.add_argument('--tls-dir2', default='')
ap.add_argument('--seed', type=int, default=7)
ap.add_argument('--expect-tails', action='store_true')   # small send buffers (tw2.sh SMALLBUF=1): tails must occur
ap.add_argument('--drop-rounds', type=int, default=1)
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
FAILS = []
P = a.port


def res(ok, name, detail=''):
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, detail), flush=True)
    if not ok:
        FAILS.append(name)


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
    t = sl // (16384 // tables)
    for i in range(n):
        if tables * i // n <= t < tables * (i + 1) // n:
            return i


def tag_on(owner, base):
    """a hash tag whose slot belongs to the given owner"""
    i = 0
    while True:
        t = '%s%d' % (base, i)
        if owner_of(slot('{%s}' % t), a.n) == owner:
            return t
        i += 1


def enc(*args):
    out = [b'*%d\r\n' % len(args)]
    for x in args:
        x = x if isinstance(x, bytes) else str(x).encode()
        out.append(b'$%d\r\n%s\r\n' % (len(x), x))
    return b''.join(out)


class Conn:
    def __init__(self, port, tls, ca=None, rcvbuf=0):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if rcvbuf:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        s.settimeout(60)
        s.connect(('127.0.0.1', port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.load_verify_locations(ca or os.path.join(a.tls_dir, 'ca.crt'))
            s = ctx.wrap_socket(s, server_hostname='127.0.0.1')
        self.s = s
        self.f = s.makefile('rb', buffering=65536)
        self.slow = 0

    def send(self, data):
        self.s.sendall(data)

    def _readn(self, n):
        if not self.slow:
            return self.f.read(n)
        out = []
        while n > 0:   # slow reader: 16 KB at a time with a pause
            d = self.f.read(min(n, 16384))
            if not d:
                break
            out.append(d)
            n -= len(d)
            time.sleep(self.slow)
        return b''.join(out)

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
            d = self._readn(n + 2)
            if len(d) != n + 2 or d[-2:] != b'\r\n':
                raise ValueError('short bulk %d of %d' % (len(d), n + 2))
            return d[:-2]
        if t == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b'%':
            return [[self.read(), self.read()] for _ in range(int(rest))]
        if t == b'_':
            return None
        raise ValueError('bad reply %r' % line[:60])

    def cmd(self, *args):
        self.send(enc(*args))
        return self.read()

    def rst(self):
        try:
            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        except Exception:
            pass
        self.close()

    def close(self):
        for x in (self.f, self.s):
            try:
                x.close()
            except Exception:
                pass


def tlsargs(d):
    return ['--tls-cert-file', os.path.join(d, 'server.crt'), '--tls-key-file', os.path.join(d, 'server.key'),
            '--tls-ca-cert-file', os.path.join(d, 'ca.crt'), '--tls-auth-clients', 'no']


d = os.path.join(a.dir, 'srv')
os.makedirs(d, exist_ok=True)
args = ['taskset', '-c', '%s,%s,%s' % (a.cpus, a.main_cpu, a.bio_cpu), a.bin, '--port', str(P), '--tls-port', str(P + 1),
        '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', d, '--logfile', os.path.join(d, 'log'),
        '--enable-debug-command', 'yes', '--daemonize', 'no', '--io-threads', '1', '--owner-threads', str(a.n),
        '--owner-port', str(P + 20), '--owner-tls-port', str(P + 40), '--owner-cpulist', a.cpus, '--server-cpulist',
        a.main_cpu, '--bio-cpulist', a.bio_cpu, '--maxclients', '20000'] + tlsargs(a.tls_dir)
srv = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for _ in range(200):
    try:
        c0 = Conn(P, False)
        c0.cmd('PING')
        break
    except Exception:
        time.sleep(0.05)
else:
    res(False, 'start')
    sys.exit(1)


def info(sec):
    r = c0.cmd('INFO', sec)
    out = {}
    for line in r.decode().split('\r\n'):
        if ':' in line:
            k, v = line.split(':', 1)
            out[k] = v
    return out


def oport(key, tls=True):
    return P + (40 if tls else 20) + owner_of(slot(key), a.n)


def val(seed, n):
    r = random.Random(seed * 1000003 + n)
    b = bytearray(r.getrandbits(8) for _ in range(min(n, 4096)))
    v = (bytes(b) * (n // max(len(b), 1) + 1))[:n]
    if n > 40:   # CRLFs and a fake RESP header inside the value
        v = v[:10] + b'\r\n$5\r\nxx' + v[18:]
    return v[:n]


tails0 = int(info('owners').get('owner_tls_write_tails', '-1'))
res(tails0 == 0, 'info.tails_field', 'owner_tls_write_tails=%d at start' % tails0)

# ---- size ----
SIZES = [1, 2, 100, 1000, 2047, 2048, 4096, 8000, 16375, 16376, 16377, 16383, 16384, 16385, 16394, 20000, 32768,
         49152, 65526, 65535, 65536, 65537, 100000, 131072, 262144, 500000, 1048575, 1048576]
bad = []
for i, n in enumerate(SIZES):
    k = '{sz%d}k' % i
    v = val(1, n)
    w = Conn(oport(k), True)
    if w.cmd('SET', k, v) != 'OK':
        bad.append('set %d' % n)
    got = w.cmd('GET', k)
    w.send(enc('GET', k) * 6 + enc('PING'))
    pipe = [w.read() for _ in range(7)]
    k2, k3 = '{sz%d}b' % i, '{sz%d}c' % i
    w.cmd('SET', k2, val(2, n // 2 + 1))
    w.cmd('SET', k3, b'seven77')
    mg = w.cmd('MGET', k, k2, k3)
    w.close()
    m = Conn(P + 1, True)
    gm = m.cmd('GET', k)
    m.close()
    pl = Conn(oport(k, False), False)
    gp = pl.cmd('GET', k)
    pl.close()
    ok = got == v and pipe == [v] * 6 + ['PONG'] and gm == v and gp == v and mg == [v, val(2, n // 2 + 1), b'seven77']
    if not ok:
        bad.append('%d (get %s pipe %s main %s plain %s mget %s)' % (n, got == v, pipe == [v] * 6 + ['PONG'], gm == v,
                                                                    gp == v, mg == [v, val(2, n // 2 + 1), b'seven77']))
o = info('owners')
res(not bad, 'size.1B_1MB', '%d sizes; ref replies %s; bad: %s' % (len(SIZES), o.get('owner_ref_replies'), bad[:4]))

# ---- slow readers ----
own = 1
tg = tag_on(own, 'slow')
BIG = [1048576, 16384, 7, 100000, 65537, 262144, 16394, 500000]
keys = []
for j, n in enumerate(BIG):
    k = '{%s}v%d' % (tg, j)
    Conn(oport(k), True).cmd('SET', k, val(3, n))
    keys.append((k, val(3, n)))


def slow_reader(idx, nget, out):
    try:
        c = Conn(P + 40 + own, True, rcvbuf=4096)
        seq = [keys[(idx + q) % len(keys)] for q in range(nget)]
        # in waves while not reading: once the socket is full, the next waves' writes fall short (tails)
        for wv in range(4):
            part = seq[wv * nget // 4:(wv + 1) * nget // 4]
            c.send(b''.join(enc('GET', k) for k, _ in part) + (enc('PING') if wv == 3 else b''))
            time.sleep(0.3)
        c.slow = 0.001
        badn = 0
        for k, v in seq:
            r = c.read()
            if r != v:
                badn += 1
        pong = c.read()
        c.close()
        out[idx] = (badn, pong)
    except Exception as e:
        out[idx] = (-1, repr(e))


def fast_neighbour(out):
    try:
        c = Conn(P + 40 + own, True)
        t = tag_on(own, 'nb')
        okn = 0
        for q in range(1500):
            k = '{%s}n%d' % (t, q % 50)
            v = b'n%d' % q
            c.send(enc('SET', k, v) + enc('GET', k))
            if c.read() == 'OK' and c.read() == v:
                okn += 1
        c.close()
        out['nb'] = okn
    except Exception as e:
        out['nb'] = repr(e)


out = {}
th = [threading.Thread(target=slow_reader, args=(q, 24, out)) for q in range(3)] + [threading.Thread(target=fast_neighbour, args=(out,))]
t0 = time.time()
for t in th:
    t.start()
for t in th:
    t.join()
tails1 = int(info('owners').get('owner_tls_write_tails', '-1'))
okslow = all(out.get(q) == (0, 'PONG') for q in range(3))
res(okslow and out.get('nb') == 1500, 'slow.readers', 'readers %s, neighbour ok %s/1500, %.1f s' %
    ([out.get(q) for q in range(3)], out.get('nb'), time.time() - t0))
res(tails1 > tails0 or not a.expect_tails, 'slow.tails_exercised', 'owner_tls_write_tails %d -> %d%s' %
    (tails0, tails1, '' if a.expect_tails else ' (default send buffers: tails need a write larger than the free space)'))

# ---- output buffer limit ----
c0.cmd('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 2mb 0 0')
d0 = int(info('stats').get('client_output_buffer_limit_disconnections', '0'))
x = Conn(P + 40 + own, True, rcvbuf=4096)
x.send(b''.join(enc('GET', keys[0][0]) for _ in range(24)))
time.sleep(2.0)
d1 = int(info('stats').get('client_output_buffer_limit_disconnections', '0'))
closed = False
try:
    x.slow = 0
    for _ in range(25):
        x.read()
except Exception:
    closed = True
x.close()
c0.cmd('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 0 0 0')
y = Conn(P + 40 + own, True)
alive = y.cmd('GET', keys[0][0]) == keys[0][1]
y.close()
res(d1 == d0 + 1 and closed and alive, 'obuf.limit_applies', 'obuf disconnections %d -> %d, client closed %s, others ok %s'
    % (d0, d1, closed, alive))

# ---- drops mid-reply ----
def dropper(idx):
    r = random.Random(idx)
    for q in range(25):
        try:
            k, _ = keys[r.randrange(len(keys))]
            c = Conn(P + 40 + own if r.random() < 0.8 else P + 1, True)
            c.send(b''.join(enc('GET', keys[(q + z) % len(keys)][0]) for z in range(8)))
            want = r.randrange(0, 300000)
            got = 0
            while got < want:
                b = c.f.read1(min(65536, want - got)) if hasattr(c.f, 'read1') else c.f.read(min(65536, want - got))
                if not b:
                    break
                got += len(b)
            (c.rst if r.random() < 0.5 else c.close)()
        except Exception as e:
            errs.append(repr(e))


for dr in range(a.drop_rounds):
    time.sleep(0.5)
    base = int(info('clients')['connected_clients'])
    errs = []
    th = [threading.Thread(target=dropper, args=(q + 100 * dr,)) for q in range(8)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    cc = -1
    t0 = time.time()
    for _ in range(150):
        cc = int(info('clients')['connected_clients'])
        if cc <= base:
            break
        time.sleep(0.1)
    left_owner = 0
    if cc > base:
        cl = c0.cmd('CLIENT', 'LIST').decode()
        print('INFO drop.clients_left %s' % cl.replace('\n', ' | ')[:1500], flush=True)
        # a main-TLS-port client (stock main path, no gathered writes) can stay too, on the parent as well
        # (seen 2026-11-16 with the parent binary: same omem / tot-net-out); the gene's owner ports must be clean
        for line in cl.split('\n'):
            lp = [f for f in line.split() if f.startswith('laddr=')]
            if lp and P + 20 <= int(lp[0].rsplit(':', 1)[1]) < P + 40 + a.n and 'cmd=client' not in line:
                left_owner += 1
    z = Conn(P + 40 + own, True)
    alive = z.cmd('GET', keys[0][0]) == keys[0][1] and z.cmd('PING') == 'PONG'
    z.close()
    res(left_owner == 0 and alive and not errs, 'drop.mid_reply.r%d' % dr, '200 drops (half RST); clients %d -> %d after '
        '%.1f s (owner-port clients left %d); alive %s; errors %s' % (base, cc, time.time() - t0, left_owner, alive, errs[:2]))

# ---- handoff with a tail ----
tg2 = tag_on(2, 'ho')
kb = ['{%s}g%d' % (tg2, q) for q in range(3)]
w = Conn(P + 42, True)
for q, k in enumerate(kb):
    w.cmd('SET', k, val(10 + q, [300000, 16394, 1048576][q]))
lk, hk = '{%s}list' % tg2, '{%s}hash' % tg2
w.cmd('DEL', lk, hk)
w.close()
item = val(20, 100000)
exp = []
pipe = []
model_list = []
for rnd in range(6):
    pipe.append(b''.join([enc('GET', kb[0]), enc('LPUSH', lk, item + b'%d' % rnd), enc('GET', kb[1]), enc('LRANGE', lk, 0, -1),
                          enc('HSET', hk, 'f%d' % rnd, rnd), enc('GET', kb[2]), enc('ECHO', b'e%d' % rnd), enc('PING')]))
    model_list.insert(0, item + b'%d' % rnd)
    exp += [val(10, 300000), len(model_list), val(11, 16394), list(model_list), 1, val(12, 1048576), b'e%d' % rnd, 'PONG']
h = Conn(P + 42, True, rcvbuf=4096)
tb = int(info('owners').get('owner_tls_write_tails', '-1'))
hb = int(info('owners').get('owner_handoffs_to_main', '0'))
for wv in pipe:   # one round per wave, not reading: later rounds meet a full socket (tails), then go to main
    h.send(wv)
    time.sleep(0.3)
h.slow = 0.001
got = []
try:
    for _ in exp:
        got.append(h.read())
except Exception as e:
    got.append(repr(e))
h.close()
ta = int(info('owners').get('owner_tls_write_tails', '-1'))
ha = int(info('owners').get('owner_handoffs_to_main', '0'))
nbad = sum(1 for x, y in zip(got, exp) if x != y) + abs(len(got) - len(exp))
res(nbad == 0 and ha > hb and (ta > tb or not a.expect_tails), 'handoff.tail_order',
    '%d replies, %d wrong; handoffs to main +%d; tails +%d' % (len(exp), nbad, ha - hb, ta - tb))

# ---- CONFIG SET of the TLS files ----
if a.tls_dir2:
    stop = threading.Event()
    tout = {'ok': 0, 'bad': 0}

    def traffic():
        c = Conn(P + 40 + own, True)
        while not stop.is_set():
            k, v = keys[tout['ok'] % len(keys)]
            if c.cmd('GET', k) == v:
                tout['ok'] += 1
            else:
                tout['bad'] += 1
        c.close()

    tt = threading.Thread(target=traffic)
    tt.start()
    time.sleep(0.5)
    r = c0.cmd('CONFIG', 'SET', 'tls-cert-file', os.path.join(a.tls_dir2, 'server.crt'), 'tls-key-file',
               os.path.join(a.tls_dir2, 'server.key'), 'tls-ca-cert-file', os.path.join(a.tls_dir2, 'ca.crt'))
    time.sleep(1.0)
    try:
        Conn(P + 40 + own, True).close()
        oldca = 'accepted'
    except Exception:
        oldca = 'refused'
    try:
        nc = Conn(P + 40 + own, True, ca=os.path.join(a.tls_dir2, 'ca.crt'))
        newget = nc.cmd('GET', keys[0][0]) == keys[0][1]
        nc.close()
    except Exception as e:
        newget = repr(e)
    time.sleep(0.5)
    stop.set()
    tt.join()
    c0.cmd('CONFIG', 'SET', 'tls-cert-file', os.path.join(a.tls_dir, 'server.crt'), 'tls-key-file',
           os.path.join(a.tls_dir, 'server.key'), 'tls-ca-cert-file', os.path.join(a.tls_dir, 'ca.crt'))
    time.sleep(0.5)
    res(r == 'OK' and oldca == 'refused' and newget is True and tout['bad'] == 0 and tout['ok'] > 0, 'tlscfg.big_values',
        'config set %r; old conn big GETs ok %d bad %d; new conn old CA %s; new CA big GET %s' %
        (r, tout['ok'], tout['bad'], oldca, newget))

# ---- model ----
rng = random.Random(a.seed)
model = {}
tg3 = tag_on(3, 'mo')
mk = ['{%s}m%d' % (tg3, q) for q in range(12)]
c = Conn(P + 43, True)
for k in mk:
    c.cmd('DEL', k)
nbad = 0
nops = 0
MS = [0, 1, 5, 100, 2048, 4000, 16376, 16384, 16394, 30000, 65536, 70000, 200000]
while nops < 1500:
    batch = []
    for _ in range(rng.randrange(1, 17)):
        k = rng.choice(mk)
        op = rng.random()
        if op < 0.35:
            v = val(rng.randrange(1000), rng.choice(MS))
            batch.append((enc('SET', k, v), 'OK'))
            model[k] = v
        elif op < 0.75:
            batch.append((enc('GET', k), model.get(k)))
        elif op < 0.85:
            ks = rng.sample(mk, 3)
            batch.append((enc('MGET', *ks), [model.get(x) for x in ks]))
        elif op < 0.93:
            v = val(rng.randrange(1000), rng.choice(MS[:8]))
            model[k] = model.get(k, b'') + v
            batch.append((enc('APPEND', k, v), len(model[k])))
        else:
            s, e = sorted([rng.randrange(0, 70000), rng.randrange(0, 70000)])
            cur = model.get(k, b'')
            batch.append((enc('GETRANGE', k, s, e), cur[s:e + 1] if cur else b''))
    c.send(b''.join(x for x, _ in batch))
    for _, want in batch:
        if c.read() != want:
            nbad += 1
        nops += 1
c.close()
res(nbad == 0, 'model.random_sizes', '%d ops, %d wrong' % (nops, nbad))

o = info('owners')
st = info('stats')
print('INFO owner_tls_write_tails %s ref_replies %s ref_copied %s tls_reads %s handoffs %s; net_output_bytes %s'
      % (o.get('owner_tls_write_tails'), o.get('owner_ref_replies'), o.get('owner_ref_copied'), o.get('owner_tls_reads'),
         o.get('owner_handoffs_to_main'), st.get('total_net_output_bytes')), flush=True)
c0.send(enc('SHUTDOWN', 'NOSAVE'))
try:
    srv.wait(15)
except Exception:
    srv.kill()
logt = open(os.path.join(d, 'log'), errors='replace').read()
crash = sum(logt.count(x) for x in ('ASSERT', 'crashed', 'Guru', 'BUG REPORT'))
res(crash == 0, 'crash_lines', '%d' % crash)
print('TLSWRITESCHECK %s (%d FAIL)' % ('PASS' if not FAILS else 'FAIL', len(FAILS)), flush=True)
sys.exit(1 if FAILS else 0)
