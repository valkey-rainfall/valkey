#!/usr/bin/env python3
"""RL-g6a-tls-records gene check: the owner record path (tls-owner-records) on one candidate server (owners on,
owner TLS ports on, loopback). --bin is a wrapper that turns the path on (verify mode: OpenSSL makes every record
too and the bytes must be equal). Parts (each prints PASS/FAIL lines; exit 1 on any FAIL):
  split    a raw client (Python SSLObject over memory BIOs) sends each command record split at every byte boundary,
           byte by byte, and many records in one send; every reply exact
  sizes    values 1 B .. 1 MB SET / GET / pipelined GETs on an owner TLS port, exact bytes back
  long     one long session: 30,000 commands in pipelines of 1-32 vs a dict model (with a small rekey setting the
           server sends KeyUpdates of its own: owner_tls_rec_keyupdates_out must grow)
  slow     a slow reader pipelining MBs of big GETs (4 KB receive buffer): replies exact and in order
  bad      a flipped tag bit, a wrong outer type, a bad version, an oversized length, a truncated record then close,
           and a close mid-record: each connection is closed by the server (alert or EOF) and the others go on
  ku       openssl s_client with 'k' (KeyUpdate) and 'K' (KeyUpdate, update requested) between PINGs, both ways
  suites   s_client with TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256 (all switch),
           and TLS 1.2 (stays on OpenSSL): every PING answered
  cli      valkey-cli --tls on the owner TLS port and the main TLS port
  stats    owner_tls_rec_mismatch == 0, owner_tls_rec_verified > 0 (verify mode), rec_conns > 0, server alive,
           no crash or assertion in the log"""
import argparse, os, random, re, socket, ssl, struct, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--cli', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--port', type=int, default=25600)   # plain; +1 TLS; owners +20..; owner TLS +40..
ap.add_argument('--cpus', default='74-77')
ap.add_argument('--main-cpu', default='78')
ap.add_argument('--bio-cpu', default='79')
ap.add_argument('--tls-dir', default='/home/ec2-user/ns-tls')
ap.add_argument('--seed', type=int, default=11)
ap.add_argument('--verify', action='store_true')
ap.add_argument('--expect-rekey', action='store_true')
ap.add_argument('--only', default='')
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
random.seed(a.seed)
FAILS = []
P = a.port
OT = P + 40
CA = os.path.join(a.tls_dir, 'ca.crt')


LASTI = {}


def res(ok, name, detail=''):
    global LASTI
    try:
        s = info()
        delta = ' [rec conns +%d, verified +%d]' % (ival(s, 'owner_tls_rec_conns') - ival(LASTI, 'owner_tls_rec_conns'),
                                                    ival(s, 'owner_tls_rec_verified') - ival(LASTI, 'owner_tls_rec_verified'))
        LASTI = s
    except Exception:
        delta = ''
    print('%s %s %s%s' % ('PASS' if ok else 'FAIL', name, detail, delta), flush=True)
    if not ok:
        FAILS.append(name)


def enc(*args):
    out = [b'*%d\r\n' % len(args)]
    for x in args:
        x = x if isinstance(x, bytes) else str(x).encode()
        out.append(b'$%d\r\n%s\r\n' % (len(x), x))
    return b''.join(out)


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
    """a hash tag whose slot belongs to the given owner (its commands run on that owner)"""
    i = 0
    while True:
        t = '%s%d' % (base, i)
        if owner_of(slot('{%s}' % t), a.n) == owner:
            return '{%s}' % t
        i += 1


class Parser:
    def __init__(self):
        self.buf = b''

    def feed(self, d):
        self.buf += d

    def one(self, i=0):
        """(value, next index) or None when incomplete"""
        b = self.buf
        j = b.find(b'\r\n', i)
        if j < 0:
            return None
        t, rest = b[i:i + 1], b[i + 1:j]
        if t in (b'+', b'-', b':'):
            return (rest if t != b':' else int(rest)), j + 2
        if t == b'$':
            n = int(rest)
            if n < 0:
                return None, j + 2
            if len(b) < j + 2 + n + 2:
                return None if True else None
            return b[j + 2:j + 2 + n], j + 2 + n + 2
        if t == b'*':
            n = int(rest)
            k = j + 2
            vals = []
            for _ in range(max(n, 0)):
                r = self.one(k)
                if r is None:
                    return None
                vals.append(r[0])
                k = r[1]
            return vals, k
        raise ValueError('bad reply %r' % b[i:i + 40])

    def take(self):
        r = self.one(0)
        if r is None:
            return False, None
        self.buf = self.buf[r[1]:]
        return True, r[0]


def cctx(suite=None):
    c = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    c.load_verify_locations(CA)
    c.check_hostname = False
    c.minimum_version = ssl.TLSVersion.TLSv1_3
    return c


class Raw:
    """TLS client over memory BIOs: the test decides how the record bytes reach the socket."""

    def __init__(self, port, rcvbuf=0):
        self.s = socket.socket()
        if rcvbuf:
            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.s.connect(('127.0.0.1', port))
        self.s.settimeout(10)
        self.inc, self.out = ssl.MemoryBIO(), ssl.MemoryBIO()
        self.o = cctx().wrap_bio(self.inc, self.out)
        self.p = Parser()
        while True:
            try:
                self.o.do_handshake()
                break
            except ssl.SSLWantReadError:
                self.flush()
                d = self.s.recv(65536)
                if not d:
                    raise EOFError('eof in handshake')
                self.inc.write(d)
        self.flush()

    def flush(self):
        d = self.out.read()
        if d:
            self.s.sendall(d)

    def record(self, data):
        """the ciphertext of data (one record per 16 KB)"""
        self.o.write(data)
        return self.out.read()

    def pump(self):
        try:
            d = self.o.read(1 << 20)
            if d:
                self.p.feed(d)
                return True
        except ssl.SSLWantReadError:
            pass
        d = self.s.recv(1 << 20)
        if not d:
            raise EOFError('eof')
        self.inc.write(d)
        return True

    def reply(self):
        while True:
            ok, v = self.p.take()
            if ok:
                return v
            self.pump()

    def cmd(self, *args):
        self.s.sendall(self.record(enc(*args)))
        return self.reply()

    def close(self):
        try:
            self.s.close()
        except Exception:
            pass


def info():
    out = subprocess.run([a.cli, '-p', str(P), 'info', 'owners'], capture_output=True, text=True, timeout=10).stdout
    d = {}
    for l in out.splitlines():
        if ':' in l:
            k, v = l.strip().split(':', 1)
            d[k] = v
    return d


def ival(d, k):
    try:
        return int(d.get(k, 0))
    except ValueError:
        return 0


def conns():
    out = subprocess.run([a.cli, '-p', str(P), 'info', 'clients'], capture_output=True, text=True, timeout=10).stdout
    m = re.search(r'connected_clients:(\d+)', out)
    return int(m.group(1)) if m else -1


T = a.tls_dir
d = os.path.join(a.dir, 'srv')
os.makedirs(d, exist_ok=True)
args = ['taskset', '-c', '%s,%s,%s' % (a.cpus, a.main_cpu, a.bio_cpu), a.bin, '--port', str(P), '--tls-port', str(P + 1),
        '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', d, '--logfile', os.path.join(d, 'log'),
        '--daemonize', 'no', '--io-threads', '1', '--owner-threads', str(a.n), '--owner-port', str(P + 20),
        '--owner-tls-port', str(OT), '--owner-cpulist', a.cpus, '--server-cpulist', a.main_cpu, '--bio-cpulist', a.bio_cpu,
        '--maxclients', '20000', '--loglevel', 'verbose', '--tls-cert-file', os.path.join(T, 'server.crt'), '--tls-key-file',
        os.path.join(T, 'server.key'), '--tls-ca-cert-file', CA, '--tls-auth-clients', 'no']
srv = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for _ in range(100):
    try:
        socket.create_connection(('127.0.0.1', P), timeout=1).close()
        break
    except OSError:
        time.sleep(0.1)
time.sleep(0.5)
i0 = info()
res(i0.get('owner_tls_records') in ('yes', 'verify'), 'stats.flag', 'owner_tls_records=%s' % i0.get('owner_tls_records'))
want = lambda p: not a.only or p in a.only.split(',')

try:
    T0, T1, T2, T3 = tag_on(0, 'a'), tag_on(1, 'b'), tag_on(2, 'c'), tag_on(3, 'd')
    if want('split'):
        c = Raw(OT)
        ok = c.cmd('PING') == b'PONG'
        nrec = 0
        for k in range(1, 200):
            v = b'v%d' % k * 3
            rec = c.record(enc('SET', T0 + 'sk%d' % k, v))
            cut = k % len(rec) or 1
            c.s.sendall(rec[:cut])
            time.sleep(0.002)
            c.s.sendall(rec[cut:])
            ok = ok and c.reply() == b'OK'
            nrec += 1
        rec = c.record(enc('GET', T0 + 'sk7'))
        c.s.sendall(rec)  # (every record made must be sent: the next one has the next sequence number)
        ok_all = c.reply() == b'v7v7v7'
        for cut in range(1, len(rec)):  # every byte boundary of one record
            rec = c.record(enc('GET', T0 + 'sk7'))
            c.s.sendall(rec[:cut])
            time.sleep(0.001)
            c.s.sendall(rec[cut:])
            ok_all = ok_all and c.reply() == b'v7v7v7'
        res(ok and ok_all, 'split.boundaries', 'records %d + %d cuts' % (nrec, len(rec) - 1))
        rec = c.record(enc('GET', T0 + 'sk9'))
        for i in range(len(rec)):
            c.s.send(rec[i:i + 1])
            time.sleep(0.0005)
        res(c.reply() == b'v9v9v9', 'split.bytewise', '%d single-byte sends' % len(rec))
        many = b''.join(c.record(enc('GET', T0 + 'sk%d' % k)) for k in range(1, 101))
        c.s.sendall(many)
        got = [c.reply() for _ in range(100)]
        res(got == [b'v%d' % k * 3 for k in range(1, 101)], 'split.manyinone', '100 records in one send (%d B)' % len(many))
        # a record holding several commands, and a command spread over several records
        blob = b''.join(enc('INCR', T0 + 'ctr') for _ in range(50))
        c.s.sendall(c.record(blob))
        got = [c.reply() for _ in range(50)]
        big = enc('SET', T0 + 'spread', b'x' * 40000)
        recs = b''.join(c.record(big[i:i + 777]) for i in range(0, len(big), 777))
        c.s.sendall(recs)
        res(got == list(range(1, 51)) and c.reply() == b'OK' and c.cmd('STRLEN', T0 + 'spread') == 40000, 'split.spread',
            'one record of 50 commands; one command over %d records' % ((len(big) + 776) // 777))
        c.close()

    if want('sizes'):
        c = Raw(OT + 1)
        ok, bad = True, []
        for n in [1, 2, 100, 1000, 16000, 16370, 16376, 16384, 16385, 16394, 32768, 65536, 100000, 1 << 20]:
            v = bytes(random.getrandbits(8) for _ in range(min(n, 4096))) * (n // 4096 + 1)
            v = v[:n]
            r1 = c.cmd('SET', T1 + 'z%d' % n, v)
            r2 = c.cmd('GET', T1 + 'z%d' % n)
            if r1 != b'OK' or r2 != v:
                ok = False
                bad.append(n)
        keys = [T1 + 'z%d' % n for n in [1, 16384, 16385, 100000]]
        c.s.sendall(c.record(b''.join(enc('GET', k) for k in keys * 4)))
        got = [c.reply() for _ in range(16)]
        exp = [c.cmd('GET', k) for k in keys] * 4
        res(ok and got == exp, 'sizes.exact', '1 B .. 1 MB, bad %s' % bad)
        c.close()

    if want('long'):
        c = Raw(OT + 2)
        model, ok, nops = {}, True, 0
        ku0 = ival(info(), 'owner_tls_rec_keyupdates_out')
        while nops < 30000:
            k = random.randint(1, 32)
            batch, exp = [], []
            for _ in range(k):
                # mostly this owner's keys (run on the owner); 1 in 8 another owner's (handed to main, which then
                # writes the reply on the record path too)
                key = (T2 if random.random() < 0.875 else T0) + 'L%d' % random.randint(0, 200)
                if random.random() < 0.5:
                    v = b'%d' % random.getrandbits(40) * random.randint(1, 30)
                    batch.append(enc('SET', key, v))
                    model[key] = v
                    exp.append(b'OK')
                else:
                    batch.append(enc('GET', key))
                    exp.append(model.get(key))
            c.s.sendall(c.record(b''.join(batch)))
            got = [c.reply() for _ in range(k)]
            if got != exp:
                ok = False
                break
            nops += k
        ku1 = ival(info(), 'owner_tls_rec_keyupdates_out')
        res(ok, 'long.model', '%d commands, server KeyUpdates sent %d' % (nops, ku1 - ku0))
        if a.expect_rekey:
            res(ku1 - ku0 >= 10, 'long.rekey', 'server KeyUpdates %d (rekey setting)' % (ku1 - ku0))
        c.close()

    if want('slow'):
        c = Raw(OT + 3, rcvbuf=4096)
        v = b'S' * 300000
        c.cmd('SET', T3 + 'slowv', v)
        reqs = b''.join(enc('GET', T3 + 'slowv') for _ in range(20))
        c.s.sendall(c.record(reqs))
        time.sleep(1.0)  # the server fills the socket and waits
        got = []
        for _ in range(20):
            got.append(c.reply())
            time.sleep(0.02)
        c2 = Raw(OT + 3)
        res(all(g == v for g in got) and c2.cmd('PING') == b'PONG', 'slow.reader', '20 x 300 KB through a 4 KB buffer')
        c.close()
        c2.close()

    if want('bad'):
        base = conns()
        f0 = ival(info(), 'owner_tls_rec_fatal')
        good = Raw(OT)
        good.cmd('PING')

        def closed(c, tmo=5.0):
            c.s.settimeout(tmo)
            try:
                while True:
                    d = c.s.recv(65536)
                    if not d:
                        return True
            except socket.timeout:
                return False
            except OSError:
                return True

        cases = []
        c = Raw(OT)
        c.cmd('PING')
        rec = bytearray(c.record(enc('PING')))
        rec[-1] ^= 1
        c.s.sendall(bytes(rec))
        cases.append(('tag', closed(c)))
        c = Raw(OT + 1)
        c.cmd('PING')
        rec = bytearray(c.record(enc('PING')))
        rec[8] ^= 0x40
        c.s.sendall(bytes(rec))
        cases.append(('body', closed(c)))
        c = Raw(OT)
        c.cmd('PING')
        c.s.sendall(b'\x16\x03\x03\x00\x20' + os.urandom(32))
        cases.append(('outertype', closed(c)))
        c = Raw(OT)
        c.cmd('PING')
        rec = bytearray(c.record(enc('PING')))
        rec[2] = 0x01
        c.s.sendall(bytes(rec))
        cases.append(('version', closed(c)))
        c = Raw(OT)
        c.cmd('PING')
        c.s.sendall(b'\x17\x03\x03\x48\x00' + os.urandom(64))
        cases.append(('oversize', closed(c)))
        c = Raw(OT)
        c.cmd('PING')
        c.s.sendall(b'\x17\x03\x03\x00\x05' + os.urandom(5))
        cases.append(('short', closed(c)))
        c = Raw(OT)
        c.cmd('PING')
        rec = c.record(enc('PING'))
        c.s.sendall(rec[:len(rec) // 2])
        c.s.close()
        cases.append(('dropmid', True))
        c = Raw(OT + 2)
        c.cmd('PING')
        rec = c.record(enc('SET', 'x', 'y' * 3000))
        c.s.sendall(rec[:100])
        try:
            c.s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        except OSError:
            pass
        c.s.close()
        cases.append(('rstmid', True))
        time.sleep(1.0)
        f1 = ival(info(), 'owner_tls_rec_fatal')
        alive = good.cmd('PING') == b'PONG'
        after = conns()
        res(all(x for _, x in cases), 'bad.closed', ' '.join('%s=%s' % c for c in cases))
        res(f1 - f0 >= 6, 'bad.fatal_alerts', 'owner_tls_rec_fatal +%d' % (f1 - f0))
        res(alive and after == base + 1, 'bad.others_ok', 'good client PONG %s, clients %d -> %d' % (alive, base, after))
        good.close()

    if want('ku'):
        k0 = info()
        script = "printf 'PING\\r\\n'; sleep 0.3; printf 'k\\n'; sleep 0.3; printf 'PING\\r\\n'; sleep 0.3; " \
                 "printf 'K\\n'; sleep 0.3; printf 'PING\\r\\n'; sleep 0.3; printf 'K\\n'; sleep 0.3; printf 'SET kuk v1\\r\\n'; " \
                 "sleep 0.3; printf 'GET kuk\\r\\n'; sleep 0.5"
        out = subprocess.run(['bash', '-c', '(%s) | timeout 20 openssl s_client -connect 127.0.0.1:%d -CAfile %s -tls1_3 '
                              '2>&1' % (script, OT, CA)], capture_output=True, text=True).stdout
        k1 = info()
        pongs = out.count('+PONG')
        kin = ival(k1, 'owner_tls_rec_keyupdates_in') - ival(k0, 'owner_tls_rec_keyupdates_in')
        kout = ival(k1, 'owner_tls_rec_keyupdates_out') - ival(k0, 'owner_tls_rec_keyupdates_out')
        res(pongs == 3 and '+OK' in out and 'v1' in out and out.count('KEYUPDATE') >= 3, 's_client.keyupdate',
            'PONG %d, KEYUPDATE lines %d' % (pongs, out.count('KEYUPDATE')))
        res(kin >= 3 and kout >= 2, 'ku.server', 'KeyUpdates read %d, sent %d (2 requested)' % (kin, kout))

    if want('suites'):
        r0 = ival(info(), 'owner_tls_rec_conns')
        allok = []
        for name, extra in [('aes128', '-tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256'),
                            ('aes256', '-tls1_3 -ciphersuites TLS_AES_256_GCM_SHA384'),
                            ('chacha', '-tls1_3 -ciphersuites TLS_CHACHA20_POLY1305_SHA256'),
                            ('tls12', '-tls1_2')]:
            script = "for i in 1 2 3 4 5; do printf 'PING\\r\\n'; sleep 0.15; done; printf 'SET su%s 1\\r\\n'; sleep 0.3" % name
            out = subprocess.run(['bash', '-c', '(%s) | timeout 20 openssl s_client -connect 127.0.0.1:%d -CAfile %s %s '
                                  '2>&1' % (script, OT + 1, CA, extra)], capture_output=True, text=True).stdout
            cipher = re.findall(r'Cipher is (\S+)', out)
            allok.append((name, out.count('+PONG') == 5 and '+OK' in out, cipher[:1]))
        r1 = ival(info(), 'owner_tls_rec_conns')
        res(all(x[1] for x in allok), 'suites.interop', ' '.join('%s=%s%s' % x for x in allok))
        res(r1 - r0 == 3, 'suites.switched', 'TLS 1.3 connections on the record path +%d (want 3; TLS 1.2 stays on OpenSSL)' % (r1 - r0))

    if want('cli'):
        ok = True
        for port in (OT, OT + 3, P + 1):
            base = [a.cli, '--tls', '--cacert', CA, '-p', str(port)]
            o1 = subprocess.run(base + ['SET', 'clik%d' % port, 'hello'], capture_output=True, text=True, timeout=10).stdout
            o2 = subprocess.run(base + ['GET', 'clik%d' % port], capture_output=True, text=True, timeout=10).stdout
            o3 = subprocess.run(base + ['-r', '50', 'INCR', 'clictr%d' % port], capture_output=True, text=True, timeout=20).stdout
            ok = ok and o1.strip() == 'OK' and o2.strip() == 'hello' and o3.split()[-1:] == ['50']
        res(ok, 'cli.tls', 'valkey-cli --tls SET / GET / 50 x INCR on owner TLS ports and the main TLS port')

    s = info()
    res(ival(s, 'owner_tls_rec_mismatch') == 0, 'stats.mismatch', 'owner_tls_rec_mismatch=%s' % s.get('owner_tls_rec_mismatch'))
    if a.verify:
        res(ival(s, 'owner_tls_rec_verified') > 1000, 'stats.verified', 'owner_tls_rec_verified=%s' % s.get('owner_tls_rec_verified'))
    res(ival(s, 'owner_tls_rec_conns') > 0, 'stats.switched', ' '.join('%s=%s' % (k, v) for k, v in s.items() if 'tls_rec' in k))
    res(srv.poll() is None, 'server.alive')
finally:
    log = open(os.path.join(d, 'log'), errors='replace').read() if os.path.exists(os.path.join(d, 'log')) else ''
    bads = [l for l in log.splitlines() if re.search(r'ASSERTION|crashed|Guru|differs from OpenSSL', l)]
    res(not bads, 'log.clean', '; '.join(bads[:3]))
    try:
        subprocess.run([a.cli, '-p', str(P), 'shutdown', 'nosave'], capture_output=True, timeout=10)
    except Exception:
        pass
    try:
        srv.wait(10)
    except Exception:
        srv.kill()
print('TOTAL PASS=%d FAIL=%d' % (0, len(FAILS)) if False else 'DONE fails=%d %s' % (len(FAILS), ','.join(FAILS)))
sys.exit(1 if FAILS else 0)
