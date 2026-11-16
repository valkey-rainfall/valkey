#!/usr/bin/env python3
"""RL-g3c error-queue check: OpenSSL errors left on an owner thread's queue must not leak into other connections.
One server (owners 4, owner TLS ports), loopback. On each owner TLS port (and the main TLS port), good TLS clients
pipeline SET/GET/INCR (replies checked against a model) while bad clients on the same port, after a good handshake
and a PING, write garbage TLS records straight to the socket (bad record MAC: SSL_read fails with SSL_ERROR_SSL and
leaves errors on that owner thread's queue), or a fatal alert, or drop mid-record. Expect: every good reply right,
every bad client closed by the server, the server alive, clients back to the baseline, decrypt errors in the log
(so the queue really was non-empty), 0 crash lines. Prints PASS/FAIL; exit 1 on any FAIL."""
import argparse, os, random, socket, ssl, struct, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, default=24300)   # plain; +1 TLS; owners +20..; owner TLS +40..
ap.add_argument('--cpus', default='50-53')
ap.add_argument('--main-cpu', default='54')
ap.add_argument('--tls-dir', default='/home/ec2-user/ns-tls')
ap.add_argument('--rounds', type=int, default=40)
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
P, N = a.port, 4
FAILS = []


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


def tconn(port):
    s = socket.create_connection(('127.0.0.1', port), timeout=20)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.load_verify_locations(os.path.join(a.tls_dir, 'ca.crt'))
    s = ctx.wrap_socket(s, server_hostname='127.0.0.1')
    return s, s.makefile('rb')


def rd(f):
    l = f.readline()
    if not l:
        raise EOFError
    if l[:1] == b'$':
        n = int(l[1:])
        return None if n < 0 else f.read(n + 2)[:-2]
    if l[:1] == b':':
        return int(l[1:])
    return l[1:-2].decode()


d = os.path.join(a.dir, 'srv')
os.makedirs(d, exist_ok=True)
T = a.tls_dir
args = ['taskset', '-c', '%s,%s' % (a.cpus, a.main_cpu), a.bin, '--port', str(P), '--tls-port', str(P + 1), '--bind',
        '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', d, '--logfile', os.path.join(d, 'log'),
        '--daemonize', 'no', '--io-threads', '1', '--owner-threads', str(N), '--owner-port', str(P + 20),
        '--owner-tls-port', str(P + 40), '--owner-cpulist', a.cpus, '--server-cpulist', a.main_cpu, '--bio-cpulist',
        a.main_cpu, '--tls-cert-file', T + '/server.crt', '--tls-key-file', T + '/server.key', '--tls-ca-cert-file',
        T + '/ca.crt', '--tls-auth-clients', 'no', '--loglevel', 'verbose']
srv = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for _ in range(200):
    try:
        c0 = socket.create_connection(('127.0.0.1', P), timeout=5)
        c0.sendall(enc('PING'))
        if c0.recv(64).startswith(b'+PONG'):
            break
    except Exception:
        time.sleep(0.05)
c0f = c0.makefile('rb')


def info(sec):
    c0.sendall(enc('INFO', sec))
    l = c0f.readline()
    body = c0f.read(int(l[1:]) + 2).decode()
    return dict(x.split(':', 1) for x in body.split('\r\n') if ':' in x)


base = int(info('clients')['connected_clients'])
ports = [P + 40 + i for i in range(N)] + [P + 1]
stop = threading.Event()
good_res = {}
bad_res = {}


def good(idx, port):
    r = random.Random(idx)
    model = {}
    nbad = nops = 0
    try:
        s, f = tconn(port)
        while not stop.is_set() or nops < 2000:
            batch, want = [], []
            for _ in range(r.randrange(1, 12)):
                k = 'g%d:%d' % (idx, r.randrange(20))
                op = r.random()
                if op < 0.4:
                    v = os.urandom(r.randrange(1, 400)).hex()
                    batch.append(enc('SET', k, v)); want.append('OK'); model[k] = v.encode()
                elif op < 0.8:
                    batch.append(enc('GET', k)); want.append(model.get(k))
                else:
                    kk = 'c%d' % idx
                    model[kk] = int(model.get(kk, 0)) + 1
                    batch.append(enc('INCR', kk)); want.append(model[kk])
            s.sendall(b''.join(batch))
            for w in want:
                if rd(f) != w:
                    nbad += 1
                nops += 1
            if nops > 200000:
                break
        s.close()
        good_res[idx] = (nops, nbad, '')
    except Exception as e:
        good_res[idx] = (nops, nbad, repr(e))


def bad(idx, port):
    r = random.Random(1000 + idx)
    closed = errs = 0
    for q in range(a.rounds):
        try:
            s, f = tconn(port)
            s.sendall(enc('PING'))
            if rd(f) != 'PONG':
                errs += 1
            kind = q % 3
            fd = s.fileno()
            if kind == 0:   # an application-data record with a garbage body: bad record MAC
                body = os.urandom(r.randrange(24, 300))
                os.write(fd, b'\x17\x03\x03' + struct.pack('>H', len(body)) + body)
            elif kind == 1:  # garbage that is not a record at all
                os.write(fd, os.urandom(r.randrange(5, 64)))
            else:            # half a record, then the connection drops
                os.write(fd, b'\x17\x03\x03\x01\x00' + os.urandom(40))
            if kind != 2:
                try:   # the server must close it
                    s.settimeout(5)
                    x = s.recv(64)
                    closed += 1 if not x else 0
                except Exception:
                    closed += 1
            s.close()
        except Exception as e:
            errs += 1
    bad_res[idx] = (closed, errs)


gt = [threading.Thread(target=good, args=(i, ports[i % len(ports)])) for i in range(10)]
bt = [threading.Thread(target=bad, args=(i, ports[i % len(ports)])) for i in range(10)]
for t in gt + bt:
    t.start()
for t in bt:
    t.join()
stop.set()
for t in gt:
    t.join()
tot = sum(v[0] for v in good_res.values())
wrong = sum(v[1] for v in good_res.values())
gerr = [v[2] for v in good_res.values() if v[2]]
res(wrong == 0 and not gerr and len(good_res) == 10, 'errq.good_clients',
    '%d replies on 5 ports next to the bad clients, %d wrong, errors %s' % (tot, wrong, gerr[:2]))
want_closed = sum(1 for q in range(a.rounds) if q % 3 != 2) * 10
closed = sum(v[0] for v in bad_res.values())
berr = sum(v[1] for v in bad_res.values())
res(closed == want_closed and berr == 0, 'errq.bad_clients_closed', '%d of %d garbage clients closed by the server, %d '
    'client-side errors' % (closed, want_closed, berr))
cc = -1
for _ in range(100):
    cc = int(info('clients')['connected_clients'])
    if cc <= base:
        break
    time.sleep(0.1)
res(cc <= base, 'errq.clients_cleaned', 'connected_clients %d -> %d' % (base, cc))
s, f = tconn(P + 40)
s.sendall(enc('PING'))
alive = rd(f) == 'PONG'
s.close()
c0.sendall(enc('SHUTDOWN', 'NOSAVE'))
try:
    srv.wait(15)
except Exception:
    srv.kill()
logt = open(os.path.join(d, 'log'), errors='replace').read()
dec = sum(logt.count(x) for x in ('decryption failed', 'bad record mac', 'wrong version number', 'record layer failure',
                                   'packet length too long', 'unexpected eof'))
crash = sum(logt.count(x) for x in ('ASSERT', 'crashed', 'Guru', 'BUG REPORT'))
res(alive and dec > 0, 'errq.server_saw_errors', 'alive %s; OpenSSL error lines in the log %d' % (alive, dec))
res(crash == 0, 'crash_lines', '%d' % crash)
print('ERRQCHECK %s (%d FAIL)' % ('PASS' if not FAILS else 'FAIL', len(FAILS)), flush=True)
sys.exit(1 if FAILS else 0)
