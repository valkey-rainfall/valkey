#!/usr/bin/env python3
"""RL-g3i-accept-storm checker: CLUSTER SLOTS on the owners, the fd table presized at startup, and closes batched per
stop, against the parent (--ref-bin, RL-2) where a reply can differ only by the run id.

Starts this build (and the parent, for the reply checks) with --n owners, owner ports and owner TLS ports, protected
mode off (so RL-g2v's owners finish TLS accepts), on 127.0.0.1 only. Checks:
  slots      CLUSTER SLOTS on every owner port (plain and TLS, RESP2 and RESP3) equals the reply main gives on its own
             port (plain and TLS), and the parent's with the run id and ports mapped; it is counted as one native
             string-family command per call (owner_string_commands) and in commandstats (cmdstat_cluster|slots)
  multi      MULTI / CLUSTER SLOTS / EXEC on an owner port: main queues it, the EXEC reply holds the same map
  acl        a user restricted to +cluster|slots +ping (not unrestricted): its CLUSTER SLOTS runs on main, same reply
  announce   CONFIG SET owner-announce-ip 10.11.12.13: the owners' CLUSTER SLOTS announce it at once; reset
  arity      CLUSTER SLOTS x: the same error as the parent; CLUSTER SHARDS / NODES / MYID / INFO unchanged
  presize    the server log says the fd table was presized for maxclients + 32 fds
  storm      --storm plain connections (one second, CLUSTER SLOTS each, then all close) and as many TLS ones:
             every client gets the map; connected_clients goes back; a PING loop on an existing owner client keeps
             answering (its max RTT is printed); owner_close_defers is printed (INFO)
  maxclients CONFIG SET maxclients to the clients connected + 5, then 12 new owner-port clients: 5 get in, 7 get
             "-ERR max number of clients reached", as on the parent
Prints PASS / FAIL lines and RLSTORMCHECK OK / FAILED."""
import argparse, asyncio, os, re, socket, ssl, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, default=20600)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--cpus', default='12-15')
ap.add_argument('--main-cpu', default='16')
ap.add_argument('--bg-cpu', default='17')
ap.add_argument('--tls-dir', required=True)
ap.add_argument('--storm', type=int, default=3000)
a = ap.parse_args()
fails = []


def res(ok, name, msg=''):
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)
    if not ok:
        fails.append(name)


class Conn:
    def __init__(self, port, tls=False):
        s = socket.create_connection(('127.0.0.1', port), timeout=20)
        if tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            s = ctx.wrap_socket(s)
        self.s = s
        self.f = s.makefile('rb')

    def raw(self, *args):
        self.s.sendall(('*%d\r\n' % len(args) + ''.join('$%d\r\n%s\r\n' % (len(str(x)), x) for x in args)).encode())
        return self.read_raw()

    def read_raw(self):
        line = self.f.readline()
        t = line[:1]
        out = line
        if t in (b'$', b'=', b'!'):
            n = int(line[1:-2])
            if n >= 0:
                out += self.f.read(n + 2)
        elif t in (b'*', b'~', b'>', b'%', b'|'):
            n = int(line[1:-2])
            if t in (b'%', b'|'):
                n *= 2
            for _ in range(max(n, 0)):
                out += self.read_raw()
        return out

    def close(self):
        try:
            self.f.close()
            self.s.close()
        except OSError:
            pass


def start(binpath, port, tag, extra=()):
    d = os.path.join(a.dir, tag)
    os.makedirs(d, exist_ok=True)
    td = a.tls_dir
    cmd = ['taskset', '-c', '%s,%s,%s' % (a.cpus, a.main_cpu, a.bg_cpu), binpath, '--port', str(port), '--bind', '127.0.0.1',
           '--tls-port', str(port + 1), '--tls-cert-file', td + '/server.crt', '--tls-key-file', td + '/server.key',
           '--tls-ca-cert-file', td + '/ca.crt', '--tls-auth-clients', 'no', '--protected-mode', 'no',
           '--owner-threads', str(a.n), '--owner-port', str(port + 10), '--owner-tls-port', str(port + 30),
           '--owner-cpulist', a.cpus, '--server-cpulist', a.main_cpu, '--bio-cpulist', a.bg_cpu, '--save', '',
           '--appendonly', 'no', '--maxclients', '20000', '--dir', d, '--logfile', d + '/log', '--daemonize', 'yes']
    cmd += list(extra)
    subprocess.run(cmd, check=True)
    for _ in range(100):
        try:
            c = Conn(port)
            if c.raw('PING') == b'+PONG\r\n':
                c.close()
                return d
        except OSError:
            pass
        time.sleep(0.1)
    raise SystemExit('cannot start ' + tag)


def stop(port):
    try:
        c = Conn(port)
        c.s.sendall(b'*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n')
        time.sleep(0.3)
        c.close()
    except OSError:
        pass
    for _ in range(100):
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            time.sleep(0.1)
        except OSError:
            return


def info(port, sect='everything'):
    c = Conn(port)
    r = c.raw('INFO', sect).decode()
    c.close()
    d = {}
    for line in r.split('\r\n'):
        if ':' in line and not line.startswith('#'):
            k, v = line.split(':', 1)
            d[k] = v
    return d


def norm(reply, base, runid):
    """Ports to owner index and the run id out, so two servers' maps compare."""
    s = reply.replace(runid[:32].encode(), b'RUNID')  # node ids: the run id's first 32 characters + the owner index
    return re.sub(rb':(\d+)\r\n', lambda m: b':P%d\r\n' % (int(m.group(1)) - base)
                  if 0 <= int(m.group(1)) - base < 64 else m.group(0), s)


P, R = a.port, a.port + 100
os.makedirs(a.dir, exist_ok=True)
dA = start(a.bin, P, 'build')
dR = start(a.ref_bin, R, 'ref')
idA, idR = info(P)['run_id'], info(R)['run_id']
try:
    # slots
    for resp in (2, 3):
        for tls in (False, True):
            mainp = P + 1 if tls else P
            m = Conn(mainp, tls)
            if resp == 3:
                m.raw('HELLO', '3')
            want = m.raw('CLUSTER', 'SLOTS')
            m.close()
            rr = Conn(R + 1 if tls else R, tls)
            if resp == 3:
                rr.raw('HELLO', '3')
            ref = rr.raw('CLUSTER', 'SLOTS')
            rr.close()
            base_a, base_r = (P + 30, R + 30) if tls else (P + 10, R + 10)
            ok, bad = True, ''
            s0 = int(info(P)['owner_string_commands'])
            c0 = info(P, 'commandstats').get('cmdstat_cluster|slots', 'calls=0').split(',')[0].split('=')[1]
            for i in range(a.n):
                op = (P + 30 if tls else P + 10) + i
                c = Conn(op, tls)
                if resp == 3:
                    c.raw('HELLO', '3')
                got = [c.raw('CLUSTER', 'SLOTS') for _ in range(3)]
                c.close()
                if any(g != want for g in got) or norm(got[0], base_a, idA) != norm(ref, base_r, idR):
                    ok, bad = False, 'port %d: %r vs main %r / ref %r' % (op, got[0][:120], want[:120], ref[:120])
            s1 = int(info(P)['owner_string_commands'])
            c1 = info(P, 'commandstats').get('cmdstat_cluster|slots', 'calls=0').split(',')[0].split('=')[1]
            res(ok, 'slots.resp%d.%s' % (resp, 'tls' if tls else 'plain'), bad)
            res(s1 - s0 >= 3 * a.n and int(c1) - int(c0) >= 3 * a.n, 'slots.counted.resp%d.%s' % (resp, 'tls' if tls else 'plain'),
                'owner_string_commands +%d, cmdstat calls +%d (want >= %d each)' % (s1 - s0, int(c1) - int(c0), 3 * a.n))
    # multi
    m = Conn(P)
    want = m.raw('CLUSTER', 'SLOTS')
    m.close()
    c = Conn(P + 10)
    r1, r2 = c.raw('MULTI'), c.raw('CLUSTER', 'SLOTS')
    ex = c.raw('EXEC')
    c.close()
    res(r1 == b'+OK\r\n' and r2 == b'+QUEUED\r\n' and ex == b'*1\r\n' + want, 'multi', '%r %r %r' % (r1, r2, ex[:80]))
    # acl
    m = Conn(P)
    m.raw('ACL', 'SETUSER', 'cs', 'on', '>pw', '-@all', '+cluster|slots', '+ping')
    m.close()
    c = Conn(P + 11)
    au = c.raw('AUTH', 'cs', 'pw')
    g = c.raw('CLUSTER', 'SLOTS')
    deny = c.raw('GET', 'x')
    c.close()
    res(au == b'+OK\r\n' and g == want and deny.startswith(b'-NOPERM'), 'acl', '%r %r %r' % (au, g[:60], deny[:60]))
    # announce
    m = Conn(P)
    m.raw('CONFIG', 'SET', 'owner-announce-ip', '10.11.12.13')
    c = Conn(P + 12)
    g = c.raw('CLUSTER', 'SLOTS')
    c.close()
    m.raw('CONFIG', 'SET', 'owner-announce-ip', '')
    c = Conn(P + 12)
    g2 = c.raw('CLUSTER', 'SLOTS')
    c.close()
    m.close()
    res(g.count(b'10.11.12.13') == a.n and g2 == want, 'announce', '%d mentions, reset %s' % (g.count(b'10.11.12.13'), g2 == want))
    # arity and the other subcommands (main runs them; run id and port numbers masked)
    ok, bad = True, ''
    for args in (('CLUSTER', 'SLOTS', 'x'), ('CLUSTER', 'SHARDS'), ('CLUSTER', 'NODES'), ('CLUSTER', 'MYID'),
                 ('CLUSTER', 'INFO'), ('CLUSTER', 'KEYSLOT', 'foo'), ('CLUSTER', 'NOSUCH')):
        c, r = Conn(P + 13), Conn(R + 13)
        x = re.sub(rb'\d{4,5}', b'N', c.raw(*args).replace(idA[:32].encode(), b'RUNID'))
        y = re.sub(rb'\d{4,5}', b'N', r.raw(*args).replace(idR[:32].encode(), b'RUNID'))
        c.close()
        r.close()
        if x != y:
            ok, bad = False, '%s: %r vs %r' % (' '.join(args), x[:160], y[:160])
    res(ok, 'arity.subcommands', bad)
    # presize
    log = open(os.path.join(dA, 'log')).read()
    res('fd table presized for 20032 fds' in log, 'presize', re.findall(r'fd table [^\n]*', log)[:1])
    # storm (plain then TLS), with a PING loop on an established owner client
    for tls in (False, True):
        before = int(info(P)['connected_clients'])
        hs0 = int(info(P).get('owner_close_defers', 0))
        stop_ev, rtts = threading.Event(), []

        def pinger():
            c = Conn(P + 10)
            while not stop_ev.is_set():
                t0 = time.perf_counter()
                r = c.raw('PING')
                rtts.append((time.perf_counter() - t0) * 1000)
                if r != b'+PONG\r\n':
                    rtts.append(-1)
                time.sleep(0.002)
            c.close()

        th = threading.Thread(target=pinger)
        th.start()

        async def one(i, ctx, out):
            await asyncio.sleep(i / a.storm)
            try:
                rd, wr = await asyncio.wait_for(asyncio.open_connection('127.0.0.1', (P + 30 if tls else P + 10) + i % a.n,
                                                                        ssl=ctx), 60)
                wr.write(b'*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n')
                data = b''
                while len(data) < len(want_r):
                    chunk = await asyncio.wait_for(rd.read(65536), 60)
                    if not chunk:
                        break
                    data += chunk
                out.append((wr, data))
            except Exception as e:  # noqa: BLE001
                out.append((None, repr(e).encode()))

        async def storm():
            ctx = None
            if tls:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            out = []
            await asyncio.gather(*(one(i, ctx, out) for i in range(a.storm)))
            mid = int(info(P)['connected_clients'])
            for w, _ in out:
                if w:
                    w.close()
            return out, mid

        m = Conn(P + 1, True) if tls else Conn(P)
        want_r = m.raw('CLUSTER', 'SLOTS')
        m.close()
        import resource
        resource.setrlimit(resource.RLIMIT_NOFILE, (resource.getrlimit(resource.RLIMIT_NOFILE)[1],) * 2)
        out, mid = asyncio.run(storm())
        time.sleep(1.0)
        stop_ev.set()
        th.join()
        good = sum(1 for w, d in out if w and d == want_r)
        for _ in range(30):
            after = int(info(P)['connected_clients'])
            if after == before:
                break
            time.sleep(0.1)
        hs1 = int(info(P).get('owner_close_defers', 0))
        kind = 'tls' if tls else 'plain'
        res(good == a.storm, 'storm.%s.replies' % kind, '%d of %d clients got the map; first bad %r' % (
            good, a.storm, next((d[:80] for w, d in out if not w or d != want_r), b'')))
        res(mid >= before + a.storm and after == before, 'storm.%s.clients' % kind,
            'connected %d -> %d during -> %d after' % (before, mid, after))
        res(-1 not in rtts and len(rtts) > 10, 'storm.%s.pinger' % kind,
            '%d PINGs, max RTT %.1f ms, p99 %.1f ms; owner_close_defers +%d' % (
                len(rtts), max(rtts or [0]), sorted(rtts)[int(0.99 * len(rtts))] if rtts else 0, hs1 - hs0))
    # maxclients: the same admission as the parent (main refuses over the limit, the owners' clients included)
    seen = {}
    for port, tag in ((P, 'build'), (R, 'ref')):
        m = Conn(port)
        time.sleep(0.3)
        cc = int(info(port)['connected_clients']) - 1  # the INFO connection itself
        m.raw('CONFIG', 'SET', 'maxclients', str(cc + 5))
        cs, errs, okc = [], 0, 0
        for i in range(12):
            try:
                c = Conn(port + 10 + i % a.n)
                cs.append(c)
                r = c.raw('PING')
            except OSError as e:
                r = repr(e).encode()
            if r == b'+PONG\r\n':
                okc += 1
            elif b'max number of clients' in r:
                errs += 1
        for c in cs:
            c.close()
        time.sleep(0.3)
        m.raw('CONFIG', 'SET', 'maxclients', '20000')
        m.close()
        seen[tag] = (okc, errs)
    res(seen['build'] == seen['ref'] and seen['build'][1] > 0 and sum(seen['build']) == 12, 'maxclients',
        'build in/refused %s, parent %s' % (seen['build'], seen['ref']))
finally:
    stop(P)
    stop(R)
print('RLSTORMCHECK %s fails=%d %s' % ('OK' if not fails else 'FAILED', len(fails), ' '.join(fails)))
sys.exit(1 if fails else 0)
