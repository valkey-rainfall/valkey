#!/usr/bin/env python3
"""RL-g2v-tls-tail gene check: TLS accepts an owner finishes itself (owner-tls-local-accept) vs the parent RL-1, where
main finishes every one. Each server: owner-threads N, owner TLS ports, protected-mode no, 127.0.0.1 only.
  storm      512 TLS connections at once over the owner TLS ports (32 threads), each sends SET + GET right after its
             handshake: replies, owner_tls_local_accepts (+512 on the child, +0 on the parent), total_connections_received
             (+512 on both), connected_clients up by 512 and back after close
  off        CONFIG SET owner-tls-local-accept no: accepts go through main again (counter flat), replies equal
  protected  protected-mode yes, default user without password: main's path (counter flat), local client accepted
  auth       requirepass: the owner accepts (+n), the first command gets NOAUTH, AUTH then GET work (both binaries)
  certuser   tls-auth-clients optional + tls-auth-clients-user CN and a client cert whose CN is an ACL user: main's path
             (counter flat), ACL WHOAMI = that user (both binaries)
  abuse      plain bytes to a TLS port, connect-and-close, handshake-then-close, TLS close mid-request: server alive,
             connected_clients back to the base, no crash lines
  kill       CLIENT KILL ID of locally accepted clients: their sockets see EOF, others still served
Prints PASS/FAIL lines and TLSACCEPTCHECK pass= fail=.
Usage: tls-accept-check.py --bin B --ref-bin R --dir D --port P --owner-port OP --owner-tls-port OTP --tls-port TP
       --tls-dir T --n 4 --cpus 52-55 --main-cpu 56 --bg-cpu 57"""
import argparse, os, socket, ssl, subprocess, threading, time

ap = argparse.ArgumentParser()
for k in ('bin', 'ref-bin', 'dir', 'tls-dir', 'cpus'):
    ap.add_argument('--' + k, required=True)
for k in ('port', 'owner-port', 'owner-tls-port', 'tls-port', 'n', 'main-cpu', 'bg-cpu'):
    ap.add_argument('--' + k, type=int, required=True)
a = ap.parse_args()
P = []


def res(ok, name, msg=''):
    P.append(ok)
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)


def enc(*args):
    out = [b'*%d\r\n' % len(args)]
    for x in args:
        x = x.encode() if isinstance(x, str) else x
        out.append(b'$%d\r\n%s\r\n' % (len(x), x))
    return b''.join(out)


def readreply(s, buf=b''):
    """One RESP reply (simple, error, integer, bulk, flat array of bulks); returns (value, rest)."""
    def need(b, n):
        while len(b) < n:
            x = s.recv(65536)
            if not x:
                raise EOFError
            b += x
        return b

    def line(b, i):
        while True:
            j = b.find(b'\r\n', i)
            if j >= 0:
                return b, j
            x = s.recv(65536)
            if not x:
                raise EOFError
            b += x

    def one(b, i):
        b, j = line(b, i)
        t, body = b[i:i + 1], b[i + 1:j]
        if t in (b'+', b'-', b':'):
            return b, j + 2, (t + body).decode()
        n = int(body)
        if t == b'$':
            if n < 0:
                return b, j + 2, None
            b = need(b, j + 2 + n + 2)
            return b, j + 2 + n + 2, b[j + 2:j + 2 + n].decode(errors='replace')
        k, out = j + 2, []
        for _ in range(max(n, 0)):
            b, k, v = one(b, k)
            out.append(v)
        return b, k, out
    buf, k, v = one(buf, 0)
    return v, buf[k:]


class Srv:
    def __init__(s, binp, tag, extra=()):
        s.own, s.pw = 0, ''
        s.d = os.path.join(a.dir, tag)
        os.makedirs(s.d, exist_ok=True)
        T = a.tls_dir
        cmd = ['taskset', '-c', '%s,%d,%d' % (a.cpus, a.main_cpu, a.bg_cpu), binp, '--port', str(a.port), '--bind',
               '127.0.0.1', '--protected-mode', 'no', '--save', '', '--appendonly', 'no', '--owner-threads', str(a.n),
               '--owner-port', str(a.owner_port), '--owner-cpulist', a.cpus, '--server-cpulist', str(a.main_cpu),
               '--bio-cpulist', str(a.bg_cpu), '--tls-port', str(a.tls_port), '--owner-tls-port', str(a.owner_tls_port),
               '--tls-cert-file', T + '/server.crt', '--tls-key-file', T + '/server.key', '--tls-ca-cert-file',
               T + '/ca.crt', '--tls-auth-clients', 'no', '--tls-replication', 'no', '--maxclients', '10000',
               '--daemonize', 'yes', '--dir', s.d, '--logfile', s.d + '/log', '--enable-debug-command', 'yes'] + list(extra)
        subprocess.run(cmd, check=True)
        for _ in range(100):
            try:
                s.cmd('PING')
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError('server did not start: ' + tag)

    def cmd(s, *args):
        s.own += 1
        c = socket.create_connection(('127.0.0.1', a.port), timeout=10)
        if s.pw:
            c.sendall(enc('AUTH', s.pw))
            readreply(c)
        c.sendall(enc(*args))
        v, _ = readreply(c)
        c.close()
        return v

    def info(s, sec):
        d = {}
        for ln in s.cmd('INFO', sec).replace('\r', '').split('\n'):
            if ':' in ln and not ln.startswith('#'):
                k, v = ln.split(':', 1)
                d[k] = v
        return d

    def num(s, sec, k):
        return int(s.info(sec).get(k, '-1'))

    def stop(s):
        try:
            s.cmd('SHUTDOWN', 'NOSAVE')
        except Exception:
            pass
        for _ in range(100):
            try:
                socket.create_connection(('127.0.0.1', a.port), timeout=0.2).close()
                time.sleep(0.1)
            except OSError:
                break

    def crash(s):
        try:
            t = open(s.d + '/log', errors='replace').read()
        except OSError:
            return -1
        return sum(t.count(x) for x in ('ASSERT', 'crashed', 'Guru', 'BUG REPORT'))


def ctx(cert=None):
    c = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    if cert:
        c.load_cert_chain(a.tls_dir + '/' + cert + '.crt', a.tls_dir + '/' + cert + '.key')
    return c


def tconn(i, c=None):
    s = socket.create_connection(('127.0.0.1', a.owner_tls_port + i % a.n), timeout=20)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return (c or ctx()).wrap_socket(s)


def storm(n, threads=32, first=None):
    """n TLS connections, each sends SET k v + GET k (or `first`) right after its handshake; returns (sockets, bad)."""
    socks, bad, lock = [], [], threading.Lock()

    def work(t):
        c = ctx()
        for j in range(t, n, threads):
            try:
                s = tconn(j, c)
                k = 'g2v:%d' % j
                if first:
                    s.sendall(enc(*first))
                    v, _ = readreply(s)
                    got = [v]
                else:
                    s.sendall(enc('SET', k, 'v%d' % j) + enc('GET', k))
                    v1, rest = readreply(s)
                    v2, _ = readreply(s, rest)
                    got = [v1, v2]
                    if got != ['+OK', 'v%d' % j]:
                        raise ValueError(repr(got))
                with lock:
                    socks.append((s, got))
            except Exception as e:  # noqa
                with lock:
                    bad.append('%d: %r' % (j, e))
    ts = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return socks, bad


def close_all(socks):
    for s, _ in socks:
        try:
            s.close()
        except Exception:
            pass


def settle(srv, base, secs=8):
    for _ in range(int(secs * 10)):
        cc = srv.num('clients', 'connected_clients')
        if cc == base:
            return cc
        time.sleep(0.1)
    return srv.num('clients', 'connected_clients')


def run(binp, tag, child):
    srv = Srv(binp, tag)
    la = lambda: srv.num('owners', 'owner_tls_local_accepts') if child else 0  # noqa
    try:
        if child:
            res(srv.cmd('CONFIG', 'GET', 'owner-tls-local-accept') == ['owner-tls-local-accept', 'yes'],
                tag + '.default_on')
        base = srv.num('clients', 'connected_clients')
        tc0, la0 = srv.num('stats', 'total_connections_received'), la()
        own0 = srv.own
        socks, bad = storm(512)
        cc = srv.num('clients', 'connected_clients')
        tc1, la1 = srv.num('stats', 'total_connections_received'), la()
        mine = srv.own - own0  # this checker's own INFO connections in between (each counts as one)
        res(not bad and len(socks) == 512, tag + '.storm.replies', '%d ok, %d bad %s' % (len(socks), len(bad), bad[:3]))
        res(tc1 - tc0 == 512 + mine, tag + '.storm.total_connections_received', '+%d (512 + %d own)' % (tc1 - tc0, mine))
        res(cc - base == 512, tag + '.storm.connected_clients', '+%d' % (cc - base))
        res(la1 - la0 == (512 if child else 0), tag + '.storm.local_accepts', '+%d' % (la1 - la0))
        # every connection still works after the storm (second command on each)
        okc = 0
        for s, _ in socks:
            s.sendall(enc('PING'))
            v, _ = readreply(s)
            okc += v == '+PONG'
        res(okc == len(socks), tag + '.storm.second_command', '%d/%d' % (okc, len(socks)))
        close_all(socks)
        res(settle(srv, base) == base, tag + '.storm.closed', 'connected_clients %d base %d' % (srv.num('clients', 'connected_clients'), base))

        if child:
            srv.cmd('CONFIG', 'SET', 'owner-tls-local-accept', 'no')
            la0, tc0 = la(), srv.num('stats', 'total_connections_received')
            own0 = srv.own
            socks, bad = storm(64)
            la1 = la()
            tcd = srv.num('stats', 'total_connections_received') - tc0 - (srv.own - own0)
            res(not bad and la1 == la0 and tcd == 64, tag + '.off', 'bad %d local +%d connections +%d' % (len(bad), la1 - la0, tcd))
            close_all(socks)
            srv.cmd('CONFIG', 'SET', 'owner-tls-local-accept', 'yes')
            settle(srv, base)

        srv.cmd('CONFIG', 'SET', 'protected-mode', 'yes')
        la0 = la()
        socks, bad = storm(32)
        res(not bad and la() == la0, tag + '.protected', 'bad %d %s local +%d' % (len(bad), bad[:2], la() - la0))
        close_all(socks)
        srv.cmd('CONFIG', 'SET', 'protected-mode', 'no')
        settle(srv, base)

        srv.cmd('CONFIG', 'SET', 'requirepass', 'g2vpw')
        srv.pw = 'g2vpw'
        la0 = la()
        socks, bad = storm(32, first=('GET', 'g2v:1'))
        noauth = sum(1 for s, g in socks if str(g[0]).startswith('-NOAUTH'))
        good = 0
        for s, _ in socks:
            s.sendall(enc('AUTH', 'g2vpw') + enc('SET', 'g2v:a', '1') + enc('GET', 'g2v:a'))
            v1, r = readreply(s)
            v2, r = readreply(s, r)
            v3, _ = readreply(s, r)
            good += [v1, v2, v3] == ['+OK', '+OK', '1']
        res(not bad and noauth == 32 and good == 32 and la() - la0 == (32 if child else 0), tag + '.auth',
            'noauth %d auth+cmds %d local +%d bad %s' % (noauth, good, la() - la0, bad[:2]))
        close_all(socks)
        srv.cmd('CONFIG', 'SET', 'requirepass', '')
        srv.pw = ''
        settle(srv, base)

        cn = subprocess.run(['openssl', 'x509', '-noout', '-subject', '-nameopt', 'multiline', '-in',
                             a.tls_dir + '/client.crt'], capture_output=True, text=True).stdout
        cn = [ln.split('=', 1)[1].strip() for ln in cn.split('\n') if 'commonName' in ln]
        if cn:
            u = cn[0]
            srv.cmd('ACL', 'SETUSER', u, 'on', 'nopass', '+@all', '~*')
            srv.cmd('CONFIG', 'SET', 'tls-auth-clients', 'optional')
            srv.cmd('CONFIG', 'SET', 'tls-auth-clients-user', 'CN')
            la0 = la()
            who = []
            for i in range(8):
                s = tconn(i, ctx('client'))
                s.sendall(enc('ACL', 'WHOAMI'))
                v, _ = readreply(s)
                who.append(v)
                s.close()
            s = tconn(0)
            s.sendall(enc('ACL', 'WHOAMI'))
            v0, _ = readreply(s)
            s.close()
            res(who == [u] * 8 and v0 == 'default' and la() == la0, tag + '.certuser',
                'whoami %s / no cert %s local +%d' % (who[:2], v0, la() - la0))
            srv.cmd('CONFIG', 'SET', 'tls-auth-clients-user', 'off')
            srv.cmd('CONFIG', 'SET', 'tls-auth-clients', 'no')
            srv.cmd('ACL', 'DELUSER', u)
        else:
            res(False, tag + '.certuser', 'no CN in client.crt')
        settle(srv, base)

        for i in range(40):
            s = socket.create_connection(('127.0.0.1', a.owner_tls_port + i % a.n))
            if i % 4 == 0:
                s.sendall(enc('PING'))  # plain bytes to a TLS port
            elif i % 4 == 1:
                pass  # connect and close
            elif i % 4 == 2:
                t = ctx().wrap_socket(s)
                t.close()  # handshake then close
                continue
            else:
                t = ctx().wrap_socket(s)
                t.sendall(enc('SET', 'g2v:big', 'x' * 100000)[:5000])  # close mid-request
                t.close()
                continue
            s.close()
        time.sleep(0.5)
        res(srv.cmd('PING') == '+PONG' and settle(srv, base) == base, tag + '.abuse',
            'connected_clients %d base %d' % (srv.num('clients', 'connected_clients'), base))

        socks, bad = storm(16)
        lst = srv.cmd('CLIENT', 'LIST')
        ids = []
        for ln in lst.split('\n'):
            f = dict(x.split('=', 1) for x in ln.split() if '=' in x)
            lp = int(f.get('laddr', ':0').rsplit(':', 1)[1] or 0)
            if a.owner_tls_port <= lp < a.owner_tls_port + a.n:
                ids.append(f['id'])
        killed = sum(int(srv.cmd('CLIENT', 'KILL', 'ID', i)[1:]) for i in ids[:8])
        eof = 0
        for s, _ in socks:
            try:
                s.sendall(enc('PING'))
                v, _ = readreply(s)
            except (EOFError, OSError, ssl.SSLError):
                eof += 1
        res(not bad and len(ids) == 16 and killed == 8 and eof == 8, tag + '.kill',
            'listed %d killed %d eof %d' % (len(ids), killed, eof))
        close_all(socks)
        res(settle(srv, base) == base, tag + '.final_clients')
    finally:
        srv.stop()
        res(srv.crash() == 0, tag + '.no_crash', 'crash lines %d' % srv.crash())


run(a.bin, 'child', True)
run(a.ref_bin, 'parent', False)
print('TLSACCEPTCHECK pass=%d fail=%d' % (sum(P), len(P) - sum(P)), flush=True)
