#!/usr/bin/env python3
"""RL-g2c-owner-cron check: the per-client cron semantics, run against a candidate and a reference binary (the parent,
whose main thread runs every client's cron with the owners stopped) started the same way, with the same steps:

  timeout     CONFIG SET timeout 2: idle clients on the owner ports, the main port and the owner TLS ports are closed
              after ~2 s; clients that keep talking (PING every 0.4 s) are not; timeout 0 drops nobody
  obuf        client-output-buffer-limit normal 0 256kb 1: a client that stops reading with ~6 MB of replies pending is
              closed by the cron once the soft limit has lasted 1 s (no more replies are appended); hard 1mb: closed at
              once; client_output_buffer_limit_disconnections counts both
  qbuf        client-query-buffer-limit 1mb: a client sending an unfinished 2 MB bulk is closed
  resize      a client whose query buffer grew to ~100 KB and that then idles 3 s gets it freed (CLIENT LIST qbuf=0)
  stats       a deterministic load (connections x fixed GET/SET/INCR/PING sequences): INFO clients / stats counters and
              CLIENT LIST equal between the two builds; mem_clients_normal and client_recent_max_input_buffer present
  mmclients   maxmemory-clients 4mb: a client holding a 6 MB unfinished bulk on an owner port is evicted
              (evicted_clients +1); the idle owner clients are not

  rl-cron-check.py --bin CAND --ref-bin PARENT --dir D --port P --owner-port OP --owner-tls-port TP --tls-dir T \
                   --n 4 --cpus 32-35 --main-cpu 36 [--bg-cpu 37]
Prints PASS/FAIL lines and RLCRONCHECK pass=N fail=M.
"""
import argparse, os, socket, ssl, subprocess, sys, time

A = None
RES = []


def say(ok, name, msg=''):
    RES.append(ok)
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)


def resp(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n%s\r\n' % (len(a), a)
    return out


class Conn:
    def __init__(self, port, tls=False, rcvbuf=None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if rcvbuf:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        s.settimeout(10)
        s.connect(('127.0.0.1', port))
        if tls:
            ctx = ssl.create_default_context(cafile=os.path.join(A.tls_dir, 'ca.crt'))
            ctx.check_hostname = False
            s = ctx.wrap_socket(s, server_hostname='127.0.0.1')
        self.s, self.buf = s, b''

    def send(self, data):
        self.s.sendall(data)

    def line(self):
        while b'\r\n' not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        l, self.buf = self.buf.split(b'\r\n', 1)
        return l

    def reply(self):
        l = self.line()
        t = l[:1]
        if t in (b'+', b'-', b':'):
            return l
        if t == b'$':
            n = int(l[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                d = self.s.recv(65536)
                if not d:
                    raise EOFError
                self.buf += d
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b'*':
            return [self.reply() for _ in range(int(l[1:]))]
        raise RuntimeError(l)

    def cmd(self, *args):
        self.send(resp(*args))
        return self.reply()

    def alive(self):
        try:
            return self.cmd('PING') == b'+PONG'
        except (OSError, EOFError, ssl.SSLError):
            return False

    def closed_by_server(self, wait=0.2):
        """True when the server closed this connection (EOF / reset), without sending anything."""
        self.s.settimeout(wait)
        try:
            d = self.s.recv(1 << 20)
            while d:
                d = self.s.recv(1 << 20)
            return True
        except socket.timeout:
            return False
        except (OSError, ssl.SSLError):
            return True
        finally:
            self.s.settimeout(10)

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def info(c, sec):
    out = {}
    for l in c.cmd('INFO', sec).decode().splitlines():
        if ':' in l:
            k, v = l.split(':', 1)
            out[k] = v
    return out


class Server:
    def __init__(self, binary, tag):
        self.bin, self.tag = binary, tag
        self.dir = os.path.join(A.dir, tag)
        os.makedirs(self.dir, exist_ok=True)
        t = A.tls_dir
        cmd = ['taskset', '-c', '%s,%d' % (A.cpus, A.main_cpu), binary, '--port', str(A.port), '--bind', '127.0.0.1',
               '--owner-threads', str(A.n), '--owner-port', str(A.owner_port), '--owner-cpulist', A.cpus,
               '--server-cpulist', str(A.main_cpu), '--bio-cpulist', str(A.bg_cpu), '--save', '', '--appendonly', 'no',
               '--protected-mode', 'no', '--maxclients', '20000', '--daemonize', 'yes', '--dir', self.dir,
               '--logfile', os.path.join(self.dir, 'log'), '--enable-debug-command', 'yes',
               '--tls-port', str(A.port + 1), '--tls-cert-file', t + '/server.crt', '--tls-key-file', t + '/server.key',
               '--tls-ca-cert-file', t + '/ca.crt', '--tls-auth-clients', 'no', '--owner-tls-port', str(A.owner_tls_port)]
        subprocess.run(cmd, check=True)
        for _ in range(100):
            try:
                self.m = Conn(A.port)
                if self.m.alive():
                    return
            except OSError:
                time.sleep(0.1)
        raise SystemExit('server %s did not start' % tag)

    def stop(self):
        try:
            self.m.send(resp('SHUTDOWN', 'NOSAVE'))
        except OSError:
            pass
        for _ in range(100):
            try:
                socket.create_connection(('127.0.0.1', A.port), 0.2).close()
                time.sleep(0.1)
            except OSError:
                return

    def crashes(self):
        try:
            return sum(1 for l in open(os.path.join(self.dir, 'log')) if 'ASSERT' in l or 'crashed' in l or 'BUG REPORT' in l)
        except OSError:
            return -1


def oport(i):
    return A.owner_port + i % A.n


def otport(i):
    return A.owner_tls_port + i % A.n


def part_timeout(srv, tag):
    m = srv.m
    m.cmd('CONFIG', 'SET', 'timeout', '0')
    base = int(info(m, 'clients')['connected_clients'])
    idle = [Conn(oport(i)) for i in range(40)] + [Conn(A.port) for _ in range(8)] + \
           [Conn(otport(i), tls=True) for i in range(8)]
    for c in idle:
        c.cmd('PING')
    time.sleep(3.0)
    alive0 = sum(c.alive() for c in idle)
    say(alive0 == len(idle), '%s.timeout0.idle_kept' % tag, 'alive %d/%d after 3 s with timeout 0' % (alive0, len(idle)))
    idle2 = [Conn(oport(i)) for i in range(40)] + [Conn(A.port) for _ in range(8)] + \
            [Conn(otport(i), tls=True) for i in range(8)]
    for c in idle2:
        c.cmd('PING')
    active = [Conn(oport(i)) for i in range(8)] + [Conn(A.port) for _ in range(2)] + \
             [Conn(otport(i), tls=True) for i in range(4)]
    for c in active + idle:
        c.cmd('PING')
    m.cmd('CONFIG', 'SET', 'timeout', '2')
    t0 = time.time()
    ok_active = True
    while time.time() - t0 < 6.5:
        for c in active:
            ok_active &= c.alive()
        m.cmd('PING')
        time.sleep(0.4)
    closed = sum(c.closed_by_server(0.05) for c in idle2)
    say(closed == len(idle2), '%s.timeout2.idle_closed' % tag, 'closed %d/%d idle (owner, main, owner TLS) after 5 s' %
        (closed, len(idle2)))
    say(ok_active, '%s.timeout2.active_kept' % tag, 'active clients answered every 0.4 s PING for 5 s')
    m.cmd('CONFIG', 'SET', 'timeout', '0')
    for c in idle + idle2 + active:
        c.close()
    time.sleep(0.5)
    n = int(info(m, 'clients')['connected_clients'])
    say(n == base, '%s.timeout.clients_back' % tag, 'connected_clients %d (before %d)' % (n, base))
    return closed


def gone(m, addr, secs):
    """Wait up to secs for the client at addr to leave CLIENT LIST (the server closed it); seconds taken or None."""
    t0 = time.time()
    while time.time() - t0 < secs:
        if not any(r.get('addr') == addr for r in client_list(m)):
            return round(time.time() - t0, 1)
        time.sleep(0.2)
    return None


def addr_of(c):
    h, p = c.s.getsockname()[:2]
    return '%s:%d' % (h, p)


def part_obuf(srv, tag):
    m = srv.m
    m.cmd('SET', '{o}big', b'x' * 100000)
    d0 = int(info(m, 'stats')['client_output_buffer_limit_disconnections'])
    m.cmd('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 0 256kb 1')
    res = {}
    for name, port in (('owner', oport(0)), ('main', A.port)):
        # no command before the flood: an owner client stays on its owner (a non-native command would make it
        # main's guest, which keeps it while replies are pending)
        c = Conn(port, rcvbuf=4096)
        c.send(resp('GET', '{o}big') * 100)  # ~10 MB of replies, never read
        time.sleep(0.5)
        early = gone(m, addr_of(c), 0.01) is not None
        res[name] = (early, gone(m, addr_of(c), 6))
        c.close()
    say(all(t is not None and not e for e, t in res.values()), '%s.obuf.soft_cron' % tag,
        'soft 256kb/1s, no more appends: (closed early, closed after s) %s' % res)
    m.cmd('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 1mb 0 0')
    hard = {}
    for name, port in (('owner', oport(1)), ('main', A.port)):
        c = Conn(port, rcvbuf=4096)
        c.send(resp('GET', '{o}big') * 100)
        hard[name] = gone(m, addr_of(c), 3)
        c.close()
    say(all(v is not None for v in hard.values()), '%s.obuf.hard' % tag, 'hard 1mb: closed after s %s' % hard)
    m.cmd('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 0 0 0')
    d1 = int(info(m, 'stats')['client_output_buffer_limit_disconnections'])
    say(d1 - d0 == 4, '%s.obuf.counted' % tag, 'client_output_buffer_limit_disconnections +%d (want 4)' % (d1 - d0))
    m.cmd('DEL', '{o}big')
    # An owner client whose replies outgrow its socket goes to main as a guest (each such read hands it over) and
    # stays there while replies are pending, so these closes are main's in both builds; the owner's own cron
    # branch is the same close-and-hand-over as its append-time close (the hard limit above).
    return d1 - d0


def part_qbuf(srv, tag):
    m = srv.m
    q0 = int(info(m, 'stats').get('client_query_buffer_limit_disconnections', 0))
    m.cmd('CONFIG', 'SET', 'client-query-buffer-limit', '1mb')
    out = {}
    for name, port in (('owner', oport(2)), ('main', A.port)):
        c = Conn(port)
        try:
            c.send(b'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2000000\r\n' + b'y' * 1500000)
        except OSError:
            pass
        out[name] = c.closed_by_server(3)
        c.close()
    m.cmd('CONFIG', 'SET', 'client-query-buffer-limit', '1gb')
    q1 = int(info(m, 'stats').get('client_query_buffer_limit_disconnections', 0))
    say(all(out.values()) and q1 - q0 == 2, '%s.qbuf.limit' % tag, 'closed %s, client_query_buffer_limit_disconnections +%d'
        % (out, q1 - q0))


def client_list(m):
    rows = []
    for l in m.cmd('CLIENT', 'LIST').decode().splitlines():
        rows.append(dict(kv.split('=', 1) for kv in l.split(' ') if '=' in kv))
    return rows


def part_resize(srv, tag):
    m = srv.m
    c = Conn(oport(3))
    c.cmd('CLIENT', 'SETNAME', 'rsz')
    # half of a 100 KB bulk: the query buffer is private, holds 50 KB and has room for the rest
    c.send(b'*3\r\n$3\r\nSET\r\n$4\r\n{r}k\r\n$100000\r\n' + b'z' * 50000)
    time.sleep(0.3)
    row0 = [r for r in client_list(m) if r.get('name') == 'rsz']
    time.sleep(3.5)
    row = [r for r in client_list(m) if r.get('name') == 'rsz']
    f0 = int(row0[0].get('qbuf-free', -1)) if row0 else -1
    f1 = int(row[0].get('qbuf-free', -1)) if row else -1
    # clientResizeQueryBuffer: idle > 2 s with input pending: sdsRemoveFreeSpace (which keeps a little room)
    say(f0 > 4096 and 0 <= f1 < f0 // 2, '%s.resize.qbuf_trimmed' % tag, 'qbuf/qbuf-free right after %s/%d, after 3.5 s idle %s/%d' %
        (row0[0].get('qbuf') if row0 else None, f0, row[0].get('qbuf') if row else None, f1))
    c.send(b'z' * 50000 + b'\r\n')
    say(c.reply() == b'+OK' and c.alive(), '%s.resize.alive' % tag, 'the rest of the SET ran; the client answers')
    c.close()
    m.cmd('DEL', '{r}k')
    return (f0, f1)


def part_stats(srv, tag):
    m = srv.m
    m.cmd('FLUSHALL')
    m.cmd('CONFIG', 'RESETSTAT')
    conns = [Conn(oport(i)) for i in range(24)] + [Conn(A.port) for _ in range(4)]
    for i, c in enumerate(conns):
        c.cmd('CLIENT', 'SETNAME', 'st%d' % i)
    for r in range(50):
        for i, c in enumerate(conns):
            k = '{s%d}k%d' % (i % 8, r % 7)
            c.send(resp('SET', k, 'v' * (r % 13 + 1)) + resp('GET', k) + resp('GET', k + 'missing') + resp('INCR', '{s%d}n' % (i % 8)) + resp('PING'))
        for c in conns:
            for _ in range(5):
                c.reply()
    big = Conn(oport(1))
    big.cmd('SET', '{s1}big', b'q' * 60000)
    time.sleep(2.5)  # a cron pass over every client
    i = info(m, 'everything')
    rows = [r for r in client_list(m) if r.get('name', '').startswith('st')]
    keys = ['connected_clients', 'total_connections_received', 'total_commands_processed', 'keyspace_hits',
            'keyspace_misses', 'total_net_input_bytes', 'total_net_output_bytes', 'rejected_connections',
            'client_output_buffer_limit_disconnections', 'client_query_buffer_limit_disconnections', 'blocked_clients',
            'evicted_clients', 'total_error_replies']
    snap = {k: i.get(k) for k in keys}
    snap['client_list_st'] = len(rows)
    snap['client_list_cmds'] = sorted((r['name'], r.get('cmd'), r.get('tot-net-in'), r.get('tot-net-out'), r.get('tot-cmds'))
                                      for r in rows)
    extra = {'mem_clients_normal': int(i.get('mem_clients_normal', 0)),
             'client_recent_max_input_buffer': int(i.get('client_recent_max_input_buffer', 0)),
             'client_recent_max_output_buffer': int(i.get('client_recent_max_output_buffer', 0))}
    for c in conns + [big]:
        c.close()
    return snap, extra


def part_mmclients(srv, tag):
    m = srv.m
    idle = [Conn(oport(i)) for i in range(8)]
    for c in idle:
        c.cmd('PING')
    e0 = int(info(m, 'stats')['evicted_clients'])
    m.cmd('CONFIG', 'SET', 'maxmemory-clients', '4mb')
    time.sleep(1.2)
    oi = info(m, 'owners')
    hog = Conn(oport(0))
    try:
        hog.send(b'*3\r\n$3\r\nSET\r\n$1\r\nh\r\n$7000000\r\n')
        for _ in range(12):
            hog.send(b'h' * 500000)
            time.sleep(0.05)
    except OSError:
        pass
    evicted = hog.closed_by_server(3)
    e1 = int(info(m, 'stats')['evicted_clients'])
    kept = sum(c.alive() for c in idle)
    m.cmd('CONFIG', 'SET', 'maxmemory-clients', '0')
    say(evicted and e1 - e0 >= 1 and kept == len(idle), '%s.mmclients.evict' % tag,
        'hog evicted %s (evicted_clients +%d), idle owner clients kept %d/%d, owner_cron while set: %s' %
        (evicted, e1 - e0, kept, len(idle), oi.get('owner_cron')))
    for c in idle + [hog]:
        c.close()


def run(binary, tag):
    srv = Server(binary, tag)
    try:
        part_timeout(srv, tag)
        part_obuf(srv, tag)
        part_qbuf(srv, tag)
        rz = part_resize(srv, tag)
        st = part_stats(srv, tag)
        st[0]['resize_qbuf_free_before_after'] = rz
        part_mmclients(srv, tag)
        o = info(srv.m, 'owners')
        print('INFO %s owners: %s' % (tag, ' '.join('%s=%s' % (k, v) for k, v in o.items() if 'cron' in k or 'held' in k)))
    finally:
        srv.stop()
    say(srv.crashes() == 0, '%s.no_crash' % tag, 'crash lines %d' % srv.crashes())
    return st


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument('--bin', required=True)
    ap.add_argument('--ref-bin', required=True)
    ap.add_argument('--dir', required=True)
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--owner-port', type=int, required=True)
    ap.add_argument('--owner-tls-port', type=int, required=True)
    ap.add_argument('--tls-dir', required=True)
    ap.add_argument('--n', type=int, default=4)
    ap.add_argument('--cpus', required=True)
    ap.add_argument('--main-cpu', type=int, required=True)
    ap.add_argument('--bg-cpu', type=int)
    A = ap.parse_args()
    if A.bg_cpu is None:
        A.bg_cpu = A.main_cpu
    cand = run(A.bin, 'cand')
    ref = run(A.ref_bin, 'ref')
    for k in cand[0]:
        say(cand[0][k] == ref[0][k], 'stats.equal.%s' % k, 'cand %s ref %s' % (str(cand[0][k])[:120], str(ref[0][k])[:120]))
    for k in cand[1]:
        say(cand[1][k] > 0 and ref[1][k] > 0 and 0.5 <= cand[1][k] / ref[1][k] <= 2, 'stats.close.%s' % k,
            'cand %d ref %d' % (cand[1][k], ref[1][k]))
    print('RLCRONCHECK pass=%d fail=%d' % (sum(RES), len(RES) - sum(RES)))
    sys.exit(0 if all(RES) else 1)


if __name__ == '__main__':
    main()
