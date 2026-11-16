#!/usr/bin/env python3
"""RL-g3b-light-cron check: what main's light iterations (timer wakes that leave the owners running) and the owners'
own table upkeep must keep, against a candidate and a reference binary (the parent, whose every main wake stops the
owners and whose main rightsizes / rehashes every table) started the same way with the same steps:

  light      (candidate) an idle server runs its serverCron ticks light: owner_light_iterations grows ~hz a second,
             owner_cron_stops does not
  expire     keys set natively on their owners with EX 3600 and others with PX 200-800 never read again: the short ones
             are expired actively (expired_keys, dbsize) as in the reference
  stats      a fixed sequence of native commands: total_commands_processed, keyspace hits/misses, net in/out equal the
             reference; instantaneous_ops_per_sec reflects a steady native load (sampled while the owners never stop)
  memory     after loading the same keys: used_memory, allocator_allocated within 5 %, rss within 20 %,
             mem_fragmentation_ratio and allocator_frag_ratio within 20 % of the reference; MEMORY DOCTOR answers
  tables     200k keys loaded and then deleted through their owners: the main table shrinks back (MEMORY STATS
             db.0 overhead.hashtable.main) as in the reference, within 2x, and below 1/8 of its peak
  save       CONFIG SET save "2 100" and 300 native writes: a BGSAVE starts within 8 s (the owners' unfolded changes
             count), rdb_last_bgsave_status ok
  verbose    loglevel verbose: ticks stay light but the 5 s DB log tick stops the world (owner_light_full_why
             verbose grows by about one per 5 s)
  defrag     activedefrag yes (when the allocator supports it): every tick stops the world (defrag), then back to light
  sigterm    SIGTERM to an idle (light) server: it exits within 5 s

  rl-light-check.py --bin CAND --ref-bin PARENT --dir D --port P --owner-port OP --n 4 --cpus 32-35 --main-cpu 36
Prints PASS/FAIL lines and RLLIGHTCHECK pass=N fail=M.
"""
import argparse, os, signal, socket, subprocess, sys, time

A = None
RES = []


def say(ok, name, msg=''):
    RES.append(bool(ok))
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)


def resp(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n%s\r\n' % (len(a), a)
    return out


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(('127.0.0.1', port), 10)
        self.buf = b''

    def fill(self):
        d = self.s.recv(1 << 20)
        if not d:
            raise EOFError
        self.buf += d

    def reply(self):
        while b'\r\n' not in self.buf:
            self.fill()
        l, self.buf = self.buf.split(b'\r\n', 1)
        t = l[:1]
        if t == b':':
            return int(l[1:])
        if t in (b'+', b'-'):
            return l
        if t == b'$':
            n = int(l[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.fill()
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b'*':
            return [self.reply() for _ in range(int(l[1:]))]
        raise RuntimeError(l)

    def cmd(self, *args):
        self.s.sendall(resp(*args))
        return self.reply()

    def pipe(self, cmds):
        """Send many commands in chunks of 500, read every reply; returns the replies."""
        out = []
        for i in range(0, len(cmds), 500):
            chunk = cmds[i:i + 500]
            self.s.sendall(b''.join(resp(*c) for c in chunk))
            out += [self.reply() for _ in chunk]
        return out

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
            crc &= 0xffff
    return crc


def slot(key):
    k = key.encode()
    s = k.find(b'{')
    if s >= 0:
        e = k.find(b'}', s + 1)
        if e > s + 1:
            k = k[s + 1:e]
    return crc16(k) % 16384


def owner_of(key):
    # owner i owns tables [1024*i/N, 1024*(i+1)/N) of 16 slots each (owner.c ownerThreadsInit)
    t = slot(key) >> 4
    for i in range(A.n):
        if 1024 * i // A.n <= t < 1024 * (i + 1) // A.n:
            return i
    return A.n - 1


def info(c, sec):
    out = {}
    for l in c.cmd('INFO', sec).decode().splitlines():
        if ':' in l:
            k, v = l.split(':', 1)
            out[k] = v
    return out


def mstats(c):
    r = c.cmd('MEMORY', 'STATS')
    d = {}
    for i in range(0, len(r) - 1, 2):
        k = r[i].decode() if isinstance(r[i], bytes) else str(r[i])
        d[k] = r[i + 1]
    return d


class Server:
    def __init__(self, binary, tag, extra=()):
        self.tag = tag
        self.dir = os.path.join(A.dir, tag)
        os.makedirs(self.dir, exist_ok=True)
        cmd = ['taskset', '-c', '%s,%d' % (A.cpus, A.main_cpu), binary, '--port', str(A.port), '--bind', '127.0.0.1',
               '--owner-threads', str(A.n), '--owner-port', str(A.owner_port), '--owner-cpulist', A.cpus,
               '--server-cpulist', str(A.main_cpu), '--bio-cpulist', str(A.bg_cpu), '--save', '', '--appendonly', 'no',
               '--protected-mode', 'no', '--daemonize', 'yes', '--dir', self.dir, '--pidfile',
               os.path.join(self.dir, 'pid'), '--logfile', os.path.join(self.dir, 'log'), '--enable-debug-command', 'yes'] + list(extra)
        subprocess.run(cmd, check=True)
        for _ in range(100):
            try:
                self.m = Conn(A.port)
                if self.m.cmd('PING') == b'+PONG':
                    break
            except OSError:
                time.sleep(0.1)
        self.o = [Conn(A.owner_port + i) for i in range(A.n)]
        self.pid = int(open(os.path.join(self.dir, 'pid')).read())

    def owner_cmds(self, cmds):
        """Run commands each on the owner of its key (argv[1]), in order per owner; returns the replies in order."""
        per = {}
        for idx, c in enumerate(cmds):
            per.setdefault(owner_of(c[1]), []).append((idx, c))
        out = [None] * len(cmds)
        for i, lst in per.items():
            for (idx, _), r in zip(lst, self.o[i].pipe([c for _, c in lst])):
                out[idx] = r
        return out

    def stop(self):
        for c in self.o:
            c.close()
        try:
            self.m.s.sendall(resp('SHUTDOWN', 'NOSAVE'))
        except OSError:
            pass
        for _ in range(100):
            try:
                os.kill(self.pid, 0)
                time.sleep(0.1)
            except OSError:
                return

    def crashes(self):
        try:
            return sum(1 for l in open(os.path.join(self.dir, 'log')) if 'ASSERT' in l or 'crashed' in l or 'BUG REPORT' in l)
        except OSError:
            return -1


def oval(srv, k):
    return info(srv.m, 'owners').get(k)


def run(binary, tag, is_cand):
    out = {}
    srv = Server(binary, tag)
    m = srv.m
    try:
        if is_cand:
            i0 = info(m, 'owners')
            time.sleep(3)
            i1 = info(m, 'owners')
            li = int(i1['owner_light_iterations']) - int(i0['owner_light_iterations'])
            cs = int(i1['owner_cron_stops']) - int(i0['owner_cron_stops'])
            say(li >= 20 and cs <= 3, '%s.light.idle' % tag, 'in 3 s: light iterations +%d, cron stops +%d' % (li, cs))
        # expire
        e0 = int(info(m, 'stats')['expired_keys'])
        long_keys = [('SET', 'el:%d' % i, 'v', 'EX', '3600') for i in range(2000)]
        short_keys = [('SET', 'es:%d' % i, 'v', 'PX', str(200 + (i * 7) % 600)) for i in range(500)]
        srv.owner_cmds(long_keys + short_keys)
        time.sleep(6)
        e1 = int(info(m, 'stats')['expired_keys'])
        ks = info(m, 'keyspace').get('db0', '')
        out['expire'] = (e1 - e0, ks.split(',avg_ttl')[0])
        say(e1 - e0 == 500 and ks.startswith('keys=2000,expires=2000'), '%s.expire.active' % tag,
            'expired_keys +%d (want 500), %s' % (e1 - e0, ks))
        m.cmd('FLUSHALL')
        time.sleep(0.5)
        # stats: an exact sequence
        m.cmd('CONFIG', 'RESETSTAT')
        seq = []
        for r in range(20):
            for i in range(200):
                k = 'st:{%d}:%d' % (i % 64, r % 5)
                seq += [('SET', k, 'x' * (r % 9 + 1)), ('GET', k), ('GET', k + 'm'), ('INCR', 'st:{%d}:n' % (i % 64))]
        srv.owner_cmds(seq)
        time.sleep(1.2)
        st = info(m, 'stats')
        out['stats'] = {k: st.get(k) for k in ('total_commands_processed', 'keyspace_hits', 'keyspace_misses',
                                                'total_net_input_bytes', 'total_net_output_bytes', 'total_error_replies')}
        # instantaneous ops under a steady native load (no main command in between)
        t0, n = time.time(), 0
        samples = []
        while time.time() - t0 < 4:
            batch = [('GET', 'st:{%d}:1' % (j % 64)) for j in range(400)]
            srv.owner_cmds(batch)
            n += len(batch)
            if time.time() - t0 > 2.5 and not samples:
                ia = info(m, 'owners')
                samples.append(int(info(m, 'stats')['instantaneous_ops_per_sec']))
                time.sleep(0.05)
        rate = n / (time.time() - t0)
        ib = info(m, 'owners')
        lb = int(ib.get('owner_light_iterations', 0)) - int(ia.get('owner_light_iterations', 0)) if is_cand else None
        say(samples and 0.4 * rate <= samples[0] <= 1.6 * rate, '%s.stats.instantaneous' % tag,
            'instantaneous_ops_per_sec %s vs measured %.0f/s (light iterations meanwhile %s)' % (samples, rate, lb))
        m.cmd('FLUSHALL')
        time.sleep(0.5)
        # memory after the same load
        load = [('SET', 'mm:%d' % i, 'y' * (50 + i % 200), 'EX', '3600') for i in range(60000)]
        srv.owner_cmds(load)
        time.sleep(1.5)
        im = info(m, 'memory')
        out['memory'] = {k: im.get(k) for k in ('used_memory', 'used_memory_rss', 'allocator_allocated',
                                                 'mem_fragmentation_ratio', 'allocator_frag_ratio', 'used_memory_peak')}
        doc = m.cmd('MEMORY', 'DOCTOR')
        say(doc and len(doc) > 20, '%s.memory.doctor' % tag, repr(doc[:70]))
        m.cmd('FLUSHALL')
        time.sleep(0.5)
        # tables: fill then delete through the owners, wait for the shrink
        keys = ['tb:%d' % i for i in range(200000)]
        srv.owner_cmds([('SET', k, '1') for k in keys])
        time.sleep(1)
        peak = mstats(m).get('db.0')
        peak_ht = None
        if isinstance(peak, list):
            for i in range(0, len(peak) - 1, 2):
                if peak[i] == b'overhead.hashtable.main':
                    peak_ht = int(peak[i + 1])
        srv.owner_cmds([('DEL', k) for k in keys[:199900]])
        time.sleep(9)
        after = mstats(m).get('db.0')
        ht = None
        if isinstance(after, list):
            for i in range(0, len(after) - 1, 2):
                if after[i] == b'overhead.hashtable.main':
                    ht = int(after[i + 1])
        out['tables'] = (peak_ht, ht)
        say(peak_ht and ht is not None and ht * 8 < peak_ht, '%s.tables.shrink' % tag,
            'overhead.hashtable.main peak %s -> after deletes %s' % (peak_ht, ht))
        m.cmd('FLUSHALL')
        # save point counts the owners' changes
        l0 = int(info(m, 'persistence')['rdb_last_save_time'])
        time.sleep(1.1)
        m.cmd('CONFIG', 'SET', 'save', '2 100')
        srv.owner_cmds([('SET', 'sv:%d' % i, 'z') for i in range(300)])
        ok, t0 = False, time.time()
        while time.time() - t0 < 8:
            p = info(m, 'persistence')
            if int(p['rdb_last_save_time']) > l0 and p.get('rdb_bgsave_in_progress') == '0':
                ok = p.get('rdb_last_bgsave_status') == 'ok'
                break
            time.sleep(0.2)
        say(ok, '%s.save.point' % tag, 'BGSAVE after %.1f s: %s' % (time.time() - t0, ok))
        m.cmd('CONFIG', 'SET', 'save', '')
        if is_cand:
            v0 = oval(srv, 'owner_light_full_why')
            m.cmd('CONFIG', 'SET', 'loglevel', 'verbose')
            i0 = info(m, 'owners')
            time.sleep(6)
            i1 = info(m, 'owners')
            m.cmd('CONFIG', 'SET', 'loglevel', 'notice')
            vv = lambda d: int(dict(x.split('=') for x in d['owner_light_full_why'].split(','))['verbose'])
            dv = vv(i1) - vv(i0)
            li = int(i1['owner_light_iterations']) - int(i0['owner_light_iterations'])
            say(1 <= dv <= 3 and li >= 40, '%s.verbose' % tag, 'loglevel verbose 6 s: verbose stops +%d, light +%d' % (dv, li))
            r = m.cmd('CONFIG', 'SET', 'activedefrag', 'yes')
            if r == b'+OK':
                i0 = info(m, 'owners')
                time.sleep(1.5)
                i1 = info(m, 'owners')
                m.cmd('CONFIG', 'SET', 'activedefrag', 'no')
                dd = lambda d: int(dict(x.split('=') for x in d['owner_light_full_why'].split(','))['defrag'])
                say(dd(i1) - dd(i0) >= 10, '%s.defrag' % tag, 'activedefrag yes 1.5 s: defrag stops +%d' % (dd(i1) - dd(i0)))
            else:
                say(True, '%s.defrag' % tag, 'activedefrag not supported here: %r' % r[:60])
            time.sleep(1)
            i0 = info(m, 'owners')
            time.sleep(1)
            i1 = info(m, 'owners')
            li = int(i1['owner_light_iterations']) - int(i0['owner_light_iterations'])
            say(li >= 6, '%s.light.again' % tag, 'light iterations +%d in 1 s after verbose and defrag' % li)
    finally:
        srv.stop()
    say(srv.crashes() == 0, '%s.no_crash' % tag, 'crash lines %d' % srv.crashes())
    if is_cand:
        s2 = Server(binary, tag + '-term')
        time.sleep(2)
        os.kill(s2.pid, signal.SIGTERM)
        t0, gone = time.time(), False
        while time.time() - t0 < 5:
            try:
                os.kill(s2.pid, 0)
                time.sleep(0.1)
            except OSError:
                gone = True
                break
        say(gone, '%s.sigterm' % tag, 'exited %.1f s after SIGTERM: %s' % (time.time() - t0, gone))
        if not gone:
            s2.stop()
    return out


def main():
    global A
    ap = argparse.ArgumentParser()
    ap.add_argument('--bin', required=True)
    ap.add_argument('--ref-bin', required=True)
    ap.add_argument('--dir', required=True)
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--owner-port', type=int, required=True)
    ap.add_argument('--n', type=int, default=4)
    ap.add_argument('--cpus', required=True)
    ap.add_argument('--main-cpu', type=int, required=True)
    ap.add_argument('--bg-cpu', type=int)
    A = ap.parse_args()
    if A.bg_cpu is None:
        A.bg_cpu = A.main_cpu
    cand = run(A.bin, 'cand', True)
    ref = run(A.ref_bin, 'ref', False)
    say(cand['expire'] == ref['expire'], 'cmp.expire', 'cand %s ref %s' % (cand['expire'], ref['expire']))
    for k in cand['stats']:
        say(cand['stats'][k] == ref['stats'][k], 'cmp.stats.%s' % k, 'cand %s ref %s' % (cand['stats'][k], ref['stats'][k]))
    cm, rm = cand['memory'], ref['memory']
    for k, tol in (('used_memory', 0.05), ('allocator_allocated', 0.05), ('used_memory_rss', 0.2),
                   ('mem_fragmentation_ratio', 0.2), ('allocator_frag_ratio', 0.2)):
        a, b = float(cm[k]), float(rm[k])
        say(abs(a - b) <= tol * max(a, b), 'cmp.memory.%s' % k, 'cand %s ref %s (tolerance %d %%)' % (cm[k], rm[k], tol * 100))
    ct, rt = cand['tables'], ref['tables']
    say(ct[1] is not None and rt[1] is not None and ct[1] <= 2 * max(rt[1], 1) and rt[1] <= 2 * max(ct[1], 1),
        'cmp.tables', 'cand peak/after %s ref %s' % (ct, rt))
    print('RLLIGHTCHECK pass=%d fail=%d' % (sum(RES), len(RES) - sum(RES)))
    sys.exit(0 if all(RES) else 1)


if __name__ == '__main__':
    main()
