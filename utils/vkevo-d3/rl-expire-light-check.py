#!/usr/bin/env python3
"""RL-g3f-owner-expire-light gene checks (one candidate server, loopback, 127.0.0.1): the light cron and active expiry.
  keys       owners expire keys (owner_active_expire yes): 2,000 keys EX 3600 + 500 keys PX 200-800 set on their
             owners, no client of main's connected: after 4 s expired_keys +500, keys=2000, and no light tick stopped
             the world for the slow expire cycle (owner_light_full_why expire +0) while ticks ran light
  fields     a hash with fields PX 300 (HSETEX, via main): the fields expire actively, and while the hash had volatile
             fields the light ticks stopped the world for main's field job (expire +n > 0); once the hash is gone and the
             world stopped, the field bound starts over: no more expire stops (3 s, expire +0)
  off        CONFIG SET owner-active-expire no: main expires every key again (owner_active_expire no after the next
             stop), 500 short keys expire, and the light ticks stop the world for it (expire +n > 0); back to yes:
             owner_active_expire yes, owner_expired_keys moves again
  paused     DEBUG SET-ACTIVE-EXPIRE 0: 300 short keys stay past their deadline (dbsize, no expired_keys);
             SET-ACTIVE-EXPIRE 1: they expire
Prints PASS / FAIL lines and RLEXPIRELIGHT pass= fail=."""
import argparse, os, socket, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, required=True)
ap.add_argument('--owner-port', type=int, required=True)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--cpus', required=True)
ap.add_argument('--main-cpu', required=True)
ap.add_argument('--bg-cpu', required=True)
ap.add_argument('--debug', action='store_true')
a = ap.parse_args()
RES = []


def say(ok, name, msg):
    RES.append(bool(ok))
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)


class R:
    def __init__(self, port):
        self.s = socket.create_connection(('127.0.0.1', port), timeout=30)
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
        if t in (b'+', b'-'):
            return r.decode()
        if t == b':':
            return int(r)
        if t in (b'$', b'='):
            n = int(r)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t == b'*':
            n = int(r)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise RuntimeError('bad reply %r' % l)

    def do(self, *c):
        self.send([c])
        return self.read()

    def pipe(self, cmds):
        self.send(cmds)
        return [self.read() for _ in cmds]

    def close(self):
        self.f.close()  # the makefile holds the socket open until it is closed too
        self.s.close()


def info(port, sec):
    r = R(port)
    v = r.do('INFO', sec)
    r.close()
    d = {}
    for line in v.decode().split('\r\n'):
        if ':' in line and not line.startswith('#'):
            k, x = line.split(':', 1)
            d[k] = x
    return d


def why(i, k='expire'):
    return int(dict(x.split('=') for x in i['owner_light_full_why'].split(','))[k])


def light(i):
    return int(i['owner_light_iterations']) - int(i['owner_light_full'])


def crc16(b):
    crc = 0
    for x in b:
        crc ^= x << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
    return crc


os.makedirs(a.dir, exist_ok=True)
subprocess.run(['taskset', '-c', '%s,%s,%s' % (a.cpus, a.main_cpu, a.bg_cpu), a.bin, '--port', str(a.port), '--bind',
                '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', a.dir, '--logfile', os.path.join(a.dir, 'log'),
                '--daemonize', 'yes', '--enable-debug-command', 'yes', '--io-threads', '1', '--owner-threads', str(a.n),
                '--owner-port', str(a.owner_port), '--owner-cpulist', a.cpus, '--server-cpulist', a.main_cpu,
                '--bio-cpulist', a.bg_cpu], check=True)
for _ in range(200):
    try:
        if R(a.port).do('PING') == 'PONG':
            break
    except OSError:
        time.sleep(0.05)
m = R(a.port)
slots = m.do('CLUSTER', 'SLOTS')
omap = [(e[0], e[1], e[2][1]) for e in slots]
m.close()
oc = {p: R(p) for _, _, p in omap}


def owner_cmds(cmds):
    g = {}
    for c in cmds:
        s = crc16(c[1].encode()) & 16383
        for lo, hi, p in omap:
            if lo <= s <= hi:
                g.setdefault(p, []).append(c)
    for p, cs in g.items():
        rs = oc[p].pipe(cs)
        assert all(x == 'OK' for x in rs), rs[:3]


def owners_info():
    # INFO through an owner port goes to main (a handoff): a file event, a full stop, nothing pending after it
    return info(a.port, 'owners')


try:
    # keys: owners expire them; no light tick stops for the slow expire cycle
    i0 = owners_info()
    owner_cmds([('SET', 'el:%d' % i, 'v', 'EX', '3600') for i in range(2000)] +
               [('SET', 'es:%d' % i, 'v', 'PX', str(200 + (i * 7) % 600)) for i in range(500)])
    e0 = int(info(a.port, 'stats')['expired_keys'])
    i1 = owners_info()
    time.sleep(4)
    i2 = owners_info()
    st = info(a.port, 'stats')
    ks = info(a.port, 'keyspace').get('db0', '')
    say(i2.get('owner_active_expire') == 'yes' and int(st['expired_keys']) - e0 == 500 and ks.startswith('keys=2000,'),
        'keys.expire', 'owner_active_expire %s, expired_keys +%d (want 500), owner_expired_keys %s, %s' % (
            i2.get('owner_active_expire'), int(st['expired_keys']) - e0, i2.get('owner_expired_keys'), ks.split(',avg')[0]))
    say(why(i2) - why(i1) == 0 and light(i2) - light(i1) >= 20, 'keys.no_expire_stops',
        'in 4 s: light ticks +%d, expire stops +%d (want 0); iterations +%d, full +%d, full why %s / %s, stops wake %s / %s, '
        'file %s / %s' % (light(i2) - light(i1), why(i2) - why(i1),
                          int(i2['owner_light_iterations']) - int(i1['owner_light_iterations']),
                          int(i2['owner_light_full']) - int(i1['owner_light_full']), i1['owner_light_full_why'],
                          i2['owner_light_full_why'], i1['owner_stop_wake'], i2['owner_stop_wake'], i1['owner_stop_file'],
                          i2['owner_stop_file']))
    # fields: main's field job, the field bound
    if a.debug:
        r_ = R(a.port)
        print('DEBUG client list:', r_.do('CLIENT', 'LIST').decode().replace('\n', ' | ')[:3000], flush=True)
        r_.close()
        oi = owners_info()
        print('DEBUG owners:', {k: v for k, v in oi.items() if 'resident' in k or 'guest' in k or 'handoffs' in k}, flush=True)
    m = R(a.port)
    r = m.do('HSETEX', 'hf', 'PX', '300', 'FIELDS', '3', 'a', '1', 'b', '2', 'c', '3')
    m.do('HSET', 'hf', 'keep', 'x')
    j1 = owners_info()
    time.sleep(2)
    j2 = owners_info()
    hl = m.do('HLEN', 'hf')
    fx = int(info(a.port, 'stats').get('expired_fields', 0))
    say(not (isinstance(r, str) and r.startswith('ERR')) and hl == 1 and fx >= 3 and why(j2) - why(j1) > 0, 'fields.expire',
        'HSETEX %r, HLEN after 2 s %r (want 1), expired_fields %d, expire stops +%d (want > 0)' % (
            r, hl, fx, why(j2) - why(j1)))
    m.do('DEL', 'hf')
    m.close()
    time.sleep(0.5)
    k1 = owners_info()  # a full stop with no hash left: the field bound starts over
    time.sleep(3)
    k2 = owners_info()
    say(why(k2) - why(k1) == 0 and k2.get('owner_expire_bound_ms') is not None, 'fields.bound_reset',
        'after the hash is gone: expire stops +%d in 3 s (want 0)' % (why(k2) - why(k1)))
    # off: main expires the keys again, with its stops
    m = R(a.port)
    m.do('CONFIG', 'SET', 'owner-active-expire', 'no')
    m.close()
    o1 = owners_info()
    e0 = int(info(a.port, 'stats')['expired_keys'])
    ok0 = int(o1['owner_expired_keys'])
    owner_cmds([('SET', 'eo:%d' % i, 'v', 'PX', str(200 + (i * 7) % 600)) for i in range(500)])
    time.sleep(4)
    o2 = owners_info()
    e1 = int(info(a.port, 'stats')['expired_keys'])
    say(o2.get('owner_active_expire') == 'no' and e1 - e0 == 500 and int(o2['owner_expired_keys']) == ok0 and
        why(o2) - why(o1) > 0, 'off.main_expires',
        'owner_active_expire %s, expired_keys +%d (want 500), owner_expired_keys +%d (want 0), expire stops +%d (want > 0)'
        % (o2.get('owner_active_expire'), e1 - e0, int(o2['owner_expired_keys']) - ok0, why(o2) - why(o1)))
    m = R(a.port)
    m.do('CONFIG', 'SET', 'owner-active-expire', 'yes')
    m.close()
    owners_info()
    owner_cmds([('SET', 'ey:%d' % i, 'v', 'PX', '200') for i in range(200)])
    time.sleep(3)
    y2 = owners_info()
    say(y2.get('owner_active_expire') == 'yes' and int(y2['owner_expired_keys']) - int(o2['owner_expired_keys']) == 200,
        'on.again', 'owner_active_expire %s, owner_expired_keys +%d (want 200)' % (
            y2.get('owner_active_expire'), int(y2['owner_expired_keys']) - int(o2['owner_expired_keys'])))
    # paused: DEBUG SET-ACTIVE-EXPIRE 0 keeps keys past their deadline (no access)
    m = R(a.port)
    m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    m.close()
    owners_info()
    n0 = R(a.port).do('DBSIZE')
    e0 = int(info(a.port, 'stats')['expired_keys'])
    owner_cmds([('SET', 'ep:%d' % i, 'v', 'PX', '200') for i in range(300)])
    time.sleep(2)
    n1 = R(a.port).do('DBSIZE')
    e1 = int(info(a.port, 'stats')['expired_keys'])
    say(n1 - n0 == 300 and e1 == e0, 'paused.keep', 'DBSIZE +%d (want 300), expired_keys +%d (want 0)' % (n1 - n0, e1 - e0))
    m = R(a.port)
    m.do('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
    m.close()
    owners_info()
    time.sleep(3)
    n2 = R(a.port).do('DBSIZE')
    e2 = int(info(a.port, 'stats')['expired_keys'])
    say(n2 == n0 and e2 - e1 == 300, 'paused.resume', 'DBSIZE back to %d (want %d), expired_keys +%d (want 300)' % (
        n2, n0, e2 - e1))
    lg = open(os.path.join(a.dir, 'log')).read()
    say(not any(x in lg for x in ('ASSERT', 'crashed', 'Guru', 'BUG REPORT')), 'crash', 'crash lines none')
finally:
    try:
        R(a.port).send([['SHUTDOWN', 'NOSAVE']])
    except OSError:
        pass
    time.sleep(0.5)
print('RLEXPIRELIGHT pass=%d fail=%d' % (sum(RES), len(RES) - sum(RES)), flush=True)
sys.exit(0 if all(RES) else 1)
