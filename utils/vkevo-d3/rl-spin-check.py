#!/usr/bin/env python3
"""RL-g2f-owner-spin gene checks (EXPLORE.md extra gate items), loopback, servers bound to 127.0.0.1.

For the candidate (--bin, spin on by default) and the parent (--ref-bin), each on the same CPUs:
  1. layout: owners pinned to CPUs of their own -> every owner may spin (owner_spin_owners = N); main pinned to owner
     0's CPU (the fleet layout) -> owner 0 never spins (N - 1); CONFIG SET owner-spin-us / owner-spin-stop-us at run
     time (0 stops the spinning: spins stop growing).
  2. idle: after a load stops, every owner thread's CPU over the next second (from /proc) is under 2% of a core, and
     stays there; owner wakeups stop.
  3. world stops under a mixed load (native GET/SET on the owner ports + a stream of main-path commands that stop the
     world): stop wait per stop and the max are not worse than the parent's (within 25% + 5 us of noise on loopback),
     and each owner's spin found the world released (stop spins hit).
  4. main not starved: the main-path commands' throughput in that same mixed load is not lower than the parent's
     (within 5%), and main's run delay (/proc schedstat) is not higher (within 20% + 2 ms/s).
Prints PASS / FAIL lines and INFO lines with the numbers.
"""
import argparse, json, os, re, socket, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, default=26579)
ap.add_argument('--owner-port', type=int, default=26600)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--cpus', default='24-27', help='owner CPUs (contiguous)')
ap.add_argument('--main-cpu', type=int, default=28)
ap.add_argument('--bg-cpu', type=int, default=29)
ap.add_argument('--load-cpus', default='30-35')
ap.add_argument('--memtier', default='/home/ec2-user/ns/memtier_benchmark')
ap.add_argument('--secs', type=int, default=8)
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
lo, hi = map(int, a.cpus.split('-'))
fails = 0


def res(ok, name, msg):
    global fails
    print('%s %s %s' % ('PASS' if ok else 'FAIL', name, msg), flush=True)
    if not ok:
        fails += 1


def cmd(port, *args):
    s = socket.create_connection(('127.0.0.1', port), timeout=5)
    out = b'*%d\r\n' % len(args) + b''.join(b'$%d\r\n%s\r\n' % (len(x), x) for x in (str(y).encode() for y in args))
    s.sendall(out)
    buf = b''
    while True:
        ch = s.recv(1 << 20)
        if not ch:
            break
        buf += ch
        if buf[:1] in b'+-:' and buf.endswith(b'\r\n'):
            break
        if buf[:1] == b'$':
            n = int(buf[1:buf.index(b'\r\n')])
            if n < 0 or len(buf) >= buf.index(b'\r\n') + 2 + n + 2:
                break
    s.close()
    return buf.decode(errors='replace')


def info(port):
    d = {}
    for line in cmd(port, 'INFO', 'everything').split('\r\n'):
        if ':' in line and not line.startswith('#'):
            k, v = line.split(':', 1)
            d[k] = v
    return d


def start(binp, tag, main_cpu, extra=()):
    d = os.path.join(a.dir, tag)
    os.makedirs(d, exist_ok=True)
    args = [binp, '--port', str(a.port), '--bind', '127.0.0.1', '--owner-threads', str(a.n), '--owner-port',
            str(a.owner_port), '--owner-cpulist', a.cpus, '--server-cpulist', str(main_cpu), '--bio-cpulist',
            str(a.bg_cpu), '--save', '', '--appendonly', 'no', '--dir', d, '--logfile', os.path.join(d, 'log'),
            '--daemonize', 'yes'] + list(extra)
    subprocess.run(args, check=True)
    for _ in range(100):
        try:
            if 'PONG' in cmd(a.port, 'PING'):
                break
        except OSError:
            time.sleep(0.1)
    pid = int(re.search(r'pid=(\d+)', subprocess.run(['ss', '-Htlnp', 'sport = :%d' % a.port], capture_output=True,
                                                    text=True).stdout).group(1))
    return pid


def stop():
    try:
        cmd(a.port, 'SHUTDOWN', 'NOSAVE')
    except OSError:
        pass
    for _ in range(100):
        if not subprocess.run(['ss', '-Htln', 'sport = :%d' % a.port], capture_output=True, text=True).stdout.strip():
            return
        time.sleep(0.1)


def threads(pid):
    out = {}
    for t in os.listdir('/proc/%d/task' % pid):
        try:
            comm = open('/proc/%d/task/%s/comm' % (pid, t)).read().strip()
            st = open('/proc/%d/task/%s/stat' % (pid, t)).read()
            f = st[st.rindex(')') + 2:].split()
            ss = open('/proc/%d/task/%s/schedstat' % (pid, t)).read().split()
            vol = [int(l.split()[1]) for l in open('/proc/%d/task/%s/status' % (pid, t)) if l.startswith('voluntary_')][0]
        except (OSError, ValueError, IndexError):
            continue
        name = comm if comm.startswith('owner_') else ('main' if int(t) == pid else None)
        if name:
            out[name] = dict(oncpu=int(ss[0]) / 1e9, rd=int(ss[1]) / 1e9, vol=vol)
    return out


def delta(t0, t1, dt):
    return {k: {m: (t1[k][m] - t0[k][m]) / dt for m in t1[k]} for k in t1 if k in t0}


def memtier(secs, ratio='1:1', extra=(), rate=0):
    rl = ['--rate-limiting', str(rate)] if rate else []
    return subprocess.Popen(['taskset', '-c', a.load_cpus, a.memtier, '-s', '127.0.0.1', '-p', str(a.owner_port),
                             '--cluster-mode', '-t', '2', '-c', '8', '--ratio', ratio, '-d', '32', '--key-maximum', '100000',
                             '--key-pattern', 'R:R', '--test-time', str(secs), '--hide-histogram'] + rl + list(extra),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def mainload(secs):
    # main-path commands on the main port (HSET is not native on the parent), several connections
    bench = os.path.join(os.path.dirname(a.bin), 'valkey-benchmark')
    return subprocess.Popen(['taskset', '-c', a.load_cpus, bench, '-p', str(a.port), '-c', '8', '-n', '100000000',
                             '-r', '100000', '-t', 'hset', '--threads', '2', '-q'], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def mixed(binp, tag, rate):
    pid = start(binp, tag, a.main_cpu)
    i0 = info(a.port)
    mt = memtier(a.secs, rate=rate)
    ml = mainload(a.secs)
    time.sleep(2)
    t0 = threads(pid); j0 = info(a.port); c0 = time.time()
    time.sleep(a.secs - 4)
    t1 = threads(pid); j1 = info(a.port); c1 = time.time()
    mt.wait()
    ml.terminate(); ml.wait()
    dt = c1 - c0
    g = lambda d, k: int(d.get(k, '0'))
    hs = lambda d: int(re.search(r'calls=(\d+)', d.get('cmdstat_hset', 'calls=0')).group(1))
    stops = g(j1, 'owner_world_stops') - g(j0, 'owner_world_stops')
    r = dict(
        stops_s=stops / dt,
        wait_per_stop=(g(j1, 'owner_world_stop_wait_us') - g(j0, 'owner_world_stop_wait_us')) / max(1, stops),
        wait_max=g(j1, 'owner_world_stop_wait_max_us'),
        hset_s=(hs(j1) - hs(j0)) / dt,
        native_s=(g(j1, 'owner_native_commands') - g(j0, 'owner_native_commands')) / dt,
        main_rd_ms_s=1000 * delta(t0, t1, dt)['main']['rd'],
        main_cpu=delta(t0, t1, dt)['main']['oncpu'],
        stop_spins=g(j1, 'owner_stop_spins') - g(j0, 'owner_stop_spins'),
        stop_hits=g(j1, 'owner_stop_spin_hits') - g(j0, 'owner_stop_spin_hits'),
        stop_sleeps=g(j1, 'owner_stop_sleeps') - g(j0, 'owner_stop_sleeps'),
        bcasts=g(j1, 'owner_resume_broadcasts') - g(j0, 'owner_resume_broadcasts'))
    stop()
    return r


# 1. layout and runtime config
pid = start(a.bin, 'layout', a.main_cpu)
i = info(a.port)
res(i.get('owner_spin_owners') == str(a.n) and i.get('owner_spin_us') == '50' and i.get('owner_spin_stop_us') == '1000',
    'spin.layout.own-cores', 'owners on CPUs of their own: owner_spin_owners %s of %d, spin_us %s, stop_us %s' % (
        i.get('owner_spin_owners'), a.n, i.get('owner_spin_us'), i.get('owner_spin_stop_us')))
mt = memtier(3); mt.wait()
s0 = int(info(a.port).get('owner_spins', '0'))
cmd(a.port, 'CONFIG', 'SET', 'owner-spin-us', '0')
cmd(a.port, 'CONFIG', 'SET', 'owner-spin-stop-us', '0')
mt = memtier(3); mt.wait()
i = info(a.port)
s1 = int(i.get('owner_spins', '0'))
res(s0 > 0 and s1 == s0 and i.get('owner_spin_us') == '0', 'spin.config.off',
    'spins under load %d; after CONFIG SET owner-spin-us 0: +%d (want 0)' % (s0, s1 - s0))
cmd(a.port, 'CONFIG', 'SET', 'owner-spin-us', '50')
cmd(a.port, 'CONFIG', 'SET', 'owner-spin-stop-us', '1000')
mt = memtier(3); mt.wait()
s2 = int(info(a.port).get('owner_spins', '0'))
res(s2 > s1, 'spin.config.on', 'after CONFIG SET owner-spin-us 50: +%d spins' % (s2 - s1))
stop()
pid = start(a.bin, 'layout-shared', lo)  # main on owner 0's CPU, as ns-server.sh lays out the fleet
i = info(a.port)
res(i.get('owner_spin_owners') == str(a.n - 1), 'spin.layout.main-core',
    'main pinned to owner 0\'s CPU %d: owner_spin_owners %s (want %d)' % (lo, i.get('owner_spin_owners'), a.n - 1))
stop()

# 2. idle: owners sleep within a second of the load stopping
pid = start(a.bin, 'idle', a.main_cpu)
mt = memtier(4)
time.sleep(2)
tl0 = threads(pid); time.sleep(1); tl1 = threads(pid)
mt.wait()
t0 = threads(pid); time.sleep(1); t1 = threads(pid); time.sleep(1); t2 = threads(pid)
dl = delta(tl0, tl1, 1.0); d1 = delta(t0, t1, 1.0); d2 = delta(t1, t2, 1.0)
own = sorted(k for k in d1 if k.startswith('owner_'))
loadcpu = [round(dl[k]['oncpu'], 3) for k in own]
c1 = [round(d1[k]['oncpu'], 4) for k in own]
c2 = [round(d2[k]['oncpu'], 4) for k in own]
w2 = [d2[k]['vol'] for k in own]
print('INFO idle owner CPU under load %s; first second after %s; second second %s; wakeups/s then %s' % (
    loadcpu, c1, c2, w2), flush=True)
res(max(c1) < 0.02 and max(c2) < 0.005 and max(w2) <= 5, 'spin.idle.sleep',
    'owner CPU after the load stopped: max %.4f then %.4f of a core (want < 0.02, < 0.005), wakeups/s max %d' % (
        max(c1), max(c2), max(w2)))
stop()

# 3 + 4. world stops and main under a mixed load, candidate vs parent, twice in alternating order. The verdicts use a
# fixed native rate (64 connections x 1,500/s = 96k/s, under both designs' capacity next to the main load: at
# 2,500/s the parent reached only 109-128k of 160k), so the owners do the same
# work in both and main's share is compared fairly; the closed-loop native load is reported too (INFO): there the
# candidate's owners serve more native commands, which makes main's stops wait for busier owners.
def mixed_pair(rate, label):
    rows = {'cand': [], 'ref': []}
    for order in (('cand', 'ref'), ('ref', 'cand')):
        for who in order:
            r = mixed(a.bin if who == 'cand' else a.ref_bin, 'mixed-%s-%s-%d' % (label, who, len(rows[who])), rate)
            rows[who].append(r)
            print('INFO mixed %s %s %s' % (label, who, json.dumps({k: round(v, 2) for k, v in r.items()})), flush=True)
    return {w: {k: sum(r[k] for r in rows[w]) / len(rows[w]) for k in rows[w][0]} for w in rows}


m = mixed_pair(0, 'closed-loop')
c, p = m['cand'], m['ref']
print('INFO closed-loop native: cand %.0f/s vs parent %.0f/s, main-path HSET %.0f vs %.0f/s, stop wait %.1f vs %.1f us'
      % (c['native_s'], p['native_s'], c['hset_s'], p['hset_s'], c['wait_per_stop'], p['wait_per_stop']), flush=True)
m = mixed_pair(1500, 'fixed-96k')
c, p = m['cand'], m['ref']
res(c['wait_per_stop'] <= p['wait_per_stop'] * 1.25 + 5, 'spin.stop.latency',
    'world stop wait per stop: cand %.1f us vs parent %.1f us (max %d vs %d us); stops/s %.0f vs %.0f' % (
        c['wait_per_stop'], p['wait_per_stop'], c['wait_max'], p['wait_max'], c['stops_s'], p['stops_s']))
res(c['stop_spins'] > 0 and c['stop_hits'] > 0, 'spin.stop.spins',
    'owners waiting out main\'s stops: %d spins, %d found the world released, %d slept; resume broadcasts %d (parent: n/a)'
    % (c['stop_spins'], c['stop_hits'], c['stop_sleeps'], c['bcasts']))
res(c['hset_s'] >= p['hset_s'] * 0.95 and abs(c['native_s'] - p['native_s']) <= 0.03 * p['native_s'], 'spin.main.throughput',
    'main-path HSET/s: cand %.0f vs parent %.0f; native/s %.0f vs %.0f' % (c['hset_s'], p['hset_s'], c['native_s'],
                                                                          p['native_s']))
res(c['main_rd_ms_s'] <= p['main_rd_ms_s'] * 1.2 + 2, 'spin.main.run-delay',
    'main waiting for its CPU: cand %.2f vs parent %.2f ms/s; main CPU %.2f vs %.2f' % (
        c['main_rd_ms_s'], p['main_rd_ms_s'], c['main_cpu'], p['main_cpu']))
print('RLSPINCHECK %s fails=%d' % ('OK' if not fails else 'FAILED', fails))
sys.exit(1 if fails else 0)
