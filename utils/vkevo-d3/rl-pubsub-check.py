#!/usr/bin/env python3
"""RL-g6e-pubsub: pub/sub model check, a candidate build against a reference build (the parent RL-2d, whose owners hand
every PUBLISH to main: the stock main path). Both servers: owner-threads N, owner-busy-poll-us 0, 127.0.0.1.

Phase A (scenario, deterministic, run the same on both; every reply and push compared in order):
  6 subscriber connections (odd ones RESP3; k % 3 == 0 on main's port, the rest on owner ports, so they are guests),
  4 publishers on owner ports and 1 on main's port, an admin connection. Random steps (seeded): SUBSCRIBE /
  UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE / SSUBSCRIBE / SUNSUBSCRIBE of a subscriber; PUBLISH / SPUBLISH of a
  publisher (also 5 pipelined); PUBLISH inside MULTI / EXEC and from EVAL (main runs those); CLIENT KILL of a
  subscriber, then a fresh one in its place; RESET of a subscriber. After each step every subscriber sends PING and
  everything it got before the pong is its step's log. Then PUBSUB NUMSUB / NUMPAT / CHANNELS / SHARDCHANNELS /
  SHARDNUMSUB and the commandstats calls of the pub/sub commands.
Phase B (stress, invariants on each build): 4 publisher threads on owner ports + 1 on main's port publish <pub>:<seq>
  to st:<c> / {ss<c>} for --secs; 4 stable subscribers (SUBSCRIBE st:*, PSUBSCRIBE st:*, SSUBSCRIBE {ss*}; RESP2 and
  RESP3) must get every message exactly once, per publisher and channel in order; 2 churn subscribers subscribe and
  unsubscribe all the time (no duplicate, in order); one subscriber of kx:* is killed and replaced every 0.3 s while
  a publisher floods kx (no crash, in order); conservation: sum of PUBLISH replies on st / ss == messages received
  by stable + churn subscribers. --bg K: K load threads (SET / GET / INCR on owner ports) during both phases.
Prints PASS / FAIL lines and RLPUBSUB lines.
"""
import argparse, os, random, socket, subprocess, sys, threading, time

ap = argparse.ArgumentParser()
ap.add_argument('--bin', required=True)
ap.add_argument('--ref-bin', required=True)
ap.add_argument('--dir', required=True)
ap.add_argument('--port', type=int, default=22600)
ap.add_argument('--owner-port', type=int, default=22610)
ap.add_argument('--ref-port', type=int, default=22601)
ap.add_argument('--ref-owner-port', type=int, default=22620)
ap.add_argument('--n', type=int, default=4)
ap.add_argument('--cpus', default='52-55')
ap.add_argument('--main-cpu', default='56')
ap.add_argument('--ref-cpus', default='52-55')
ap.add_argument('--ref-main-cpu', default='58')
ap.add_argument('--seed', type=int, default=7)
ap.add_argument('--steps', type=int, default=300)
ap.add_argument('--secs', type=float, default=6)
ap.add_argument('--bg', type=int, default=0)
a = ap.parse_args()
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


class Conn:
    def __init__(self, port, resp3=False, timeout=10):
        self.s = socket.create_connection(('127.0.0.1', port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b''
        if resp3:
            self.cmd('HELLO', '3')

    def send(self, *args):
        self.s.sendall(enc(*args))

    def _line(self):
        while b'\r\n' not in self.buf:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError('eof')
            self.buf += d
        i = self.buf.index(b'\r\n')
        line, self.buf = self.buf[:i], self.buf[i + 2:]
        return line

    def _n(self, n):
        while len(self.buf) < n + 2:
            d = self.s.recv(1 << 16)
            if not d:
                raise EOFError('eof')
            self.buf += d
        v, self.buf = self.buf[:n], self.buf[n + 2:]
        return v

    def read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in (b'+', b'-', b':', b',', b'(', b'#', b'_'):
            return (t + rest).decode()
        if t in (b'$', b'='):
            n = int(rest)
            return None if n < 0 else self._n(n).decode(errors='replace')
        if t in (b'*', b'>', b'~'):
            n = int(rest)
            if n < 0:
                return None
            v = [self.read() for _ in range(n)]
            return ('>' if t == b'>' else '') and ['>'] + v or v
        if t == b'%':
            n = int(rest)
            return {str(self.read()): self.read() for _ in range(n)}
        if t == b'|':
            n = int(rest)
            for _ in range(2 * n):
                self.read()
            return self.read()
        raise ValueError('bad reply %r' % line[:60])

    def cmd(self, *args):
        self.send(*args)
        return self.read()

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def norm(v):
    """A push in RESP3 and its RESP2 array compare as the same message."""
    if isinstance(v, list) and v and v[0] == '>':
        return v[1:]
    return v


def start(bin_, port, oport, cpus, mcpu, d):
    os.makedirs(d, exist_ok=True)
    args = ['taskset', '-c', '%s,%s' % (cpus, mcpu), bin_, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
            '--appendonly', 'no', '--owner-threads', str(a.n), '--owner-port', str(oport), '--owner-cpulist', cpus,
            '--server-cpulist', mcpu, '--owner-busy-poll-us', '0', '--enable-debug-command', 'yes', '--daemonize', 'yes',
            '--dir', d, '--logfile', os.path.join(d, 'log')]
    subprocess.run(args, check=True)
    for _ in range(100):
        try:
            c = Conn(port)
            if c.cmd('PING') == '+PONG':
                c.close()
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError('server %d did not start' % port)


def stop(port):
    try:
        c = Conn(port)
        c.send('SHUTDOWN', 'NOSAVE')
        c.close()
    except OSError:
        pass
    for _ in range(100):
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            time.sleep(0.1)
        except OSError:
            return


def info(port, sec):
    c = Conn(port)
    txt = c.cmd('INFO', sec)
    c.close()
    d = {}
    for l in txt.split('\r\n'):
        if ':' in l and not l.startswith('#'):
            k, v = l.split(':', 1)
            d[k] = v
    return d


def bg_load(port, oport, stop_ev, nthreads, seed):
    def run(t):
        r = random.Random(seed * 100 + t)
        c = Conn(oport + t % a.n)
        while not stop_ev.is_set():
            k = 'bg:%d' % r.randrange(10000)
            c.send('SET', k, 'v%d' % r.randrange(1 << 30))
            c.send('GET', k)
            c.send('INCR', 'bgc:%d' % r.randrange(100))
            for _ in range(3):
                c.read()
        c.close()
    ts = [threading.Thread(target=run, args=(t,), daemon=True) for t in range(nthreads)]
    for t in ts:
        t.start()
    return ts


CH = ['ch:a', 'ch:b', 'ch:c', 'news.x', 'news.y', 'zz']
PAT = ['ch:*', 'news.*', '*x', 'z?']
SCH = ['{s0}', '{s1}', '{s2}', '{s3}']


def scenario(port, oport):
    r = random.Random(a.seed)
    log = []
    admin = Conn(port)

    def newsub(k):
        p = port if k % 3 == 0 else oport + k % a.n
        s = Conn(p, resp3=bool(k % 2))
        s.cmd('CLIENT', 'SETNAME', 'sub%d' % k)
        return s
    subs = [newsub(k) for k in range(6)]
    pubs = [Conn(oport + j % a.n, resp3=(j == 2)) for j in range(4)] + [Conn(port)]

    def sync():
        out = []
        for k, s in enumerate(subs):
            s.send('PING', 'sync')
            got = []
            while True:
                v = norm(s.read())
                if v in ('+sync', 'sync') or (isinstance(v, list) and len(v) == 2 and v[0] == 'pong'):
                    break
                got.append(v)
            out.append(got)
        return out
    for step in range(a.steps):
        x = r.random()
        if x < 0.30:
            k = r.randrange(6)
            op = r.choice(['SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE', 'SSUBSCRIBE', 'SUNSUBSCRIBE'])
            arg = r.choice(PAT) if op in ('PSUBSCRIBE', 'PUNSUBSCRIBE') else \
                r.choice(SCH) if op in ('SSUBSCRIBE', 'SUNSUBSCRIBE') else r.choice(CH)
            subs[k].send(op, arg)
            ev = ('sub', k, op, arg)
        elif x < 0.80:
            j = r.randrange(5)
            if r.random() < 0.25:
                ch = r.choice(SCH)
                ev = ('spub', j, ch, pubs[j].cmd('SPUBLISH', ch, 'm%d' % step))
            else:
                ch = r.choice(CH + ['ch:%d' % r.randrange(3)])
                ev = ('pub', j, ch, pubs[j].cmd('PUBLISH', ch, 'm%d' % step))
        elif x < 0.87:
            j = r.randrange(5)
            for q in range(5):
                if q % 2:
                    pubs[j].send('SPUBLISH', r.choice(SCH), 'p%d.%d' % (step, q))
                else:
                    pubs[j].send('PUBLISH', r.choice(CH), 'p%d.%d' % (step, q))
            ev = ('pipe', j, [pubs[j].read() for _ in range(5)])
        elif x < 0.91:
            j = r.randrange(5)
            pubs[j].send('MULTI')
            pubs[j].send('PUBLISH', r.choice(CH), 'x%d' % step)
            pubs[j].send('SET', 'k%d' % j, step)
            pubs[j].send('EXEC')
            ev = ('multi', j, [pubs[j].read() for _ in range(4)])
        elif x < 0.94:
            j = r.randrange(5)
            ev = ('eval', j, pubs[j].cmd('EVAL', "return redis.call('publish', KEYS[1], ARGV[1])", '1', r.choice(CH),
                                          'e%d' % step))
        elif x < 0.97:
            k = r.randrange(6)
            cid = subs[k].cmd('CLIENT', 'ID') if False else None
            lst = admin.cmd('CLIENT', 'LIST')
            ids = [l.split(' ')[0].split('=')[1] for l in lst.split('\n') if ' name=sub%d ' % k in l]
            n = admin.cmd('CLIENT', 'KILL', 'ID', ids[0]) if ids else None
            # drain what reached the killed socket, then a fresh subscriber
            try:
                while True:
                    subs[k].read()
            except (EOFError, OSError, ValueError):
                pass
            subs[k].close()
            subs[k] = newsub(k)
            ev = ('kill', k, n)
        else:
            k = r.randrange(6)
            ev = ('reset', k, subs[k].cmd('RESET'))
            subs[k].cmd('CLIENT', 'SETNAME', 'sub%d' % k)
            if k % 2:
                subs[k].cmd('HELLO', '3')
        log.append((step, ev, sync()))
    intro = [admin.cmd('PUBSUB', 'NUMSUB', *CH), admin.cmd('PUBSUB', 'NUMPAT'),
             sorted(admin.cmd('PUBSUB', 'CHANNELS')), sorted(admin.cmd('PUBSUB', 'SHARDCHANNELS')),
             admin.cmd('PUBSUB', 'SHARDNUMSUB', *SCH)]
    for s in subs + pubs + [admin]:
        s.close()
    st = info(port, 'commandstats')
    calls = {k: v.split(',')[0] for k, v in st.items()
             if any(x in k for x in ('publish', 'subscribe', 'pubsub'))}
    return log, intro, calls


def stress(port, oport, tag):
    stop_ev = threading.Event()
    sent = {}  # (pub, kind, ch) -> list of seq
    replies = {'st': 0, 'ss': 0}
    lock = threading.Lock()
    NC = 6

    def reader(conn, out, stop_after):
        try:
            while True:
                v = norm(conn.read())
                if isinstance(v, list) and v and v[0] in ('message', 'pmessage', 'smessage'):
                    out.append((v[0], v[-2], v[-1]))
                elif v in ('+sync', 'sync') or (isinstance(v, list) and len(v) == 2 and v[0] == 'pong'):
                    if stop_after.is_set():
                        return
        except (EOFError, OSError, ValueError):
            return
    stable, got_stable, ths = [], [], []
    done_ev = threading.Event()
    for k in range(4):
        s = Conn(port if k == 0 else oport + k % a.n, resp3=bool(k % 2))
        s.send('SUBSCRIBE', *['st:%d' % c for c in range(NC)])
        s.send('PSUBSCRIBE', 'st:*')
        s.send('SSUBSCRIBE', *['{ss%d}' % c for c in range(1)])
        for c in range(1, NC):
            s.send('SSUBSCRIBE', '{ss%d}' % c)
        n_ack = NC + 1 + NC
        for _ in range(n_ack):
            s.read()
        out = []
        stable.append(s)
        got_stable.append(out)
        t = threading.Thread(target=reader, args=(s, out, done_ev), daemon=True)
        t.start()
        ths.append(t)
    churn_out = [[], []]

    def churn(i):
        r = random.Random(a.seed + 50 + i)
        s = Conn(oport + i % a.n, resp3=bool(i))
        out = churn_out[i]
        subscribed = False
        while not stop_ev.is_set():
            if subscribed:
                s.send('UNSUBSCRIBE')
                s.send('SUNSUBSCRIBE')
            else:
                s.send('SUBSCRIBE', *['st:%d' % c for c in range(NC) if r.random() < 0.5] or ['st:0'])
                s.send('SSUBSCRIBE', '{ss%d}' % r.randrange(NC))
            subscribed = not subscribed
            # read until both acks of this round came (count subscribe / unsubscribe kinds of replies)
            s.send('PING', 'sync')
            while True:
                v = norm(s.read())
                if isinstance(v, list) and v and v[0] in ('message', 'pmessage', 'smessage'):
                    out.append((v[0], v[-2], v[-1]))
                elif v in ('+sync', 'sync') or (isinstance(v, list) and len(v) == 2 and v[0] == 'pong'):
                    break
            time.sleep(r.random() * 0.01)
        s.send('UNSUBSCRIBE')
        s.send('SUNSUBSCRIBE')
        s.send('PING', 'sync')
        while True:
            v = norm(s.read())
            if isinstance(v, list) and v and v[0] in ('message', 'pmessage', 'smessage'):
                out.append((v[0], v[-2], v[-1]))
            elif v in ('+sync', 'sync') or (isinstance(v, list) and len(v) == 2 and v[0] == 'pong'):
                break
        s.close()
    kill_out = []

    def killer():
        admin = Conn(port)
        while not stop_ev.is_set():
            s = Conn(oport, resp3=False)
            s.cmd('CLIENT', 'SETNAME', 'victim')
            s.send('PSUBSCRIBE', 'kx:*')
            s.read()
            out = []
            t = threading.Thread(target=reader, args=(s, out, threading.Event()), daemon=True)
            t.start()
            time.sleep(0.3)
            lst = admin.cmd('CLIENT', 'LIST')
            ids = [l.split(' ')[0].split('=')[1] for l in lst.split('\n') if ' name=victim ' in l]
            if ids:
                admin.cmd('CLIENT', 'KILL', 'ID', ids[0])
            t.join(5)
            s.close()
            kill_out.append(out)
        admin.close()

    def publisher(j):
        r = random.Random(a.seed * 7 + j)
        p = Conn(port if j == 4 else oport + j % a.n, resp3=(j == 1))
        seq = 0
        while not stop_ev.is_set():
            batch = []
            for _ in range(8):
                seq += 1
                x = r.random()
                if x < 0.6:
                    ch, kind = 'st:%d' % r.randrange(NC), 'st'
                    p.send('PUBLISH', ch, '%d:%d' % (j, seq))
                elif x < 0.9:
                    ch, kind = '{ss%d}' % r.randrange(NC), 'ss'
                    p.send('SPUBLISH', ch, '%d:%d' % (j, seq))
                else:
                    ch, kind = 'kx:%d' % r.randrange(3), 'kx'
                    p.send('PUBLISH', ch, '%d:%d' % (j, seq))
                batch.append((kind, ch, seq))
            for kind, ch, sq in batch:
                v = p.read()
                with lock:
                    sent.setdefault((j, kind, ch), []).append(sq)
                    if kind in replies:
                        replies[kind] += int(v[1:])
        p.close()
    bg = bg_load(port, oport, stop_ev, a.bg, a.seed) if a.bg else []
    workers = [threading.Thread(target=publisher, args=(j,), daemon=True) for j in range(5)] + \
              [threading.Thread(target=churn, args=(i,), daemon=True) for i in range(2)] + \
              [threading.Thread(target=killer, daemon=True)]
    for t in workers:
        t.start()
    time.sleep(a.secs)
    stop_ev.set()
    for t in workers:
        t.join(30)
    done_ev.set()
    for s in stable:
        s.send('PING', 'sync')
    for t in ths:
        t.join(30)
    for t in bg:
        t.join(10)
    for s in stable:
        s.close()
    # every stable subscriber: each st / ss message exactly once per matching subscription, in order
    ok_once, ok_order, bad = True, True, ''
    for k, out in enumerate(got_stable):
        per = {}
        for typ, ch, m in out:
            pj, sq = m.split(':')
            per.setdefault((typ, int(pj), ch), []).append(int(sq))
        for (j, kind, ch), seqs in sent.items():
            if kind == 'kx':
                continue
            for typ in (('message', 'pmessage') if kind == 'st' else ('smessage',)):
                g = per.get((typ, j, ch), [])
                if g != seqs:
                    if sorted(g) != sorted(set(seqs)):
                        ok_once = False
                    else:
                        ok_order = False
                    bad = bad or 'sub %d %s %s pub %d: got %d of %d' % (k, typ, ch, j, len(g), len(seqs))
    res(ok_once, '%s.stress.stable_exactly_once' % tag, bad)
    res(ok_order, '%s.stress.stable_order' % tag)
    # churn and killed subscribers: no duplicate, per publisher and channel in order
    def ordered(outs):
        for out in outs:
            last = {}
            for typ, ch, m in out:
                pj, sq = m.split(':')
                key = (typ, pj, ch)
                if last.get(key, -1) >= int(sq):
                    return False
                last[key] = int(sq)
        return True
    res(ordered(churn_out), '%s.stress.churn_order_no_dup' % tag, 'churn msgs %d' % sum(len(o) for o in churn_out))
    res(ordered(kill_out), '%s.stress.killed_order_no_dup' % tag, 'victims %d msgs %d' % (len(kill_out), sum(len(o) for o in kill_out)))
    recv = sum(len(o) for o in got_stable) + sum(len(o) for o in churn_out)
    res(recv == replies['st'] + replies['ss'], '%s.stress.conservation' % tag,
        'replies %d received %d published %d' % (replies['st'] + replies['ss'], recv, sum(len(v) for v in sent.values())))
    return sum(len(v) for v in sent.values())


def main():
    os.makedirs(a.dir, exist_ok=True)
    results = {}
    for tag, bin_, port, oport, cpus, mcpu in (('cand', a.bin, a.port, a.owner_port, a.cpus, a.main_cpu),
                                                ('ref', a.ref_bin, a.ref_port, a.ref_owner_port, a.ref_cpus, a.ref_main_cpu)):
        start(bin_, port, oport, cpus, mcpu, os.path.join(a.dir, tag))
        try:
            stop_ev = threading.Event()
            bg = bg_load(port, oport, stop_ev, a.bg, a.seed) if a.bg else []
            t0 = time.time()
            results[tag] = scenario(port, oport)
            stop_ev.set()
            for t in bg:
                t.join(10)
            ta = time.time() - t0
            npub = stress(port, oport, tag)
            ow = info(port, 'owners')
            print('RLPUBSUB %s scenario %.1f s, stress published %d; %s' % (
                tag, ta, npub, ' '.join('%s=%s' % (k, v) for k, v in ow.items() if 'pubsub' in k or k in (
                    'owner_handoffs_to_main', 'owner_world_stops', 'owner_native_commands'))), flush=True)
            if tag == 'cand':
                res(ow.get('owner_pubsub_mismatch', '0') == '0', 'cand.owner_pubsub_mismatch_0', ow.get('owner_pubsub_mismatch'))
                res(int(ow.get('owner_pubsub_native', '0')) > 0, 'cand.native_publishes_ran', ow.get('owner_pubsub_native'))
        finally:
            stop(port)
            crash = subprocess.run(['grep', '-cE', 'ASSERT|crashed|Guru|BUG REPORT', os.path.join(a.dir, tag, 'log')],
                                   capture_output=True, text=True).stdout.strip()
            res(crash == '0', '%s.no_crash' % tag, crash)
    (la, ia, ca), (lb, ib, cb) = results['cand'], results['ref']
    first = next((i for i, (x, y) in enumerate(zip(la, lb)) if x != y), None)
    res(len(la) == len(lb) and first is None, 'scenario.replies_and_pushes_equal',
        'steps %d' % len(la) + ('' if first is None else ' first diff at step %d: cand %r ref %r' % (
            first, str(la[first])[:400], str(lb[first])[:400])))
    pushes = sum(len(g) for _, _, sy in la for g in sy)
    pubs = sum(1 for _, ev, _ in la if ev[0] in ('pub', 'spub'))
    print('RLPUBSUB scenario steps %d publishes %d pushes received %d' % (len(la), pubs, pushes), flush=True)
    res(ia == ib, 'scenario.pubsub_introspection_equal', str(ia)[:200])
    res(ca == cb, 'scenario.commandstats_equal', str(sorted(ca.items()))[:300])
    print('RLPUBSUB done fails=%d' % len(FAILS), flush=True)
    sys.exit(1 if FAILS else 0)


if __name__ == '__main__':
    main()
