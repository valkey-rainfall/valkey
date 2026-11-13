#!/usr/bin/env bash
# Train the GCC profile that src/Makefile uses (pgo/gcc, see PGO there).
#
# Usage, from the repository root:
#   BENCH=/path/to/valkey-benchmark utils/pgo-train.sh [seconds-per-step]
#
# BENCH must not be instrumented (use a plain build from another tree). It
# needs __rand_beg__ and ';' support (valkey-benchmark from late 2026).
# The script builds with PGO=gen, runs the training load against 127.0.0.1,
# optionally runs part of the test suite (PGO_TESTS=0 skips it), moves the
# counters to pgo/gcc and leaves the tree clean. Commit pgo/gcc afterwards.
# The profile only fits the exact sources and compiler it was trained with.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${BENCH:?set BENCH to a valkey-benchmark that is not instrumented}"
T=${1:-20}
PORT=${PGO_PORT:-27100}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

rm -rf pgo/gen
make distclean > /dev/null
make -j"$(nproc)" PGO=gen > "$WORK/build.log" 2>&1 || { tail -30 "$WORK/build.log"; exit 1; }

PID=
srv() {
    src/valkey-server --port "$PORT" --bind 127.0.0.1 --save '' --appendonly no \
        --dir "$WORK" "$@" > "$WORK/server.log" 2>&1 &
    PID=$!
    for _ in $(seq 100); do
        "$BENCH" -p "$PORT" -n 1 -c 1 -q -t ping > /dev/null 2>&1 && return 0
        sleep 0.1
    done
    echo "server did not start: $*" >&2
    tail -5 "$WORK/server.log" >&2
    exit 1
}
stop() { kill -TERM "$PID"; wait "$PID" || true; }
b() { "$BENCH" -p "$PORT" -q "$@" > /dev/null 2>&1 || true; }
run() { timeout "$T" "$BENCH" -p "$PORT" -q -n 2000000000 "$@" > /dev/null 2>&1 || true; }
half() { timeout $((T / 2)) "$BENCH" -p "$PORT" -q -n 2000000000 "$@" > /dev/null 2>&1 || true; }
K=(-r 3000000 --threads 16)

echo "GET shapes (scorecard K1, K3, K5, K7)"
srv --io-threads 8 --io-threads-epoll-thread yes
b -t set "${K[@]}" -n 3000000 -d 16 -P 64 -c 200
run -t get "${K[@]}" -d 16 -P 64 -c 400
run -t get "${K[@]}" -d 16 -P 10 -c 400
run -t get "${K[@]}" -d 16 -P 1 -c 2000
b -t set -r 100000 -n 200000 -d 4096 -P 16 -c 100
half -t get -r 100000 -d 4096 -P 10 -c 400
stop

echo "SET and mixed shapes with write lanes forced (K2, K3, K4, K6)"
srv --io-threads 8 --io-threads-epoll-thread yes --io-threads-parallel-writes-min 1
run -t set "${K[@]}" -d 16 -P 64 -c 400
run -t set "${K[@]}" -d 16 -P 1 -c 2000
b -t set "${K[@]}" -n 3000000 -d 64 -P 64 -c 200
run -t get "${K[@]}" -d 64 -P 10 -c 400
run -t set "${K[@]}" -d 16 -P 10 -c 100 &
BG=$!
run -t get "${K[@]}" -d 16 -P 10 -c 400
wait "$BG" || true
stop

echo "Data types at io-threads 4, shipped defaults (R01-R12)"
srv --io-threads 4
for t in sadd hset zadd lpush rpush lpop rpop incr mset xadd spop zpopmin; do
    half -t "$t" -r 100000 -P 10 -c 200
done
b -t zadd -r 100000 -n 200000 -P 10 -c 50
b -t sadd -r 100000 -n 200000 -P 10 -c 50
b -t set -r 100000 -n 200000 -P 10 -c 50
half -r 100000 -P 10 -c 200 -- ZINCRBY myzset 1 element:__rand_int__
half -r 100000 -P 10 -c 200 -- ZRANK myzset element:__rand_int__
half -r 100000 -P 10 -c 200 --count 100 -- ZRANGE myzset __rand_beg__ __rand_end__
half -r 100000 -P 10 -c 200 --count 100 -- ZRANGEBYSCORE myzset __rand_beg__ __rand_end__
half -r 100000 -P 10 -c 200 -- ZADD myzset __rand_int__ element:__rand_int__ ';' ZREM myzset element:__rand_int__
half -r 100000 -P 10 -c 200 -- ZPOPMIN myzset ';' ZADD myzset __rand_int__ nm:__rand_int__
half -r 100000 -P 10 -c 200 -- SISMEMBER myset element:__rand_int__
half -r 100000 -P 10 -c 200 -- MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__
half -t ping -P 10 -c 200
half -t get -r 100000 -P 1 -c 400
stop

echo "io-threads 1 and 2 (R13-R16)"
for n in 1 2; do
    srv --io-threads "$n"
    b -t set -r 3000000 -n 1000000 -d 16 -P 64 -c 100
    half -t get -r 3000000 -d 16 -P 10 -c 400
    half -t set -r 3000000 -d 16 -P 10 -c 400
    stop
done

echo "Eviction, AOF, BGSAVE, expiry (R17-R20)"
srv --io-threads 4 --maxmemory 256mb --maxmemory-policy allkeys-lru
half -t set -r 3000000 -d 128 -P 10 -c 400
stop
srv --io-threads 4 --appendonly yes --appendfsync everysec
half -t set -r 3000000 -d 16 -P 10 -c 400
half -r 3000000 -P 10 -c 400 -- SET key:__rand_int__ 0123456789abcdef EX 3
run -t get -r 3000000 -P 10 -c 400 &
BG=$!
sleep $((T / 3))
b -n 1 -c 1 -- BGSAVE
b -n 1 -c 1 -- BGREWRITEAOF
wait "$BG" || true
stop

if [ "${PGO_TESTS:-1}" = 1 ]; then
    echo "Test suite subset (gives rare paths a nonzero count)"
    ./runtest --clients 16 --baseport $((PORT + 100)) --portcount 2000 \
        --single unit/type/list --single unit/type/hash --single unit/type/set \
        --single unit/type/zset --single unit/type/string --single unit/type/incr \
        --single unit/type/stream --single unit/expire --single unit/multi \
        --single unit/scripting --single unit/functions --single unit/pubsub \
        --single unit/maxmemory --single unit/keyspace --single unit/bitops \
        --single unit/hyperloglog --single unit/geo --single unit/acl \
        --single unit/tracking --single unit/info --single unit/other \
        --single integration/replication-4 --single integration/aof \
        > "$WORK/runtest.log" 2>&1 || true
    grep -E '^\[(ok|err)\]' "$WORK/runtest.log" | cut -c1-5 | sort | uniq -c
fi

rm -rf pgo/gcc
mv pgo/gen pgo/gcc
make distclean > /dev/null
echo "profile: $(find pgo/gcc -name '*.gcda' | wc -l) files, $(du -sh pgo/gcc | cut -f1)"
