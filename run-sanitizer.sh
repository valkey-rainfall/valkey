#!/usr/bin/env bash
# Sanitizer pass for the read-offload port. Usage: run-sanitizer.sh <tsan|asan> [<sha> <label>]
# Runs in the matching worktree; logs land in <worktree>/sanitizer-logs[-<label>]/.
# With <sha> <label>, the worktree is checked out at <sha> first (e.g. the untouched base
# for a pre-existing-race baseline).
set -u
kind="$1"
sha="${2:-}"
label="${3:-}"
wt="/local/home/rainval/valkey-worktrees/read-offload-port-$kind"
logdir="$wt/sanitizer-logs${label:+-$label}"
mkdir -p "$logdir"
cd "$wt" || exit 1
if [ -n "$sha" ]; then
  git checkout -q -- tests/support/server.tcl
  git checkout -q --detach "$sha" || { echo "checkout $sha failed" | tee "$logdir/status.txt"; exit 1; }
  # Stale .d dependency files from the other SHA's build reference headers that may not exist here.
  make distclean > /dev/null 2>&1
fi

case "$kind" in
  tsan) san=thread;  harness_port=21079; base_port=21111 ;;   # upstream defaults
  asan) san=address; harness_port=23079; base_port=23111 ;;   # disjoint so both can run at once
  *) echo "unknown kind $kind"; exit 1 ;;
esac

echo "[$(date -u +%FT%TZ)] tip $(git rev-parse --short HEAD) kind=$kind" | tee "$logdir/status.txt"

# Harness bump: default io-threads 2 is below the offload gate (>2), exercises nothing.
sed -i 's/dict set config "io-threads" 2$/dict set config "io-threads" 4/' tests/support/server.tcl
grep -q '"io-threads" 4' tests/support/server.tcl || { echo "harness bump failed" | tee -a "$logdir/status.txt"; exit 1; }

echo "[$(date -u +%FT%TZ)] building" | tee -a "$logdir/status.txt"
make -j8 SANITIZER=$san > "$logdir/build.log" 2>&1 || { echo "BUILD FAILED" | tee -a "$logdir/status.txt"; exit 1; }
make -j8 -C tests/modules > "$logdir/build-modules.log" 2>&1 || echo "module build failed (moduleapi units will be skipped)" | tee -a "$logdir/status.txt"

units=(
  unit/type/string unit/type/incr unit/type/list unit/type/list-2 unit/type/list-3
  unit/type/set unit/type/zset unit/type/hash unit/type/stream unit/type/stream-cgroups
  unit/expire unit/networking unit/introspection unit/introspection-2 unit/keyspace
  unit/multi unit/tracking unit/scan unit/scripting unit/functions unit/pubsub
  unit/pubsubshard unit/info unit/info-command unit/latency-monitor unit/maxmemory
  unit/bitops unit/geo unit/hyperloglog unit/sort unit/dump unit/lazyfree unit/querybuf
  unit/protocol unit/auth unit/acl unit/client-eviction
  unit/moduleapi/keyspace_events unit/moduleapi/misc unit/moduleapi/datatype
  unit/moduleapi/hooks unit/moduleapi/commandfilter unit/moduleapi/blockedclient
  unit/moduleapi/scan
)
args=()
for u in "${units[@]}"; do args+=(--single "$u"); done

# Collect every distinct report rather than halting on the first: with -fno-sanitize-recover
# the server still aborts on a report, but the harness moves on to the next test, so we see
# the full set. Known-benign upstream races are suppressed via tsan.supp.
export TSAN_OPTIONS="halt_on_error=0 second_deadlock_stack=1 history_size=7 suppressions=/local/home/rainval/valkey-worktrees/read-offload-port/tsan.supp"
export ASAN_OPTIONS="detect_leaks=0 abort_on_error=1"
# gcc's sanitizer runtimes ship only as libXsan.so.N.0.0 here (no soname symlink, runtime
# RPMs not installed); ~/.local/lib/sanitizer-rt holds the libtsan.so.0 / libasan.so.6 links.
export LD_LIBRARY_PATH="$HOME/.local/lib/sanitizer-rt${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if ldd src/valkey-server | grep -q 'not found'; then
  { echo "unresolved shared libs:"; ldd src/valkey-server | grep 'not found'; } | tee -a "$logdir/status.txt"; exit 1
fi

echo "[$(date -u +%FT%TZ)] running $((${#units[@]})) units, cluster-mode, io-threads 4" | tee -a "$logdir/status.txt"
./runtest --cluster-mode --clients 4 --timeout 2400 --dont-clean --port "$harness_port" --baseport "$base_port" "${args[@]}" > "$logdir/tcl.log" 2>&1
rc=$?
echo "[$(date -u +%FT%TZ)] runtest exit=$rc" | tee -a "$logdir/status.txt"
grep -E '^\[(ok|err|exception)\]|All tests passed|WARNING: ThreadSanitizer|ERROR: AddressSanitizer|crashed by signal|ASSERTION FAILED' "$logdir/tcl.log" \
  | awk '/^\[ok\]/{ok++} /^\[err\]/{err++} /^\[exception\]/{exc++} /ThreadSanitizer|AddressSanitizer|crashed|ASSERTION/{san++} END{printf "ok=%d err=%d exception=%d sanitizer_hits=%d\n", ok, err, exc, san}' \
  | tee -a "$logdir/status.txt"
grep -E '^\[err\]|^\[exception\]|WARNING: ThreadSanitizer|ERROR: AddressSanitizer' "$logdir/tcl.log" | head -40 > "$logdir/failures.txt"
echo "[$(date -u +%FT%TZ)] DONE" | tee -a "$logdir/status.txt"
