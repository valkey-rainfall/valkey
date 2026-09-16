#!/usr/bin/env bash
# tsan-hist.sh <tcl.log> -> "count func file:line" of the first non-runtime frame of each report
awk '/WARNING: ThreadSanitizer: data race/{grab=1; next}
     grab && /^ *#[0-9]+ /{ if ($0 !~ /libtsan|interceptor/) { sub(/^ *#[0-9]+ /,""); split($0,a," "); print a[1]" "a[2]; grab=0 } }' "$1" \
  | sed 's#/local/home/rainval/valkey-worktrees/read-offload-port-tsan/##' | sort | uniq -c | sort -rn
