#!/usr/bin/env bash
# run_probe.sh — collect jitter probe data at multiple load levels.
# Usage: run_probe.sh <label> <outdir> [reps] [load_levels...]
# Defaults: reps=5, load levels 0 4 16 32.
# Writes <outdir>/<label>-load<L>-rep<R>.json
set -euo pipefail

LABEL="${1:?usage: run_probe.sh <label> <outdir> [reps] [loads...]}"
OUTDIR="${2:?usage: run_probe.sh <label> <outdir> [reps] [loads...]}"
REPS="${3:-5}"
shift $(( $# >= 3 ? 3 : 2 ))
LOADS=("$@")
[ ${#LOADS[@]} -eq 0 ] && LOADS=(0 4 16 32)

HERE="$(cd "$(dirname "$0")" && pwd)"
PROBE="$HERE/jitter_probe"
[ -x "$PROBE" ] || gcc -O2 -Wall -Wextra -Werror -o "$PROBE" "$HERE/jitter_probe.c"

mkdir -p "$OUTDIR"
for load in "${LOADS[@]}"; do
  for rep in $(seq 1 "$REPS"); do
    out="$OUTDIR/${LABEL}-load${load}-rep${rep}.json"
    "$PROBE" --load "$load" --label "${LABEL}-load${load}-rep${rep}" > "$out"
    echo "wrote $out"
    sleep 1   # let the box settle between reps
  done
done
echo "DONE: $(ls "$OUTDIR"/${LABEL}-*.json | wc -l) files"
