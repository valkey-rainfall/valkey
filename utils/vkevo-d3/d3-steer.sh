#!/usr/bin/env bash
# D3 NIC flow steering (run as root on the server host). Each owner port gets its own NIC queue:
#   RX: ethtool ntuple rule  "tcp4 dst-port BASE+i -> queue (i mod NQ)"  (ENA 2.17 on c8gn accepts
#       rules only with an explicit loc; rule i is placed at loc LOC0+i).
#   TX: XPS by RX queue (tx-q/xps_rxqs = rx-q), so replies leave on the queue the request came in on
#       and TX completions run on the same IRQ CPU.
# The IRQ -> CPU layout is NOT changed (bench layout: queue q IRQ on CPU 95-q, off the worker set).
# Same mechanism as D1's d1-steer.sh; the owner ports start at 6400 (owner-port).
#
# Usage:
#   d3-steer.sh apply   --n N [--base-port 6400] [--dev auto] [--loc0 0]
#   d3-steer.sh restore [--dev auto]       # delete the rules apply added, clear xps_rxqs (D0 layout)
#   d3-steer.sh status  [--dev auto]       # rules + queue -> IRQ CPU
#   d3-steer.sh verify  [--dev auto] [--secs 5]   # per-queue RX packet deltas while traffic runs
# Limit: one queue per port. At N < NQ only N queues (and N IRQ CPUs) carry owner traffic; at N > NQ
# ports share queues round-robin (N=62 on 32 queues: 2 ports per queue on 30 of them).
set -euo pipefail

cmd=${1:-}; shift || true
N=0; BASE=6400; DEV=auto; LOC0=0; SECS=5
while [ $# -gt 0 ]; do
  case $1 in
    --n) N=$2; shift 2 ;;
    --base-port) BASE=$2; shift 2 ;;
    --dev) DEV=$2; shift 2 ;;
    --loc0) LOC0=$2; shift 2 ;;
    --secs) SECS=$2; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done
[ "$DEV" = auto ] && DEV=$(ip -o route get 8.8.8.8 | awk '{for (i = 1; i < NF; i++) if ($i == "dev") print $(i + 1)}')
STATE=/run/vkevo-d3-steer.$DEV
NQ=$(ls -d /sys/class/net/"$DEV"/queues/rx-* | wc -l)

# hex bitmap with one bit set, in the comma-separated 32-bit word format sysfs uses
bitmap() {
  local bit=$1 nbits=$2 words out="" w
  words=$(((nbits + 31) / 32))
  for ((w = words - 1; w >= 0; w--)); do
    if ((bit / 32 == w)); then out+=$(printf '%08x' $((1 << (bit % 32)))); else out+=00000000; fi
    ((w > 0)) && out+=,
  done
  echo "$out"
}
irq_cpu() { # queue -> IRQ CPU list (ENA names its vectors <dev>-Tx-Rx-<q>)
  local irq; irq=$(awk -v n="$DEV-Tx-Rx-$1" '$NF == n {sub(":", "", $1); print $1}' /proc/interrupts)
  [ -n "$irq" ] && cat /proc/irq/"$irq"/smp_affinity_list || echo "?"
}

case $cmd in
apply)
  [ "$N" -gt 0 ] || { echo "--n required" >&2; exit 2; }
  [ -e "$STATE" ] && { echo "steering already applied ($STATE); run restore first" >&2; exit 1; }
  ethtool -K "$DEV" ntuple on
  : > "$STATE.tmp"
  for ((i = 0; i < N; i++)); do
    p=$((BASE + i)); q=$((i % NQ)); loc=$((LOC0 + i))
    ethtool -N "$DEV" flow-type tcp4 dst-port "$p" action "$q" loc "$loc"
    echo "$loc" >> "$STATE.tmp"
  done
  for ((q = 0; q < NQ; q++)); do bitmap "$q" "$NQ" > /sys/class/net/"$DEV"/queues/tx-"$q"/xps_rxqs; done
  mv "$STATE.tmp" "$STATE"
  # read back: every rule must be present with its queue
  have=$(ethtool -n "$DEV" | grep -c '^Filter:' || true)
  [ "$have" -ge "$N" ] || { echo "only $have rules after apply (want $N)" >&2; exit 1; }
  echo "applied $N rules on $DEV ($NQ queues): port $BASE+i -> queue i mod $NQ; xps_rxqs tx-q <- rx-q"
  for ((i = 0; i < N && i < 64; i++)); do q=$((i % NQ)); printf '  %d->q%d(cpu %s)' $((BASE + i)) "$q" "$(irq_cpu $q)"; done
  echo
  ;;
restore)
  if [ -e "$STATE" ]; then
    while read -r loc; do ethtool -N "$DEV" delete "$loc" 2>/dev/null || true; done < "$STATE"
    rm -f "$STATE"
  fi
  for ((q = 0; q < NQ; q++)); do echo 0 > /sys/class/net/"$DEV"/queues/tx-"$q"/xps_rxqs; done
  echo "restored $DEV: $(ethtool -n "$DEV" | grep -c '^Filter:' || true) ntuple rules left, xps_rxqs cleared"
  ;;
status)
  ethtool -n "$DEV" | awk '/^Filter:/{f=$2} /Dest port:/{p=$3} /Action:/{print "  loc " f " dst-port " p " -> queue " $NF}'
  for ((q = 0; q < NQ; q++)); do printf 'q%d:cpu%s:xps_rxqs=%s ' "$q" "$(irq_cpu $q)" "$(cat /sys/class/net/"$DEV"/queues/tx-"$q"/xps_rxqs)"; done
  echo
  ;;
verify)
  snap() { ethtool -S "$DEV" | awk '/queue_[0-9]+_rx_cnt:/{sub("queue_", "", $1); sub("_rx_cnt:", "", $1); print $1, $2}' | sort; }
  a=$(snap); sleep "$SECS"; b=$(snap)
  join <(echo "$a") <(echo "$b") | awk -v s="$SECS" '{d = $3 - $2; t += d; q[$1] = d} END {
    for (i in q) printf "q%s %.0f pkt/s (%.1f%%)\n", i, q[i] / s, t ? 100 * q[i] / t : 0 }' | sort -V
  ;;
*) sed -n '2,15p' "$0"; exit 2 ;;
esac
