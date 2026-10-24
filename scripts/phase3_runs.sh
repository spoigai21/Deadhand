#!/usr/bin/env bash
# Phase 3 measurement runs, one at a time so the throughput figures share the machine with nothing else.
# Needs: the reference oracle ledgers (scripts/phase3_oracle_runs.sh), the exports (deadhead.phase3_data), the jar (cd consumer && gradle jar).
# Usage: scripts/phase3_runs.sh [throughput|resolution|weekend|latency|kill ...]   (default: all, in that order)
set -euo pipefail
cd "$(dirname "$0")/.."
S=".venv/bin/python -m deadhead.phase3_service configs/phase3.yaml"
L=results/phase3/logs; mkdir -p $L
throughput() { for W in 1 2 4 8; do $S --day 2016-06-16 --workers $W > $L/svc_W${W}_k6.out 2>&1; done; }
resolution() { for W in 4 8; do $S --day 2016-06-16 --workers $W --key-res 7 > $L/svc_W${W}_k7.out 2>&1; done; }
weekend()    { $S --day 2016-06-18 --workers 4 > $L/svc_W4_k6_sat.out 2>&1; }
latency()    { $S --day 2016-06-16 --workers 4 --speed 1 --pace-from 21:00 --until 21:15 > $L/svc_W4_x1.out 2>&1; }
kill_test()  { for A in range cooperative; do for SEED in 0 1 2 3 4; do
                 H=$(printf "%02d:%02d" $((9 + SEED / 2)) $(( (SEED % 2) * 30 )))
                 $S --day 2016-06-16 --workers 4 --seed $SEED --assignor $A --kill-at $H --until 12:00 > $L/svc_kill_${A}_seed${SEED}.out 2>&1
               done; done; }
for g in "${@:-throughput resolution weekend latency kill}"; do
  for x in $g; do case $x in kill) kill_test ;; *) $x ;; esac; echo "$x done $(date)"; done
done
