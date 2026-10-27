#!/usr/bin/env bash
# Phase 6 measurement runs, one at a time with the machine to itself, each under the resource monitor.
# Needs: Phase 3's exports and jar, and python -m deadhead.phase6_data configs/phase6.yaml (the extra fleets and ring tables).
# Usage: scripts/phase6_runs.sh [workers|fleet|ring|x1 ...]   (default: all, in that order)
set -euo pipefail
cd "$(dirname "$0")/.."
S=".venv/bin/python -m deadhead.phase3_service configs/phase6.yaml --day 2016-06-16 --monitor"
L=results/phase6/logs; mkdir -p $L
workers() { for W in 1 4; do $S --workers $W > $L/cost_W${W}.out 2>&1; done; }
fleet()   { for N in 3000 5000 9750; do $S --workers 4 --fleet $N > $L/cost_W4_N${N}.out 2>&1; done; }
ring()    { for R in 2 5; do $S --workers 4 --ring $R > $L/cost_W4_R${R}.out 2>&1; done; }
x1()      { $S --workers 4 --speed 1 --pace-from 04:00 --until 04:15 > $L/cost_W4_x1_0400.out 2>&1
            $S --workers 4 --speed 1 --pace-from 21:00 --until 21:15 > $L/cost_W4_x1_2100.out 2>&1; }
for g in ${@:-workers fleet ring x1}; do
  $g; echo "$g done $(date)"
done
