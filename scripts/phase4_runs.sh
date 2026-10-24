#!/usr/bin/env bash
# Phase 4 measurement runs: the batched oracle, three replays at a time (about an hour per batch of three on the M5), then the
# service with the window. W = 0 at ring 3 is the Phase 3 reference oracle (data/phase3/assignments_ref_*), not re-run.
# Usage: scripts/phase4_runs.sh [sweep|operating|radius|weekend|service ...]   OP=<W> picks the operating point (default 10)
set -euo pipefail
cd "$(dirname "$0")/.."
OP=${OP:-10}
L=results/phase4/logs; mkdir -p $L
B=".venv/bin/python -m deadhead.batch configs/phase4.yaml"
S=".venv/bin/python -m deadhead.phase3_service configs/phase4.yaml"
run() { $B --day "$1" --window "$2" ${3:+--ring $3} ${4:+--seed $4} ${5:+$5} > "$L/batch_$1_W$2_R${3:-3}_seed${4:-0}${5:+_noroll}.log" 2>&1; }
sweep()     { run 2016-06-16 2 & run 2016-06-16 5 & run 2016-06-16 10 & wait
              run 2016-06-16 20 & run 2016-06-16 30 & run 2016-06-16 60 & wait; }
operating() { run 2016-06-16 $OP 3 1 & run 2016-06-16 $OP 3 2 & run 2016-06-16 $OP 3 3 & wait
              run 2016-06-16 $OP 3 4 & run 2016-06-16 $OP 3 0 --no-roll & run 2016-06-16 2 3 0 --no-roll & wait; }
radius()    { run 2016-06-16 0 2 & run 2016-06-16 0 5 & run 2016-06-16 $OP 2 & wait
              run 2016-06-16 $OP 5 & wait; }
weekend()   { run 2016-06-18 5 & run 2016-06-18 $OP & run 2016-06-18 30 & wait; }
service()   { $S --day 2016-06-16 --workers 4 --window $OP > $L/svc_W4_k6_w$OP.out 2>&1; }
for g in "${@:-sweep operating radius weekend service}"; do
  for x in $g; do $x; echo "$x done $(date)"; done
done
