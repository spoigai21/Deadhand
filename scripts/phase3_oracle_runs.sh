#!/usr/bin/env bash
# Phase 3 reference oracle: the Phase 2 model with a deterministic cruise point and the Mode A shard columns.
# Weekday seeds 0-4 and weekend seed 0, three replays at a time (about 30 min per batch on the M5).
set -euo pipefail
cd "$(dirname "$0")/.."
run() { .venv/bin/python -m deadhead.oracle configs/phase3.yaml --day "$1" --rule road --seed "$2" --run-id "ref_$1_seed$2" > "results/phase3/logs/ref_$1_seed$2.log" 2>&1; }
run 2016-06-16 0 & run 2016-06-16 1 & run 2016-06-18 0 & wait
run 2016-06-16 2 & run 2016-06-16 3 & run 2016-06-16 4 & wait
echo done
