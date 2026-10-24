#!/usr/bin/env bash
# Phase 5 replays: the oracle with the hourly speed calibration (weekday seeds 0-4, weekend seed 0) and one free-flow weekday
# replay with the same radius columns, three at a time (~35 min per batch of three on the M5). Needs data/phase5/speed_multipliers.json.
set -euo pipefail
cd "$(dirname "$0")/.."
L=results/phase5/logs; mkdir -p $L
O=".venv/bin/python -m deadhead.oracle configs/phase5.yaml --rule road"
run() { $O --day "$1" --seed "$2" ${3:-} --run-id "$4" > "$L/$4.log" 2>&1; }
run 2016-06-16 0 "" cal_2016-06-16_seed0 & run 2016-06-16 0 --free-flow ff_2016-06-16_seed0 & run 2016-06-18 0 "" cal_2016-06-18_seed0 & wait
run 2016-06-16 1 "" cal_2016-06-16_seed1 & run 2016-06-16 2 "" cal_2016-06-16_seed2 & run 2016-06-16 3 "" cal_2016-06-16_seed3 & wait
run 2016-06-16 4 "" cal_2016-06-16_seed4 & wait
echo "phase 5 replays done $(date)"
