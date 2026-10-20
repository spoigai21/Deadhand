#!/bin/sh
# Phase 2 replays, in batches of up to 8 concurrent oracles against one OSRM container.
# Usage: scripts/phase2_runs.sh <fleet_size> [logdir]
set -e
N=${1:?fleet size}; LOG=${2:-results/phase2/logs}; mkdir -p "$LOG"
CFG=configs/phase2.yaml; PY=.venv/bin/python
run() { $PY -m deadhead.oracle $CFG "$@" > "$LOG/$(echo "$@" | tr ' -' '__').log" 2>&1; }
batch() { for job in "$@"; do (eval run $job) & done; wait; }
# weekday, both rules, five seeds
batch "--day 2016-06-16 --rule road --fleet $N --seed 0" "--day 2016-06-16 --rule road --fleet $N --seed 1" "--day 2016-06-16 --rule road --fleet $N --seed 2" "--day 2016-06-16 --rule road --fleet $N --seed 3" \
      "--day 2016-06-16 --rule road --fleet $N --seed 4" "--day 2016-06-16 --rule straight --fleet $N --seed 0" "--day 2016-06-16 --rule straight --fleet $N --seed 1" "--day 2016-06-16 --rule straight --fleet $N --seed 2"
# weekend, both rules, five seeds, plus the two remaining weekday straight-line seeds
batch "--day 2016-06-16 --rule straight --fleet $N --seed 3" "--day 2016-06-16 --rule straight --fleet $N --seed 4" "--day 2016-06-18 --rule road --fleet $N --seed 0" "--day 2016-06-18 --rule road --fleet $N --seed 1" \
      "--day 2016-06-18 --rule road --fleet $N --seed 2" "--day 2016-06-18 --rule road --fleet $N --seed 3" "--day 2016-06-18 --rule road --fleet $N --seed 4" "--day 2016-06-18 --rule straight --fleet $N --seed 0"
# the rest of the weekend straight-line seeds, and the sensitivities: fleet × 0.7 and × 1.3, abandonment 300 s and 900 s (weekday, seed 0)
N7=$(( N * 7 / 10 )); N13=$(( N * 13 / 10 ))
batch "--day 2016-06-18 --rule straight --fleet $N --seed 1" "--day 2016-06-18 --rule straight --fleet $N --seed 2" "--day 2016-06-18 --rule straight --fleet $N --seed 3" "--day 2016-06-18 --rule straight --fleet $N --seed 4" \
      "--day 2016-06-16 --rule road --fleet $N7 --seed 0" "--day 2016-06-16 --rule road --fleet $N13 --seed 0" "--day 2016-06-16 --rule road --fleet $N --seed 0 --abandon 300" "--day 2016-06-16 --rule road --fleet $N --seed 0 --abandon 900"
# the supply model without repositioning (idle at the dropoff forever): the recorded failure, both rules at the baseline fleet and the road rule at a larger one
batch "--day 2016-06-16 --rule road --fleet $N --seed 0 --repositioning none" "--day 2016-06-16 --rule straight --fleet $N --seed 0 --repositioning none" "--day 2016-06-16 --rule road --fleet $N13 --seed 0 --repositioning none"
echo "phase2 runs done"
