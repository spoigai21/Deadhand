# Phase 6 summary — what it costs to run

Runs 2026-10-26, `configs/phase6.yaml`, Apple M5 laptop (10 cores, 16 GB) with OSRM v6.0.0 and Kafka 3.9.1 in containers
under Docker Desktop. Results: `cost_cpu-apple-m5.json` (the aggregate), one `svc_cost_*.json` per run with the resource
monitor's samples. Figure: `figures/phase6_cost.png`. Every number is in `NUMBERS.md`. **Supply is simulated**
(`ASSUMPTIONS.md`). **The price is a list price with its date ($0.319 per hour, an 8-vCPU ARM instance, read 2026-10-26
from a third-party listing) and the laptop is not that machine:** the cost is the laptop's wall clock at that rate.

1. **A city-day costs about seven cents of machine time.** The weekday's 391,042 requests replay through the service in
   **752.5 s at four workers**: 0.21 machine-hours, **$0.067**, $0.17 per million decisions. At ×1 the day costs the day,
   $7.66. The run holds 4.25 GB at its peak.
2. **The bottleneck is the routing call inside one worker, not the machine.** The OSRM table call is **57% of a decision's
   processing**, the broker acknowledgement 22%. But the run's own work is **1.5 cores on ten** (JVMs 0.8, OSRM 0.5, Kafka
   0.2): the largest partition's worker runs one decision at a time and the other three wait at the gate for 60% of the
   day. P8 guessed OSRM at two cores; it is half a core, waited on.
3. **Most of the machine's busy time was not the run.** 45% busy, of which the run was a third; the virtual machine Docker
   Desktop runs the containers in cost more than the containers (1.17 against 0.73 cores), other applications 0.9, the
   kernel 0.9. The figure hatches the plumbing. A Linux host would not have it.
4. **Fleet moves the cost; the radius does not.** 3,000 drivers: **2.06× faster** than 7,500; 9,750: 24% slower; the work
   stays at 1.5 cores and the wall follows the routing calls. Ring 2: 9% faster; ring 5: **the same wall as ring 3**,
   because the fifty-candidate cap already binds by day. P8 understated the fleet and overstated the radius.
5. **At ×1.** The day costs the day, $7.66. In the paced quarter-hours the run's own processes use **0.38 cores at the
   peak and 0.48 in the quiet hour**, 4 to 5% of ten, with the workers' idle polling the largest part at both and OSRM 0.03
   of a core at the peak; match latency 20 ms median at 21:00 (339 ms max over 6,213 decisions), 19 ms at 04:00. P8 was
   right at the peak and wrong at 04:00, where the floor is the idle loop, not the load.
6. **One bug, found by the quiet hour.** At 04:00 the match latency came out at 22 s median: each worker paced from its own
   origin and the group stalled on the worker whose first request came latest (`BUGS.md` #8). The group now shares one
   origin through the clock topic; both windows were re-run, and the Phase 3 figure at 21:00 is restated beside the new one.
7. **The predictions.** P8 right on the cost, on memory and on the broker; wrong on which process is the bottleneck, on the
   fleet's and the radius's sizes, and silent on the plumbing, which was the largest bucket. The record stays in
   `PREDICTION.md`.
