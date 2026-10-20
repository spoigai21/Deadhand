# Phase 2 summary — the stream and the oracle

Runs 2026-10-20, `configs/phase2.yaml`, Apple M5, OSRM v6.0.0 free-flow, Kafka 3.9.1, H3 resolution 8.
Results: `finding1_cpu-apple-m5.json` and the per-run `run_*.json` files. Figures: `figures/phase2_*.png`.
Every number is in `NUMBERS.md`. **Supply is simulated** (`ASSUMPTIONS.md`).

1. **The stream and the oracle work.** A day of real demand (391,042 requests) is a Kafka topic with
   8 partitions keyed by coarse H3 cell; one Python consumer merges the partitions into event-time
   order and matches at 235 decisions per second, 4 ms per decision, a day in 28 minutes. The same
   seed gives byte-identical assignments twice. No request was matched twice and no driver was
   claimed twice in any of the 28 replays.
2. **Finding 1, the straight-line shortcut.** On the weekday, the straight line picks a slower driver
   than the road in **44% of decisions**; on those decisions the rider waits **40 s longer at the
   median, 130 s at p90**. End to end the road-aware rule cuts the median wait from 125 s to 103 s and
   empty driving from 912 m to 763 m per ride. The straight-line rule's promised pickup time is missed
   on 94% of rides, by 1.8× at the median.
3. **Where the cost lives.** Picks whose route crosses water are 3% of decisions and carry **a quarter
   of the total extra wait** (157 s median against 38 s). On the map the worst cells sit by the East
   River crossings and the RFK Bridge approach.
4. **What the rate depends on.** Supply density: 15% at 3,000 drivers, 44% at 7,500, 53% at 9,750,
   and 19–24% in the evening peak when the nearest driver is a kilometre away. The conditional cost
   stays at 35–45 s everywhere.
5. **The supply model had to change.** Drivers idle at their dropoffs serve only half the day at any
   fleet size tried; the baseline is now a fleet that cruises toward the previous hour's demand, and
   the failed model is kept in the numbers as a sensitivity row.
6. **The prediction was wrong on the rate (15% predicted, 44% measured) and right on the water share.**
   The record stays in `PREDICTION.md`.

Bugs found: `BUGS.md` #1 (OSRM's negative distance, again) and #2 (an empty OSRM reply under eight
concurrent replays killed a run; the client now retries).
