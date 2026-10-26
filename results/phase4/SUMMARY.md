# Phase 4 summary — batching

Runs 2026-10-23 and 2026-10-24, `configs/phase4.yaml`, Apple M5 (16 GB) with the OSRM v6.0.0 container, the
Kafka 3.9.1 broker and the Java 21 workers all on the one machine. Results: `batching_cpu-apple-m5.json`,
one `run_*.json` per batched replay, the service's `svc_*_w60.json`. Figures: `figures/phase4_*.png`. Every
number is in `NUMBERS.md`. **Supply is simulated** (`ASSUMPTIONS.md`).

1. **The window is first a retry, then a batch.** The single decision abandons a rider whose nearest idle
   driver is over the patience T; a window gives that rider the next window's drivers. On the weekday the
   served share goes from **93.3% at W = 0 to 96.7% at W = 10–20**, the served riders' mean wait *falls*
   (135 s to 122 s) and the empty distance falls with it (1,071 m to 790 m per ride). The whole gain is the
   supply-starved evening: at 18:00 the single decision serves 79% of riders at a 212 s median wait, a 10 s
   window 90% at 112 s. Without the retry (`--no-roll`) a 2 s window changes almost nothing; a 60 s window
   alone removes 38% of the deadhead and 19 s of wait.
2. **The operating point by the rule stated beforehand is W = 60 s**, not the 10 s P6 guessed: the step to
   60 buys a minute of leg for 26 s of rider wait, the step to 120 (replayed after the planned grid ran out)
   for 106 s. Over five seeds at W = 60: **96.0% served, 127.8 s mean wait, 649 m per ride, 243,816 empty
   km a day** against 93.3%, 135.5 s, 1,071 m and 390,705 km at W = 0; seed noise is a tenth of a point.
   The rule's one number hides three: **no window at night, 30 s by day, 60 s or more in the evening**; on
   the weekend, where supply is not starved, 30 s in every band.
3. **The two approximations do not shrink under batching.** The straight-line solve disagrees with the
   road solve on more riders as the batch grows (44.7% at W = 0, 52.3% at 60): an assignment couples its
   pairs, so one wrong cost moves several. Its net cost falls 13% only because it hands a fifth of the
   riders a faster driver by accident. The shard's slower-pick rate rises the same way (8.8% to 11.0%)
   while its blind spot nearly vanishes (3.8% to 0.2% with no visible driver) and its rider-hours fall
   from 923 to 656.
4. **The matching radius matters less with a window.** Widening from ring 2 to ring 5 at W = 0 serves 1.5
   points more riders for 32 s of wait and 304 m per ride; at W = 60 the same widening serves 0.3 points
   more for 4 s and 57 m. The next window's supply does what the wider ring did.
5. **The service with a 60 s window** (4 workers, resolution 6) decides every request once, claims no
   driver twice, serves **91.6% against the batched oracle's 96.0%** (the same 4.4-point shard gap as at
   W = 0) and cuts its empty distance by 43% against the Phase 3 service; throughput falls from 523 to 316
   decisions/s because a window closes only when the slowest partition is past it.
6. **The prediction.** P6 wrong on the operating point by six times and wrong on Finding 1's direction;
   right that the shard's boundary survives the window. The record stays in `PREDICTION.md`.

No bug was found in Phase 4; the one surprise in the method, the rule reaching the edge of its grid, is
recorded in `ASSUMPTIONS.md` and the grid extended in `configs/phase4.yaml`.
