# Phase 5 summary — when the map changes under you

Runs 2026-10-24 and 2026-10-25, `configs/phase5.yaml`, Apple M5 (16 GB) with the OSRM v6.0.0 container local.
Results: `speeds_cpu-apple-m5.json` (the calibration), `calibration_cpu-apple-m5.json` (the replays
aggregated), one `run_*.json` per replay. Figures: `figures/phase5_*.png`. Every number is in `NUMBERS.md`.
**Supply is simulated** (`ASSUMPTIONS.md`). Everything here is the oracle at W = 0; the Java worker does not
take the multiplier. **Recorded taxi time includes stops, lights and the pickup itself: this is a calibration
of free-flow routing to recorded taxi time, not a traffic model.**

1. **The speed profile falls out of the trip data.** Odometer over timestamps gives **3.8–4.1 m/s from 08:00
   to 18:00** against the free-flow constant of 8.2; the multiplier on OSRM's free-flow durations (median of
   recorded over free-flow on 78,918 routed real trips) is a plateau of **2.0–2.24 from 10:00 to 18:00**,
   1.6–1.7 in the evening, and 1.12 at 05:00. P7 guessed 1.8 at the evening peak (close) and under 1.2 at
   night (wrong except at 05:00).
2. **Zones do not change decisions.** Zone-hour factors spread 0.85–1.09 around the city's hour; the
   busiest zone is the city's hour, because it has half the trips. A zone factor changes the road-best
   driver in **0.79% of decisions** (3.6 s median gain). The per-segment update was not built; the hourly
   multiplier is the calibration. P7 right.
3. **The map changes the supply before it changes any decision.** The 7,500 drivers of Phase 2 were sized at
   free flow. With the trips' own speeds the same fleet serves **86.9% of the weekday against 93.3%**, 11%
   of riders are promised more than ten minutes and abandon (against 1%), and the served riders' median wait
   is **210 s against 103 s**. At 18:00, 69% are served and 29% abandon.
4. **The two approximations under the calibration.** Finding 1's whole-day rate is unchanged as it must be
   (43.4% against 44.3%: a uniform factor scales every candidate alike), but its conditional cost is the
   factor times the free-flow cost, **83 s against 40 s** at the median, 151 s at 18:00, and the day's total
   is **2.3× the free-flow figure** (6,735 against 2,870 rider-hours), not the third P7 guessed. Finding 2's
   rate **rises** (8.7% to 11.3% over the day, 10.6% to 14.5% at the peak), against P7: with fewer idle
   drivers per cell the one across the edge is more often the only one; its median cost goes from 64 s to
   142 s. By hour both rates move only through the supply: Finding 1 falls to 32% at 14:00 and rises to 34%
   at 18:00 where free flow had 18%.
5. **The matching radius by hour.** Scored beside every decision: at 18:00 ring 3 leaves **6.4% of riders a
   driver two minutes faster beyond it** and 2.1% with none; at 13:00 ring 2 loses 3.5% and ring 3 2.0%.
   The smallest ring with under 1% loss is **5 in the evening and at midnight, 4 by day, 2 only from 02:00 to
   04:00**; free flow and Saturday give the same picture. P7 was right at 18:00 and wrong by a ring by day,
   where the fifty-candidate cap binds and a wider ring is free.
6. **The predictions.** P7 right on the evening multiplier, on zones not mattering and on the 18:00 radius;
   wrong on the night speeds, on the size of Finding 1's cost (three times too low) and on the direction of
   Finding 2's rate. The record stays in `PREDICTION.md`.

No bug was found in Phase 5. The finding that conditions everything, that the fleet calibration was a
free-flow calibration, is recorded in `ASSUMPTIONS.md`.
