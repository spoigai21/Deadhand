# PREDICTION — written before the measurement that tests it. Appended to, never edited.

Each entry is dated and states what was known when it was written. A wrong prediction stays here
with the result beside it.

---

## Phase 0, 2026-10-19 — what was verified before predicting

**Trip data.** The TLC's own Parquet files have been rewritten to the zone-ID schema for every
year: June 2016 and June 2015 were downloaded and checked, and neither carries coordinates. Rule G4
earned its keep on the first evening. Coordinates survive in NYC Open Data's copy, **"2016 Yellow
Taxi Trip Data" (dataset `uacg-pexx`)**, which carries `pickup_longitude`, `pickup_latitude`,
`dropoff_longitude`, `dropoff_latitude` alongside the zone IDs. **Dataset year: 2016, January to
June** (coordinates stop in July 2016). The week of **Monday 13 to Sunday 19 June 2016** is pulled,
one hour per request. Row counts, junk shares and the two chosen days are in the block below,
appended when the pull completed.

**Map.** Geofabrik New York State extract, data timestamp 2026-10-18, clipped to the city bounding
box. OSRM v6.0.0, car profile, MLD: **642,217 nodes, 756,257 edges**, built in under 10 s, peak RAM
733 MB. The map is ten years newer than the demand; the road network changed little, and the
difference is a stated limit, not a correction.

**Routing cost.** Single route 1.1 ms median; a one-pickup-to-50-candidates table 6.3 ms; a
50×50 table 47 ms; a 1000×1000 table 2.45 s. Real pickups snap to the graph at 5 m median, 20 m
p90, none unsnappable. **Decision: route per pair. No hex-to-hex matrix.** H3 resolution 8 for
candidate search and stream keying.

**Water crossings.** Computed from OSM, not hand-picked: 2,844 drivable bridge or tunnel ways, 390
of which intersect coastline, waterway or water-area geometry, giving **144 named water crossings**.
The list includes every East River, Harlem River and Hudson crossing by name, and a long tail of
expressway segments over creeks, which is correct and stays.

---

## Predictions, 2026-10-19 — committed before Phase 1 runs

### P1. The disagreement (Phase 1)

- Trip pairs (real pickup → dropoff): road distance ÷ straight-line distance **median 1.25,
  p90 1.6**. Manhattan's grid makes a diagonal trip about 1.27× the straight line by itself.
- Pickup legs (driver → pickup, inside the matching radius): **median 1.35, p90 2.0**. Short legs
  are worse than trips, because one-way streets and avenue spacing matter more over 500 m than
  over 5 km.
- The worst pairs will straddle the **East River** (Manhattan ↔ Brooklyn/Queens below the
  Queensboro) and the **Harlem River**, with ratios above 4. Hudson crossings will barely appear,
  because almost no yellow-taxi pickups sit in New Jersey.

### P2. Finding 1 — the straight-line shortcut (Phase 2)

- **Wrong-driver rate: 15% of decisions** pick a different driver with a non-zero road-time gap,
  at a 50-candidate ring on the weekday.
- **Conditional cost on those decisions: median 60 s of extra wait, p90 3 min**, and median 0.5 km
  of extra empty driving.
- Decisions involving a water crossing will be **under 3%** of all decisions but will carry
  **over a quarter** of the total extra wait.
- Full-replay (Mode B) median wait will differ between the rules by **5–10%**, which is the number
  that would make a reader say "the shortcut is fine". The conditional number is the finding.
- Promised-ETA error for the straight-line rule: **median 40% optimistic**.

### P3. Finding 2 — the geographic shard (Phase 3)

Design note recorded here because it changes the prediction: Kafka's default key hashing would
scatter adjacent cells across workers, so the stream is keyed by the **resolution-6 parent cell**
(edge about 3.2 km) and each worker owns contiguous regions; the resolution-8 cell is for
candidate search only.

- With 4 workers: **8% of decisions** have their road-nearest driver in another worker's region.
  With 8 workers: **12%**.
- Conditional cost: **median 45 s of extra wait** — lower than Finding 1, because a region edge is
  a straight cut and the lost driver is usually only slightly better.
- The halo mitigation, if built as the upper line, removes **over 90%** of that cost.

### P4. Throughput, latency and the compiled consumer (Phase 3)

- The bottleneck is the routing call, not the consumer language. One worker, Java or Python,
  sustains **about 150 decisions per second** against a 1×50 table at 6 ms.
- **Java will not beat Python by enough to matter per worker** (under 1.5×). The honest Phase 3
  result is scaling with N, not language: **near-linear to 4 workers, flattening by 8** as the
  single OSRM container saturates on 10 cores.
- A full weekday (about 350k clean requests) at 8 workers replays in **about 6 minutes**.
- Median match latency at ×1 replay speed: **under 15 ms**; p99 under 100 ms.

### P5. The kill test (Phase 3)

- **The first run will find a bug.** Prediction: at least one request is matched twice or one
  driver claimed twice on the first kill-test attempt, from the window between claim and offset
  commit. It will be fixed and recorded in `BUGS.md`, and the fixed version will show zero
  duplicates across 10 runs under the cooperative assignor.
- Eager rebalance will stall the group for **5–10 s** per kill; cooperative for **under 2 s**.

### P6. Batching (Phase 4)

- The operating point will be **W = 10 s** on the weekday: past it, deadhead stops falling and wait
  rises roughly linearly.
- Batching **shrinks** Finding 1 by about a third, because the assignment solve on road costs
  corrects some pairings the greedy straight-line rule got wrong.

---

## Phase 0 facts appended when the pull completed

*(appended by hand from `results/phase0/data_check_cpu-apple-m5_seed0.json`)*

**Pulled 2026-10-19** from NYC Open Data `uacg-pexx`, 13–19 June 2016, one hour per request,
7 files, 86 MB as Parquet. **2,575,502 rows**, all with non-null coordinates.

| Check | Rows | Share |
|---|---|---|
| Pickup coordinate exactly zero | 32,320 | 1.25% |
| Dropoff coordinate exactly zero | 29,820 | 1.16% |
| Pickup outside the city box | 32,822 | 1.27% (includes the zeros) |
| Dropoff outside the city box | 32,002 | 1.24% |
| Duration ≤ 0 or > 6 h | 6,589 | 0.26% |
| Distance ≤ 0 or > 100 mi | 16,520 | 0.64% |
| Newark flat rate (rate code 3; Newark is outside the state extract) | 5,142 | 0.20% |
| **Clean after all filters** | **2,522,854** | **97.96%** |

Clean rows per day: Mon 13 333,902 · Tue 14 356,084 · Wed 15 375,408 · **Thu 16 391,042** ·
Fri 17 380,119 · **Sat 18 362,824** · Sun 19 323,475. Mean demand 4.2 requests/s over the week;
**peak hour Thursday 16 June, 21:00–22:00, 24,860 requests (6.9/s)**.

**The two days: Thursday 16 June 2016 (weekday) and Saturday 18 June 2016 (weekend).** Neither is
a holiday. Thursday is the busiest day of the week, which makes it the hardest case for the service
and the best-populated one for the findings.

**Loader:** Polars 2.0.0 reads the week in 0.07 s, pandas 3.0.6 in 0.10 s. At this size the
choice does not matter; **Polars stays** because the Phase 1 pair sampling and the Phase 5 speed
calibration are group-by work it does without a copy. Nothing bigger than a week is ever loaded.

**What the demand rate means for the system** (recorded now so it is not discovered later): at
4–7 requests per second, real-time replay does not stress anything. The throughput numbers in P4
are therefore stated at **maximum replay speed**, not ×1, and the ×1 figure is latency only.

---

## Phase 1 result, 2026-10-19 — against P1

*Appended after the run. P1 above is unchanged.* Numbers in `NUMBERS.md`, Phase 1 section.

| P1 said | Phase 1 found | Verdict |
|---|---|---|
| Trips: median 1.25, p90 1.6 | **median 1.32, p90 1.63** (both days) | p90 right, median 6% worse than predicted. The grid alone is not the whole story: a quarter of trips cross water. |
| Pickup legs: median 1.35, p90 2.0 | **median 2.00, p90 4.85** at the baseline density; **1.47 median for 1–2 km legs, 2.70 for 100–250 m legs** | **Wrong by a wide margin.** The shape was right (short legs are worse) and the size was not: at the lengths a matcher actually compares, the straight line is off by 2× at the median, not 1.35×. The headline moves with supply density only because density sets the leg length; the per-band numbers hold at 0.1, 0.2 and 0.5. |
| Worst pairs straddle the East River and the Harlem River, ratios above 4 | The worst legs are drivers **already inside a bridge or tunnel approach**: Brooklyn-Battery Tunnel 66.7×, Queensboro Bridge upper level 62.2×, Queens-Midtown Tunnel 45.5×, Williamsburg Bridge 53.5×. The worst trips start on one-way avenues (1st Avenue 21.8×) and the Queensboro approaches. The Harlem River does not appear. Airport terminal loops are worse than anything on the street (JFK Terminal 4, 86.8×). | **Half right.** East River crossings dominate the worst list, but the mechanism is being *on* the crossing, not being across it. The Harlem River prediction was wrong. |
| Hudson crossings barely appear | Holland and Lincoln Tunnels are the 4th and 6th most-used crossings by real trips, but none is in a worst-30 list | Right for the worst pairs, wrong about how often the Hudson is crossed at all. |
| (not predicted) | Trips that cross water have a median ratio of **1.36 against 1.31** for trips that do not | A river adds little to a committed trip. The disagreement lives in the short leg, not the long trip. This is the number Phase 2 is built on. |

**What this changes for P2.** The wrong-driver rate was predicted at 15% on the assumption that legs
disagree by 1.35× at the median. At 2× with a p90 near 5, the straight-line ranking of a 50-candidate
set will be reshuffled far more often than that. P2 stays as written; it is probably too low.

---

## Phase 2 result, 2026-10-20 — against P2

*Appended after the run. P2 above is unchanged.* Numbers in `NUMBERS.md`, Phase 2 section. Weekday,
7,500 simulated drivers cruising toward demand, abandonment at a promised 600 s, five seeds.

| P2 said | Phase 2 found | Verdict |
|---|---|---|
| Wrong-driver rate 15% at a 50-candidate ring | **44.4%** in the road-aware replay, 40.0% in the straight-line mirror; 50.3% on the weekend | **Wrong by 3×.** Phase 1 had already shown why: the leg a matcher compares is 130–300 m most of the day, and the straight line is 2.7× off at that length. The rate follows supply density (15% at 3,000 drivers, 53% at 9,750) and drops to 19–24% in the evening peak when the nearest candidate is a kilometre away. |
| Conditional cost: median 60 s, p90 3 min, 0.5 km extra empty driving | **median 40 s, p90 130 s, mean 64 s; 237 m** | Right order, both too high. Many wrong picks are cheap: a driver one block further by road. The p99 is 8 minutes. |
| Water crossings: under 3% of decisions, over a quarter of the total extra wait | **3.3% of decisions, 25.4% of the extra wait**; 157 s median when the straight-line pick crosses water against 38 s when it does not | **Right.** The one prediction that landed, and the one that matters for the map. |
| Mode B median wait differs by 5–10% | **125 s against 103 s, 18%**; p90 325 against 306 s; 912 against 763 empty metres per ride | Underestimated. End to end the shortcut costs 22 s of wait and 150 m of empty driving per ride, and 50,000 empty kilometres a day across the fleet. |
| Promised-ETA error for the straight-line rule: median 40% optimistic | **The promise is missed on 93.7% of rides; actual is 1.82× promised at the median and 7× at p90**; 63 s late at the median | Far worse than predicted, because the promise is a straight line at one constant speed and the leg is short. |
| (not predicted) | The supply model as written (idle at the dropoff, no repositioning) serves 52% of the day at 7,500 drivers and 65% at 9,750. The baseline had to become "cruise toward demand", which is the sensitivity row the plan listed. The failed model's numbers stay in `NUMBERS.md`. | A modelling result, recorded before the finding it conditions. |

**What this changes for P3.** The boundary cost prediction (8% of decisions at 4 workers) was made
with the 15% figure in mind. The candidate sets are denser than assumed, so a region edge will cut
through more of them; P3 is probably also too low. It stays as written.

## Phase 3 result, 2026-10-21 — against P3, P4 and P5

*Appended after the runs. P3, P4 and P5 above are unchanged.* Numbers in `NUMBERS.md`, Phase 3 section.
Weekday, 7,500 simulated drivers, five seeds for the oracle and the kill test, seed 0 for the service runs,
Apple M5 with the broker, the router and the workers on one machine.

| P3 said | Phase 3 found | Verdict |
|---|---|---|
| 8% of decisions at 4 workers, 12% at 8 | **8.7% slower pick, plus 4.0% with no visible driver at all, at 4 workers; 8.8% and 4.0% at 8** (resolution 6) | **Right on the rate at 4**, after the Phase 2 note said it was probably too low. Wrong at 8: more workers cost nothing more, because the three partitions that carry 96% of the day are already on three workers at N = 4. The prediction imagined workers as regions; they are hash buckets. |
| Conditional cost: median 45 s, lower than Finding 1 because a region edge is a straight cut | **64 s at the median, 252 s at p90**; 85–116 s in the evening | Too low, and the wrong direction: higher than Finding 1's 40 s. The lost driver is often the only close one, not a slightly better one. |
| Halo removes over 90% | **100% at resolution 6 and 7, 75% at resolution 8** | **Right.** |
| (not predicted) | At resolution 8 the hash scatters neighbours and the shard picks a slower driver on 33% of decisions; at resolution 6 one partition holds 58% of the day. No resolution both balances and keeps the candidates. | The finding the prediction did not see coming. |

| P4 said | Phase 3 found | Verdict |
|---|---|---|
| One worker, Java or Python, about 150 decisions/s against a 6 ms table | **Java 332/s, Python 235/s**; the 1-to-50 table call is 1.75 ms on a fresh connection, 2.67 ms kept-alive | Both faster than predicted and the table a third of the guess; the routing call is still the decision (56% of the worker's time). |
| Java under 1.5× Python per worker | **1.41×**; the decision is 3 ms against 4 ms | **Right, just.** The language is not the point (G12); the service stays in Java because the consumer-group semantics are the subject, not the speed. |
| Near-linear to 4 workers, flattening by 8 as OSRM saturates | **1.57× at 4, 1.62× at 8**, flat because of the key, not the router: partition 0 holds 58% of the day and the ceiling is 1.73× | Right shape, wrong cause. OSRM had capacity left: resolution 7 reached 1.97×. |
| A full weekday at 8 workers in about 6 minutes | **12 minutes** at 8 and at 4; 10 at resolution 7 | Twice too optimistic, for the balance reason. |
| ×1 median latency under 15 ms, p99 under 100 ms | **18 ms median, 63 ms p99, 134 ms max** (4 workers, 21:00–21:15) | p99 right; the median 3 ms over, the fleet-log acknowledgement round trip the prediction did not include. |

| P5 said | Phase 3 found | Verdict |
|---|---|---|
| The first run will find a double-match bug, from the window between claim and offset commit | **No request matched twice and no driver claimed twice in any run, dry or real.** The first run found three other things (`BUGS.md` #5–#7): a lost tail of 8,724 requests, a 12–24 s stall from a too-short peer expiry, and a false positive in the double-claim check itself. | Wrong on the bug, right that the first run would find one. The claim-acknowledged-before-decide order held on the first try. |
| Eager stalls the group 5–10 s per kill, cooperative under 2 s | **Takeover 6.0–7.2 s eager, 6.6–7.5 s cooperative**; the orphaned partitions pause 11–17 s and 10.5–15.6 s. Cooperative saves 5–6 s of refolding after the detection, not the detection. | Eager right; cooperative wrong by 3×. An assignor cannot beat the failure detector: a kill is a missed heartbeat, and the 6 s session timeout is the floor under both. |
| Zero duplicates across 10 runs under the cooperative assignor | **Zero conflicting decisions and zero double claims in all 10 runs under both assignors**; 16–204 redelivered records per run, every one re-emitted identically | **Right.** |

**What this changes for P6.** Nothing in the prediction. Batching runs inside a worker, so it inherits the
shard's boundary: the batch sees the same region the single decision saw. The W = 10 s guess stays as written.

## Prediction P7, 2026-10-23 — committed before the Phase 5 calibration is computed

*The plan's predictions ran to P6. This one is written before `deadhead/phase5_speeds.py` has been run, so no calibration
number was known when it was written. Appended, as the rule says; it is not edited afterwards.*

### P7. When the map changes under you (Phase 5)

- Observed taxi speed from the odometer and the timestamps will be **about 5 m/s at the evening peak and about 9 m/s at
  04:00**, against OSRM's free-flow median of about 8 m/s (the straight-line speed constant came from it). As a multiplier
  on free-flow durations: **about 1.8× at 17:00–19:00, under 1.2× between 02:00 and 05:00**, measured as the median of
  recorded over free-flow duration on the Phase 1 routed trips.
- Zone speeds will differ: Midtown about 1.3× slower than the hourly city figure at midday, the outer boroughs faster.
  But a zone multiplier will change the road-best driver in **under 3% of decisions**, because the candidates of one
  decision sit in one or two zones. The per-segment update (`osrm-customize`) will therefore not be built, and the
  hourly multiplier stays the calibration.
- **Finding 1's rate does not move with a uniform hourly multiplier** (it scales every candidate alike); its cost in
  seconds grows with it: the conditional median goes from 40 s to **about 60 s at the peak hours**, and the day's total
  extra wait rises by about a third. The same for Finding 2's cost. The rates by hour change only through the fleet:
  slower legs mean fewer idle drivers at peak, so the rates fall a little in the evening, as they already do.
- The right matching radius changes by hour. At 18:00, when the nearest driver is a kilometre away, ring 3 leaves
  **over 5% of riders** a better driver in rings 4–5; at 13:00 ring 2 loses under 1% and the fifty-candidate cap, not
  the ring, is what binds. Recommended: **ring 2 by day in the core, ring 5 at night and in the evening peak**.

## Phase 4 result, 2026-10-24 — against P6

*Appended after the runs. P6 above is unchanged.* Numbers in `NUMBERS.md`, Phase 4 section. Weekday, 7,500
simulated drivers, five seeds at the operating point, seed 0 elsewhere; the window W was replayed at 2, 5, 10, 20,
30 and 60 s as planned, then at 120 s because the operating rule had not stopped inside the planned grid.

| P6 said | Phase 4 found | Verdict |
|---|---|---|
| The operating point will be W = 10 s on the weekday | **W = 60 s** by the rule stated before the sweep (the largest W whose step still removes leg time at under 60 s of rider wait per minute removed). Up to W = 20 the served riders' wait and the empty distance fall *together* (135 s and 1,071 m at W = 0; 122 s and 790 m at 20); the step to 60 costs 26 s per minute removed, the step to 120 costs 106. | **Wrong by six times.** The guess imagined a window as a pure cost to the rider; on a day where supply runs out at 18:00 a window is first a *retry*: the rider the single decision abandons gets the next window's drivers. The rule's answer is one number for a day that wants three: no window at night, 30 s by day, 60 s or more in the evening. |
| Past the operating point, deadhead stops falling and wait rises roughly linearly | Deadhead keeps falling through 120 s (649 m at 60, 549 at 120); wait rises faster than linearly (+5 s from 30 to 60, +21 s from 60 to 120) | Wrong on the deadhead, right on the direction of the wait. |
| Batching shrinks Finding 1 by about a third, because the assignment on road costs corrects pairings the greedy straight-line rule got wrong | The straight-line solve disagrees with the road solve on **more** riders as the batch grows: **44.7% at W = 0, 52.3% at 60**; its net cost falls 13% (2,864 to 2,483 rider-hours) and its gross rises 24% | **Wrong.** An assignment couples its pairs: one wrong cost moves several riders, so the rate rises; the net falls only because the straight-line solve hands a fifth of the riders a faster driver by accident. |
| (from the Phase 3 note) Batching inherits the shard's boundary; the W = 10 guess stays | The shard's slower-pick rate rises with the window (8.8% to 11.0%) while its blind spot nearly vanishes (3.8% to 0.2% with no visible driver) and its rider-hours fall from 923 to 656. The service with a 60 s window serves 91.6% against the batched oracle's 96.0%: the same 4.4-point shard gap as at W = 0. | Right that the boundary survives the window; the window moves the cost from "no driver" to "a slower driver". |
| (not predicted) | On the every-rider wait the retry across windows is a net cost at W = 60: batching alone gives 138.8 s, batching with the retry 146.5 s, against 166.6 s at W = 0. The weekend wants W = 30 s in every band of the day. | The decomposition the plan did not ask for and the write-up needed. |

**What this changes for P7.** Nothing in the prediction. Phase 5 is measured on the single decision (W = 0), so the
window does not enter it; the calibration scales every candidate alike, which is the premise P7 already rests on.

## Phase 5 result, 2026-10-25 — against P7

*Appended after the runs. P7 above is unchanged.* Numbers in `NUMBERS.md`, Phase 5 section. The calibration is from
the week's trip records and the Phase 1 routed trips; the replays are the oracle at W = 0, weekday seeds 0–4 and
Saturday seed 0 with the hourly multiplier, and one weekday replay at free flow with the same radius columns.

| P7 said | Phase 5 found | Verdict |
|---|---|---|
| Observed speed about 5 m/s at the evening peak and about 9 m/s at 04:00, against a free-flow median of about 8 | **3.8–4.1 m/s from 08:00 to 18:00**, 8.2 at 05:00, 5.7 at midnight; free flow 8.24 | Too fast by day (the taxi averages under 4 m/s for ten hours), about right at the trough. |
| Multiplier about 1.8× at 17:00–19:00, under 1.2× between 02:00 and 05:00 | **2.13 / 2.04 / 1.91 at 17, 18, 19**; **1.32 / 1.27 / 1.24 / 1.12 at 02–05**; the peak is 2.24 at 15:00, not in the evening | Right within a fifth at the evening hours; wrong at night, where only 05:00 gets under 1.2. The day's shape was guessed as a commute peak; it is a plateau from 10:00 to 18:00. |
| Midtown about 1.3× slower than the hourly city figure at midday; a zone factor changes the road-best driver in under 3% of decisions, so `osrm-customize` is not built | The busiest zone (Midtown) is 1.02–1.08× the city's hour; zone-hours spread 0.85–1.09 (p10–p90); a zone factor changes the pick in **0.79%** of decisions (3.6 s median gain) | **Right on the decision**, wrong on Midtown: the city factor already *is* Midtown, which has half the trips. The per-segment update was not built. |
| Finding 1's rate does not move with a uniform multiplier; its conditional median goes from 40 s to about 60 s at the peak; the day's total extra wait rises by about a third | Whole-day rate **43.4% against 44.3%**; conditional median **83 s** (151 s at 18:00); the day's total **2.3×** (6,735 against 2,870 rider-hours). By hour the rate moves a lot through the supply: 32% against 52% at 14:00, 34% against 18% at 18:00 | Right on the whole-day rate and on the direction; **wrong by a factor of three on the size** of the cost, because the multiplier is 2, not 1.5, and because the fleet sized at free flow runs short under it: 11% of riders abandon, so the long legs that remain are the costly ones. |
| Finding 2's cost rises the same way; its rates fall a little in the evening because slower legs mean fewer idle drivers | Finding 2's rate **rises**: 8.7% to 11.3% over the day, 10.6% to 14.5% at the peak; its conditional median 64 s to 142 s; its rider-hours 904 to 2,440 | Wrong on the direction of the rate. Fewer idle drivers per cell make the one across the edge more often the only one. |
| At 18:00 ring 3 leaves over 5% of riders a better driver in rings 4–5; at 13:00 ring 2 loses under 1% and the fifty-candidate cap binds; recommended ring 2 by day in the core, ring 5 at night and in the evening peak | At 18:00 **6.4%** of decisions have a driver beyond ring 3 at least 30 s faster (two minutes faster at the median) and 2.1% have none within it; at 13:00 ring 2 loses **3.5%**, ring 3 2.0%, and the cap binds (50 visible). Recommended: **ring 5 in the evening and at midnight, ring 4 by day, ring 2 only from 02:00 to 04:00** | Right at 18:00 and on the cap; wrong by one ring by day (the cap makes a wider ring free, so the rule takes it) and wrong on the night, which is not one thing: the small hours want ring 2, midnight wants ring 5. |
| (not predicted) | The fleet calibration of Phase 2 was a free-flow calibration. The same 7,500 drivers serve 93.3% of the weekday at free flow and **86.9%** with the trips' own speeds, with 11% abandoning against 1%. | The result the phase was for: the map changing under you changes the supply before it changes any decision. |
