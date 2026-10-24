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
