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
