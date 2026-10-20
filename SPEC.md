# Deadhead — a real-time ride-matching service on a real day of New York demand, sharded by map cell and measured under failure

*(**Deadhead** is the industry term for a vehicle driving empty — the miles between dropping one
rider and reaching the next. It is the cost this project's two findings are measured in.
Specced 2026-10-06, re-scoped three times as the prior art was checked, and **inverted on
2026-10-19**: the system is now the project and the measurements are the findings inside it.
Read "Prior art" below before anything else.)*

**One line:** build the matcher as a real system — a day of real trip demand streamed through
Kafka, routed on the real road network, sharded by H3 cell across a Java consumer group — and
report what it does: throughput, latency, what sharding by geography hides, what the
straight-line shortcut costs, and whether a worker crash ever matches a rider twice.

**What kind of result this is.** A **distributed-systems** build first: a consumer group, partition
assignment, rebalance, exactly-once matching under at-least-once delivery, all measured on named
hardware. A **geospatial** one throughout: a road graph, travel-time matrices, a hexagonal spatial
index, and the places where the road network and the map grid both diverge from the plane. It is
not a better dispatch algorithm and never claims to be one.

---

## Ground rules

These apply to every phase, every figure and every sentence in the README.

| # | Rule |
|---|---|
| G1 | **Never report a number that is not in `NUMBERS.md`** with the results file and seed that produced it. Nothing is quoted from memory. |
| G2 | **Supply is simulated and is labelled as such everywhere.** The demand side is real trip data. There is no public dataset of idle-driver positions, so supply is a model and its assumptions live in `ASSUMPTIONS.md`. The README says so next to the first number, not in a footnote. |
| G3 | **Never claim a result about any company's dispatch system.** This is a public taxi and for-hire-vehicle log. Nothing here is a statement about how anyone's marketplace performs. |
| G4 | **Never trust the dataset's documentation.** Count the rows and check the columns yourself. The schema changed across years (Phase 0). |
| G5 | **Report losses and ties as prominently as wins.** "The shortcut is fine", "sharding costs nothing", "Java is no faster" are real, publishable findings and must not be buried. |
| G6 | **If the project drifts back to "vary driver supply, plot wait time", stop.** That question is answered in public repositories already. |
| G7 | **Never claim to beat any dispatch algorithm.** The claim is the system's measured properties and the two priced approximations. The simulators cited below are ahead on algorithms and are not services. |
| G8 | **Cite the prior art in the README by name**, and say what each does better. |
| G9 | **Never write "nobody has built this" or "nobody has measured this."** Write what is built and measured here that is not built and measured there. |
| G10 | **The unit of analysis for both findings is the decision, not the route.** A cost averaged over the decisions where both rules agreed has become the already-published thing. |
| G11 | **No tool enters for its name.** Every row of the Stack table has a stated engineering reason and a phase that measures it. The refused list is as important as the chosen one. |
| G12 | **A null result is a result.** If the Java consumer does not beat Python by enough to matter, say so and keep Python. If sharding hides nothing, say so. |
| G13 | **Three things separate this from a whiteboard exercise, and none may be cut to save time:** real demand data rather than generated events; every number on named hardware; and a kill test checked against a correctness oracle. |

---

## Prior art, checked honestly — read before building

**Two searches were run on 2026-10-06 and both changed the project.**

**First search: is "ride-hailing simulator" a crowded category?** Yes. A long tail of ride-hailing
clone apps, and real simulators including one whose stated question is *"how do driver supply and
rider demand levels affect service quality."* **Publicly answered more than once.** Not built again.

**Second search: does anyone route on a real road network?** Yes, and well: **AMoD2** (high-capacity
ride-sharing, Manhattan map and taxi data, three dispatch algorithms), a **deep-RL taxi
dispatcher** that routes shortest paths to pickups, **tsharesim**, and academic work on scalable
ridesharing simulation. **Road-network routing is not the differentiator.**

**Third search, 2026-10-19: has anyone priced the straight-line approximation?** Yes.
**nextmv, "Haversine vs. OSRM: distance and cost experiments on a vehicle routing problem"**
reports a mean cost gap per vehicle of roughly **27%**. An **open ride-hailing repository** carries
the issue *"compare Haversine against OSRM on 10 real local routes"*. That is route error, priced.

**Fourth search, 2026-10-19, which inverted the project.** Two things:

- **The decision-level question — does the straight line pick the wrong driver — is still
  unmeasured in public, but its answer is already industry practice.** Grab's engineering blog
  states that the driver nearest by straight line may not arrive first because of detours, and
  ranks by road ETA for that reason. HyperTrack's product documents the standard pattern:
  shortlist by straight line, rank by drive time. A measurement whose answer practitioners
  already act on is a good *finding* and a weak *identity*.
- **No public project combines a road-routed matcher, a stream of real city demand, sharding by
  map cell, and failure testing.** The closest, **EasyRide** and **TaxiOptimizer**, stream NYC taxi
  data through Kafka and Spark but do not route on roads or shard a matcher. The academic
  simulators above route properly but are batch research code, not services. The system-design
  write-ups describe exactly this architecture and never build or measure it.

### What this project is, after four searches

**The system is the project.** A matcher that consumes a real day's requests as a stream, routes
on the real road graph, is sharded by H3 cell across a consumer group written in Java, and is
measured: how many requests per second, how long a match takes, what happens when a worker dies.

**Two approximations are priced inside it, as findings, by the same method (G10):**

1. **The straight-line shortcut.** Over the same candidate drivers, how often does ranking by
   straight-line distance pick a different driver than ranking by road travel time, and what does
   that cost in wait and empty miles on exactly those decisions.
2. **The geographic shard.** A rider near a cell edge whose nearest driver is owned by another
   worker cannot see that driver. How often, and what does it cost. This is the same question
   asked of the architecture instead of the map, and it is the one no write-up prices.

**What this means for the build:** cite AMoD2 and the DRL dispatcher for algorithms, nextmv and
the ride-hailing issue for route error, Grab and HyperTrack for the practice, EasyRide and
TaxiOptimizer for the stream. State plainly what each does that this does not. Never claim
novelty for routing, for streaming, or for the architecture; claim it for the combination being
**built, measured on real demand, and tested under failure** (G13).

---

## Honest limits, written before any results

State these in the README, not a footnote.

1. **The demand is real; the supply is simulated.** Trip records say when and where people were
   picked up. No public dataset says where idle drivers were. Supply is a model (`ASSUMPTIONS.md`).
2. **This is a public taxi / for-hire-vehicle log**, not any company's dispatch system.
3. **Riders in the data never cancel.** Abandonment is a modelled parameter and every wait-time
   and match-rate number must be read with that stated.
4. **Free-flow routing is not live traffic.** Phase 5 adds time-of-day speeds calibrated from the
   trip data; until then travel times are optimistic and labelled so.
5. **Every figure carries its hardware and its seed.** One seed is an anecdote.
6. **One city.** New York is unusually cut up by water, which makes the straight-line error
   visible — the *best* case for Finding 1, not the typical one. Say so.
7. **A sharded replay on one machine is not a production system.** The consumer group is real and
   the rebalance is real. It is not multi-region, not behind a load balancer, and not a dispatcher.

---

## What the repository contains

| Path | What |
|---|---|
| `README.md` | what was built, the numbers, the two findings, the honest limits, the prior art — in that order |
| `SPEC.md` | this file |
| `PREDICTION.md` | predictions written before the measurement that tests them; appended to, never edited |
| `ASSUMPTIONS.md` | the supply model and every modelled parameter, with the value used and why |
| `NUMBERS.md` | every figure, with the results file and seed behind it |
| `BUGS.md` | what broke, what caught it, the fix — the kill-test bugs belong here |
| `configs/` | one YAML per experiment; nothing tunable lives in code |
| `results/phase*/` | one JSON per run, named `<measurement>_<hardware>_seed<N>.json`, plus a `SUMMARY.md` per phase |
| `figures/` | every chart in the README, regenerated from `results/` by one script |
| `deadhead/` | the Python package: data, routing client, spatial index, supply model, the oracle matcher, metrics |
| `consumer/` | the Java consumer group: the service (Phase 3) |
| `docker-compose.yml` | OSRM, Kafka and N consumers, pinned versions, reproducible from a clean machine |
| `tests/` | see "Tests that must exist" |

**The README opens with the system and its numbers**, in plain prose: what it consumes, how it
routes, how it is sharded, requests per second, match latency, what the kill test showed. Then
the two findings. Then limits, then prior art. The words **geospatial**, **distributed**,
**real-time** and **location** appear in that opening because they are true.

---

## The supply model — `ASSUMPTIONS.md`

Supply is the one thing the data does not give, so its model is written down before Phase 2 and
every result inherits it.

| Parameter | Baseline | Why | Sensitivity run |
|---|---|---|---|
| **Fleet size** | N drivers, fixed for the day, chosen so the road-aware matcher serves 90–95% of requests inside the abandonment threshold | A calibration, not a claim about any real fleet. | N × 0.7 and N × 1.3 |
| **Initial positions** | drawn from real **dropoff** locations in the hour before the replay starts | The only location signal in the data for where an idle vehicle plausibly is. | uniform over the demand footprint |
| **After a dropoff** | the driver goes idle at the dropoff point and waits | Repositioning is a dispatcher's decision and this is not a dispatcher (G7). | drift toward the nearest high-demand cell |
| **Trip leg** | the trip takes the **recorded real duration** and ends at the **recorded dropoff** | The real trip is replayed, not re-routed. Only the pickup leg — the deadhead — is simulated. | none |
| **Pickup leg speed** | OSRM free-flow until Phase 5 | Stated optimism, labelled. | Phase 5 |
| **Abandonment** | a rider abandons if the **promised** pickup time exceeds T seconds (T = 600) | Real riders never cancel (limit 3). Promised, not actual: the straight-line rule promises a straight-line ETA and is held to it. | T = 300, 900 |
| **Matching radius** | the H3 ring covering R metres around the pickup | Every rule and every worker searches the same candidate set. | per hour in Phase 5 |
| **Randomness** | one seed drives positions, fleet draw and tie-breaks | Determinism test in `tests/`. | 5 seeds minimum, spread reported |

Anything not in this table is not a parameter; if it becomes one, it goes in the table first.

---

## Phase 0 — verify the data and the map, then predict

**The trip data.** NYC TLC changed its schema: older yellow-taxi files carry pickup and dropoff
coordinates; later files and all for-hire-vehicle files carry only zone IDs. **This project needs
coordinates.** Expected, and to be verified by hand (G4): coordinates through **June 2016**,
zone IDs from July 2016; confirm the coordinate columns survived the later Parquet re-release.
Pick the most recent year that passes. Count rows; report junk coordinates dropped; report the
share of trips whose endpoints snap to the road graph. Pick **one weekday and one weekend day**,
named, neither a holiday. Measure load time for Polars and pandas and drop the loser.

**The road network.** Stand up **OSRM** on the city's OpenStreetMap extract with the **MLD**
pipeline (so Phase 5 can re-customise speeds without rebuilding), and raise the table-size limit.
Use **OSMnx** only to pull and inspect the graph. Measure graph size, memory, single-route
latency, and one `table` call at K × K for K in {10, 50, 200, 1000}. List every bridge and
tunnel edge that crosses a waterway, named (OSM `bridge` / `tunnel` tags); Finding 1 uses it.

**H3.** Start at resolution 8 (edge about 460 m); Appendix A decides. Every figure states its
resolution.

**Then predict, and commit it.** Requests per second one Java worker will sustain, and the
speed-up at 4 and 8 workers. The straight-line wrong-driver rate and its conditional cost. The
share of decisions the geographic shard will hide, at resolution 8 and 4 workers. Whether the
kill test will match anyone twice on the first attempt. A wrong prediction kept in the repository
is worth more than a right one written afterwards.

**Done when:** `PREDICTION.md` committed with the dataset year, confirmed columns, row counts,
the two days, graph size, routing latencies, and all four predictions.

---

## Phase 1 — the road network as the ground truth

**Build:** the travel-time layer on OSRM's `table` service — per-pair if Phase 0 says it is
affordable, otherwise a cached hex-to-hex matrix at the chosen resolution. H3 indexes every
driver and request regardless, because the candidate search is a hex-ring lookup in both designs.

**Measure the disagreement** over two kinds of pair: real pickup → dropoff *trips*, and simulated
driver → real pickup *legs* at the distances the matcher actually compares. For each: road
distance ÷ straight-line distance and road time ÷ straight-line time, median and p90; per-cell
map of the median; the worst pairs listed with the crossing from the Phase 0 list that explains
them. **Name the bridges and rivers it finds.**

**Done when:** the distribution plots for both pair kinds, the worst pairs on a map with crossings
named, and the routing layer benchmarked.

---

## Phase 2 — the stream and the oracle

**Build:** the demand as a Kafka stream and a single-process Python matcher that consumes it.
This matcher is **the correctness oracle** for the service in Phase 3: it sees every partition,
so it has no boundary, and its `assignments` output is what the sharded service is checked
against.

Stream design, fixed here:

- topic `requests`, **keyed by the pickup's coarse H3 cell** (see Phase 3 for why coarse), replayed with an event-time clock at a
  stated speed factor (×1 for latency figures; as fast as possible for throughput);
- topic `assignments`, keyed by `request_id`, one record per decision — the audit trail every
  metric and every test reads;
- the partition count is fixed and recorded; driver state lives in the matcher's memory.

The driver-ranking rule is swappable behind one interface — **(a) straight-line**, nearest by
haversine; **(b) road-aware**, nearest by OSRM travel time — and **both rank the same candidate
set**: the idle drivers in the same H3 ring. If the candidate sets differed, the measurement
would be of the search radius, not of the distance function.

**Finding 1 — the straight-line shortcut.** Two evaluation modes, because after the first
disagreement the two replays diverge and later decisions stop being comparable:

- **Mode A — paired, one-step (the headline).** In the road-aware replay, at every decision,
  also compute what the straight-line rule would have picked from the same candidates. Record
  whether it differed, the extra road time and empty distance the straight-line pick would have
  cost, and whether that route uses a water crossing. Ties count separately. Run the mirror in
  the straight-line replay to show the rate is not an artefact of which world measured it.
- **Mode B — full replay.** Each rule end to end, for the system-level metrics.

**Metrics, kept for the whole project:** wait time (median, p90); deadhead time and distance;
utilisation; match rate inside the abandonment threshold; **promised-ETA error** (promised pickup
time versus actual, per rule); and **wrong-driver rate with its conditional cost** — the share of
decisions where the rules pick different drivers with a non-zero road-time gap, and the wait and
deadhead delta on exactly those decisions, by hour, by cell on a map, and by water crossing.

**Done when:** the oracle runs a full city-day deterministically; the Mode B table; the Mode A
rate and conditional cost with map and hourly breakdown; and a plain statement of what the
shortcut costs — reported just as prominently if it is "barely anything" (G5).

---

## Phase 3 — the service: a Java consumer group sharded by H3 cell

*This is the project.* Phase 2 runs one consumer reading every partition — a replay, not a
system. Real matchers shard, and the obvious way to shard a geospatial matcher is **by map cell**,
which is convenient and has an unpriced cost, exactly like the straight line did.

**Build:** the matcher as a **Kafka consumer group** of N instances in **Java**, each owning the
partitions — and therefore the H3 cells — assigned to it, consuming `requests` and producing
`assignments` with the same schema as the oracle. Java because the consumer-group protocol,
partition assignment and rebalance semantics are JVM-native, the Java client is the reference
implementation, and the throughput question is meaningless measured through a Python client's
GIL. Fixed design decisions:

- the stream is keyed by the **coarse parent cell** (H3 resolution 6, edge about 3.2 km), not
  the fine cell: Kafka's key hashing would scatter adjacent fine cells across workers and every
  decision would lose most of its candidates. Each worker therefore owns contiguous regions, and
  the resolution-8 cell is for candidate search only. The region size is a parameter; Finding 2
  is reported against it;
- at-least-once consumption with an **idempotent assignment store keyed by `request_id`**, and
  offsets committed only after the assignment is durably recorded;
- a driver can be claimed by exactly one request: claim is a compare-and-swap on driver state,
  so two workers cannot both take a driver that straddles their boundary;
- the partition-assignment strategy is a configuration value, so **eager** (`RangeAssignor`) and
  **cooperative** (`CooperativeStickyAssignor`) rebalances are measured against each other;
- the batching window (Phase 4) is a tumbling event-time window inside the worker, W = 0 here.

**Measure three things, on named hardware:**

1. **Throughput and latency.** Requests per second sustained and match latency (median, p99)
   for N ∈ {1, 2, 4, 8} workers against the Phase 2 oracle, at replay speed ×1 and at maximum.
   This is where the compiled consumer earns its place or doesn't — **if Java does not beat
   Python by enough to matter, say so and keep Python** (G12).
2. **Finding 2 — the boundary cost.** Compare the service's `assignments` to the oracle's,
   request by request. Every disagreement is a decision the shard made differently because the
   nearest driver was invisible to it. Report the rate and the conditional wait and deadhead
   cost, by N and by H3 resolution, exactly as Finding 1 is reported (G10). The obvious
   mitigation — a one-ring **halo** of neighbouring cells' driver positions shared with each
   worker — may be built only as a bounded upper line on the same chart; it is not the
   contribution (G7).
3. **The kill test.** Kill a worker mid-replay, at a stated point, K times with different seeds.
   Measure how long the group is unavailable, how many requests were delayed and by how much,
   and **whether any request was matched twice or any driver claimed twice** — checked against
   the oracle, not against the service's own logs. Eager against cooperative. At-least-once
   delivery plus a matcher is a duplicate-assignment bug waiting to happen; the oracle and the
   `request_id`-keyed store are what prove it did not — or show exactly when it does. Every bug
   this finds goes in `BUGS.md` with what caught it.

**Done when:** a throughput and latency table against the oracle with hardware named; a
boundary-cost number with the resolution and N it was measured at; and a kill-test result over K
runs stating whether any rider or driver was ever double-matched, and under which assignor.

---

## Phase 4 — batching, on the service

**Build:** accumulate requests for W seconds and solve the assignment over the batch with road
travel time as the cost — one `table` call per batch, `scipy.optimize.linear_sum_assignment` in
the oracle and the equivalent in the worker, unmatched requests rolling into the next window
until they abandon. In stream terms, a tumbling event-time window.

**Measure:** the full metric set across W ∈ {0, 2, 5, 10, 20, 30, 60} s and matching radius; the
marginal trade — seconds of extra wait per minute of deadhead removed; and whether batching
shrinks or leaves alone the two priced approximations. Batching has diminishing returns; **find
the point and recommend an operating point with its justification.**

**Done when:** a wait-versus-deadhead curve with the operating point stated, and the batching ×
approximation interaction reported.

---

## Phase 5 — when the map changes under you

**Build:** time-of-day speeds calibrated from the trip data itself — every record carries an
odometer distance and two timestamps, so observed average speed by hour (and by zone-hour if it
varies enough) falls out of the demand file. Apply as an hourly multiplier on OSRM durations
first; move to per-segment updates via `osrm-customize` only if per-zone speeds differ enough to
change a decision. State the caveat: observed taxi speed includes stops; a calibration, not a
traffic model.

**Measure:** do Findings 1 and 2 grow or shrink at peak? Does the right matching radius change
by hour?

**Done when:** both findings and the recommended radius reported per hour of the day.

---

## Phase 6 — what it costs to run

**Measure:** wall-clock and machine cost to replay one city-day through the service, at ×1 and at
maximum speed; how it scales with fleet size, radius and N; which stage dominates — routing
calls, the broker, or the worker. Hardware named; cost at a stated cloud list price with its date.

**Done when:** one cost-per-city-day figure with hardware named and the bottleneck identified.

---

## Appendix A — what the hex resolution is worth (optional)

Resolutions 7, 8, 9 (edge about 1.2 km, 460 m, 170 m): candidate-search cost per decision; matrix
size if the hex-matrix design is in use; Finding 1's share attributable to hex aggregation alone;
and Finding 2 as a function of resolution — finer cells mean more boundaries. **Done when:** a
resolution-versus-cost-versus-quality table with a recommendation.

## Appendix B — the charging extension (optional)

`REQUIREMENTS-charging.md`. The same priced-approximation method applied to charger selection,
with real station locations. Does not start until Phases 0–4 are done.

---

## Data

**NYC TLC trip records** — public, large, standard; Phase 0 picks a year with coordinates.
**OpenStreetMap** for the road graph — **OSRM** for routing, **OSMnx** for inspection. **H3** for
spatial indexing and stream partitioning.

## Measurement discipline

- Every number in `NUMBERS.md` with its results file and seed. One seed is an anecdote.
- A weekday and a weekend day. Losses and ties written up as prominently as wins.
- Predictions committed before the run that tests them, and never edited afterwards.
- Every figure names its hardware, H3 resolution, worker count, speed model and abandonment
  threshold.

## Stack

| Layer | Choice | Why this one |
|---|---|---|
| Service | **Java**, Kafka consumer group | Consumer-group protocol and rebalance semantics are JVM-native; the Java client is the reference implementation; a throughput number through a Python client's GIL would not mean anything. **If it does not beat Python by enough to matter, say so and keep Python.** |
| Transport | **Kafka** | The demand replay is a stream partitioned by H3 cell; the consumer group and its rebalance are the subject of Phase 3, not decoration. |
| Routing | **OSRM** | What the cited prior art uses, so route numbers are like-for-like; the many-to-many `table` call is what makes per-pair routing affordable. MLD build so Phase 5 can re-customise speeds. |
| Spatial index | **H3** | Candidate search is a hex-ring lookup, the stream is partitioned by cell, and the shard boundary is a cell boundary. |
| Graph inspection | **OSMnx** | Pull the drivable graph, find the bridges and crossings. Not the routing engine. |
| Oracle and analysis | **Python**, Polars or pandas (Phase 0 decides), SciPy, Matplotlib | The single-consumer reference, the measurement, the figures. |
| CI | pytest and the Java tests in GitHub Actions; `docker-compose` for OSRM, Kafka and N workers | Reproducible from a clean machine. |

**Refused on purpose:** a new language for its own sake (Java is already in use and covers the
JVM requirement); **Spark / Hive / Presto** (one city-day fits in memory, and a cluster job would
lie about the scale); **Flink** (the Phase 4 window is legible without a second stream
processor); **a second datastore** (the `assignments` topic is the audit trail).

## Tests that must exist

- **Determinism:** the same config and seed produce byte-identical `assignments` from the oracle twice.
- **Oracle agreement:** with N = 1 the service's `assignments` equal the oracle's exactly.
- **Same candidates:** for every decision, both ranking rules were offered the same candidate set.
- **No double match:** no `request_id` appears twice with different drivers, and no driver is
  claimed by two live requests, in every run and every kill test.
- **Distance sanity:** haversine never exceeds road distance; road time is never negative.
- **Spatial sanity:** every H3 ring lookup stays within the stated radius; every snapped point is
  within a stated distance of its input.
- **Ledger:** every number in `README.md` appears in `NUMBERS.md` with a results file that exists.
