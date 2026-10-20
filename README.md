# Deadhead

A real-time ride-matching service built on a real day of New York taxi demand, routed on the real
road network, sharded by map cell across a Java consumer group, and measured under worker failure.
**Deadhead** is the industry term for a vehicle driving empty between rides; it is the cost the
two findings here are measured in.

**Status: Phase 2 complete — the demand streams through Kafka, a single-process matcher replays a
full day, and the straight-line shortcut is priced at the level of the decision.** The sharded Java
service (Phase 3) is not built yet. `SPEC.md` is the plan; `PREDICTION.md` holds the guesses written before each
measurement, with the outcome beside them; `NUMBERS.md` holds every figure with the file behind it;
`ASSUMPTIONS.md` is the supply model.

## Honest limits (read these first)

1. **The demand is real; the supply is simulated.** Trip records say when and where people were
   picked up. No public dataset says where idle drivers were, so drivers are a model
   (`ASSUMPTIONS.md`, from Phase 2).
2. **This is a public taxi log**, not any company's dispatch system. Nothing here is a claim about
   how anyone's marketplace performs.
3. **Riders in the data never cancel.** Abandonment is a modelled parameter.
4. **Free-flow routing is not live traffic** until Phase 5, and a calibration after it.
5. **Every number names its hardware and seed.** The lane so far is one laptop: Apple M5, 16 GB.
6. **One city.** New York's rivers make straight-line error unusually visible: the best case for
   that finding, not the typical one.
7. **A sharded replay on one machine is not a production system.**
8. **The map is ten years newer than the demand** (OpenStreetMap 2026-10-18; trips June 2016).

## What Phase 0 established

- **Coordinates.** The TLC's own trip files have been rewritten without pickup and dropoff
  coordinates for every year. NYC Open Data's copy of the 2016 file still has them. The week of
  13–19 June 2016 was pulled: 2,575,502 trips, 97.96% clean. The two study days are Thursday 16
  June (391,042 requests) and Saturday 18 June (362,824).
- **The road network.** 642,217 nodes and 756,257 edges behind OSRM. One route answers in about
  1 ms; one pickup against 50 candidate drivers in 6 ms. Routing per pair is affordable, so no
  hex-to-hex matrix is built.
- **Water crossings, computed not hand-picked.** 144 named bridges and tunnels whose road
  geometry crosses water, from the Verrazzano-Narrows to the Harlem River bridges.

## Phase 1 — how far the road disagrees with the straight line

**The supply side is simulated** (`ASSUMPTIONS.md`): an idle driver is a real dropoff from the
prior hour, thinned to a stated density. The demand side, the map, and every route are real.

Two kinds of pair were routed on the real road network and compared with the straight line between
their endpoints. All figures are in `NUMBERS.md`; the plots are in `figures/`.

- **Real trips** (40,000 per day, pickup to dropoff): the road is **1.32× the straight line at the
  median, 1.63× at the 90th percentile**, the same on a Thursday and a Saturday. A quarter of trips
  cross water, and those are barely worse (1.36 against 1.31). On a committed trip the river costs
  little.
- **Simulated pickup legs** (driver to pickup, the 50 nearest candidates by straight line for 6,000
  requests per day): the road is **2.0× the straight line at the median and 4.9× at the 90th
  percentile**. The leg a matcher compares is short, 270 m at the median, and short legs are where
  one-way streets, divided avenues and limited-access roads bite: **2.7× for legs under 250 m,
  1.9× for 250–500 m, 1.5× for 1–2 km.** Those per-band numbers hold when the supply density is
  halved or more than doubled; only the headline moves, because density sets the leg length.
- **The worst pairs are named, not hand-picked.** Outside the airports, the worst legs are drivers
  already inside a bridge or tunnel approach: a driver 122 m from a pickup but committed to the
  Brooklyn-Battery Tunnel is 8.2 km away by road (67×); the Queensboro Bridge upper level (62×),
  the Queens-Midtown Tunnel (46×) and the Williamsburg Bridge (54×) do the same. The worst trips
  start on one-way avenues. The airport terminal loops are worse than any street: a driver on
  JFK Terminal 4 Departures 103 m from a pickup is 8.9 km away by road.
- **The prediction was wrong.** `PREDICTION.md` guessed 1.35× for legs; the measurement is 2.0×.
  The guess for trips was close. The record stays.

![distributions](figures/phase1_ratio_distributions.png)
![legs by distance](figures/phase1_legs_by_distance.png)
![worst pairs](figures/phase1_worst_pairs.png)

**The routing layer** answers 3,500 routes per second and 2,000 one-to-50 tables per second from
eight Python threads against one OSRM container on the laptop. A day of 400,000 decisions is a few
minutes of routing. Per-pair routing stays; no hex-to-hex matrix.

## Phase 2 — the stream, the oracle, and Finding 1

**Supply is simulated** (`ASSUMPTIONS.md`): 7,500 drivers, starting at real dropoffs, idle at their
dropoff after each trip, cruising toward the previous hour's demand after five idle minutes. The
demand, the map and every route are real. The fleet size is a calibration: the smallest fleet at
which the road-aware rule matches 90–95% of the day inside a promised 600 s. **The model as first
written, with no repositioning, could not serve the day at any size tried**, and that failure is
recorded in `NUMBERS.md` alongside the baseline.

**What runs.** A day of demand is a Kafka topic with 8 partitions keyed by the pickup's coarse H3
cell. One Python consumer reads every partition, merges them into event-time order, keeps the fleet
in memory, and writes one record per decision to an `assignments` topic keyed by request id. Two
ranking rules sit behind one interface and rank the same candidate set, the 50 nearest idle drivers
by straight line within three hex rings: **straight-line** picks the nearest by air, **road-aware**
picks the nearest by road time. At every decision the matcher also records what the other rule
would have picked. It runs at 235 decisions per second, 4 ms per decision, a day in 28 minutes, and
the same seed gives byte-identical assignments twice. No request was matched twice and no driver
claimed twice in 28 replays. All figures below are five-seed means; the spread is in `NUMBERS.md`.

**Finding 1: what the straight-line shortcut costs, decision by decision.**

- On the weekday, the straight line picks a slower driver than the road in **44% of decisions**.
  On exactly those decisions the rider waits **40 s longer at the median and 130 s at the 90th
  percentile**, and the driver drives 237 m further empty.
- End to end (each rule replayed on its own), the road-aware rule cuts the median wait from
  **125 s to 103 s** and empty driving from **912 m to 763 m per ride**: 50,000 empty kilometres a
  day across the fleet. Match rates are the same within half a point.
- The straight-line rule's promised pickup time is missed on **94% of rides**, by 1.8× at the median.
- **Water is where the cost concentrates.** Straight-line picks whose route crosses a river are 3%
  of decisions and carry **a quarter of the total extra wait**: 157 s at the median against 38 s
  elsewhere. On the map the worst cells sit at the East River crossings and the RFK Bridge approach.
- **The rate depends on how dense supply is.** With 3,000 drivers it is 15%; with 9,750 it is 53%;
  in the evening peak, when the nearest driver is a kilometre away, it falls to about 20%. The
  conditional cost does not move: 35–45 s at the median in every hour and at every fleet size.
- The weekend repeats the weekday: 50% of decisions, 40 s median.
- **The prediction was wrong on the rate** (15% predicted) **and right on the water share.**

![by hour](figures/phase2_wrong_driver_by_hour.png)
![map](figures/phase2_wrong_driver_map.png)
![mode b](figures/phase2_mode_b.png)
![calibration](figures/phase2_calibration.png)

## Prior art, by name

Road-network ride-sharing simulators exist and are good: **AMoD2**, a deep-RL taxi dispatcher,
**tsharesim**. The cost of straight-line distance on a committed route is published by **nextmv**
(Haversine vs. OSRM). **Grab**'s engineering blog and **HyperTrack**'s product both rank drivers by
road ETA for exactly the reason this project measures. **EasyRide** and **TaxiOptimizer** stream
NYC taxi data through Kafka. What is built and measured here that is not there: the combination,
as a sharded service on real demand, with a kill test checked against an oracle, and two
approximations priced at the level of the decision.

## Reproduce Phase 0

```bash
uv sync --extra dev
brew install osmium-tool
curl -o data/osm/new-york-latest.osm.pbf https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf
scripts/osm_clip.sh && scripts/osm_prepare.sh && scripts/osrm_build.sh
docker compose up -d osrm
.venv/bin/python -m deadhead.fetch_opendata 2016-06-13 2016-06-19 data/tlc/opendata
.venv/bin/python -m deadhead.phase0_data data/tlc/opendata results/phase0
.venv/bin/python -m deadhead.phase0_crossings data/osm results/phase0
.venv/bin/python -m deadhead.phase0_osrm data/tlc/opendata/yellow_2016-06-16.parquet results/phase0
.venv/bin/python -m pytest -q
```

## Reproduce Phase 2

```bash
docker compose up -d kafka
.venv/bin/python -m deadhead.stream configs/phase2.yaml                 # both days into Kafka, ~3 s
.venv/bin/python -m deadhead.oracle configs/phase2.yaml --day 2016-06-16 --rule road --seed 0   # one replay, ~28 min
scripts/phase2_runs.sh 7500                                              # all 28 replays, ~3 h on one laptop
.venv/bin/python -m deadhead.phase2_metrics configs/phase2.yaml
.venv/bin/python -m deadhead.figures results figures
.venv/bin/python -m pytest -q
```

## Reproduce Phase 1

```bash
.venv/bin/python -m deadhead.phase1_disagreement configs/phase1.yaml   # ~2 min; writes results/phase1 and data/phase1
.venv/bin/python -m deadhead.figures results figures
.venv/bin/python -m pytest -q
```
