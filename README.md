# Deadhead

A real-time ride-matching service built on a real day of New York taxi demand, routed on the real
road network, sharded by map cell across a Java consumer group, and measured under worker failure.
**Deadhead** is the industry term for a vehicle driving empty between rides; it is the cost the
two findings here are measured in.

**Status: Phase 0 complete — data and map verified, predictions committed.** No results yet.
`SPEC.md` is the plan; `PREDICTION.md` holds the guesses written before any measurement;
`NUMBERS.md` will hold every figure with the file behind it.

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
