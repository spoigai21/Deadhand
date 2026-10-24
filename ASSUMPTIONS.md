# ASSUMPTIONS — the supply model and every modelled parameter

The demand side of this project is real: recorded pickups and dropoffs with coordinates and
timestamps. The supply side is not in any public dataset, so it is a model, and this file is the
model. Every parameter below has the value used, the reason, and the sensitivity run that bounds it.
**Anything not in this file is not a parameter.** If something becomes one, it is added here first.

The ground rule is G2 in `SPEC.md`: supply is simulated and is labelled as such everywhere.

## Where an idle vehicle is

| Parameter | Value | Why | Sensitivity |
|---|---|---|---|
| Idle positions | real **dropoff** locations from the **prior 60 minutes** (`supply_pool_window_s: 3600`) | The only signal in the data for where an empty vehicle plausibly is. A vehicle that just dropped someone off is empty, at a known place, at a known time. | none; this is the data, not a choice |
| Idle fraction | **0.2** of those dropoffs (`idle_fraction`), drawn with the run's seed | Not every dropoff is a vehicle that is still idle a few minutes later; most are re-hired at once. 0.2 is a density choice, not a measurement of any real fleet. It puts a few thousand idle positions across the city in a busy hour, which is the order of magnitude a fleet of some thousands of vehicles would leave idle. | **0.1 and 0.5**, both run in Phase 1 (`legs_sensitivity`). A denser fleet makes legs shorter and the short-leg ratio larger. The result is reported by straight-line distance band so it can be read at any density. |
| Matching radius | the H3 **resolution-8** cells within **3 rings** of the pickup cell (`ring_k: 3`) | A hex-ring lookup is the candidate search in every design in `SPEC.md`. Three rings at resolution 8 reach roughly 2 km from the pickup. | Phase 5 may vary it by hour |
| Candidate set | the **nearest 50 by straight line** inside the radius (`candidates_k: 50`) | This is the shortlist every rule and every worker sees (G10). The cap is by straight line because that is how a shortlist is cheap to build; the ranking rules then differ in how they order it. | Appendix A |

## How a pickup leg is costed

| Parameter | Value | Why | Sensitivity |
|---|---|---|---|
| Road distance and time | OSRM, car profile, **free-flow** speeds | What the cited prior art uses; the only routing engine in the stack. Free-flow is stated optimism until Phase 5 recalibrates by hour from the trips' own odometer and timestamps. | Phase 5 |
| Straight-line time | straight-line distance ÷ **one constant speed**, the median road speed OSRM assigns over the sampled real trips (`straight_line_speed_mps` in the Phase 1 results, about 8.3 m/s) | A straight-line rule needs a speed to turn its distance into a promised time. One constant is what a simple simulator uses, and taking it from the same routing engine means the time ratio measures geometry plus speed structure, not a speed-unit mismatch. | none |
| Snap tolerance | pairs where either endpoint snaps more than **250 m** to the road graph are excluded and counted (`max_snap_m`) | A point 250 m from any road is a coordinate error (a point in the river, a park, a pier) and routing it says nothing about the matcher. Phase 0 measured a 5 m median snap over real pickups. | counts reported per run |
| Minimum pair length | pairs shorter than **100 m** in a straight line are excluded and counted (`min_straight_line_m`) | The ratio is undefined at zero and meaningless under the snap distance. | counts reported per run |
| Airports | pairs touching the JFK or LGA bounding box are flagged and reported separately | Terminal loops produce honest but extreme ratios that are not about the city's streets. They are kept in the distributions and shown apart. | `distance_ratio_excluding_airports` |

## The replay (Phase 2)

Phase 2 runs the whole day as a stream: the oracle consumes every request in event-time order and
keeps the fleet in memory. The pickup-leg rows above apply unchanged. These rows are added.

| Parameter | Baseline | Why | Sensitivity |
|---|---|---|---|
| **Fleet size** | **N = 7,500** (`fleet_size` in `configs/phase2.yaml`), fixed for the day | Chosen from the calibration curve (`results/phase2/finding1_*.json`, `fleet_calibration`) as the smallest fleet at which the road-aware rule matches 90–95% of requests inside the abandonment threshold. A calibration, not a claim about any real fleet. | N × 0.7 and N × 1.3 |
| **Initial positions** | N real dropoffs from the hour before midnight, drawn with the seed | The only location signal for where an empty vehicle plausibly is at the start. | none |
| **Trip leg** | the matched driver takes the **recorded real duration** and ends at the **recorded dropoff** | The real trip is replayed, not re-routed. Only the pickup leg is simulated. | none |
| **After a dropoff** | the driver goes idle at the dropoff point | Repositioning is a dispatcher's decision and this is not a dispatcher (G7). | see the next row |
| **Repositioning** | **cruise toward demand**: a driver idle for `cruise_after_s` (300 s) drives to the busiest resolution-8 cell of the previous clock hour within `cruise_ring_k` (5 rings, about 4 km), to a real pickup point drawn from that cell and hour; a driver already in the busiest nearby cell stays. The driver is unavailable while driving and goes idle at the target. | **The idle-at-dropoff model failed its own calibration and the failure is recorded** (`repositioning: none` runs in `NUMBERS.md`): drivers stranded at outer-borough and airport dropoffs never return, and even 8,000 drivers serve barely half the weekday with 60% of the fleet idle. A fleet that cruises for fares is what the recorded fleet did; the previous hour's real pickups are the map it cruises on, with no lookahead. Cruising kilometres are counted and reported apart from the matcher's deadhead. | `repositioning: none` |
| **Abandonment** | the rider abandons if the **promised** pickup time exceeds **T = 600 s**; an abandoned request claims no driver | Real riders never cancel (limit 3). Promised, not actual: the straight-line rule promises straight-line distance ÷ `straight_line_speed_mps` and is held to it, so it never abandons inside the ring; the road rule promises the road time. | T = 300, 900 |
| **No candidates** | a request with no idle driver in the ring is unserved and counted separately from abandonment | Both are unmatched; they have different causes. | none |
| **Wait** | the road time of the chosen driver's pickup leg at free-flow speed, for both rules | The straight-line rule's rider still waits for the road. | Phase 5 |
| **Randomness** | one seed drives the initial positions, the cruise targets and tie-breaks | The determinism test runs the oracle twice. | **5 seeds**, spread reported |
| **Stream** | `requests-<day>`, 8 partitions, keyed by the pickup's resolution-6 parent cell; the oracle merges partitions into event-time order by watermark | The key is Phase 3's sharding key; the oracle sees every partition so it has no boundary. | Phase 3 |

## The service (Phase 3)

The Java consumer group runs the same model as the oracle. Everything above applies unchanged. These rows are what
the sharded service adds, all in `configs/phase3.yaml`.

| Parameter | Baseline | Why | Sensitivity |
|---|---|---|---|
| **Region** | a worker owns the partitions the group assigns it; a partition holds every resolution-6 cell whose key hashes to it (CRC32 of the key modulo 8, the producers' default partitioner); a worker sees the idle drivers in the cells of its own partitions and no others | This is the design under test: the cheap geographic shard, with the boundary it implies. A ring-3 candidate search that reaches a cell of another worker's partition sees nothing there. | key resolution **7** (service), **5–8** (Mode A); a one-ring halo as the upper line (Mode A only, G7) |
| **Worker count** | N ∈ {1, 2, 4, 8} over the fixed 8 partitions; eager (`RangeAssignor`, contiguous partition ranges) or cooperative (`CooperativeStickyAssignor`) assignment | The two assignors the Java client ships; Mode A uses the range mapping because it is the deterministic one. | both assignors in the kill test |
| **Driver state** | each partition's drivers are a fold of that partition of the `fleet-<run>` log; a driver is in exactly one partition's log, the one its current cell belongs to; every record carries a version that only increases per driver | A killed worker's successor rebuilds the partition from the log and cannot resurrect a driver another region already holds. | none |
| **Durability order** | for a match: the driver's departure record is acknowledged, then the decision record, then the arrival and the assignment are sent; offsets are committed after the batch is flushed | At-least-once consumption with an idempotent store (the decided requests in the fold): a successor re-emits, never re-decides. The order is what makes a kill between any two steps repairable from the log. | the kill test |
| **Event time across workers** | at maximum replay speed no worker processes a request more than `watermark_slack_s` (30 s) of event time ahead of the slowest live worker's next request, through a shared clock topic; at a paced speed the wall clock does this | The oracle merges partitions by watermark in one process; the group needs the same across processes, or a sparse partition's worker replays its day in seconds and trades drivers with regions still at breakfast. | none; the slack bounds the skew, which is reported |
| **Time passing without requests** | a worker releases and cruises its drivers up to the group's allowed time even when it has no request to process | A partition with 600 requests a day still has drivers whose trips end. | none |
| **Failure detection** | `session.timeout.ms` 6,000, `heartbeat.interval.ms` 2,000; a peer's clock expires after two session timeouts or at the next rebalance | The Java client's defaults (45 s) would make every kill-test number a measurement of the default. | none; stated with every figure |
| **Cruise target point** | the real pickup point at index hash(driver, cruise count) of the cell-hour's points sorted by coordinates (`cruise_point: hash`) | The Phase 2 oracle drew it with the seeded RNG, which a worker in another language cannot reproduce; the hash makes the Java worker and the reference oracle draw the identical point, so the N = 1 service is a mirror of the oracle and any disagreement at N > 1 is the shard's. | Phase 2 used `random`; same model, different draw |
| **Hardware** | workers as local JVM processes on the same machine as the broker and OSRM, named in every results file | One machine is what there is; the throughput figures are a measurement of this machine and say so. | none |

## Batching (Phase 4)

The batched oracle (`deadhead/batch.py`) and the worker with a window run the same model. These rows are what the
window adds, all in `configs/phase4.yaml`.

| Parameter | Baseline | Why | Sensitivity |
|---|---|---|---|
| **Window** | a tumbling event-time window of W seconds aligned to midnight; a request is decided when the window holding its arrival closes, so a rider queues between 0 and W seconds before any driver is looked at | The stream definition of batching (`SPEC.md`), the same in the oracle and in each worker; a worker closes a window only once every partition it owns is known to be past it. | **W ∈ {0, 2, 5, 10, 20, 30, 60} s** |
| **The solve** | one road-time table from the union of the batch's candidates to the batch's pickups, then a minimum-cost assignment over road time; a pair is on offer only if the rider's promise (time already waited plus the road time) is within T; the solve serves the most riders that can be served, then the least total road time | The batch's objective is total rider wait, with service first: leaving a servable rider for a cheaper total is not what a dispatcher is for. One `table` call per batch (split into blocks only past the router's 1,000-location limit, never needed in the runs). | none |
| **Candidates** | per rider, exactly as at W = 0: the idle drivers within `ring_k` rings of the pickup cell, nearest `candidates_k` by straight line; the batch's driver set is their union | The same shortlist as the single decision, so the window is the only change. | ring 2 and 5 (the matching radius) |
| **Rolling** | a rider not served when the window closes waits for the next window, as long as that window closes within T of the request; a rider whose every candidate promises more than T abandons at once (the W = 0 rule); a rider never served is counted as abandoned if a driver was ever on offer and as no-candidates otherwise | `SPEC.md`: unmatched requests roll into the next window until they abandon. The patience T is the one abandonment parameter; the rider's whole wait, queue included, is held to it. | `roll_unserved: false`: an unserved rider is final at once, as at W = 0 |
| **Wait** | queue time plus the chosen driver's road time | Both are the rider's. The leg alone is reported beside it, because the leg is the driver's empty time. | none |
| **The straight-line mirror** | the same batch solved with straight-line time as the cost and the promise, over the same candidates; compared rider by rider with the road solve | Finding 1 under batching, paired (G10). A batch solve gives and takes: some riders get a faster driver from the straight-line solve, so the net rider-hours are reported beside the gross. | none |
| **The shard mirror** | the same batch solved as N workers would, each over the drivers of its own regions (key resolution r, contiguous partition ranges); configurations `batch_shards` | Finding 2 under batching, paired, with the same visibility rule the Phase 3 workers have. | `s6n4` (the Phase 3 headline), `s8n4` |
| **W = 0** | the Phase 3 reference oracle, not re-run | Same model, same seeds, same cruise point. | — |

## Time-of-day speeds (Phase 5)

| Parameter | Baseline | Why | Sensitivity |
|---|---|---|---|
| **The calibration** | every OSRM duration (pickup legs and cruises) is multiplied by the hour's factor: the median, over the Phase 1 routed real trips picked up in that hour, of recorded duration over free-flow duration (`data/phase5/speed_multipliers.json`, from `deadhead/phase5_speeds.py`) | The trip data carries an odometer distance and two timestamps, so the day's speed profile falls out of it; the routed trips give the factor relative to the router that the matcher uses. **Observed taxi time includes stops, lights and the pickup itself: this is a calibration of free-flow routing to recorded taxi time, not a traffic model.** | free flow (the Phase 2–4 replays) |
| **Zones** | not applied. Zone-hour factors (pickup resolution-6 cell × hour, at least 30 trips) are computed and their spread reported; a per-segment update (`osrm-customize`) is built only if a zone factor changes the road-best driver often enough to matter | `SPEC.md`: hourly multiplier first, per-segment only if zone speeds differ enough to change a decision. The test is run on the Phase 1 candidate legs (the driver's own zone-hour factor against the city's hour). | — |
| **What the factor changes** | the leg time and so the promise, the wait, abandonment and when a driver is free again; a rider's straight-line promise is still distance over one constant speed | A uniform factor within an hour scales every candidate alike, so the road rule's pick, and therefore Finding 1's and Finding 2's rates, do not move by construction; their costs in seconds do, and the fleet's state does, which moves the rates by hour through supply. | — |
| **The radius** | the model keeps ring 3 and the nearest 50; beside every decision, the road-best within 1, 2, 4 and 5 rings of the nearest 100 idle drivers within 5 rings is scored against the model's pick (`ring<r>_x`, `ring<r>_v`) | Mode A for the radius: what each ring would have cost or gained, by hour, without changing the replay. The recommended ring per hour is the smallest whose loss against ring 5 (a driver at least 30 s faster, or a driver where the ring has none) is under 1% of the hour's decisions; the rule is stated with the result. | the threshold and the tolerance are parameters of the metric, not of the model |
