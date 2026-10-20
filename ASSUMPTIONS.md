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

## What the replay will add (Phase 2, not yet run)

These rows are the plan from `SPEC.md` and are not yet exercised by any result.

| Parameter | Baseline | Sensitivity |
|---|---|---|
| Fleet size | N, fixed for the day, chosen so the road-aware matcher serves 90–95% of requests inside the abandonment threshold | N × 0.7, N × 1.3 |
| Initial positions | real dropoffs in the hour before the replay starts | uniform over the demand footprint |
| After a dropoff | idle at the dropoff point | drift toward the nearest high-demand cell |
| Trip leg | the recorded real duration, ending at the recorded dropoff | none |
| Abandonment | the rider abandons if the **promised** pickup time exceeds 600 s | 300 s, 900 s |
| Randomness | one seed drives positions, fleet draw and tie-breaks | 5 seeds minimum |
