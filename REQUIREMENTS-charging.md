# Deadhead — the charging extension (Phase 8)

**Status: requirements, not a build order.** Drafted 2026-10-18, revised 2026-10-19.

`SPEC.md` Phases 0-7 are unchanged and come first. This is **Phase 8**, and it is written as an
extension because the thesis has to survive it. If adding charging turns Deadhead into a
different project, it does not get added.

---

## 0. Why charging is the bridge, and not a bolt-on

`SPEC.md` ground rule G6: *if the project drifts back to "vary driver supply, plot wait time",
stop.* G7: *never sell this as a better dispatcher; sell the measurement.* Those two rules decide
the shape of everything below.

Deadhead's claim is one sentence: **route error is published, decision error is not.** nextmv and
a public ride-hailing repository have both measured how wrong the straight line is about a trip
you have already decided to take. What is unmeasured is what it costs to *choose* wrong because
of it.

Charging is the same question asked where the answer is **worse**, and that is the whole
justification:

| | A pickup | A charging detour |
|---|---|---|
| Typical distance | short, often within one zone | long, and crosses the city |
| Straight-line error | bounded | compounds — the river you ignored is now in the middle of the trip |
| Cost of being wrong | one rider waits | a vehicle is out of service for the length of the error, **plus** it queues behind every other vehicle that made the same mistake |

That last cell is the finding worth having. A pickup misrouted by two minutes costs two minutes.
A charger chosen on straight-line distance can send five vehicles to the same site because they
all computed the same wrong "nearest", and the queue may be **superlinear in the approximation
error**. The simple simulators have no charging and the serious ones have no straight-line
baseline to compare against, so the comparison is open.

**So Phase 8 is the same measurement, not a new project.** It stays a methods question about a
**fleet** — vehicles, positions, utilisation, time out of service — and it never becomes a
charging dispatcher.

---

## 1. Hard requirements

These inherit every ground rule in `SPEC.md` and add the following.

| # | Requirement |
|---|---|
| R1 | **Charger locations are REAL, from the NREL Alternative Fuels Data Center station API** (public, free key). Not generated, not jittered, not "representative". |
| R2 | **Battery state, charge curves and vehicle energy use are SIMULATED and labelled as such in the README**, in the same sentence as the real data — the same treatment supply gets under G2. There is no public dataset of individual vehicle state of charge. |
| R3 | **No vehicle model is named. No manufacturer is named.** A generic EV with a stated battery capacity and a stated consumption rate. Naming a make invites a claim about that make's real behaviour, which is G3. |
| R4 | Every number in `NUMBERS.md` with its results file and seed (G1). |
| R5 | If charging turns out not to change the straight-line conclusion, **that is the result** and it leads the write-up (G5, G12). |
| R6 | Prior art named in the README before any claim. Charging-aware fleet simulators exist in the academic literature; the narrow claim is the **comparison to a straight-line baseline**, not the simulation. |
| R7 | **The charger snapshot and the trip data are from different years, and the README says so.** The demand is from a year with coordinates (2016 or earlier); the station list is pulled today. The measurement is "this demand against today's charger network", and it is never presented as a historical reconstruction. |

---

## 2. Data

| Source | What it gives | Real or simulated | Verify in Phase 0 / R8.1 |
|---|---|---|---|
| NYC TLC trip records | demand: real pickups and dropoffs, with coordinates | **real** | already a Phase 0 item — the year must carry coordinates, not zone ids |
| OpenStreetMap, routed by **OSRM** (OSMnx to pull and inspect) | the drivable road graph and travel times | **real** | node/edge counts, single-route latency, `table` cost at batch size |
| **NREL AFDC station API** | charger coordinates, connector types, port counts, network, open date | **real** | **count the stations yourself and check the fields** (G4). Filter to electric, public access, open status, DC fast charging, inside the city bounding box. Record the pull date in `NUMBERS.md`; stations open and close. Keep the raw response in `results/phase8/`. |
| Battery capacity, consumption, charge power, charge thresholds | state of charge over time | **simulated** | stated constants in `configs/charging.yaml`, cited to a public spec sheet without naming the vehicle, never presented as measured |
| Charger occupancy / queueing | who is plugged in right now | **simulated** | no public feed exists. Say so. |

**The honest line for the README**, which must appear next to the first Phase 8 chart and not in
a footnote: *demand and charger locations are real; vehicle supply, battery state and charger
occupancy are simulated, and the chargers are today's network against an earlier year's demand.*

### The vehicle and charger model — `ASSUMPTIONS.md`, Phase 8 section

| Parameter | Baseline | Sensitivity |
|---|---|---|
| Battery capacity | one stated figure in kWh | ±25% |
| Consumption | one stated figure in kWh per km, applied to road distance (not straight-line distance — the vehicle drives the road) | ±25% |
| Charge power | one stated DC fast-charge figure in kW, constant from the arrival state of charge to the target | a tapered curve, as the bound on the constant-rate approximation (§4) |
| Decide to charge when | state of charge falls below a stated threshold at a dropoff | two thresholds |
| Charge to | a stated target state of charge | one alternative |
| Starting state of charge | drawn uniformly from a stated band at the start of the replay | one alternative band |
| Queue | first come, first served; one vehicle per port; ports from the station record | ports × 0.5 as the pessimistic case |

A vehicle driving to a charger is **deadhead**, and it is counted by the Phase 2 metric. A
vehicle charging or queueing is **out of service**, and requests it would otherwise have served
are the cost.

---

## 3. The requirements, each with its own "done when"

### R8.0 — Pre-register the answer

Same discipline as Phase 0. **Before any of it runs**, append to `PREDICTION.md`:

- predicted ratio of road travel time to straight-line travel time **for charging detours**,
  stated as a number, next to the already-committed prediction for pickups;
- a prediction of whether the error is **larger** for charging trips than for pickups, and why;
- a prediction of how many vehicles pile onto one site under straight-line selection, and the
  predicted shape of the pile-up relationship (linear, superlinear, absent).

**Done when:** committed, with a hash, before the first charging run.

### R8.1 — Chargers on the graph

Snap every real station to the road network. Build the travel-time lookup from any hex to every
station, at the H3 resolution Phase 5 settled on (or the resolution fixed in §6).

**Measure:** how far each station moves when snapped (a station snapped 400 m to the wrong side
of a divided road is a silent error); stations that fall outside the graph entirely; stations
inside the bounding box but outside the demand footprint; matrix build time and memory.

**Done when:** a station table with snap distances, and the count of stations discarded and why.

### R8.2 — The disagreement, again

Over all (vehicle position → station) pairs that a vehicle could plausibly consider, the
distribution of **road travel time ÷ straight-line travel time**, compared directly against the
Phase 1 distributions for pickup legs and trips.

**This is the headline.** One chart, three distributions, same axes.

**Done when:** the distributions are plotted together and the difference is stated as a number
with a confidence interval. **If charging detours are no worse than pickups, say so in the first
line of the Phase 8 write-up** and Phase 8 stops here, having answered its question.

### R8.3 — Two charger-selection rules

The same swappable-rule structure as Phase 2's matchers:

1. **straight-line nearest** — the baseline, and what a simple simulator would do;
2. **road travel time nearest** — the honest version.

Both rules choose from the same candidate set, exactly as Phase 2 requires of the matchers.

Nothing cleverer. A third rule that considers queue length is the obvious next idea and is
**explicitly out of scope** — it turns the project into "a better charging dispatcher", which is
G7. If it gets built at all it is reported as a bounded upper line on the same chart, never as the
contribution.

**Measure, per rule:** time out of service per vehicle; distance driven empty to charge
(deadhead); queue depth at the busiest site; rides unserved because a vehicle was charging or
queueing. Report the paired one-step disagreement (Mode A in Phase 2) as well as the full replay.

**Done when:** a table of those four metrics for both rules, over the same demand replay and the
same seeds.

### R8.4 — The pile-up

Specifically: under straight-line selection, how often do **k vehicles choose the same station
within one window** when road travel time says they should not have. Report the distribution of
k, the worst site, and the extra out-of-service time that pile-up caused.

This is the superlinear claim from §0. It is the thing that would be new.

**Done when:** k is plotted against approximation error for that origin, and the relationship is
stated plainly — linear, superlinear, or absent — with its sensitivity to the queue model.

### R8.5 — Cost to serve, with energy

Extend Phase 6's single number so it carries energy: cost per completed ride including the
empty distance driven to charge and the rides lost to charging time.

**Done when:** one number, both rules, in a sentence an engineer could paste into a planning doc.

---

## 4. Honest limits, written before any results

Copied forward from `SPEC.md`'s discipline, and extended:

- **No charger occupancy feed exists**, so queueing is modelled, and a modelled queue is the
  weakest link in R8.4. State the model's assumptions in the README and show the result's
  sensitivity to them — if the pile-up finding disappears under a plausible alternative
  assumption, it is not a finding.
- **Charge curves are nonlinear and vehicle-specific.** A single constant rate is an
  approximation, and this project is *about* pricing approximations, so the irony has to be
  handled in the open: state it, and bound its effect with the tapered-curve sensitivity run.
- **Station data is a snapshot, and from a different year than the demand** (R7). Stations open
  and close, and the network at the time of the trip data was far smaller. Record the pull date.
- **The recorded fleet was not electric.** The demand is real; the vehicles that served it are
  replaced in the model by generic EVs. No claim is made about the real fleet's energy use.
- **One city.** Every Deadhead result is a claim about one road network, and New York's water is
  the best case for the finding, not the typical one.

---

## 5. What it reports

Two sentences, each a measurement with its real/simulated split, and neither shipping until
`NUMBERS.md` backs it:

> *Over N real trips on the city road graph, straight-line selection understated travel time by
> X%, and the error was Y× worse for charging detours than for pickups.*

> *Straight-line charger selection sent k vehicles to the same site, turning an approximation
> error into a queue — a [superlinear / linear / no] relationship between the two, over real
> station locations and simulated battery state.*

Both are **measurements**, not claims about a dispatcher. If the honest sentence is "charging did
not change the conclusion", that sentence replaces both.

---

## 6. Sequencing

**Phase 8 does not start until Phases 0-4 are done** — the charging comparison is meaningless
without the pickup distribution to compare it against.

**Decide before building:** whether Phase 5 (H3 resolution) runs before Phase 8. R8.1 needs a
settled resolution, so either Phase 5 runs first or Phase 8 fixes resolution 8 and says why.
