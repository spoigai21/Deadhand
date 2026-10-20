# BUGS — what broke, what caught it, the fix

| # | Phase | What broke | What caught it | Fix |
|---|---|---|---|---|
| 1 | 1 | OSRM's `table` call returned a road distance of **−0.1 m and −0.2 m** (duration 0) for two candidate legs whose driver and pickup both snapped to the same point on 7th Avenue, 107 m from the pickup's recorded coordinates. The straight-line distance was 104 m, so the ratio came out negative. | `tests/test_phase1.py::test_distance_sanity_haversine_never_exceeds_road`, the "road time is never negative" rule from `SPEC.md`, on the first full run. Two rows in 1.5 million. | Pairs whose road distance is under 1 m are excluded and counted as `same_road_point` in the results file. The sign of the number was the symptom; the cause is two endpoints collapsing to one road node, which says nothing about the matcher. |
