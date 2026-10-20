# Phase 1 summary — the road network as the ground truth

Run 2026-10-19, `configs/phase1.yaml`, seed 0, Apple M5, OSRM v6.0.0 free-flow, H3 resolution 8.
Results: `disagreement_cpu-apple-m5_seed0.json`. Figures: `figures/phase1_*.png`. Every number is in
`NUMBERS.md`. Supply is simulated (`ASSUMPTIONS.md`).

1. **Real trips: the road is 1.32× the straight line at the median, 1.63× at p90.** Crossing water
   adds little to a committed trip (median 1.36 crossing against 1.31 not). Predicted 1.25 / 1.6.
2. **Simulated pickup legs: 2.0× at the median, 4.9× at p90.** The legs a matcher compares are
   short (median 270 m), and short legs are where the straight line fails: 2.7× under 250 m, 1.9×
   at 250–500 m, 1.5× at 1–2 km. The per-band numbers are stable across idle fractions 0.1, 0.2 and
   0.5. Predicted 1.35 / 2.0, which was wrong.
3. **The worst pairs are drivers already on a crossing**, not drivers across a river: Brooklyn-Battery
   Tunnel 67×, Queensboro Bridge upper level 62×, Williamsburg Bridge 54×, Queens-Midtown Tunnel 46×.
   Airport terminal loops are worse than any street (JFK Terminal 4, 87×). The worst real trips start
   on one-way avenues (1st Avenue, 22×). The Harlem River, predicted, does not appear.
4. **The routing layer:** 3,500 routes/s and 2,000 one-to-50 tables/s from 8 threads; route per
   pair stays.

Exclusions, Thursday: 144 trips and 55 legs snapped more than 250 m; 117 trips and 310 legs had both
ends on one road point (`BUGS.md` #1); 99 trips and 33,773 legs were under 100 m in a straight line;
1 request had no candidate in the ring. Nothing was unroutable.
