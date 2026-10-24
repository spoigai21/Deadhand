# Phase 3 summary — the service, sharded by map cell

Runs 2026-10-20 and 2026-10-21, `configs/phase3.yaml`, Apple M5 (16 GB) with the Kafka 3.9.1 broker, the
OSRM v6.0.0 container and the Java 21 workers all on the one machine. Results: `finding2_cpu-apple-m5.json`,
the reference oracle's `run_ref_*.json` and the service's `svc_*.json`. Figures: `figures/phase3_*.png`.
Every number is in `NUMBERS.md`. **Supply is simulated** (`ASSUMPTIONS.md`).

1. **The service works and mirrors the oracle.** A Kafka consumer group in Java, each worker owning the
   partitions (and so the resolution-6 cells) the group assigns it, driver state as a per-partition fold of
   a versioned fleet log, a shared event-time clock across workers, at-least-once consumption with an
   idempotent decision store. At one worker it gives **the same status and the same driver as the Python
   oracle on all 391,042 requests**, so every difference at more workers is the shard's.
2. **Finding 2, the boundary cost.** With 4 workers at resolution 6 the shard picks a slower driver on
   **8.7% of decisions** and has no visible driver on another **4.0%**; on the slower picks the rider waits
   **64 s longer at the median, 252 s at p90**. Eight workers cost the same. The evening is worst: 11–15%
   of decisions, 85–116 s. End to end the service matches 89.3% of the day against the oracle's 93.3%,
   and the survivors' median wait falls by selection: a dashboard would call the shard an improvement.
3. **No key balances and keeps the candidates.** At resolution 6 one Midtown cell is half the weekday and
   one partition 58% of it (ceiling 1.73× one worker); at resolution 8 the hash scatters neighbours and
   the shard sees 5 of 50 candidates (33% slower picks). A one-ring halo, built as the upper line only,
   removes all of the cost at resolution 6 and 7.
4. **Throughput and latency, on this machine.** One Java worker 332 decisions/s against Python's 235
   (**1.41×**): the routing call is the decision and the language is not the point (G12). The group
   scales with the key's balance: 523/s at 4 workers (1.57×, under the 1.73× ceiling), nothing more at 8,
   653/s at resolution 7 at the Finding 2 cost. At ×1 in the evening peak a request is decided 18 ms after
   it is due at the median, 63 ms at p99.
5. **The kill test.** A worker killed mid-morning ten times, five seeds under each assignor: **no request
   matched twice and no driver claimed twice in any run**, every request decided exactly once, every driver
   in one region. Takeover 6–7.5 s under both assignors, the 6 s session timeout being the floor;
   cooperative rebalancing saves the refolding after detection (2 s against 7–8 s), not the detection.
6. **The predictions.** P3 right on the rate at 4 workers and wrong on the cost's direction; P4 right that
   Java would not matter per worker and wrong about why scaling flattens; P5 right that the first kill run
   would find a bug and wrong about which (none of the three was a double match). The record stays in
   `PREDICTION.md`.

Bugs found: `BUGS.md` #3 (an hour-23 lookahead in the cruise map), #4 (a 43 ms keep-alive stall in the
router's replies), #5 (a lost tail of 8,724 requests at end of day), #6 (a too-short peer expiry and
workers leaving the group on their own), #7 (a false positive in the double-claim check).
