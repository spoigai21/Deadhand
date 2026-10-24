"""Phase 2: the single-process matcher that consumes the demand stream. It sees every partition, so it
has no boundary: it is the correctness oracle the sharded service (Phase 3) is checked against.

The driver-ranking rule is swappable behind one interface and both rules rank the SAME candidate set
(the idle drivers in the H3 ring, nearest K by straight line). Every decision also computes what the
other rule would have picked from those candidates (Mode A, paired one-step), in whichever replay is
running, so the wrong-driver rate is measured in both worlds.

Usage: python -m deadhead.oracle configs/phase2.yaml --day 2016-06-16 --rule road|straight [--fleet N] [--seed S] [--abandon T] [--limit M]
Writes assignments to Kafka topic assignments-<run_id> (keyed by request_id), a parquet cache under
data/phase2/, and a run summary under results/phase2/.
"""
import argparse, json, time, collections, zlib
import h3
from pathlib import Path
import numpy as np, polars as pl, yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from .data import load_week, day_requests, PU_DT
from .fleet import Fleet
from .geo import cells, disk
from .routing import OSRM
from .stream import requests_topic, ensure_topic
from .phase1_disagreement import Crossings
from .phase0_data import hardware

RULES = ("straight", "road")

def rank(rule, hav, road_s):
    """Index of the chosen candidate. Candidates arrive sorted by (straight-line distance, id); np.argmin keeps the first on ties."""
    if rule == "straight":
        return 0
    return int(np.argmin(road_s))

class Oracle:
    def __init__(self, cfg, day, rule, fleet_size, seed, abandon_s, run_id):
        assert rule in RULES
        self.cfg, self.day, self.rule, self.n, self.seed, self.T, self.run_id = cfg, day, rule, fleet_size, seed, abandon_s, run_id
        self.osrm = OSRM(cfg["osrm"], threads=1)
        self.crossings = Crossings(cfg["crossings_geojson"])
        self.v_sl = cfg["straight_line_speed_mps"]
        week = load_week(cfg["trips_dir"])
        self.repositioning = cfg.get("repositioning", "cruise")
        cruise_after = cfg["cruise_after_s"] if self.repositioning == "cruise" else None
        day_start = float(pl.Series([day]).str.to_datetime().dt.epoch("s")[0])
        self.fleet, self.pool_rows = Fleet.from_dropoffs(week, day, fleet_size, cfg["initial_positions_window_s"], seed, cfg["h3_resolution"], cfg["ring_k"], cruise_after, day_start)
        # the demand map a cruising driver uses: real pickups of the previous clock hour, by resolution-8 cell (ASSUMPTIONS.md).
        # Hour 0 reads the previous day's hour 23, which the window includes. No lookahead: at time t only hour(t)-1 is read.
        # The window ends at 23:00 of the day so that hour 23 holds the previous day's pickups only (BUGS.md #3).
        start = pl.lit(day).str.to_datetime() - pl.duration(hours=1); end = pl.lit(day).str.to_datetime() + pl.duration(hours=23)
        pk = week.filter((pl.col(PU_DT) >= start) & (pl.col(PU_DT) < end))
        pk_cell = cells(pk["pickup_longitude"].to_numpy(), pk["pickup_latitude"].to_numpy(), cfg["h3_resolution"])
        pk = pk.with_columns(pl.Series("cell", pk_cell), pl.col(PU_DT).dt.hour().alias("hour"))
        # cruise_point "random": a pickup drawn with the seeded RNG (Phase 2). "hash": index from (driver, cruise count) over the
        # points sorted by (lon, lat), so a worker written in another language (Phase 3) draws the identical point.
        self.cruise_point = cfg.get("cruise_point", "random")
        if self.cruise_point == "hash":
            pk = pk.sort("pickup_longitude", "pickup_latitude")
        self.demand = {}      # (hour, cell) -> (lons, lats)
        for (h, c), g in pk.group_by("hour", "cell"):
            self.demand[(int(h), int(c))] = (g["pickup_longitude"].to_numpy(), g["pickup_latitude"].to_numpy())
        self.rng = np.random.default_rng(seed + 1000)
        # Phase 3, Mode A for the geographic shard: at every decision, which candidates a worker owning the request's
        # region could see, for each key resolution and worker count (RangeAssignor over the fixed partition count)
        self.shard_res = cfg.get("shard_key_resolutions", []); self.shard_workers = cfg.get("shard_workers", [])
        self._parent_cache = {}; self._part_cache = {}; self._disk1_cache = {}
        self.phase_dir = cfg.get("phase_dir", "phase2")
        self.cruise_k = cfg["cruise_ring_k"]
        self._cruise_disk = {}
        # Phase 5: time-of-day speeds, an hourly multiplier on every OSRM duration (legs and cruises); None = free flow
        self.mult = None
        if cfg.get("speed_multipliers"):
            self.mult = [float(json.load(open(cfg["speed_multipliers"]))["hourly"][str(h)]) for h in range(24)]
        self._now = 0.0
        # Phase 5: the matching radius by hour. The model keeps ring_k and candidates_k; a wider superset is scored beside it (Mode A)
        self.radius_rings = list(cfg.get("radius_rings_mode_a", [])); self.radius_k = cfg.get("radius_superset_k", 100); self.radius_ring = cfg.get("radius_superset_ring", 5)
        self._gd_cache = {}
        self.records = []
        self.topic = ensure_topic(cfg["kafka"], f"assignments-{run_id}", cfg["partitions"], recreate=True)
        self.producer = Producer({"bootstrap.servers": cfg["kafka"], "enable.idempotence": True, "linger.ms": 50, "compression.type": "lz4", "queue.buffering.max.messages": 500000})
        self.counts = collections.Counter()
        self.table_ms = []

    def _parent(self, cell8, r):
        k = (cell8, r)
        if k not in self._parent_cache:
            self._parent_cache[k] = h3.cell_to_parent(h3.int_to_str(cell8), r)
        return self._parent_cache[k]

    def _partition(self, key):
        """The partition the producer's default partitioner gives a key: CRC32 modulo the partition count."""
        if key not in self._part_cache:
            self._part_cache[key] = zlib.crc32(key.encode()) % self.cfg["partitions"]
        return self._part_cache[key]

    def _disk1(self, key):
        if key not in self._disk1_cache:
            self._disk1_cache[key] = set(h3.grid_disk(key, 1))
        return self._disk1_cache[key]

    def shard_columns(self, ids, road_s, i_rd, pu_cell, out):
        """For each (key resolution r, worker count n): the road pick among the candidates visible to the worker that owns the
        request's partition, and its road-time gap to the oracle's pick. s<r>n<n>: own regions only; h<r>n<n>: plus a
        one-ring halo of neighbouring key cells. _x = extra seconds (None if no candidate is visible), _v = visible count."""
        P = self.cfg["partitions"]
        for r in self.shard_res:
            req_key = self._parent(int(pu_cell), r); req_part = self._partition(req_key); halo = self._disk1(req_key)
            keys = [self._parent(int(self.fleet.cell[d]), r) for d in ids]
            parts = np.array([self._partition(k) for k in keys]); in_halo = np.array([k in halo for k in keys])
            for n in self.shard_workers:
                own = (parts * n // P) == (req_part * n // P)
                for tag, mask in ((f"s{r}n{n}", own), (f"h{r}n{n}", own | in_halo)):
                    if mask.any():
                        j = int(np.argmin(np.where(mask, road_s, np.inf)))
                        out[f"{tag}_x"] = float(road_s[j] - road_s[i_rd]); out[f"{tag}_v"] = int(mask.sum())
                    else:
                        out[f"{tag}_x"] = None; out[f"{tag}_v"] = 0

    def sample_target(self, t, cell, d):
        """The busiest cell of the previous hour within cruise_ring_k of the driver; a real pickup from it. None = stay."""
        h = ((t // 3600) % 24 - 1) % 24
        if cell not in self._cruise_disk:
            self._cruise_disk[cell] = sorted(disk(cell, self.cruise_k))
        best, best_n = None, 0
        for c in self._cruise_disk[cell]:
            g = self.demand.get((h, c))
            if g is not None and len(g[0]) > best_n:
                best, best_n = c, len(g[0])
        if best is None or best == cell:
            return None
        lons, lats = self.demand[(h, best)]
        if self.cruise_point == "hash":
            j = ((int(d) * 2654435761 + int(self.fleet.cruise_n[d]) * 40503) & 0xFFFFFFFF) % len(lons)
        else:
            j = int(self.rng.integers(len(lons)))
        return float(lons[j]), float(lats[j])

    def mult_at(self, t):
        return self.mult[int((t // 3600) % 24)] if self.mult else 1.0

    def _grid_distance(self, a, b):
        k = (int(a), int(b))
        if k not in self._gd_cache:
            self._gd_cache[k] = h3.grid_distance(h3.int_to_str(k[0]), h3.int_to_str(k[1]))
        return self._gd_cache[k]

    def _route_mt(self, a, b):
        r = self.osrm.route(a, b)
        return (r["road_m"], r["road_s"] * self.mult_at(self._now)) if r else None

    def decide(self, rec):
        t = rec["t"]; t_wall = time.perf_counter(); self._now = t
        self.fleet.release(t)
        if self.fleet.cruise_after_s is not None:
            self.fleet.cruise(t, self.sample_target, self._route_mt)
        pu_cell = cells([rec["pu_lon"]], [rec["pu_lat"]], self.cfg["h3_resolution"])[0]
        K = self.cfg["candidates_k"]; m_h = self.mult_at(t)
        if self.radius_rings:     # the superset for the radius question; the model's own set is the nearest K within ring_k, as always
            ids, hav = self.fleet.candidates(rec["pu_lon"], rec["pu_lat"], pu_cell, self.radius_k, ring=self.radius_ring)
            gd = np.array([self._grid_distance(pu_cell, self.fleet.cell[d]) for d in ids], dtype=int)
            keep = np.zeros(len(ids), dtype=bool); keep[np.flatnonzero(gd <= self.cfg["ring_k"])[:K]] = True
        else:
            ids, hav = self.fleet.candidates(rec["pu_lon"], rec["pu_lat"], pu_cell, K)
            gd = np.zeros(len(ids), dtype=int); keep = np.ones(len(ids), dtype=bool)
        out = {"request_id": rec["request_id"], "t": t, "hour": rec["hour"], "rule": self.rule, "pu_lon": rec["pu_lon"], "pu_lat": rec["pu_lat"],
               "pu_cell": f"{int(pu_cell):x}", "key_cell": rec["key_cell"], "n_candidates": int(keep.sum()), "trip_s": rec["trip_s"]}
        if self.mult:
            out["speed_multiplier"] = m_h
        if len(ids) == 0:
            for r in self.radius_rings:
                out[f"ring{r}_x"] = None; out[f"ring{r}_v"] = 0
            out.update(status="no_candidates", driver=-1)
            self.counts["no_candidates"] += 1
            return self._finish(out, t_wall)
        t_tab = time.perf_counter()
        tb = self.osrm.table_many_to_one([(self.fleet.lon[d], self.fleet.lat[d]) for d in ids], (rec["pu_lon"], rec["pu_lat"]))
        self.table_ms.append((time.perf_counter() - t_tab) * 1000)
        # OSRM returns a tiny negative distance when both ends snap to one road point (BUGS.md #1): clamp at zero
        road_s = np.array([np.inf if v is None else max(v, 0.0) for v in tb["road_s"]]); road_m = np.array([np.inf if v is None else max(v, 0.0) for v in tb["road_m"]])
        ok = np.isfinite(road_s)
        if not ok.all():  # an unroutable candidate is dropped from the set before either rule sees it; counted
            self.counts["unroutable_candidates"] += int((~ok).sum())
            ids, hav, road_s, road_m, gd, keep = ids[ok], hav[ok], road_s[ok], road_m[ok], gd[ok], keep[ok]
        road_s = road_s * m_h                                  # the calibrated leg time (1.0 under free flow)
        b = np.flatnonzero(keep)
        if len(b) == 0:
            for r in self.radius_rings:
                mask = gd <= r; out[f"ring{r}_x"] = None; out[f"ring{r}_v"] = int(mask.sum())
            out.update(status="no_candidates", driver=-1, n_candidates=0); self.counts["no_candidates"] += 1
            return self._finish(out, t_wall)
        ids_b, hav_b, road_s_b, road_m_b = ids[b], hav[b], road_s[b], road_m[b]
        i_sl, i_rd = rank("straight", hav_b, road_s_b), rank("road", hav_b, road_s_b)
        i = i_sl if self.rule == "straight" else i_rd
        promised = hav_b[i] / self.v_sl if self.rule == "straight" else road_s_b[i]
        # Mode A, paired one-step: what the other rule would have taken from the same candidates
        differs = bool(ids_b[i_sl] != ids_b[i_rd] and road_s_b[i_sl] > road_s_b[i_rd])
        tie = bool(ids_b[i_sl] != ids_b[i_rd] and road_s_b[i_sl] == road_s_b[i_rd])
        out.update(sl_driver=int(ids_b[i_sl]), road_driver=int(ids_b[i_rd]), differs=differs, tie=tie,
                   extra_s=float(road_s_b[i_sl] - road_s_b[i_rd]), extra_m=float(road_m_b[i_sl] - road_m_b[i_rd]),
                   sl_hav_m=float(hav_b[i_sl]), road_hav_m=float(hav_b[i_rd]), sl_road_s=float(road_s_b[i_sl]), road_road_s=float(road_s_b[i_rd]),
                   sl_crosses_water=None, road_crosses_water=None)
        if differs:
            for key, d in (("sl_crosses_water", ids_b[i_sl]), ("road_crosses_water", ids_b[i_rd])):
                rt = self.osrm.route((self.fleet.lon[d], self.fleet.lat[d]), (rec["pu_lon"], rec["pu_lat"]), geometry=True)
                out[key] = bool(self.crossings.on_route(rt["geometry"])) if rt else None
        if self.shard_res:
            self.shard_columns(ids_b, road_s_b, i_rd, pu_cell, out)
        # Mode A for the radius: the road-best within r rings against the model's pick (negative = a wider ring would have done better)
        for r in self.radius_rings:
            mask = (gd <= r) & keep if r <= self.cfg["ring_k"] else (gd <= r)
            if mask.any():
                j = int(np.argmin(np.where(mask, road_s, np.inf))); out[f"ring{r}_x"] = float(road_s[j] - road_s_b[i_rd]); out[f"ring{r}_v"] = int(mask.sum())
            else:
                out[f"ring{r}_x"] = None; out[f"ring{r}_v"] = 0
        out.update(promised_eta_s=float(promised), actual_eta_s=float(road_s_b[i]), leg_m=float(road_m_b[i]), driver=int(ids_b[i]),
                   driver_lon=float(self.fleet.lon[ids_b[i]]), driver_lat=float(self.fleet.lat[ids_b[i]]))
        if promised > self.T:
            out.update(status="abandoned"); self.counts["abandoned"] += 1
            return self._finish(out, t_wall)
        busy_until = t + road_s_b[i] + rec["trip_s"]
        self.fleet.claim(ids_b[i], busy_until, rec["do_lon"], rec["do_lat"])
        out.update(status="matched", busy_until=float(busy_until)); self.counts["matched"] += 1
        return self._finish(out, t_wall)

    def _finish(self, out, t_wall):
        out["decision_ms"] = round((time.perf_counter() - t_wall) * 1000, 3)
        self.records.append(out)
        while True:
            try:
                self.producer.produce(self.topic, key=str(out["request_id"]), value=json.dumps(out).encode()); break
            except BufferError:
                self.producer.poll(0.1)
        if len(self.records) % 5000 == 0:
            self.producer.poll(0)
        return out

    def run(self, limit=None):
        """Consume requests-<day> from the beginning over all partitions, merged into event-time order by a watermark:
        a record is processed only when every unfinished partition has a record at or beyond its time."""
        cfg = self.cfg; topic = requests_topic(self.day)
        c = Consumer({"bootstrap.servers": cfg["kafka"], "group.id": f"oracle-{self.run_id}", "auto.offset.reset": "earliest", "enable.auto.commit": False, "fetch.max.bytes": 50 * 1024 * 1024})
        parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions)
        c.assign([TopicPartition(topic, p, 0) for p in parts])
        queues = {p: collections.deque() for p in parts}; ended = set()
        n = 0; t0 = time.perf_counter(); last_t = -1
        while len(ended) < len(parts):
            msgs = c.consume(num_messages=4000, timeout=2.0)
            if not msgs and len(ended) < len(parts):
                continue
            for m in msgs:
                if m.error():
                    raise RuntimeError(m.error())
                rec = json.loads(m.value())
                if rec["type"] == "eod":
                    ended.add(m.partition())
                else:
                    queues[m.partition()].append(rec)
            while True:
                heads = [(q[0]["t"], q[0]["request_id"], p) for p, q in queues.items() if q]
                if not heads or any(not queues[p] and p not in ended for p in parts):
                    break
                t, rid, p = min(heads)
                rec = queues[p].popleft()
                assert t >= last_t, "event time went backwards"
                last_t = t
                self.decide(rec); n += 1
                if n % 50000 == 0:
                    print(f"{self.run_id}: {n} decisions, {n / (time.perf_counter() - t0):.0f}/s, {dict(self.counts)}", flush=True)
                if limit and n >= limit:
                    ended = set(parts); queues = {p: collections.deque() for p in parts}; break
        c.close(); self.producer.flush()
        return self.summary(n, time.perf_counter() - t0)

    def summary(self, n, seconds):
        df = pl.DataFrame(self.records, infer_schema_length=None).with_columns(pl.col("sl_crosses_water", "road_crosses_water").cast(pl.Boolean))
        m = df.filter(pl.col("status") == "matched")
        dec = df.filter(pl.col("n_candidates") > 0)
        s = {"run_id": self.run_id, "day": self.day, "rule": self.rule, "fleet_size": self.n, "seed": self.seed, "abandon_after_s": self.T, "repositioning": self.repositioning,
             "cruising": {"cruises": self.fleet.cruises, "stays": self.fleet.stays, "km_total": round(self.fleet.cruise_m / 1000, 1), "hours_total": round(self.fleet.cruise_s / 3600, 1), "cruise_after_s": self.fleet.cruise_after_s, "cruise_ring_k": self.cruise_k},
             "config": {k: self.cfg[k] for k in ("partitions", "key_resolution", "h3_resolution", "ring_k", "candidates_k", "straight_line_speed_mps", "initial_positions_window_s")},
             "hardware": hardware(), "initial_position_pool_rows": self.pool_rows,
             "decisions": n, "seconds": round(seconds, 1), "decisions_per_s": round(n / seconds, 1),
             "table_ms_median": round(float(np.median(self.table_ms)), 2) if self.table_ms else None,
             "decision_ms_median": round(float(df["decision_ms"].median()), 2), "decision_ms_p99": round(float(df["decision_ms"].quantile(0.99)), 2),
             "counts": dict(self.counts), "match_rate": round(m.height / n, 4),
             "wait_s": {"median": round(float(m["actual_eta_s"].median()), 1), "p90": round(float(m["actual_eta_s"].quantile(0.9)), 1), "mean": round(float(m["actual_eta_s"].mean()), 1)},
             "deadhead_m_per_ride": {"median": round(float(m["leg_m"].median()), 1), "mean": round(float(m["leg_m"].mean()), 1)},
             "deadhead_km_total": round(float(m["leg_m"].sum()) / 1000, 1),
             "promised_eta_error_s": {"median": round(float((m["actual_eta_s"] - m["promised_eta_s"]).median()), 1), "p90": round(float((m["actual_eta_s"] - m["promised_eta_s"]).quantile(0.9)), 1)},
             "utilisation": round(float((m["actual_eta_s"] + m["trip_s"]).sum()) / (self.n * 86400), 4),
             "utilisation_including_cruising": round((float((m["actual_eta_s"] + m["trip_s"]).sum()) + self.fleet.cruise_s) / (self.n * 86400), 4),
             "mode_a": {"decisions_with_candidates": dec.height, "wrong_driver_rate": round(float(dec["differs"].mean()), 4), "tie_rate": round(float(dec["tie"].mean()), 4)}}
        w = dec.filter(pl.col("differs"))
        if w.height:
            s["mode_a"].update({"conditional_extra_wait_s": {"median": round(float(w["extra_s"].median()), 1), "p90": round(float(w["extra_s"].quantile(0.9)), 1), "mean": round(float(w["extra_s"].mean()), 1)},
                                "conditional_extra_deadhead_m": {"median": round(float(w["extra_m"].median()), 1), "p90": round(float(w["extra_m"].quantile(0.9)), 1)},
                                "share_sl_pick_crosses_water": round(float(w["sl_crosses_water"].fill_null(False).mean()), 4),
                                "extra_wait_share_from_water_crossing_picks": round(float(w.filter(pl.col("sl_crosses_water") == True)["extra_s"].sum() / w["extra_s"].sum()), 4),
                                "total_extra_wait_s_if_straight_line_everywhere": round(float(dec["extra_s"].sum()), 0)})
        s["cruise_point"] = self.cruise_point; s["speed_calibration"] = "hourly multiplier" if self.mult else "free flow"
        Path(f"data/{self.phase_dir}").mkdir(parents=True, exist_ok=True); Path(f"results/{self.phase_dir}").mkdir(parents=True, exist_ok=True)
        df.write_parquet(f"data/{self.phase_dir}/assignments_{self.run_id}.parquet")
        Path(f"results/{self.phase_dir}/run_{self.run_id}_{self.cfg['hardware_lane']}.json").write_text(json.dumps(s, indent=1))
        return s

def run_id_for(day, rule, fleet, abandon, seed, repositioning):
    return f"{day}_{rule}_N{fleet}_T{abandon}_seed{seed}_{repositioning}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config"); ap.add_argument("--day", required=True); ap.add_argument("--rule", required=True, choices=RULES)
    ap.add_argument("--fleet", type=int); ap.add_argument("--seed", type=int); ap.add_argument("--abandon", type=int); ap.add_argument("--limit", type=int); ap.add_argument("--run-id")
    ap.add_argument("--repositioning", choices=("cruise", "none")); ap.add_argument("--ring", type=int, help="candidate ring (ring_k), the matching radius; Phase 4 sensitivity")
    ap.add_argument("--free-flow", action="store_true", help="Phase 5: ignore speed_multipliers (the free-flow comparison replay)")
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.config))
    if a.ring:
        cfg["ring_k"] = a.ring
    if a.free_flow:
        cfg["speed_multipliers"] = None
    if a.repositioning:
        cfg["repositioning"] = a.repositioning
    fleet = a.fleet or cfg["fleet_size"]; seed = cfg["seed"] if a.seed is None else a.seed; T = a.abandon or cfg["abandon_after_s"]
    rid = a.run_id or run_id_for(a.day, a.rule, fleet, T, seed, cfg.get("repositioning", "cruise"))
    s = Oracle(cfg, a.day, a.rule, fleet, seed, T, rid).run(limit=a.limit)
    print(json.dumps({k: s[k] for k in ("run_id", "decisions", "seconds", "decisions_per_s", "table_ms_median", "counts", "match_rate", "wait_s", "deadhead_m_per_ride", "promised_eta_error_s", "utilisation", "utilisation_including_cruising", "cruising", "mode_a")}, indent=1))

if __name__ == "__main__":
    main()
