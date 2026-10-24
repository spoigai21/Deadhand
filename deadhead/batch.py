"""Phase 4: batching. The oracle with a tumbling event-time window of W seconds: requests are held until the window closes,
then assigned together by a minimum-cost assignment over road travel time (scipy's linear_sum_assignment), one routing
table per batch. A request that is not served rolls into the next window until the rider's patience T runs out.

W = 0 is the Phase 2/3 oracle (deadhead.oracle, one decision per request). Every batch also solves the two mirrors that
price the approximations under batching (Mode A, paired): the straight-line solve over the same candidates, and the
shard solve (each worker's batch over its own regions only) for the configurations in `batch_shards`.

Usage: python -m deadhead.batch configs/phase4.yaml --day 2016-06-16 --window 10 [--ring 3] [--seed 0] [--run-id ID]
Writes assignments-<run_id> to Kafka, data/phase4/assignments_<run_id>.parquet and results/phase4/run_<run_id>_<hw>.json.
"""
import argparse, collections, json, time
import numpy as np, polars as pl, yaml
from scipy.optimize import linear_sum_assignment
from confluent_kafka import Consumer, TopicPartition
from .oracle import Oracle
from .geo import cells
from .stream import requests_topic

BIG = 1e9     # an infeasible pair; every feasible batch total is under 1e6, so the solve serves as many riders as it can first

def solve(cost):
    """Minimum-cost assignment of requests (rows) to drivers (columns) where BIG marks a pair that is not on offer.
    Serves the most riders that can be served, then the least total cost. Returns {row: column} over feasible pairs."""
    if cost.size == 0:
        return {}
    rows, cols = linear_sum_assignment(cost)
    return {int(r): int(c) for r, c in zip(rows, cols) if cost[r, c] < BIG}

def run_id_for(day, window_s, ring, fleet, abandon, seed):
    return f"{day}_batch_W{window_s:g}_R{ring}_N{fleet}_T{abandon}_seed{seed}"

class BatchOracle(Oracle):
    def __init__(self, cfg, day, fleet_size, seed, abandon_s, run_id, window_s):
        super().__init__(cfg, day, "road", fleet_size, seed, abandon_s, run_id)
        assert window_s > 0, "W = 0 is deadhead.oracle"
        self.W = float(window_s)
        self.day_start = float(pl.Series([day]).str.to_datetime().dt.epoch("s")[0])
        self.pending = []                     # requests waiting for a window to close
        self.batch_id = 0; self.batch_sizes = []; self.table_calls = 0; self.batch_ms = []
        self.shards = list(cfg.get("batch_shards", []))
        self.roll = bool(cfg.get("roll_unserved", True))     # sensitivity: an unserved request is final at once, as at W = 0
        self._shard_cfg = [(tag, int(tag[1]), int(tag[3:])) for tag in self.shards]   # "s6n4" -> (tag, key resolution 6, 4 workers)

    # ------------------------------------------------------------ one window
    def shard_solve(self, r, n, items, dl, cost_rd):
        """The batch as N workers would solve it, each over the drivers of its own regions (key resolution r, contiguous
        partition ranges): a block-diagonal mask on the same costs. Returns ({row: column}, visible feasible count per row)."""
        P = self.cfg["partitions"]
        w_req = np.array([self._partition(self._parent(q["pu_cell_int"], r)) * n // P for q in items])
        w_drv = np.array([self._partition(self._parent(int(self.fleet.cell[d]), r)) * n // P for d in dl])
        visible = w_req[:, None] == w_drv[None, :]
        cost = np.where(visible, cost_rd, BIG)
        return solve(cost), (cost < BIG).sum(axis=1)

    def decide_batch(self, t_c):
        """Close the window at event time t_c: time passes for the fleet, then every pending request is decided or rolled."""
        self.fleet.release(t_c)
        if self.fleet.cruise_after_s is not None:
            self.fleet.cruise(t_c, self.sample_target, self._route_mt)
        items, self.pending = self.pending, []
        if not items:
            return
        t_wall = time.perf_counter()
        self.batch_id += 1; R = len(items); self.batch_sizes.append(R)
        cand, drivers = [], {}
        for q in items:
            rec = q["rec"]
            ids, hav = self.fleet.candidates(rec["pu_lon"], rec["pu_lat"], q["pu_cell_int"], self.cfg["candidates_k"])
            cand.append((ids, hav))
            for d in ids:
                drivers.setdefault(int(d), len(drivers))
        dl = list(drivers); D = len(dl)
        road_s = np.full((R, D), np.inf); road_m = np.full((R, D), np.inf); hav_m = np.full((R, D), np.inf)
        if D:
            t_tab = time.perf_counter()
            tb = self.osrm.table_many_to_many([(self.fleet.lon[d], self.fleet.lat[d]) for d in dl], [(q["rec"]["pu_lon"], q["rec"]["pu_lat"]) for q in items])
            self.table_ms.append((time.perf_counter() - t_tab) * 1000); self.table_calls += tb["calls"]
            for i, (ids, hav) in enumerate(cand):
                for d, h in zip(ids, hav):
                    j = drivers[int(d)]; s, m = tb["road_s"][j][i], tb["road_m"][j][i]
                    if s is None:                               # unroutable: dropped from the set, counted (as in the oracle)
                        self.counts["unroutable_candidates"] += 1; continue
                    road_s[i, j] = max(s, 0.0); road_m[i, j] = max(m, 0.0); hav_m[i, j] = h   # BUGS.md #1: clamp
        waited = np.array([t_c - q["rec"]["t"] for q in items])
        # the road rule: cost is road time; a pair is on offer only if its promise (time waited + road time) is within T
        feas_rd = np.isfinite(road_s) & (waited[:, None] + road_s <= self.T)
        cost_rd = np.where(feas_rd, road_s, BIG)
        A_rd = solve(cost_rd)
        # the straight-line mirror: the same candidates, cost and promise from the straight line at the constant speed
        feas_sl = np.isfinite(hav_m) & (waited[:, None] + hav_m / self.v_sl <= self.T)
        A_sl = solve(np.where(feas_sl, hav_m / self.v_sl, BIG))
        shard = {tag: self.shard_solve(r, n, items, dl, cost_rd) for tag, r, n in self._shard_cfg}
        outs = []
        for i, q in enumerate(items):
            rec = q["rec"]; ids = cand[i][0]
            has_cand = bool(np.isfinite(road_s[i]).any()); q["had_candidates"] = q["had_candidates"] or has_cand
            q["rounds"] += 1
            out = {"request_id": rec["request_id"], "t": rec["t"], "hour": rec["hour"], "rule": "road", "pu_lon": rec["pu_lon"], "pu_lat": rec["pu_lat"],
                   "pu_cell": f"{q['pu_cell_int']:x}", "key_cell": rec["key_cell"], "n_candidates": int(len(ids)), "trip_s": rec["trip_s"],
                   "window_s": self.W, "batch_id": self.batch_id, "batch_size": R, "decided_at": float(t_c), "queue_s": float(waited[i]), "rounds": q["rounds"],
                   "sl_crosses_water": None, "road_crosses_water": None}
            if i in A_rd:
                j = A_rd[i]; d = dl[j]
                out.update(driver=int(d), promised_eta_s=float(road_s[i, j]), actual_eta_s=float(road_s[i, j]), wait_s=float(waited[i] + road_s[i, j]), leg_m=float(road_m[i, j]),
                           road_hav_m=float(hav_m[i, j]), driver_lon=float(self.fleet.lon[d]), driver_lat=float(self.fleet.lat[d]), road_driver=int(d), road_road_s=float(road_s[i, j]))
                if i in A_sl:                                   # Mode A, Finding 1 under batching: the straight-line solve's driver for this rider
                    k = A_sl[i]
                    out.update(sl_driver=int(dl[k]), sl_road_s=float(road_s[i, k]), sl_hav_m=float(hav_m[i, k]),
                               extra_s=float(road_s[i, k] - road_s[i, j]), extra_m=float(road_m[i, k] - road_m[i, j]),
                               differs=bool(dl[k] != d and road_s[i, k] > road_s[i, j]), tie=bool(dl[k] != d and road_s[i, k] == road_s[i, j]))
                else:
                    out.update(sl_driver=-1, sl_road_s=None, sl_hav_m=None, extra_s=None, extra_m=None, differs=None, tie=None)
                for tag, (A_s, vis) in shard.items():           # Mode A, Finding 2 under batching: the shard's driver for this rider
                    out[f"{tag}_v"] = int(vis[i]); out[f"{tag}_x"] = float(road_s[i, A_s[i]] - road_s[i, j]) if i in A_s else None
                busy_until = t_c + road_s[i, j] + rec["trip_s"]
                self.fleet.claim(d, busy_until, rec["do_lon"], rec["do_lat"])
                out.update(status="matched", busy_until=float(busy_until)); self.counts["matched"] += 1
                outs.append(out)
                continue
            out.update(driver=-1, sl_driver=(int(dl[A_sl[i]]) if i in A_sl else -1), differs=None, tie=None, extra_s=None, extra_m=None)
            for tag, (A_s, vis) in shard.items():
                out[f"{tag}_v"] = int(vis[i]); out[f"{tag}_x"] = None
            if has_cand and not feas_rd[i].any():               # candidates, but every promise is over T: the rider abandons (the oracle's rule)
                out.update(status="abandoned"); self.counts["abandoned"] += 1; outs.append(out)
            elif self.roll and t_c + self.W - rec["t"] <= self.T:   # another window closes inside the rider's patience: roll
                self.pending.append(q); self.counts["rolled"] += 1
            elif q["had_candidates"]:                           # served nobody in time: waited out
                out.update(status="abandoned"); self.counts["abandoned"] += 1; outs.append(out)
            else:
                out.update(status="no_candidates"); self.counts["no_candidates"] += 1; outs.append(out)
        ms = (time.perf_counter() - t_wall) * 1000; self.batch_ms.append(ms)
        for out in outs:
            out["batch_ms"] = round(ms, 3); out["decision_ms"] = round(ms / R, 3)
            self._emit(out)

    def _emit(self, out):
        self.records.append(out)
        while True:
            try:
                self.producer.produce(self.topic, key=str(out["request_id"]), value=json.dumps(out).encode()); break
            except BufferError:
                self.producer.poll(0.1)
        if len(self.records) % 5000 == 0:
            self.producer.poll(0)

    # ------------------------------------------------------------ the stream
    def run(self, limit=None):
        """Consume requests-<day> merged into event-time order (as the oracle does); a window closes when the merged stream
        reaches its end, so the window holds exactly the requests of its interval."""
        cfg = self.cfg; topic = requests_topic(self.day)
        c = Consumer({"bootstrap.servers": cfg["kafka"], "group.id": f"oracle-{self.run_id}", "auto.offset.reset": "earliest", "enable.auto.commit": False, "fetch.max.bytes": 50 * 1024 * 1024})
        parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions)
        c.assign([TopicPartition(topic, p, 0) for p in parts])
        queues = {p: collections.deque() for p in parts}; ended = set()
        n = 0; t0 = time.perf_counter(); last_t = -1; close = self.day_start + self.W
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
                while t >= close:                               # the stream has passed the window's end: close it (and any empty ones after it)
                    self.decide_batch(close); close += self.W
                rec["t"] = float(rec["t"])
                self.pending.append({"rec": rec, "pu_cell_int": int(cells([rec["pu_lon"]], [rec["pu_lat"]], cfg["h3_resolution"])[0]), "had_candidates": False, "rounds": 0})
                n += 1
                if n % 50000 == 0:
                    print(f"{self.run_id}: {n} requests, {len(self.records)} decided, {n / (time.perf_counter() - t0):.0f}/s, {dict(self.counts)}", flush=True)
                if limit and n >= limit:
                    ended = set(parts); queues = {p: collections.deque() for p in parts}; break
        while self.pending:                                     # the day is over: the last windows close until every rider is decided
            self.decide_batch(close); close += self.W
        c.close(); self.producer.flush()
        return self.summary(n, time.perf_counter() - t0)

    def summary(self, n, seconds):
        s = super().summary(n, seconds)
        df = pl.DataFrame(self.records, infer_schema_length=None); m = df.filter(pl.col("status") == "matched")
        s["window_s"] = self.W; s["ring_k"] = self.cfg["ring_k"]; s["roll_unserved"] = self.roll
        s["batches"] = {"count": len(self.batch_sizes), "size_median": float(np.median(self.batch_sizes)), "size_max": int(max(self.batch_sizes)), "size_mean": round(float(np.mean(self.batch_sizes)), 1),
                        "table_calls": self.table_calls, "batch_ms_median": round(float(np.median(self.batch_ms)), 1), "batch_ms_p99": round(float(np.quantile(self.batch_ms, 0.99)), 1),
                        "rolled_rounds_total": int(self.counts.get("rolled", 0)), "requests_rolled_at_least_once": int((df["rounds"] > 1).sum())}
        s["rider_wait_s"] = {"median": round(float(m["wait_s"].median()), 1), "p90": round(float(m["wait_s"].quantile(0.9)), 1), "mean": round(float(m["wait_s"].mean()), 1)}
        s["queue_s"] = {"median": round(float(m["queue_s"].median()), 1), "mean": round(float(m["queue_s"].mean()), 1)}
        s["mode_a"]["straight_line_served_too"] = round(float((m["sl_driver"] >= 0).mean()), 4)
        for tag in self.shards:
            x = m[f"{tag}_x"]; v = m[f"{tag}_v"]
            s["mode_a"][tag] = {"slower_rate": round(float((x > 0).fill_null(False).mean()), 4), "no_visible_rate": round(float((v == 0).mean()), 4),
                                "unserved_by_shard_rate": round(float((x.is_null() & (v > 0)).mean()), 4),
                                "conditional_extra_wait_s_median": round(float(x.filter(x > 0).median()), 1) if (x > 0).any() else None}
        from pathlib import Path
        Path(f"results/{self.phase_dir}/run_{self.run_id}_{self.cfg['hardware_lane']}.json").write_text(json.dumps(s, indent=1))
        return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config"); ap.add_argument("--day", required=True); ap.add_argument("--window", type=float, required=True, help="W in seconds; 0 runs deadhead.oracle")
    ap.add_argument("--ring", type=int); ap.add_argument("--no-roll", action="store_true"); ap.add_argument("--seed", type=int); ap.add_argument("--fleet", type=int); ap.add_argument("--abandon", type=int); ap.add_argument("--limit", type=int); ap.add_argument("--run-id")
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.config))
    if a.ring:
        cfg["ring_k"] = a.ring
    if a.no_roll:
        cfg["roll_unserved"] = False
    fleet = a.fleet or cfg["fleet_size"]; seed = cfg["seed"] if a.seed is None else a.seed; T = a.abandon or cfg["abandon_after_s"]
    rid = a.run_id or run_id_for(a.day, a.window, cfg["ring_k"], fleet, T, seed) + ("_noroll" if a.no_roll else "")
    if a.window == 0:
        s = Oracle(cfg, a.day, "road", fleet, seed, T, rid).run(limit=a.limit)
    else:
        s = BatchOracle(cfg, a.day, fleet, seed, T, rid, a.window).run(limit=a.limit)
    print(json.dumps({k: s[k] for k in ("run_id", "decisions", "seconds", "decisions_per_s", "table_ms_median", "counts", "match_rate", "wait_s", "rider_wait_s", "deadhead_m_per_ride", "deadhead_km_total", "batches", "mode_a") if k in s}, indent=1))

if __name__ == "__main__":
    main()
