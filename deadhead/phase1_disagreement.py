"""Phase 1: how much the road network disagrees with the straight line, over the pairs a matcher sees.

Two kinds of pair. Real pickup -> dropoff *trips*, routed one by one. Simulated driver -> real pickup
*legs*: for a sampled request, the candidate set the matcher would compare (ASSUMPTIONS.md: idle
positions are real dropoffs in the prior hour, the nearest K by straight line inside the H3 ring),
routed as one many-to-one table. For both: road ÷ straight-line distance, road ÷ straight-line time,
the per-cell median, and the worst pairs with the water crossing on their route named.

Usage: python -m deadhead.phase1_disagreement configs/phase1.yaml
Writes results/phase1/disagreement_<lane>_seed<N>.json and raw pairs under data/phase1/.
"""
import json, sys, time
from pathlib import Path
import numpy as np, polars as pl, yaml
from shapely.geometry import LineString, shape
from shapely.strtree import STRtree
from .data import load_week, day_requests, supply_pool, PU_DT, DO_DT
from .geo import haversine_m, cells, disk
from .routing import OSRM
from .phase0_data import hardware

HIST_EDGES = [round(1 + 0.05 * i, 2) for i in range(0, 81)]  # 1.00 .. 5.00; the last bin is open

class Crossings:
    """Named water crossings from Phase 0, indexed so a route geometry can be asked which ones it uses."""
    def __init__(self, geojson_path):
        feats = json.load(open(geojson_path))["features"]
        self.geoms = [shape(f["geometry"]) for f in feats]
        self.names = [f["properties"].get("name") or f["properties"].get("bridge:name") or f["properties"].get("tunnel:name") or "(unnamed)" for f in feats]
        self.tree = STRtree(self.geoms)

    def on_route(self, coords):
        if not coords or len(coords) < 2:
            return []
        line = LineString(coords)
        hits = self.tree.query(line, predicate="intersects")
        return sorted({self.names[i] for i in hits})

def quantiles(x):
    x = np.asarray(x, dtype=float)
    return {"n": int(x.size), "median": round(float(np.median(x)), 3), "p90": round(float(np.quantile(x, 0.9)), 3),
            "p99": round(float(np.quantile(x, 0.99)), 3), "mean": round(float(x.mean()), 3), "max": round(float(x.max()), 3)}

def hist(x):
    counts, _ = np.histogram(np.asarray(x, dtype=float), bins=HIST_EDGES + [np.inf])
    return {"edges": HIST_EDGES, "counts": counts.tolist()}

def summarise(df: pl.DataFrame, n_worst, cfg, v_sl):
    out = {"distance_ratio": quantiles(df["dist_ratio"]), "time_ratio": quantiles(df["time_ratio"]),
           "straight_line_m": quantiles(df["hav_m"]), "road_m": quantiles(df["road_m"]), "road_s": quantiles(df["road_s"]),
           "hist_distance_ratio": hist(df["dist_ratio"]), "hist_time_ratio": hist(df["time_ratio"])}
    by_hour = df.group_by("hour").agg(pl.len().alias("n"), pl.col("dist_ratio").median().alias("median_dist_ratio"),
                                      pl.col("time_ratio").median().alias("median_time_ratio")).sort("hour")
    out["by_hour"] = {int(r["hour"]): {"n": r["n"], "median_dist_ratio": round(r["median_dist_ratio"], 3), "median_time_ratio": round(r["median_time_ratio"], 3)} for r in by_hour.iter_rows(named=True)}
    by_cell = (df.group_by("pu_cell").agg(pl.len().alias("n"), pl.col("dist_ratio").median().alias("median_dist_ratio"),
                                          pl.col("time_ratio").median().alias("median_time_ratio"), pl.col("pu_lon").mean(), pl.col("pu_lat").mean())
               .filter(pl.col("n") >= 20).sort("median_dist_ratio", descending=True))
    out["by_cell"] = [{"cell": f"{int(r['pu_cell']):x}", "n": r["n"], "median_dist_ratio": round(r["median_dist_ratio"], 3),
                       "median_time_ratio": round(r["median_time_ratio"], 3), "lon": round(r["pu_lon"], 5), "lat": round(r["pu_lat"], 5)} for r in by_cell.iter_rows(named=True)]
    out["cells_with_20_pairs"] = len(out["by_cell"])
    bands = cfg["distance_bands_m"]
    out["by_straight_line_band"] = []
    for lo, hi in zip(bands[:-1], bands[1:]):
        b = df.filter((pl.col("hav_m") >= lo) & (pl.col("hav_m") < hi))
        if b.height:
            out["by_straight_line_band"].append({"band_m": [lo, hi], "n": b.height, "share": round(b.height / df.height, 4),
                                                  "median_dist_ratio": round(float(b["dist_ratio"].median()), 3), "p90_dist_ratio": round(float(b["dist_ratio"].quantile(0.9)), 3),
                                                  "median_time_ratio": round(float(b["time_ratio"].median()), 3), "median_extra_s": round(float((b["road_s"] - b["hav_m"] / v_sl).median()), 1)})
    ap = df.filter(pl.col("airport") != "")
    out["share_touching_airport"] = round(ap.height / df.height, 4)
    out["distance_ratio_airport"] = quantiles(ap["dist_ratio"]) if ap.height else None
    out["distance_ratio_excluding_airports"] = quantiles(df.filter(pl.col("airport") == "")["dist_ratio"])
    if "crossings" in df.columns:
        crossing = df.filter(pl.col("crossings") != "")
        out["share_crossing_water"] = round(crossing.height / df.height, 4)
        out["distance_ratio_crossing_water"] = quantiles(crossing["dist_ratio"]) if crossing.height else None
        out["distance_ratio_not_crossing"] = quantiles(df.filter(pl.col("crossings") == "")["dist_ratio"])
        counts = crossing["crossings"].str.split("|").explode().value_counts().sort("count", descending=True).head(25)
        out["most_used_crossings"] = {r["crossings"]: r["count"] for r in counts.iter_rows(named=True)}
    cols = [c for c in ("trip_id", "request_id", "hour", "pu_lon", "pu_lat", "do_lon", "do_lat", "hav_m", "road_m", "road_s", "dist_ratio", "time_ratio", "crossings", "airport", "road_a", "road_b") if c in df.columns]
    out["worst_by_ratio"] = df.sort("dist_ratio", descending=True).head(n_worst).select(cols).to_dicts()
    if "road_a" in df.columns:
        top = df.sort("dist_ratio", descending=True).head(200)
        out["roads_at_origin_of_worst_200"] = {r["road_a"]: r["count"] for r in top["road_a"].value_counts().sort("count", descending=True).head(12).iter_rows(named=True)}
    worst_ex = df.filter(pl.col("airport") == "").sort("dist_ratio", descending=True)
    if "request_id" in df.columns:  # one leg per request, else one pickup beside a tunnel mouth fills the list with its 50 candidates
        worst_ex = worst_ex.unique(subset=["request_id"], keep="first", maintain_order=True)
    out["worst_by_ratio_excluding_airports"] = worst_ex.head(n_worst).select(cols).to_dicts()
    out["worst_by_extra_m"] = df.with_columns((pl.col("road_m") - pl.col("hav_m")).alias("extra_m")).sort("extra_m", descending=True).head(n_worst).select(cols + ["extra_m"]).to_dicts()
    return out

def airport_flag(df, airports):
    """Which airport box, if any, a pair touches at either end. Terminal loops produce honest but extreme ratios."""
    expr = pl.lit("")
    for name, (x0, y0, x1, y1) in reversed(list(airports.items())):
        inside = lambda lon, lat: (pl.col(lon) >= x0) & (pl.col(lon) <= x1) & (pl.col(lat) >= y0) & (pl.col(lat) <= y1)
        expr = pl.when(inside("pu_lon", "pu_lat") | inside("do_lon", "do_lat")).then(pl.lit(name)).otherwise(expr)
    return df.with_columns(expr.alias("airport"))

def route_trips(osrm, demand, n, seed, crossings, cfg):
    samp = demand.sample(min(n, demand.height), seed=seed)
    pairs = [((r["pickup_longitude"], r["pickup_latitude"]), (r["dropoff_longitude"], r["dropoff_latitude"])) for r in samp.iter_rows(named=True)]
    t0 = time.perf_counter()
    res = osrm.routes(pairs, geometry=True)
    elapsed = time.perf_counter() - t0
    rows, excl = [], {"unroutable": 0, "snap_over_max": 0, "too_short": 0, "same_road_point": 0}
    for r, (a, b), rt in zip(samp.iter_rows(named=True), pairs, res):
        if rt is None:
            excl["unroutable"] += 1; continue
        if max(rt["snap_a_m"], rt["snap_b_m"]) > cfg["max_snap_m"]:
            excl["snap_over_max"] += 1; continue
        if rt["road_m"] < 1.0:  # both ends snapped to the same road point; OSRM can return a tiny negative distance here (BUGS.md #1)
            excl["same_road_point"] += 1; continue
        hav = float(haversine_m(a[0], a[1], b[0], b[1]))
        if hav < cfg["min_straight_line_m"]:
            excl["too_short"] += 1; continue
        rows.append({"trip_id": r["trip_id"], "hour": r[PU_DT].hour, "pu_lon": a[0], "pu_lat": a[1], "do_lon": b[0], "do_lat": b[1],
                     "hav_m": hav, "road_m": rt["road_m"], "road_s": rt["road_s"], "recorded_s": r["duration_s"],
                     "snap_a_m": rt["snap_a_m"], "snap_b_m": rt["snap_b_m"], "road_a": rt["road_a"], "road_b": rt["road_b"], "crossings": "|".join(crossings.on_route(rt["geometry"]))})
    df = pl.DataFrame(rows)
    df = df.with_columns(pl.Series("pu_cell", cells(df["pu_lon"].to_numpy(), df["pu_lat"].to_numpy(), cfg["h3_resolution"])))
    return df, excl, {"routes": len(pairs), "seconds": round(elapsed, 2), "routes_per_s": round(len(pairs) / elapsed, 1)}

def route_legs(osrm, demand, pool, n, seed, cfg, idle_fraction):
    """For each sampled request: the candidate set (nearest K idle positions by straight line inside the ring), routed driver -> pickup.
    The pool of idle positions is a seeded fraction of the prior-hour dropoffs (ASSUMPTIONS.md)."""
    res_h3, k, K, window = cfg["h3_resolution"], cfg["ring_k"], cfg["candidates_k"], cfg["supply_pool_window_s"]
    pool = pool.sample(fraction=idle_fraction, seed=seed).sort(DO_DT)
    do_t = pool[DO_DT].dt.epoch("s").to_numpy()
    do_lon, do_lat = pool["dropoff_longitude"].to_numpy(), pool["dropoff_latitude"].to_numpy()
    do_cell = cells(do_lon, do_lat, res_h3)
    samp = demand.sample(min(n, demand.height), seed=seed)
    pu_t = samp[PU_DT].dt.epoch("s").to_numpy()
    pu_lon, pu_lat = samp["pickup_longitude"].to_numpy(), samp["pickup_latitude"].to_numpy()
    pu_cell = cells(pu_lon, pu_lat, res_h3)
    jobs, meta, no_candidates, cand_counts = [], [], 0, []
    for i in range(samp.height):
        lo, hi = np.searchsorted(do_t, pu_t[i] - window), np.searchsorted(do_t, pu_t[i])
        ring = np.fromiter(disk(pu_cell[i], k), dtype=np.uint64)
        idx = lo + np.flatnonzero(np.isin(do_cell[lo:hi], ring))
        if idx.size == 0:
            no_candidates += 1; continue
        hav = haversine_m(do_lon[idx], do_lat[idx], pu_lon[i], pu_lat[i])
        order = np.argsort(hav, kind="stable")[:K]
        idx, hav = idx[order], hav[order]
        cand_counts.append(int(idx.size))
        jobs.append(([(float(do_lon[j]), float(do_lat[j])) for j in idx], (float(pu_lon[i]), float(pu_lat[i]))))
        meta.append((i, idx, hav))
    t0 = time.perf_counter()
    res = osrm.tables(jobs)
    elapsed = time.perf_counter() - t0
    rows, excl = [], {"unroutable": 0, "snap_over_max": 0, "too_short": 0, "same_road_point": 0, "requests_without_candidates": no_candidates, "table_calls_failed": 0}
    for (i, idx, hav), (srcs, dst), tb in zip(meta, jobs, res):
        if tb is None:
            excl["table_calls_failed"] += 1; continue
        if tb["snap_dst_m"] > cfg["max_snap_m"]:
            excl["snap_over_max"] += len(idx); continue
        for rank, (j, h, s, m, snap, rname) in enumerate(zip(idx, hav, tb["road_s"], tb["road_m"], tb["snap_src_m"], tb["road_src"])):
            if s is None or m is None:
                excl["unroutable"] += 1; continue
            if snap > cfg["max_snap_m"]:
                excl["snap_over_max"] += 1; continue
            if m < 1.0:  # both ends snapped to the same road point (BUGS.md #1)
                excl["same_road_point"] += 1; continue
            if h < cfg["min_straight_line_m"]:
                excl["too_short"] += 1; continue
            rows.append({"request_id": int(samp["trip_id"][i]), "hour": int(samp[PU_DT][i].hour), "rank_by_straight_line": rank,
                         "pu_lon": dst[0], "pu_lat": dst[1], "do_lon": float(do_lon[j]), "do_lat": float(do_lat[j]),
                         "hav_m": float(h), "road_m": float(m), "road_s": float(s), "snap_a_m": float(snap), "snap_b_m": float(tb["snap_dst_m"]),
                         "road_a": rname, "road_b": tb["road_dst"], "pu_cell": int(pu_cell[i])})
    df = pl.DataFrame(rows).with_columns(pl.col("pu_cell").cast(pl.UInt64))
    cc = np.asarray(cand_counts)
    return df, excl, {"idle_fraction": idle_fraction, "idle_positions": pool.height, "table_calls": len(jobs), "seconds": round(elapsed, 2), "tables_per_s": round(len(jobs) / elapsed, 1),
                      "candidates_per_request": {"median": float(np.median(cc)), "p10": float(np.quantile(cc, 0.1)), "share_at_cap": round(float((cc == K).mean()), 3)},
                      "farthest_candidate_m": quantiles(df["hav_m"]) if df.height else None}

def bench(osrm, pairs, jobs, cfg):
    out = {}
    for th in (1, 4, 8):
        o = OSRM(cfg["osrm"], threads=th)
        t0 = time.perf_counter(); o.routes(pairs); dt = time.perf_counter() - t0
        t1 = time.perf_counter(); o.tables(jobs); dt2 = time.perf_counter() - t1
        out[f"threads_{th}"] = {"routes": len(pairs), "routes_per_s": round(len(pairs) / dt, 1), "tables_1xK": len(jobs), "tables_per_s": round(len(jobs) / dt2, 1)}
    return out

def main(config_path):
    cfg = yaml.safe_load(open(config_path))
    seed = cfg["seed"]
    osrm = OSRM(cfg["osrm"], cfg["threads"])
    crossings = Crossings(cfg["crossings_geojson"])
    week = load_week(cfg["trips_dir"])
    out = {"config": cfg, "hardware": hardware(), "days": {}}
    trips_all, legs_all = {}, {}
    for day in cfg["days"]:
        demand = day_requests(week, day)
        trips, excl_t, t_timing = route_trips(osrm, demand, cfg["n_trips_per_day"], seed, crossings, cfg)
        pool = supply_pool(week, day, cfg["supply_pool_window_s"])
        legs, excl_l, l_timing = route_legs(osrm, demand, pool, cfg["n_requests_per_day"], seed, cfg, cfg["idle_fraction"])
        trips_all[day], legs_all[day] = airport_flag(trips, cfg["airports"]), airport_flag(legs, cfg["airports"])
        out["days"][day] = {"requests_on_day": demand.height, "prior_hour_dropoff_pool_rows": pool.height, "trips": {"excluded": excl_t, "timing": t_timing}, "legs": {"excluded": excl_l, "timing": l_timing}, "legs_sensitivity": {}}
        for f in cfg["idle_fraction_sensitivity"]:
            lg, ex, tm = route_legs(osrm, demand, pool, cfg["n_requests_per_day"], seed, cfg, f)
            out["days"][day]["legs_sensitivity"][str(f)] = {"excluded": ex, "timing": tm, "_df": airport_flag(lg, cfg["airports"])}
        print(day, "trips", trips.height, excl_t, t_timing, "| legs", legs.height, excl_l, l_timing["seconds"], flush=True)
    # The straight-line rule needs a speed to turn a distance into a promised time. One constant, from the data:
    # the median road speed OSRM assigns over all sampled trips. Stated in ASSUMPTIONS.md.
    all_trips = pl.concat(trips_all.values())
    v_sl = float(np.median(all_trips["road_m"].to_numpy() / all_trips["road_s"].to_numpy()))
    out["straight_line_speed_mps"] = round(v_sl, 3)
    Path("data/phase1").mkdir(parents=True, exist_ok=True)
    for day in cfg["days"]:
        for kind, df in (("trips", trips_all[day]), ("legs", legs_all[day])):
            df = df.with_columns((pl.col("road_m") / pl.col("hav_m")).alias("dist_ratio"), (pl.col("road_s") / (pl.col("hav_m") / v_sl)).alias("time_ratio"))
            if kind == "legs":  # name the crossings on the worst legs only; the table call carries no geometry
                worst = df.sort("dist_ratio", descending=True).head(cfg["n_worst"])
                names = {}
                for r in worst.iter_rows(named=True):
                    rt = osrm.route((r["do_lon"], r["do_lat"]), (r["pu_lon"], r["pu_lat"]), geometry=True)
                    names[(r["request_id"], r["do_lon"], r["do_lat"])] = "|".join(crossings.on_route(rt["geometry"])) if rt else ""
                df = df.with_columns(pl.struct(["request_id", "do_lon", "do_lat"]).map_elements(lambda s: names.get((s["request_id"], s["do_lon"], s["do_lat"]), ""), return_dtype=pl.String).alias("crossings"))
            df.write_parquet(f"data/phase1/pairs_{day}_{kind}.parquet")
            s = summarise(df, cfg["n_worst"], cfg, v_sl)
            if kind == "legs":  # only the worst legs were routed with geometry, so the water-crossing share is not defined for legs
                for key in ("share_crossing_water", "distance_ratio_crossing_water", "distance_ratio_not_crossing", "most_used_crossings"):
                    s.pop(key, None)
            s["snap_m"] = {"a": quantiles(df["snap_a_m"]), "b": quantiles(df["snap_b_m"])}
            out["days"][day][kind].update(s)
        for f, d in out["days"][day]["legs_sensitivity"].items():
            lg = d.pop("_df").with_columns((pl.col("road_m") / pl.col("hav_m")).alias("dist_ratio"), (pl.col("road_s") / (pl.col("hav_m") / v_sl)).alias("time_ratio"))
            lg.write_parquet(f"data/phase1/pairs_{day}_legs_idle{f}.parquet")
            ss = summarise(lg, 5, cfg, v_sl)
            d.update({k: ss[k] for k in ("distance_ratio", "time_ratio", "straight_line_m", "by_straight_line_band", "share_touching_airport", "distance_ratio_excluding_airports")})
    # worst-pair geometries for the map, legs and trips, weekday. Airport terminal loops are excluded from the map
    # (they are reported as numbers); the map is about the city's streets and water.
    day0 = cfg["days"][0]
    geoms = []
    for kind in ("legs", "trips"):
        for r in out["days"][day0][kind]["worst_by_ratio_excluding_airports"][:15]:
            rt = osrm.route((r["do_lon"], r["do_lat"]), (r["pu_lon"], r["pu_lat"]), geometry=True) if kind == "legs" else osrm.route((r["pu_lon"], r["pu_lat"]), (r["do_lon"], r["do_lat"]), geometry=True)
            geoms.append({"kind": kind, "dist_ratio": round(r["dist_ratio"], 2), "hav_m": round(r["hav_m"]), "road_m": round(r["road_m"]), "crossings": r.get("crossings", ""),
                          "road_a": r.get("road_a", ""), "road_b": r.get("road_b", ""), "a": [round(r["do_lon" if kind == "legs" else "pu_lon"], 5), round(r["do_lat" if kind == "legs" else "pu_lat"], 5)], "b": [round(r["pu_lon" if kind == "legs" else "do_lon"], 5), round(r["pu_lat" if kind == "legs" else "do_lat"], 5)], "geometry": [[round(x, 5), round(y, 5)] for x, y in rt["geometry"]] if rt else []})
    out["worst_pair_geometries"] = geoms
    # the routing layer, benchmarked at the shapes Phase 2 and 3 will use
    tp = trips_all[day0].head(cfg["bench_routes"])
    pairs = [((a, b), (c, d)) for a, b, c, d in tp.select("pu_lon", "pu_lat", "do_lon", "do_lat").rows()]
    lg = legs_all[day0]
    jobs = [([(r["do_lon"], r["do_lat"]) for r in g.iter_rows(named=True)], (g["pu_lon"][0], g["pu_lat"][0])) for _, g in list(lg.group_by("request_id", maintain_order=True))[:300]]
    out["routing_benchmark"] = bench(osrm, pairs, jobs, cfg)
    path = Path("results/phase1") / f"disagreement_{cfg['hardware_lane']}_seed{seed}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1, default=str))
    for day in cfg["days"]:
        for kind in ("trips", "legs"):
            d = out["days"][day][kind]
            print(day, kind, "distance", d["distance_ratio"], "time", d["time_ratio"])
            print("  worst:", [(round(w["dist_ratio"], 1), w.get("crossings")) for w in d["worst_by_ratio"][:5]])
    print("straight-line speed m/s", v_sl, "| bench", out["routing_benchmark"])

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "configs/phase1.yaml")
