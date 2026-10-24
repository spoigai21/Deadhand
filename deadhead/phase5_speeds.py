"""Phase 5: time-of-day speeds from the trip data itself. Every record carries an odometer distance and two timestamps, so
observed average speed by hour falls out of the demand file; the Phase 1 routed trips (recorded duration against OSRM's
free-flow duration for the same pickup and dropoff) give the multiplier the oracle applies to every OSRM duration.

Caveat, stated with every number: observed taxi time includes stops, lights and the pickup itself. This is a calibration
of free-flow routing to recorded taxi time, not a traffic model.

Usage: python -m deadhead.phase5_speeds configs/phase5.yaml
Writes data/phase5/speed_multipliers.json (hour -> multiplier; zone-hour -> multiplier) and results/phase5/speeds_<lane>.json.
"""
import json, sys
from pathlib import Path
import numpy as np, polars as pl, yaml, h3
from .data import load_week, PU_DT
from .geo import cells

MILE = 1609.344

def odometer_speeds(week):
    """Average speed by hour from the odometer and the timestamps, over the week, with the plausibility cuts stated."""
    w = week.filter((pl.col("trip_distance") >= 0.3) & (pl.col("duration_s") >= 60) & (pl.col("duration_s") <= 3 * 3600))
    w = w.with_columns((pl.col("trip_distance") * MILE / pl.col("duration_s")).alias("mps"), pl.col(PU_DT).dt.hour().alias("hour"), pl.col(PU_DT).dt.date().cast(pl.Utf8).alias("day"))
    w = w.filter(pl.col("mps") < 35)
    by_hour = w.group_by("hour").agg(pl.len().alias("trips"), pl.col("mps").median().alias("median_mps"), pl.col("mps").quantile(0.1).alias("p10_mps"), pl.col("mps").quantile(0.9).alias("p90_mps")).sort("hour")
    by_day_hour = w.group_by("day", "hour").agg(pl.col("mps").median().alias("median_mps")).sort("day", "hour")
    return {"rows_used": w.height, "rows_total": week.height, "cuts": "distance >= 0.3 mi, 60 s <= duration <= 3 h, speed < 35 m/s",
            "by_hour": [{"hour": int(h), "trips": int(n), "median_mps": round(float(m), 2), "p10_mps": round(float(a), 2), "p90_mps": round(float(b), 2)} for h, n, m, a, b in by_hour.iter_rows()],
            "by_day_hour": {d: {} for d in by_day_hour["day"].unique().to_list()} | {}}, w

def multipliers(trips, zone_res, min_zone_trips):
    """recorded / free-flow duration on the routed real trips: the median by hour, and by (pickup zone, hour)."""
    t = trips.filter((pl.col("road_s") > 30) & (pl.col("recorded_s") >= 60) & (pl.col("recorded_s") <= 3 * 3600)).with_columns((pl.col("recorded_s") / pl.col("road_s")).alias("ratio"))
    t = t.filter(pl.col("ratio") < 10)
    t = t.with_columns(pl.Series("zone", [h3.cell_to_parent(h3.int_to_str(int(c, 16)) if isinstance(c, str) else h3.int_to_str(int(c)), zone_res) for c in t["pu_cell"].to_list()]))
    hourly = t.group_by("hour").agg(pl.len().alias("trips"), pl.col("ratio").median().alias("m"), pl.col("ratio").quantile(0.25).alias("q25"), pl.col("ratio").quantile(0.75).alias("q75")).sort("hour")
    hm = {int(h): float(m) for h, m in zip(hourly["hour"], hourly["m"])}
    zone = t.group_by("zone", "hour").agg(pl.len().alias("trips"), pl.col("ratio").median().alias("m")).filter(pl.col("trips") >= min_zone_trips).sort("zone", "hour")
    zone = zone.with_columns(pl.Series("rel", [float(m) / hm[int(h)] for h, m in zip(zone["hour"], zone["m"])]))
    return t, hourly, hm, zone

def zone_changes_the_pick(legs, hm, zone_m, zone_res, day):
    """On the Phase 1 candidate legs (up to 50 per request): how often the road-best driver differs when every leg's free-flow
    time is scaled by the multiplier of the driver's own zone-hour instead of the city's hour."""
    z = legs.with_columns(pl.Series("zone", [h3.cell_to_parent(c, zone_res) for c in (h3.latlng_to_cell(float(la), float(lo), 8) for lo, la in zip(legs["do_lon"], legs["do_lat"]))]))
    zm = {(r["zone"], int(r["hour"])): float(r["m"]) for r in zone_m.iter_rows(named=True)}
    z = z.with_columns(pl.Series("m_zone", [zm.get((zz, int(h)), hm[int(h)]) for zz, h in zip(z["zone"], z["hour"])]))
    z = z.with_columns((pl.col("road_s") * pl.col("m_zone")).alias("road_s_zone"))
    g = z.group_by("request_id").agg(pl.col("road_s").arg_min().alias("i_ff"), pl.col("road_s_zone").arg_min().alias("i_z"), pl.len().alias("n"),
                                     (pl.col("road_s_zone").min() - pl.col("road_s_zone").sort_by("road_s").first()).alias("gain_s"), pl.col("hour").first().alias("hour"))
    g = g.filter(pl.col("n") >= 2)
    changed = g.filter(pl.col("i_ff") != pl.col("i_z"))
    byh = g.group_by("hour").agg(pl.len().alias("n"), (pl.col("i_ff") != pl.col("i_z")).mean().alias("changed")).sort("hour")
    return {"day": day, "requests": g.height, "pick_changes_share": round(changed.height / g.height, 4),
            "gain_s_when_changed": {"median": round(float(-changed["gain_s"].median()), 1), "p90": round(float(-changed["gain_s"].quantile(0.1)), 1)} if changed.height else None,
            "by_hour": [{"hour": int(h), "n": int(n), "changed": round(float(c), 4)} for h, n, c in byh.iter_rows()]}

def main(config_path):
    cfg = yaml.safe_load(open(config_path)); lane = cfg["hardware_lane"]
    zone_res = cfg.get("speed_zone_resolution", 6); min_zone = cfg.get("speed_zone_min_trips", 30)
    week = load_week(cfg["trips_dir"])
    odo, _ = odometer_speeds(week)
    trips = pl.concat([pl.read_parquet(f"data/phase1/pairs_{d}_trips.parquet").with_columns(pl.lit(d).alias("day")) for d in cfg["days"]])
    t, hourly, hm, zone = multipliers(trips, zone_res, min_zone)
    per_day = {d: {int(h): round(float(m), 3) for h, m in t.filter(pl.col("day") == d).group_by("hour").agg(pl.col("ratio").median().alias("m")).sort("hour").iter_rows()} for d in cfg["days"]}
    rel = zone["rel"]
    changes = [zone_changes_the_pick(pl.read_parquet(f"data/phase1/pairs_{d}_legs.parquet"), hm, zone, zone_res, d) for d in cfg["days"]]
    res = {"caveat": "observed taxi time includes stops, lights and the pickup itself: a calibration of free-flow routing to recorded taxi time, not a traffic model",
           "odometer_speed": odo, "free_flow_speed_mps_constant": cfg["straight_line_speed_mps"],
           "multiplier": {"definition": "median over the Phase 1 routed real trips of recorded duration / OSRM free-flow duration, by pickup hour", "trips_used": t.height,
                          "by_hour": [{"hour": int(h), "trips": int(n), "m": round(float(m), 3), "q25": round(float(a), 3), "q75": round(float(b), 3)} for h, n, m, a, b in hourly.iter_rows()],
                          "by_day": per_day, "peak_hour": max(hm, key=hm.get), "peak_m": round(max(hm.values()), 3), "trough_hour": min(hm, key=hm.get), "trough_m": round(min(hm.values()), 3)},
           "zones": {"resolution": zone_res, "min_trips": min_zone, "zone_hours": zone.height, "zones": int(zone["zone"].n_unique()),
                     "relative_to_hour": {"p10": round(float(rel.quantile(0.1)), 3), "median": round(float(rel.median()), 3), "p90": round(float(rel.quantile(0.9)), 3),
                                          "share_outside_15pct": round(float(((rel < 0.85) | (rel > 1.15)).mean()), 4), "share_outside_30pct": round(float(((rel < 0.7) | (rel > 1.3)).mean()), 4)},
                     "busiest_zone_hours": [{"zone": r["zone"], "hour": int(r["hour"]), "trips": int(r["trips"]), "m": round(float(r["m"]), 3), "relative": round(float(r["rel"]), 3)} for r in zone.sort("trips", descending=True).head(12).iter_rows(named=True)],
                     "pick_changes": changes}}
    Path("data/phase5").mkdir(parents=True, exist_ok=True); Path("results/phase5").mkdir(parents=True, exist_ok=True)
    Path("data/phase5/speed_multipliers.json").write_text(json.dumps({"hourly": {str(h): round(hm[h], 4) for h in range(24)}, "zone_hour": {f"{r['zone']}:{int(r['hour'])}": round(float(r["m"]), 4) for r in zone.iter_rows(named=True)}, "zone_resolution": zone_res}, indent=1))
    Path(f"results/phase5/speeds_{lane}.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({"multiplier_by_hour": {r["hour"]: r["m"] for r in res["multiplier"]["by_hour"]}, "odometer_mps_by_hour": {r["hour"]: r["median_mps"] for r in odo["by_hour"]},
                      "zones": res["zones"]["relative_to_hour"], "pick_changes": [{k: c[k] for k in ("day", "pick_changes_share", "gain_s_when_changed")} for c in changes]}, indent=1))

if __name__ == "__main__":
    main(sys.argv[1])
