"""Phase 2 metrics: Mode B (full replay per rule) and Mode A (paired one-step) from the oracle's runs.

Reads every results/phase2/run_*.json and the assignments ledgers (parquet cache under data/phase2/,
rebuilt from the Kafka topic when missing). Writes results/phase2/finding1_<lane>.json.

Usage: python -m deadhead.phase2_metrics configs/phase2.yaml
"""
import glob, json, re, sys
from pathlib import Path
import numpy as np, polars as pl, yaml
from .stream import read_topic
from .geo import boundary_lonlat

RUN_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_(straight|road)_N(\d+)_T(\d+)_seed(\d+)_(cruise|none)$")

def load_runs(results_dir="results/phase2"):
    runs = []
    for f in sorted(glob.glob(f"{results_dir}/run_*.json")):
        s = json.load(open(f))
        if RUN_RE.match(s["run_id"]):
            s["_file"] = f; runs.append(s)
    return runs

def load_ledger(cfg, run_id):
    p = Path(f"data/phase2/assignments_{run_id}.parquet")
    if not p.exists():
        recs = read_topic(cfg["kafka"], f"assignments-{run_id}")
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([json.loads(v) for _, _, _, v in recs]).write_parquet(p)
    return pl.read_parquet(p)

def spread(vals, nd=3):
    v = np.asarray([x for x in vals if x is not None], dtype=float)
    if v.size == 0:
        return None
    return {"mean": round(float(v.mean()), nd), "min": round(float(v.min()), nd), "max": round(float(v.max()), nd), "n_seeds": int(v.size)}

def mode_b_row(group):
    g = lambda path: [eval("r" + "".join(f"[{k!r}]" for k in path)) for r in group]
    return {"seeds": sorted(r["seed"] for r in group),
            "match_rate": spread([r["match_rate"] for r in group], 4),
            "abandoned_share": spread([r["counts"].get("abandoned", 0) / r["decisions"] for r in group], 4),
            "no_candidates_share": spread([r["counts"].get("no_candidates", 0) / r["decisions"] for r in group], 4),
            "wait_s_median": spread([r["wait_s"]["median"] for r in group], 1), "wait_s_p90": spread([r["wait_s"]["p90"] for r in group], 1),
            "deadhead_m_per_ride_median": spread([r["deadhead_m_per_ride"]["median"] for r in group], 1), "deadhead_km_total": spread([r["deadhead_km_total"] for r in group], 1),
            "utilisation": spread([r["utilisation"] for r in group], 4),
            "promised_eta_error_s_median": spread([r["promised_eta_error_s"]["median"] for r in group], 1), "promised_eta_error_s_p90": spread([r["promised_eta_error_s"]["p90"] for r in group], 1),
            "decisions_per_s": spread([r["decisions_per_s"] for r in group], 1), "decision_ms_median": spread([r["decision_ms_median"] for r in group], 2), "decision_ms_p99": spread([r["decision_ms_p99"] for r in group], 2)}

def mode_a_row(group):
    ma = [r["mode_a"] for r in group]
    return {"seeds": sorted(r["seed"] for r in group),
            "wrong_driver_rate": spread([m["wrong_driver_rate"] for m in ma], 4), "tie_rate": spread([m["tie_rate"] for m in ma], 4),
            "conditional_extra_wait_s_median": spread([m.get("conditional_extra_wait_s", {}).get("median") for m in ma], 1),
            "conditional_extra_wait_s_p90": spread([m.get("conditional_extra_wait_s", {}).get("p90") for m in ma], 1),
            "conditional_extra_wait_s_mean": spread([m.get("conditional_extra_wait_s", {}).get("mean") for m in ma], 1),
            "conditional_extra_deadhead_m_median": spread([m.get("conditional_extra_deadhead_m", {}).get("median") for m in ma], 1),
            "share_sl_pick_crosses_water": spread([m.get("share_sl_pick_crosses_water") for m in ma], 4),
            "extra_wait_share_from_water_crossing_picks": spread([m.get("extra_wait_share_from_water_crossing_picks") for m in ma], 4),
            "total_extra_wait_hours_if_straight_line_everywhere": spread([m.get("total_extra_wait_s_if_straight_line_everywhere", 0) / 3600 for m in ma], 1)}

def mode_a_breakdowns(cfg, runs):
    """By hour, by cell and by water crossing, pooled over seeds, from the ledgers of the road-aware replays."""
    dfs = [load_ledger(cfg, r["run_id"]).filter(pl.col("n_candidates") > 0) for r in runs]
    df = pl.concat(dfs, how="diagonal")
    by_hour = (df.group_by("hour").agg(pl.len().alias("decisions"), pl.col("differs").mean().alias("wrong_driver_rate"),
                                       pl.col("extra_s").filter(pl.col("differs")).median().alias("conditional_extra_wait_s_median"),
                                       pl.col("extra_s").sum().alias("extra_wait_s_total"), pl.col("sl_hav_m").median().alias("sl_pick_straight_line_m_median")).sort("hour"))
    by_cell = (df.rename({"pu_cell": "cell"}).group_by("cell").agg(pl.len().alias("decisions"), pl.col("differs").mean().alias("wrong_driver_rate"),
                                          pl.col("extra_s").filter(pl.col("differs")).median().alias("conditional_extra_wait_s_median"),
                                          pl.col("pu_lon").mean().alias("lon"), pl.col("pu_lat").mean().alias("lat"))
               .filter(pl.col("decisions") >= 30).sort("wrong_driver_rate", descending=True))
    w = df.filter(pl.col("differs"))
    bands = [0, 100, 250, 500, 1000, 2000, 100000]
    by_band = []
    for lo, hi in zip(bands[:-1], bands[1:]):
        b = df.filter((pl.col("sl_hav_m") >= lo) & (pl.col("sl_hav_m") < hi))
        if b.height:
            bw = b.filter(pl.col("differs"))
            by_band.append({"sl_pick_straight_line_band_m": [lo, hi], "decisions": b.height, "share": round(b.height / df.height, 4), "wrong_driver_rate": round(float(b["differs"].mean()), 4),
                            "conditional_extra_wait_s_median": round(float(bw["extra_s"].median()), 1) if bw.height else None})
    water = {"decisions_where_sl_pick_crosses_water": int(w["sl_crosses_water"].fill_null(False).sum()),
             "share_of_wrong_decisions": round(float(w["sl_crosses_water"].fill_null(False).mean()), 4),
             "share_of_all_decisions": round(float(w["sl_crosses_water"].fill_null(False).sum() / df.height), 4),
             "share_of_total_extra_wait": round(float(w.filter(pl.col("sl_crosses_water") == True)["extra_s"].sum() / w["extra_s"].sum()), 4),
             "conditional_extra_wait_s_median_when_crossing": round(float(w.filter(pl.col("sl_crosses_water") == True)["extra_s"].median()), 1) if w.filter(pl.col("sl_crosses_water") == True).height else None,
             "conditional_extra_wait_s_median_when_not": round(float(w.filter(pl.col("sl_crosses_water") != True)["extra_s"].median()), 1)}
    return {"pooled_decisions": df.height, "seeds": len(dfs),
            "by_hour": {int(r["hour"]): {k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items() if k != "hour"} for r in by_hour.iter_rows(named=True)},
            "by_cell": [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()} for r in by_cell.iter_rows(named=True)],
            "by_straight_line_band": by_band, "water": water,
            "extra_wait_distribution_on_wrong_decisions": {"p50": round(float(w["extra_s"].quantile(0.5)), 1), "p75": round(float(w["extra_s"].quantile(0.75)), 1),
                                                           "p90": round(float(w["extra_s"].quantile(0.9)), 1), "p99": round(float(w["extra_s"].quantile(0.99)), 1), "max": round(float(w["extra_s"].max()), 1)}}

def main(config_path):
    cfg = yaml.safe_load(open(config_path))
    runs = load_runs()
    N, T, day0 = cfg["fleet_size"], cfg["abandon_after_s"], cfg["days"][0]
    base_model = cfg.get("repositioning", "cruise")
    is_base = lambda r: r.get("repositioning", "cruise") == base_model
    out = {"config": cfg, "runs": [r["run_id"] for r in runs], "fleet_calibration": [], "mode_b": {}, "mode_a": {}, "sensitivity": {}}
    for r in sorted([r for r in runs if is_base(r) and r["day"] == day0 and r["rule"] == "road" and r["abandon_after_s"] == T and r["seed"] == cfg["seed"]], key=lambda r: r["fleet_size"]):
        out["fleet_calibration"].append({"fleet_size": r["fleet_size"], "match_rate": r["match_rate"], "abandoned": r["counts"].get("abandoned", 0), "no_candidates": r["counts"].get("no_candidates", 0),
                                         "wait_s_median": r["wait_s"]["median"], "wait_s_p90": r["wait_s"]["p90"], "utilisation": r["utilisation"], "wrong_driver_rate": r["mode_a"]["wrong_driver_rate"]})
    for day in cfg["days"]:
        for rule in ("straight", "road"):
            group = [r for r in runs if is_base(r) and r["day"] == day and r["rule"] == rule and r["fleet_size"] == N and r["abandon_after_s"] == T]
            if group:
                out["mode_b"].setdefault(day, {})[rule] = mode_b_row(group)
                out["mode_a"].setdefault(day, {})[f"measured_in_{rule}_replay"] = mode_a_row(group)
        road = [r for r in runs if is_base(r) and r["day"] == day and r["rule"] == "road" and r["fleet_size"] == N and r["abandon_after_s"] == T]
        if road:
            out["mode_a"][day]["breakdowns_road_replay"] = mode_a_breakdowns(cfg, road)
    for r in runs:
        baseline = is_base(r) and r["fleet_size"] == N and r["abandon_after_s"] == T
        if r["day"] == day0 and r["seed"] == cfg["seed"] and not baseline and (not is_base(r) or r["fleet_size"] in {int(N * f) for f in cfg["fleet_sensitivity"]} | {N}):
            out["sensitivity"][r["run_id"]] = {"rule": r["rule"], "repositioning": r.get("repositioning", "cruise"), "fleet_size": r["fleet_size"], "abandon_after_s": r["abandon_after_s"], "match_rate": r["match_rate"],
                                               "no_candidates_share": round(r["counts"].get("no_candidates", 0) / r["decisions"], 4), "wait_s": r["wait_s"], "utilisation": r["utilisation"], "deadhead_m_per_ride_median": r["deadhead_m_per_ride"]["median"],
                                               "wrong_driver_rate": r["mode_a"]["wrong_driver_rate"], "conditional_extra_wait_s": r["mode_a"].get("conditional_extra_wait_s")}
    path = Path(f"results/phase2/finding1_{cfg['hardware_lane']}.json")
    path.write_text(json.dumps(out, indent=1))
    print(json.dumps({"calibration": out["fleet_calibration"], "mode_b": out["mode_b"], "mode_a": {d: {k: v for k, v in m.items() if k != "breakdowns_road_replay"} for d, m in out["mode_a"].items()}, "sensitivity": out["sensitivity"]}, indent=1))

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "configs/phase2.yaml")
