"""Phase 5 metrics: the two findings by hour of the day under the calibrated speeds against free flow, and the matching
radius by hour, from the Phase 5 replays (results/phase5/run_cal_*.json, run_ff_*.json and their ledgers).

Writes results/phase5/calibration_<lane>.json and figures/phase5_*.png.
Usage: python -m deadhead.phase5_metrics configs/phase5.yaml
"""
import glob, json, re, sys
from pathlib import Path
import numpy as np, polars as pl, yaml

RUN_RE = re.compile(r"^(cal|ff)_(\d{4}-\d{2}-\d{2})_seed(\d+)$")
RINGS = (1, 2, 3, 4, 5)

def q(s, p):
    return None if s is None or s.len() == 0 else round(float(s.quantile(p)), 1)

def spread(vals, nd=3):
    v = [x for x in vals if x is not None]
    return None if not v else {"mean": round(float(np.mean(v)), nd), "min": round(min(v), nd), "max": round(max(v), nd), "n": len(v)}

def runs():
    out = {}
    for f in sorted(glob.glob("results/phase5/run_*.json")):
        s = json.load(open(f)); m = RUN_RE.match(s["run_id"])
        p = Path(f"data/phase5/assignments_{s['run_id']}.parquet")
        if m and p.exists():
            out[(m.group(1), m.group(2), int(m.group(3)))] = {"summary": s, "ledger": pl.read_parquet(p)}
    return out

def findings(df, shard="s6n4"):
    """Finding 1 and Finding 2 (Mode A) and the service figures for one ledger, whole day."""
    dec = df.filter(pl.col("n_candidates") > 0); m = df.filter(pl.col("status") == "matched"); w = dec.filter(pl.col("differs"))
    r = {"requests": df.height, "match_rate": round(m.height / df.height, 4), "abandoned_rate": round(float((df["status"] == "abandoned").mean()), 4),
         "no_candidates_rate": round(float((df["status"] == "no_candidates").mean()), 4),
         "wait_s": {"median": q(m["actual_eta_s"], 0.5), "p90": q(m["actual_eta_s"], 0.9), "mean": round(float(m["actual_eta_s"].mean()), 1)},
         "deadhead_m_per_ride": {"median": q(m["leg_m"], 0.5), "mean": round(float(m["leg_m"].mean()), 1)},
         "finding1": {"wrong_driver_rate": round(float(dec["differs"].mean()), 4), "conditional_extra_wait_s": {"median": q(w["extra_s"], 0.5), "p90": q(w["extra_s"], 0.9), "mean": round(float(w["extra_s"].mean()), 1)},
                      "total_extra_wait_h": round(float(dec["extra_s"].sum()) / 3600, 1), "conditional_extra_deadhead_m": q(w["extra_m"], 0.5)}}
    if f"{shard}_x" in dec.columns:
        x = dec[f"{shard}_x"]; slow = dec.filter(pl.col(f"{shard}_x") > 0)
        r["finding2"] = {"config": shard, "slower_pick_rate": round(slow.height / dec.height, 4), "no_visible_candidate_rate": round(float(x.is_null().mean()), 4),
                         "conditional_extra_wait_s": {"median": q(slow[f"{shard}_x"], 0.5), "p90": q(slow[f"{shard}_x"], 0.9)}, "total_extra_wait_h": round(float(x.fill_null(0).sum()) / 3600, 1)}
    return r

def by_hour(df, shard="s6n4"):
    rows = []
    for h in range(24):
        d = df.filter(pl.col("hour") == h); dec = d.filter(pl.col("n_candidates") > 0); m = d.filter(pl.col("status") == "matched"); w = dec.filter(pl.col("differs"))
        if d.height == 0:
            continue
        row = {"hour": h, "requests": d.height, "match_rate": round(m.height / d.height, 4), "abandoned_rate": round(float((d["status"] == "abandoned").mean()), 4),
               "no_candidates_rate": round(float((d["status"] == "no_candidates").mean()), 4), "wait_median_s": q(m["actual_eta_s"], 0.5), "leg_m_median": q(m["leg_m"], 0.5),
               "nearest_candidate_m_median": q(dec["road_hav_m"], 0.5) if "road_hav_m" in dec.columns else None,
               "f1_rate": round(float(dec["differs"].mean()), 4) if dec.height else None, "f1_cond_median_s": q(w["extra_s"], 0.5), "f1_extra_wait_h": round(float(dec["extra_s"].sum()) / 3600, 2)}
        if "speed_multiplier" in d.columns:
            row["speed_multiplier"] = round(float(d["speed_multiplier"][0]), 3)
        if f"{shard}_x" in dec.columns and dec.height:
            x = dec[f"{shard}_x"]; slow = dec.filter(pl.col(f"{shard}_x") > 0)
            row.update(f2_rate=round(slow.height / dec.height, 4), f2_no_visible=round(float(x.is_null().mean()), 4), f2_cond_median_s=q(slow[f"{shard}_x"], 0.5), f2_extra_wait_h=round(float(x.fill_null(0).sum()) / 3600, 2))
        rows.append(row)
    return rows

def radius(df, loss_threshold_s=30.0, tolerance=0.01):
    """The matching radius by hour. For every ring r, the loss against the widest ring scored (5): the share of the hour's
    decisions where ring 5 has a driver at least `loss_threshold_s` faster than ring r's best, or a driver where ring r has
    none. The recommended ring is the smallest whose loss is under `tolerance`."""
    out = []
    has5 = df.filter(pl.col("ring5_v") > 0)
    for h in range(24):
        d = has5.filter(pl.col("hour") == h)
        if d.height == 0:
            continue
        row = {"hour": h, "decisions_with_a_driver_in_ring5": d.height, "rings": {}}
        best5 = d["ring5_x"]                                  # relative to the ring-3 pick; a decision with no ring-3 pick has null
        for r in RINGS:
            x = d[f"ring{r}_x"]; v = d[f"ring{r}_v"]
            # loss: ring r has nobody while ring 5 has, or ring 5's best beats ring r's best by the threshold
            none = (v == 0)
            gap = (x.fill_null(0) - best5.fill_null(0))        # both relative to the same pick: ring r's best minus ring 5's best, >= 0
            loss = none | (gap >= loss_threshold_s)
            row["rings"][r] = {"loss_share": round(float(loss.mean()), 4), "no_driver_share": round(float(none.mean()), 4), "visible_median": float(v.median()),
                               "gap_vs_ring5_median_s": round(float(gap.filter(~none).median()), 1) if (~none).any() else None}
        row["recommended_ring"] = next((r for r in RINGS if row["rings"][r]["loss_share"] < tolerance), 5)
        # the model's ring 3 against ring 5, as a rate and a cost
        gain = d.filter(pl.col("ring5_x") < 0)
        row["ring3_vs_ring5"] = {"share_with_a_faster_driver_beyond_ring3": round(gain.height / d.height, 4), "median_gain_s": round(float(-gain["ring5_x"].median()), 1) if gain.height else None,
                                 "share_no_driver_in_ring3": round(float((d["ring3_v"] == 0).mean()), 4)}
        out.append(row)
    return out

def figures(res, speeds, out="figures"):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    Path(out).mkdir(exist_ok=True)
    # the calibration
    fig, ax = plt.subplots(1, 2, figsize=(9.5, 3.4))
    mh = speeds["multiplier"]["by_hour"]; od = speeds["odometer_speed"]["by_hour"]
    ax[0].plot([r["hour"] for r in mh], [r["m"] for r in mh], "-o", color="#1f5fbf"); ax[0].fill_between([r["hour"] for r in mh], [r["q25"] for r in mh], [r["q75"] for r in mh], color="#1f5fbf", alpha=.15)
    ax[0].set_xlabel("hour of day"); ax[0].set_ylabel("recorded ÷ free-flow duration, median (IQR)"); ax[0].set_title("the multiplier on OSRM durations", fontsize=10); ax[0].grid(alpha=.3)
    ax[1].plot([r["hour"] for r in od], [r["median_mps"] for r in od], "-o", color="#d1495b"); ax[1].axhline(speeds["free_flow_speed_mps_constant"], ls="--", color="gray", label="free-flow constant (Phase 1)")
    ax[1].set_xlabel("hour of day"); ax[1].set_ylabel("odometer ÷ timestamps, median m/s"); ax[1].set_title("observed taxi speed, the week", fontsize=10); ax[1].legend(frameon=False, fontsize=8); ax[1].grid(alpha=.3)
    fig.suptitle("Time-of-day speed from the trip data (a calibration, not a traffic model)"); fig.tight_layout(); fig.savefig(f"{out}/phase5_speeds.png", dpi=150); plt.close(fig)
    # the findings by hour, calibrated against free flow
    cal = res["by_hour"].get("cal"); ff = res["by_hour"].get("ff")
    if cal and ff:
        fig, ax = plt.subplots(1, 3, figsize=(13, 3.4))
        for rows, lab, ls in ((ff, "free flow", "--"), (cal, "calibrated", "-")):
            H = [r["hour"] for r in rows]
            ax[0].plot(H, [r["f1_rate"] * 100 for r in rows], ls, marker="o", ms=3, color="#d1495b", label=f"Finding 1, {lab}")
            ax[0].plot(H, [(r.get("f2_rate") or 0) * 100 for r in rows], ls, marker="s", ms=3, color="#1f5fbf", label=f"Finding 2, {lab}")
            ax[1].plot(H, [r["f1_cond_median_s"] or 0 for r in rows], ls, marker="o", ms=3, color="#d1495b", label=f"Finding 1, {lab}")
            ax[1].plot(H, [r.get("f2_cond_median_s") or 0 for r in rows], ls, marker="s", ms=3, color="#1f5fbf", label=f"Finding 2, {lab}")
            ax[2].plot(H, [r["match_rate"] * 100 for r in rows], ls, marker="o", ms=3, color="#2a9d8f", label=f"served, {lab}")
            ax[2].plot(H, [r["abandoned_rate"] * 100 for r in rows], ls, marker="x", ms=3, color="#e07a1f", label=f"abandoned, {lab}")
        ax[0].set_ylabel("share of decisions, %"); ax[1].set_ylabel("extra wait on those decisions, median s"); ax[2].set_ylabel("share of requests, %")
        for a in ax: a.set_xlabel("hour of day"); a.grid(alpha=.3); a.legend(frameon=False, fontsize=7)
        fig.suptitle("The two approximations by hour: free flow against the calibrated speeds (weekday, seed 0)"); fig.tight_layout(); fig.savefig(f"{out}/phase5_by_hour.png", dpi=150); plt.close(fig)
    # the radius by hour
    rad = res["radius"].get("cal")
    if rad:
        fig, ax = plt.subplots(1, 2, figsize=(9.5, 3.4))
        H = [r["hour"] for r in rad]
        for r_, col in zip(RINGS, ("#9ecae1", "#6baed6", "#3182bd", "#08519c", "#08306b")):
            ax[0].plot(H, [r["rings"][r_]["loss_share"] * 100 for r in rad], "-o", ms=3, color=col, label=f"ring {r_}")
        ax[0].axhline(1, ls=":", color="gray"); ax[0].set_ylabel("decisions a wider ring would improve by 30 s or serve, %"); ax[0].set_yscale("symlog", linthresh=1); ax[0].legend(frameon=False, fontsize=7, ncol=2)
        ax[1].step(H, [r["recommended_ring"] for r in rad], where="mid", color="#1f5fbf"); ax[1].set_ylabel("smallest ring with under 1% loss"); ax[1].set_yticks(RINGS)
        for a in ax: a.set_xlabel("hour of day"); a.grid(alpha=.3)
        fig.suptitle("The matching radius by hour (calibrated speeds, weekday, seed 0)"); fig.tight_layout(); fig.savefig(f"{out}/phase5_radius.png", dpi=150); plt.close(fig)

def main(config_path):
    cfg = yaml.safe_load(open(config_path)); lane = cfg["hardware_lane"]
    speeds = json.load(open(f"results/phase5/speeds_{lane}.json"))
    R = runs()
    res = {"calibration": {"multiplier_by_hour": {r["hour"]: r["m"] for r in speeds["multiplier"]["by_hour"]}, "peak": (speeds["multiplier"]["peak_hour"], speeds["multiplier"]["peak_m"]), "trough": (speeds["multiplier"]["trough_hour"], speeds["multiplier"]["trough_m"]),
                           "zones": speeds["zones"]["relative_to_hour"], "pick_changes": [{k: c[k] for k in ("day", "pick_changes_share", "gain_s_when_changed")} for c in speeds["zones"]["pick_changes"]]},
           "whole_day": {}, "by_hour": {}, "radius": {}, "seeds": {}}
    for (kind, day, seed), r in R.items():
        res["whole_day"][f"{kind}_{day}_seed{seed}"] = findings(r["ledger"]) | {"decisions_per_s": r["summary"]["decisions_per_s"], "cruise_km": r["summary"]["cruising"]["km_total"]}
    for kind in ("cal", "ff"):
        k = (kind, "2016-06-16", 0)
        if k in R:
            res["by_hour"][kind] = by_hour(R[k]["ledger"]); res["radius"][kind] = radius(R[k]["ledger"])
    if ("cal", "2016-06-18", 0) in R:
        res["by_hour"]["cal_weekend"] = by_hour(R[("cal", "2016-06-18", 0)]["ledger"]); res["radius"]["cal_weekend"] = radius(R[("cal", "2016-06-18", 0)]["ledger"])
    cal_seeds = [findings(r["ledger"]) for (kind, day, seed), r in R.items() if kind == "cal" and day == "2016-06-16"]
    if cal_seeds:
        res["seeds"] = {"n": len(cal_seeds), "match_rate": spread([s["match_rate"] for s in cal_seeds], 4), "abandoned_rate": spread([s["abandoned_rate"] for s in cal_seeds], 4),
                        "wait_median_s": spread([s["wait_s"]["median"] for s in cal_seeds], 1),
                        "f1_rate": spread([s["finding1"]["wrong_driver_rate"] for s in cal_seeds], 4), "f1_cond_median_s": spread([s["finding1"]["conditional_extra_wait_s"]["median"] for s in cal_seeds], 1),
                        "f1_total_h": spread([s["finding1"]["total_extra_wait_h"] for s in cal_seeds], 1),
                        "f2_rate": spread([s.get("finding2", {}).get("slower_pick_rate") for s in cal_seeds], 4), "f2_no_visible": spread([s.get("finding2", {}).get("no_visible_candidate_rate") for s in cal_seeds], 4),
                        "f2_cond_median_s": spread([s.get("finding2", {}).get("conditional_extra_wait_s", {}).get("median") for s in cal_seeds], 1), "f2_total_h": spread([s.get("finding2", {}).get("total_extra_wait_h") for s in cal_seeds], 1)}
    # peak against off-peak, calibrated against free flow
    def band(rows, hours):
        sel = [r for r in rows if r["hour"] in hours]; n = sum(r["requests"] for r in sel)
        return {"f1_rate": round(sum(r["f1_rate"] * r["requests"] for r in sel if r["f1_rate"] is not None) / n, 4), "f1_extra_wait_h": round(sum(r["f1_extra_wait_h"] for r in sel), 1),
                "f2_rate": round(sum((r.get("f2_rate") or 0) * r["requests"] for r in sel) / n, 4), "f2_extra_wait_h": round(sum(r.get("f2_extra_wait_h") or 0 for r in sel), 1),
                "match_rate": round(sum(r["match_rate"] * r["requests"] for r in sel) / n, 4), "abandoned_rate": round(sum(r["abandoned_rate"] * r["requests"] for r in sel) / n, 4)}
    if "cal" in res["by_hour"] and "ff" in res["by_hour"]:
        res["peak_vs_offpeak"] = {kind: {"peak_17_19": band(res["by_hour"][kind], {17, 18, 19}), "midday_11_15": band(res["by_hour"][kind], {11, 12, 13, 14, 15}), "night_1_5": band(res["by_hour"][kind], {1, 2, 3, 4, 5})} for kind in ("cal", "ff")}
    figures(res, speeds)
    Path(f"results/phase5/calibration_{lane}.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({k: res[k] for k in ("calibration", "seeds", "peak_vs_offpeak") if k in res}, indent=1)[:5000])

if __name__ == "__main__":
    main(sys.argv[1])
