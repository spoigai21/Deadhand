"""Phase 4 metrics: the wait-versus-deadhead curve over the window W, the marginal trade, the operating point by a stated
rule, batching's effect on the two priced approximations (Mode A inside every batch), and the service with the window
against the batched oracle.

Reads results/phase4/run_*.json with the ledgers in data/phase4/, the Phase 3 reference ledgers as W = 0, and the
service runs. Writes results/phase4/batching_<lane>.json and figures/phase4_*.png.

Usage: python -m deadhead.phase4_metrics configs/phase4.yaml
"""
import glob, json, re, sys
from pathlib import Path
import numpy as np, polars as pl, yaml

RUN_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_batch_W(\d+)_R(\d)_N(\d+)_T(\d+)_seed(\d+)(_noroll)?$")
T_DEFAULT = 600

def q(s, p):
    return None if s is None or s.len() == 0 else round(float(s.quantile(p)), 1)

def spread(vals, nd=3):
    v = [x for x in vals if x is not None]
    return None if not v else {"mean": round(float(np.mean(v)), nd), "min": round(min(v), nd), "max": round(max(v), nd), "n": len(v)}

def runs(cfg):
    """Every batched run plus the W = 0 reference ledgers (Phase 3), keyed by (day, W, ring, seed, roll)."""
    out = {}
    for f in sorted(glob.glob("results/phase4/run_*.json")):
        s = json.load(open(f)); m = RUN_RE.match(s["run_id"])
        if not m:
            continue
        day, W, R, N, T, seed, noroll = m.groups()
        p = Path(f"data/phase4/assignments_{s['run_id']}.parquet")
        if p.exists():
            out[(day, int(W), int(R), int(seed), noroll is None)] = {"summary": s, "ledger": p}
    for f in sorted(glob.glob("results/phase3/run_ref_*.json")):
        s = json.load(open(f)); m = re.match(r"^ref_(\d{4}-\d{2}-\d{2})_seed(\d+)$", s["run_id"])
        p = Path(f"data/phase3/assignments_{s['run_id']}.parquet")
        if m and p.exists():
            out[(m.group(1), 0, 3, int(m.group(2)), True)] = {"summary": s, "ledger": p}
    return out

def ledger_metrics(df, T=T_DEFAULT, shards=("s6n4", "s8n4")):
    """The full metric set from one ledger. W = 0 ledgers have no queue: wait is the leg."""
    n = df.height
    if "wait_s" not in df.columns:
        df = df.with_columns(pl.col("actual_eta_s").alias("wait_s"), pl.lit(0.0).alias("queue_s"), pl.lit(1).alias("rounds"), pl.lit(1).alias("batch_size"))
    m = df.filter(pl.col("status") == "matched")
    served_after_roll = m.filter(pl.col("rounds") > 1).height
    r = {"requests": n, "matched": m.height, "match_rate": round(m.height / n, 4),
         "abandoned_rate": round(float((df["status"] == "abandoned").mean()), 4), "no_candidates_rate": round(float((df["status"] == "no_candidates").mean()), 4),
         "served_after_a_roll": served_after_roll, "served_after_a_roll_share": round(served_after_roll / max(1, m.height), 4),
         "queue_s": {"median": q(m["queue_s"], 0.5), "p90": q(m["queue_s"], 0.9), "mean": round(float(m["queue_s"].mean()), 1)},
         "leg_s": {"median": q(m["actual_eta_s"], 0.5), "p90": q(m["actual_eta_s"], 0.9), "mean": round(float(m["actual_eta_s"].mean()), 1)},
         "wait_s": {"median": q(m["wait_s"], 0.5), "p90": q(m["wait_s"], 0.9), "mean": round(float(m["wait_s"].mean()), 1)},
         # every rider counted: an unserved rider is charged the patience T
         "wait_all_riders_mean_s": round((float(m["wait_s"].sum()) + T * (n - m.height)) / n, 1),
         "deadhead_m_per_ride": {"median": q(m["leg_m"], 0.5), "mean": round(float(m["leg_m"].mean()), 1)},
         "deadhead_km_total": round(float(m["leg_m"].sum()) / 1000, 1),
         "batch_size": {"median": float(m["batch_size"].median()), "max": int(m["batch_size"].max())}}
    # Finding 1 inside the batch: the straight-line solve over the same candidates (W = 0: the straight-line pick)
    both = m.filter(pl.col("sl_driver") >= 0) if "sl_driver" in m.columns else m
    diff = both.filter(pl.col("differs") == True)
    r["finding1"] = {"served_by_both_rules_share": round(both.height / max(1, m.height), 4),
                     "slower_pick_rate": round(diff.height / max(1, both.height), 4),
                     "faster_pick_rate": round(float((both["extra_s"] < 0).mean()), 4) if both.height else None,   # the batch gives and takes: a rider the straight-line solve served better
                     "conditional_extra_wait_s": {"median": q(diff["extra_s"], 0.5), "p90": q(diff["extra_s"], 0.9), "mean": round(float(diff["extra_s"].mean()), 1) if diff.height else None},
                     "extra_wait_h_gross": round(float(diff["extra_s"].sum()) / 3600, 1), "extra_wait_h_net": round(float(both["extra_s"].sum()) / 3600, 1)}
    # Finding 2 inside the batch: each worker's solve over its own regions
    r["finding2"] = {}
    for tag in shards:
        if f"{tag}_x" not in m.columns:
            continue
        x, v = m[f"{tag}_x"], m[f"{tag}_v"]
        slow = m.filter(pl.col(f"{tag}_x") > 0)
        r["finding2"][tag] = {"slower_pick_rate": round(slow.height / max(1, m.height), 4), "no_visible_candidate_rate": round(float((v == 0).mean()), 4),
                              "unserved_by_the_shard_rate": round(float((x.is_null() & (v > 0)).mean()), 4),
                              "conditional_extra_wait_s": {"median": q(slow[f"{tag}_x"], 0.5), "p90": q(slow[f"{tag}_x"], 0.9)},
                              "extra_wait_h": round(float(slow[f"{tag}_x"].sum()) / 3600, 1),          # gross: the slower picks' extra wait
                              "extra_wait_h_net": round(float(x.fill_null(0).sum()) / 3600, 1)}       # net: a batch solve also hands some riders a faster driver than the global one
    # by hour: match rate, wait, queue
    byh = m.group_by("hour").agg(pl.len().alias("served"), pl.col("wait_s").median().alias("wait_median"), pl.col("queue_s").mean().alias("queue_mean"), pl.col("leg_m").mean().alias("leg_m_mean")).sort("hour")
    tot = df.group_by("hour").agg(pl.len().alias("n")).sort("hour")
    r["by_hour"] = [{"hour": int(h), "match_rate": round(s / nn, 4), "wait_median_s": round(float(w), 1), "queue_mean_s": round(float(qu), 1), "leg_m_mean": round(float(lm), 1)}
                    for (h, s, w, qu, lm), nn in zip(byh.iter_rows(), tot["n"].to_list())]
    return r

def curve(points):
    """The marginal trade along W: seconds of rider wait per minute of deadhead (leg time) removed, step to step and from W = 0."""
    pts = sorted(points, key=lambda p: p["W"])
    out = []
    for i, p in enumerate(pts):
        row = {"W": p["W"], "wait_mean_s": p["wait_s"]["mean"], "wait_median_s": p["wait_s"]["median"], "wait_all_riders_mean_s": p["wait_all_riders_mean_s"],
               "queue_mean_s": p["queue_s"]["mean"], "leg_mean_s": p["leg_s"]["mean"], "deadhead_m_mean": p["deadhead_m_per_ride"]["mean"], "deadhead_km_total": p["deadhead_km_total"],
               "match_rate": p["match_rate"]}
        if i > 0:
            a = pts[i - 1]
            d_leg_min = (a["leg_s"]["mean"] - p["leg_s"]["mean"]) / 60
            row["step_deadhead_removed_min_per_ride"] = round(d_leg_min, 3)
            row["step_extra_wait_s_per_ride"] = round(p["wait_s"]["mean"] - a["wait_s"]["mean"], 1)
            row["step_wait_s_per_deadhead_min_removed"] = round((p["wait_s"]["mean"] - a["wait_s"]["mean"]) / d_leg_min, 1) if abs(d_leg_min) > 1e-9 else None
        if i > 0:
            z = pts[0]
            d0 = (z["leg_s"]["mean"] - p["leg_s"]["mean"]) / 60
            row["from_w0_wait_s_per_deadhead_min_removed"] = round((p["wait_s"]["mean"] - z["wait_s"]["mean"]) / d0, 1) if abs(d0) > 1e-9 else None
            row["from_w0_deadhead_km_removed"] = round(z["deadhead_km_total"] - p["deadhead_km_total"], 1)
        out.append(row)
    return out

def operating_point(rows, parity_s_per_min=60.0):
    """The stated rule: the largest W whose step from the previous W still buys a minute of deadhead for under a minute of
    rider wait (60 s per minute: parity between a rider's second and a driver's second), and whose deadhead still falls."""
    best = rows[0]["W"]
    for r in rows[1:]:
        price = r.get("step_wait_s_per_deadhead_min_removed"); removed = r.get("step_deadhead_removed_min_per_ride")
        if price is None or removed is None or removed <= 0 or price > parity_s_per_min:
            break
        best = r["W"]
    return {"W": best, "rule": f"largest W whose step still removes deadhead at under {parity_s_per_min:.0f} s of rider wait per minute removed"}

def service_runs(cfg):
    out = []
    for f in sorted(glob.glob("results/phase4/svc_svc_*.json")):
        s = json.load(open(f)); p = Path(f"data/phase4/svc_{s['run_id']}.parquet")
        if p.exists():
            out.append((s, pl.read_parquet(p)))
    return out

def effective(df):
    return df.filter(~pl.col("redelivered")).unique(subset=["request_id"], keep="first") if "redelivered" in df.columns else df

def figures(res, out="figures"):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    Path(out).mkdir(exist_ok=True)
    thu = res["curve"].get("2016-06-16"); sat = res["curve"].get("2016-06-18")
    if thu:
        fig, axes = plt.subplots(1, 2, figsize=(9.5, 3.6))
        for rows, lab, col in ((thu, "weekday", "#1f5fbf"), (sat, "weekend", "#2a9d8f")):
            if not rows:
                continue
            x = [r["deadhead_m_mean"] for r in rows]; y = [r["wait_mean_s"] for r in rows]
            axes[0].plot(x, y, "-o", color=col, label=lab)
            for r in rows:
                axes[0].annotate(f"W={r['W']:g}", (r["deadhead_m_mean"], r["wait_mean_s"]), textcoords="offset points", xytext=(4, 4), fontsize=8)
            axes[1].plot([r["W"] for r in rows], [r["match_rate"] * 100 for r in rows], "-o", color=col, label=lab)
        axes[0].set_xlabel("empty metres per ride, mean"); axes[0].set_ylabel("served riders' wait, mean s (queue + leg)"); axes[0].legend(frameon=False); axes[0].grid(alpha=.3)
        axes[1].set_xlabel("window W, s"); axes[1].set_ylabel("riders served, %"); axes[1].grid(alpha=.3)
        op = res["operating_point"]["W"]
        fig.suptitle(f"Batching: what a longer window buys and costs (7,500 drivers; operating point W = {op:g} s)")
        fig.tight_layout(); fig.savefig(f"{out}/phase4_tradeoff.png", dpi=150); plt.close(fig)
    inter = res["interaction"].get("2016-06-16")
    if inter:
        fig, axes = plt.subplots(1, 2, figsize=(9.5, 3.6))
        W = [r["W"] for r in inter]
        axes[0].plot(W, [r["finding1_slower_pick_rate"] * 100 for r in inter], "-o", color="#d1495b", label="straight line picks a slower driver")
        axes[0].plot(W, [r["finding2_s6n4_slower_pick_rate"] * 100 for r in inter], "-s", color="#1f5fbf", label="shard (res 6, 4 workers) picks a slower driver")
        axes[0].plot(W, [r["finding2_s6n4_no_visible_rate"] * 100 for r in inter], "--s", color="#1f5fbf", alpha=.6, label="shard sees no driver")
        axes[0].set_xlabel("window W, s"); axes[0].set_ylabel("share of served riders, %"); axes[0].legend(frameon=False, fontsize=8); axes[0].grid(alpha=.3)
        axes[1].plot(W, [r["finding1_extra_wait_h_net"] for r in inter], "-o", color="#d1495b", label="straight line, net rider-hours lost")
        axes[1].plot(W, [r["finding2_s6n4_extra_wait_h"] for r in inter], "-s", color="#1f5fbf", label="shard, rider-hours lost")
        axes[1].set_xlabel("window W, s"); axes[1].set_ylabel("rider-hours over the day"); axes[1].legend(frameon=False, fontsize=8); axes[1].grid(alpha=.3)
        fig.suptitle("Do the two approximations shrink under batching? (weekday, Mode A inside every batch)")
        fig.tight_layout(); fig.savefig(f"{out}/phase4_interaction.png", dpi=150); plt.close(fig)

def main(config_path):
    cfg = yaml.safe_load(open(config_path)); T = cfg["abandon_after_s"]
    allruns = runs(cfg)
    per_run = {}
    for key, r in allruns.items():
        day, W, R, seed, roll = key
        lm = ledger_metrics(pl.read_parquet(r["ledger"]), T, cfg.get("batch_shards", []))
        s = r["summary"]
        lm["run"] = {"run_id": s["run_id"], "day": day, "W": W, "ring": R, "seed": seed, "roll": roll, "decisions_per_s": s["decisions_per_s"], "seconds": s["seconds"],
                     "batches": s.get("batches"), "cruise_km": s["cruising"]["km_total"], "utilisation": s["utilisation"]}
        per_run[f"{day}_W{W}_R{R}_seed{seed}" + ("" if roll else "_noroll")] = lm
    # the curve: seed 0, ring 3, rolling, per day
    res = {"config": {k: cfg[k] for k in ("windows_s", "radius_rings", "batch_shards", "fleet_size", "abandon_after_s", "candidates_k", "ring_k")}, "runs": per_run, "curve": {}, "interaction": {}}
    for day in cfg["days"]:
        pts = [dict(W=W, **per_run[f"{day}_W{W}_R3_seed0"]) for W in cfg["windows_s"] if f"{day}_W{W}_R3_seed0" in per_run]
        if len(pts) >= 2:
            res["curve"][day] = curve(pts)
            res["interaction"][day] = [{"W": p["W"], "finding1_slower_pick_rate": p["finding1"]["slower_pick_rate"], "finding1_conditional_median_s": p["finding1"]["conditional_extra_wait_s"]["median"],
                                        "finding1_extra_wait_h_net": p["finding1"]["extra_wait_h_net"], "finding1_extra_wait_h_gross": p["finding1"]["extra_wait_h_gross"],
                                        "finding1_served_by_both_share": p["finding1"]["served_by_both_rules_share"],
                                        "finding2_s6n4_slower_pick_rate": p["finding2"].get("s6n4", {}).get("slower_pick_rate"), "finding2_s6n4_no_visible_rate": p["finding2"].get("s6n4", {}).get("no_visible_candidate_rate"),
                                        "finding2_s6n4_unserved_rate": p["finding2"].get("s6n4", {}).get("unserved_by_the_shard_rate"), "finding2_s6n4_extra_wait_h": p["finding2"].get("s6n4", {}).get("extra_wait_h"),
                                        "finding2_s6n4_conditional_median_s": p["finding2"].get("s6n4", {}).get("conditional_extra_wait_s", {}).get("median"),
                                        "finding2_s8n4_slower_pick_rate": p["finding2"].get("s8n4", {}).get("slower_pick_rate"), "finding2_s8n4_extra_wait_h": p["finding2"].get("s8n4", {}).get("extra_wait_h")} for p in pts]
    res["operating_point"] = operating_point(res["curve"]["2016-06-16"]) if "2016-06-16" in res["curve"] else {"W": None}
    # the operating point over seeds, and the no-roll and radius sensitivities
    op = res["operating_point"]["W"]
    if op:
        rows = [per_run[k] for k in per_run if re.match(rf"^2016-06-16_W{op}_R3_seed\d$", k)]
        res["operating_point"]["seeds"] = {"n": len(rows), "match_rate": spread([r["match_rate"] for r in rows], 4), "wait_mean_s": spread([r["wait_s"]["mean"] for r in rows], 1),
                                           "wait_median_s": spread([r["wait_s"]["median"] for r in rows], 1), "deadhead_m_mean": spread([r["deadhead_m_per_ride"]["mean"] for r in rows], 1),
                                           "deadhead_km_total": spread([r["deadhead_km_total"] for r in rows], 0),
                                           "finding1_slower_pick_rate": spread([r["finding1"]["slower_pick_rate"] for r in rows], 4), "finding1_extra_wait_h_net": spread([r["finding1"]["extra_wait_h_net"] for r in rows], 1),
                                           "finding2_s6n4_slower_pick_rate": spread([r["finding2"].get("s6n4", {}).get("slower_pick_rate") for r in rows], 4)}
        w0 = [per_run[k] for k in per_run if re.match(r"^2016-06-16_W0_R3_seed\d$", k)]
        res["operating_point"]["w0_seeds"] = {"n": len(w0), "match_rate": spread([r["match_rate"] for r in w0], 4), "wait_mean_s": spread([r["wait_s"]["mean"] for r in w0], 1),
                                              "deadhead_m_mean": spread([r["deadhead_m_per_ride"]["mean"] for r in w0], 1), "deadhead_km_total": spread([r["deadhead_km_total"] for r in w0], 0)}
    res["sensitivity"] = {"no_roll": {k: {kk: per_run[k][kk] for kk in ("match_rate", "wait_s", "wait_all_riders_mean_s", "deadhead_m_per_ride", "deadhead_km_total", "finding1", "finding2")} for k in per_run if k.endswith("_noroll")},
                          "radius": {k: {kk: per_run[k][kk] for kk in ("match_rate", "wait_s", "wait_all_riders_mean_s", "deadhead_m_per_ride", "deadhead_km_total", "finding1")} for k in per_run if re.match(r"^2016-06-16_W\d+_R[25]_seed0$", k)}}
    # the service with the window against the batched oracle (same W, seed 0) and against the Phase 3 service at W = 0
    res["service"] = []
    for s, df in service_runs(cfg):
        eff = effective(df); m = eff.filter(pl.col("status") == "matched")
        W = int(s.get("window_s") or 0)
        row = {"run_id": s["run_id"], "workers": s["workers"], "W": W, "requests": eff.height, "decisions_per_s_wall": s["decisions_per_s_wall"], "wall_s": s["wall_s"],
               "match_rate": round(m.height / eff.height, 4), "wait_s": {"median": q(m["wait_s"], 0.5), "p90": q(m["wait_s"], 0.9), "mean": round(float(m["wait_s"].mean()), 1)},
               "queue_s_mean": round(float(m["queue_s"].mean()), 1), "deadhead_m_mean": round(float(m["leg_m"].mean()), 1), "deadhead_km_total": round(float(m["leg_m"].sum()) / 1000, 1),
               "no_candidates_rate": round(float((eff["status"] == "no_candidates").mean()), 4), "redelivered_records": int(df["redelivered"].sum()),
               "duplicate_decisions": int(eff.height - eff["request_id"].n_unique())}
        mm = m.sort("driver", "now")
        row["double_claims"] = mm.filter((pl.col("driver") == pl.col("driver").shift(1)) & (pl.col("now") < pl.col("busy_until").shift(1))).height
        ok = f"{s['day']}_W{W}_R3_seed{s['seed']}"
        if ok in per_run:
            o = per_run[ok]; row["oracle_same_W"] = {"match_rate": o["match_rate"], "wait_s": o["wait_s"], "deadhead_m_mean": o["deadhead_m_per_ride"]["mean"], "deadhead_km_total": o["deadhead_km_total"]}
        p3 = Path("results/phase3/svc_svc_2016-06-16_W4_k6_range_seed0_cpu-apple-m5.json")
        if p3.exists() and s["workers"] == 4:
            s3 = json.load(open(p3)); l3 = effective(pl.read_parquet("data/phase3/svc_svc_2016-06-16_W4_k6_range_seed0.parquet")); m3 = l3.filter(pl.col("status") == "matched")
            row["service_w0"] = {"match_rate": round(m3.height / l3.height, 4), "wait_s": {"median": q(m3["actual_eta_s"], 0.5), "mean": round(float(m3["actual_eta_s"].mean()), 1)},
                                 "deadhead_m_mean": round(float(m3["leg_m"].mean()), 1), "deadhead_km_total": round(float(m3["leg_m"].sum()) / 1000, 1), "decisions_per_s_wall": s3["decisions_per_s_wall"]}
        res["service"].append(row)
    figures(res)
    Path("results/phase4").mkdir(exist_ok=True)
    Path(f"results/phase4/batching_{cfg['hardware_lane']}.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({"curve": res["curve"], "operating_point": res["operating_point"], "service": res["service"]}, indent=1)[:6000])

if __name__ == "__main__":
    main(sys.argv[1])
