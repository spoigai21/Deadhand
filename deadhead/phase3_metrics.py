"""Phase 3 metrics: the service against the oracle.

  balance     how the day's requests fall across the 8 partitions by key resolution (why the group cannot be balanced)
  mode_a      Finding 2, paired one-step, from the reference oracle ledgers: at every decision, the road pick among the
              candidates a worker owning the request's region could see, by key resolution, worker count and halo
  mode_b      the service's full replay against the oracle's, request by request, per run
  throughput  decisions per second and decision latency by worker count; match latency at x1 replay speed
  kill        the kill-test runs: stall, delayed requests, duplicate or conflicting assignments, double claims, the
              fleet log audited for a driver present in two regions or in none

Writes results/phase3/finding2_<lane>.json and the figures. Usage: python -m deadhead.phase3_metrics configs/phase3.yaml
"""
import glob, json, re, sys, zlib
from collections import defaultdict
from pathlib import Path
import h3, numpy as np, polars as pl, yaml
from .data import load_week, day_requests
from .stream import read_topic
from .phase3_data import partition_of

FLEET = 7500

def spread(vals, nd=3):
    v = [float(x) for x in vals if x is not None and not (isinstance(x, float) and np.isnan(x))]
    return None if not v else {"mean": round(float(np.mean(v)), nd), "min": round(min(v), nd), "max": round(max(v), nd), "n": len(v)}

def q(s, p): return None if s.len() == 0 else round(float(s.quantile(p)), 1)

# ------------------------------------------------------------------ balance
def balance(cfg):
    week = load_week(cfg["trips_dir"]); out = {}
    for day in cfg["days"]:
        d = day_requests(week, day)
        c8 = [h3.latlng_to_cell(la, lo, cfg["h3_resolution"]) for lo, la in zip(d["pickup_longitude"].to_list(), d["pickup_latitude"].to_list())]
        out[day] = {}
        for r in cfg["shard_key_resolutions"]:
            keys = [h3.cell_to_parent(c, r) for c in c8]
            parts = np.bincount([partition_of(k, cfg["partitions"]) for k in keys], minlength=cfg["partitions"]) / len(keys)
            cells = pl.Series(keys).value_counts(sort=True)
            out[day][str(r)] = {"distinct_cells": cells.height, "partition_shares": [round(float(x), 4) for x in parts], "busiest_partition_share": round(float(parts.max()), 4),
                                "busiest_cell_share": round(float(cells["count"][0] / len(keys)), 4), "top3_cell_shares": [round(float(x) / len(keys), 4) for x in cells["count"][:3].to_list()],
                                "ceiling_speedup": round(float(1 / parts.max()), 2)}
    return out

# ------------------------------------------------------------------ Mode A
def ref_ledgers(cfg):
    out = {}
    for f in sorted(glob.glob("data/phase3/assignments_ref_*.parquet")):
        m = re.match(r".*assignments_ref_(\d{4}-\d{2}-\d{2})_seed(\d+)\.parquet", f)
        out[(m.group(1), int(m.group(2)))] = pl.read_parquet(f)
    return out

def mode_a_cell(dec, col):
    """Rate and conditional cost for one shard column over the decisions with candidates."""
    x = dec[col + "_x"]; v = dec[col + "_v"]
    seen = dec.filter(pl.col(col + "_x").is_not_null())
    wrong = seen.filter(pl.col(col + "_x") > 0)
    return {"no_visible_candidate_rate": round(float(x.is_null().mean()), 4), "wrong_driver_rate": round(float((x > 0).fill_null(False).mean()), 4),
            "wrong_driver_rate_given_visible": round(float((seen[col + "_x"] > 0).mean()), 4) if seen.height else None,
            "visible_candidates_median": float(v.median()),
            "conditional_extra_wait_s": {"median": q(wrong[col + "_x"], 0.5), "p90": q(wrong[col + "_x"], 0.9), "mean": round(float(wrong[col + "_x"].mean()), 1) if wrong.height else None},
            "total_extra_wait_h": round(float(x.fill_null(0).sum()) / 3600, 1)}

def mode_a(cfg, ledgers):
    out = {"by_day": {}, "headline_config": {"key_resolution": cfg["key_resolution"], "workers": 4, "assignor": "range (contiguous partition ranges)"}}
    for (day, seed), df in ledgers.items():
        dec = df.filter(pl.col("n_candidates") > 0)
        cells = {}
        for r in cfg["shard_key_resolutions"]:
            for n in cfg["shard_workers"]:
                for tag in (f"s{r}n{n}", f"h{r}n{n}"):
                    cells[tag] = mode_a_cell(dec, tag)
        d = out["by_day"].setdefault(day, {"seeds": {}})
        d["seeds"][str(seed)] = {"decisions_with_candidates": dec.height, "cells": cells}
        if seed == 0:
            # by hour and by partition for the headline configuration
            tag = f"s{cfg['key_resolution']}n4"
            d["by_hour"] = dec.group_by("hour").agg(((pl.col(tag + "_x") > 0).fill_null(False)).mean().alias("rate"), pl.col(tag + "_x").is_null().mean().alias("no_visible"),
                                                   pl.col(tag + "_x").filter(pl.col(tag + "_x") > 0).median().alias("cond_median_s"), pl.len().alias("n")).sort("hour").to_dicts()
            d["by_partition"] = dec.with_columns(pl.col("key_cell").map_elements(lambda k: partition_of(k, cfg["partitions"]), return_dtype=pl.Int64).alias("partition")) \
                                   .group_by("partition").agg(((pl.col(tag + "_x") > 0).fill_null(False)).mean().alias("rate"), pl.col(tag + "_x").is_null().mean().alias("no_visible"), pl.len().alias("n")).sort("partition").to_dicts()
    # the spread over seeds, weekday
    for day, d in out["by_day"].items():
        agg = {}
        for tag in next(iter(d["seeds"].values()))["cells"]:
            rows = [s["cells"][tag] for s in d["seeds"].values()]
            agg[tag] = {"wrong_driver_rate": spread([r["wrong_driver_rate"] for r in rows], 4), "no_visible_candidate_rate": spread([r["no_visible_candidate_rate"] for r in rows], 4),
                        "conditional_extra_wait_s_median": spread([r["conditional_extra_wait_s"]["median"] for r in rows], 1), "conditional_extra_wait_s_p90": spread([r["conditional_extra_wait_s"]["p90"] for r in rows], 1),
                        "total_extra_wait_h": spread([r["total_extra_wait_h"] for r in rows], 1), "visible_candidates_median": spread([r["visible_candidates_median"] for r in rows], 1)}
        d["spread"] = agg
    return out

# ------------------------------------------------------------------ the service runs
def svc_runs():
    runs = []
    for f in sorted(glob.glob("results/phase3/svc_svc_*.json")):
        s = json.load(open(f)); s["_file"] = f
        p = Path(f"data/phase3/svc_{s['run_id']}.parquet")
        if p.exists():
            runs.append(s)
    return runs

def effective(df):
    """One record per request: the first decision (the re-emitted copy of a redelivered request is identical by construction)."""
    return df.sort("request_id", "redelivered", "done_wall_ms").unique("request_id", keep="first")

def mode_b(cfg, run, ledgers):
    df = pl.read_parquet(f"data/phase3/svc_{run['run_id']}.parquet"); svc = effective(df)
    ref = ledgers.get((run["day"], run["seed"]))
    if ref is None:
        return None
    t_max = svc["t"].max()
    ref = ref.filter(pl.col("t") <= t_max)
    j = ref.select("request_id", "t", "hour", pl.col("status").alias("o_status"), pl.col("driver").alias("o_driver"), pl.col("actual_eta_s").alias("o_wait"), pl.col("leg_m").alias("o_leg"), "n_candidates") \
           .join(svc.select("request_id", pl.col("status").alias("s_status"), pl.col("driver").alias("s_driver"), pl.col("actual_eta_s").alias("s_wait"), pl.col("leg_m").alias("s_leg"), pl.col("n_candidates").alias("s_cand"), "delay_s", "worker", "partition"), on="request_id", how="left")
    both = j.filter((pl.col("o_status") == "matched") & (pl.col("s_status") == "matched"))
    diff = both.filter(pl.col("o_driver") != pl.col("s_driver"))
    sm = svc.filter(pl.col("status") == "matched"); om_ = ref.filter(pl.col("status") == "matched")
    r = {"run_id": run["run_id"], "day": run["day"], "workers": run["workers"], "seed": run["seed"], "key_resolution": run["key_resolution"], "assignor": run["assignor"],
         "requests": ref.height, "decided_by_service": int(j["s_status"].is_not_null().sum()), "missing_in_service": int(j["s_status"].is_null().sum()),
         "same_status_rate": round(float((j["o_status"] == j["s_status"]).mean()), 4),
         "same_driver_rate_when_both_matched": round(float((both["o_driver"] == both["s_driver"]).mean()), 4) if both.height else None,
         "disagreement_rate": round(float(((j["o_status"] != j["s_status"]) | ((j["o_driver"] != j["s_driver"]) & (j["o_status"] == "matched"))).fill_null(True).mean()), 4),
         "wait_delta_when_different_driver_s": {"median": q(diff["s_wait"] - diff["o_wait"], 0.5), "p90": q(diff["s_wait"] - diff["o_wait"], 0.9), "mean": round(float((diff["s_wait"] - diff["o_wait"]).mean()), 1) if diff.height else None, "n": diff.height},
         "service": {"match_rate": round(sm.height / svc.height, 4), "wait_s": {"median": q(sm["actual_eta_s"], 0.5), "p90": q(sm["actual_eta_s"], 0.9)}, "deadhead_m_per_ride": {"median": q(sm["leg_m"], 0.5), "mean": round(float(sm["leg_m"].mean()), 1)},
                     "deadhead_km_total": round(float(sm["leg_m"].sum()) / 1000, 1), "candidates_median": float(svc.filter(pl.col("n_candidates") > 0)["n_candidates"].median()), "no_candidates_rate": round(float((svc["status"] == "no_candidates").mean()), 4)},
         "oracle": {"match_rate": round(om_.height / ref.height, 4), "wait_s": {"median": q(om_["actual_eta_s"], 0.5), "p90": q(om_["actual_eta_s"], 0.9)}, "deadhead_m_per_ride": {"median": q(om_["leg_m"], 0.5), "mean": round(float(om_["leg_m"].mean()), 1)},
                    "deadhead_km_total": round(float(om_["leg_m"].sum()) / 1000, 1), "candidates_median": float(ref.filter(pl.col("n_candidates") > 0)["n_candidates"].median()), "no_candidates_rate": round(float((ref["status"] == "no_candidates").mean()), 4)},
         "delay_s": {"median": q(svc["delay_s"], 0.5), "p99": q(svc["delay_s"], 0.99), "max": round(float(svc["delay_s"].max()), 1)}}
    r["by_partition"] = j.filter(pl.col("s_status").is_not_null()).group_by("partition").agg(pl.len().alias("n"), (((pl.col("o_driver") != pl.col("s_driver")) & (pl.col("o_status") == "matched")) | (pl.col("o_status") != pl.col("s_status"))).mean().alias("disagreement")).sort("partition").to_dicts()
    return r

def throughput(cfg, runs):
    rows = []
    for s in runs:
        if s.get("until") or s.get("kill"):
            continue
        ws = s["workers_summary"]
        rows.append({"day": s["day"], "workers": s["workers"], "key_resolution": s["key_resolution"], "seed": s["seed"], "requests": s["requests_decided"], "wall_s": s["wall_s"],
                     "decisions_per_s_wall": s["decisions_per_s_wall"], "decisions_per_s_active": s.get("decisions_per_s_active"), "decision_ms": s.get("decision_ms"),
                     "per_worker_decisions": {w: v["decisions"] for w, v in sorted(ws.items())}, "busiest_worker_share": round(max(v["decisions"] for v in ws.values()) / max(1, s["requests_decided"]), 3),
                     "table_ms_median": round(float(np.median([v["table_ms_median"] for v in ws.values() if v["table_ms_median"] == v["table_ms_median"]])), 2),
                     "gated_polls": sum(v["gated_polls"] for v in ws.values()), "osrm_calls": sum(v["osrm_calls"] for v in ws.values()), "cruises": sum(v["cruises"] for v in ws.values())})
    # the Python baseline is the Phase 2 single-process replay on an otherwise idle machine; the Phase 3 reference replays ran three at a time
    oracle = {}
    for f in glob.glob("results/phase2/run_*_road_N7500_T600_seed0_cruise_*.json") + glob.glob("results/phase3/run_ref_*.json"):
        s = json.load(open(f)); oracle[s["run_id"]] = {"decisions_per_s": s["decisions_per_s"], "seconds": s["seconds"], "decision_ms_median": s["decision_ms_median"], "decision_ms_p99": s["decision_ms_p99"], "table_ms_median": s["table_ms_median"], "concurrent_replays": 1 if "phase2" in f else 3}
    lat = [s for s in runs if s.get("speed") not in (None, "max")]
    return {"service": sorted(rows, key=lambda r: (r["day"], r["key_resolution"], r["workers"])), "oracle_python": oracle,
            "latency_at_speed": [{"run_id": s["run_id"], "speed": s["speed"], "window": [s["pace_from"], s["until"]], "workers": s["workers"], **s.get("match_latency_ms_at_speed", {})} for s in lat]}

# ------------------------------------------------------------------ the kill test
def fleet_audit(cfg, run_id):
    """Fold every partition of the fleet log the way a worker does and count where each driver ended up."""
    recs = read_topic(cfg["kafka"], f"fleet-{run_id}")
    present = defaultdict(dict); last = defaultdict(dict)
    for part, off, key, val in sorted(recs, key=lambda r: (r[0], r[1])):
        j = json.loads(val); k = j["k"]
        if k == "r":
            continue
        d, v = j["id"], j["v"]
        if last[part].get(d, -1) > v:
            continue
        last[part][d] = v
        if k == "x":
            present[part].pop(d, None)
        else:
            present[part][d] = v
    where = defaultdict(list)
    for part, ds in present.items():
        for d in ds:
            where[d].append(part)
    return {"records": len(recs), "drivers_present": len(where), "in_two_regions": sum(1 for v in where.values() if len(v) > 1), "missing": FLEET - len(where)}

def kill_metrics(cfg, run, ledgers):
    df = pl.read_parquet(f"data/phase3/svc_{run['run_id']}.parquet")
    k = run["kill"]; kill_ms = k["wall_ms"]
    # duplicates and conflicts in the raw record stream
    per = df.group_by("request_id").agg(pl.len().alias("n"), pl.col("driver").n_unique().alias("drivers"), pl.col("status").n_unique().alias("statuses"))
    dup = per.filter(pl.col("n") > 1)
    conflict = per.filter((pl.col("drivers") > 1) | (pl.col("statuses") > 1))
    eff = effective(df)
    # a double claim: the same driver claimed (at `now`, the time the decision was made) before the previous claim's trip ended.
    # The oracle's check uses t because there now == t; a request the kill delayed is matched at now > t, after the driver is free.
    m = eff.filter(pl.col("status") == "matched").sort("driver", "now")
    prev_until = m["busy_until"].shift(1); prev_driver = m["driver"].shift(1)
    overlap = m.filter((pl.col("driver") == prev_driver) & (pl.col("now") < prev_until))
    overlap_by_t = m.sort("driver", "t").filter((pl.col("driver") == pl.col("driver").shift(1)) & (pl.col("t") < pl.col("busy_until").shift(1)))
    # stall per partition: the widest gap in decision completion times inside the minute around the kill
    stalls = {}
    for p in sorted(eff["partition"].unique().to_list()):
        w = eff.filter((pl.col("partition") == p) & (pl.col("done_wall_ms") > kill_ms - 60000) & (pl.col("done_wall_ms") < kill_ms + 120000)).sort("done_wall_ms")["done_wall_ms"]
        if w.len() < 2:
            continue
        gaps = w.diff().drop_nulls(); i = int(gaps.arg_max())
        stalls[str(p)] = {"max_gap_s": round(float(gaps.max()) / 1000, 2), "gap_starts_s_after_kill": round(float(w[i] - kill_ms) / 1000, 2), "orphaned": p in k["owned"]}
    orphan = [v["max_gap_s"] for p, v in stalls.items() if v["orphaned"]]; other = [v["max_gap_s"] for p, v in stalls.items() if not v["orphaned"]]
    delayed = eff.filter(pl.col("delay_s") > 0)
    ev = [e for e in run["events"] if e["event"] in ("revoked", "assigned", "folded")]
    takeover = [e for e in ev if e["event"] == "assigned" and e["wall_ms"] > kill_ms and any(p in e["partitions"] for p in k["owned"])]
    r = {"run_id": run["run_id"], "assignor": run["assignor"], "seed": run["seed"], "killed": k["worker"], "killed_owned": k["owned"], "kill_event_time_h": round((k["event_t"] % 86400) / 3600, 2),
         "records": df.height, "requests": eff.height, "redelivered_records": int(df["redelivered"].sum()), "duplicate_request_ids": dup.height, "conflicting_assignments": conflict.height,
         "double_claims": overlap.height, "overlaps_by_request_time_explained_by_delay": overlap_by_t.height, "takeover_s_after_kill": round((takeover[0]["wall_ms"] - kill_ms) / 1000, 2) if takeover else None,
         "stall_orphaned_partitions_s": {"max": max(orphan) if orphan else None, "min": min(orphan) if orphan else None},
         "stall_other_partitions_s": {"max": max(other) if other else None, "min": min(other) if other else None},
         "delayed_requests": delayed.height, "delay_s": {"median": q(delayed["delay_s"], 0.5), "p90": q(delayed["delay_s"], 0.9), "max": round(float(delayed["delay_s"].max()), 1) if delayed.height else None},
         "stalls": stalls, "fleet_audit": fleet_audit(cfg, run["run_id"])}
    ws = run["workers_summary"]
    r["fold_ms_after_kill_total"] = sum(v["fold_ms_total"] for w, v in ws.items() if w != k["worker"])
    mb = mode_b(cfg, run, ledgers)
    if mb:
        r["vs_oracle"] = {kk: mb[kk] for kk in ("disagreement_rate", "same_driver_rate_when_both_matched", "missing_in_service")}
        ref = ledgers[(run["day"], run["seed"])]
        kt = k["event_t"]
        for name, cond in (("before_kill", pl.col("t") < kt), ("after_kill", pl.col("t") >= kt)):
            sub = effective(df).filter(cond).select("request_id", pl.col("driver").alias("s_driver"), pl.col("status").alias("s_status"))
            jj = ref.filter(cond).select("request_id", "driver", "status").join(sub, on="request_id", how="left")
            r["vs_oracle"][name] = round(float(((jj["status"] != jj["s_status"]) | ((jj["driver"] != jj["s_driver"]) & (jj["status"] == "matched"))).fill_null(True).mean()), 4)
    return r

# ------------------------------------------------------------------ figures
def figures(res, out="figures"):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    from .figures import BLUE, ORANGE, AQUA, INK, INK2, GRID, SURFACE
    Path(out).mkdir(exist_ok=True)
    day = "2016-06-16"; ma = res["mode_a"]["by_day"][day]["spread"]; cfg = res["config"]
    # Finding 2 by worker count, one line per key resolution; halo as the dashed upper line
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    colors = {5: "#9bc4f2", 6: BLUE, 7: "#1c5cab", 8: "#0d366b"}
    for r in cfg["shard_key_resolutions"]:
        ns = cfg["shard_workers"]
        axes[0].plot(ns, [100 * ma[f"s{r}n{n}"]["wrong_driver_rate"]["mean"] for n in ns], "-o", color=colors[r], lw=2, ms=5, label=f"key resolution {r}")
        axes[0].plot(ns, [100 * ma[f"h{r}n{n}"]["wrong_driver_rate"]["mean"] for n in ns], "--", color=colors[r], lw=1.2)
        axes[1].plot(ns, [ma[f"s{r}n{n}"]["conditional_extra_wait_s_median"]["mean"] for n in ns], "-o", color=colors[r], lw=2, ms=5)
    axes[0].plot([], [], "--", color=INK2, lw=1.2, label="with a one-ring halo (upper line)")
    axes[0].set_xlabel("workers"); axes[0].set_ylabel("decisions where the shard picks a slower driver, %"); axes[0].set_xticks(cfg["shard_workers"]); axes[0].legend(frameon=False, fontsize=8)
    axes[1].set_xlabel("workers"); axes[1].set_ylabel("extra wait on those decisions, median s"); axes[1].set_xticks(cfg["shard_workers"])
    fig.suptitle("Finding 2: what the geographic shard costs (weekday, 7,500 drivers, 5 seeds, Mode A)", color=INK, fontsize=10)
    fig.tight_layout(); fig.savefig(f"{out}/phase3_boundary_cost.png", dpi=150); plt.close(fig)
    # throughput by worker count against the oracle
    tp = [r for r in res["throughput"]["service"] if r["day"] == day and r["key_resolution"] == cfg["key_resolution"]]
    if tp:
        fig, ax = plt.subplots(figsize=(5.5, 3.4))
        ax.plot([r["workers"] for r in tp], [r["decisions_per_s_wall"] for r in tp], "-o", color=BLUE, lw=2, ms=6, label="Java consumer group")
        tp7 = [r for r in res["throughput"]["service"] if r["day"] == day and r["key_resolution"] == 7]
        if tp7:
            ax.plot([r["workers"] for r in tp7], [r["decisions_per_s_wall"] for r in tp7], "-s", color=AQUA, lw=2, ms=6, label="key resolution 7")
        o = res["throughput"]["oracle_python"].get(f"{day}_road_N7500_T600_seed0_cruise")
        if o:
            ax.axhline(o["decisions_per_s"], color=ORANGE, lw=1.5, ls="--", label="Python oracle, one process")
        bal = res["balance"][day][str(cfg["key_resolution"])]
        ax.axhline(tp[0]["decisions_per_s_wall"] * bal["ceiling_speedup"], color=INK2, lw=1, ls=":", label=f"ceiling: busiest partition holds {100 * bal['busiest_partition_share']:.0f}%")
        ax.set_xlabel("workers"); ax.set_ylabel("decisions per second, whole day"); ax.set_xticks([r["workers"] for r in tp]); ax.legend(frameon=False, fontsize=8)
        ax.set_title("Throughput at maximum replay speed (weekday)", fontsize=10, color=INK)
        fig.tight_layout(); fig.savefig(f"{out}/phase3_throughput.png", dpi=150); plt.close(fig)
    # the kill test: stall per assignor
    ks = res.get("kill", [])
    if ks:
        fig, ax = plt.subplots(figsize=(6, 3.4))
        for i, (a, color) in enumerate((("range", ORANGE), ("cooperative", BLUE))):
            rows = [r for r in ks if r["assignor"] == a]
            xs = [r["seed"] + (i - 0.5) * 0.3 for r in rows]
            ax.bar(xs, [r["stall_orphaned_partitions_s"]["max"] or 0 for r in rows], width=0.28, color=color, label=f"{'eager (RangeAssignor)' if a == 'range' else 'cooperative (CooperativeStickyAssignor)'}: orphaned partitions")
            ax.bar(xs, [r["stall_other_partitions_s"]["max"] or 0 for r in rows], width=0.28, color=color, alpha=0.35, hatch="///", label=f"same, other partitions")
        ax.set_xlabel("run (seed)"); ax.set_ylabel("longest pause in decisions, s"); ax.legend(frameon=False, fontsize=7)
        ax.set_title("Kill test: one of four workers killed mid-replay", fontsize=10, color=INK)
        fig.tight_layout(); fig.savefig(f"{out}/phase3_kill_test.png", dpi=150); plt.close(fig)

def main(config_path):
    cfg = yaml.safe_load(open(config_path))
    ledgers = ref_ledgers(cfg); runs = svc_runs()
    res = {"config": {k: cfg[k] for k in ("partitions", "key_resolution", "shard_key_resolutions", "shard_workers", "fleet_size", "abandon_after_s", "watermark_slack_s", "session_timeout_ms", "candidates_k", "ring_k")},
           "balance": balance(cfg), "mode_a": mode_a(cfg, ledgers) if ledgers else None,
           "mode_b": [m for m in (mode_b(cfg, r, ledgers) for r in runs if not r.get("kill")) if m], "throughput": throughput(cfg, runs),
           "kill": [kill_metrics(cfg, r, ledgers) for r in runs if r.get("kill")]}
    Path("results/phase3").mkdir(parents=True, exist_ok=True)
    Path(f"results/phase3/finding2_{cfg['hardware_lane']}.json").write_text(json.dumps(res, indent=1, default=float))
    if res["mode_a"]:
        figures(res)
    return res

if __name__ == "__main__":
    main(sys.argv[1])
