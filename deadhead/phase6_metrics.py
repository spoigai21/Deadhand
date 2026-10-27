"""Phase 6 metrics: what one city-day costs to replay through the service, where the machine time goes, and how the
cost moves with fleet size, matching radius and worker count, from the monitored runs (results/phase6/svc_cost_*.json)
and the Phase 3 and Phase 4 service runs for the worker counts and the window that were not re-run.

Writes results/phase6/cost_<lane>.json and figures/phase6_cost.png.
Usage: python -m deadhead.phase6_metrics configs/phase6.yaml
"""
import glob, json, re, subprocess, sys
from pathlib import Path
import yaml
from .phase3_service import load_config
from .resources import summarize

PROF_RE = re.compile(r"(\d+) calls, ([\d.]+) s")
STAGES = ("osrm_table", "osrm_route", "cruise", "await_ack", "commit")          # inside a decision's processing
WAITS = ("poll_requests", "poll_fleetlog", "poll_clocks")                        # outside it: waiting for records, the watermark gate among them

def prof(ws):
    """worker profile strings -> {stage: seconds}."""
    out = {}
    for k, v in ws.get("profile", {}).items():
        m = PROF_RE.match(v); out[k] = float(m.group(2)) if m else 0.0
    return out

def worker_profile(s):
    """The stages summed over the run's workers, as a share of their processing time."""
    tot = {}
    for ws in s["workers_summary"].values():
        for k, v in prof(ws).items():
            tot[k] = tot.get(k, 0.0) + v
    p = tot.get("process", 0.0)
    known = sum(tot.get(k, 0.0) for k in STAGES)
    shares = {k: round(tot.get(k, 0.0) / p, 4) for k in STAGES if p} if p else {}
    if p:
        shares["other"] = round(max(0.0, p - known) / p, 4)
    return {"process_s": round(p, 1), "waiting_s": round(sum(tot.get(k, 0.0) for k in WAITS), 1), "stage_s": {k: round(v, 1) for k, v in tot.items()}, "shares": shares}

def machine(s):
    """Where the machine's time went, from the resource monitor: core-seconds by process over the run."""
    r = s.get("resources")
    if not r:
        return None
    wall = r["wall_s"]; se = summarize(r["raw"], wall, r["ncpu"]) if r.get("raw") else r["series"]
    jvm_cpu = sum(r["jvm_cpu_seconds"].values())
    cs = lambda k: se.get(k, {}).get("core_seconds", 0.0)
    cc = r.get("container_cpu_seconds", {})
    osrm, kafka = cc.get("deadhand-osrm-1", cs("deadhand-osrm-1_cpu")), cc.get("kafka", cs("kafka_cpu"))       # exact when the socket probe ran
    busy = se.get("machine_busy_pct", {})
    # the work: the containers' own CPU (seen inside the virtual machine) and the JVMs'; the plumbing: what the host spends
    # running the virtual machine and Docker's port forwarding beyond the containers' own work, and the kernel
    machine_cs = busy.get("core_seconds") or 0.0
    parts = {"osrm": round(osrm, 1), "kafka": round(kafka, 1), "jvm_workers": round(jvm_cpu, 1), "launcher": cs("launcher_cpu"),
             "vm_and_docker_overhead": round(max(0.0, cs("host_vm") + cs("host_docker") - osrm - kafka), 1),
             "other_host_processes": round(max(0.0, cs("host_other") - cs("launcher_cpu")), 1)}
    parts["kernel_and_unattributed"] = round(max(0.0, machine_cs - sum(parts.values())), 1)
    acc = sum(parts.values())
    work = osrm + kafka + jvm_cpu
    return {"wall_s": wall, "samples": r["samples"], "ncpu": r["ncpu"], "machine_busy_mean_pct": busy.get("mean"), "machine_busy_p95_pct": busy.get("p95"),
            "machine_core_seconds": busy.get("core_seconds"), "cores_busy_mean": round(busy.get("mean", 0) / 100 * r["ncpu"], 2),
            "core_seconds": parts, "accounted_core_seconds": round(acc, 1),
            "shares_of_accounted": {k: round(v / acc, 3) for k, v in parts.items()} if acc else {},
            "work_core_seconds": round(work, 1), "work_cores_mean": round(work / wall, 2) if wall else None,
            "work_shares": {"osrm": round(osrm / work, 3), "jvm_workers": round(jvm_cpu / work, 3), "kafka": round(kafka / work, 3)} if work else {},
            "unaccounted_core_seconds": round((busy.get("core_seconds") or 0) - acc, 1),
            "jvm_rss_mb_max": {w: se.get(f"jvm_{w}_mb", {}).get("max") for w in r["jvm_cpu_seconds"]},
            "jvm_rss_mb_max_sum": round(sum(se.get(f"jvm_{w}_mb", {}).get("max", 0) for w in r["jvm_cpu_seconds"]), 0),
            "osrm_mb_max": se.get("deadhand-osrm-1_mb", {}).get("max"), "kafka_mb_max": se.get("kafka_mb", {}).get("max"),
            "osrm_cores_mean": round(se.get("deadhand-osrm-1_cpu", {}).get("mean", 0) / 100, 2), "kafka_cores_mean": round(se.get("kafka_cpu", {}).get("mean", 0) / 100, 2),
            "jvm_cores_mean": round(jvm_cpu / wall, 2) if wall else None}

def paced_window(s, phase_dir):
    """At a paced speed: the machine over the paced window only (from the group's pace origin, recovered from the ledger's due
    times, for the window's length), not over the whole run, which replays from midnight at maximum speed first."""
    r = s.get("resources")
    f = Path(f"data/{phase_dir}/svc_{s['run_id']}.parquet")
    if not r or not r.get("raw") or not s.get("pace_from") or not s.get("until") or not f.exists():
        return None
    import polars as pl
    hm = lambda x: int(x[:2]) * 3600 + int(x[3:5]) * 60
    df = pl.read_parquet(f); t0 = df["t"].min(); pace_t = t0 + hm(s["pace_from"]); length = hm(s["until"]) - hm(s["pace_from"])
    w = df.filter(pl.col("t") >= pace_t).with_columns(origin=pl.col("due_wall_ms") - ((pl.col("t") - pace_t) * 1000).cast(pl.Int64))
    origins = w.group_by("worker").agg(pl.col("origin").min())["origin"]
    o = origins.min() / 1000
    raw = [x for x in r["raw"] if o <= x["t"] <= o + length]
    if len(raw) < 2:
        return None
    se = summarize(raw, length, r["ncpu"]); busy = se.get("machine_busy_pct", {})
    jvm = sum(se.get(k, {}).get("mean", 0) for k in se if k.startswith("jvm_") and k.endswith("_cpu")) / 100
    osrm = se.get("deadhand-osrm-1_cpu", {}).get("mean", 0) / 100; kafka = se.get("kafka_cpu", {}).get("mean", 0) / 100
    return {"window_s": length, "samples": len(raw), "pace_origin_spread_s_across_workers": round((origins.max() - origins.min()) / 1000, 3),
            "machine_busy_mean_pct": busy.get("mean"), "machine_busy_p95_pct": busy.get("p95"), "cores_busy_mean": round((busy.get("mean") or 0) / 100 * r["ncpu"], 2),
            "osrm_cores_mean": round(osrm, 3), "kafka_cores_mean": round(kafka, 3), "jvm_cores_mean": round(jvm, 3), "work_cores_mean": round(osrm + kafka + jvm, 3),
            "vm_and_docker_cores_mean": round(max(0.0, (se.get("host_vm", {}).get("mean", 0) + se.get("host_docker", {}).get("mean", 0)) / 100 - osrm - kafka), 3),
            "other_host_cores_mean": round(se.get("host_other", {}).get("mean", 0) / 100, 3),
            "decisions_in_window": int(w["request_id"].n_unique())}

def cost(wall_s, price):
    h = wall_s / 3600
    return {"machine_hours": round(h, 4), "usd": round(h * price["usd_per_hour"], 4), "usd_per_hour": price["usd_per_hour"], "instance": price["instance"]}

def row(s, price, source):
    r = {"run_id": s["run_id"], "source": source, "workers": s["workers"], "fleet": s["fleet_size"], "ring": s.get("ring_k", 3), "key_resolution": s["key_resolution"],
         "window_s": s.get("window_s", 0), "speed": s["speed"], "pace_from": s.get("pace_from"), "until": s.get("until"),
         "wall_s": s["wall_s"], "decisions": s["requests_decided"], "decisions_per_s": s["decisions_per_s_wall"],
         "decision_ms": s.get("decision_ms"), "table_ms_median": {w: ws.get("table_ms_median") for w, ws in s["workers_summary"].items()},
         "osrm_calls": sum(ws.get("osrm_calls", 0) for ws in s["workers_summary"].values()),
         "worker_profile": worker_profile(s), "machine": machine(s), "paced_window": paced_window(s, "phase6") if source == "phase6" else None}
    if s["speed"] == "max":
        r["cost_per_city_day"] = cost(s["wall_s"], price)
    if s.get("match_latency_ms_at_speed"):
        r["match_latency_ms"] = s["match_latency_ms_at_speed"]
    return r

def load_runs(cfg):
    lane = cfg["hardware_lane"]; price = cfg["price"]; out = []
    for f in sorted(glob.glob(f"results/phase6/svc_cost_*_{lane}.json")):
        out.append(row(json.load(open(f)), price, "phase6"))
    day = cfg["day"]
    for w in (1, 2, 4, 8):                                                       # the Phase 3 table, for the worker counts not re-run
        f = Path(f"results/phase3/svc_svc_{day}_W{w}_k6_range_seed0_{lane}.json")
        if f.exists():
            out.append(row(json.load(open(f)), price, "phase3"))
    for w in (4, 8):
        f = Path(f"results/phase3/svc_svc_{day}_W{w}_k7_range_seed0_{lane}.json")
        if f.exists():
            out.append(row(json.load(open(f)), price, "phase3"))
    f = Path(f"results/phase4/svc_svc_{day}_W4_k6_range_seed0_w60_{lane}.json")
    if f.exists():
        out.append(row(json.load(open(f)), price, "phase4"))
    return out

def pick(rows, **kw):
    return [r for r in rows if all(r.get(k) == v for k, v in kw.items())]

def main(config_path):
    cfg = load_config(config_path); lane = cfg["hardware_lane"]; price = cfg["price"]
    rows = load_runs(cfg)
    base_fleet, base_ring = cfg["fleet_size"], cfg["ring_k"]
    maxed = [r for r in rows if r["speed"] == "max" and r["window_s"] == 0 and r["key_resolution"] == 6]
    op = pick(maxed, source="phase6", workers=cfg["workers"], fleet=base_fleet, ring=base_ring)
    op = op[0] if op else None
    mem_gb = round(int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout or 0) / 2**30, 1)
    res = {"lane": lane, "hardware": {**(json.load(open(glob.glob(f"results/phase6/svc_cost_*_{lane}.json")[0]))["hardware"] if op else {}), "memory_gb": mem_gb,
                                      "ncpu": op["machine"]["ncpu"] if op and op["machine"] else None},
           "price": price, "operating_point": {"workers": cfg["workers"], "fleet": base_fleet, "ring": base_ring, "window_s": 0, "key_resolution": 6}}
    if op:
        m = op["machine"]; alt = {a["instance"]: a for a in price.get("alternatives", [])}
        res["cost_per_city_day"] = {**op["cost_per_city_day"], "wall_s": op["wall_s"], "decisions": op["decisions"], "decisions_per_s": op["decisions_per_s"],
                                    "cores_busy_mean": m["cores_busy_mean"] if m else None, "machine_busy_mean_pct": m["machine_busy_mean_pct"] if m else None,
                                    "usd_per_million_decisions": round(op["cost_per_city_day"]["usd"] / op["decisions"] * 1e6, 3),
                                    "fits_4_vcpu": (m["work_cores_mean"] + m["core_seconds"]["kernel_and_unattributed"] / op["wall_s"] <= 4) if m else None,
                                    "fits_4_vcpu_basis": "the run's own processes (OSRM, JVMs, Kafka) plus all kernel and unattributed time, mean cores; the laptop's VM plumbing and other applications excluded",
                                    "usd_on_4_vcpu_if_same_wall": round(op["wall_s"] / 3600 * alt["c8g.xlarge"]["usd_per_hour"], 4) if "c8g.xlarge" in alt else None,
                                    "memory_needed_gb": round(((m["jvm_rss_mb_max_sum"] or 0) + (m["osrm_mb_max"] or 0) + (m["kafka_mb_max"] or 0)) / 1024, 2) if m else None}
        res["bottleneck"] = {"machine": m, "worker_profile": op["worker_profile"]}
    # scaling tables
    res["by_workers"] = sorted([{k: r[k] for k in ("source", "workers", "key_resolution", "wall_s", "decisions_per_s", "cost_per_city_day")} | {"machine": r["machine"] and {k: r["machine"][k] for k in ("cores_busy_mean", "core_seconds", "machine_core_seconds")}}
                                for r in maxed + [r for r in rows if r["speed"] == "max" and r["key_resolution"] == 7] if r["fleet"] == base_fleet and r["ring"] == base_ring],
                               key=lambda r: (r["key_resolution"], r["workers"], r["source"]))
    res["by_fleet"] = sorted([{k: r[k] for k in ("fleet", "wall_s", "decisions", "decisions_per_s", "osrm_calls", "table_ms_median", "cost_per_city_day")} | {"cores_busy_mean": r["machine"]["cores_busy_mean"] if r["machine"] else None, "osrm_core_seconds": r["machine"]["core_seconds"]["osrm"] if r["machine"] else None, "jvm_core_seconds": r["machine"]["core_seconds"]["jvm_workers"] if r["machine"] else None}
                              for r in pick(maxed, source="phase6", workers=cfg["workers"], ring=base_ring)], key=lambda r: r["fleet"])
    res["by_ring"] = sorted([{k: r[k] for k in ("ring", "wall_s", "decisions", "decisions_per_s", "osrm_calls", "table_ms_median", "cost_per_city_day")} | {"cores_busy_mean": r["machine"]["cores_busy_mean"] if r["machine"] else None, "osrm_core_seconds": r["machine"]["core_seconds"]["osrm"] if r["machine"] else None, "jvm_core_seconds": r["machine"]["core_seconds"]["jvm_workers"] if r["machine"] else None}
                             for r in pick(maxed, source="phase6", workers=cfg["workers"], fleet=base_fleet)], key=lambda r: r["ring"])
    w60 = pick(rows, source="phase4")
    if w60:
        res["with_window_60s"] = {k: w60[0][k] for k in ("wall_s", "decisions_per_s", "cost_per_city_day")}
    # x1: the day costs the day; what the machine does meanwhile
    x1 = []
    for r in pick(rows, source="phase6", speed=1.0):
        m = r["machine"]
        x1.append({"window": [r["pace_from"], r["until"]], "decisions_in_run": r["decisions"], "match_latency_ms": r.get("match_latency_ms"),
                   "paced_window": r.get("paced_window"),
                   "machine_busy_mean_pct": m["machine_busy_mean_pct"] if m else None, "cores_busy_mean": m["cores_busy_mean"] if m else None,
                   "osrm_cores_mean": m["osrm_cores_mean"] if m else None, "kafka_cores_mean": m["kafka_cores_mean"] if m else None, "jvm_cores_mean": m["jvm_cores_mean"] if m else None,
                   "note": "the run replays from midnight at maximum speed and paces from the window's start; the machine figures are over the whole run, the paced window dominates it"})
    res["at_x1"] = {"windows": x1, "cost_per_city_day": {"machine_hours": 24, "usd": round(24 * price["usd_per_hour"], 2), "usd_per_hour": price["usd_per_hour"], "instance": price["instance"]}}
    res["runs"] = rows
    Path("results/phase6").mkdir(parents=True, exist_ok=True)
    out = Path(f"results/phase6/cost_{lane}.json"); out.write_text(json.dumps(res, indent=1))
    figures(res)
    return res

def figures(res, out="figures"):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    Path(out).mkdir(exist_ok=True)
    runs = [r for r in res["runs"] if r["source"] == "phase6" and r["speed"] == "max" and r["machine"]]
    if not runs:
        return
    runs.sort(key=lambda r: (r["workers"], r["fleet"], r["ring"]))
    labels = [f"N{r['workers']} F{r['fleet']} R{r['ring']}" for r in runs]
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.6), gridspec_kw={"width_ratios": [1.5, 1]})
    parts = [("osrm", "#1f5fbf", "OSRM (routing)", None), ("jvm_workers", "#d1495b", "workers (JVM)", None), ("kafka", "#2a9d8f", "Kafka", None),
             ("vm_and_docker_overhead", "#c9ccd1", "the laptop's VM and Docker plumbing (not the run's)", "//")]
    bottom = [0.0] * len(runs)
    for k, col, lab, hatch in parts:
        v = [r["machine"]["core_seconds"].get(k, 0.0) for r in runs]
        ax[0].bar(labels, v, bottom=bottom, color=col, label=lab, width=0.7, edgecolor="white", linewidth=1, hatch=hatch); bottom = [b + x for b, x in zip(bottom, v)]
    ax[0].set_ylabel("core-seconds over the run"); ax[0].tick_params(axis="x", rotation=45, labelsize=7); ax[0].set_ylim(0, max(bottom) * 1.35); ax[0].legend(frameon=False, fontsize=7, loc="upper left")
    ax[0].set_title("Where the machine's time goes (N workers, F fleet, R ring); other apps and the kernel not shown", fontsize=9)
    ax[1].bar(labels, [r["wall_s"] for r in runs], color="#1f5fbf", width=0.7)
    for i, r in enumerate(runs):
        ax[1].text(i, r["wall_s"], f"${r['cost_per_city_day']['usd']:.3f}", ha="center", va="bottom", fontsize=7)
    ax[1].set_ylabel("wall seconds for the city-day"); ax[1].tick_params(axis="x", rotation=45, labelsize=7)
    ax[1].set_title(f"Wall and cost at ${res['price']['usd_per_hour']}/h ({res['price']['instance']}, list, {res['price']['read_on']})", fontsize=9)
    fig.suptitle("Phase 6 — what one city-day costs to replay through the service (maximum speed, weekday, seed 0)"); fig.tight_layout(); fig.savefig(f"{out}/phase6_cost.png", dpi=150); plt.close(fig)

if __name__ == "__main__":
    r = main(sys.argv[1])
    print(json.dumps({k: r[k] for k in r if k not in ("runs", "bottleneck")}, indent=1))
