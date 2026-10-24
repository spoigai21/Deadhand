"""Phase 3: run the Java consumer group against a day of the stream and collect what it did.

One run = N workers in one consumer group, started as local JVM processes against the broker and OSRM on this machine.
The launcher creates the run's topics, seeds the fleet log with the initial driver positions (the oracle's, same seed),
starts the workers, watches the clock topic, kills a worker at the stated event time if asked, and when every worker
reports its partitions ended, stops them and drains assignments-<run_id> into data/phase3/svc_<run_id>.parquet with a
run summary in results/phase3/svc_<run_id>_<hardware>.json.

Usage: python -m deadhead.phase3_service configs/phase3.yaml --day 2016-06-16 --workers 4 [--seed 0] [--key-res 6]
         [--assignor range|cooperative] [--speed 1 --pace-from 21:00] [--until 12:00] [--kill-at 09:00] [--run-id X]
"""
import argparse, json, os, signal, subprocess, sys, time
from pathlib import Path
import polars as pl, yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from .stream import ensure_topic, requests_topic, read_topic
from .phase3_data import partition_of
from .phase0_data import hardware

JAR = "consumer/build/libs/deadhead-consumer.jar"

def hhmm_to_t(day_start, s):
    h, m = s.split(":"); return day_start + int(h) * 3600 + int(m) * 60

def seed_fleet(cfg, day, seed, key_res, run_id):
    """The initial fleet into fleet-<run_id>, one upsert per driver in the partition of its region."""
    f = json.loads(Path(f"data/phase3/fleet_{day}_N{cfg['fleet_size']}_seed{seed}.json").read_text())
    cells = json.loads(Path(f"data/phase3/cells_{day}.json").read_text())["cells"]
    p = Producer({"bootstrap.servers": cfg["kafka"], "enable.idempotence": True, "linger.ms": 20})
    t0 = f["day_start"]; by_part = {}
    for d, lon, lat, cell in f["drivers"]:
        part = partition_of(cells[cell][f"p{key_res}"], cfg["partitions"]); by_part[part] = by_part.get(part, 0) + 1
        rec = {"k": "d", "id": d, "lon": lon, "lat": lat, "cell": cell, "busy_until": -1.0, "idle_since": t0, "cruise_n": 0, "v": int(t0), "gen": 0, "w": "init", "g": -1}
        p.produce(f"fleet-{run_id}", partition=part, key=str(d), value=json.dumps(rec).encode())
    p.flush()
    return t0, by_part

def members(cfg, run_id):
    """worker -> partitions, from the group's current assignment."""
    admin = AdminClient({"bootstrap.servers": cfg["kafka"]})
    g = list(admin.describe_consumer_groups([f"svc-{run_id}"]).values())[0].result()
    return {m.client_id: sorted(tp.partition for tp in (m.assignment.topic_partitions if m.assignment else [])) for m in g.members}

def run(cfg, day, n_workers, seed, key_res, assignor, speed, pace_from, until, kill_at, run_id, slack=None, window=0):
    rid = run_id
    admin = AdminClient({"bootstrap.servers": cfg["kafka"]})
    for g in (f"svc-{rid}",):            # a run id reused must not inherit committed offsets from an earlier run
        try: list(admin.delete_consumer_groups([g]).values())[0].result()
        except Exception: pass
    for name, parts in ((f"fleet-{rid}", cfg["partitions"]), (f"assignments-{rid}", cfg["partitions"]), (f"clock-{rid}", 1)):
        ensure_topic(cfg["kafka"], name, parts, recreate=True)
    t0, fleet_by_part = seed_fleet(cfg, day, seed, key_res, rid)
    topic = requests_topic(day, key_res)
    out_dir = cfg.get("phase_dir", "phase3")                 # Phase 4 reuses the launcher and writes under its own directory
    logs = Path(f"results/{out_dir}/logs"); logs.mkdir(parents=True, exist_ok=True)
    args = [f"--run={rid}", f"--requests={topic}", f"--cells=data/phase3/cells_{day}.json", f"--demand=data/phase3/demand_{day}.json", f"--key-res={key_res}",
            f"--partitions={cfg['partitions']}", f"--candidates={cfg['candidates_k']}", f"--abandon={cfg['abandon_after_s']}", f"--cruise-after={cfg['cruise_after_s']}",
            f"--slack={cfg['watermark_slack_s'] if slack is None else slack}", f"--assignor={assignor}", f"--session-timeout={cfg['session_timeout_ms']}", f"--heartbeat={cfg['heartbeat_interval_ms']}",
            f"--kafka={cfg['kafka']}", f"--osrm={cfg['osrm']}", "--exit-when-done"]
    if window:
        args += [f"--window={window:g}", f"--day-start={t0:.0f}"]       # Phase 4: the tumbling event-time window inside the worker
    if speed:
        args += [f"--speed={speed}", f"--pace-from={hhmm_to_t(t0, pace_from) if pace_from else t0}"]
    if until:
        args.append(f"--until={hhmm_to_t(t0, until)}")
    procs, files = {}, {}
    wall_start = time.time()
    for i in range(n_workers):
        w = f"w{i}"
        files[w] = open(logs / f"{rid}_{w}.log", "w")
        procs[w] = subprocess.Popen(["java", "-Xmx2g", "-XX:+UseParallelGC", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", JAR, f"--worker={w}"] + args, stdout=files[w], stderr=subprocess.STDOUT)
    c = Consumer({"bootstrap.servers": cfg["kafka"], "group.id": f"launcher-{rid}", "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.assign([TopicPartition(f"clock-{rid}", 0, 0)])
    state, kill = {}, None
    kill_t = hhmm_to_t(t0, kill_at) if kill_at else None
    victim_part = cfg.get("kill_victim_partition", 0)
    last_print = 0
    try:
        while True:
            for m in c.consume(num_messages=1000, timeout=0.5):
                if m.error():
                    continue
                j = json.loads(m.value()); state[j["w"]] = j
            dead = [w for w, pr in procs.items() if pr.poll() is not None and (kill is None or w != kill["worker"])]
            if dead and not all(state.get(w, {}).get("done") for w in dead):
                raise RuntimeError(f"worker(s) {dead} exited early; see {logs}")
            live = [w for w in procs if procs[w].poll() is None]
            if kill_t and kill is None and any(j.get("clock", 0) >= kill_t for j in state.values()):
                owners = members(cfg, rid)
                victim = next((w for w, ps in owners.items() if victim_part in ps), None)
                if victim is None:
                    raise RuntimeError(f"no member owns partition {victim_part}: {owners}")
                procs[victim].send_signal(signal.SIGKILL)
                kill = {"worker": victim, "wall_ms": int(time.time() * 1000), "event_t": kill_t, "owned": owners[victim], "assignment_before": owners}
                print(f"{rid}: killed {victim} (partitions {owners[victim]}) at event time {kill_at}", flush=True)
            if live and all(state.get(w, {}).get("done") for w in live):
                break
            if not live:
                break
            if time.time() - last_print > 30:
                last_print = time.time(); print(f"{rid}: " + ", ".join(f"{w}@{(j.get('clock', 0) - t0) / 3600:.2f}h/{j.get('decisions', 0)}" for w, j in sorted(state.items())), flush=True)
    finally:
        for w, pr in procs.items():
            if pr.poll() is None:
                pr.send_signal(signal.SIGTERM)
        for pr in procs.values():
            try: pr.wait(timeout=30)
            except subprocess.TimeoutExpired: pr.kill()
        for f in files.values(): f.close()
        c.close()
    wall = time.time() - wall_start
    summaries, events = {}, []
    for w in procs:
        for line in open(logs / f"{rid}_{w}.log"):
            if line.startswith("SUMMARY "):
                summaries[w] = json.loads(line[8:]); events += [{**e, "worker": w} for e in summaries[w].pop("events")]
            elif line.startswith("EVENT "):
                pass
    recs = [json.loads(v) for _, _, _, v in read_topic(cfg["kafka"], f"assignments-{rid}")]
    df = pl.DataFrame(recs, infer_schema_length=None)
    Path(f"data/{out_dir}").mkdir(exist_ok=True, parents=True)
    df.write_parquet(f"data/{out_dir}/svc_{rid}.parquet")
    first = df.filter(~pl.col("redelivered")) if df.height else df
    s = {"run_id": rid, "day": day, "workers": n_workers, "seed": seed, "key_resolution": key_res, "assignor": assignor, "speed": speed or "max", "pace_from": pace_from, "until": until, "window_s": window,
         "watermark_slack_s": cfg["watermark_slack_s"] if slack is None else slack, "session_timeout_ms": cfg["session_timeout_ms"], "fleet_size": cfg["fleet_size"], "fleet_by_partition": fleet_by_part,
         "hardware": hardware(), "jvm": subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()[0],
         "wall_s": round(wall, 1), "records": df.height, "requests_decided": int(first["request_id"].n_unique()) if df.height else 0,
         "redelivered_records": int(df["redelivered"].sum()) if df.height else 0,
         "decisions_per_s_wall": round(first["request_id"].n_unique() / wall, 1) if df.height else 0,
         "kill": kill, "events": sorted(events, key=lambda e: e["wall_ms"]), "workers_summary": summaries}
    if df.height:
        span = (first["done_wall_ms"].max() - first["start_wall_ms"].min()) / 1000
        s["decisions_per_s_active"] = round(first.height / span, 1) if span > 0 else None
        s["decision_ms"] = {"median": float(first["decision_ms"].median()), "p99": float(first["decision_ms"].quantile(0.99))}
        if speed:
            lat = (first.filter(pl.col("t") >= hhmm_to_t(t0, pace_from) if pace_from else pl.lit(True))["done_wall_ms"] - first.filter(pl.col("t") >= hhmm_to_t(t0, pace_from) if pace_from else pl.lit(True))["due_wall_ms"])
            s["match_latency_ms_at_speed"] = {"median": float(lat.median()), "p99": float(lat.quantile(0.99)), "max": float(lat.max()), "n": lat.len()}
        s["counts"] = first.group_by("status").len().to_dict(as_series=False)
        s["delay_s"] = {"median": float(first["delay_s"].median()), "p99": float(first["delay_s"].quantile(0.99)), "max": float(first["delay_s"].max()), "over_30s": int((first["delay_s"] > 30).sum())}
    Path(f"results/{out_dir}").mkdir(exist_ok=True, parents=True)
    Path(f"results/{out_dir}/svc_{rid}_{cfg['hardware_lane']}.json").write_text(json.dumps(s, indent=1))
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config"); ap.add_argument("--day", required=True); ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--seed", type=int, default=0); ap.add_argument("--key-res", type=int); ap.add_argument("--assignor", default="range", choices=("range", "cooperative"))
    ap.add_argument("--speed", type=float); ap.add_argument("--pace-from"); ap.add_argument("--until"); ap.add_argument("--kill-at"); ap.add_argument("--run-id"); ap.add_argument("--slack", type=float)
    ap.add_argument("--window", type=float, default=0, help="Phase 4: the worker's batching window in seconds (0 = one decision per request)")
    a = ap.parse_args()
    cfg = yaml.safe_load(open(a.config))
    key_res = a.key_res or cfg["key_resolution"]
    rid = a.run_id or f"svc_{a.day}_W{a.workers}_k{key_res}_{a.assignor}_seed{a.seed}" + (f"_x{a.speed:g}" if a.speed else "") + (f"_until{a.until.replace(':', '')}" if a.until else "") + (f"_kill{a.kill_at.replace(':', '')}" if a.kill_at else "") + (f"_w{a.window:g}" if a.window else "")
    s = run(cfg, a.day, a.workers, a.seed, key_res, a.assignor, a.speed, a.pace_from, a.until, a.kill_at, rid, a.slack, a.window)
    print(json.dumps({k: s[k] for k in s if k not in ("events", "workers_summary")}, indent=1))
    for w, ws in sorted(s["workers_summary"].items()):
        print(w, {k: ws[k] for k in ("decisions", "matched", "redelivered", "cruises", "folds", "fold_ms_total", "decision_ms_median", "table_ms_median", "wall_s")})

if __name__ == "__main__":
    main()
