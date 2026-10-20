"""Phase 0: benchmark the routing layer. Single-route latency and table latency at K x K.

Points are drawn from real clean pickups in the trip file so the benchmark routes where demand is.
Usage: python -m deadhead.phase0_osrm <trips.csv|.parquet with pickup_longitude/latitude> results/phase0 [http://localhost:5001]
"""
import json, sys, time, statistics, random
from pathlib import Path
import requests, polars as pl

def sample_points(parquet, n, seed=0):
    reader = pl.read_csv if str(parquet).endswith(".csv") else pl.read_parquet
    df = reader(parquet, columns=["pickup_longitude", "pickup_latitude"])
    df = df.filter((pl.col("pickup_longitude").is_between(-74.27, -73.68)) & (pl.col("pickup_latitude").is_between(40.49, 40.93)))
    return df.sample(n, seed=seed).rows()

def coords(pts):
    return ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in pts)

def bench_route(base, pts, n=500):
    lat = []
    for i in range(n):
        a, b = pts[2 * i], pts[2 * i + 1]
        t = time.perf_counter()
        r = requests.get(f"{base}/route/v1/driving/{coords([a, b])}", params={"overview": "false"}).json()
        lat.append((time.perf_counter() - t) * 1000)
        assert r["code"] == "Ok", r
    return {"n": n, "median_ms": round(statistics.median(lat), 2), "p90_ms": round(sorted(lat)[int(0.9 * n)], 2), "max_ms": round(max(lat), 2)}

def bench_table(base, pts, k, reps=5):
    lat = []
    for rep in range(reps):
        src = pts[rep * k:(rep + 1) * k]
        t = time.perf_counter()
        r = requests.get(f"{base}/table/v1/driving/{coords(src)}", params={"annotations": "duration,distance"}).json()
        lat.append((time.perf_counter() - t) * 1000)
        assert r["code"] == "Ok", r.get("message")
        nulls = sum(1 for row in r["durations"] for v in row if v is None)
    return {"k": k, "cells": k * k, "reps": reps, "median_ms": round(statistics.median(lat), 1), "max_ms": round(max(lat), 1),
            "us_per_cell": round(1000 * statistics.median(lat) / (k * k), 2), "unroutable_cells_last_rep": nulls}

def main(parquet, outdir, base="http://localhost:5001"):
    random.seed(0)
    pts = sample_points(parquet, 6000)
    # warm up
    requests.get(f"{base}/route/v1/driving/{coords(pts[:2])}")
    out = {"osrm_base": base, "route": bench_route(base, pts), "table": [bench_table(base, pts, k) for k in (10, 50, 200, 1000)]}
    # how far does OSRM move a real pickup when snapping it to the road graph
    snaps = []
    for lon, lat in pts[:1000]:
        r = requests.get(f"{base}/nearest/v1/driving/{lon:.6f},{lat:.6f}").json()
        snaps.append(r["waypoints"][0]["distance"] if r["code"] == "Ok" else None)
    ok = [s for s in snaps if s is not None]
    out["snap_m"] = {"n": len(snaps), "unsnappable": len(snaps) - len(ok), "median": round(statistics.median(ok), 1),
                     "p90": round(sorted(ok)[int(0.9 * len(ok))], 1), "over_100m": sum(1 for s in ok if s > 100)}
    Path(outdir).mkdir(parents=True, exist_ok=True)
    (Path(outdir) / "osrm_bench_cpu-apple-m5_seed0.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main(*sys.argv[1:])
