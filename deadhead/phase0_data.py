"""Phase 0: verify the trip data by hand. Counts rows, checks columns, measures junk, times loaders.

Input: the per-day Parquet files pulled by fetch_opendata.py (NYC Open Data uacg-pexx, 2016, with coordinates).
Usage: python -m deadhead.phase0_data data/tlc/opendata results/phase0
"""
import json, sys, time, platform, subprocess, glob
from pathlib import Path

BBOX = dict(lon_min=-74.27, lat_min=40.49, lon_max=-73.68, lat_max=40.93)  # same box as the OSM clip

def hardware():
    cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
    return {"cpu": cpu, "os": platform.platform(), "python": platform.python_version()}

def time_load(loader):
    t = time.perf_counter(); n = loader(); return n, round(time.perf_counter() - t, 3)

def main(indir, outdir):
    import polars as pl, pandas as pd, pyarrow.parquet as pq
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    files = sorted(glob.glob(f"{indir}/yellow_*.parquet"))
    columns = {f.name: str(f.type) for f in pq.read_schema(files[0])}
    rows_meta = sum(pq.read_metadata(f).num_rows for f in files)
    n_polars, t_polars = time_load(lambda: pl.read_parquet(files).height)
    n_pandas, t_pandas = time_load(lambda: sum(len(pd.read_parquet(f)) for f in files))
    df = pl.read_parquet(files)
    assert df.height == rows_meta == n_polars == n_pandas
    lon_pu, lat_pu, lon_do, lat_do = "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"
    pu_dt, do_dt = "tpep_pickup_datetime", "tpep_dropoff_datetime"
    has_coords = all(c in columns for c in (lon_pu, lat_pu, lon_do, lat_do))

    def inbox(lon, lat):
        return (pl.col(lon) >= BBOX["lon_min"]) & (pl.col(lon) <= BBOX["lon_max"]) & (pl.col(lat) >= BBOX["lat_min"]) & (pl.col(lat) <= BBOX["lat_max"])
    null_coord = df.filter(pl.any_horizontal(pl.col(c).is_null() for c in (lon_pu, lat_pu, lon_do, lat_do))).height
    zero_pu = df.filter((pl.col(lon_pu) == 0) | (pl.col(lat_pu) == 0)).height
    zero_do = df.filter((pl.col(lon_do) == 0) | (pl.col(lat_do) == 0)).height
    out_pu = df.filter(~inbox(lon_pu, lat_pu).fill_null(False)).height
    out_do = df.filter(~inbox(lon_do, lat_do).fill_null(False)).height
    dur = (pl.col(do_dt) - pl.col(pu_dt)).dt.total_seconds()
    bad_dur = df.filter((dur <= 0) | (dur > 6 * 3600)).height
    bad_dist = df.filter((pl.col("trip_distance") <= 0) | (pl.col("trip_distance") > 100)).height
    newark = df.filter(pl.col("ratecodeid") == 3).height
    clean = df.filter(inbox(lon_pu, lat_pu) & inbox(lon_do, lat_do) & (dur > 0) & (dur <= 6 * 3600) & (pl.col("trip_distance") > 0) & (pl.col("trip_distance") <= 100))
    per_day = clean.with_columns(pl.col(pu_dt).dt.date().alias("d")).group_by("d").agg(pl.len().alias("rows"), pl.col(pu_dt).dt.weekday().first().alias("weekday")).sort("d")
    days = {str(r["d"]): {"rows": r["rows"], "weekday": r["weekday"]} for r in per_day.iter_rows(named=True)}
    hourly = clean.with_columns(pl.col(pu_dt).dt.truncate("1h").alias("h")).group_by("h").len().sort("len", descending=True)
    peak = hourly.row(0)
    # observed speed by hour, the Phase 5 calibration source: odometer miles / duration
    spd = (clean.with_columns(((pl.col("trip_distance") * 1609.344) / dur).alias("mps"), pl.col(pu_dt).dt.hour().alias("hour"))
           .filter(pl.col("mps").is_between(0.5, 40)).group_by("hour").agg(pl.col("mps").median().alias("median_mps")).sort("hour"))
    out = {
        "source": {"dataset": "NYC Open Data uacg-pexx, 2016 Yellow Taxi Trip Data", "files": [Path(f).name for f in files],
                    "pulled": json.load(open(f"{indir}/fetch_log.json"))["pulled"] if Path(f"{indir}/fetch_log.json").exists() else None},
        "hardware": hardware(), "columns": columns, "has_coordinates": has_coords,
        "rows": rows_meta,
        "load_seconds": {"polars": t_polars, "pandas": t_pandas, "polars_version": pl.__version__, "pandas_version": pd.__version__},
        "junk": {"null_coord": null_coord, "pickup_zero_coord": zero_pu, "dropoff_zero_coord": zero_do, "pickup_outside_bbox": out_pu,
                  "dropoff_outside_bbox": out_do, "bad_duration": bad_dur, "bad_distance": bad_dist, "newark_ratecode_3": newark},
        "rows_clean": clean.height, "clean_share": round(clean.height / df.height, 4), "bbox": BBOX,
        "clean_rows_per_day": days,
        "peak_hour": {"hour": str(peak[0]), "requests": peak[1], "per_second": round(peak[1] / 3600, 2)},
        "mean_requests_per_second": round(clean.height / (len(files) * 86400), 2),
        "median_speed_mps_by_hour": {int(r["hour"]): round(r["median_mps"], 2) for r in spd.iter_rows(named=True)},
    }
    (outdir / "data_check_cpu-apple-m5_seed0.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps({k: out[k] for k in ("has_coordinates", "rows", "load_seconds", "junk", "rows_clean", "clean_share", "clean_rows_per_day", "peak_hour", "mean_requests_per_second")}, indent=2, default=str))

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
