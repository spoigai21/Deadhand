"""Pull 2016 yellow-taxi trips WITH coordinates from NYC Open Data (dataset uacg-pexx), one hour per request.

The TLC's own Parquet files were re-published without coordinates for every year (verified 2026-10-19);
NYC Open Data's copy of the 2016 file still carries pickup/dropoff longitude and latitude.
Usage: python -m deadhead.fetch_opendata 2016-06-13 2016-06-19 data/tlc/opendata
"""
import sys, time, io, json
from datetime import date, datetime, timedelta
from pathlib import Path
import requests, polars as pl

BASE = "https://data.cityofnewyork.us/resource/uacg-pexx.csv"
SELECT = "tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,ratecodeid,pulocationid,dolocationid"
SCHEMA = {"tpep_pickup_datetime": pl.Datetime, "tpep_dropoff_datetime": pl.Datetime, "passenger_count": pl.Int32, "trip_distance": pl.Float64,
          "pickup_longitude": pl.Float64, "pickup_latitude": pl.Float64, "dropoff_longitude": pl.Float64, "dropoff_latitude": pl.Float64,
          "ratecodeid": pl.Int32, "pulocationid": pl.Int32, "dolocationid": pl.Int32}

def fetch_hour(start: datetime):
    end = start + timedelta(hours=1) - timedelta(seconds=1)
    params = {"$select": SELECT, "$where": f"tpep_pickup_datetime between '{start:%Y-%m-%dT%H:%M:%S}' and '{end:%Y-%m-%dT%H:%M:%S}'", "$limit": 200000}
    for attempt in range(5):
        try:
            r = requests.get(BASE, params=params, timeout=600)
            r.raise_for_status()
            df = pl.read_csv(io.BytesIO(r.content), schema_overrides=SCHEMA, try_parse_dates=True)
            if df.height >= 200000:
                raise RuntimeError("hour hit the page limit; split it")
            return df
        except Exception as e:
            print(f"  retry {attempt} {start}: {e}", flush=True); time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"gave up on {start}")

def main(d0, d1, outdir):
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    log = {"dataset": "uacg-pexx", "url": BASE, "pulled": time.strftime("%Y-%m-%d"), "days": {}}
    d, end = date.fromisoformat(d0), date.fromisoformat(d1)
    while d <= end:
        out = outdir / f"yellow_{d}.parquet"
        if out.exists():
            d += timedelta(days=1); continue
        t0 = time.perf_counter(); parts = []
        for h in range(24):
            parts.append(fetch_hour(datetime(d.year, d.month, d.day, h)))
        df = pl.concat(parts)
        df.write_parquet(out)
        log["days"][str(d)] = {"rows": df.height, "seconds": round(time.perf_counter() - t0, 1)}
        print(f"{d}: {df.height} rows in {log['days'][str(d)]['seconds']} s", flush=True)
        d += timedelta(days=1)
    (outdir / "fetch_log.json").write_text(json.dumps(log, indent=2))

if __name__ == "__main__":
    main(*sys.argv[1:])
