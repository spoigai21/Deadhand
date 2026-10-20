"""Shared trip loading. The clean filter is the one Phase 0 counted; it is defined once here."""
import glob
from pathlib import Path
import polars as pl

BBOX = dict(lon_min=-74.27, lat_min=40.49, lon_max=-73.68, lat_max=40.93)  # same box as the OSM clip
PU_DT, DO_DT = "tpep_pickup_datetime", "tpep_dropoff_datetime"
COLS = [PU_DT, DO_DT, "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "trip_distance"]

def _inbox(lon, lat):
    return (pl.col(lon) >= BBOX["lon_min"]) & (pl.col(lon) <= BBOX["lon_max"]) & (pl.col(lat) >= BBOX["lat_min"]) & (pl.col(lat) <= BBOX["lat_max"])

def clean(df: pl.DataFrame) -> pl.DataFrame:
    dur = (pl.col(DO_DT) - pl.col(PU_DT)).dt.total_seconds()
    return (df.filter(_inbox("pickup_longitude", "pickup_latitude") & _inbox("dropoff_longitude", "dropoff_latitude")
                      & (dur > 0) & (dur <= 6 * 3600) & (pl.col("trip_distance") > 0) & (pl.col("trip_distance") <= 100))
              .with_columns(dur.alias("duration_s")))

def load_week(trips_dir) -> pl.DataFrame:
    files = sorted(glob.glob(f"{trips_dir}/yellow_*.parquet"))
    return clean(pl.read_parquet(files, columns=COLS)).sort(PU_DT).with_row_index("trip_id")

def day_requests(week: pl.DataFrame, day: str) -> pl.DataFrame:
    """Clean trips whose pickup falls on the given date."""
    return week.filter(pl.col(PU_DT).dt.date() == pl.lit(day).str.to_date())

def supply_pool(week: pl.DataFrame, day: str, window_s: int) -> pl.DataFrame:
    """Dropoffs that can serve as an idle position for some request on the day: from window_s before midnight to the end of the day."""
    start = pl.lit(day).str.to_datetime() - pl.duration(seconds=window_s)
    end = pl.lit(day).str.to_datetime() + pl.duration(days=1)
    return week.filter((pl.col(DO_DT) >= start) & (pl.col(DO_DT) < end)).sort(DO_DT)
