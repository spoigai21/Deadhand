"""Straight-line distance and H3 helpers. One definition of haversine for the whole project."""
import math
import numpy as np
import h3

EARTH_R = 6371008.8

def haversine_m(lon1, lat1, lon2, lat2):
    """Vectorised great-circle distance in metres. Accepts scalars or numpy arrays."""
    lon1, lat1, lon2, lat2 = (np.radians(np.asarray(x, dtype=float)) for x in (lon1, lat1, lon2, lat2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_R * np.arcsin(np.sqrt(a))

def cells(lons, lats, res):
    """H3 cell (as int) for each point."""
    return np.fromiter((h3.str_to_int(h3.latlng_to_cell(float(la), float(lo), res)) for lo, la in zip(lons, lats)), dtype=np.uint64, count=len(lons))

def disk(cell_int, k):
    return {h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(int(cell_int)), k)}

def boundary_lonlat(cell_int):
    return [(lng, lat) for lat, lng in h3.cell_to_boundary(h3.int_to_str(int(cell_int)))]
