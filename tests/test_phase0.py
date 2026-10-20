"""Phase 0 sanity tests. OSRM tests skip when the server is not running."""
import json, math, pathlib
import pytest, requests

OSRM = "http://localhost:5001"
R = pathlib.Path(__file__).resolve().parents[1] / "results" / "phase0"

def osrm_up():
    try:
        return requests.get(f"{OSRM}/route/v1/driving/-73.9857,40.7484;-73.9772,40.7527", timeout=2).json()["code"] == "Ok"
    except Exception:
        return False

def haversine_m(lon1, lat1, lon2, lat2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371008.8 * math.asin(math.sqrt(a))

@pytest.mark.skipif(not osrm_up(), reason="OSRM not running on :5001")
@pytest.mark.parametrize("a,b", [
    ((-73.9857, 40.7484), (-73.9772, 40.7527)),   # Midtown, same side
    ((-73.9903, 40.7081), (-73.9925, 40.6950)),   # Lower Manhattan to Brooklyn across the East River
    ((-73.9442, 40.8116), (-73.9339, 40.8186)),   # Harlem to the Bronx across the Harlem River
])
def test_road_distance_never_below_straight_line(a, b):
    r = requests.get(f"{OSRM}/route/v1/driving/{a[0]},{a[1]};{b[0]},{b[1]}", params={"overview": "false"}).json()
    assert r["code"] == "Ok"
    road = r["routes"][0]["distance"]
    assert road >= 0.95 * haversine_m(*a, *b)   # 5% slack for the snap of both endpoints
    assert r["routes"][0]["duration"] > 0

def test_water_crossings_are_computed_and_named():
    d = json.loads((R / "water_crossings_cpu-apple-m5_seed0.json").read_text())
    assert d["water_crossing_ways"] > 0 and d["named_water_crossings"] > 50
    names = {c["name"] for c in d["crossings"]}
    for expected in ("Brooklyn Bridge", "Williamsburg Bridge", "Lincoln Tunnel", "Queens-Midtown Tunnel"):
        assert expected in names, f"{expected} not found by the crossing computation"

def test_routing_benchmark_recorded():
    d = json.loads((R / "osrm_bench_cpu-apple-m5_seed0.json").read_text())
    assert d["route"]["median_ms"] < 10 and d["snap_m"]["unsnappable"] == 0
