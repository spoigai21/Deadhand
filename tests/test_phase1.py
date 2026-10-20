"""Phase 1 tests: distance sanity, spatial sanity, and the results ledger, over the raw pairs and the results file."""
import glob, json, pathlib
import numpy as np, polars as pl, pytest, yaml
from deadhead.geo import haversine_m, cells, disk

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = sorted(glob.glob(str(ROOT / "results/phase1/disagreement_cpu-apple-m5_seed*.json")))
PAIRS = sorted(glob.glob(str(ROOT / "data/phase1/pairs_*.parquet")))
CFG = yaml.safe_load(open(ROOT / "configs/phase1.yaml"))

def test_haversine_known_distance():
    # Empire State Building to Grand Central Terminal is about 860 m in a straight line
    assert 800 < haversine_m(-73.9857, 40.7484, -73.9772, 40.7527) < 950
    assert haversine_m(-74.0, 40.7, -74.0, 40.7) == 0

@pytest.mark.skipif(not PAIRS, reason="Phase 1 raw pairs not generated (data/phase1 is not committed; run the phase)")
@pytest.mark.parametrize("path", PAIRS)
def test_distance_sanity_haversine_never_exceeds_road(path):
    df = pl.read_parquet(path)
    assert (df["road_s"] >= 0).all() and (df["road_m"] >= 0).all()
    # road distance is between snapped points; allow the two snap distances as slack
    slack = df["snap_a_m"] + df["snap_b_m"] + 1.0
    assert (df["road_m"] + slack >= df["hav_m"]).all(), f"road shorter than straight line in {path}"

@pytest.mark.skipif(not PAIRS, reason="Phase 1 raw pairs not generated")
@pytest.mark.parametrize("path", [p for p in PAIRS if "_legs" in p])
def test_spatial_sanity_candidates_inside_ring_and_snap_bound(path):
    df = pl.read_parquet(path)
    assert (df["snap_a_m"] <= CFG["max_snap_m"]).all() and (df["snap_b_m"] <= CFG["max_snap_m"]).all()
    assert (df["hav_m"] >= CFG["min_straight_line_m"]).all()
    # every candidate's cell is within ring_k of the pickup cell
    sample = df.sample(min(2000, df.height), seed=0)
    do_cells = cells(sample["do_lon"].to_numpy(), sample["do_lat"].to_numpy(), CFG["h3_resolution"])
    for pu_cell, do_cell in zip(sample["pu_cell"].to_numpy(), do_cells):
        assert int(do_cell) in disk(int(pu_cell), CFG["ring_k"])
    # and no request was offered more than the stated cap
    assert df.group_by("request_id").len()["len"].max() <= CFG["candidates_k"]

@pytest.mark.skipif(not RESULTS, reason="Phase 1 results missing")
def test_results_file_is_complete_and_named_crossings_appear():
    d = json.load(open(RESULTS[0]))
    assert d["config"]["seed"] == 0 and "hardware" in d
    for day in d["config"]["days"]:
        for kind in ("trips", "legs"):
            s = d["days"][day][kind]
            assert s["distance_ratio"]["n"] > 1000 and s["distance_ratio"]["median"] >= 1.0
            assert s["time_ratio"]["median"] > 0 and len(s["by_cell"]) > 50 and len(s["worst_by_ratio"]) == d["config"]["n_worst"]
        assert len(d["days"][day]["legs_sensitivity"]) == len(d["config"]["idle_fraction_sensitivity"])
    named = " | ".join(n for day in d["config"]["days"] for n in d["days"][day]["trips"]["most_used_crossings"])
    for expected in ("Queensboro Bridge", "Queens-Midtown Tunnel", "Williamsburg Bridge", "Brooklyn Bridge"):
        assert expected in named, f"{expected} not among the most-used crossings: {named}"

@pytest.mark.skipif(not RESULTS, reason="Phase 1 results missing")
def test_ledger_every_phase1_number_has_a_results_file():
    numbers = (ROOT / "NUMBERS.md").read_text()
    for f in set(__import__("re").findall(r"`results/phase1/[^`]+`", numbers)):
        assert (ROOT / f.strip("`")).exists(), f"{f} named in NUMBERS.md but missing"
