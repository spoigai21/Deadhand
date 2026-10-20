"""Phase 2 tests: determinism, same candidates for both rules, no double match, and the ledger."""
import glob, json, pathlib, re, subprocess, sys
import numpy as np, polars as pl, pytest, requests
from deadhead.fleet import Fleet
from deadhead.oracle import rank

ROOT = pathlib.Path(__file__).resolve().parents[1]
LEDGERS = sorted(p for p in glob.glob(str(ROOT / "data/phase2/assignments_*.parquet")) if re.search(r"assignments_\d{4}-", p))

def services_up():
    try:
        ok = requests.get("http://localhost:5001/route/v1/driving/-73.9857,40.7484;-73.9772,40.7527", timeout=2).json()["code"] == "Ok"
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=3)
        return ok
    except Exception:
        return False

def test_both_rules_rank_the_same_candidate_set():
    # candidates sorted by straight line; the road rule may pick any index, the straight-line rule the first
    hav = np.array([100.0, 150.0, 400.0]); road_s = np.array([300.0, 60.0, 90.0])
    assert rank("straight", hav, road_s) == 0 and rank("road", hav, road_s) == 1
    # ties go to the first (nearest by straight line, then lowest id), deterministically
    assert rank("road", hav, np.array([60.0, 60.0, 90.0])) == 0

def test_fleet_candidates_are_idle_inside_the_ring_and_one_claim_only():
    f = Fleet([-73.98, -73.981, -73.90], [40.75, 40.751, 40.70], res=8, ring_k=3)
    from deadhead.geo import cells
    pu_cell = cells([-73.98], [40.75], 8)[0]
    ids, hav = f.candidates(-73.98, 40.75, pu_cell, 50)
    assert list(ids) == [0, 1] and hav[0] <= hav[1]          # driver 2 is far outside the ring
    f.claim(0, busy_until=1000.0, do_lon=-73.97, do_lat=40.76)
    assert list(f.candidates(-73.98, 40.75, pu_cell, 50)[0]) == [1]
    with pytest.raises(AssertionError):
        f.claim(0, busy_until=2000.0, do_lon=-73.97, do_lat=40.76)   # already claimed
    f.release(1000.0)
    assert 0 in f.candidates(-73.97, 40.76, cells([-73.97], [40.76], 8)[0], 50)[0]

@pytest.mark.skipif(not services_up(), reason="OSRM on :5001 and Kafka on :9092 are needed")
def test_oracle_is_deterministic_on_a_slice():
    for rid in ("test_det_a", "test_det_b"):
        subprocess.run([sys.executable, "-m", "deadhead.oracle", "configs/phase2.yaml", "--day", "2016-06-18", "--rule", "road", "--limit", "400", "--run-id", rid], cwd=ROOT, check=True, capture_output=True)
    a = pl.read_parquet(ROOT / "data/phase2/assignments_test_det_a.parquet").drop("decision_ms")
    b = pl.read_parquet(ROOT / "data/phase2/assignments_test_det_b.parquet").drop("decision_ms")
    assert a.equals(b)

@pytest.mark.skipif(not LEDGERS, reason="no Phase 2 ledgers generated")
@pytest.mark.parametrize("path", LEDGERS)
def test_no_double_match(path):
    df = pl.read_parquet(path)
    assert df["request_id"].n_unique() == df.height, "a request_id appears twice"
    m = df.filter(pl.col("status") == "matched").sort("driver", "t")
    # a driver is never claimed while still busy from an earlier claim
    prev_until = m["busy_until"].shift(1); prev_driver = m["driver"].shift(1)
    overlap = m.filter((pl.col("driver") == prev_driver) & (pl.col("t") < prev_until))
    assert overlap.height == 0, f"{overlap.height} overlapping claims in {path}"
    assert (m["actual_eta_s"] >= 0).all() and (m["leg_m"] >= 0).all()
    # Mode A sanity: the straight-line pick never has a smaller road time than the road pick
    dec = df.filter(pl.col("n_candidates") > 0)
    assert (dec["extra_s"] >= 0).all()

@pytest.mark.skipif(not (ROOT / "results/phase2").exists(), reason="no Phase 2 results")
def test_ledger_every_phase2_number_has_a_results_file():
    numbers = (ROOT / "NUMBERS.md").read_text()
    for f in set(re.findall(r"`results/phase2/[^`]+`", numbers)):
        assert (ROOT / f.strip("`")).exists(), f"{f} named in NUMBERS.md but missing"
