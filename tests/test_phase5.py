"""Phase 5 tests: the calibration file is sane, the hourly multiplier scales a decision without changing its pick, and every
Phase 5 ledger keeps the radius columns' invariants (the model's own ring is the zero line; narrower rings never gain,
wider rings never lose)."""
import glob, json, pathlib
import numpy as np, polars as pl, pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MULT = ROOT / "data/phase5/speed_multipliers.json"
LEDGERS = sorted(glob.glob(str(ROOT / "data/phase5/assignments_cal_*.parquet"))) + sorted(glob.glob(str(ROOT / "data/phase5/assignments_ff_*.parquet")))

@pytest.mark.skipif(not MULT.exists(), reason="calibration not computed")
def test_multipliers_cover_every_hour_and_are_plausible():
    cal = json.load(open(MULT))
    hourly = [cal["hourly"][str(h)] for h in range(24)]
    assert all(1.0 <= m <= 4.0 for m in hourly), hourly
    assert max(hourly) > min(hourly) * 1.1, "the day has no shape"
    assert len(cal["zone_hour"]) > 100

def test_a_uniform_multiplier_keeps_the_pick():
    from deadhead.oracle import rank
    rng = np.random.default_rng(1)
    for _ in range(100):
        hav = np.sort(rng.random(20) * 2000); road = hav * (1 + rng.random(20) * 1.5) / 8
        m = 1 + rng.random() * 2
        assert rank("road", hav, road) == rank("road", hav, road * m) and rank("straight", hav, road) == rank("straight", hav, road * m)

@pytest.mark.skipif(not LEDGERS, reason="no Phase 5 ledgers yet")
@pytest.mark.parametrize("path", LEDGERS)
def test_radius_columns_keep_their_invariants(path):
    df = pl.read_parquet(path)
    m = df.filter(pl.col("status") == "matched")
    assert (m["ring3_x"].fill_null(0).abs() < 1e-9).all(), "the model's own ring must be the zero line"
    for r in (1, 2):
        assert (m[f"ring{r}_x"].fill_null(0) >= -1e-9).all(), f"ring {r} cannot beat ring 3"
        assert (m[f"ring{r}_v"] <= m["ring3_v"]).all()
    for r in (4, 5):
        assert (m[f"ring{r}_x"].fill_null(0) <= 1e-9).all(), f"ring {r} cannot lose to ring 3"
        assert (m[f"ring{r}_v"] >= m["ring3_v"]).all()
    # the road rule promises what it delivers; abandonment is exactly a promise over T
    assert (m["promised_eta_s"] == m["actual_eta_s"]).all()
    ab = df.filter(pl.col("status") == "abandoned")
    assert (ab["promised_eta_s"] > 600).all() and (m["actual_eta_s"] <= 600).all()
    if "speed_multiplier" in df.columns:
        assert df["speed_multiplier"].n_unique() <= 24 and (df["speed_multiplier"] >= 1.0).all()
