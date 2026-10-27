"""Phase 6 tests: the resource monitor's parsing and arithmetic, the price is stated with its date, and the cost file,
when it exists, is consistent with the runs it was computed from."""
import json, pathlib
import pytest, yaml
from deadhead.resources import ResourceMonitor, _cputime
from deadhead.phase6_metrics import worker_profile, cost

ROOT = pathlib.Path(__file__).resolve().parents[1]
COST = sorted((ROOT / "results/phase6").glob("cost_*.json"))
PACED = sorted((ROOT / "data/phase6").glob("svc_cost_*_x1_*.parquet"))

def test_cputime_formats():
    assert _cputime("0:00.01") == pytest.approx(0.01)
    assert _cputime("12:34.56") == pytest.approx(754.56)
    assert _cputime("1:02:03") == pytest.approx(3723)
    assert _cputime("2-01:00:00") == pytest.approx(2 * 86400 + 3600)

def test_summary_arithmetic():
    m = ResourceMonitor({"w0": 1}, 2); m.ncpu = 10
    m.samples = [{"t": 0, "jvm_w0_cpu": 50.0, "machine_busy_pct": 20.0}, {"t": 2, "jvm_w0_cpu": 150.0, "machine_busy_pct": 40.0}]
    m.cpu_seconds = {"w0": 99.0}
    s = m.summary(100)
    assert s["series"]["jvm_w0_cpu"]["mean"] == 100 and s["series"]["jvm_w0_cpu"]["core_seconds"] == 100      # a core for 100 s
    assert s["series"]["machine_busy_pct"]["core_seconds"] == 300                                               # 30% of 10 cores for 100 s
    assert s["series"]["jvm_w0_cpu"]["p95"] == 150 and s["series"]["jvm_w0_cpu"]["max"] == 150
    assert s["jvm_cpu_seconds"] == {"w0": 99.0}

def test_worker_profile_shares():
    s = {"workers_summary": {"w0": {"profile": {"process": "10 calls, 100.0 s", "osrm_table": "10 calls, 56.0 s", "await_ack": "10 calls, 18.0 s"}},
                             "w1": {"profile": {"process": "10 calls, 100.0 s", "osrm_table": "10 calls, 44.0 s"}}}}
    p = worker_profile(s)
    assert p["process_s"] == 200 and p["shares"]["osrm_table"] == 0.5 and p["shares"]["await_ack"] == 0.09
    assert abs(sum(p["shares"].values()) - 1) < 1e-6

def test_price_is_stated_with_its_date():
    cfg = yaml.safe_load(open(ROOT / "configs/phase6.yaml"))
    p = cfg["price"]
    assert p["usd_per_hour"] > 0 and p["read_on"] and p["instance"] and p["source"]
    assert cost(3600, p)["usd"] == pytest.approx(p["usd_per_hour"])

@pytest.mark.skipif(not COST, reason="Phase 6 not aggregated yet")
def test_cost_file_is_consistent():
    r = json.load(open(COST[-1]))
    if "cost_per_city_day" not in r:
        pytest.skip("the operating-point run is not in yet")
    c = r["cost_per_city_day"]; p = r["price"]
    assert c["usd"] == pytest.approx(c["wall_s"] / 3600 * p["usd_per_hour"], rel=1e-3)
    assert r["at_x1"]["cost_per_city_day"]["usd"] == pytest.approx(24 * p["usd_per_hour"], rel=1e-3)
    m = r["bottleneck"]["machine"]
    assert 0 < m["cores_busy_mean"] <= m["ncpu"]
    assert abs(sum(m["shares_of_accounted"].values()) - 1) < 1e-3
    assert abs(sum(r["bottleneck"]["worker_profile"]["shares"].values()) - 1) < 1e-3
    for row in r["by_fleet"] + r["by_ring"]:
        assert row["cost_per_city_day"]["usd"] == pytest.approx(row["wall_s"] / 3600 * p["usd_per_hour"], rel=1e-3)
    windows = r["at_x1"]["windows"]
    assert len(windows) == len({tuple(w["window"]) for w in windows}), "each paced window once"
    for w in windows:
        if w.get("paced_window"):
            assert w["paced_window"]["pace_origin_spread_s_across_workers"] < 1.0                    # BUGS.md #8
            assert w["paced_window"]["work_cores_mean"] <= w["paced_window"]["cores_busy_mean"] + 0.05

@pytest.mark.parametrize("ledger", PACED, ids=[p.stem[-14:] for p in PACED])
def test_paced_workers_share_one_origin(ledger):
    """BUGS.md #8: at a paced speed every worker must pace from the same wall-clock origin, or the group stalls at the
    watermark gate on the worker whose first paced request comes latest. The origin is recoverable from each decision's
    due time minus its event offset; across workers it must agree to well under the 30 s slack."""
    import polars as pl, re
    df = pl.read_parquet(ledger)
    hh, mm = re.search(r"until(\d\d)(\d\d)", ledger.name).groups()
    t0 = df["t"].min(); pace_from = t0 + (int(hh) * 3600 + int(mm) * 60) - 15 * 60       # the window is 15 min long
    w = df.filter(pl.col("t") >= pace_from).with_columns(origin=pl.col("due_wall_ms") - ((pl.col("t") - pace_from) * 1000).cast(pl.Int64))
    origins = w.group_by("worker").agg(pl.col("origin").min())["origin"]
    assert origins.max() - origins.min() < 1000, f"pace origins differ by {(origins.max() - origins.min()) / 1000:.1f} s across workers"
    lat = w["done_wall_ms"] - w["due_wall_ms"]
    assert lat.median() < 1000 and lat.max() < 30000, "a paced window should not stall at the gate"
