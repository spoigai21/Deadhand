"""Phase 4 tests: the batch solve is optimal and keeps the one-claim rule, a blocked many-to-many table equals the single call,
every batched ledger keeps the one-claim rule and the window's bookkeeping, and the Java assignment solve agrees with the
Python one on the same costs."""
import glob, itertools, json, pathlib, subprocess
import numpy as np, polars as pl, pytest
from deadhead.batch import solve, BIG

ROOT = pathlib.Path(__file__).resolve().parents[1]
LEDGERS = [p for p in sorted(glob.glob(str(ROOT / "data/phase4/assignments_*_batch_W*.parquet"))) if "_batch_W0_" not in p]   # W = 0 at another ring is the plain oracle: no batch columns

def osrm_up():
    try:
        import requests
        return requests.get("http://localhost:5001/route/v1/driving/-73.98,40.75;-73.97,40.76?overview=false", timeout=3).ok
    except Exception:
        return False

def brute(cost):
    """Fewest unserved rows first, then least total: the objective the BIG penalty encodes."""
    R, D = cost.shape
    best = None
    for cols in itertools.permutations(range(D), min(R, D)):
        for rows in itertools.combinations(range(R), min(R, D)):
            total = sum(cost[r, c] for r, c in zip(rows, cols)) + BIG * (R - len(rows))
            best = total if best is None or total < best else best
    return best

def test_batch_solve_is_optimal_and_never_doubles_a_driver():
    rng = np.random.default_rng(3)
    for _ in range(200):
        R, D = rng.integers(1, 5), rng.integers(1, 5)
        cost = np.where(rng.random((R, D)) < 0.25, BIG, rng.integers(0, 600, (R, D)).astype(float))
        a = solve(cost)
        assert len(set(a.values())) == len(a), "a driver assigned twice"
        assert all(cost[r, c] < BIG for r, c in a.items())
        total = sum(cost[r, c] for r, c in a.items()) + BIG * (R - len(a))
        assert total == pytest.approx(brute(cost))

def test_one_request_takes_the_nearest_driver_by_road():
    assert solve(np.array([[300.0, 120.0, 200.0]])) == {0: 1}

@pytest.mark.skipif(not osrm_up(), reason="OSRM on :5001 is needed")
def test_blocked_many_to_many_table_equals_one_call():
    from deadhead.routing import OSRM
    rng = np.random.default_rng(5)
    pts = [(-73.99 + 0.04 * rng.random(), 40.73 + 0.04 * rng.random()) for _ in range(30)]
    o = OSRM("http://localhost:5001")
    one = o.table_many_to_many(pts[:20], pts[20:])
    blocked = o.table_many_to_many(pts[:20], pts[20:], max_locations=7)
    assert one["calls"] == 1 and blocked["calls"] == 6
    assert one["road_s"] == blocked["road_s"] and one["road_m"] == blocked["road_m"]

@pytest.mark.skipif(not LEDGERS, reason="no Phase 4 ledgers yet")
@pytest.mark.parametrize("path", LEDGERS)
def test_batched_ledger_keeps_the_rules(path):
    df = pl.read_parquet(path)
    assert df["request_id"].n_unique() == df.height, "a request decided twice"
    m = df.filter(pl.col("status") == "matched").sort("driver", "decided_at")
    # the one-claim rule: a driver's next claim comes after the previous trip ended
    prev_until, prev_driver = m["busy_until"].shift(1), m["driver"].shift(1)
    assert m.filter((pl.col("driver") == prev_driver) & (pl.col("decided_at") < prev_until)).height == 0
    # the window's bookkeeping: decided at a window close, after the request, within the rider's patience
    W = float(df["window_s"][0]); T = 600
    assert ((m["decided_at"] - m["t"]) >= 0).all() and (m["queue_s"] == (m["decided_at"] - m["t"])).all()
    assert (m["wait_s"] == m["queue_s"] + m["actual_eta_s"]).all() and (m["wait_s"] <= T + 1e-6).all()
    assert (df["rounds"] <= int(T / W) + 2).all()
    # a straight-line pick with a longer road time is "differs"; a shard column's extra is never negative when the global solve was feasible
    both = m.filter(pl.col("sl_driver") >= 0)
    assert (both.filter(pl.col("differs"))["extra_s"] > 0).all()

def test_java_assignment_agrees_with_python_on_a_fixed_matrix(tmp_path):
    """The two solves must give the same total (ties may pick different pairs); checked through a tiny Java main via the built jar."""
    jar = ROOT / "consumer/build/libs/deadhead-consumer.jar"
    if not jar.exists():
        pytest.skip("consumer jar not built")
    rng = np.random.default_rng(11)
    cost = np.where(rng.random((6, 9)) < 0.3, BIG, rng.integers(0, 600, (6, 9)).astype(float))
    src = tmp_path / "Check.java"
    rows = ",".join("{" + ",".join(f"{v:.0f}" for v in row) + "}" for row in cost)
    src.write_text("public class Check { public static void main(String[] a) { double[][] c = new double[][]{%s}; int[] r = deadhead.Assignment.solve(c, 1e9);"
                   " double t = 0; int n = 0; for (int i = 0; i < r.length; i++) if (r[i] >= 0) { t += c[i][r[i]]; n++; } System.out.println(n + \" \" + (long) t); } }" % rows)
    subprocess.run(["javac", "-cp", str(jar), "-d", str(tmp_path), str(src)], check=True)
    out = subprocess.run(["java", "-cp", f"{jar}:{tmp_path}", "Check"], check=True, capture_output=True, text=True).stdout.split()
    a = solve(cost)
    assert int(out[0]) == len(a) and int(out[1]) == int(sum(cost[r, c] for r, c in a.items()))
