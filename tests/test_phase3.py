"""Phase 3 tests: the partition function the workers compute matches the broker's, the tables cover every cell a driver can
occupy, the Mode A shard columns are consistent, every service ledger keeps the one-claim rule and never re-decides a
redelivered request, and the Java unit tests pass."""
import glob, json, pathlib, re, shutil, subprocess, zlib
import polars as pl, pytest
from deadhead.phase3_data import partition_of

ROOT = pathlib.Path(__file__).resolve().parents[1]
SVC = sorted(glob.glob(str(ROOT / "data/phase3/svc_svc_*.parquet")))
REF = sorted(glob.glob(str(ROOT / "data/phase3/assignments_ref_*.parquet")))

def kafka_up():
    try:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=3); return True
    except Exception:
        return False

def test_partition_function_is_crc32_modulo_count():
    assert partition_of("862a100dfffffff", 8) == zlib.crc32(b"862a100dfffffff") % 8
    assert {partition_of(f"{i:x}", 8) for i in range(500)} == set(range(8))

@pytest.mark.skipif(not kafka_up(), reason="Kafka on :9092 is needed")
def test_partition_function_matches_the_broker():
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "test-partitions", "auto.offset.reset": "earliest", "enable.auto.commit": False})
    topic = "requests-2016-06-16"
    c.assign([TopicPartition(topic, p, 0) for p in range(8)])
    seen = 0
    for m in c.consume(num_messages=4000, timeout=10):
        if m.key() != b"eod":
            assert partition_of(m.key().decode(), 8) == m.partition(); seen += 1
    c.close()
    assert seen > 1000

@pytest.mark.skipif(not (ROOT / "data/phase3/cells_2016-06-16.json").exists(), reason="phase 3 exports not generated")
def test_tables_cover_every_initial_driver_cell_and_every_cruise_target():
    cells = json.loads((ROOT / "data/phase3/cells_2016-06-16.json").read_text())["cells"]
    fleet = json.loads((ROOT / "data/phase3/fleet_2016-06-16_N7500_seed0.json").read_text())["drivers"]
    assert len(fleet) == 7500 and all(d[3] in cells for d in fleet)
    demand = json.loads((ROOT / "data/phase3/demand_2016-06-16.json").read_text())["hours"]
    assert set(demand) == {str(h) for h in range(24)}
    assert all(c in cells for h in demand.values() for c in h)
    c = next(iter(cells.values()))
    assert len(c["d3"]) == 37 and len(c["d5"]) == 91 and c["d3"] == sorted(c["d3"], key=lambda x: int(x, 16))

@pytest.mark.skipif(not REF, reason="no reference ledgers")
def test_mode_a_shard_columns_are_consistent():
    df = pl.read_parquet(REF[0]).filter(pl.col("n_candidates") > 0)
    for r in (5, 6, 7, 8):
        prev = None
        for n in (2, 4, 8):
            x, h = df[f"s{r}n{n}_x"], df[f"h{r}n{n}_x"]
            assert (x.drop_nulls() >= 0).all() and (h.drop_nulls() >= 0).all()
            # the halo sees a superset: never a worse pick, never fewer visible candidates
            both = df.filter(pl.col(f"s{r}n{n}_x").is_not_null())
            assert (both[f"h{r}n{n}_x"] <= both[f"s{r}n{n}_x"]).all() and (df[f"h{r}n{n}_v"] >= df[f"s{r}n{n}_v"]).all()
            # contiguous ranges nest as the worker count doubles: more workers never see more
            if prev is not None:
                assert (df[f"s{r}n{n}_v"] <= df[f"s{r}n{prev}_v"]).all()
            prev = n
    # one worker over every partition would see everything: the own-region column at N=8 with resolution 5 keys is the coarsest real shard
    assert float(df["s6n8_v"].mean()) <= float(df["s6n2_v"].mean())

@pytest.mark.skipif(not SVC, reason="no service ledgers")
@pytest.mark.parametrize("path", SVC)
def test_service_ledger_one_claim_and_idempotent_redelivery(path):
    df = pl.read_parquet(path)
    first = df.filter(~pl.col("redelivered"))
    assert first["request_id"].n_unique() == first.height, "a request was decided twice"
    re_ = df.filter(pl.col("redelivered"))
    if re_.height:
        j = re_.select("request_id", pl.col("driver").alias("d2"), pl.col("status").alias("s2")).join(first.select("request_id", "driver", "status"), on="request_id")
        assert j.height == re_.height and (j["driver"] == j["d2"]).all() and (j["status"] == j["s2"]).all(), "a redelivered request was re-decided"
    m = first.filter(pl.col("status") == "matched").sort("driver", "now")
    overlap = m.filter((pl.col("driver") == pl.col("driver").shift(1)) & (pl.col("now") < pl.col("busy_until").shift(1)))
    assert overlap.height == 0, f"{overlap.height} double claims in {path}"
    assert (m["actual_eta_s"] >= 0).all() and (m["leg_m"] >= 0).all() and (m["actual_eta_s"] <= 600).all()

@pytest.mark.skipif(shutil.which("gradle") is None, reason="gradle is needed for the Java unit tests")
def test_java_unit_tests_pass():
    r = subprocess.run(["gradle", "--offline", "test", "-q"], cwd=ROOT / "consumer", capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
