"""The demand as a Kafka stream, and the assignments ledger read back from it.

Producer: one topic per day, `requests-<day>`, keyed by the pickup's H3 resolution-6 parent cell so
a worker owns contiguous regions (SPEC.md Phase 3). Records are JSON, in event-time order. After the
last request the producer writes one end-of-day control record to every partition so a consumer can
tell an idle partition from an unfinished one.

Ledger: `read_topic` drains a topic from the beginning to its current end and returns the records.

Usage: python -m deadhead.stream configs/phase2.yaml 2016-06-16
"""
import json, sys, time
import h3, polars as pl, yaml
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from .data import load_week, day_requests, PU_DT

def requests_topic(day):
    return f"requests-{day}"

def ensure_topic(bootstrap, name, partitions, recreate=False):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if name in existing and recreate:
        list(admin.delete_topics([name]).values())[0].result()
        time.sleep(1.0)
        existing = admin.list_topics(timeout=10).topics
    if name not in existing:
        list(admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)]).values())[0].result()
    return name

def produce_day(cfg, day):
    """Replay one day's clean requests into requests-<day>. Returns the count and the producer's rate."""
    week = load_week(cfg["trips_dir"])
    demand = day_requests(week, day).sort(PU_DT, "trip_id").with_columns(pl.col(PU_DT).dt.epoch("s").alias("t_epoch"), pl.col(PU_DT).dt.hour().alias("hour"))
    topic = ensure_topic(cfg["kafka"], requests_topic(day), cfg["partitions"], recreate=True)
    # idempotent so retries cannot reorder a partition: the oracle's event-time merge relies on per-partition order
    p = Producer({"bootstrap.servers": cfg["kafka"], "enable.idempotence": True, "linger.ms": 20, "batch.num.messages": 10000, "compression.type": "lz4", "queue.buffering.max.messages": 200000})
    speed = cfg.get("replay_speed", "max")
    t0_event = int(demand["t_epoch"][0]); t0_wall = time.perf_counter()
    n = 0; t_start = time.perf_counter()
    for r in demand.iter_rows(named=True):
        t = int(r["t_epoch"])
        if speed != "max":
            lag = (t - t0_event) / float(speed) - (time.perf_counter() - t0_wall)
            if lag > 0:
                time.sleep(lag)
        key = h3.cell_to_parent(h3.latlng_to_cell(r["pickup_latitude"], r["pickup_longitude"], cfg["h3_resolution"]), cfg["key_resolution"])
        rec = {"type": "request", "request_id": int(r["trip_id"]), "t": t, "hour": int(r["hour"]),
               "pu_lon": r["pickup_longitude"], "pu_lat": r["pickup_latitude"], "do_lon": r["dropoff_longitude"], "do_lat": r["dropoff_latitude"],
               "trip_s": int(r["duration_s"]), "key_cell": key}
        while True:
            try:
                p.produce(topic, key=key, value=json.dumps(rec).encode()); break
            except BufferError:
                p.poll(0.05)
        n += 1
        if n % 20000 == 0:
            p.poll(0)
    p.flush()
    for part in range(cfg["partitions"]):
        p.produce(topic, partition=part, key=b"eod", value=json.dumps({"type": "eod", "t": t + 1}).encode())
    p.flush()
    dt = time.perf_counter() - t_start
    return {"topic": topic, "requests": n, "partitions": cfg["partitions"], "key_resolution": cfg["key_resolution"], "seconds": round(dt, 2), "records_per_s": round(n / dt, 1), "replay_speed": speed}

def read_topic(bootstrap, topic, group="ledger-reader"):
    """Drain a topic from the beginning to the end offsets as of the call. Returns a list of (partition, offset, key, value)."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{group}-{time.time_ns()}", "auto.offset.reset": "earliest", "enable.auto.commit": False, "fetch.max.bytes": 50 * 1024 * 1024})
    md = c.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, pnum, 0) for pnum in md.partitions]
    ends = {tp.partition: c.get_watermark_offsets(tp, timeout=10)[1] for tp in parts}
    c.assign(parts)
    out, done = [], {pnum for pnum, end in ends.items() if end == 0}
    while len(done) < len(parts):
        msgs = c.consume(num_messages=5000, timeout=5.0)
        if not msgs:
            break
        for m in msgs:
            if m.error():
                continue
            out.append((m.partition(), m.offset(), m.key(), m.value()))
            if m.offset() + 1 >= ends[m.partition()]:
                done.add(m.partition())
    c.close()
    return out

if __name__ == "__main__":
    cfg = yaml.safe_load(open(sys.argv[1]))
    for day in sys.argv[2:] or cfg["days"]:
        print(json.dumps(produce_day(cfg, day)))
