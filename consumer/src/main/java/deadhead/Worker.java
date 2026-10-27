package deadhead;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * One member of the consumer group: the matcher for the partitions (and so the H3 regions) the group assigns it.
 *
 * Consumes requests-<day>, merges its partitions into event-time order, matches each request among the idle drivers
 * in the cells it owns (nearest 50 by straight line, ranked by OSRM road time: the oracle's road rule), and writes
 * the decision to assignments-<run> with the oracle's schema. Driver state is a fold of the fleet-<run> log
 * (one partition per request partition): a claim is written there and acknowledged before the assignment goes out,
 * and offsets are committed only after that, so a killed worker's successor rebuilds the same state and finds the
 * request already decided. At maximum replay speed the workers keep each other within a watermark slack of event
 * time through the clock-<run> topic; at a paced speed the wall clock does that, from one origin the group shares
 * through the same topic (the first member to reach a paced request sets it; a member whose first paced request
 * comes later must not start its clock later, or the others stall on it at the watermark gate).
 */
public final class Worker {
    // ---- configuration (every value is passed by the launcher from configs/phase3.yaml) ----
    final Map<String, String> a;
    final String me, runId, bootstrap, requestsTopic, fleetTopic, assignTopic, clockTopic;
    final int partitions, candidatesK, gen0 = 0;
    final double abandonS, cruiseAfterS, slackS, speed, paceFromT, untilT;
    final long peerExpiryMs;
    final double windowS, dayStart;                                     // Phase 4: the tumbling event-time window (0 = one decision per request)
    final Tables tables; final Osrm osrm; final ObjectMapper om = new ObjectMapper();

    // ---- clients ----
    KafkaConsumer<String, String> requests, fleetLog, clocks;
    KafkaProducer<String, String> producer;

    // ---- state ----
    final Map<Integer, Region> regions = new HashMap<>();
    final Map<Integer, Region> parked = new HashMap<>();              // kept across an eager revoke in case the partition comes back
    final Map<Integer, ArrayDeque<Request>> queues = new HashMap<>();
    final Map<Integer, Long> processed = new HashMap<>();              // next offset to commit per owned partition
    final Map<Integer, Long> committedAt = new HashMap<>();
    final Map<Integer, Double> lastT = new HashMap<>();
    final Set<Integer> ended = new HashSet<>();
    final Map<Integer, Long> eodOffset = new HashMap<>();             // committed only once the partition's buffered records are processed
    final Set<Integer> paused = new HashSet<>();
    final Map<String, double[]> peers = new HashMap<>();              // worker -> {watermark, wall ms last heard}
    int gen = -1;
    double clock = 0;                                                  // this worker's event time; never goes backwards
    long paceStartWall = 0; long lastClockPub = 0; long lastCommit = 0; int sinceCommit = 0;
    volatile boolean stop = false; boolean done = false; boolean idleLast = false;
    final List<String> events = new ArrayList<>();
    long nDecisions, nMatched, nAbandoned, nNoCand, nRedelivered, nCruises, nStays, nUnroutable, nFolds, foldMs, nGated;
    double cruiseM, cruiseS; final List<Double> decisionMs = new ArrayList<>(), tableMs = new ArrayList<>();
    long startWall;

    record Request(int partition, long offset, JsonNode rec, double t, long id) {}
    static final class Pending { final Request rq; int rounds; boolean hadCandidates; Pending(Request rq) { this.rq = rq; } }
    final List<Pending> pending = new ArrayList<>();                   // Phase 4: requests waiting for the window to close (and those rolled from earlier windows)
    final Map<Integer, Long> popped = new HashMap<>();                 // next offset after the last request taken out of each partition's queue
    double nextClose = 0; long nBatches, nRolled;
    static final double BIG = 1e9;

    /** Wall-clock accounting by stage: name -> {count, nanoseconds}. */
    final Map<String, long[]> prof = new TreeMap<>();
    void prof(String name, long t0) { long[] v = prof.computeIfAbsent(name, k -> new long[2]); v[0]++; v[1] += System.nanoTime() - t0; }

    Worker(Map<String, String> a) throws Exception {
        this.a = a;
        me = a.get("worker"); runId = a.get("run"); bootstrap = a.getOrDefault("kafka", "localhost:9092");
        requestsTopic = a.get("requests"); fleetTopic = "fleet-" + runId; assignTopic = "assignments-" + runId; clockTopic = "clock-" + runId;
        partitions = Integer.parseInt(a.getOrDefault("partitions", "8")); candidatesK = Integer.parseInt(a.getOrDefault("candidates", "50"));
        abandonS = Double.parseDouble(a.getOrDefault("abandon", "600")); cruiseAfterS = Double.parseDouble(a.getOrDefault("cruise-after", "300"));
        slackS = Double.parseDouble(a.getOrDefault("slack", "30")); speed = Double.parseDouble(a.getOrDefault("speed", "0"));
        paceFromT = Double.parseDouble(a.getOrDefault("pace-from", "0")); untilT = Double.parseDouble(a.getOrDefault("until", "1e18"));
        tables = new Tables(a.get("cells"), a.get("demand"), Integer.parseInt(a.getOrDefault("key-res", "6")), partitions);
        osrm = new Osrm(a.getOrDefault("osrm", "http://localhost:5001"));
        peerExpiryMs = 2L * Long.parseLong(a.getOrDefault("session-timeout", "6000"));
        windowS = Double.parseDouble(a.getOrDefault("window", "0")); dayStart = Double.parseDouble(a.getOrDefault("day-start", "0"));
    }

    // ---------------------------------------------------------------- Kafka setup
    void open() {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "svc-" + runId);
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, me);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        c.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(50 * 1024 * 1024));
        c.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, a.getOrDefault("session-timeout", "6000"));
        c.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, a.getOrDefault("heartbeat", "2000"));
        c.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        c.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, a.getOrDefault("assignor", "range").equals("cooperative")
                ? "org.apache.kafka.clients.consumer.CooperativeStickyAssignor" : "org.apache.kafka.clients.consumer.RangeAssignor");
        requests = new KafkaConsumer<>(c);
        Properties m = new Properties(); m.putAll(c);
        m.put(ConsumerConfig.GROUP_ID_CONFIG, "fleetlog-" + runId + "-" + me); m.put(ConsumerConfig.CLIENT_ID_CONFIG, me + "-fleet"); m.remove(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        m.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
        fleetLog = new KafkaConsumer<>(m);
        Properties k = new Properties(); k.putAll(m);
        k.put(ConsumerConfig.GROUP_ID_CONFIG, "clock-" + runId + "-" + me); k.put(ConsumerConfig.CLIENT_ID_CONFIG, me + "-clock");
        clocks = new KafkaConsumer<>(k);
        TopicPartition ctp = new TopicPartition(clockTopic, 0);
        clocks.assign(List.of(ctp)); clocks.seekToEnd(List.of(ctp));
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, me + "-producer");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");      // per-partition order survives a retry
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");           // every acknowledged write is on the decision path
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producer = new KafkaProducer<>(p);
        requests.subscribe(List.of(requestsTopic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> parts) { revoked(parts); }
            public void onPartitionsAssigned(Collection<TopicPartition> parts) { assigned(parts); }
            public void onPartitionsLost(Collection<TopicPartition> parts) { event("lost", parts); for (TopicPartition tp : parts) dropPartition(tp.partition()); }
        });
    }

    void event(String what, Collection<TopicPartition> parts) {
        List<Integer> ps = new ArrayList<>(); for (TopicPartition tp : parts) ps.add(tp.partition()); Collections.sort(ps);
        String s = String.format(Locale.ROOT, "{\"event\":\"%s\",\"wall_ms\":%d,\"gen\":%d,\"partitions\":%s,\"clock\":%.0f}", what, System.currentTimeMillis(), gen, ps, clock);
        events.add(s); System.out.println("EVENT " + s); System.out.flush();
    }

    void revoked(Collection<TopicPartition> parts) {
        event("revoked", parts);
        producer.flush(); commit(parts);
        for (TopicPartition tp : parts) { int p = tp.partition(); Region r = regions.remove(p); if (r != null) parked.put(p, r); queues.remove(p); processed.remove(p); committedAt.remove(p); ended.remove(p); paused.remove(p); popped.remove(p); eodOffset.remove(p); pending.removeIf(x -> x.rq.partition == p); }
        syncFleetAssignment();
    }

    void dropPartition(int p) { regions.remove(p); parked.remove(p); queues.remove(p); processed.remove(p); committedAt.remove(p); ended.remove(p); paused.remove(p); popped.remove(p); eodOffset.remove(p); pending.removeIf(x -> x.rq.partition == p); }

    void assigned(Collection<TopicPartition> parts) {
        gen = requests.groupMetadata().generationId();
        peers.clear();                                   // the group changed: a departed member's last watermark must not gate the survivors
        event("assigned", parts);
        Map<TopicPartition, Long> ends = requests.endOffsets(parts);
        for (TopicPartition tp : parts) {
            int p = tp.partition();
            queues.put(p, new ArrayDeque<>()); lastT.putIfAbsent(p, 0.0);
            long pos = requests.position(tp); committedAt.put(p, pos); processed.put(p, pos); popped.put(p, pos);
            if (pos >= ends.get(tp)) ended.add(p);           // a partition whose end-of-day was already consumed by a previous owner
            Region kept = parked.remove(p);
            if (kept != null) regions.put(p, kept);
        }
        parked.clear();
        syncFleetAssignment();
        for (TopicPartition tp : parts) if (!regions.containsKey(tp.partition())) fold(tp.partition());
    }

    /** The fleet-log consumer mirrors the owned request partitions; positions of partitions kept across the rebalance are preserved. */
    void syncFleetAssignment() {
        Map<Integer, Long> pos = new HashMap<>();
        for (TopicPartition tp : fleetLog.assignment()) if (regions.containsKey(tp.partition())) pos.put(tp.partition(), fleetLog.position(tp));
        List<TopicPartition> want = new ArrayList<>();
        for (int p : regions.keySet()) want.add(new TopicPartition(fleetTopic, p));
        for (int p : queues.keySet()) if (!regions.containsKey(p)) want.add(new TopicPartition(fleetTopic, p));
        fleetLog.assign(want);
        for (TopicPartition tp : want) { Long q = pos.get(tp.partition()); if (q != null) fleetLog.seek(tp, q); }
    }

    /** Rebuild a partition's regions from its fleet log, start to current end, then re-emit the arrivals of its last departures (idempotent). */
    void fold(int p) {
        long t0 = System.currentTimeMillis();
        TopicPartition tp = new TopicPartition(fleetTopic, p);
        Region r = new Region(p, cruiseAfterS);
        long end = fleetLog.endOffsets(List.of(tp)).get(tp);
        fleetLog.seekToBeginning(List.of(tp));
        ArrayDeque<JsonNode> tail = new ArrayDeque<>();
        long n = 0;
        while (fleetLog.position(tp) < end) {
            for (ConsumerRecord<String, String> rec : fleetLog.poll(Duration.ofMillis(500)).records(tp)) {
                try {
                    JsonNode j = om.readTree(rec.value());
                    r.apply(j); n++;
                    if (j.get("k").asText().equals("x")) { tail.add(j); if (tail.size() > 500) tail.poll(); }
                } catch (Exception e) { throw new RuntimeException(e); }
                if (rec.offset() + 1 >= end) break;
            }
        }
        regions.put(p, r);
        // a departure whose arrival never reached its destination partition (the writer died in between) is re-sent; versions make duplicates harmless
        for (JsonNode x : tail) sendDriver(x.get("q").asInt(), Region.driverOf(x), "d", -1);
        nFolds++; foldMs += System.currentTimeMillis() - t0;
        event("folded", List.of(tp)); System.out.println("FOLD partition " + p + " records " + n + " drivers " + r.drivers.size() + " decided " + r.decided.size() + " ms " + (System.currentTimeMillis() - t0));
    }

    void commit(Collection<TopicPartition> parts) {
        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        for (TopicPartition tp : parts) {
            Long next = processed.get(tp.partition()), was = committedAt.get(tp.partition());
            if (next != null && (was == null || next > was)) m.put(tp, new OffsetAndMetadata(next));
        }
        if (!m.isEmpty()) { requests.commitSync(m); for (TopicPartition tp : m.keySet()) committedAt.put(tp.partition(), m.get(tp).offset()); }
        lastCommit = System.currentTimeMillis(); sinceCommit = 0;
    }

    void commitOwned() { List<TopicPartition> ps = new ArrayList<>(); for (int p : queues.keySet()) ps.add(new TopicPartition(requestsTopic, p)); producer.flush(); commit(ps); }

    // ---------------------------------------------------------------- the fleet log, writing side
    ObjectNode driverNode(Driver d, String k) {
        ObjectNode n = om.createObjectNode();
        n.put("k", k).put("id", d.id).put("lon", d.lon).put("lat", d.lat).put("cell", d.cell).put("busy_until", d.busyUntil).put("idle_since", d.idleSince)
         .put("cruise_n", d.cruiseN).put("v", d.version).put("gen", d.gen).put("w", me).put("g", gen);
        return n;
    }

    Future<RecordMetadata> sendDriver(int partition, Driver d, String k, int q) {
        ObjectNode n = driverNode(d, k);
        if (q >= 0) n.put("q", q);
        return producer.send(new ProducerRecord<>(fleetTopic, partition, String.valueOf(d.id), n.toString()));
    }

    void await(Future<RecordMetadata> f) { long t0 = System.nanoTime(); try { f.get(); } catch (InterruptedException | ExecutionException e) { throw new RuntimeException(e); } prof("await_ack", t0); }

    /** Move driver d (currently idle in region `from`) to its next state; apply locally, then write: tombstone or in-place upsert first. */
    Future<RecordMetadata> move(Region from, Driver d, Driver next, int q) {
        from.claimIdle(d);
        if (q == from.partition) { from.upsert(next); return sendDriver(from.partition, next, "d", -1); }
        from.remove(d.id); from.lastVersion.put(d.id, next.version);
        Region to = regions.get(q);
        if (to != null) to.upsert(next);
        return sendDriver(from.partition, next, "x", q);
    }

    // ---------------------------------------------------------------- time
    double groupMin() {
        long now = System.currentTimeMillis(); double min = ownWatermark();
        for (Iterator<Map.Entry<String, double[]>> it = peers.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, double[]> e = it.next();
            if (now - e.getValue()[1] > peerExpiryMs) { it.remove(); continue; }
            min = Math.min(min, e.getValue()[0]);
        }
        return min;
    }

    double ownWatermark() {
        double wm = 1e18;
        for (int p : queues.keySet()) {
            ArrayDeque<Request> q = queues.get(p);
            if (q.isEmpty() && ended.contains(p)) continue;
            wm = Math.min(wm, q.isEmpty() ? lastT.get(p) : q.peek().t);
        }
        return wm;
    }

    void publishClock(boolean force) {
        long now = System.currentTimeMillis();
        if (!force && now - lastClockPub < 100) return;
        lastClockPub = now;
        double wm = ownWatermark();
        producer.send(new ProducerRecord<>(clockTopic, 0, me, String.format(Locale.ROOT, "{\"w\":\"%s\",\"wm\":%.1f,\"wall\":%d,\"done\":%s,\"clock\":%.0f,\"decisions\":%d,\"pace\":%d}", me, wm, now, done, clock, nDecisions, paceStartWall)));
    }

    void pollClocks() {
        for (ConsumerRecord<String, String> rec : clocks.poll(Duration.ZERO)) {
            try {
                JsonNode j = om.readTree(rec.value()); String w = j.get("w").asText();
                if (!w.equals(me)) peers.put(w, new double[]{j.get("wm").asDouble(), System.currentTimeMillis()});
                long pace = j.path("pace").asLong(0);              // the group paces from one wall-clock origin: the earliest any member set
                if (pace > 0 && (paceStartWall == 0 || pace < paceStartWall)) paceStartWall = pace;
            }
            catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    void pollFleetLog() {
        if (fleetLog.assignment().isEmpty()) return;
        for (ConsumerRecord<String, String> rec : fleetLog.poll(Duration.ZERO)) {
            Region r = regions.get(rec.partition());
            if (r == null) continue;
            try {
                JsonNode j = om.readTree(rec.value());
                if (me.equals(j.path("w").asText()) && j.path("g").asInt(-2) == gen) continue;    // my own write in this generation: applied when written
                r.apply(j);
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    /** Time passes for every owned region: trips end, idle drivers cruise toward demand. */
    void tick(double now) throws Exception {
        if (now < clock) now = clock;
        clock = now;
        for (Region r : regions.values()) r.release(now);
        long t0 = System.nanoTime();
        for (Region r : new ArrayList<>(regions.values())) cruise(r, now);
        prof("cruise", t0);
    }

    void cruise(Region r, double now) throws Exception {
        while (!r.cruise.isEmpty() && r.cruise.peek().at() <= now) {
            Region.CruiseEvent e = r.cruise.poll();
            Driver d = r.drivers.get(e.id());
            if (d == null || d.gen != e.gen() || !d.idle() || !r.isIdleHere(d)) continue;
            String target = tables.cruiseTarget(now, d.cell);
            if (target == null) { r.cruise.add(new Region.CruiseEvent(now + cruiseAfterS, d.id, d.gen)); nStays++; continue; }
            double[] pt = tables.cruisePoint(now, target, d.id, d.cruiseN);
            long tr = System.nanoTime(); double[] rt = osrm.route(new double[]{d.lon, d.lat}, pt); prof("osrm_route", tr);
            double roadM, roadS;
            if (rt != null) { roadM = rt[0]; roadS = rt[1]; } else { double h = Geo.haversineM(d.lon, d.lat, pt[0], pt[1]); roadM = h * 1.3; roadS = h * 1.3 / 8.0; }
            Driver next = new Driver(d.id, pt[0], pt[1], target, now + roadS, -1, d.cruiseN + 1, Math.max(d.version + 1, (long) now), d.gen);
            int q = tables.regionOf(target);
            Future<RecordMetadata> f = move(r, d, next, q);
            if (q != r.partition) { await(f); sendDriver(q, next, "d", -1); }
            nCruises++; cruiseM += roadM; cruiseS += roadS;
            publishClock(false);                              // a burst of cruising must not look like silence to the peers
        }
    }

    /** Candidates: idle drivers in the ring-3 disk of the pickup cell, in the regions this worker owns, nearest K by straight line, (distance, id) order. {hav, id, region}. */
    List<double[]> candidates(JsonNode rec, int[] invisibleCells) {
        double puLon = rec.get("pu_lon").asDouble(), puLat = rec.get("pu_lat").asDouble();
        Tables.Cell pc = tables.cells.get(rec.get("pu_cell").asText());
        List<double[]> cand = new ArrayList<>();
        for (String c : pc.d3()) {
            Tables.Cell ci = tables.cells.get(c);
            if (ci == null) continue;
            int reg = tables.partitionOf(tables.key(c));
            Region r = regions.get(reg);
            if (r == null) { invisibleCells[0]++; continue; }
            HashSet<Integer> s = r.idle.get(c);
            if (s == null) continue;
            for (int id : s) { Driver d = r.drivers.get(id); cand.add(new double[]{Geo.haversineM(d.lon, d.lat, puLon, puLat), id, reg}); }
        }
        cand.sort((x, y) -> { int c = Double.compare(x[0], y[0]); return c != 0 ? c : Double.compare(x[1], y[1]); });
        if (cand.size() > candidatesK) cand = new ArrayList<>(cand.subList(0, candidatesK));
        return cand;
    }

    // ---------------------------------------------------------------- the decision
    void process(Request rq) throws Exception {
        long startWallMs = System.currentTimeMillis();
        JsonNode rec = rq.rec; double t = rq.t; int p = rq.partition;
        Region home = regions.get(p);
        double now = Math.max(clock, t);
        double dueWall = speed > 0 && t >= paceFromT ? paceStartWall + (t - paceFromT) / speed * 1000 : startWallMs;
        String prior = home.decided.get(rq.id);
        if (prior != null) { redeliver(home, om.readTree(prior), rq, startWallMs); return; }
        tick(now);
        ObjectNode out = om.createObjectNode();
        out.put("request_id", rq.id).put("t", t).put("hour", rec.get("hour").asInt()).put("now", now).put("delay_s", now - t).put("worker", me).put("gen", gen)
           .put("partition", p).put("offset", rq.offset).put("rule", "road").put("pu_cell", rec.get("pu_cell").asText()).put("key_cell", rec.get("key_cell").asText())
           .put("trip_s", rec.get("trip_s").asInt()).put("redelivered", false);
        double puLon = rec.get("pu_lon").asDouble(), puLat = rec.get("pu_lat").asDouble();
        int[] inv = new int[1]; List<double[]> cand = candidates(rec, inv); int invisibleCells = inv[0];
        out.put("n_candidates", cand.size()).put("invisible_cells", invisibleCells);
        if (cand.isEmpty()) { out.put("status", "no_candidates").put("driver", -1); nNoCand++; finish(home, rq, out, null, null, -1, startWallMs, dueWall); return; }
        List<double[]> srcs = new ArrayList<>();
        for (double[] c : cand) { Driver d = regions.get((int) c[2]).drivers.get((int) c[1]); srcs.add(new double[]{d.lon, d.lat}); }
        long t1 = System.nanoTime();
        double[][] tb = osrm.tableManyToOne(srcs, new double[]{puLon, puLat});
        tableMs.add((System.nanoTime() - t1) / 1e6); prof("osrm_table", t1);
        int best = -1; double bestS = Double.POSITIVE_INFINITY; int unroutable = 0;
        for (int i = 0; i < cand.size(); i++) {
            double s = tb == null ? Double.NaN : tb[0][i];
            if (Double.isNaN(s)) { unroutable++; continue; }
            if (s < bestS) { bestS = s; best = i; }               // first minimum in (distance, id) order, as the oracle's argmin
        }
        nUnroutable += unroutable; out.put("n_unroutable", unroutable);
        if (best < 0) { out.put("status", "no_candidates").put("driver", -1); nNoCand++; finish(home, rq, out, null, null, -1, startWallMs, dueWall); return; }
        Region from = regions.get((int) cand.get(best)[2]); Driver d = from.drivers.get((int) cand.get(best)[1]);
        out.put("driver", d.id).put("promised_eta_s", bestS).put("actual_eta_s", bestS).put("leg_m", tb[1][best]).put("sl_hav_m", cand.get(best)[0])
           .put("driver_lon", d.lon).put("driver_lat", d.lat).put("driver_region", from.partition);
        if (bestS > abandonS) { out.put("status", "abandoned"); nAbandoned++; finish(home, rq, out, null, null, -1, startWallMs, dueWall); return; }
        double busyUntil = now + bestS + rec.get("trip_s").asInt();
        String doCell = rec.get("do_cell").asText();
        Driver next = new Driver(d.id, rec.get("do_lon").asDouble(), rec.get("do_lat").asDouble(), doCell, busyUntil, -1, d.cruiseN, Math.max(d.version + 1, (long) now), d.gen + 1);
        int q = tables.regionOf(doCell);
        out.put("status", "matched").put("busy_until", busyUntil).put("dest_region", q);
        nMatched++;
        finish(home, rq, out, from, next, q, startWallMs, dueWall);
    }

    /**
     * Durability order for a match: (1) the driver's departure in its own region's log, acknowledged; (2) the decision in the
     * request's partition, acknowledged; (3) the arrival in the destination region's log and the assignment record. A kill
     * between any two steps leaves a state the successor repairs from the log: a departed driver whose arrival is re-sent,
     * or a decided request whose assignment is re-emitted. It can never leave one driver idle in two regions.
     */
    void finish(Region home, Request rq, ObjectNode out, Region from, Driver next, int q, long startWallMs, double dueWall) {
        ObjectNode r = om.createObjectNode();
        r.put("k", "r").put("request_id", rq.id).put("w", me).put("g", gen);
        Future<RecordMetadata> f1 = null;
        if (from != null) {
            f1 = move(from, from.drivers.get(next.id), next, q);
            r.set("m", driverNode(next, "d").put("q", q).put("from", from.partition));
            if (from.partition != home.partition) await(f1);       // a different partition: no ordering guarantee, so wait
        }
        r.set("a", out);
        home.decided.put(rq.id, r.toString());
        await(producer.send(new ProducerRecord<>(fleetTopic, home.partition, String.valueOf(rq.id), r.toString())));
        if (from != null && q != from.partition) sendDriver(q, next, "d", -1);
        long doneWall = System.currentTimeMillis();
        out.put("due_wall_ms", (long) dueWall).put("start_wall_ms", startWallMs).put("done_wall_ms", doneWall).put("decision_ms", doneWall - startWallMs);
        producer.send(new ProducerRecord<>(assignTopic, String.valueOf(rq.id), out.toString()));
        nDecisions++; decisionMs.add((double) (doneWall - startWallMs));
    }

    /** A request this partition already decided (at-least-once: the previous owner died after deciding, before committing): re-emit, never re-decide. */
    void redeliver(Region home, JsonNode r, Request rq, long startWallMs) {
        ObjectNode out = (ObjectNode) r.get("a");
        out.put("redelivered", true).put("redelivered_by", me).put("redelivered_gen", gen).put("done_wall_ms", System.currentTimeMillis());
        if (r.has("m")) { JsonNode m = r.get("m"); int q = m.get("q").asInt(), from = m.get("from").asInt(); if (q != from) sendDriver(q, Region.driverOf(m), "d", -1); }
        producer.send(new ProducerRecord<>(assignTopic, String.valueOf(rq.id), out.toString()));
        nRedelivered++;
    }

    // ---------------------------------------------------------------- the loop
    void run() throws Exception {
        open();
        startWall = System.currentTimeMillis();
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { stop = true; try { main.join(30000); } catch (InterruptedException ignored) {} }));   // let the loop finish, commit and print its summary
        while (!stop) {
            boolean idle = true;
            long tp0 = System.nanoTime();
            // a worker with nothing to do (gated by the group, or waiting for records) must not spin: it would take the CPU from the one that has work
            ConsumerRecords<String, String> polled = requests.poll(Duration.ofMillis(idleLast ? 5 : 0)); prof("poll_requests", tp0);
            for (ConsumerRecord<String, String> rec : polled) {
                int p = rec.partition();
                ArrayDeque<Request> q = queues.get(p);
                if (q == null) continue;
                JsonNode j = om.readTree(rec.value());
                if (j.get("type").asText().equals("eod")) { ended.add(p); if (q.isEmpty()) processed.put(p, rec.offset() + 1); else eodOffset.put(p, rec.offset() + 1); continue; }
                q.add(new Request(p, rec.offset(), j, j.get("t").asDouble(), j.get("request_id").asLong()));
            }
            for (int p : queues.keySet()) {
                TopicPartition tp = new TopicPartition(requestsTopic, p);
                int n = queues.get(p).size();
                if (n > 2000 && !paused.contains(p)) { requests.pause(List.of(tp)); paused.add(p); }
                else if (n < 500 && paused.contains(p)) { requests.resume(List.of(tp)); paused.remove(p); }
            }
            long tf0 = System.nanoTime(); pollFleetLog(); prof("poll_fleetlog", tf0); long tc0 = System.nanoTime(); pollClocks(); prof("poll_clocks", tc0);
            double gmin = groupMin();
            if (windowS > 0) { if (batchStep(gmin)) idle = false; } else for (int k = 0; k < 200; k++) {
                Request head = null; int hp = -1; boolean waiting = false;
                for (int p : queues.keySet()) {
                    ArrayDeque<Request> q = queues.get(p);
                    if (q.isEmpty()) { if (ended.contains(p)) continue; waiting = true; break; }   // an ended partition still drains its buffer
                    Request h = q.peek();
                    if (head == null || h.t < head.t || (h.t == head.t && h.id < head.id)) { head = h; hp = p; }
                }
                if (waiting || head == null) break;
                if (head.t >= untilT) { ended.add(hp); queues.get(hp).clear(); continue; }
                if (head.t > gmin + slackS) { nGated++; break; }
                if (speed > 0 && head.t >= paceFromT) {
                    if (paceStartWall == 0) { paceStartWall = System.currentTimeMillis(); publishClock(true); }
                    long due = (long) (paceStartWall + (head.t - paceFromT) / speed * 1000), nowMs = System.currentTimeMillis();
                    if (due > nowMs) { if (due - nowMs > 5) break; Thread.sleep(due - nowMs); }
                }
                queues.get(hp).poll();
                long tpr = System.nanoTime(); process(head); prof("process", tpr); idle = false;
                lastT.put(hp, head.t); processed.put(hp, head.offset + 1); sinceCommit++;
                if (queues.get(hp).isEmpty() && eodOffset.containsKey(hp)) processed.put(hp, eodOffset.remove(hp));
                if (nDecisions % 20000 == 0) System.out.printf(Locale.ROOT, "%s: %d decisions, %.0f/s, matched %d, clock %.0f%n", me, nDecisions, nDecisions * 1000.0 / Math.max(1, System.currentTimeMillis() - startWall), nMatched, clock);
            }
            if (idle) {                                           // time still passes for the regions this worker owns, up to what the group allows
                double upto = Math.min(ownWatermark(), gmin + slackS);
                if (upto > clock && upto < 1e17) tick(upto);
            }
            if (sinceCommit >= 200 || (sinceCommit > 0 && System.currentTimeMillis() - lastCommit > 1000)) { long tcm = System.nanoTime(); commitOwned(); prof("commit", tcm); }
            boolean allEnded = !queues.isEmpty() && ended.containsAll(queues.keySet()) && !queuesHaveWork();
            if (allEnded != done) { done = allEnded; publishClock(true); if (done) { commitOwned(); event("done", List.of()); } }
            publishClock(false);
            idleLast = idle;
        }
        commitOwned();
        publishClock(true);
        producer.flush();
        System.out.println("SUMMARY " + summary());
        requests.close(); fleetLog.close(); clocks.close(); producer.close();
    }

    boolean queuesHaveWork() { if (!pending.isEmpty()) return true; for (ArrayDeque<Request> q : queues.values()) if (!q.isEmpty()) return true; return false; }
    boolean queuesEmpty() { for (ArrayDeque<Request> q : queues.values()) if (!q.isEmpty()) return false; return true; }

    // ---------------------------------------------------------------- Phase 4: the window
    /**
     * Take every request before the next window close out of the owned queues; when every owned partition is known to be past the
     * close (its head is at or after it, or it has ended), and the group's time and the pace allow it, close the window. The offset
     * committed for a partition never passes a request still pending, so a successor re-reads it.
     */
    boolean batchStep(double gmin) throws Exception {
        boolean worked = false;
        if (nextClose == 0) {
            double first = 1e18;
            for (ArrayDeque<Request> q : queues.values()) if (!q.isEmpty()) first = Math.min(first, q.peek().t);
            if (first >= 1e17) return false;
            nextClose = dayStart + windowS * Math.floor((first - dayStart) / windowS) + windowS;
        }
        for (int k = 0; k < 20; k++) {
            boolean waiting = false;
            for (int p : new ArrayList<>(queues.keySet())) {
                ArrayDeque<Request> q = queues.get(p);
                while (!q.isEmpty() && q.peek().t < nextClose) {
                    Request h = q.poll();
                    if (h.t >= untilT) { ended.add(p); q.clear(); break; }
                    popped.put(p, h.offset + 1); lastT.put(p, h.t);
                    Region home = regions.get(p); String prior = home.decided.get(h.id);
                    if (prior != null) redeliver(home, om.readTree(prior), h, System.currentTimeMillis()); else pending.add(new Pending(h));
                }
                if (q.isEmpty() && !ended.contains(p)) waiting = true;
            }
            if (waiting) break;
            boolean drained = ended.containsAll(queues.keySet()) && queuesEmpty();
            if (drained && pending.isEmpty()) break;
            if (!drained && nextClose > gmin + slackS) { nGated++; break; }
            if (speed > 0 && nextClose >= paceFromT) {
                if (paceStartWall == 0) { paceStartWall = System.currentTimeMillis(); publishClock(true); }
                long due = (long) (paceStartWall + (nextClose - paceFromT) / speed * 1000), nowMs = System.currentTimeMillis();
                if (due > nowMs) { if (due - nowMs > 5) break; Thread.sleep(due - nowMs); }
            }
            updateProcessed();
            long tpr = System.nanoTime(); processBatch(nextClose); prof("process", tpr); worked = true;
            nextClose += windowS;
            updateProcessed();
        }
        return worked;
    }

    /** The offset to commit per partition: past everything taken out of its queue, but never past a request still pending. */
    void updateProcessed() {
        for (int p : queues.keySet()) {
            Long base = popped.get(p); if (base == null) continue;
            long next = base;
            for (Pending x : pending) if (x.rq.partition == p) next = Math.min(next, x.rq.offset);
            processed.put(p, next);
            if (next == base && queues.get(p).isEmpty() && eodOffset.containsKey(p)) processed.put(p, eodOffset.remove(p));
        }
    }

    /** Close the window at `close`: time passes, then the pending requests are assigned together over this worker's regions. */
    void processBatch(double close) throws Exception {
        long startWallMs = System.currentTimeMillis();
        double now = Math.max(clock, close);
        tick(now);
        if (pending.isEmpty()) return;
        nBatches++;
        int R = pending.size();
        List<List<double[]>> cand = new ArrayList<>(R); int[] invis = new int[R];
        Map<Integer, Integer> col = new HashMap<>(); List<int[]> cols = new ArrayList<>();       // column -> {driver id, region}
        for (int i = 0; i < R; i++) {
            int[] inv = new int[1]; List<double[]> c = candidates(pending.get(i).rq.rec, inv); cand.add(c); invis[i] = inv[0];
            for (double[] x : c) if (!col.containsKey((int) x[1])) { col.put((int) x[1], cols.size()); cols.add(new int[]{(int) x[1], (int) x[2]}); }
        }
        int D = cols.size();
        double[][] roadS = new double[R][D], roadM = new double[R][D];
        for (double[] row : roadS) Arrays.fill(row, Double.NaN);
        if (D > 0) {
            List<double[]> src = new ArrayList<>(D), dst = new ArrayList<>(R);
            for (int[] c : cols) { Driver d = regions.get(c[1]).drivers.get(c[0]); src.add(new double[]{d.lon, d.lat}); }
            for (Pending x : pending) dst.add(new double[]{x.rq.rec.get("pu_lon").asDouble(), x.rq.rec.get("pu_lat").asDouble()});
            long t1 = System.nanoTime();
            double[][][] tb = osrm.tableManyToMany(src, dst, 1000);
            tableMs.add((System.nanoTime() - t1) / 1e6); prof("osrm_table", t1);
            for (int i = 0; i < R; i++) for (double[] x : cand.get(i)) {
                int j = col.get((int) x[1]); double sec = tb[0][j][i];
                if (Double.isNaN(sec)) { nUnroutable++; continue; }
                roadS[i][j] = sec; roadM[i][j] = tb[1][j][i];
            }
        }
        // the road rule: cost is road time; a pair is on offer only if its promise (time waited + road time) is within T
        double[][] cost = new double[R][D]; double[] waited = new double[R];
        for (int i = 0; i < R; i++) { waited[i] = close - pending.get(i).rq.t; for (int j = 0; j < D; j++) cost[i][j] = (!Double.isNaN(roadS[i][j]) && waited[i] + roadS[i][j] <= abandonS) ? roadS[i][j] : BIG; }
        long ts = System.nanoTime(); int[] as = Assignment.solve(cost, BIG); prof("solve", ts);
        List<Pending> next = new ArrayList<>();
        for (int i = 0; i < R; i++) {
            Pending x = pending.get(i); Request rq = x.rq; JsonNode rec = rq.rec; int p = rq.partition; Region home = regions.get(p);
            x.rounds++;
            boolean hasCand = false, feasible = false;
            for (int j = 0; j < D; j++) { if (!Double.isNaN(roadS[i][j])) hasCand = true; if (cost[i][j] < BIG) feasible = true; }
            x.hadCandidates |= hasCand;
            double dueWall = speed > 0 && rq.t >= paceFromT ? paceStartWall + (rq.t - paceFromT) / speed * 1000 : startWallMs;
            ObjectNode out = om.createObjectNode();
            out.put("request_id", rq.id).put("t", rq.t).put("hour", rec.get("hour").asInt()).put("now", now).put("delay_s", now - rq.t).put("worker", me).put("gen", gen)
               .put("partition", p).put("offset", rq.offset).put("rule", "road").put("pu_cell", rec.get("pu_cell").asText()).put("key_cell", rec.get("key_cell").asText())
               .put("trip_s", rec.get("trip_s").asInt()).put("redelivered", false)
               .put("window_s", windowS).put("batch_size", R).put("decided_at", close).put("queue_s", waited[i]).put("rounds", x.rounds)
               .put("n_candidates", cand.get(i).size()).put("invisible_cells", invis[i]);
            if (as[i] >= 0) {
                int j = as[i]; int[] c = cols.get(j); Region from = regions.get(c[1]); Driver d = from.drivers.get(c[0]);
                double sec = roadS[i][j], hav = Double.NaN;
                for (double[] e : cand.get(i)) if ((int) e[1] == d.id) { hav = e[0]; break; }
                out.put("driver", d.id).put("promised_eta_s", sec).put("actual_eta_s", sec).put("wait_s", waited[i] + sec).put("leg_m", roadM[i][j]).put("sl_hav_m", hav)
                   .put("driver_lon", d.lon).put("driver_lat", d.lat).put("driver_region", from.partition);
                double busyUntil = now + sec + rec.get("trip_s").asInt();
                String doCell = rec.get("do_cell").asText();
                Driver nxt = new Driver(d.id, rec.get("do_lon").asDouble(), rec.get("do_lat").asDouble(), doCell, busyUntil, -1, d.cruiseN, Math.max(d.version + 1, (long) now), d.gen + 1);
                int q = tables.regionOf(doCell);
                out.put("status", "matched").put("busy_until", busyUntil).put("dest_region", q);
                nMatched++; finish(home, rq, out, from, nxt, q, startWallMs, dueWall);
            } else if (hasCand && !feasible) {                       // candidates, but every promise is over T: the rider abandons (the oracle's rule)
                out.put("status", "abandoned").put("driver", -1); nAbandoned++; finish(home, rq, out, null, null, -1, startWallMs, dueWall);
            } else if (close + windowS - rq.t <= abandonS) {          // another window closes inside the rider's patience: roll
                next.add(x); nRolled++;
            } else if (x.hadCandidates) {
                out.put("status", "abandoned").put("driver", -1); nAbandoned++; finish(home, rq, out, null, null, -1, startWallMs, dueWall);
            } else {
                out.put("status", "no_candidates").put("driver", -1); nNoCand++; finish(home, rq, out, null, null, -1, startWallMs, dueWall);
            }
        }
        pending.clear(); pending.addAll(next);
    }

    static double pct(List<Double> xs, double q) { if (xs.isEmpty()) return Double.NaN; List<Double> s = new ArrayList<>(xs); Collections.sort(s); return s.get((int) Math.min(s.size() - 1, Math.floor(q * s.size()))); }

    String summary() {
        ObjectNode s = om.createObjectNode();
        s.put("worker", me).put("run", runId).put("decisions", nDecisions).put("matched", nMatched).put("abandoned", nAbandoned).put("no_candidates", nNoCand)
         .put("redelivered", nRedelivered).put("cruises", nCruises).put("stays", nStays).put("cruise_km", cruiseM / 1000).put("cruise_h", cruiseS / 3600)
         .put("unroutable_candidates", nUnroutable).put("folds", nFolds).put("fold_ms_total", foldMs).put("gated_polls", nGated)
         .put("osrm_calls", osrm.calls).put("osrm_retries", osrm.retries).put("wall_s", (System.currentTimeMillis() - startWall) / 1000.0)
         .put("decision_ms_median", pct(decisionMs, 0.5)).put("decision_ms_p99", pct(decisionMs, 0.99)).put("table_ms_median", pct(tableMs, 0.5))
         .put("window_s", windowS).put("batches", nBatches).put("rolled_rounds", nRolled);
        ObjectNode pr = om.createObjectNode(); for (Map.Entry<String, long[]> e : prof.entrySet()) pr.put(e.getKey(), String.format(Locale.ROOT, "%d calls, %.1f s", e.getValue()[0], e.getValue()[1] / 1e9)); s.set("profile", pr);
        s.putPOJO("events", events.stream().map(e -> { try { return om.readTree(e); } catch (Exception x) { throw new RuntimeException(x); } }).toList());
        return s.toString();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (String s : args) { String[] kv = s.startsWith("--") ? s.substring(2).split("=", 2) : new String[]{s, ""}; a.put(kv[0], kv.length > 1 ? kv[1] : ""); }
        new Worker(a).run();
    }
}
