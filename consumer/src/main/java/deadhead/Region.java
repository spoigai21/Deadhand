package deadhead;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

/**
 * The drivers of one partition's regions, as a fold of that partition of the fleet log. Every state change a worker
 * makes is written to the log before the request's offset is committed, so a worker that takes the partition over
 * rebuilds exactly this by folding the log (the idempotent store keyed by request_id lives here too).
 *
 * Log records (JSON, one per line): k = "d" upsert of a driver's full state, "x" tombstone (the driver left for
 * partition q; carries the state it left with), "r" a decision (the assignment record and, if matched, the move).
 * Every driver record carries a version that only increases across the driver's moves: a stale record, replayed or
 * re-emitted, never overwrites a newer one.
 */
public final class Region {
    public record BusyEvent(double at, int id, long version) implements Comparable<BusyEvent> {
        public int compareTo(BusyEvent o) { int c = Double.compare(at, o.at); return c != 0 ? c : Integer.compare(id, o.id); }
    }
    public record CruiseEvent(double at, int id, int gen) implements Comparable<CruiseEvent> {
        public int compareTo(CruiseEvent o) { int c = Double.compare(at, o.at); if (c != 0) return c; c = Integer.compare(id, o.id); return c != 0 ? c : Integer.compare(gen, o.gen); }
    }

    public final int partition;
    public final Map<Integer, Driver> drivers = new HashMap<>();
    public final Map<String, HashSet<Integer>> idle = new HashMap<>();
    public final PriorityQueue<BusyEvent> busy = new PriorityQueue<>();
    public final PriorityQueue<CruiseEvent> cruise = new PriorityQueue<>();
    public final Map<Integer, Long> lastVersion = new HashMap<>();
    public final Map<Long, String> decided = new HashMap<>();     // request_id -> the "r" record, for an idempotent re-emit
    public final double cruiseAfterS;
    public long applied, staleSkipped, idleCount;

    public Region(int partition, double cruiseAfterS) { this.partition = partition; this.cruiseAfterS = cruiseAfterS; }

    public static Driver driverOf(JsonNode r) {
        return new Driver(r.get("id").asInt(), r.get("lon").asDouble(), r.get("lat").asDouble(), r.get("cell").asText(), r.get("busy_until").asDouble(),
                r.get("idle_since").asDouble(), r.get("cruise_n").asInt(), r.get("v").asLong(), r.get("gen").asInt());
    }

    /** Apply one log record. Returns false if it was stale (older than what the region already holds for that driver). */
    public boolean apply(JsonNode r) {
        String k = r.get("k").asText();
        if (k.equals("r")) { decided.put(r.get("request_id").asLong(), r.toString()); applied++; return true; }
        int id = r.get("id").asInt(); long v = r.get("v").asLong();
        Long have = lastVersion.get(id);
        if (have != null && have > v) { staleSkipped++; return false; }
        lastVersion.put(id, v);
        if (k.equals("x")) { remove(id); applied++; return true; }
        upsert(driverOf(r)); applied++;
        return true;
    }

    public void upsert(Driver d) {
        Driver old = drivers.put(d.id, d);
        if (old != null && old.idle()) unidle(old);
        lastVersion.merge(d.id, d.version, Math::max);
        if (d.idle()) { idle.computeIfAbsent(d.cell, c -> new HashSet<>()).add(d.id); idleCount++; cruise.add(new CruiseEvent(d.idleSince + cruiseAfterS, d.id, d.gen)); }
        else busy.add(new BusyEvent(d.busyUntil, d.id, d.version));
    }

    public void remove(int id) {
        Driver old = drivers.remove(id);
        if (old != null && old.idle()) unidle(old);
    }

    private void unidle(Driver d) {
        HashSet<Integer> s = idle.get(d.cell);
        if (s != null && s.remove(d.id)) idleCount--;
    }

    public boolean isIdleHere(Driver d) {
        HashSet<Integer> s = idle.get(d.cell);
        return s != null && s.contains(d.id);
    }

    /** Take an idle driver out of the index (the one-claim rule: it must be there). */
    public void claimIdle(Driver d) {
        HashSet<Integer> s = idle.get(d.cell);
        if (s == null || !s.remove(d.id)) throw new IllegalStateException("driver " + d.id + " claimed while not idle in partition " + partition);
        idleCount--;
    }

    /** Drivers whose trip or cruise ended by `now` go idle where they are; cruising is scheduled from now. */
    public void release(double now) {
        while (!busy.isEmpty() && busy.peek().at <= now) {
            BusyEvent e = busy.poll();
            Driver d = drivers.get(e.id);
            if (d == null || d.version != e.version || d.idle()) continue;    // moved or re-claimed since: stale
            d.busyUntil = -1; d.idleSince = now;
            idle.computeIfAbsent(d.cell, c -> new HashSet<>()).add(d.id); idleCount++;
            cruise.add(new CruiseEvent(now + cruiseAfterS, d.id, d.gen));
        }
    }
}
