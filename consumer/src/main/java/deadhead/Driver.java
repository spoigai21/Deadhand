package deadhead;

/** One simulated driver as a region sees it. Position is where the driver is, or will be when busyUntil passes. */
public final class Driver {
    public final int id;
    public double lon, lat;
    public String cell;          // resolution-8 cell of (lon, lat)
    public double busyUntil;     // event time; < 0 = idle now
    public double idleSince;     // event time the driver last went idle (cruising is scheduled from it)
    public int cruiseN;          // cruises so far: the deterministic target point is drawn from (id, cruiseN)
    public long version;         // strictly increasing per driver across moves; a stale record never overwrites a newer one
    public int gen;              // bumped on every claim so a stale cruise event is ignored (the oracle's generation counter)

    public Driver(int id, double lon, double lat, String cell, double busyUntil, double idleSince, int cruiseN, long version, int gen) {
        this.id = id; this.lon = lon; this.lat = lat; this.cell = cell; this.busyUntil = busyUntil; this.idleSince = idleSince;
        this.cruiseN = cruiseN; this.version = version; this.gen = gen;
    }

    public boolean idle() { return busyUntil < 0; }
}
