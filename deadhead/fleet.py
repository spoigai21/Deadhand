"""The simulated supply (ASSUMPTIONS.md). N drivers; positions from real dropoffs; idle at the dropoff after a trip.
State lives in memory in the matcher. A claim removes the driver from the idle index at once."""
import heapq
import numpy as np, polars as pl
from .data import DO_DT
from .geo import haversine_m, cells, disk

class Fleet:
    def __init__(self, lons, lats, res, ring_k, cruise_after_s=None, start_t=0.0):
        self.n = len(lons)
        self.lon = np.asarray(lons, dtype=float).copy(); self.lat = np.asarray(lats, dtype=float).copy()
        self.res, self.ring_k = res, ring_k
        self.cell = cells(self.lon, self.lat, res)
        self.busy_until = np.full(self.n, -1.0)
        self.idle_by_cell = {}
        for d in range(self.n):
            self.idle_by_cell.setdefault(int(self.cell[d]), set()).add(d)
        self.heap = []            # (busy_until, driver, dropoff lon, dropoff lat)
        self._disk_cache = {}; self._disk_cache_r = {}
        self.claims = 0
        # repositioning (ASSUMPTIONS.md): a driver idle for cruise_after_s drives toward demand; None = idle at the dropoff forever
        self.cruise_after_s = cruise_after_s
        self.cruise_heap = []     # (cruise_at, driver, generation)
        self.generation = np.zeros(self.n, dtype=np.int64)   # bumped on every claim so a stale cruise event is ignored
        self.cruises = 0; self.cruise_m = 0.0; self.cruise_s = 0.0; self.stays = 0
        self.cruise_n = np.zeros(self.n, dtype=np.int64)   # cruises so far per driver; the deterministic target point is drawn from it
        if cruise_after_s is not None:
            for d in range(self.n):
                heapq.heappush(self.cruise_heap, (start_t + cruise_after_s, d, 0))

    @classmethod
    def from_dropoffs(cls, week: pl.DataFrame, day: str, n: int, window_s: int, seed: int, res: int, ring_k: int, cruise_after_s=None, start_t=0.0):
        """Initial positions: real dropoffs from the window_s before the day starts (the hour before midnight)."""
        start = pl.lit(day).str.to_datetime() - pl.duration(seconds=window_s)
        end = pl.lit(day).str.to_datetime()
        pool = week.filter((pl.col(DO_DT) >= start) & (pl.col(DO_DT) < end))
        rng = np.random.default_rng(seed)
        idx = rng.choice(pool.height, size=n, replace=pool.height < n)
        return cls(pool["dropoff_longitude"].to_numpy()[idx], pool["dropoff_latitude"].to_numpy()[idx], res, ring_k, cruise_after_s, start_t), pool.height

    def release(self, t):
        """Drivers whose trip ended by t go idle at their dropoff."""
        while self.heap and self.heap[0][0] <= t:
            _, d, lon, lat = heapq.heappop(self.heap)
            self.lon[d], self.lat[d] = lon, lat
            self.cell[d] = cells([lon], [lat], self.res)[0]
            self.busy_until[d] = -1.0
            self.idle_by_cell.setdefault(int(self.cell[d]), set()).add(d)
            if self.cruise_after_s is not None:
                heapq.heappush(self.cruise_heap, (t + self.cruise_after_s, d, int(self.generation[d])))

    def cruise(self, t, sample_target, route):
        """Drivers idle since before t - cruise_after_s drive toward demand: sample_target(t, cell, driver) -> (lon, lat) in the
        busiest nearby cell of the previous hour, or None to stay put. They are unavailable while driving and go idle at
        the target. route(a, b) -> (road_m, road_s) or None."""
        while self.cruise_heap and self.cruise_heap[0][0] <= t:
            _, d, gen = heapq.heappop(self.cruise_heap)
            if gen != self.generation[d] or self.busy_until[d] >= 0:
                continue                      # claimed since, or already moving
            s = self.idle_by_cell.get(int(self.cell[d]))
            if s is None or d not in s:
                continue
            target = sample_target(t, int(self.cell[d]), d)
            if target is None:                # already in the busiest cell around: stay, look again later
                heapq.heappush(self.cruise_heap, (t + self.cruise_after_s, d, gen)); self.stays += 1
                continue
            lon, lat = target
            r = route((self.lon[d], self.lat[d]), (lon, lat))
            road_m, road_s = r if r else (float(haversine_m(self.lon[d], self.lat[d], lon, lat)) * 1.3, float(haversine_m(self.lon[d], self.lat[d], lon, lat)) * 1.3 / 8.0)
            s.discard(d)
            self.busy_until[d] = t + road_s
            heapq.heappush(self.heap, (t + road_s, d, lon, lat))
            self.cruises += 1; self.cruise_m += road_m; self.cruise_s += road_s; self.cruise_n[d] += 1

    def ring(self, cell):
        c = int(cell)
        if c not in self._disk_cache:
            self._disk_cache[c] = disk(c, self.ring_k)
        return self._disk_cache[c]

    def ring_r(self, cell, k):
        key = (int(cell), k)
        if key not in self._disk_cache_r:
            self._disk_cache_r[key] = disk(cell, k)
        return self._disk_cache_r[key]

    def candidates(self, pu_lon, pu_lat, pu_cell, k, ring=None):
        """Idle drivers inside the ring (ring_k, or `ring` rings), the nearest k by straight line, sorted by (distance, id). Returns (ids, hav_m)."""
        ids = []
        for c in (self.ring(pu_cell) if ring is None else self.ring_r(pu_cell, ring)):
            s = self.idle_by_cell.get(c)
            if s:
                ids.extend(s)
        if not ids:
            return np.empty(0, dtype=int), np.empty(0)
        ids = np.array(sorted(ids))
        hav = haversine_m(self.lon[ids], self.lat[ids], pu_lon, pu_lat)
        order = np.lexsort((ids, hav))[:k]
        return ids[order], hav[order]

    def claim(self, d, busy_until, do_lon, do_lat):
        s = self.idle_by_cell[int(self.cell[d])]
        assert d in s, f"driver {d} claimed while not idle"   # the one-claim rule, enforced
        s.discard(d)
        self.busy_until[d] = busy_until
        self.generation[d] += 1
        heapq.heappush(self.heap, (busy_until, d, do_lon, do_lat))
        self.claims += 1
