"""The routing layer: a thin OSRM client. Route per pair (Phase 0 decided no hex-to-hex matrix) and
one-to-many tables for a request against its candidate set. Every call returns snap distances so the
spatial-sanity test can hold the layer to a stated bound."""
from concurrent.futures import ThreadPoolExecutor
import time
import requests

class OSRM:
    def __init__(self, base="http://localhost:5001", threads=8):
        self.base = base
        self.threads = threads
        self.session = requests.Session()
        self.session.mount("http://", requests.adapters.HTTPAdapter(pool_connections=threads, pool_maxsize=threads * 2))
        # one connection per call: on a kept-alive connection through Docker's port proxy a small reply arrives 40 ms late (BUGS.md #4)
        self.session.headers["Connection"] = "close"

    @staticmethod
    def _coords(pts):
        return ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in pts)

    def _get(self, url, params, tries=5):
        """One OSRM call, retried on a dropped connection or a non-JSON reply (seen under eight concurrent replays, BUGS.md #2)."""
        for i in range(tries):
            try:
                return self.session.get(url, params=params, timeout=30).json()
            except (requests.RequestException, ValueError) as e:
                if i == tries - 1:
                    raise
                time.sleep(0.2 * (i + 1))

    def route(self, a, b, geometry=False):
        """Road distance (m) and duration (s) from a to b, plus how far each endpoint was snapped (m).
        Returns None when OSRM finds no route."""
        params = {"overview": "simplified" if geometry else "false", "geometries": "geojson"}
        r = self._get(f"{self.base}/route/v1/driving/{self._coords([a, b])}", params)
        if r.get("code") != "Ok":
            return None
        rt = r["routes"][0]
        out = {"road_m": rt["distance"], "road_s": rt["duration"],
               "snap_a_m": r["waypoints"][0]["distance"], "snap_b_m": r["waypoints"][1]["distance"],
               "road_a": r["waypoints"][0].get("name", ""), "road_b": r["waypoints"][1].get("name", "")}
        if geometry:
            out["geometry"] = rt["geometry"]["coordinates"]
        return out

    def routes(self, pairs, geometry=False):
        with ThreadPoolExecutor(self.threads) as ex:
            return list(ex.map(lambda p: self.route(p[0], p[1], geometry), pairs))

    def table_many_to_one(self, sources, dest):
        """Road duration and distance from every source to one destination. None entries are unroutable."""
        pts = list(sources) + [dest]
        n = len(sources)
        params = {"annotations": "duration,distance", "sources": ";".join(map(str, range(n))), "destinations": str(n)}
        r = self._get(f"{self.base}/table/v1/driving/{self._coords(pts)}", params)
        if r.get("code") != "Ok":
            return None
        return {"road_s": [row[0] for row in r["durations"]], "road_m": [row[0] for row in r["distances"]],
                "snap_src_m": [w["distance"] for w in r["sources"]], "snap_dst_m": r["destinations"][0]["distance"],
                "road_src": [w.get("name", "") for w in r["sources"]], "road_dst": r["destinations"][0].get("name", "")}

    def table_many_to_many(self, sources, dests, max_locations=1000):
        """Road duration and distance from every source to every destination: two len(sources) x len(dests) lists, None where
        unroutable. osrm-routed caps the sources and the destinations of one call (--max-table-size, 1000 here); a batch
        beyond that is split into blocks and `calls` says how many calls it took (one for every batch in the runs)."""
        S, D = len(sources), len(dests)
        road_s = [[None] * D for _ in range(S)]; road_m = [[None] * D for _ in range(S)]
        calls = 0
        for d0 in range(0, D, max_locations):
            dd = dests[d0:d0 + max_locations]
            for s0 in range(0, S, max_locations):
                ss = sources[s0:s0 + max_locations]
                pts = list(ss) + list(dd)
                params = {"annotations": "duration,distance", "sources": ";".join(map(str, range(len(ss)))), "destinations": ";".join(map(str, range(len(ss), len(pts))))}
                r = self._get(f"{self.base}/table/v1/driving/{self._coords(pts)}", params); calls += 1
                if r.get("code") != "Ok":
                    raise RuntimeError(f"OSRM table: {r}")
                for i, row in enumerate(r["durations"]):
                    road_s[s0 + i][d0:d0 + len(dd)] = row
                for i, row in enumerate(r["distances"]):
                    road_m[s0 + i][d0:d0 + len(dd)] = row
        return {"road_s": road_s, "road_m": road_m, "calls": calls}

    def tables(self, jobs):
        with ThreadPoolExecutor(self.threads) as ex:
            return list(ex.map(lambda j: self.table_many_to_one(*j), jobs))
