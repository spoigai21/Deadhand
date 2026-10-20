"""The routing layer: a thin OSRM client. Route per pair (Phase 0 decided no hex-to-hex matrix) and
one-to-many tables for a request against its candidate set. Every call returns snap distances so the
spatial-sanity test can hold the layer to a stated bound."""
from concurrent.futures import ThreadPoolExecutor
import requests

class OSRM:
    def __init__(self, base="http://localhost:5001", threads=8):
        self.base = base
        self.threads = threads
        self.session = requests.Session()
        self.session.mount("http://", requests.adapters.HTTPAdapter(pool_connections=threads, pool_maxsize=threads * 2))

    @staticmethod
    def _coords(pts):
        return ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in pts)

    def route(self, a, b, geometry=False):
        """Road distance (m) and duration (s) from a to b, plus how far each endpoint was snapped (m).
        Returns None when OSRM finds no route."""
        params = {"overview": "simplified" if geometry else "false", "geometries": "geojson"}
        r = self.session.get(f"{self.base}/route/v1/driving/{self._coords([a, b])}", params=params).json()
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
        r = self.session.get(f"{self.base}/table/v1/driving/{self._coords(pts)}", params=params).json()
        if r.get("code") != "Ok":
            return None
        return {"road_s": [row[0] for row in r["durations"]], "road_m": [row[0] for row in r["distances"]],
                "snap_src_m": [w["distance"] for w in r["sources"]], "snap_dst_m": r["destinations"][0]["distance"],
                "road_src": [w.get("name", "") for w in r["sources"]], "road_dst": r["destinations"][0].get("name", "")}

    def tables(self, jobs):
        with ThreadPoolExecutor(self.threads) as ex:
            return list(ex.map(lambda j: self.table_many_to_one(*j), jobs))
