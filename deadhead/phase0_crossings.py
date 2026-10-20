"""Phase 0: list every drivable bridge/tunnel way that crosses water, named. Computed, not hand-picked.

Inputs are GeoJSON exports from the clipped OSM file (see scripts/osm_prepare.sh):
  crossings.geojson  ways with a highway tag AND a bridge or tunnel tag
  water.geojson      coastline lines, waterway lines, natural=water areas
A way is a water crossing if its geometry intersects any water geometry.
Usage: python -m deadhead.phase0_crossings data/osm results/phase0
"""
import json, sys
from pathlib import Path
import geopandas as gpd
from shapely.ops import unary_union

DRIVABLE = {"motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential",
            "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link", "living_street"}

def main(osmdir, outdir):
    osmdir, outdir = Path(osmdir), Path(outdir)
    cr = gpd.read_file(osmdir / "crossings.geojson")
    wa = gpd.read_file(osmdir / "water.geojson")
    cr = cr[cr.geometry.type.isin(["LineString", "MultiLineString"])]
    cr = cr[cr["highway"].isin(DRIVABLE)]
    water_lines = wa[wa.geometry.type.isin(["LineString", "MultiLineString"])].geometry
    water_areas = wa[wa.geometry.type.isin(["Polygon", "MultiPolygon"])].geometry
    water = unary_union(list(water_lines) + [g.boundary for g in water_areas])
    hits = cr[cr.geometry.intersects(water)].copy()
    kind = hits.apply(lambda r: "tunnel" if str(r.get("tunnel", "")).lower() not in ("", "no", "nan", "none") else "bridge", axis=1)
    hits["kind"] = kind
    hits["name"] = hits["name"].fillna("(unnamed)")
    # metres: project to a local metric CRS for lengths
    hits_m = hits.to_crs(32618)
    hits["length_m"] = hits_m.geometry.length.round(0)
    named = (hits.groupby(["name", "kind"]).agg(ways=("name", "size"), length_m=("length_m", "sum"),
             highway=("highway", lambda s: ",".join(sorted(set(s))))).reset_index().sort_values("length_m", ascending=False))
    out = {
        "drivable_bridge_or_tunnel_ways": int(len(cr)),
        "water_crossing_ways": int(len(hits)),
        "named_water_crossings": int((named["name"] != "(unnamed)").sum()),
        "crossings": [dict(name=r["name"], kind=r["kind"], ways=int(r["ways"]), length_m=float(r["length_m"]), highway=r["highway"]) for _, r in named.iterrows()],
    }
    (outdir / "water_crossings_cpu-apple-m5_seed0.json").write_text(json.dumps(out, indent=2))
    hits.to_file(outdir / "water_crossings.geojson", driver="GeoJSON")
    print(f"drivable bridge/tunnel ways: {len(cr)}; crossing water: {len(hits)}; named crossings: {out['named_water_crossings']}")
    for c in out["crossings"][:25]:
        print(f"  {c['kind']:6s} {c['length_m']:7.0f} m  {c['name']}")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
