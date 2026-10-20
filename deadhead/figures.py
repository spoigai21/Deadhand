"""Every chart in the README, regenerated from results/ by this one script.

Usage: python -m deadhead.figures results figures
"""
import json, sys, glob
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection, LineCollection
from matplotlib.colors import LinearSegmentedColormap, Normalize
from .geo import boundary_lonlat

# palette: two categorical slots, one sequential blue ramp, text and surface tokens
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb"
RAMP = LinearSegmentedColormap.from_list("blue", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])
plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
                     "axes.titlecolor": INK, "axes.spines.top": False, "axes.spines.right": False, "figure.facecolor": SURFACE,
                     "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6})
DAYNAME = {"2016-06-16": "Thursday 16 June 2016", "2016-06-18": "Saturday 18 June 2016"}

def cdf(h):
    edges, counts = np.array(h["edges"] + [5.5]), np.array(h["counts"], dtype=float)
    return edges[1:], np.cumsum(counts) / counts.sum()

def fig_distributions(d, out):
    days = list(d["days"])
    fig, axes = plt.subplots(len(days), 2, figsize=(9, 3.2 * len(days)), sharex=True, sharey=True)
    axes = np.atleast_2d(axes)
    for i, day in enumerate(days):
        for j, (key, label) in enumerate((("hist_distance_ratio", "road distance ÷ straight-line distance"), ("hist_time_ratio", "road time ÷ straight-line time"))):
            ax = axes[i, j]
            for kind, color, name in (("trips", BLUE, "real trips (pickup → dropoff)"), ("legs", ORANGE, "simulated pickup legs (driver → pickup)")):
                x, y = cdf(d["days"][day][kind][key])
                ax.step(x, y, where="post", color=color, lw=2, label=name)
                med = d["days"][day][kind][key.replace("hist_", "")]["median"]
                ax.plot([med], [0.5], "o", ms=7, color=color, mec=SURFACE, mew=1.5)
                ax.annotate(f"median {med:.2f}", (med, 0.5), xytext=(6, -12 if kind == "trips" else 6), textcoords="offset points", color=INK2, fontsize=8)
            ax.set_xlim(1, 5.5); ax.set_ylim(0, 1)
            ax.set_title(f"{DAYNAME.get(day, day)}: {label}", fontsize=9.5, loc="left")
            if i == len(days) - 1: ax.set_xlabel("ratio (last bin is open)")
            if j == 0: ax.set_ylabel("share of pairs at or below")
    axes[0, 0].legend(loc="lower right", frameon=False, fontsize=8)
    fig.suptitle("How far the road disagrees with the straight line. OSRM free-flow, H3 res 8 ring 3, idle fraction 0.2, seed 0, Apple M5.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase1_ratio_distributions.png", dpi=160); plt.close(fig)

def fig_legs_by_distance(d, out):
    days = list(d["days"])
    fig, axes = plt.subplots(1, len(days), figsize=(9, 3.4), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, day in zip(axes, days):
        series = [("0.2 (baseline)", d["days"][day]["legs"]["by_straight_line_band"], BLUE)]
        for f, s in sorted(d["days"][day]["legs_sensitivity"].items()):
            series.append((f, s["by_straight_line_band"], ORANGE if f == "0.1" else AQUA))
        labels = [f"{b['band_m'][0]}–{b['band_m'][1] if b['band_m'][1] < 50000 else '∞'}" for b in series[0][1]]
        xs = np.arange(len(labels))
        for name, bands, color in series:
            y = [b["median_dist_ratio"] for b in bands][:len(labels)]
            ax.plot(xs[:len(y)], y, "-o", color=color, lw=2, ms=6, mec=SURFACE, mew=1, label=f"idle fraction {name}")
            if name.startswith("0.2"):
                for xi, yi in zip(xs[:len(y)], y):
                    ax.annotate(f"{yi:.2f}", (xi, yi), xytext=(0, 7), textcoords="offset points", fontsize=7.5, color=INK2, ha="center")
        ax.set_xticks(xs); ax.set_xticklabels(labels, rotation=0); ax.set_xlabel("straight-line distance of the leg, m")
        ax.set_title(DAYNAME.get(day, day), loc="left", fontsize=9.5)
        ax.axhline(1, color=GRID, lw=1)
    axes[0].set_ylabel("median road ÷ straight-line distance")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("Pickup legs: the shorter the leg, the worse the straight line. Median by distance band at three supply densities.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase1_legs_by_distance.png", dpi=160); plt.close(fig)

WATER_BG = Path("data/osm/water.geojson")  # Phase 0 water geometry; a map background only, drawn if the data directory is present

def draw_water(ax):
    if not WATER_BG.exists():
        return
    feats = json.load(open(WATER_BG))["features"]
    coast = [np.array(f["geometry"]["coordinates"]) for f in feats if f["geometry"]["type"] == "LineString" and f["properties"].get("natural") == "coastline"]
    ax.add_collection(LineCollection(coast, colors="#c9c7c0", linewidths=0.7, zorder=0))
    polys = [np.array(ring) for f in feats if f["geometry"]["type"] == "MultiPolygon" and f["properties"].get("natural") == "water"
             for poly in f["geometry"]["coordinates"] for ring in poly[:1]]
    ax.add_collection(PolyCollection(polys, facecolors="#ecebe6", edgecolors="none", zorder=0))

def crossings_lines(path):
    feats = json.load(open(path))["features"]
    return [np.array(f["geometry"]["coordinates"]) for f in feats if f["geometry"]["type"] == "LineString"]

def fig_cell_maps(d, out, crossings_path):
    day = list(d["days"])[0]
    for kind, title in (("trips", "real trips, by pickup cell"), ("legs", "simulated pickup legs, by pickup cell")):
        cells = d["days"][day][kind]["by_cell"]
        polys = [boundary_lonlat(int(c["cell"], 16)) for c in cells]
        vals = np.array([c["median_dist_ratio"] for c in cells])
        fig, ax = plt.subplots(figsize=(7.5, 8))
        draw_water(ax)
        ax.add_collection(LineCollection(crossings_lines(crossings_path), colors="#b8b7b1", linewidths=0.8, zorder=1))
        vmax = float(np.quantile(vals, 0.98))
        vmin = 1.0 if kind == "legs" else float(np.quantile(vals, 0.02))
        pc = PolyCollection(polys, array=vals, cmap=RAMP, norm=Normalize(vmin=vmin, vmax=vmax), edgecolors=SURFACE, linewidths=0.3, zorder=2)
        ax.add_collection(pc)
        lons = [c["lon"] for c in cells]; lats = [c["lat"] for c in cells]
        ax.set_xlim(min(lons) - 0.01, max(lons) + 0.01); ax.set_ylim(min(lats) - 0.01, max(lats) + 0.01)
        ax.set_aspect(1 / np.cos(np.radians(40.75))); ax.grid(False); ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values(): s.set_visible(False)
        cb = fig.colorbar(pc, ax=ax, shrink=0.5, pad=0.01); cb.set_label("median road ÷ straight-line distance"); cb.outline.set_visible(False)
        ax.set_title(f"Median road ÷ straight-line distance: {title}", loc="left", fontsize=10)
        fig.suptitle(f"{DAYNAME.get(day, day)}. H3 resolution 8; the {len(cells)} cells with at least 20 pairs.\nGrey lines: the 390 water-crossing ways from Phase 0; shaded: water. OSRM free-flow, idle fraction 0.2, seed 0, Apple M5.", fontsize=8.5, color=INK2, x=0.01, ha="left", y=0.995)
        fig.tight_layout(rect=(0, 0, 1, 0.96)); fig.savefig(out / f"phase1_cell_map_{kind}.png", dpi=160); plt.close(fig)

def fig_worst_pairs(d, out, crossings_path):
    geoms = d["worst_pair_geometries"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 6.5))
    for ax, kind, title in zip(axes, ("legs", "trips"), ("worst 15 simulated pickup legs", "worst 15 real trips")):
        g = [x for x in geoms if x["kind"] == kind and x["geometry"]]
        draw_water(ax)
        ax.add_collection(LineCollection(crossings_lines(crossings_path), colors="#b8b7b1", linewidths=0.8, zorder=1))
        seen = set()
        for i, x in enumerate(g):
            geo = np.array(x["geometry"])
            ax.plot(geo[:, 0], geo[:, 1], color=ORANGE if kind == "legs" else BLUE, lw=1.6, zorder=3)
            ax.plot([x["a"][0], x["b"][0]], [x["a"][1], x["b"][1]], ls="--", color=INK2, lw=0.9, zorder=2)
            ax.plot(*x["b"], "o", ms=5, color=INK, mec=SURFACE, mew=1, zorder=4)
            why = ", ".join(x["crossings"].split("|")[:2]) if x["crossings"] else f"{x['road_a']} → {x['road_b']}".strip(" →")
            if len(seen) < 7 and why not in seen:
                seen.add(why)
                ax.annotate(f"{x['dist_ratio']:.0f}× {why}", (geo[len(geo) // 2, 0], geo[len(geo) // 2, 1]), fontsize=6.5, color=INK,
                            xytext=(8, 14 - 14 * (len(seen) % 3)), textcoords="offset points", bbox=dict(boxstyle="round,pad=0.15", fc=SURFACE, ec="none", alpha=0.85), zorder=5)
        pts = np.vstack([np.array(x["geometry"]) for x in g])
        pad = 0.02 if kind == "legs" else 0.03
        ax.set_xlim(pts[:, 0].min() - pad, pts[:, 0].max() + pad); ax.set_ylim(pts[:, 1].min() - pad, pts[:, 1].max() + pad)
        ax.set_aspect(1 / np.cos(np.radians(40.75))); ax.grid(False); ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values(): s.set_visible(False)
        ax.set_title(f"{title}: road (solid), straight line (dashed), pickup (dot)\nairport pairs excluded; one leg per request; the seven worst distinct causes labelled", loc="left", fontsize=8.5)
    fig.suptitle(f"The worst pairs by road ÷ straight-line distance, {DAYNAME.get(list(d['days'])[0])}, labelled with the water crossing on the route or, if none, the roads at each end.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase1_worst_pairs.png", dpi=160); plt.close(fig)

def main(results="results", figures="figures"):
    out = Path(figures); out.mkdir(exist_ok=True)
    files = sorted(glob.glob(f"{results}/phase1/disagreement_cpu-apple-m5_seed*.json"))
    if files:
        d = json.load(open(files[0]))
        crossings = d["config"]["crossings_geojson"]
        fig_distributions(d, out); fig_legs_by_distance(d, out); fig_cell_maps(d, out, crossings); fig_worst_pairs(d, out, crossings)
        print("phase1 figures from", files[0])

if __name__ == "__main__":
    main(*sys.argv[1:])
