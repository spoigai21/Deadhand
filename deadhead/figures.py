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

# ---------- Phase 2 ----------

def fig_calibration(d, out):
    cal = d["fleet_calibration"]
    if len(cal) < 2:
        return
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.3))
    xs = [c["fleet_size"] for c in cal]
    ax = axes[0]
    ax.plot(xs, [100 * c["match_rate"] for c in cal], "-o", color=BLUE, lw=2, ms=6, mec=SURFACE)
    for x, c in zip(xs, cal):
        ax.annotate(f"{100 * c['match_rate']:.1f}%", (x, 100 * c["match_rate"]), xytext=(0, 7), textcoords="offset points", ha="center", fontsize=7.5, color=INK2)
    ax.axhspan(90, 95, color="#ecebe6", zorder=0); ax.set_ylabel("requests matched inside 600 s, %"); ax.set_xlabel("simulated fleet size")
    ax.set_title("Fleet size calibration: the road-aware rule, Thursday, seed 0", loc="left", fontsize=9.5)
    ax = axes[1]
    ax.plot(xs, [c["wait_s_median"] for c in cal], "-o", color=BLUE, lw=2, ms=6, mec=SURFACE, label="median wait")
    ax.plot(xs, [c["wait_s_p90"] for c in cal], "-o", color=ORANGE, lw=2, ms=6, mec=SURFACE, label="p90 wait")
    ax.set_ylabel("rider wait, s (free-flow pickup leg)"); ax.set_xlabel("simulated fleet size"); ax.legend(frameon=False, fontsize=8)
    ax.set_title("Wait against fleet size", loc="left", fontsize=9.5)
    fig.suptitle("Supply is simulated (ASSUMPTIONS.md). The shaded band is the 90–95% target the baseline fleet is chosen in.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase2_calibration.png", dpi=160); plt.close(fig)

def fig_wrong_driver_by_hour(d, out):
    days = [day for day in d["mode_a"] if "breakdowns_road_replay" in d["mode_a"][day]]
    if not days:
        return
    fig, axes = plt.subplots(1, 2, figsize=(9.5, 3.4))
    for day, color in zip(days, (BLUE, ORANGE)):
        bh = d["mode_a"][day]["breakdowns_road_replay"]["by_hour"]
        hours = sorted(int(h) for h in bh)
        axes[0].plot(hours, [100 * bh[str(h)]["wrong_driver_rate"] for h in hours], "-o", color=color, lw=2, ms=4, mec=SURFACE, label=DAYNAME.get(day, day))
        axes[1].plot(hours, [bh[str(h)]["conditional_extra_wait_s_median"] for h in hours], "-o", color=color, lw=2, ms=4, mec=SURFACE, label=DAYNAME.get(day, day))
    axes[0].set_ylabel("wrong-driver rate, %"); axes[0].set_ylim(0, None)
    axes[1].set_ylabel("median extra wait, s"); axes[1].set_ylim(0, None)
    for ax, t in zip(axes, ("Wrong-driver rate by hour", "Conditional cost by hour")):
        ax.set_xlabel("hour of day"); ax.set_xticks(range(0, 24, 3)); ax.set_title(t, loc="left", fontsize=9.5)
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("Finding 1, Mode A: measured in the road-aware replay, pooled over seeds. Free-flow routing; supply simulated.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase2_wrong_driver_by_hour.png", dpi=160); plt.close(fig)

def fig_wrong_driver_map(d, out, crossings_path):
    day = next((day for day in d["mode_a"] if "breakdowns_road_replay" in d["mode_a"][day]), None)
    if day is None:
        return
    cells = d["mode_a"][day]["breakdowns_road_replay"]["by_cell"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 7))
    for ax, key, label, vmax_q in zip(axes, ("wrong_driver_rate", "conditional_extra_wait_s_median"), ("share of decisions where the straight line picks a slower driver", "median extra wait on those decisions, s"), (0.98, 0.95)):
        vals = np.array([c[key] if c[key] is not None else np.nan for c in cells]); okm = ~np.isnan(vals)
        polys = [boundary_lonlat(int(c["cell"], 16)) for c, k in zip(cells, okm) if k]
        draw_water(ax); ax.add_collection(LineCollection(crossings_lines(crossings_path), colors="#b8b7b1", linewidths=0.8, zorder=1))
        pc = PolyCollection(polys, array=vals[okm], cmap=RAMP, norm=Normalize(vmin=float(np.nanquantile(vals, 0.02)), vmax=float(np.nanquantile(vals, vmax_q))), edgecolors=SURFACE, linewidths=0.3, zorder=2)
        ax.add_collection(pc)
        lons = [c["lon"] for c in cells]; lats = [c["lat"] for c in cells]
        ax.set_xlim(min(lons) - 0.01, max(lons) + 0.01); ax.set_ylim(min(lats) - 0.01, max(lats) + 0.01)
        ax.set_aspect(1 / np.cos(np.radians(40.75))); ax.grid(False); ax.set_xticks([]); ax.set_yticks([])
        for sp in ax.spines.values(): sp.set_visible(False)
        cb = fig.colorbar(pc, ax=ax, shrink=0.45, pad=0.01); cb.set_label(label, fontsize=8); cb.outline.set_visible(False)
        ax.set_title(label[0].upper() + label[1:], loc="left", fontsize=9)
    fig.suptitle(f"Finding 1 on the map: {DAYNAME.get(day, day)}, H3 resolution 8, cells with at least 30 decisions, pooled over seeds. Shaded: water; grey: crossings.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase2_wrong_driver_map.png", dpi=160); plt.close(fig)

def fig_mode_b(d, out):
    days = list(d["mode_b"])
    if not days:
        return
    metrics = [("wait_s_median", "median wait, s"), ("wait_s_p90", "p90 wait, s"), ("deadhead_m_per_ride_median", "median empty metres per ride"), ("match_rate", "match rate")]
    fig, axes = plt.subplots(1, len(metrics), figsize=(11, 3.2))
    for ax, (key, label) in zip(axes, metrics):
        x = np.arange(len(days)); w = 0.36
        for j, (rule, color, name) in enumerate((("straight", ORANGE, "straight-line rule"), ("road", BLUE, "road-aware rule"))):
            vals = [d["mode_b"][day].get(rule, {}).get(key, {}) for day in days]
            means = [v.get("mean", 0) if v else 0 for v in vals]
            bars = ax.bar(x + (j - 0.5) * w, means, w * 0.94, color=color, label=name)
            for b_, v in zip(bars, vals):
                if v:
                    ax.errorbar(b_.get_x() + b_.get_width() / 2, v["mean"], yerr=[[v["mean"] - v["min"]], [v["max"] - v["mean"]]], fmt="none", ecolor=INK2, elinewidth=1, capsize=2)
                    ax.annotate(f"{v['mean']:.2f}" if key == "match_rate" else f"{v['mean']:.0f}", (b_.get_x() + b_.get_width() / 2, v["mean"]), xytext=(0, 4), textcoords="offset points", ha="center", fontsize=7.5, color=INK2)
        ax.set_xticks(x); ax.set_xticklabels([DAYNAME.get(day, day).split(" ")[0] for day in days]); ax.set_title(label, loc="left", fontsize=9.5); ax.set_ylim(0, None)
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("Finding 1, Mode B: each rule replayed end to end. Bars are the mean over seeds; whiskers the min and max. Supply simulated; free-flow routing.", fontsize=9, color=INK2, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(out / "phase2_mode_b.png", dpi=160); plt.close(fig)

def main(results="results", figures="figures"):
    out = Path(figures); out.mkdir(exist_ok=True)
    files = sorted(glob.glob(f"{results}/phase1/disagreement_cpu-apple-m5_seed*.json"))
    if files:
        d = json.load(open(files[0]))
        crossings = d["config"]["crossings_geojson"]
        fig_distributions(d, out); fig_legs_by_distance(d, out); fig_cell_maps(d, out, crossings); fig_worst_pairs(d, out, crossings)
        print("phase1 figures from", files[0])
    files = sorted(glob.glob(f"{results}/phase2/finding1_cpu-apple-m5*.json"))
    if files:
        d = json.load(open(files[0]))
        fig_calibration(d, out); fig_wrong_driver_by_hour(d, out); fig_wrong_driver_map(d, out, d["config"]["crossings_geojson"]); fig_mode_b(d, out)
        print("phase2 figures from", files[0])

if __name__ == "__main__":
    main(*sys.argv[1:])
