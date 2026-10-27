"""Phase 6: the extra fleets and ring tables the cost sweeps replay through the service, exported beside Phase 3's.

  data/phase3/fleet_<day>_N<n>_seed<s>.json   one per fleet size in the config (the oracle's draw, as Phase 3 exports it)
  data/phase3/cells_<day>_R<r>.json           the Phase 3 cell table with the search disk at ring r (d3 is the worker's name for it)

Usage: python -m deadhead.phase6_data configs/phase6.yaml
"""
import json, sys
from pathlib import Path
import h3, polars as pl
from .data import load_week
from .fleet import Fleet
from .phase3_service import load_config

def export(cfg):
    out = Path("data/phase3"); day, seed, res = cfg["day"], cfg["seed"], cfg["h3_resolution"]
    day_start = float(pl.Series([day]).str.to_datetime().dt.epoch("s")[0])
    week = None; made = []
    for n in cfg["fleets"]:
        p = out / f"fleet_{day}_N{n}_seed{seed}.json"
        if p.exists():
            continue
        week = week if week is not None else load_week(cfg["trips_dir"])
        fleet, _ = Fleet.from_dropoffs(week, day, n, cfg["initial_positions_window_s"], seed, res, cfg["ring_k"], cfg["cruise_after_s"], day_start)
        rows = [[d, float(fleet.lon[d]), float(fleet.lat[d]), f"{int(fleet.cell[d]):x}"] for d in range(fleet.n)]
        p.write_text(json.dumps({"day": day, "day_start": day_start, "seed": seed, "drivers": rows})); made.append(p.name)
    cells = json.loads((out / f"cells_{day}.json").read_text())
    for r in cfg["rings"]:
        p = out / f"cells_{day}_R{r}.json"
        if r == cfg["ring_k"] or p.exists():
            continue
        t = {c: {**v, "d3": sorted(h3.grid_disk(c, r), key=lambda x: int(x, 16))} for c, v in cells["cells"].items()}
        p.write_text(json.dumps({**cells, "ring": r, "cells": t})); made.append(p.name)
    return {"day": day, "fleets": cfg["fleets"], "rings": cfg["rings"], "written": made}

if __name__ == "__main__":
    print(json.dumps(export(load_config(sys.argv[1]))))
