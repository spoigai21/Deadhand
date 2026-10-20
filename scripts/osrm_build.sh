#!/bin/sh
# Build OSRM routing data (MLD pipeline) from the clipped city extract. Takes ~10 s on an M5.
set -e
cd "$(dirname "$0")/.."
IMG=ghcr.io/project-osrm/osrm-backend:v6.0.0
mkdir -p data/osrm && cp data/osm/nyc.osm.pbf data/osrm/nyc.osm.pbf
docker run --rm -t -v "$PWD/data/osrm:/data" $IMG osrm-extract -p /opt/car.lua /data/nyc.osm.pbf
docker run --rm -t -v "$PWD/data/osrm:/data" $IMG osrm-partition /data/nyc.osrm
docker run --rm -t -v "$PWD/data/osrm:/data" $IMG osrm-customize /data/nyc.osrm
