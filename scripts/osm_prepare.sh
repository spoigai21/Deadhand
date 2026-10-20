#!/bin/sh
# Phase 0: derive the crossing and water layers from the clipped city extract, then build OSRM (MLD).
set -e
cd "$(dirname "$0")/.."
OSM=data/osm
osmium tags-filter $OSM/nyc.osm.pbf w/bridge w/tunnel -o $OSM/bt.pbf --overwrite
osmium tags-filter $OSM/bt.pbf w/highway -o $OSM/crossings.pbf --overwrite
osmium export $OSM/crossings.pbf -o $OSM/crossings.geojson --overwrite
osmium tags-filter $OSM/nyc.osm.pbf w/natural=coastline w/waterway w/natural=water r/natural=water r/type=multipolygon -o $OSM/water_raw.pbf --overwrite
osmium tags-filter $OSM/water_raw.pbf natural=coastline natural=water waterway -o $OSM/water.pbf --overwrite
osmium export $OSM/water.pbf -o $OSM/water.geojson --overwrite
osmium tags-filter $OSM/nyc.osm.pbf w/highway -o $OSM/highways.pbf --overwrite
osmium fileinfo -e $OSM/nyc.osm.pbf | grep -E 'Number of (nodes|ways|relations)' > $OSM/counts_all.txt
osmium fileinfo -e $OSM/highways.pbf | grep -E 'Number of (nodes|ways)' > $OSM/counts_highways.txt
echo "osm layers done"
