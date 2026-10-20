#!/bin/sh
# Clip the Geofabrik New York State extract to the city bounding box used everywhere in this project.
set -e
cd "$(dirname "$0")/.."
osmium extract --bbox -74.27,40.49,-73.68,40.93 --strategy complete_ways -o data/osm/nyc.osm.pbf --overwrite data/osm/new-york-latest.osm.pbf
