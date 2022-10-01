#!/bin/bash
mkdir -p /data
curl -L -s "${URL}" -o /data/input.osm.pbf
RUST_LOG="info" ./rosm import -i data/input.osm.pbf -c "${CS}"
